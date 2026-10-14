//! Long-lived agent identity and serial message decisions; execution lives in Run.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[path = "actors/task.rs"]
mod task;
pub use task::*;
#[path = "actors/queue.rs"]
mod queue;
pub use queue::*;
#[path = "actors/registered.rs"]
mod registered;
pub use registered::*;
#[path = "actors/memory.rs"]
mod memory;
pub use memory::*;
#[path = "actors/reminder.rs"]
mod reminder;
pub use reminder::*;
#[path = "actors/supervision.rs"]
mod supervision;
pub use supervision::*;

pub const RUN_COMPLETED: &str = "$run_completed";
pub const MESSAGE_REJECTED: &str = "$message_rejected";
pub const SUPERVISION_RECOVERED: &str = "$supervision_recovered";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMessage {
    pub id: MessageId,
    pub recipient: AgentId,
    pub sender: Option<AgentId>,
    pub correlation_id: Option<MessageId>,
    pub kind: String,
    pub payload: Value,
}
impl AgentMessage {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.kind.trim().is_empty() || self.kind.len() > 128 {
            return Err(DomainError::InvalidConfiguration(
                "message kind requires 1..128 bytes".into(),
            ));
        }
        Ok(())
    }
    pub fn validate_public(&self) -> Result<(), DomainError> {
        self.validate()?;
        if self.kind.starts_with('$') || self.payload.to_string().len() > 65_536 {
            return Err(DomainError::InvalidConfiguration(
                "public messages require an unreserved kind and at most 65536 payload bytes".into(),
            ));
        }
        Ok(())
    }
    pub fn correlation(&self) -> MessageId {
        self.correlation_id.unwrap_or(self.id)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Ready,
    Paused,
    Blocked,
}
impl AgentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Paused => "paused",
            Self::Blocked => "blocked",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentRecord {
    pub id: AgentId,
    pub definition: DefinitionId,
    pub config: Value,
    #[serde(default)]
    pub queue_policy: AgentQueuePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervision: Option<ActorSupervision>,
    pub state: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_reminders: Vec<MessageId>,
    pub status: AgentStatus,
    pub revision: u64,
    pub last_message_sequence: i64,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}
#[derive(Clone, Debug, Serialize)]
pub struct Agent {
    pub(crate) record: AgentRecord,
}
impl Agent {
    pub fn new(
        id: AgentId,
        definition: &dyn AgentDefinition,
        config: Value,
        now: i64,
    ) -> Result<Self, DomainError> {
        Self::new_with_queue_policy(id, definition, config, AgentQueuePolicy::default(), now)
    }
    pub fn new_with_queue_policy(
        id: AgentId,
        definition: &dyn AgentDefinition,
        config: Value,
        queue_policy: AgentQueuePolicy,
        now: i64,
    ) -> Result<Self, DomainError> {
        Self::new_with_policies(id, definition, config, queue_policy, None, now)
    }
    pub fn new_with_policies(
        id: AgentId,
        definition: &dyn AgentDefinition,
        config: Value,
        queue_policy: AgentQueuePolicy,
        supervision: Option<SupervisionPolicy>,
        now: i64,
    ) -> Result<Self, DomainError> {
        queue_policy.validate()?;
        if supervision.is_some() && !definition.supports_supervision() {
            return Err(DomainError::InvalidConfiguration(
                "actor behavior does not support supervised queue suspension".into(),
            ));
        }
        let supervision = supervision.map(ActorSupervision::new).transpose()?;
        let pinned = definition.id();
        if pinned.name.trim().is_empty() || pinned.version == 0 {
            return Err(DomainError::InvalidState);
        }
        let state = definition.initialize(&config)?;
        Ok(Self {
            record: AgentRecord {
                id,
                definition: pinned,
                config,
                queue_policy,
                supervision,
                state,
                pending_reminders: vec![],
                status: AgentStatus::Ready,
                revision: 0,
                last_message_sequence: 0,
                error: None,
                created_at: now,
                updated_at: now,
            },
        })
    }
    pub fn record(&self) -> &AgentRecord {
        &self.record
    }
    pub fn decide(
        &self,
        definition: &dyn AgentDefinition,
        message: &AgentMessage,
    ) -> Result<AgentTransition, DomainError> {
        if self.record.status != AgentStatus::Ready
            || message.recipient != self.record.id
            || definition.id() != self.record.definition
        {
            return Err(DomainError::InvalidState);
        }
        message.validate()?;
        let suspend_next = self
            .record
            .supervision
            .as_ref()
            .is_some_and(|h| h.pauses_on(message));
        let transition = definition.transition_supervised(
            &self.record.config,
            &self.record.state,
            message,
            suspend_next,
        )?;
        if suspend_next
            && transition
                .commands
                .iter()
                .any(|c| matches!(c, AgentCommand::StartRun { .. }))
        {
            return Err(DomainError::InvalidWorkflow(
                "supervision pause must retain queued work".into(),
            ));
        }
        if transition.commands.len() > 16
            || transition
                .commands
                .iter()
                .filter(|c| matches!(c, AgentCommand::StartRun { .. }))
                .count()
                > 1
        {
            return Err(DomainError::InvalidConfiguration(
                "a message may emit at most 16 commands and start at most one run".into(),
            ));
        }
        Ok(transition)
    }
    pub(crate) fn processed(
        &mut self,
        state: Option<Value>,
        message: &AgentMessage,
        sequence: i64,
        now: i64,
    ) -> Result<(), DomainError> {
        // Held tasks can follow newer runtime receipts. The store admits only queued
        // envelopes and commits their processed status with this actor revision.
        if self.record.status != AgentStatus::Ready
            || sequence <= 0
            || (sequence <= self.record.last_message_sequence && message.kind != "task")
        {
            return Err(DomainError::InvalidState);
        }
        if let Some(state) = state {
            self.record.state = state;
            if let Some(health) = &mut self.record.supervision {
                health.observe(message, now)?;
                if health.pause.is_some() {
                    self.record.status = AgentStatus::Paused;
                    self.record.error = Some(
                        "supervision failure threshold reached; explicit recovery required".into(),
                    );
                }
            }
        }
        self.record.last_message_sequence = self.record.last_message_sequence.max(sequence);
        self.bump(now)
    }
    pub(crate) fn block(&mut self, reason: String, now: i64) -> Result<(), DomainError> {
        self.record.status = AgentStatus::Blocked;
        self.record.error = Some(reason);
        self.bump(now)
    }
    pub(crate) fn memory_cleared(&mut self, state: Value, now: i64) -> Result<(), DomainError> {
        self.record.state = state;
        self.bump(now)
    }
    pub(crate) fn reminder_scheduled(
        &mut self,
        reminder: &ActorReminder,
        now: i64,
    ) -> Result<(), DomainError> {
        if reminder.message.recipient != self.record.id
            || reminder.id != reminder.message.id
            || reminder.status != ReminderStatus::Scheduled
            || self.record.pending_reminders.contains(&reminder.id)
        {
            return Err(DomainError::InvalidState);
        }
        if self.record.pending_reminders.len() >= MAX_PENDING_REMINDERS {
            return Err(DomainError::InvalidConfiguration(format!(
                "actor allows at most {MAX_PENDING_REMINDERS} pending reminders"
            )));
        }
        self.record.pending_reminders.push(reminder.id);
        self.bump(now)
    }
    pub(crate) fn reminder_settled(&mut self, id: MessageId, now: i64) -> Result<(), DomainError> {
        let index = self
            .record
            .pending_reminders
            .iter()
            .position(|pending| *pending == id)
            .ok_or(DomainError::InvalidState)?;
        self.record.pending_reminders.remove(index);
        self.bump(now)
    }
    /// A cancellation receipt can precede registration without reserving a pending slot.
    pub(crate) fn reminder_cancelled_before_registration(
        &mut self,
        reminder: &ActorReminder,
        now: i64,
    ) -> Result<(), DomainError> {
        if reminder.status != ReminderStatus::Cancelled
            || reminder.message.recipient != self.record.id
            || reminder.id != reminder.message.id
            || self.record.pending_reminders.contains(&reminder.id)
        {
            return Err(DomainError::InvalidState);
        }
        self.bump(now)
    }
    pub(crate) fn control(&mut self, paused: bool, now: i64) -> Result<(), DomainError> {
        if !paused
            && self
                .record
                .supervision
                .as_ref()
                .is_some_and(|h| h.pause.is_some())
        {
            return Err(DomainError::InvalidConfiguration(
                "actor supervision is paused; use explicit supervision recovery".into(),
            ));
        }
        let status = if paused {
            AgentStatus::Paused
        } else {
            AgentStatus::Ready
        };
        if self.record.status != status {
            self.record.status = status;
            self.record.error = None;
            self.bump(now)?;
        }
        Ok(())
    }
    fn bump(&mut self, now: i64) -> Result<(), DomainError> {
        self.record.revision = self
            .record
            .revision
            .checked_add(1)
            .ok_or(DomainError::InvalidState)?;
        self.record.updated_at = now;
        Ok(())
    }
    pub(crate) fn cancelled_state(&mut self, state: Value, now: i64) -> Result<(), DomainError> {
        if self.record.state != state {
            self.record.state = state;
            self.bump(now)?;
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageStatus {
    Queued,
    Processed,
    Rejected,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MailboxEntry {
    pub sequence: i64,
    pub message: AgentMessage,
    pub status: MessageStatus,
    pub error: Option<String>,
    pub processed_revision: Option<u64>,
    pub received_at: i64,
    pub processed_at: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentRun {
    pub agent_id: AgentId,
    pub run_id: RunId,
    pub request_id: MessageId,
    pub correlation_id: MessageId,
    pub completion_message_id: MessageId,
    pub notified: bool,
}
pub enum AgentCommand {
    Send {
        recipient: AgentId,
        correlation_id: MessageId,
        kind: String,
        payload: Value,
    },
    /// Check a future run before accepting queued work, without persisting or executing it.
    ValidateRun {
        workflow: DefinitionId,
        input: Value,
        attempt_limit: u32,
    },
    StartRun {
        request_id: MessageId,
        correlation_id: MessageId,
        workflow: DefinitionId,
        input: Value,
        attempt_limit: u32,
    },
}
pub struct AgentTransition {
    pub state: Value,
    pub commands: Vec<AgentCommand>,
}
/// Pure versioned behavior. I/O is expressed as messages or durable run intents.
pub trait AgentDefinition: Send + Sync {
    fn id(&self) -> DefinitionId;
    fn initialize(&self, config: &Value) -> Result<Value, DomainError>;
    fn supports_supervision(&self) -> bool {
        false
    }
    /// Finish the current task and retain pending work when supervision will pause.
    fn transition_supervised(
        &self,
        config: &Value,
        state: &Value,
        message: &AgentMessage,
        suspend_next: bool,
    ) -> Result<AgentTransition, DomainError> {
        if suspend_next {
            return Err(DomainError::InvalidWorkflow(
                "supervised suspension unsupported".into(),
            ));
        }
        self.transition(config, state, message)
    }
    fn memory(&self, _state: &Value) -> Result<Option<ActorMemory>, DomainError> {
        Ok(None)
    }
    fn clear_memory(&self, _state: &Value) -> Result<Value, DomainError> {
        Err(DomainError::InvalidConfiguration(
            "actor memory is not enabled for this behavior".into(),
        ))
    }
    fn transition(
        &self,
        config: &Value,
        state: &Value,
        message: &AgentMessage,
    ) -> Result<AgentTransition, DomainError>;
    /// Remove accepted queued requests without starting work; active runs finish via their controls.
    fn cancel_requests(
        &self,
        _config: &Value,
        _state: &Value,
        _requests: &[AgentMessage],
        _reply: Option<&Value>,
    ) -> Result<Value, DomainError> {
        Err(DomainError::InvalidConfiguration(
            "agent behavior does not support accepted-task cancellation".into(),
        ))
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, DomainError> {
    serde_json::from_value(value.clone())
        .map_err(|e| DomainError::InvalidConfiguration(e.to_string()))
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorConfig {
    pub worker: AgentId,
}
#[derive(Clone, Serialize, Deserialize)]
struct Coordination {
    correlation: MessageId,
    requester: Option<AgentId>,
    result: Option<Value>,
}
#[derive(Default, Serialize, Deserialize)]
struct CoordinatorState {
    jobs: Vec<Coordination>,
}
pub struct CoordinatorAgent;
impl AgentDefinition for CoordinatorAgent {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("coordinator", 1)
    }
    fn initialize(&self, config: &Value) -> Result<Value, DomainError> {
        let _: CoordinatorConfig = decode(config)?;
        Ok(json!({"jobs":[]}))
    }
    fn cancel_requests(
        &self,
        _config: &Value,
        state: &Value,
        requests: &[AgentMessage],
        reply: Option<&Value>,
    ) -> Result<Value, DomainError> {
        let mut state: CoordinatorState = decode(state)?;
        if let Some(reply) = reply {
            for job in &mut state.jobs {
                if job.result.is_none()
                    && requests.iter().any(|r| r.correlation() == job.correlation)
                {
                    job.result = Some(reply.clone());
                }
            }
        }
        Ok(serde_json::to_value(state).unwrap())
    }
    fn transition(
        &self,
        config: &Value,
        state: &Value,
        message: &AgentMessage,
    ) -> Result<AgentTransition, DomainError> {
        let config: CoordinatorConfig = decode(config)?;
        let mut state: CoordinatorState = decode(state)?;
        let commands = match message.kind.as_str() {
            "task" => {
                let correlation = message.correlation();
                if state.jobs.iter().any(|j| j.correlation == correlation) {
                    return Err(DomainError::InvalidConfiguration(
                        "correlation already has a task".into(),
                    ));
                }
                state.jobs.push(Coordination {
                    correlation,
                    requester: message.sender,
                    result: None,
                });
                vec![AgentCommand::Send {
                    recipient: config.worker,
                    correlation_id: correlation,
                    kind: "task".into(),
                    payload: message.payload.clone(),
                }]
            }
            "task_result" | MESSAGE_REJECTED => {
                let valid_sender = if message.kind == MESSAGE_REJECTED {
                    message.sender.is_none() && message.payload["agent_id"] == json!(config.worker)
                } else {
                    message.sender == Some(config.worker)
                };
                if !valid_sender {
                    return Err(DomainError::InvalidState);
                }
                let job = state
                    .jobs
                    .iter_mut()
                    .find(|j| Some(j.correlation) == message.correlation_id)
                    .ok_or(DomainError::InvalidState)?;
                if let Some(result) = &job.result {
                    if result == &message.payload {
                        return Ok(AgentTransition {
                            state: serde_json::to_value(state).unwrap(),
                            commands: vec![],
                        });
                    }
                    return Err(DomainError::InvalidState);
                }
                job.result = Some(message.payload.clone());
                job.requester
                    .map(|recipient| AgentCommand::Send {
                        recipient,
                        correlation_id: job.correlation,
                        kind: "task_result".into(),
                        payload: message.payload.clone(),
                    })
                    .into_iter()
                    .collect()
            }
            _ => {
                return Err(DomainError::InvalidConfiguration(
                    "coordinator accepts task and task_result".into(),
                ));
            }
        };
        Ok(AgentTransition {
            state: serde_json::to_value(state).unwrap(),
            commands,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowWorkerConfig {
    pub workflow: DefinitionId,
    pub input: Value,
    pub attempt_limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryPolicy>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Work {
    request_id: MessageId,
    correlation: MessageId,
    requester: Option<AgentId>,
    payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    already_counted: bool,
}
#[derive(Default, Serialize, Deserialize)]
struct WorkerState {
    active: Option<Work>,
    pending: Vec<Work>,
    completed: u64,
    last_result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory: Option<ActorMemory>,
}
pub struct WorkflowWorkerAgent;
impl WorkflowWorkerAgent {
    /// Rebind a settled root request without launching or rebuilding its saved Run.
    pub(crate) fn resume_failed_turn(
        agent: &mut Agent,
        message: &AgentMessage,
        run: &RunRecord,
        now: i64,
    ) -> Result<(), DomainError> {
        let config: RegisteredAgentConfig = decode(&agent.record.config)?;
        let invocation: crate::domain::integration::AgentInvocation = decode(&run.input)?;
        let mut state: WorkerState = decode(&agent.record.state)?;
        if agent.record.definition != RegisteredAgentDefinition.id()
            || config.registration.definition
                != crate::domain::planner::PlannerWorkflow::definition_id()
            || config.registration.workflow_id() != run.workflow
            || invocation.registration != config.registration
            || agent.record.status != AgentStatus::Ready
            || message.kind != "task"
            || message.sender.is_some()
            || message.recipient != agent.record.id
            || state.active.is_some()
            || !state.pending.is_empty()
            || state.last_result.as_ref().is_none_or(|reply| {
                reply["request_id"] != json!(message.id)
                    || reply["run_id"] != json!(run.id)
                    || reply["status"] != "failed"
            })
        {
            return Err(DomainError::InvalidState);
        }
        state.active = Some(Work {
            request_id: message.id,
            correlation: message.correlation(),
            requester: None,
            payload: message.payload.clone(),
            memory_generation: invocation.input["actor_memory"]["generation"].as_u64(),
            already_counted: true,
        });
        agent.record.state = serde_json::to_value(state).map_err(|_| DomainError::InvalidState)?;
        agent.bump(now)
    }
    fn initialize_state(config: &Value) -> Result<Value, DomainError> {
        let config: WorkflowWorkerConfig = decode(config)?;
        if config.attempt_limit == 0
            || config.workflow.version == 0
            || config.workflow.name.is_empty()
        {
            return Err(DomainError::InvalidConfiguration(
                "worker needs a pinned workflow and positive attempt limit".into(),
            ));
        }
        let state = WorkerState {
            memory: config.memory.map(ActorMemory::new).transpose()?,
            ..WorkerState::default()
        };
        Ok(serde_json::to_value(state).unwrap())
    }
    fn input(
        config: &WorkflowWorkerConfig,
        work: &Work,
        memory: Option<&ActorMemory>,
        actor_id: AgentId,
    ) -> Value {
        let mut input = config.input.clone();
        // Coding workers pin model/tool settings and accept only a new goal per task.
        if let Some(goal) = work.payload["goal"].as_str()
            && input.get("goal").is_some()
        {
            input["goal"] = json!(goal);
        }
        if let Some(memory) = memory {
            input["actor_memory"] = json!(memory.snapshot(actor_id));
        }
        input
    }
    fn launch(
        config: &WorkflowWorkerConfig,
        work: &mut Work,
        memory: Option<&ActorMemory>,
        actor_id: AgentId,
    ) -> AgentCommand {
        work.memory_generation = memory.map(|m| m.generation);
        AgentCommand::StartRun {
            request_id: work.request_id,
            correlation_id: work.correlation,
            workflow: config.workflow.clone(),
            input: Self::input(config, work, memory, actor_id),
            attempt_limit: config.attempt_limit,
        }
    }
}
impl AgentDefinition for WorkflowWorkerAgent {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("workflow_worker", 1)
    }
    fn supports_supervision(&self) -> bool {
        true
    }
    fn initialize(&self, config: &Value) -> Result<Value, DomainError> {
        let worker: WorkflowWorkerConfig = decode(config)?;
        if worker.memory.is_some() || worker.input.get("follow_up").is_some() {
            return Err(DomainError::InvalidConfiguration(
                "actor memory and follow-ups require a native registered agent".into(),
            ));
        }
        Self::initialize_state(config)
    }
    fn memory(&self, state: &Value) -> Result<Option<ActorMemory>, DomainError> {
        let state: WorkerState = decode(state)?;
        if let Some(memory) = &state.memory {
            memory.validate()?;
        }
        Ok(state.memory)
    }
    fn clear_memory(&self, state: &Value) -> Result<Value, DomainError> {
        let mut state: WorkerState = decode(state)?;
        state
            .memory
            .as_mut()
            .ok_or_else(|| DomainError::InvalidConfiguration("actor memory is disabled".into()))?
            .clear()?;
        Ok(serde_json::to_value(state).unwrap())
    }
    fn cancel_requests(
        &self,
        _config: &Value,
        state: &Value,
        requests: &[AgentMessage],
        _reply: Option<&Value>,
    ) -> Result<Value, DomainError> {
        let mut state: WorkerState = decode(state)?;
        state
            .pending
            .retain(|work| !requests.iter().any(|r| r.id == work.request_id));
        Ok(serde_json::to_value(state).unwrap())
    }
    fn transition(
        &self,
        config: &Value,
        state: &Value,
        message: &AgentMessage,
    ) -> Result<AgentTransition, DomainError> {
        self.transition_supervised(config, state, message, false)
    }
    fn transition_supervised(
        &self,
        config: &Value,
        state: &Value,
        message: &AgentMessage,
        suspend_next: bool,
    ) -> Result<AgentTransition, DomainError> {
        let config: WorkflowWorkerConfig = decode(config)?;
        let mut state: WorkerState = decode(state)?;
        let mut commands = vec![];
        if state.memory.as_ref().map(|m| &m.policy) != config.memory.as_ref() {
            return Err(DomainError::InvalidState);
        }
        match message.kind.as_str() {
            "task" => {
                let mut work = Work {
                    request_id: message.id,
                    correlation: message.correlation(),
                    requester: message.sender,
                    payload: message.payload.clone(),
                    memory_generation: None,
                    already_counted: false,
                };
                if state.active.is_none() && state.pending.is_empty() {
                    commands.push(Self::launch(
                        &config,
                        &mut work,
                        state.memory.as_ref(),
                        message.recipient,
                    ));
                    state.active = Some(work);
                } else {
                    commands.push(AgentCommand::ValidateRun {
                        workflow: config.workflow.clone(),
                        input: Self::input(
                            &config,
                            &work,
                            state.memory.as_ref(),
                            message.recipient,
                        ),
                        attempt_limit: config.attempt_limit,
                    });
                    state.pending.push(work);
                    if state.active.is_none() {
                        let mut work = state.pending.remove(0);
                        commands.push(Self::launch(
                            &config,
                            &mut work,
                            state.memory.as_ref(),
                            message.recipient,
                        ));
                        state.active = Some(work);
                    }
                }
            }
            RUN_COMPLETED => {
                let active = state.active.take().ok_or(DomainError::InvalidState)?;
                if message.sender.is_some()
                    || message.payload["request_id"] != json!(active.request_id)
                    || message.correlation_id != Some(active.correlation)
                {
                    return Err(DomainError::InvalidState);
                }
                if !active.already_counted {
                    state.completed = state
                        .completed
                        .checked_add(1)
                        .ok_or(DomainError::InvalidState)?;
                }
                state.last_result = Some(message.payload.clone());
                if message.payload["status"] == "succeeded"
                    && let Some(memory) = &mut state.memory
                    && active.memory_generation == Some(memory.generation)
                {
                    let goal = active.payload["goal"]
                        .as_str()
                        .or_else(|| config.input["goal"].as_str())
                        .ok_or(DomainError::InvalidState)?;
                    memory.remember(MemoryEntry {
                        task_id: active.request_id,
                        goal: goal.into(),
                        result: message.payload["result"].clone(),
                    })?;
                }
                if let Some(recipient) = active.requester {
                    commands.push(AgentCommand::Send {
                        recipient,
                        correlation_id: active.correlation,
                        kind: "task_result".into(),
                        payload: message.payload.clone(),
                    });
                }
                if !suspend_next && !state.pending.is_empty() {
                    let mut work = state.pending.remove(0);
                    commands.push(Self::launch(
                        &config,
                        &mut work,
                        state.memory.as_ref(),
                        message.recipient,
                    ));
                    state.active = Some(work);
                }
            }
            SUPERVISION_RECOVERED => {
                if message.sender.is_some() || message.correlation_id.is_some() {
                    return Err(DomainError::InvalidState);
                }
                if state.active.is_none() && !state.pending.is_empty() {
                    let mut work = state.pending.remove(0);
                    commands.push(Self::launch(
                        &config,
                        &mut work,
                        state.memory.as_ref(),
                        message.recipient,
                    ));
                    state.active = Some(work);
                }
            }
            _ => {
                return Err(DomainError::InvalidConfiguration(
                    "worker accepts task messages".into(),
                ));
            }
        }
        Ok(AgentTransition {
            state: serde_json::to_value(state).unwrap(),
            commands,
        })
    }
}
