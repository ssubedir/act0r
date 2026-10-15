use super::{Engine, Error, ports::*};
use crate::domain::{actors::*, *};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct ActorService {
    engine: Engine,
    store: Arc<dyn ActorStore>,
    definitions: Arc<HashMap<DefinitionId, Arc<dyn AgentDefinition>>>,
    agent_registry: Option<Arc<super::AgentRegistry>>,
    inspection_reader: Option<Arc<dyn TaskInspectionReadPort>>,
    goal_control: Option<Arc<dyn GoalControlPort>>,
    changes_reader: Option<Arc<dyn WorkspaceChangesReadPort>>,
    application_inspector: Option<Arc<dyn PatchApplicationInspectionPort>>,
    project_instructions: Option<Arc<dyn ProjectInstructionsPort>>,
}
impl ActorService {
    pub fn new(
        engine: Engine,
        store: Arc<dyn ActorStore>,
        definitions: Vec<Arc<dyn AgentDefinition>>,
    ) -> Result<Self, Error> {
        let mut registry = HashMap::new();
        for definition in definitions {
            if registry.insert(definition.id(), definition).is_some() {
                return Err(Error::Unavailable("duplicate agent definition".into()));
            }
        }
        Ok(Self {
            engine,
            store,
            definitions: Arc::new(registry),
            agent_registry: None,
            inspection_reader: None,
            goal_control: None,
            changes_reader: None,
            application_inspector: None,
            project_instructions: None,
        })
    }
    pub fn with_agent_registry(mut self, registry: Arc<super::AgentRegistry>) -> Self {
        self.agent_registry = Some(registry);
        self
    }
    pub fn with_project_instructions(mut self, reader: Arc<dyn ProjectInstructionsPort>) -> Self {
        self.project_instructions = Some(reader);
        self
    }
    pub fn with_inspection_reader(mut self, reader: Arc<dyn TaskInspectionReadPort>) -> Self {
        self.inspection_reader = Some(reader);
        self
    }
    pub fn with_goal_control(mut self, control: Arc<dyn GoalControlPort>) -> Self {
        self.goal_control = Some(control);
        self
    }
    pub fn with_changes_reader(mut self, reader: Arc<dyn WorkspaceChangesReadPort>) -> Self {
        self.changes_reader = Some(reader);
        self
    }
    pub fn with_application_inspector(
        mut self,
        reader: Arc<dyn PatchApplicationInspectionPort>,
    ) -> Self {
        self.application_inspector = Some(reader);
        self
    }
    /// Return committed task artifacts, never recalculate from the live workspace.
    pub async fn task_changes(&self, id: MessageId) -> Result<change_review::TaskChanges, Error> {
        self.changes_reader
            .as_ref()
            .ok_or_else(|| {
                Error::Unavailable("workspace change review reader is not configured".into())
            })?
            .task_changes(id)
            .await
    }
    pub fn patch_applications(&self) -> Result<super::PatchApplicationService, Error> {
        Ok(super::PatchApplicationService::new(
            self.engine.clone(),
            self.changes_reader.clone().ok_or_else(|| {
                Error::Unavailable("workspace change review reader is not configured".into())
            })?,
        )
        .with_inspector(self.application_inspector.clone()))
    }
    pub(crate) fn cancellation_decision(&self) -> ActorCancellation {
        let definitions = self.definitions.clone();
        Box::new(move |agent, requests, reply| {
            let definition = definitions.get(&agent.record().definition).ok_or_else(|| {
                Error::Unavailable(
                    "agent definition is needed to remove accepted queued work".into(),
                )
            })?;
            Ok(definition.cancel_requests(
                &agent.record().config,
                &agent.record().state,
                requests,
                reply,
            )?)
        })
    }
    pub async fn cancel_goal(&self, id: MessageId) -> Result<goal::GoalCancellationView, Error> {
        let control = self.goal_controls()?;
        let now = self.engine.clock.now_ms();
        let receipt = control.request_goal_cancellation(id, now).await?;
        self.settle_goal_cancellation(receipt).await
    }
    pub(super) fn goal_controls(&self) -> Result<&Arc<dyn GoalControlPort>, Error> {
        self.goal_control
            .as_ref()
            .ok_or_else(|| Error::Unavailable("goal controls are not configured".into()))
    }
    pub(super) async fn settle_goal_cancellation(
        &self,
        receipt: goal::GoalCancellationReceipt,
    ) -> Result<goal::GoalCancellationView, Error> {
        let control = self.goal_controls()?;
        let now = self.engine.clock.now_ms();
        // Cleanup is bounded; the worker resumes any remainder behind the permanent admission guard.
        for _ in 0..64 {
            if !control
                .advance_goal_cancellation(now, self.cancellation_decision())
                .await?
            {
                break;
            }
        }
        control.goal_cancellation(receipt.root_task_id).await
    }
    pub async fn pause_goal(&self, id: MessageId) -> Result<goal::GoalPauseRecord, Error> {
        self.control_goal_pause(id, true).await
    }
    pub async fn resume_goal(&self, id: MessageId) -> Result<goal::GoalPauseRecord, Error> {
        self.control_goal_pause(id, false).await
    }
    async fn control_goal_pause(
        &self,
        id: MessageId,
        paused: bool,
    ) -> Result<goal::GoalPauseRecord, Error> {
        self.goal_control
            .as_ref()
            .ok_or_else(|| Error::Unavailable("goal controls are not configured".into()))?
            .control_goal_pause(id, paused, self.engine.clock.now_ms())
            .await
    }
    pub async fn inspect_task(
        &self,
        id: MessageId,
        options: super::TaskInspectionOptions,
    ) -> Result<super::TaskInspection, Error> {
        super::TaskInspectionService::new(
            self.inspection_reader.clone().ok_or_else(|| {
                Error::Unavailable("task inspection reader is not configured".into())
            })?,
        )
        .inspect(id, options)
        .await
    }
    pub async fn create(
        &self,
        id: AgentId,
        definition: DefinitionId,
        config: Value,
    ) -> Result<AgentId, Error> {
        self.create_with_queue_policy(id, definition, config, AgentQueuePolicy::default())
            .await
    }
    pub async fn create_with_queue_policy(
        &self,
        id: AgentId,
        definition: DefinitionId,
        config: Value,
        queue_policy: AgentQueuePolicy,
    ) -> Result<AgentId, Error> {
        self.create_with_policies(id, definition, config, queue_policy, None)
            .await
    }
    pub async fn create_with_policies(
        &self,
        id: AgentId,
        definition: DefinitionId,
        config: Value,
        queue_policy: AgentQueuePolicy,
        supervision: Option<SupervisionPolicy>,
    ) -> Result<AgentId, Error> {
        if definition == RegisteredAgentDefinition.id() {
            let pinned: RegisteredAgentConfig = serde_json::from_value(config.clone())?;
            self.agent_registry
                .as_ref()
                .ok_or_else(|| {
                    Error::Unavailable("agent adapter registry is not configured".into())
                })?
                .check_config(&pinned.registration, &pinned.input)?;
        }
        let definition = self
            .definitions
            .get(&definition)
            .ok_or_else(|| Error::Unavailable("agent definition is not registered".into()))?;
        let actor = Agent::new_with_policies(
            id,
            definition.as_ref(),
            config,
            queue_policy,
            supervision,
            self.engine.clock.now_ms(),
        )?;
        self.store.create_actor(actor).await?;
        Ok(id)
    }
    pub async fn status(&self, id: AgentId) -> Result<Agent, Error> {
        self.store.actor(id).await
    }
    pub async fn supervision(&self, id: AgentId) -> Result<Option<ActorSupervision>, Error> {
        Ok(self.store.actor(id).await?.record().supervision.clone())
    }
    pub async fn recover_supervision(&self, id: AgentId) -> Result<(), Error> {
        self.store
            .recover_actor_supervision(id, self.engine.clock.now_ms())
            .await
    }
    pub async fn schedule_task(
        &self,
        message: AgentMessage,
        deadline: TimerDeadline,
    ) -> Result<ActorReminder, Error> {
        ActorReminder::validate_message(&message)?;
        self.store
            .schedule_actor_reminder(message, deadline, self.engine.clock.now_ms())
            .await
    }
    pub async fn reminder(&self, id: MessageId) -> Result<ActorReminder, Error> {
        self.store.actor_reminder(id).await
    }
    pub async fn reminders(&self, id: AgentId) -> Result<Vec<ActorReminder>, Error> {
        self.store.actor_reminders(id).await
    }
    pub async fn cancel_reminder(&self, id: MessageId) -> Result<ActorReminder, Error> {
        self.store
            .cancel_actor_reminder(id, self.engine.clock.now_ms())
            .await
    }
    pub async fn memory(&self, id: AgentId) -> Result<Option<ActorMemory>, Error> {
        let actor = self.store.actor(id).await?;
        self.inspect_memory(&actor)
    }
    pub(crate) fn inspect_memory(&self, actor: &Agent) -> Result<Option<ActorMemory>, Error> {
        let definition = self
            .definitions
            .get(&actor.record().definition)
            .ok_or_else(|| {
                Error::Unavailable("agent definition is needed to inspect memory".into())
            })?;
        Ok(definition.memory(&actor.record().state)?)
    }
    pub(crate) fn session_memory_decision(
        &self,
        request: crate::domain::session_memory::ClearMemoryRequest,
    ) -> ActorMemoryDecision {
        let definitions = self.definitions.clone();
        Box::new(move |actor| {
            let definition = definitions.get(&actor.record().definition).ok_or_else(|| {
                Error::Unavailable("agent definition is needed to clear memory".into())
            })?;
            let mut memory = definition.memory(&actor.record().state)?.ok_or_else(|| {
                DomainError::InvalidConfiguration("memory is disabled for this actor".into())
            })?;
            request.check(&memory)?;
            memory.clear()?;
            let state = definition.clear_memory(&actor.record().state)?;
            if definition.memory(&state)?.as_ref() != Some(&memory) {
                return Err(DomainError::InvalidState.into());
            }
            Ok(state)
        })
    }
    pub async fn clear_memory(&self, id: AgentId) -> Result<(), Error> {
        let definitions = self.definitions.clone();
        self.store
            .clear_actor_memory(
                id,
                self.engine.clock.now_ms(),
                Box::new(move |actor| {
                    let definition =
                        definitions.get(&actor.record().definition).ok_or_else(|| {
                            Error::Unavailable("agent definition is needed to clear memory".into())
                        })?;
                    Ok(definition.clear_memory(&actor.record().state)?)
                }),
            )
            .await
    }
    pub async fn queue(&self, id: AgentId) -> Result<AgentQueue, Error> {
        self.store.actor_queue(id).await
    }
    pub async fn mailbox(&self, id: AgentId) -> Result<Vec<MailboxEntry>, Error> {
        self.store.mailbox(id).await
    }
    pub async fn runs(&self, id: AgentId) -> Result<Vec<AgentRun>, Error> {
        self.store.actor_runs(id).await
    }
    pub async fn task(&self, id: MessageId) -> Result<AgentTaskView, Error> {
        self.store.actor_task(id).await
    }
    pub async fn tasks(&self, id: AgentId) -> Result<Vec<AgentTaskView>, Error> {
        self.store.actor_tasks(id).await
    }
    pub async fn cancel_task(&self, id: MessageId) -> Result<AgentTaskView, Error> {
        let definitions = self.definitions.clone();
        self.store
            .cancel_actor_task(
                id,
                self.engine.clock.now_ms(),
                Box::new(move |agent, requests, reply| {
                    let definition =
                        definitions.get(&agent.record().definition).ok_or_else(|| {
                            Error::Unavailable(
                                "agent definition is needed to remove accepted queued work".into(),
                            )
                        })?;
                    Ok(definition.cancel_requests(
                        &agent.record().config,
                        &agent.record().state,
                        requests,
                        reply,
                    )?)
                }),
            )
            .await
    }
    pub async fn send(&self, message: AgentMessage) -> Result<MessageId, Error> {
        message.validate_public()?;
        self.store
            .enqueue_message(message, self.engine.clock.now_ms())
            .await
    }
    pub async fn pause(&self, id: AgentId) -> Result<(), Error> {
        self.store
            .control_actor(id, true, self.engine.clock.now_ms())
            .await
    }
    pub async fn resume(&self, id: AgentId) -> Result<(), Error> {
        self.store
            .control_actor(id, false, self.engine.clock.now_ms())
            .await
    }
    pub(crate) async fn restore_tool_implementation(&self, id: AgentId) -> Result<(), Error> {
        let actor = self.store.actor(id).await?;
        if actor.record().status != AgentStatus::Blocked
            || actor.record().definition != RegisteredAgentDefinition.id()
        {
            return Ok(());
        }
        let pinned: RegisteredAgentConfig = serde_json::from_value(actor.record().config.clone())?;
        if self.agent_registry.as_ref().is_some_and(|registry| {
            registry
                .check_config(&pinned.registration, &pinned.input)
                .is_ok()
        }) && self
            .engine
            .definitions
            .contains_key(&pinned.registration.workflow_id())
        {
            self.store
                .restore_actor_implementation(
                    id,
                    actor.record().revision,
                    self.engine.clock.now_ms(),
                )
                .await?;
        }
        Ok(())
    }
}
impl WorkCoordinator for ActorService {
    fn advance(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            let now = self.engine.clock.now_ms();
            if let Some(control) = &self.goal_control
                && control
                    .advance_goal_cancellation(now, self.cancellation_decision())
                    .await?
            {
                return Ok(true);
            }
            if self.store.notify_run_completion(now).await? {
                return Ok(true);
            }
            let engine = self.engine.clone();
            match self
                .store
                .advance_delegation(
                    now,
                    Box::new(move |run, id, event| {
                        if let DelegationEvent::DeadlineElapsed { child_completed_at } = event {
                            return run.expire_delegation(id, child_completed_at, now);
                        }
                        if run.record().status.is_unfinished()
                            && !run.record().controls.cancel_requested
                            && let Some(reason) = engine.incompatibility(run)
                        {
                            run.block(reason, now);
                            return Ok(());
                        }
                        if let DelegationEvent::ChildCompleted(outcome) = event {
                            run.task_completed(
                                id,
                                outcome,
                                engine
                                    .definitions
                                    .get(&run.record().workflow)
                                    .map(|w| w.as_ref()),
                                now,
                            )?;
                        }
                        Ok(())
                    }),
                )
                .await?
            {
                DelegationProgress::Changed => return Ok(true),
                DelegationProgress::CancelChild(id) => {
                    self.cancel_task(id).await?;
                    return Ok(true);
                }
                DelegationProgress::Idle => {}
            }
            let definitions = self.definitions.clone();
            if self.store.publish_due_actor_reminder(now).await? {
                return Ok(true);
            }
            let workflows = self.engine.definitions.clone();
            let registry = self.agent_registry.clone();
            let project_instructions = self.project_instructions.clone();
            self.store
                .process_message(
                    now,
                    Box::new(move |agent, message, origins| {
                        if agent.record().definition == RegisteredAgentDefinition.id() {
                            let pinned: RegisteredAgentConfig =
                                match serde_json::from_value(agent.record().config.clone()) {
                                    Ok(config) => config,
                                    Err(error) => {
                                        return ActorDecisionOutcome::Block(error.to_string());
                                    }
                                };
                            let compatible = registry
                                .as_ref()
                                .ok_or_else(|| {
                                    Error::Unavailable(
                                        "agent adapter registry is not configured".into(),
                                    )
                                })
                                .and_then(|r| r.check_config(&pinned.registration, &pinned.input));
                            if let Err(error) = compatible {
                                return ActorDecisionOutcome::Block(error.to_string());
                            }
                        }
                        let Some(definition) = definitions.get(&agent.record().definition) else {
                            return ActorDecisionOutcome::Block(format!(
                                "missing agent definition {} v{}",
                                agent.record().definition.name,
                                agent.record().definition.version
                            ));
                        };
                        let transition = match agent.decide(definition.as_ref(), message) {
                            Ok(transition) => transition,
                            Err(error) => return ActorDecisionOutcome::Reject(error.to_string()),
                        };
                        let mut effects = ActorEffects {
                            state: transition.state,
                            messages: vec![],
                            runs: vec![],
                        };
                        for command in transition.commands {
                            match command {
                                AgentCommand::Send {
                                    recipient,
                                    correlation_id,
                                    kind,
                                    payload,
                                } => {
                                    let outgoing = AgentMessage {
                                        id: MessageId::new(),
                                        recipient,
                                        sender: Some(agent.record().id),
                                        correlation_id: Some(correlation_id),
                                        kind,
                                        payload,
                                    };
                                    if let Err(error) = outgoing.validate() {
                                        return ActorDecisionOutcome::Reject(error.to_string());
                                    }
                                    if outgoing.kind.starts_with('$') {
                                        return ActorDecisionOutcome::Reject(
                                            "agent messages cannot impersonate runtime events"
                                                .into(),
                                        );
                                    }
                                    effects.messages.push(outgoing);
                                }
                                AgentCommand::ValidateRun {
                                    workflow,
                                    input,
                                    attempt_limit,
                                } => {
                                    let input = match planner_actor_input(
                                        agent,
                                        message.id,
                                        input,
                                        origins,
                                        project_instructions.as_deref(),
                                        false,
                                    ) {
                                        Ok(input) => input,
                                        Err(error) => {
                                            return ActorDecisionOutcome::Reject(error.to_string());
                                        }
                                    };
                                    let Some(definition) = workflows.get(&workflow) else {
                                        return ActorDecisionOutcome::Block(format!(
                                            "missing workflow {} v{}",
                                            workflow.name, workflow.version
                                        ));
                                    };
                                    if let Err(error) =
                                        Run::start(definition.as_ref(), input, attempt_limit, now)
                                    {
                                        return ActorDecisionOutcome::Reject(error.to_string());
                                    }
                                }
                                AgentCommand::StartRun {
                                    request_id,
                                    correlation_id,
                                    workflow,
                                    input,
                                    attempt_limit,
                                } => {
                                    let input = match planner_actor_input(
                                        agent,
                                        request_id,
                                        input,
                                        origins,
                                        project_instructions.as_deref(),
                                        true,
                                    ) {
                                        Ok(input) => input,
                                        Err(error) => {
                                            return ActorDecisionOutcome::Reject(error.to_string());
                                        }
                                    };
                                    let Some(definition) = workflows.get(&workflow) else {
                                        return ActorDecisionOutcome::Block(format!(
                                            "missing workflow {} v{}",
                                            workflow.name, workflow.version
                                        ));
                                    };
                                    let run = match Run::start(
                                        definition.as_ref(),
                                        input,
                                        attempt_limit,
                                        now,
                                    ) {
                                        Ok(run) => run,
                                        Err(error) => {
                                            return ActorDecisionOutcome::Reject(error.to_string());
                                        }
                                    };
                                    let link = AgentRun {
                                        agent_id: agent.record().id,
                                        run_id: run.record().id,
                                        request_id,
                                        correlation_id,
                                        completion_message_id: MessageId::new(),
                                        notified: false,
                                    };
                                    effects.runs.push((run, link));
                                }
                            }
                        }
                        ActorDecisionOutcome::Apply(effects)
                    }),
                    Some(Box::new(move |input| {
                        input.validate()?;
                        Ok(Run::start(
                            &session_summary::SessionSummaryWorkflow,
                            serde_json::to_value(input)?,
                            8,
                            now,
                        )?)
                    })),
                )
                .await
        })
    }
    fn has_pending(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            if let Some(control) = &self.goal_control
                && control.goal_cancellation_work_pending().await?
            {
                return Ok(true);
            }
            self.store.actor_work_pending().await
        })
    }
}

/// Scheduling context comes from accepted task records, never from task payloads.
fn planner_actor_input(
    agent: &Agent,
    task_id: MessageId,
    input: Value,
    origins: &ActorTaskOrigins,
    instructions: Option<&dyn ProjectInstructionsPort>,
    launching: bool,
) -> Result<Value, DomainError> {
    if agent.record().definition != RegisteredAgentDefinition.id() {
        return Ok(input);
    }
    let mut invocation: integration::AgentInvocation = serde_json::from_value(input)
        .map_err(|e| DomainError::InvalidConfiguration(e.to_string()))?;
    if let Some(summary) = origins.session_summaries.get(&task_id) {
        summary.validate()?;
        invocation.input["session_summary"] = serde_json::json!(summary);
        if let Some(covered) = origins.summarized_memory.get(&task_id)
            && let Some(value) = invocation
                .input
                .get("actor_memory")
                .filter(|v| !v.is_null())
        {
            let mut memory: MemorySnapshot =
                serde_json::from_value(value.clone()).map_err(|_| DomainError::InvalidState)?;
            memory.compact(covered)?;
            invocation.input["actor_memory"] = serde_json::json!(memory);
        }
    }
    if let Some(prepared) = origins.workspace_inputs.get(&task_id) {
        let config: RegisteredAgentConfig = serde_json::from_value(agent.record().config.clone())
            .map_err(|_| DomainError::InvalidState)?;
        prepared.check_worker(agent.record().id, &config)?;
        if task_id != prepared.task_id {
            return Err(DomainError::InvalidState);
        }
        invocation.input["workspace"] = serde_json::json!(prepared.workspace);
        invocation.input["workspace_input"] = serde_json::json!(prepared);
        if let Some(paths) = &prepared.request.writable_paths {
            invocation.input["writable_paths"] = serde_json::json!(paths);
        }
    }
    if invocation.registration.definition == planner::PlannerWorkflow::definition_id() {
        let config: planner::PlannerConfig = serde_json::from_value(invocation.input.clone())
            .map_err(|e| DomainError::InvalidConfiguration(e.to_string()))?;
        if let Some(policy) = config.follow_up {
            let context = if let Some(origin) = origins.follow_ups.get(&task_id) {
                let context = origin.context(agent.record().id, task_id)?;
                if context.policy != policy {
                    return Err(DomainError::InvalidState);
                }
                context
            } else {
                follow_up::FollowUpContext {
                    actor_id: agent.record().id,
                    task_id,
                    root_task_id: task_id,
                    depth: 0,
                    policy,
                }
            };
            context.validate()?;
            invocation.input["follow_up_context"] = serde_json::json!(context);
        }
    }
    let native = [
        planner::PlannerWorkflow::definition_id(),
        answer::ToolAgentWorkflow::definition_id(),
        DefinitionId::new("answer_agent", 1),
    ];
    if native.contains(&invocation.registration.definition)
        && invocation.input["load_project_instructions"] == true
        && launching
    {
        let workspace = invocation.input["workspace"]
            .as_str()
            .ok_or(DomainError::InvalidState)?;
        let snapshot = instructions
            .ok_or_else(|| {
                DomainError::InvalidConfiguration(
                    "project instruction reader is not configured".into(),
                )
            })?
            .capture_project_instructions(workspace);
        snapshot.validate()?;
        invocation.input["project_instructions"] = serde_json::json!(snapshot);
    }
    serde_json::to_value(invocation).map_err(|e| DomainError::InvalidConfiguration(e.to_string()))
}
