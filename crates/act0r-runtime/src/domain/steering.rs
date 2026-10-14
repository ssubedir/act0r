//! Durable task-scoped instructions delivered to the next fresh native model request.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const MAX_MESSAGES: usize = 16;
pub const MAX_TEXT_BYTES: usize = 4096;
pub const MAX_CONTEXT_BYTES: usize = 16_384;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SteeringRequest {
    pub message_id: MessageId,
    pub run_id: RunId,
    pub text: String,
}
impl SteeringRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.message_id.to_string() == "00000000-0000-0000-0000-000000000000"
            || self.text.trim().is_empty()
            || self.text.len() > MAX_TEXT_BYTES
        {
            return Err(DomainError::InvalidConfiguration(
                "steering requires a nonzero message identity and 1..4096 UTF-8 bytes of text"
                    .into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SteeringApplication {
    pub activity_id: ActivityId,
    pub applied_at: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SteeringEntry {
    pub request: SteeringRequest,
    pub sequence: u64,
    pub saved_at: i64,
    pub application: Option<SteeringApplication>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SteeringReceipt {
    pub session_id: SessionId,
    pub task_id: MessageId,
    pub entry: SteeringEntry,
    pub replayed: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct TurnSteering {
    pub session_id: SessionId,
    pub task_id: MessageId,
    pub run_id: Option<RunId>,
    pub can_send: bool,
    pub blocked_reason: Option<String>,
    pub terminal: bool,
    pub items: Vec<SteeringEntry>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SteeringInstruction {
    pub message_id: MessageId,
    pub sequence: u64,
    pub text: String,
}
pub(crate) fn instructions(entries: &[SteeringEntry]) -> Vec<SteeringInstruction> {
    entries
        .iter()
        .map(|e| SteeringInstruction {
            message_id: e.request.message_id,
            sequence: e.sequence,
            text: e.request.text.clone(),
        })
        .collect()
}
pub(crate) fn validate_instructions(items: &[SteeringInstruction]) -> Result<(), DomainError> {
    if items.len() > MAX_MESSAGES
        || serde_json::to_vec(items)
            .map_err(|_| DomainError::InvalidState)?
            .len()
            > MAX_CONTEXT_BYTES
        || items.iter().enumerate().any(|(i, item)| {
            item.sequence != i as u64 + 1
                || item.text.trim().is_empty()
                || item.text.len() > MAX_TEXT_BYTES
                || items[..i]
                    .iter()
                    .any(|old| old.message_id == item.message_id)
        })
    {
        return Err(DomainError::InvalidConfiguration(
            "steering is limited to 16 ordered messages and 16384 serialized context bytes per task".into(),
        ));
    }
    Ok(())
}
impl Run {
    pub(super) fn steering_native_state(
        &self,
    ) -> Result<(&serde_json::Value, DefinitionId), DomainError> {
        let supported = |id: &DefinitionId| {
            *id == planner::PlannerWorkflow::definition_id()
                || *id == answer::ToolAgentWorkflow::definition_id()
        };
        if supported(&self.record.workflow) {
            return Ok((&self.record.state, self.record.workflow.clone()));
        }
        let invocation: integration::AgentInvocation =
            serde_json::from_value(self.record.input.clone()).map_err(|_| {
                DomainError::SessionConflict("this Run is not a supported native agent".into())
            })?;
        if !supported(&invocation.registration.definition)
            || invocation.registration.workflow_id() != self.record.workflow
            || self.record.state["registration"] != json!(invocation.registration)
        {
            return Err(DomainError::SessionConflict(
                "this Run is not a pinned native agent".into(),
            ));
        }
        Ok((
            &self.record.state["state"],
            invocation.registration.definition,
        ))
    }
    pub(super) fn prepare_steering(
        &self,
        entries: &[SteeringEntry],
    ) -> Result<(serde_json::Value, ActivitySpec), DomainError> {
        let (saved, definition) = self.steering_native_state()?;
        let (state, spec) = if definition == answer::ToolAgentWorkflow::definition_id() {
            answer::steered_model(
                saved,
                instructions(entries),
                worker_messages::instructions(&self.record.peer_messages),
            )?
        } else {
            planner::steered_model(
                saved,
                instructions(entries),
                worker_messages::instructions(&self.record.peer_messages),
            )?
        };
        if self.record.workflow == definition {
            Ok((state, spec))
        } else {
            let mut wrapped = self.record.state.clone();
            wrapped["state"] = state;
            Ok((wrapped, spec))
        }
    }
    pub fn steering_allowed(&self) -> Result<(), DomainError> {
        self.steering_native_state()?;
        if !(self.record.status.is_unfinished() || self.record.status == RunStatus::Blocked)
            || self.record.controls.cancel_requested
        {
            return Err(DomainError::SessionConflict(
                "only an unfinished native agent task accepts steering".into(),
            ));
        }
        if self.record.steering.len() >= MAX_MESSAGES {
            return Err(DomainError::SessionConflict(
                "this task already has 16 steering messages".into(),
            ));
        }
        Ok(())
    }
    pub fn worker_steering_allowed(
        &self,
        task: &actors::AgentTaskRecord,
        pinned: &actors::RegisteredAgentConfig,
    ) -> Result<(), DomainError> {
        let (_, config) = self.task_permission_binding(task, pinned)?;
        if config.completion_policy != Some(completion::CompletionPolicy::RequirePassingTests) {
            return Err(DomainError::SessionConflict(
                "worker guidance requires a native coding assignment".into(),
            ));
        }
        self.steering_allowed()
    }
    pub(crate) fn queue_steering(
        &mut self,
        request: SteeringRequest,
        now: i64,
    ) -> Result<SteeringEntry, DomainError> {
        request.validate()?;
        self.steering_allowed()?;
        if request.run_id != self.record.id {
            return Err(DomainError::SessionConflict(
                "steering belongs to a different Run".into(),
            ));
        }
        let entry = SteeringEntry {
            request,
            sequence: self.record.steering.len() as u64 + 1,
            saved_at: now,
            application: None,
        };
        let mut entries = self.record.steering.clone();
        entries.push(entry.clone());
        let instructions = instructions(&entries);
        validate_instructions(&instructions)?;
        // Refuse an instruction that cannot fit the pinned input allowance now.
        self.prepare_steering(&entries)?;
        self.record.steering = entries;
        self.queue_guidance_wake(entry.sequence, now);
        self.event("steering_received", json!({"entry":entry}), None, None, now);
        Ok(entry)
    }
    /// Apply only to an unattempted request. Retries and explicit checkpoint recovery stay exact.
    pub(super) fn admit_with_steering(
        &mut self,
        now: i64,
    ) -> Result<Option<ActivityExecution>, DomainError> {
        let Some(activity) = self.next_due(now) else {
            return Ok(None);
        };
        if activity.spec.handler != DefinitionId::new("agent.model", 1)
            || activity.attempts != 0
            || activity.preserve_input
            || !(self.record.steering.iter().any(|e| e.application.is_none())
                || self
                    .record
                    .peer_messages
                    .iter()
                    .any(|e| e.application.is_none()))
        {
            return self.admit_prepared_request(now);
        }
        let id = activity.id;
        let mut candidate = self.clone();
        let (state, spec) = match self.prepare_steering(&self.record.steering) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.block(
                    format!("pending steering cannot fit the next model request: {error}"),
                    now,
                );
                return Ok(None);
            }
        };
        candidate.record.state = state;
        let activity = candidate
            .activities
            .iter_mut()
            .find(|a| a.id == id)
            .ok_or(DomainError::InvalidState)?;
        activity.spec = spec;
        let input = activity.spec.input.clone();
        let usage = activity.spec.usage.clone();
        let mut applied = Vec::new();
        for entry in &mut candidate.record.steering {
            if entry.application.is_none() {
                entry.application = Some(SteeringApplication {
                    activity_id: id,
                    applied_at: now,
                });
                applied.push(entry.request.message_id);
            }
        }
        if !applied.is_empty() {
            candidate.event(
                "steering_applied",
                json!({"message_ids":applied,"input":input,"usage":usage}),
                Some(id),
                None,
                now,
            );
        }
        let mut peer_ids = Vec::new();
        for entry in &mut candidate.record.peer_messages {
            if entry.application.is_none() {
                entry.application = Some(SteeringApplication {
                    activity_id: id,
                    applied_at: now,
                });
                peer_ids.push(entry.message_id);
            }
        }
        if !peer_ids.is_empty() {
            candidate.event(
                "peer_messages_applied",
                json!({"message_ids":peer_ids}),
                Some(id),
                None,
                now,
            );
        }
        if let Some(execution) = candidate.admit_prepared_request(now)? {
            *self = candidate;
            Ok(Some(execution))
        } else {
            // Budget refusal must not claim an instruction was delivered.
            self.admit_prepared_request(now)
        }
    }
}
