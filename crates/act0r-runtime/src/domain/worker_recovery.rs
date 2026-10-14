//! Hold a physical coding assignment before publishing its terminal outcome.
use super::{turn_recovery::RetryTurnRequest, *};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRecoveryDecision {
    Retry,
    AcceptFailure,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerRecoveryCheckpoint {
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub held_at: i64,
    pub decision: Option<WorkerRecoveryDecision>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRecoveryRequest {
    pub request_id: MessageId,
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub expected_history_sequence: u64,
    pub decision: WorkerRecoveryDecision,
}
impl WorkerRecoveryRequest {
    pub fn checkpoint(&self) -> RetryTurnRequest {
        RetryTurnRequest {
            request_id: self.request_id,
            run_id: self.run_id,
            activity_id: self.activity_id,
            attempt_id: self.attempt_id,
            expected_history_sequence: self.expected_history_sequence,
        }
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        self.checkpoint().validate()
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerRecoveryReceipt {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub request: WorkerRecoveryRequest,
    pub resumed_activity_id: Option<ActivityId>,
    pub previous_error: Failure,
    pub saved_at: i64,
    pub replayed: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct WorkerRecovery {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub run_id: Option<RunId>,
    pub activity_id: Option<ActivityId>,
    pub attempt_id: Option<AttemptId>,
    pub history_sequence: u64,
    pub pending: bool,
    pub error: Option<Failure>,
    pub target: Option<agent::ModelTarget>,
    pub usage: Option<UsageLedger>,
    pub retry_allowed: bool,
    pub accept_allowed: bool,
    pub blocked_reason: Option<String>,
    pub retry_blocked_reason: Option<String>,
    pub receipts: Vec<WorkerRecoveryReceipt>,
}
impl Run {
    pub fn needs_worker_recovery(&self) -> bool {
        self.record
            .worker_recovery
            .as_ref()
            .is_some_and(|r| r.decision.is_none())
    }
    pub(crate) fn task_recovery_binding(
        &self,
        task: &actors::AgentTaskRecord,
        pinned: &actors::RegisteredAgentConfig,
    ) -> Result<(workspace_handoff::PreparedWorkspace, answer::AnswerConfig), DomainError> {
        self.failed_model_checkpoint()?;
        let binding = self.task_workspace_binding(task, pinned)?;
        if binding.1.completion_policy != Some(completion::CompletionPolicy::RequirePassingTests) {
            return Err(DomainError::InvalidState);
        }
        Ok(binding)
    }
    pub(crate) fn hold_worker_failure(&mut self, now: i64) -> Result<bool, DomainError> {
        let (activity, attempt) = self.failed_model_checkpoint()?;
        if self
            .record
            .worker_recovery
            .as_ref()
            .is_some_and(|r| r.attempt_id == attempt.id)
        {
            return Ok(false);
        }
        let checkpoint = WorkerRecoveryCheckpoint {
            activity_id: activity.id,
            attempt_id: attempt.id,
            held_at: now,
            decision: None,
        };
        self.event(
            "worker_recovery_needed",
            json!({"checkpoint":checkpoint,"error":self.record.error}),
            Some(checkpoint.activity_id),
            Some(checkpoint.attempt_id),
            now,
        );
        self.record.worker_recovery = Some(checkpoint);
        Ok(true)
    }
    pub(crate) fn decide_worker_recovery(
        &mut self,
        request: &WorkerRecoveryRequest,
        now: i64,
    ) -> Result<Option<ActivityId>, DomainError> {
        request.validate()?;
        let checkpoint = self
            .record
            .worker_recovery
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        if !self.needs_worker_recovery()
            || request.run_id != self.record.id
            || request.activity_id != checkpoint.activity_id
            || request.attempt_id != checkpoint.attempt_id
            || request.expected_history_sequence != self.record.history_sequence
        {
            return Err(DomainError::InvalidState);
        }
        self.failed_model_checkpoint()?;
        let resumed = match request.decision {
            WorkerRecoveryDecision::Retry => {
                Some(self.retry_failed_model(&request.checkpoint(), now)?)
            }
            WorkerRecoveryDecision::AcceptFailure => None,
        };
        self.event(
            "worker_recovery_decided",
            json!({"request":request,"resumed_activity_id":resumed}),
            Some(request.activity_id),
            Some(request.attempt_id),
            now,
        );
        if resumed.is_some() {
            self.record.worker_recovery = None;
        } else {
            self.record
                .worker_recovery
                .as_mut()
                .ok_or(DomainError::InvalidState)?
                .decision = Some(request.decision);
        }
        Ok(resumed)
    }
}
