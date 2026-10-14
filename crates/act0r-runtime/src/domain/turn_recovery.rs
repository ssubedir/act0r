//! Explicit recovery of a settled root turn at its saved model checkpoint.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryTurnRequest {
    pub request_id: MessageId,
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub expected_history_sequence: u64,
}
impl RetryTurnRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.request_id.to_string() == "00000000-0000-0000-0000-000000000000"
            || self.expected_history_sequence == 0
            || self.expected_history_sequence > i64::MAX as u64
        {
            return Err(DomainError::InvalidConfiguration(
                "recovery requires a nonzero decision identity and saved history sequence".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryTurnReceipt {
    pub session_id: SessionId,
    pub task_id: MessageId,
    pub request: RetryTurnRequest,
    pub resumed_activity_id: ActivityId,
    pub previous_error: Failure,
    pub saved_at: i64,
    pub replayed: bool,
}
#[derive(Debug, Serialize)]
pub struct TurnRecovery {
    pub session_id: SessionId,
    pub task_id: MessageId,
    pub run_id: Option<RunId>,
    pub activity_id: Option<ActivityId>,
    pub attempt_id: Option<AttemptId>,
    pub history_sequence: u64,
    pub error: Option<Failure>,
    pub target: Option<agent::ModelTarget>,
    pub usage: Option<UsageLedger>,
    pub retry_allowed: bool,
    pub blocked_reason: Option<String>,
    pub receipts: Vec<RetryTurnReceipt>,
}
impl Run {
    pub fn failed_model_checkpoint(&self) -> Result<(&Activity, &Attempt), DomainError> {
        if self.record.status != RunStatus::Failed
            || self.record.controls.cancel_requested
            || self.record.controls.pause_requested
            || self.record.controls.boundary_pause
            || self.record.wait.is_some()
            || self.record.delegations.iter().any(|d| d.outcome.is_none())
            || self
                .timers
                .iter()
                .any(|t| t.status == TimerStatus::Scheduled)
            || self.activities.iter().any(|a| {
                matches!(
                    a.status,
                    ActivityStatus::Queued
                        | ActivityStatus::Running
                        | ActivityStatus::Indeterminate
                )
            })
        {
            return Err(DomainError::InvalidConfiguration(
                "turn has no settled failed model checkpoint".into(),
            ));
        }
        let attempt = self.attempts.last().ok_or(DomainError::InvalidState)?;
        let activity = self
            .activities
            .iter()
            .find(|a| a.id == attempt.activity_id)
            .ok_or(DomainError::InvalidState)?;
        let error = activity.error.as_ref().ok_or(DomainError::InvalidState)?;
        if activity.status != ActivityStatus::Failed
            || activity.active_attempt.is_some()
            || activity.spec.handler != DefinitionId::new("agent.model", 1)
            || activity.spec.recovery != RecoveryPolicy::SafeToRepeat
            || attempt.status != AttemptStatus::Failed
            || !(error.retryable
                || matches!(
                    error.code.as_str(),
                    "model_invalid_json" | "model_invalid_action" | "model_incomplete"
                ))
            || self.record.error.as_ref() != Some(error)
            || !matches!(
                error.code.as_str(),
                "model_timeout"
                    | "model_connection"
                    | "model_transport"
                    | "model_http_status"
                    | "model_response_read"
                    | "model_invalid_json"
                    | "model_invalid_action"
                    | "model_incomplete"
            )
        {
            return Err(DomainError::InvalidConfiguration(
                "only transient provider errors and invalid model responses can be retried here"
                    .into(),
            ));
        }
        Ok((activity, attempt))
    }
    pub fn validate_model_recovery_budget(&self) -> Result<(), DomainError> {
        let (activity, _) = self.failed_model_checkpoint()?;
        if self.record.allowance.admitted >= self.record.allowance.limit
            || activity
                .spec
                .usage
                .as_ref()
                .is_some_and(|r| !self.record.usage.can_reserve(r))
        {
            return Err(DomainError::InvalidConfiguration(
                "saved attempt, model-call or token budget cannot admit recovery".into(),
            ));
        }
        Ok(())
    }
    /// Add one explicit model attempt. Previous activity, outcomes, state and charges stay intact.
    pub(crate) fn retry_failed_model(
        &mut self,
        request: &RetryTurnRequest,
        now: i64,
    ) -> Result<ActivityId, DomainError> {
        request.validate()?;
        self.validate_model_recovery_budget()?;
        let (activity, attempt) = self.failed_model_checkpoint()?;
        if request.run_id != self.record.id
            || request.expected_history_sequence != self.record.history_sequence
            || request.activity_id != activity.id
            || request.attempt_id != attempt.id
        {
            return Err(DomainError::InvalidState);
        }
        let error = activity.error.clone().ok_or(DomainError::InvalidState)?;
        let ready_at = error.retry_after.as_ref().map_or(now, |advice| {
            advice.deadline(attempt.ended_at.unwrap_or(now)).max(now)
        });
        let mut spec = activity.spec.clone();
        spec.retry.max_attempts = 1;
        self.record.status = RunStatus::Queued;
        self.record.error = None;
        self.record.result = None;
        self.event(
            "turn_recovery_requested",
            json!({"request":request,"previous_error":error}),
            Some(request.activity_id),
            Some(request.attempt_id),
            now,
        );
        self.apply(
            Transition {
                state: self.record.state.clone(),
                commands: vec![Command::Schedule(spec)],
            },
            now,
        )?;
        let activity = self
            .activities
            .last_mut()
            .ok_or(DomainError::InvalidState)?;
        activity.ready_at = ready_at;
        activity.preserve_input = true;
        Ok(activity.id)
    }
}
