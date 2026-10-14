//! Planner-owned, reversible admission holds on exact captured assignments.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_PAUSE_ACTIONS: u32 = 4;
pub const MAX_REASON_BYTES: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PauseWorker {
    pub worker: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency: Option<String>,
}
impl PauseWorker {
    pub fn validate(&self) -> Result<(), tools::ToolError> {
        if self.reason.trim().is_empty() || self.reason.len() > MAX_REASON_BYTES {
            return Err(tools::ToolError::new(
                "worker_pause_reason",
                "A pause/resume reason needs 1..2048 UTF-8 bytes.",
            ));
        }
        if !matches!(self.worker.as_str(), "coder" | "coder2") {
            return Err(tools::ToolError::new(
                "worker_pause_scope",
                "Choose coder or coder2 captured in this guidance step; other assignments cannot be paused or resumed.",
            ));
        }
        Ok(())
    }
}

/// A saved edge between two physical assignments in one planner join.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerDependency {
    pub activity_id: ActivityId,
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub planner_run_id: RunId,
    pub worker: String,
    pub task_id: MessageId,
    pub run_id: RunId,
    pub prerequisite: String,
    pub prerequisite_task_id: MessageId,
    pub prerequisite_run_id: RunId,
    pub hold_revision: u64,
    pub saved_at: i64,
    pub settled_at: Option<i64>,
    pub outcome: Option<actors::AgentTaskStatus>,
    pub review_timer_id: Option<TimerId>,
    pub closed: bool,
}
impl WorkerDependency {
    pub fn settle(
        &mut self,
        outcome: actors::AgentTaskStatus,
        now: i64,
    ) -> Result<(), DomainError> {
        if self.closed || self.outcome.is_some() || !outcome.is_terminal() {
            return Err(DomainError::InvalidState);
        }
        self.outcome = Some(outcome);
        self.settled_at = Some(now);
        self.validate()
    }
    pub fn capture_review(&mut self, timer: TimerId) -> Result<(), DomainError> {
        if self.closed || self.outcome.is_none() || self.review_timer_id.is_some() {
            return Err(DomainError::InvalidState);
        }
        self.review_timer_id = Some(timer);
        self.validate()
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        if !matches!(self.worker.as_str(), "coder" | "coder2")
            || !matches!(self.prerequisite.as_str(), "coder" | "coder2")
            || self.worker == self.prerequisite
            || self.task_id == self.prerequisite_task_id
            || self.run_id == self.prerequisite_run_id
            || self.hold_revision == 0
            || self.hold_revision > 9_007_199_254_740_991
            || self.outcome.is_some_and(|s| !s.is_terminal())
            || self.settled_at.is_some() != self.outcome.is_some()
            || self.review_timer_id.is_some() && self.outcome.is_none()
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerHoldRecord {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub run_id: RunId,
    pub planner_run_id: RunId,
    pub activity_id: ActivityId,
    pub paused: bool,
    pub revision: u64,
    pub reason: String,
    pub saved_at: i64,
}
impl PlannerHoldRecord {
    pub fn transition(
        &mut self,
        paused: bool,
        activity_id: ActivityId,
        reason: String,
        now: i64,
    ) -> Result<(), DomainError> {
        if self.paused == paused {
            return Err(DomainError::InvalidState);
        }
        self.revision = self
            .revision
            .checked_add(1)
            .filter(|r| *r <= 9_007_199_254_740_991)
            .ok_or(DomainError::InvalidState)?;
        self.paused = paused;
        self.activity_id = activity_id;
        self.reason = reason;
        self.saved_at = now;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlannerPauseReceipt {
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub request: PauseWorker,
    pub paused: bool,
    pub task_id: Option<MessageId>,
    pub target_run_id: Option<RunId>,
    pub saved_at: i64,
    pub result: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GuidancePause {
    pub planner_paused: bool,
    pub manual_paused: bool,
    pub inherited_held: bool,
    pub revision: u64,
}
