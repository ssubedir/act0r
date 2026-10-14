//! Reversible admission holds for one physical assignment and its descendants.
use super::{actors::AgentTaskStatus, goal::GoalPauseRecord, *};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentPauseRequest {
    pub request_id: MessageId,
    pub expected_revision: u64,
    pub paused: bool,
}
impl AssignmentPauseRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.request_id.to_string() == "00000000-0000-0000-0000-000000000000"
            || self.expected_revision >= 9_007_199_254_740_991
        {
            return Err(DomainError::InvalidConfiguration(
                "an assignment hold needs a nonnil identity and a safe revision".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssignmentPauseReceipt {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub request: AssignmentPauseRequest,
    pub run_id: Option<RunId>,
    pub status: AgentTaskStatus,
    pub control: GoalPauseRecord,
    pub saved_at: i64,
    pub replayed: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct WorkerPause {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub run_id: Option<RunId>,
    pub status: AgentTaskStatus,
    pub control: GoalPauseRecord,
    /// A separate ancestor hold remains after this assignment's resume.
    pub inherited_held: bool,
    pub planner_hold: Option<planner_pause::PlannerHoldRecord>,
    pub execution_held: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_dependency: Option<MessageId>,
    /// Already admitted operations may still commit their results and usage.
    pub admitted_operations: u64,
    pub settled: bool,
    pub can_pause: bool,
    pub can_resume: bool,
    pub blocked_reason: Option<String>,
    pub receipts: Vec<AssignmentPauseReceipt>,
}
