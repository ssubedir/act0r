//! Stop one physical assignment without changing its actor or sibling tasks.
use super::{actors::AgentTaskStatus, *};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopAssignmentRequest {
    pub request_id: MessageId,
}
impl StopAssignmentRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.request_id.to_string() == "00000000-0000-0000-0000-000000000000" {
            return Err(DomainError::InvalidConfiguration(
                "a stop request needs a nonnil identity".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopAssignmentReceipt {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub request: StopAssignmentRequest,
    pub run_id: Option<RunId>,
    pub status: AgentTaskStatus,
    pub saved_at: i64,
    pub replayed: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct WorkerCancellation {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub run_id: Option<RunId>,
    pub status: AgentTaskStatus,
    pub requested: bool,
    pub settled: bool,
    pub can_stop: bool,
    pub blocked_reason: Option<String>,
    pub receipts: Vec<StopAssignmentReceipt>,
}
