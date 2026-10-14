//! Durable decisions to stop an exact assignment in the current planner join.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_STOPS: u32 = 2;
pub const MAX_REASON_BYTES: usize = 2048;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopWorker {
    pub worker: String,
    pub reason: String,
}
impl StopWorker {
    pub fn validate(&self) -> Result<(), tools::ToolError> {
        if self.reason.trim().is_empty() || self.reason.len() > MAX_REASON_BYTES {
            return Err(tools::ToolError::new(
                "worker_stop_reason",
                "A stop reason needs 1..2048 UTF-8 bytes.",
            ));
        }
        if !matches!(self.worker.as_str(), "coder" | "coder2") {
            return Err(tools::ToolError::new(
                "worker_stop_scope",
                "Choose coder or coder2 captured in this guidance step; other assignments cannot be stopped.",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlannerStopReceipt {
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub request: StopWorker,
    pub task_id: Option<MessageId>,
    pub target_run_id: Option<RunId>,
    pub saved_at: i64,
    pub result: Value,
}
