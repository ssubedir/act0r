use super::{ActivityId, AttemptId, EventId, RunId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryEvent {
    pub id: EventId,
    pub run_id: RunId,
    pub sequence: u64,
    pub kind: String,
    pub payload_version: u32,
    pub payload: Value,
    pub activity_id: Option<ActivityId>,
    pub attempt_id: Option<AttemptId>,
    pub at: i64,
}
