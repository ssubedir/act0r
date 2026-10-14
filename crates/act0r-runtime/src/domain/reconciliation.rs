use super::{ActivityId, AttemptId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An operator's assertion about one interrupted attempt, not a provider outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationRequest {
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub decision: ReconciliationDecision,
    pub evidence: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReconciliationDecision {
    Accept {
        // Require the field; serde_json::Value otherwise treats a missing field as null.
        #[serde(deserialize_with = "Value::deserialize")]
        result: Value,
    },
    Retry {},
    Fail {
        message: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReconciliationReceipt {
    pub request: ReconciliationRequest,
    pub recorded_at: i64,
}
