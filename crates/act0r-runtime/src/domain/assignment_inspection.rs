//! Bounded, immutable evidence from the planner's exact current assignment.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const MAX_INSPECTIONS: u32 = 3;
pub const MAX_OUTPUT_BYTES: usize = 16_384;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectAssignment {
    pub worker: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InspectionReceipt {
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub request: InspectAssignment,
    pub result: Value,
}
/// Explicit UTF-8 truncation retains the original size; snippets are never complete results.
pub(crate) fn snippet(text: &str, max: usize) -> Value {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    json!({"text":&text[..end],"bytes":text.len(),"truncated":end < text.len()})
}
