//! Operator recovery of the native harness's journaled edits and fixed tests.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRecoveryRequest {
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub decision: ToolRecoveryDecision,
    pub evidence: String,
    pub operation_stopped: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolRecoveryDecision {
    Retry {},
    Fail { message: String },
}
impl ToolRecoveryRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if !self.operation_stopped {
            return Err(DomainError::InvalidReconciliation(
                "confirm the previous operation and any command or test process have stopped"
                    .into(),
            ));
        }
        if self.evidence.trim().is_empty()
            || self.evidence.len() > 16384
            || matches!(&self.decision, ToolRecoveryDecision::Fail { message } if message.trim().is_empty() || message.len() > 16384)
        {
            return Err(DomainError::InvalidReconciliation(
                "inspection evidence and failure reason must contain 1 to 16384 UTF-8 bytes".into(),
            ));
        }
        Ok(())
    }
}
impl From<ToolRecoveryRequest> for ReconciliationRequest {
    fn from(request: ToolRecoveryRequest) -> Self {
        Self {
            activity_id: request.activity_id,
            attempt_id: request.attempt_id,
            evidence: request.evidence,
            decision: match request.decision {
                ToolRecoveryDecision::Retry {} => ReconciliationDecision::Retry {},
                ToolRecoveryDecision::Fail { message } => ReconciliationDecision::Fail { message },
            },
        }
    }
}
pub fn supported(request: &tools::NativeToolRequest, handler: &DefinitionId) -> bool {
    request.definition.id.version == 1
        && ["replace_text", "create_file", "run_tests", "run_command"]
            .contains(&request.definition.id.name.as_str())
        && request.definition.handler() == *handler
        && request.definition.requires_approval
        && request.definition.recovery == RecoveryPolicy::RequiresReconciliation
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffectState {
    NotStarted,
    Ready,
    AlreadyApplied,
    Conflicted,
    TestOutcomeUnknown,
    CommandOutcomeUnknown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolInspection {
    pub workspace: String,
    pub journal_exists: bool,
    pub state: ToolEffectState,
    pub path: Option<String>,
    pub before_sha256: Option<String>,
    pub after_sha256: Option<String>,
    pub observed_sha256: Option<String>,
    pub conflict: Option<String>,
}
