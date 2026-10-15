//! Operator recovery of journaled tools and external MCP actions.
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
    Accept {
        #[serde(deserialize_with = "serde_json::Value::deserialize")]
        result: serde_json::Value,
    },
    Retry {},
    Fail {
        message: String,
    },
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
                ToolRecoveryDecision::Accept { result } => {
                    ReconciliationDecision::Accept { result }
                }
                ToolRecoveryDecision::Retry {} => ReconciliationDecision::Retry {},
                ToolRecoveryDecision::Fail { message } => ReconciliationDecision::Fail { message },
            },
        }
    }
}
pub fn supported(request: &tools::NativeToolRequest, handler: &DefinitionId) -> bool {
    request.definition.id.version == 1
        && (["replace_text", "create_file", "run_tests", "run_command"]
            .contains(&request.definition.id.name.as_str())
            || external_action(request))
        && request.definition.handler() == *handler
        && request.definition.requires_approval
        && request.definition.recovery == RecoveryPolicy::RequiresReconciliation
}
pub fn external_action(request: &tools::NativeToolRequest) -> bool {
    request
        .definition
        .external
        .as_ref()
        .is_some_and(|contract| !contract.read_only)
        && !request.definition.requires_workspace
        && request.workspace.is_none()
        && request.writable_paths.is_empty()
        && request.test_command.is_none()
        && request.project_command.is_none()
}

/// Operator-supplied results must fit the same text/JSON contract as live MCP calls.
pub fn validate_external_result(
    request: &tools::NativeToolRequest,
    result: &serde_json::Value,
) -> Result<(), DomainError> {
    let valid = external_action(request)
        && result.is_object()
        && result["ok"] == true
        && result.get("isError").is_none_or(|value| value == false)
        && result["content"].as_array().is_some_and(|content| {
            content
                .iter()
                .all(|item| item["type"] == "text" && item["text"].is_string())
        })
        && serde_json::to_vec(result)
            .is_ok_and(|bytes| bytes.len() <= request.definition.max_output_bytes as usize);
    if !valid {
        return Err(DomainError::InvalidReconciliation("accept requires a verified successful MCP result with ok:true, text content, and any structuredContent, within the saved output limit".into()));
    }
    request
        .definition
        .external
        .as_ref()
        .unwrap()
        .validate_output(result.get("structuredContent"))
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
