//! Verified completion is a domain decision over immutable workspace/test evidence.
use super::{ActivityId, DomainError, Failure, change_review::*, tools::*};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionPolicy {
    RequirePassingTests,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionStatus {
    Verified,
    Refused,
    Incomplete,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionReason {
    TestsNotRun,
    TestsStale,
    WrongTestCommand,
    TestsFailed,
    UnknownTestResult,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionReport {
    pub status: CompletionStatus,
    pub reason: Option<CompletionReason>,
    pub activity_id: ActivityId,
    pub workspace_digest: String,
    pub test_activity_id: Option<ActivityId>,
}
pub fn validate_config(
    policy: Option<CompletionPolicy>,
    review: bool,
    workspace: Option<&str>,
    command: Option<&WorkspaceTestCommand>,
    tools: &[ToolDefinition],
) -> Result<(), DomainError> {
    if policy.is_some()
        && (!review
            || workspace.is_none()
            || command.is_none()
            || !tools.iter().any(|t| {
                t.id == super::DefinitionId::new("run_tests", 1)
                    && t.requires_workspace
                    && t.requires_test_command
            }))
    {
        return Err(DomainError::InvalidConfiguration(
            "verified completion requires change_review, a managed workspace, and pinned run_tests v1 with a test command".into(),
        ));
    }
    Ok(())
}
pub fn incomplete(summary: &str) -> Result<Failure, DomainError> {
    if summary.trim().is_empty() || summary.len() > 60_000 {
        return Err(DomainError::InvalidConfiguration(
            "finish_incomplete requires a nonempty summary of at most 60000 bytes".into(),
        ));
    }
    Ok(Failure::new("coding_incomplete", summary, false))
}
pub fn assess(
    changes: &WorkspaceChangeSet,
    command: &WorkspaceTestCommand,
    incomplete: bool,
) -> CompletionReport {
    let reason = match &changes.tests {
        None => Some(CompletionReason::TestsNotRun),
        Some(t) if &t.command != command => Some(CompletionReason::WrongTestCommand),
        Some(t)
            if t.before_digest != t.after_digest || t.after_digest != changes.workspace_digest =>
        {
            Some(CompletionReason::TestsStale)
        }
        Some(t)
            if t.result["ok"] == false
                || t.result["exit_code"].as_i64().is_some_and(|c| c != 0) =>
        {
            Some(CompletionReason::TestsFailed)
        }
        Some(t) if t.result["ok"] != true || t.result["exit_code"].as_i64() != Some(0) => {
            Some(CompletionReason::UnknownTestResult)
        }
        Some(_) => None,
    };
    CompletionReport {
        status: if incomplete {
            CompletionStatus::Incomplete
        } else if reason.is_some() {
            CompletionStatus::Refused
        } else {
            CompletionStatus::Verified
        },
        reason,
        activity_id: changes.activity_id,
        workspace_digest: changes.workspace_digest.clone(),
        test_activity_id: changes.tests.as_ref().map(|t| t.activity_id),
    }
}
impl CompletionReport {
    pub fn feedback(&self) -> Value {
        json!({"completion":self,"message":"Completion refused. Run the pinned tests against the final workspace and repair failures before finishing. Use finish_incomplete with an explanation if the goal cannot be verified within the remaining budget."})
    }
}
pub(crate) fn action_schema() -> Value {
    json!({"type":"object","properties":{"kind":{"type":"string","enum":["finish_incomplete"]},"summary":{"type":"string","minLength":1,"maxLength":60000}},"required":["kind","summary"],"additionalProperties":false})
}
pub(crate) const INSTRUCTIONS: &str = " Successful finish and schedule_follow_up require fresh passing results from your pinned run_tests command that match the final workspace capture. Child test results do not verify your integrated workspace. A refused completion is saved as feedback; repair or retest within the remaining budget. Use finish_incomplete with a bounded explanation to save your patch and stop as coding_incomplete when verification cannot be completed.";
