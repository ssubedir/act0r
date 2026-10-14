//! Explicit, checkpointed application of one immutable task change set.
use super::{change_review::*, *};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub const APPROVAL_KEY: &str = "patch_application_approval";
pub fn workflow() -> DefinitionId {
    DefinitionId::new("workspace.apply", 1)
}
pub fn handler(name: &str) -> DefinitionId {
    DefinitionId::new(format!("workspace.apply.{name}"), 1)
}
pub fn is_handler(id: &DefinitionId) -> bool {
    ["prepare", "write"].iter().any(|name| id == &handler(name))
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationInput {
    pub task_id: MessageId,
    pub target: String,
    pub changes: WorkspaceChangeSet,
}
impl ApplicationInput {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.changes.validate()?;
        if self.target.is_empty() || self.target.len() > 4096 || self.target.contains('\0') {
            return Err(DomainError::InvalidConfiguration(
                "a bounded target path is required".into(),
            ));
        }
        let mut paths = BTreeSet::new();
        for file in &self.changes.files {
            if !paths.insert(file.path.to_ascii_lowercase()) {
                return Err(DomainError::InvalidConfiguration(
                    "case-aliased change paths are refused".into(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationFile {
    pub path: String,
    pub before_sha256: Option<String>,
    pub after_sha256: Option<String>,
    pub observed_sha256: Option<String>,
    pub conflict: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationPreview {
    pub run_id: RunId,
    pub preparation_id: ActivityId,
    pub input_digest: String,
    pub target: String,
    pub root_identity: String,
    pub files: Vec<ApplicationFile>,
    pub test_status: TestEvidenceStatus,
    pub tests: Option<TestEvidence>,
}
impl ApplicationPreview {
    pub fn has_conflicts(&self) -> bool {
        self.files.iter().any(|f| f.conflict.is_some())
    }
    pub fn validate_for(&self, input: &ApplicationInput) -> Result<(), DomainError> {
        input.validate()?;
        if self.files.len() != input.changes.files.len()
            || self.target.is_empty()
            || self.target.len() > 4096
            || self.root_identity.is_empty()
            || self.root_identity.len() > 256
            || self.input_digest.len() != 64
            || !self.input_digest.bytes().all(|b| b.is_ascii_hexdigit())
            || self.test_status != input.changes.test_status
            || self.tests != input.changes.tests
        {
            return Err(DomainError::InvalidState);
        }
        for (file, diff) in self.files.iter().zip(&input.changes.files) {
            if file.path != diff.path
                || file.before_sha256 != diff.before_sha256
                || file.after_sha256 != diff.after_sha256
                || file
                    .conflict
                    .as_ref()
                    .is_some_and(|c| c.is_empty() || c.len() > 4096)
                || (file.conflict.is_none() && file.observed_sha256 != file.before_sha256)
            {
                return Err(DomainError::InvalidState);
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationStatus {
    Applied,
    Conflicted,
    Rejected,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationReport {
    pub run_id: RunId,
    pub target: String,
    pub status: ApplicationStatus,
    pub applied_files: Vec<String>,
    pub conflicts: Vec<ApplicationFile>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    pub input: ApplicationInput,
    pub preview: ApplicationPreview,
}

/// Browser recovery permits resuming the saved write or recording failure, never invented results.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationRecoveryRequest {
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub decision: ApplicationRecoveryDecision,
    pub evidence: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationRecoveryDecision {
    Retry {},
    Fail { message: String },
}
impl From<ApplicationRecoveryRequest> for ReconciliationRequest {
    fn from(request: ApplicationRecoveryRequest) -> Self {
        Self {
            activity_id: request.activity_id,
            attempt_id: request.attempt_id,
            evidence: request.evidence,
            decision: match request.decision {
                ApplicationRecoveryDecision::Retry {} => ReconciliationDecision::Retry {},
                ApplicationRecoveryDecision::Fail { message } => {
                    ReconciliationDecision::Fail { message }
                }
            },
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationFileProgress {
    Pending,
    Intended,
    Applied,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationFileInspection {
    pub file: ApplicationFile,
    pub progress: ApplicationFileProgress,
}
/// A read-only observation. Execution checks the hashes again under its project lock.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationInspection {
    pub journal_exists: bool,
    pub started: bool,
    pub files: Vec<ApplicationFileInspection>,
    pub report: Option<ApplicationReport>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Preparing,
    AwaitingApproval,
    Applying,
    Finished,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    input: ApplicationInput,
    phase: Phase,
    preview: Option<ApplicationPreview>,
}
fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, DomainError> {
    serde_json::from_value(value.clone()).map_err(|e| DomainError::InvalidWorkflow(e.to_string()))
}
fn activity(name: &str, input: Value, recovery: RecoveryPolicy) -> Command {
    Command::Schedule(ActivitySpec {
        handler: handler(name),
        input,
        recovery,
        retry: RetryPolicy::default(),
        usage: None,
    })
}
pub struct PatchApplicationWorkflow;
impl WorkflowDefinition for PatchApplicationWorkflow {
    fn id(&self) -> DefinitionId {
        workflow()
    }
    fn validate_state(&self, value: &Value) -> Result<(), DomainError> {
        let state: State = decode(value)?;
        state.input.validate()?;
        match (&state.phase, &state.preview) {
            (Phase::Preparing, None) => Ok(()),
            (Phase::Preparing, Some(_)) | (_, None) => Err(DomainError::InvalidState),
            (_, Some(preview)) => preview.validate_for(&state.input),
        }
    }
    fn transition(&self, value: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        if let WorkflowEvent::Started(value) = event {
            let input: ApplicationInput = decode(value)?;
            input.validate()?;
            let command = activity("prepare", json!(input), RecoveryPolicy::SafeToRepeat);
            return Ok(Transition {
                state: json!(State {
                    input,
                    phase: Phase::Preparing,
                    preview: None
                }),
                commands: vec![command],
            });
        }
        self.validate_state(value)?;
        let mut state: State = decode(value)?;
        let command = match (&state.phase, event) {
            (
                Phase::Preparing,
                WorkflowEvent::ActivityCompleted {
                    activity_id,
                    handler: id,
                    result,
                },
            ) if id == &handler("prepare") => {
                let preview: ApplicationPreview = decode(result)?;
                preview.validate_for(&state.input)?;
                if preview.preparation_id != *activity_id {
                    return Err(DomainError::InvalidState);
                }
                let command = if preview.has_conflicts() {
                    state.phase = Phase::Finished;
                    Command::Complete(json!(ApplicationReport {
                        run_id: preview.run_id,
                        target: preview.target.clone(),
                        status: ApplicationStatus::Conflicted,
                        applied_files: vec![],
                        conflicts: preview
                            .files
                            .iter()
                            .filter(|f| f.conflict.is_some())
                            .cloned()
                            .collect()
                    }))
                } else {
                    state.phase = Phase::AwaitingApproval;
                    Command::WaitForInput {
                        key: APPROVAL_KEY.into(),
                        prompt: "Apply this saved patch to the reviewed target?".into(),
                        payload: json!(preview),
                    }
                };
                state.preview = Some(preview);
                command
            }
            (Phase::AwaitingApproval, WorkflowEvent::InputReceived { key, payload, .. })
                if key == APPROVAL_KEY =>
            {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Approval {
                    approved: bool,
                }
                let approval: Approval = decode(payload)?;
                let preview = state.preview.clone().ok_or(DomainError::InvalidState)?;
                if approval.approved {
                    state.phase = Phase::Applying;
                    activity(
                        "write",
                        json!(WriteRequest {
                            input: state.input.clone(),
                            preview
                        }),
                        RecoveryPolicy::RequiresReconciliation,
                    )
                } else {
                    state.phase = Phase::Finished;
                    Command::Complete(json!(ApplicationReport {
                        run_id: preview.run_id,
                        target: preview.target,
                        status: ApplicationStatus::Rejected,
                        applied_files: vec![],
                        conflicts: vec![]
                    }))
                }
            }
            (
                Phase::Applying,
                WorkflowEvent::ActivityCompleted {
                    handler: id,
                    result,
                    ..
                },
            ) if id == &handler("write") => {
                let report: ApplicationReport = decode(result)?;
                let preview = state.preview.as_ref().ok_or(DomainError::InvalidState)?;
                let paths: Vec<_> = preview.files.iter().map(|f| f.path.clone()).collect();
                if report.run_id != preview.run_id
                    || report.target != preview.target
                    || match report.status {
                        ApplicationStatus::Applied => {
                            report.applied_files != paths || !report.conflicts.is_empty()
                        }
                        ApplicationStatus::Conflicted => {
                            !report.applied_files.is_empty()
                                || report.conflicts.is_empty()
                                || report
                                    .conflicts
                                    .iter()
                                    .any(|f| !paths.contains(&f.path) || f.conflict.is_none())
                        }
                        ApplicationStatus::Rejected => true,
                    }
                {
                    return Err(DomainError::InvalidState);
                }
                state.phase = Phase::Finished;
                Command::Complete(json!(report))
            }
            _ => return Err(DomainError::InvalidState),
        };
        Ok(Transition {
            state: json!(state),
            commands: vec![command],
        })
    }
}
