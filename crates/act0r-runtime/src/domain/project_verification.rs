//! Explicit verification of an applied project's reviewed, immutable snapshot.
use super::{task::SnapshotFile, tools::WorkspaceTestCommand, *};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub const APPROVAL_KEY: &str = "project_verification_approval";
pub fn workflow() -> DefinitionId {
    DefinitionId::new("project.verify", 1)
}
pub fn handler(name: &str) -> DefinitionId {
    DefinitionId::new(format!("project.verify.{name}"), 1)
}
pub fn is_handler(id: &DefinitionId) -> bool {
    ["prepare", "run"].iter().any(|n| id == &handler(n))
}
fn hash_valid(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedFile {
    pub path: String,
    pub sha256: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationInput {
    pub application_id: RunId,
    pub task_id: MessageId,
    pub target: String,
    pub root_identity: String,
    pub applied_files: Vec<AppliedFile>,
    pub command: WorkspaceTestCommand,
}
impl VerificationInput {
    pub fn validate(&self) -> Result<(), DomainError> {
        harness::validate_workspace(&self.target)?;
        self.command.validate()?;
        let unique: BTreeSet<_> = self
            .applied_files
            .iter()
            .map(|f| f.path.to_ascii_lowercase())
            .collect();
        if self.root_identity.is_empty()
            || self.root_identity.len() > 256
            || self.applied_files.is_empty()
            || self.applied_files.len() > 1000
            || unique.len() != self.applied_files.len()
            || self.applied_files.iter().any(|f| {
                !tools::portable_file_path(&f.path)
                    || f.sha256.as_ref().is_some_and(|h| !hash_valid(h))
            })
        {
            return Err(DomainError::InvalidConfiguration(
                "verification requires the applied target identity and bounded file hashes".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSnapshot {
    pub run_id: RunId,
    pub preparation_id: ActivityId,
    pub root_identity: String,
    pub captured_at: i64,
    pub digest: String,
    pub files: Vec<SnapshotFile>,
}
impl ProjectSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        let mut previous: Option<&str> = None;
        let mut names = BTreeSet::new();
        let mut bytes = 0u64;
        for f in &self.files {
            if !tools::portable_file_path(&f.path)
                || !hash_valid(&f.sha256)
                || previous.is_some_and(|p| p >= f.path.as_str())
                || !names.insert(f.path.to_ascii_lowercase())
            {
                return Err(DomainError::InvalidState);
            }
            previous = Some(&f.path);
            bytes = bytes.saturating_add(f.bytes);
        }
        let digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&self.files).map_err(|_| DomainError::InvalidState)?)
        );
        if self.files.len() > 10000
            || bytes > 64 * 1024 * 1024
            || self.root_identity.is_empty()
            || self.root_identity.len() > 256
            || self.captured_at < 0
            || self.digest != digest
            || serde_json::to_vec(self)
                .map_err(|_| DomainError::InvalidState)?
                .len()
                > 4 * 1024 * 1024
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
    pub fn validate_for(&self, input: &VerificationInput) -> Result<(), DomainError> {
        input.validate()?;
        self.validate()?;
        if self.root_identity != input.root_identity
            || input.applied_files.iter().any(|f| {
                self.files
                    .iter()
                    .find(|s| s.path == f.path)
                    .map(|s| &s.sha256)
                    != f.sha256.as_ref()
            })
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationRequest {
    pub input: VerificationInput,
    pub snapshot: ProjectSnapshot,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationOutput {
    pub ok: bool,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub elapsed_ms: u64,
    pub output_truncated: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Verified,
    Failed,
    Changed,
    Rejected,
}
#[derive(Debug, Serialize)]
pub struct VerificationSummary {
    pub run_id: RunId,
    pub status: RunStatus,
    pub outcome: Option<VerificationStatus>,
}
#[derive(Debug, Serialize)]
pub struct VerificationPage {
    pub items: Vec<VerificationSummary>,
    pub truncated: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationReport {
    pub run_id: RunId,
    pub application_id: RunId,
    pub target: String,
    pub command: WorkspaceTestCommand,
    pub status: VerificationStatus,
    pub checked_at: i64,
    pub before: Option<ProjectSnapshot>,
    pub after: Option<ProjectSnapshot>,
    pub output: Option<VerificationOutput>,
    pub reason: Option<String>,
}
impl VerificationReport {
    pub fn validate_for(&self, request: &VerificationRequest) -> Result<(), DomainError> {
        request.snapshot.validate_for(&request.input)?;
        if self.run_id != request.snapshot.run_id
            || self.application_id != request.input.application_id
            || self.target != request.input.target
            || self.command != request.input.command
            || self.checked_at < 0
            || self
                .reason
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 4096)
        {
            return Err(DomainError::InvalidState);
        }
        for snapshot in [&self.before, &self.after].into_iter().flatten() {
            snapshot.validate()?;
            if snapshot.run_id != self.run_id
                || snapshot.preparation_id != request.snapshot.preparation_id
            {
                return Err(DomainError::InvalidState);
            }
        }
        if let Some(o) = &self.output
            && (o.stdout.len() > 262144
                || o.stderr.len() > 262144
                || o.ok != (o.exit_code == Some(0))
                || o.output_truncated != (o.stdout_truncated || o.stderr_truncated))
        {
            return Err(DomainError::InvalidState);
        }
        let matching = self.before.as_ref().is_some_and(|b| {
            b.digest == request.snapshot.digest && b.root_identity == request.snapshot.root_identity
        }) && self.after.as_ref().is_some_and(|a| {
            a.digest == request.snapshot.digest && a.root_identity == request.snapshot.root_identity
        });
        let valid = match self.status {
            VerificationStatus::Verified => {
                matching && self.output.as_ref().is_some_and(|o| o.ok) && self.reason.is_none()
            }
            VerificationStatus::Failed => matching && self.output.as_ref().is_some_and(|o| !o.ok),
            VerificationStatus::Changed => !matching && self.reason.is_some(),
            VerificationStatus::Rejected => {
                self.before.is_none() && self.after.is_none() && self.output.is_none()
            }
        };
        if !valid {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerificationInspection {
    pub saved_report: bool,
    pub project_matches: bool,
    pub observed_digest: Option<String>,
    pub error: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Preparing,
    AwaitingApproval,
    Running,
    Finished,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    input: VerificationInput,
    phase: Phase,
    snapshot: Option<ProjectSnapshot>,
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
pub struct ProjectVerificationWorkflow;
impl WorkflowDefinition for ProjectVerificationWorkflow {
    fn id(&self) -> DefinitionId {
        workflow()
    }
    fn validate_state(&self, value: &Value) -> Result<(), DomainError> {
        let state: State = decode(value)?;
        state.input.validate()?;
        match (&state.phase, &state.snapshot) {
            (Phase::Preparing, None) => Ok(()),
            (Phase::Preparing, Some(_)) | (_, None) => Err(DomainError::InvalidState),
            (_, Some(s)) => s.validate_for(&state.input),
        }
    }
    fn transition(&self, value: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        if let WorkflowEvent::Started(input) = event {
            let input: VerificationInput = decode(input)?;
            input.validate()?;
            return Ok(Transition {
                state: json!(State {
                    input: input.clone(),
                    phase: Phase::Preparing,
                    snapshot: None
                }),
                commands: vec![activity(
                    "prepare",
                    json!(input),
                    RecoveryPolicy::SafeToRepeat,
                )],
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
                let snapshot: ProjectSnapshot = decode(result)?;
                snapshot.validate_for(&state.input)?;
                if snapshot.preparation_id != *activity_id {
                    return Err(DomainError::InvalidState);
                }
                state.phase = Phase::AwaitingApproval;
                state.snapshot = Some(snapshot.clone());
                Command::WaitForInput {
                    key: APPROVAL_KEY.into(),
                    prompt: "Run the reviewed verification command in the original project?".into(),
                    payload: json!(VerificationRequest {
                        input: state.input.clone(),
                        snapshot
                    }),
                }
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
                let snapshot = state.snapshot.clone().ok_or(DomainError::InvalidState)?;
                let request = VerificationRequest {
                    input: state.input.clone(),
                    snapshot,
                };
                if approval.approved {
                    state.phase = Phase::Running;
                    activity(
                        "run",
                        json!(request),
                        RecoveryPolicy::RequiresReconciliation,
                    )
                } else {
                    state.phase = Phase::Finished;
                    Command::Complete(json!(VerificationReport {
                        run_id: request.snapshot.run_id,
                        application_id: state.input.application_id,
                        target: state.input.target.clone(),
                        command: state.input.command.clone(),
                        status: VerificationStatus::Rejected,
                        checked_at: request.snapshot.captured_at,
                        before: None,
                        after: None,
                        output: None,
                        reason: None
                    }))
                }
            }
            (
                Phase::Running,
                WorkflowEvent::ActivityCompleted {
                    handler: id,
                    result,
                    ..
                },
            ) if id == &handler("run") => {
                let report: VerificationReport = decode(result)?;
                report.validate_for(&VerificationRequest {
                    input: state.input.clone(),
                    snapshot: state.snapshot.clone().ok_or(DomainError::InvalidState)?,
                })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> VerificationRequest {
        let sha = "1".repeat(64);
        let files = vec![SnapshotFile {
            path: "logic.txt".into(),
            sha256: sha.clone(),
            bytes: 6,
        }];
        VerificationRequest {
            input: VerificationInput {
                application_id: RunId::new(),
                task_id: MessageId::new(),
                target: std::env::temp_dir()
                    .join("verification-unit")
                    .to_string_lossy()
                    .into_owned(),
                root_identity: "root-identity".into(),
                applied_files: vec![AppliedFile {
                    path: "logic.txt".into(),
                    sha256: Some(sha),
                }],
                command: WorkspaceTestCommand {
                    argv: vec!["custom-check".into()],
                    timeout_ms: 1000,
                },
            },
            snapshot: ProjectSnapshot {
                run_id: RunId::new(),
                preparation_id: ActivityId::new(),
                root_identity: "root-identity".into(),
                captured_at: 1,
                digest: format!("{:x}", Sha256::digest(serde_json::to_vec(&files).unwrap())),
                files,
            },
        }
    }
    #[test]
    fn passing_output_cannot_verify_changed_inputs_or_a_different_command() {
        let request = request();
        let mut report = VerificationReport {
            run_id: request.snapshot.run_id,
            application_id: request.input.application_id,
            target: request.input.target.clone(),
            command: request.input.command.clone(),
            status: VerificationStatus::Verified,
            checked_at: 2,
            before: Some(request.snapshot.clone()),
            after: Some(request.snapshot.clone()),
            output: Some(VerificationOutput {
                ok: true,
                exit_code: Some(0),
                stdout: "passed".into(),
                stderr: String::new(),
                elapsed_ms: 1,
                output_truncated: false,
                stdout_truncated: false,
                stderr_truncated: false,
            }),
            reason: None,
        };
        report.validate_for(&request).unwrap();
        let after = report.after.as_mut().unwrap();
        after.files[0].sha256 = "2".repeat(64);
        after.digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&after.files).unwrap())
        );
        assert!(report.validate_for(&request).is_err());
        report.status = VerificationStatus::Changed;
        report.reason = Some("inputs changed".into());
        report.validate_for(&request).unwrap();
        report.command.argv = vec!["other-check".into()];
        assert!(report.validate_for(&request).is_err());
    }
    #[test]
    fn approval_only_authorizes_the_existing_prepared_command_and_snapshot() {
        let request = request();
        let workflow = ProjectVerificationWorkflow;
        let initial = workflow
            .transition(&Value::Null, &WorkflowEvent::Started(json!(request.input)))
            .unwrap();
        let prepared = workflow
            .transition(
                &initial.state,
                &WorkflowEvent::ActivityCompleted {
                    activity_id: request.snapshot.preparation_id,
                    handler: handler("prepare"),
                    result: json!(request.snapshot),
                },
            )
            .unwrap();
        assert!(
            workflow
                .transition(
                    &prepared.state,
                    &WorkflowEvent::InputReceived {
                        wait_id: WaitId::new(),
                        key: APPROVAL_KEY.into(),
                        payload: json!({"approved":true,"command":["other"]})
                    }
                )
                .is_err()
        );
        let admitted = workflow
            .transition(
                &prepared.state,
                &WorkflowEvent::InputReceived {
                    wait_id: WaitId::new(),
                    key: APPROVAL_KEY.into(),
                    payload: json!({"approved":true}),
                },
            )
            .unwrap();
        let Command::Schedule(spec) = &admitted.commands[0] else {
            panic!("expected verification activity")
        };
        assert_eq!(spec.handler, handler("run"));
        assert_eq!(spec.recovery, RecoveryPolicy::RequiresReconciliation);
        assert_eq!(spec.input, json!(request));
        let rejected = workflow
            .transition(
                &prepared.state,
                &WorkflowEvent::InputReceived {
                    wait_id: WaitId::new(),
                    key: APPROVAL_KEY.into(),
                    payload: json!({"approved":false}),
                },
            )
            .unwrap();
        assert!(
            matches!(&rejected.commands[0],Command::Complete(report) if report["status"]=="rejected")
        );
    }
}
