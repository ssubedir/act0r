//! Pure checkpoints for task-scoped workspace review. Filesystem I/O stays in activities.
use super::{
    task::{FileDiff, WorkspaceSnapshot},
    *,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const MAX_PATCH_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_REVIEW_BYTES: u64 = 32 * 1024 * 1024;
pub fn handler(name: &str) -> DefinitionId {
    DefinitionId::new(format!("workspace.{name}"), 1)
}
pub fn is_handler(id: &DefinitionId) -> bool {
    ["baseline", "fingerprint", "changes"]
        .iter()
        .any(|n| id == &handler(n))
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestEvidence {
    pub activity_id: ActivityId,
    pub command: tools::WorkspaceTestCommand,
    pub before_digest: String,
    pub after_digest: String,
    pub result: Value,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestEvidenceStatus {
    NotRun,
    Matching,
    Stale,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceChangeSet {
    pub version: u32,
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub baseline_digest: String,
    pub workspace_digest: String,
    pub files: Vec<FileDiff>,
    pub changed_bytes: u64,
    pub test_status: TestEvidenceStatus,
    pub tests: Option<TestEvidence>,
}
impl WorkspaceChangeSet {
    pub fn patch(&self) -> String {
        self.files.iter().map(|f| f.patch.as_str()).collect()
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        let valid_hash = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        let expected = match &self.tests {
            None => TestEvidenceStatus::NotRun,
            Some(t)
                if t.before_digest == t.after_digest && t.after_digest == self.workspace_digest =>
            {
                TestEvidenceStatus::Matching
            }
            Some(_) => TestEvidenceStatus::Stale,
        };
        let mut previous: Option<&str> = None;
        let mut total = 0u64;
        let mut patch_bytes = 0usize;
        for f in &self.files {
            if !tools::portable_file_path(&f.path)
                || f.path.chars().any(char::is_control)
                || previous.is_some_and(|p| p >= f.path.as_str())
                || f.before_sha256 == f.after_sha256
                || f.before_sha256
                    .iter()
                    .chain(&f.after_sha256)
                    .any(|s| !valid_hash(s))
            {
                return Err(DomainError::InvalidState);
            }
            previous = Some(&f.path);
            total = total
                .checked_add(f.changed_bytes)
                .ok_or(DomainError::InvalidState)?;
            patch_bytes = patch_bytes
                .checked_add(f.patch.len())
                .ok_or(DomainError::InvalidState)?;
        }
        if self.version != 1
            || !valid_hash(&self.baseline_digest)
            || !valid_hash(&self.workspace_digest)
            || self.files.len() > 10_000
            || patch_bytes > MAX_PATCH_BYTES
            || self.changed_bytes != total
            || self.test_status != expected
        {
            return Err(DomainError::InvalidState);
        }
        if let Some(t) = &self.tests {
            t.command.validate()?;
            if !valid_hash(&t.before_digest)
                || !valid_hash(&t.after_digest)
                || serde_json::to_vec(&t.result)
                    .map_err(|_| DomainError::InvalidState)?
                    .len()
                    > 1_048_576
            {
                return Err(DomainError::InvalidState);
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| DomainError::InvalidState)?
            .len() as u64
            > MAX_REVIEW_BYTES
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewRequest {
    pub workspace: String,
    #[serde(default)]
    pub baseline: Option<WorkspaceSnapshot>,
    #[serde(default)]
    pub tests: Option<TestEvidence>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskChanges {
    pub task_id: MessageId,
    pub run_id: Option<RunId>,
    pub run_status: Option<RunStatus>,
    pub enabled: bool,
    pub changes: Option<WorkspaceChangeSet>,
    pub error: Option<String>,
    /// Assessment of this immutable candidate; refused captures can precede more work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<completion::CompletionReport>,
}

#[derive(Serialize, Deserialize)]
enum Completion {
    Result(Value),
    Incomplete(Failure),
    FollowUp {
        result: Value,
        goal: String,
        milliseconds: u64,
    },
}
impl Completion {
    fn command(self, report: Option<&completion::CompletionReport>) -> Command {
        let attach = |mut result: Value| {
            if let Some(report) = report {
                result["completion"] = json!(report);
            }
            result
        };
        match self {
            Self::Result(v) => Command::Complete(attach(v)),
            Self::Incomplete(failure) => Command::Fail(failure),
            Self::FollowUp {
                result,
                goal,
                milliseconds,
            } => Command::CompleteWithFollowUp {
                result: attach(result),
                goal,
                milliseconds,
            },
        }
    }
}
#[derive(Serialize, Deserialize)]
enum Phase {
    Baseline(ActivitySpec),
    Ready,
    BeforeTest(ActivitySpec),
    Testing {
        before_digest: String,
        command: tools::WorkspaceTestCommand,
    },
    AfterTest {
        evidence: TestEvidence,
        event_handler: DefinitionId,
    },
    Final(Completion),
    Finished,
}
#[derive(Serialize, Deserialize)]
struct ReviewState {
    baseline: Option<WorkspaceSnapshot>,
    tests: Option<TestEvidence>,
    phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completion: Option<completion::CompletionReport>,
}
const KEY: &str = "change_review_state";
pub(crate) fn ready_for_worker_changes(state: &Value) -> bool {
    state
        .get(KEY)
        .and_then(|v| decode::<ReviewState>(v).ok())
        .is_some_and(|r| r.baseline.is_some() && matches!(r.phase, Phase::Ready))
}
pub(crate) fn invalidate_tests(state: &mut Value) -> Result<(), DomainError> {
    let mut review: ReviewState = decode(state.get(KEY).ok_or(DomainError::InvalidState)?)?;
    if !matches!(review.phase, Phase::Ready) {
        return Err(DomainError::InvalidState);
    }
    review.tests = None;
    review.completion = None;
    state[KEY] = serde_json::to_value(review).map_err(|_| DomainError::InvalidState)?;
    Ok(())
}
fn decode<T: serde::de::DeserializeOwned>(v: &Value) -> Result<T, DomainError> {
    serde_json::from_value(v.clone()).map_err(|e| DomainError::InvalidWorkflow(e.to_string()))
}
fn fingerprint(result: &Value) -> Result<String, DomainError> {
    let hash: String = decode(&result["digest"])?;
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(DomainError::InvalidState);
    }
    Ok(hash)
}
fn enabled(v: &Value) -> bool {
    v["change_review"].as_bool().unwrap_or(false)
}
fn request(state: &Value, review: &ReviewState, name: &str) -> Result<Command, DomainError> {
    let workspace = state["config"]["workspace"]
        .as_str()
        .ok_or(DomainError::InvalidState)?;
    Ok(Command::Schedule(ActivitySpec {
        handler: handler(name),
        input: json!(ReviewRequest {
            workspace: workspace.into(),
            baseline: if name == "changes" {
                review.baseline.clone()
            } else {
                None
            },
            tests: if name == "changes" {
                review.tests.clone()
            } else {
                None
            }
        }),
        retry: RetryPolicy::default(),
        recovery: RecoveryPolicy::SafeToRepeat,
        usage: None,
    }))
}
fn save(mut transition: Transition, review: ReviewState) -> Result<Transition, DomainError> {
    transition.state[KEY] = serde_json::to_value(review).map_err(|_| DomainError::InvalidState)?;
    Ok(transition)
}
pub fn validate_state(state: &Value) -> Result<(), DomainError> {
    if enabled(&state["config"]) != state.get(KEY).is_some() {
        return Err(DomainError::InvalidState);
    }
    if let Some(v) = state.get(KEY) {
        let _: ReviewState = decode(v)?;
    }
    Ok(())
}
/// Read the saved assessment without exposing checkpoint internals to adapters.
pub fn completion_report(
    state: &Value,
) -> Result<Option<completion::CompletionReport>, DomainError> {
    state
        .get(KEY)
        .map(decode::<ReviewState>)
        .transpose()
        .map(|r| r.and_then(|r| r.completion))
}
fn intercept(mut t: Transition, mut review: ReviewState) -> Result<Transition, DomainError> {
    for command in &mut t.commands {
        if matches!(
            command,
            Command::Complete(_) | Command::CompleteWithFollowUp { .. } | Command::Fail(_)
        ) {
            let completion = match std::mem::replace(command, Command::PauseAtBoundary) {
                Command::Complete(v) => Completion::Result(v),
                Command::Fail(failure) => Completion::Incomplete(failure),
                Command::CompleteWithFollowUp {
                    result,
                    goal,
                    milliseconds,
                } => Completion::FollowUp {
                    result,
                    goal,
                    milliseconds,
                },
                _ => return Err(DomainError::InvalidState),
            };
            review.phase = Phase::Final(completion);
            *command = request(&t.state, &review, "changes")?;
        } else if let Command::Schedule(spec) = command
            && tools::is_native_tool_handler(&spec.handler)
            && spec.input["definition"]["requires_project_command"] == true
        {
            // Commands may change files even on failure or produce side effects outside
            // the captured paths. A matching digest alone cannot keep old tests fresh.
            review.tests = None;
        } else if let Command::Schedule(spec) = command
            && tools::is_native_tool_handler(&spec.handler)
            && spec.input["definition"]["id"]["name"] == "run_tests"
            && spec.input["definition"]["id"]["version"] == 1
        {
            review.phase = Phase::BeforeTest(spec.clone());
            *command = request(&t.state, &review, "fingerprint")?;
        }
    }
    save(t, review)
}
/// Share checkpoints between the existing answering and planner policies without a new policy.
pub fn transition(
    state: &Value,
    event: &WorkflowEvent,
    inner: impl Fn(&Value, &WorkflowEvent) -> Result<Transition, DomainError>,
) -> Result<Transition, DomainError> {
    if let WorkflowEvent::Started(input) = event {
        let mut t = inner(state, event)?;
        if !enabled(input) {
            return Ok(t);
        }
        let index = t
            .commands
            .iter()
            .position(|c| matches!(c, Command::Schedule(_)))
            .ok_or(DomainError::InvalidState)?;
        let Command::Schedule(first) = t.commands.remove(index) else {
            return Err(DomainError::InvalidState);
        };
        let review = ReviewState {
            baseline: None,
            tests: None,
            phase: Phase::Baseline(first),
            completion: None,
        };
        t.commands
            .insert(index, request(&t.state, &review, "baseline")?);
        return save(t, review);
    }
    validate_state(state)?;
    let Some(v) = state.get(KEY) else {
        return inner(state, event);
    };
    let mut review: ReviewState = decode(v)?;
    if let WorkflowEvent::ActivityCompleted {
        activity_id,
        handler: event_handler,
        result,
    } = event
    {
        match std::mem::replace(&mut review.phase, Phase::Ready) {
            Phase::Baseline(first) if event_handler == &handler("baseline") => {
                review.baseline = Some(decode(result)?);
                return save(
                    Transition {
                        state: state.clone(),
                        commands: vec![Command::Schedule(first)],
                    },
                    review,
                );
            }
            Phase::BeforeTest(call) if event_handler == &handler("fingerprint") => {
                review.phase = Phase::Testing {
                    before_digest: fingerprint(result)?,
                    command: decode(&call.input["test_command"])?,
                };
                return save(
                    Transition {
                        state: state.clone(),
                        commands: vec![Command::Schedule(call)],
                    },
                    review,
                );
            }
            Phase::Testing {
                before_digest,
                command,
            } if tools::is_native_tool_handler(event_handler) => {
                review.phase = Phase::AfterTest {
                    evidence: TestEvidence {
                        activity_id: *activity_id,
                        command,
                        before_digest,
                        after_digest: String::new(),
                        result: result.clone(),
                    },
                    event_handler: event_handler.clone(),
                };
                let command = request(state, &review, "fingerprint")?;
                return save(
                    Transition {
                        state: state.clone(),
                        commands: vec![command],
                    },
                    review,
                );
            }
            Phase::AfterTest {
                mut evidence,
                event_handler: original_handler,
            } if event_handler == &handler("fingerprint") => {
                evidence.after_digest = fingerprint(result)?;
                let original = WorkflowEvent::ActivityCompleted {
                    activity_id: evidence.activity_id,
                    handler: original_handler,
                    result: evidence.result.clone(),
                };
                review.tests = Some(evidence);
                return intercept(inner(state, &original)?, review);
            }
            Phase::Final(completion) if event_handler == &handler("changes") => {
                let changes: WorkspaceChangeSet = decode(result)?;
                changes.validate()?;
                if changes.activity_id != *activity_id
                    || changes.tests != review.tests
                    || Some(&changes.baseline_digest) != review.baseline.as_ref().map(|b| &b.digest)
                {
                    return Err(DomainError::InvalidState);
                }
                review.phase = Phase::Finished;
                if let Some(policy) = state["config"].get("completion_policy")
                    && !policy.is_null()
                {
                    let _: completion::CompletionPolicy = decode(policy)?;
                    let command = decode(&state["config"]["test_command"])?;
                    let report = completion::assess(
                        &changes,
                        &command,
                        matches!(&completion, Completion::Incomplete(_)),
                    );
                    let refused = report.status == completion::CompletionStatus::Refused;
                    let feedback = report.feedback();
                    review.completion = Some(report);
                    if refused {
                        review.phase = Phase::Ready;
                        return intercept(
                            inner(state, &WorkflowEvent::CompletionRejected { feedback })?,
                            review,
                        );
                    }
                }
                let command = completion.command(review.completion.as_ref());
                return save(
                    Transition {
                        state: state.clone(),
                        commands: vec![command],
                    },
                    review,
                );
            }
            Phase::Ready => return intercept(inner(state, event)?, review),
            _ => return Err(DomainError::InvalidState),
        }
    }
    if !matches!(review.phase, Phase::Ready) {
        return Err(DomainError::InvalidState);
    }
    intercept(inner(state, event)?, review)
}
