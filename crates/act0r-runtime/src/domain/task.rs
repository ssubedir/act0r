//! Coding tasks own candidate identity, a common baseline, and aggregate admission policy.
use super::branch::{BranchOrigin, BranchSeed, Checkpoint, ForkRecord, PreparationStatus};
use super::{agent::*, *};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskBudget {
    pub max_attempts: u32,
    pub max_model_calls: u32,
    pub max_tokens: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskConfig {
    pub agent: AgentConfig,
    pub candidates: u32,
    pub budget: TaskBudget,
    /// Fixed tests and build configuration. Files outside writable_paths are also frozen.
    pub protected_paths: Vec<String>,
}
impl TaskConfig {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.agent.validate()?;
        if !(1..=16).contains(&self.candidates)
            || !(1..=10_000).contains(&self.budget.max_attempts)
            || self.budget.max_model_calls == 0
            || self.budget.max_tokens == 0
            || self.protected_paths.is_empty()
            || self
                .protected_paths
                .iter()
                .chain(&self.agent.writable_paths)
                .any(|p| !valid_path_rule(p))
            || self
                .protected_paths
                .iter()
                .any(|p| self.agent.writable_paths.iter().any(|w| overlaps(p, w)))
        {
            return Err(DomainError::InvalidConfiguration(
                "tasks require 1..16 candidates, positive budgets (at most 10,000 attempts), and protected relative paths disjoint from writable paths".into(),
            ));
        }
        Ok(())
    }
}
fn valid_path_rule(rule: &str) -> bool {
    let rule = rule.strip_suffix('/').unwrap_or(rule);
    !rule.is_empty()
        && !rule.contains(['\\', ':'])
        && rule.split('/').all(|c| {
            !c.is_empty()
                && c != "."
                && c != ".."
                && !c.ends_with(['.', ' '])
                && !matches!(
                    c.to_ascii_lowercase().as_str(),
                    ".git"
                        | ".act0r"
                        | ".act0r-tools"
                        | ".build"
                        | "target"
                        | "node_modules"
                        | ".aws"
                        | ".env"
                )
                && !c.to_ascii_lowercase().starts_with(".env.")
        })
}
pub fn matches_path(path: &str, rule: &str) -> bool {
    let path = path.to_ascii_lowercase();
    let rule = rule.to_ascii_lowercase();
    if rule.ends_with('/') {
        path.starts_with(&rule)
    } else {
        path == rule
    }
}
fn overlaps(a: &str, b: &str) -> bool {
    matches_path(a.trim_end_matches('/'), b)
        || matches_path(b.trim_end_matches('/'), a)
        || a.eq_ignore_ascii_case(b)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotFile {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshot {
    pub version: u32,
    pub task_id: TaskId,
    pub workspace: String,
    pub digest: String,
    pub files: Vec<SnapshotFile>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileDiff {
    pub path: String,
    pub before_sha256: Option<String>,
    pub after_sha256: Option<String>,
    pub patch: String,
    pub changed_bytes: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateEvaluation {
    pub passed: bool,
    pub tests: Value,
    pub diff: Vec<FileDiff>,
    pub changed_bytes: u64,
    pub snapshot_digest: String,
    pub candidate_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_tests_digest: Option<String>,
    pub error: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalTests {
    pub snapshot: WorkspaceSnapshot,
    pub test_command: Vec<String>,
}
impl FinalTests {
    pub fn validate(
        &self,
        config: &TaskConfig,
        baseline: &WorkspaceSnapshot,
    ) -> Result<(), DomainError> {
        if self.snapshot.version != 1
            || self.snapshot.files.is_empty()
            || self.test_command.first().is_none_or(|s| s.is_empty())
            || self.snapshot.files.iter().any(|f| {
                !valid_path_rule(&f.path)
                    || !config
                        .protected_paths
                        .iter()
                        .any(|p| matches_path(&f.path, p))
                    || config
                        .agent
                        .writable_paths
                        .iter()
                        .any(|p| matches_path(&f.path, p))
                    || baseline
                        .files
                        .iter()
                        .any(|b| b.path.eq_ignore_ascii_case(&f.path))
            })
        {
            return Err(DomainError::InvalidConfiguration("final tests require distinct protected files outside the candidate baseline and a fixed command".into()));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationRequest {
    pub snapshot: WorkspaceSnapshot,
    pub workspace: String,
    pub test_command: Vec<String>,
    pub writable_paths: Vec<String>,
    pub protected_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_tests: Option<FinalTests>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateInput {
    pub config: AgentConfig,
    pub snapshot: WorkspaceSnapshot,
    pub protected_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_tests: Option<FinalTests>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<BranchSeed>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<super::search::SearchRun>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CandidateState {
    pub(crate) agent: Value,
    pub(crate) input: CandidateInput,
    pub(crate) finish: Option<Value>,
}
impl CandidateState {
    fn evaluation_request(&self) -> EvaluationRequest {
        EvaluationRequest {
            snapshot: self.input.snapshot.clone(),
            workspace: self.input.config.workspace.clone(),
            test_command: self.input.final_tests.as_ref().map_or_else(
                || self.input.config.test_command.clone(),
                |t| t.test_command.clone(),
            ),
            writable_paths: self.input.config.writable_paths.clone(),
            protected_paths: self.input.protected_paths.clone(),
            final_tests: self.input.final_tests.clone(),
        }
    }
    pub(crate) fn evaluate(&self) -> Result<Command, DomainError> {
        Ok(Command::Schedule(ActivitySpec {
            handler: DefinitionId::new("coding.evaluate", 1),
            input: serde_json::to_value(self.evaluation_request())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?,
            retry: RetryPolicy::default(),
            recovery: if self.input.config.repeat_safe_tests {
                RecoveryPolicy::SafeToRepeat
            } else {
                RecoveryPolicy::RequiresReconciliation
            },
            usage: None,
        }))
    }
}
/// Pinned agent-v2 behavior plus a durable, independent fixed-test verification step.
pub struct CandidateWorkflow;
impl WorkflowDefinition for CandidateWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("coding.candidate", 1)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        fn decode<T: serde::de::DeserializeOwned>(v: &Value) -> Result<T, DomainError> {
            serde_json::from_value(v.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))
        }
        fn encode<T: Serialize>(v: &T) -> Result<Value, DomainError> {
            serde_json::to_value(v).map_err(|e| DomainError::InvalidWorkflow(e.to_string()))
        }
        let mut state: CandidateState = if let WorkflowEvent::Started(value) = event {
            let input: CandidateInput = decode(value)?;
            if input.branch.is_some() {
                return Err(DomainError::InvalidConfiguration(
                    "fork seeds require coding.candidate version 2".into(),
                ));
            }
            CandidateState {
                agent: Value::Null,
                input,
                finish: None,
            }
        } else {
            decode(state)?
        };
        if let WorkflowEvent::InputReceived { key, payload, .. } = event
            && key == "candidate_verification"
        {
            if state.finish.is_none() {
                return Err(DomainError::InvalidState);
            }
            let approved = payload["approved"].as_bool().ok_or_else(|| {
                DomainError::InvalidConfiguration(
                    "verification approval needs an approved boolean".into(),
                )
            })?;
            let command = if approved {
                state.evaluate()?
            } else {
                let mut finish = state.finish.take().ok_or(DomainError::InvalidState)?;
                finish["tests_passed"] = Value::Bool(false);
                finish["evaluation"] = encode(&CandidateEvaluation {
                    passed: false,
                    tests: Value::Null,
                    diff: vec![],
                    changed_bytes: 0,
                    snapshot_digest: state.input.snapshot.digest.clone(),
                    candidate_digest: String::new(),
                    final_tests_digest: state
                        .input
                        .final_tests
                        .as_ref()
                        .map(|t| t.snapshot.digest.clone()),
                    error: Some("final verification denied".into()),
                })?;
                Command::Complete(finish)
            };
            return Ok(Transition {
                state: encode(&state)?,
                commands: vec![command],
            });
        }
        if let WorkflowEvent::ActivityCompleted {
            handler, result, ..
        } = event
            && handler == &DefinitionId::new("coding.evaluate", 1)
        {
            let evaluation: CandidateEvaluation = decode(result)?;
            let mut finish = state.finish.take().ok_or(DomainError::InvalidState)?;
            finish["tests_passed"] = Value::Bool(evaluation.passed);
            finish["evaluation"] = encode(&evaluation)?;
            return Ok(Transition {
                state: encode(&state)?,
                commands: vec![Command::Complete(finish)],
            });
        }
        let started = WorkflowEvent::Started(encode(&state.input.config)?);
        let transition = AgentWorkflow.transition(
            &state.agent,
            if matches!(event, WorkflowEvent::Started(_)) {
                &started
            } else {
                event
            },
        )?;
        state.agent = transition.state;
        let mut commands = vec![];
        for command in transition.commands {
            if let Command::Complete(result) = command {
                state.finish = Some(result);
                commands.push(if state.input.config.require_approval {
                    Command::WaitForInput {
                        key: "candidate_verification".into(),
                        prompt: "Approve final fixed tests in a clean verification workspace?".into(),
                        payload: serde_json::json!({"workspace": state.input.config.workspace, "test_command":state.evaluation_request().test_command, "snapshot_digest":state.input.snapshot.digest}),
                    }
                } else { state.evaluate()? });
            } else {
                commands.push(command);
            }
        }
        Ok(Transition {
            state: encode(&state)?,
            commands,
        })
    }
}

/// Version 2 starts from a pinned branch seed; continuation remains agent-v2 behavior.
pub struct BranchCandidateWorkflow;
impl WorkflowDefinition for BranchCandidateWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("coding.candidate", 2)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        if let WorkflowEvent::Started(value) = event {
            let input: CandidateInput = serde_json::from_value(value.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            input.config.validate()?;
            let seed = input.branch.as_ref().ok_or(DomainError::InvalidState)?;
            let request: ModelRequest = serde_json::from_value(seed.next_model.input.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            if seed.agent.pending.is_some()
                || seed.next_model.handler != DefinitionId::new("agent.model", 1)
                || request.turn != seed.agent.turn
                || serde_json::to_value(&seed.agent.config).ok()
                    != serde_json::to_value(&input.config).ok()
            {
                return Err(DomainError::InvalidState);
            }
            let next = seed.next_model.clone();
            let state = CandidateState {
                agent: serde_json::to_value(&seed.agent)
                    .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?,
                finish: None,
                input,
            };
            Ok(Transition {
                commands: vec![
                    Command::ConfigureUsage(UsageBudget {
                        max_model_calls: state.input.config.max_model_calls,
                        max_tokens: state.input.config.max_tokens,
                    }),
                    Command::Schedule(next),
                ],
                state: serde_json::to_value(state)
                    .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?,
            })
        } else {
            CandidateWorkflow.transition(state, event)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Running,
    Waiting,
    Succeeded,
    NoSolution,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateRecord {
    pub ordinal: u32,
    pub run_id: RunId,
    pub workspace: String,
    pub status: RunStatus,
    pub evaluation: Option<CandidateEvaluation>,
    pub error: Option<Failure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<BranchOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<super::search::SearchRun>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: TaskId,
    pub version: u32,
    pub config: TaskConfig,
    pub snapshot: WorkspaceSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_tests: Option<FinalTests>,
    pub status: TaskStatus,
    pub candidates: Vec<CandidateRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forks: Vec<ForkRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_id: Option<SearchId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auxiliary_runs: Vec<RunId>,
    pub selected: Option<RunId>,
    pub attempts: u32,
    pub usage: UsageLedger,
    pub created_at: i64,
    pub updated_at: i64,
}
#[derive(Clone, Debug, Serialize)]
pub struct CodingTask {
    pub(crate) record: TaskRecord,
}
impl CodingTask {
    pub fn record(&self) -> &TaskRecord {
        &self.record
    }
    pub fn new(
        config: TaskConfig,
        snapshot: WorkspaceSnapshot,
        now: i64,
    ) -> Result<Self, DomainError> {
        config.validate()?;
        let run_ids = (0..config.candidates).map(|_| RunId::new()).collect();
        Self::with_identities(config, snapshot, run_ids, None, now)
    }
    pub fn with_identities(
        config: TaskConfig,
        snapshot: WorkspaceSnapshot,
        run_ids: Vec<RunId>,
        final_tests: Option<FinalTests>,
        now: i64,
    ) -> Result<Self, DomainError> {
        config.validate()?;
        if run_ids.len() != config.candidates as usize
            || run_ids
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != run_ids.len()
        {
            return Err(DomainError::InvalidConfiguration(
                "candidate identities must be distinct and match the configured count".into(),
            ));
        }
        if let Some(tests) = &final_tests {
            tests.validate(&config, &snapshot)?;
        }
        if snapshot.version != 1
            || snapshot.files.is_empty()
            || config
                .protected_paths
                .iter()
                .any(|p| !snapshot.files.iter().any(|f| matches_path(&f.path, p)))
        {
            return Err(DomainError::InvalidConfiguration(
                "every protected path must match a snapshot file".into(),
            ));
        }
        let candidates = run_ids
            .into_iter()
            .enumerate()
            .map(|(ordinal, run_id)| CandidateRecord {
                ordinal: ordinal as u32,
                run_id,
                workspace: String::new(),
                status: RunStatus::Queued,
                evaluation: None,
                error: None,
                origin: None,
                search: None,
            })
            .collect();
        let usage = UsageLedger {
            budget: Some(UsageBudget {
                max_model_calls: Some(config.budget.max_model_calls),
                max_tokens: Some(config.budget.max_tokens),
            }),
            ..Default::default()
        };
        Ok(Self {
            record: TaskRecord {
                id: snapshot.task_id,
                version: 1,
                config,
                snapshot,
                final_tests,
                status: TaskStatus::Running,
                candidates,
                forks: vec![],
                search_id: None,
                auxiliary_runs: vec![],
                selected: None,
                attempts: 0,
                usage,
                created_at: now,
                updated_at: now,
            },
        })
    }
    pub(crate) fn plan_fork(
        &mut self,
        id: ForkId,
        checkpoint: &Checkpoint,
        count: u32,
        now: i64,
    ) -> Result<ForkRecord, DomainError> {
        if let Some(existing) = self.record.forks.iter().find(|f| f.id == id) {
            return if existing.checkpoint_id == checkpoint.record().id
                && existing.run_ids.len() == count as usize
            {
                Ok(existing.clone())
            } else {
                Err(DomainError::ConflictingOutcome)
            };
        }
        let first_ordinal = self
            .record
            .forks
            .iter()
            .map(|f| f.first_ordinal + f.run_ids.len() as u32)
            .chain(self.record.candidates.iter().map(|c| c.ordinal + 1))
            .max()
            .unwrap_or(0);
        if !(1..=15).contains(&count)
            || first_ordinal + count > 16
            || checkpoint.record().task_id != self.record.id
            || checkpoint.record().status != PreparationStatus::Ready
            || !matches!(
                self.record.status,
                TaskStatus::Running | TaskStatus::Waiting
            )
        {
            return Err(DomainError::InvalidConfiguration("fork requires an open task, a ready checkpoint, and at most 16 planned candidates in total".into()));
        }
        let plan = ForkRecord {
            id,
            checkpoint_id: checkpoint.record().id,
            task_id: self.record.id,
            first_ordinal,
            run_ids: (0..count).map(|_| RunId::new()).collect(),
            status: PreparationStatus::Preparing,
            error: None,
            created_at: now,
            updated_at: now,
            search: None,
        };
        self.record.forks.push(plan.clone());
        self.record.status = TaskStatus::Running;
        self.record.updated_at = now;
        Ok(plan)
    }
    pub(crate) fn activate_fork(
        &mut self,
        id: ForkId,
        checkpoint: &Checkpoint,
        runs: &[Run],
        now: i64,
    ) -> Result<(), DomainError> {
        let plan = self
            .record
            .forks
            .iter_mut()
            .find(|f| f.id == id)
            .ok_or(DomainError::InvalidState)?;
        if plan.status != PreparationStatus::Preparing
            || plan.checkpoint_id != checkpoint.record().id
            || runs.iter().map(|r| r.record().id).collect::<Vec<_>>() != plan.run_ids
        {
            return Err(DomainError::InvalidState);
        }
        if let Some(search) = &plan.search
            && (search.actions.len() != runs.len()
                || Some(search.search_id) != self.record.search_id
                || !self.record.auxiliary_runs.contains(&search.proposal_run_id))
        {
            return Err(DomainError::InvalidState);
        }
        for (index, run) in runs.iter().enumerate() {
            let input: CandidateInput = serde_json::from_value(run.record().input.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            let seed = input.branch.as_ref().ok_or(DomainError::InvalidState)?;
            let mut expected_seed = checkpoint.record().seed.clone();
            expected_seed.agent.config = input.config.clone();
            let state: CandidateState = serde_json::from_value(run.record().state.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            let expected_workflow = if let Some(search) = &plan.search {
                super::search::candidate_workflow(search.version)?.id()
            } else {
                BranchCandidateWorkflow.id()
            };
            let expected_activity = if let Some(search) = &plan.search {
                if input.search
                    != Some(super::search::SearchRun {
                        version: search.version,
                        search_id: search.search_id,
                        initial_action: Some(search.actions[index].clone()),
                    })
                {
                    return Err(DomainError::InvalidState);
                }
                serde_json::json!({"action":search.actions[index], "request_id":null})
            } else {
                if input.search.is_some() {
                    return Err(DomainError::InvalidState);
                }
                expected_seed.next_model.input.clone()
            };
            if run.record().workflow != expected_workflow
                || serde_json::to_value(seed).ok() != serde_json::to_value(&expected_seed).ok()
                || state.agent
                    != serde_json::to_value(&expected_seed.agent)
                        .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?
                || state.finish.is_some()
                || serde_json::to_value(&state.input).ok().as_ref() != Some(&run.record().input)
                || run.record().allowance.admitted != expected_seed.origin.attempts
                || run.record().allowance.limit != self.record.config.budget.max_attempts
                || serde_json::to_value(&run.record().usage).ok()
                    != serde_json::to_value(&expected_seed.origin.usage).ok()
                || run.record().status != RunStatus::Queued
                || !run.attempts().is_empty()
                || !run.timers().is_empty()
                || run.activities().len() != 1
                || run.activities()[0].status != ActivityStatus::Queued
                || run.activities()[0].attempts != 0
                || (plan.search.is_none()
                    && serde_json::to_value(&run.activities()[0].spec).ok()
                        != serde_json::to_value(&expected_seed.next_model).ok())
                || run.activities()[0].spec.input != expected_activity
                || (plan.search.is_some()
                    && (run.activities()[0].spec.handler != DefinitionId::new("search.action", 1)
                        || run.activities()[0].spec.usage.is_some()))
            {
                return Err(DomainError::InvalidState);
            }
            self.record.candidates.push(CandidateRecord {
                ordinal: plan.first_ordinal + index as u32,
                run_id: run.record().id,
                workspace: input.config.workspace,
                status: run.record().status,
                evaluation: None,
                error: None,
                origin: Some(seed.origin.clone()),
                search: input.search,
            });
        }
        plan.status = PreparationStatus::Ready;
        plan.updated_at = now;
        self.record.updated_at = now;
        Ok(())
    }
    pub(crate) fn fail_fork(
        &mut self,
        id: ForkId,
        message: String,
        now: i64,
    ) -> Result<(), DomainError> {
        let plan = self
            .record
            .forks
            .iter_mut()
            .find(|f| f.id == id)
            .ok_or(DomainError::InvalidState)?;
        if plan.status != PreparationStatus::Preparing {
            return Err(DomainError::InvalidState);
        }
        plan.status = PreparationStatus::Failed;
        plan.error = Some(message);
        plan.updated_at = now;
        Ok(())
    }
    /// Called under the same unit of work that would publish an admitted attempt.
    pub fn permits_admission(&self, before: &Run, proposed: &Run) -> bool {
        let attempts = proposed
            .record()
            .allowance
            .admitted
            .saturating_sub(before.record().allowance.admitted);
        if attempts == 0 {
            return true;
        }
        let calls = proposed
            .record()
            .usage
            .admitted_model_calls
            .saturating_sub(before.record().usage.admitted_model_calls);
        let tokens = proposed
            .record()
            .usage
            .charged_tokens()
            .saturating_sub(before.record().usage.charged_tokens());
        self.record
            .attempts
            .checked_add(attempts)
            .is_some_and(|n| n <= self.record.config.budget.max_attempts)
            && self
                .record
                .usage
                .admitted_model_calls
                .checked_add(calls)
                .is_some_and(|n| n <= self.record.config.budget.max_model_calls)
            && (calls == 0
                || self
                    .record
                    .usage
                    .charged_tokens()
                    .checked_add(tokens)
                    .is_some_and(|n| n <= self.record.config.budget.max_tokens))
    }
    /// Rebuild accounting from persisted candidate ledgers; never refund uncertain calls.
    pub(crate) fn refresh(&mut self, runs: &[Run], now: i64) -> Result<(), DomainError> {
        if runs.len() != self.record.candidates.len() + self.record.auxiliary_runs.len() {
            return Err(DomainError::InvalidState);
        }
        let mut usage = UsageLedger {
            budget: self.record.usage.budget.clone(),
            ..Default::default()
        };
        let mut attempts = 0u32;
        for candidate in &mut self.record.candidates {
            let run = runs
                .iter()
                .find(|r| r.record().id == candidate.run_id)
                .ok_or(DomainError::InvalidState)?;
            if run.record().workflow
                != if let Some(search) = &candidate.search {
                    super::search::candidate_workflow(search.version)?.id()
                } else if candidate.origin.is_some() {
                    BranchCandidateWorkflow.id()
                } else {
                    CandidateWorkflow.id()
                }
            {
                return Err(DomainError::InvalidState);
            }
            let input: CandidateInput = serde_json::from_value(run.record().input.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            let mut expected_config = self.record.config.agent.clone();
            expected_config.workspace = input.config.workspace.clone();
            if input.snapshot != self.record.snapshot
                || input.search != candidate.search
                || input.final_tests != self.record.final_tests
                || serde_json::to_value(input.branch.as_ref().map(|b| &b.origin)).ok()
                    != serde_json::to_value(&candidate.origin).ok()
                || input.protected_paths != self.record.config.protected_paths
                || serde_json::to_value(&input.config)
                    .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?
                    != serde_json::to_value(expected_config)
                        .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?
            {
                return Err(DomainError::InvalidWorkflow(
                    "candidate does not match its task's pinned configuration and baseline".into(),
                ));
            }
            candidate.workspace = input.config.workspace;
            candidate.status = run.record().status;
            candidate.error = run.record().error.clone();
            candidate.evaluation = run
                .record()
                .result
                .as_ref()
                .and_then(|v| v.get("evaluation"))
                .map(|v| {
                    serde_json::from_value(v.clone())
                        .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))
                })
                .transpose()?;
            let (local_attempts, local_usage) = if let Some(origin) = &candidate.origin {
                (
                    run.record()
                        .allowance
                        .admitted
                        .checked_sub(origin.attempts)
                        .ok_or(DomainError::InvalidState)?,
                    run.record()
                        .usage
                        .since(&origin.usage)
                        .ok_or(DomainError::InvalidState)?,
                )
            } else {
                (run.record().allowance.admitted, run.record().usage.clone())
            };
            let u = &local_usage;
            attempts = attempts
                .checked_add(local_attempts)
                .ok_or(DomainError::InvalidState)?;
            usage.admitted_model_calls = usage
                .admitted_model_calls
                .checked_add(u.admitted_model_calls)
                .ok_or(DomainError::InvalidState)?;
            usage.actual_input_tokens = usage
                .actual_input_tokens
                .saturating_add(u.actual_input_tokens);
            usage.actual_output_tokens = usage
                .actual_output_tokens
                .saturating_add(u.actual_output_tokens);
            usage.held_tokens = usage.held_tokens.saturating_add(u.held_tokens);
            usage.uncertain_calls = usage.uncertain_calls.saturating_add(u.uncertain_calls);
        }
        for id in &self.record.auxiliary_runs {
            let run = runs
                .iter()
                .find(|r| r.record().id == *id)
                .ok_or(DomainError::InvalidState)?;
            let input: super::search::ProposalInput =
                serde_json::from_value(run.record().input.clone())
                    .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            if run.record().workflow != super::search::ProposalWorkflow.id()
                || input.task_id != self.record.id
                || Some(input.search_id) != self.record.search_id
            {
                return Err(DomainError::InvalidState);
            }
            attempts = attempts
                .checked_add(run.record().allowance.admitted)
                .ok_or(DomainError::InvalidState)?;
            let u = &run.record().usage;
            usage.admitted_model_calls = usage
                .admitted_model_calls
                .checked_add(u.admitted_model_calls)
                .ok_or(DomainError::InvalidState)?;
            usage.actual_input_tokens = usage
                .actual_input_tokens
                .saturating_add(u.actual_input_tokens);
            usage.actual_output_tokens = usage
                .actual_output_tokens
                .saturating_add(u.actual_output_tokens);
            usage.held_tokens = usage.held_tokens.saturating_add(u.held_tokens);
            usage.uncertain_calls = usage.uncertain_calls.saturating_add(u.uncertain_calls);
        }
        self.record.attempts = attempts;
        self.record.usage = usage;
        self.record.updated_at = now;
        if self
            .record
            .forks
            .iter()
            .any(|f| f.status == PreparationStatus::Preparing)
            || self.record.candidates.iter().any(|c| c.status.is_active())
            || runs.iter().any(|r| {
                self.record.auxiliary_runs.contains(&r.record().id) && r.record().status.is_active()
            })
        {
            self.record.status = TaskStatus::Running;
        } else if runs.iter().any(|r| {
            self.record.auxiliary_runs.contains(&r.record().id)
                && matches!(
                    r.record().status,
                    RunStatus::Waiting | RunStatus::Paused | RunStatus::Blocked
                )
        }) || self.record.candidates.iter().any(|c| {
            matches!(
                c.status,
                RunStatus::Waiting | RunStatus::Paused | RunStatus::Blocked
            )
        }) {
            self.record.status = TaskStatus::Waiting;
        } else {
            self.record.selected = self
                .record
                .candidates
                .iter()
                .filter(|c| {
                    c.status == RunStatus::Succeeded
                        && c.evaluation.as_ref().is_some_and(|e| {
                            e.passed
                                && e.snapshot_digest == self.record.snapshot.digest
                                && e.final_tests_digest
                                    == self
                                        .record
                                        .final_tests
                                        .as_ref()
                                        .map(|t| t.snapshot.digest.clone())
                        })
                })
                .min_by_key(|c| {
                    (
                        c.evaluation.as_ref().map_or(u64::MAX, |e| e.changed_bytes),
                        c.ordinal,
                    )
                })
                .map(|c| c.run_id);
            self.record.status = if self.record.selected.is_some() {
                TaskStatus::Succeeded
            } else {
                TaskStatus::NoSolution
            };
        }
        Ok(())
    }
}
