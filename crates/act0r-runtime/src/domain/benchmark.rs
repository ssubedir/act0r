//! Frozen benchmark inputs, trial progression, and comparison statistics.
use super::{agent::*, search::*, task::*, *};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkCase {
    pub name: String,
    pub goal: String,
    pub source: String,
    pub final_tests_source: String,
    #[serde(default)]
    pub script: Option<Vec<AgentAction>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_script: Option<Vec<AgentAction>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scripted_proposals: Vec<AgentAction>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkSearch {
    #[serde(default = "default_search_version")]
    pub version: u32,
    pub width: u32,
    pub warmup_steps: u32,
    pub rollout_steps: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkConfig {
    pub version: u32,
    pub name: String,
    pub repetitions: u32,
    pub agent: AgentConfig,
    pub budget: TaskBudget,
    pub protected_paths: Vec<String>,
    pub final_test_command: Vec<String>,
    pub cases: Vec<BenchmarkCase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<BenchmarkSearch>,
}
impl BenchmarkConfig {
    pub fn validate(&self) -> Result<(), DomainError> {
        if !matches!((self.version, &self.search), (1, None) | (2, Some(_)))
            || self.name.trim().is_empty()
            || self.name.len() > 128
            || !(1..=20).contains(&self.repetitions)
            || self.cases.is_empty()
            || self.cases.len() > 50
            || self.cases.len() * self.repetitions as usize * self.arms().len() > 200
            || self.final_test_command.first().is_none_or(|s| s.is_empty())
            || self.cases.iter().any(|c| {
                c.name.trim().is_empty()
                    || c.name.len() > 128
                    || c.goal.trim().is_empty()
                    || c.source.is_empty()
                    || c.final_tests_source.is_empty()
            })
            || self
                .cases
                .iter()
                .map(|c| &c.name)
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.cases.len()
        {
            return Err(DomainError::InvalidConfiguration("benchmark requires v1 without search or v2 with search, a name, distinct cases, final tests, 1..20 repetitions, and at most 200 trials".into()));
        }
        for index in 0..self.cases.len() {
            self.task_config(index, 1)?.validate()?;
            if self.search.is_some() {
                self.search_config(index)?.validate()?;
            } else if self.cases[index].search_script.is_some()
                || !self.cases[index].scripted_proposals.is_empty()
            {
                return Err(DomainError::InvalidConfiguration(
                    "search fixtures require benchmark v2".into(),
                ));
            }
        }
        Ok(())
    }
    pub fn arms(&self) -> Vec<(BenchmarkStrategy, u32)> {
        let mut arms = vec![
            (BenchmarkStrategy::Independent, 1),
            (BenchmarkStrategy::Independent, 3),
        ];
        if self.search.is_some() {
            arms.push((BenchmarkStrategy::BoundedSearch, 1));
        }
        arms
    }
    pub fn search_config(&self, index: usize) -> Result<SearchConfig, DomainError> {
        let settings = self.search.as_ref().ok_or(DomainError::InvalidState)?;
        let case = self.cases.get(index).ok_or(DomainError::InvalidState)?;
        let mut task = self.task_config(index, 1)?;
        if task.agent.target.provider == "scripted"
            && let Some(script) = &case.search_script
        {
            task.agent.script = script.clone();
        }
        Ok(SearchConfig {
            version: settings.version,
            strategy: DefinitionId::new("feedback_cost", 1),
            task,
            width: settings.width,
            warmup_steps: settings.warmup_steps,
            rollout_steps: settings.rollout_steps,
            scripted_proposals: if self.agent.target.provider == "scripted" {
                case.scripted_proposals.clone()
            } else {
                vec![]
            },
            final_tests: None,
        })
    }
    /// All arms receive the same total caps; an individual candidate may use the full cap.
    pub fn task_config(&self, case: usize, candidates: u32) -> Result<TaskConfig, DomainError> {
        let case = self.cases.get(case).ok_or(DomainError::InvalidState)?;
        let mut agent = self.agent.clone();
        agent.goal = case.goal.clone();
        agent.max_model_calls = Some(self.budget.max_model_calls);
        agent.max_tokens = Some(self.budget.max_tokens);
        if let Some(script) = &case.script {
            agent.script = script.clone();
        }
        Ok(TaskConfig {
            agent,
            candidates,
            budget: self.budget.clone(),
            protected_paths: self.protected_paths.clone(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrozenCase {
    pub baseline: WorkspaceSnapshot,
    pub final_tests: WorkspaceSnapshot,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkStatus {
    Running,
    Waiting,
    Completed,
}
impl BenchmarkStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialStatus {
    Planned,
    Preparing,
    Running,
    Waiting,
    Completed,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrialFailure {
    pub run_id: RunId,
    pub failure: Failure,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrialMetrics {
    pub success: bool,
    pub attempts: u32,
    pub usage: UsageLedger,
    /// UTC wall time includes preparation and downtime, clamped at zero on clock reversal.
    pub elapsed_ms: u64,
    pub selected: Option<CandidateRecord>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<SearchRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<TrialFailure>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkStrategy {
    #[default]
    Independent,
    BoundedSearch,
}
impl BenchmarkStrategy {
    pub fn is_independent(&self) -> bool {
        *self == Self::Independent
    }
    pub fn label(self, candidates: u32) -> String {
        match self {
            Self::Independent => format!(
                "{candidates} independent candidate{}",
                if candidates == 1 { "" } else { "s" }
            ),
            Self::BoundedSearch => "bounded search".into(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrialRecord {
    pub ordinal: u32,
    pub case_index: u32,
    pub repetition: u32,
    pub candidates: u32,
    #[serde(default, skip_serializing_if = "BenchmarkStrategy::is_independent")]
    pub strategy: BenchmarkStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_id: Option<SearchId>,
    pub task_id: TaskId,
    pub run_ids: Vec<RunId>,
    pub status: TrialStatus,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub metrics: Option<TrialMetrics>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ArmSummary {
    pub candidates: u32,
    #[serde(default, skip_serializing_if = "BenchmarkStrategy::is_independent")]
    pub strategy: BenchmarkStrategy,
    pub trials: u32,
    pub completed: u32,
    pub successes: u32,
    pub success_rate: Option<f64>,
    pub model_calls: u64,
    pub charged_tokens: u64,
    pub held_tokens: u64,
    pub attempts: u64,
    pub elapsed_ms: u64,
    pub mean_elapsed_ms: Option<f64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaseSummary {
    pub name: String,
    pub baseline_digest: String,
    pub final_tests_digest: String,
    pub arms: Vec<ArmSummary>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    pub arms: Vec<ArmSummary>,
    pub cases: Vec<CaseSummary>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkRecord {
    pub id: BenchmarkId,
    pub version: u32,
    pub config: BenchmarkConfig,
    pub inputs: Vec<FrozenCase>,
    pub trials: Vec<TrialRecord>,
    pub status: BenchmarkStatus,
    pub summary: BenchmarkSummary,
    pub created_at: i64,
    pub updated_at: i64,
}
#[derive(Clone, Debug, Serialize)]
pub struct Benchmark {
    pub(crate) record: BenchmarkRecord,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchmarkStep {
    Begin(TaskId),
    Prepare(TaskId),
}

impl Benchmark {
    pub fn record(&self) -> &BenchmarkRecord {
        &self.record
    }
    pub fn new(
        config: BenchmarkConfig,
        inputs: Vec<FrozenCase>,
        now: i64,
    ) -> Result<Self, DomainError> {
        config.validate()?;
        if inputs.len() != config.cases.len() {
            return Err(DomainError::InvalidConfiguration(
                "benchmark inputs must match its cases".into(),
            ));
        }
        for (index, frozen) in inputs.iter().enumerate() {
            let cfg = config.task_config(index, 1)?;
            CodingTask::new(cfg.clone(), frozen.baseline.clone(), now)?;
            FinalTests {
                snapshot: frozen.final_tests.clone(),
                test_command: config.final_test_command.clone(),
            }
            .validate(&cfg, &frozen.baseline)?;
        }
        let mut trials = Vec::new();
        for case_index in 0..inputs.len() {
            for repetition in 0..config.repetitions {
                // Rotate arm order across repetitions to reduce fixed-order bias.
                let mut arms = config.arms();
                let rotation = repetition as usize % arms.len();
                arms.rotate_left(rotation);
                for (strategy, candidates) in arms {
                    trials.push(TrialRecord {
                        ordinal: trials.len() as u32,
                        case_index: case_index as u32,
                        repetition,
                        candidates,
                        strategy,
                        search_id: (strategy == BenchmarkStrategy::BoundedSearch)
                            .then(SearchId::new),
                        task_id: TaskId::new(),
                        run_ids: (0..candidates).map(|_| RunId::new()).collect(),
                        status: TrialStatus::Planned,
                        started_at: None,
                        completed_at: None,
                        metrics: None,
                    });
                }
            }
        }
        let mut job = Self {
            record: BenchmarkRecord {
                id: BenchmarkId::new(),
                version: config.version,
                config,
                inputs,
                trials,
                status: BenchmarkStatus::Running,
                summary: BenchmarkSummary {
                    arms: vec![],
                    cases: vec![],
                },
                created_at: now,
                updated_at: now,
            },
        };
        job.refresh_summary();
        Ok(job)
    }
    pub fn next_step(&self) -> Option<BenchmarkStep> {
        if let Some(trial) = self
            .record
            .trials
            .iter()
            .find(|t| t.status != TrialStatus::Completed)
        {
            return match trial.status {
                TrialStatus::Planned => Some(BenchmarkStep::Begin(trial.task_id)),
                TrialStatus::Preparing => Some(BenchmarkStep::Prepare(trial.task_id)),
                _ => None,
            };
        }
        None
    }
    pub fn trial(&self, id: TaskId) -> Result<&TrialRecord, DomainError> {
        self.record
            .trials
            .iter()
            .find(|t| t.task_id == id)
            .ok_or(DomainError::InvalidState)
    }
    pub fn begin(&mut self, id: TaskId, now: i64) -> Result<(), DomainError> {
        if self.next_step() != Some(BenchmarkStep::Begin(id)) {
            return Err(DomainError::InvalidState);
        }
        let trial = self
            .record
            .trials
            .iter_mut()
            .find(|t| t.task_id == id)
            .ok_or(DomainError::InvalidState)?;
        trial.status = TrialStatus::Preparing;
        trial.started_at = Some(now);
        self.record.updated_at = now;
        Ok(())
    }
    pub(crate) fn activate(
        &mut self,
        task: &TaskRecord,
        search: Option<&SearchRecord>,
    ) -> Result<(), DomainError> {
        if self.next_step() != Some(BenchmarkStep::Prepare(task.id)) {
            return Err(DomainError::InvalidState);
        }
        self.validate_task(task, search)?;
        let trial = self
            .record
            .trials
            .iter_mut()
            .find(|t| t.task_id == task.id)
            .ok_or(DomainError::InvalidState)?;
        trial.status = TrialStatus::Running;
        self.record.updated_at = task.created_at;
        Ok(())
    }
    fn validate_task(
        &self,
        task: &TaskRecord,
        search: Option<&SearchRecord>,
    ) -> Result<(), DomainError> {
        let trial = self.trial(task.id)?;
        let expected = if trial.strategy == BenchmarkStrategy::BoundedSearch {
            let config = self
                .record
                .config
                .search_config(trial.case_index as usize)?;
            let search = search.ok_or(DomainError::InvalidState)?;
            if Some(search.id) != trial.search_id
                || search.task_id != task.id
                || search.root != trial.run_ids[0]
                || serde_json::to_value(&search.config).unwrap()
                    != serde_json::to_value(&config).unwrap()
                || task.candidates.first().map(|c| c.run_id) != Some(search.root)
                || task
                    .candidates
                    .iter()
                    .any(|c| c.search.as_ref().map(|s| s.search_id) != Some(search.id))
                || task
                    .auxiliary_runs
                    .iter()
                    .any(|id| *id != search.proposal_run_id)
            {
                return Err(DomainError::InvalidWorkflow(
                    "search trial differs from its pinned plan".into(),
                ));
            }
            config.task
        } else {
            if search.is_some()
                || task.candidates.iter().map(|c| c.run_id).collect::<Vec<_>>() != trial.run_ids
            {
                return Err(DomainError::InvalidWorkflow(
                    "independent trial identities changed".into(),
                ));
            }
            self.record
                .config
                .task_config(trial.case_index as usize, trial.candidates)?
        };
        let inputs = &self.record.inputs[trial.case_index as usize];
        let final_tests = FinalTests {
            snapshot: inputs.final_tests.clone(),
            test_command: self.record.config.final_test_command.clone(),
        };
        if task.search_id != trial.search_id
            || serde_json::to_value(&task.config)
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?
                != serde_json::to_value(expected)
                    .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?
            || task.snapshot.digest != inputs.baseline.digest
            || task.snapshot.files != inputs.baseline.files
            || task.final_tests.as_ref() != Some(&final_tests)
        {
            return Err(DomainError::InvalidWorkflow(
                "trial task differs from its pinned case, model, budget, or identities".into(),
            ));
        }
        Ok(())
    }
    /// The storage adapter calls this in the task mutation's unit of work.
    pub(crate) fn observe(
        &mut self,
        task: &TaskRecord,
        search: Option<&SearchRecord>,
    ) -> Result<bool, DomainError> {
        self.validate_task(task, search)?;
        let trial = self
            .record
            .trials
            .iter_mut()
            .find(|t| t.task_id == task.id)
            .ok_or(DomainError::InvalidState)?;
        if trial.status == TrialStatus::Completed {
            return Ok(false);
        }
        if !matches!(trial.status, TrialStatus::Running | TrialStatus::Waiting) {
            return Err(DomainError::InvalidState);
        }
        let next = if let Some(search) = search {
            if matches!(search.phase, SearchPhase::Completed | SearchPhase::Failed) {
                TrialStatus::Completed
            } else if task.status == TaskStatus::Waiting {
                TrialStatus::Waiting
            } else {
                TrialStatus::Running
            }
        } else {
            match task.status {
                TaskStatus::Running => TrialStatus::Running,
                TaskStatus::Waiting => TrialStatus::Waiting,
                _ => TrialStatus::Completed,
            }
        };
        if trial.status == next {
            return Ok(false);
        }
        trial.status = next;
        let now = task
            .updated_at
            .max(search.map_or(task.updated_at, |s| s.updated_at));
        if next == TrialStatus::Completed {
            trial.completed_at = Some(now);
            let selected = task
                .selected
                .and_then(|id| task.candidates.iter().find(|c| c.run_id == id))
                .cloned();
            trial.metrics = Some(TrialMetrics {
                success: task.status == TaskStatus::Succeeded
                    && selected.is_some()
                    && search.is_none_or(|s| s.verified),
                attempts: task.attempts,
                usage: task.usage.clone(),
                elapsed_ms: now
                    .saturating_sub(trial.started_at.ok_or(DomainError::InvalidState)?)
                    .max(0) as u64,
                selected,
                error: search.and_then(|s| s.error.clone()).or_else(|| {
                    (task.status == TaskStatus::NoSolution)
                        .then(|| "no candidate passed final verification".into())
                }),
                search: search.cloned(),
                failures: task
                    .candidates
                    .iter()
                    .filter_map(|c| {
                        c.error
                            .clone()
                            .filter(|failure| failure.code != "cancelled")
                            .map(|failure| TrialFailure {
                                run_id: c.run_id,
                                failure,
                            })
                    })
                    .collect(),
            });
        }
        self.record.updated_at = now;
        self.refresh_summary();
        Ok(true)
    }
    pub fn fail_preparation(
        &mut self,
        id: TaskId,
        message: String,
        now: i64,
    ) -> Result<(), DomainError> {
        if self.next_step() != Some(BenchmarkStep::Prepare(id)) {
            return Err(DomainError::InvalidState);
        }
        let trial = self
            .record
            .trials
            .iter_mut()
            .find(|t| t.task_id == id)
            .ok_or(DomainError::InvalidState)?;
        trial.status = TrialStatus::Completed;
        trial.completed_at = Some(now);
        trial.metrics = Some(TrialMetrics {
            success: false,
            attempts: 0,
            usage: UsageLedger::default(),
            elapsed_ms: now
                .saturating_sub(trial.started_at.ok_or(DomainError::InvalidState)?)
                .max(0) as u64,
            selected: None,
            error: Some(message),
            search: None,
            failures: vec![],
        });
        self.record.updated_at = now;
        self.refresh_summary();
        Ok(())
    }
    fn refresh_summary(&mut self) {
        fn arms(
            trials: &[&TrialRecord],
            configured: &[(BenchmarkStrategy, u32)],
        ) -> Vec<ArmSummary> {
            configured
                .iter()
                .copied()
                .map(|(strategy, candidates)| {
                    let mut arm = ArmSummary {
                        candidates,
                        strategy,
                        ..Default::default()
                    };
                    for trial in trials
                        .iter()
                        .filter(|t| t.candidates == candidates && t.strategy == strategy)
                    {
                        arm.trials += 1;
                        if let Some(metrics) = &trial.metrics {
                            arm.completed += 1;
                            arm.successes += u32::from(metrics.success);
                            arm.model_calls = arm
                                .model_calls
                                .saturating_add(u64::from(metrics.usage.admitted_model_calls));
                            arm.charged_tokens = arm
                                .charged_tokens
                                .saturating_add(metrics.usage.charged_tokens());
                            arm.held_tokens =
                                arm.held_tokens.saturating_add(metrics.usage.held_tokens);
                            arm.attempts = arm.attempts.saturating_add(u64::from(metrics.attempts));
                            arm.elapsed_ms = arm.elapsed_ms.saturating_add(metrics.elapsed_ms);
                        }
                    }
                    if arm.completed > 0 {
                        arm.success_rate =
                            Some(f64::from(arm.successes) / f64::from(arm.completed));
                        arm.mean_elapsed_ms =
                            Some(arm.elapsed_ms as f64 / f64::from(arm.completed));
                    }
                    arm
                })
                .collect()
        }
        self.record.summary = BenchmarkSummary {
            arms: arms(
                &self.record.trials.iter().collect::<Vec<_>>(),
                &self.record.config.arms(),
            ),
            cases: self
                .record
                .inputs
                .iter()
                .enumerate()
                .map(|(index, input)| CaseSummary {
                    name: self.record.config.cases[index].name.clone(),
                    baseline_digest: input.baseline.digest.clone(),
                    final_tests_digest: input.final_tests.digest.clone(),
                    arms: arms(
                        &self
                            .record
                            .trials
                            .iter()
                            .filter(|t| t.case_index as usize == index)
                            .collect::<Vec<_>>(),
                        &self.record.config.arms(),
                    ),
                })
                .collect(),
        };
        self.record.status = if self
            .record
            .trials
            .iter()
            .all(|t| t.status == TrialStatus::Completed)
        {
            BenchmarkStatus::Completed
        } else if self
            .record
            .trials
            .iter()
            .any(|t| t.status == TrialStatus::Waiting)
        {
            BenchmarkStatus::Waiting
        } else {
            BenchmarkStatus::Running
        };
    }
}
