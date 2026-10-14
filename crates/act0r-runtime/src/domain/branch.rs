//! Checkpoints pin a quiescent candidate and its continuation; tasks own fork membership.
use super::{agent::*, task::*, *};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchOrigin {
    pub checkpoint_id: CheckpointId,
    pub parent_run_id: RunId,
    pub parent_revision: u64,
    pub parent_history_sequence: u64,
    pub attempts: u32,
    pub usage: UsageLedger,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchSeed {
    pub origin: BranchOrigin,
    pub agent: AgentState,
    pub next_model: ActivitySpec,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparationStatus {
    Preparing,
    Ready,
    Failed,
}
impl PreparationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub id: CheckpointId,
    pub version: u32,
    pub task_id: TaskId,
    pub artifact_id: TaskId,
    pub status: PreparationStatus,
    pub input: CandidateInput,
    pub seed: BranchSeed,
    pub snapshot: Option<WorkspaceSnapshot>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}
#[derive(Clone, Debug, Serialize)]
pub struct Checkpoint {
    pub(crate) record: CheckpointRecord,
}
impl Checkpoint {
    pub fn record(&self) -> &CheckpointRecord {
        &self.record
    }
    pub(crate) fn new(
        id: CheckpointId,
        task: &CodingTask,
        run: &Run,
        now: i64,
    ) -> Result<Self, DomainError> {
        if !task
            .record()
            .candidates
            .iter()
            .any(|c| c.run_id == run.record().id)
            || !matches!(run.record().workflow, ref w if *w == CandidateWorkflow.id() || *w == BranchCandidateWorkflow.id() || *w == super::search::SearchCandidateWorkflow.id() || *w == super::search::FeedbackSearchCandidateWorkflow.id())
            || run.record().status != RunStatus::Paused
            || !(run.record().controls.pause_requested || run.record().controls.boundary_pause)
            || run.record().controls.cancel_requested
            || run.record().wait.is_some()
            || run.activities().iter().any(|a| {
                matches!(
                    a.status,
                    ActivityStatus::Running | ActivityStatus::Indeterminate
                )
            })
            || run
                .timers()
                .iter()
                .any(|t| t.status == TimerStatus::Scheduled)
        {
            return Err(DomainError::InvalidConfiguration(
                "checkpoint requires a paused coding candidate with no active or uncertain work"
                    .into(),
            ));
        }
        let state: CandidateState = serde_json::from_value(run.record().state.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        let agent: AgentState = serde_json::from_value(state.agent)
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        let queued: Vec<_> = run
            .activities()
            .iter()
            .filter(|a| a.status == ActivityStatus::Queued)
            .collect();
        if state.finish.is_some()
            || agent.pending.is_some()
            || queued.len() != 1
            || queued[0].spec.handler != DefinitionId::new("agent.model", 1)
            || queued[0].attempts != 0
            || queued[0].active_attempt.is_some()
            || serde_json::to_value(&state.input).ok().as_ref() != Some(&run.record().input)
            || serde_json::to_value(&agent.config).ok()
                != serde_json::to_value(&state.input.config).ok()
        {
            return Err(DomainError::InvalidConfiguration("checkpoint requires a saved, unattempted next model request after a completed step".into()));
        }
        let request: ModelRequest = serde_json::from_value(queued[0].spec.input.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        if request.turn != agent.turn {
            return Err(DomainError::InvalidState);
        }
        Ok(Self {
            record: CheckpointRecord {
                id,
                version: 1,
                task_id: task.record().id,
                artifact_id: TaskId::new(),
                status: PreparationStatus::Preparing,
                input: state.input,
                seed: BranchSeed {
                    origin: BranchOrigin {
                        checkpoint_id: id,
                        parent_run_id: run.record().id,
                        parent_revision: run.record().revision,
                        parent_history_sequence: run.record().history_sequence,
                        attempts: run.record().allowance.admitted,
                        usage: run.record().usage.clone(),
                    },
                    agent,
                    next_model: queued[0].spec.clone(),
                },
                snapshot: None,
                error: None,
                created_at: now,
                updated_at: now,
            },
        })
    }
    pub(crate) fn publish(
        &mut self,
        snapshot: WorkspaceSnapshot,
        parent: &Run,
        now: i64,
    ) -> Result<(), DomainError> {
        if self.record.status == PreparationStatus::Ready {
            return if self.record.snapshot.as_ref() == Some(&snapshot) {
                Ok(())
            } else {
                Err(DomainError::ConflictingOutcome)
            };
        }
        let origin = &self.record.seed.origin;
        if self.record.status != PreparationStatus::Preparing
            || snapshot.task_id != self.record.artifact_id
            || snapshot.version != 1
            || snapshot.files.is_empty()
            || parent.record().id != origin.parent_run_id
            || parent.record().status != RunStatus::Paused
            || parent.record().revision != origin.parent_revision
            || parent.record().history_sequence != origin.parent_history_sequence
        {
            return Err(DomainError::InvalidConfiguration(
                "checkpoint parent changed during capture or snapshot identity is invalid".into(),
            ));
        }
        self.record.snapshot = Some(snapshot);
        self.record.status = PreparationStatus::Ready;
        self.record.updated_at = now;
        Ok(())
    }
    pub(crate) fn fail(&mut self, error: String, now: i64) -> Result<(), DomainError> {
        if self.record.status != PreparationStatus::Preparing {
            return Err(DomainError::InvalidState);
        }
        self.record.status = PreparationStatus::Failed;
        self.record.error = Some(error);
        self.record.updated_at = now;
        Ok(())
    }
    pub(crate) fn fork_run(
        &self,
        id: RunId,
        workspace: String,
        limit: u32,
        now: i64,
    ) -> Result<Run, DomainError> {
        if self.record.status != PreparationStatus::Ready {
            return Err(DomainError::InvalidState);
        }
        let mut input = self.record.input.clone();
        input.config.workspace = workspace;
        let mut seed = self.record.seed.clone();
        seed.agent.config = input.config.clone();
        input.branch = Some(seed.clone());
        Run::start_forked(
            id,
            serde_json::to_value(input).map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?,
            limit,
            &seed.origin,
            now,
        )
    }
    pub(crate) fn fork_search_run(
        &self,
        id: RunId,
        workspace: String,
        limit: u32,
        marker: super::search::SearchRun,
        now: i64,
    ) -> Result<Run, DomainError> {
        if self.record.status != PreparationStatus::Ready {
            return Err(DomainError::InvalidState);
        }
        let mut input = self.record.input.clone();
        input.config.workspace = workspace;
        let mut seed = self.record.seed.clone();
        seed.agent.config = input.config.clone();
        input.branch = Some(seed.clone());
        let workflow = super::search::candidate_workflow(marker.version)?;
        input.search = Some(marker);
        Run::start_forked_with(
            id,
            workflow,
            serde_json::to_value(input).unwrap(),
            limit,
            &seed.origin,
            now,
        )
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkRecord {
    pub id: ForkId,
    pub checkpoint_id: CheckpointId,
    pub task_id: TaskId,
    pub first_ordinal: u32,
    pub run_ids: Vec<RunId>,
    pub status: PreparationStatus,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<super::search::SearchFork>,
}
