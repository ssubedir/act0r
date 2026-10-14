use super::{BranchService, Engine, Error, ports::*};
use crate::domain::{branch::PreparationStatus, search::*, task::*, *};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
pub struct SearchService {
    engine: Engine,
    store: Arc<dyn SearchStore>,
    tasks: Arc<dyn TaskStore>,
    branches: Arc<BranchService>,
    workspaces: Arc<dyn CandidateWorkspaces>,
    strategy: Arc<dyn SearchStrategy>,
}
enum Action {
    Resume(RunId),
    Checkpoint,
    Propose,
    Expand,
    Score(RunId),
    Feedback(RunId),
    Select(Option<RunId>),
    Finish,
    Fail(String),
}
impl SearchService {
    pub fn new(
        engine: Engine,
        store: Arc<dyn SearchStore>,
        tasks: Arc<dyn TaskStore>,
        branches: Arc<BranchService>,
        workspaces: Arc<dyn CandidateWorkspaces>,
        strategy: Arc<dyn SearchStrategy>,
    ) -> Self {
        Self {
            engine,
            store,
            tasks,
            branches,
            workspaces,
            strategy,
        }
    }
    pub async fn start(
        &self,
        source: PathBuf,
        config: SearchConfig,
        base: &Path,
    ) -> Result<SearchId, Error> {
        config.validate()?;
        if self.strategy.id() != config.strategy {
            return Err(Error::Unavailable(
                "configured search strategy version is unavailable".into(),
            ));
        }
        let task_id = TaskId::new();
        let (snapshot, final_tests) = if let Some(tests) = &config.final_tests {
            let (snapshot, final_snapshot) = self
                .workspaces
                .freeze_case(source, base.join(&tests.source), task_id, TaskId::new())
                .await?;
            (
                snapshot,
                Some(FinalTests {
                    snapshot: final_snapshot,
                    test_command: tests.test_command.clone(),
                }),
            )
        } else {
            (self.workspaces.snapshot(source, task_id).await?, None)
        };
        let now = self.engine.clock.now_ms();
        let root_id = RunId::new();
        let id = SearchId::new();
        let (search, task, root) = self
            .prepare(id, config, snapshot, root_id, final_tests, now)
            .await?;
        self.store.create_search(search, task, root).await?;
        Ok(id)
    }
    /// Prepare a root from frozen inputs without publishing any durable state.
    pub(crate) async fn prepare(
        &self,
        id: SearchId,
        config: SearchConfig,
        snapshot: WorkspaceSnapshot,
        root_id: RunId,
        final_tests: Option<FinalTests>,
        now: i64,
    ) -> Result<(Search, CodingTask, Run), Error> {
        config.validate()?;
        if self.strategy.id() != config.strategy {
            return Err(Error::Unavailable(
                "configured search strategy version is unavailable".into(),
            ));
        }
        let task_id = snapshot.task_id;
        let mut task = CodingTask::with_identities(
            config.task.clone(),
            snapshot.clone(),
            vec![root_id],
            final_tests.clone(),
            now,
        )?;
        task.record.search_id = Some(id);
        let marker = SearchRun {
            search_id: id,
            initial_action: None,
            version: config.version,
        };
        task.record.candidates[0].search = Some(marker.clone());
        let mut agent = config.task.agent.clone();
        agent.workspace = self.workspaces.fork(snapshot.clone(), root_id).await?;
        let root = Run::start_with_id(
            root_id,
            candidate_workflow(config.version)?,
            serde_json::to_value(CandidateInput {
                config: agent,
                snapshot,
                protected_paths: config.task.protected_paths.clone(),
                final_tests,
                branch: None,
                search: Some(marker),
            })?,
            config.task.budget.max_attempts,
            now,
        )?;
        let search = Search::new(id, config, task_id, root_id, now)?;
        Ok((search, task, root))
    }
    pub async fn status(&self, id: SearchId) -> Result<Search, Error> {
        self.store.search(id).await
    }
    pub async fn task(&self, id: SearchId) -> Result<CodingTask, Error> {
        let search = self.status(id).await?;
        self.tasks.task(search.record().task_id).await
    }
    async fn action(&self, search: &Search) -> Result<Option<Action>, Error> {
        let r = search.record();
        if self.strategy.id() != r.config.strategy {
            return Err(Error::Unavailable(
                "pinned search strategy version is unavailable".into(),
            ));
        }
        Ok(match r.phase {
            SearchPhase::Warmup => {
                let run = self.engine.status(r.root).await?;
                if run.record().status == RunStatus::Paused
                    && !run.record().controls.pause_requested
                {
                    let state: SearchCandidateState =
                        serde_json::from_value(run.record().state.clone())?;
                    Some(if state.steps >= r.config.warmup_steps || state.finished {
                        Action::Checkpoint
                    } else {
                        Action::Resume(r.root)
                    })
                } else if !run.record().status.is_unfinished()
                    && run.record().status != RunStatus::Blocked
                {
                    Some(Action::Fail(format!(
                        "root stopped: {}",
                        run.record().status.as_str()
                    )))
                } else {
                    None
                }
            }
            SearchPhase::Checkpoint => match self.branches.checkpoint(r.checkpoint_id).await {
                Err(Error::CheckpointNotFound(_)) => Some(Action::Checkpoint),
                Err(error) => return Err(error),
                Ok(cp) => match cp.record().status {
                    PreparationStatus::Ready => Some(Action::Propose),
                    PreparationStatus::Failed => Some(Action::Fail(
                        cp.record()
                            .error
                            .clone()
                            .unwrap_or_else(|| "checkpoint failed".into()),
                    )),
                    PreparationStatus::Preparing => None,
                },
            },
            SearchPhase::Proposing => {
                let run = self.engine.status(r.proposal_run_id).await?;
                if run.record().status == RunStatus::Succeeded {
                    Some(Action::Expand)
                } else if !run.record().status.is_unfinished()
                    && run.record().status != RunStatus::Blocked
                {
                    Some(Action::Fail(
                        run.record()
                            .error
                            .as_ref()
                            .map(|e| e.message.clone())
                            .unwrap_or_else(|| "proposal generation stopped".into()),
                    ))
                } else {
                    None
                }
            }
            SearchPhase::RollingOut => {
                let fork = self.branches.fork(r.fork_id).await?;
                if fork.status == PreparationStatus::Failed {
                    Some(Action::Fail(
                        fork.error
                            .unwrap_or_else(|| "fork preparation failed".into()),
                    ))
                } else if fork.status == PreparationStatus::Preparing {
                    None
                } else {
                    for node in r.nodes.iter().filter(|n| n.result.is_none()) {
                        let run = self.engine.status(node.run_id).await?;
                        if run.record().status == RunStatus::Paused
                            && !run.record().controls.pause_requested
                        {
                            let state: SearchCandidateState =
                                serde_json::from_value(run.record().state.clone())?;
                            return Ok(Some(
                                if state.steps >= r.config.rollout_steps
                                    || state.finished
                                    || state.agent["tests_passed"] == true
                                {
                                    if search.needs_feedback(&run)? {
                                        Action::Feedback(node.run_id)
                                    } else {
                                        Action::Score(node.run_id)
                                    }
                                } else {
                                    Action::Resume(node.run_id)
                                },
                            ));
                        }
                        if !run.record().status.is_unfinished()
                            && run.record().status != RunStatus::Blocked
                        {
                            return Ok(Some(Action::Score(node.run_id)));
                        }
                    }
                    if r.nodes.iter().all(|n| n.result.is_some()) {
                        let selected = self.strategy.select(r)?;
                        if let Some(id) = selected
                            && self
                                .engine
                                .status(id)
                                .await?
                                .record()
                                .controls
                                .pause_requested
                        {
                            None
                        } else {
                            Some(Action::Select(selected))
                        }
                    } else {
                        None
                    }
                }
            }
            SearchPhase::Verifying => {
                let run = self
                    .engine
                    .status(r.selected.ok_or(DomainError::InvalidState)?)
                    .await?;
                (!run.record().status.is_unfinished() && run.record().status != RunStatus::Blocked)
                    .then_some(Action::Finish)
            }
            SearchPhase::Completed | SearchPhase::Failed => None,
        })
    }
    async fn apply(&self, id: SearchId, action: Action) -> Result<(), Error> {
        let now = self.engine.clock.now_ms();
        match action {
            Action::Resume(run) => self.engine.resume(run).await,
            Action::Checkpoint => {
                let search = self.status(id).await?;
                self.store.checkpoint_requested(id, now).await?;
                self.branches
                    .create_checkpoint(search.record().checkpoint_id, search.record().root)
                    .await?;
                Ok(())
            }
            Action::Propose => self.store.begin_proposals(id, now).await,
            Action::Expand => self.store.begin_rollouts(id, now).await,
            Action::Score(run) => self.store.score_rollout(id, run, now).await,
            Action::Feedback(run) => self.store.begin_feedback(id, run, now).await,
            Action::Select(run) => self.store.select_search(id, run, now).await,
            Action::Finish => self.store.finish_search(id, None, now).await,
            Action::Fail(error) => self.store.finish_search(id, Some(error), now).await,
        }
    }
}
impl WorkCoordinator for SearchService {
    fn advance(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            for id in self.store.active_searches().await? {
                let search = self.status(id).await?;
                if let Some(action) = self.action(&search).await? {
                    match self.apply(id, action).await {
                        Err(Error::Domain(
                            error @ (DomainError::InvalidConfiguration(_)
                            | DomainError::InvalidWorkflow(_)),
                        )) => {
                            self.store
                                .finish_search(
                                    id,
                                    Some(error.to_string()),
                                    self.engine.clock.now_ms(),
                                )
                                .await?
                        }
                        result => result?,
                    }
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
    fn has_pending(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            for id in self.store.active_searches().await? {
                if self.action(&self.status(id).await?).await?.is_some() {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
}
