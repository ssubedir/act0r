use super::{Engine, Error, ports::*};
use crate::domain::{task::*, *};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone)]
pub struct TaskService {
    engine: Engine,
    store: Arc<dyn TaskStore>,
    workspaces: Arc<dyn CandidateWorkspaces>,
}
impl TaskService {
    pub fn new(
        engine: Engine,
        store: Arc<dyn TaskStore>,
        workspaces: Arc<dyn CandidateWorkspaces>,
    ) -> Self {
        Self {
            engine,
            store,
            workspaces,
        }
    }
    pub async fn start(&self, source: PathBuf, config: TaskConfig) -> Result<TaskId, Error> {
        config.validate()?;
        let id = TaskId::new();
        let snapshot = self.workspaces.snapshot(source, id).await?;
        let now = self.engine.clock.now_ms();
        let mut task = CodingTask::new(config, snapshot.clone(), now)?;
        let mut runs = Vec::new();
        for candidate in &task.record().candidates {
            let mut config = task.record().config.agent.clone();
            config.workspace = self
                .workspaces
                .fork(snapshot.clone(), candidate.run_id)
                .await?;
            runs.push(Run::start_with_id(
                candidate.run_id,
                &CandidateWorkflow,
                serde_json::to_value(CandidateInput {
                    config,
                    snapshot: snapshot.clone(),
                    protected_paths: task.record().config.protected_paths.clone(),
                    final_tests: None,
                    branch: None,
                    search: None,
                })?,
                task.record().config.budget.max_attempts,
                now,
            )?);
        }
        task.refresh(&runs, now)?;
        self.store.create_task(task, runs).await?;
        Ok(id)
    }
    pub async fn status(&self, id: TaskId) -> Result<CodingTask, Error> {
        self.store.task(id).await
    }
    pub(crate) async fn prepare_trial(
        &self,
        id: TaskId,
        config: TaskConfig,
        baseline: WorkspaceSnapshot,
        run_ids: Vec<RunId>,
        final_tests: FinalTests,
        now: i64,
    ) -> Result<(CodingTask, Vec<Run>), Error> {
        let snapshot = self.workspaces.branch_snapshot(baseline, id).await?;
        let mut task =
            CodingTask::with_identities(config, snapshot.clone(), run_ids, Some(final_tests), now)?;
        let mut runs = Vec::new();
        for candidate in &task.record().candidates {
            let mut config = task.record().config.agent.clone();
            config.workspace = self
                .workspaces
                .fork(snapshot.clone(), candidate.run_id)
                .await?;
            runs.push(Run::start_with_id(
                candidate.run_id,
                &CandidateWorkflow,
                serde_json::to_value(CandidateInput {
                    config,
                    snapshot: snapshot.clone(),
                    protected_paths: task.record().config.protected_paths.clone(),
                    final_tests: task.record().final_tests.clone(),
                    branch: None,
                    search: None,
                })?,
                task.record().config.budget.max_attempts,
                now,
            )?);
        }
        task.refresh(&runs, now)?;
        Ok((task, runs))
    }
}
