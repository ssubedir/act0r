use super::{Engine, Error, ports::*};
use crate::domain::{branch::*, *};
use std::sync::Arc;

#[derive(Clone)]
pub struct BranchService {
    engine: Engine,
    store: Arc<dyn BranchStore>,
    tasks: Arc<dyn TaskStore>,
    workspaces: Arc<dyn CandidateWorkspaces>,
}
impl BranchService {
    pub fn new(
        engine: Engine,
        store: Arc<dyn BranchStore>,
        tasks: Arc<dyn TaskStore>,
        workspaces: Arc<dyn CandidateWorkspaces>,
    ) -> Self {
        Self {
            engine,
            store,
            tasks,
            workspaces,
        }
    }
    /// Requests preparation; the owned worker performs filesystem work and publishes it.
    pub async fn create_checkpoint(
        &self,
        id: CheckpointId,
        parent: RunId,
    ) -> Result<Checkpoint, Error> {
        self.store
            .begin_checkpoint(id, parent, self.engine.clock.now_ms())
            .await
    }
    pub async fn checkpoint(&self, id: CheckpointId) -> Result<Checkpoint, Error> {
        self.store.checkpoint(id).await
    }
    pub async fn create_fork(
        &self,
        id: ForkId,
        checkpoint: CheckpointId,
        count: u32,
    ) -> Result<ForkRecord, Error> {
        self.store
            .begin_fork(id, checkpoint, count, self.engine.clock.now_ms())
            .await
    }
    pub async fn fork(&self, id: ForkId) -> Result<ForkRecord, Error> {
        self.store.fork_record(id).await
    }
    async fn capture(&self, id: CheckpointId) -> Result<(), Error> {
        let checkpoint = self.store.checkpoint(id).await?;
        let record = checkpoint.record();
        let result = self
            .workspaces
            .capture_checkpoint(record.input.clone(), record.artifact_id)
            .await;
        match result {
            Ok(snapshot) => match self
                .store
                .publish_checkpoint(id, snapshot, self.engine.clock.now_ms())
                .await
            {
                Err(Error::Domain(error)) => {
                    self.store
                        .fail_checkpoint(id, error.to_string(), self.engine.clock.now_ms())
                        .await
                }
                result => result,
            },
            Err(error) => {
                self.store
                    .fail_checkpoint(id, error.to_string(), self.engine.clock.now_ms())
                    .await
            }
        }
    }
    async fn prepare_fork(&self, id: ForkId) -> Result<(), Error> {
        let plan = self.store.fork_record(id).await?;
        let checkpoint = self.store.checkpoint(plan.checkpoint_id).await?;
        let task = self.tasks.task(plan.task_id).await?;
        let snapshot = checkpoint
            .record()
            .snapshot
            .clone()
            .ok_or(crate::domain::DomainError::InvalidState)?;
        let mut runs = vec![];
        for (index, child) in plan.run_ids.into_iter().enumerate() {
            let workspace = match self
                .workspaces
                .fork_checkpoint(snapshot.clone(), child)
                .await
            {
                Ok(workspace) => workspace,
                Err(error) => {
                    return self
                        .store
                        .fail_fork(id, error.to_string(), self.engine.clock.now_ms())
                        .await;
                }
            };
            runs.push(if let Some(search) = &plan.search {
                checkpoint.fork_search_run(
                    child,
                    workspace,
                    task.record().config.budget.max_attempts,
                    crate::domain::search::SearchRun {
                        version: search.version,
                        search_id: search.search_id,
                        initial_action: Some(search.actions[index].clone()),
                    },
                    self.engine.clock.now_ms(),
                )?
            } else {
                checkpoint.fork_run(
                    child,
                    workspace,
                    task.record().config.budget.max_attempts,
                    self.engine.clock.now_ms(),
                )?
            });
        }
        self.store
            .publish_fork(id, runs, self.engine.clock.now_ms())
            .await
    }
}
impl WorkCoordinator for BranchService {
    fn advance(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            if let Some(id) = self.store.pending_checkpoints().await?.first().copied() {
                self.capture(id).await?;
                return Ok(true);
            }
            if let Some(id) = self.store.pending_forks().await?.first().copied() {
                self.prepare_fork(id).await?;
                return Ok(true);
            }
            Ok(false)
        })
    }
    fn has_pending(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            Ok(!self.store.pending_checkpoints().await?.is_empty()
                || !self.store.pending_forks().await?.is_empty())
        })
    }
}

pub struct Coordinators {
    items: Vec<Arc<dyn WorkCoordinator>>,
}
impl Coordinators {
    pub fn new(items: Vec<Arc<dyn WorkCoordinator>>) -> Self {
        Self { items }
    }
}
impl WorkCoordinator for Coordinators {
    fn advance(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            for item in &self.items {
                if item.advance().await? {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
    fn has_pending(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            for item in &self.items {
                if item.has_pending().await? {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
}
