use super::{
    Engine, Error,
    ports::{OwnershipGuard, PortFuture},
};
use crate::domain::*;
use std::sync::Arc;

pub struct Worker {
    engine: Engine,
    ownership: Arc<dyn OwnershipGuard>,
}
impl Engine {
    pub(crate) fn task_test_permission_decision(&self) -> super::ports::TaskTestPermissionDecision {
        let engine = self.clone();
        Box::new(move |run, grant| {
            if let Some(reason) = engine.incompatibility(run) {
                return Err(Error::Unavailable(reason));
            }
            let definition = engine
                .definitions
                .get(&run.record().workflow)
                .ok_or_else(|| Error::Unavailable("workflow definition unavailable".into()))?;
            run.authorize_task_tests(grant, definition.as_ref(), engine.clock.now_ms())?;
            Ok(())
        })
    }
    pub(crate) fn task_edit_permission_decision(&self) -> super::ports::TaskEditPermissionDecision {
        let engine = self.clone();
        Box::new(move |run, grant| {
            if let Some(reason) = engine.incompatibility(run) {
                return Err(Error::Unavailable(reason));
            }
            let definition = engine
                .definitions
                .get(&run.record().workflow)
                .ok_or_else(|| Error::Unavailable("workflow definition unavailable".into()))?;
            run.authorize_task_edits(grant, definition.as_ref(), engine.clock.now_ms())?;
            Ok(())
        })
    }
    pub(crate) fn test_permission_decision(&self) -> super::ports::TestPermissionDecision {
        let engine = self.clone();
        Box::new(move |run, grant| {
            if let Some(reason) = engine.incompatibility(run) {
                return Err(Error::Unavailable(reason));
            }
            let definition = engine
                .definitions
                .get(&run.record().workflow)
                .ok_or_else(|| Error::Unavailable("workflow definition unavailable".into()))?;
            run.authorize_test_command(grant, definition.as_ref(), engine.clock.now_ms())?;
            Ok(())
        })
    }
    pub async fn worker(&self) -> Result<Worker, Error> {
        let guard = self.ownership.acquire()?;
        let worker = Worker {
            engine: self.clone(),
            ownership: Arc::from(guard),
        };
        for id in self.store.active_runs().await? {
            let engine = self.clone();
            let now = self.clock.now_ms();
            self.store
                .mutate(
                    id,
                    Box::new(move |run| {
                        run.recover(now);
                        if run.record().status.is_active()
                            && let Some(reason) = engine.incompatibility(run)
                        {
                            run.block(reason, now);
                        }
                        Ok(None)
                    }),
                )
                .await?;
        }
        Ok(worker)
    }
}
impl Worker {
    pub fn execution_timeout_ms(&self, execution: &ActivityExecution, default_ms: u64) -> u64 {
        self.engine
            .executor
            .request_timeout_ms(execution)
            .map_or(default_ms, |deadline| {
                default_ms.max(deadline.saturating_add(1000))
            })
    }
    pub async fn advance_work(&mut self) -> Result<bool, Error> {
        if self
            .engine
            .store
            .authorize_task_test_wait(self.engine.task_test_permission_decision())
            .await?
        {
            return Ok(true);
        }
        if self
            .engine
            .store
            .authorize_task_edit_wait(self.engine.task_edit_permission_decision())
            .await?
        {
            return Ok(true);
        }
        if self
            .engine
            .store
            .authorize_test_wait(self.engine.test_permission_decision())
            .await?
        {
            return Ok(true);
        }
        match &self.engine.coordinator {
            Some(coordinator) => coordinator.advance().await,
            None => Ok(false),
        }
    }
    pub async fn has_active_runs(&self) -> Result<bool, Error> {
        if let Some(coordinator) = &self.engine.coordinator
            && coordinator.has_pending().await?
        {
            return Ok(true);
        }
        for id in self.engine.store.runnable_runs().await? {
            if self.engine.store.get(id).await?.record().status.is_active() {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub async fn fire_due_timer(&mut self) -> Result<bool, Error> {
        let engine = self.engine.clone();
        let now = engine.clock.now_ms();
        self.engine
            .store
            .mutate_due_timer(
                now,
                Box::new(move |run, timer_id| {
                    if let Some(reason) = engine.incompatibility(run) {
                        run.block(reason, now);
                        return Ok(true);
                    }
                    let definition =
                        engine
                            .definitions
                            .get(&run.record().workflow)
                            .ok_or_else(|| {
                                DomainError::InvalidWorkflow("definition disappeared".into())
                            })?;
                    run.fire_timer(timer_id, definition.as_ref(), now)
                }),
            )
            .await
    }
    pub async fn admit(&mut self) -> Result<Option<ActivityExecution>, Error> {
        let engine = self.engine.clone();
        let now = engine.clock.now_ms();
        self.engine
            .store
            .mutate_due(
                now,
                Box::new(move |run| {
                    if let Some(reason) = engine.incompatibility(run) {
                        run.block(reason, now);
                        return Ok(None);
                    }
                    let definition =
                        engine
                            .definitions
                            .get(&run.record().workflow)
                            .ok_or_else(|| {
                                DomainError::InvalidWorkflow("definition disappeared".into())
                            })?;
                    run.finish_closed_planner_question(definition.as_ref(), now)?;
                    run.admit(now)
                }),
            )
            .await
    }
    /// Owned I/O retains database ownership; admission and completion remain worker decisions.
    pub fn execute(&self, execution: ActivityExecution) -> PortFuture<'static, Outcome> {
        let executor = self.engine.executor.clone();
        let ownership = self.ownership.clone();
        Box::pin(async move {
            let result = executor.execute(execution).await;
            drop(ownership);
            result
        })
    }
    pub async fn complete(
        &mut self,
        execution: ActivityExecution,
        outcome: Outcome,
    ) -> Result<(), Error> {
        let engine = self.engine.clone();
        let now = engine.clock.now_ms();
        self.engine
            .store
            .mutate(
                execution.run_id,
                Box::new(move |run| {
                    let definition =
                        engine
                            .definitions
                            .get(&run.record().workflow)
                            .ok_or_else(|| {
                                DomainError::InvalidWorkflow("definition disappeared".into())
                            })?;
                    run.complete(&execution, outcome, definition.as_ref(), now)?;
                    Ok(None)
                }),
            )
            .await?;
        Ok(())
    }
    pub async fn step(&mut self) -> Result<bool, Error> {
        if self.advance_work().await? {
            return Ok(true);
        }
        if self.fire_due_timer().await? {
            return Ok(true);
        }
        let Some(execution) = self.admit().await? else {
            return Ok(false);
        };
        let outcome = match self.execute(execution.clone()).await {
            Ok(outcome) => outcome,
            Err(error) => Outcome::Uncertain(format!("executor error: {error}")),
        };
        self.complete(execution, outcome).await?;
        Ok(true)
    }
}
