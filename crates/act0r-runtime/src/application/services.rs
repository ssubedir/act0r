use super::{Error, ports::*};
use crate::domain::*;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct Engine {
    pub(crate) store: Arc<dyn ExecutionStore>,
    pub(crate) executor: Arc<dyn ActivityExecutor>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) ownership: Arc<dyn WorkerOwnership>,
    pub(crate) definitions: Arc<HashMap<DefinitionId, Arc<dyn WorkflowDefinition>>>,
    pub(crate) coordinator: Option<Arc<dyn WorkCoordinator>>,
}
impl Engine {
    pub fn new(
        store: Arc<dyn ExecutionStore>,
        executor: Arc<dyn ActivityExecutor>,
        clock: Arc<dyn Clock>,
        ownership: Arc<dyn WorkerOwnership>,
        definitions: Vec<Arc<dyn WorkflowDefinition>>,
    ) -> Result<Self, Error> {
        let mut registry = HashMap::new();
        for definition in definitions {
            let id = definition.id();
            if registry.insert(id.clone(), definition).is_some() {
                return Err(Error::Unavailable(format!(
                    "duplicate workflow {} v{}",
                    id.name, id.version
                )));
            }
        }
        Ok(Self {
            store,
            executor,
            clock,
            ownership,
            definitions: Arc::new(registry),
            coordinator: None,
        })
    }
    pub fn with_coordinator(mut self, coordinator: Arc<dyn WorkCoordinator>) -> Self {
        self.coordinator = Some(coordinator);
        self
    }
    pub async fn start(
        &self,
        workflow: DefinitionId,
        input: Value,
        attempt_limit: u32,
    ) -> Result<RunId, Error> {
        let definition = self.definitions.get(&workflow).ok_or_else(|| {
            Error::Unavailable(format!("{} v{}", workflow.name, workflow.version))
        })?;
        let run = Run::start(
            definition.as_ref(),
            input,
            attempt_limit,
            self.clock.now_ms(),
        )?;
        let id = run.record().id;
        self.store.create(run).await?;
        Ok(id)
    }
    pub async fn status(&self, id: RunId) -> Result<Run, Error> {
        self.store.get(id).await
    }
    pub async fn history(&self, id: RunId) -> Result<Vec<HistoryEvent>, Error> {
        self.store.history(id).await
    }
    pub async fn shared_budgets(
        &self,
        id: RunId,
    ) -> Result<Vec<shared_budget::SharedBudgetView>, Error> {
        self.store.shared_budgets(id).await
    }

    pub async fn pause(&self, id: RunId) -> Result<(), Error> {
        self.control(id, |run, now| run.pause(now)).await
    }
    pub async fn resume(&self, id: RunId) -> Result<(), Error> {
        let engine = self.clone();
        let now = self.clock.now_ms();
        self.store
            .mutate(
                id,
                Box::new(move |run| {
                    if run.record().status == RunStatus::Blocked {
                        if let Some(reason) = engine.incompatibility(run) {
                            return Err(DomainError::InvalidWorkflow(reason));
                        }
                        run.restore_implementation(now)?;
                    }
                    run.resume(now)?;
                    Ok(None)
                }),
            )
            .await?;
        Ok(())
    }
    pub async fn cancel(&self, id: RunId) -> Result<(), Error> {
        self.control(id, |run, now| run.cancel(now)).await
    }
    async fn control(
        &self,
        id: RunId,
        change: fn(&mut Run, i64) -> Result<(), DomainError>,
    ) -> Result<(), Error> {
        let now = self.clock.now_ms();
        self.store
            .mutate(
                id,
                Box::new(move |run| {
                    change(run, now)?;
                    Ok(None)
                }),
            )
            .await?;
        Ok(())
    }
    pub async fn signal(&self, id: RunId, wait: WaitId, payload: Value) -> Result<(), Error> {
        let definitions = self.definitions.clone();
        let now = self.clock.now_ms();
        self.store
            .mutate(
                id,
                Box::new(move |run| {
                    let workflow = definitions.get(&run.record().workflow).ok_or_else(|| {
                        DomainError::InvalidWorkflow("definition unavailable".into())
                    })?;
                    run.signal(wait, payload, workflow.as_ref(), now)?;
                    Ok(None)
                }),
            )
            .await?;
        Ok(())
    }

    pub async fn reconcile(&self, id: RunId, request: ReconciliationRequest) -> Result<(), Error> {
        let definitions = self.definitions.clone();
        let now = self.clock.now_ms();
        self.store
            .mutate(
                id,
                Box::new(move |run| {
                    let workflow = definitions.get(&run.record().workflow);
                    run.reconcile(request, workflow.map(|w| w.as_ref()), now)?;
                    Ok(None)
                }),
            )
            .await?;
        Ok(())
    }

    pub(crate) fn incompatibility(&self, run: &Run) -> Option<String> {
        let Some(definition) = self.definitions.get(&run.record().workflow) else {
            return Some(format!(
                "missing workflow {} v{}",
                run.record().workflow.name,
                run.record().workflow.version
            ));
        };
        if let Err(error) = definition.validate_state(&run.record().state) {
            return Some(error.to_string());
        }
        run.activities()
            .iter()
            .filter(|a| matches!(a.status, ActivityStatus::Queued | ActivityStatus::Running))
            .find(|a| !self.executor.supports(&a.spec.handler))
            .map(|a| {
                format!(
                    "missing handler {} v{}",
                    a.spec.handler.name, a.spec.handler.version
                )
            })
    }
}
