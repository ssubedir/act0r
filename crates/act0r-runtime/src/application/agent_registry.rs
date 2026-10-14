use super::{Error, ports::*};
use crate::domain::{integration::*, *};
use serde_json::Value;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

struct RegisteredAdapter {
    registration: AgentRegistration,
    adapter: Arc<dyn AgentAdapter>,
    workflow: Arc<dyn WorkflowDefinition>,
}
pub struct AgentRegistry {
    adapters: HashMap<DefinitionId, RegisteredAdapter>,
}
impl AgentRegistry {
    pub fn new(adapters: Vec<Arc<dyn AgentAdapter>>) -> Result<Self, Error> {
        let mut registry = HashMap::new();
        for adapter in adapters {
            let registration = adapter.registration();
            let workflow = Arc::new(RegisteredAgentWorkflow::new(
                registration.clone(),
                adapter.definition(),
            )?);
            if registry
                .insert(
                    registration.adapter.clone(),
                    RegisteredAdapter {
                        registration,
                        adapter,
                        workflow,
                    },
                )
                .is_some()
            {
                return Err(Error::Unavailable(
                    "duplicate agent adapter registration".into(),
                ));
            }
        }
        Ok(Self { adapters: registry })
    }
    pub fn registrations(&self) -> Vec<AgentRegistration> {
        let mut registrations: Vec<_> = self
            .adapters
            .values()
            .map(|a| a.registration.clone())
            .collect();
        registrations.sort_by(|a, b| {
            (&a.adapter.name, a.adapter.version).cmp(&(&b.adapter.name, b.adapter.version))
        });
        registrations
    }
    pub fn workflows(&self) -> Vec<Arc<dyn WorkflowDefinition>> {
        self.adapters.values().map(|a| a.workflow.clone()).collect()
    }
    pub fn registration(&self, id: &DefinitionId) -> Result<&AgentRegistration, Error> {
        self.adapters
            .get(id)
            .map(|a| &a.registration)
            .ok_or_else(|| {
                Error::Unavailable(format!(
                    "agent adapter {} v{} is not registered",
                    id.name, id.version
                ))
            })
    }
    pub fn check(&self, pinned: &AgentRegistration) -> Result<(), Error> {
        if self.registration(&pinned.adapter)? != pinned {
            return Err(Error::Unavailable(
                "pinned agent capabilities or workflow differ; restore the registered version"
                    .into(),
            ));
        }
        Ok(())
    }
    pub fn check_config(&self, pinned: &AgentRegistration, config: &Value) -> Result<(), Error> {
        self.check(pinned)?;
        self.adapters
            .get(&pinned.adapter)
            .ok_or_else(|| Error::Unavailable("agent adapter is not registered".into()))?
            .adapter
            .check_config(config)
    }
    pub async fn prepare(
        &self,
        id: &DefinitionId,
        config: Value,
        source: Option<PathBuf>,
    ) -> Result<Value, Error> {
        self.adapters
            .get(id)
            .ok_or_else(|| {
                Error::Unavailable(format!(
                    "agent adapter {} v{} is not registered",
                    id.name, id.version
                ))
            })?
            .adapter
            .prepare(config, source)
            .await
    }
    pub fn pin_config(
        &self,
        id: &DefinitionId,
        config: Value,
        workspace: Option<String>,
    ) -> Result<Value, Error> {
        self.adapters
            .get(id)
            .ok_or_else(|| {
                Error::Unavailable(format!(
                    "agent adapter {} v{} is not registered",
                    id.name, id.version
                ))
            })?
            .adapter
            .pin_config(config, workspace)
    }
}
