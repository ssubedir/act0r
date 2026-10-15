use super::{Engine, Error, ports::WorkspaceFactory};
use crate::domain::{
    RunId, WorkflowDefinition,
    actors::AgentDefinition,
    agent::{AgentConfig, AgentWorkflow},
};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone)]
pub struct AgentService {
    engine: Engine,
    workspaces: Arc<dyn WorkspaceFactory>,
    tasks: Option<super::TaskService>,
    benchmarks: Option<Arc<super::BenchmarkService>>,
    branches: Option<Arc<super::BranchService>>,
    search: Option<Arc<super::SearchService>>,
    actors: Option<Arc<super::ActorService>>,
    registry: Option<Arc<super::AgentRegistry>>,
    tool_registry: Option<Arc<super::ToolRegistry>>,
    teams: Option<Arc<super::TeamService>>,
    storage: Option<super::StorageService>,
}
impl AgentService {
    pub fn new(engine: Engine, workspaces: Arc<dyn WorkspaceFactory>) -> Self {
        Self {
            engine,
            workspaces,
            tasks: None,
            benchmarks: None,
            branches: None,
            search: None,
            actors: None,
            registry: None,
            tool_registry: None,
            teams: None,
            storage: None,
        }
    }
    pub fn with_tool_registry(mut self, tools: Arc<super::ToolRegistry>) -> Self {
        self.tool_registry = Some(tools);
        self
    }
    pub fn with_storage(mut self, storage: super::StorageService) -> Self {
        self.storage = Some(storage);
        self
    }
    pub fn storage(&self) -> Result<&super::StorageService, Error> {
        self.storage
            .as_ref()
            .ok_or_else(|| Error::Unavailable("storage inspection is not installed".into()))
    }
    pub fn with_teams(mut self, teams: Arc<super::TeamService>) -> Self {
        self.teams = Some(teams);
        self
    }
    pub fn teams(&self) -> Result<&super::TeamService, Error> {
        self.teams
            .as_deref()
            .ok_or_else(|| Error::Unavailable("team services are not configured".into()))
    }
    pub fn tool_definitions(&self) -> Result<Vec<crate::domain::tools::ToolDefinition>, Error> {
        Ok(self
            .tool_registry
            .as_ref()
            .ok_or_else(|| Error::Unavailable("native tool registry is not configured".into()))?
            .definitions())
    }
    pub fn unavailable_tools(&self) -> &[super::UnavailableTool] {
        self.tool_registry
            .as_ref()
            .map_or(&[], |tools| tools.unavailable_tools())
    }
    pub fn with_tasks(mut self, tasks: super::TaskService) -> Self {
        self.tasks = Some(tasks);
        self
    }
    pub fn tasks(&self) -> Result<&super::TaskService, Error> {
        self.tasks
            .as_ref()
            .ok_or_else(|| Error::Unavailable("coding task services are not configured".into()))
    }
    pub fn with_benchmarks(mut self, benchmarks: Arc<super::BenchmarkService>) -> Self {
        self.benchmarks = Some(benchmarks);
        self
    }
    pub fn benchmarks(&self) -> Result<&super::BenchmarkService, Error> {
        self.benchmarks
            .as_deref()
            .ok_or_else(|| Error::Unavailable("benchmark services are not configured".into()))
    }
    pub async fn start(
        &self,
        source: PathBuf,
        mut config: AgentConfig,
        attempt_limit: u32,
    ) -> Result<RunId, Error> {
        config.validate()?;
        if attempt_limit == 0 {
            return Err(crate::domain::DomainError::InvalidConfiguration(
                "positive attempt allowance required".into(),
            )
            .into());
        }
        config.workspace = self.workspaces.prepare(source).await?;
        self.engine
            .start(
                AgentWorkflow.id(),
                serde_json::to_value(config)?,
                attempt_limit,
            )
            .await
    }
    pub fn with_branches(mut self, branches: Arc<super::BranchService>) -> Self {
        self.branches = Some(branches);
        self
    }
    pub fn branches(&self) -> Result<&super::BranchService, Error> {
        self.branches
            .as_deref()
            .ok_or_else(|| Error::Unavailable("branch services are not configured".into()))
    }
    pub fn with_search(mut self, search: Arc<super::SearchService>) -> Self {
        self.search = Some(search);
        self
    }
    pub fn search(&self) -> Result<&super::SearchService, Error> {
        self.search
            .as_deref()
            .ok_or_else(|| Error::Unavailable("search services are not configured".into()))
    }
    pub fn with_actors(mut self, actors: Arc<super::ActorService>) -> Self {
        self.actors = Some(actors);
        self
    }
    pub fn actors(&self) -> Result<&super::ActorService, Error> {
        self.actors
            .as_deref()
            .ok_or_else(|| Error::Unavailable("agent actor services are not configured".into()))
    }
    pub fn with_agent_registry(mut self, registry: Arc<super::AgentRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }
    pub fn registrations(
        &self,
    ) -> Result<Vec<crate::domain::integration::AgentRegistration>, Error> {
        Ok(self
            .registry
            .as_ref()
            .ok_or_else(|| Error::Unavailable("agent adapter registry is not configured".into()))?
            .registrations())
    }
    /// Select the current installed adapter when a caller does not pin a version.
    pub fn latest_adapter(&self, name: &str) -> Result<crate::domain::DefinitionId, Error> {
        self.registrations()?
            .into_iter()
            .filter(|r| r.adapter.name == name)
            .max_by_key(|r| r.adapter.version)
            .map(|r| r.adapter)
            .ok_or_else(|| Error::Unavailable(format!("agent adapter {name} is not registered")))
    }
    pub async fn create_registered_actor(
        &self,
        adapter: crate::domain::DefinitionId,
        source: Option<PathBuf>,
        mut config: serde_json::Value,
        attempt_limit: u32,
        queue_policy: crate::domain::actors::AgentQueuePolicy,
    ) -> Result<crate::domain::AgentId, Error> {
        queue_policy.validate()?;
        if attempt_limit == 0 {
            return Err(crate::domain::DomainError::InvalidConfiguration(
                "positive attempt allowance required".into(),
            )
            .into());
        }
        let registry = self
            .registry
            .as_ref()
            .ok_or_else(|| Error::Unavailable("agent adapter registry is not configured".into()))?;
        let registration = registry.registration(&adapter)?.clone();
        let supervision = config
            .as_object_mut()
            .and_then(|object| object.remove("supervision"))
            .map(serde_json::from_value::<crate::domain::actors::SupervisionPolicy>)
            .transpose()?;
        if let Some(policy) = &supervision {
            policy.validate()?;
        }
        if config.get("actor_memory").is_some()
            || config.get("session_summary").is_some()
            || config.get("project_instructions").is_some()
        {
            return Err(crate::domain::DomainError::InvalidConfiguration(
                "actor_memory, session_summary and project_instructions are selected by the runtime"
                    .into(),
            )
            .into());
        }
        let memory = config
            .as_object_mut()
            .and_then(|object| object.remove("memory"))
            .map(serde_json::from_value::<crate::domain::actors::MemoryPolicy>)
            .transpose()?;
        if let Some(policy) = &memory {
            policy.validate()?;
            if ![
                AgentWorkflow.id(),
                crate::domain::answer::AnswerWorkflow.id(),
                crate::domain::answer::ToolAgentWorkflow::definition_id(),
                crate::domain::planner::PlannerWorkflow::definition_id(),
            ]
            .contains(&registration.definition)
            {
                return Err(crate::domain::DomainError::InvalidConfiguration(
                    "actor memory requires a native coding, answering, or planner integration"
                        .into(),
                )
                .into());
            }
        }
        if !self
            .engine
            .definitions
            .contains_key(&registration.workflow_id())
        {
            return Err(Error::Unavailable(
                "registered agent workflow is not installed in the engine".into(),
            ));
        }
        let actors = self.actors()?;
        let input = registry.prepare(&adapter, config, source).await?;
        if registration.definition == crate::domain::planner::PlannerWorkflow::definition_id() {
            let planner: crate::domain::planner::PlannerConfig =
                serde_json::from_value(input.clone())?;
            for worker in planner.workers {
                let actor = actors.status(worker.agent_id).await?;
                if actor.record().definition
                    != crate::domain::actors::RegisteredAgentDefinition.id()
                {
                    return Err(Error::Unavailable(
                        "planner workers must be registered agent actors".into(),
                    ));
                }
                let pinned: crate::domain::actors::RegisteredAgentConfig =
                    serde_json::from_value(actor.record().config.clone())?;
                registry.check(&pinned.registration)?;
            }
        }
        actors
            .create_with_policies(
                crate::domain::AgentId::new(),
                crate::domain::actors::RegisteredAgentDefinition.id(),
                serde_json::to_value(crate::domain::actors::RegisteredAgentConfig {
                    registration,
                    input,
                    attempt_limit,
                    memory,
                })?,
                queue_policy,
                supervision,
            )
            .await
    }
    pub async fn create_coding_actor(
        &self,
        source: PathBuf,
        config: AgentConfig,
        attempt_limit: u32,
    ) -> Result<crate::domain::AgentId, Error> {
        self.create_coding_actor_with_queue_policy(
            source,
            config,
            attempt_limit,
            crate::domain::actors::AgentQueuePolicy::default(),
        )
        .await
    }
    pub async fn create_coding_actor_with_queue_policy(
        &self,
        source: PathBuf,
        config: AgentConfig,
        attempt_limit: u32,
        queue_policy: crate::domain::actors::AgentQueuePolicy,
    ) -> Result<crate::domain::AgentId, Error> {
        self.create_registered_actor(
            crate::domain::DefinitionId::new("coding", 1),
            Some(source),
            serde_json::to_value(config)?,
            attempt_limit,
            queue_policy,
        )
        .await
    }
}
