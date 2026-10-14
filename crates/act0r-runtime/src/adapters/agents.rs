use crate::{
    application::{Error, ports::*},
    domain::{agent::*, answer::*, integration::*, planner::*, *},
};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc};

fn capabilities(checkpoints: CheckpointGranularity) -> AgentCapabilities {
    AgentCapabilities {
        checkpoints,
        cooperative_cancellation: true,
        human_input_waits: true,
        usage: UsageReporting::ReportedOrReserved,
        branching: false,
        delegation: false,
        task_groups: false,
    }
}
pub struct CodingAgentAdapter {
    workspaces: Arc<dyn WorkspaceFactory>,
}
impl CodingAgentAdapter {
    pub fn new(workspaces: Arc<dyn WorkspaceFactory>) -> Self {
        Self { workspaces }
    }
}
impl AgentAdapter for CodingAgentAdapter {
    fn registration(&self) -> AgentRegistration {
        AgentRegistration {
            contract_version: 1,
            adapter: DefinitionId::new("coding", 1),
            definition: AgentWorkflow.id(),
            capabilities: capabilities(CheckpointGranularity::ModelAndTool),
        }
    }
    fn definition(&self) -> Arc<dyn WorkflowDefinition> {
        Arc::new(AgentWorkflow)
    }
    fn prepare(&self, config: Value, source: Option<PathBuf>) -> PortFuture<'_, Value> {
        Box::pin(async move {
            let mut config: AgentConfig = serde_json::from_value(config)?;
            config.validate()?;
            let source = source.ok_or_else(|| {
                Error::Unavailable("coding adapter requires a source workspace".into())
            })?;
            config.workspace = self.workspaces.prepare(source).await?;
            Ok(serde_json::to_value(config)?)
        })
    }
}
pub struct AnswerAgentAdapter;
impl AgentAdapter for AnswerAgentAdapter {
    fn pin_config(&self, config: Value, workspace: Option<String>) -> Result<Value, Error> {
        if workspace.is_some() {
            return Err(Error::Unavailable(
                "answer adapter does not use a source workspace".into(),
            ));
        }
        let config: AnswerConfig = serde_json::from_value(config)?;
        if config.project_instructions.is_some() {
            return Err(DomainError::InvalidConfiguration(
                "project_instructions is selected by the actor runtime".into(),
            )
            .into());
        }
        if !config.tools.is_empty()
            || !config.tool_definitions.is_empty()
            || config.workspace.is_some()
            || config.accept_workspace_handoff
            || config.workspace_input.is_some()
        {
            return Err(DomainError::InvalidConfiguration(
                "use the tool adapter for native tool calls".into(),
            )
            .into());
        }
        config.validate()?;
        Ok(serde_json::to_value(config)?)
    }
    fn check_config(&self, config: &Value) -> Result<(), Error> {
        self.pin_config(config.clone(), None).map(|_| ())
    }
    fn registration(&self) -> AgentRegistration {
        AgentRegistration {
            contract_version: 1,
            adapter: DefinitionId::new("answer", 1),
            definition: AnswerWorkflow.id(),
            capabilities: capabilities(CheckpointGranularity::ModelDecision),
        }
    }
    fn definition(&self) -> Arc<dyn WorkflowDefinition> {
        Arc::new(AnswerWorkflow)
    }
    fn prepare(&self, config: Value, source: Option<PathBuf>) -> PortFuture<'_, Value> {
        Box::pin(async move {
            self.pin_config(config, source.map(|p| p.to_string_lossy().into_owned()))
        })
    }
}
#[derive(Default)]
pub struct PlannerAgentAdapter {
    tools: Arc<crate::application::ToolRegistry>,
    workspaces: Option<Arc<dyn WorkspaceFactory>>,
}
impl PlannerAgentAdapter {
    pub fn new(tools: Arc<crate::application::ToolRegistry>) -> Self {
        Self {
            tools,
            workspaces: None,
        }
    }
    pub fn with_workspaces(mut self, workspaces: Arc<dyn WorkspaceFactory>) -> Self {
        self.workspaces = Some(workspaces);
        self
    }
}
/// Tool-enabled answering uses the same policy, with a separately pinned checkpoint contract.
pub struct ToolAgentAdapter {
    tools: Arc<crate::application::ToolRegistry>,
    workspaces: Option<Arc<dyn WorkspaceFactory>>,
}
impl ToolAgentAdapter {
    pub fn new(tools: Arc<crate::application::ToolRegistry>) -> Self {
        Self {
            tools,
            workspaces: None,
        }
    }
    pub fn with_workspaces(mut self, workspaces: Arc<dyn WorkspaceFactory>) -> Self {
        self.workspaces = Some(workspaces);
        self
    }
}
impl AgentAdapter for ToolAgentAdapter {
    fn pin_config(&self, config: Value, workspace: Option<String>) -> Result<Value, Error> {
        let mut config: AnswerConfig = serde_json::from_value(config)?;
        if config.project_instructions.is_some() {
            return Err(DomainError::InvalidConfiguration(
                "project_instructions is selected by the actor runtime".into(),
            )
            .into());
        }
        if config.workspace_input.is_some() {
            return Err(DomainError::InvalidConfiguration(
                "workspace_input is selected by the actor runtime".into(),
            )
            .into());
        }
        config.workspace = selected_workspace(&config.workspace, &workspace)?;
        if !config.tool_definitions.is_empty() {
            return Err(DomainError::InvalidConfiguration(
                "tool_definitions are pinned by the runtime; configure tools instead".into(),
            )
            .into());
        }
        if config.workspace.is_some() {
            self.tools.pin_workspace_defaults(&mut config.tools);
        }
        config.tool_definitions = self.tools.pin(&config.tools)?;
        if !config.tools.is_empty() {
            config
                .clarification_policy
                .get_or_insert_with(Default::default);
        }
        config.context.get_or_insert(ContextPolicy {
            max_input_bytes: config.max_input_bytes,
            ..ContextPolicy::default()
        });
        config.validate()?;
        Ok(serde_json::to_value(config)?)
    }
    fn check_config(&self, config: &Value) -> Result<(), Error> {
        let config: AnswerConfig = serde_json::from_value(config.clone())?;
        if config.project_instructions.is_some() {
            return Err(DomainError::InvalidConfiguration(
                "project_instructions is selected by the actor runtime".into(),
            )
            .into());
        }
        if config.workspace_input.is_some() {
            return Err(DomainError::InvalidConfiguration(
                "workspace_input is selected by the actor runtime".into(),
            )
            .into());
        }
        config.validate()?;
        self.tools.check_pins(&config.tool_definitions)
    }
    fn registration(&self) -> AgentRegistration {
        AgentRegistration {
            contract_version: 1,
            adapter: DefinitionId::new("tool", 1),
            definition: ToolAgentWorkflow::definition_id(),
            capabilities: capabilities(CheckpointGranularity::ModelAndTool),
        }
    }
    fn definition(&self) -> Arc<dyn WorkflowDefinition> {
        Arc::new(ToolAgentWorkflow::new(self.tools.definitions()))
    }
    fn prepare(&self, config: Value, source: Option<PathBuf>) -> PortFuture<'_, Value> {
        Box::pin(async move {
            let mut config: AnswerConfig = serde_json::from_value(self.pin_config(
                config,
                source.as_ref().map(|p| p.to_string_lossy().into_owned()),
            )?)?;
            config.workspace = prepare_workspace(&self.workspaces, source).await?;
            config.validate()?;
            Ok(serde_json::to_value(config)?)
        })
    }
}
impl AgentAdapter for PlannerAgentAdapter {
    fn pin_config(&self, config: Value, workspace: Option<String>) -> Result<Value, Error> {
        let mut config: PlannerConfig = serde_json::from_value(config)?;
        if config.project_instructions.is_some() {
            return Err(DomainError::InvalidConfiguration(
                "project_instructions is selected by the actor runtime".into(),
            )
            .into());
        }
        config.workspace = selected_workspace(&config.workspace, &workspace)?;
        if config.follow_up_context.is_some() {
            return Err(DomainError::InvalidConfiguration(
                "follow_up_context is selected by the actor runtime".into(),
            )
            .into());
        }
        if !config.tool_definitions.is_empty() {
            return Err(DomainError::InvalidConfiguration(
                "tool_definitions are pinned by the runtime; configure tools instead".into(),
            )
            .into());
        }
        if config.workspace.is_some() {
            self.tools.pin_workspace_defaults(&mut config.tools);
        }
        config.tool_definitions = self.tools.pin(&config.tools)?;
        config.context.get_or_insert(ContextPolicy {
            max_input_bytes: config.max_input_bytes,
            ..ContextPolicy::default()
        });
        config.validate()?;
        Ok(serde_json::to_value(config)?)
    }
    fn check_config(&self, config: &Value) -> Result<(), Error> {
        let config: PlannerConfig = serde_json::from_value(config.clone())?;
        if config.project_instructions.is_some() {
            return Err(DomainError::InvalidConfiguration(
                "project_instructions is selected by the actor runtime".into(),
            )
            .into());
        }
        config.validate()?;
        self.tools.check_pins(&config.tool_definitions)
    }
    fn registration(&self) -> AgentRegistration {
        let mut capabilities = capabilities(CheckpointGranularity::ModelAndTool);
        capabilities.delegation = true;
        capabilities.task_groups = true;
        AgentRegistration {
            contract_version: 1,
            adapter: DefinitionId::new("planner", 3),
            definition: PlannerWorkflow::definition_id(),
            capabilities,
        }
    }
    fn definition(&self) -> Arc<dyn WorkflowDefinition> {
        Arc::new(PlannerWorkflow::new(self.tools.definitions()))
    }
    fn prepare(&self, config: Value, source: Option<PathBuf>) -> PortFuture<'_, Value> {
        Box::pin(async move {
            let mut config: PlannerConfig = serde_json::from_value(self.pin_config(
                config,
                source.as_ref().map(|p| p.to_string_lossy().into_owned()),
            )?)?;
            config.workspace = prepare_workspace(&self.workspaces, source).await?;
            config.validate()?;
            Ok(serde_json::to_value(config)?)
        })
    }
}

fn selected_workspace(
    pinned: &Option<String>,
    source: &Option<String>,
) -> Result<Option<String>, Error> {
    if pinned.is_some() {
        return Err(DomainError::InvalidConfiguration(
            "workspace is pinned by the runtime; supply a source directory instead".into(),
        )
        .into());
    }
    // Validate all configuration before allocating the managed copy.
    Ok(source.clone())
}
async fn prepare_workspace(
    factory: &Option<Arc<dyn WorkspaceFactory>>,
    source: Option<PathBuf>,
) -> Result<Option<String>, Error> {
    match source {
        Some(source) => Ok(Some(
            factory
                .as_ref()
                .ok_or_else(|| {
                    Error::Unavailable(
                        "workspace factory is not configured for this adapter".into(),
                    )
                })?
                .prepare(source)
                .await?,
        )),
        None => Ok(None),
    }
}
