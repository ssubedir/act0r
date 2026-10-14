use super::*;
use crate::domain::integration::{AgentInvocation, AgentRegistration};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredAgentConfig {
    pub registration: AgentRegistration,
    pub input: Value,
    pub attempt_limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryPolicy>,
}
impl RegisteredAgentConfig {
    fn worker(&self) -> Result<Value, DomainError> {
        self.registration.validate()?;
        if self.input.get("actor_memory").is_some()
            || self.input.get("session_summary").is_some()
            || self.input.get("follow_up_context").is_some()
            || self.input.get("workspace_input").is_some()
            || self.input.get("project_instructions").is_some()
        {
            return Err(DomainError::InvalidConfiguration(
                "actor_memory, session_summary, follow_up_context, workspace_input, and project_instructions are selected by the runtime"
                    .into(),
            ));
        }
        if self.memory.is_some()
            && ![
                crate::domain::agent::AgentWorkflow.id(),
                crate::domain::answer::AnswerWorkflow.id(),
                crate::domain::answer::ToolAgentWorkflow::definition_id(),
                crate::domain::planner::PlannerWorkflow::definition_id(),
            ]
            .contains(&self.registration.definition)
        {
            return Err(DomainError::InvalidConfiguration(
                "actor memory requires a native coding, answering, or planner integration".into(),
            ));
        }
        serde_json::to_value(WorkflowWorkerConfig {
            workflow: self.registration.workflow_id(),
            input: self.input.clone(),
            attempt_limit: self.attempt_limit,
            memory: self.memory.clone(),
        })
        .map_err(|e| DomainError::InvalidConfiguration(e.to_string()))
    }
}
/// Reuses the sequential worker policy, adding a pinned invocation around each Run.
pub struct RegisteredAgentDefinition;
impl AgentDefinition for RegisteredAgentDefinition {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("agent_worker", 1)
    }
    fn supports_supervision(&self) -> bool {
        true
    }
    fn initialize(&self, config: &Value) -> Result<Value, DomainError> {
        let config: RegisteredAgentConfig = decode(config)?;
        WorkflowWorkerAgent::initialize_state(&config.worker()?)
    }
    fn memory(&self, state: &Value) -> Result<Option<ActorMemory>, DomainError> {
        WorkflowWorkerAgent.memory(state)
    }
    fn clear_memory(&self, state: &Value) -> Result<Value, DomainError> {
        WorkflowWorkerAgent.clear_memory(state)
    }
    fn transition(
        &self,
        config: &Value,
        state: &Value,
        message: &AgentMessage,
    ) -> Result<AgentTransition, DomainError> {
        self.transition_supervised(config, state, message, false)
    }
    fn transition_supervised(
        &self,
        config: &Value,
        state: &Value,
        message: &AgentMessage,
        suspend_next: bool,
    ) -> Result<AgentTransition, DomainError> {
        let config: RegisteredAgentConfig = decode(config)?;
        let mut transition = WorkflowWorkerAgent.transition_supervised(
            &config.worker()?,
            state,
            message,
            suspend_next,
        )?;
        for command in &mut transition.commands {
            if let AgentCommand::StartRun { input, .. } | AgentCommand::ValidateRun { input, .. } =
                command
            {
                *input = serde_json::to_value(AgentInvocation {
                    registration: config.registration.clone(),
                    input: input.clone(),
                })
                .map_err(|e| DomainError::InvalidConfiguration(e.to_string()))?;
            }
        }
        Ok(transition)
    }
    fn cancel_requests(
        &self,
        config: &Value,
        state: &Value,
        requests: &[AgentMessage],
        reply: Option<&Value>,
    ) -> Result<Value, DomainError> {
        let config: RegisteredAgentConfig = decode(config)?;
        WorkflowWorkerAgent.cancel_requests(&config.worker()?, state, requests, reply)
    }
}
