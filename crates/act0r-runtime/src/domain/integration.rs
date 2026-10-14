//! Agent integration metadata and durable invocation boundaries.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointGranularity {
    TaskCompletion,
    ModelDecision,
    ModelAndTool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageReporting {
    Unavailable,
    ReportedOrReserved,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCapabilities {
    pub checkpoints: CheckpointGranularity,
    pub cooperative_cancellation: bool,
    pub human_input_waits: bool,
    pub usage: UsageReporting,
    pub branching: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub delegation: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub task_groups: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRegistration {
    pub contract_version: u32,
    pub adapter: DefinitionId,
    /// The implementation inside the registered workflow wrapper.
    pub definition: DefinitionId,
    pub capabilities: AgentCapabilities,
}
impl AgentRegistration {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.contract_version != 1
            || self.adapter.name.trim().is_empty()
            || self.adapter.name.len() > 128
            || self.adapter.version == 0
            || self.definition.name.trim().is_empty()
            || self.definition.version == 0
        {
            return Err(DomainError::InvalidConfiguration("agent registration requires contract version 1 and versioned adapter/workflow identities".into()));
        }
        Ok(())
    }
    pub fn workflow_id(&self) -> DefinitionId {
        DefinitionId::new(
            format!("registered_agent.{}", self.adapter.name),
            self.adapter.version,
        )
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInvocation {
    pub registration: AgentRegistration,
    pub input: Value,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisteredState {
    registration: AgentRegistration,
    state: Value,
}

pub struct RegisteredAgentWorkflow {
    registration: AgentRegistration,
    definition: Arc<dyn WorkflowDefinition>,
}
impl RegisteredAgentWorkflow {
    pub fn new(
        registration: AgentRegistration,
        definition: Arc<dyn WorkflowDefinition>,
    ) -> Result<Self, DomainError> {
        registration.validate()?;
        if registration.definition != definition.id() {
            return Err(DomainError::InvalidConfiguration(
                "agent registration does not match its workflow implementation".into(),
            ));
        }
        Ok(Self {
            registration,
            definition,
        })
    }
    fn check(&self, registration: &AgentRegistration) -> Result<(), DomainError> {
        if registration != &self.registration {
            return Err(DomainError::InvalidWorkflow("pinned agent registration differs; restore its adapter and capabilities or register a new version".into()));
        }
        Ok(())
    }
}
impl WorkflowDefinition for RegisteredAgentWorkflow {
    fn id(&self) -> DefinitionId {
        self.registration.workflow_id()
    }
    fn prepare_input(&self, input: Value) -> Result<Value, DomainError> {
        let mut invocation: AgentInvocation = serde_json::from_value(input)
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        self.check(&invocation.registration)?;
        if let Some(value) = invocation.input.get("actor_memory") {
            let mut memory: actors::MemorySnapshot = serde_json::from_value(value.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            memory.validate()?;
            loop {
                invocation.input["actor_memory"] = serde_json::to_value(&memory)
                    .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
                match self.definition.transition(
                    &Value::Null,
                    &WorkflowEvent::Started(invocation.input.clone()),
                ) {
                    Ok(_) => break,
                    Err(DomainError::ContextLimit { .. }) if memory.omit_oldest()? => {}
                    Err(error) => return Err(error),
                }
            }
        }
        serde_json::to_value(invocation).map_err(|e| DomainError::InvalidWorkflow(e.to_string()))
    }
    fn validate_state(&self, state: &Value) -> Result<(), DomainError> {
        let state: RegisteredState = serde_json::from_value(state.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        self.check(&state.registration)?;
        self.definition.validate_state(&state.state)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let transition = if let WorkflowEvent::Started(input) = event {
            let invocation: AgentInvocation = serde_json::from_value(input.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            self.check(&invocation.registration)?;
            self.definition
                .transition(&Value::Null, &WorkflowEvent::Started(invocation.input))?
        } else {
            let state: RegisteredState = serde_json::from_value(state.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            self.check(&state.registration)?;
            self.definition.transition(&state.state, event)?
        };
        if !self.registration.capabilities.human_input_waits
            && transition
                .commands
                .iter()
                .any(|c| matches!(c, Command::WaitForInput { .. }))
        {
            return Err(DomainError::InvalidWorkflow(
                "agent emitted a human-input wait without declaring that capability".into(),
            ));
        }
        if !self.registration.capabilities.delegation
            && transition.commands.iter().any(|c| {
                matches!(
                    c,
                    Command::DelegateTask { .. }
                        | Command::DelegatePreparedTask(_)
                        | Command::DelegatePreparedGroup(_)
                        | Command::DelegateTasks { .. }
                        | Command::ConfigureDelegation(_)
                )
            })
        {
            return Err(DomainError::InvalidWorkflow(
                "agent emitted delegation without declaring that capability".into(),
            ));
        }
        if !self.registration.capabilities.task_groups
            && transition.commands.iter().any(|c| {
                matches!(
                    c,
                    Command::DelegateTasks { .. } | Command::DelegatePreparedGroup(_)
                )
            })
        {
            return Err(DomainError::InvalidWorkflow(
                "agent emitted a task group without declaring that capability".into(),
            ));
        }
        Ok(Transition {
            state: serde_json::to_value(RegisteredState {
                registration: self.registration.clone(),
                state: transition.state,
            })
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?,
            commands: transition.commands,
        })
    }
}
