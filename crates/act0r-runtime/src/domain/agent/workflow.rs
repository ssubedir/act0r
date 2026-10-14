use super::*;
use crate::domain::*;
use serde_json::{Value, json};

pub struct AgentWorkflow;
fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, DomainError> {
    serde_json::from_value(value.clone()).map_err(|e| DomainError::InvalidWorkflow(e.to_string()))
}
fn encode<T: serde::Serialize>(value: &T) -> Result<Value, DomainError> {
    serde_json::to_value(value).map_err(|e| DomainError::InvalidWorkflow(e.to_string()))
}
impl WorkflowDefinition for AgentWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("agent", 2)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        transition(state, event)
    }
}

fn transition(state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
    if let WorkflowEvent::Started(input) = event {
        let config: AgentConfig = decode(input)?;
        config.validate()?;
        if config.workspace.is_empty() {
            return Err(DomainError::InvalidConfiguration(
                "workspace required".into(),
            ));
        }
        let mut state = AgentState {
            config,
            turn: 0,
            observations: vec![],
            pending: None,
            tests_passed: false,
            context: ContextState::default(),
        };
        return Ok(Transition {
            commands: vec![
                Command::ConfigureUsage(UsageBudget {
                    max_model_calls: state.config.max_model_calls,
                    max_tokens: state.config.max_tokens,
                }),
                model(&mut state)?,
            ],
            state: encode(&state)?,
        });
    }
    let mut state: AgentState = decode(state)?;

    let command = match event {
        WorkflowEvent::ActivityCompleted {
            handler, result, ..
        } if handler == &DefinitionId::new("agent.model", 1) => {
            let decision: ModelDecision = decode(result)?;
            state.turn += 1;
            match decision.action {
                AgentAction::CallTool { .. } => {
                    return Err(DomainError::InvalidWorkflow(
                        "coding actors do not dispatch native registry tools".into(),
                    ));
                }
                AgentAction::Finish { summary } if state.tests_passed => Command::Complete(
                    json!({"summary": summary, "workspace": state.config.workspace, "tests_passed": true, "model_turns": state.turn}),
                ),
                action @ AgentAction::Finish { .. } => {
                    observe(
                        &mut state,
                        action,
                        json!({"error": "run the fixed tests successfully after the final edit before finishing"}),
                    )?;
                    model(&mut state)?
                }
                action @ AgentAction::AskHuman { .. } => {
                    let AgentAction::AskHuman { question } = &action else {
                        return Err(DomainError::InvalidState);
                    };
                    let command = Command::WaitForInput {
                        key: "agent_answer".into(),
                        prompt: question.clone(),
                        payload: encode(&action)?,
                    };
                    state.pending = Some(action);
                    command
                }
                action if state.config.require_approval && action.needs_approval() => {
                    let command = Command::WaitForInput {
                        key: "tool_approval".into(),
                        prompt: "Approve this workspace operation?".into(),
                        payload: json!({"action": action, "workspace": state.config.workspace, "test_command": state.config.test_command}),
                    };
                    state.pending = Some(action);
                    command
                }
                action => {
                    state.pending = Some(action.clone());
                    tool(&state, action)?
                }
            }
        }
        WorkflowEvent::ActivityCompleted {
            handler, result, ..
        } if handler == &DefinitionId::new("agent.tool", 1) => {
            let action = state.pending.take().ok_or(DomainError::InvalidState)?;
            match &action {
                AgentAction::ReplaceText { .. } => state.tests_passed = false,
                AgentAction::RunTests => {
                    state.tests_passed = result["ok"] == true && result["exit_code"] == 0
                }
                _ => {}
            }
            observe(&mut state, action, result.clone())?;
            model(&mut state)?
        }
        WorkflowEvent::InputReceived { key, payload, .. } => {
            let action = state.pending.clone().ok_or(DomainError::InvalidState)?;
            if key == "tool_approval" {
                let approved = payload["approved"].as_bool().ok_or_else(|| {
                    DomainError::InvalidConfiguration(
                        "approval input needs an approved boolean".into(),
                    )
                })?;
                if approved {
                    tool(&state, action)?
                } else {
                    state.pending = None;
                    observe(&mut state, action, json!({"approved": false}))?;
                    model(&mut state)?
                }
            } else if key == "agent_answer" {
                state.pending = None;
                observe(&mut state, action, payload.clone())?;
                model(&mut state)?
            } else {
                return Err(DomainError::InvalidWorkflow(
                    "unexpected agent input key".into(),
                ));
            }
        }
        _ => {
            return Err(DomainError::InvalidWorkflow(
                "unexpected agent event".into(),
            ));
        }
    };
    Ok(Transition {
        state: encode(&state)?,
        commands: vec![command],
    })
}
pub(crate) fn observe(
    state: &mut AgentState,
    action: AgentAction,
    result: Value,
) -> Result<(), DomainError> {
    state.context.total_observations = state
        .context
        .total_observations
        .checked_add(1)
        .ok_or(DomainError::InvalidState)?;

    state.observations.push(Observation { action, result });
    Ok(())
}
pub(crate) fn model(state: &mut AgentState) -> Result<Command, DomainError> {
    let request = ModelRequest {
        target: state.config.target.clone(),
        native_tool_names: vec![],
        native_tool_flattened: false,
        prompt: String::new(),
        instructions: crate::domain::session_summary::native_instructions(
            context::bounded_instructions(),
            state.config.actor_memory.as_ref(),
            state.config.session_summary.as_ref(),
        ),
        schema: action_schema(),
        max_output_tokens: state.config.max_output_tokens,
        turn: state.turn,
        script: state.config.script.clone(),
        scripted_delay_ms: state.config.scripted_delay_ms,
        context: None,
    };
    let request = context::select(state, request)?;
    Ok(Command::Schedule(ActivitySpec {
        handler: DefinitionId::new("agent.model", 1),
        input: encode(&request)?,
        retry: request.target.policy.retry(),
        recovery: RecoveryPolicy::SafeToRepeat,
        usage: Some(request.reservation()),
    }))
}
fn tool(state: &AgentState, action: AgentAction) -> Result<Command, DomainError> {
    let recovery = if matches!(action, AgentAction::RunTests) && !state.config.repeat_safe_tests {
        RecoveryPolicy::RequiresReconciliation
    } else {
        RecoveryPolicy::SafeToRepeat
    };
    let request = ToolRequest {
        workspace: state.config.workspace.clone(),
        action,
        test_command: state.config.test_command.clone(),
        writable_paths: state.config.writable_paths.clone(),
    };
    Ok(Command::Schedule(ActivitySpec {
        handler: DefinitionId::new("agent.tool", 1),
        input: encode(&request)?,
        retry: RetryPolicy::default(),
        recovery,
        usage: None,
    }))
}
