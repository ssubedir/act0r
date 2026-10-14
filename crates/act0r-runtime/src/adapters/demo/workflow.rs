use crate::domain::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DemoInput {
    pub message: String,
    #[serde(default)]
    pub delay_ms: u32,
}
pub struct DemoWorkflow;
impl WorkflowDefinition for DemoWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("demo", 1)
    }
    fn transition(&self, _state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let (stage, command) = match event {
            WorkflowEvent::Started(input) => {
                let input: DemoInput = serde_json::from_value(input.clone())
                    .map_err(|e| DomainError::InvalidConfiguration(e.to_string()))?;
                (
                    "preparing",
                    schedule(
                        "prepare_input",
                        serde_json::to_value(input)
                            .map_err(|e| DomainError::InvalidConfiguration(e.to_string()))?,
                    ),
                )
            }
            WorkflowEvent::ActivityCompleted {
                handler, result, ..
            } => match handler.name.as_str() {
                "prepare_input" => ("deciding", schedule("mock_decision", result.clone())),
                "mock_decision" => ("executing_tool", schedule("echo_tool", result.clone())),
                "echo_tool" => ("complete", Command::Complete(result.clone())),
                _ => {
                    return Err(DomainError::InvalidWorkflow(
                        "unexpected demo handler".into(),
                    ));
                }
            },
            WorkflowEvent::TimerFired { .. }
            | WorkflowEvent::InputReceived { .. }
            | WorkflowEvent::TaskCompleted { .. }
            | WorkflowEvent::TaskGroupCompleted { .. }
            | WorkflowEvent::PlannerQuestionClosed { .. }
            | WorkflowEvent::CompletionRejected { .. } => {
                return Err(DomainError::InvalidWorkflow(
                    "unexpected timer in activity demo".into(),
                ));
            }
        };
        Ok(Transition {
            state: json!({"stage": stage}),
            commands: vec![command],
        })
    }
}
fn schedule(name: &str, input: Value) -> Command {
    Command::Schedule(ActivitySpec {
        handler: DefinitionId::new(name, 1),
        input,
        retry: RetryPolicy::default(),
        recovery: RecoveryPolicy::SafeToRepeat,
        usage: None,
    })
}
