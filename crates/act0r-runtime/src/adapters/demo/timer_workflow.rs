use crate::domain::*;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TimerInput {
    message: String,
    wait_ms: u64,
}

pub struct TimerDemoWorkflow;
impl WorkflowDefinition for TimerDemoWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("timer_demo", 1)
    }
    fn transition(&self, _state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let (stage, command) = match event {
            WorkflowEvent::Started(input) => {
                let input: TimerInput = serde_json::from_value(input.clone())
                    .map_err(|e| DomainError::InvalidConfiguration(e.to_string()))?;
                (
                    "waiting",
                    Command::StartTimer(TimerSpec::after(
                        input.wait_ms,
                        json!({"message": input.message}),
                    )),
                )
            }
            WorkflowEvent::TimerFired { payload, .. } => (
                "executing_tool",
                Command::Schedule(ActivitySpec {
                    handler: DefinitionId::new("echo_tool", 1),
                    input: json!({"arguments": {"text": payload["message"]}}),
                    retry: RetryPolicy::default(),
                    recovery: RecoveryPolicy::SafeToRepeat,
                    usage: None,
                }),
            ),
            WorkflowEvent::ActivityCompleted {
                handler, result, ..
            } if handler == &DefinitionId::new("echo_tool", 1) => {
                ("complete", Command::Complete(result.clone()))
            }
            _ => {
                return Err(DomainError::InvalidWorkflow(
                    "unexpected timer demo event".into(),
                ));
            }
        };
        Ok(Transition {
            state: json!({"stage": stage}),
            commands: vec![command],
        })
    }
}
