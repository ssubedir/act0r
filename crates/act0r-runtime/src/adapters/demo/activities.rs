use crate::{application::ports::*, domain::*};
use serde_json::json;
use std::time::Duration;

pub struct DemoExecutor;
impl ActivityExecutor for DemoExecutor {
    fn supports(&self, id: &DefinitionId) -> bool {
        id.version == 1
            && matches!(
                id.name.as_str(),
                "prepare_input" | "mock_decision" | "echo_tool"
            )
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            let delay = execution.input["delay_ms"].as_u64().unwrap_or(0);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            let result = match execution.handler.name.as_str() {
                "prepare_input" => {
                    json!({"message": execution.input["message"].as_str().unwrap_or("").trim(), "delay_ms": delay})
                }
                "mock_decision" => {
                    json!({"tool": "echo", "arguments": {"text": execution.input["message"]}, "delay_ms": delay})
                }
                "echo_tool" => {
                    json!({"text": execution.input["arguments"]["text"], "tool": "echo"})
                }
                _ => {
                    return Ok(Outcome::Failure(Failure::new(
                        "unknown_handler",
                        "unknown demo handler",
                        false,
                    )));
                }
            };
            Ok(Outcome::Success(result))
        })
    }
}
