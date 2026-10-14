//! Application-owned tool using only act0r's public extension API.
use act0r::{
    Error,
    application::ports::{NativeTool, PortFuture},
    domain::{RecoveryPolicy, tools::*},
};
use serde::Deserialize;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct WordCountTool {
    definition: ToolDefinition,
    executions: AtomicUsize,
}
impl WordCountTool {
    pub fn new() -> Result<Self, Error> {
        let definition = ToolDefinition::builder("word_count", 1, RecoveryPolicy::SafeToRepeat)
            .description("Count whitespace-separated words in the supplied UTF-8 text. Empty text has zero words.")
            .required_parameter("text", ToolValueType::String, "Text whose words should be counted.")
            .byte_limits(4096, 256)
            .build()?;
        Ok(Self {
            definition,
            executions: AtomicUsize::new(0),
        })
    }
    // Demo instrumentation; durable progress belongs to the harness, not this counter.
    pub fn executions(&self) -> usize {
        self.executions.load(Ordering::SeqCst)
    }
}
impl NativeTool for WordCountTool {
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input {
                text: String,
            }
            let input: Input = serde_json::from_value(call.arguments)?;
            self.executions.fetch_add(1, Ordering::SeqCst);
            Ok(NativeToolResult::Success(
                json!({"words": input.text.split_whitespace().count()}),
            ))
        })
    }
}
