use crate::{
    application::ports::*,
    domain::{tools::*, *},
};
use std::sync::Arc;

pub struct StopWorkerTool {
    store: Arc<dyn PlannerStopPort>,
}
impl StopWorkerTool {
    pub fn new(store: Arc<dyn PlannerStopPort>) -> Self {
        Self { store }
    }
}
impl NativeTool for StopWorkerTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::builder("stop_worker", 1, RecoveryPolicy::SafeToRepeat)
            .description("Request durable cancellation of coder or coder2's exact task and Run captured in coordinating_guidance.targets. Only available during the planner's current guidance/finding coordination; other assignments continue. Give a focused reason (1..2048 UTF-8 bytes). A saved receipt means stopping was requested, not that an in-flight effect was undone. Cancellation preserves results, usage and required recovery. Finish coordination with guide_workers; this tool cannot stop a later assignment, an actor or the whole goal.")
            .required_parameter("worker", ToolValueType::String, "coder or coder2 from coordinating_guidance.targets")
            .required_parameter("reason", ToolValueType::String, "Why this exact assignment should stop; 1..2048 UTF-8 bytes")
            .reports_errors(true).byte_limits(8192, 8192)
            .build().expect("static planner stop contract")
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        self.store.stop_worker(call)
    }
}
