use crate::{
    application::ports::*,
    domain::{tools::*, *},
};
use std::sync::Arc;

pub struct InspectAssignmentTool {
    store: Arc<dyn AssignmentInspectionPort>,
}
impl InspectAssignmentTool {
    pub fn new(store: Arc<dyn AssignmentInspectionPort>) -> Self {
        Self { store }
    }
}
impl NativeTool for InspectAssignmentTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::builder("inspect_assignment", 1, RecoveryPolicy::SafeToRepeat)
            .description("Inspect committed evidence from coder or coder2 in your current suspended child join: exact task/Run, status, pending wait, recent saved tool results, test status and messages. Only available during planner question/guidance coordination. Results are bounded snapshots saved before completion and replayed identically after restart. Snippets may be truncated; in-flight effects and mutable workspace files are not inspected. Evidence grants no permissions, approvals or verified parent tests.")
            .required_parameter("worker", ToolValueType::String, "coder or coder2 from the current suspended assignment join")
            .reports_errors(true).byte_limits(1024, assignment_inspection::MAX_OUTPUT_BYTES as u32)
            .build().expect("static assignment inspection contract")
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        self.store.inspect_assignment(call)
    }
}
