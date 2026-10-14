use crate::{
    application::ports::*,
    domain::{tools::*, *},
};
use std::sync::Arc;

pub struct WorkerPauseTool {
    store: Arc<dyn PlannerPausePort>,
    paused: bool,
}
impl WorkerPauseTool {
    pub fn new(store: Arc<dyn PlannerPausePort>, paused: bool) -> Self {
        Self { store, paused }
    }
}
impl NativeTool for WorkerPauseTool {
    fn definition(&self) -> ToolDefinition {
        let (name, description) = if self.paused {
            (
                "pause_worker",
                "Save a durable planner-owned pause on coder or coder2's exact task and Run captured in coordinating_guidance.targets. Its subtree holds new execution while siblings continue; already admitted effects can settle. Give a focused reason (1..2048 UTF-8 bytes). Original deadlines still apply. Finish coordination with guide_workers; arrange a later finding or human guidance to resume dependent work, without polling.",
            )
        } else {
            (
                "resume_worker",
                "Release only this planner's durable pause on coder or coder2's exact captured task and Run. Manual assignment, ancestor goal and actor/Run pauses remain authoritative, as do recovery and reconciliation blocks. Give a focused reason (1..2048 UTF-8 bytes). This continues the original saved assignment; it cannot restart completed/stopping work or release another owner's hold. Finish coordination with guide_workers.",
            )
        };
        ToolDefinition::builder(name, 1, RecoveryPolicy::SafeToRepeat)
            .description(description)
            .required_parameter(
                "worker",
                ToolValueType::String,
                "coder or coder2 from coordinating_guidance.targets",
            )
            .required_parameter(
                "reason",
                ToolValueType::String,
                "Why to pause or resume this assignment; 1..2048 UTF-8 bytes",
            )
            .reports_errors(true)
            .byte_limits(8192, 8192)
            .build()
            .expect("static planner pause contract")
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        self.store.control_worker_pause(call, self.paused)
    }
}

pub struct WaitForWorkerTool {
    store: Arc<dyn PlannerPausePort>,
}
impl WaitForWorkerTool {
    pub fn new(store: Arc<dyn PlannerPausePort>) -> Self {
        Self { store }
    }
}
impl NativeTool for WaitForWorkerTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::builder("wait_for_worker", 1, RecoveryPolicy::SafeToRepeat)
            .description("Pause one captured coding assignment until another captured sibling settles. Save worker, dependency and reason. The prerequisite's success, failure or stop durably wakes this same planner to inspect its saved outcome and explicitly resume, guide or stop the waiting worker. This never automatically releases a hold, imports code or verifies tests. Self-dependencies and cycles are refused. Shares the pause/resume allowance. Finish with guide_workers.")
            .required_parameter("worker", ToolValueType::String, "coder or coder2 to hold from coordinating_guidance.targets")
            .required_parameter("dependency", ToolValueType::String, "The other captured coder whose completion should wake this planner")
            .required_parameter("reason", ToolValueType::String, "Why the assignment needs its prerequisite; 1..2048 UTF-8 bytes")
            .reports_errors(true).byte_limits(8192, 8192).build().expect("static worker dependency contract")
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        self.store.control_worker_pause(call, true)
    }
}
