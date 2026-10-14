//! Read-only links between durable findings, planner evidence and exact worker deliveries.
use crate::domain::{
    assignment_inspection::InspectionReceipt, planner_guidance::*,
    planner_pause::PlannerPauseReceipt, planner_stop::PlannerStopReceipt, worker_messages::*, *,
};
use serde::Serialize;

pub const MAX_COORDINATION_BATCHES: usize = 32;
pub const MAX_COORDINATION_FINDINGS: usize = 64;

#[derive(Clone, Debug, Serialize)]
pub struct PlannerCoordination {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub run_id: RunId,
    pub execution_held: bool,
    pub findings: Vec<CoordinationFinding>,
    pub dependencies: Vec<planner_pause::WorkerDependency>,
    pub transfers: Vec<WorkerTransferSummary>,
    pub batches: Vec<CoordinationBatch>,
    pub truncated: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct WorkerTransferSummary {
    pub activity_id: ActivityId,
    pub dependency: planner_pause::WorkerDependency,
    pub phase: &'static str,
    pub status: String,
    pub artifact_id: Option<ActivityId>,
    pub files: Vec<patch_application::ApplicationFile>,
    pub file_count: usize,
    pub error: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct CoordinationFinding {
    pub receipt: WorkerMessageReceipt,
    pub text_truncated: bool,
    pub batch_id: Option<TimerId>,
    pub status: &'static str,
}
#[derive(Clone, Debug, Serialize)]
pub struct CoordinationBatch {
    pub timer_id: TimerId,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub status: &'static str,
    pub guidance: PlannerGuidance,
    pub inspections: Vec<CoordinationInspection>,
    pub stops: Vec<CoordinationStop>,
    pub pauses: Vec<CoordinationPause>,
    pub decision: Option<CoordinationDecision>,
}
#[derive(Clone, Debug, Serialize)]
pub struct CoordinationDecision {
    pub activity_id: ActivityId,
    pub activity_status: ActivityStatus,
    pub saved_at: i64,
    pub request: GuideWorkers,
    pub text_truncated: bool,
    pub deliveries: Vec<WorkerMessageItem>,
}
#[derive(Clone, Debug, Serialize)]
pub struct CoordinationInspection {
    pub receipt: InspectionReceipt,
    pub activity_status: ActivityStatus,
}
#[derive(Clone, Debug, Serialize)]
pub struct CoordinationStop {
    pub receipt: PlannerStopReceipt,
    pub activity_status: ActivityStatus,
    pub text_truncated: bool,
    pub current_status: Option<actors::AgentTaskStatus>,
    pub settled: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct CoordinationPause {
    pub receipt: PlannerPauseReceipt,
    pub activity_status: ActivityStatus,
    pub text_truncated: bool,
    pub current_status: Option<actors::AgentTaskStatus>,
    pub current_revision: Option<u64>,
    pub planner_paused: bool,
    pub manual_paused: bool,
    pub inherited_held: bool,
    pub execution_held: bool,
    pub admitted_operations: u64,
    pub settled: bool,
}
