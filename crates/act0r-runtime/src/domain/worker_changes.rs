//! Approved immutable sibling patches, bound to the dependency's exact held assignment.
use super::{child_changes::Inspection, planner_pause::WorkerDependency, *};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const MAX_INSPECTIONS: u32 = 3;
pub const MAX_IMPORTS: u32 = 1;
pub fn handler(name: &str) -> DefinitionId {
    DefinitionId::new(format!("planner.worker_changes.{name}"), 1)
}
pub fn is_handler(id: &DefinitionId) -> bool {
    ["inspect", "import"].iter().any(|n| id == &handler(n))
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectWorkerChanges {
    pub dependency: WorkerDependency,
    pub offset: u32,
}
impl InspectWorkerChanges {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.dependency.validate()?;
        if self.dependency.closed
            || self.dependency.outcome != Some(actors::AgentTaskStatus::Succeeded)
            || self.dependency.review_timer_id.is_none()
            || self.offset > 4 * 1024 * 1024
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
}
pub fn activity(name: &str, input: serde_json::Value) -> Command {
    let Command::Schedule(mut spec) = child_changes::activity(name, input) else {
        unreachable!()
    };
    spec.handler = handler(name);
    Command::Schedule(spec)
}
impl Run {
    /// Admission invalidates tests before a possibly partial filesystem write.
    /// Preserve the original baseline so the downstream patch includes its prerequisite.
    pub(crate) fn worker_changes_started(
        &mut self,
        activity: ActivityId,
        inspection: &Inspection,
        now: i64,
    ) -> Result<(), DomainError> {
        let d = inspection
            .request
            .dependency
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        if self.record.id != d.run_id {
            return Err(DomainError::InvalidState);
        }
        let state = if self.record.state["config"].is_object() {
            &mut self.record.state
        } else {
            &mut self.record.state["state"]
        };
        change_review::invalidate_tests(state)?;
        if let Some(status) = state
            .get_mut("context")
            .and_then(|c| c.get_mut("test_status"))
            && status.is_object()
        {
            status["stale"] = json!(true);
        }
        self.record.revision += 1;
        self.event("worker_changes_started", json!({"transfer_activity_id":activity,"dependency_id":d.activity_id,"prerequisite_task_id":d.prerequisite_task_id,"artifact_id":inspection.artifact_id,"tests_invalidated":true}), None, None, now);
        Ok(())
    }
    pub(crate) fn worker_changes_received(
        &mut self,
        activity: ActivityId,
        inspection: &Inspection,
        report: &patch_application::ApplicationReport,
        now: i64,
    ) -> Result<(), DomainError> {
        let d = inspection
            .request
            .dependency
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        if self.record.id != d.run_id {
            return Err(DomainError::InvalidState);
        }
        self.record.revision += 1;
        self.event("worker_changes_received",json!({"transfer_activity_id":activity,"dependency_id":d.activity_id,"prerequisite_task_id":d.prerequisite_task_id,"artifact_id":inspection.artifact_id,"status":report.status,"applied_files":report.applied_files}),None,None,now);
        Ok(())
    }
}
