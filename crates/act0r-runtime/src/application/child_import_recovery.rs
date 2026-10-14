//! Reconcile an approved child import inside its owning planner Run.
use super::{Engine, Error, ports::*};
use crate::domain::{child_changes::Inspection, patch_application::ApplicationInspection, *};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct ChildImportRecoveryView {
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub request: Inspection,
    pub diagnostic: Option<Outcome>,
    pub interrupted_at: Option<i64>,
    pub paused: bool,
    pub cancelled: bool,
    pub retry_allowed: bool,
    pub inspection: Option<ApplicationInspection>,
    pub inspection_error: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct ChildImportRecoveryItem {
    pub task_id: MessageId,
    pub run_id: RunId,
    pub status: RunStatus,
    pub recovery: Option<ChildImportRecoveryView>,
    pub reconciliation_receipts: Vec<ReconciliationReceipt>,
}
#[derive(Debug, Serialize)]
pub struct ChildImportRecoveries {
    pub items: Vec<ChildImportRecoveryItem>,
    pub truncated: bool,
}
pub struct ChildImportRecoveryService {
    engine: Engine,
    inspector: Arc<dyn ChildImportInspectionPort>,
}
impl ChildImportRecoveryService {
    pub fn new(engine: Engine, inspector: Arc<dyn ChildImportInspectionPort>) -> Self {
        Self { engine, inspector }
    }
    fn request(run: &Run, activity: &Activity) -> Result<Inspection, Error> {
        if ![
            child_changes::handler("import"),
            worker_changes::handler("import"),
        ]
        .contains(&activity.spec.handler)
            || activity.spec.recovery != RecoveryPolicy::RequiresReconciliation
        {
            return Err(DomainError::InvalidReconciliation(
                "decision must identify a saved planner child import".into(),
            )
            .into());
        }
        let request: Inspection = serde_json::from_value(activity.spec.input.clone())?;
        request.validate()?;
        let input = if run.record().workflow == planner::PlannerWorkflow::definition_id() {
            run.record().input.clone()
        } else {
            let invocation: integration::AgentInvocation =
                serde_json::from_value(run.record().input.clone())?;
            invocation.registration.validate()?;
            if invocation.registration.adapter != DefinitionId::new("planner", 3)
                || invocation.registration.definition != planner::PlannerWorkflow::definition_id()
                || invocation.registration.workflow_id() != run.record().workflow
            {
                return Err(DomainError::InvalidReconciliation(
                    "import must belong to its pinned native planner".into(),
                )
                .into());
            }
            invocation.input
        };
        let config: planner::PlannerConfig = serde_json::from_value(input)?;
        if activity.spec.handler == worker_changes::handler("import") {
            let d = request
                .request
                .dependency
                .as_ref()
                .ok_or(DomainError::InvalidState)?;
            if !config.worker_changes
                || d.planner_run_id != run.record().id
                || request.preview.run_id != run.record().id
                || !config
                    .workers
                    .iter()
                    .any(|w| w.name == d.prerequisite && w.agent_id == request.request.recipient)
                // Keep exact reconciliation receipts usable after this guidance batch
                // closes. The fired timer retains its original immutable capture.
                || !run.timers().iter().any(|t| {
                    Some(t.id) == d.review_timer_id
                        && t.status == TimerStatus::Fired
                        && t.payload["guidance"]["dependencies"]
                            .as_array()
                            .is_some_and(|items| items.iter().any(|p| p == &serde_json::json!(d)))
                })
            {
                return Err(DomainError::InvalidState.into());
            }
            return Ok(request);
        }
        if request.request.dependency.is_some()
            || !config.child_changes
            || !config.change_review
            || config.workspace.as_deref() != Some(&request.request.target.workspace)
            || config.writable_paths != request.request.target.writable_paths
            || request.preview.run_id != run.record().id
            || !config.workers.iter().any(|worker| {
                worker.name == request.request.worker
                    && worker.agent_id == request.request.recipient
            })
        {
            return Err(DomainError::InvalidReconciliation(
                "import differs from the planner's pinned permissions and child identity".into(),
            )
            .into());
        }
        Ok(request)
    }
    pub async fn status(
        &self,
        task: MessageId,
        run: Run,
    ) -> Result<ChildImportRecoveryItem, Error> {
        let mut recovery = None;
        if run.record().status == RunStatus::Blocked
            && !run.record().controls.cancel_requested
            && run
                .record()
                .error
                .as_ref()
                .is_some_and(|error| error.code == "uncertain_outcome")
            && let Some(activity) = run.activities().iter().find(|activity| {
                activity.status == ActivityStatus::Indeterminate
                    && activity.active_attempt.is_none()
                    && [
                        child_changes::handler("import"),
                        worker_changes::handler("import"),
                    ]
                    .contains(&activity.spec.handler)
            })
            && let Some(attempt) = run.attempts().iter().find(|attempt| {
                attempt.activity_id == activity.id
                    && attempt.ordinal == activity.attempts
                    && attempt.status == AttemptStatus::Interrupted
                    && matches!(attempt.outcome, Some(Outcome::Uncertain(_)))
            })
        {
            let request = Self::request(&run, activity)?;
            let (inspection, inspection_error) = match self
                .inspector
                .inspect_import(run.record().id, activity.id, request.clone())
                .await
            {
                Ok(view) => (Some(view), None),
                Err(error) => (None, Some(error.to_string())),
            };
            recovery = Some(ChildImportRecoveryView {
                activity_id: activity.id,
                attempt_id: attempt.id,
                request,
                diagnostic: attempt.outcome.clone(),
                interrupted_at: attempt.ended_at,
                paused: run.record().controls.pause_requested
                    || run.record().controls.boundary_pause,
                cancelled: false,
                retry_allowed: run.record().allowance.admitted < run.record().allowance.limit
                    && activity.attempts < activity.spec.retry.max_attempts
                    && inspection.as_ref().is_some_and(|view| {
                        view.report.is_some()
                            || view.files.iter().all(|file| file.file.conflict.is_none())
                    }),
                inspection,
                inspection_error,
            });
        }
        let reconciliation_receipts = run
            .record()
            .reconciliation_receipts
            .iter()
            .filter(|receipt| {
                run.activities().iter().any(|activity| {
                    activity.id == receipt.request.activity_id
                        && Self::request(&run, activity).is_ok()
                })
            })
            .cloned()
            .collect();
        Ok(ChildImportRecoveryItem {
            task_id: task,
            run_id: run.record().id,
            status: run.record().status,
            recovery,
            reconciliation_receipts,
        })
    }
    pub async fn reconcile(
        &self,
        run: Run,
        request: tool_recovery::ToolRecoveryRequest,
    ) -> Result<(), Error> {
        request.validate()?;
        let activity = run
            .activities()
            .iter()
            .find(|activity| activity.id == request.activity_id)
            .ok_or(DomainError::UnknownAttempt)?;
        Self::request(&run, activity)?;
        if !run.attempts().iter().any(|attempt| {
            attempt.id == request.attempt_id && attempt.activity_id == request.activity_id
        }) {
            return Err(DomainError::UnknownAttempt.into());
        }
        // The engine atomically validates the current attempt, controls and exact receipt replay.
        // Execution rechecks the journal and original hashes before resuming any writes.
        self.engine.reconcile(run.record().id, request.into()).await
    }
}
