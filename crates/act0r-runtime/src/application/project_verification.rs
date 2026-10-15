use super::{Engine, Error, ports::ProjectVerificationInspectionPort};
use crate::domain::{project_verification::*, *};
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;
#[derive(Debug, Serialize)]
pub struct VerificationRecovery {
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub interrupted_at: Option<i64>,
    pub diagnostic: Option<Outcome>,
    pub request: VerificationRequest,
    pub paused: bool,
    pub retry_allowed: bool,
    pub inspection: Option<VerificationInspection>,
    pub inspection_error: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct ProjectVerificationView {
    pub run_id: RunId,
    pub application_id: RunId,
    pub task_id: MessageId,
    pub status: RunStatus,
    pub input: VerificationInput,
    pub snapshot: Option<ProjectSnapshot>,
    pub report: Option<VerificationReport>,
    pub wait: Option<HumanWait>,
    pub error: Option<Failure>,
    pub recovery: Option<VerificationRecovery>,
    pub reconciliation_receipts: Vec<ReconciliationReceipt>,
}
pub struct ProjectVerificationService {
    engine: Engine,
    inspector: Arc<dyn ProjectVerificationInspectionPort>,
}
impl ProjectVerificationService {
    pub(crate) async fn inspect_refresh(
        &self,
        source: String,
        workspace: String,
    ) -> Result<(project_refresh::RefreshSnapshot, Vec<task::SnapshotFile>), Error> {
        self.inspector.inspect_refresh(source, workspace).await
    }
    pub(crate) async fn lock_refresh(
        &self,
        snapshot: project_refresh::RefreshSnapshot,
    ) -> Result<Box<dyn super::ports::OwnershipGuard>, Error> {
        self.inspector.lock_refresh(snapshot).await
    }
    pub(crate) async fn lock_repair_copies(
        &self,
        team: &teams::TeamRecord,
        digest: &str,
    ) -> Result<Box<dyn super::ports::OwnershipGuard>, Error> {
        let copies = team
            .members
            .iter()
            .filter(|m| m.needs_workspace())
            .map(|m| {
                m.workspace
                    .as_ref()
                    .map(|w| w.path.clone())
                    .ok_or(DomainError::InvalidState)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.inspector
            .lock_repair_copies(copies, digest.into())
            .await
    }
    pub(crate) async fn require_current_project(&self, id: RunId) -> Result<(), Error> {
        let run = self.engine.status(id).await?;
        if run.record().workflow != workflow() || run.record().status != RunStatus::Succeeded {
            return Err(DomainError::InvalidState.into());
        }
        let request = VerificationRequest {
            input: serde_json::from_value(run.record().input.clone())?,
            snapshot: serde_json::from_value(run.record().state["snapshot"].clone())?,
        };
        let activity = run
            .activities()
            .iter()
            .find(|a| a.spec.handler == handler("run"))
            .ok_or(DomainError::InvalidState)?;
        let observed = self.inspector.inspect(activity.id, request).await?;
        if !observed.project_matches {
            return Err(DomainError::SessionConflict("the original project changed since this verification; cancel any saved repair preparation and verify the current project before repairing".into()).into());
        }
        Ok(())
    }
    pub fn new(engine: Engine, inspector: Arc<dyn ProjectVerificationInspectionPort>) -> Self {
        Self { engine, inspector }
    }
    pub(crate) fn prepare(&self, id: RunId, input: VerificationInput) -> Result<Run, Error> {
        Ok(Run::start_with_id(
            id,
            self.engine
                .definitions
                .get(&workflow())
                .ok_or_else(|| {
                    Error::Unavailable("project verification workflow is not installed".into())
                })?
                .as_ref(),
            json!(input),
            20,
            self.engine.clock.now_ms(),
        )?)
    }
    pub async fn status(&self, run: Run) -> Result<ProjectVerificationView, Error> {
        if run.record().workflow != workflow() {
            return Err(Error::NotFound(run.record().id));
        }
        let input: VerificationInput = serde_json::from_value(run.record().input.clone())?;
        input.validate()?;
        let snapshot = run
            .record()
            .state
            .get("snapshot")
            .filter(|s| !s.is_null())
            .map(|s| serde_json::from_value::<ProjectSnapshot>(s.clone()))
            .transpose()?;
        let report = run
            .record()
            .result
            .clone()
            .map(serde_json::from_value::<VerificationReport>)
            .transpose()?;
        if let (Some(report), Some(snapshot)) = (&report, &snapshot) {
            report.validate_for(&VerificationRequest {
                input: input.clone(),
                snapshot: snapshot.clone(),
            })?;
        }
        let mut recovery = None;
        if run.record().status == RunStatus::Blocked
            && !run.record().controls.cancel_requested
            && run
                .record()
                .error
                .as_ref()
                .is_some_and(|f| f.code == "uncertain_outcome")
            && let Some(activity) = run.activities().iter().find(|a| {
                a.spec.handler == handler("run")
                    && a.status == ActivityStatus::Indeterminate
                    && a.active_attempt.is_none()
            })
            && let Some(attempt) = run.attempts().iter().find(|a| {
                a.activity_id == activity.id
                    && a.ordinal == activity.attempts
                    && a.status == AttemptStatus::Interrupted
                    && matches!(a.outcome, Some(Outcome::Uncertain(_)))
            })
        {
            let request: VerificationRequest = serde_json::from_value(activity.spec.input.clone())?;
            let observed = self.inspector.inspect(activity.id, request.clone()).await;
            let (inspection, inspection_error) = match observed {
                Ok(v) => (Some(v), None),
                Err(e) => (None, Some(e.to_string())),
            };
            recovery = Some(VerificationRecovery {
                activity_id: activity.id,
                attempt_id: attempt.id,
                interrupted_at: attempt.ended_at,
                diagnostic: attempt.outcome.clone(),
                request,
                paused: run.record().controls.pause_requested
                    || run.record().controls.boundary_pause,
                retry_allowed: run.record().allowance.admitted < run.record().allowance.limit
                    && activity.attempts < activity.spec.retry.max_attempts
                    && inspection
                        .as_ref()
                        .is_some_and(|v| v.saved_report || v.project_matches),
                inspection,
                inspection_error,
            });
        }
        Ok(ProjectVerificationView {
            run_id: run.record().id,
            application_id: input.application_id,
            task_id: input.task_id,
            status: run.record().status,
            input,
            snapshot,
            report,
            wait: run.record().wait.clone(),
            error: run.record().error.clone(),
            recovery,
            reconciliation_receipts: run.record().reconciliation_receipts.clone(),
        })
    }
    pub async fn respond(&self, run: Run, wait: WaitId, approved: bool) -> Result<(), Error> {
        if run.record().workflow != workflow() {
            return Err(Error::NotFound(run.record().id));
        }
        self.engine
            .signal(run.record().id, wait, json!({"approved":approved}))
            .await
    }
    pub async fn reconcile(
        &self,
        run: Run,
        request: tool_recovery::ToolRecoveryRequest,
    ) -> Result<(), Error> {
        request.validate()?;
        if matches!(
            request.decision,
            tool_recovery::ToolRecoveryDecision::Accept { .. }
        ) {
            return Err(DomainError::InvalidReconciliation("project verification requires its own saved completion receipt; accept is only supported for MCP actions".into()).into());
        }
        if run.record().workflow != workflow()
            || !run
                .activities()
                .iter()
                .any(|a| a.id == request.activity_id && a.spec.handler == handler("run"))
        {
            return Err(DomainError::InvalidReconciliation(
                "recovery must identify this verification's saved process attempt".into(),
            )
            .into());
        }
        let saved = run
            .record()
            .reconciliation_receipts
            .iter()
            .any(|r| r.request == request.clone().into());
        if !saved
            && matches!(
                request.decision,
                tool_recovery::ToolRecoveryDecision::Retry {}
            )
            && self
                .status(run.clone())
                .await?
                .recovery
                .is_none_or(|r| !r.retry_allowed)
        {
            return Err(DomainError::InvalidReconciliation(
                "verify the saved project snapshot or completion receipt before retrying".into(),
            )
            .into());
        }
        self.engine.reconcile(run.record().id, request.into()).await
    }
}
