use super::{Engine, Error, ports::*};
use crate::domain::{patch_application::*, *};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationView {
    pub run_id: RunId,
    pub task_id: MessageId,
    pub status: RunStatus,
    pub preview: Option<ApplicationPreview>,
    pub report: Option<ApplicationReport>,
    pub wait: Option<HumanWait>,
    pub error: Option<Failure>,
    #[serde(default)]
    pub recovery: Option<ApplicationRecoveryView>,
    #[serde(default)]
    pub reconciliation_receipts: Vec<ReconciliationReceipt>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationRecoveryView {
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub interrupted_at: Option<i64>,
    pub diagnostic: Option<Outcome>,
    pub paused: bool,
    pub retry_allowed: bool,
    pub inspection: Option<ApplicationInspection>,
    pub inspection_error: Option<String>,
}
#[derive(Clone)]
pub struct PatchApplicationService {
    engine: Engine,
    changes: Arc<dyn WorkspaceChangesReadPort>,
    inspector: Option<Arc<dyn PatchApplicationInspectionPort>>,
}
impl PatchApplicationService {
    pub fn new(engine: Engine, changes: Arc<dyn WorkspaceChangesReadPort>) -> Self {
        Self {
            engine,
            changes,
            inspector: None,
        }
    }
    pub fn with_inspector(
        mut self,
        inspector: Option<Arc<dyn PatchApplicationInspectionPort>>,
    ) -> Self {
        self.inspector = inspector;
        self
    }
    /// Creates a separate Run; its owned worker prepares a read-only target preview.
    pub async fn preview(&self, task: MessageId, target: PathBuf) -> Result<RunId, Error> {
        let id = RunId::new();
        let run = self.prepare_preview(id, task, target).await?;
        self.engine.store.create(run).await?;
        Ok(id)
    }
    pub(crate) async fn prepare_preview(
        &self,
        id: RunId,
        task: MessageId,
        target: PathBuf,
    ) -> Result<Run, Error> {
        if !target.is_absolute() {
            return Err(DomainError::InvalidConfiguration(
                "application target must be absolute".into(),
            )
            .into());
        }
        let target = target
            .into_os_string()
            .into_string()
            .map_err(|_| Error::Unavailable("application target must be UTF-8".into()))?;
        let saved = self.changes.task_changes(task).await?;
        if saved.run_status != Some(RunStatus::Succeeded) {
            return Err(Error::Unavailable(
                "only a successfully completed task can be applied".into(),
            ));
        }
        let changes = saved
            .changes
            .ok_or_else(|| Error::Unavailable("task has no committed change set".into()))?;
        Ok(Run::start_with_id(
            id,
            self.engine
                .definitions
                .get(&workflow())
                .ok_or_else(|| {
                    Error::Unavailable("patch application workflow is not installed".into())
                })?
                .as_ref(),
            json!(ApplicationInput {
                task_id: task,
                target,
                changes
            }),
            20,
            self.engine.clock.now_ms(),
        )?)
    }
    pub async fn status(&self, id: RunId) -> Result<ApplicationView, Error> {
        let run = self.engine.status(id).await?;
        if run.record().workflow != workflow() {
            return Err(Error::Unavailable("Run is not a patch application".into()));
        }
        let input: ApplicationInput = serde_json::from_value(run.record().input.clone())?;
        let mut recovery = None;
        if run.record().status == RunStatus::Blocked
            && run
                .record()
                .error
                .as_ref()
                .is_some_and(|error| error.code == "uncertain_outcome")
            && !run.record().controls.cancel_requested
            && let Some(activity) = run.activities().iter().find(|activity| {
                activity.spec.handler == handler("write")
                    && activity.status == ActivityStatus::Indeterminate
                    && activity.active_attempt.is_none()
            })
            && let Some(attempt) = run.attempts().iter().find(|attempt| {
                attempt.activity_id == activity.id
                    && attempt.ordinal == activity.attempts
                    && attempt.status == AttemptStatus::Interrupted
                    && matches!(attempt.outcome, Some(Outcome::Uncertain(_)))
            })
        {
            let observed = match &self.inspector {
                Some(inspector) => match serde_json::from_value(activity.spec.input.clone()) {
                    Ok(request) => inspector.inspect(id, activity.id, request).await,
                    Err(error) => Err(error.into()),
                },
                None => Err(Error::Unavailable(
                    "application journal inspection is not configured".into(),
                )),
            };
            let (inspection, inspection_error) = match observed {
                Ok(inspection) => (Some(inspection), None),
                Err(error) => (None, Some(error.to_string())),
            };
            recovery = Some(ApplicationRecoveryView {
                activity_id: activity.id,
                attempt_id: attempt.id,
                interrupted_at: attempt.ended_at,
                diagnostic: attempt.outcome.clone(),
                paused: run.record().controls.pause_requested
                    || run.record().controls.boundary_pause,
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
        Ok(ApplicationView {
            run_id: id,
            task_id: input.task_id,
            status: run.record().status,
            preview: run
                .record()
                .state
                .get("preview")
                .filter(|p| !p.is_null())
                .map(|p| serde_json::from_value(p.clone()))
                .transpose()?,
            report: run
                .record()
                .result
                .clone()
                .map(serde_json::from_value)
                .transpose()?,
            wait: run.record().wait.clone(),
            error: run.record().error.clone(),
            recovery,
            reconciliation_receipts: run.record().reconciliation_receipts.clone(),
        })
    }
    /// Exact attempt identities and receipts retain the engine's transactional recovery rules.
    pub async fn reconcile(
        &self,
        id: RunId,
        request: ApplicationRecoveryRequest,
    ) -> Result<(), Error> {
        let run = self.engine.status(id).await?;
        if run.record().workflow != workflow()
            || !run.activities().iter().any(|activity| {
                activity.id == request.activity_id && activity.spec.handler == handler("write")
            })
            || !run.attempts().iter().any(|attempt| {
                attempt.id == request.attempt_id && attempt.activity_id == request.activity_id
            })
        {
            return Err(DomainError::InvalidReconciliation(
                "decision must identify a saved patch write attempt".into(),
            )
            .into());
        }
        self.engine.reconcile(id, request.into()).await
    }
    /// Bind approval to the one persisted preview/wait; no filesystem operation runs here.
    pub async fn approve(&self, id: RunId, approved: bool) -> Result<(), Error> {
        let run = self.engine.status(id).await?;
        if run.record().workflow != workflow() {
            return Err(Error::Unavailable("Run is not a patch application".into()));
        }
        let wait = run
            .record()
            .wait
            .as_ref()
            .filter(|w| w.key == APPROVAL_KEY)
            .map(|w| w.id)
            .or_else(|| run.record().input_receipts.last().map(|r| r.wait_id))
            .ok_or_else(|| Error::Unavailable("application is not awaiting approval".into()))?;
        self.engine
            .signal(id, wait, json!({"approved":approved}))
            .await
    }
    /// Browser decisions identify the exact saved wait, including after lost acknowledgements.
    pub async fn respond(&self, id: RunId, wait: WaitId, approved: bool) -> Result<(), Error> {
        let run = self.engine.status(id).await?;
        if run.record().workflow != workflow() {
            return Err(
                DomainError::InvalidConfiguration("Run is not a patch application".into()).into(),
            );
        }
        self.engine
            .signal(id, wait, json!({"approved":approved}))
            .await
    }
}
