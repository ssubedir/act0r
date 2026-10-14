//! Immutable direct-child inspection plus the shared patch journal under managed ownership.
use super::{
    patch_applications::LocalPatchApplications,
    workspace::{LocalTools, atomic_write, link, read_with_limit},
};
use crate::{
    application::{Error, ports::*},
    domain::{child_changes::*, patch_application::ApplicationInput, *},
};
use serde_json::json;
use std::{fs, sync::Arc};

#[derive(Clone)]
pub struct LocalChildChanges {
    source: Arc<dyn ChildChangesReadPort>,
    workers: Option<Arc<dyn WorkerChangesReadPort>>,
    applications: Arc<LocalPatchApplications>,
    tools: Arc<LocalTools>,
}
impl ChildImportInspectionPort for LocalChildChanges {
    fn inspect_import(
        &self,
        run: RunId,
        activity: ActivityId,
        request: Inspection,
    ) -> PortFuture<'_, crate::domain::patch_application::ApplicationInspection> {
        let adapter = self.clone();
        Box::pin(async move {
            let authority = request.clone();
            let workers = adapter.workers.clone();
            let view = tokio::task::spawn_blocking(move || {
                request.validate()?;
                let path = adapter
                    .applications
                    .inspection_directory(request.preview.preparation_id)?
                    .join("inspection.json");
                let metadata = fs::symlink_metadata(&path)?;
                if link(&metadata) || !metadata.is_file() {
                    return Err(Error::Unavailable(
                        "invalid child inspection receipt".into(),
                    ));
                }
                let saved: Inspection =
                    serde_json::from_slice(&read_with_limit(&path, 4 * 1024 * 1024)?)?;
                if saved != request {
                    return Err(Error::Unavailable(
                        "import differs from the saved inspection receipt".into(),
                    ));
                }
                adapter
                    .applications
                    .inspect_managed_import(run, activity, &request, &adapter.tools)
            })
            .await
            .map_err(|error| {
                Error::Unavailable(format!("child import inspection failed: {error}"))
            })??;
            if authority.request.dependency.is_some() && view.report.is_none() {
                workers
                    .ok_or_else(|| {
                        Error::Unavailable("worker patch handoff is not installed".into())
                    })?
                    .validate_worker_import(run, authority)
                    .await?;
            }
            Ok(view)
        })
    }
}
impl LocalChildChanges {
    pub fn new(
        source: Arc<dyn ChildChangesReadPort>,
        applications: Arc<LocalPatchApplications>,
        tools: Arc<LocalTools>,
    ) -> Self {
        Self {
            source,
            workers: None,
            applications,
            tools,
        }
    }
    pub fn with_worker_changes(mut self, workers: Arc<dyn WorkerChangesReadPort>) -> Self {
        self.workers = Some(workers);
        self
    }
    async fn inspect(&self, call: &ActivityExecution) -> Result<Inspection, Error> {
        let transfer: Option<worker_changes::InspectWorkerChanges> =
            if call.handler == worker_changes::handler("inspect") {
                Some(serde_json::from_value(call.input.clone())?)
            } else {
                None
            };
        let request: Option<InspectRequest> = if transfer.is_none() {
            Some(serde_json::from_value(call.input.clone())?)
        } else {
            None
        };
        if let Some(r) = &request {
            r.validate()?;
        }
        if let Some(r) = &transfer {
            r.validate()?;
        }
        let receipt = self
            .applications
            .directory(call.activity_id)?
            .join("inspection.json");
        if fs::symlink_metadata(&receipt).is_ok() {
            if link(&fs::symlink_metadata(&receipt)?) {
                return Err(Error::Unavailable(
                    "invalid child inspection receipt".into(),
                ));
            }
            let saved: Inspection =
                serde_json::from_slice(&read_with_limit(&receipt, 4 * 1024 * 1024)?)?;
            saved.validate()?;
            if request.as_ref().is_some_and(|r| &saved.request != r)
                || transfer.as_ref().is_some_and(|r| {
                    saved.request.dependency.as_ref() != Some(&r.dependency)
                        || saved.request.offset != r.offset
                })
                || saved.preview.run_id != call.run_id
                || saved.preview.preparation_id != call.activity_id
            {
                return Err(Error::Unavailable(
                    "child inspection receipt identity differs".into(),
                ));
            }
            return Ok(saved);
        }
        let (request, source) = if let Some(transfer) = transfer {
            self.workers
                .as_ref()
                .ok_or_else(|| Error::Unavailable("worker patch handoff is not installed".into()))?
                .worker_changes(call.run_id, transfer)
                .await?
        } else {
            let request = request.ok_or(DomainError::InvalidState)?;
            let source = self
                .source
                .child_changes(call.run_id, request.clone())
                .await?;
            (request, source)
        };
        let changes = source
            .changes
            .ok_or_else(|| Error::Unavailable("child change artifact is missing".into()))?;
        changes.validate()?;
        if !source.enabled
            || source.run_status != Some(RunStatus::Succeeded)
            || source.run_id != Some(changes.run_id)
        {
            return Err(Error::Unavailable(
                "child artifact differs from its successful task Run".into(),
            ));
        }
        // Bounds on metadata separate from the patch's existing 4 MiB limit.
        if changes.files.len() > 128 {
            return Err(Error::Unavailable(
                "child imports support at most 128 changed files".into(),
            ));
        }
        let patch = changes.patch();
        let start = request.offset as usize;
        if start > patch.len() || !patch.is_char_boundary(start) {
            return Err(Error::Unavailable(
                "patch offset must be a UTF-8 boundary within the saved patch".into(),
            ));
        }
        let mut end = (start + PAGE_BYTES).min(patch.len());
        while !patch.is_char_boundary(end) {
            end -= 1;
        }
        let input = ApplicationInput {
            task_id: source.task_id,
            target: request.target.workspace.clone(),
            changes,
        };
        let mut preparation = call.clone();
        preparation.input = json!(input);
        let preview =
            self.applications
                .prepare_managed(&preparation, &self.tools, &request.target)?;
        let inspection = Inspection {
            request,
            task_id: source.task_id,
            child_run_id: input.changes.run_id,
            artifact_id: input.changes.activity_id,
            preview,
            patch: patch[start..end].into(),
            patch_bytes: patch.len() as u32,
            next_offset: (end < patch.len()).then_some(end as u32),
        };
        inspection.validate()?;
        atomic_write(&receipt, &serde_json::to_vec(&inspection)?)?;
        Ok(inspection)
    }
}
impl ChildChangesPort for LocalChildChanges {
    fn execute(&self, call: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if call.handler == handler("inspect")
                || call.handler == worker_changes::handler("inspect")
            {
                return Ok(match self.inspect(&call).await {
                    Ok(inspection) => Outcome::Success(json!({"ok":true,"inspection":inspection})),
                    Err(Error::Unavailable(message)) => Outcome::Success(
                        json!({"ok":false,"error":{"code":"child_changes_unavailable","message":message}}),
                    ),
                    Err(error) => Outcome::Failure(Failure::new(
                        "child_changes_inspect",
                        error.to_string(),
                        matches!(
                            error,
                            Error::Io(_) | Error::Ownership(_) | Error::Storage(_)
                        ),
                    )),
                });
            }
            if call.handler != handler("import")
                && call.handler != worker_changes::handler("import")
            {
                return Err(Error::Unavailable(
                    "unsupported child changes handler".into(),
                ));
            }
            let inspection: Inspection = serde_json::from_value(call.input.clone())?;
            if call.handler == worker_changes::handler("import") {
                if inspection.request.dependency.is_none() {
                    return Err(DomainError::InvalidState.into());
                }
                let view = match self.applications.inspect_managed_import(
                    call.run_id,
                    call.activity_id,
                    &inspection,
                    &self.tools,
                ) {
                    Ok(view) => view,
                    Err(error) => return Ok(Outcome::Uncertain(error.to_string())),
                };
                if view.report.is_none() {
                    let validation = self
                        .workers
                        .as_ref()
                        .ok_or_else(|| {
                            Error::Unavailable("worker patch handoff is not installed".into())
                        })?
                        .validate_worker_import(call.run_id, inspection.clone())
                        .await;
                    if let Err(error) = validation {
                        return Ok(match error {
                            Error::Unavailable(message) => Outcome::Success(
                                json!({"ok":false,"error":{"code":"worker_changes_unavailable","message":message}}),
                            ),
                            error => Outcome::Failure(Failure::new(
                                "worker_changes_validation",
                                error.to_string(),
                                false,
                            )),
                        });
                    }
                }
            } else if inspection.request.dependency.is_some() {
                return Err(DomainError::InvalidState.into());
            }
            let result = (|| {
                let receipt = self
                    .applications
                    .directory(inspection.preview.preparation_id)?
                    .join("inspection.json");
                if link(&fs::symlink_metadata(&receipt)?) {
                    return Err(Error::Unavailable(
                        "invalid child inspection receipt".into(),
                    ));
                }
                let saved: Inspection =
                    serde_json::from_slice(&read_with_limit(&receipt, 4 * 1024 * 1024)?)?;
                if saved != inspection {
                    return Err(Error::Unavailable(
                        "import differs from the saved inspection receipt".into(),
                    ));
                }
                self.applications
                    .write_managed(&call, &inspection, &self.tools)
            })();
            Ok(match result {
                Ok(report) => Outcome::Success(json!(report)),
                Err(Error::Ownership(message)) => {
                    Outcome::Failure(Failure::new("child_import_busy", message, true))
                }
                Err(error) => Outcome::Uncertain(error.to_string()),
            })
        })
    }
}
