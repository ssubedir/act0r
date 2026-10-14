//! Bounded source snapshots and the existing process runner under project ownership.
use super::{
    candidates::{digest, manifest},
    patch_applications::{LocalPatchApplications, root_identity, safe_directory},
    workspace::{PRIVATE, atomic_write, execute_command, link, read_with_limit},
};
use crate::{
    application::{Error, ports::*},
    domain::{project_verification::*, *},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
mod refresh;

#[derive(Clone)]
pub struct LocalProjectVerification {
    root: PathBuf,
    receipts: Arc<LocalPatchApplications>,
    applications: Arc<LocalPatchApplications>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Prepared {
    input: VerificationInput,
    snapshot: ProjectSnapshot,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    activity_id: ActivityId,
    request: VerificationRequest,
    report: VerificationReport,
}
fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
fn known_preflight(error: Error) -> Error {
    crate::domain::tools::ToolError::new("project_verification_preflight", error.to_string()).into()
}
fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Error> {
    let meta = fs::symlink_metadata(path)?;
    if link(&meta) || !meta.is_file() {
        return Err(Error::Unavailable(
            "invalid verification receipt file".into(),
        ));
    }
    Ok(serde_json::from_slice(&read_with_limit(
        path,
        // The frozen request and two observed manifests can each approach 4 MiB;
        // applied-path metadata and bounded process output need room as well.
        32 * 1024 * 1024,
    )?)?)
}
impl LocalProjectVerification {
    pub fn new(root: PathBuf, applications: Arc<LocalPatchApplications>) -> Self {
        Self {
            receipts: Arc::new(LocalPatchApplications::new(root.clone())),
            root,
            applications,
        }
    }
    fn target(&self, input: &VerificationInput) -> Result<PathBuf, Error> {
        input.validate()?;
        let root = safe_directory(Path::new(&input.target))?;
        if root.to_string_lossy() != input.target
            || root_identity(&root)? != input.root_identity
            || root.join(PRIVATE).join("managed").try_exists()?
        {
            return Err(Error::Unavailable(
                "applied project directory identity changed or became a managed workspace".into(),
            ));
        }
        Ok(root)
    }
    fn snapshot(
        &self,
        root: &Path,
        run: RunId,
        preparation: ActivityId,
    ) -> Result<ProjectSnapshot, Error> {
        let identity = root_identity(root)?;
        let files = manifest(root)?;
        if manifest(root)? != files || root_identity(root)? != identity {
            return Err(Error::Unavailable(
                "project changed while recording its snapshot".into(),
            ));
        }
        let snapshot = ProjectSnapshot {
            run_id: run,
            preparation_id: preparation,
            root_identity: identity,
            captured_at: now(),
            digest: digest(&files)?,
            files,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
    fn directory(&self, id: ActivityId) -> Result<PathBuf, Error> {
        // Inspection uses existing storage only, unlike admission/receipt publication.
        let root = safe_directory(&self.root)?;
        let path = root.join(id.to_string());
        safe_directory(&path)
    }
    fn prepared(&self, request: &VerificationRequest) -> Result<PathBuf, Error> {
        request.snapshot.validate_for(&request.input)?;
        let directory = self.directory(request.snapshot.preparation_id)?;
        let saved: Prepared = read(&directory.join("prepared.json"))?;
        if saved.input != request.input || saved.snapshot != request.snapshot {
            return Err(Error::Unavailable(
                "verification differs from its exact reviewed preparation".into(),
            ));
        }
        Ok(directory)
    }
    fn saved_report(
        &self,
        directory: &Path,
        activity: ActivityId,
        request: &VerificationRequest,
    ) -> Result<Option<VerificationReport>, Error> {
        let path = directory.join(format!("report-{activity}.json"));
        if !path.try_exists()? {
            return Ok(None);
        }
        let receipt: Receipt = read(&path)?;
        if receipt.activity_id != activity || receipt.request != *request {
            return Err(Error::Unavailable(
                "verification receipt belongs to another request".into(),
            ));
        }
        receipt.report.validate_for(request)?;
        Ok(Some(receipt.report))
    }
    fn prepare(&self, execution: &ActivityExecution) -> Result<ProjectSnapshot, Error> {
        let input: VerificationInput = serde_json::from_value(execution.input.clone())?;
        input.validate()?;
        let directory = self.receipts.directory(execution.activity_id)?;
        let path = directory.join("prepared.json");
        if path.try_exists()? {
            let saved: Prepared = read(&path)?;
            saved.snapshot.validate_for(&input)?;
            if saved.input != input
                || saved.snapshot.run_id != execution.run_id
                || saved.snapshot.preparation_id != execution.activity_id
            {
                return Err(Error::Unavailable(
                    "prepared project snapshot identity changed".into(),
                ));
            }
            return Ok(saved.snapshot);
        }
        let root = self.target(&input)?;
        let _guard = self.applications.project_lock(&root)?;
        let snapshot = self.snapshot(&root, execution.run_id, execution.activity_id)?;
        snapshot.validate_for(&input)?;
        atomic_write(
            &path,
            &serde_json::to_vec(&Prepared {
                input,
                snapshot: snapshot.clone(),
            })?,
        )?;
        Ok(snapshot)
    }
    async fn run(&self, execution: &ActivityExecution) -> Result<VerificationReport, Error> {
        let request: VerificationRequest = serde_json::from_value(execution.input.clone())?;
        if request.snapshot.run_id != execution.run_id {
            return Err(Error::Unavailable(
                "verification Run identity changed".into(),
            ));
        }
        let directory = self.prepared(&request).map_err(known_preflight)?;
        if let Some(report) = self
            .saved_report(&directory, execution.activity_id, &request)
            .map_err(known_preflight)?
        {
            return Ok(report);
        }
        let adapter = self.clone();
        let frozen = request.clone();
        let preflight = tokio::task::spawn_blocking(move || -> Result<_, Error> {
            let root = adapter.target(&frozen.input)?;
            let guard = adapter.applications.project_lock(&root)?;
            let before = adapter.snapshot(
                &root,
                frozen.snapshot.run_id,
                frozen.snapshot.preparation_id,
            )?;
            Ok((root, guard, before))
        })
        .await
        .map_err(|e| known_preflight(Error::Unavailable(e.to_string())))?
        .map_err(known_preflight)?;
        let (root, _guard, before) = preflight;
        let mut report = VerificationReport {
            run_id: execution.run_id,
            application_id: request.input.application_id,
            target: request.input.target.clone(),
            command: request.input.command.clone(),
            checked_at: now(),
            status: VerificationStatus::Changed,
            before: Some(before.clone()),
            after: None,
            output: None,
            reason: Some(
                "Project changed after snapshot review; prepare a new verification.".into(),
            ),
        };
        if before.digest == request.snapshot.digest {
            let output: VerificationOutput = serde_json::from_value(
                execute_command(
                    &root,
                    &request.input.command.argv,
                    Some(request.input.command.timeout_ms),
                )
                .await?,
            )?;
            let adapter = self.clone();
            let frozen = request.clone();
            let after = tokio::task::spawn_blocking(move || -> Result<_, Error> {
                let root = adapter.target(&frozen.input)?;
                adapter.snapshot(
                    &root,
                    frozen.snapshot.run_id,
                    frozen.snapshot.preparation_id,
                )
            })
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
            match after {
                Ok(snapshot) => {
                    let matches = snapshot.digest == before.digest;
                    report.status = if !matches {
                        VerificationStatus::Changed
                    } else if output.ok {
                        VerificationStatus::Verified
                    } else {
                        VerificationStatus::Failed
                    };
                    report.reason=(!matches).then(||"Project inputs changed while the command ran; passing output does not verify the reviewed snapshot.".into());
                    report.after = Some(snapshot);
                }
                Err(error) => {
                    report.reason = Some(
                        format!("Could not record the project after verification: {error}")
                            .chars()
                            .take(2000)
                            .collect(),
                    );
                }
            }
            report.output = Some(output);
            report.checked_at = now();
        }
        report.validate_for(&request)?;
        atomic_write(
            &directory.join(format!("report-{}.json", execution.activity_id)),
            &serde_json::to_vec(&Receipt {
                activity_id: execution.activity_id,
                request,
                report: report.clone(),
            })?,
        )?;
        Ok(report)
    }
}
impl ProjectVerificationPort for LocalProjectVerification {
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if execution.handler == handler("prepare") {
                let adapter = self.clone();
                let result = tokio::task::spawn_blocking(move || adapter.prepare(&execution))
                    .await
                    .map_err(|e| Error::Unavailable(e.to_string()))?;
                return Ok(match result {
                    Ok(snapshot) => Outcome::Success(json!(snapshot)),
                    Err(error) => Outcome::Failure(Failure::new(
                        "project_inspection",
                        error.to_string(),
                        matches!(error, Error::Ownership(_)),
                    )),
                });
            }
            if execution.handler == handler("run") {
                return Ok(match self.run(&execution).await {
                    Ok(report) => Outcome::Success(json!(report)),
                    Err(Error::ToolRejected(error)) => Outcome::Failure(Failure::new(
                        "project_verification_preflight",
                        error.to_string(),
                        false,
                    )),
                    Err(error) => {
                        Outcome::Uncertain(format!("project verification outcome unknown: {error}"))
                    }
                });
            }
            Err(Error::Unavailable(
                "unsupported project verification handler".into(),
            ))
        })
    }
}
struct RepairCopiesGuard {
    _locks: Vec<fs::File>,
}
impl OwnershipGuard for RepairCopiesGuard {}
fn lock_verified_copies(
    root: PathBuf,
    copies: Vec<String>,
    expected: String,
) -> Result<Box<dyn OwnershipGuard>, Error> {
    if copies.is_empty() || copies.len() > 17 {
        return Err(DomainError::InvalidState.into());
    }
    let tools = super::workspace::LocalTools::new(
        root.parent()
            .ok_or(DomainError::InvalidState)?
            .join("workspaces"),
    );
    let mut locks = vec![];
    for copy in copies {
        let workspace = tools.workspace(&copy)?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let lock = loop {
            match tools.lock(&workspace) {
                Ok(lock) => break lock,
                Err(Error::Ownership(_)) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(25))
                }
                Err(error) => return Err(error),
            }
        };
        let files = manifest(&workspace)?;
        if digest(&files)? != expected || manifest(&workspace)? != files {
            return Err(DomainError::SessionConflict("prepared repair copy changed; cancel preparation and verify the current project again".into()).into());
        }
        locks.push(lock);
    }
    Ok(Box::new(RepairCopiesGuard { _locks: locks }))
}
impl ProjectVerificationInspectionPort for LocalProjectVerification {
    fn inspect_refresh(
        &self,
        source: String,
        workspace: String,
    ) -> PortFuture<'_, (project_refresh::RefreshSnapshot, Vec<task::SnapshotFile>)> {
        let adapter = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || adapter.refresh_snapshot(source, workspace))
                .await
                .map_err(|e| Error::Unavailable(e.to_string()))?
        })
    }
    fn lock_refresh(
        &self,
        snapshot: project_refresh::RefreshSnapshot,
    ) -> PortFuture<'_, Box<dyn OwnershipGuard>> {
        let adapter = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || adapter.lock_refresh_snapshot(snapshot))
                .await
                .map_err(|e| Error::Unavailable(e.to_string()))?
        })
    }
    fn lock_repair_copies(
        &self,
        copies: Vec<String>,
        expected: String,
    ) -> PortFuture<'_, Box<dyn OwnershipGuard>> {
        let root = self.root.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || lock_verified_copies(root, copies, expected))
                .await
                .map_err(|e| Error::Unavailable(e.to_string()))?
        })
    }
    fn inspect(
        &self,
        activity: ActivityId,
        request: VerificationRequest,
    ) -> PortFuture<'_, VerificationInspection> {
        Box::pin(async move {
            let adapter = self.clone();
            tokio::task::spawn_blocking(move || {
                let directory = adapter.prepared(&request)?;
                let saved_report = adapter
                    .saved_report(&directory, activity, &request)?
                    .is_some();
                let observed = adapter.target(&request.input).and_then(|root| {
                    adapter.snapshot(
                        &root,
                        request.snapshot.run_id,
                        request.snapshot.preparation_id,
                    )
                });
                let (project_matches, observed_digest, error) = match observed {
                    Ok(s) => (s.digest == request.snapshot.digest, Some(s.digest), None),
                    Err(e) => (false, None, Some(e.to_string())),
                };
                Ok(VerificationInspection {
                    saved_report,
                    project_matches,
                    observed_digest,
                    error,
                })
            })
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?
        })
    }
}
