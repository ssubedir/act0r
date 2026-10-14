//! Read-only recovery projection; filesystem execution retains the authoritative hash checks.
use super::*;

impl LocalPatchApplications {
    pub(super) fn load_write_journal(
        &self,
        directory: &Path,
        run: RunId,
        activity: ActivityId,
        request: &WriteRequest,
    ) -> Result<(bool, Journal), Error> {
        let path = directory.join("write.json");
        let exists = match fs::symlink_metadata(&path) {
            Ok(meta) if link(&meta) || !meta.is_file() => {
                return Err(refused("invalid application journal"));
            }
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        let digest = json_hash(request)?;
        let journal = if exists {
            serde_json::from_slice::<Journal>(&read_with_limit(&path, 4 * 1024 * 1024)?)?
        } else {
            Journal {
                run_id: run,
                activity_id: activity,
                request_digest: digest.clone(),
                started: false,
                progress: vec![FileProgress::Pending; request.preview.files.len()],
                report: None,
            }
        };
        if journal.run_id != run
            || journal.activity_id != activity
            || journal.request_digest != digest
            || journal.progress.len() != request.preview.files.len()
        {
            return Err(refused("application journal identity differs"));
        }
        let mut pending = false;
        let mut intended = false;
        for progress in &journal.progress {
            match progress {
                FileProgress::Applied if pending || intended => {
                    return Err(refused("invalid application progress order"));
                }
                FileProgress::Intended if pending || intended => {
                    return Err(refused("invalid application intent order"));
                }
                FileProgress::Intended => intended = true,
                FileProgress::Pending => pending = true,
                _ => {}
            }
        }
        if !journal.started && journal.progress.iter().any(|p| *p != FileProgress::Pending) {
            return Err(refused("file progress has no operation intent"));
        }
        if let Some(report) = &journal.report
            && (report.run_id != run
                || report.target != request.preview.target
                || match report.status {
                    ApplicationStatus::Applied => {
                        !journal.started
                            || journal.progress.iter().any(|p| *p != FileProgress::Applied)
                            || report.applied_files
                                != request
                                    .preview
                                    .files
                                    .iter()
                                    .map(|file| file.path.clone())
                                    .collect::<Vec<_>>()
                            || !report.conflicts.is_empty()
                    }
                    ApplicationStatus::Conflicted => {
                        journal.started
                            || !report.applied_files.is_empty()
                            || report.conflicts.is_empty()
                    }
                    ApplicationStatus::Rejected => true,
                })
        {
            return Err(refused("invalid saved application report"));
        }
        Ok((exists, journal))
    }
    fn inspect_write(
        &self,
        run: RunId,
        activity: ActivityId,
        request: WriteRequest,
    ) -> Result<ApplicationInspection, Error> {
        self.inspect_write_for(run, activity, request, None)
    }
    pub(in crate::adapters) fn inspection_directory(
        &self,
        id: ActivityId,
    ) -> Result<PathBuf, Error> {
        let storage = safe_directory(&self.root)?;
        safe_directory(&storage.join(id.to_string()))
    }
    pub(in crate::adapters) fn inspect_managed_import(
        &self,
        run: RunId,
        activity: ActivityId,
        inspection: &crate::domain::child_changes::Inspection,
        tools: &LocalTools,
    ) -> Result<ApplicationInspection, Error> {
        inspection.validate()?;
        let directory = self.inspection_directory(inspection.preview.preparation_id)?;
        let preparation = self.load_preparation(&directory.join("prepared.json"))?;
        if preparation.input.task_id != inspection.task_id
            || preparation.input.changes.run_id != inspection.child_run_id
            || preparation.input.changes.activity_id != inspection.artifact_id
        {
            return Err(refused("import differs from the saved child artifact"));
        }
        self.inspect_write_for(
            run,
            activity,
            WriteRequest {
                input: preparation.input,
                preview: inspection.preview.clone(),
            },
            Some((tools, &inspection.request.target)),
        )
    }
    fn inspect_write_for(
        &self,
        run: RunId,
        activity: ActivityId,
        request: WriteRequest,
        managed: Option<(&LocalTools, &crate::domain::child_changes::ManagedTarget)>,
    ) -> Result<ApplicationInspection, Error> {
        request.preview.validate_for(&request.input)?;
        if request.preview.run_id != run || request.preview.has_conflicts() {
            return Err(refused("inspection does not match an applicable preview"));
        }
        // Unlike execution's directory(), inspection must never create missing storage.
        let directory = self.inspection_directory(request.preview.preparation_id)?;
        let preparation = self.load_preparation(&directory.join("prepared.json"))?;
        if preparation.preview != request.preview
            || json!(preparation.input) != json!(request.input)
            || preparation.managed.as_ref() != managed.map(|(_, pinned)| pinned)
        {
            return Err(refused("inspection differs from prepared artifact"));
        }
        let (journal_exists, journal) =
            self.load_write_journal(&directory, run, activity, &request)?;
        let target = safe_directory(Path::new(&request.preview.target)).and_then(|target| {
            if target.to_string_lossy() != request.preview.target
                || root_identity(&target)? != request.preview.root_identity
            {
                return Err(refused(
                    "target directory identity or ownership changed since preview",
                ));
            }
            if let Some((tools, pinned)) = managed {
                pinned.validate()?;
                if tools.workspace(&pinned.workspace)? != target
                    || request.input.target != pinned.workspace
                    || request
                        .preview
                        .files
                        .iter()
                        .any(|file| !pinned.permits(&file.path))
                {
                    return Err(refused("managed target or import permissions differ"));
                }
            } else if target.join(PRIVATE).join("managed").try_exists()? {
                return Err(refused("target directory ownership changed since preview"));
            }
            Ok(target)
        });
        let mut files = Vec::with_capacity(request.preview.files.len());
        for (file, progress) in request.preview.files.iter().zip(&journal.progress) {
            let observed = match &target {
                Ok(root) => target_path(root, &file.path).and_then(|path| contents(&path)),
                Err(error) => Err(refused(error.to_string())),
            };
            let expected = if *progress == FileProgress::Applied {
                &file.after_sha256
            } else {
                &file.before_sha256
            };
            let acceptable = observed.as_ref().is_ok_and(|text| {
                text_hash(text) == *expected
                    || (*progress == FileProgress::Intended && text_hash(text) == file.after_sha256)
            });
            let mut file = file.clone();
            file.observed_sha256 = observed.as_ref().ok().and_then(text_hash);
            file.conflict = if acceptable {
                None
            } else {
                Some(observed.err().map_or_else(
                    || "target differs from journal file hashes".into(),
                    |error| error.to_string(),
                ))
            };
            files.push(ApplicationFileInspection {
                file,
                progress: *progress,
            });
        }
        Ok(ApplicationInspection {
            journal_exists,
            started: journal.started,
            files,
            report: journal.report,
        })
    }
}
impl PatchApplicationInspectionPort for LocalPatchApplications {
    fn inspect(
        &self,
        run: RunId,
        activity: ActivityId,
        request: WriteRequest,
    ) -> PortFuture<'_, ApplicationInspection> {
        let adapter = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || adapter.inspect_write(run, activity, request))
                .await
                .map_err(|error| refused(format!("application inspection failed: {error}")))?
        })
    }
}
