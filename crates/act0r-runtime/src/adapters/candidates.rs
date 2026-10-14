use super::workspace::{
    LocalTools, PRIVATE, atomic_write, copy_tree, excluded, hash, link, read_with_limit, run_tests,
};
use crate::{
    application::{Error, ports::*},
    domain::{task::*, *},
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

const MAX_SNAPSHOT: u64 = 64 * 1024 * 1024;
const MAX_EDIT: u64 = 1024 * 1024;
const MAX_PATCH: usize = 4 * 1024 * 1024;

pub struct LocalCandidates {
    snapshots: PathBuf,
    workspaces: PathBuf,
}
impl LocalCandidates {
    pub(super) fn load_snapshot(&self, id: TaskId) -> Result<WorkspaceSnapshot, Error> {
        let directory = self.snapshots.join(id.to_string());
        if link(&fs::symlink_metadata(&directory)?)
            || link(&fs::symlink_metadata(directory.join(PRIVATE))?)
        {
            return Err(Error::Unavailable("snapshot path is a link".into()));
        }
        let path = directory.join(PRIVATE).join("snapshot.json");
        if link(&fs::symlink_metadata(&path)?) {
            return Err(Error::Unavailable("snapshot metadata is a link".into()));
        }
        let snapshot: WorkspaceSnapshot =
            serde_json::from_slice(&read_with_limit(&path, 4 * 1024 * 1024)?)?;
        if snapshot.task_id != id {
            return Err(Error::Unavailable(
                "snapshot identity differs from its directory".into(),
            ));
        }
        self.verify(&snapshot)?;
        Ok(snapshot)
    }
    pub fn new(snapshots: PathBuf, workspaces: PathBuf) -> Self {
        Self {
            snapshots,
            workspaces,
        }
    }
    fn snapshot_sync(&self, source: PathBuf, id: TaskId) -> Result<WorkspaceSnapshot, Error> {
        self.capture(source, id, false)
    }
    pub(super) fn capture(
        &self,
        source: PathBuf,
        id: TaskId,
        verified_branch: bool,
    ) -> Result<WorkspaceSnapshot, Error> {
        fs::create_dir_all(&self.snapshots)?;
        let root = fs::canonicalize(&self.snapshots)?;
        let destination = root.join(id.to_string());
        if destination.exists() {
            if link(&fs::symlink_metadata(&destination)?) {
                return Err(Error::Unavailable("invalid snapshot directory".into()));
            }
            let snapshot: WorkspaceSnapshot = serde_json::from_slice(&read_with_limit(
                &destination.join(PRIVATE).join("snapshot.json"),
                MAX_PATCH as u64,
            )?)?;
            self.verify(&snapshot)?;
            return Ok(snapshot);
        }
        let source = fs::canonicalize(source)?;
        let expected_files = if verified_branch {
            Some(manifest(&source)?)
        } else {
            None
        };
        let storage_root = root
            .parent()
            .ok_or_else(|| Error::Unavailable("snapshot parent missing".into()))?;
        if !source.is_dir() || (!verified_branch && source.starts_with(storage_root)) {
            return Err(Error::Unavailable(
                "snapshot source must be a project outside managed storage".into(),
            ));
        }
        let staging = tempfile::tempdir_in(&root)?;
        let exclusion_root = if verified_branch {
            staging.path()
        } else {
            storage_root
        };
        copy_tree(&source, staging.path(), exclusion_root, &mut (0, 0))?;
        let files = manifest(staging.path())?;
        if expected_files
            .as_ref()
            .is_some_and(|expected| expected != &files)
            || (verified_branch && manifest(&source)? != files)
        {
            return Err(Error::Unavailable(
                "workspace changed during snapshot capture".into(),
            ));
        }
        let snapshot = WorkspaceSnapshot {
            version: 1,
            task_id: id,
            workspace: destination.to_string_lossy().into_owned(),
            digest: digest(&files)?,
            files,
        };
        fs::create_dir(staging.path().join(PRIVATE))?;
        atomic_write(
            &staging.path().join(PRIVATE).join("snapshot.json"),
            &serde_json::to_vec(&snapshot)?,
        )?;
        publish(staging, &destination)?;
        self.verify(&snapshot)?;
        Ok(snapshot)
    }
    pub(super) fn verify(&self, snapshot: &WorkspaceSnapshot) -> Result<PathBuf, Error> {
        let expected = fs::canonicalize(&self.snapshots)?.join(snapshot.task_id.to_string());
        let path = fs::canonicalize(&snapshot.workspace)?;
        if snapshot.version != 1 || path != expected || link(&fs::symlink_metadata(&expected)?) {
            return Err(Error::Unavailable(
                "invalid snapshot identity or path".into(),
            ));
        }
        let metadata = path.join(PRIVATE).join("snapshot.json");
        if link(&fs::symlink_metadata(path.join(PRIVATE))?)
            || link(&fs::symlink_metadata(&metadata)?)
        {
            return Err(Error::Unavailable("invalid snapshot metadata".into()));
        }
        let saved: WorkspaceSnapshot =
            serde_json::from_slice(&read_with_limit(&metadata, MAX_PATCH as u64)?)?;
        let files = manifest(&path)?;
        if saved != *snapshot || files != snapshot.files || digest(&files)? != snapshot.digest {
            return Err(Error::Unavailable(
                "snapshot content no longer matches its recorded digest".into(),
            ));
        }
        Ok(path)
    }
    fn fork_sync(&self, snapshot: WorkspaceSnapshot, id: RunId) -> Result<String, Error> {
        let source = self.verify(&snapshot)?;
        fs::create_dir_all(&self.workspaces)?;
        let root = fs::canonicalize(&self.workspaces)?;
        let destination = root.join(id.to_string());
        let origin =
            json!({"task_id":snapshot.task_id,"run_id":id,"snapshot_digest":snapshot.digest});
        if destination.exists() {
            let path = LocalTools::new(root).workspace(&destination.to_string_lossy())?;
            let marker = path.join(PRIVATE).join("candidate.json");
            if link(&fs::symlink_metadata(&marker)?)
                || serde_json::from_slice::<serde_json::Value>(&read_with_limit(&marker, 4096)?)?
                    != origin
            {
                return Err(Error::Unavailable(
                    "candidate identity has a conflicting snapshot".into(),
                ));
            }
            return Ok(path.to_string_lossy().into_owned());
        }
        let staging = tempfile::tempdir_in(&root)?;
        copy_tree(&source, staging.path(), &root, &mut (0, 0))?;
        if manifest(staging.path())? != snapshot.files {
            return Err(Error::Unavailable("snapshot changed during fork".into()));
        }
        fs::create_dir(staging.path().join(PRIVATE))?;
        atomic_write(
            &staging.path().join(PRIVATE).join("managed"),
            b"act0r workspace v1\n",
        )?;
        atomic_write(
            &staging.path().join(PRIVATE).join("candidate.json"),
            &serde_json::to_vec(&origin)?,
        )?;
        publish(staging, &destination)?;
        Ok(fs::canonicalize(destination)?
            .to_string_lossy()
            .into_owned())
    }
    fn prepare_evaluation(&self, request: &EvaluationRequest) -> Result<PreparedEvaluation, Error> {
        let snapshot = self.verify(&request.snapshot)?;
        let candidate = LocalTools::new(self.workspaces.clone()).workspace(&request.workspace)?;
        let files = manifest(&candidate)?;
        let baseline: BTreeMap<_, _> = request
            .snapshot
            .files
            .iter()
            .map(|f| (f.path.clone(), f))
            .collect();
        let proposed: BTreeMap<_, _> = files.iter().map(|f| (f.path.clone(), f)).collect();
        let names: BTreeSet<_> = baseline.keys().chain(proposed.keys()).cloned().collect();
        let staging = tempfile::tempdir_in(&self.workspaces)?;
        let storage_root = fs::canonicalize(&self.workspaces)?;
        copy_tree(&snapshot, staging.path(), &storage_root, &mut (0, 0))?;
        if manifest(staging.path())? != request.snapshot.files {
            return Err(Error::Unavailable(
                "snapshot changed during evaluation preparation".into(),
            ));
        }
        let mut diff = Vec::new();
        let mut patch_bytes = 0;
        for name in names {
            let before = baseline.get(&name);
            let after = proposed.get(&name);
            if before == after {
                continue;
            }
            if request
                .protected_paths
                .iter()
                .any(|p| matches_path(&name, p))
                || !request
                    .writable_paths
                    .iter()
                    .any(|p| matches_path(&name, p))
            {
                return Err(Error::Unavailable(format!(
                    "candidate changed protected or unwritable file {name}"
                )));
            }
            let old = if before.is_some() {
                read_with_limit(&snapshot.join(&name), MAX_EDIT)?
            } else {
                vec![]
            };
            let new = if after.is_some() {
                read_with_limit(&candidate.join(&name), MAX_EDIT)?
            } else {
                vec![]
            };
            if before.is_some_and(|f| hash(&old) != f.sha256)
                || after.is_some_and(|f| hash(&new) != f.sha256)
            {
                return Err(Error::Unavailable(
                    "files changed while collecting candidate diff".into(),
                ));
            }
            let old_text = std::str::from_utf8(&old)
                .map_err(|_| Error::Unavailable("candidate diff requires UTF-8 files".into()))?;
            let new_text = std::str::from_utf8(&new)
                .map_err(|_| Error::Unavailable("candidate diff requires UTF-8 files".into()))?;
            let (patch, changed_bytes) =
                patch(&name, before.is_some(), after.is_some(), old_text, new_text);
            patch_bytes += patch.len();
            if patch_bytes > MAX_PATCH {
                return Err(Error::Unavailable("candidate diff exceeds 4 MiB".into()));
            }
            let target = staging.path().join(&name);
            if after.is_some() {
                fs::create_dir_all(
                    target
                        .parent()
                        .ok_or_else(|| Error::Unavailable("invalid candidate path".into()))?,
                )?;
                atomic_write(&target, &new)?;
            } else {
                // Target comes from a validated baseline manifest within this temporary workspace.
                if !target.starts_with(staging.path()) {
                    return Err(Error::Unavailable("invalid candidate path".into()));
                }
                fs::remove_file(target)?;
            }
            diff.push(FileDiff {
                path: name,
                before_sha256: before.map(|f| f.sha256.clone()),
                after_sha256: after.map(|f| f.sha256.clone()),
                patch,
                changed_bytes,
            });
        }
        if manifest(staging.path())? != files {
            return Err(Error::Unavailable(
                "evaluation inputs do not match candidate".into(),
            ));
        }
        if let Some(final_tests) = &request.final_tests {
            if final_tests.test_command != request.test_command {
                return Err(Error::Unavailable(
                    "final test command does not match its pinned configuration".into(),
                ));
            }
            let oracle = self.verify(&final_tests.snapshot)?;
            for file in &final_tests.snapshot.files {
                if !request
                    .protected_paths
                    .iter()
                    .any(|p| matches_path(&file.path, p))
                    || request
                        .writable_paths
                        .iter()
                        .any(|p| matches_path(&file.path, p))
                    || files
                        .iter()
                        .any(|f| f.path.eq_ignore_ascii_case(&file.path))
                {
                    return Err(Error::Unavailable(
                        "final tests must be distinct protected files".into(),
                    ));
                }
                let bytes = read_with_limit(&oracle.join(&file.path), MAX_EDIT)?;
                if hash(&bytes) != file.sha256 {
                    return Err(Error::Unavailable(
                        "final tests changed while preparing verification".into(),
                    ));
                }
                let target = staging.path().join(&file.path);
                fs::create_dir_all(
                    target
                        .parent()
                        .ok_or_else(|| Error::Unavailable("invalid final test path".into()))?,
                )?;
                atomic_write(&target, &bytes)?;
            }
        }
        let verification_files = manifest(staging.path())?;
        let evaluation = CandidateEvaluation {
            passed: false,
            tests: serde_json::Value::Null,
            changed_bytes: diff.iter().map(|d| d.changed_bytes).sum(),
            diff,
            snapshot_digest: request.snapshot.digest.clone(),
            candidate_digest: digest(&files)?,
            final_tests_digest: request
                .final_tests
                .as_ref()
                .map(|t| t.snapshot.digest.clone()),
            error: None,
        };
        Ok(PreparedEvaluation {
            staging,
            files: verification_files,
            candidate_files: files,
            evaluation,
        })
    }
}
struct PreparedEvaluation {
    staging: tempfile::TempDir,
    files: Vec<SnapshotFile>,
    candidate_files: Vec<SnapshotFile>,
    evaluation: CandidateEvaluation,
}
fn publish(staging: tempfile::TempDir, destination: &Path) -> Result<(), Error> {
    fn sync_tree(path: &Path) -> Result<(), Error> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                sync_tree(&entry.path())?;
            } else {
                fs::OpenOptions::new()
                    .write(true)
                    .open(entry.path())?
                    .sync_all()?;
            }
        }
        #[cfg(unix)]
        std::fs::File::open(path)?.sync_all()?;
        Ok(())
    }
    sync_tree(staging.path())?;
    // Each publication is a complete directory. The destination is never a partial copy.
    fs::rename(staging.path(), destination)?;
    #[cfg(unix)]
    std::fs::File::open(
        destination
            .parent()
            .ok_or_else(|| Error::Unavailable("missing publication parent".into()))?,
    )?
    .sync_all()?;
    Ok(())
}
pub(super) fn manifest(root: &Path) -> Result<Vec<SnapshotFile>, Error> {
    fn visit(
        root: &Path,
        path: &Path,
        result: &mut Vec<SnapshotFile>,
        bytes: &mut u64,
        entries: &mut usize,
    ) -> Result<(), Error> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::Unavailable("snapshot paths require UTF-8 names".into()))?;
            if excluded(&name) {
                continue;
            }
            if name.ends_with(['.', ' ']) || name.contains([':', '\\']) {
                return Err(Error::Unavailable(
                    "snapshot contains an ambiguous path".into(),
                ));
            }
            let meta = fs::symlink_metadata(entry.path())?;
            if link(&meta) {
                return Err(Error::Unavailable(
                    "snapshot or candidate contains a symlink or reparse point".into(),
                ));
            }
            *entries += 1;
            if *entries > 10_000 {
                return Err(Error::Unavailable("snapshot exceeds 10,000 entries".into()));
            }
            if meta.is_dir() {
                visit(root, &entry.path(), result, bytes, entries)?;
            } else if meta.is_file() {
                let content = read_with_limit(&entry.path(), MAX_SNAPSHOT)?;
                *bytes = bytes.saturating_add(content.len() as u64);
                if *bytes > MAX_SNAPSHOT {
                    return Err(Error::Unavailable("snapshot exceeds 64 MiB".into()));
                }
                result.push(SnapshotFile {
                    path: entry
                        .path()
                        .strip_prefix(root)
                        .map_err(|_| Error::Unavailable("invalid snapshot path".into()))?
                        .to_string_lossy()
                        .replace('\\', "/"),
                    sha256: hash(&content),
                    bytes: content.len() as u64,
                });
            } else {
                return Err(Error::Unavailable(
                    "snapshot contains a special file".into(),
                ));
            }
        }
        Ok(())
    }
    let mut files = vec![];
    visit(root, root, &mut files, &mut 0, &mut 0)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let unique: BTreeSet<_> = files.iter().map(|f| f.path.to_ascii_lowercase()).collect();
    if unique.len() != files.len() {
        return Err(Error::Unavailable(
            "snapshot contains case-aliased paths".into(),
        ));
    }
    Ok(files)
}
pub(super) fn digest(files: &[SnapshotFile]) -> Result<String, Error> {
    Ok(hash(&serde_json::to_vec(files)?))
}
pub(super) fn patch(
    path: &str,
    exists_before: bool,
    exists_after: bool,
    old: &str,
    new: &str,
) -> (String, u64) {
    let a: Vec<_> = old.split_inclusive('\n').collect();
    let b: Vec<_> = new.split_inclusive('\n').collect();
    let prefix = a.iter().zip(&b).take_while(|(a, b)| a == b).count();
    let suffix = a[prefix..]
        .iter()
        .rev()
        .zip(b[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let start = prefix.saturating_sub(3);
    let a_end = (a.len() - suffix + 3).min(a.len());
    let b_end = (b.len() - suffix + 3).min(b.len());
    let mut result = format!(
        "--- {}\n+++ {}\n@@ -{},{} +{},{} @@\n",
        if exists_before {
            format!("a/{path}")
        } else {
            "/dev/null".into()
        },
        if exists_after {
            format!("b/{path}")
        } else {
            "/dev/null".into()
        },
        if a_end == start { start } else { start + 1 },
        a_end - start,
        if b_end == start { start } else { start + 1 },
        b_end - start
    );
    fn line(result: &mut String, marker: char, line: &str) {
        result.push(marker);
        result.push_str(line);
        if !line.ends_with('\n') {
            result.push_str("\n\\ No newline at end of file\n");
        }
    }
    for text in &a[start..prefix] {
        line(&mut result, ' ', text);
    }
    for text in &a[prefix..a.len() - suffix] {
        line(&mut result, '-', text);
    }
    for text in &b[prefix..b.len() - suffix] {
        line(&mut result, '+', text);
    }
    for text in &a[a.len() - suffix..a_end] {
        line(&mut result, ' ', text);
    }
    let changed_bytes = a[prefix..a.len() - suffix]
        .iter()
        .chain(&b[prefix..b.len() - suffix])
        .map(|s| s.len() as u64)
        .sum();
    (result, changed_bytes)
}
impl CandidateWorkspaces for LocalCandidates {
    fn snapshot(&self, source: PathBuf, id: TaskId) -> PortFuture<'_, WorkspaceSnapshot> {
        Box::pin(async move { self.snapshot_sync(source, id) })
    }
    fn fork(&self, snapshot: WorkspaceSnapshot, id: RunId) -> PortFuture<'_, String> {
        Box::pin(async move { self.fork_sync(snapshot, id) })
    }
    fn capture_checkpoint(
        &self,
        input: CandidateInput,
        artifact: TaskId,
    ) -> PortFuture<'_, WorkspaceSnapshot> {
        Box::pin(async move {
            self.verify(&input.snapshot)?;
            let source =
                LocalTools::new(self.workspaces.clone()).workspace(&input.config.workspace)?;
            let before = manifest(&source)?;
            let snapshot = self.capture(source.clone(), artifact, true)?;
            if before != snapshot.files || manifest(&source)? != before {
                return Err(Error::Unavailable(
                    "candidate changed during checkpoint capture".into(),
                ));
            }
            let baseline: BTreeMap<_, _> =
                input.snapshot.files.iter().map(|f| (&f.path, f)).collect();
            let captured: BTreeMap<_, _> = snapshot.files.iter().map(|f| (&f.path, f)).collect();
            for name in baseline.keys().chain(captured.keys()) {
                if baseline.get(name) != captured.get(name)
                    && (input.protected_paths.iter().any(|p| matches_path(name, p))
                        || !input
                            .config
                            .writable_paths
                            .iter()
                            .any(|p| matches_path(name, p)))
                {
                    return Err(Error::Unavailable(format!(
                        "checkpoint contains a protected or unwritable change: {name}"
                    )));
                }
            }
            Ok(snapshot)
        })
    }
    fn fork_checkpoint(&self, snapshot: WorkspaceSnapshot, id: RunId) -> PortFuture<'_, String> {
        Box::pin(async move {
            let workspace = self.fork_sync(snapshot.clone(), id)?;
            if manifest(Path::new(&workspace))? != snapshot.files {
                return Err(Error::Unavailable(
                    "unpublished branch differs from its checkpoint".into(),
                ));
            }
            Ok(workspace)
        })
    }
    fn freeze_case(
        &self,
        source: PathBuf,
        final_source: PathBuf,
        baseline_id: TaskId,
        final_id: TaskId,
    ) -> PortFuture<'_, (WorkspaceSnapshot, WorkspaceSnapshot)> {
        Box::pin(async move {
            let source = fs::canonicalize(source)?;
            let final_source = fs::canonicalize(final_source)?;
            if source.starts_with(&final_source) || final_source.starts_with(&source) {
                return Err(Error::Unavailable(
                    "final tests must be outside the agent source tree".into(),
                ));
            }
            Ok((
                self.snapshot_sync(source, baseline_id)?,
                self.snapshot_sync(final_source, final_id)?,
            ))
        })
    }
    fn branch_snapshot(
        &self,
        snapshot: WorkspaceSnapshot,
        id: TaskId,
    ) -> PortFuture<'_, WorkspaceSnapshot> {
        Box::pin(async move {
            let source = self.verify(&snapshot)?;
            let branch = self.capture(source, id, true)?;
            if branch.files != snapshot.files || branch.digest != snapshot.digest {
                return Err(Error::Unavailable(
                    "branched snapshot differs from its baseline".into(),
                ));
            }
            Ok(branch)
        })
    }
    fn evaluate(
        &self,
        request: EvaluationRequest,
        _activity: ActivityId,
    ) -> PortFuture<'_, CandidateEvaluation> {
        Box::pin(async move {
            let mut prepared = match self.prepare_evaluation(&request) {
                Ok(p) => p,
                Err(error) => {
                    return Ok(CandidateEvaluation {
                        passed: false,
                        tests: serde_json::Value::Null,
                        diff: vec![],
                        changed_bytes: 0,
                        snapshot_digest: request.snapshot.digest.clone(),
                        candidate_digest: String::new(),
                        final_tests_digest: request
                            .final_tests
                            .as_ref()
                            .map(|t| t.snapshot.digest.clone()),
                        error: Some(error.to_string()),
                    });
                }
            };
            prepared.evaluation.tests =
                run_tests(prepared.staging.path(), &request.test_command).await?;
            prepared.evaluation.passed = prepared.evaluation.tests["ok"] == true
                && prepared.evaluation.tests["exit_code"] == 0;
            let stable = self.verify(&request.snapshot).is_ok()
                && request
                    .final_tests
                    .as_ref()
                    .is_none_or(|t| self.verify(&t.snapshot).is_ok())
                && manifest(prepared.staging.path()).is_ok_and(|files| files == prepared.files)
                && manifest(Path::new(&request.workspace))
                    .is_ok_and(|files| files == prepared.candidate_files);
            if !stable {
                prepared.evaluation.passed = false;
                prepared.evaluation.error =
                    Some("evaluation inputs changed during fixed tests".into());
            }
            Ok(prepared.evaluation)
        })
    }
}

pub struct CandidateExecutor {
    workspaces: Arc<dyn CandidateWorkspaces>,
    fallback: Arc<dyn ActivityExecutor>,
}
impl CandidateExecutor {
    pub fn new(
        workspaces: Arc<dyn CandidateWorkspaces>,
        fallback: Arc<dyn ActivityExecutor>,
    ) -> Self {
        Self {
            workspaces,
            fallback,
        }
    }
}
impl ActivityExecutor for CandidateExecutor {
    fn supports(&self, handler: &DefinitionId) -> bool {
        handler == &DefinitionId::new("coding.evaluate", 1) || self.fallback.supports(handler)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if execution.handler == DefinitionId::new("coding.evaluate", 1) {
                let request = serde_json::from_value(execution.input)?;
                return Ok(Outcome::Success(serde_json::to_value(
                    self.workspaces
                        .evaluate(request, execution.activity_id)
                        .await?,
                )?));
            }
            self.fallback.execute(execution).await
        })
    }
}
