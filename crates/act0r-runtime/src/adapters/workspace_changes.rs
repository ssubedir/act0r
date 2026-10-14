//! Reuse the candidate snapshot/diff format under native workspace ownership.
use super::{
    candidates::{LocalCandidates, digest, manifest, patch},
    workspace::{LocalTools, PRIVATE, atomic_write, hash, link, read_with_limit},
};
use crate::{
    application::{Error, ports::*},
    domain::{change_review::*, task::FileDiff, *},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

pub struct LocalWorkspaceChanges {
    tools: LocalTools,
    snapshots: LocalCandidates,
    snapshot_root: PathBuf,
}
#[derive(Serialize, Deserialize)]
struct Receipt {
    run_id: RunId,
    activity_id: ActivityId,
    handler: DefinitionId,
    input: Value,
    result: Value,
}
impl LocalWorkspaceChanges {
    pub fn new(snapshot_root: PathBuf, workspaces: PathBuf) -> Self {
        Self {
            tools: LocalTools::new(workspaces.clone()),
            snapshots: LocalCandidates::new(snapshot_root.clone(), workspaces),
            snapshot_root,
        }
    }
    fn collect(
        &self,
        workspace: &Path,
        execution: &ActivityExecution,
        request: &ReviewRequest,
    ) -> Result<WorkspaceChangeSet, Error> {
        let baseline = request
            .baseline
            .as_ref()
            .ok_or_else(|| Error::Unavailable("review requires the saved task baseline".into()))?;
        let original = self.snapshots.verify(baseline)?;
        let files = manifest(workspace)?;
        let before: BTreeMap<_, _> = baseline.files.iter().map(|f| (f.path.clone(), f)).collect();
        let after: BTreeMap<_, _> = files.iter().map(|f| (f.path.clone(), f)).collect();
        let names: BTreeSet<_> = before.keys().chain(after.keys()).cloned().collect();
        let mut diffs = vec![];
        let mut patch_bytes = 0usize;
        for name in names {
            if before.get(&name) == after.get(&name) {
                continue;
            }
            if !tools::portable_file_path(&name) || name.chars().any(char::is_control) {
                return Err(Error::Unavailable(
                    "review requires portable file paths".into(),
                ));
            }
            let old = if before.contains_key(&name) {
                read_with_limit(&original.join(&name), 1_048_576)?
            } else {
                vec![]
            };
            let new = if after.contains_key(&name) {
                read_with_limit(&workspace.join(&name), 1_048_576)?
            } else {
                vec![]
            };
            if before.get(&name).is_some_and(|f| hash(&old) != f.sha256)
                || after.get(&name).is_some_and(|f| hash(&new) != f.sha256)
            {
                return Err(Error::Unavailable(
                    "workspace changed while collecting review".into(),
                ));
            }
            let old = std::str::from_utf8(&old)
                .map_err(|_| Error::Unavailable("changed files must be UTF-8".into()))?;
            let new = std::str::from_utf8(&new)
                .map_err(|_| Error::Unavailable("changed files must be UTF-8".into()))?;
            if old.contains('\0') || new.contains('\0') {
                return Err(Error::Unavailable("changed files must be text".into()));
            }
            let (mut patch, changed_bytes) = patch(
                &name,
                before.contains_key(&name),
                after.contains_key(&name),
                old,
                new,
            );
            // Empty file creation/deletion has no text hunk; Git's mode headers preserve it.
            if old.is_empty() && new.is_empty() {
                let mode = if before.contains_key(&name) {
                    "deleted"
                } else {
                    "new"
                };
                patch = format!("diff --git a/{name} b/{name}\n{mode} file mode 100644\n");
            }
            patch_bytes += patch.len();
            if patch_bytes > MAX_PATCH_BYTES {
                return Err(Error::Unavailable("review patch exceeds 4 MiB".into()));
            }
            diffs.push(FileDiff {
                path: name.clone(),
                before_sha256: before.get(&name).map(|f| f.sha256.clone()),
                after_sha256: after.get(&name).map(|f| f.sha256.clone()),
                patch,
                changed_bytes,
            });
        }
        if manifest(workspace)? != files || self.snapshots.verify(baseline).is_err() {
            return Err(Error::Unavailable(
                "workspace or baseline changed during review".into(),
            ));
        }
        let workspace_digest = digest(&files)?;
        let test_status = match &request.tests {
            None => TestEvidenceStatus::NotRun,
            Some(t) if t.before_digest == t.after_digest && t.after_digest == workspace_digest => {
                TestEvidenceStatus::Matching
            }
            Some(_) => TestEvidenceStatus::Stale,
        };
        let result = WorkspaceChangeSet {
            version: 1,
            run_id: execution.run_id,
            activity_id: execution.activity_id,
            baseline_digest: baseline.digest.clone(),
            workspace_digest,
            changed_bytes: diffs.iter().map(|f| f.changed_bytes).sum(),
            files: diffs,
            test_status,
            tests: request.tests.clone(),
        };
        result.validate()?;
        Ok(result)
    }
    fn execute_sync(&self, execution: ActivityExecution) -> Result<Value, Error> {
        if !is_handler(&execution.handler) {
            return Err(Error::Unavailable(
                "unsupported change-review operation".into(),
            ));
        }
        let request: ReviewRequest = serde_json::from_value(execution.input.clone())?;
        if execution.handler != handler("changes")
            && (request.baseline.is_some() || request.tests.is_some())
        {
            return Err(Error::Unavailable(
                "capture does not accept a final review payload".into(),
            ));
        }
        let workspace = self.tools.workspace(&request.workspace)?;
        let _guard = self.tools.lock(&workspace)?;
        let receipt_path = workspace
            .join(PRIVATE)
            .join(format!("review-{}.json", execution.activity_id));
        match fs::symlink_metadata(&receipt_path) {
            Ok(metadata) => {
                if link(&metadata) {
                    return Err(Error::Unavailable("invalid review receipt".into()));
                }
                let receipt: Receipt = serde_json::from_slice(&read_with_limit(
                    &receipt_path,
                    MAX_REVIEW_BYTES + 4 * 1024 * 1024,
                )?)?;
                if receipt.run_id != execution.run_id
                    || receipt.activity_id != execution.activity_id
                    || receipt.handler != execution.handler
                    || receipt.input != execution.input
                {
                    return Err(Error::Unavailable(
                        "review receipt identity or input differs".into(),
                    ));
                }
                if execution.handler == handler("baseline") {
                    let snapshot: task::WorkspaceSnapshot =
                        serde_json::from_value(receipt.result.clone())?;
                    if snapshot.task_id.to_string() != execution.activity_id.to_string() {
                        return Err(Error::Unavailable(
                            "baseline receipt identity differs".into(),
                        ));
                    }
                    self.snapshots.verify(&snapshot)?;
                } else if execution.handler == handler("changes") {
                    let changes: WorkspaceChangeSet =
                        serde_json::from_value(receipt.result.clone())?;
                    changes.validate()?;
                    if changes.run_id != execution.run_id
                        || changes.activity_id != execution.activity_id
                        || changes.tests != request.tests
                        || request.baseline.as_ref().map(|b| &b.digest)
                            != Some(&changes.baseline_digest)
                    {
                        return Err(Error::Unavailable(
                            "change-set receipt differs from its pinned capture".into(),
                        ));
                    }
                } else {
                    let hash = receipt.result["digest"]
                        .as_str()
                        .ok_or_else(|| Error::Unavailable("invalid fingerprint receipt".into()))?;
                    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                        return Err(Error::Unavailable("invalid fingerprint receipt".into()));
                    }
                }
                return Ok(receipt.result);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let result = if execution.handler == handler("baseline") {
            let id: TaskId = execution
                .activity_id
                .to_string()
                .parse()
                .map_err(|_| Error::Unavailable("invalid baseline identity".into()))?;
            let existed = self.snapshot_root.join(id.to_string()).exists();
            let before = manifest(&workspace)?;
            let snapshot = self.snapshots.capture(workspace.clone(), id, true)?;
            if !existed && (snapshot.files != before || manifest(&workspace)? != before) {
                return Err(Error::Unavailable(
                    "workspace changed while capturing baseline".into(),
                ));
            }
            json!(snapshot)
        } else if execution.handler == handler("fingerprint") {
            let before = manifest(&workspace)?;
            if manifest(&workspace)? != before {
                return Err(Error::Unavailable(
                    "workspace changed while fingerprinting".into(),
                ));
            }
            json!({"digest":digest(&before)?})
        } else {
            json!(self.collect(&workspace, &execution, &request)?)
        };
        let receipt = Receipt {
            run_id: execution.run_id,
            activity_id: execution.activity_id,
            handler: execution.handler,
            input: execution.input,
            result: result.clone(),
        };
        let bytes = serde_json::to_vec(&receipt)?;
        if bytes.len() as u64 > MAX_REVIEW_BYTES + 4 * 1024 * 1024 {
            return Err(Error::Unavailable(
                "review receipt exceeds its byte limit".into(),
            ));
        }
        atomic_write(&receipt_path, &bytes)?;
        Ok(result)
    }
}
impl WorkspaceChangesPort for LocalWorkspaceChanges {
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Value> {
        Box::pin(async move { self.execute_sync(execution) })
    }
}
