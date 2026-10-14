//! Filesystem preparation and a per-file journal, coordinated by the existing Run engine.
use super::{
    candidates::patch,
    workspace::{LocalTools, PRIVATE, atomic_write, excluded, hash, link, read_with_limit},
};
use crate::{
    application::{Error, ports::*},
    domain::{patch_application::*, *},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs::{self, File},
    io::Write,
    path::{Component, Path, PathBuf},
};
mod inspection;
#[cfg(test)]
mod recovery_tests;
use crate::domain::patch_application::ApplicationFileProgress as FileProgress;

const MAX_FILE: u64 = 1024 * 1024;
const MAX_PREPARATION: u64 = 64 * 1024 * 1024;
const MAX_CONTENT: usize = 32 * 1024 * 1024;
#[cfg(test)]
type ApplicationProbe = std::sync::Arc<dyn Fn(&str) + Send + Sync>;
#[derive(Clone)]
pub struct LocalPatchApplications {
    root: PathBuf,
    #[cfg(test)]
    probe: Option<ApplicationProbe>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StagedFile {
    before: Option<String>,
    after: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    input: ApplicationInput,
    preview: ApplicationPreview,
    files: Vec<StagedFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    managed: Option<child_changes::ManagedTarget>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    run_id: RunId,
    activity_id: ActivityId,
    request_digest: String,
    started: bool,
    progress: Vec<FileProgress>,
    report: Option<ApplicationReport>,
}
fn refused(message: impl Into<String>) -> Error {
    Error::Unavailable(message.into())
}
fn json_hash<T: Serialize>(value: &T) -> Result<String, Error> {
    Ok(hash(&serde_json::to_vec(value)?))
}
pub(super) fn safe_directory(path: &Path) -> Result<PathBuf, Error> {
    if !path.is_absolute() {
        return Err(refused("target must be an absolute directory path"));
    }
    let mut current = PathBuf::new();
    for c in path.components() {
        if matches!(c, Component::ParentDir) {
            return Err(refused("parent traversal is refused"));
        }
        current.push(c);
        if matches!(c, Component::Prefix(_)) {
            continue;
        }
        let meta = fs::symlink_metadata(&current)?;
        if link(&meta) || !meta.is_dir() {
            return Err(refused(
                "target directory contains a link, reparse point, or non-directory",
            ));
        }
    }
    Ok(fs::canonicalize(path)?)
}
pub(super) fn root_identity(root: &Path) -> Result<String, Error> {
    let meta = fs::metadata(root)?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        Ok(format!("windows:{}", meta.creation_time()))
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(format!("unix:{}:{}", meta.dev(), meta.ino()))
    }
    #[cfg(not(any(windows, unix)))]
    {
        Ok(format!("created:{:?}", meta.created()?))
    }
}
// Validate every parent, including case aliases on case-insensitive targets. Parents must exist.
fn target_path(root: &Path, name: &str) -> Result<PathBuf, Error> {
    if !tools::portable_file_path(name)
        || name.chars().any(char::is_control)
        || name.split('/').any(excluded)
    {
        return Err(refused("invalid application file path"));
    }
    let components: Vec<_> = name.split('/').collect();
    let mut path = root.to_path_buf();
    for (index, component) in components.iter().enumerate() {
        let mut count = 0;
        for entry in fs::read_dir(&path)? {
            count += 1;
            if count > 10_000 {
                return Err(refused("target directory exceeds 10,000 entries"));
            }
            let entry = entry?;
            let sibling = entry.file_name();
            let sibling = sibling.to_string_lossy();
            if sibling.eq_ignore_ascii_case(component) && sibling != *component {
                return Err(refused("target contains a case-aliased path"));
            }
        }
        path.push(component);
        match fs::symlink_metadata(&path) {
            Ok(meta) => {
                if link(&meta) {
                    return Err(refused("target file path contains a link or reparse point"));
                }
                if index + 1 == components.len() {
                    if !meta.is_file() {
                        return Err(refused("target path is not a regular file"));
                    }
                } else if !meta.is_dir() {
                    return Err(refused("target parent is not a directory"));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && index + 1 == components.len() => {
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(path)
}
fn contents(path: &Path) -> Result<Option<String>, Error> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if link(&meta) || !meta.is_file() {
                return Err(refused("target must be a regular, unlinked file"));
            }
            let bytes = read_with_limit(path, MAX_FILE)?;
            let text =
                String::from_utf8(bytes).map_err(|_| refused("target file must be UTF-8"))?;
            if text.contains('\0') {
                return Err(refused("target file must be text"));
            }
            Ok(Some(text))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn text_hash(text: &Option<String>) -> Option<String> {
    text.as_ref().map(|s| hash(s.as_bytes()))
}

/// Decode the canonical single-hunk format emitted by our saved review adapter, without Git.
/// Exact context, line counts, canonical re-encoding, and SHA-256 all have to agree.
fn reconstruct(diff: &task::FileDiff, before: &Option<String>) -> Result<Option<String>, Error> {
    let old = before.as_deref().unwrap_or("");
    let empty_mode = format!(
        "diff --git a/{0} b/{0}\n{1} file mode 100644\n",
        diff.path,
        if diff.before_sha256.is_some() {
            "deleted"
        } else {
            "new"
        }
    );
    let new = if diff.patch == empty_mode && old.is_empty() {
        String::new()
    } else {
        let header = format!(
            "--- {}\n+++ {}\n",
            if diff.before_sha256.is_some() {
                format!("a/{}", diff.path)
            } else {
                "/dev/null".into()
            },
            if diff.after_sha256.is_some() {
                format!("b/{}", diff.path)
            } else {
                "/dev/null".into()
            }
        );
        let body = diff
            .patch
            .strip_prefix(&header)
            .ok_or_else(|| refused("saved patch has invalid file headers"))?;
        let (hunk, lines) = body
            .split_once('\n')
            .ok_or_else(|| refused("saved patch is missing a hunk"))?;
        let coordinates = hunk
            .strip_prefix("@@ -")
            .and_then(|s| s.strip_suffix(" @@"))
            .ok_or_else(|| refused("invalid patch hunk"))?;
        let (a, b) = coordinates
            .split_once(" +")
            .ok_or_else(|| refused("invalid patch coordinates"))?;
        let coord = |s: &str| -> Result<(usize, usize), Error> {
            let (start, count) = s
                .split_once(',')
                .ok_or_else(|| refused("invalid patch range"))?;
            Ok((
                start.parse().map_err(|_| refused("invalid patch range"))?,
                count.parse().map_err(|_| refused("invalid patch range"))?,
            ))
        };
        let (a_start, a_count) = coord(a)?;
        let (b_start, b_count) = coord(b)?;
        let start = if a_count == 0 {
            a_start
        } else {
            a_start
                .checked_sub(1)
                .ok_or_else(|| refused("invalid old patch offset"))?
        };
        let new_start = if b_count == 0 {
            b_start
        } else {
            b_start
                .checked_sub(1)
                .ok_or_else(|| refused("invalid new patch offset"))?
        };
        let source: Vec<_> = old.split_inclusive('\n').collect();
        if start > source.len() || start != new_start {
            return Err(refused("patch offset differs from target"));
        }
        let mut output = source[..start].concat();
        let mut consumed = 0usize;
        let mut produced = 0usize;
        let mut lines = lines.split_inclusive('\n').peekable();
        while let Some(line) = lines.next() {
            let marker = line
                .as_bytes()
                .first()
                .copied()
                .ok_or_else(|| refused("empty patch line"))?;
            if !matches!(marker, b' ' | b'-' | b'+') {
                return Err(refused("invalid patch line"));
            }
            let mut text = &line[1..];
            if lines.peek() == Some(&"\\ No newline at end of file\n") {
                lines.next();
                text = text
                    .strip_suffix('\n')
                    .ok_or_else(|| refused("invalid newline marker"))?;
            }
            if marker != b'+' {
                if source.get(start + consumed) != Some(&text) {
                    return Err(refused("patch context differs from target"));
                }
                consumed += 1;
            }
            if marker != b'-' {
                output.push_str(text);
                produced += 1;
            }
            if output.len() as u64 > MAX_FILE {
                return Err(refused("prepared file exceeds 1 MiB"));
            }
        }
        if consumed != a_count || produced != b_count || start + consumed > source.len() {
            return Err(refused("patch line counts differ"));
        }
        output.push_str(&source[start + consumed..].concat());
        let (canonical, changed_bytes) = patch(
            &diff.path,
            diff.before_sha256.is_some(),
            diff.after_sha256.is_some(),
            old,
            &output,
        );
        if canonical != diff.patch || changed_bytes != diff.changed_bytes {
            return Err(refused("saved patch is not canonical"));
        }
        output
    };
    if new.len() as u64 > MAX_FILE || new.contains('\0') {
        return Err(refused("prepared file exceeds the text limits"));
    }
    let after = diff.after_sha256.as_ref().map(|_| new.clone());
    if text_hash(before) != diff.before_sha256
        || text_hash(&after) != diff.after_sha256
        || (after.is_none() && !new.is_empty())
    {
        return Err(refused("saved patch does not match its file hashes"));
    }
    Ok(after)
}

impl LocalPatchApplications {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            #[cfg(test)]
            probe: None,
        }
    }
    fn probe(&self, boundary: &str) {
        #[cfg(test)]
        if let Some(probe) = &self.probe {
            probe(boundary);
        }
        #[cfg(not(test))]
        let _ = boundary;
    }
    pub(super) fn directory(&self, id: ActivityId) -> Result<PathBuf, Error> {
        if !self.root.is_absolute() {
            return Err(refused("application storage root must be absolute"));
        }
        let mut current = PathBuf::new();
        for component in self.root.components() {
            if matches!(component, Component::ParentDir) {
                return Err(refused("application storage cannot traverse parents"));
            }
            current.push(component);
            if matches!(component, Component::Prefix(_)) {
                continue;
            }
            match fs::symlink_metadata(&current) {
                Ok(meta) if link(&meta) || !meta.is_dir() => {
                    return Err(refused(
                        "application storage contains a link or non-directory",
                    ));
                }
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => fs::create_dir(&current)?,
                Err(e) => return Err(e.into()),
            }
        }
        let root = safe_directory(&fs::canonicalize(&self.root)?)?;
        // Also reject links in the originally configured adapter path.
        safe_directory(&self.root)?;
        let directory = root.join(id.to_string());
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        safe_directory(&directory)
    }
    fn load_preparation(&self, path: &Path) -> Result<Preparation, Error> {
        if link(&fs::symlink_metadata(path)?) {
            return Err(refused("invalid application preparation receipt"));
        }
        let preparation: Preparation =
            serde_json::from_slice(&read_with_limit(path, MAX_PREPARATION)?)?;
        preparation.preview.validate_for(&preparation.input)?;
        if json_hash(&preparation.input)? != preparation.preview.input_digest {
            return Err(refused("preparation input digest differs"));
        }
        if preparation.preview.has_conflicts() {
            if !preparation.files.is_empty() {
                return Err(refused("conflicted preview contains staged writes"));
            }
        } else {
            if preparation.files.len() != preparation.input.changes.files.len() {
                return Err(refused("prepared file count differs"));
            }
            let mut total = 0usize;
            for (staged, diff) in preparation
                .files
                .iter()
                .zip(&preparation.input.changes.files)
            {
                for content in staged.before.iter().chain(&staged.after) {
                    total += content.len();
                    if content.len() as u64 > MAX_FILE
                        || content.contains('\0')
                        || total > MAX_CONTENT
                    {
                        return Err(refused("prepared content exceeds its text bounds"));
                    }
                }
                if reconstruct(diff, &staged.before)? != staged.after {
                    return Err(refused("prepared bytes differ from the saved artifact"));
                }
            }
        }
        Ok(preparation)
    }
    fn prepare(&self, execution: &ActivityExecution) -> Result<ApplicationPreview, Error> {
        self.prepare_for(execution, None)
    }
    pub(super) fn prepare_managed(
        &self,
        execution: &ActivityExecution,
        tools: &LocalTools,
        target: &child_changes::ManagedTarget,
    ) -> Result<ApplicationPreview, Error> {
        self.prepare_for(execution, Some((tools, target)))
    }
    fn prepare_for(
        &self,
        execution: &ActivityExecution,
        managed: Option<(&LocalTools, &child_changes::ManagedTarget)>,
    ) -> Result<ApplicationPreview, Error> {
        let input: ApplicationInput = serde_json::from_value(execution.input.clone())?;
        input.validate()?;
        if let Some((_, target)) = managed {
            target.validate()?;
            if input.target != target.workspace {
                return Err(refused("import target differs from pinned workspace"));
            }
        }
        let directory = self.directory(execution.activity_id)?;
        let receipt = directory.join("prepared.json");
        if fs::symlink_metadata(&receipt).is_ok() {
            let saved = self.load_preparation(&receipt)?;
            if saved.preview.run_id != execution.run_id
                || saved.preview.preparation_id != execution.activity_id
                || json!(saved.input) != json!(input)
                || saved.managed.as_ref() != managed.map(|(_, target)| target)
            {
                return Err(refused("preparation receipt identity differs"));
            }
            return Ok(saved.preview);
        }
        let target = safe_directory(Path::new(&input.target))?;
        let _guard = if let Some((tools, pinned)) = managed {
            if tools.workspace(&pinned.workspace)? != target {
                return Err(refused("invalid managed import target"));
            }
            Some(tools.lock(&target)?)
        } else {
            None
        };
        if managed.is_none() && target.join(PRIVATE).join("managed").try_exists()? {
            return Err(refused(
                "application target cannot be a managed actor workspace",
            ));
        }
        if target.starts_with(safe_directory(&self.root)?) {
            return Err(refused(
                "application target cannot contain application receipts",
            ));
        }
        let identity = root_identity(&target)?;
        let mut files = vec![];
        let mut staged = vec![];
        let mut total = 0usize;
        for diff in &input.changes.files {
            let mut file = ApplicationFile {
                path: diff.path.clone(),
                before_sha256: diff.before_sha256.clone(),
                after_sha256: diff.after_sha256.clone(),
                observed_sha256: None,
                conflict: None,
            };
            let preparation = (|| {
                if managed.is_some_and(|(_, pinned)| !pinned.permits(&diff.path)) {
                    return Err(refused(
                        "child patch path is outside the parent's writable paths",
                    ));
                }
                let path = target_path(&target, &diff.path)?;
                let before = contents(&path)?;
                file.observed_sha256 = text_hash(&before);
                if file.observed_sha256 != file.before_sha256 {
                    return Err(refused("target differs from the task baseline"));
                }
                let after = reconstruct(diff, &before)?;
                total +=
                    before.as_ref().map_or(0, String::len) + after.as_ref().map_or(0, String::len);
                if total > MAX_CONTENT {
                    return Err(refused("prepared before/after content exceeds 32 MiB"));
                }
                Ok(StagedFile { before, after })
            })();
            match preparation {
                Ok(content) => staged.push(content),
                Err(e) => file.conflict = Some(e.to_string()),
            }
            files.push(file);
        }
        if safe_directory(&target)? != target || root_identity(&target)? != identity {
            return Err(refused("target directory changed during preparation"));
        }
        // A preview is only published when all initially observed changed files still agree.
        for file in &mut files {
            if file.conflict.is_none() {
                match target_path(&target, &file.path).and_then(|p| contents(&p)) {
                    Ok(content) if text_hash(&content) == file.observed_sha256 => {}
                    _ => file.conflict = Some("target changed during preparation".into()),
                }
            }
        }
        let preview = ApplicationPreview {
            run_id: execution.run_id,
            preparation_id: execution.activity_id,
            input_digest: json_hash(&input)?,
            target: target.to_string_lossy().into_owned(),
            root_identity: identity,
            files,
            test_status: input.changes.test_status,
            tests: input.changes.tests.clone(),
        };
        if preview.has_conflicts() {
            staged.clear();
        }
        preview.validate_for(&input)?;
        let bytes = serde_json::to_vec(&Preparation {
            input,
            preview: preview.clone(),
            files: staged,
            managed: managed.map(|(_, target)| target.clone()),
        })?;
        if bytes.len() as u64 > MAX_PREPARATION {
            return Err(refused("application preparation exceeds 64 MiB"));
        }
        atomic_write(&receipt, &bytes)?;
        self.probe("prepared");
        Ok(preview)
    }
    pub(super) fn project_lock(&self, root: &Path) -> Result<File, Error> {
        let private = root.join(PRIVATE);
        match fs::create_dir(&private) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        safe_directory(&private)?;
        let path = private.join("patch-application.lock");
        if fs::symlink_metadata(&path).is_ok_and(|m| link(&m) || !m.is_file()) {
            return Err(refused("invalid project application lock"));
        }
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                Err(Error::Ownership(path.display().to_string()))
            }
            Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
        }
    }
    fn write(&self, execution: &ActivityExecution) -> Result<ApplicationReport, Error> {
        self.write_for(execution, None)
    }
    pub(super) fn write_managed(
        &self,
        execution: &ActivityExecution,
        inspection: &child_changes::Inspection,
        tools: &LocalTools,
    ) -> Result<ApplicationReport, Error> {
        inspection.validate()?;
        let preparation = self.load_preparation(
            &self
                .directory(inspection.preview.preparation_id)?
                .join("prepared.json"),
        )?;
        if preparation.input.task_id != inspection.task_id
            || preparation.input.changes.run_id != inspection.child_run_id
            || preparation.input.changes.activity_id != inspection.artifact_id
            || preparation.preview != inspection.preview
        {
            return Err(refused("import differs from inspected child artifact"));
        }
        let mut call = execution.clone();
        call.input = json!(WriteRequest {
            input: preparation.input,
            preview: inspection.preview.clone()
        });
        self.write_for(&call, Some((tools, &inspection.request.target)))
    }
    fn write_for(
        &self,
        execution: &ActivityExecution,
        managed: Option<(&LocalTools, &child_changes::ManagedTarget)>,
    ) -> Result<ApplicationReport, Error> {
        let request: WriteRequest = serde_json::from_value(execution.input.clone())?;
        request.preview.validate_for(&request.input)?;
        if request.preview.run_id != execution.run_id || request.preview.has_conflicts() {
            return Err(refused("write does not match an applicable preview"));
        }
        let directory = self.directory(request.preview.preparation_id)?;
        let preparation = self.load_preparation(&directory.join("prepared.json"))?;
        if preparation.preview != request.preview
            || json!(preparation.input) != json!(request.input)
            || preparation.managed.as_ref() != managed.map(|(_, target)| target)
        {
            return Err(refused("approved request differs from prepared artifact"));
        }
        let journal_path = directory.join("write.json");
        let (_, mut journal) = self.load_write_journal(
            &directory,
            execution.run_id,
            execution.activity_id,
            &request,
        )?;
        if let Some(report) = journal.report {
            return Ok(report);
        }
        let target = safe_directory(Path::new(&request.preview.target))?;
        if target.to_string_lossy() != request.preview.target
            || root_identity(&target)? != request.preview.root_identity
        {
            return Err(refused("target directory identity changed since preview"));
        }
        if managed.is_none() && target.join(PRIVATE).join("managed").try_exists()? {
            return Err(refused(
                "application target became a managed actor workspace",
            ));
        }
        let _guard = if let Some((tools, pinned)) = managed {
            pinned.validate()?;
            if tools.workspace(&pinned.workspace)? != target
                || request.input.target != pinned.workspace
                || request
                    .preview
                    .files
                    .iter()
                    .any(|f| !pinned.permits(&f.path))
            {
                return Err(refused("import differs from pinned workspace permissions"));
            }
            tools.lock(&target)?
        } else {
            self.project_lock(&target)?
        };
        let mut conflicts = vec![];
        for (index, file) in request.preview.files.iter().enumerate() {
            let expected = match journal.progress[index] {
                FileProgress::Applied => &file.after_sha256,
                FileProgress::Pending => &file.before_sha256,
                FileProgress::Intended => &file.before_sha256,
            };
            let observed = target_path(&target, &file.path).and_then(|p| contents(&p));
            let acceptable = observed.as_ref().is_ok_and(|text| {
                text_hash(text) == *expected
                    || (journal.progress[index] == FileProgress::Intended
                        && text_hash(text) == file.after_sha256)
            });
            if !acceptable {
                let mut conflict = file.clone();
                conflict.observed_sha256 = observed.as_ref().ok().and_then(text_hash);
                conflict.conflict = Some(observed.err().map_or_else(
                    || "target differs from approved file hashes".into(),
                    |e| e.to_string(),
                ));
                conflicts.push(conflict);
            }
        }
        if !conflicts.is_empty() {
            if journal.started {
                return Err(refused(format!(
                    "partial application requires reconciliation; conflicting files: {}",
                    conflicts
                        .iter()
                        .map(|f| f.path.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            let report = ApplicationReport {
                run_id: execution.run_id,
                target: request.preview.target,
                status: ApplicationStatus::Conflicted,
                applied_files: vec![],
                conflicts,
            };
            journal.report = Some(report.clone());
            atomic_write(&journal_path, &serde_json::to_vec(&journal)?)?;
            return Ok(report);
        }
        // Save whole-operation intent before any project-file change.
        journal.started = true;
        atomic_write(&journal_path, &serde_json::to_vec(&journal)?)?;
        self.probe("started");
        for (index, staged) in preparation.files.iter().enumerate() {
            let file = &request.preview.files[index];
            let target = safe_directory(&target)?;
            if root_identity(&target)? != request.preview.root_identity {
                return Err(refused("target identity changed during application"));
            }
            let path = target_path(&target, &file.path)?;
            let observed = text_hash(&contents(&path)?);
            if journal.progress[index] == FileProgress::Applied {
                if observed != file.after_sha256 {
                    return Err(refused(
                        "an applied file changed; reconciliation is required",
                    ));
                }
                continue;
            }
            // Recognize only an effect covered by a saved per-file intent, never an unrelated matching edit.
            if !(journal.progress[index] == FileProgress::Intended && observed == file.after_sha256)
            {
                if observed != file.before_sha256 {
                    return Err(refused(
                        "target changed during application; reconciliation is required",
                    ));
                }
                journal.progress[index] = FileProgress::Intended;
                atomic_write(&journal_path, &serde_json::to_vec(&journal)?)?;
                self.probe("file_intent");
                if let Some(content) = &staged.after {
                    let mut temporary = tempfile::NamedTempFile::new_in(
                        path.parent().ok_or_else(|| refused("missing parent"))?,
                    )?;
                    temporary.write_all(content.as_bytes())?;
                    if file.before_sha256.is_some() {
                        temporary
                            .as_file()
                            .set_permissions(fs::metadata(&path)?.permissions())?;
                    }
                    temporary.as_file().sync_all()?;
                    // Recheck immediately before replacement; external editors do not take our lock.
                    target_path(&target, &file.path)?;
                    if text_hash(&contents(&path)?) != file.before_sha256 {
                        return Err(refused("target changed before replacement"));
                    }
                    if file.before_sha256.is_none() {
                        temporary
                            .persist_noclobber(&path)
                            .map_err(|e| Error::Io(e.error))?;
                    } else {
                        temporary.persist(&path).map_err(|e| Error::Io(e.error))?;
                    }
                } else {
                    fs::remove_file(&path)?;
                }
                #[cfg(unix)]
                File::open(path.parent().ok_or_else(|| refused("missing parent"))?)?.sync_all()?;
                self.probe("file_effect");
            }
            if text_hash(&contents(&path)?) != file.after_sha256 {
                return Err(refused("applied file verification failed"));
            }
            journal.progress[index] = FileProgress::Applied;
            atomic_write(&journal_path, &serde_json::to_vec(&journal)?)?;
            self.probe("file_saved");
        }
        for file in &request.preview.files {
            if text_hash(&contents(&target_path(&target, &file.path)?)?) != file.after_sha256 {
                return Err(refused(
                    "target changed before final application verification",
                ));
            }
        }
        let report = ApplicationReport {
            run_id: execution.run_id,
            target: request.preview.target,
            status: ApplicationStatus::Applied,
            applied_files: request
                .preview
                .files
                .iter()
                .map(|f| f.path.clone())
                .collect(),
            conflicts: vec![],
        };
        journal.report = Some(report.clone());
        atomic_write(&journal_path, &serde_json::to_vec(&journal)?)?;
        self.probe("completed");
        Ok(report)
    }
}
impl PatchApplicationPort for LocalPatchApplications {
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if !is_handler(&execution.handler) {
                return Err(refused("unsupported patch application handler"));
            }
            let writing = execution.handler == handler("write");
            let result = if writing {
                self.write(&execution).map(|r| json!(r))
            } else {
                self.prepare(&execution).map(|p| json!(p))
            };
            Ok(match result {
                Ok(result) => Outcome::Success(result),
                Err(Error::Ownership(message)) => {
                    Outcome::Failure(Failure::new("application_busy", message, true))
                }
                Err(error) if writing => Outcome::Uncertain(error.to_string()),
                Err(error) => Outcome::Failure(Failure::new(
                    "application_prepare",
                    error.to_string(),
                    matches!(error, Error::Io(_)),
                )),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Engine,
        adapters::{file_lock::FileOwnership, sqlite::SqliteStore, system_clock::SystemClock},
    };
    use std::{
        process::{Command, Stdio},
        sync::Arc,
        time::{Duration, Instant},
    };
    struct Executor(Arc<LocalPatchApplications>);
    impl ActivityExecutor for Executor {
        fn supports(&self, id: &DefinitionId) -> bool {
            is_handler(id)
        }
        fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
            self.0.execute(execution)
        }
    }
    async fn engine(root: &Path, boundary: Option<String>) -> Engine {
        let store = Arc::new(
            SqliteStore::open(root.join("application.db"))
                .await
                .unwrap(),
        );
        let mut adapter = LocalPatchApplications::new(root.join("applications"));
        if let Some(boundary) = boundary {
            let marker = root.join("boundary-reached");
            adapter.probe = Some(Arc::new(move |name| {
                if name == boundary {
                    fs::write(&marker, name).unwrap();
                    loop {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
            }));
        }
        Engine::new(
            store.clone(),
            Arc::new(Executor(Arc::new(adapter))),
            Arc::new(SystemClock),
            Arc::new(FileOwnership::new(store.database_path())),
            vec![Arc::new(PatchApplicationWorkflow)],
        )
        .unwrap()
    }
    fn change(path: &str, before: Option<&str>, after: Option<&str>) -> task::FileDiff {
        let (mut text, bytes) = patch(
            path,
            before.is_some(),
            after.is_some(),
            before.unwrap_or(""),
            after.unwrap_or(""),
        );
        if before.unwrap_or("").is_empty() && after.unwrap_or("").is_empty() {
            text = format!(
                "diff --git a/{path} b/{path}\n{} file mode 100644\n",
                if before.is_some() { "deleted" } else { "new" }
            );
        }
        task::FileDiff {
            path: path.into(),
            before_sha256: before.map(|s| hash(s.as_bytes())),
            after_sha256: after.map(|s| hash(s.as_bytes())),
            patch: text,
            changed_bytes: bytes,
        }
    }
    fn input(root: &Path, files: Vec<task::FileDiff>) -> ApplicationInput {
        ApplicationInput {
            task_id: MessageId::new(),
            target: root.to_string_lossy().into_owned(),
            changes: change_review::WorkspaceChangeSet {
                version: 1,
                run_id: RunId::new(),
                activity_id: ActivityId::new(),
                baseline_digest: hash(b"baseline"),
                workspace_digest: hash(b"after"),
                changed_bytes: files.iter().map(|f| f.changed_bytes).sum(),
                files,
                test_status: change_review::TestEvidenceStatus::NotRun,
                tests: None,
            },
        }
    }
    #[tokio::test]
    async fn corrupted_patches_oversized_files_and_content_bounds_never_schedule_writes() {
        for mode in ["patch", "file_size", "content_size"] {
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("project");
            fs::create_dir(&target).unwrap();
            let mut files = vec![];
            if mode == "content_size" {
                let prefix = "unchanged line for preserved file contents\n".repeat(22_000);
                let before = format!("{prefix}old tail\n");
                let after = format!("{prefix}new tail\n");
                for n in 0..20 {
                    let path = format!("file-{n:02}.txt");
                    fs::write(target.join(&path), &before).unwrap();
                    files.push(change(&path, Some(&before), Some(&after)));
                }
            } else {
                fs::write(
                    target.join("a"),
                    if mode == "file_size" {
                        "x".repeat(MAX_FILE as usize + 1)
                    } else {
                        "old".into()
                    },
                )
                .unwrap();
                files.push(change("a", Some("old"), Some("new")));
                if mode == "patch" {
                    files[0].patch = files[0].patch.replace("+new", "+BAD");
                }
            }
            let engine = engine(dir.path(), None).await;
            let id = engine
                .start(workflow(), json!(input(&target, files)), 10)
                .await
                .unwrap();
            let mut worker = engine.worker().await.unwrap();
            let prepare = call(&mut worker).await;
            worker
                .complete(prepare.clone(), worker.execute(prepare).await.unwrap())
                .await
                .unwrap();
            let run = engine.status(id).await.unwrap();
            let report: ApplicationReport =
                serde_json::from_value(run.record().result.clone().unwrap()).unwrap();
            assert_eq!(report.status, ApplicationStatus::Conflicted, "{mode}");
            assert_eq!(run.activities().len(), 1);
            assert!(!target.join(PRIVATE).exists());
            if mode == "content_size" {
                assert!(
                    report.conflicts.iter().any(|f| f
                        .conflict
                        .as_ref()
                        .unwrap()
                        .contains("32 MiB"))
                );
            }
        }
    }
    #[tokio::test]
    async fn artifact_paths_cannot_modify_runtime_or_project_private_files() {
        for name in [
            ".act0r-tools/managed",
            ".git/config",
            "src/.env",
            "target/private",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("project");
            fs::create_dir_all(target.join("src")).unwrap();
            let engine = engine(dir.path(), None).await;
            let id = engine
                .start(
                    workflow(),
                    json!(input(&target, vec![change(name, None, Some("forged"))])),
                    10,
                )
                .await
                .unwrap();
            let mut worker = engine.worker().await.unwrap();
            let prepare = call(&mut worker).await;
            worker
                .complete(prepare.clone(), worker.execute(prepare).await.unwrap())
                .await
                .unwrap();
            let run = engine.status(id).await.unwrap();
            let report: ApplicationReport =
                serde_json::from_value(run.record().result.clone().unwrap()).unwrap();
            assert_eq!(report.status, ApplicationStatus::Conflicted, "{name}");
            assert_eq!(run.activities().len(), 1);
            assert!(!target.join(name).exists());
            assert!(!target.join(PRIVATE).exists());
        }
    }
    #[tokio::test]
    async fn linked_target_roots_and_linked_file_parents_are_refused() {
        for root_link in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let outside = dir.path().join("outside");
            fs::create_dir(&outside).unwrap();
            fs::write(outside.join("a"), "old").unwrap();
            let project = dir.path().join("project");
            fs::create_dir(&project).unwrap();
            let linked = if root_link {
                dir.path().join("linked")
            } else {
                project.join("src")
            };
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                let output = Command::new("cmd")
                    .args(["/c", "mklink", "/J"])
                    .arg(&linked)
                    .arg(&outside)
                    .creation_flags(0x08000000)
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            #[cfg(unix)]
            std::os::unix::fs::symlink(&outside, &linked).unwrap();
            let target = if root_link {
                linked.clone()
            } else {
                project.clone()
            };
            let path = if root_link { "a" } else { "src/a" };
            let engine = engine(dir.path(), None).await;
            let id = engine
                .start(
                    workflow(),
                    json!(input(&target, vec![change(path, Some("old"), Some("new"))])),
                    10,
                )
                .await
                .unwrap();
            let mut worker = engine.worker().await.unwrap();
            let prepare = call(&mut worker).await;
            worker
                .complete(prepare.clone(), worker.execute(prepare).await.unwrap())
                .await
                .unwrap();
            let run = engine.status(id).await.unwrap();
            if root_link {
                assert_eq!(run.record().status, RunStatus::Failed);
            } else {
                assert_eq!(
                    run.record().result.as_ref().unwrap()["status"],
                    "conflicted"
                );
            }
            assert_eq!(fs::read_to_string(outside.join("a")).unwrap(), "old");
            assert!(!outside.join(PRIVATE).exists());
            #[cfg(windows)]
            fs::remove_dir(&linked).unwrap();
            #[cfg(unix)]
            fs::remove_file(&linked).unwrap();
        }
    }
    async fn call(worker: &mut crate::Worker) -> ActivityExecution {
        for _ in 0..100 {
            if let Some(call) = worker.admit().await.unwrap() {
                return call;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no admitted application activity");
    }
    #[test]
    fn canonical_patch_reconstruction_covers_empty_crlf_unicode_and_no_final_newline() {
        for (before, after) in [
            (Some(""), None),
            (None, Some("")),
            (Some("a\r\nβ\r\nc"), Some("a\r\nγ\r\nc")),
            (Some("one\n"), Some("one")),
            (Some("one"), Some("one\n")),
            (
                Some("one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine"),
                Some("one\ntwo\nthree\nfour\nNEW\nsix\nseven\neight\nnine"),
            ),
            (None, Some("created\n")),
            (Some("deleted\n"), None),
        ] {
            let diff = change("file.txt", before, after);
            assert_eq!(
                reconstruct(&diff, &before.map(str::to_owned)).unwrap(),
                after.map(str::to_owned)
            );
            let mut forged = diff.clone();
            forged.patch.push_str("+forged\n");
            assert!(reconstruct(&forged, &before.map(str::to_owned)).is_err());
        }
    }
    #[tokio::test]
    async fn creation_deletion_empty_files_receipts_and_exact_input_binding() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("project");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("a"), "to delete").unwrap();
        fs::write(target.join("empty-old"), "").unwrap();
        let input = input(
            &target,
            vec![
                change("a", Some("to delete"), None),
                change("b", None, Some("new β")),
                change("empty-new", None, Some("")),
                change("empty-old", Some(""), None),
            ],
        );
        let engine = engine(dir.path(), None).await;
        let id = engine.start(workflow(), json!(input), 10).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        let prepare = call(&mut worker).await;
        let prepared = worker.execute(prepare.clone()).await.unwrap();
        assert!(matches!(prepared, Outcome::Success(_)), "{prepared:?}");
        // A repeat reuses the original published preview, even if the live target changes.
        fs::write(target.join("b"), "late").unwrap();
        assert_eq!(prepared, worker.execute(prepare.clone()).await.unwrap());
        let mut forged = prepare.clone();
        forged.input["task_id"] = json!(MessageId::new());
        assert!(matches!(
            worker.execute(forged).await.unwrap(),
            Outcome::Failure(_)
        ));
        fs::remove_file(target.join("b")).unwrap();
        worker.complete(prepare, prepared).await.unwrap();
        let wait = engine
            .status(id)
            .await
            .unwrap()
            .record()
            .wait
            .clone()
            .unwrap();
        engine
            .signal(id, wait.id, json!({"approved":true}))
            .await
            .unwrap();
        let write = call(&mut worker).await;
        let result = worker.execute(write.clone()).await.unwrap();
        assert!(matches!(result, Outcome::Success(_)), "{result:?}");
        assert!(!target.join("a").exists());
        assert!(!target.join("empty-old").exists());
        assert_eq!(fs::read_to_string(target.join("b")).unwrap(), "new β");
        assert_eq!(fs::read(target.join("empty-new")).unwrap(), b"");
        fs::write(target.join("b"), "after publication").unwrap();
        assert_eq!(result, worker.execute(write.clone()).await.unwrap());
        let mut forged = write.clone();
        forged.input["preview"]["target"] = json!("other");
        assert!(matches!(
            worker.execute(forged).await.unwrap(),
            Outcome::Uncertain(_)
        ));
        worker.complete(write, result).await.unwrap();
        assert_eq!(
            engine.status(id).await.unwrap().record().status,
            RunStatus::Succeeded
        );
        assert_eq!(
            fs::read_to_string(target.join("b")).unwrap(),
            "after publication"
        );
    }
    #[tokio::test]
    async fn project_ownership_contention_does_not_write_or_reset_saved_intent() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("project");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("a"), "old").unwrap();
        let engine = engine(dir.path(), None).await;
        let id = engine
            .start(
                workflow(),
                json!(input(&target, vec![change("a", Some("old"), Some("new"))])),
                10,
            )
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        let prepare = call(&mut worker).await;
        worker
            .complete(prepare.clone(), worker.execute(prepare).await.unwrap())
            .await
            .unwrap();
        let wait = engine
            .status(id)
            .await
            .unwrap()
            .record()
            .wait
            .clone()
            .unwrap();
        engine
            .signal(id, wait.id, json!({"approved":true}))
            .await
            .unwrap();
        let adapter = LocalPatchApplications::new(dir.path().join("applications"));
        let guard = adapter.project_lock(&target).unwrap();
        let write = call(&mut worker).await;
        let outcome = worker.execute(write.clone()).await.unwrap();
        assert!(
            matches!(&outcome,Outcome::Failure(f) if f.code == "application_busy" && f.retryable)
        );
        worker.complete(write.clone(), outcome).await.unwrap();
        assert_eq!(fs::read_to_string(target.join("a")).unwrap(), "old");
        let saved = engine.status(id).await.unwrap();
        assert_eq!(saved.activities().last().unwrap().spec.input, write.input);
        assert_eq!(saved.activities().last().unwrap().id, write.activity_id);
        assert!(saved.activities().last().unwrap().ready_at > saved.record().updated_at);
        drop(guard);
        let retry = call(&mut worker).await;
        assert_eq!(retry.activity_id, write.activity_id);
        worker
            .complete(retry.clone(), worker.execute(retry).await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            engine.status(id).await.unwrap().record().status,
            RunStatus::Succeeded
        );
    }
    #[tokio::test]
    #[ignore = "child host killed at synced application boundaries"]
    async fn killed_application_host_fixture() {
        let root = std::env::current_dir().unwrap();
        let boundary = fs::read_to_string(root.join("boundary")).unwrap();
        let engine = engine(&root, Some(boundary)).await;
        let mut worker = engine.worker().await.unwrap();
        let call = call(&mut worker).await;
        let outcome = worker.execute(call.clone()).await.unwrap();
        worker.complete(call, outcome).await.unwrap();
        panic!("host failed to stop at selected boundary");
    }
    #[tokio::test]
    async fn killed_hosts_preserve_partial_writes_and_require_explicit_reconciliation() {
        for boundary in [
            "prepared",
            "started",
            "file_intent",
            "file_effect",
            "file_saved",
            "completed",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("project");
            fs::create_dir(&target).unwrap();
            fs::write(target.join("a"), "old a").unwrap();
            fs::write(target.join("b"), "old b").unwrap();
            let engine_before = engine(dir.path(), None).await;
            let id = engine_before
                .start(
                    workflow(),
                    json!(input(
                        &target,
                        vec![
                            change("a", Some("old a"), Some("new a")),
                            change("b", Some("old b"), Some("new b"))
                        ]
                    )),
                    10,
                )
                .await
                .unwrap();
            if boundary != "prepared" {
                let mut worker = engine_before.worker().await.unwrap();
                let prepare = call(&mut worker).await;
                worker
                    .complete(prepare.clone(), worker.execute(prepare).await.unwrap())
                    .await
                    .unwrap();
                let wait = engine_before
                    .status(id)
                    .await
                    .unwrap()
                    .record()
                    .wait
                    .clone()
                    .unwrap();
                engine_before
                    .signal(id, wait.id, json!({"approved":true}))
                    .await
                    .unwrap();
            }
            drop(engine_before);
            fs::write(dir.path().join("boundary"), boundary).unwrap();
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--ignored",
                    "--exact",
                    "adapters::patch_applications::tests::killed_application_host_fixture",
                    "--nocapture",
                ])
                .current_dir(dir.path())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                command.creation_flags(0x08000000);
            }
            let mut child = command.spawn().unwrap();
            let start = Instant::now();
            while !dir.path().join("boundary-reached").exists() {
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("{boundary} child exited early: {status}");
                }
                if start.elapsed() > Duration::from_secs(30) {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("{boundary} boundary timed out");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            child.kill().unwrap();
            child.wait().unwrap();
            let engine = engine(dir.path(), None).await;
            let mut worker = engine.worker().await.unwrap();
            let recovered = engine.status(id).await.unwrap();
            if boundary == "prepared" {
                assert_eq!(recovered.record().status, RunStatus::Running);
                assert_eq!(
                    recovered.activities().last().unwrap().status,
                    ActivityStatus::Queued
                );
                let prepare = call(&mut worker).await;
                worker
                    .complete(prepare.clone(), worker.execute(prepare).await.unwrap())
                    .await
                    .unwrap();
                let wait = engine
                    .status(id)
                    .await
                    .unwrap()
                    .record()
                    .wait
                    .clone()
                    .unwrap();
                engine
                    .signal(id, wait.id, json!({"approved":true}))
                    .await
                    .unwrap();
            } else {
                assert_eq!(recovered.record().status, RunStatus::Blocked, "{boundary}");
                assert!(!worker.step().await.unwrap());
                assert!(engine.resume(id).await.is_err());
                let activity = recovered.activities().last().unwrap();
                let attempt = recovered.attempts().last().unwrap();
                if boundary == "completed" {
                    fs::write(target.join("a"), "later user edit").unwrap();
                }
                if boundary == "file_effect" {
                    // An unrelated partial-state edit must stop recovery without touching it.
                    fs::write(target.join("b"), "user conflict").unwrap();
                }
                let decision = ReconciliationRequest {
                    activity_id: activity.id,
                    attempt_id: attempt.id,
                    decision: ReconciliationDecision::Retry {},
                    evidence: "Stopped host and inspected the saved per-file journal.".into(),
                };
                engine.reconcile(id, decision.clone()).await.unwrap();
                engine.reconcile(id, decision).await.unwrap();
            }
            let write = call(&mut worker).await;
            let outcome = worker.execute(write.clone()).await.unwrap();
            worker.complete(write.clone(), outcome).await.unwrap();
            if boundary == "file_effect" {
                assert_eq!(
                    engine.status(id).await.unwrap().record().status,
                    RunStatus::Blocked
                );
                assert_eq!(fs::read_to_string(target.join("a")).unwrap(), "new a");
                assert_eq!(
                    fs::read_to_string(target.join("b")).unwrap(),
                    "user conflict"
                );
                fs::write(target.join("b"), "old b").unwrap();
                let run = engine.status(id).await.unwrap();
                engine.reconcile(id,ReconciliationRequest {activity_id:write.activity_id,attempt_id:run.attempts().last().unwrap().id,decision:ReconciliationDecision::Retry {},evidence:"Resolved the conflict to the pinned baseline; journal-owned applied file retained.".into()}).await.unwrap();
                let retry = call(&mut worker).await;
                worker
                    .complete(retry.clone(), worker.execute(retry).await.unwrap())
                    .await
                    .unwrap();
            }
            assert_eq!(
                engine.status(id).await.unwrap().record().status,
                RunStatus::Succeeded,
                "{boundary}"
            );
            assert_eq!(
                fs::read_to_string(target.join("a")).unwrap(),
                if boundary == "completed" {
                    "later user edit"
                } else {
                    "new a"
                }
            );
            assert_eq!(fs::read_to_string(target.join("b")).unwrap(), "new b");
            assert_eq!(
                engine
                    .status(id)
                    .await
                    .unwrap()
                    .record()
                    .usage
                    .admitted_model_calls,
                0
            );
        }
    }
}
