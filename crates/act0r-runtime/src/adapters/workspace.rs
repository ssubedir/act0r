use crate::{
    application::{Error, ports::*},
    domain::{
        ActivityId,
        agent::*,
        tools::{
            ToolError, WorkspaceEdit, WorkspaceTestCommand, portable_file_path,
            validate_writable_paths,
        },
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
};
use tokio::io::AsyncReadExt;

pub(super) const MAX_FILE: u64 = 1024 * 1024;
pub(super) const MAX_OUTPUT: usize = 16 * 1024;
pub(super) const PRIVATE: &str = ".act0r-tools";

pub struct LocalWorkspaces {
    root: PathBuf,
}
impl LocalWorkspaces {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }
    fn prepare_sync(&self, source: PathBuf) -> Result<String, Error> {
        let source = fs::canonicalize(source)?;
        if !source.is_dir() {
            return Err(Error::Unavailable("source must be a directory".into()));
        }
        fs::create_dir_all(&self.root)?;
        let root = fs::canonicalize(&self.root)?;
        if source.starts_with(&root) {
            return Err(Error::Unavailable(
                "source cannot be a managed workspace".into(),
            ));
        }
        let destination = root.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir(&destination)?;
        let mut counts = (0usize, 0u64);
        copy_tree(&source, &destination, &root, &mut counts)?;
        fs::create_dir(destination.join(PRIVATE))?;
        atomic_write(
            &destination.join(PRIVATE).join("managed"),
            b"act0r workspace v1\n",
        )?;
        Ok(fs::canonicalize(destination)?
            .to_string_lossy()
            .into_owned())
    }
}
impl WorkspaceFactory for LocalWorkspaces {
    fn prepare(&self, source: PathBuf) -> PortFuture<'_, String> {
        Box::pin(async move { self.prepare_sync(source) })
    }
}
pub(super) fn excluded(name: &str) -> bool {
    let name = name.trim_end_matches(['.', ' ']).to_ascii_lowercase();
    matches!(
        name.as_str(),
        ".git" | ".act0r" | ".act0r-tools" | ".build" | "target" | "node_modules" | ".aws" | ".env"
    ) || name.starts_with(".env.")
}
fn reserved_component(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or(component)
        .trim_end()
        .to_ascii_uppercase();
    matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || ["COM", "LPT"].iter().any(|prefix| {
        stem.strip_prefix(prefix).is_some_and(|suffix| {
            matches!(
                suffix,
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            )
        })
    })
}
/// Detect symbolic links and Windows reparse points before handling a local file.
pub fn link(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_type().is_symlink() || metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}
pub(super) fn copy_tree(
    source: &Path,
    destination: &Path,
    root: &Path,
    counts: &mut (usize, u64),
) -> Result<(), Error> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        if excluded(&name.to_string_lossy()) {
            continue;
        }
        let meta = fs::symlink_metadata(entry.path())?;
        if link(&meta) {
            return Err(Error::Unavailable(
                "workspace source contains a symlink or reparse point".into(),
            ));
        }
        if fs::canonicalize(entry.path())?.starts_with(root) {
            continue;
        }
        counts.0 += 1;
        counts.1 = counts
            .1
            .saturating_add(if meta.is_file() { meta.len() } else { 0 });
        if counts.0 > 10_000 || counts.1 > 64 * 1024 * 1024 {
            return Err(Error::Unavailable(
                "workspace copy exceeds 10,000 entries or 64 MiB".into(),
            ));
        }
        let target = destination.join(name);
        if meta.is_dir() {
            fs::create_dir(&target)?;
            copy_tree(&entry.path(), &target, root, counts)?;
        } else if meta.is_file() {
            fs::copy(entry.path(), target)?;
        } else {
            return Err(Error::Unavailable(
                "workspace source contains a special file".into(),
            ));
        }
    }
    Ok(())
}
/// Save a complete local file through the workspace adapter's atomic write path.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Unavailable("missing parent directory".into()))?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|e| Error::Io(e.error))?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}
pub(super) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(super) fn bounded_read(path: &Path) -> Result<Vec<u8>, Error> {
    read_with_limit(path, MAX_FILE).map_err(|error| match error {
        Error::Unavailable(message) => ToolError::new("file_limit", message).into(),
        Error::Io(error) => inspection_io(error),
        error => error,
    })
}
// Only errors from the requested inspection are feedback. Workspace/marker/lock validation
// keeps its execution-error semantics, so a model cannot repair or replace runtime authority.
pub(super) fn inspection_io(error: std::io::Error) -> Error {
    match error.kind() {
        std::io::ErrorKind::NotFound => ToolError::new(
            "file_not_found",
            "the requested path does not exist in the managed workspace",
        )
        .into(),
        std::io::ErrorKind::PermissionDenied => ToolError::new(
            "permission_denied",
            "the requested path cannot be inspected with the current filesystem permissions",
        )
        .into(),
        std::io::ErrorKind::NotADirectory => ToolError::new(
            "not_a_directory",
            "a requested path component is not a directory",
        )
        .into(),
        _ => Error::Io(error),
    }
}
pub(super) fn read_with_limit(path: &Path, limit: u64) -> Result<Vec<u8>, Error> {
    if fs::metadata(path)?.len() > limit {
        return Err(Error::Unavailable("file exceeds tool size limit".into()));
    }
    use std::io::Read;
    let mut bytes = Vec::new();
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(Error::Unavailable("file grew beyond the tool limit".into()));
    }
    Ok(bytes)
}

#[derive(Clone)]
pub struct LocalTools {
    root: PathBuf,
}
#[path = "workspace/create.rs"]
mod create;
#[path = "workspace/reads.rs"]
pub(super) mod reads;
#[path = "workspace/recovery.rs"]
mod recovery;
#[cfg(test)]
#[path = "workspace/recovery_tests.rs"]
mod recovery_tests;
#[path = "workspace/search.rs"]
mod search;
#[derive(Serialize, Deserialize)]
struct EditIntent {
    input_hash: String,
    old_hash: String,
    new_hash: String,
    content: String,
}
fn edit_rejection(feedback: bool, code: &str, message: &str) -> Error {
    if feedback {
        ToolError::new(code, message).into()
    } else {
        Error::Unavailable(message.into())
    }
}
impl LocalTools {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }
    pub(super) fn workspace(&self, workspace: &str) -> Result<PathBuf, Error> {
        if link(&fs::symlink_metadata(workspace)?) {
            return Err(Error::Unavailable(
                "symlinks and reparse points are not allowed".into(),
            ));
        }
        let path = fs::canonicalize(workspace)?;
        let root = fs::canonicalize(&self.root)?;
        if path.parent() != Some(root.as_path()) {
            return Err(Error::Unavailable(
                "tools require a managed workspace".into(),
            ));
        }
        for component in [
            path.clone(),
            path.join(PRIVATE),
            path.join(PRIVATE).join("managed"),
        ] {
            if link(&fs::symlink_metadata(component)?) {
                return Err(Error::Unavailable(
                    "symlinks and reparse points are not allowed".into(),
                ));
            }
        }
        if fs::read(path.join(PRIVATE).join("managed"))? != b"act0r workspace v1\n" {
            return Err(Error::Unavailable("invalid workspace marker".into()));
        }
        Ok(path)
    }
    pub(super) fn file(&self, workspace: &Path, name: &str) -> Result<PathBuf, Error> {
        let target = self.entry(workspace, name)?;
        if !target.is_file() {
            return Err(
                ToolError::new("not_a_file", "the requested path is not a regular file").into(),
            );
        }
        Ok(target)
    }
    pub(super) fn directory(&self, workspace: &Path, name: &str) -> Result<PathBuf, Error> {
        let target = if name == "." {
            workspace.to_path_buf()
        } else {
            self.entry(workspace, name)?
        };
        if !target.is_dir() {
            return Err(
                ToolError::new("not_a_directory", "the requested path is not a directory").into(),
            );
        }
        Ok(target)
    }
    fn entry(&self, workspace: &Path, name: &str) -> Result<PathBuf, Error> {
        let relative = Path::new(name);
        if name.is_empty()
            || name.contains('\0')
            || name.contains(':')
            // Tool paths use '/' on every host; Unix treats '\\' as a filename character.
            || name.contains('\\')
            || relative
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
            || relative
                .components()
                .any(|c| excluded(&c.as_os_str().to_string_lossy()))
        {
            return Err(ToolError::new(
                "path_refused",
                "path must be a permitted relative file path using '/' separators",
            )
            .into());
        }
        let mut target = workspace.to_path_buf();
        for component in relative.components() {
            target.push(component);
            if link(&fs::symlink_metadata(&target).map_err(inspection_io)?) {
                return Err(ToolError::new(
                    "path_refused",
                    "symlinks and reparse points are not allowed",
                )
                .into());
            }
        }
        let target = fs::canonicalize(target).map_err(inspection_io)?;
        if !target.starts_with(workspace) {
            return Err(ToolError::new("path_refused", "path is outside the workspace").into());
        }
        Ok(target)
    }
    fn edit(
        &self,
        workspace: &Path,
        request: &ToolRequest,
        activity: ActivityId,
        feedback: bool,
    ) -> Result<Value, Error> {
        let AgentAction::ReplaceText { path, old, new } = &request.action else {
            return Err(Error::Unavailable(
                "edit requires a replacement action".into(),
            ));
        };
        let journal = workspace.join(PRIVATE).join(format!("{activity}.json"));
        let has_intent = match fs::symlink_metadata(&journal) {
            Ok(metadata) if link(&metadata) => {
                return Err(Error::Unavailable("invalid edit journal".into()));
            }
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        // Once an intent exists, a refused path or changed file cannot be treated as
        // a fresh model-correctable rejection: a previous attempt may have published it.
        let preflight = || -> Result<(PathBuf, Vec<u8>), Error> {
            if feedback && !portable_file_path(path) {
                return Err(ToolError::new(
                    "path_refused",
                    "edit paths must use permitted relative components and '/' separators",
                )
                .into());
            }
            let target = self.file(workspace, path)?;
            let normalized = path.replace('\\', "/");
            let allowed = request.writable_paths.iter().any(|p| {
                if p.ends_with('/') {
                    normalized.starts_with(p)
                } else {
                    normalized == *p
                }
            });
            if !allowed {
                return Err(edit_rejection(
                    feedback,
                    "path_not_writable",
                    "file is outside the configured writable paths",
                ));
            }
            Ok((target.clone(), bounded_read(&target)?))
        };
        let (target, current) = preflight().map_err(|error| {
            if has_intent && feedback {
                Error::Unavailable("cannot verify a journaled edit; reconciliation required".into())
            } else {
                error
            }
        })?;
        let input_hash = hash(&serde_json::to_vec(request)?);
        let intent: EditIntent = if has_intent {
            let intent: EditIntent =
                serde_json::from_slice(&read_with_limit(&journal, MAX_FILE * 8)?)?;
            if intent.input_hash != input_hash {
                return Err(Error::Unavailable(
                    "activity identity reused with different tool input".into(),
                ));
            }
            if intent.content.len() as u64 > MAX_FILE
                || hash(intent.content.as_bytes()) != intent.new_hash
            {
                return Err(Error::Unavailable(
                    "invalid edit journal content; reconciliation required".into(),
                ));
            }
            intent
        } else {
            let content = String::from_utf8(current.clone()).map_err(|_| {
                edit_rejection(feedback, "invalid_utf8", "editing requires UTF-8 text")
            })?;
            let matches = content.match_indices(old.as_str()).take(2).count();
            if !feedback && (old.is_empty() || matches != 1) {
                return Err(Error::Unavailable(
                    "old text must match exactly once".into(),
                ));
            }
            if old.is_empty() {
                return Err(ToolError::new("empty_old_text", "old text must be nonempty").into());
            }
            match matches {
                0 => {
                    return Err(ToolError::new(
                        "text_not_found",
                        "old text does not match the file",
                    )
                    .into());
                }
                1 => {}
                _ => {
                    return Err(ToolError::new(
                        "text_not_unique",
                        "old text must match exactly once",
                    )
                    .into());
                }
            }
            let content = content.replacen(old.as_str(), new, 1);
            if content.len() as u64 > MAX_FILE {
                return Err(edit_rejection(
                    feedback,
                    "file_limit",
                    "replacement exceeds file size limit",
                ));
            }
            let intent = EditIntent {
                input_hash,
                old_hash: hash(&current),
                new_hash: hash(content.as_bytes()),
                content,
            };
            atomic_write(&journal, &serde_json::to_vec(&intent)?)?;
            intent
        };
        let current_hash = hash(&current);
        if current_hash == intent.new_hash {
            return Ok(json!({"ok":true,"path":path,"already_applied":true}));
        }
        if current_hash != intent.old_hash {
            return Err(Error::Unavailable(
                "file changed since edit intent; reconciliation required".into(),
            ));
        }
        atomic_write(&target, intent.content.as_bytes())?;
        Ok(json!({"ok":true,"path":path,"already_applied":false}))
    }
    async fn run(&self, request: ToolRequest, activity: ActivityId) -> Result<Value, Error> {
        self.run_with_feedback(request, activity, false).await
    }
    async fn run_with_feedback(
        &self,
        request: ToolRequest,
        activity: ActivityId,
        feedback: bool,
    ) -> Result<Value, Error> {
        let workspace = self.workspace(&request.workspace)?;
        let _guard = self.lock(&workspace)?;
        match &request.action {
            AgentAction::ListFiles => {
                let mut paths = vec![];
                list(&workspace, &workspace, &mut paths)?;
                paths.sort();
                Ok(json!({"ok":true,"files":paths}))
            }
            AgentAction::ReadFile { path } => {
                let content = bounded_read(&self.file(&workspace, path)?)?;
                let text = String::from_utf8(content)
                    .map_err(|_| ToolError::new("invalid_utf8", "reading requires UTF-8 text"))?;
                let mut end = text.len().min(MAX_OUTPUT);
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                Ok(json!({"ok":true,"path":path,"text":&text[..end],"truncated":end < text.len()}))
            }
            AgentAction::ReplaceText { .. } => self.edit(&workspace, &request, activity, feedback),
            AgentAction::RunTests => run_tests(&workspace, &request.test_command).await,
            _ => Err(Error::Unavailable("action is not a workspace tool".into())),
        }
    }
    pub(super) fn lock(&self, workspace: &Path) -> Result<File, Error> {
        let lock_path = workspace.join(PRIVATE).join("ownership.lock");
        if lock_path.exists() && link(&fs::symlink_metadata(&lock_path)?) {
            return Err(Error::Unavailable("invalid tool lock".into()));
        }
        let guard = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        guard
            .try_lock()
            .map_err(|_| Error::Ownership("another tool owns this workspace".into()))?;
        Ok(guard)
    }
}
fn list(root: &Path, path: &Path, result: &mut Vec<String>) -> Result<(), Error> {
    for entry in fs::read_dir(path).map_err(inspection_io)? {
        let entry = entry.map_err(inspection_io)?;
        if excluded(&entry.file_name().to_string_lossy()) {
            continue;
        }
        let meta = fs::symlink_metadata(entry.path()).map_err(inspection_io)?;
        if link(&meta) {
            return Err(ToolError::new(
                "path_refused",
                "workspace contains a symlink or reparse point",
            )
            .into());
        }
        if meta.is_dir() {
            list(root, &entry.path(), result)?;
        } else if meta.is_file() {
            if result.len() >= 1_000 {
                return Err(
                    ToolError::new("listing_limit", "file listing exceeds 1,000 files").into(),
                );
            }
            result.push(
                entry
                    .path()
                    .strip_prefix(root)
                    .map_err(|_| Error::Unavailable("invalid workspace path".into()))?
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
    Ok(())
}
async fn capture(mut reader: impl tokio::io::AsyncRead + Unpin) -> Result<(String, bool), Error> {
    let mut retained = Vec::new();
    let mut truncated = false;
    let mut buffer = [0u8; 4096];
    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        let keep = n.min(MAX_OUTPUT - retained.len());
        retained.extend_from_slice(&buffer[..keep]);
        truncated |= keep < n;
    }
    Ok((String::from_utf8_lossy(&retained).into_owned(), truncated))
}
pub(super) async fn run_tests(workspace: &Path, args: &[String]) -> Result<Value, Error> {
    execute_command(workspace, args, None).await
}
pub(super) async fn execute_command(
    workspace: &Path,
    args: &[String],
    timeout_ms: Option<u32>,
) -> Result<Value, Error> {
    let executable = args
        .first()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Unavailable("command is empty".into()))?;
    let mut command = tokio::process::Command::new(executable);
    command
        .args(&args[1..])
        .current_dir(workspace)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    // Copied candidates can share package identities and file timestamps. A
    // parent build cache can then reuse another branch's compiled test binaries.
    command.env("CARGO_TARGET_DIR", workspace.join("target"));
    // Project processes do not inherit API keys, regardless of provider.
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .ends_with("_API_KEY")
        {
            command.env_remove(name);
        }
    }
    super::process_tree::ProcessTree::configure(&mut command);
    let started = std::time::Instant::now();
    let mut child = command.spawn()?;
    let tree = match super::process_tree::ProcessTree::attach(&child) {
        Ok(tree) => tree,
        Err(error) => {
            child.kill().await?;
            return Err(error.into());
        }
    };
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Unavailable("missing command stdout".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::Unavailable("missing command stderr".into()))?;
    let collect = async {
        tokio::try_join!(
            async {
                let status = child.wait().await?;
                // A completed parent must not leave background children holding pipes
                // or modifying the workspace after ownership is released.
                tree.terminate().await?;
                Ok::<_, Error>(status)
            },
            capture(stdout),
            capture(stderr)
        )
    };
    let result = match timeout_ms {
        Some(ms) => tokio::time::timeout(
            std::time::Duration::from_millis(u64::from(ms)).saturating_sub(started.elapsed()),
            collect,
        )
        .await
        .map_err(|_| {
            Error::Unavailable("command process timed out; reconciliation required".into())
        })
        .and_then(|result| result),
        None => collect.await,
    };
    let (status, stdout, stderr) = match result {
        Ok(result) => result,
        Err(error) => {
            tree.terminate().await?;
            child.wait().await?;
            return Err(error);
        }
    };
    let mut result = json!({"ok":status.success(),"exit_code":status.code(),"stdout":stdout.0,"stderr":stderr.0,"output_truncated":stdout.1 || stderr.1});
    if timeout_ms.is_some() {
        result["elapsed_ms"] =
            json!(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
        result["stdout_truncated"] = json!(stdout.1);
        result["stderr_truncated"] = json!(stderr.1);
    }
    Ok(result)
}
impl WorkspaceTestRunner for LocalTools {
    fn run_tests(
        &self,
        workspace: String,
        command: WorkspaceTestCommand,
        _activity: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            command.validate()?;
            let workspace = self.workspace(&workspace)?;
            let _guard = self.lock(&workspace)?;
            execute_command(&workspace, &command.argv, Some(command.timeout_ms)).await
        })
    }
}
impl WorkspaceCommandRunner for LocalTools {
    fn run_command(
        &self,
        workspace: String,
        selected: crate::domain::tools::NamedWorkspaceCommand,
        _activity: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            selected.command.validate()?;
            let workspace = self.workspace(&workspace)?;
            let _guard = self.lock(&workspace)?;
            let mut result = execute_command(
                &workspace,
                &selected.command.argv,
                Some(selected.command.timeout_ms),
            )
            .await?;
            result["command_name"] = json!(selected.name);
            Ok(result)
        })
    }
}
impl ToolRunner for LocalTools {
    fn execute(&self, request: ToolRequest, activity: ActivityId) -> PortFuture<'_, Value> {
        Box::pin(async move {
            match self.run(request, activity).await {
                Ok(result) => Ok(result),
                Err(error) => Ok(json!({"ok":false,"error":error.to_string()})),
            }
        })
    }
}
impl WorkspaceReader for LocalTools {
    fn read_files(
        &self,
        workspace: String,
        batch: crate::domain::tools::WorkspaceReadBatch,
        _activity: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move { self.read_files_sync(workspace, batch) })
    }
    fn search_text(
        &self,
        workspace: String,
        search: crate::domain::tools::WorkspaceSearch,
        _activity: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move { self.search_sync(workspace, search) })
    }
    fn inspect(
        &self,
        workspace: String,
        action: crate::domain::tools::WorkspaceReadAction,
        activity: ActivityId,
    ) -> PortFuture<'_, Value> {
        let action = match action {
            crate::domain::tools::WorkspaceReadAction::ListFiles => AgentAction::ListFiles,
            crate::domain::tools::WorkspaceReadAction::ReadFile { path } => {
                AgentAction::ReadFile { path }
            }
        };
        Box::pin(async move {
            self.run(
                ToolRequest {
                    workspace,
                    action,
                    test_command: vec![],
                    writable_paths: vec![],
                },
                activity,
            )
            .await
        })
    }
}
impl WorkspaceWriter for LocalTools {
    fn create_file(
        &self,
        workspace: String,
        writable_paths: Vec<String>,
        create: crate::domain::tools::WorkspaceCreate,
        activity: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move { self.create_sync(workspace, writable_paths, create, activity) })
    }
    fn replace_text(
        &self,
        workspace: String,
        writable_paths: Vec<String>,
        edit: WorkspaceEdit,
        activity: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            validate_writable_paths(&writable_paths)?;
            if writable_paths.is_empty() {
                return Err(Error::Unavailable(
                    "workspace writes require pinned writable paths".into(),
                ));
            }
            self.run_with_feedback(
                ToolRequest {
                    workspace,
                    writable_paths,
                    test_command: vec![],
                    action: AgentAction::ReplaceText {
                        path: edit.path,
                        old: edit.old,
                        new: edit.new,
                    },
                },
                activity,
                true,
            )
            .await
        })
    }
}
