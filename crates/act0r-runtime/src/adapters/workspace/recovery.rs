//! Read-only evidence for interrupted native operations. Never acquires or creates a lock.
use super::*;
use crate::domain::{
    tool_recovery::*,
    tools::{NativeToolRequest, WorkspaceCreate},
};

fn journal_exists(path: &Path) -> Result<bool, Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if link(&metadata) || !metadata.is_file() => {
            Err(Error::Unavailable("invalid tool journal".into()))
        }
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
impl LocalTools {
    fn inspect_sync(
        &self,
        activity: ActivityId,
        request: NativeToolRequest,
    ) -> Result<ToolInspection, Error> {
        let workspace = request
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unavailable("saved tool has no workspace".into()))?;
        let root = self.workspace(workspace)?;
        let mut view = ToolInspection {
            workspace: workspace.clone(),
            journal_exists: false,
            state: ToolEffectState::NotStarted,
            path: None,
            before_sha256: None,
            after_sha256: None,
            observed_sha256: None,
            conflict: None,
        };
        if request.definition.id.name == "run_tests" {
            view.state = ToolEffectState::TestOutcomeUnknown;
            return Ok(view);
        }
        if request.definition.id.name == "run_command" {
            view.state = ToolEffectState::CommandOutcomeUnknown;
            return Ok(view);
        }
        let journal = root.join(PRIVATE).join(format!("{activity}.json"));
        view.journal_exists = journal_exists(&journal)?;
        match request.definition.id.name.as_str() {
            "replace_text" => {
                let edit: WorkspaceEdit = serde_json::from_value(request.arguments)?;
                view.path = Some(edit.path.clone());
                if !portable_file_path(&edit.path) {
                    return Err(Error::Unavailable("invalid saved edit path".into()));
                }
                let target = self.file(&root, &edit.path)?;
                if !request.writable_paths.iter().any(|path| {
                    if path.ends_with('/') {
                        edit.path.starts_with(path)
                    } else {
                        edit.path == *path
                    }
                }) {
                    return Err(Error::Unavailable(
                        "file is outside pinned writable paths".into(),
                    ));
                }
                let current = bounded_read(&target)?;
                let observed = hash(&current);
                view.observed_sha256 = Some(observed.clone());
                let journal_request = ToolRequest {
                    workspace: workspace.clone(),
                    writable_paths: request.writable_paths,
                    test_command: vec![],
                    action: AgentAction::ReplaceText {
                        path: edit.path,
                        old: edit.old.clone(),
                        new: edit.new.clone(),
                    },
                };
                let intent = if view.journal_exists {
                    let intent: EditIntent =
                        serde_json::from_slice(&read_with_limit(&journal, MAX_FILE * 8)?)?;
                    if intent.input_hash != hash(&serde_json::to_vec(&journal_request)?)
                        || intent.content.len() as u64 > MAX_FILE
                        || hash(intent.content.as_bytes()) != intent.new_hash
                    {
                        return Err(Error::Unavailable(
                            "edit journal differs from saved request or is corrupt".into(),
                        ));
                    }
                    intent
                } else {
                    let text = String::from_utf8(current)
                        .map_err(|_| Error::Unavailable("editing requires UTF-8 text".into()))?;
                    if edit.old.is_empty() || text.match_indices(&edit.old).take(2).count() != 1 {
                        view.state = ToolEffectState::Conflicted;
                        view.conflict=Some("Without an intent journal, the original text must still match exactly once.".into());
                        return Ok(view);
                    }
                    let content = text.replacen(&edit.old, &edit.new, 1);
                    if content.len() as u64 > MAX_FILE {
                        return Err(Error::Unavailable(
                            "replacement exceeds file size limit".into(),
                        ));
                    }
                    EditIntent {
                        input_hash: hash(&serde_json::to_vec(&journal_request)?),
                        old_hash: observed.clone(),
                        new_hash: hash(content.as_bytes()),
                        content,
                    }
                };
                view.before_sha256 = Some(intent.old_hash.clone());
                view.after_sha256 = Some(intent.new_hash.clone());
                if view.journal_exists && observed == intent.new_hash {
                    view.state = ToolEffectState::AlreadyApplied;
                } else if observed == intent.old_hash {
                    view.state = if view.journal_exists {
                        ToolEffectState::Ready
                    } else {
                        ToolEffectState::NotStarted
                    };
                } else {
                    view.state = ToolEffectState::Conflicted;
                    view.conflict = Some(
                        "File differs from both the journal's original and intended content."
                            .into(),
                    );
                }
            }
            "create_file" => {
                let create: WorkspaceCreate = serde_json::from_value(request.arguments)?;
                view.path = Some(create.path.clone());
                if create.content.len() as u64 > MAX_FILE {
                    return Err(Error::Unavailable(
                        "creation exceeds file size limit".into(),
                    ));
                }
                let (target, metadata) =
                    create::target(&root, &create.path, &request.writable_paths)?;
                let content_hash = hash(create.content.as_bytes());
                view.after_sha256 = Some(content_hash.clone());
                if view.journal_exists {
                    let intent: create::CreateIntent =
                        serde_json::from_slice(&read_with_limit(&journal, MAX_FILE * 8)?)?;
                    let input_hash = hash(&serde_json::to_vec(&(
                        "create_file",
                        1,
                        workspace,
                        &request.writable_paths,
                        &create,
                    ))?);
                    if intent.kind != "create_file"
                        || intent.version != 1
                        || intent.input_hash != input_hash
                        || intent.content != create.content
                        || intent.content_hash != content_hash
                    {
                        return Err(Error::Unavailable(
                            "creation journal differs from saved request or is corrupt".into(),
                        ));
                    }
                }
                if let Some(metadata) = metadata {
                    if !metadata.is_file() {
                        return Err(Error::Unavailable(
                            "creation target is not a regular file".into(),
                        ));
                    }
                    let observed = hash(&read_with_limit(&target, MAX_FILE)?);
                    view.observed_sha256 = Some(observed.clone());
                    if view.journal_exists && observed == content_hash {
                        view.state = ToolEffectState::AlreadyApplied;
                    } else {
                        view.state = ToolEffectState::Conflicted;
                        view.conflict=Some("An existing target is recoverable only when its content matches this activity's saved journal.".into());
                    }
                } else if view.journal_exists {
                    view.state = ToolEffectState::Ready;
                }
            }
            _ => return Err(Error::Unavailable("unsupported tool recovery".into())),
        }
        Ok(view)
    }
}
impl ToolRecoveryInspectionPort for LocalTools {
    fn inspect_tool(
        &self,
        activity: ActivityId,
        request: NativeToolRequest,
    ) -> PortFuture<'_, ToolInspection> {
        let tools = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || tools.inspect_sync(activity, request))
                .await
                .map_err(|error| Error::Unavailable(error.to_string()))?
        })
    }
}
