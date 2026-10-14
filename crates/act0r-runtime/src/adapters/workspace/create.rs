use super::*;
use crate::domain::tools::WorkspaceCreate;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateIntent {
    pub(super) kind: String,
    pub(super) version: u32,
    pub(super) input_hash: String,
    pub(super) content_hash: String,
    pub(super) content: String,
}

pub(super) fn target(
    workspace: &Path,
    path: &str,
    writable: &[String],
) -> Result<(PathBuf, Option<fs::Metadata>), Error> {
    if !portable_file_path(path)
        || path
            .split('/')
            .any(|p| excluded(p) || reserved_component(p))
    {
        return Err(ToolError::new(
            "path_refused",
            "creation requires a permitted relative file path with '/' separators",
        )
        .into());
    }
    if !writable.iter().any(|p| {
        if p.ends_with('/') {
            path.starts_with(p)
        } else {
            path == p
        }
    }) {
        return Err(ToolError::new(
            "path_not_writable",
            "file is outside the configured writable paths",
        )
        .into());
    }
    let mut parent = workspace.to_path_buf();
    let mut components = path.split('/').peekable();
    while let Some(component) = components.next() {
        if components.peek().is_none() {
            let canonical = fs::canonicalize(&parent).map_err(inspection_io)?;
            if !canonical.starts_with(workspace) {
                return Err(ToolError::new(
                    "path_refused",
                    "parent is outside the managed workspace",
                )
                .into());
            }
            let leaf = canonical.join(component);
            let metadata = match fs::symlink_metadata(&leaf) {
                Ok(metadata) if link(&metadata) => {
                    return Err(ToolError::new(
                        "path_refused",
                        "symlinks and reparse points are not allowed",
                    )
                    .into());
                }
                Ok(metadata) => Some(metadata),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(inspection_io(error)),
            };
            return Ok((leaf, metadata));
        }
        parent.push(component);
        let metadata = fs::symlink_metadata(&parent).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ToolError::new(
                    "parent_not_found",
                    "create_file requires an existing parent directory",
                )
                .into()
            } else {
                inspection_io(error)
            }
        })?;
        if link(&metadata) {
            return Err(ToolError::new(
                "path_refused",
                "symlinks and reparse points are not allowed",
            )
            .into());
        }
        if !metadata.is_dir() {
            return Err(ToolError::new(
                "not_a_directory",
                "a parent path component is not a directory",
            )
            .into());
        }
    }
    Err(Error::Unavailable("invalid validated creation path".into()))
}

fn publish_new(path: &Path, content: &[u8]) -> Result<(), Error> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Unavailable("missing parent directory".into()))?;
    let mut staged = tempfile::NamedTempFile::new_in(parent)?;
    staged.write_all(content)?;
    staged.as_file().sync_all()?;
    // Never replaces the destination, including if it appeared after the preflight.
    staged
        .persist_noclobber(path)
        .map_err(|error| Error::Io(error.error))?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

impl LocalTools {
    pub(super) fn create_sync(
        &self,
        workspace: String,
        writable: Vec<String>,
        create: WorkspaceCreate,
        activity: ActivityId,
    ) -> Result<Value, Error> {
        validate_writable_paths(&writable)?;
        if writable.is_empty() {
            return Err(Error::Unavailable(
                "workspace creation requires pinned writable paths".into(),
            ));
        }
        let root = self.workspace(&workspace)?;
        let _guard = self.lock(&root)?;
        let journal = root.join(PRIVATE).join(format!("{activity}.json"));
        let has_intent = match fs::symlink_metadata(&journal) {
            Ok(metadata) if link(&metadata) || !metadata.is_file() => {
                return Err(Error::Unavailable(
                    "invalid creation journal; reconciliation required".into(),
                ));
            }
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if create.content.len() as u64 > MAX_FILE {
            return Err(if has_intent {
                Error::Unavailable(
                    "cannot verify journaled creation; reconciliation required".into(),
                )
            } else {
                ToolError::new("file_limit", "creation exceeds the 1 MiB file size limit").into()
            });
        }
        let (target, current) = target(&root, &create.path, &writable).map_err(|error| {
            if has_intent {
                Error::Unavailable(
                    "cannot verify journaled creation path; reconciliation required".into(),
                )
            } else {
                error
            }
        })?;
        let input_hash = hash(&serde_json::to_vec(&(
            "create_file",
            1,
            &workspace,
            &writable,
            &create,
        ))?);
        let intent = if has_intent {
            let intent: CreateIntent =
                serde_json::from_slice(&read_with_limit(&journal, MAX_FILE * 8)?)?;
            if intent.kind != "create_file"
                || intent.version != 1
                || intent.input_hash != input_hash
                || intent.content != create.content
                || intent.content.len() as u64 > MAX_FILE
                || hash(intent.content.as_bytes()) != intent.content_hash
            {
                return Err(Error::Unavailable(
                    "creation journal differs from the saved request; reconciliation required"
                        .into(),
                ));
            }
            intent
        } else {
            if current.is_some() {
                return Err(ToolError::new("file_exists","create_file refuses an existing target; inspect it and choose a permitted edit or a new path").into());
            }
            let intent = CreateIntent {
                kind: "create_file".into(),
                version: 1,
                input_hash,
                content_hash: hash(create.content.as_bytes()),
                content: create.content.clone(),
            };
            atomic_write(&journal, &serde_json::to_vec(&intent)?)?;
            intent
        };
        if let Some(metadata) = current {
            if !metadata.is_file() {
                return Err(Error::Unavailable(
                    "creation target is no longer a regular file; reconciliation required".into(),
                ));
            }
            let bytes = read_with_limit(&target, MAX_FILE)?;
            if hash(&bytes) != intent.content_hash {
                return Err(Error::Unavailable(
                    "creation target conflicts with the saved content; reconciliation required"
                        .into(),
                ));
            }
            return Ok(
                json!({"ok":true,"path":create.path,"bytes":bytes.len(),"already_applied":true}),
            );
        }
        publish_new(&target, intent.content.as_bytes())?;
        Ok(
            json!({"ok":true,"path":create.path,"bytes":intent.content.len(),"already_applied":false}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publication_preserves_a_target_that_appeared_after_preflight() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("new.rs");
        fs::write(&target, "concurrent contents").unwrap();
        assert!(publish_new(&target, b"requested contents").is_err());
        assert_eq!(fs::read_to_string(target).unwrap(), "concurrent contents");
    }
}
