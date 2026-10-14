use super::*;
use crate::domain::tools::{
    MAX_READ_BATCH_TEXT_BYTES, MAX_READ_FILE_TEXT_BYTES, MAX_READ_FILES, WorkspaceReadBatch,
};

impl LocalTools {
    pub(super) fn read_files_sync(
        &self,
        workspace: String,
        batch: WorkspaceReadBatch,
    ) -> Result<Value, Error> {
        batch.validate()?;
        let workspace = self.workspace(&workspace)?;
        // Hold the same managed-workspace lock for the entire batch.
        let _guard = self.lock(&workspace)?;
        let mut result = ReadBatchResult::new();
        for path in batch.paths {
            let read = (|| -> Result<String, Error> {
                if path.len() > 4096 {
                    return Err(ToolError::new(
                        "path_refused",
                        "relative file paths must not exceed 4 KiB",
                    )
                    .into());
                }
                let content = bounded_read(&self.file(&workspace, &path)?)?;
                String::from_utf8(content).map_err(|_| {
                    ToolError::new("invalid_utf8", "reading requires UTF-8 text").into()
                })
            })();
            result.push(path, read)?;
        }
        Ok(result.finish())
    }
}

/// Preserve the same ordered, bounded observation for local and MCP readers.
pub(crate) struct ReadBatchResult {
    remaining: usize,
    files: Vec<Value>,
    truncated: bool,
}
impl ReadBatchResult {
    pub(crate) fn new() -> Self {
        Self {
            remaining: MAX_READ_BATCH_TEXT_BYTES,
            files: Vec::new(),
            truncated: false,
        }
    }
    pub(crate) fn push(&mut self, path: String, read: Result<String, Error>) -> Result<(), Error> {
        match read {
            Ok(text) => {
                let mut end = text.len().min(MAX_READ_FILE_TEXT_BYTES).min(self.remaining);
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                self.remaining -= end;
                self.truncated |= end < text.len();
                self.files.push(
                    json!({"ok":true,"path":path,"text":&text[..end],"truncated":end < text.len()}),
                );
            }
            Err(Error::ToolRejected(error)) => self
                .files
                .push(json!({"ok":false,"path":path,"error":error})),
            Err(Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidFilename
                ) =>
            {
                self.files.push(json!({"ok":false,"path":path,"error":ToolError::new("path_refused", "the requested path is invalid on this host")}));
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }
    pub(crate) fn finish(self) -> Value {
        json!({"ok":true,"files":self.files,"truncated":self.truncated,"text_bytes":MAX_READ_BATCH_TEXT_BYTES - self.remaining,
            "limits":{"max_files":MAX_READ_FILES,"max_file_bytes":MAX_FILE,"max_file_text_bytes":MAX_READ_FILE_TEXT_BYTES,"max_total_text_bytes":MAX_READ_BATCH_TEXT_BYTES}})
    }
}
