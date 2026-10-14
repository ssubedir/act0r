//! Bounded multi-file inspection through the existing read-only workspace port.
use crate::{
    application::{Error, ports::*},
    domain::{Failure, RecoveryPolicy, tools::*},
};
use std::sync::Arc;

pub struct ReadFilesTool {
    reader: Arc<dyn WorkspaceReader>,
}
impl ReadFilesTool {
    pub fn new(reader: Arc<dyn WorkspaceReader>) -> Self {
        Self { reader }
    }
}
impl NativeTool for ReadFilesTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::builder("read_files", 1, RecoveryPolicy::SafeToRepeat)
            .description("Read up to eight known relative UTF-8 project files in one saved batch. Prefer this to separate read_file calls when inspecting several known files. Results preserve request order, including duplicates, with per-file ok/text/truncated or error. Check every file's ok flag. Files are limited to 1 MiB, text to 16 KiB per file and 64 KiB total in request order; truncated text is incomplete evidence. Private/generated paths and links are refused. One batch uses one durable tool activity.")
            .required_parameter("paths", ToolValueType::StringArray { max_items: MAX_READ_FILES }, "One to eight relative file paths, each at most 4 KiB, for example [\"README.md\",\"src/main.rs\"].")
            .requires_workspace(true)
            .reports_errors(true)
            .byte_limits(32_768, 524_288)
            .build().expect("static batched read contract")
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            self.definition().validate_arguments(&call.arguments)?;
            let Some(workspace) = call.workspace else {
                return Ok(NativeToolResult::Failure(Failure::new(
                    "tool_workspace",
                    "a pinned actor workspace is required",
                    false,
                )));
            };
            let batch: WorkspaceReadBatch = serde_json::from_value(call.arguments)?;
            if let Err(error) = batch.validate() {
                return Ok(NativeToolResult::Rejected(error));
            }
            Ok(
                match self
                    .reader
                    .read_files(workspace, batch, call.activity_id)
                    .await
                {
                    Ok(result) => NativeToolResult::Success(result),
                    Err(Error::ToolRejected(error)) => NativeToolResult::Rejected(error),
                    Err(Error::Ownership(_)) => NativeToolResult::Failure(Failure::new(
                        "workspace_busy",
                        "another tool owns the workspace",
                        true,
                    )),
                    Err(Error::Io(error))
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::Interrupted
                                | std::io::ErrorKind::WouldBlock
                                | std::io::ErrorKind::TimedOut
                        ) =>
                    {
                        NativeToolResult::Failure(Failure::new(
                            "workspace_io",
                            "temporary workspace read failure",
                            true,
                        ))
                    }
                    Err(error) => NativeToolResult::Failure(Failure::new(
                        "workspace_read",
                        error.to_string(),
                        false,
                    )),
                },
            )
        })
    }
}
