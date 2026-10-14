//! Native search capability behind the read-only application port.
use crate::{
    application::{Error, ports::*},
    domain::{Failure, tools::*},
};
use std::{collections::BTreeMap, sync::Arc};

pub struct SearchTextTool {
    reader: Arc<dyn WorkspaceReader>,
}
impl SearchTextTool {
    pub fn new(reader: Arc<dyn WorkspaceReader>) -> Self {
        Self { reader }
    }
}
impl NativeTool for SearchTextTool {
    fn definition(&self) -> ToolDefinition {
        let parameter = |value_type, description: &str, required| ToolParameter {
            value_type,
            description: description.into(),
            required,
        };
        let mut definition = super::native_tools::definition(
            "search_text",
            "Case-sensitive literal search in the managed workspace. Returns one match per line with relative path, 1-based line number, and a UTF-8 snippet. Private/generated paths and binary/oversized files are excluded. Limits are explicit: check truncated and limits_hit before treating absence as evidence. Results are saved; use read_file before editing a match.",
            BTreeMap::from([
                (
                    "query".into(),
                    parameter(
                        ToolValueType::String,
                        "Nonempty literal, at most 256 UTF-8 bytes; no NUL or line breaks. Regex syntax is literal.",
                        true,
                    ),
                ),
                (
                    "path".into(),
                    parameter(
                        ToolValueType::String,
                        "Optional relative file or directory using '/' separators; omit or null for the workspace root.",
                        false,
                    ),
                ),
                (
                    "max_matches".into(),
                    parameter(
                        ToolValueType::Integer,
                        "Maximum matching lines, 1–100; omit or null for 20.",
                        false,
                    ),
                ),
                (
                    "max_files".into(),
                    parameter(
                        ToolValueType::Integer,
                        "Maximum regular files considered, including skipped files, 1–1000; omit or null for 200.",
                        false,
                    ),
                ),
            ]),
        );
        definition.requires_workspace = true;
        definition.reports_errors = true;
        definition.max_input_bytes = 8192;
        definition.max_output_bytes = 32_768;
        definition
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
            let search: WorkspaceSearch = match serde_json::from_value(call.arguments) {
                Ok(search) => search,
                Err(_) => {
                    return Ok(NativeToolResult::Rejected(ToolError::new(
                        "search_limit",
                        "search limits must be positive integers within their declared bounds",
                    )));
                }
            };
            if let Err(error) = search.validate() {
                return Ok(NativeToolResult::Rejected(error));
            }
            Ok(
                match self
                    .reader
                    .search_text(workspace, search, call.activity_id)
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
                            "temporary workspace search failure",
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
