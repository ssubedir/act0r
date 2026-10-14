use crate::{
    application::{Error, ports::*},
    domain::{tools::*, *},
};
use std::{collections::BTreeMap, sync::Arc};

pub struct CreateFileTool {
    writer: Arc<dyn WorkspaceWriter>,
}
impl CreateFileTool {
    pub fn new(writer: Arc<dyn WorkspaceWriter>) -> Self {
        Self { writer }
    }
}
impl NativeTool for CreateFileTool {
    fn definition(&self) -> ToolDefinition {
        let mut definition = super::native_tools::definition(
            "create_file",
            "Create one new UTF-8 file under pinned writable paths. Its parent directory must already exist. Refuses any existing target, even identical content. Every creation requires approval; interrupted writes require reconciliation. Expected refusals return ok:false. The JSON argument object is limited to 64 KiB.",
            BTreeMap::from([
                ("path".into(), ToolParameter {value_type:ToolValueType::String, description:"Relative file path using '/' separators with an existing parent directory.".into(), required:true}),
                ("content".into(), ToolParameter {value_type:ToolValueType::String, description:"Complete UTF-8 contents of the new file; may be empty.".into(), required:true}),
            ]),
        );
        definition.requires_workspace = true;
        definition.requires_writable_paths = true;
        definition.requires_approval = true;
        definition.reports_errors = true;
        definition.recovery = RecoveryPolicy::RequiresReconciliation;
        definition.max_input_bytes = 65_536;
        definition
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let definition = self.definition();
            for (validation, code) in [
                (definition.validate_arguments(&call.arguments), "tool_input"),
                (
                    validate_tool_permissions(
                        std::slice::from_ref(&definition),
                        &call.writable_paths,
                    ),
                    "tool_permissions",
                ),
            ] {
                if let Err(error) = validation {
                    return Ok(NativeToolResult::Failure(Failure::new(
                        code,
                        error.to_string(),
                        false,
                    )));
                }
            }
            let Some(workspace) = call.workspace else {
                return Ok(NativeToolResult::Failure(Failure::new(
                    "tool_workspace",
                    "a pinned actor workspace is required",
                    false,
                )));
            };
            let create: WorkspaceCreate = serde_json::from_value(call.arguments)?;
            match self
                .writer
                .create_file(workspace, call.writable_paths, create, call.activity_id)
                .await
            {
                Ok(result) => Ok(NativeToolResult::Success(result)),
                Err(Error::ToolRejected(error)) => Ok(NativeToolResult::Rejected(error)),
                Err(Error::Ownership(_)) => Ok(NativeToolResult::Failure(Failure::new(
                    "workspace_busy",
                    "another tool owns the workspace",
                    true,
                ))),
                Err(error) => Err(error),
            }
        })
    }
}
