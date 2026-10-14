//! Small typed local tools. Applications can register their own NativeTool implementations.
use crate::{
    application::{Error, ToolRegistry, ports::*},
    domain::{tools::*, *},
};
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

pub fn local_tool_registry() -> Result<ToolRegistry, Error> {
    ToolRegistry::new(vec![Arc::new(EchoTool), Arc::new(SumTool)])
}
pub fn workspace_tool_registry(runner: Arc<dyn WorkspaceReader>) -> Result<ToolRegistry, Error> {
    ToolRegistry::new(vec![
        Arc::new(EchoTool),
        Arc::new(SumTool),
        Arc::new(ListFilesTool::new(runner.clone())),
        Arc::new(ReadFileTool::new(runner.clone())),
        Arc::new(super::workspace_reads::ReadFilesTool::new(runner)),
    ])
}
/// Hosts can retain an inspection-only registry or explicitly install the write capability.
pub fn editable_workspace_tool_registry(
    reader: Arc<dyn WorkspaceReader>,
    writer: Arc<dyn WorkspaceWriter>,
) -> Result<ToolRegistry, Error> {
    ToolRegistry::new(editable_tools(reader, writer))
}
/// Hosts explicitly install process execution independently of inspection and editing.
pub fn tested_workspace_tool_registry(
    reader: Arc<dyn WorkspaceReader>,
    writer: Arc<dyn WorkspaceWriter>,
    tests: Arc<dyn WorkspaceTestRunner>,
) -> Result<ToolRegistry, Error> {
    let mut tools = editable_tools(reader, writer);
    tools.push(Arc::new(RunTestsTool::new(tests)));
    ToolRegistry::new(tools)
}
fn editable_tools(
    reader: Arc<dyn WorkspaceReader>,
    writer: Arc<dyn WorkspaceWriter>,
) -> Vec<Arc<dyn NativeTool>> {
    vec![
        Arc::new(EchoTool),
        Arc::new(SumTool),
        Arc::new(ListFilesTool::new(reader.clone())),
        Arc::new(ReadFileTool::new(reader.clone())),
        Arc::new(super::workspace_reads::ReadFilesTool::new(reader)),
        Arc::new(ReplaceTextTool::new(writer)),
    ]
}
/// Full native host catalog; earlier constructors retain their original capabilities.
pub fn agent_tool_registry(
    reader: Arc<dyn WorkspaceReader>,
    writer: Arc<dyn WorkspaceWriter>,
    tests: Arc<dyn WorkspaceTestRunner>,
    results: Arc<dyn ResultRecallReadPort>,
) -> Result<ToolRegistry, Error> {
    let mut tools = editable_tools(reader.clone(), writer.clone());
    tools.push(Arc::new(super::workspace_search::SearchTextTool::new(
        reader,
    )));
    tools.push(Arc::new(super::file_creation::CreateFileTool::new(writer)));
    tools.push(Arc::new(RunTestsTool::new(tests)));
    tools.extend(super::result_recall::result_recall_tools(results));
    ToolRegistry::new(tools)
}
fn parameter(value_type: ToolValueType, description: &str) -> ToolParameter {
    ToolParameter {
        value_type,
        description: description.into(),
        required: true,
    }
}
pub(super) fn definition(
    name: &str,
    description: &str,
    parameters: BTreeMap<String, ToolParameter>,
) -> ToolDefinition {
    ToolDefinition {
        id: DefinitionId::new(name, 1),
        description: description.into(),
        parameters,
        external: None,
        retry: RetryPolicy::default(),
        recovery: RecoveryPolicy::SafeToRepeat,
        requires_approval: false,
        requires_workspace: false,
        requires_writable_paths: false,
        requires_test_command: false,
        requires_project_command: false,
        reports_errors: false,
        max_input_bytes: 4096,
        max_output_bytes: 8192,
    }
}

pub struct RunTestsTool {
    runner: Arc<dyn WorkspaceTestRunner>,
}

pub struct RunCommandTool {
    runner: Arc<dyn WorkspaceCommandRunner>,
}
impl RunCommandTool {
    pub fn new(runner: Arc<dyn WorkspaceCommandRunner>) -> Self {
        Self { runner }
    }
}
impl NativeTool for RunCommandTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::builder("run_command", 1, RecoveryPolicy::RequiresReconciliation)
            .description("Run one named host-pinned project command after approval. Saves bounded stdout/stderr, exit status and elapsed time. Any execution invalidates previous tests; this tool never supplies test verification. Interrupted execution requires reconciliation.")
            .required_parameter("name", ToolValueType::String, "Exact name from the host-pinned project_commands catalog")
            .requires_workspace(true)
            .requires_approval(true)
            .requires_project_command(true)
            .byte_limits(4096, 262_144)
            .build()
            .expect("static command tool contract")
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let definition = self.definition();
            definition.validate_arguments(&call.arguments)?;
            validate_project_command(&definition, &call.arguments, call.project_command.as_ref())?;
            let workspace = call
                .workspace
                .ok_or_else(|| Error::Unavailable("a pinned actor workspace is required".into()))?;
            let selected = call
                .project_command
                .ok_or_else(|| Error::Unavailable("a pinned project command is required".into()))?;
            match self
                .runner
                .run_command(workspace, selected, call.activity_id)
                .await
            {
                Ok(result) => Ok(NativeToolResult::Success(result)),
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
impl RunTestsTool {
    pub fn new(runner: Arc<dyn WorkspaceTestRunner>) -> Self {
        Self { runner }
    }
}
impl NativeTool for RunTestsTool {
    fn definition(&self) -> ToolDefinition {
        let mut definition = definition(
            "run_tests",
            "Run the host-pinned test command in the managed workspace with empty arguments and approval. Saves exit status, elapsed milliseconds, and bounded stdout/stderr. Nonzero exit is feedback for repair; interrupted or timed-out execution requires reconciliation.",
            BTreeMap::new(),
        );
        definition.requires_workspace = true;
        definition.requires_test_command = true;
        definition.requires_approval = true;
        definition.recovery = RecoveryPolicy::RequiresReconciliation;
        // 16 KiB per pipe, including worst-case JSON escaping and lossy UTF-8.
        definition.max_output_bytes = 262_144;
        definition
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let definition = self.definition();
            definition.validate_arguments(&call.arguments)?;
            validate_tool_test_command(
                std::slice::from_ref(&definition),
                call.test_command.as_ref(),
            )?;
            let workspace = call
                .workspace
                .ok_or_else(|| Error::Unavailable("a pinned actor workspace is required".into()))?;
            let command = call
                .test_command
                .ok_or_else(|| Error::Unavailable("a pinned test command is required".into()))?;
            match self
                .runner
                .run_tests(workspace, command, call.activity_id)
                .await
            {
                Ok(result) => Ok(NativeToolResult::Success(result)),
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

pub struct ReplaceTextTool {
    writer: Arc<dyn WorkspaceWriter>,
}
impl ReplaceTextTool {
    pub fn new(writer: Arc<dyn WorkspaceWriter>) -> Self {
        Self { writer }
    }
}
impl NativeTool for ReplaceTextTool {
    fn definition(&self) -> ToolDefinition {
        let mut definition = definition(
            "replace_text",
            "Replace one exact unique match in a permitted relative UTF-8 file (at most 1 MiB). Read before editing. Writable paths are pinned by the host; every edit requires approval. Expected refusals return ok:false; interrupted writes require reconciliation.",
            BTreeMap::from([
                (
                    "path".into(),
                    parameter(
                        ToolValueType::String,
                        "Relative file path using '/' separators.",
                    ),
                ),
                (
                    "old".into(),
                    parameter(
                        ToolValueType::String,
                        "Nonempty text that must match exactly once.",
                    ),
                ),
                (
                    "new".into(),
                    parameter(ToolValueType::String, "Replacement text; may be empty."),
                ),
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
            if let Err(error) = definition.validate_arguments(&call.arguments) {
                return Ok(NativeToolResult::Failure(Failure::new(
                    "tool_input",
                    error.to_string(),
                    false,
                )));
            }
            if let Err(error) =
                validate_tool_permissions(std::slice::from_ref(&definition), &call.writable_paths)
            {
                return Ok(NativeToolResult::Failure(Failure::new(
                    "tool_permissions",
                    error.to_string(),
                    false,
                )));
            }
            let Some(workspace) = call.workspace else {
                return Ok(NativeToolResult::Failure(Failure::new(
                    "tool_workspace",
                    "a pinned actor workspace is required",
                    false,
                )));
            };
            let edit: WorkspaceEdit = serde_json::from_value(call.arguments)?;
            match self
                .writer
                .replace_text(workspace, call.writable_paths, edit, call.activity_id)
                .await
            {
                Ok(result) => Ok(NativeToolResult::Success(result)),
                Err(Error::ToolRejected(error)) => Ok(NativeToolResult::Rejected(error)),
                // Lock contention proves this invocation made no change.
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

pub struct ListFilesTool {
    runner: Arc<dyn WorkspaceReader>,
}
impl ListFilesTool {
    pub fn new(runner: Arc<dyn WorkspaceReader>) -> Self {
        Self { runner }
    }
}
impl NativeTool for ListFilesTool {
    fn definition(&self) -> ToolDefinition {
        workspace_definition(
            "list_files",
            "List up to 1,000 project files in sorted relative paths using the actor's managed workspace. Private and generated directories are excluded.",
            BTreeMap::new(),
        )
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            if let Err(error) = self.definition().validate_arguments(&call.arguments) {
                return Ok(NativeToolResult::Failure(Failure::new(
                    "tool_input",
                    error.to_string(),
                    false,
                )));
            }
            inspect_workspace(self.runner.as_ref(), call, WorkspaceReadAction::ListFiles).await
        })
    }
}
pub struct ReadFileTool {
    runner: Arc<dyn WorkspaceReader>,
}
impl ReadFileTool {
    pub fn new(runner: Arc<dyn WorkspaceReader>) -> Self {
        Self { runner }
    }
}
impl NativeTool for ReadFileTool {
    fn definition(&self) -> ToolDefinition {
        workspace_definition(
            "read_file",
            "Read a permitted relative UTF-8 file in the actor's managed workspace. Files are limited to 1 MiB; text is limited to 16 KiB with an explicit truncated flag.",
            BTreeMap::from([(
                "path".into(),
                parameter(
                    ToolValueType::String,
                    "Relative project file path, such as src/lib.rs.",
                ),
            )]),
        )
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            if let Err(error) = self.definition().validate_arguments(&call.arguments) {
                return Ok(NativeToolResult::Failure(Failure::new(
                    "tool_input",
                    error.to_string(),
                    false,
                )));
            }
            let path = call.arguments["path"].as_str().unwrap().to_owned();
            inspect_workspace(
                self.runner.as_ref(),
                call,
                WorkspaceReadAction::ReadFile { path },
            )
            .await
        })
    }
}
fn workspace_definition(
    name: &str,
    description: &str,
    parameters: BTreeMap<String, ToolParameter>,
) -> ToolDefinition {
    let mut definition = definition(name, description, parameters);
    definition.id.version = 2;
    definition.description.push_str(" Expected inspection problems return ok:false with a structured error; choose another permitted action using that feedback.");
    definition.requires_workspace = true;
    definition.reports_errors = true;
    definition.max_output_bytes = 131_072;
    definition
}
async fn inspect_workspace(
    runner: &dyn WorkspaceReader,
    call: NativeToolCall,
    action: WorkspaceReadAction,
) -> Result<NativeToolResult, Error> {
    let Some(workspace) = call.workspace else {
        return Ok(NativeToolResult::Failure(Failure::new(
            "tool_workspace",
            "a pinned actor workspace is required",
            false,
        )));
    };
    // These adapters only expose inspection actions. Neither commands nor writable paths come from the model.
    Ok(
        match runner.inspect(workspace, action, call.activity_id).await {
            Ok(result) => NativeToolResult::Success(result),
            Err(Error::ToolRejected(error)) => NativeToolResult::Rejected(error),
            Err(Error::Ownership(message)) => {
                NativeToolResult::Failure(Failure::new("workspace_busy", message, true))
            }
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
                    "temporary filesystem inspection failure",
                    true,
                ))
            }
            Err(error) => {
                NativeToolResult::Failure(Failure::new("workspace_read", error.to_string(), false))
            }
        },
    )
}
pub struct EchoTool;
impl NativeTool for EchoTool {
    fn definition(&self) -> ToolDefinition {
        definition(
            "echo",
            "Return the supplied text.",
            BTreeMap::from([(
                "text".into(),
                parameter(ToolValueType::String, "Text to return."),
            )]),
        )
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input {
                text: String,
            }
            let input: Input = serde_json::from_value(call.arguments)?;
            Ok(NativeToolResult::Success(json!({"text":input.text})))
        })
    }
}
pub struct SumTool;
impl NativeTool for SumTool {
    fn definition(&self) -> ToolDefinition {
        definition(
            "sum",
            "Add two signed 64-bit integers, reporting overflow as a known failure.",
            BTreeMap::from([
                (
                    "left".into(),
                    parameter(ToolValueType::Integer, "First integer."),
                ),
                (
                    "right".into(),
                    parameter(ToolValueType::Integer, "Second integer."),
                ),
            ]),
        )
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input {
                left: i64,
                right: i64,
            }
            let input: Input = match serde_json::from_value(call.arguments) {
                Ok(input) => input,
                Err(_) => {
                    return Ok(NativeToolResult::Failure(Failure::new(
                        "sum_input",
                        "sum requires signed 64-bit integers",
                        false,
                    )));
                }
            };
            Ok(match input.left.checked_add(input.right) {
                Some(sum) => NativeToolResult::Success(json!({"sum":sum})),
                None => NativeToolResult::Failure(Failure::new(
                    "sum_overflow",
                    "signed integer addition overflowed",
                    false,
                )),
            })
        })
    }
}
