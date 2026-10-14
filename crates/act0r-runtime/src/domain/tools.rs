//! Serializable native tool contracts. Validation is pure and performs no I/O.
use super::{
    ActivityId, ActivitySpec, AttemptId, Command, DefinitionId, DomainError, Failure,
    RecoveryPolicy, RetryPolicy, RunId,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[path = "tools/schema.rs"]
mod schema;
pub use schema::ExternalToolContract;

/// Model-facing projection only; saved decisions and tool intents remain canonical.
pub(crate) fn flat_action_schema(action: Value) -> Value {
    let arguments = &action["properties"]["arguments"];
    let mut properties = arguments["properties"].as_object().unwrap().clone();
    properties.insert("kind".into(), action["properties"]["tool"].clone());
    let mut required = arguments["required"].as_array().unwrap().clone();
    required.insert(0, json!("kind"));
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}

pub(crate) fn flat_model_action(action: Value) -> Value {
    if action["kind"] == "call_tool" {
        let mut fields = action["arguments"].as_object().unwrap().clone();
        fields.insert("kind".into(), action["tool"].clone());
        return json!(fields);
    }
    action
}

pub(crate) fn flat_model_observation(mut observation: Value) -> Value {
    if let Some(action) = observation.get_mut("action") {
        *action = flat_model_action(action.clone());
    }
    observation
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolValueType {
    String,
    Integer,
    Number,
    Boolean,
    /// A nonempty, explicitly bounded list of strings; other nested values remain unsupported.
    StringArray {
        max_items: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolParameter {
    pub value_type: ToolValueType,
    pub description: String,
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDefinition {
    pub id: DefinitionId,
    pub description: String,
    pub parameters: BTreeMap<String, ToolParameter>,
    /// A host-pinned schema and opaque implementation identity for external tools.
    /// Omission preserves existing serialized contracts exactly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external: Option<ExternalToolContract>,
    pub retry: RetryPolicy,
    pub recovery: RecoveryPolicy,
    pub requires_approval: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub requires_workspace: bool,
    /// Writes require runtime-pinned permissions outside model arguments.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub requires_writable_paths: bool,
    /// Fixed host command and deadline, separate from model arguments.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub requires_test_command: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub requires_project_command: bool,
    /// Expected rejections are saved as observations, independently of execution failures.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub reports_errors: bool,
    pub max_input_bytes: u32,
    pub max_output_bytes: u32,
}

/// Build the existing serialized contract with validated parameters and explicit recovery.
#[must_use]
pub struct ToolDefinitionBuilder {
    definition: ToolDefinition,
    duplicate_parameter: bool,
}
impl ToolDefinitionBuilder {
    pub fn external_contract(mut self, contract: ExternalToolContract) -> Self {
        self.definition.external = Some(contract);
        self
    }
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.definition.description = description.into();
        self
    }
    pub fn required_parameter(
        self,
        name: impl Into<String>,
        value_type: ToolValueType,
        description: impl Into<String>,
    ) -> Self {
        self.parameter(name, value_type, description, true)
    }
    pub fn optional_parameter(
        self,
        name: impl Into<String>,
        value_type: ToolValueType,
        description: impl Into<String>,
    ) -> Self {
        self.parameter(name, value_type, description, false)
    }
    fn parameter(
        mut self,
        name: impl Into<String>,
        value_type: ToolValueType,
        description: impl Into<String>,
        required: bool,
    ) -> Self {
        if self
            .definition
            .parameters
            .insert(
                name.into(),
                ToolParameter {
                    value_type,
                    description: description.into(),
                    required,
                },
            )
            .is_some()
        {
            self.duplicate_parameter = true;
        }
        self
    }
    pub fn byte_limits(mut self, input: u32, output: u32) -> Self {
        self.definition.max_input_bytes = input;
        self.definition.max_output_bytes = output;
        self
    }
    pub fn retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.definition.retry = retry;
        self
    }
    pub fn requires_approval(mut self, required: bool) -> Self {
        self.definition.requires_approval = required;
        self
    }
    pub fn requires_workspace(mut self, required: bool) -> Self {
        self.definition.requires_workspace = required;
        self
    }
    pub fn requires_writable_paths(mut self, required: bool) -> Self {
        self.definition.requires_writable_paths = required;
        self
    }
    pub fn requires_test_command(mut self, required: bool) -> Self {
        self.definition.requires_test_command = required;
        self
    }
    pub fn requires_project_command(mut self, required: bool) -> Self {
        self.definition.requires_project_command = required;
        self
    }
    pub fn reports_errors(mut self, enabled: bool) -> Self {
        self.definition.reports_errors = enabled;
        self
    }
    pub fn build(self) -> Result<ToolDefinition, DomainError> {
        if self.duplicate_parameter {
            return Err(DomainError::InvalidConfiguration(
                "duplicate tool parameter declaration".into(),
            ));
        }
        self.definition.validate()?;
        Ok(self.definition)
    }
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
impl ToolDefinition {
    /// Defaults: 4 KiB input, 8 KiB output, default retries, and no scoped permissions,
    /// approval, or error feedback. Recovery must be chosen by the tool author.
    pub fn builder(
        name: impl Into<String>,
        version: u32,
        recovery: RecoveryPolicy,
    ) -> ToolDefinitionBuilder {
        ToolDefinitionBuilder {
            definition: Self {
                id: DefinitionId::new(name, version),
                description: String::new(),
                parameters: BTreeMap::new(),
                external: None,
                retry: RetryPolicy::default(),
                recovery,
                requires_approval: false,
                requires_workspace: false,
                requires_writable_paths: false,
                requires_test_command: false,
                requires_project_command: false,
                reports_errors: false,
                max_input_bytes: 4096,
                max_output_bytes: 8192,
            },
            duplicate_parameter: false,
        }
    }
    pub fn action_schema(&self) -> Value {
        let mut arguments = self.input_schema();
        // Strict model schemas require every key; optional parameters can be null.
        if let Some(contract) = &self.external {
            arguments = contract.model_schema();
        } else {
            arguments["required"] = json!(self.parameters.keys().collect::<Vec<_>>());
        }
        json!({"type":"object","properties":{"kind":{"type":"string","enum":["call_tool"]},"tool":{"type":"string","enum":[self.id.name]},"arguments":arguments},"required":["kind","tool","arguments"],"additionalProperties":false})
    }
    pub fn action_schema_with_commands(&self, commands: &ProjectCommands) -> Value {
        let mut schema = self.action_schema();
        if self.requires_project_command {
            schema["properties"]["arguments"]["properties"]["name"]["enum"] =
                json!(commands.keys().collect::<Vec<_>>());
        }
        schema
    }
    pub fn prompt_metadata(&self) -> Value {
        let mut metadata =
            json!({"name":self.id.name,"version":self.id.version,"description":self.description});
        if self.requires_workspace {
            metadata["requires_workspace"] = json!(true);
        }
        if self.reports_errors {
            metadata["reports_errors"] = json!(true);
        }
        if self.requires_writable_paths {
            metadata["requires_writable_paths"] = json!(true);
        }
        if self.requires_test_command {
            metadata["requires_test_command"] = json!(true);
        }
        if self.requires_project_command {
            metadata["requires_project_command"] = json!(true);
        }
        metadata
    }
    pub fn handler(&self) -> DefinitionId {
        DefinitionId::new(
            "native.tool",
            if self.requires_project_command {
                6
            } else if self.requires_test_command {
                5
            } else if self.requires_writable_paths {
                4
            } else if self.reports_errors {
                3
            } else if self.requires_workspace {
                2
            } else {
                1
            },
        )
    }
    /// Persist an approval wait or one activity intent; this never dispatches a tool.
    pub fn request_call(&self, arguments: &Value) -> Result<Command, DomainError> {
        self.request_call_in(arguments, None)
    }
    pub fn request_call_in(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
    ) -> Result<Command, DomainError> {
        self.request_call_with_permissions(arguments, workspace, &[])
    }
    pub fn request_call_with_permissions(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
        writable_paths: &[String],
    ) -> Result<Command, DomainError> {
        self.request_call_with_context(arguments, workspace, writable_paths, None)
    }
    pub fn request_call_with_context(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
        writable_paths: &[String],
        test_command: Option<&WorkspaceTestCommand>,
    ) -> Result<Command, DomainError> {
        self.request_resolved(arguments, workspace, writable_paths, test_command, None)
    }
    fn request_resolved(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
        writable_paths: &[String],
        test_command: Option<&WorkspaceTestCommand>,
        selected: Option<NamedWorkspaceCommand>,
    ) -> Result<Command, DomainError> {
        self.validate_arguments(arguments)?;
        validate_project_command(self, arguments, selected.as_ref())?;
        let workspace = self.workspace_context(workspace)?;
        let writable_paths = self.write_context(writable_paths)?;
        let test_command = self.test_context(test_command)?;
        if self.requires_approval {
            let mut payload =
                json!({"tool":self.id,"arguments":arguments,"recovery":self.recovery});
            if let Some(workspace) = workspace {
                payload["workspace"] = json!(workspace);
            }
            if !writable_paths.is_empty() {
                payload["writable_paths"] = json!(writable_paths);
            }
            if let Some(command) = test_command {
                payload["test_command"] = json!(command);
            }
            if let Some(selected) = selected {
                payload["project_command"] = json!(selected);
            }
            Ok(Command::WaitForInput {
                key: "native_tool_approval".into(),
                prompt: format!("Approve native tool {}?", self.id.name),
                payload,
            })
        } else {
            self.schedule_resolved(arguments, workspace, writable_paths, test_command, selected)
        }
    }
    pub fn schedule_call(&self, arguments: &Value) -> Result<Command, DomainError> {
        self.schedule_call_in(arguments, None)
    }
    pub fn schedule_call_in(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
    ) -> Result<Command, DomainError> {
        self.schedule_call_with_permissions(arguments, workspace, &[])
    }
    pub fn schedule_call_with_permissions(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
        writable_paths: &[String],
    ) -> Result<Command, DomainError> {
        self.schedule_call_with_context(arguments, workspace, writable_paths, None)
    }
    pub fn schedule_call_with_context(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
        writable_paths: &[String],
        test_command: Option<&WorkspaceTestCommand>,
    ) -> Result<Command, DomainError> {
        self.schedule_resolved(arguments, workspace, writable_paths, test_command, None)
    }
    fn schedule_resolved(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
        writable_paths: &[String],
        test_command: Option<&WorkspaceTestCommand>,
        selected: Option<NamedWorkspaceCommand>,
    ) -> Result<Command, DomainError> {
        self.validate_arguments(arguments)?;
        validate_project_command(self, arguments, selected.as_ref())?;
        let workspace = self.workspace_context(workspace)?.map(str::to_owned);
        let writable_paths = self.write_context(writable_paths)?.to_vec();
        Ok(Command::Schedule(ActivitySpec {
            handler: self.handler(),
            input: serde_json::to_value(NativeToolRequest {
                definition: self.clone(),
                arguments: arguments.clone(),
                workspace,
                writable_paths,
                test_command: self.test_context(test_command)?.cloned(),
                project_command: selected,
            })
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?,
            retry: self.retry.clone(),
            recovery: self.recovery,
            usage: None,
        }))
    }
    fn test_context<'a>(
        &self,
        command: Option<&'a WorkspaceTestCommand>,
    ) -> Result<Option<&'a WorkspaceTestCommand>, DomainError> {
        if self.requires_test_command {
            validate_tool_test_command(std::slice::from_ref(self), command)?;
            Ok(command)
        } else {
            Ok(None)
        }
    }
    fn write_context<'a>(&self, paths: &'a [String]) -> Result<&'a [String], DomainError> {
        if self.requires_writable_paths {
            validate_tool_permissions(std::slice::from_ref(self), paths)?;
            Ok(paths)
        } else {
            Ok(&[])
        }
    }
    fn workspace_context<'a>(
        &self,
        workspace: Option<&'a str>,
    ) -> Result<Option<&'a str>, DomainError> {
        if self.requires_workspace {
            validate_tool_workspace(std::slice::from_ref(self), workspace)?;
            Ok(workspace)
        } else {
            Ok(None)
        }
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        if let Some(contract) = &self.external {
            contract.validate()?;
            if !self.parameters.is_empty()
                || !contract.read_only
                || !self.requires_workspace
                || self.requires_writable_paths
                || self.requires_test_command
                || self.requires_project_command
                || self.requires_approval
                || !self.reports_errors
                || self.recovery != RecoveryPolicy::SafeToRepeat
            {
                return Err(DomainError::InvalidConfiguration("external schema tools require a read-only, repeat-safe workspace contract without native parameters or write capabilities".into()));
            }
        }
        if !valid_name(&self.id.name)
            || self.id.version == 0
            || self.description.trim().is_empty()
            || self.description.len() > 4096
            || self.parameters.len() > 32
            || self
                .parameters
                .iter()
                .any(|(name, p)| !valid_name(name) || p.description.len() > 4096
                    || matches!(p.value_type, ToolValueType::StringArray { max_items } if !(1..=32).contains(&max_items)))
            || self.retry.max_attempts == 0
            || self.retry.max_delay_ms < self.retry.delay_ms
            || self.max_input_bytes == 0
            || self.max_input_bytes > 1_048_576
            || self.max_output_bytes == 0
            || self.max_output_bytes > 1_048_576
            || (self.requires_writable_paths && !self.requires_workspace)
            || (self.requires_test_command && !self.requires_workspace)
            || (self.requires_project_command
                && (!self.requires_workspace
                    || !self.requires_approval
                    || self.requires_test_command
                    || self.requires_writable_paths
                    || self.parameters.len() != 1
                    || self.parameters.get("name").is_none_or(|parameter| {
                        !parameter.required || parameter.value_type != ToolValueType::String
                    })
                    || self.recovery != RecoveryPolicy::RequiresReconciliation))
        {
            return Err(DomainError::InvalidConfiguration("tool definitions require a versioned name, bounded parameters, positive byte limits, and a valid retry policy".into()));
        }
        Ok(())
    }
    /// Flat objects support primitives and bounded string lists. Extra fields are refused.
    pub fn validate_arguments(&self, arguments: &Value) -> Result<(), DomainError> {
        self.validate()?;
        let object = arguments.as_object().ok_or_else(|| {
            DomainError::InvalidWorkflow("tool arguments must be an object".into())
        })?;
        if arguments.to_string().len() > self.max_input_bytes as usize {
            return Err(DomainError::InvalidWorkflow(
                "tool input byte limit exceeded".into(),
            ));
        }
        if let Some(contract) = &self.external {
            return contract.validate_arguments(arguments);
        }
        if object
            .keys()
            .any(|name| !self.parameters.contains_key(name))
        {
            return Err(DomainError::InvalidWorkflow("unknown tool argument".into()));
        }
        for (name, parameter) in &self.parameters {
            let Some(value) = object.get(name) else {
                if parameter.required {
                    return Err(DomainError::InvalidWorkflow(format!(
                        "missing tool argument {name}"
                    )));
                }
                continue;
            };
            let valid = match parameter.value_type {
                ToolValueType::String => value.is_string(),
                ToolValueType::Boolean => value.is_boolean(),
                ToolValueType::Number => value.is_number(),
                ToolValueType::Integer => value.is_i64() || value.is_u64(),
                ToolValueType::StringArray { max_items } => value.as_array().is_some_and(|items| {
                    !items.is_empty()
                        && items.len() <= max_items as usize
                        && items.iter().all(Value::is_string)
                }),
            };
            if !valid && !(value.is_null() && !parameter.required) {
                return Err(DomainError::InvalidWorkflow(format!(
                    "wrong type for tool argument {name}"
                )));
            }
        }
        Ok(())
    }
    pub fn input_schema(&self) -> Value {
        if let Some(contract) = &self.external {
            return contract.input_schema.clone();
        }
        let properties: BTreeMap<_, _> = self
            .parameters
            .iter()
            .map(|(name, p)| {
                let value = match p.value_type {
                    ToolValueType::StringArray { max_items } => json!({"type":"array","items":{"type":"string"},"minItems":1,"maxItems":max_items}),
                    _ => json!({"type":p.value_type}),
                };
                let mut schema = if p.required { value }
                    else { json!({"anyOf":[value,{"type":"null"}]}) };
                schema["description"] = json!(p.description);
                (name, schema)
            })
            .collect();
        let required: Vec<_> = self
            .parameters
            .iter()
            .filter(|(_, p)| p.required)
            .map(|(name, _)| name)
            .collect();
        json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
    }
}

pub fn validate_tool_bindings(
    ids: &[DefinitionId],
    definitions: &[ToolDefinition],
) -> Result<(), DomainError> {
    if ids.len() > 32
        || ids.len() != definitions.len()
        || ids
            .iter()
            .map(|id| &id.name)
            .collect::<std::collections::HashSet<_>>()
            .len()
            != ids.len()
    {
        return Err(DomainError::InvalidConfiguration(
            "a tool allowlist needs at most 32 distinct names and matching pinned definitions"
                .into(),
        ));
    }
    for (id, definition) in ids.iter().zip(definitions) {
        definition.validate()?;
        if id != &definition.id {
            return Err(DomainError::InvalidConfiguration(
                "tool allowlist and pinned definitions differ".into(),
            ));
        }
    }
    Ok(())
}
pub fn allowed_tool<'a>(
    definitions: &'a [ToolDefinition],
    name: &str,
) -> Result<&'a ToolDefinition, DomainError> {
    definitions
        .iter()
        .find(|tool| tool.id.name == name)
        .ok_or_else(|| {
            DomainError::InvalidWorkflow(format!(
                "native tool {name} is not allowed for this actor"
            ))
        })
}
pub fn check_tool_catalog(
    pins: &[ToolDefinition],
    installed: &[ToolDefinition],
) -> Result<(), DomainError> {
    for pin in pins {
        if !installed.contains(pin) {
            return Err(DomainError::InvalidWorkflow(format!(
                "restore pinned native tool {} v{} and its exact contract",
                pin.id.name, pin.id.version
            )));
        }
    }
    Ok(())
}

/// Workspace context is supplied by the host; it is never a model argument.
pub fn validate_tool_workspace(
    definitions: &[ToolDefinition],
    workspace: Option<&str>,
) -> Result<(), DomainError> {
    if workspace.is_some_and(|root| root.trim().is_empty() || root.len() > 4096)
        || (workspace.is_none() && definitions.iter().any(|tool| tool.requires_workspace))
    {
        return Err(DomainError::InvalidConfiguration("workspace tools require a bounded runtime-managed workspace; supply a source directory at actor creation".into()));
    }
    Ok(())
}

/// Portable permission entries: exact relative files or directory prefixes ending in '/'.
/// Filesystem/private-path checks remain the workspace adapter's responsibility.
pub fn validate_tool_permissions(
    definitions: &[ToolDefinition],
    paths: &[String],
) -> Result<(), DomainError> {
    let needs_write = definitions.iter().any(|tool| tool.requires_writable_paths);
    if (needs_write && paths.is_empty()) || (!needs_write && !paths.is_empty()) {
        return Err(DomainError::InvalidConfiguration(
            "writable paths are required only for write tools".into(),
        ));
    }
    validate_writable_paths(paths)
}
pub fn validate_writable_paths(paths: &[String]) -> Result<(), DomainError> {
    let mut unique = std::collections::HashSet::new();
    if paths.len() > 128
        || paths.iter().map(String::len).sum::<usize>() > 16_384
        || paths.iter().any(|path| {
            !portable_file_path(path.strip_suffix('/').unwrap_or(path)) || !unique.insert(path)
        })
    {
        return Err(DomainError::InvalidConfiguration(
            "write tools require 1..128 distinct relative writable paths (16 KiB total); use '/' separators and a trailing '/' for directory prefixes".into(),
        ));
    }
    Ok(())
}
pub fn portable_file_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && !path
            .chars()
            .any(|c| c.is_control() || matches!(c, ':' | '\\' | '*' | '?' | '"' | '<' | '>' | '|'))
        && path.split('/').all(|part| {
            !part.is_empty() && part != "." && part != ".." && !part.ends_with(['.', ' '])
        })
}

/// A scope may narrow exact files or directory prefixes, never widen them.
pub fn validate_write_scope(paths: &[String], allowed: &[String]) -> Result<(), DomainError> {
    validate_writable_paths(paths)?;
    if paths.is_empty()
        || paths
            .iter()
            .any(|path| !writable_path_permitted(allowed, path))
    {
        return Err(DomainError::InvalidConfiguration(
            "task writable paths must be nonempty and within the pinned writable paths".into(),
        ));
    }
    Ok(())
}

pub fn writable_path_permitted(allowed: &[String], path: &str) -> bool {
    allowed.iter().any(|entry| {
        if entry.ends_with('/') {
            path.starts_with(entry)
        } else {
            path == entry
        }
    })
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeToolRequest {
    pub definition: ToolDefinition,
    pub arguments: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writable_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test_command: Option<WorkspaceTestCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_command: Option<NamedWorkspaceCommand>,
}

/// Activity ID is the stable idempotency key across attempts; attempt ID identifies this execution.
pub struct NativeToolCall {
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub arguments: Value,
    pub workspace: Option<String>,
    pub writable_paths: Vec<String>,
    pub test_command: Option<WorkspaceTestCommand>,
    pub project_command: Option<NamedWorkspaceCommand>,
}
pub enum NativeToolResult {
    Success(Value),
    Rejected(ToolError),
    Failure(Failure),
}

/// Expected tool-level problems. Messages are bounded data for the next model decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(deny_unknown_fields)]
#[error("{message}")]
pub struct ToolError {
    pub code: String,
    pub message: String,
}
impl ToolError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
    pub fn observation(&self) -> Result<Value, DomainError> {
        if !valid_name(&self.code) || self.message.trim().is_empty() || self.message.len() > 4096 {
            return Err(DomainError::InvalidWorkflow(
                "tool feedback requires a bounded code and a message of at most 4 KiB".into(),
            ));
        }
        Ok(json!({"ok":false,"error":self}))
    }
}
pub fn is_native_tool_handler(handler: &DefinitionId) -> bool {
    handler.name == "native.tool" && matches!(handler.version, 1..=6)
}

/// Only this resolved, host-pinned entry crosses the approval/execution boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedWorkspaceCommand {
    pub name: String,
    pub command: WorkspaceTestCommand,
}
pub type ProjectCommands = BTreeMap<String, WorkspaceTestCommand>;
pub const PROJECT_COMMAND_INSTRUCTIONS: &str = " Call run_command with only a configured name from project_commands. The host pins executable, arguments, workspace, and timeout; you cannot override them. Each call needs approval. Completed nonzero exit is feedback; interrupted execution requires reconciliation. Every command may change files and invalidates earlier passing test evidence. Only fresh run_tests results verify completion.";
pub fn validate_project_commands(commands: &ProjectCommands) -> Result<(), DomainError> {
    if commands.len() > 16
        || commands.keys().any(|name| !valid_name(name))
        || serde_json::to_vec(commands)
            .map_err(|_| DomainError::InvalidState)?
            .len()
            > 32_768
    {
        return Err(DomainError::InvalidConfiguration("project commands need at most 16 distinct names (1..64 ASCII letters, digits, '_' or '-') and a 32 KiB catalog".into()));
    }
    for command in commands.values() {
        command.validate()?;
    }
    Ok(())
}
pub fn validate_tool_project_commands(
    definitions: &[ToolDefinition],
    commands: &ProjectCommands,
) -> Result<(), DomainError> {
    if definitions.iter().any(|tool| tool.requires_project_command) == commands.is_empty() {
        return Err(DomainError::InvalidConfiguration(
            "project commands are required only for command tools".into(),
        ));
    }
    validate_project_commands(commands)
}
pub fn validate_project_command(
    definition: &ToolDefinition,
    arguments: &Value,
    selected: Option<&NamedWorkspaceCommand>,
) -> Result<(), DomainError> {
    if definition.requires_project_command != selected.is_some() {
        return Err(DomainError::InvalidConfiguration(
            "a resolved project command is required only for command tools".into(),
        ));
    }
    if let Some(selected) = selected {
        if !valid_name(&selected.name) || arguments["name"].as_str() != Some(&selected.name) {
            return Err(DomainError::InvalidConfiguration(
                "command name differs from the pinned selection".into(),
            ));
        }
        selected.command.validate()?;
    }
    Ok(())
}
impl ToolDefinition {
    fn selected_command(
        &self,
        arguments: &Value,
        commands: &ProjectCommands,
    ) -> Result<Option<NamedWorkspaceCommand>, DomainError> {
        if !self.requires_project_command {
            return Ok(None);
        }
        validate_project_commands(commands)?;
        let name = arguments["name"].as_str().ok_or_else(|| {
            DomainError::InvalidWorkflow("run_command requires a configured name".into())
        })?;
        let command = commands.get(name).ok_or_else(|| {
            DomainError::InvalidWorkflow(format!("project command {name} is not configured"))
        })?;
        Ok(Some(NamedWorkspaceCommand {
            name: name.into(),
            command: command.clone(),
        }))
    }
    pub fn request_call_with_commands(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
        paths: &[String],
        tests: Option<&WorkspaceTestCommand>,
        commands: &ProjectCommands,
    ) -> Result<Command, DomainError> {
        let selected = self.selected_command(arguments, commands)?;
        self.request_resolved(arguments, workspace, paths, tests, selected)
    }
    /// The Auto policy authorizes routine work in its managed copy at launch.
    /// Keep the original definition and recovery policy in every activity receipt.
    #[allow(clippy::too_many_arguments)]
    pub fn request_agent_call(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
        paths: &[String],
        tests: Option<&WorkspaceTestCommand>,
        commands: &ProjectCommands,
        automatic: bool,
    ) -> Result<Command, DomainError> {
        if automatic
            && self.requires_workspace
            && self.id.version == 1
            && matches!(
                self.id.name.as_str(),
                "replace_text" | "create_file" | "run_tests" | "run_command"
            )
        {
            self.schedule_call_with_commands(arguments, workspace, paths, tests, commands)
        } else {
            self.request_call_with_commands(arguments, workspace, paths, tests, commands)
        }
    }
    pub fn schedule_call_with_commands(
        &self,
        arguments: &Value,
        workspace: Option<&str>,
        paths: &[String],
        tests: Option<&WorkspaceTestCommand>,
        commands: &ProjectCommands,
    ) -> Result<Command, DomainError> {
        let selected = self.selected_command(arguments, commands)?;
        self.schedule_resolved(arguments, workspace, paths, tests, selected)
    }
}

/// Actor-pinned executable and argv. No shell is added by the runtime.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceTestCommand {
    pub argv: Vec<String>,
    #[serde(default = "test_timeout")]
    pub timeout_ms: u32,
}
fn test_timeout() -> u32 {
    60_000
}
impl WorkspaceTestCommand {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.argv.is_empty()
            || self.argv.len() > 128
            || self.argv[0].trim().is_empty()
            || self.argv.iter().any(|arg| arg.contains('\0'))
            || self.argv.iter().map(String::len).sum::<usize>() > 16_384
            || !(1..=600_000).contains(&self.timeout_ms)
        {
            return Err(DomainError::InvalidConfiguration(
                "test command requires 1..128 argv entries (16 KiB total), a nonempty executable, no NUL bytes, and a timeout of 1..600000 ms".into(),
            ));
        }
        Ok(())
    }
}
pub fn validate_tool_test_command(
    definitions: &[ToolDefinition],
    command: Option<&WorkspaceTestCommand>,
) -> Result<(), DomainError> {
    if definitions.iter().any(|tool| tool.requires_test_command) != command.is_some() {
        return Err(DomainError::InvalidConfiguration(
            "a pinned test command is required only for test tools".into(),
        ));
    }
    if let Some(command) = command {
        command.validate()?;
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceEdit {
    pub path: String,
    pub old: String,
    pub new: String,
}

/// Create one new UTF-8 file; runtime supplies the workspace and permissions.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceCreate {
    pub path: String,
    pub content: String,
}

/// Bounded, case-sensitive literal search. The host supplies the managed root.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSearch {
    pub query: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub max_matches: Option<u32>,
    #[serde(default)]
    pub max_files: Option<u32>,
}
impl WorkspaceSearch {
    pub fn validate(&self) -> Result<(), ToolError> {
        if self.query.is_empty()
            || self.query.len() > 256
            || self.query.contains(['\0', '\r', '\n'])
        {
            return Err(ToolError::new(
                "search_query",
                "query must contain 1–256 UTF-8 bytes without NUL or line breaks",
            ));
        }
        if self.path.as_ref().is_some_and(|p| !portable_file_path(p)) {
            return Err(ToolError::new(
                "path_refused",
                "search path must use relative portable components and '/' separators; omit it for the root",
            ));
        }
        if !matches!(self.max_matches, None | Some(1..=100))
            || !matches!(self.max_files, None | Some(1..=1000))
        {
            return Err(ToolError::new(
                "search_limit",
                "max_matches must be 1–100 and max_files must be 1–1000",
            ));
        }
        Ok(())
    }
}

/// Portable project inspection actions; workspace access does not expose command execution.
pub enum WorkspaceReadAction {
    ListFiles,
    ReadFile { path: String },
}

pub const MAX_READ_FILES: u32 = 8;
pub const MAX_READ_FILE_TEXT_BYTES: usize = 16 * 1024;
pub const MAX_READ_BATCH_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_READ_BATCH_INPUT_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceReadBatch {
    pub paths: Vec<String>,
}
impl WorkspaceReadBatch {
    pub fn validate(&self) -> Result<(), ToolError> {
        if self.paths.is_empty() || self.paths.len() > MAX_READ_FILES as usize {
            return Err(ToolError::new(
                "batch_limit",
                "request between one and eight file paths",
            ));
        }
        if self.paths.iter().map(String::len).sum::<usize>() > MAX_READ_BATCH_INPUT_BYTES
            || serde_json::to_vec(self)
                .map_err(|_| ToolError::new("batch_input", "invalid batch"))?
                .len()
                > MAX_READ_BATCH_INPUT_BYTES
        {
            return Err(ToolError::new(
                "batch_input",
                "batch arguments exceed 32 KiB",
            ));
        }
        Ok(())
    }
}

pub(crate) fn model_contract_version() -> u32 {
    4
}
