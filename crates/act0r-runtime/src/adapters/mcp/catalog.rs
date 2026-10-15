//! Configuration-selected MCP tools use the existing native tool activity boundary.
use super::*;
use crate::domain::{Failure, RecoveryPolicy};
use rmcp::{model::Tool, service::ServiceError};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfig {
    pub servers: Vec<McpServerConfig>,
    /// Select external inspection tools directly for fresh workspace actors.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub replace_inspection: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerConfig {
    pub name: String,
    pub command: McpFilesystemConfig,
    pub tools: Vec<McpToolPolicy>,
    /// Explicit trusted host environment, independent of the model's environment.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpToolPolicy {
    pub name: String,
    /// Explicit host assertion. False selects approval-gated service actions.
    /// Server annotations never grant repeat safety or approval.
    pub read_only: bool,
    /// Project access is required unless the host explicitly opts out for this tool.
    #[serde(
        default = "workspace_required",
        skip_serializing_if = "workspace_is_required"
    )]
    pub requires_workspace: bool,
    #[serde(default)]
    pub path_parameters: Vec<String>,
    /// Existing relative directories, including "." for the project root.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub directory_parameters: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}
fn workspace_required() -> bool {
    true
}
fn workspace_is_required(required: &bool) -> bool {
    *required
}
fn invalid(message: &str) -> Error {
    Error::Unavailable(message.into())
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
impl McpConfig {
    pub fn from_environment() -> Result<Self, Error> {
        let Some(path) = std::env::var_os("ACT0R_MCP_CONFIG") else {
            return Ok(Self::default());
        };
        Self::load(Path::new(&path))
    }
    /// Resolve the same host configuration path for startup and local diagnostics.
    pub fn configuration_path(database: &Path) -> PathBuf {
        std::env::var_os("ACT0R_MCP_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                database
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join("mcp.json")
            })
    }
    /// Load and validate local configuration without starting server processes.
    pub fn for_database(database: &Path) -> Result<Self, Error> {
        if std::env::var_os("ACT0R_MCP_CONFIG").is_some() {
            return Self::from_environment();
        }
        let path = Self::configuration_path(database);
        match fs::symlink_metadata(&path) {
            Ok(_) => Self::load(&path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error.into()),
        }
    }
    fn load(path: &Path) -> Result<Self, Error> {
        let bytes = super::super::workspace::read_with_limit(path, 65_536)?;
        let config: Self = serde_json::from_slice(&bytes)?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), Error> {
        let mut names = HashSet::new();
        let mut aliases = HashSet::new();
        if self.replace_inspection && self.servers.is_empty() {
            return Err(invalid(
                "MCP inspection requires at least one configured server",
            ));
        }
        if self.replace_inspection
            && !self
                .servers
                .iter()
                .flat_map(|server| &server.tools)
                .any(|tool| tool.requires_workspace)
        {
            return Err(invalid(
                "MCP inspection replacement requires at least one workspace tool",
            ));
        }
        if self.servers.len() > 4 || self.servers.iter().map(|s| s.tools.len()).sum::<usize>() > 8 {
            return Err(invalid(
                "MCP configuration permits at most four servers and eight allowed tools",
            ));
        }
        for server in &self.servers {
            server.command.validate()?;
            let mut keys = HashSet::new();
            if server.env.len() > 8
                || server.env.iter().any(|(key, value)| {
                    key.is_empty()
                        || key.len() > 64
                        || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                        || !keys.insert(key.to_ascii_uppercase())
                        || value.len() > 8192
                        || value.contains('\0')
                })
            {
                return Err(invalid(
                    "MCP server environment requires at most eight distinct bounded variable names and values",
                ));
            }
            if !valid_name(&server.name) || !names.insert(&server.name) || server.tools.is_empty() {
                return Err(invalid(
                    "MCP server names must be distinct valid identifiers with a nonempty tool allowlist",
                ));
            }
            let mut tools = HashSet::new();
            for policy in &server.tools {
                if !policy.read_only && policy.requires_workspace {
                    return Err(invalid(
                        "MCP actions require requires_workspace:false; project mutations use the native workspace tools",
                    ));
                }
                if !policy.requires_workspace
                    && (!policy.path_parameters.is_empty()
                        || !policy.directory_parameters.is_empty())
                {
                    return Err(invalid(
                        "MCP tools without a workspace cannot declare scoped file or directory parameters",
                    ));
                }
                let alias = policy.alias(server);
                if policy.name.is_empty()
                    || policy.name.len() > 128
                    || !tools.insert(&policy.name)
                    || !valid_name(&alias)
                    || !alias.starts_with("mcp_")
                    || !aliases.insert(alias)
                    || policy.path_parameters.len() + policy.directory_parameters.len() > 32
                    || policy
                        .path_parameters
                        .iter()
                        .chain(&policy.directory_parameters)
                        .collect::<HashSet<_>>()
                        .len()
                        != policy.path_parameters.len() + policy.directory_parameters.len()
                {
                    return Err(invalid(
                        "MCP tools need distinct names/aliases and an explicit read_only host policy",
                    ));
                }
            }
        }
        Ok(())
    }
}
impl McpToolPolicy {
    fn alias(&self, server: &McpServerConfig) -> String {
        self.alias
            .clone()
            .unwrap_or_else(|| format!("mcp_{}_{}", server.name, self.name))
    }
}
impl McpServerConfig {
    async fn connect(&self, workspace: &Path) -> Result<Session, Error> {
        let args = self
            .command
            .args
            .iter()
            .map(|arg| {
                if arg == "{workspace}" {
                    workspace.as_os_str().to_owned()
                } else {
                    arg.into()
                }
            })
            .collect();
        Session::start_with_env(&self.command, workspace, args, &self.env).await
    }
    fn definition(&self, policy: &McpToolPolicy, tool: &Tool) -> Result<ToolDefinition, Error> {
        let input_schema = serde_json::to_value(&tool.input_schema)?;
        let output_schema = tool
            .output_schema
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?;
        let mut identity = json!({"server":self.name,"command":self.command,"policy":policy});
        if !self.env.is_empty() {
            identity["env"] = json!(self.env);
        }
        let binding = format!("mcp:{:x}", Sha256::digest(serde_json::to_vec(&identity)?));
        let contract = ExternalToolContract {
            binding,
            read_only: policy.read_only,
            input_schema,
            output_schema,
        };
        contract.validate()?;
        for parameter in policy
            .path_parameters
            .iter()
            .chain(&policy.directory_parameters)
        {
            let schema = &contract.input_schema["properties"][parameter];
            if schema["type"] != "string"
                && !(schema["type"] == "array" && schema["items"]["type"] == "string")
            {
                return Err(invalid(
                    "MCP path parameters must name declared string or string-array parameters",
                ));
            }
        }
        let description = tool
            .description
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(&policy.name);
        let mut description = if policy.requires_workspace {
            format!(
                "{description}\nReturns saved MCP content and structuredContent with an ok flag. Configured file path arguments {:?} must use relative project file paths; the runtime supplies the managed root.",
                policy.path_parameters
            )
        } else {
            format!(
                "{description}\nReturns saved MCP content and structuredContent with an ok flag. Runs without project context in an empty temporary working directory."
            )
        };
        if !policy.directory_parameters.is_empty() {
            description.push_str(&format!(" Directory arguments {:?} use relative project directories, or . for the root. The server inspects a temporary project snapshot excluding private/generated files and links. Returned absolute paths refer to that temporary snapshot; use relative paths in subsequent calls.", policy.directory_parameters));
        }
        if !policy.read_only {
            description = format!(
                "MCP action {}/{}: {description}\nRequires human approval for each call. An unknown or failed response after dispatch requires reconciliation before retrying.",
                self.name, policy.name
            );
        }
        Ok(ToolDefinition::builder(
            policy.alias(self),
            1,
            if policy.read_only {
                RecoveryPolicy::SafeToRepeat
            } else {
                RecoveryPolicy::RequiresReconciliation
            },
        )
        .description(description)
        .external_contract(contract)
        .requires_approval(!policy.read_only)
        .requires_workspace(policy.requires_workspace)
        .reports_errors(true)
        .byte_limits(32_768, 65_536)
        .build()?)
    }
}
async fn catalog(session: &Session) -> Result<Vec<Tool>, Error> {
    let mut tools = Vec::new();
    let mut names = HashSet::new();
    let mut cursor = None;
    for _ in 0..4 {
        let page = session
            .client
            .list_tools(cursor)
            .await
            .map_err(|_| disconnected())?;
        for tool in page.tools {
            if tools.len() == 256 || !names.insert(tool.name.clone()) {
                return Err(invalid(
                    "MCP catalog exceeds 256 tools or contains duplicate names",
                ));
            }
            tools.push(tool);
        }
        let Some(next) = page.next_cursor else {
            return Ok(tools);
        };
        cursor = Some(rmcp::model::PaginatedRequestParams::default().with_cursor(Some(next)));
    }
    Err(invalid("MCP catalog exceeds four pages"))
}

/// Discover only host-allowlisted contracts against an empty temporary root. No tool
/// calls occur at startup. Every execution rechecks its selected contract in its own scope.
pub async fn discover_tools(
    config: McpConfig,
    local: Arc<LocalTools>,
    catalog_parent: &Path,
) -> Result<Vec<Arc<dyn NativeTool>>, Error> {
    config.validate()?;
    if config.servers.is_empty() {
        return Ok(vec![]);
    }
    fs::create_dir_all(catalog_parent)?;
    let root = tempfile::Builder::new()
        .prefix("mcp-catalog-")
        .tempdir_in(catalog_parent)?;
    let root_path = fs::canonicalize(root.path())?;
    let mut tools: Vec<Arc<dyn NativeTool>> = vec![];
    for server in config.servers {
        let definitions =
            tokio::time::timeout(Duration::from_millis(server.command.timeout_ms), async {
                let mut session = server.connect(&root_path).await?;
                let result = catalog(&session).await;
                session.close().await;
                result
            })
            .await
            .map_err(|_| timeout_error())??;
        for policy in &server.tools {
            let tool = definitions
                .iter()
                .find(|t| t.name.as_ref() == policy.name)
                .ok_or_else(|| {
                    invalid("an allowed MCP tool is missing from the discovered catalog")
                })?;
            let definition = server.definition(policy, tool)?;
            tools.push(Arc::new(McpNativeTool {
                server: server.clone(),
                policy: policy.clone(),
                definition,
                local: local.clone(),
                scratch_root: root_path.parent().unwrap().to_path_buf(),
            }));
        }
    }
    Ok(tools)
}
struct McpNativeTool {
    server: McpServerConfig,
    policy: McpToolPolicy,
    definition: ToolDefinition,
    local: Arc<LocalTools>,
    scratch_root: PathBuf,
}
/// Windows can retain the server's working-directory handle briefly after its job
/// is killed. Retry disposal outside the cancelled activity without holding a worker.
struct InspectionView(Option<tempfile::TempDir>);
impl InspectionView {
    fn new(workspace: &Path) -> Result<Self, Error> {
        let parent = workspace.join(super::super::workspace::PRIVATE);
        // The workspace lock excludes live inspections. Reclaim views left by a
        // host exit before Drop ran, without touching journals or following links.
        for entry in fs::read_dir(&parent)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with("mcp-inspection-")
                && metadata.is_dir()
                && !super::super::workspace::link(&metadata)
            {
                let path = fs::canonicalize(entry.path())?;
                if path.parent() != Some(parent.as_path()) {
                    return Err(invalid(
                        "MCP snapshot cleanup must stay in the private workspace directory",
                    ));
                }
                if let Err(error) = fs::remove_dir_all(path) {
                    tracing::warn!(%error, "could not reclaim an old MCP inspection snapshot");
                }
            }
        }
        Self::create(&parent, "mcp-inspection-")
    }
    fn create(parent: &Path, prefix: &str) -> Result<Self, Error> {
        let directory = tempfile::Builder::new().prefix(prefix).tempdir_in(parent)?;
        if !directory.path().is_absolute() || directory.path().parent() != Some(parent) {
            return Err(invalid(
                "MCP inspection view must stay in the private workspace directory",
            ));
        }
        Ok(Self(Some(directory)))
    }
    fn path(&self) -> &Path {
        self.0.as_ref().unwrap().path()
    }
}
impl Drop for InspectionView {
    fn drop(&mut self) {
        let directory = self.0.take().unwrap();
        let path = directory.path().to_path_buf();
        if directory.close().is_ok() {
            return;
        }
        let cleanup = move || {
            for _ in 0..100 {
                match fs::remove_dir_all(&path) {
                    Ok(()) => return,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
                    Err(_) => std::thread::sleep(Duration::from_millis(50)),
                }
            }
            tracing::warn!("MCP inspection snapshot cleanup did not complete");
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn_blocking(cleanup);
        } else {
            cleanup();
        }
    }
}
impl McpNativeTool {
    async fn invoke(&self, call: NativeToolCall, dispatched: &AtomicBool) -> Result<Value, Error> {
        let contract = self.definition.external.as_ref().unwrap();
        let mut arguments = contract.arguments(&call.arguments);
        if !self.policy.requires_workspace {
            let directory = InspectionView::create(&self.scratch_root, "mcp-general-")?;
            return self
                .invoke_in(directory.path(), arguments, dispatched)
                .await;
        }
        let workspace = self.local.workspace(
            call.workspace
                .as_deref()
                .ok_or_else(|| invalid("MCP tool requires a pinned managed workspace"))?,
        )?;
        let _lock = self.local.lock(&workspace)?;
        let path_count: usize = self
            .policy
            .path_parameters
            .iter()
            .chain(&self.policy.directory_parameters)
            .filter_map(|name| arguments.get(name))
            .map(|value| {
                if value.is_null() {
                    0
                } else {
                    value.as_array().map_or(1, Vec::len)
                }
            })
            .sum();
        if path_count > MAX_READ_FILES as usize {
            return Err(ToolError::new(
                "path_limit",
                "one MCP activity may inspect at most eight configured paths",
            )
            .into());
        }
        for name in &self.policy.path_parameters {
            let Some(value) = arguments.get_mut(name) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            if let Some(paths) = value.as_array_mut() {
                for path in paths {
                    *path = self.path(&workspace, path)?;
                }
            } else {
                *value = self.path(&workspace, value)?;
            }
        }
        // Directory tools may recurse. Give them a disposable project-only view,
        // reusing the workspace copier's exclusions, link refusal and size limits.
        // Keep private journals, environment files and generated trees out of MCP.
        for name in &self.policy.directory_parameters {
            let Some(value) = arguments.get_mut(name) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            if let Some(paths) = value.as_array_mut() {
                for path in paths {
                    *path = self.directory(&workspace, path)?;
                }
            } else {
                *value = self.directory(&workspace, value)?;
            }
        }
        let view = if self.policy.directory_parameters.is_empty() {
            None
        } else {
            let view = InspectionView::new(&workspace)?;
            super::super::workspace::copy_tree(&workspace, view.path(), view.path(), &mut (0, 0))?;
            for name in self
                .policy
                .path_parameters
                .iter()
                .chain(&self.policy.directory_parameters)
            {
                let Some(value) = arguments.get_mut(name) else {
                    continue;
                };
                let map = |value: &mut Value| -> Result<(), Error> {
                    if let Some(path) = value.as_str() {
                        let relative = Path::new(path)
                            .strip_prefix(&workspace)
                            .map_err(|_| invalid("MCP scoped path is outside the workspace"))?;
                        *value = json!(view.path().join(relative));
                    }
                    Ok(())
                };
                if let Some(paths) = value.as_array_mut() {
                    for path in paths {
                        map(path)?;
                    }
                } else {
                    map(value)?;
                }
            }
            Some(view)
        };
        let root = view
            .as_ref()
            .map_or(workspace.as_path(), |view| view.path());
        self.invoke_in(root, arguments, dispatched).await
    }
    async fn invoke_in(
        &self,
        root: &Path,
        arguments: Value,
        dispatched: &AtomicBool,
    ) -> Result<Value, Error> {
        let contract = self.definition.external.as_ref().unwrap();
        let mut session = self.server.connect(root).await?;
        let outcome = async {
            let tools = catalog(&session).await?;
            let tool = tools
                .iter()
                .find(|t| t.name.as_ref() == self.policy.name)
                .ok_or_else(|| {
                    invalid("restore the pinned MCP tool; it is no longer advertised")
                })?;
            if self.server.definition(&self.policy, tool)? != self.definition {
                return Err(invalid(
                    "restore the exact pinned MCP tool contract before retrying",
                ));
            }
            // From this boundary onward, loss of a response cannot establish that
            // an action had no effect. Never turn it into an automatic retry.
            dispatched.store(true, Ordering::Relaxed);
            let result = session
                .client
                .call_tool(
                    CallToolRequestParams::new(self.policy.name.clone())
                        .with_arguments(arguments.as_object().unwrap().clone()),
                )
                .await
                .map_err(|error| match error {
                    ServiceError::McpError(_) => {
                        invalid("MCP server rejected the tool request at the protocol layer")
                    }
                    _ => disconnected(),
                })?;
            if !self.policy.read_only && result.is_error == Some(true) {
                return Err(invalid(
                    "MCP action reported an error; inspect the service for partial effects",
                ));
            }
            if result
                .content
                .iter()
                .any(|content| content.as_text().is_none())
            {
                return Err(invalid(
                    "this MCP adapter supports text and structured JSON results",
                ));
            }
            if result.is_error != Some(true) {
                contract.validate_output(result.structured_content.as_ref())?;
            }
            let mut saved = serde_json::to_value(&result)?;
            saved["ok"] = json!(result.is_error != Some(true));
            if !self.policy.read_only
                && saved.to_string().len() > self.definition.max_output_bytes as usize
            {
                return Err(invalid(
                    "MCP action result exceeded its pinned output limit",
                ));
            }
            Ok(saved)
        }
        .await;
        session.close().await;
        outcome
    }
    fn path(&self, workspace: &Path, value: &Value) -> Result<Value, Error> {
        let Some(path) = value.as_str().filter(|path| path.len() <= 4096) else {
            return Err(ToolError::new(
                "path_refused",
                "MCP file paths require bounded relative strings",
            )
            .into());
        };
        let file = self.local.file(workspace, path)?;
        if fs::metadata(&file).map_err(inspection_io)?.len() > MAX_FILE {
            return Err(
                ToolError::new("file_limit", "MCP scoped files are limited to 1 MiB").into(),
            );
        }
        Ok(json!(file))
    }
    fn directory(&self, workspace: &Path, value: &Value) -> Result<Value, Error> {
        let path = value
            .as_str()
            .filter(|path| path.len() <= 4096)
            .ok_or_else(|| {
                ToolError::new(
                    "path_refused",
                    "MCP directory paths require bounded relative strings",
                )
            })?;
        Ok(json!(self.local.directory(workspace, path)?))
    }
}
impl NativeTool for McpNativeTool {
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            self.definition.validate_arguments(&call.arguments)?;
            let dispatched = AtomicBool::new(false);
            let outcome = tokio::time::timeout(
                Duration::from_millis(self.server.command.timeout_ms),
                self.invoke(call, &dispatched),
            )
            .await;
            let outcome = outcome.unwrap_or_else(|_| Err(timeout_error()));
            if !self.policy.read_only
                && dispatched.load(Ordering::Relaxed)
                && let Err(error) = &outcome
            {
                return Ok(NativeToolResult::Uncertain(format!(
                    "MCP action outcome requires inspection before retry: {error}"
                )));
            }
            Ok(match outcome {
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
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::WouldBlock
                    ) =>
                {
                    NativeToolResult::Failure(Failure::new(
                        "mcp_connection",
                        "temporary MCP connection failure",
                        true,
                    ))
                }
                Err(error) => {
                    NativeToolResult::Failure(Failure::new("mcp_tool", error.to_string(), false))
                }
            })
        })
    }
}
