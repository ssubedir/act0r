//! Configured MCP tools and a compatibility filesystem inspection adapter.
//! The engine journals the tool activity; this adapter owns only its local server process.
use super::{
    process_tree::ProcessTree,
    workspace::{
        LocalTools, MAX_FILE, MAX_OUTPUT, bounded_read, inspection_io, reads::ReadBatchResult,
    },
};
use crate::{
    application::{Error, ports::*},
    domain::{ActivityId, tools::*},
};
use rmcp::{RoleClient, ServiceExt, model::CallToolRequestParams, service::RunningService};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
};

mod catalog;
pub use catalog::{McpConfig, McpServerConfig, McpToolPolicy, discover_tools};

/// Trusted host configuration, never supplied by the model. The managed workspace is
/// appended as the server's sole runtime root. Use a direct executable, not a shell.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpFilesystemConfig {
    pub program: PathBuf,
    pub args: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}
fn default_timeout() -> u64 {
    30_000
}
impl McpFilesystemConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if !self.program.is_absolute()
            || !self.program.is_file()
            || self.args.len() > 16
            || self.args.iter().any(|s| s.len() > 4096 || s.contains('\0'))
            || !(1..=60_000).contains(&self.timeout_ms)
        {
            return Err(Error::Unavailable("MCP filesystem configuration requires an absolute executable, at most 16 bounded arguments, and a 1–60000 ms deadline".into()));
        }
        Ok(())
    }
    pub fn from_environment() -> Result<Option<Self>, Error> {
        let Some(path) = std::env::var_os("ACT0R_MCP_FILESYSTEM_CONFIG") else {
            return Ok(None);
        };
        let bytes = super::workspace::read_with_limit(Path::new(&path), 65_536)?;
        let config: Self = serde_json::from_slice(&bytes)?;
        config.validate()?;
        Ok(Some(config))
    }
}

/// File reads use MCP. Listing and text search retain their existing local adapters.
/// No advertised server writes, prompts, sampling or root changes are exposed.
pub struct McpWorkspaceReader {
    local: Arc<LocalTools>,
    config: McpFilesystemConfig,
}
impl McpWorkspaceReader {
    pub fn new(local: Arc<LocalTools>, config: McpFilesystemConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self { local, config })
    }
    fn path(&self, workspace: &Path, path: &str) -> Result<PathBuf, Error> {
        let file = self.local.file(workspace, path)?;
        if fs::metadata(&file).map_err(inspection_io)?.len() > MAX_FILE {
            return Err(ToolError::new("file_limit", "file exceeds tool size limit").into());
        }
        Ok(file)
    }
    async fn read(&self, workspace: String, path: String) -> Result<Value, Error> {
        let workspace = self.local.workspace(&workspace)?;
        let _lock = self.local.lock(&workspace)?;
        let file = self.path(&workspace, &path)?;
        let result = tokio::time::timeout(Duration::from_millis(self.config.timeout_ms), async {
            let mut server = Session::connect(&self.config, &workspace).await?;
            let result = server.read(&file).await;
            server.close().await;
            result
        })
        .await
        .map_err(|_| timeout_error())??;
        let mut end = result.len().min(MAX_OUTPUT);
        while !result.is_char_boundary(end) {
            end -= 1;
        }
        Ok(json!({"ok":true,"path":path,"text":&result[..end],"truncated":end < result.len()}))
    }
    async fn batch(&self, workspace: String, batch: WorkspaceReadBatch) -> Result<Value, Error> {
        batch.validate()?;
        let workspace = self.local.workspace(&workspace)?;
        let _lock = self.local.lock(&workspace)?;
        // Refuse paths before starting a server or sending any path to MCP.
        let files: Vec<_> = batch
            .paths
            .into_iter()
            .map(|path| {
                let file = if path.len() > 4096 {
                    Err(
                        ToolError::new("path_refused", "relative file paths must not exceed 4 KiB")
                            .into(),
                    )
                } else {
                    self.path(&workspace, &path)
                };
                (path, file)
            })
            .collect();
        tokio::time::timeout(Duration::from_millis(self.config.timeout_ms), async {
            let mut server = if files.iter().any(|(_, file)| file.is_ok()) {
                Some(Session::connect(&self.config, &workspace).await?)
            } else {
                None
            };
            let mut result = ReadBatchResult::new();
            let outcome = async {
                for (path, file) in files {
                    let text = match file {
                        Ok(file) => server.as_mut().unwrap().read(&file).await,
                        Err(error) => Err(error),
                    };
                    result.push(path, text)?;
                }
                Ok(result.finish())
            }
            .await;
            if let Some(server) = server.as_mut() {
                server.close().await;
            }
            outcome
        })
        .await
        .map_err(|_| timeout_error())?
    }
}
impl WorkspaceReader for McpWorkspaceReader {
    fn inspect(
        &self,
        workspace: String,
        action: WorkspaceReadAction,
        activity: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            match action {
                WorkspaceReadAction::ReadFile { path } => self.read(workspace, path).await,
                action => self.local.inspect(workspace, action, activity).await,
            }
        })
    }
    fn read_files(
        &self,
        workspace: String,
        batch: WorkspaceReadBatch,
        _: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move { self.batch(workspace, batch).await })
    }
    fn search_text(
        &self,
        workspace: String,
        search: WorkspaceSearch,
        activity: ActivityId,
    ) -> PortFuture<'_, Value> {
        self.local.search_text(workspace, search, activity)
    }
}
fn timeout_error() -> Error {
    std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "MCP filesystem deadline exceeded",
    )
    .into()
}
fn disconnected() -> Error {
    std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "MCP filesystem connection interrupted",
    )
    .into()
}

struct ServerProcess {
    // Tree ownership also terminates descendants when this future is cancelled.
    tree: ProcessTree,
    child: Child,
}
struct Session {
    client: RunningService<RoleClient, ()>,
    process: ServerProcess,
}
impl Session {
    async fn connect(config: &McpFilesystemConfig, workspace: &Path) -> Result<Self, Error> {
        let args = config
            .args
            .iter()
            .map(std::ffi::OsString::from)
            .chain(std::iter::once(workspace.as_os_str().to_owned()))
            .collect::<Vec<_>>();
        let mut session = Self::start(config, workspace, args).await?;
        if let Err(error) = session.check_tool().await {
            session.close().await;
            return Err(error);
        }
        Ok(session)
    }
    async fn start(
        config: &McpFilesystemConfig,
        workspace: &Path,
        args: Vec<std::ffi::OsString>,
    ) -> Result<Self, Error> {
        Self::start_with_env(config, workspace, args, &Default::default()).await
    }
    async fn start_with_env(
        config: &McpFilesystemConfig,
        workspace: &Path,
        args: Vec<std::ffi::OsString>,
        env: &std::collections::BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let mut command = Command::new(&config.program);
        command
            .args(args)
            .current_dir(workspace)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        // Keep only OS process essentials; model credentials and NODE_OPTIONS are not inherited.
        for key in ["SystemRoot", "WINDIR", "PATH", "TEMP", "TMP"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        for (key, value) in env {
            command.env(key, value);
        }
        ProcessTree::configure(&mut command);
        let mut child = command.spawn()?;
        let tree = ProcessTree::attach(&child)?;
        let stdout = child.stdout.take().ok_or_else(disconnected)?;
        let stdin = child.stdin.take().ok_or_else(disconnected)?;
        let process = ServerProcess { tree, child };
        // Eight 1 MiB files can each expand sixfold in JSON and appear in both content
        // and structuredContent. Bound the session without refusing that valid batch.
        let client =
            ().serve((stdout.take(128 * 1024 * 1024), stdin))
                .await
                .map_err(|_| disconnected())?;
        Ok(Self { client, process })
    }
    async fn check_tool(&self) -> Result<(), Error> {
        let mut cursor = None;
        let mut found = None;
        for _ in 0..4 {
            let catalog = self
                .client
                .list_tools(cursor)
                .await
                .map_err(|_| disconnected())?;
            if catalog.tools.len() > 256 {
                return Err(Error::Unavailable(
                    "MCP filesystem tool catalog exceeds its limit".into(),
                ));
            }
            if let Some(tool) = catalog
                .tools
                .into_iter()
                .find(|t| t.name == "read_text_file")
            {
                found = Some(tool);
                break;
            }
            let Some(next) = catalog.next_cursor else {
                break;
            };
            cursor = Some(rmcp::model::PaginatedRequestParams::default().with_cursor(Some(next)));
        }
        let tool = found.ok_or_else(|| {
            Error::Unavailable(
                "MCP filesystem server must expose read_text_file within four catalog pages".into(),
            )
        })?;
        let schema = serde_json::to_value(&tool.input_schema)?;
        if schema["type"] != "object"
            || schema["properties"]["path"]["type"] != "string"
            || schema["required"]
                .as_array()
                .is_none_or(|keys| keys != &[json!("path")])
        {
            return Err(Error::Unavailable("MCP read_text_file must accept a required string path with no other required arguments".into()));
        }
        Ok(())
    }
    async fn read(&self, file: &Path) -> Result<String, Error> {
        let result = self
            .client
            .call_tool(
                CallToolRequestParams::new("read_text_file")
                    .with_arguments(json!({"path":file}).as_object().unwrap().clone()),
            )
            .await
            .map_err(|_| disconnected())?;
        if result.is_error == Some(true) {
            return Err(ToolError::new(
                "mcp_read_refused",
                "the MCP filesystem server refused this read; choose another permitted file",
            )
            .into());
        }
        let text = if result.content.len() == 1 {
            result.content[0].as_text().map(|text| text.text.clone())
        } else {
            None
        }
        .ok_or_else(|| {
            Error::Unavailable("MCP read_text_file must return one text content block".into())
        })?;
        if text.len() as u64 > MAX_FILE {
            return Err(ToolError::new("file_limit", "file grew beyond the tool limit").into());
        }
        // Node decodes invalid UTF-8 using replacement characters. Only this ambiguous
        // case needs local byte validation to preserve the existing strict UTF-8 contract.
        if text.contains('\u{fffd}') {
            String::from_utf8(bounded_read(file)?)
                .map_err(|_| ToolError::new("invalid_utf8", "reading requires UTF-8 text"))?;
        }
        Ok(text)
    }
    async fn close(&mut self) {
        let _ = self.client.close_with_timeout(Duration::from_secs(1)).await;
        let _ = self.process.tree.terminate().await;
        let _ = tokio::time::timeout(Duration::from_secs(2), self.process.child.wait()).await;
    }
}
