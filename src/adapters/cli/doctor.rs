//! Local setup diagnostics. Never bootstrap an engine, HTTP client or MCP process.
use super::chat::{self, ActionFormat};
use crate::{
    Error,
    adapters::{
        mcp::{McpConfig, McpFilesystemConfig},
        model::ModelRouter,
    },
};
use clap::Args;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

#[derive(Args)]
pub(super) struct DoctorOptions {
    /// Check access to an optional project and its database placement.
    #[arg(long)]
    project: Option<PathBuf>,
    /// New-chat model name; otherwise use ACT0R_MODEL or the harness default.
    #[arg(long)]
    model: Option<String>,
    /// Full Chat Completions URL; otherwise use ACT0R_MODEL_ENDPOINT.
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long, value_enum)]
    action_format: Option<ActionFormat>,
    /// New-chat request deadline in milliseconds (default 90000).
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=600_000))]
    model_timeout_ms: Option<u32>,
}

#[derive(Serialize)]
struct Check {
    name: String,
    status: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    fix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
}
impl Check {
    fn new(name: &str, path: Option<&Path>, result: Result<String, String>, fix: &str) -> Self {
        let (status, message, fix) = match result {
            Ok(message) => ("ok", message, None),
            Err(message) => ("error", message, Some(fix.into())),
        };
        Self {
            name: name.into(),
            status,
            message,
            fix,
            path: path.map(|path| path.to_string_lossy().into_owned()),
        }
    }
}

pub(super) fn execute(
    options: &DoctorOptions,
    database: &Path,
    json_output: bool,
) -> Result<(), Error> {
    let target = chat::model_target(
        options.model.as_deref(),
        options.endpoint.as_deref(),
        options.action_format,
        options.model_timeout_ms,
    )?;
    let mut checks = Vec::new();
    let name_valid = !target.model.trim().is_empty() && target.model.len() <= 256;
    checks.push(Check::new(
        "model",
        None,
        if name_valid {
            target
                .validate()
                .map(|_| "Model and action format configured.".into())
                .map_err(|e| e.to_string())
        } else {
            Err("Model name must be nonblank and fit within 256 UTF-8 bytes.".into())
        },
        "Set --model or ACT0R_MODEL to the model identifier served by your endpoint.",
    ));
    let endpoint = ModelRouter::validate_endpoint(&target.endpoint).and_then(|local| {
        if target.endpoint.len() <= 4096 {
            Ok(local)
        } else {
            Err("model endpoint exceeds 4096 UTF-8 bytes")
        }
    });
    checks.push(Check::new("endpoint", None,
        endpoint.map(|_| "Compatible endpoint URL configured; connectivity not tested.".into()).map_err(str::to_owned),
        "Set --endpoint or ACT0R_MODEL_ENDPOINT to the full HTTPS Chat Completions URL (loopback HTTP is allowed), without credentials, query parameters or fragments."));
    let key_available = ModelRouter::environment_key_available();
    checks.push(Check::new("credentials", None,
        if key_available { Ok("OPENAI_API_KEY is present; validity not tested.".into()) }
        else if endpoint == Ok(true) { Ok("Loopback endpoint: OPENAI_API_KEY is optional.".into()) }
        else if endpoint.is_err() { Ok("No key found; credential requirements depend on a valid endpoint.".into()) }
        else { Err("OPENAI_API_KEY is missing or blank for the hosted endpoint.".into()) },
        "Set OPENAI_API_KEY in the environment that launches act0r, then rerun doctor. Hosted compatible services, including NVIDIA, use this same variable."));

    let resolved_database = resolve_path(database);
    checks.push(Check::new("database", Some(database),
        resolved_database.as_ref().map_err(io_message).and_then(|path| {
            check_database(path).map(|_| format!("Database path is usable: {}", path.display())).map_err(|e| io_message(&e))
        }),
        "Choose --db with a regular read/write file (or a new filename) and a writable parent directory for SQLite and its sidecars."));
    let workspaces = resolved_database
        .as_ref()
        .ok()
        .and_then(|p| p.parent())
        .map(|p| p.join(".act0r/workspaces"));
    if let Some(path) = &workspaces {
        checks.push(Check::new("workspaces", Some(path),
            check_directory(path).map(|_| "Managed workspace directory can be created or written.".into()).map_err(|e| io_message(&e)),
            "Make the database parent's .act0r/workspaces directory writable, or select another --db location."));
    }

    if let Some(project) = &options.project {
        checks.push(Check::new("project", Some(project),
            check_project(project).and_then(|path| {
                if let Ok(database) = &resolved_database {
                    chat::validate_database_project(database, &path).map_err(|e| e.to_string())?;
                }
                if workspaces.as_ref().is_some_and(|root| resolve_path(root).is_ok_and(|root| path.starts_with(root))) {
                    return Err("Project is inside the managed workspace directory.".into());
                }
                Ok(format!("Project directory is readable: {}", path.display()))
            }),
            "Choose a readable project directory outside managed workspaces. Keep --db outside the project or under its excluded .act0r/ or .build/ directory."));
    }

    let mcp_path = McpConfig::configuration_path(database);
    let mcp = McpConfig::for_database(database);
    checks.push(Check::new("mcp_config", Some(&mcp_path), match &mcp {
        Ok(config) if config.servers.is_empty() => Ok("No MCP servers configured; built-in inspection remains available.".into()),
        Ok(config) => Ok(format!("{} server(s), {} allowed tool(s), {} inspection. Servers not started.", config.servers.len(), config.servers.iter().map(|s| s.tools.len()).sum::<usize>(), if config.replace_inspection { "MCP-only" } else { "additive" })),
        Err(error) => Err(config_error(error)),
    }, "Check mcp.json beside --db, or the ACT0R_MCP_CONFIG override. Use the documented schema, absolute existing executables, bounded arguments and explicit read_only tool policies (docs/mcp.md)."));
    if let Ok(config) = &mcp {
        for server in &config.servers {
            executable_check(
                &mut checks,
                &format!("mcp_server:{}", server.name),
                &server.command,
            );
        }
    }
    // Startup also supports the optional compatibility file-reader bridge.
    if let Some(path) = std::env::var_os("ACT0R_MCP_FILESYSTEM_CONFIG") {
        let path = PathBuf::from(path);
        match McpFilesystemConfig::from_environment() {
            Ok(Some(config)) => executable_check(&mut checks, "mcp_file_reader", &config),
            Ok(None) => {}
            Err(error) => checks.push(Check::new("mcp_file_reader", Some(&path), Err(config_error(&error)),
                "Fix ACT0R_MCP_FILESYSTEM_CONFIG using the compatibility bridge format in docs/mcp.md, or unset it to use local reads.")),
        }
    }

    let ready = checks.iter().all(|check| check.status == "ok");
    let mut report = json!({
        "ready":ready,
        "scope":"local_configuration",
        "model":{
            "name":name_valid.then_some(&target.model),
            "endpoint":endpoint.is_ok().then_some(&target.endpoint),
            "action_format":target.action_format,
            "request_timeout_ms":target.policy.request_timeout_ms,
            "api_key_present":key_available,
        },
        "checks":checks,
    });
    // Fixed diagnostics avoid echoing malformed input; also redact the configured key
    // if a caller accidentally included it in a model name or a filesystem path.
    if let Ok(key) = std::env::var("OPENAI_API_KEY")
        && !key.trim().is_empty()
    {
        redact(&mut report, &key);
    }
    if json_output {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("act0r doctor — local chat setup");
        let model = &report["model"];
        println!(
            "Model: {}\nEndpoint: {}\nActions: {}\nRequest timeout: {} ms",
            visible(model["name"].as_str().unwrap_or("invalid")),
            visible(
                model["endpoint"]
                    .as_str()
                    .unwrap_or("invalid (not displayed)")
            ),
            model["action_format"].as_str().unwrap_or("unknown"),
            model["request_timeout_ms"]
        );
        for check in report["checks"].as_array().unwrap() {
            println!(
                "[{}] {}: {}",
                check["status"].as_str().unwrap(),
                check["name"].as_str().unwrap(),
                visible(check["message"].as_str().unwrap())
            );
            if let Some(path) = check["path"].as_str() {
                println!("  Path: {}", visible(path));
            }
            if let Some(fix) = check["fix"].as_str() {
                println!("  Fix: {fix}");
            }
        }
        println!(
            "{} Local checks do not test model connectivity/authentication, MCP handshakes or project commands.",
            if ready {
                "Local setup checks passed."
            } else {
                "Resolve the errors above and rerun doctor."
            }
        );
    }
    if ready {
        Ok(())
    } else {
        Err(Error::Unavailable("local setup checks failed".into()))
    }
}

fn io_message(error: &io::Error) -> String {
    format!("Filesystem check failed ({:?}).", error.kind())
}
fn config_error(error: &Error) -> String {
    match error {
        Error::Json(error) => format!(
            "Invalid MCP JSON or schema at line {}, column {}.",
            error.line(),
            error.column()
        ),
        Error::Io(error) => io_message(error),
        Error::Unavailable(message) => message.clone(),
        _ => "MCP configuration could not be validated.".into(),
    }
}
fn executable_check(checks: &mut Vec<Check>, name: &str, config: &McpFilesystemConfig) {
    checks.push(Check::new(name, Some(&config.program), check_executable(&config.program)
        .map(|_| "Server executable exists; command was not launched.".into()).map_err(|e| io_message(&e)),
        "Set command.program to an absolute installed executable with execute permission. Use a direct executable and argument array; shell wrappers are not added."));
}
fn check_executable(path: &Path) -> io::Result<()> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
    }
    Ok(())
}
fn check_project(path: &Path) -> Result<PathBuf, String> {
    let path = fs::canonicalize(path).map_err(|e| io_message(&e))?;
    let text = path.to_str().ok_or("Project path must be UTF-8.")?;
    crate::domain::harness::validate_workspace(text).map_err(|e| e.to_string())?;
    let mut entries = fs::read_dir(&path).map_err(|e| io_message(&e))?;
    if let Some(entry) = entries.next() {
        entry.map_err(|e| io_message(&e))?;
    }
    Ok(path)
}
fn check_database(path: &Path) -> io::Result<()> {
    match fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            if metadata.permissions().readonly() {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            // No create/truncate and no SQLite connection: existing contents are untouched.
            fs::OpenOptions::new().read(true).write(true).open(path)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    check_directory(path.parent().ok_or(io::ErrorKind::InvalidInput)?)
}
fn check_directory(path: &Path) -> io::Result<()> {
    let mut existing = path;
    loop {
        match fs::symlink_metadata(existing) {
            Ok(_) => {
                let metadata = fs::metadata(existing)?;
                if !metadata.is_dir() {
                    return Err(io::ErrorKind::NotADirectory.into());
                }
                #[cfg(unix)]
                if metadata.permissions().readonly() {
                    return Err(io::ErrorKind::PermissionDenied.into());
                }
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                existing = existing.parent().ok_or(error)?
            }
            Err(error) => return Err(error),
        }
    }
    // Probe effective access (including ACLs), not just permission bits. No permanent
    // directories, database files or workspace files are created.
    let probe = tempfile::Builder::new()
        .prefix("act0r-doctor-")
        .tempfile_in(existing)?;
    probe.close()
}
fn resolve_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match fs::symlink_metadata(existing) {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(existing.file_name().ok_or(io::ErrorKind::InvalidInput)?);
                existing = existing.parent().ok_or(error)?;
            }
            Err(error) => return Err(error),
        }
    }
    let mut path = fs::canonicalize(existing)?;
    for component in missing.into_iter().rev() {
        path.push(component);
    }
    Ok(path)
}
fn redact(value: &mut Value, key: &str) {
    match value {
        Value::String(text) => *text = text.replace(key, "[REDACTED]"),
        Value::Array(items) => items.iter_mut().for_each(|item| redact(item, key)),
        Value::Object(items) => items.values_mut().for_each(|item| redact(item, key)),
        _ => {}
    }
}
fn visible(text: &str) -> String {
    text.chars()
        .flat_map(|ch| {
            if ch.is_control() {
                ch.escape_default().collect::<Vec<_>>()
            } else {
                vec![ch]
            }
        })
        .collect()
}
