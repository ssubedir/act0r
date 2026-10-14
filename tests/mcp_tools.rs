use act0r::{
    Error, Worker,
    adapters::{
        mcp::{McpFilesystemConfig, McpWorkspaceReader},
        native_tools::ReadFileTool,
        workspace::{LocalTools, LocalWorkspaces},
        workspace_reads::ReadFilesTool,
    },
    application::{ToolRegistry, ports::*},
    bootstrap::local_app_with_mcp,
    domain::{actors::*, tools::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

fn executable(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|path| {
            path.join(if cfg!(windows) {
                format!("{name}.exe")
            } else {
                name.into()
            })
        })
        .find(|path| path.is_file())
        .expect("Node.js is required for these optional MCP integration tests")
}
fn config(root: &Path, mode: &str) -> McpFilesystemConfig {
    McpFilesystemConfig {
        program: executable("node"),
        args: vec![
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/mcp-filesystem.mjs")
                .to_string_lossy()
                .into(),
            mode.into(),
            root.join("calls.jsonl").to_string_lossy().into(),
        ],
        timeout_ms: 10_000,
    }
}
fn events(root: &Path) -> Vec<Value> {
    fs::read_to_string(root.join("calls.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn tool_calls(root: &Path) -> Vec<Value> {
    events(root)
        .into_iter()
        .filter(|v| v["method"] == "tools/call")
        .collect()
}
async fn fixture(root: &Path) -> (String, Arc<LocalTools>) {
    let source = root.join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("README.md"), "Original evidence.\r\n😀\n").unwrap();
    fs::write(source.join("src/lib.rs"), "fn durable() {}\n").unwrap();
    let managed = root.join("workspaces");
    let workspace = LocalWorkspaces::new(managed.clone())
        .prepare(source)
        .await
        .unwrap();
    (workspace, Arc::new(LocalTools::new(managed)))
}
fn execution(definition: &ToolDefinition, workspace: &str, arguments: Value) -> ActivityExecution {
    let Command::Schedule(spec) = definition
        .schedule_call_in(&arguments, Some(workspace))
        .unwrap()
    else {
        panic!()
    };
    ActivityExecution {
        run_id: RunId::new(),
        activity_id: ActivityId::new(),
        attempt_id: AttemptId::new(),
        ordinal: 1,
        handler: spec.handler,
        input: spec.input,
    }
}

#[tokio::test]
async fn invalid_scope_and_refused_batches_never_start_a_server_and_contracts_stay_identical() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, local) = fixture(dir.path()).await;
    // The test executable is never started: these requests are refused before dispatch.
    let config = McpFilesystemConfig {
        program: std::env::current_exe().unwrap(),
        args: vec![],
        timeout_ms: 100,
    };
    let reader = Arc::new(McpWorkspaceReader::new(local.clone(), config).unwrap());
    assert_eq!(
        ReadFileTool::new(local.clone()).definition(),
        ReadFileTool::new(reader.clone()).definition()
    );
    assert_eq!(
        ReadFilesTool::new(local.clone()).definition(),
        ReadFilesTool::new(reader.clone()).definition()
    );
    let error = reader
        .inspect(
            dir.path().to_string_lossy().into(),
            WorkspaceReadAction::ReadFile {
                path: "README.md".into(),
            },
            ActivityId::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unavailable(_)));
    let paths: Vec<String> = [
        "../outside",
        ".env",
        ".act0r-tools/managed",
        "missing",
        "src",
        "target/private",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    let result = reader
        .read_files(workspace, WorkspaceReadBatch { paths }, ActivityId::new())
        .await
        .unwrap();
    assert!(
        result["files"]
            .as_array()
            .unwrap()
            .iter()
            .all(|f| f["ok"] == false)
    );
    assert!(events(dir.path()).is_empty());
    let mut bad = McpFilesystemConfig {
        program: PathBuf::from("node"),
        args: vec![],
        timeout_ms: 100,
    };
    assert!(bad.validate().is_err());
    bad.program = std::env::current_exe().unwrap();
    bad.timeout_ms = 0;
    assert!(bad.validate().is_err());
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_tools -- --ignored"]
async fn mcp_reads_match_local_observations_and_only_dispatch_permitted_read_tools() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, local) = fixture(dir.path()).await;
    fs::write(Path::new(&workspace).join("raw.bin"), [255, 254]).unwrap();
    fs::write(
        Path::new(&workspace).join("replacement.txt"),
        "valid � character",
    )
    .unwrap();
    fs::write(Path::new(&workspace).join("big.txt"), vec![b'x'; 1_048_577]).unwrap();
    fs::write(Path::new(&workspace).join("large.txt"), "😀".repeat(6000)).unwrap();
    let reader =
        Arc::new(McpWorkspaceReader::new(local.clone(), config(dir.path(), "success")).unwrap());
    for file in ["README.md", "large.txt", "replacement.txt"] {
        assert_eq!(
            local
                .inspect(
                    workspace.clone(),
                    WorkspaceReadAction::ReadFile { path: file.into() },
                    ActivityId::new()
                )
                .await
                .unwrap(),
            reader
                .inspect(
                    workspace.clone(),
                    WorkspaceReadAction::ReadFile { path: file.into() },
                    ActivityId::new()
                )
                .await
                .unwrap()
        );
    }
    let batch = WorkspaceReadBatch {
        paths: [
            "README.md",
            "missing",
            "src/lib.rs",
            "../outside",
            "raw.bin",
            "big.txt",
            "README.md",
            ".env",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
    };
    let result = reader
        .read_files(workspace.clone(), batch.clone(), ActivityId::new())
        .await
        .unwrap();
    assert_eq!(
        result,
        local
            .read_files(workspace.clone(), batch, ActivityId::new())
            .await
            .unwrap()
    );
    let calls = tool_calls(dir.path());
    assert_eq!(calls.len(), 7);
    assert!(
        calls
            .iter()
            .all(|v| v["params"]["name"] == "read_text_file")
    );
    assert!(calls.iter().all(|v| {
        Path::new(v["params"]["arguments"]["path"].as_str().unwrap()).starts_with(&workspace)
    }));
    let starts: Vec<_> = events(dir.path())
        .into_iter()
        .filter(|v| v["kind"] == "started")
        .collect();
    assert_eq!(starts.len(), 4); // One process per read or entire batch.
    assert!(starts.iter().all(|v| v["credentials"] == false
        && fs::canonicalize(v["root"].as_str().unwrap()).unwrap()
            == fs::canonicalize(v["cwd"].as_str().unwrap()).unwrap()));
    // Listing retains its native contract and does not spawn another MCP process.
    assert_eq!(
        reader
            .inspect(
                workspace.clone(),
                WorkspaceReadAction::ListFiles,
                ActivityId::new()
            )
            .await
            .unwrap(),
        local
            .inspect(workspace, WorkspaceReadAction::ListFiles, ActivityId::new())
            .await
            .unwrap()
    );
    assert_eq!(
        events(dir.path())
            .iter()
            .filter(|v| v["kind"] == "started")
            .count(),
        4
    );
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_tools -- --ignored"]
async fn expected_mcp_errors_malformed_results_and_disconnects_have_distinct_outcomes() {
    for mode in [
        "success",
        "malformed",
        "oversized",
        "incompatible",
        "disconnect_once",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (workspace, local) = fixture(dir.path()).await;
        fs::write(Path::new(&workspace).join("refused.txt"), "safe").unwrap();
        let reader = Arc::new(McpWorkspaceReader::new(local, config(dir.path(), mode)).unwrap());
        let tool = Arc::new(ReadFileTool::new(reader));
        let definition = tool.definition();
        let registry = ToolRegistry::new(vec![tool]).unwrap();
        let outcome = registry
            .execute(execution(
                &definition,
                &workspace,
                json!({"path":if mode=="success" {"refused.txt"} else {"README.md"}}),
            ))
            .await
            .unwrap();
        match mode {
            "success" => {
                let result = outcome.result().unwrap();
                assert_eq!(result["error"]["code"], "mcp_read_refused");
                assert!(!result.to_string().contains("private remote"));
            }
            "oversized" => assert_eq!(outcome.result().unwrap()["error"]["code"], "file_limit"),
            _ => {
                let Outcome::Failure(failure) = outcome else {
                    panic!()
                };
                assert_eq!(failure.retryable, mode == "disconnect_once");
            }
        }
        if mode == "incompatible" {
            assert!(tool_calls(dir.path()).is_empty());
        }
    }
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_tools -- --ignored"]
async fn maximum_escaped_batch_fits_the_transport_and_keeps_existing_output_limits() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, local) = fixture(dir.path()).await;
    fs::write(
        Path::new(&workspace).join("escaped.txt"),
        vec![0; 1_048_576],
    )
    .unwrap();
    let mut cfg = config(dir.path(), "success");
    cfg.timeout_ms = 30_000;
    let reader = McpWorkspaceReader::new(local.clone(), cfg).unwrap();
    let batch = WorkspaceReadBatch {
        paths: vec!["escaped.txt".into(); 8],
    };
    let result = reader
        .read_files(workspace.clone(), batch.clone(), ActivityId::new())
        .await
        .unwrap();
    assert_eq!(
        result,
        local
            .read_files(workspace, batch, ActivityId::new())
            .await
            .unwrap()
    );
    assert_eq!(result["text_bytes"], 65_536);
    assert!(result.to_string().len() <= 524_288);
    assert_eq!(tool_calls(dir.path()).len(), 8);
}

async fn settle(worker: &mut Worker) {
    for _ in 0..100 {
        if !worker.step().await.unwrap() {
            if !worker.has_active_runs().await.unwrap() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    panic!("worker did not settle");
}
async fn start(
    database: &Path,
    cfg: McpFilesystemConfig,
    source: PathBuf,
) -> (act0r::Engine, act0r::application::AgentService, MessageId) {
    let (engine, agents) = local_app_with_mcp(database, cfg).await.unwrap();
    let actor = agents.create_registered_actor(DefinitionId::new("tool",1),Some(source),json!({
        "goal":"Inspect files", "target":{"provider":"scripted","model":"mcp-test","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
        "tools":[{"name":"read_files","version":1}],
        "script":[{"kind":"call_tool","tool":"read_files","arguments":{"paths":["README.md","missing","src/lib.rs"]}},
            {"kind":"finish","summary":"Used durable MCP evidence"}]
    }),10,Default::default()).await.unwrap();
    let task = agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            id: MessageId::new(),
            sender: None,
            recipient: actor,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Inspect files"}),
        })
        .await
        .unwrap();
    (engine, agents, task)
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_tools -- --ignored"]
async fn saved_mcp_result_is_reused_after_restart_and_uncommitted_reads_repeat_safely() {
    for committed in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path()).await;
        let database = dir.path().join("durable.db");
        let cfg = config(dir.path(), "success");
        let (engine, agents, task) = start(&database, cfg.clone(), dir.path().join("source")).await;
        let mut worker = engine.worker().await.unwrap();
        let call = loop {
            worker.advance_work().await.unwrap();
            if let Some(call) = worker.admit().await.unwrap() {
                if call.handler.name == "native.tool" {
                    break call;
                }
                worker
                    .complete(call.clone(), worker.execute(call).await.unwrap())
                    .await
                    .unwrap();
            }
        };
        let result = worker.execute(call.clone()).await.unwrap();
        assert_eq!(tool_calls(dir.path()).len(), 2);
        if committed {
            worker.complete(call.clone(), result.clone()).await.unwrap();
        }
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app_with_mcp(&database, cfg).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        let saved = engine.status(call.run_id).await.unwrap();
        assert_eq!(saved.record().status, RunStatus::Succeeded);
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        let activity = saved
            .activities()
            .iter()
            .find(|a| a.id == call.activity_id)
            .unwrap();
        assert_eq!(activity.result.as_ref(), result.result());
        assert_eq!(tool_calls(dir.path()).len(), if committed { 2 } else { 4 });
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == call.activity_id)
                .count(),
            if committed { 1 } else { 2 }
        );
    }
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_tools -- --ignored"]
async fn disconnected_server_retries_the_saved_activity_without_repeating_the_model_decision() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path()).await;
    let (engine, agents, task) = start(
        &dir.path().join("retry.db"),
        config(dir.path(), "disconnect_once"),
        dir.path().join("source"),
    )
    .await;
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    let run = agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .run_id
        .unwrap();
    let saved = engine.status(run).await.unwrap();
    assert_eq!(saved.record().status, RunStatus::Succeeded);
    assert_eq!(saved.record().usage.admitted_model_calls, 2);
    let activity = saved
        .activities()
        .iter()
        .find(|a| a.spec.handler.name == "native.tool")
        .unwrap();
    assert_eq!(
        activity.result.as_ref().unwrap()["files"][0]["text"],
        "Original evidence.\r\n😀\n"
    );
    assert_eq!(
        saved
            .attempts()
            .iter()
            .filter(|a| a.activity_id == activity.id)
            .count(),
        2
    );
    assert_eq!(tool_calls(dir.path()).len(), 3);
}

#[test]
#[ignore = "requires Node.js; cargo test --test mcp_tools -- --ignored"]
fn shipped_cli_uses_the_environment_selected_reader_and_keeps_credentials_out_of_mcp() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("cli.db");
    let source = dir.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "CLI MCP evidence").unwrap();
    let server = dir.path().join("server.json");
    fs::write(
        &server,
        serde_json::to_vec(&config(dir.path(), "success")).unwrap(),
    )
    .unwrap();
    let actor_config = dir.path().join("actor.json");
    fs::write(
        &actor_config,
        serde_json::to_vec(&json!({
            "goal":"Inspect project", "target":{"provider":"scripted","model":"cli","endpoint":""},
            "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
            "tools":[{"name":"read_file","version":2}],
            "script":[{"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
                {"kind":"finish","summary":"Used saved evidence"}]
        }))
        .unwrap(),
    )
    .unwrap();
    let input = dir.path().join("input.json");
    fs::write(&input, br#"{"goal":"Inspect project"}"#).unwrap();
    let cli = |arguments: &[&str]| {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"));
        command
            .arg("--db")
            .arg(&database)
            .arg("--json")
            .args(arguments)
            .env("ACT0R_MCP_FILESYSTEM_CONFIG", &server)
            .env("OPENAI_API_KEY", "dummy-test-credential")
            .env("NODE_OPTIONS", "--invalid-option-must-not-reach-the-server");
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let actor = cli(&[
        "actor",
        "agent",
        "create",
        "tool",
        "--source",
        source.to_str().unwrap(),
        "--config-file",
        actor_config.to_str().unwrap(),
    ]);
    let task = cli(&[
        "actor",
        "send",
        actor["agent_id"].as_str().unwrap(),
        "--input-file",
        input.to_str().unwrap(),
    ]);
    cli(&["worker", "run", "--until-idle"]);
    let saved = cli(&[
        "actor",
        "task",
        "status",
        task["message_id"].as_str().unwrap(),
    ]);
    assert_eq!(saved["status"], "succeeded");
    assert_eq!(tool_calls(dir.path()).len(), 1);
    assert!(
        events(dir.path())
            .iter()
            .filter(|e| e["kind"] == "started")
            .all(|e| e["credentials"] == false)
    );
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_tools -- --ignored"]
async fn deadline_and_cancellation_release_workspace_and_kill_server_descendants() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (workspace, local) = fixture(dir.path()).await;
        let mut cfg = config(dir.path(), "hang");
        cfg.timeout_ms = if cancel { 10_000 } else { 2_000 };
        let reader = Arc::new(McpWorkspaceReader::new(local.clone(), cfg).unwrap());
        let scope = workspace.clone();
        let handle = tokio::spawn(async move {
            reader
                .inspect(
                    scope,
                    WorkspaceReadAction::ReadFile {
                        path: "README.md".into(),
                    },
                    ActivityId::new(),
                )
                .await
        });
        let mut pids = Vec::new();
        for _ in 0..200 {
            let log = events(dir.path());
            if log.iter().any(|v| v["kind"] == "child") {
                pids = log
                    .into_iter()
                    .filter(|v| matches!(v["kind"].as_str(), Some("started" | "child")))
                    .map(|v| v["pid"].as_u64().unwrap() as u32)
                    .collect();
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(pids.len(), 2, "server and child were not started");
        if cancel {
            handle.abort();
            assert!(handle.await.unwrap_err().is_cancelled());
        } else {
            assert!(
                matches!(handle.await.unwrap(), Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::TimedOut)
            );
        }
        assert!(
            local
                .inspect(
                    workspace,
                    WorkspaceReadAction::ReadFile {
                        path: "README.md".into()
                    },
                    ActivityId::new()
                )
                .await
                .is_ok()
        );
        for pid in pids {
            for _ in 0..100 {
                if !alive(pid) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(!alive(pid), "MCP process {pid} survived cancellation");
        }
    }
}
fn alive(pid: u32) -> bool {
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Threading::*;
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                return false;
            }
            let mut code = 0;
            let running = GetExitCodeProcess(handle, &mut code) != 0 && code == 259;
            windows_sys::Win32::Foundation::CloseHandle(handle);
            running
        }
    }
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
}
