use act0r::{
    Worker,
    adapters::{
        mcp::{McpConfig, discover_tools},
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{ToolRegistry, ports::*},
    bootstrap::local_app_with_mcp_tools,
    domain::{actors::*, tools::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

fn config(root: &Path, mode: &str) -> McpConfig {
    let node = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|path| path.join(if cfg!(windows) { "node.exe" } else { "node" }))
        .find(|path| path.is_file())
        .expect("Node.js is required");
    serde_json::from_value(json!({"replace_inspection":true,"servers":[{"name":"files","command":{
        "program":node,"args":[Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mcp-filesystem.mjs"), mode, root.join("calls.jsonl"),"{workspace}"],"timeout_ms":10000},
        "tools":[{"name":"read_text_file","read_only":true,"path_parameters":["path"]},
            {"name":"list_directory","read_only":true,"directory_parameters":["path"]},
            {"name":"find_text","read_only":true,"directory_parameters":["path"]}]}]})).unwrap()
}
fn source(root: &Path) -> PathBuf {
    let path = root.join("source");
    fs::create_dir_all(path.join("src")).unwrap();
    fs::write(path.join("README.md"), "A durable project.\n").unwrap();
    fs::write(path.join("src/lib.rs"), "fn durable() {}\n").unwrap();
    path
}
fn calls(root: &Path) -> Vec<Value> {
    fs::read_to_string(root.join("calls.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|value| value["method"] == "tools/call")
        .collect()
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
async fn registry(root: &Path, mode: &str) -> (ToolRegistry, String) {
    let workspace = LocalWorkspaces::new(root.join("workspaces"))
        .prepare(source(root))
        .await
        .unwrap();
    let tools = discover_tools(
        config(root, mode),
        Arc::new(LocalTools::new(root.join("workspaces"))),
        &root.join("workspaces"),
    )
    .await
    .unwrap();
    (ToolRegistry::new(tools).unwrap(), workspace)
}
async fn settle(worker: &mut Worker) {
    for _ in 0..150 {
        if !worker.step().await.unwrap() && !worker.has_active_runs().await.unwrap() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("worker did not settle");
}

#[tokio::test]
#[ignore = "requires Node.js"]
async fn directory_and_content_search_are_external_tools_over_filtered_disposable_views() {
    let root = tempfile::tempdir().unwrap();
    let (registry, workspace) = registry(root.path(), "success").await;
    let stale_view = Path::new(&workspace).join(".act0r-tools/mcp-inspection-interrupted");
    fs::create_dir(&stale_view).unwrap();
    fs::write(stale_view.join("abandoned.txt"), "left by a stopped host").unwrap();
    for name in [".env", ".act0r-tools/journal.json", "target/secret.txt"] {
        let file = Path::new(&workspace).join(name);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, "private durable content").unwrap();
    }
    let list = registry
        .definition(&DefinitionId::new("mcp_files_list_directory", 1))
        .unwrap();
    for path in [
        "..",
        ".env",
        ".act0r-tools",
        "target",
        "missing",
        "README.md",
    ] {
        let outcome = registry
            .execute(execution(list, &workspace, json!({"path":path})))
            .await
            .unwrap();
        assert_eq!(outcome.result().unwrap()["ok"], false);
    }
    assert!(calls(root.path()).is_empty());
    let outcome = registry
        .execute(execution(list, &workspace, json!({"path":"."})))
        .await
        .unwrap();
    assert_eq!(
        outcome.result().unwrap()["structuredContent"]["names"],
        json!(["README.md", "src"])
    );
    let remote = calls(root.path());
    assert!(
        !stale_view.exists(),
        "a later inspection must reclaim an interrupted snapshot"
    );
    let view = Path::new(remote[0]["params"]["arguments"]["path"].as_str().unwrap());
    assert!(view.starts_with(Path::new(&workspace).join(".act0r-tools")));
    assert!(
        !view.exists(),
        "completed directory snapshot must be removed"
    );
    let find = registry
        .definition(&DefinitionId::new("mcp_files_find_text", 1))
        .unwrap();
    let outcome = registry
        .execute(execution(
            find,
            &workspace,
            json!({"path":".","pattern":"durable"}),
        ))
        .await
        .unwrap();
    assert_eq!(
        outcome.result().unwrap()["structuredContent"]["matches"],
        json!(["README.md", "src/lib.rs"])
    );
    assert!(
        fs::read_to_string(Path::new(&workspace).join(".env"))
            .unwrap()
            .contains("private")
    );
    assert_eq!(
        fs::read_dir(Path::new(&workspace).join(".act0r-tools"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with("mcp-inspection-"))
            .count(),
        0
    );
}

#[tokio::test]
#[ignore = "requires Node.js"]
async fn cancelling_a_directory_call_removes_its_snapshot_and_server_tree() {
    let root = tempfile::tempdir().unwrap();
    let (registry, workspace) = registry(root.path(), "hang").await;
    let definition = registry
        .definition(&DefinitionId::new("mcp_files_list_directory", 1))
        .unwrap()
        .clone();
    let call = execution(&definition, &workspace, json!({"path":"."}));
    let task = tokio::spawn(async move { registry.execute(call).await });
    for _ in 0..200 {
        if !calls(root.path()).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let records = calls(root.path());
    assert_eq!(records.len(), 1);
    let snapshot = PathBuf::from(records[0]["params"]["arguments"]["path"].as_str().unwrap());
    assert!(snapshot.is_dir());
    task.abort();
    let _ = task.await;
    for _ in 0..100 {
        if !snapshot.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !snapshot.exists(),
        "cancelled MCP directory view must be removed"
    );
}

#[tokio::test]
#[ignore = "requires Node.js"]
async fn every_fresh_harness_role_uses_mcp_inspection_and_saved_assemblies_keep_their_catalog() {
    use act0r::domain::harness::{HarnessCoding, HarnessRequest};
    for mode in ["inspection", "discovery", "coding"] {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("demo.db");
        let cfg = config(root.path(), "success");
        let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
            .await
            .unwrap();
        let request = HarnessRequest { auto_execute:false,discover_project:mode == "discovery", message_id:MessageId::new(),prompt:"Inspect project".into(), workspace:Some(source(root.path()).to_string_lossy().into()),budget:None,
            target:serde_json::from_value(json!({"provider":"openai","model":"unused","endpoint":"http://127.0.0.1:1234/v1/chat/completions"})).unwrap(),
            coding:(mode == "coding").then(|| HarnessCoding { writable_paths:vec!["src/lib.rs".into()],test_command:WorkspaceTestCommand {argv:vec!["node".into(),"--version".into()],timeout_ms:1000}, coder_count:2,project_commands:Default::default() }) };
        let receipt = agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .start(request.clone())
            .await
            .unwrap();
        let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
        for member in &team.members {
            let tools = member.pinned_config["input"]["tools"].as_array().unwrap();
            assert!(!tools.iter().any(|tool| matches!(
                tool["name"].as_str(),
                Some("read_file" | "read_files" | "list_files" | "search_text")
            )));
            assert!(
                tools
                    .iter()
                    .any(|tool| tool["name"] == "mcp_files_read_text_file")
            );
            assert!(
                tools
                    .iter()
                    .any(|tool| tool["name"] == "mcp_files_find_text")
            );
        }
        drop(agents);
        drop(engine);
        let mut changed_mode = cfg;
        changed_mode.replace_inspection = false;
        let (_, agents) = local_app_with_mcp_tools(&database, changed_mode)
            .await
            .unwrap();
        let retry = agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .start(request)
            .await
            .unwrap();
        assert_eq!(retry.team_id, receipt.team_id);
        assert_eq!(
            json!(
                agents
                    .teams()
                    .unwrap()
                    .get(receipt.team_id)
                    .await
                    .unwrap()
                    .members
            ),
            json!(team.members)
        );
        assert!(calls(root.path()).is_empty());
    }
}

#[tokio::test]
#[ignore = "requires Node.js"]
async fn mcp_first_catalog_preserves_saved_native_actors_and_replays_committed_external_results() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("demo.db");
    let mut cfg = config(root.path(), "success");
    cfg.replace_inspection = false;
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
        .await
        .unwrap();
    let old_config = json!({"goal":"Inspect project", "target":{"provider":"scripted","model":"inspection","endpoint":""},"max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
        "tools":[{"name":"read_file","version":2}], "script":[{"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},{"kind":"finish","summary":"Read project"}]});
    let old = agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            Some(source(root.path())),
            old_config.clone(),
            10,
            Default::default(),
        )
        .await
        .unwrap();
    let old_pins = agents
        .actors()
        .unwrap()
        .status(old)
        .await
        .unwrap()
        .record()
        .config
        .clone();
    drop(agents);
    drop(engine);
    cfg.replace_inspection = true;
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
        .await
        .unwrap();
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .status(old)
            .await
            .unwrap()
            .record()
            .config,
        old_pins
    );
    let mut fresh_config = old_config;
    fresh_config["script"][0]["tool"] = json!("mcp_files_read_text_file");
    let fresh = agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            Some(root.path().join("source")),
            fresh_config,
            10,
            Default::default(),
        )
        .await
        .unwrap();
    let fresh_pins = agents
        .actors()
        .unwrap()
        .status(fresh)
        .await
        .unwrap()
        .record()
        .config
        .clone();
    assert!(
        !fresh_pins["input"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id["name"] == "read_file")
    );
    for actor in [old, fresh] {
        agents
            .actors()
            .unwrap()
            .send(AgentMessage {
                id: MessageId::new(),
                recipient: actor,
                sender: None,
                correlation_id: None,
                kind: "task".into(),
                payload: json!({"goal":"Inspect project"}),
            })
            .await
            .unwrap();
    }
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        calls(root.path()).len(),
        1,
        "saved native actor must still use its native reader"
    );
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .status(fresh)
            .await
            .unwrap()
            .record()
            .config,
        fresh_pins
    );
    settle(&mut engine.worker().await.unwrap()).await;
    assert_eq!(
        calls(root.path()).len(),
        1,
        "terminal replay must not repeat MCP"
    );
}

#[tokio::test]
#[ignore = "requires Node.js"]
async fn mcp_only_discovery_records_inspection_before_accepting_the_setup_proposal() {
    use act0r::domain::{agent::ModelTarget, harness::HarnessRequest};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let model = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().route("/v1/chat/completions", axum::routing::post(|axum::Json(body):axum::Json<Value>| async move {
            assert!(!body["tools"].as_array().unwrap().iter().any(|tool| tool["function"]["name"] == "read_file"));
            let prompt: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            let mut action = if prompt["turn"] == 0 {
                json!({"kind":"mcp_files_read_text_file","path":"README.md"})
            } else {
                assert_eq!(prompt["project_discovery"]["inspection_recorded"], true);
                json!({"kind":"propose_project_setup","proposal":{"summary":"Use existing project setup","writable_paths":["README.md"],"test_command":{"argv":["node","--version"],"timeout_ms":1000},"commands":[]}})
            };
            let name = action.as_object_mut().unwrap().remove("kind").unwrap();
            axum::Json(json!({"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"id":"inspection","type":"function","function":{"name":name,"arguments":action.to_string()}}]}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))
        }))).await.unwrap();
    });
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) =
        local_app_with_mcp_tools(root.path().join("demo.db"), config(root.path(), "success"))
            .await
            .unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    let receipt = harness.start(HarnessRequest { auto_execute:false, discover_project:true,message_id:MessageId::new(),prompt:"Inspect project and propose setup".into(), workspace:Some(source(root.path()).to_string_lossy().into()),budget:None,
        target:serde_json::from_value::<ModelTarget>(json!({"provider":"openai","model":"fixture","endpoint":endpoint,"protocol":"chat_completions","action_format":"function_calling"})).unwrap(), coding:None }).await.unwrap();
    settle(&mut engine.worker().await.unwrap()).await;
    let proposal = harness
        .project_setup(receipt.session_id, receipt.task_id)
        .await
        .unwrap();
    assert!(json!(proposal)["proposal"].is_object());
    assert_eq!(calls(root.path()).len(), 1);
    model.abort();
}

#[test]
#[ignore = "requires Node.js"]
fn cli_loads_one_host_config_automatically_and_explicit_environment_takes_precedence() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join(".act0r")).unwrap();
    fs::write(
        root.path().join(".act0r/mcp.json"),
        serde_json::to_vec(&config(root.path(), "success")).unwrap(),
    )
    .unwrap();
    let empty = root.path().join("empty.json");
    fs::write(&empty, r#"{"servers":[]}"#).unwrap();
    let catalog = |explicit: bool| {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"));
        command
            .current_dir(root.path())
            .arg("--db")
            .arg(root.path().join(".act0r/demo.db"))
            .args(["--json", "actor", "tool", "list"])
            .env_remove("ACT0R_MCP_CONFIG")
            .env_remove("ACT0R_MCP_FILESYSTEM_CONFIG");
        if explicit {
            command.env("ACT0R_MCP_CONFIG", &empty);
        }
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
    assert!(
        catalog(false)
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["id"]["name"] == "mcp_files_read_text_file")
    );
    assert!(
        !catalog(true)
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["id"]["name"] == "mcp_files_read_text_file")
    );
    assert!(calls(root.path()).is_empty());
}

#[tokio::test]
#[ignore = "requires Node.js"]
async fn explicit_server_environment_is_applied_and_part_of_the_pinned_binding() {
    let root = tempfile::tempdir().unwrap();
    let workspace = LocalWorkspaces::new(root.path().join("workspaces"))
        .prepare(source(root.path()))
        .await
        .unwrap();
    let local = Arc::new(LocalTools::new(root.path().join("workspaces")));
    let mut cfg = config(root.path(), "success");
    cfg.servers[0]
        .env
        .insert("ACT0R_MCP_TEST".into(), "configured".into());
    let first = discover_tools(cfg.clone(), local.clone(), &root.path().join("workspaces"))
        .await
        .unwrap();
    let registry = ToolRegistry::new(first).unwrap();
    let id = DefinitionId::new("mcp_files_list_directory", 1);
    let original = registry.definition(&id).unwrap();
    assert_eq!(
        registry
            .execute(execution(original, &workspace, json!({"path":"."})))
            .await
            .unwrap()
            .result()
            .unwrap()["ok"],
        true
    );
    let starts: Vec<Value> = fs::read_to_string(root.path().join("calls.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|value: &Value| value["kind"] == "started")
        .collect();
    assert!(!starts.is_empty());
    assert!(starts.iter().all(
        |value| value["configuredEnvironment"] == "configured" && value["credentials"] == false
    ));
    cfg.servers[0]
        .env
        .insert("ACT0R_MCP_TEST".into(), "changed".into());
    let changed = ToolRegistry::new(
        discover_tools(cfg.clone(), local, &root.path().join("workspaces"))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_ne!(
        original.external.as_ref().unwrap().binding,
        changed
            .definition(&id)
            .unwrap()
            .external
            .as_ref()
            .unwrap()
            .binding
    );
    assert!(changed.check_pins(std::slice::from_ref(original)).is_err());
    cfg.servers[0]
        .env
        .insert("act0r_mcp_test".into(), "conflicting casing".into());
    assert!(cfg.validate().is_err());
}
