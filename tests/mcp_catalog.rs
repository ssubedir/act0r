use act0r::{
    Worker,
    adapters::{
        mcp::{McpConfig, discover_tools},
        native_tools::EchoTool,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{AgentService, ToolRegistry, ports::*},
    bootstrap::local_app_with_mcp_tools,
    domain::{actors::*, agent::*, tools::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const INFO: &str = "mcp_files_get_file_info";
fn node() -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|path| path.join(if cfg!(windows) { "node.exe" } else { "node" }))
        .find(|path| path.is_file())
        .expect("Node.js is required for these optional MCP tests")
}
fn config(root: &Path, mode: &str, names: &[&str]) -> McpConfig {
    serde_json::from_value(json!({"servers":[{"name":"files","command":{
        "program":node(),"args":[Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mcp-filesystem.mjs"),
            mode,root.join("calls.jsonl"),"{workspace}"],"timeout_ms":10000},
        "tools":names.iter().map(|name| json!({"name":name,"read_only":true,"path_parameters":["path"]})).collect::<Vec<_>>()
    }]})).unwrap()
}
fn calls(root: &Path) -> Vec<Value> {
    fs::read_to_string(root.join("calls.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|v: &Value| v["method"] == "tools/call")
        .collect()
}
fn source(root: &Path) -> PathBuf {
    let path = root.join("source");
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("README.md"), "Durable MCP evidence.\n").unwrap();
    path
}
fn actor_config() -> Value {
    // The actor does not list the MCP tool: the trusted host config selects it.
    json!({"goal":"Inspect metadata", "target":{"provider":"scripted","model":"mcp-catalog","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
        "tools":[],"script":[{"kind":"call_tool","tool":INFO,"arguments":{"path":"README.md","label":null}},
            {"kind":"finish","summary":"Used the saved MCP metadata"}]})
}
async fn create(agents: &AgentService, source: PathBuf) -> AgentId {
    agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            Some(source),
            actor_config(),
            10,
            Default::default(),
        )
        .await
        .unwrap()
}
async fn send(agents: &AgentService, actor: AgentId) -> MessageId {
    agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Inspect metadata"}),
        })
        .await
        .unwrap()
}
async fn admit_tool(worker: &mut Worker) -> ActivityExecution {
    for _ in 0..100 {
        worker.advance_work().await.unwrap();
        if let Some(call) = worker.admit().await.unwrap() {
            if call.handler.name == "native.tool" {
                return call;
            }
            let request: ModelRequest = serde_json::from_value(call.input.clone()).unwrap();
            let functions = request.function_actions().unwrap();
            let info = functions.iter().find(|f| f.name == INFO).unwrap();
            assert_eq!(info.parameters["properties"]["path"]["minLength"], 1);
            worker
                .complete(call.clone(), worker.execute(call).await.unwrap())
                .await
                .unwrap();
        }
    }
    panic!("no MCP tool intent");
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
async fn registry(root: &Path, mode: &str) -> (ToolRegistry, ToolDefinition, String) {
    let src = source(root);
    let managed = root.join("workspaces");
    let workspace = LocalWorkspaces::new(managed.clone())
        .prepare(src)
        .await
        .unwrap();
    let local = Arc::new(LocalTools::new(managed.clone()));
    let tools = discover_tools(config(root, mode, &["get_file_info"]), local, &managed)
        .await
        .unwrap();
    let definition = tools[0].definition();
    assert!(
        calls(root).is_empty(),
        "startup discovery must not invoke a tool"
    );
    (ToolRegistry::new(tools).unwrap(), definition, workspace)
}

#[test]
fn external_schema_contracts_preserve_constraints_nullable_projection_and_old_pins() {
    let input = json!({"type":"object","properties":{
        "path":{"type":"string","minLength":1},"mode":{"type":"string","enum":["brief","full"]},
        "filter":{"type":"object","properties":{"limit":{"type":"integer","minimum":1}},"required":[]},
        "nullable":{"type":["string","null"]}},"required":["path"],"additionalProperties":false});
    let contract = ExternalToolContract {
        binding: "host-binding-1".into(),
        read_only: true,
        input_schema: input.clone(),
        output_schema: Some(
            json!({"type":"object","properties":{"size":{"type":"integer"}},"required":["size"]}),
        ),
    };
    let definition = ToolDefinition::builder("mcp_test_info", 1, RecoveryPolicy::SafeToRepeat)
        .description("Metadata")
        .external_contract(contract.clone())
        .requires_workspace(true)
        .reports_errors(true)
        .build()
        .unwrap();
    assert_eq!(definition.input_schema(), input);
    let args = json!({"path":"README.md","mode":null,"filter":{"limit":null},"nullable":null});
    definition.validate_arguments(&args).unwrap();
    assert_eq!(
        contract.arguments(&args),
        json!({"path":"README.md","filter":{},"nullable":null})
    );
    let schema = definition.action_schema();
    assert_eq!(
        schema["properties"]["arguments"]["required"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        schema["properties"]["arguments"]["properties"]["mode"]["anyOf"][0]["enum"],
        json!(["brief", "full"])
    );
    for args in [
        json!({"path":""}),
        json!({"path":"a","mode":"invalid"}),
        json!({"path":"a","filter":{"limit":0}}),
        json!({"path":"a","extra":1}),
    ] {
        assert!(definition.validate_arguments(&args).is_err());
    }
    assert!(
        contract
            .validate_output(Some(&json!({"size":"wrong"})))
            .is_err()
    );
    contract.validate_output(Some(&json!({"size":4}))).unwrap();
    let mut bad = contract.clone();
    bad.input_schema["properties"]["path"] = json!({"$ref":"file:///private/schema.json"});
    assert!(bad.validate().is_err());
    bad = contract;
    bad.input_schema["properties"]["kind"] = json!({"type":"string"});
    assert!(bad.validate().is_err());
    let echo = EchoTool.definition();
    assert!(json!(echo).get("external").is_none());
    assert_eq!(
        serde_json::from_value::<ToolDefinition>(json!(echo)).unwrap(),
        echo
    );
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_catalog -- --ignored"]
async fn configured_metadata_tool_runs_without_a_rust_wrapper_and_reuses_exact_saved_evidence() {
    let root = tempfile::tempdir().unwrap();
    let cfg = config(root.path(), "success", &["get_file_info"]);
    let database = root.path().join("demo.db");
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
        .await
        .unwrap();
    let definitions = agents.tool_definitions().unwrap();
    let definition = definitions.iter().find(|d| d.id.name == INFO).unwrap();
    assert_eq!(
        definition.external.as_ref().unwrap().input_schema["properties"]["label"]["enum"],
        json!(["metadata"])
    );
    assert!(
        !definitions
            .iter()
            .any(|d| d.id.name == "mcp_files_write_file")
    );
    let actor = create(&agents, source(root.path())).await;
    let state = agents.actors().unwrap().status(actor).await.unwrap();
    assert!(
        state.record().config["input"]["tools"]
            .as_array()
            .unwrap()
            .contains(&json!(definition.id))
    );
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let call = admit_tool(&mut worker).await;
    let outcome = worker.execute(call.clone()).await.unwrap();
    let saved = outcome.result().unwrap().clone();
    assert_eq!(
        saved["structuredContent"]["size"],
        fs::metadata(Path::new(call.input["workspace"].as_str().unwrap()).join("README.md"))
            .unwrap()
            .len()
    );
    worker.complete(call.clone(), outcome).await.unwrap();
    let before = engine.status(call.run_id).await.unwrap();
    let next = before.activities().last().unwrap().spec.input.clone();
    let usage = json!(before.record().usage);
    let workspace = Path::new(call.input["workspace"].as_str().unwrap());
    fs::write(workspace.join("README.md"), "changed after saved result").unwrap();
    let first = calls(root.path());
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["params"]["name"], "get_file_info");
    assert!(first[0]["params"]["arguments"].get("label").is_none());
    assert!(
        Path::new(first[0]["params"]["arguments"]["path"].as_str().unwrap()).starts_with(workspace)
    );
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    let restored = engine.status(call.run_id).await.unwrap();
    assert_eq!(restored.activities().last().unwrap().spec.input, next);
    assert_eq!(json!(restored.record().usage), usage);
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    let restored = engine.status(call.run_id).await.unwrap();
    assert_eq!(restored.record().status, RunStatus::Succeeded);
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(
        restored
            .activities()
            .iter()
            .find(|a| a.id == call.activity_id)
            .unwrap()
            .result,
        Some(saved)
    );
    assert_eq!(calls(root.path()).len(), 1);
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_catalog -- --ignored"]
async fn schema_drift_after_discovery_blocks_dispatch_and_restoring_it_recovers_the_same_intent() {
    let root = tempfile::tempdir().unwrap();
    let (registry, definition, workspace) = registry(root.path(), "success").await;
    fs::write(root.path().join("calls.jsonl.mode"), "changed").unwrap();
    let call = execution(&definition, &workspace, json!({"path":"README.md"}));
    let Outcome::Failure(failure) = registry.execute(call.clone()).await.unwrap() else {
        panic!()
    };
    assert!(!failure.retryable);
    assert!(calls(root.path()).is_empty());
    fs::write(root.path().join("calls.jsonl.mode"), "success").unwrap();
    assert!(registry.execute(call).await.unwrap().result().unwrap()["ok"] == true);
    assert_eq!(calls(root.path()).len(), 1);
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_catalog -- --ignored"]
async fn changed_schema_or_binding_on_restart_blocks_admission_and_restores_saved_activity() {
    for binding in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("restore.db");
        let cfg = config(root.path(), "success", &["get_file_info"]);
        let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
            .await
            .unwrap();
        let actor = create(&agents, source(root.path())).await;
        send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let call = admit_tool(&mut worker).await;
        // Leave the repeat-safe intent admitted, with no MCP execution yet.
        drop(worker);
        drop(agents);
        drop(engine);
        let mut changed = cfg.clone();
        if binding {
            changed.servers[0].command.timeout_ms += 1;
        } else {
            fs::write(root.path().join("calls.jsonl.mode"), "changed").unwrap();
        }
        let (engine, agents) = local_app_with_mcp_tools(&database, changed).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        assert_eq!(
            engine.status(call.run_id).await.unwrap().record().status,
            RunStatus::Blocked
        );
        assert!(engine.resume(call.run_id).await.is_err());
        assert!(calls(root.path()).is_empty());
        drop(worker);
        drop(agents);
        drop(engine);
        fs::write(root.path().join("calls.jsonl.mode"), "success").unwrap();
        let (engine, agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        engine.resume(call.run_id).await.unwrap();
        settle(&mut worker).await;
        let saved = engine.status(call.run_id).await.unwrap();
        assert_eq!(saved.record().status, RunStatus::Succeeded);
        assert_eq!(
            saved
                .activities()
                .iter()
                .filter(|a| a.spec.handler.name == "native.tool")
                .count(),
            1
        );
        assert_eq!(
            saved
                .activities()
                .iter()
                .find(|a| a.id == call.activity_id)
                .unwrap()
                .result
                .as_ref()
                .unwrap()["ok"],
            true
        );
        assert_eq!(calls(root.path()).len(), 1);
        drop(agents);
    }
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_catalog -- --ignored"]
async fn widening_host_allowlist_changes_only_fresh_workspace_actors() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("allowlist.db");
    let cfg = config(root.path(), "success", &["get_file_info"]);
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    let old = create(&agents, source(root.path())).await;
    let saved = agents
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
    let cfg = config(root.path(), "success", &["get_file_info", "read_text_file"]);
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    let restored = agents.actors().unwrap().status(old).await.unwrap();
    assert_eq!(restored.record().config, saved);
    let new = create(&agents, source(root.path())).await;
    let new = agents.actors().unwrap().status(new).await.unwrap();
    let ids = new.record().config["input"]["tools"].as_array().unwrap();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&json!(DefinitionId::new("mcp_files_read_text_file", 1))));
    let task = send(&agents, old).await;
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_catalog -- --ignored"]
async fn harness_pins_configured_tools_for_inspection_discovery_and_coding_without_changing_saved_assemblies()
 {
    use act0r::domain::harness::{HarnessCoding, HarnessRequest};
    for mode in ["inspection", "discovery", "coding"] {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("harness.db");
        let cfg = config(root.path(), "success", &["get_file_info"]);
        let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
            .await
            .unwrap();
        let src = source(root.path());
        let request = HarnessRequest {
            auto_execute: false,
            discover_project: mode == "discovery",
            message_id: MessageId::new(),
            prompt: "Inspect the project".into(),
            workspace: Some(src.to_string_lossy().into()),
            budget: None,
            target: ModelTarget {
                provider: "openai".into(),
                model: "unused".into(),
                endpoint: "http://127.0.0.1:1234/v1/chat/completions".into(),
                protocol: ModelProtocol::ChatCompletions,
                action_format: Default::default(),
                policy: Default::default(),
            },
            coding: if mode == "coding" {
                Some(HarnessCoding {
                    writable_paths: vec!["README.md".into()],
                    test_command: WorkspaceTestCommand {
                        argv: vec!["unused".into()],
                        timeout_ms: 1000,
                    },
                    coder_count: 1,
                    project_commands: Default::default(),
                })
            } else {
                None
            },
        };
        let receipt = agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .start(request.clone())
            .await
            .unwrap();
        let saved = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
        for member in &saved.members {
            assert!(
                member.pinned_config["input"]["tools"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(DefinitionId::new(INFO, 1)))
            );
        }
        drop(agents);
        drop(engine);
        let mut wider = cfg;
        let reader = config(root.path(), "success", &["read_text_file"])
            .servers
            .remove(0)
            .tools
            .remove(0);
        wider.servers[0].tools.push(reader);
        let (_, agents) = local_app_with_mcp_tools(&database, wider).await.unwrap();
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
                    .get(retry.team_id)
                    .await
                    .unwrap()
                    .members
            ),
            json!(saved.members)
        );
        assert!(calls(root.path()).is_empty());
    }
}

#[test]
#[ignore = "requires Node.js; cargo test --test mcp_catalog -- --ignored"]
fn shipped_cli_discovers_configured_tools_and_uses_the_default_without_an_actor_allowlist() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("cli.db");
    let server = root.path().join("server.json");
    fs::write(
        &server,
        serde_json::to_vec(&config(root.path(), "success", &["get_file_info"])).unwrap(),
    )
    .unwrap();
    let actor = root.path().join("actor.json");
    fs::write(&actor, serde_json::to_vec(&actor_config()).unwrap()).unwrap();
    let input = root.path().join("input.json");
    fs::write(&input, br#"{"goal":"Inspect metadata"}"#).unwrap();
    let cli = |args: &[&str]| {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"));
        command
            .arg("--db")
            .arg(&database)
            .arg("--json")
            .args(args)
            .env("ACT0R_MCP_CONFIG", &server);
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
    let catalog = cli(&["actor", "tool", "list"]);
    assert!(
        catalog
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["id"]["name"] == INFO)
    );
    let src = source(root.path());
    let created = cli(&[
        "actor",
        "agent",
        "create",
        "tool",
        "--source",
        src.to_str().unwrap(),
        "--config-file",
        actor.to_str().unwrap(),
    ]);
    let task = cli(&[
        "actor",
        "send",
        created["agent_id"].as_str().unwrap(),
        "--input-file",
        input.to_str().unwrap(),
    ]);
    cli(&["worker", "run", "--until-idle"]);
    let status = cli(&[
        "actor",
        "task",
        "status",
        task["message_id"].as_str().unwrap(),
    ]);
    assert_eq!(status["status"], "succeeded");
    assert_eq!(calls(root.path()).len(), 1);
}

#[tokio::test]
#[ignore = "requires Node.js; cargo test --test mcp_catalog -- --ignored"]
async fn scope_refusals_protocol_errors_tool_errors_schema_errors_and_output_bounds_are_saved_correctly()
 {
    for mode in [
        "success",
        "tool_error",
        "protocol_error",
        "bad_output",
        "oversized",
        "disconnect_once",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (registry, definition, workspace) = registry(root.path(), mode).await;
        if mode == "success" {
            for path in ["../outside", ".act0r-tools/managed", "missing", ".env"] {
                let outcome = registry
                    .execute(execution(&definition, &workspace, json!({"path":path})))
                    .await
                    .unwrap();
                assert_eq!(outcome.result().unwrap()["ok"], false);
            }
            assert!(calls(root.path()).is_empty());
        } else {
            let outcome = registry
                .execute(execution(
                    &definition,
                    &workspace,
                    json!({"path":"README.md"}),
                ))
                .await
                .unwrap();
            if mode == "tool_error" {
                assert_eq!(outcome.result().unwrap()["ok"], false);
            } else {
                let Outcome::Failure(failure) = outcome else {
                    panic!()
                };
                assert_eq!(failure.retryable, mode == "disconnect_once");
                if mode == "oversized" {
                    assert_eq!(failure.code, "tool_output_limit");
                }
            }
        }
    }
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path(), "success", &["get_file_info"]);
    cfg.servers[0].tools[0].read_only = false;
    assert!(cfg.validate().is_err());
    cfg.servers[0].tools[0].read_only = true;
    let duplicate = cfg.servers[0].tools[0].clone();
    cfg.servers[0].tools.push(duplicate);
    assert!(cfg.validate().is_err());
}
