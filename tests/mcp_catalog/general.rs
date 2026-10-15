use super::*;
const LOOKUP: &str = "mcp_service_lookup";

fn general_config(root: &Path, mode: &str) -> McpConfig {
    let mut cfg = config(root, mode, &[]);
    cfg.servers[0].name = "service".into();
    cfg.servers[0].tools = serde_json::from_value(json!([
        {"name":"lookup","read_only":true,"requires_workspace":false}
    ]))
    .unwrap();
    cfg
}
fn tool_actor() -> Value {
    json!({"goal":"Look up a service fact","target":{"provider":"scripted","model":"local","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,"tools":[],
        "script":[{"kind":"call_tool","tool":LOOKUP,"arguments":{"query":"durability"}},
            {"kind":"finish","summary":"Used the saved service answer"}]})
}
async fn admit_general(worker: &mut Worker) -> ActivityExecution {
    for _ in 0..100 {
        worker.advance_work().await.unwrap();
        if let Some(call) = worker.admit().await.unwrap() {
            if call.handler.name == "native.tool" {
                return call;
            }
            let request: ModelRequest = serde_json::from_value(call.input.clone()).unwrap();
            assert_eq!(request.native_tool_names, vec![LOOKUP]);
            assert!(!request.prompt.contains("writable_paths"));
            worker
                .complete(call.clone(), worker.execute(call).await.unwrap())
                .await
                .unwrap();
        }
    }
    panic!("no general MCP intent");
}
fn general_execution(definition: &ToolDefinition) -> ActivityExecution {
    let Command::Schedule(spec) = definition
        .schedule_call(&json!({"query":"durability"}))
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
pub(super) async fn no_scratch(root: &Path) {
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if fs::read_dir(root).unwrap().next().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("MCP scratch directories were not removed");
}

#[test]
fn workspace_policy_defaults_preserve_existing_config_and_general_tools_forbid_scoped_paths() {
    let value = json!({"servers":[{"name":"service","command":{"program":std::env::current_exe().unwrap(),"args":[]},
        "tools":[{"name":"lookup","read_only":true}]}]});
    let cfg: McpConfig = serde_json::from_value(value.clone()).unwrap();
    cfg.validate().unwrap();
    assert!(cfg.servers[0].tools[0].requires_workspace);
    let original = serde_json::to_value(&cfg.servers[0].tools[0]).unwrap();
    assert!(original.get("requires_workspace").is_none());
    let mut explicit = value;
    explicit["servers"][0]["tools"][0]["requires_workspace"] = json!(true);
    let explicit: McpConfig = serde_json::from_value(explicit).unwrap();
    assert_eq!(
        serde_json::to_value(&explicit.servers[0].tools[0]).unwrap(),
        original
    );
    let mut cfg = cfg;
    cfg.servers[0].tools[0].requires_workspace = false;
    cfg.validate().unwrap();
    assert_eq!(
        serde_json::to_value(&cfg.servers[0].tools[0]).unwrap()["requires_workspace"],
        false
    );
    for directory in [false, true] {
        let mut bad = cfg.clone();
        let policy = &mut bad.servers[0].tools[0];
        if directory {
            policy.directory_parameters.push("path".into());
        } else {
            policy.path_parameters.push("path".into());
        }
        assert!(bad.validate().is_err());
    }
    let mut bad = cfg.clone();
    bad.servers[0].tools[0].read_only = false;
    bad.validate().unwrap();
    bad.servers[0].tools[0].requires_workspace = true;
    assert!(bad.validate().is_err());
    cfg.replace_inspection = true;
    assert!(
        cfg.validate().is_err(),
        "general tools cannot replace project inspection"
    );

    let contract = ExternalToolContract {
        binding: "host-selected".into(),
        read_only: true,
        input_schema: json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),
        output_schema: None,
    };
    let definition = ToolDefinition::builder("mcp_lookup", 1, RecoveryPolicy::SafeToRepeat)
        .description("Read a fact")
        .external_contract(contract)
        .reports_errors(true)
        .build()
        .unwrap();
    let Command::Schedule(spec) = definition
        .schedule_call_in(&json!({}), Some("/a/project"))
        .unwrap()
    else {
        panic!()
    };
    let request: NativeToolRequest = serde_json::from_value(spec.input).unwrap();
    assert!(request.workspace.is_none());
    let mut invalid = definition;
    invalid.requires_approval = true;
    assert!(invalid.validate().is_err());
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn general_defaults_work_in_plain_chat_and_project_actors_without_granting_file_tools() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = general_config(root.path(), "success");
    cfg.servers[0].tools.push(
        serde_json::from_value(
            json!({"name":"get_file_info","read_only":true,"path_parameters":["path"]}),
        )
        .unwrap(),
    );
    cfg.replace_inspection = true;
    let database = root.path().join("mixed.db");
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
        .await
        .unwrap();
    let input = harness::HarnessRequest {
        auto_execute: false,
        discover_project: false,
        budget: None,
        message_id: MessageId::new(),
        prompt: "Look up durability".into(),
        workspace: None,
        coding: None,
        target: ModelTarget {
            provider: "openai".into(),
            model: "unused".into(),
            endpoint: "http://127.0.0.1:1/v1/chat/completions".into(),
            protocol: ModelProtocol::ChatCompletions,
            action_format: ModelActionFormat::FunctionCalling,
            policy: Default::default(),
        },
    };
    let receipt = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .start(input)
        .await
        .unwrap();
    let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
    let planner = &team.members[0].pinned_config["input"];
    assert_eq!(planner["tools"], json!([{"name":LOOKUP,"version":1}]));
    assert!(planner.get("workspace").is_none());
    assert!(team.members.iter().all(|member| member.workspace.is_none()));
    let definition: ToolDefinition =
        serde_json::from_value(planner["tool_definitions"][0].clone()).unwrap();
    assert!(!definition.requires_workspace);
    assert!(definition.external.unwrap().read_only);
    let helper: act0r::domain::answer::AnswerConfig =
        serde_json::from_value(team.members[1].pinned_config["input"].clone()).unwrap();
    assert!(helper.tools.is_empty());
    let saved = team.members[0].pinned_config.clone();
    for project in [None, Some(source(root.path()))] {
        let actor = agents
            .create_registered_actor(
                DefinitionId::new("tool", 1),
                project.clone(),
                tool_actor(),
                10,
                Default::default(),
            )
            .await
            .unwrap();
        let state = agents.actors().unwrap().status(actor).await.unwrap();
        let tools = state.record().config["input"]["tools"].as_array().unwrap();
        assert!(tools.contains(&json!({"name":LOOKUP,"version":1})));
        assert_eq!(tools.len(), if project.is_some() { 2 } else { 1 });
        assert!(!tools.iter().any(|tool| tool["name"] == "read_file"));
    }
    drop(agents);
    drop(engine);
    // Changed host defaults affect fresh actors, not an existing general conversation.
    cfg.servers[0].tools.remove(0);
    let (_engine, agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
    assert_eq!(team.members[0].pinned_config, saved);
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn general_result_and_exact_continuation_survive_restart_without_requerying_the_service() {
    let root = tempfile::tempdir().unwrap();
    let cfg = general_config(root.path(), "success");
    let database = root.path().join("saved.db");
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
        .await
        .unwrap();
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            None,
            tool_actor(),
            10,
            Default::default(),
        )
        .await
        .unwrap();
    send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let call = admit_general(&mut worker).await;
    assert!(call.input.get("workspace").is_none());
    let outcome = worker.execute(call.clone()).await.unwrap();
    assert_eq!(
        outcome.result().unwrap()["structuredContent"]["answer"],
        "saved service answer"
    );
    worker.complete(call.clone(), outcome).await.unwrap();
    let before = engine.status(call.run_id).await.unwrap();
    let saved = before
        .activities()
        .iter()
        .find(|a| a.id == call.activity_id)
        .unwrap()
        .clone();
    let next = before.activities().last().unwrap().spec.input.clone();
    let request: ModelRequest = serde_json::from_value(next.clone()).unwrap();
    assert!(request.prompt.contains("saved service answer"));
    let records: Vec<Value> = fs::read_to_string(root.path().join("calls.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let scope = records
        .iter()
        .find(|v| v["kind"] == "general_scope")
        .unwrap();
    assert_eq!(scope["entries"], json!([]));
    let cwd = Path::new(scope["cwd"].as_str().unwrap());
    assert_eq!(
        fs::canonicalize(cwd.parent().unwrap()).unwrap(),
        fs::canonicalize(root.path().join(".act0r/workspaces")).unwrap()
    );
    assert!(
        cwd.file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("mcp-general-")
    );
    no_scratch(&root.path().join(".act0r/workspaces")).await;
    fs::write(
        root.path().join("calls.jsonl.answer"),
        "changed remote answer",
    )
    .unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, _agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    assert_eq!(
        engine
            .status(call.run_id)
            .await
            .unwrap()
            .activities()
            .last()
            .unwrap()
            .spec
            .input,
        next
    );
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    let after = engine.status(call.run_id).await.unwrap();
    assert_eq!(after.record().status, RunStatus::Succeeded);
    assert_eq!(
        json!(
            after
                .activities()
                .iter()
                .find(|a| a.id == call.activity_id)
                .unwrap()
        ),
        json!(saved)
    );
    assert_eq!(calls(root.path()).len(), 1);
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn changed_general_contract_or_workspace_policy_blocks_saved_work_until_restored() {
    for policy_change in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let cfg = general_config(root.path(), "success");
        let database = root.path().join("blocked.db");
        let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
            .await
            .unwrap();
        let actor = agents
            .create_registered_actor(
                DefinitionId::new("tool", 1),
                None,
                tool_actor(),
                10,
                Default::default(),
            )
            .await
            .unwrap();
        send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let call = admit_general(&mut worker).await;
        drop(worker);
        drop(agents);
        drop(engine);
        let mut changed = cfg.clone();
        if policy_change {
            changed.servers[0].tools[0].requires_workspace = true;
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
        let (engine, _agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        engine.resume(call.run_id).await.unwrap();
        settle(&mut worker).await;
        assert_eq!(
            engine.status(call.run_id).await.unwrap().record().status,
            RunStatus::Succeeded
        );
        assert_eq!(calls(root.path()).len(), 1);
    }
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn general_calls_keep_scope_schema_errors_output_limits_and_timeout_cleanup() {
    for mode in ["success", "tool_error", "bad_output", "oversized", "hang"] {
        let root = tempfile::tempdir().unwrap();
        let mut cfg = general_config(root.path(), mode);
        if mode == "hang" {
            cfg.servers[0].command.timeout_ms = 1500;
        }
        let managed = root.path().join("scratch");
        let tools = discover_tools(cfg, Arc::new(LocalTools::new(managed.clone())), &managed)
            .await
            .unwrap();
        let definition = tools[0].definition();
        let registry = ToolRegistry::new(tools).unwrap();
        let mut forged = general_execution(&definition);
        forged.input["workspace"] = json!(root.path());
        assert!(registry.execute(forged).await.is_err());
        assert!(definition.schedule_call(&json!({"query":""})).is_err());
        assert!(calls(root.path()).is_empty());
        let outcome = registry
            .execute(general_execution(&definition))
            .await
            .unwrap();
        if mode == "success" || mode == "tool_error" {
            assert_eq!(outcome.result().unwrap()["ok"], mode == "success");
        } else {
            assert!(matches!(outcome, Outcome::Failure(_)));
        }
        no_scratch(&managed).await;
    }
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn cancelled_general_call_removes_its_empty_working_directory() {
    let root = tempfile::tempdir().unwrap();
    let managed = root.path().join("scratch");
    let tools = discover_tools(
        general_config(root.path(), "hang"),
        Arc::new(LocalTools::new(managed.clone())),
        &managed,
    )
    .await
    .unwrap();
    let call = general_execution(&tools[0].definition());
    let registry = ToolRegistry::new(tools).unwrap();
    let task = tokio::spawn(async move { registry.execute(call).await });
    tokio::time::timeout(Duration::from_secs(6), async {
        while calls(root.path()).is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("general tool was not dispatched");
    assert!(fs::read_dir(&managed).unwrap().next().is_some());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    no_scratch(&managed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn plain_cli_chat_calls_mcp_and_restores_the_saved_question_and_result() {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("chat.db");
    let cfg = general_config(root.path(), "success");
    fs::write(
        root.path().join("mcp.json"),
        serde_json::to_vec(&cfg).unwrap(),
    )
    .unwrap();
    let received = Arc::new(Mutex::new(Vec::<Value>::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let requests = received.clone();
    let calls_count = count.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let app=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move|axum::Json(body):axum::Json<Value>|{
        let index=calls_count.fetch_add(1,Ordering::SeqCst);
        let tools=body["tools"].as_array().unwrap();
        assert!(tools.iter().any(|t|t["function"]["name"]==LOOKUP));
        assert!(!tools.iter().any(|t|t["function"]["name"]=="read_file"));
        requests.lock().unwrap().push(body);
        let (name,args)=match index {
            0=>(LOOKUP,json!({"query":"durability"})),
            1=>("ask_human",json!({"question":"Continue with the saved service answer?"})),
            2=>("finish",json!({"summary":"Used the saved service answer after restart."})),
            _=>panic!("unexpected model call {index}"),
        };
        async move {axum::Json(json!({"id":format!("mcp-general-{index}"),"choices":[{"message":{"tool_calls":[{"id":format!("tool-{index}"),"type":"function","function":{"name":name,"arguments":args.to_string()}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
    }));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let invoke = |args: Vec<String>| {
        let database = database.clone();
        tokio::task::spawn_blocking(move || {
            let output = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"))
                .arg("--db")
                .arg(database)
                .args(["--json", "chat"])
                .args(args)
                .env_remove("OPENAI_API_KEY")
                .env_remove("ACT0R_MCP_CONFIG")
                .env_remove("ACT0R_MCP_FILESYSTEM_CONFIG")
                .stdin(std::process::Stdio::null())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>()
        })
    };
    let first = invoke(vec![
        "--model".into(),
        "local".into(),
        "--endpoint".into(),
        endpoint,
        "--action-format".into(),
        "function-calling".into(),
        "Look up durability".into(),
    ])
    .await
    .unwrap();
    let session = first.iter().find(|e| e["event"] == "session").unwrap()["data"]["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let question = &first.iter().find(|e| e["event"] == "question").unwrap()["data"];
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(calls(root.path()).len(), 1);
    assert!(!first.iter().any(|e| e["event"] == "patch"));
    let run: RunId = question["run_id"].as_str().unwrap().parse().unwrap();
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
        .await
        .unwrap();
    let before = engine.status(run).await.unwrap();
    let saved = before
        .activities()
        .iter()
        .find(|a| a.spec.handler.name == "native.tool")
        .unwrap()
        .clone();
    assert!(saved.spec.input.get("workspace").is_none());
    drop(agents);
    drop(engine);
    fs::write(root.path().join("calls.jsonl.answer"), "new service data").unwrap();
    let second = invoke(vec![
        "--resume".into(),
        session.clone(),
        "Yes, use the saved answer.".into(),
    ])
    .await
    .unwrap();
    assert_eq!(
        second.iter().find(|e| e["event"] == "question").unwrap()["data"]["wait"]["id"],
        question["wait"]["id"]
    );
    assert!(
        second
            .iter()
            .any(|e| e["event"] == "response" && e["data"]["status"] == "succeeded")
    );
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert_eq!(calls(root.path()).len(), 1);
    let prompt = received.lock().unwrap()[2]["messages"][1]["content"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(prompt.contains("saved service answer"));
    assert!(!prompt.contains("new service data"));
    invoke(vec!["--resume".into(), session]).await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert_eq!(calls(root.path()).len(), 1);
    let (engine, _agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    let after = engine.status(run).await.unwrap();
    assert_eq!(after.record().status, RunStatus::Succeeded);
    assert_eq!(
        json!(
            after
                .activities()
                .iter()
                .find(|a| a.id == saved.id)
                .unwrap()
        ),
        json!(saved)
    );
    no_scratch(&root.path().join(".act0r/workspaces")).await;
    server.abort();
}
