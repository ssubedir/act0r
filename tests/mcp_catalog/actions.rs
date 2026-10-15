use super::*;

const ACTION: &str = "mcp_service_record_action";
fn config_action(root: &Path, mode: &str) -> McpConfig {
    let mut cfg = config(root, mode, &[]);
    cfg.servers[0].name = "service".into();
    cfg.servers[0].tools = serde_json::from_value(json!([
        {"name":"record_action","read_only":false,"requires_workspace":false}
    ]))
    .unwrap();
    cfg
}
fn action_arguments() -> Value {
    json!({"text":"approved local change","note":null})
}
fn effects(root: &Path) -> Vec<Value> {
    fs::read_to_string(root.join("calls.jsonl.effects"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
async fn actor_action(agents: &AgentService, policy: &str, repeat: bool) -> AgentId {
    let mut cfg = json!({"goal":"Record a local test action",
        "target":{"provider":"scripted","model":"local","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,"tools":[],
        "script":[{"kind":"call_tool","tool":ACTION,"arguments":action_arguments()}]});
    if repeat {
        let action = cfg["script"][0].clone();
        cfg["script"].as_array_mut().unwrap().push(action);
    }
    cfg["script"]
        .as_array_mut()
        .unwrap()
        .push(json!({"kind":"finish","summary":"Action handled"}));
    let id = if policy == "planner" {
        let helper = agents.create_registered_actor(DefinitionId::new("answer",1),None,
            json!({"goal":"Answer","target":{"provider":"scripted","model":"local","endpoint":""},
                "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,
                "script":[{"kind":"finish","summary":"Done"}]}),10,Default::default()).await.unwrap();
        cfg["workers"] =
            json!([{"name":"helper","agent_id":helper,"description":"Answer questions"}]);
        cfg["delegation"] = json!({"max_tasks":2,"max_depth":1});
        DefinitionId::new("planner", 3)
    } else {
        DefinitionId::new("tool", 1)
    };
    agents
        .create_registered_actor(id, None, cfg, 20, Default::default())
        .await
        .unwrap()
}
async fn waiting(
    engine: &act0r::Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
) -> (RunId, HumanWait) {
    settle(worker).await;
    let task = agents.actors().unwrap().task(task).await.unwrap();
    let run = task.run_id.unwrap();
    let saved = engine.status(run).await.unwrap();
    assert_eq!(saved.record().status, RunStatus::Waiting);
    (run, saved.record().wait.clone().unwrap())
}
async fn admitted_action(worker: &mut Worker) -> ActivityExecution {
    for _ in 0..100 {
        worker.advance_work().await.unwrap();
        if let Some(call) = worker.admit().await.unwrap() {
            assert_eq!(call.handler.name, "native.tool");
            assert_eq!(call.input["definition"]["id"]["name"], ACTION);
            return call;
        }
    }
    panic!("approved action was not admitted");
}

#[test]
fn external_actions_require_approval_reconciliation_and_no_project_authority_even_in_auto() {
    let contract = ExternalToolContract {
        binding: "service-action".into(),
        read_only: false,
        input_schema: json!({"type":"object","properties":{"text":{"type":"string"},"note":{"type":"string"}},"required":["text"]}),
        output_schema: None,
    };
    let action = ToolDefinition::builder(ACTION, 1, RecoveryPolicy::RequiresReconciliation)
        .description("MCP action service/record_action")
        .external_contract(contract)
        .requires_approval(true)
        .reports_errors(true)
        .build()
        .unwrap();
    for automatic in [false, true] {
        let Command::WaitForInput {
            prompt, payload, ..
        } = action
            .request_agent_call(
                &action_arguments(),
                Some("/project"),
                &[],
                None,
                &Default::default(),
                automatic,
            )
            .unwrap()
        else {
            panic!("action bypassed approval");
        };
        assert!(prompt.contains("external action"));
        assert_eq!(
            payload["arguments"],
            json!({"text":"approved local change"})
        );
        assert_eq!(payload["binding"], "service-action");
        assert!(payload.get("workspace").is_none());
    }
    assert_eq!(action.prompt_metadata()["requires_approval"], true);
    for change in [
        "approval",
        "recovery",
        "workspace",
        "writes",
        "tests",
        "commands",
        "feedback",
    ] {
        let mut bad = action.clone();
        match change {
            "approval" => bad.requires_approval = false,
            "recovery" => bad.recovery = RecoveryPolicy::SafeToRepeat,
            "workspace" => bad.requires_workspace = true,
            "writes" => bad.requires_writable_paths = true,
            "tests" => bad.requires_test_command = true,
            "commands" => bad.requires_project_command = true,
            _ => bad.reports_errors = false,
        }
        assert!(bad.validate().is_err(), "{change}");
    }
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn approval_and_denial_survive_restart_and_duplicate_responses_never_repeat_an_action() {
    for policy in ["tool", "planner"] {
        for approved in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let database = root.path().join("approval.db");
            let cfg = config_action(root.path(), "success");
            let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
                .await
                .unwrap();
            let actor = actor_action(&agents, policy, false).await;
            let task = send(&agents, actor).await;
            let mut worker = engine.worker().await.unwrap();
            let (run, wait) = waiting(&engine, &agents, &mut worker, task).await;
            assert_eq!(wait.key, "native_tool_approval");
            assert!(
                wait.payload["description"]
                    .as_str()
                    .unwrap()
                    .contains("service/record_action")
            );
            assert_eq!(
                wait.payload["arguments"],
                json!({"text":"approved local change"})
            );
            assert!(calls(root.path()).is_empty());
            assert!(effects(root.path()).is_empty());
            drop(worker);
            drop(agents);
            drop(engine);
            let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
                .await
                .unwrap();
            let mut worker = engine.worker().await.unwrap();
            assert_eq!(
                json!(engine.status(run).await.unwrap().record().wait),
                json!(wait)
            );
            assert!(
                engine
                    .signal(run, wait.id, json!({"approved":"yes"}))
                    .await
                    .is_err()
            );
            engine
                .signal(
                    run,
                    wait.id,
                    json!({"approved":approved,"arguments":{"text":"forged"}}),
                )
                .await
                .unwrap();
            engine
                .signal(
                    run,
                    wait.id,
                    json!({"approved":approved,"arguments":{"text":"forged"}}),
                )
                .await
                .unwrap();
            assert!(
                engine
                    .signal(run, wait.id, json!({"approved":!approved}))
                    .await
                    .is_err()
            );
            settle(&mut worker).await;
            let saved = engine.status(run).await.unwrap();
            assert_eq!(saved.record().status, RunStatus::Succeeded);
            assert_eq!(
                effects(root.path()),
                if approved {
                    vec![json!({"text":"approved local change"})]
                } else {
                    vec![]
                }
            );
            let tool = saved
                .activities()
                .iter()
                .find(|a| a.spec.handler.name == "native.tool")
                .cloned();
            if let Some(tool) = &tool {
                assert_eq!(tool.attempts, 1);
            }
            drop(worker);
            drop(agents);
            drop(engine);
            let (engine, _agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
            let mut worker = engine.worker().await.unwrap();
            settle(&mut worker).await;
            let saved = engine.status(run).await.unwrap();
            assert_eq!(
                json!(
                    saved
                        .activities()
                        .iter()
                        .find(|a| a.spec.handler.name == "native.tool")
                ),
                json!(tool)
            );
            assert_eq!(calls(root.path()).len(), usize::from(approved));
        }
    }
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn service_actions_do_not_break_project_discovery_and_remain_approved_in_coding() {
    use act0r::domain::harness::{HarnessCoding, HarnessRequest};
    for mode in ["inspect", "discovery", "coding"] {
        let root = tempfile::tempdir().unwrap();
        let mut cfg = config_action(root.path(), "success");
        cfg.servers[0].tools.push(
            serde_json::from_value(
                json!({"name":"get_file_info","read_only":true,"path_parameters":["path"]}),
            )
            .unwrap(),
        );
        cfg.replace_inspection = true;
        let (_engine, agents) = local_app_with_mcp_tools(&root.path().join("project.db"), cfg)
            .await
            .unwrap();
        let request = HarnessRequest {
            auto_execute: mode == "coding",
            discover_project: mode == "discovery",
            message_id: MessageId::new(),
            prompt: "Work on the project".into(),
            workspace: Some(source(root.path()).to_string_lossy().into()),
            budget: None,
            target: ModelTarget {
                provider: "openai".into(),
                model: "unused".into(),
                endpoint: "http://127.0.0.1:1/v1/chat/completions".into(),
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
            .start(request)
            .await
            .unwrap();
        let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
        let planner = &team.members[0].pinned_config["input"];
        let definitions = planner["tool_definitions"].as_array().unwrap();
        let action = definitions.iter().find(|d| d["id"]["name"] == ACTION);
        if mode == "discovery" {
            assert!(action.is_none());
        } else {
            let action: ToolDefinition = serde_json::from_value(action.unwrap().clone()).unwrap();
            assert!(action.requires_approval);
            assert_eq!(action.recovery, RecoveryPolicy::RequiresReconciliation);
        }
        assert!(
            definitions
                .iter()
                .any(|d| d["id"]["name"] == "mcp_service_get_file_info")
        );
        assert!(effects(root.path()).is_empty());
    }
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn each_model_action_needs_its_own_approval_even_for_identical_arguments() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app_with_mcp_tools(
        &root.path().join("repeat.db"),
        config_action(root.path(), "success"),
    )
    .await
    .unwrap();
    let actor = actor_action(&agents, "tool", true).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let (run, first) = waiting(&engine, &agents, &mut worker, task).await;
    let approved = json!({"approved":true});
    engine
        .signal(run, first.id, approved.clone())
        .await
        .unwrap();
    let (_, second) = waiting(&engine, &agents, &mut worker, task).await;
    assert_ne!(first.id, second.id);
    assert_eq!(effects(root.path()).len(), 1);
    engine.signal(run, first.id, approved).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        engine
            .status(run)
            .await
            .unwrap()
            .record()
            .wait
            .as_ref()
            .unwrap()
            .id,
        second.id
    );
    assert_eq!(effects(root.path()).len(), 1);
    engine
        .signal(run, second.id, json!({"approved":false}))
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    assert_eq!(effects(root.path()).len(), 1);
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn uncertain_actions_block_across_restart_and_verified_results_continue_without_redispatch() {
    // Success with no local commit simulates a host exit after the service changed.
    for mode in [
        "success",
        "disconnect_once",
        "hang",
        "protocol_error",
        "tool_error",
        "bad_output",
        "malformed",
        "oversized",
    ] {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("unknown.db");
        let mut cfg = config_action(root.path(), mode);
        if mode == "hang" {
            cfg.servers[0].command.timeout_ms = 1500;
        }
        let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
            .await
            .unwrap();
        let actor = actor_action(&agents, "tool", false).await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let (run, wait) = waiting(&engine, &agents, &mut worker, task).await;
        engine
            .signal(run, wait.id, json!({"approved":true}))
            .await
            .unwrap();
        let call = admitted_action(&mut worker).await;
        let outcome = worker.execute(call.clone()).await.unwrap();
        if mode == "success" {
            assert!(outcome.result().is_some());
        } else {
            assert!(
                matches!(outcome, Outcome::Uncertain(_)),
                "{mode}: {outcome:?}"
            );
            worker.complete(call.clone(), outcome).await.unwrap();
        }
        assert_eq!(effects(root.path()).len(), 1, "{mode}");
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, _agents) = local_app_with_mcp_tools(&database, cfg.clone())
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        let blocked = engine.status(run).await.unwrap();
        assert_eq!(blocked.record().status, RunStatus::Blocked, "{mode}");
        assert_eq!(
            blocked.record().error.as_ref().unwrap().code,
            "uncertain_outcome"
        );
        assert_eq!(
            blocked
                .activities()
                .iter()
                .find(|a| a.id == call.activity_id)
                .unwrap()
                .status,
            ActivityStatus::Indeterminate
        );
        assert!(engine.resume(run).await.is_err());
        assert_eq!(calls(root.path()).len(), 1);
        assert_eq!(effects(root.path()).len(), 1);
        let decision = ReconciliationRequest {
            activity_id: call.activity_id,
            attempt_id: call.attempt_id,
            evidence:
                "Inspected the local service's persisted effect and receipt; the operation stopped."
                    .into(),
            decision: ReconciliationDecision::Accept {
                result: json!({"ok":true,"content":[],"structuredContent":{"receipt":"effect-1"}}),
            },
        };
        engine.reconcile(run, decision.clone()).await.unwrap();
        engine.reconcile(run, decision.clone()).await.unwrap();
        settle(&mut worker).await;
        let saved = engine.status(run).await.unwrap();
        assert_eq!(saved.record().status, RunStatus::Succeeded, "{mode}");
        assert_eq!(saved.record().reconciliation_receipts.len(), 1);
        assert_eq!(calls(root.path()).len(), 1);
        drop(worker);
        drop(_agents);
        drop(engine);
        let (engine, _agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        engine.reconcile(run, decision).await.unwrap();
        settle(&mut worker).await;
        assert_eq!(calls(root.path()).len(), 1);
        assert_eq!(effects(root.path()).len(), 1);
        super::general::no_scratch(&root.path().join(".act0r/workspaces")).await;
    }
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn predispatch_contract_changes_or_timeouts_never_become_unknown_effects() {
    for mode in ["changed", "startup_hang"] {
        let root = tempfile::tempdir().unwrap();
        let mut cfg = config_action(root.path(), "success");
        cfg.servers[0].command.timeout_ms = 1500;
        let managed = root.path().join("scratch");
        let tools = discover_tools(cfg, Arc::new(LocalTools::new(managed.clone())), &managed)
            .await
            .unwrap();
        let Command::Schedule(spec) = tools[0]
            .definition()
            .schedule_call(&action_arguments())
            .unwrap()
        else {
            panic!()
        };
        let registry = ToolRegistry::new(tools).unwrap();
        fs::write(root.path().join("calls.jsonl.mode"), mode).unwrap();
        let outcome = registry
            .execute(ActivityExecution {
                run_id: RunId::new(),
                activity_id: ActivityId::new(),
                attempt_id: AttemptId::new(),
                ordinal: 1,
                handler: spec.handler,
                input: spec.input,
            })
            .await
            .unwrap();
        assert!(
            matches!(outcome, Outcome::Failure(_)),
            "{mode}: {outcome:?}"
        );
        assert!(calls(root.path()).is_empty());
        assert!(effects(root.path()).is_empty());
        super::general::no_scratch(&managed).await;
    }
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn retry_requires_an_attempt_specific_inspection_decision_and_keeps_the_approved_arguments() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("retry.db");
    let cfg = config_action(root.path(), "disconnect_before_effect_once");
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    let actor = actor_action(&agents, "tool", false).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let (run, wait) = waiting(&engine, &agents, &mut worker, task).await;
    engine
        .signal(run, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    let first = admitted_action(&mut worker).await;
    let outcome = worker.execute(first.clone()).await.unwrap();
    assert!(matches!(outcome, Outcome::Uncertain(_)));
    worker.complete(first.clone(), outcome).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(calls(root.path()).len(), 1);
    assert!(effects(root.path()).is_empty());
    let decision = ReconciliationRequest {
        activity_id: first.activity_id,
        attempt_id: first.attempt_id,
        evidence: "Inspected the service; the process stopped before writing any effect.".into(),
        decision: ReconciliationDecision::Retry {},
    };
    let mut invalid = decision.clone();
    invalid.evidence.clear();
    assert!(engine.reconcile(run, invalid).await.is_err());
    engine.reconcile(run, decision.clone()).await.unwrap();
    engine.reconcile(run, decision.clone()).await.unwrap();
    let second = admitted_action(&mut worker).await;
    assert_eq!(second.activity_id, first.activity_id);
    assert_ne!(second.attempt_id, first.attempt_id);
    assert_eq!(second.input, first.input);
    assert_eq!(second.ordinal, 2);
    let outcome = worker.execute(second.clone()).await.unwrap();
    worker.complete(second, outcome).await.unwrap();
    settle(&mut worker).await;
    engine.reconcile(run, decision).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    assert_eq!(calls(root.path()).len(), 2);
    assert_eq!(
        effects(root.path()),
        vec![json!({"text":"approved local change"})]
    );
}

#[tokio::test]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn approved_action_is_not_dispatched_under_a_changed_host_policy() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("policy.db");
    let cfg = config_action(root.path(), "success");
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
        .await
        .unwrap();
    let actor = actor_action(&agents, "tool", false).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let (run, wait) = waiting(&engine, &agents, &mut worker, task).await;
    engine
        .signal(run, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let mut changed = cfg.clone();
    changed.servers[0].tools[0].read_only = true;
    let (engine, agents) = local_app_with_mcp_tools(&database, changed).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Blocked
    );
    assert!(engine.resume(run).await.is_err());
    assert!(calls(root.path()).is_empty());
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, _agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    engine.resume(run).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    assert_eq!(effects(root.path()).len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Node.js; included in MCP CI checks"]
async fn cli_shows_exact_action_approval_and_resumes_without_duplicate_execution() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("chat.db");
    fs::write(
        root.path().join("mcp.json"),
        serde_json::to_vec(&config_action(root.path(), "success")).unwrap(),
    )
    .unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let app=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
        let index=requests.fetch_add(1,Ordering::SeqCst);
        assert!(body["tools"].as_array().unwrap().iter().any(|t|t["function"]["name"]==ACTION));
        let (name,args)=match index {
            0=>(ACTION,action_arguments()),
            1=>{
                assert!(body["messages"].to_string().contains("effect-1"));
                ("finish",json!({"summary":"Saved action receipt"}))
            },
            _=>panic!("unexpected model call"),
        };
        async move { axum::Json(json!({"id":format!("mcp-action-{index}"),"choices":[{"message":{"tool_calls":[{"id":format!("call-{index}"),"type":"function","function":{"name":name,"arguments":args.to_string()}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":10}})) }
    }));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let invoke = |args: Vec<String>| {
        let db = database.clone();
        tokio::task::spawn_blocking(move || {
            let output = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"))
                .arg("--db")
                .arg(db)
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
        "Record a local test action".into(),
    ])
    .await
    .unwrap();
    let session = first.iter().find(|e| e["event"] == "session").unwrap()["data"]["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let approval = &first.iter().find(|e| e["event"] == "approval").unwrap()["data"];
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(effects(root.path()).is_empty());
    let details = &first
        .iter()
        .find(|e| e["event"] == "approval_details")
        .unwrap()["data"];
    assert_eq!(
        details["arguments"],
        json!({"text":"approved local change"})
    );
    assert!(
        details["description"]
            .as_str()
            .unwrap()
            .contains("service/record_action")
    );
    let second = invoke(vec!["--resume".into(), session.clone(), "yes".into()])
        .await
        .unwrap();
    assert_eq!(
        second.iter().find(|e| e["event"] == "approval").unwrap()["data"]["wait"]["id"],
        approval["wait"]["id"]
    );
    assert!(
        second
            .iter()
            .any(|e| e["event"] == "response" && e["data"]["status"] == "succeeded")
    );
    invoke(vec!["--resume".into(), session]).await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(
        effects(root.path()),
        vec![json!({"text":"approved local change"})]
    );
    assert_eq!(calls(root.path()).len(), 1);
    server.abort();
}
