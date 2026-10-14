use super::*;

#[path = "context.rs"]
mod context;
#[path = "creation.rs"]
mod creation;
#[path = "recall.rs"]
mod recall;
#[path = "search.rs"]
mod search;

fn test_command(timeout_ms: u32) -> WorkspaceTestCommand {
    WorkspaceTestCommand {
        argv: vec![
            std::env::current_exe()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "--ignored".into(),
            "--exact".into(),
            "native_tests::test_process_fixture".into(),
            "--nocapture".into(),
        ],
        timeout_ms,
    }
}
fn tests_config() -> Value {
    let mut cfg = config();
    cfg["tools"] = json!([{"name":"run_tests","version":1}]);
    cfg["test_command"] = json!(test_command(10_000));
    cfg["script"] = json!([
        {"kind":"call_tool","tool":"run_tests","arguments":{}},
        {"kind":"finish","summary":"Used saved test results."}
    ]);
    cfg
}
fn test_source(root: &Path, mode: &str) -> PathBuf {
    let src = source(root);
    fs::write(src.join("test-mode"), mode).unwrap();
    src
}
async fn approve_tests(app: &App, task: MessageId) -> RunId {
    let (run, wait) = approval(app, task).await;
    assert_eq!(
        wait.payload["tool"],
        json!({"name":"run_tests","version":1})
    );
    assert_eq!(wait.payload["arguments"], json!({}));
    assert_eq!(wait.payload["recovery"], "RequiresReconciliation");
    assert!(wait.payload.get("writable_paths").is_none());
    assert_eq!(
        wait.payload["test_command"]["argv"],
        json!(test_command(1).argv)
    );
    app.engine
        .signal(run, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    app.engine
        .signal(run, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    run
}
fn execution_count(workspace: &Path) -> usize {
    fs::read_to_string(workspace.join(".act0r-tools/test-runs"))
        .unwrap_or_default()
        .lines()
        .count()
}

// The host pins this executable and these arguments. Modes are trusted test fixtures,
// not commands or permissions supplied by a model.
#[test]
#[ignore = "invoked as a fixed subprocess by native test-tool integration tests"]
fn test_process_fixture() {
    use std::io::Write;
    let workspace = std::env::current_dir().unwrap();
    let private = workspace.join(".act0r-tools");
    let mut ledger = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(private.join("test-runs"))
        .unwrap();
    writeln!(ledger, "executed").unwrap();
    let mode = fs::read_to_string(workspace.join("test-mode")).unwrap();
    match mode.as_str() {
        "verify" => {
            let text = fs::read_to_string(workspace.join("README.md")).unwrap();
            println!("README evidence: {text}");
            assert_eq!(
                text, "Correct project evidence.\n",
                "expected corrected project evidence"
            );
        }
        "output" => {
            // Include control bytes and invalid UTF-8 to exercise JSON and lossy expansion.
            let bytes = [0, 0xff, b'"', b'\\'].repeat(12_000);
            std::io::stdout().write_all(&bytes).unwrap();
            std::io::stderr().write_all(&bytes).unwrap();
        }
        "hang" => {
            fs::write(
                private.join("process-ready"),
                std::process::id().to_string(),
            )
            .unwrap();
            let started = Instant::now();
            while !private.join("release-process").exists() {
                assert!(
                    started.elapsed() < Duration::from_secs(15),
                    "fixture release deadline"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            fs::write(private.join("process-finished"), "released").unwrap();
        }
        "pass" => println!("fixed native tests passed"),
        "search_verify" => {
            let text = fs::read_to_string(workspace.join("src/lib.rs")).unwrap();
            assert_eq!(text, "pub fn answer() -> i64 { 19 + 23 }\n");
            println!("Search-guided function repair passed.");
        }
        mode => panic!("unknown fixture mode {mode}"),
    }
}

struct RepairFromTests;
impl ModelClient for RepairFromTests {
    fn generate(&self, request: ModelRequest, activity: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            if request.target.model == "assistant" {
                return ModelRouter::new(None)
                    .unwrap()
                    .generate(request, activity)
                    .await;
            }
            let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
            let observations = prompt
                .get("tool_observations")
                .unwrap_or(&prompt["observations"]);
            let result = observations
                .as_array()
                .and_then(|items| items.last())
                .map(|last| &last["result"]);
            let action = match request.turn {
                0 => AgentAction::CallTool {
                    tool: "read_file".into(),
                    arguments: json!({"path":"README.md"}),
                },
                1 => {
                    assert_eq!(result.unwrap()["text"], "Original project evidence.\n");
                    AgentAction::CallTool {
                        tool: "replace_text".into(),
                        arguments: json!({"path":"README.md","old":"Original","new":"Broken"}),
                    }
                }
                2 | 4 => AgentAction::CallTool {
                    tool: "run_tests".into(),
                    arguments: json!({}),
                },
                3 => {
                    let failed = result.unwrap();
                    assert_eq!(failed["ok"], false);
                    assert_ne!(failed["exit_code"], 0);
                    assert!(
                        failed["stderr"]
                            .as_str()
                            .unwrap()
                            .contains("expected corrected project evidence")
                    );
                    AgentAction::CallTool {
                        tool: "replace_text".into(),
                        arguments: json!({"path":"README.md","old":"Broken","new":"Correct"}),
                    }
                }
                5 if request.target.model == "planner" => {
                    let passed = result.unwrap();
                    assert_eq!(passed["ok"], true);
                    assert_eq!(passed["exit_code"], 0);
                    AgentAction::DelegateTask {
                        writable_paths: None,
                        worker: "reviewer".into(),
                        goal: format!("Review saved tests: {passed}"),
                    }
                }
                5 | 6 => AgentAction::Finish {
                    summary: "Repaired using saved failing and passing tests.".into(),
                },
                _ => panic!("unexpected repair turn {}", request.turn),
            };
            Ok(ModelReply {
                decision: Ok(ModelDecision {
                    action,
                    request_id: None,
                    diagnostics: None,
                }),
                usage: Some(TokenUsage {
                    input_tokens: 10,
                    output_tokens: 10,
                }),
                retryable: false,
                diagnostics: None,
                retry_after: None,
            })
        })
    }
}

#[tokio::test]
async fn failed_tests_guide_repairs_and_saved_results_survive_restart_and_delegation() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("repair.db");
        let src = test_source(dir.path(), "verify");
        let calls = Arc::new(Calls::default());
        let mut app = open(&db, calls.clone(), Some(Arc::new(RepairFromTests))).await;
        let mut cfg = tests_config();
        cfg["tools"] = json!([{"name":"read_file","version":2},{"name":"replace_text","version":1},{"name":"run_tests","version":1}]);
        cfg["writable_paths"] = json!(["README.md"]);
        cfg["script"] = json!([]);
        cfg["max_model_calls"] = json!(7);
        if policy == "planner" {
            cfg["goal_budget"] = json!({"max_model_calls":9,"max_tokens":300000});
        }
        let actor = create(&app, policy, Some(src.clone()), cfg, 16).await;
        let task = app.agents.actors().unwrap().send(AgentMessage {
            id:MessageId::new(), sender:None, recipient:actor, correlation_id:None, kind:"task".into(),
            payload:json!({"goal":"Repair the project", "test_command":{"argv":["forged-command"],"timeout_ms":1}}),
        }).await.unwrap();
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let (read, _) = native_once(&mut worker).await;
        assert!(read.input.get("test_command").is_none());
        model_once(&mut worker).await;
        approve_edit(&app, task, true).await;
        let (edit, _) = native_once(&mut worker).await;
        assert!(edit.input.get("test_command").is_none());
        let workspace = PathBuf::from(read.input["workspace"].as_str().unwrap());
        let mut test_ids = vec![];
        for passing in [false, true] {
            model_once(&mut worker).await;
            let run = approve_tests(&app, task).await;
            let (tests, outcome) = native_once(&mut worker).await;
            assert_eq!(tests.handler, DefinitionId::new("native.tool", 5));
            assert_eq!(tests.input["test_command"], json!(test_command(10_000)));
            assert!(tests.input.get("writable_paths").is_none());
            let result = outcome.result().unwrap();
            assert_eq!(result["ok"], passing);
            assert!(result["elapsed_ms"].is_u64());
            test_ids.push(tests.activity_id);
            let saved = app.engine.status(run).await.unwrap();
            let next = saved.activities().last().unwrap().spec.input.clone();
            let usage = json!(saved.record().usage);
            drop(worker);
            drop(app);
            app = open(&db, calls.clone(), Some(Arc::new(RepairFromTests))).await;
            worker = app.engine.worker().await.unwrap();
            let restored = app.engine.status(run).await.unwrap();
            assert_eq!(restored.activities().last().unwrap().spec.input, next);
            assert_eq!(json!(restored.record().usage), usage);
            assert_eq!(
                restored
                    .activities()
                    .iter()
                    .find(|a| a.id == tests.activity_id)
                    .unwrap()
                    .result
                    .as_ref()
                    .unwrap(),
                result
            );
            assert_eq!(execution_count(&workspace), if passing { 2 } else { 1 });
            if !passing {
                model_once(&mut worker).await;
                approve_edit(&app, task, true).await;
                native_once(&mut worker).await;
            }
        }
        // A later change cannot replace already-saved evidence given to the reviewer.
        fs::write(workspace.join("README.md"), "later workspace content").unwrap();
        settle(&mut worker).await;
        if policy == "planner" {
            let child = app
                .agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .delegation
                .unwrap();
            assert!(child.goal.contains("Correct project evidence."));
            assert!(child.goal.contains("\"exit_code\":0"));
            assert!(!child.goal.contains("later workspace content"));
            let waiting = app.agents.actors().unwrap().task(child.id).await.unwrap();
            let wait = waiting.wait.unwrap();
            drop(worker);
            drop(app);
            app = open(&db, calls.clone(), Some(Arc::new(RepairFromTests))).await;
            worker = app.engine.worker().await.unwrap();
            app.engine
                .signal(
                    waiting.run_id.unwrap(),
                    wait.id,
                    json!({"answer":"reviewed"}),
                )
                .await
                .unwrap();
            settle(&mut worker).await;
            let tree = app
                .agents
                .actors()
                .unwrap()
                .inspect_task(task, Default::default())
                .await
                .unwrap();
            assert_eq!(tree.summary.visible_usage.admitted_model_calls, 9);
        }
        let task = app.agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(task.status, AgentTaskStatus::Succeeded);
        let run = app.engine.status(task.run_id.unwrap()).await.unwrap();
        for id in test_ids {
            assert_eq!(
                run.attempts()
                    .iter()
                    .filter(|a| a.activity_id == id)
                    .count(),
                1
            );
        }
        assert_eq!(execution_count(&workspace), 2);
        assert_eq!(
            fs::read_to_string(src.join("README.md")).unwrap(),
            "Original project evidence.\n"
        );
    }
}

#[tokio::test]
async fn test_commands_are_validated_before_copy_and_pinned_outside_model_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(
        &dir.path().join("pins.db"),
        Arc::new(Calls::default()),
        None,
    )
    .await;
    let src = test_source(dir.path(), "pass");
    let mut invalid = vec![
        Value::Null,
        json!({"argv":[]}),
        json!({"argv":[""]}),
        json!({"argv":["x\u{0}"]}),
        json!({"argv":vec!["x";129]}),
        json!({"argv":["x".repeat(16385)]}),
        json!({"argv":["x"],"timeout_ms":0}),
        json!({"argv":["x"],"timeout_ms":600001}),
        json!({"argv":["x"],"shell":true}),
    ];
    for command in invalid.drain(..) {
        let mut cfg = tests_config();
        cfg["test_command"] = command;
        assert!(
            app.agents
                .create_registered_actor(
                    DefinitionId::new("tool", 1),
                    Some(src.clone()),
                    cfg,
                    8,
                    Default::default()
                )
                .await
                .is_err()
        );
    }
    assert!(!dir.path().join("workspaces").exists());
    let mut unused = config();
    unused["test_command"] = json!(test_command(1000));
    assert!(
        app.agents
            .create_registered_actor(
                DefinitionId::new("tool", 1),
                Some(src.clone()),
                unused,
                8,
                Default::default()
            )
            .await
            .is_err()
    );
    let runner = Arc::new(LocalTools::new(dir.path().join("workspaces")));
    let definition = RunTestsTool::new(runner.clone()).definition();
    assert!(
        definition
            .schedule_call_in(&json!({}), Some("workspace"))
            .is_err()
    );
    assert!(
        definition
            .schedule_call_with_context(
                &json!({"command":"forged"}),
                Some("workspace"),
                &[],
                Some(&test_command(1000))
            )
            .is_err()
    );
    let prepared = LocalWorkspaces::new(dir.path().join("workspaces"))
        .prepare(src)
        .await
        .unwrap();
    let registry =
        act0r::application::ToolRegistry::new(vec![Arc::new(RunTestsTool::new(runner))]).unwrap();
    let Command::Schedule(spec) = definition
        .schedule_call_with_context(
            &json!({}),
            Some(&prepared),
            &[],
            Some(&test_command(10_000)),
        )
        .unwrap()
    else {
        panic!()
    };
    let execution = ActivityExecution {
        ordinal: 0,
        run_id: RunId::new(),
        activity_id: ActivityId::new(),
        attempt_id: AttemptId::new(),
        handler: spec.handler,
        input: spec.input,
    };
    for mutation in ["handler", "pin", "missing", "arguments", "permissions"] {
        let mut forged = execution.clone();
        match mutation {
            "handler" => forged.handler.version = 4,
            "pin" => forged.input["definition"]["recovery"] = json!("SafeToRepeat"),
            "missing" => {
                forged.input.as_object_mut().unwrap().remove("test_command");
            }
            "arguments" => forged.input["arguments"] = json!({"argv":["forged"]}),
            "permissions" => forged.input["writable_paths"] = json!(["README.md"]),
            _ => unreachable!(),
        }
        let result = registry.execute(forged).await;
        assert!(
            result.is_err() || matches!(result, Ok(Outcome::Failure(_))),
            "{mutation}"
        );
        assert_eq!(execution_count(Path::new(&prepared)), 0);
    }
    let result = registry.execute(execution).await.unwrap();
    assert_eq!(result.result().unwrap()["ok"], true);
    let echo = EchoTool.definition();
    let old: ToolDefinition = serde_json::from_value(json!(echo)).unwrap();
    assert!(!old.requires_test_command);
    assert!(json!(old).get("requires_test_command").is_none());
    assert_eq!(echo.handler(), DefinitionId::new("native.tool", 1));
}

#[tokio::test]
async fn test_output_is_drained_bounded_and_safe_under_json_expansion() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("workspaces");
    let workspace = LocalWorkspaces::new(root.clone())
        .prepare(test_source(dir.path(), "output"))
        .await
        .unwrap();
    let tools = act0r::application::ToolRegistry::new(vec![Arc::new(RunTestsTool::new(Arc::new(
        LocalTools::new(root),
    )))])
    .unwrap();
    let definition = tools.definitions().pop().unwrap();
    let Command::Schedule(spec) = definition
        .schedule_call_with_context(
            &json!({}),
            Some(&workspace),
            &[],
            Some(&test_command(10_000)),
        )
        .unwrap()
    else {
        panic!()
    };
    let outcome = tools
        .execute(ActivityExecution {
            ordinal: 0,
            run_id: RunId::new(),
            activity_id: ActivityId::new(),
            attempt_id: AttemptId::new(),
            handler: spec.handler,
            input: spec.input,
        })
        .await
        .unwrap();
    let result = outcome.result().unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(result["stdout_truncated"], true);
    assert_eq!(result["stderr_truncated"], true);
    assert_eq!(result["output_truncated"], true);
    assert!(result.to_string().len() <= definition.max_output_bytes as usize);
    for stream in ["stdout", "stderr"] {
        assert!(result[stream].as_str().unwrap().len() <= 3 * 16 * 1024);
    }
}

#[tokio::test]
async fn tests_hold_workspace_ownership_and_timeout_requires_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("workspaces");
    let workspace = LocalWorkspaces::new(root.clone())
        .prepare(test_source(dir.path(), "hang"))
        .await
        .unwrap();
    let runner = Arc::new(LocalTools::new(root));
    let running = {
        let runner = runner.clone();
        let workspace = workspace.clone();
        tokio::spawn(async move {
            runner
                .run_tests(workspace, test_command(2000), ActivityId::new())
                .await
        })
    };
    let ready = Path::new(&workspace).join(".act0r-tools/process-ready");
    let start = Instant::now();
    while !ready.exists() {
        assert!(start.elapsed() < Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(matches!(
        runner
            .inspect(
                workspace.clone(),
                WorkspaceReadAction::ListFiles,
                ActivityId::new()
            )
            .await,
        Err(Error::Ownership(_))
    ));
    let error = running.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("timed out"));
    // The direct child was reaped before ownership is made available to another tool.
    runner
        .inspect(workspace, WorkspaceReadAction::ListFiles, ActivityId::new())
        .await
        .unwrap();
    let db = dir.path().join("timeout.db");
    let app = open(&db, Arc::new(Calls::default()), None).await;
    let mut cfg = tests_config();
    cfg["test_command"]["timeout_ms"] = json!(1000);
    let actor = create(&app, "tool", Some(dir.path().join("source")), cfg, 8).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let run = approve_tests(&app, task).await;
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(worker.execution_timeout_ms(&execution, 100), 7000);
    let error = worker.execute(execution.clone()).await.unwrap_err();
    let outcome = Outcome::Uncertain(format!("executor error: {error}"));
    worker.complete(execution, outcome).await.unwrap();
    assert_eq!(
        app.engine.status(run).await.unwrap().record().status,
        RunStatus::Blocked
    );
    assert!(worker.admit().await.unwrap().is_none());
}

#[tokio::test]
async fn test_approval_denial_pause_and_cancellation_survive_restart() {
    for policy in ["tool", "planner"] {
        for mode in [
            "deny",
            "pause",
            "cancel",
            "inflight_pause",
            "inflight_cancel",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("controls.db");
            let calls = Arc::new(Calls::default());
            let app = open(&db, calls.clone(), None).await;
            let actor = create(
                &app,
                policy,
                Some(test_source(dir.path(), "pass")),
                tests_config(),
                8,
            )
            .await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            model_once(&mut worker).await;
            let (run, wait) = approval(&app, task).await;
            let workspace = PathBuf::from(wait.payload["workspace"].as_str().unwrap());
            if mode == "deny" {
                app.engine
                    .signal(run, wait.id, json!({"approved":false}))
                    .await
                    .unwrap();
            } else {
                if mode == "pause" {
                    app.agents.actors().unwrap().pause_goal(task).await.unwrap();
                }
                app.engine
                    .signal(run, wait.id, json!({"approved":true}))
                    .await
                    .unwrap();
                if mode == "cancel" {
                    app.agents
                        .actors()
                        .unwrap()
                        .cancel_goal(task)
                        .await
                        .unwrap();
                }
                if mode.starts_with("inflight") {
                    let execution = worker.admit().await.unwrap().unwrap();
                    let outcome = worker.execute(execution.clone()).await.unwrap();
                    if mode.ends_with("pause") {
                        app.agents.actors().unwrap().pause_goal(task).await.unwrap();
                    } else {
                        app.agents
                            .actors()
                            .unwrap()
                            .cancel_goal(task)
                            .await
                            .unwrap();
                    }
                    worker.complete(execution, outcome).await.unwrap();
                }
            }
            if mode.contains("pause") {
                assert!(worker.admit().await.unwrap().is_none());
            }
            drop(worker);
            drop(app);
            let app = open(&db, calls.clone(), None).await;
            let mut worker = app.engine.worker().await.unwrap();
            if mode.contains("pause") {
                app.agents
                    .actors()
                    .unwrap()
                    .resume_goal(task)
                    .await
                    .unwrap();
            }
            settle(&mut worker).await;
            assert_eq!(
                app.engine.status(run).await.unwrap().record().status,
                if mode.contains("cancel") {
                    RunStatus::Cancelled
                } else {
                    RunStatus::Succeeded
                }
            );
            let count = usize::from(mode.contains("pause") || mode.starts_with("inflight"));
            assert_eq!(execution_count(&workspace), count, "{policy}/{mode}");
            assert_eq!(calls.requests.lock().unwrap().len(), count);
            if mode == "deny" {
                let run = app.engine.status(run).await.unwrap();
                let next: &str = run.activities().last().unwrap().spec.input["prompt"]
                    .as_str()
                    .unwrap();
                let prompt: Value = serde_json::from_str(next).unwrap();
                let observations = prompt
                    .get("tool_observations")
                    .unwrap_or(&prompt["observations"]);
                assert_eq!(observations[0]["result"]["approved"], false);
            }
        }
    }
}

#[tokio::test]
async fn completed_tests_remain_saved_when_continuation_budgets_or_context_are_exhausted() {
    for policy in ["tool", "planner"] {
        for mode in [
            "attempt_before",
            "attempt_after",
            "model",
            "context",
            "shared",
        ] {
            if mode == "shared" && policy == "tool" {
                continue;
            }
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("limits.db");
            let calls = Arc::new(Calls::default());
            let app = open(&db, calls.clone(), None).await;
            let src = test_source(dir.path(), "output");
            let mut cfg = tests_config();
            cfg["max_input_bytes"] = json!(262144);
            if mode == "model" {
                cfg["max_model_calls"] = json!(1);
            }
            if mode == "shared" {
                cfg["goal_budget"] = json!({"max_model_calls":1,"max_tokens":300000});
            }
            if mode == "context" {
                let baseline = create(&app, policy, Some(src.clone()), cfg.clone(), 8).await;
                send(&app, baseline).await;
                let mut worker = app.engine.worker().await.unwrap();
                let model = model_once(&mut worker).await;
                let request: ModelRequest = serde_json::from_value(model.input).unwrap();
                cfg["max_input_bytes"] = json!(request.context_input_bytes());
                drop(worker);
            }
            let actor = create(
                &app,
                policy,
                Some(src),
                cfg,
                match mode {
                    "attempt_before" => 1,
                    "attempt_after" => 2,
                    _ => 8,
                },
            )
            .await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            model_once(&mut worker).await;
            let run = approve_tests(&app, task).await;
            settle(&mut worker).await;
            let saved = app.engine.status(run).await.unwrap();
            assert_eq!(
                saved.record().status,
                if mode == "context" {
                    RunStatus::Failed
                } else {
                    RunStatus::BudgetExhausted
                },
                "{policy}/{mode}"
            );
            let tests = saved
                .activities()
                .iter()
                .find(|a| a.spec.handler == DefinitionId::new("native.tool", 5))
                .unwrap();
            if mode == "attempt_before" {
                assert!(tests.result.is_none());
                assert_eq!(calls.requests.lock().unwrap().len(), 0);
            } else {
                assert_eq!(tests.result.as_ref().unwrap()["ok"], true);
                assert_eq!(tests.result.as_ref().unwrap()["output_truncated"], true);
                assert_eq!(calls.requests.lock().unwrap().len(), 1);
            }
            let before = json!(saved);
            drop(worker);
            drop(app);
            let app = open(&db, calls, None).await;
            let mut worker = app.engine.worker().await.unwrap();
            settle(&mut worker).await;
            assert_eq!(json!(app.engine.status(run).await.unwrap()), before);
        }
    }
}

#[tokio::test]
async fn test_completion_and_next_model_request_roll_back_and_commit_together() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let app = open(&db, Arc::new(Calls::default()), None).await;
    let actor = create(
        &app,
        "tool",
        Some(test_source(dir.path(), "pass")),
        tests_config(),
        8,
    )
    .await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let run = approve_tests(&app, task).await;
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    let workspace = PathBuf::from(execution.input["workspace"].as_str().unwrap());
    let before = json!(app.engine.status(run).await.unwrap());
    let history = json!(app.engine.history(run).await.unwrap());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_test_continuation BEFORE INSERT ON activities WHEN json_extract(NEW.payload,'$.spec.handler.name')='agent.model' BEGIN SELECT RAISE(ABORT,'test result rollback'); END").execute(&pool).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(json!(app.engine.status(run).await.unwrap()), before);
    assert_eq!(json!(app.engine.history(run).await.unwrap()), history);
    assert_eq!(execution_count(&workspace), 1);
    sqlx::query("DROP TRIGGER reject_test_continuation")
        .execute(&pool)
        .await
        .unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    worker.complete(execution, outcome).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        app.engine.status(run).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    assert_eq!(execution_count(&workspace), 1);
}

#[tokio::test]
#[ignore = "invoked and killed by the native test-tool recovery parent"]
async fn test_worker_crash_fixture() {
    let root = PathBuf::from(std::env::var("ACT0R_NATIVE_TEST_CRASH_DIR").unwrap());
    let stage = std::env::var("ACT0R_NATIVE_TEST_CRASH_STAGE").unwrap();
    let app = open(&root.join("crash.db"), Arc::new(Calls::default()), None).await;
    let actor = create(
        &app,
        "planner",
        Some(test_source(
            &root,
            if stage == "running" { "hang" } else { "pass" },
        )),
        tests_config(),
        8,
    )
    .await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let (run, wait) = approval(&app, task).await;
    let workspace = PathBuf::from(wait.payload["workspace"].as_str().unwrap());
    let mut tests = None;
    let mut running = None;
    if stage != "approval" {
        approve_tests(&app, task).await;
        if stage != "intent" {
            let execution = worker.admit().await.unwrap().unwrap();
            tests = Some(execution.clone());
            if stage == "running" {
                running = Some(tokio::spawn(worker.execute(execution)));
                let started = Instant::now();
                while !workspace.join(".act0r-tools/process-ready").exists() {
                    assert!(started.elapsed() < Duration::from_secs(10));
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            } else if stage == "executed" || stage == "saved" {
                let outcome = worker.execute(execution.clone()).await.unwrap();
                if stage == "saved" {
                    worker.complete(execution, outcome).await.unwrap();
                }
            }
        }
    }
    fs::write(
        root.join("ready.json"),
        serde_json::to_vec(
            &json!({"task":task,"run":run,"wait":wait,"execution":tests,"workspace":workspace}),
        )
        .unwrap(),
    )
    .unwrap();
    // Keep worker/database ownership until the parent actually kills this process.
    let _retained = (app, worker, running);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn killed_workers_preserve_test_approvals_intents_results_and_uncertain_processes() {
    for stage in [
        "approval", "intent", "admitted", "running", "executed", "saved",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "native_tests::test_worker_crash_fixture",
            ])
            .env("ACT0R_NATIVE_TEST_CRASH_DIR", root)
            .env("ACT0R_NATIVE_TEST_CRASH_STAGE", stage)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let start = Instant::now();
        while !root.join("ready.json").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "fixture exited {stage}"
            );
            if start.elapsed() > Duration::from_secs(25) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("fixture timeout {stage}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let ready: Value =
            serde_json::from_slice(&fs::read(root.join("ready.json")).unwrap()).unwrap();
        let task: MessageId = serde_json::from_value(ready["task"].clone()).unwrap();
        let run: RunId = serde_json::from_value(ready["run"].clone()).unwrap();
        let workspace = PathBuf::from(ready["workspace"].as_str().unwrap());
        let app = open(&root.join("crash.db"), Arc::new(Calls::default()), None).await;
        let before = app.engine.status(run).await.unwrap();
        let next = before.activities().last().unwrap().spec.input.clone();
        let usage = json!(before.record().usage);
        let mut worker = app.engine.worker().await.unwrap();
        if stage == "approval" {
            assert_eq!(
                json!(
                    app.agents
                        .actors()
                        .unwrap()
                        .task(task)
                        .await
                        .unwrap()
                        .wait
                        .unwrap()
                ),
                ready["wait"]
            );
            approve_tests(&app, task).await;
        } else if matches!(stage, "admitted" | "running" | "executed") {
            let execution: ActivityExecution =
                serde_json::from_value(ready["execution"].clone()).unwrap();
            let blocked = app.engine.status(run).await.unwrap();
            assert_eq!(blocked.record().status, RunStatus::Blocked);
            assert_eq!(
                blocked.activities()[1].status,
                ActivityStatus::Indeterminate
            );
            assert!(worker.admit().await.unwrap().is_none());
            if stage == "running" {
                #[cfg(windows)]
                {
                    // Closing the killed host's job terminates its test process too.
                    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
                    use windows_sys::Win32::System::Threading::{
                        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
                    };
                    let pid = fs::read_to_string(workspace.join(".act0r-tools/process-ready"))
                        .unwrap()
                        .parse::<u32>()
                        .unwrap();
                    let start = Instant::now();
                    loop {
                        // SAFETY: this is our controlled fixture PID; handles are checked,
                        // owned once, and only queried (never terminate an unrelated PID).
                        let process =
                            unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
                        if process.is_null() {
                            assert_eq!(std::io::Error::last_os_error().raw_os_error(), Some(87));
                            break;
                        }
                        let process = unsafe { OwnedHandle::from_raw_handle(process) };
                        let mut code = 0;
                        assert_ne!(
                            unsafe { GetExitCodeProcess(process.as_raw_handle(), &mut code) },
                            0
                        );
                        if code != 259 {
                            break;
                        }
                        assert!(start.elapsed() < Duration::from_secs(10));
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    assert!(!workspace.join(".act0r-tools/process-finished").exists());
                }
                #[cfg(unix)]
                {
                    // SIGKILL prevents the host's process-group guard from running.
                    // Inspect and stop this controlled orphan before authorizing retry.
                    fs::write(workspace.join(".act0r-tools/release-process"), "release").unwrap();
                    let start = Instant::now();
                    while !workspace.join(".act0r-tools/process-finished").exists() {
                        assert!(start.elapsed() < Duration::from_secs(10));
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                fs::write(workspace.join("test-mode"), "pass").unwrap();
            }
            let request=ReconciliationRequest {activity_id:execution.activity_id,attempt_id:execution.attempt_id,
                decision:ReconciliationDecision::Retry {},evidence:"Inspected the fixed fixture and verified its previous process has stopped; authorize one new test attempt.".into()};
            app.engine.reconcile(run, request.clone()).await.unwrap();
            app.engine.reconcile(run, request).await.unwrap();
            let retry = worker.admit().await.unwrap().unwrap();
            assert_eq!(retry.activity_id, execution.activity_id);
            assert_ne!(retry.attempt_id, execution.attempt_id);
            assert_eq!(retry.input, execution.input);
            let outcome = worker.execute(retry.clone()).await.unwrap();
            worker.complete(retry, outcome).await.unwrap();
        } else if stage == "saved" {
            let saved = app.engine.status(run).await.unwrap();
            assert_eq!(saved.activities().last().unwrap().spec.input, next);
            assert_eq!(json!(saved.record().usage), usage);
            assert_eq!(saved.activities()[1].result.as_ref().unwrap()["ok"], true);
        }
        settle(&mut worker).await;
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .status,
            AgentTaskStatus::Succeeded,
            "{stage}"
        );
        assert_eq!(
            execution_count(&workspace),
            if matches!(stage, "executed" | "running") {
                2
            } else {
                1
            },
            "{stage}"
        );
        let saved = app.engine.status(run).await.unwrap();
        let activity = saved
            .activities()
            .iter()
            .find(|a| a.spec.handler == DefinitionId::new("native.tool", 5))
            .unwrap();
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == activity.id)
                .count(),
            if matches!(stage, "admitted" | "running" | "executed") {
                2
            } else {
                1
            },
            "{stage}"
        );
    }
}
