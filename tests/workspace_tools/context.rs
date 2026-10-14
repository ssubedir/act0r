use super::*;

const READS_BEFORE: u32 = 10;
const TESTS_FINISHED: u32 = READS_BEFORE + 5;
const REVIEW_TURN: u32 = TESTS_FINISHED + 10;
// Fit the current planner instructions plus one essential result while evicting history.
const LONG_CONTEXT_BYTES: u32 = 10000;
struct LongRepair;
impl ModelClient for LongRepair {
    fn generate(&self, request: ModelRequest, activity: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            if request.target.model == "assistant" {
                return ModelRouter::new(None)
                    .unwrap()
                    .generate(request, activity)
                    .await;
            }
            let selection = request.context.as_ref().unwrap();
            assert_eq!(selection.input_bytes, request.context_input_bytes());
            assert!(selection.input_bytes <= u64::from(LONG_CONTEXT_BYTES));
            assert!(selection.included_observations <= 2);
            let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
            let observations = prompt
                .get("tool_observations")
                .unwrap_or(&prompt["observations"]);
            let result = observations
                .as_array()
                .and_then(|a| a.last())
                .map(|o| &o["result"]);
            let call = |tool: &str, arguments: Value| AgentAction::CallTool {
                tool: tool.into(),
                arguments,
            };
            let action = if request.turn < READS_BEFORE
                || (TESTS_FINISHED..REVIEW_TURN).contains(&request.turn)
            {
                call("read_file", json!({"path":"EVIDENCE.md"}))
            } else {
                match request.turn - READS_BEFORE {
                    0 => call("read_file", json!({"path":"README.md"})),
                    1 => {
                        assert_eq!(result.unwrap()["text"], "Original project evidence.\n");
                        call(
                            "replace_text",
                            json!({"path":"README.md","old":"Original","new":"Broken"}),
                        )
                    }
                    2 | 4 => call("run_tests", json!({})),
                    3 => {
                        let result = result.unwrap();
                        assert_eq!(result["ok"], false);
                        assert!(
                            result["stderr"]
                                .as_str()
                                .unwrap()
                                .contains("expected corrected project evidence")
                        );
                        assert_eq!(prompt["test_status"]["passed"], false);
                        call(
                            "replace_text",
                            json!({"path":"README.md","old":"Broken","new":"Correct"}),
                        )
                    }
                    _ if request.turn == REVIEW_TURN => {
                        assert_eq!(prompt["test_status"]["passed"], true);
                        assert_eq!(prompt["test_status"]["stale"], false);
                        assert_eq!(prompt["test_status"]["exit_code"], 0);
                        assert!(
                            !request.prompt.contains("README evidence:"),
                            "old test output was not evicted"
                        );
                        if request.target.model == "planner" {
                            AgentAction::DelegateTask {
                                writable_paths: None,
                                worker: "reviewer".into(),
                                goal: format!(
                                    "Review saved passing test status: {}",
                                    prompt["test_status"]
                                ),
                            }
                        } else {
                            AgentAction::Finish {
                                summary: "Long repair completed with saved passing tests.".into(),
                            }
                        }
                    }
                    _ if request.turn == REVIEW_TURN + 1 => AgentAction::Finish {
                        summary: "Long repair and delegated review completed.".into(),
                    },
                    _ => panic!("unexpected long repair turn {}", request.turn),
                }
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
fn long_config() -> Value {
    let mut cfg = tests_config();
    cfg["script"] = json!([]);
    cfg["max_model_calls"] = json!(40);
    cfg["max_tokens"] = json!(1000000);
    cfg["max_input_bytes"] = json!(12000);
    cfg["context"] = json!({"version":1,"max_input_bytes":LONG_CONTEXT_BYTES,"max_observations":2});
    cfg["tools"] = json!([{"name":"read_file","version":2},{"name":"replace_text","version":1},{"name":"run_tests","version":1}]);
    cfg["writable_paths"] = json!(["README.md"]);
    cfg
}
fn long_source(root: &Path) -> PathBuf {
    let src = test_source(root, "verify");
    fs::write(
        src.join("EVIDENCE.md"),
        "bounded evidence 🙂\\\n".repeat(140),
    )
    .unwrap();
    src
}
async fn until_tests(app: &App, worker: &mut Worker, task: MessageId, count: usize) -> RunId {
    for _ in 0..150 {
        worker.step().await.unwrap();
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        if let Some(wait) = view.wait {
            app.engine
                .signal(view.run_id.unwrap(), wait.id, json!({"approved":true}))
                .await
                .unwrap();
        }
        if let Some(run) = view.run_id {
            let saved = app.engine.status(run).await.unwrap();
            if saved
                .activities()
                .iter()
                .filter(|a| {
                    a.spec.input["definition"]["id"]["name"] == "run_tests" && a.result.is_some()
                })
                .count()
                == count
            {
                return run;
            }
            assert!(
                !matches!(
                    saved.record().status,
                    RunStatus::Failed | RunStatus::BudgetExhausted
                ),
                "{:?}",
                saved.record().error
            );
        }
    }
    panic!("test count {count} not reached");
}
fn context(run: &Run) -> Value {
    run.record().state["state"]["context"].clone()
}
fn next_input(run: &Run) -> Value {
    run.activities().last().unwrap().spec.input.clone()
}
async fn finish_review(app: &App, worker: &mut Worker, task: MessageId) {
    settle_long(worker).await;
    if let Some(child) = app
        .agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .delegation
    {
        assert!(child.goal.contains("\"passed\":true"));
        assert!(child.goal.contains("\"exit_code\":0"));
        let child = app.agents.actors().unwrap().task(child.id).await.unwrap();
        if let Some(wait) = child.wait {
            app.engine
                .signal(child.run_id.unwrap(), wait.id, json!({"answer":"reviewed"}))
                .await
                .unwrap();
        }
        settle_long(worker).await;
    }
}
async fn settle_long(worker: &mut Worker) {
    for _ in 0..300 {
        if !worker.step().await.unwrap() && !worker.has_active_runs().await.unwrap() {
            return;
        }
    }
    panic!("long worker did not settle");
}
async fn verify_completed(app: &App, task: MessageId, retried_model: bool) {
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(view.status, AgentTaskStatus::Succeeded, "{:?}", view);
    let saved = app.engine.status(view.run_id.unwrap()).await.unwrap();
    let context = context(&saved);
    assert!(context["observations"].as_array().unwrap().len() <= 2);
    assert!(
        context["selection"]["omitted_observations"]
            .as_u64()
            .unwrap()
            > 20
    );
    let tests: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
        .collect();
    assert_eq!(tests.len(), 2);
    assert_eq!(tests[0].result.as_ref().unwrap()["ok"], false);
    assert_eq!(tests[1].result.as_ref().unwrap()["ok"], true);
    let workspace = Path::new(tests[0].spec.input["workspace"].as_str().unwrap());
    assert_eq!(execution_count(workspace), 2);
    assert_eq!(context["test_status"]["activity_id"], json!(tests[1].id));
    assert!(
        saved
            .activities()
            .iter()
            .find(|a| a.spec.input["definition"]["id"]["name"] == "read_file")
            .unwrap()
            .result
            .as_ref()
            .unwrap()["text"]
            .as_str()
            .unwrap()
            .contains("bounded evidence")
    );
    for model in saved
        .activities()
        .iter()
        .filter(|a| a.spec.handler.name == "agent.model")
    {
        let request: ModelRequest = serde_json::from_value(model.spec.input.clone()).unwrap();
        let selection = request.context.as_ref().unwrap();
        assert_eq!(request.context_input_bytes(), selection.input_bytes);
        assert!(selection.input_bytes <= u64::from(LONG_CONTEXT_BYTES));
        assert!(selection.included_observations <= 2);
    }
    for tests in tests {
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == tests.id)
                .count(),
            1
        );
    }
    assert_eq!(
        saved.record().usage.uncertain_calls,
        u32::from(retried_model)
    );
}

#[tokio::test]
async fn long_native_repairs_keep_bounded_context_across_failed_tests_model_retry_and_review() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("long.db");
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), Some(Arc::new(LongRepair))).await;
        let src = long_source(dir.path());
        let mut cfg = long_config();
        if policy == "planner" {
            cfg["goal_budget"] = json!({"max_model_calls":30,"max_tokens":1000000});
        }
        let actor = create(&app, policy, Some(src.clone()), cfg, 64).await;
        let task=app.agents.actors().unwrap().send(AgentMessage{id:MessageId::new(),sender:None,recipient:actor,correlation_id:None,kind:"task".into(),
            payload:json!({"goal":"Long repair","context":{"max_input_bytes":1,"max_observations":1}})}).await.unwrap();
        let mut worker = app.engine.worker().await.unwrap();
        let run = until_tests(&app, &mut worker, task, 1).await;
        let before = app.engine.status(run).await.unwrap();
        let selected = context(&before);
        let input = next_input(&before);
        let usage = json!(before.record().usage);
        assert_eq!(selected["test_status"]["passed"], false);
        let interrupted = worker.admit().await.unwrap().unwrap();
        assert_eq!(interrupted.input, input);
        if policy == "planner" {
            app.agents.actors().unwrap().pause_goal(task).await.unwrap();
        }
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), Some(Arc::new(LongRepair))).await;
        let mut worker = app.engine.worker().await.unwrap();
        let restored = app.engine.status(run).await.unwrap();
        assert_eq!(context(&restored), selected);
        assert_eq!(next_input(&restored), input);
        assert_eq!(
            restored.record().usage.admitted_model_calls,
            before.record().usage.admitted_model_calls + 1
        );
        assert!(worker.admit().await.unwrap().is_none());
        app.clock.0.store(1500, Ordering::SeqCst);
        if policy == "planner" {
            assert!(worker.admit().await.unwrap().is_none());
            app.agents
                .actors()
                .unwrap()
                .resume_goal(task)
                .await
                .unwrap();
            assert_eq!(context(&app.engine.status(run).await.unwrap()), selected);
        }
        let retry = worker.admit().await.unwrap().unwrap();
        assert_eq!(retry.input, input);
        assert_eq!(retry.activity_id, interrupted.activity_id);
        assert_ne!(retry.attempt_id, interrupted.attempt_id);
        worker
            .complete(retry.clone(), worker.execute(retry).await.unwrap())
            .await
            .unwrap();
        let run = until_tests(&app, &mut worker, task, 2).await;
        let before = app.engine.status(run).await.unwrap();
        let input = next_input(&before);
        let selected = context(&before);
        assert_eq!(selected["test_status"]["passed"], true);
        assert_ne!(json!(before.record().usage), usage);
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), Some(Arc::new(LongRepair))).await;
        let mut worker = app.engine.worker().await.unwrap();
        let restored = app.engine.status(run).await.unwrap();
        assert_eq!(next_input(&restored), input);
        assert_eq!(context(&restored), selected);
        app.clock.0.store(1500, Ordering::SeqCst);
        finish_review(&app, &mut worker, task).await;
        verify_completed(&app, task, true).await;
        if policy == "planner" {
            assert_eq!(
                app.agents
                    .actors()
                    .unwrap()
                    .inspect_task(task, Default::default())
                    .await
                    .unwrap()
                    .summary
                    .visible_usage
                    .admitted_model_calls,
                30
            );
        }
        assert_eq!(
            fs::read_to_string(src.join("README.md")).unwrap(),
            "Original project evidence.\n"
        );
    }
}

#[tokio::test]
async fn native_context_selection_rolls_back_with_the_next_request_and_pins_defaults_before_copy() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let app = open(&db, Arc::new(Calls::default()), None).await;
    let src = long_source(dir.path());
    for invalid in [
        json!({"version":2}),
        json!({"max_input_bytes":0}),
        json!({"max_observations":0}),
        json!({"typo":true}),
    ] {
        let mut cfg = long_config();
        cfg["context"] = invalid;
        assert!(
            app.agents
                .create_registered_actor(
                    DefinitionId::new("tool", 1),
                    Some(src.clone()),
                    cfg,
                    10,
                    Default::default()
                )
                .await
                .is_err()
        );
    }
    assert!(!dir.path().join("workspaces").exists());
    let mut cfg = config();
    cfg["max_input_bytes"] = json!(9000);
    cfg["tools"] = json!([{"name":"read_file","version":2}]);
    cfg["script"] = json!([{"kind":"call_tool","tool":"read_file","arguments":{"path":"EVIDENCE.md"}},
        {"kind":"call_tool","tool":"read_file","arguments":{"path":"EVIDENCE.md"}},
        {"kind":"finish","summary":"done"}]);
    let actor = create(&app, "tool", Some(src), cfg, 10).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    native_once(&mut worker).await;
    model_once(&mut worker).await;
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    let before = json!(app.engine.status(execution.run_id).await.unwrap());
    let history = json!(app.engine.history(execution.run_id).await.unwrap());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_context_continuation BEFORE INSERT ON activities WHEN json_extract(NEW.payload,'$.spec.handler.name')='agent.model' BEGIN SELECT RAISE(ABORT,'context rollback'); END").execute(&pool).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(
        json!(app.engine.status(execution.run_id).await.unwrap()),
        before
    );
    assert_eq!(
        json!(app.engine.history(execution.run_id).await.unwrap()),
        history
    );
    sqlx::query("DROP TRIGGER reject_context_continuation")
        .execute(&pool)
        .await
        .unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    worker.complete(execution.clone(), outcome).await.unwrap();
    settle(&mut worker).await;
    let saved = app.engine.status(execution.run_id).await.unwrap();
    assert_eq!(saved.record().status, RunStatus::Succeeded);
    let context = context(&saved);
    assert_eq!(context["total_observations"], 2);
    assert_eq!(context["selection"]["policy"]["max_input_bytes"], 9000);
    assert_eq!(context["observations"].as_array().unwrap().len(), 1);
    assert_eq!(context["selection"]["omitted_observations"], 1);
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status,
        AgentTaskStatus::Succeeded
    );
}

#[tokio::test]
#[ignore = "invoked and killed by the native context recovery parent"]
async fn context_crash_fixture() {
    let root = PathBuf::from(std::env::var("ACT0R_NATIVE_CONTEXT_CRASH_DIR").unwrap());
    let stage = std::env::var("ACT0R_NATIVE_CONTEXT_CRASH_STAGE").unwrap();
    let app = open(
        &root.join("crash.db"),
        Arc::new(Calls::default()),
        Some(Arc::new(LongRepair)),
    )
    .await;
    let mut cfg = long_config();
    cfg["goal_budget"] = json!({"max_model_calls":30,"max_tokens":1000000});
    let actor = create(&app, "planner", Some(long_source(&root)), cfg, 64).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    let run = until_tests(&app, &mut worker, task, 1).await;
    let mut execution = None;
    if stage == "model_admitted" {
        execution = Some(worker.admit().await.unwrap().unwrap());
    }
    if matches!(stage.as_str(), "context_saved" | "human_wait") {
        until_tests(&app, &mut worker, task, 2).await;
        if stage == "context_saved" {
            for _ in 0..50 {
                let saved = app.engine.status(run).await.unwrap();
                if next_input(&saved)["turn"] == REVIEW_TURN {
                    break;
                }
                worker.step().await.unwrap();
            }
        } else {
            settle_long(&mut worker).await;
        }
    }
    let saved = app.engine.status(run).await.unwrap();
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    let child_wait = if stage == "human_wait" {
        let child = app
            .agents
            .actors()
            .unwrap()
            .task(view.delegation.as_ref().unwrap().id)
            .await
            .unwrap();
        Some((child.task.id, child.wait.unwrap()))
    } else {
        None
    };
    fs::write(root.join("ready.json"),serde_json::to_vec(&json!({"task":task,"run":run,"context":context(&saved),"input":next_input(&saved),"execution":execution,
        "view":view,"child_wait":child_wait})).unwrap()).unwrap();
    let _retained = (app, worker);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn killed_native_workers_reuse_selected_context_and_saved_test_evidence() {
    for stage in [
        "test_saved",
        "model_admitted",
        "context_saved",
        "human_wait",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "native_tests::context::context_crash_fixture",
            ])
            .env("ACT0R_NATIVE_CONTEXT_CRASH_DIR", root)
            .env("ACT0R_NATIVE_CONTEXT_CRASH_STAGE", stage)
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
            if start.elapsed() > Duration::from_secs(30) {
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
        let app = open(
            &root.join("crash.db"),
            Arc::new(Calls::default()),
            Some(Arc::new(LongRepair)),
        )
        .await;
        let mut worker = app.engine.worker().await.unwrap();
        let saved = app.engine.status(run).await.unwrap();
        assert_eq!(context(&saved), ready["context"]);
        assert_eq!(next_input(&saved), ready["input"]);
        if stage == "human_wait" {
            let child: MessageId = serde_json::from_value(ready["child_wait"][0].clone()).unwrap();
            let restored = app.agents.actors().unwrap().task(child).await.unwrap();
            assert_eq!(json!(restored.wait.unwrap()), ready["child_wait"][1]);
        }
        if stage == "model_admitted" {
            assert!(worker.admit().await.unwrap().is_none());
            app.clock.0.store(1500, Ordering::SeqCst);
            let retry = worker.admit().await.unwrap().unwrap();
            let original: ActivityExecution =
                serde_json::from_value(ready["execution"].clone()).unwrap();
            assert_eq!(retry.input, original.input);
            assert_eq!(retry.activity_id, original.activity_id);
            assert_ne!(retry.attempt_id, original.attempt_id);
            worker
                .complete(retry.clone(), worker.execute(retry).await.unwrap())
                .await
                .unwrap();
        }
        if matches!(stage, "test_saved" | "model_admitted") {
            until_tests(&app, &mut worker, task, 2).await;
        }
        finish_review(&app, &mut worker, task).await;
        verify_completed(&app, task, stage == "model_admitted").await;
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .inspect_task(task, Default::default())
                .await
                .unwrap()
                .summary
                .visible_usage
                .admitted_model_calls,
            if stage == "model_admitted" { 30 } else { 29 }
        );
    }
}
