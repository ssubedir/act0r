use super::*;
struct RecallModel;
impl ModelClient for RecallModel {
    fn generate(&self, request: ModelRequest, activity: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            if request.target.model == "assistant" {
                return ModelRouter::new(None)
                    .unwrap()
                    .generate(request, activity)
                    .await;
            }
            let selected = request.context.as_ref().unwrap();
            assert!(selected.input_bytes <= 16000);
            assert!(selected.included_observations <= 1);
            let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
            let observations = prompt
                .get("tool_observations")
                .unwrap_or(&prompt["observations"]);
            let result = &observations
                .as_array()
                .unwrap()
                .last()
                .map(|o| o["result"].clone())
                .unwrap_or(Value::Null);
            let call = |tool: &str, arguments: Value| AgentAction::CallTool {
                tool: tool.into(),
                arguments,
            };
            let action = match request.turn {
                0 => call("run_tests", json!({})),
                1..=6 => call(
                    "echo",
                    json!({"text":format!("later observation {}",request.turn)}),
                ),
                7 => {
                    assert!(!request.prompt.contains("README evidence:"));
                    call("list_results", json!({"tool":"run_tests","limit":1}))
                }
                8 => {
                    assert_eq!(result["results"].as_array().unwrap().len(), 1);
                    assert_eq!(result["results"][0]["ok"], false);
                    assert_eq!(
                        result["results"][0]["activity_id"],
                        prompt["test_status"]["activity_id"]
                    );
                    call(
                        "read_result",
                        json!({"activity_id":result["results"][0]["activity_id"],"max_bytes":4096}),
                    )
                }
                9 => {
                    assert_eq!(result["complete"], true);
                    assert_eq!(result["next_offset"], Value::Null);
                    let original: Value =
                        serde_json::from_str(result["text"].as_str().unwrap()).unwrap();
                    assert_eq!(original["ok"], false);
                    assert_eq!(original["exit_code"], 101);
                    assert!(
                        original["stderr"]
                            .as_str()
                            .unwrap()
                            .contains("expected corrected project evidence")
                    );
                    if request.target.model == "planner" {
                        AgentAction::DelegateTask {
                            writable_paths: None,
                            worker: "reviewer".into(),
                            goal: format!(
                                "Review recalled failure evidence: {}",
                                original["stderr"]
                            ),
                        }
                    } else {
                        AgentAction::Finish {
                            summary: "Used recalled test failure without rerunning tests.".into(),
                        }
                    }
                }
                10 => AgentAction::Finish {
                    summary: "Reviewed recalled evidence.".into(),
                },
                _ => panic!("unexpected recall model turn"),
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
fn recall_config() -> Value {
    let mut cfg = tests_config();
    cfg["script"] = json!([]);
    cfg["max_model_calls"] = json!(12);
    cfg["max_tokens"] = json!(1000000);
    cfg["context"] = json!({"max_observations":1,"max_input_bytes":16000});
    cfg["tools"] = json!([{"name":"run_tests","version":1},{"name":"echo","version":1},{"name":"list_results","version":1},{"name":"read_result","version":1}]);
    cfg
}
async fn until_intent(app: &App, worker: &mut Worker, task: MessageId, tool: &str) -> RunId {
    for _ in 0..100 {
        worker.step().await.unwrap();
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        if let Some(wait) = view.wait {
            app.engine
                .signal(view.run_id.unwrap(), wait.id, json!({"approved":true}))
                .await
                .unwrap();
        }
        if let Some(id) = view.run_id {
            let saved = app.engine.status(id).await.unwrap();
            assert!(saved.record().error.is_none(), "{:?}", saved.record().error);
            let next = saved.activities().last().unwrap();
            if next.status == ActivityStatus::Queued
                && next.spec.input["definition"]["id"]["name"] == tool
            {
                return id;
            }
        }
    }
    panic!("recall intent not reached");
}
async fn finish(app: &App, worker: &mut Worker, task: MessageId) {
    settle(worker).await;
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    if let Some(child) = view.delegation {
        assert!(child.goal.contains("expected corrected project evidence"));
        let child = app.agents.actors().unwrap().task(child.id).await.unwrap();
        let wait = child.wait.unwrap();
        app.engine
            .signal(
                child.run_id.unwrap(),
                wait.id,
                json!({"answer":"reviewed recalled evidence"}),
            )
            .await
            .unwrap();
        settle(worker).await;
    }
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
async fn verify(app: &App, run: RunId) {
    let saved = app.engine.status(run).await.unwrap();
    let tests: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
        .collect();
    assert_eq!(tests.len(), 1);
    assert_eq!(tests[0].result.as_ref().unwrap()["ok"], false);
    assert_eq!(
        execution_count(Path::new(
            tests[0].spec.input["workspace"].as_str().unwrap()
        )),
        1
    );
    let read = saved
        .activities()
        .iter()
        .find(|a| a.spec.input["definition"]["id"]["name"] == "read_result")
        .unwrap();
    let recalled: Value =
        serde_json::from_str(read.result.as_ref().unwrap()["text"].as_str().unwrap()).unwrap();
    assert_eq!(&recalled, tests[0].result.as_ref().unwrap());
    assert_eq!(
        read.result.as_ref().unwrap()["source"]["activity_id"],
        json!(tests[0].id)
    );
    assert_eq!(
        saved.record().state["state"]["context"]["test_status"]["passed"],
        false
    );
}

#[tokio::test]
async fn actors_recall_evicted_test_output_after_restart_retry_and_goal_hold_without_rerunning_tests()
 {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("recall.db");
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), Some(Arc::new(RecallModel))).await;
        let mut cfg = recall_config();
        if policy == "planner" {
            cfg["goal_budget"] = json!({"max_model_calls":13,"max_tokens":1000000});
        }
        let actor = create(
            &app,
            policy,
            Some(test_source(dir.path(), "verify")),
            cfg,
            40,
        )
        .await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        let run = until_intent(&app, &mut worker, task, "list_results").await;
        let before = app.engine.status(run).await.unwrap();
        let next = before.activities().last().unwrap().spec.input.clone();
        assert!(
            !before.record().state["state"]["context"]["observations"]
                .to_string()
                .contains("README evidence:")
        );
        // Current files now disagree with the historical test result being recalled.
        let tests = before
            .activities()
            .iter()
            .find(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
            .unwrap();
        fs::write(
            Path::new(tests.spec.input["workspace"].as_str().unwrap()).join("README.md"),
            "Correct project evidence.\n",
        )
        .unwrap();
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), Some(Arc::new(RecallModel))).await;
        let mut worker = app.engine.worker().await.unwrap();
        assert_eq!(
            app.engine
                .status(run)
                .await
                .unwrap()
                .activities()
                .last()
                .unwrap()
                .spec
                .input,
            next
        );
        until_intent(&app, &mut worker, task, "read_result").await;
        let execution = worker.admit().await.unwrap().unwrap();
        let outcome = worker.execute(execution.clone()).await.unwrap();
        if policy == "planner" {
            app.agents.actors().unwrap().pause_goal(task).await.unwrap();
        }
        drop(worker);
        drop(app);
        let app = open(&db, calls, Some(Arc::new(RecallModel))).await;
        let mut worker = app.engine.worker().await.unwrap();
        assert!(worker.admit().await.unwrap().is_none());
        app.clock.0.store(1100, Ordering::SeqCst);
        if policy == "planner" {
            assert!(worker.admit().await.unwrap().is_none());
            app.agents
                .actors()
                .unwrap()
                .resume_goal(task)
                .await
                .unwrap();
        }
        let retry = worker.admit().await.unwrap().unwrap();
        assert_eq!(retry.activity_id, execution.activity_id);
        assert_eq!(retry.input, execution.input);
        assert_ne!(retry.attempt_id, execution.attempt_id);
        let repeated = worker.execute(retry.clone()).await.unwrap();
        assert_eq!(json!(repeated), json!(outcome));
        worker.complete(retry, repeated).await.unwrap();
        finish(&app, &mut worker, task).await;
        verify(&app, run).await;
    }
}

#[tokio::test]
async fn recalled_evidence_survives_model_budget_exhaustion_without_rerunning_tests() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let app = open(
            &dir.path().join("budget.db"),
            Arc::new(Calls::default()),
            Some(Arc::new(RecallModel)),
        )
        .await;
        let mut cfg = recall_config();
        cfg["max_model_calls"] = json!(9);
        let actor = create(
            &app,
            policy,
            Some(test_source(dir.path(), "verify")),
            cfg,
            40,
        )
        .await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        let run = until_intent(&app, &mut worker, task, "read_result").await;
        settle(&mut worker).await;
        assert_eq!(
            app.engine.status(run).await.unwrap().record().status,
            RunStatus::BudgetExhausted
        );
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .usage
                .unwrap()
                .admitted_model_calls,
            9
        );
        verify(&app, run).await;
    }
}

#[tokio::test]
#[ignore = "invoked and killed by the durable result recall recovery parent"]
async fn recall_crash_fixture() {
    let root = PathBuf::from(std::env::var("ACT0R_RECALL_CRASH_DIR").unwrap());
    let stage = std::env::var("ACT0R_RECALL_CRASH_STAGE").unwrap();
    let app = open(
        &root.join("crash.db"),
        Arc::new(Calls::default()),
        Some(Arc::new(RecallModel)),
    )
    .await;
    let mut cfg = recall_config();
    cfg["goal_budget"] = json!({"max_model_calls":13,"max_tokens":1000000});
    let actor = create(&app, "planner", Some(test_source(&root, "verify")), cfg, 40).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    let run = until_intent(&app, &mut worker, task, "list_results").await;
    let mut execution = None;
    let mut returned = None;
    if stage != "list_queued" {
        until_intent(&app, &mut worker, task, "read_result").await;
        if matches!(stage.as_str(), "read_admitted" | "read_returned") {
            let admitted = worker.admit().await.unwrap().unwrap();
            if stage == "read_returned" {
                returned = Some(worker.execute(admitted.clone()).await.unwrap());
            }
            execution = Some(admitted);
        } else {
            worker.step().await.unwrap();
            if stage == "child_wait" {
                settle(&mut worker).await;
            }
        }
    }
    let saved = app.engine.status(run).await.unwrap();
    let child_wait = if stage == "child_wait" {
        let parent = app.agents.actors().unwrap().task(task).await.unwrap();
        let child = app
            .agents
            .actors()
            .unwrap()
            .task(parent.delegation.unwrap().id)
            .await
            .unwrap();
        Some((child.task.id, child.wait.unwrap()))
    } else {
        None
    };
    fs::write(root.join("ready.json"), serde_json::to_vec(&json!({"task":task,"run":run,
        "input":saved.activities().last().unwrap().spec.input,"state":saved.record().state,
        "usage":saved.record().usage,"execution":execution,"returned":returned,"child_wait":child_wait})).unwrap()).unwrap();
    let _retained = (app, worker);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn killed_workers_recall_saved_results_across_intent_dispatch_return_commit_and_child_wait() {
    for stage in [
        "list_queued",
        "read_admitted",
        "read_returned",
        "read_saved",
        "child_wait",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "native_tests::recall::recall_crash_fixture",
            ])
            .env("ACT0R_RECALL_CRASH_DIR", root)
            .env("ACT0R_RECALL_CRASH_STAGE", stage)
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
            Some(Arc::new(RecallModel)),
        )
        .await;
        let mut worker = app.engine.worker().await.unwrap();
        let saved = app.engine.status(run).await.unwrap();
        assert_eq!(
            saved.activities().last().unwrap().spec.input,
            ready["input"]
        );
        assert_eq!(saved.record().state, ready["state"]);
        assert_eq!(json!(saved.record().usage), ready["usage"]);
        if stage == "child_wait" {
            let child: MessageId = serde_json::from_value(ready["child_wait"][0].clone()).unwrap();
            assert_eq!(
                json!(
                    app.agents
                        .actors()
                        .unwrap()
                        .task(child)
                        .await
                        .unwrap()
                        .wait
                        .unwrap()
                ),
                ready["child_wait"][1]
            );
        }
        if matches!(stage, "read_admitted" | "read_returned") {
            assert!(worker.admit().await.unwrap().is_none());
            app.clock.0.store(1100, Ordering::SeqCst);
            let retry = worker.admit().await.unwrap().unwrap();
            let original: ActivityExecution =
                serde_json::from_value(ready["execution"].clone()).unwrap();
            assert_eq!(retry.activity_id, original.activity_id);
            assert_ne!(retry.attempt_id, original.attempt_id);
            assert_eq!(retry.input, original.input);
            let outcome = worker.execute(retry.clone()).await.unwrap();
            if stage == "read_returned" {
                assert_eq!(json!(outcome), ready["returned"]);
            }
            worker.complete(retry, outcome).await.unwrap();
            assert_eq!(
                json!(app.engine.status(run).await.unwrap().record().usage),
                ready["usage"]
            );
        }
        finish(&app, &mut worker, task).await;
        verify(&app, run).await;
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
            13
        );
    }
}
