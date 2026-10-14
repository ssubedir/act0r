use super::*;
use act0r::{adapters::file_creation::CreateFileTool, application::ToolRegistry};
const CONTENT: &str = "Created project evidence 🙂\n";
const TARGET: &str = "src/new.rs";

fn creation_config() -> Value {
    let mut cfg = config();
    cfg["tools"] = json!([{"name":"create_file","version":1},{"name":"read_file","version":2}]);
    cfg["writable_paths"] = json!([TARGET]);
    cfg["max_model_calls"] = json!(4);
    cfg["script"] = json!([
        {"kind":"call_tool","tool":"create_file","arguments":{"path":TARGET,"content":CONTENT}},
        {"kind":"call_tool","tool":"read_file","arguments":{"path":TARGET}},
        {"kind":"finish","summary":"Created and inspected the file."}]);
    cfg
}
fn creation_execution(
    definition: &ToolDefinition,
    workspace: &str,
    permissions: &[String],
    arguments: Value,
) -> ActivityExecution {
    let Command::Schedule(spec) = definition
        .schedule_call_with_permissions(&arguments, Some(workspace), permissions)
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
async fn approve_creation(app: &App, task: MessageId, approved: bool) -> RunId {
    let (run, wait) = approval(app, task).await;
    assert_eq!(wait.key, "native_tool_approval");
    assert_eq!(
        wait.payload["tool"],
        json!({"name":"create_file","version":1})
    );
    assert_eq!(
        wait.payload["arguments"],
        json!({"path":TARGET,"content":CONTENT})
    );
    assert_eq!(wait.payload["writable_paths"], json!([TARGET]));
    app.engine
        .signal(run, wait.id, json!({"approved":approved}))
        .await
        .unwrap();
    app.engine
        .signal(run, wait.id, json!({"approved":approved}))
        .await
        .unwrap();
    run
}
async fn direct_fixture(root: &Path) -> (String, Arc<LocalTools>, ToolRegistry, ToolDefinition) {
    let managed = root.join("workspaces");
    let workspace = LocalWorkspaces::new(managed.clone())
        .prepare(source(root))
        .await
        .unwrap();
    let local = Arc::new(LocalTools::new(managed));
    let definition = CreateFileTool::new(local.clone()).definition();
    let registry = ToolRegistry::new(vec![Arc::new(CreateFileTool::new(local.clone()))]).unwrap();
    (workspace, local, registry, definition)
}

#[tokio::test]
async fn creation_refuses_existing_private_unpermitted_and_invalid_targets_without_journaling() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, local, registry, definition) = direct_fixture(dir.path()).await;
    let root = Path::new(&workspace);
    let permissions: Vec<String> = [
        "README.md",
        "README.md/",
        "src/",
        "src",
        "new.txt",
        "empty.txt",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    for (path, code) in [
        ("README.md", "file_exists"),
        ("src", "file_exists"),
        ("other.txt", "path_not_writable"),
        ("src/missing/new.rs", "parent_not_found"),
        ("README.md/new.rs", "not_a_directory"),
        ("../outside.txt", "path_refused"),
        ("/outside.txt", "path_refused"),
        ("src\\new.rs", "path_refused"),
        ("src//new.rs", "path_refused"),
        ("src/./new.rs", "path_refused"),
        ("src/new. ", "path_refused"),
        (".env", "path_refused"),
        (".act0r-tools/managed", "path_refused"),
        ("src/NUL.rs", "path_refused"),
        ("src/COM1.txt", "path_refused"),
        ("src/CONIN$", "path_refused"),
    ] {
        let execution = creation_execution(
            &definition,
            &workspace,
            &permissions,
            json!({"path":path,"content":"new contents"}),
        );
        let output = registry.execute(execution).await.unwrap();
        assert_eq!(output.result().unwrap()["error"]["code"], code, "{path}");
    }
    assert!(!root.join("src/missing").exists());
    assert_eq!(fs::read_dir(root.join(".act0r-tools")).unwrap().count(), 2); // marker and ownership lock
    assert_eq!(
        fs::read_to_string(root.join("README.md")).unwrap(),
        "Original project evidence.\n"
    );
    for (path, content) in [("new.txt", CONTENT), ("empty.txt", "")] {
        let execution = creation_execution(
            &definition,
            &workspace,
            &permissions,
            json!({"path":path,"content":content}),
        );
        let result = registry.execute(execution.clone()).await.unwrap();
        assert_eq!(
            result.result().unwrap(),
            &json!({"ok":true,"path":path,"bytes":content.len(),"already_applied":false})
        );
        assert_eq!(fs::read(root.join(path)).unwrap(), content.as_bytes());
        let fresh = creation_execution(
            &definition,
            &workspace,
            &permissions,
            json!({"path":path,"content":content}),
        );
        assert_eq!(
            registry.execute(fresh).await.unwrap().result().unwrap()["error"]["code"],
            "file_exists"
        );
    }
    assert!(
        definition
            .schedule_call_with_permissions(
                &json!({"path":TARGET,"content":"x".repeat(65536)}),
                Some(&workspace),
                &permissions
            )
            .is_err()
    );
    assert!(
        matches!(local.create_file(workspace,permissions,WorkspaceCreate {path:TARGET.into(),content:"x".repeat(1048577)},ActivityId::new()).await,Err(Error::ToolRejected(error)) if error.code=="file_limit")
    );
}

#[tokio::test]
async fn creation_journal_recognizes_saved_contents_and_preserves_conflicts_and_tampering() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, _, registry, definition) = direct_fixture(dir.path()).await;
    let root = Path::new(&workspace);
    let mut execution = creation_execution(
        &definition,
        &workspace,
        &[TARGET.into()],
        json!({"path":TARGET,"content":CONTENT}),
    );
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(".act0r-tools/ownership.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    assert!(
        matches!(registry.execute(execution.clone()).await.unwrap(),Outcome::Failure(f) if f.code=="workspace_busy" && f.retryable)
    );
    drop(lock);
    assert_eq!(
        registry
            .execute(execution.clone())
            .await
            .unwrap()
            .result()
            .unwrap()["already_applied"],
        false
    );
    let modified = fs::metadata(root.join(TARGET)).unwrap().modified().unwrap();
    execution.attempt_id = AttemptId::new();
    assert_eq!(
        registry
            .execute(execution.clone())
            .await
            .unwrap()
            .result()
            .unwrap()["already_applied"],
        true
    );
    assert_eq!(
        fs::metadata(root.join(TARGET)).unwrap().modified().unwrap(),
        modified
    );
    let journal = root.join(format!(".act0r-tools/{}.json", execution.activity_id));
    let original = fs::read(&journal).unwrap();
    for change in ["content", "content_hash", "input_hash", "kind", "version"] {
        let mut value: Value = serde_json::from_slice(&original).unwrap();
        value[change] = if change == "version" {
            json!(2)
        } else {
            json!("tampered")
        };
        fs::write(&journal, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            registry.execute(execution.clone()).await.is_err(),
            "{change}"
        );
    }
    fs::write(&journal, original).unwrap();
    execution.input["arguments"]["content"] = json!("changed request");
    assert!(registry.execute(execution.clone()).await.is_err());
    execution.input["arguments"]["content"] = json!(CONTENT);
    execution.input["writable_paths"] = json!([TARGET, "src/"]);
    assert!(registry.execute(execution.clone()).await.is_err());
    execution.input["writable_paths"] = json!([TARGET]);
    fs::write(root.join(TARGET), "conflicting contents").unwrap();
    assert!(registry.execute(execution.clone()).await.is_err());
    assert_eq!(
        fs::read_to_string(root.join(TARGET)).unwrap(),
        "conflicting contents"
    );
    fs::remove_file(root.join(TARGET)).unwrap();
    assert_eq!(
        registry.execute(execution).await.unwrap().result().unwrap()["already_applied"],
        false
    );
    assert_eq!(fs::read_to_string(root.join(TARGET)).unwrap(), CONTENT);
}

#[tokio::test]
async fn creation_refuses_linked_parents_leaf_journals_and_replaced_roots() {
    for mode in ["parent", "leaf", "journal", "root"] {
        let dir = tempfile::tempdir().unwrap();
        let (workspace, _, registry, definition) = direct_fixture(dir.path()).await;
        let root = Path::new(&workspace);
        let foreign = dir.path().join("foreign");
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join("sentinel"), "keep").unwrap();
        let path = if mode == "parent" {
            "linked/new.rs"
        } else {
            TARGET
        };
        let execution = creation_execution(
            &definition,
            &workspace,
            &["linked/".into(), TARGET.into()],
            json!({"path":path,"content":CONTENT}),
        );
        match mode {
            "parent" => directory_link(&foreign, &root.join("linked")),
            "leaf" => directory_link(&foreign, &root.join(TARGET)),
            "journal" => directory_link(
                &foreign,
                &root.join(format!(".act0r-tools/{}.json", execution.activity_id)),
            ),
            "root" => {
                fs::rename(root, root.with_extension("original")).unwrap();
                directory_link(&foreign, root);
            }
            _ => unreachable!(),
        }
        let outcome = registry.execute(execution).await;
        if matches!(mode, "parent" | "leaf") {
            assert_eq!(
                outcome.unwrap().result().unwrap()["error"]["code"],
                "path_refused"
            );
        } else {
            assert!(outcome.is_err());
        }
        assert_eq!(fs::read_dir(&foreign).unwrap().count(), 1);
        assert_eq!(
            fs::read_to_string(foreign.join("sentinel")).unwrap(),
            "keep"
        );
    }
}

struct CreationReview;
impl ModelClient for CreationReview {
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
                .unwrap()
                .last()
                .map(|v| v["result"].clone())
                .unwrap_or(Value::Null);
            let action = match request.turn {
                0 => AgentAction::CallTool {
                    tool: "create_file".into(),
                    arguments: json!({"path":TARGET,"content":CONTENT}),
                },
                1 => {
                    assert_eq!(result["ok"], true);
                    AgentAction::CallTool {
                        tool: "read_file".into(),
                        arguments: json!({"path":TARGET}),
                    }
                }
                2 => {
                    assert_eq!(result["text"], CONTENT);
                    if request.target.model == "planner" {
                        AgentAction::DelegateTask {
                            writable_paths: None,
                            worker: "reviewer".into(),
                            goal: format!("Review {}", result["text"].as_str().unwrap()),
                        }
                    } else {
                        AgentAction::Finish {
                            summary: "Created and inspected the file.".into(),
                        }
                    }
                }
                3 => AgentAction::Finish {
                    summary: "Reviewed the created file.".into(),
                },
                _ => panic!("unexpected creation turn"),
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
async fn finish_creation(app: &App, worker: &mut Worker, task: MessageId) {
    settle(worker).await;
    if let Some(child) = app
        .agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .delegation
    {
        assert_eq!(child.goal, format!("Review {CONTENT}"));
        let child = app.agents.actors().unwrap().task(child.id).await.unwrap();
        app.engine
            .signal(
                child.run_id.unwrap(),
                child.wait.unwrap().id,
                json!({"answer":"reviewed"}),
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

#[tokio::test]
async fn creation_approvals_uncertain_effects_and_saved_reads_survive_restarts_for_both_policies() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("restart.db");
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), Some(Arc::new(CreationReview))).await;
        let actor = create(
            &app,
            policy,
            Some(source(dir.path())),
            creation_config(),
            16,
        )
        .await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let (run, wait) = approval(&app, task).await;
        let workspace = PathBuf::from(wait.payload["workspace"].as_str().unwrap());
        assert!(!workspace.join(TARGET).exists());
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), Some(Arc::new(CreationReview))).await;
        let mut worker = app.engine.worker().await.unwrap();
        assert_eq!(json!(approval(&app, task).await.1), json!(wait));
        approve_creation(&app, task, true).await;
        let execution = worker.admit().await.unwrap().unwrap();
        assert_eq!(execution.handler, DefinitionId::new("native.tool", 4));
        let first = worker.execute(execution.clone()).await.unwrap();
        assert_eq!(first.result().unwrap()["already_applied"], false);
        let modified = fs::metadata(workspace.join(TARGET))
            .unwrap()
            .modified()
            .unwrap();
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), Some(Arc::new(CreationReview))).await;
        let mut worker = app.engine.worker().await.unwrap();
        assert_eq!(
            app.engine.status(run).await.unwrap().record().status,
            RunStatus::Blocked
        );
        assert!(worker.admit().await.unwrap().is_none());
        app.engine.reconcile(run,ReconciliationRequest {activity_id:execution.activity_id,attempt_id:execution.attempt_id,
            decision:ReconciliationDecision::Retry {},evidence:"Stopped worker; inspected the exact created file and authorize journal verification.".into()}).await.unwrap();
        let retry = worker.admit().await.unwrap().unwrap();
        assert_eq!(retry.activity_id, execution.activity_id);
        assert_eq!(retry.input, execution.input);
        assert_ne!(retry.attempt_id, execution.attempt_id);
        let output = worker.execute(retry.clone()).await.unwrap();
        assert_eq!(output.result().unwrap()["already_applied"], true);
        assert_eq!(
            fs::metadata(workspace.join(TARGET))
                .unwrap()
                .modified()
                .unwrap(),
            modified
        );
        worker
            .complete(retry.clone(), output.clone())
            .await
            .unwrap();
        worker.complete(retry, output).await.unwrap();
        model_once(&mut worker).await;
        native_once(&mut worker).await;
        let saved = app.engine.status(run).await.unwrap();
        let next = saved.activities().last().unwrap().spec.input.clone();
        let usage = json!(saved.record().usage);
        fs::write(workspace.join(TARGET), "changed after saved read").unwrap();
        drop(worker);
        drop(app);
        let app = open(&db, calls, Some(Arc::new(CreationReview))).await;
        let mut worker = app.engine.worker().await.unwrap();
        let restored = app.engine.status(run).await.unwrap();
        assert_eq!(restored.activities().last().unwrap().spec.input, next);
        assert_eq!(json!(restored.record().usage), usage);
        finish_creation(&app, &mut worker, task).await;
        assert_eq!(
            fs::read_to_string(workspace.join(TARGET)).unwrap(),
            "changed after saved read"
        );
        assert!(!dir.path().join("source").join(TARGET).exists());
    }
}

#[tokio::test]
async fn creation_denial_goal_holds_and_cancellation_preserve_admission_and_files() {
    for policy in ["tool", "planner"] {
        for mode in ["deny", "held", "cancel"] {
            let dir = tempfile::tempdir().unwrap();
            let app = open(
                &dir.path().join("controls.db"),
                Arc::new(Calls::default()),
                None,
            )
            .await;
            let mut cfg = creation_config();
            cfg["script"] = json!([{ "kind":"call_tool","tool":"create_file","arguments":{"path":TARGET,"content":CONTENT}}, {"kind":"finish","summary":"Handled creation."}]);
            let actor = create(&app, policy, Some(source(dir.path())), cfg, 8).await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            model_once(&mut worker).await;
            let (_, wait) = approval(&app, task).await;
            let path = Path::new(wait.payload["workspace"].as_str().unwrap()).join(TARGET);
            if mode == "held" {
                app.agents.actors().unwrap().pause_goal(task).await.unwrap();
            }
            let run = approve_creation(&app, task, mode != "deny").await;
            if mode == "held" {
                assert!(worker.admit().await.unwrap().is_none());
                assert!(!path.exists());
                app.agents
                    .actors()
                    .unwrap()
                    .resume_goal(task)
                    .await
                    .unwrap();
            }
            if mode == "cancel" {
                let execution = worker.admit().await.unwrap().unwrap();
                let outcome = worker.execute(execution.clone()).await.unwrap();
                app.agents
                    .actors()
                    .unwrap()
                    .cancel_goal(task)
                    .await
                    .unwrap();
                worker.complete(execution, outcome).await.unwrap();
            }
            settle(&mut worker).await;
            assert_eq!(path.exists(), mode != "deny");
            let saved = app.engine.status(run).await.unwrap();
            assert_eq!(
                saved.record().status,
                if mode == "cancel" {
                    RunStatus::Cancelled
                } else {
                    RunStatus::Succeeded
                }
            );
            assert!(!dir.path().join("source").join(TARGET).exists());
        }
    }
}

#[tokio::test]
async fn successful_creation_marks_prior_test_evidence_stale_until_tests_run_again() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let src = test_source(dir.path(), "verify");
        fs::write(src.join("README.md"), "Correct project evidence.\n").unwrap();
        let app = open(
            &dir.path().join("stale.db"),
            Arc::new(Calls::default()),
            None,
        )
        .await;
        let mut cfg = creation_config();
        cfg["tools"] = json!([{"name":"run_tests","version":1},{"name":"create_file","version":1}]);
        cfg["test_command"] = json!(test_command(10000));
        cfg["script"] = json!([
            {"kind":"call_tool","tool":"run_tests","arguments":{}},
            {"kind":"call_tool","tool":"create_file","arguments":{"path":TARGET,"content":CONTENT}},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},
            {"kind":"finish","summary":"Created and retested."}]);
        let actor = create(&app, policy, Some(src), cfg, 12).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let run = approve_tests(&app, task).await;
        native_once(&mut worker).await;
        assert_eq!(
            app.engine.status(run).await.unwrap().record().state["state"]["context"]["test_status"]
                ["stale"],
            false
        );
        model_once(&mut worker).await;
        approve_creation(&app, task, true).await;
        native_once(&mut worker).await;
        assert_eq!(
            app.engine.status(run).await.unwrap().record().state["state"]["context"]["test_status"]
                ["stale"],
            true
        );
        model_once(&mut worker).await;
        approve_tests(&app, task).await;
        native_once(&mut worker).await;
        let saved = app.engine.status(run).await.unwrap();
        assert_eq!(
            saved.record().state["state"]["context"]["test_status"]["stale"],
            false
        );
        assert_eq!(
            saved.record().state["state"]["context"]["test_status"]["passed"],
            true
        );
        settle(&mut worker).await;
    }
}

#[tokio::test]
async fn creation_completion_rolls_back_with_continuation_and_duplicate_delivery_keeps_saved_output()
 {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("atomic.db");
    let calls = Arc::new(Calls::default());
    let app = open(&db, calls.clone(), None).await;
    let actor = create(
        &app,
        "tool",
        Some(source(dir.path())),
        creation_config(),
        10,
    )
    .await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let run = approve_creation(&app, task, true).await;
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    let before = json!(app.engine.status(run).await.unwrap());
    let history = json!(app.engine.history(run).await.unwrap());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_creation_continuation BEFORE INSERT ON activities WHEN json_extract(NEW.payload,'$.spec.handler.name')='agent.model' BEGIN SELECT RAISE(ABORT,'creation rollback'); END").execute(&pool).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(json!(app.engine.status(run).await.unwrap()), before);
    assert_eq!(json!(app.engine.history(run).await.unwrap()), history);
    assert_eq!(
        fs::read_to_string(Path::new(execution.input["workspace"].as_str().unwrap()).join(TARGET))
            .unwrap(),
        CONTENT
    );
    sqlx::query("DROP TRIGGER reject_creation_continuation")
        .execute(&pool)
        .await
        .unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    let history = json!(app.engine.history(run).await.unwrap());
    worker.complete(execution, outcome).await.unwrap();
    assert_eq!(json!(app.engine.history(run).await.unwrap()), history);
    settle(&mut worker).await;
    assert_eq!(
        calls
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| matches!(&r.action,AgentAction::CallTool {tool,..} if tool=="create_file"))
            .count(),
        1
    );
}

#[tokio::test]
async fn created_files_and_saved_results_survive_model_attempt_and_context_exhaustion() {
    for policy in ["tool", "planner"] {
        for mode in ["model", "attempt", "context"] {
            let dir = tempfile::tempdir().unwrap();
            let app = open(
                &dir.path().join("limits.db"),
                Arc::new(Calls::default()),
                None,
            )
            .await;
            let src = source(dir.path());
            let mut cfg = creation_config();
            cfg["script"] = json!([{ "kind":"call_tool","tool":"create_file","arguments":{"path":TARGET,"content":CONTENT}}, {"kind":"finish","summary":"Created."}]);
            if mode == "model" {
                cfg["max_model_calls"] = json!(1);
            }
            if mode == "context" {
                let baseline = create(&app, policy, Some(src.clone()), cfg.clone(), 8).await;
                send(&app, baseline).await;
                let mut worker = app.engine.worker().await.unwrap();
                let model = model_once(&mut worker).await;
                let request: ModelRequest = serde_json::from_value(model.input).unwrap();
                cfg["max_input_bytes"] = json!(request.context_input_bytes());
                cfg["context"] =
                    json!({"max_input_bytes":request.context_input_bytes(),"max_observations":16});
            }
            let actor = create(
                &app,
                policy,
                Some(src),
                cfg,
                if mode == "attempt" { 2 } else { 8 },
            )
            .await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            model_once(&mut worker).await;
            let run = approve_creation(&app, task, true).await;
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
            let created = saved
                .activities()
                .iter()
                .find(|a| a.spec.input["definition"]["id"]["name"] == "create_file")
                .unwrap();
            assert_eq!(created.result.as_ref().unwrap()["ok"], true);
            assert_eq!(
                fs::read_to_string(
                    Path::new(created.spec.input["workspace"].as_str().unwrap()).join(TARGET)
                )
                .unwrap(),
                CONTENT
            );
        }
    }
}

#[tokio::test]
#[ignore = "invoked and killed by the durable file creation recovery parent"]
async fn creation_crash_fixture() {
    let root = PathBuf::from(std::env::var("ACT0R_CREATION_CRASH_DIR").unwrap());
    let stage = std::env::var("ACT0R_CREATION_CRASH_STAGE").unwrap();
    let calls = Arc::new(Calls {
        requests: Mutex::new(vec![]),
        ledger: Some(root.join("calls.log")),
    });
    let app = open(
        &root.join("crash.db"),
        calls,
        Some(Arc::new(CreationReview)),
    )
    .await;
    let mut cfg = creation_config();
    cfg["goal_budget"] = json!({"max_model_calls":6,"max_tokens":300000});
    let actor = create(&app, "planner", Some(source(&root)), cfg, 16).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let (run, wait) = approval(&app, task).await;
    let mut execution = None;
    if stage != "approval" {
        approve_creation(&app, task, true).await;
        if stage != "queued" {
            let admitted = worker.admit().await.unwrap().unwrap();
            if stage != "admitted" {
                let output = worker.execute(admitted.clone()).await.unwrap();
                if stage != "applied" {
                    worker.complete(admitted.clone(), output).await.unwrap();
                }
            }
            execution = Some(admitted);
        }
        if matches!(stage.as_str(), "read_saved" | "child_wait") {
            model_once(&mut worker).await;
            native_once(&mut worker).await;
        }
        if stage == "child_wait" {
            settle(&mut worker).await;
        }
    }
    let saved = app.engine.status(run).await.unwrap();
    let child_wait = if stage == "child_wait" {
        let child = app
            .agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .delegation
            .unwrap();
        let child = app.agents.actors().unwrap().task(child.id).await.unwrap();
        Some((child.task.id, child.wait.unwrap()))
    } else {
        None
    };
    fs::write(root.join("ready.json"),serde_json::to_vec(&json!({"task":task,"actor":actor,"run":run,"wait":wait,
        "execution":execution,"state":saved.record().state,"next":saved.activities().last().unwrap().spec.input,
        "usage":saved.record().usage,"child_wait":child_wait})).unwrap()).unwrap();
    let _retained = (app, worker);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn killed_creation_workers_preserve_approvals_intents_effects_saved_results_and_child_waits()
{
    for stage in [
        "approval",
        "queued",
        "admitted",
        "applied",
        "saved",
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
                "native_tests::creation::creation_crash_fixture",
            ])
            .env("ACT0R_CREATION_CRASH_DIR", root)
            .env("ACT0R_CREATION_CRASH_STAGE", stage)
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
        let calls = Arc::new(Calls {
            requests: Mutex::new(vec![]),
            ledger: Some(root.join("calls.log")),
        });
        let app = open(
            &root.join("crash.db"),
            calls,
            Some(Arc::new(CreationReview)),
        )
        .await;
        let workspace = PathBuf::from(ready["wait"]["payload"]["workspace"].as_str().unwrap());
        let mut worker = app.engine.worker().await.unwrap();
        let saved = app.engine.status(run).await.unwrap();
        assert_eq!(saved.record().state, ready["state"]);
        assert_eq!(saved.activities().last().unwrap().spec.input, ready["next"]);
        assert_eq!(json!(saved.record().usage), ready["usage"]);
        if stage == "approval" {
            assert_eq!(json!(approval(&app, task).await.1), ready["wait"]);
            approve_creation(&app, task, true).await;
        }
        if matches!(stage, "admitted" | "applied") {
            assert_eq!(saved.record().status, RunStatus::Blocked);
            assert!(worker.admit().await.unwrap().is_none());
            let original: ActivityExecution =
                serde_json::from_value(ready["execution"].clone()).unwrap();
            let modified = fs::metadata(workspace.join(TARGET))
                .ok()
                .map(|m| m.modified().unwrap());
            assert_eq!(modified.is_some(), stage == "applied");
            app.engine.reconcile(run,ReconciliationRequest {activity_id:original.activity_id,attempt_id:original.attempt_id,
                decision:ReconciliationDecision::Retry {},evidence:"Killed worker exited; inspected the target and authorize creation journal verification.".into()}).await.unwrap();
            let retry = worker.admit().await.unwrap().unwrap();
            assert_eq!(retry.activity_id, original.activity_id);
            assert_eq!(retry.input, original.input);
            assert_ne!(retry.attempt_id, original.attempt_id);
            let output = worker.execute(retry.clone()).await.unwrap();
            assert_eq!(
                output.result().unwrap()["already_applied"],
                stage == "applied"
            );
            if let Some(modified) = modified {
                assert_eq!(
                    fs::metadata(workspace.join(TARGET))
                        .unwrap()
                        .modified()
                        .unwrap(),
                    modified
                );
            }
            worker.complete(retry, output).await.unwrap();
        }
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
        if matches!(stage, "read_saved" | "child_wait") {
            fs::write(workspace.join(TARGET), "changed after saved read").unwrap();
        }
        finish_creation(&app, &mut worker, task).await;
        let saved = app.engine.status(run).await.unwrap();
        let creations: Vec<_> = saved
            .activities()
            .iter()
            .filter(|a| a.spec.input["definition"]["id"]["name"] == "create_file")
            .collect();
        assert_eq!(creations.len(), 1);
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == creations[0].id)
                .count(),
            if matches!(stage, "admitted" | "applied") {
                2
            } else {
                1
            }
        );
        assert_eq!(
            fs::read_to_string(root.join("calls.log"))
                .unwrap()
                .lines()
                .filter(|l| l.starts_with("create "))
                .count(),
            if stage == "applied" { 2 } else { 1 }
        );
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
            6
        );
        assert!(!root.join("source").join(TARGET).exists());
    }
}
