use super::*;
use act0r::{adapters::workspace_search::SearchTextTool, application::ToolRegistry};

async fn fixture(root: &Path) -> (String, Arc<LocalTools>, ToolRegistry, ToolDefinition) {
    let managed = root.join("workspaces");
    let workspace = LocalWorkspaces::new(managed.clone())
        .prepare(source(root))
        .await
        .unwrap();
    let local = Arc::new(LocalTools::new(managed));
    let tool = Arc::new(SearchTextTool::new(local.clone()));
    let definition = tool.definition();
    (
        workspace,
        local,
        ToolRegistry::new(vec![tool]).unwrap(),
        definition,
    )
}
fn execution(definition: &ToolDefinition, workspace: &str, args: Value) -> ActivityExecution {
    let Command::Schedule(spec) = definition.schedule_call_in(&args, Some(workspace)).unwrap()
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
async fn search(
    registry: &ToolRegistry,
    definition: &ToolDefinition,
    workspace: &str,
    args: Value,
) -> Value {
    registry
        .execute(execution(definition, workspace, args))
        .await
        .unwrap()
        .result()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn search_is_literal_case_sensitive_unicode_bounded_and_excludes_private_binary_generated_files()
 {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, _, registry, definition) = fixture(dir.path()).await;
    let root = Path::new(&workspace);
    fs::write(
        root.join("src/a.rs"),
        "😀 [needle].* [needle].*\r\n[NEEDLE].*\n[needle].*\n",
    )
    .unwrap();
    fs::write(
        root.join("src/z.rs"),
        format!("{}[needle].*{}\n", "🙂".repeat(200), "🙂".repeat(200)),
    )
    .unwrap();
    fs::write(root.join("raw.bin"), [0xff, 0xfe]).unwrap();
    fs::write(root.join("null-bytes.txt"), "[needle].*\0").unwrap();
    fs::write(root.join("big.txt"), vec![b'x'; 1_048_577]).unwrap();
    for path in [
        ".env",
        ".ENV.local",
        ".act0r-tools/private.txt",
        "target/generated.txt",
        "node_modules/dep.txt",
    ] {
        fs::create_dir_all(root.join(path).parent().unwrap()).unwrap();
        fs::write(root.join(path), "[needle].* private").unwrap();
    }
    let args = json!({"query":"[needle].*","path":null,"max_files":null,"max_matches":null});
    let found = search(&registry, &definition, &workspace, args.clone()).await;
    assert_eq!(found["ok"], true);
    assert_eq!(found["truncated"], false);
    let hits = found["matches"].as_array().unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0]["path"], "src/a.rs");
    assert_eq!(hits[0]["line"], 1);
    assert_eq!(hits[0]["snippet"], "😀 [needle].* [needle].*");
    assert_eq!(hits[1]["line"], 3);
    assert_eq!(hits[2]["snippet_truncated"], true);
    for hit in hits {
        assert!(hit["snippet"].as_str().unwrap().contains("[needle].*"));
        assert!(hit["snippet"].as_str().unwrap().len() <= 512);
    }
    assert_eq!(found["skipped"]["binary"], 2);
    assert_eq!(found["skipped"]["oversized"], 1);
    assert!(!found.to_string().contains("private"));
    assert_eq!(
        search(&registry, &definition, &workspace, args).await,
        found
    );
    let scoped = search(
        &registry,
        &definition,
        &workspace,
        json!({"query":"😀","path":"src/a.rs"}),
    )
    .await;
    assert_eq!(scoped["matches"].as_array().unwrap().len(), 1);
    assert_eq!(scoped["files_considered"], 1);
    let empty = search(
        &registry,
        &definition,
        &workspace,
        json!({"query":"not present","path":"src"}),
    )
    .await;
    assert_eq!(empty["matches"], json!([]));
    assert_eq!(empty["truncated"], false);
}

#[tokio::test]
async fn search_reports_match_file_output_byte_entry_and_depth_limits() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, _, registry, definition) = fixture(dir.path()).await;
    let root = Path::new(&workspace);
    fs::write(root.join("src/a.txt"), "needle\nneedle\n").unwrap();
    fs::write(root.join("src/b.txt"), "needle\n").unwrap();
    let matches = search(
        &registry,
        &definition,
        &workspace,
        json!({"query":"needle","path":"src","max_matches":1}),
    )
    .await;
    assert_eq!(matches["matches"].as_array().unwrap().len(), 1);
    assert_eq!(matches["limits_hit"], json!(["matches"]));
    let files = search(
        &registry,
        &definition,
        &workspace,
        json!({"query":"needle","path":"src","max_files":1}),
    )
    .await;
    assert_eq!(files["files_considered"], 1);
    assert_eq!(files["limits_hit"], json!(["files"]));
    fs::write(
        root.join("escaped.txt"),
        format!("{} needle\n", "\u{1}".repeat(700)).repeat(100),
    )
    .unwrap();
    let output = search(
        &registry,
        &definition,
        &workspace,
        json!({"query":"\u{1}","path":"escaped.txt","max_matches":100}),
    )
    .await;
    assert_eq!(output["limits_hit"], json!(["output"]));
    assert!(output["matches"].as_array().unwrap().len() < 100);
    assert!(serde_json::to_vec(&output).unwrap().len() <= definition.max_output_bytes as usize);
    fs::create_dir(root.join("bytes")).unwrap();
    for index in 0..9 {
        fs::write(
            root.join(format!("bytes/{index}.txt")),
            vec![b'x'; 1_048_576],
        )
        .unwrap();
    }
    let bytes = search(
        &registry,
        &definition,
        &workspace,
        json!({"query":"missing","path":"bytes"}),
    )
    .await;
    assert_eq!(bytes["limits_hit"], json!(["bytes"]));
    assert_eq!(bytes["bytes_read"], 8_388_608);
    fs::create_dir(root.join("many")).unwrap();
    for index in 0..10_001 {
        fs::write(root.join(format!("many/{index}.txt")), "needle").unwrap();
    }
    let entries = search(
        &registry,
        &definition,
        &workspace,
        json!({"query":"needle","path":"many"}),
    )
    .await;
    assert_eq!(entries["limits_hit"], json!(["entries"]));
    assert_eq!(entries["matches"], json!([]));
    let deep = root.join("d").join(vec!["d"; 32].join("/"));
    fs::create_dir_all(&deep).unwrap();
    fs::write(deep.join("leaf.txt"), "needle").unwrap();
    let depth = search(
        &registry,
        &definition,
        &workspace,
        json!({"query":"needle","path":"d"}),
    )
    .await;
    assert_eq!(depth["limits_hit"], json!(["depth"]));
    assert_eq!(depth["matches"], json!([]));
    for result in [matches, files, output, bytes, entries, depth] {
        assert_eq!(result["truncated"], true);
    }
}

#[tokio::test]
async fn search_refusals_preserve_runtime_authority_scope_and_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, local, registry, definition) = fixture(dir.path()).await;
    for (args, code) in [
        (json!({"query":""}), "search_query"),
        (json!({"query":"x".repeat(257)}), "search_query"),
        (json!({"query":"a\nb"}), "search_query"),
        (json!({"query":"a\0b"}), "search_query"),
        (json!({"query":"x","max_matches":0}), "search_limit"),
        (json!({"query":"x","max_matches":101}), "search_limit"),
        (json!({"query":"x","max_files":1001}), "search_limit"),
        (json!({"query":"x","max_files":-1}), "search_limit"),
        (json!({"query":"x","path":"../outside"}), "path_refused"),
        (json!({"query":"x","path":"/absolute"}), "path_refused"),
        (json!({"query":"x","path":"src\\lib.rs"}), "path_refused"),
        (json!({"query":"x","path":"."}), "path_refused"),
        (json!({"query":"x","path":".env"}), "path_refused"),
        (json!({"query":"x","path":".act0r-tools"}), "path_refused"),
        (json!({"query":"x","path":"CON.txt"}), "path_refused"),
        (json!({"query":"x","path":"missing"}), "file_not_found"),
    ] {
        let value = search(&registry, &definition, &workspace, args).await;
        assert_eq!(value["ok"], false);
        assert_eq!(value["error"]["code"], code);
    }
    let mut call = execution(&definition, &workspace, json!({"query":"Original"}));
    call.input["arguments"]["workspace"] = json!("forged-root");
    assert!(
        matches!(registry.execute(call).await.unwrap(), Outcome::Failure(f) if f.code == "tool_input")
    );
    let mut call = execution(&definition, &workspace, json!({"query":"Original"}));
    call.input["workspace"] = Value::Null;
    assert!(
        matches!(registry.execute(call).await.unwrap(), Outcome::Failure(f) if f.code == "tool_workspace")
    );
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(Path::new(&workspace).join(".act0r-tools/ownership.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    assert!(
        matches!(registry.execute(execution(&definition, &workspace, json!({"query":"Original"}))).await.unwrap(), Outcome::Failure(f) if f.code == "workspace_busy" && f.retryable)
    );
    drop(lock);
    let outside = dir.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("secret.txt"), "secret evidence").unwrap();
    directory_link(&outside, &Path::new(&workspace).join("linked"));
    for path in [Value::Null, json!("linked"), json!("linked/secret.txt")] {
        let value = search(
            &registry,
            &definition,
            &workspace,
            json!({"query":"secret","path":path}),
        )
        .await;
        assert_eq!(value["error"]["code"], "path_refused");
        assert!(!value.to_string().contains("secret evidence"));
    }
    fs::write(
        Path::new(&workspace).join(".act0r-tools/managed"),
        "invalid",
    )
    .unwrap();
    assert!(
        local
            .search_text(
                workspace.clone(),
                WorkspaceSearch {
                    query: "x".into(),
                    path: None,
                    max_matches: None,
                    max_files: None
                },
                ActivityId::new()
            )
            .await
            .is_err()
    );
    assert!(
        matches!(registry.execute(execution(&definition, &workspace, json!({"query":"x"}))).await.unwrap(), Outcome::Failure(f) if f.code == "workspace_read" && !f.retryable)
    );
    assert_eq!(
        fs::read_to_string(outside.join("secret.txt")).unwrap(),
        "secret evidence"
    );
}

fn search_config() -> Value {
    let mut cfg = config();
    cfg["tools"] = json!([{"name":"search_text","version":1}]);
    cfg["script"] = json!([{"kind":"call_tool","tool":"search_text","arguments":{"query":"Original"}}, {"kind":"finish","summary":"Used saved search."}]);
    cfg
}
struct Repair;
impl ModelClient for Repair {
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
                .map(|o| o["result"].clone())
                .unwrap_or(Value::Null);
            let call = |tool: &str, arguments: Value| AgentAction::CallTool {
                tool: tool.into(),
                arguments,
            };
            let action = match request.turn {
                0 => call("search_text", json!({"query":"fn answer"})),
                1 => {
                    assert_eq!(result["matches"].as_array().unwrap().len(), 1);
                    assert_eq!(result["matches"][0]["line"], 1);
                    call("read_file", json!({"path":result["matches"][0]["path"]}))
                }
                2 => {
                    assert!(result["text"].as_str().unwrap().contains("19 - 23"));
                    call(
                        "replace_text",
                        json!({"path":result["path"],"old":"19 - 23","new":"19 + 23"}),
                    )
                }
                3 => call("run_tests", json!({})),
                4 => {
                    assert_eq!(result["exit_code"], 0);
                    call("echo", json!({"text":"later evidence"}))
                }
                5 => {
                    assert!(!request.prompt.contains("\"snippet\""));
                    call("list_results", json!({"tool":"search_text","limit":1}))
                }
                6 => call(
                    "read_result",
                    json!({"activity_id":result["results"][0]["activity_id"],"max_bytes":4096}),
                ),
                7 => {
                    let original: Value =
                        serde_json::from_str(result["text"].as_str().unwrap()).unwrap();
                    assert_eq!(original["matches"].as_array().unwrap().len(), 1);
                    assert!(
                        original["matches"][0]["snippet"]
                            .as_str()
                            .unwrap()
                            .contains("19 - 23")
                    );
                    if request.target.model == "planner" {
                        AgentAction::DelegateTask {
                            writable_paths: None,
                            worker: "reviewer".into(),
                            goal: format!(
                                "Review saved search: {}",
                                original["matches"][0]["path"].as_str().unwrap()
                            ),
                        }
                    } else {
                        AgentAction::Finish {
                            summary: "Repaired from saved search and passing tests.".into(),
                        }
                    }
                }
                8 => AgentAction::Finish {
                    summary: "Reviewed the search-guided repair.".into(),
                },
                _ => panic!("unexpected turn {}", request.turn),
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
async fn both_policies_find_read_repair_test_and_recall_the_saved_search_across_restarts() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("repair.db");
        let src = test_source(dir.path(), "search_verify");
        fs::write(
            src.join("src/lib.rs"),
            "pub fn answer() -> i64 { 19 - 23 }\n",
        )
        .unwrap();
        let calls = Arc::new(Calls::default());
        let mut app = open(&db, calls.clone(), Some(Arc::new(Repair))).await;
        let mut cfg = search_config();
        cfg["script"] = json!([]);
        cfg["tools"] = json!([{"name":"search_text","version":1},{"name":"read_file","version":2},{"name":"replace_text","version":1},{"name":"run_tests","version":1},{"name":"echo","version":1},{"name":"list_results","version":1},{"name":"read_result","version":1}]);
        cfg["writable_paths"] = json!(["src/lib.rs"]);
        cfg["test_command"] = json!(test_command(10000));
        cfg["max_model_calls"] = json!(9);
        cfg["context"] = json!({"max_observations":2,"max_input_bytes":20000});
        if policy == "planner" {
            cfg["goal_budget"] = json!({"max_model_calls":11,"max_tokens":300000});
        }
        let actor = create(&app, policy, Some(src.clone()), cfg, 24).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let (searched, _) = native_once(&mut worker).await;
        assert_eq!(searched.handler, DefinitionId::new("native.tool", 3));
        assert!(searched.input.get("writable_paths").is_none());
        assert!(searched.input.get("test_command").is_none());
        let workspace = PathBuf::from(searched.input["workspace"].as_str().unwrap());
        fs::write(
            workspace.join("README.md"),
            "fn answer changed after saved search",
        )
        .unwrap();
        let before = json!(app.engine.status(searched.run_id).await.unwrap());
        drop(worker);
        drop(app);
        app = open(&db, calls.clone(), Some(Arc::new(Repair))).await;
        worker = app.engine.worker().await.unwrap();
        assert_eq!(
            json!(app.engine.status(searched.run_id).await.unwrap()),
            before
        );
        model_once(&mut worker).await;
        native_once(&mut worker).await;
        model_once(&mut worker).await;
        let (run, wait) = approval(&app, task).await;
        app.engine
            .signal(run, wait.id, json!({"approved":true}))
            .await
            .unwrap();
        native_once(&mut worker).await;
        model_once(&mut worker).await;
        approve_tests(&app, task).await;
        native_once(&mut worker).await;
        model_once(&mut worker).await;
        native_once(&mut worker).await;
        model_once(&mut worker).await;
        native_once(&mut worker).await;
        model_once(&mut worker).await;
        let (recalled, recall_result) = native_once(&mut worker).await;
        let before = json!(app.engine.status(run).await.unwrap());
        drop(worker);
        drop(app);
        app = open(&db, calls.clone(), Some(Arc::new(Repair))).await;
        worker = app.engine.worker().await.unwrap();
        assert_eq!(json!(app.engine.status(run).await.unwrap()), before);
        worker.complete(recalled, recall_result).await.unwrap();
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
            assert_eq!(child.goal, "Review saved search: src/lib.rs");
            let child = app.agents.actors().unwrap().task(child.id).await.unwrap();
            app.engine
                .signal(
                    child.run_id.unwrap(),
                    child.wait.unwrap().id,
                    json!({"answer":"reviewed"}),
                )
                .await
                .unwrap();
            settle(&mut worker).await;
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
        assert_eq!(
            fs::read_to_string(workspace.join("src/lib.rs")).unwrap(),
            "pub fn answer() -> i64 { 19 + 23 }\n"
        );
        assert_eq!(
            fs::read_to_string(src.join("src/lib.rs")).unwrap(),
            "pub fn answer() -> i64 { 19 - 23 }\n"
        );
        assert_eq!(
            calls
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(
                    |r| matches!(&r.action, AgentAction::CallTool {tool,..} if tool=="search_text")
                )
                .count(),
            1
        );
        let saved = app.engine.status(run).await.unwrap();
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == searched.activity_id)
                .count(),
            1
        );
        assert_eq!(
            saved
                .activities()
                .iter()
                .filter(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn search_results_survive_goal_holds_cancellation_and_exhausted_continuations() {
    for policy in ["tool", "planner"] {
        for mode in ["pause", "cancel", "model", "attempt"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("controls.db");
            let calls = Arc::new(Calls::default());
            let mut app = open(&db, calls.clone(), None).await;
            let mut cfg = search_config();
            if mode == "model" {
                cfg["max_model_calls"] = json!(1);
            }
            let actor = create(
                &app,
                policy,
                Some(source(dir.path())),
                cfg,
                if mode == "attempt" { 2 } else { 10 },
            )
            .await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            model_once(&mut worker).await;
            let call = worker.admit().await.unwrap().unwrap();
            let outcome = worker.execute(call.clone()).await.unwrap();
            if mode == "pause" {
                app.agents.actors().unwrap().pause_goal(task).await.unwrap();
            }
            if mode == "cancel" {
                app.agents
                    .actors()
                    .unwrap()
                    .cancel_goal(task)
                    .await
                    .unwrap();
            }
            worker
                .complete(call.clone(), outcome.clone())
                .await
                .unwrap();
            settle(&mut worker).await;
            let saved = app.engine.status(call.run_id).await.unwrap();
            assert_eq!(saved.activities()[1].result, outcome.result().cloned());
            assert!(worker.admit().await.unwrap().is_none());
            if matches!(mode, "model" | "attempt") {
                assert_eq!(saved.record().status, RunStatus::BudgetExhausted);
            }
            let next = saved.activities().last().unwrap().spec.input.clone();
            drop(worker);
            drop(app);
            app = open(&db, calls.clone(), None).await;
            worker = app.engine.worker().await.unwrap();
            assert_eq!(
                app.engine
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
            if mode == "pause" {
                assert!(worker.admit().await.unwrap().is_none());
                app.agents
                    .actors()
                    .unwrap()
                    .resume_goal(task)
                    .await
                    .unwrap();
                settle(&mut worker).await;
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
            assert_eq!(calls.requests.lock().unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn search_completion_rolls_back_with_the_next_request_and_keeps_one_saved_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("atomic.db");
    let calls = Arc::new(Calls::default());
    let app = open(&db, calls.clone(), None).await;
    let actor = create(&app, "tool", Some(source(dir.path())), search_config(), 10).await;
    send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let call = worker.admit().await.unwrap().unwrap();
    let output = worker.execute(call.clone()).await.unwrap();
    let before = json!(app.engine.status(call.run_id).await.unwrap());
    let history = json!(app.engine.history(call.run_id).await.unwrap());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_search_continuation BEFORE INSERT ON activities WHEN json_extract(NEW.payload,'$.spec.handler.name')='agent.model' BEGIN SELECT RAISE(ABORT,'search rollback'); END").execute(&pool).await.unwrap();
    assert!(worker.complete(call.clone(), output.clone()).await.is_err());
    assert_eq!(json!(app.engine.status(call.run_id).await.unwrap()), before);
    assert_eq!(
        json!(app.engine.history(call.run_id).await.unwrap()),
        history
    );
    sqlx::query("DROP TRIGGER reject_search_continuation")
        .execute(&pool)
        .await
        .unwrap();
    fs::write(
        Path::new(call.input["workspace"].as_str().unwrap()).join("README.md"),
        "Changed current file",
    )
    .unwrap();
    worker.complete(call.clone(), output.clone()).await.unwrap();
    let history = json!(app.engine.history(call.run_id).await.unwrap());
    worker.complete(call.clone(), output.clone()).await.unwrap();
    assert_eq!(
        json!(app.engine.history(call.run_id).await.unwrap()),
        history
    );
    assert_eq!(
        app.engine.status(call.run_id).await.unwrap().activities()[1].result,
        output.result().cloned()
    );
    settle(&mut worker).await;
    assert_eq!(calls.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn search_evidence_is_saved_when_it_cannot_fit_the_pinned_context() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let app = open(
            &dir.path().join("context.db"),
            Arc::new(Calls::default()),
            None,
        )
        .await;
        let src = source(dir.path());
        fs::write(
            src.join("README.md"),
            format!("Original {}\n", "x".repeat(700)),
        )
        .unwrap();
        let mut cfg = search_config();
        let baseline = create(&app, policy, Some(src.clone()), cfg.clone(), 8).await;
        let baseline_task = send(&app, baseline).await;
        let mut worker = app.engine.worker().await.unwrap();
        let model = model_once(&mut worker).await;
        let request: ModelRequest = serde_json::from_value(model.input).unwrap();
        app.agents
            .actors()
            .unwrap()
            .cancel_goal(baseline_task)
            .await
            .unwrap();
        settle(&mut worker).await;
        let bound = request.context_input_bytes() + 16;
        cfg["max_input_bytes"] = json!(bound);
        cfg["context"] = json!({"max_input_bytes":bound,"max_observations":16});
        let actor = create(&app, policy, Some(src), cfg, 8).await;
        let task = send(&app, actor).await;
        settle(&mut worker).await;
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        let saved = app.engine.status(view.run_id.unwrap()).await.unwrap();
        assert_eq!(saved.record().error.as_ref().unwrap().code, "context_limit");
        assert_eq!(saved.activities()[1].status, ActivityStatus::Succeeded);
        assert_eq!(
            saved.activities()[1].result.as_ref().unwrap()["matches"][0]["snippet_truncated"],
            true
        );
        assert_eq!(saved.record().usage.admitted_model_calls, 1);
    }
}

#[tokio::test]
#[ignore = "invoked and killed by the workspace-search recovery test"]
async fn search_crash_fixture() {
    let root = PathBuf::from(std::env::var("ACT0R_SEARCH_CRASH_DIR").unwrap());
    let stage = std::env::var("ACT0R_SEARCH_CRASH_STAGE").unwrap();
    let calls = Arc::new(Calls {
        requests: Mutex::new(vec![]),
        ledger: Some(root.join("calls.log")),
    });
    let app = open(&root.join("crash.db"), calls, None).await;
    let actor = create(&app, "tool", Some(source(&root)), search_config(), 10).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let call = if stage == "queued" {
        None
    } else {
        let call = worker.admit().await.unwrap().unwrap();
        if matches!(stage.as_str(), "executed" | "saved") {
            let output = worker.execute(call.clone()).await.unwrap();
            if stage == "saved" {
                worker.complete(call.clone(), output).await.unwrap();
            }
        }
        Some(call)
    };
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    let run = view.run_id.unwrap();
    let saved = app.engine.status(run).await.unwrap();
    let workspace = app
        .agents
        .actors()
        .unwrap()
        .status(actor)
        .await
        .unwrap()
        .record()
        .config["input"]["workspace"]
        .clone();
    fs::write(root.join("ready.json"), serde_json::to_vec(&json!({"task":task,"run":run,"call":call,
        "workspace":workspace,"next":saved.activities().last().unwrap().spec.input,"usage":saved.record().usage})).unwrap()).unwrap();
    let _keep = (app, worker);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn killed_search_workers_retry_uncommitted_reads_and_reuse_committed_results() {
    for stage in ["queued", "admitted", "executed", "saved"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "native_tests::search::search_crash_fixture",
            ])
            .env("ACT0R_SEARCH_CRASH_DIR", root)
            .env("ACT0R_SEARCH_CRASH_STAGE", stage)
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
        fs::write(
            Path::new(ready["workspace"].as_str().unwrap()).join("README.md"),
            "Updated project evidence.",
        )
        .unwrap();
        let calls = Arc::new(Calls {
            requests: Mutex::new(vec![]),
            ledger: Some(root.join("calls.log")),
        });
        let app = open(&root.join("crash.db"), calls, None).await;
        let mut worker = app.engine.worker().await.unwrap();
        let restored = app.engine.status(run).await.unwrap();
        assert_ne!(restored.record().status, RunStatus::Blocked);
        assert_eq!(
            restored.activities().last().unwrap().spec.input,
            ready["next"]
        );
        assert_eq!(json!(restored.record().usage), ready["usage"]);
        if matches!(stage, "admitted" | "executed") {
            assert!(worker.admit().await.unwrap().is_none());
            app.clock.0.store(1100, Ordering::SeqCst);
        }
        settle(&mut worker).await;
        let saved = app.engine.status(run).await.unwrap();
        let activity = &saved.activities()[1];
        assert_eq!(
            activity.spec.input["definition"]["id"]["name"],
            "search_text"
        );
        let matches = activity.result.as_ref().unwrap()["matches"]
            .as_array()
            .unwrap();
        assert_eq!(matches.len(), if stage == "saved" { 1 } else { 0 });
        if stage == "saved" {
            assert!(matches[0]["snippet"].as_str().unwrap().contains("Original"));
        }
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == activity.id)
                .count(),
            if matches!(stage, "admitted" | "executed") {
                2
            } else {
                1
            }
        );
        if let Some(call) = ready["call"].as_object() {
            assert_eq!(json!(activity.id), call["activity_id"]);
        }
        assert_eq!(
            fs::read_to_string(root.join("calls.log"))
                .unwrap()
                .lines()
                .count(),
            if stage == "executed" { 2 } else { 1 }
        );
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
        assert_eq!(saved.record().usage.admitted_model_calls, 2);
    }
}
