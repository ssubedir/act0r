use super::*;
use act0r::{adapters::workspace_reads::ReadFilesTool, application::ToolRegistry};

fn batch_config() -> Value {
    let mut cfg = config();
    cfg["tools"] = json!([{"name":"read_files","version":1}]);
    cfg["script"] = json!([
        {"kind":"call_tool","tool":"read_files","arguments":{"paths":["README.md","missing.md","src/lib.rs"]}},
        {"kind":"finish","summary":"Used the saved multi-file evidence"}
    ]);
    cfg
}

#[tokio::test]
async fn oversized_batch_context_keeps_the_entire_saved_result_without_an_extra_model_call() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let app = open(
            &dir.path().join("batch-context.db"),
            Arc::new(Calls::default()),
            None,
        )
        .await;
        let src = source(dir.path());
        let mut cfg = batch_config();
        let baseline = create(&app, policy, Some(src.clone()), cfg.clone(), 20).await;
        let task = send(&app, baseline).await;
        let mut worker = app.engine.worker().await.unwrap();
        let execution = model_once(&mut worker).await;
        let request: ModelRequest = serde_json::from_value(execution.input).unwrap();
        app.agents
            .actors()
            .unwrap()
            .cancel_goal(task)
            .await
            .unwrap();
        settle(&mut worker).await;
        let bound = request.context_input_bytes() + 16;
        cfg["max_input_bytes"] = json!(bound);
        cfg["context"] = json!({"max_input_bytes":bound,"max_observations":16});
        let actor = create(&app, policy, Some(src), cfg, 20).await;
        let task = send(&app, actor).await;
        settle(&mut worker).await;
        let task = app.agents.actors().unwrap().task(task).await.unwrap();
        let run = app.engine.status(task.run_id.unwrap()).await.unwrap();
        assert_eq!(run.record().error.as_ref().unwrap().code, "context_limit");
        assert_eq!(run.record().usage.admitted_model_calls, 1);
        let result = run.activities()[1].result.as_ref().unwrap();
        assert_eq!(run.activities()[1].status, ActivityStatus::Succeeded);
        assert_eq!(result["files"].as_array().unwrap().len(), 3);
        assert_eq!(result["files"][0]["text"], "Original project evidence.\n");
        assert_eq!(result["files"][1]["error"]["code"], "file_not_found");
    }
}

async fn fixture(root: &Path) -> (String, Arc<LocalTools>, ToolRegistry, ToolDefinition) {
    let managed = root.join("workspaces");
    let workspace = LocalWorkspaces::new(managed.clone())
        .prepare(source(root))
        .await
        .unwrap();
    let local = Arc::new(LocalTools::new(managed));
    let tool = Arc::new(ReadFilesTool::new(local.clone()));
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
async fn read(
    registry: &ToolRegistry,
    definition: &ToolDefinition,
    workspace: &str,
    paths: Value,
) -> Value {
    registry
        .execute(execution(definition, workspace, json!({"paths":paths})))
        .await
        .unwrap()
        .result()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn batched_reads_preserve_order_duplicates_and_per_file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, _, registry, definition) = fixture(dir.path()).await;
    fs::write(Path::new(&workspace).join("raw.bin"), [255, 254]).unwrap();
    fs::write(Path::new(&workspace).join("big.txt"), vec![b'x'; 1_048_577]).unwrap();
    let paths = json!([
        "src/lib.rs",
        "missing.md",
        "README.md",
        "../outside.txt",
        "raw.bin",
        "big.txt",
        "README.md",
        ".env"
    ]);
    let result = read(&registry, &definition, &workspace, paths.clone()).await;
    assert_eq!(result["ok"], true);
    assert_eq!(result["truncated"], false);
    assert_eq!(
        result["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["path"].clone())
            .collect::<Vec<_>>(),
        paths.as_array().unwrap().clone()
    );
    assert_eq!(
        result["files"][0]["text"],
        "pub fn durable() -> bool { true }\n"
    );
    assert_eq!(result["files"][2], result["files"][6]);
    for (index, code) in [
        (1, "file_not_found"),
        (3, "path_refused"),
        (4, "invalid_utf8"),
        (5, "file_limit"),
        (7, "path_refused"),
    ] {
        assert_eq!(result["files"][index]["ok"], false);
        assert_eq!(result["files"][index]["error"]["code"], code);
    }
    assert!(!result.to_string().contains("private-source-data"));
    assert_eq!(
        read(&registry, &definition, &workspace, paths).await,
        result
    );
    let long_path = read(
        &registry,
        &definition,
        &workspace,
        json!(["README.md", "x".repeat(4097)]),
    )
    .await;
    assert_eq!(long_path["files"][0]["ok"], true);
    assert_eq!(long_path["files"][1]["error"]["code"], "path_refused");
    let denied = read(
        &registry,
        &definition,
        &workspace,
        json!([
            ".act0r-tools/managed",
            "target/generated.txt",
            "src",
            "C:outside.txt",
            r"\\host\share\outside.txt",
            r"\outside.txt",
            r"src\lib.rs",
            r".act0r-tools\managed"
        ]),
    )
    .await;
    for (i, code) in [
        "path_refused",
        "path_refused",
        "not_a_file",
        "path_refused",
        "path_refused",
        "path_refused",
        "path_refused",
        "path_refused",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            denied["files"][i]["error"]["code"], *code,
            "{}",
            denied["files"][i]["path"]
        );
    }
}

#[tokio::test]
async fn batched_reads_bound_unicode_text_and_escaped_serialized_output() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, _, registry, definition) = fixture(dir.path()).await;
    let root = Path::new(&workspace);
    let paths = (0..8).map(|i| format!("part{i}.txt")).collect::<Vec<_>>();
    for path in &paths {
        fs::write(root.join(path), "😀".repeat(6000)).unwrap();
    }
    let result = read(&registry, &definition, &workspace, json!(paths)).await;
    assert_eq!(result["text_bytes"], 65_536);
    assert_eq!(result["truncated"], true);
    let files = result["files"].as_array().unwrap();
    for (i, file) in files.iter().enumerate() {
        assert_eq!(file["ok"], true);
        assert_eq!(file["truncated"], true);
        let text = file["text"].as_str().unwrap();
        assert_eq!(text.len(), if i < 4 { 16_384 } else { 0 });
        assert!(text.chars().all(|c| c == '😀'));
    }
    fs::write(
        root.join(&paths[0]),
        format!("{}😀tail", "a".repeat(16_383)),
    )
    .unwrap();
    let result = read(&registry, &definition, &workspace, json!([paths[0]])).await;
    assert_eq!(result["files"][0]["text"].as_str().unwrap().len(), 16_383);
    for path in &paths {
        fs::write(root.join(path), "\u{1}".repeat(20_000)).unwrap();
    }
    let result = read(&registry, &definition, &workspace, json!(paths)).await;
    assert_eq!(result["text_bytes"], 65_536);
    assert!(serde_json::to_vec(&result).unwrap().len() <= definition.max_output_bytes as usize);
    fs::write(root.join("empty.txt"), "").unwrap();
    assert_eq!(
        read(&registry, &definition, &workspace, json!(["empty.txt"])).await["files"][0]["truncated"],
        false
    );
}

#[tokio::test]
async fn batch_limits_authority_and_workspace_lock_are_not_per_file_feedback() {
    let dir = tempfile::tempdir().unwrap();
    let (workspace, local, registry, definition) = fixture(dir.path()).await;
    for paths in [
        json!([]),
        json!(vec!["x"; 9]),
        json!(["README.md", null]),
        json!("README.md"),
    ] {
        assert!(
            definition
                .validate_arguments(&json!({"paths":paths}))
                .is_err()
        );
    }
    assert!(
        definition
            .validate_arguments(&json!({"paths":["README.md"],"workspace":"forged"}))
            .is_err()
    );
    let batch = WorkspaceReadBatch {
        paths: vec!["x".repeat(32_768)],
    };
    assert!(
        local
            .read_files(workspace.clone(), batch, ActivityId::new())
            .await
            .is_err()
    );
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(Path::new(&workspace).join(".act0r-tools/ownership.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    let call = execution(&definition, &workspace, json!({"paths":["README.md"]}));
    let Outcome::Failure(failure) = registry.execute(call.clone()).await.unwrap() else {
        panic!()
    };
    assert_eq!(failure.code, "workspace_busy");
    assert!(failure.retryable);
    drop(lock);
    assert_eq!(
        registry
            .execute(call.clone())
            .await
            .unwrap()
            .result()
            .unwrap()["files"][0]["ok"],
        true
    );
    fs::write(
        Path::new(&workspace).join(".act0r-tools/managed"),
        "bad marker",
    )
    .unwrap();
    let Outcome::Failure(failure) = registry.execute(call).await.unwrap() else {
        panic!()
    };
    assert_eq!(failure.code, "workspace_read");
    assert!(!failure.retryable);
}

#[tokio::test]
async fn saved_batch_and_exact_next_request_survive_restart_without_reading_changed_files() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("batch.db");
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), None).await;
        let actor = create(&app, policy, Some(source(dir.path())), batch_config(), 20).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let call = worker.admit().await.unwrap().unwrap();
        assert_eq!(call.handler, DefinitionId::new("native.tool", 3));
        // Close/reopen after intent admission, before the repeat-safe read executes.
        let id = call.activity_id;
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), None).await;
        let mut worker = app.engine.worker().await.unwrap();
        app.clock.0.store(1100, Ordering::SeqCst);
        let recovered = worker.admit().await.unwrap().unwrap();
        assert_eq!(recovered.activity_id, id);
        let workspace = recovered.input["workspace"].as_str().unwrap().to_owned();
        let outcome = worker.execute(recovered.clone()).await.unwrap();
        let saved = outcome.result().unwrap().clone();
        assert_eq!(saved["files"][1]["error"]["code"], "file_not_found");
        worker.complete(recovered, outcome).await.unwrap();
        // Save the exact next model intent, then mutate the live managed files.
        let run = app.engine.status(call.run_id).await.unwrap();
        let next = run
            .activities()
            .iter()
            .find(|a| a.status == ActivityStatus::Queued && a.spec.handler.name == "agent.model")
            .unwrap();
        let pinned = next.spec.input.clone();
        fs::write(
            Path::new(&workspace).join("README.md"),
            "changed after saved read",
        )
        .unwrap();
        fs::write(Path::new(&workspace).join("missing.md"), "now exists").unwrap();
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), None).await;
        let mut worker = app.engine.worker().await.unwrap();
        app.clock.0.store(1100, Ordering::SeqCst);
        let next = worker.admit().await.unwrap().unwrap();
        assert_eq!(next.input, pinned);
        assert!(next.input.to_string().contains("Original project evidence"));
        assert!(!next.input.to_string().contains("changed after saved read"));
        worker
            .complete(next.clone(), worker.execute(next).await.unwrap())
            .await
            .unwrap();
        settle(&mut worker).await;
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(view.status, AgentTaskStatus::Succeeded);
        let run = app.engine.status(view.run_id.unwrap()).await.unwrap();
        let reads = run
            .activities()
            .iter()
            .filter(|a| a.spec.handler.name == "native.tool")
            .collect::<Vec<_>>();
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].result.as_ref(), Some(&saved));
        assert_eq!(calls.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn whole_batch_and_next_request_commit_atomically_and_duplicate_completion_is_inert() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("batch-atomic.db");
    let calls = Arc::new(Calls::default());
    let app = open(&db, calls.clone(), None).await;
    let actor = create(&app, "tool", Some(source(dir.path())), batch_config(), 20).await;
    send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let call = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(call.clone()).await.unwrap();
    let before = json!(app.engine.status(call.run_id).await.unwrap());
    let history = json!(app.engine.history(call.run_id).await.unwrap());
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    sqlx::query("CREATE TRIGGER reject_batch_continuation BEFORE INSERT ON activities WHEN json_extract(NEW.payload,'$.spec.handler.name')='agent.model' BEGIN SELECT RAISE(ABORT,'batch rollback'); END").execute(&pool).await.unwrap();
    assert!(
        worker
            .complete(call.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(json!(app.engine.status(call.run_id).await.unwrap()), before);
    assert_eq!(
        json!(app.engine.history(call.run_id).await.unwrap()),
        history
    );
    sqlx::query("DROP TRIGGER reject_batch_continuation")
        .execute(&pool)
        .await
        .unwrap();
    fs::write(
        Path::new(call.input["workspace"].as_str().unwrap()).join("README.md"),
        "changed after execution",
    )
    .unwrap();
    worker
        .complete(call.clone(), outcome.clone())
        .await
        .unwrap();
    let history = json!(app.engine.history(call.run_id).await.unwrap());
    worker
        .complete(call.clone(), outcome.clone())
        .await
        .unwrap();
    assert_eq!(
        json!(app.engine.history(call.run_id).await.unwrap()),
        history
    );
    let run = app.engine.status(call.run_id).await.unwrap();
    assert_eq!(run.activities()[1].result, outcome.result().cloned());
    assert_eq!(
        run.activities()[1].result.as_ref().unwrap()["files"][0]["text"],
        "Original project evidence.\n"
    );
    settle(&mut worker).await;
    assert_eq!(calls.requests.lock().unwrap().len(), 1);
    pool.close().await;
}
