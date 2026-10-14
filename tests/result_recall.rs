use act0r::{
    Engine, Error, Worker,
    adapters::{
        agent_executor::AgentExecutor,
        demo::DemoExecutor,
        file_lock::FileOwnership,
        model::ModelRouter,
        native_tools::EchoTool,
        result_recall::{ListResultsTool, ReadResultTool, result_recall_tools},
        sqlite::SqliteStore,
        system_clock::SystemClock,
        workspace::LocalTools,
    },
    application::{ToolRegistry, ports::*},
    domain::{result_recall::*, tools::*, *},
};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Evidence(Arc<AtomicUsize>);
impl NativeTool for Evidence {
    fn definition(&self) -> ToolDefinition {
        let mut definition = EchoTool.definition();
        definition.max_input_bytes = 262144;
        definition.max_output_bytes = 262144;
        definition
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(NativeToolResult::Success(serde_json::from_str(
                call.arguments["text"].as_str().unwrap(),
            )?))
        })
    }
}
struct Fixture;
fn wait() -> Command {
    Command::WaitForInput {
        key: "next".into(),
        prompt: "next fixture activity".into(),
        payload: Value::Null,
    }
}
impl WorkflowDefinition for Fixture {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("recall_fixture", 1)
    }
    fn transition(&self, _: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let commands = match event {
            WorkflowEvent::Started(_) | WorkflowEvent::ActivityCompleted { .. } => vec![wait()],
            WorkflowEvent::InputReceived { payload, .. } => vec![Command::Schedule(
                serde_json::from_value(payload.clone()).unwrap(),
            )],
            _ => panic!("unexpected fixture event"),
        };
        Ok(Transition {
            state: Value::Null,
            commands,
        })
    }
}
struct Batch;
impl WorkflowDefinition for Batch {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("recall_batch", 1)
    }
    fn transition(&self, _: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let commands = if let WorkflowEvent::Started(input) = event {
            serde_json::from_value::<Vec<ActivitySpec>>(input.clone())
                .unwrap()
                .into_iter()
                .map(Command::Schedule)
                .collect()
        } else {
            vec![]
        };
        Ok(Transition {
            state: Value::Null,
            commands,
        })
    }
}
struct App {
    engine: Engine,
    store: Arc<SqliteStore>,
    catalog: Arc<ToolRegistry>,
    executions: Arc<AtomicUsize>,
}
async fn open(db: &Path) -> App {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let executions = Arc::new(AtomicUsize::new(0));
    let mut tools: Vec<Arc<dyn NativeTool>> = vec![Arc::new(Evidence(executions.clone()))];
    tools.extend(result_recall_tools(store.clone()));
    let catalog = Arc::new(ToolRegistry::new(tools).unwrap());
    let engine = Engine::new(
        store.clone(),
        Arc::new(
            AgentExecutor::new(
                Arc::new(ModelRouter::new(None).unwrap()),
                Arc::new(LocalTools::new(db.parent().unwrap().join("workspaces"))),
                Arc::new(DemoExecutor),
            )
            .with_native_tools(catalog.clone()),
        ),
        Arc::new(SystemClock),
        Arc::new(FileOwnership::new(db)),
        vec![Arc::new(Fixture), Arc::new(Batch)],
    )
    .unwrap();
    App {
        engine,
        store,
        catalog,
        executions,
    }
}
fn spec(app: &App, name: &str, args: Value) -> ActivitySpec {
    let Command::Schedule(spec) = app
        .catalog
        .definitions()
        .into_iter()
        .find(|d| d.id.name == name)
        .unwrap()
        .schedule_call(&args)
        .unwrap()
    else {
        panic!("fixture requires an activity");
    };
    spec
}
async fn start(app: &App) -> RunId {
    app.engine
        .start(Fixture.id(), Value::Null, 200)
        .await
        .unwrap()
}
async fn queue(app: &App, run: RunId, name: &str, args: Value) -> ActivityId {
    let before = app.engine.status(run).await.unwrap();
    app.engine
        .signal(
            run,
            before.record().wait.as_ref().unwrap().id,
            json!(spec(app, name, args)),
        )
        .await
        .unwrap();
    app.engine
        .status(run)
        .await
        .unwrap()
        .activities()
        .last()
        .unwrap()
        .id
}
async fn dispatch(
    app: &App,
    worker: &mut Worker,
    run: RunId,
    name: &str,
    args: Value,
) -> (ActivityId, Value) {
    let id = queue(app, run, name, args).await;
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.activity_id, id);
    let outcome = worker.execute(execution.clone()).await.unwrap();
    let result = outcome.result().unwrap().clone();
    worker.complete(execution, outcome).await.unwrap();
    (id, result)
}
fn scope(run_id: RunId, activity_id: ActivityId) -> RecallScope {
    RecallScope {
        run_id,
        activity_id,
    }
}

#[tokio::test]
async fn recall_pages_reconstruct_exact_json_and_list_only_metadata_without_mutating_records() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("pages.db")).await;
    let run = start(&app).await;
    let mut worker = app.engine.worker().await.unwrap();
    let mut values = vec![
        json!({"text":"\"\\\n🙂".repeat(800),"ok":false,"float":1.25}),
        json!(["array", true, 42]),
        json!("🙂string"),
        json!(42),
        json!(true),
        Value::Null,
    ];
    values.extend((0..16).map(|n| json!({"index":n})));
    let mut sources = vec![];
    for value in &values {
        sources.push(
            dispatch(
                &app,
                &mut worker,
                run,
                "echo",
                json!({"text":value.to_string()}),
            )
            .await
            .0,
        );
    }
    let (list_id, listed) =
        dispatch(&app, &mut worker, run, "list_results", json!({"limit":2})).await;
    assert_eq!(listed["results"].as_array().unwrap().len(), 2);
    assert_eq!(listed["next_after"], json!(sources[1]));
    assert!(!listed.to_string().contains("array"));
    let before = json!(app.engine.status(run).await.unwrap());
    let history = json!(app.engine.history(run).await.unwrap());
    let page = app
        .store
        .list_results(
            scope(run, list_id),
            ListResultsQuery {
                after: Some(sources[1]),
                tool: Some("echo".into()),
                limit: Some(16),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        page.results
            .iter()
            .map(|r| r.activity_id)
            .collect::<Vec<_>>(),
        sources[2..18]
    );
    assert_eq!(page.next_after, Some(sources[17]));
    let last = app
        .store
        .list_results(
            scope(run, list_id),
            ListResultsQuery {
                after: page.next_after,
                tool: None,
                limit: Some(16),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        last.results
            .iter()
            .map(|r| r.activity_id)
            .collect::<Vec<_>>(),
        sources[18..]
    );
    assert!(last.next_after.is_none());
    assert_eq!(json!(app.engine.status(run).await.unwrap()), before);
    assert_eq!(json!(app.engine.history(run).await.unwrap()), history);
    let read_id = queue(
        &app,
        run,
        "read_result",
        json!({"activity_id":sources[0],"max_bytes":4}),
    )
    .await;
    for (id, value) in sources.iter().zip(values) {
        let mut offset = 0;
        let mut text = String::new();
        loop {
            let chunk = app
                .store
                .read_result(
                    scope(run, read_id),
                    ReadResultQuery {
                        activity_id: *id,
                        offset: Some(offset),
                        max_bytes: Some(4096),
                    },
                )
                .await
                .unwrap();
            assert!(chunk.text.len() <= 4096);
            assert_eq!(chunk.offset, offset);
            assert_eq!(chunk.source.result_bytes, value.to_string().len() as u64);
            text.push_str(&chunk.text);
            if let Some(next) = chunk.next_offset {
                assert!(next > offset);
                assert!(!chunk.complete);
                offset = next;
            } else {
                assert_eq!(chunk.complete, offset == 0);
                break;
            }
        }
        assert_eq!(text, value.to_string());
    }
    assert_eq!(app.executions.load(Ordering::SeqCst), 22);
}

#[test]
fn recall_query_and_chunk_bounds_are_explicit() {
    for n in [0, 17, u32::MAX] {
        assert!(
            ListResultsQuery {
                limit: Some(n),
                ..Default::default()
            }
            .page_size()
            .is_err()
        );
    }
    for n in [0, 3, 4097, u32::MAX] {
        assert!(
            ReadResultQuery {
                activity_id: ActivityId::new(),
                offset: None,
                max_bytes: Some(n)
            }
            .bounds()
            .is_err()
        );
    }
    let source = ResultMetadata {
        activity_id: ActivityId::new(),
        tool: DefinitionId::new("echo", 1),
        result_bytes: 10,
        ok: None,
    };
    let chunk = ResultChunk::from_bytes(source.clone(), 0, vec![b'a', b'b', 0xf0, 0x9f]).unwrap();
    assert_eq!(chunk.text, "ab");
    assert_eq!(chunk.next_offset, Some(2));
    assert!(!chunk.complete);
    assert!(ResultChunk::from_bytes(source.clone(), 11, vec![]).is_err());
    assert!(ResultChunk::from_bytes(source.clone(), 0, vec![0x9f]).is_err());
    assert!(ResultChunk::from_bytes(source, 0, vec![b'a'; 4097]).is_err());
}

#[tokio::test]
async fn recall_refuses_foreign_cursors_models_self_and_invalid_boundaries_with_saved_feedback() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("scope.db")).await;
    let run = start(&app).await;
    let foreign = start(&app).await;
    let mut worker = app.engine.worker().await.unwrap();
    let (source, _) = dispatch(
        &app,
        &mut worker,
        run,
        "echo",
        json!({"text":"\"🙂value\""}),
    )
    .await;
    let (other, _) = dispatch(
        &app,
        &mut worker,
        foreign,
        "echo",
        json!({"text":"{\"secret\":42}"}),
    )
    .await;
    let model = ActivitySpec {
        handler: DefinitionId::new("agent.model", 1),
        input: json!({"private":"model metadata"}),
        retry: RetryPolicy::default(),
        recovery: RecoveryPolicy::SafeToRepeat,
        usage: None,
    };
    let before = app.engine.status(run).await.unwrap();
    app.engine
        .signal(run, before.record().wait.as_ref().unwrap().id, json!(model))
        .await
        .unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let model_id = execution.activity_id;
    worker
        .complete(execution, Outcome::Success(json!({"model_secret":true})))
        .await
        .unwrap();
    let (list, output) = dispatch(
        &app,
        &mut worker,
        run,
        "list_results",
        json!({"after":null,"tool":null,"limit":null}),
    )
    .await;
    assert_eq!(output["results"].as_array().unwrap().len(), 1);
    assert_eq!(output["results"][0]["activity_id"], json!(source));
    for target in [other, model_id, list, ActivityId::new()] {
        let (_, feedback) = dispatch(
            &app,
            &mut worker,
            run,
            "read_result",
            json!({"activity_id":target}),
        )
        .await;
        assert_eq!(feedback["ok"], false);
        assert_eq!(feedback["error"]["code"], "result_unavailable");
    }
    let (_, feedback) = dispatch(
        &app,
        &mut worker,
        run,
        "list_results",
        json!({"after":other}),
    )
    .await;
    assert_eq!(feedback["error"]["code"], "result_unavailable");
    for args in [
        json!({"activity_id":source,"offset":2}),
        json!({"activity_id":source,"offset":999}),
    ] {
        let (_, feedback) = dispatch(&app, &mut worker, run, "read_result", args).await;
        assert_eq!(feedback["error"]["code"], "result_offset");
    }
    for args in [
        json!({"activity_id":"not-an-id"}),
        json!({"activity_id":source,"max_bytes":0}),
    ] {
        let (_, feedback) = dispatch(&app, &mut worker, run, "read_result", args).await;
        assert_eq!(feedback["error"]["code"], "result_input");
    }
    let read = queue(&app, run, "read_result", json!({"activity_id":source})).await;
    assert!(matches!(
        app.store
            .read_result(
                scope(foreign, read),
                ReadResultQuery {
                    activity_id: other,
                    offset: None,
                    max_bytes: None
                }
            )
            .await,
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        app.store
            .list_results(scope(run, model_id), Default::default())
            .await,
        Err(Error::Unavailable(_))
    ));
    let definition = ReadResultTool::new(app.store.clone()).definition();
    assert!(
        definition
            .validate_arguments(&json!({"activity_id":source,"run_id":foreign}))
            .is_err()
    );
    assert!(
        ListResultsTool::new(app.store.clone())
            .definition()
            .validate_arguments(&json!({"run_id":foreign}))
            .is_err()
    );
    assert_eq!(app.executions.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn recall_intent_horizon_excludes_later_completion_even_when_the_source_was_queued_first() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("horizon.db")).await;
    let run = app
        .engine
        .start(
            Batch.id(),
            json!([
                spec(&app, "echo", json!({"text":"{\"later\":true}"})),
                spec(&app, "list_results", json!({}))
            ]),
            20,
        )
        .await
        .unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    let source = worker.admit().await.unwrap().unwrap();
    worker
        .complete(source.clone(), worker.execute(source).await.unwrap())
        .await
        .unwrap();
    let caller = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(caller.clone()).await.unwrap();
    assert_eq!(outcome.result().unwrap()["results"], json!([]));
    worker.complete(caller.clone(), outcome).await.unwrap();
    assert!(
        app.store
            .list_results(scope(run, caller.activity_id), Default::default())
            .await
            .unwrap()
            .results
            .is_empty()
    );
}

#[tokio::test]
async fn recall_completion_rolls_back_and_duplicate_delivery_reuses_the_saved_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let app = open(&db).await;
    let run = start(&app).await;
    let mut worker = app.engine.worker().await.unwrap();
    let (source, _) = dispatch(
        &app,
        &mut worker,
        run,
        "echo",
        json!({"text":"{\"saved\":true}"}),
    )
    .await;
    let caller = queue(&app, run, "read_result", json!({"activity_id":source})).await;
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.activity_id, caller);
    let before = json!(app.engine.status(run).await.unwrap());
    let history = json!(app.engine.history(run).await.unwrap());
    let outcome = worker.execute(execution.clone()).await.unwrap();
    assert_eq!(json!(app.engine.status(run).await.unwrap()), before);
    assert_eq!(json!(app.engine.history(run).await.unwrap()), history);
    let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}", db.display()))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_recall BEFORE UPDATE ON activities WHEN json_extract(NEW.payload,'$.spec.input.definition.id.name')='read_result' AND NEW.status='succeeded' BEGIN SELECT RAISE(ABORT,'recall rollback'); END")
        .execute(&pool).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(json!(app.engine.status(run).await.unwrap()), before);
    assert_eq!(json!(app.engine.history(run).await.unwrap()), history);
    sqlx::query("DROP TRIGGER reject_recall")
        .execute(&pool)
        .await
        .unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    let after = json!(app.engine.status(run).await.unwrap());
    let history = json!(app.engine.history(run).await.unwrap());
    worker.complete(execution, outcome).await.unwrap();
    assert_eq!(json!(app.engine.status(run).await.unwrap()), after);
    assert_eq!(json!(app.engine.history(run).await.unwrap()), history);
    assert_eq!(app.executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn explicitly_reconciled_results_can_be_recalled_without_repeating_the_original_effect() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("accepted.db")).await;
    let run = start(&app).await;
    let mut source = spec(&app, "echo", json!({"text":"{\"accepted\":true}"}));
    // A workflow may conservatively require reconciliation for a known-safe fixture.
    source.recovery = RecoveryPolicy::RequiresReconciliation;
    let saved = app.engine.status(run).await.unwrap();
    app.engine
        .signal(run, saved.record().wait.as_ref().unwrap().id, json!(source))
        .await
        .unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let returned = worker
        .execute(execution.clone())
        .await
        .unwrap()
        .result()
        .unwrap()
        .clone();
    drop(worker);
    let mut worker = app.engine.worker().await.unwrap();
    assert_eq!(
        app.engine.status(run).await.unwrap().record().status,
        RunStatus::Blocked
    );
    app.engine
        .reconcile(
            run,
            ReconciliationRequest {
                activity_id: execution.activity_id,
                attempt_id: execution.attempt_id,
                decision: ReconciliationDecision::Accept {
                    result: returned.clone(),
                },
                evidence:
                    "Verified captured fixture result before accepting its uncertain completion."
                        .into(),
            },
        )
        .await
        .unwrap();
    let (_, listed) = dispatch(&app, &mut worker, run, "list_results", json!({})).await;
    assert_eq!(
        listed["results"][0]["activity_id"],
        json!(execution.activity_id)
    );
    let (_, recalled) = dispatch(
        &app,
        &mut worker,
        run,
        "read_result",
        json!({"activity_id":execution.activity_id}),
    )
    .await;
    assert_eq!(
        serde_json::from_str::<Value>(recalled["text"].as_str().unwrap()).unwrap(),
        returned
    );
    assert_eq!(app.executions.load(Ordering::SeqCst), 1);
}
