//! Native model choices exercise durable peer notes through real actor assignments.
use super::worker_recovery::{admit, answer, child, work};
use super::*;
use act0r::{
    adapters::sqlite::SqliteStore,
    application::ports::WorkerMessagePort,
    domain::{
        ActivityExecution, DefinitionId, RunId,
        tools::{NativeToolCall, NativeToolRequest},
    },
};
use std::sync::Mutex;

struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl Model {
    async fn start(notes: Vec<Value>) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let prompt: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                seen.lock().unwrap().push(prompt.clone());
                let turn = prompt["turn"].as_u64().unwrap() as usize;
                let action = match prompt["goal"].as_str().unwrap() {
                    "Worker 0" if turn == 0 => json!({"kind":"ask_human","question":"Start sending findings?"}),
                    "Worker 0" if turn <= notes.len() => json!({"kind":"call_tool","tool":"send_worker_message","arguments":notes[turn-1]}),
                    "Worker 1" if turn <= 1 => json!({"kind":"ask_human","question":"Continue after peer notes?"}),
                    "Worker 0" | "Worker 1" => json!({"kind":"finish_incomplete","summary":"Fixture retains saved evidence"}),
                    _ if turn == 0 => json!({"kind":"delegate_tasks","tasks":[{"worker":"coder","goal":"Worker 0","writable_paths":["src/"]},{"worker":"coder2","goal":"Worker 1","writable_paths":["src/"]}]}),
                    _ => json!({"kind":"finish_incomplete","summary":"Parent received the joined outcomes"}),
                };
                async move { axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}})) }
            }));
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }
}
impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn input(model: &Model, root: &Path) -> Value {
    let project = root.join("project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(project.join("src/lib.rs"), "Saved source\n").unwrap();
    json!({"message_id":MessageId::new(),"prompt":"Coordinate independent repairs","workspace":fs::canonicalize(project).unwrap(),
        "target":{"provider":"openai","model":"worker-message-fixture","endpoint":model.endpoint,"protocol":"chat_completions"},
        "coding":{"writable_paths":["src/"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000},"coder_count":2}})
}
async fn start(server: &Server, model: &Model, root: &Path) -> Value {
    post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input(model, root),
        StatusCode::ACCEPTED,
    )
    .await
}
fn base(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn run(child: &Value) -> RunId {
    child["run_id"].as_str().unwrap().parse().unwrap()
}
fn call(execution: &ActivityExecution) -> NativeToolCall {
    let r: NativeToolRequest = serde_json::from_value(execution.input.clone()).unwrap();
    NativeToolCall {
        run_id: execution.run_id,
        activity_id: execution.activity_id,
        attempt_id: execution.attempt_id,
        arguments: r.arguments,
        workspace: r.workspace,
        writable_paths: r.writable_paths,
        test_command: r.test_command,
        project_command: r.project_command,
    }
}

#[tokio::test]
async fn peer_and_parent_notes_survive_pauses_restart_and_join_without_creating_tasks() {
    let model = Model::start(vec![
        json!({"recipient":"coder2","text":"Keep the response field named id."}),
        json!({"recipient":"parent","text":"The client expects id, not identifier."}),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("messages.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let sender = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    let held = format!(
        "{}/workers/{}/pause",
        base(&server, &receipt),
        recipient["task_id"].as_str().unwrap()
    );
    post(
        &client(),
        &held,
        &json!({"request_id":MessageId::new(),"expected_revision":0,"paused":true}),
        StatusCode::ACCEPTED,
    )
    .await;
    let pool = summary::pool(&db).await;
    let tasks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    answer(&server, &receipt, &sender).await;
    settle(&engine).await;
    let saved = child(&server, &receipt, 1).await;
    assert_eq!(saved["messages"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(saved["messages"]["items"][0]["application"], Value::Null);
    assert_eq!(
        saved["messages"]["items"][0]["receipt"]["recipient_task_id"],
        recipient["task_id"]
    );
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    let before = engine.status(run(&recipient)).await.unwrap();
    post(&client(), &format!("{}/workers/{}/steering",base(&server,&receipt),recipient["task_id"].as_str().unwrap()),
        &json!({"message_id":MessageId::new(),"run_id":recipient["run_id"],"text":"Keep changes within the assigned paths."}),StatusCode::ACCEPTED).await;
    pool.close().await;
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(
        child(&server, &receipt, 1).await["messages"],
        saved["messages"]
    );
    let path = format!(
        "{}/workers/{}/pause",
        base(&server, &receipt),
        recipient["task_id"].as_str().unwrap()
    );
    post(
        &client(),
        &path,
        &json!({"request_id":MessageId::new(),"expected_revision":1,"paused":false}),
        StatusCode::ACCEPTED,
    )
    .await;
    answer(&server, &receipt, &recipient).await;
    settle(&engine).await;
    let delivered = child(&server, &receipt, 1).await;
    assert!(delivered["messages"]["items"][0]["application"]["activity_id"].is_string());
    let after = engine.status(run(&recipient)).await.unwrap();
    assert_eq!(after.record().peer_messages.len(), 1);
    assert_eq!(
        after.record().usage.admitted_model_calls,
        before.record().usage.admitted_model_calls + 1
    );
    let seen = model.requests.lock().unwrap().clone();
    let prompt = seen
        .iter()
        .find(|p| p["goal"] == "Worker 1" && p["turn"] == 1)
        .unwrap();
    assert_eq!(prompt["peer_messages"][0]["sender"], "coder");
    assert_eq!(
        prompt["peer_messages"][0]["text"],
        "Keep the response field named id."
    );
    assert_eq!(
        prompt["steering"][0]["text"],
        "Keep changes within the assigned paths."
    );
    answer(&server, &receipt, &recipient).await;
    settle(&engine).await;
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    assert!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|p| p["goal"] == "Coordinate independent repairs"
                && p["turn"] == 1
                && p["peer_messages"][0]["sender"] == "coder")
    );
    let pool = summary::pool(&db).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_tasks")
            .fetch_one(&pool)
            .await
            .unwrap(),
        tasks
    );
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn note_and_receipt_roll_back_together_and_an_uncommitted_sender_attempt_replays_after_restart()
 {
    let model = Model::start(vec![json!({"recipient":"coder2","text":"Stable finding"})]).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("atomic.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let sender = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &sender).await;
    let mut worker = engine.worker().await.unwrap();
    let model_step = admit(&mut worker).await;
    assert_eq!(model_step.handler, DefinitionId::new("agent.model", 1));
    let outcome = worker.execute(model_step.clone()).await.unwrap();
    worker.complete(model_step, outcome).await.unwrap();
    let execution = admit(&mut worker).await;
    assert_eq!(
        execution.input["definition"]["id"]["name"],
        "send_worker_message"
    );
    let before = engine
        .status(run(&recipient))
        .await
        .unwrap()
        .record()
        .history_sequence;
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER fail_note BEFORE INSERT ON harness_worker_messages BEGIN SELECT RAISE(ABORT,'fixture rollback'); END;").execute(&pool).await.unwrap();
    assert!(worker.execute(execution.clone()).await.is_err());
    assert!(
        engine
            .status(run(&recipient))
            .await
            .unwrap()
            .record()
            .peer_messages
            .is_empty()
    );
    assert_eq!(
        engine
            .status(run(&recipient))
            .await
            .unwrap()
            .record()
            .history_sequence,
        before
    );
    sqlx::raw_sql("DROP TRIGGER fail_note;")
        .execute(&pool)
        .await
        .unwrap();
    let (first, duplicate) = tokio::join!(
        worker.execute(execution.clone()),
        worker.execute(execution.clone())
    );
    assert_eq!(
        serde_json::to_value(first.unwrap()).unwrap(),
        serde_json::to_value(duplicate.unwrap()).unwrap()
    );
    assert_eq!(
        engine
            .status(run(&recipient))
            .await
            .unwrap()
            .record()
            .peer_messages
            .len(),
        1
    );
    let store = SqliteStore::open(&db).await.unwrap();
    let mut changed = call(&execution);
    changed.arguments["text"] = json!("Changed finding");
    assert!(store.send_worker_message(changed).await.is_err());
    store.close().await;
    pool.close().await;
    drop(worker);
    server.close().await;
    drop(engine);
    // The accepted note committed; the sending activity's completion deliberately did not.
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let mut worker = engine.worker().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let replay = admit(&mut worker).await;
    assert_eq!(replay.activity_id, execution.activity_id);
    assert_ne!(replay.attempt_id, execution.attempt_id);
    let outcome = worker.execute(replay.clone()).await.unwrap();
    worker.complete(replay, outcome).await.unwrap();
    drop(worker);
    settle(&engine).await;
    assert_eq!(
        engine
            .status(run(&recipient))
            .await
            .unwrap()
            .record()
            .peer_messages
            .len(),
        1
    );
    assert_eq!(
        child(&server, &receipt, 0).await["messages"]["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    server.close().await;
}

#[tokio::test]
async fn unknown_self_and_helper_recipients_and_invalid_text_are_saved_feedback() {
    let model = Model::start(vec![
        json!({"recipient":"coder","text":"Self"}),
        json!({"recipient":"helper","text":"Other role"}),
        json!({"recipient":"unknown","text":"Unknown"}),
        json!({"recipient":"coder2","text":" "}),
        json!({"recipient":"coder2","text":"x".repeat(2049)}),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("refused.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let sender = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &sender).await;
    settle(&engine).await;
    let items = child(&server, &receipt, 0).await["messages"]["items"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(items.len(), 5);
    assert!(
        items
            .iter()
            .all(|i| i["receipt"]["error"].is_object() && i["receipt"]["message"].is_null())
    );
    assert_eq!(items[0]["receipt"]["error"]["code"], "message_recipient");
    assert_eq!(items[3]["receipt"]["error"]["code"], "message_input");
    assert!(
        child(&server, &receipt, 1).await["messages"]["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    server.close().await;
}

#[tokio::test]
async fn stopped_recipients_keep_pending_notes_without_delivery_or_reassignment() {
    let model = Model::start(vec![
        json!({"recipient":"coder2","text":"Finding before stop"}),
        json!({"recipient":"coder2","text":"Finding after stop"}),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("stop.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let sender = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &sender).await;
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..2 {
        let e = admit(&mut worker).await;
        let out = worker.execute(e.clone()).await.unwrap();
        worker.complete(e, out).await.unwrap();
    }
    drop(worker);
    post(
        &client(),
        &format!(
            "{}/workers/{}/cancellation",
            base(&server, &receipt),
            recipient["task_id"].as_str().unwrap()
        ),
        &json!({"request_id":MessageId::new()}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let items = child(&server, &receipt, 0).await["messages"]["items"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["recipient_terminal"], true);
    assert_eq!(items[0]["application"], Value::Null);
    assert_eq!(
        items[1]["receipt"]["error"]["code"],
        "message_recipient_closed"
    );
    assert_eq!(
        engine
            .status(run(&recipient))
            .await
            .unwrap()
            .record()
            .peer_messages
            .len(),
        1
    );
    server.close().await;
}

#[tokio::test]
async fn peer_context_and_send_counts_are_bounded_without_reseting_usage_or_the_join() {
    let model = Model::start(
        (0..33)
            .map(|_| json!({"recipient":"coder2","text":"x"}))
            .collect(),
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("limits.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let sender = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &sender).await;
    settle(&engine).await;
    let items = child(&server, &receipt, 0).await["messages"]["items"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(items.len(), 33);
    assert_eq!(
        items
            .iter()
            .filter(|i| i["receipt"]["message"].is_object())
            .count(),
        16
    );
    assert_eq!(items[32]["receipt"]["error"]["code"], "message_capacity");
    assert_eq!(
        engine
            .status(run(&sender))
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        35
    );
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    server.close().await;
}

#[tokio::test]
async fn messages_wait_for_a_fresh_request_after_exact_model_failure_recovery() {
    let model = Model::start(vec![
        json!({"recipient":"coder2","text":"Finding saved during model recovery"}),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("model-retry.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let sender = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &recipient).await;
    let mut worker = engine.worker().await.unwrap();
    let e = admit(&mut worker).await;
    assert_eq!(e.run_id, run(&recipient));
    let frozen = e.input.clone();
    super::worker_recovery::fail(&mut worker, e, "model_invalid_action").await;
    drop(worker);
    answer(&server, &receipt, &sender).await;
    settle(&engine).await;
    let waiting = child(&server, &receipt, 1).await;
    assert_eq!(waiting["messages"]["items"][0]["application"], Value::Null);
    assert!(waiting["messages"]["items"][0]["receipt"]["message"].is_object());
    let path = format!(
        "{}/workers/{}/recovery",
        base(&server, &receipt),
        recipient["task_id"].as_str().unwrap()
    );
    let checkpoint = get(&client(), &path).await;
    post(&client(), &path, &json!({"request_id":MessageId::new(),"run_id":checkpoint["run_id"],"activity_id":checkpoint["activity_id"],"attempt_id":checkpoint["attempt_id"],"expected_history_sequence":checkpoint["history_sequence"],"decision":"retry"}),StatusCode::ACCEPTED).await;
    let mut worker = engine.worker().await.unwrap();
    let replay = admit(&mut worker).await;
    assert_eq!(replay.input, frozen);
    let outcome = worker.execute(replay.clone()).await.unwrap();
    worker.complete(replay, outcome).await.unwrap();
    drop(worker);
    assert_eq!(
        child(&server, &receipt, 1).await["messages"]["items"][0]["application"],
        Value::Null
    );
    answer(&server, &receipt, &recipient).await;
    settle(&engine).await;
    assert!(child(&server, &receipt, 1).await["messages"]["items"][0]["application"].is_object());
    assert!(
        model
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|p| p["goal"] == "Worker 1"
                && p["turn"] == 2
                && p["peer_messages"][0]["text"] == "Finding saved during model recovery")
    );
    server.close().await;
}

#[tokio::test]
async fn escaped_text_is_acknowledged_compactly_and_serialized_peer_context_is_bounded() {
    let text = "\u{0001}".repeat(2048);
    let model = Model::start(vec![
        json!({"recipient":"coder2","text":text}),
        json!({"recipient":"coder2","text":text}),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("escaped.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let sender = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &sender).await;
    settle(&engine).await;
    let messages = child(&server, &receipt, 0).await["messages"]["items"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["receipt"]["message"]["text"], text);
    assert_eq!(
        messages[1]["receipt"]["error"]["code"],
        "message_recipient_unavailable"
    );
    let source = engine.status(run(&sender)).await.unwrap();
    let sends: Vec<_> = source
        .activities()
        .iter()
        .filter(|a| a.spec.input["definition"]["id"]["name"] == "send_worker_message")
        .collect();
    assert_eq!(sends.len(), 2);
    assert!(sends[0].result.as_ref().unwrap()["ok"] == true);
    assert!(sends[0].result.as_ref().unwrap().to_string().len() < 1024);
    assert_eq!(
        engine
            .status(run(&child(&server, &receipt, 1).await))
            .await
            .unwrap()
            .record()
            .peer_messages
            .len(),
        1
    );
    server.close().await;
}
