use super::*;
use act0r::domain::{RunId, SessionId};
use std::{
    future::IntoFuture,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

pub(super) struct Model {
    pub(super) endpoint: String,
    pub(super) requests: Arc<Mutex<Vec<Value>>>,
    pub(super) fail: Arc<AtomicBool>,
    task: JoinHandle<std::io::Result<()>>,
}
impl Model {
    pub(super) async fn start() -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let fail = Arc::new(AtomicBool::new(false));
        let seen = requests.clone();
        let failure = fail.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let router=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
            let prompt:Value=serde_json::from_str(body["messages"].as_array().unwrap().last().unwrap()["content"].as_str().unwrap()).unwrap();
            seen.lock().unwrap().push(prompt.clone());
            let summarizing=prompt.get("source_turns").is_some();
            let action=if summarizing && failure.load(Ordering::SeqCst) {
                json!({"kind":"finish","summary":" "})
            } else if summarizing {
                json!({"kind":"finish","summary":"Saved decisions: retain durable history and use Rust. 🦀"})
            } else if prompt["goal"].as_str().is_some_and(|s|s.starts_with("Ask before")) && prompt["turn"]==0 {
                json!({"kind":"ask_human","question":"May I continue?"})
            } else if prompt["goal"].as_str().is_some_and(|s|s.starts_with("Delegate")) && prompt["turn"]==0 && prompt.get("workers").is_some() {
                json!({"kind":"delegate_task","worker":"helper","goal":"Check durable history"})
            } else { json!({"kind":"finish","summary":"Keep durable history. Use Rust."}) };
            async move {axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":action}).to_string()}}],"usage":{"prompt_tokens":80,"completion_tokens":20}}))}
        }));
        let task = tokio::spawn(axum::serve(listener, router).into_future());
        Self {
            endpoint,
            requests,
            fail,
            task,
        }
    }
    pub(super) fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}
impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(super) fn input(endpoint: &str) -> Value {
    json!({"message_id":MessageId::new(),"prompt":"Remember our durable history and Rust decision","target":{"provider":"openai","model":"summary-fixture","endpoint":endpoint,"protocol":"chat_completions","policy":{"retry_delay_ms":0,"max_retry_delay_ms":0}}})
}
pub(super) fn decision(view: &Value) -> Value {
    json!({"request_id":MessageId::new(),"expected_revision":view["revision"],"through_sequence":view["next_through_sequence"]})
}
pub(super) async fn pool(path: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(path)
            .foreign_keys(true),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn summary_survives_restart_exact_retries_and_pins_future_planner_and_helper_context() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("summary.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let session = first["session_id"].as_str().unwrap();
    let mut base = format!("{}/v1/harness/sessions/{session}", server.url);
    let unsettled = get(&client, &format!("{base}/summary")).await;
    assert!(
        unsettled["blocked_reason"]
            .as_str()
            .unwrap()
            .contains("active")
    );
    post(
        &client,
        &format!("{base}/summary"),
        &json!({"request_id":MessageId::new(),"expected_revision":0,"through_sequence":1}),
        StatusCode::CONFLICT,
    )
    .await;
    settle(&engine).await;
    let old = get(
        &client,
        &format!("{base}/turns/{}/memory", first["task_id"].as_str().unwrap()),
    )
    .await;
    let ready = get(&client, &format!("{base}/summary")).await;
    assert_eq!(ready["next_sources"][0]["task_id"], first["task_id"]);
    assert!(ready["budget"]["max_model_calls"].is_null());
    let request = decision(&ready);
    let receipt = post(
        &client,
        &format!("{base}/summary"),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(model.count(), 1, "admission does not call model inline");
    assert_eq!(
        get(&client, &format!("{base}/summary")).await["pending"],
        true
    );
    post(
        &client,
        &format!("{base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Too early"}),
        StatusCode::CONFLICT,
    )
    .await;
    // Idempotent replays of an existing turn still work during summary processing.
    let mut original = input(&model.endpoint);
    original["message_id"] = first["task_id"].clone();
    post(
        &client,
        &format!("{base}/turns"),
        &json!({"message_id":original["message_id"],"prompt":original["prompt"]}),
        StatusCode::ACCEPTED,
    )
    .await;
    let metadata = get(&client, &format!("{base}/metadata")).await;
    assert!(
        metadata["archive_blocked_reason"]
            .as_str()
            .unwrap()
            .contains("summary")
    );
    post(
        &client,
        &format!("{base}/metadata"),
        &json!({"kind":"archive","expected_revision":metadata["revision"]}),
        StatusCode::CONFLICT,
    )
    .await;
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    base = format!("{}/v1/harness/sessions/{session}", server.url);
    let replay = post(
        &client,
        &format!("{base}/summary"),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(replay["run_id"], receipt["run_id"]);
    assert_eq!(replay["replayed"], true);
    settle(&engine).await;
    assert_eq!(model.count(), 2);
    let saved = get(&client, &format!("{base}/summary")).await;
    assert_eq!(saved["pending"], false);
    assert_eq!(saved["summary"]["through_sequence"], 1);
    assert_eq!(saved["last_attempt"]["usage"]["admitted_model_calls"], 1);
    let run = engine
        .status(
            receipt["run_id"]
                .as_str()
                .unwrap()
                .parse::<RunId>()
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        run.record().input["sources"][0]["prompt"],
        original["prompt"]
    );
    assert_eq!(
        run.record().input["sources"][0]["response"],
        "Keep durable history. Use Rust."
    );
    assert_eq!(
        get(
            &client,
            &format!("{base}/turns/{}/memory", first["task_id"].as_str().unwrap())
        )
        .await,
        old
    );
    let future = post(
        &client,
        &format!("{base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Delegate a history check"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let snapshots = get(
        &client,
        &format!(
            "{base}/turns/{}/memory",
            future["task_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(snapshots["runs"].as_array().unwrap().len(), 2);
    for run in snapshots["runs"].as_array().unwrap() {
        assert_eq!(run["session_summary"], saved["summary"]);
    }
    let prompts = model.requests.lock().unwrap().clone();
    for prompt in &prompts[2..] {
        assert_eq!(prompt["session_summary"], saved["summary"]);
    }
    let next = get(&client, &format!("{base}/summary")).await;
    assert_eq!(next["next_sources"].as_array().unwrap().len(), 1);
    let next_receipt = post(
        &client,
        &format!("{base}/summary"),
        &decision(&next),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let folded = get(&client, &format!("{base}/summary")).await;
    assert_eq!(folded["summary"]["previous_run_id"], receipt["run_id"]);
    assert_eq!(folded["summary"]["through_sequence"], 2);
    assert_eq!(folded["summary"]["run_id"], next_receipt["run_id"]);
    post(
        &client,
        &format!("{base}/summary"),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(
        get(&client, &format!("{base}/summary")).await,
        folded,
        "old retries cannot replace newer summaries"
    );
    server.close().await;
}

#[tokio::test]
async fn failed_summary_preserves_previous_context_and_conflicting_scope_or_revision_is_refused() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("failure.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        first["session_id"].as_str().unwrap()
    );
    settle(&engine).await;
    let ready = get(&client, &format!("{base}/summary")).await;
    let request = decision(&ready);
    post(
        &client,
        &format!("{base}/summary"),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let saved = get(&client, &format!("{base}/summary")).await["summary"].clone();
    post(
        &client,
        &format!("{base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Another decision"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let ready = get(&client, &format!("{base}/summary")).await;
    let second = decision(&ready);
    model.fail.store(true, Ordering::SeqCst);
    post(
        &client,
        &format!("{base}/summary"),
        &second,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let failed = get(&client, &format!("{base}/summary")).await;
    assert_eq!(failed["summary"], saved);
    assert_eq!(failed["pending"], false);
    assert_eq!(failed["revision"], 2);
    assert_eq!(failed["last_attempt"]["status"], "failed");
    assert_eq!(failed["next_sources"][0]["sequence"], 2);
    let mut stale = second.clone();
    stale["request_id"] = json!(MessageId::new());
    post(
        &client,
        &format!("{base}/summary"),
        &stale,
        StatusCode::CONFLICT,
    )
    .await;
    let mut changed = second.clone();
    changed["through_sequence"] = json!(1);
    post(
        &client,
        &format!("{base}/summary"),
        &changed,
        StatusCode::CONFLICT,
    )
    .await;
    post(
        &client,
        &format!(
            "{}/v1/harness/sessions/{}/summary",
            server.url,
            SessionId::new()
        ),
        &second,
        StatusCode::CONFLICT,
    )
    .await;
    model.fail.store(false, Ordering::SeqCst);
    let retry = post(
        &client,
        &format!("{base}/summary"),
        &decision(&failed),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        get(&client, &format!("{base}/summary")).await["summary"]["run_id"],
        retry["run_id"]
    );
    server.close().await;
}

#[tokio::test]
async fn concurrent_summary_admission_and_failed_transactions_leave_no_orphan_runs() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("atomic.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        first["session_id"].as_str().unwrap()
    );
    settle(&engine).await;
    let ready = get(&client, &format!("{base}/summary")).await;
    let conn = pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_summary BEFORE INSERT ON harness_summary_heads BEGIN SELECT RAISE(ABORT,'injected admission failure'); END;").execute(&conn).await.unwrap();
    let response = client
        .post(format!("{base}/summary"))
        .json(&decision(&ready))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM runs WHERE json_extract(payload,'$.workflow.name')='session_summary'").fetch_one(&conn).await.unwrap(),0);
    sqlx::raw_sql("DROP TRIGGER reject_summary;")
        .execute(&conn)
        .await
        .unwrap();
    let req1 = decision(&ready);
    let req2 = decision(&ready);
    let (one, two) = tokio::join!(
        client.post(format!("{base}/summary")).json(&req1).send(),
        client.post(format!("{base}/summary")).json(&req2).send()
    );
    let one = one.unwrap();
    let two = two.unwrap();
    assert!(
        (one.status() == StatusCode::ACCEPTED && two.status() == StatusCode::CONFLICT)
            || (two.status() == StatusCode::ACCEPTED && one.status() == StatusCode::CONFLICT)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM harness_summary_requests")
            .fetch_one(&conn)
            .await
            .unwrap(),
        1
    );
    sqlx::raw_sql("CREATE TRIGGER reject_publish BEFORE UPDATE ON harness_summary_heads WHEN NEW.pending_run_id IS NULL BEGIN SELECT RAISE(ABORT,'injected publication failure'); END;").execute(&conn).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.step().await.is_err());
    drop(worker);
    let pending = get(&client, &format!("{base}/summary")).await;
    assert_eq!(pending["pending"], true);
    assert!(pending["summary"].is_null());
    let run = engine
        .status(
            pending["last_attempt"]["run_id"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(run.record().status, act0r::domain::RunStatus::Succeeded);
    assert!(run.record().result.is_none());
    assert_eq!(
        run.record().usage.actual_input_tokens,
        0,
        "model outcome and usage rolled back with publication"
    );
    sqlx::raw_sql("DROP TRIGGER reject_publish;")
        .execute(&conn)
        .await
        .unwrap();
    conn.close().await;
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    settle(&engine).await;
    let recovered = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .summary(first["session_id"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap();
    assert!(!recovered.pending);
    assert_eq!(recovered.summary.unwrap().run_id, run.record().id);
    assert_eq!(
        model.count(),
        3,
        "uncommitted external model result can repeat safely after ownership recovery"
    );
}

#[tokio::test]
async fn summaries_batch_whole_turns_and_refuse_oversized_sources_without_skipping_them() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("bounded.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        first["session_id"].as_str().unwrap()
    );
    for sequence in 2..=33 {
        post(
            &client,
            &format!("{base}/turns"),
            &json!({"message_id":MessageId::new(),"prompt":format!("Decision {sequence} 🦀")}),
            StatusCode::ACCEPTED,
        )
        .await;
    }
    settle(&engine).await;
    let view = get(&client, &format!("{base}/summary")).await;
    assert_eq!(view["remaining_turns"], 33);
    assert_eq!(view["next_through_sequence"], 32);
    assert_eq!(view["next_sources"].as_array().unwrap().len(), 32);
    post(
        &client,
        &format!("{base}/summary"),
        &decision(&view),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let next = get(&client, &format!("{base}/summary")).await;
    assert_eq!(next["next_sources"][0]["sequence"], 33);
    assert_eq!(next["remaining_turns"], 1);
    post(
        &client,
        &format!("{base}/summary"),
        &decision(&next),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        get(&client, &format!("{base}/summary")).await["summary"]["through_sequence"],
        33
    );
    let mut oversized = input(&model.endpoint);
    oversized["prompt"] = json!("é".repeat(25000));
    let large = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &oversized,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let large_base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        large["session_id"].as_str().unwrap()
    );
    post(
        &client,
        &format!("{large_base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"A small later turn"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let view = get(&client, &format!("{large_base}/summary")).await;
    assert!(view["blocked_reason"].as_str().unwrap().contains("48 KiB"));
    assert!(view["next_sources"].as_array().unwrap().is_empty());
    assert_eq!(view["remaining_turns"], 2);
    post(
        &client,
        &format!("{large_base}/summary"),
        &json!({"request_id":MessageId::new(),"expected_revision":0,"through_sequence":2}),
        StatusCode::CONFLICT,
    )
    .await;
    server.close().await;
}
