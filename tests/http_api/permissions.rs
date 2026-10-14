use super::*;
use act0r::domain::{RunId, SessionId};
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

struct Model {
    endpoint: String,
    calls: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}
impl Model {
    async fn start(actions: Vec<Value>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener,axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let index=count.fetch_add(1,Ordering::SeqCst);
                let action=actions.get(index).unwrap_or_else(|| panic!("unexpected model call {index}")).clone();
                async move {axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
            }))).await.unwrap();
        });
        Self {
            endpoint,
            calls,
            task,
        }
    }
}
impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn tests() -> Value {
    json!({"kind":"call_tool","tool":"run_tests","arguments":{}})
}
fn finish() -> Value {
    json!({"kind":"finish","summary":"Verified by the saved test command"})
}
async fn start(server: &Server, model: &Model, root: &Path) -> Value {
    let project = root.join("project");
    fs::create_dir_all(&project).unwrap();
    fs::write(project.join("README.md"), "Before\n").unwrap();
    post(&client(),&format!("{}/v1/harness/sessions",server.url),&json!({
        "message_id":MessageId::new(),"prompt":"Test permissions","workspace":fs::canonicalize(project).unwrap(),
        "target":{"provider":"openai","model":"permission-fixture","endpoint":model.endpoint,"protocol":"chat_completions"},
        "coding":{"writable_paths":["README.md"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000}}
    }),StatusCode::ACCEPTED).await
}
fn goal(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn base(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn policy(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/permissions",
        server.url,
        receipt["session_id"].as_str().unwrap()
    )
}
async fn view(server: &Server, receipt: &Value) -> Value {
    get(&client(), &format!("{}/permissions", base(server, receipt))).await
}
fn allow(view: &Value) -> Value {
    json!({"request_id":Uuid::new_v4(),"expected_revision":view["state"]["revision"],"action":{"kind":"allow_test_command","task_id":view["eligible_wait"]["task_id"],"run_id":view["eligible_wait"]["run_id"],"wait_id":view["eligible_wait"]["wait_id"]}})
}
fn revoke(view: &Value) -> Value {
    json!({"request_id":Uuid::new_v4(),"expected_revision":view["state"]["revision"],"action":{"kind":"revoke_test_command","grant_id":view["state"]["test_command"]["grant_id"]}})
}
async fn approve(server: &Server, receipt: &Value) {
    let url = goal(server, receipt);
    let item = wait(
        &get(&client(), &format!("{url}/inbox")).await,
        "tool_approval",
    );
    answer(
        &client(),
        &url,
        &item,
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
}

#[tokio::test]
async fn test_permission_survives_restart_and_turns_and_revocation_restores_per_call_approval() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("permission.db");
    let model=Model::start(vec![tests(),tests(),finish(),tests(),json!({"kind":"ask_human","question":"Continue after revocation?"}),json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"README.md","old":"Before","new":"After"}}),tests(),finish(),tests()]).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let first = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let initial = view(&server, &first).await;
    assert_eq!(initial["state"]["revision"], 0);
    assert!(initial["state"]["test_command"].is_null());
    assert_eq!(
        initial["eligible_wait"]["request"]["test_command"]["argv"],
        json!(["rustc", "--version"])
    );
    // Approve once keeps the default policy, so the next test still waits.
    approve(&server, &first).await;
    settle(&engine).await;
    let current = view(&server, &first).await;
    assert_eq!(current["state"]["revision"], 0);
    assert_ne!(
        initial["eligible_wait"]["wait_id"],
        current["eligible_wait"]["wait_id"]
    );
    let request = allow(&current);
    let http = client();
    let url = policy(&server, &first);
    let (a, b) = tokio::join!(
        post(&http, &url, &request, StatusCode::OK),
        post(&http, &url, &request, StatusCode::OK)
    );
    assert_ne!(a["replayed"], b["replayed"]);
    assert_eq!(a["state"], b["state"]);
    let id: RunId = current["eligible_wait"]["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(engine.status(id).await.unwrap().record().wait.is_none());
    // Restart before executing the newly authorized test.
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    assert_eq!(view(&server, &first).await["state"], a["state"]);
    settle(&engine).await;
    assert_eq!(
        get(&client(), &format!("{}/changes", base(&server, &first))).await["completion"]["status"],
        "verified"
    );
    let second = post(
        &client(),
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            first["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"Continue testing"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let inbox = get(&client(), &format!("{}/inbox", goal(&server, &second))).await;
    let question = wait(&inbox, "question");
    assert_eq!(model.calls.load(Ordering::SeqCst), 5);
    let second_run: RunId = question["run_id"].as_str().unwrap().parse().unwrap();
    let history = engine.history(second_run).await.unwrap();
    let authorization = history
        .iter()
        .find(|e| e.kind == "test_command_authorized")
        .unwrap();
    assert_eq!(authorization.payload["grant_id"], request["request_id"]);
    assert_eq!(
        authorization.payload["request"]["test_command"]["argv"],
        json!(["rustc", "--version"])
    );
    let saved_run = engine.status(second_run).await.unwrap();
    let test = saved_run
        .activities()
        .iter()
        .find(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
        .unwrap();
    assert_eq!(test.result.as_ref().unwrap()["ok"], true);
    assert!(
        history
            .iter()
            .any(|e| e.kind == "attempt_finished" && e.activity_id == Some(test.id))
    );
    let before_revoke = view(&server, &second).await;
    let stop = revoke(&before_revoke);
    post(&client(), &policy(&server, &second), &stop, StatusCode::OK).await;
    // A retry of the OLD grant recovers its original receipt; it never enables permission again.
    let replay = post(
        &client(),
        &policy(&server, &first),
        &request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay["replayed"], true);
    assert!(view(&server, &second).await["state"]["test_command"].is_null());
    answer(
        &client(),
        &goal(&server, &second),
        &question,
        json!({"kind":"answer","answer":"Continue"}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let edit = wait(
        &get(&client(), &format!("{}/inbox", goal(&server, &second))).await,
        "tool_approval",
    );
    assert_eq!(edit["wait"]["payload"]["tool"]["name"], "replace_text");
    assert!(view(&server, &second).await["eligible_wait"].is_null());
    approve(&server, &second).await;
    settle(&engine).await;
    assert!(view(&server, &second).await["eligible_wait"].is_object());
    approve(&server, &second).await;
    settle(&engine).await;
    assert_eq!(
        get(&client(), &format!("{}/changes", base(&server, &second))).await["completion"]["status"],
        "verified"
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let third = post(
        &client(),
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            first["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"Ask again"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let third_view = view(&server, &third).await;
    assert_eq!(third_view["state"]["revision"], 2);
    assert!(third_view["state"]["test_command"].is_null());
    assert!(third_view["eligible_wait"].is_object());
    assert_eq!(third_view["history"].as_array().unwrap().len(), 2);
    assert_eq!(
        fs::read_to_string(root.path().join("project/README.md")).unwrap(),
        "Before\n"
    );
    server.close().await;
}

#[tokio::test]
async fn permission_decisions_check_identity_scope_staleness_and_atomic_rollback() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("rollback.db");
    let model = Model::start(vec![tests(), tests()]).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let first = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let current = view(&server, &first).await;
    let request = allow(&current);
    let other = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let other_request = allow(&view(&server, &other).await);
    post(
        &client(),
        &policy(&server, &other),
        &request,
        StatusCode::NOT_FOUND,
    )
    .await;
    let mut stale = request.clone();
    stale["action"]["run_id"] = json!(RunId::new());
    post(
        &client(),
        &policy(&server, &first),
        &stale,
        StatusCode::CONFLICT,
    )
    .await;
    let mut invalid = request.clone();
    invalid["action"]["command"] = json!(["anything"]);
    post(
        &client(),
        &policy(&server, &first),
        &invalid,
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    let id: RunId = current["eligible_wait"]["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let before = engine.status(id).await.unwrap();
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_permission BEFORE INSERT ON harness_permission_receipts BEGIN SELECT RAISE(ABORT,'permission receipt fixture'); END;").execute(&pool).await.unwrap();
    post(
        &client(),
        &policy(&server, &first),
        &request,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(
        serde_json::to_value(engine.status(id).await.unwrap()).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    assert_eq!(view(&server, &first).await["state"]["revision"], 0);
    sqlx::query("DROP TRIGGER reject_permission")
        .execute(&pool)
        .await
        .unwrap();
    let saved = post(
        &client(),
        &policy(&server, &first),
        &request,
        StatusCode::OK,
    )
    .await;
    let mut changed = request.clone();
    changed["expected_revision"] = json!(1);
    post(
        &client(),
        &policy(&server, &first),
        &changed,
        StatusCode::CONFLICT,
    )
    .await;
    post(
        &client(),
        &policy(&server, &other),
        &request,
        StatusCode::CONFLICT,
    )
    .await;
    let mut stale_revoke = revoke(&view(&server, &first).await);
    stale_revoke["expected_revision"] = json!(0);
    post(
        &client(),
        &policy(&server, &first),
        &stale_revoke,
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(view(&server, &first).await["state"], saved["state"]);
    // Cancellation rejects a fresh grant but leaves revocation available.
    post(
        &client(),
        &format!("{}/cancel", goal(&server, &other)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    post(
        &client(),
        &policy(&server, &other),
        &other_request,
        StatusCode::CONFLICT,
    )
    .await;
    // Use the previously reviewed valid wait: cancellation invalidates its authority.
    let mut cancelled = request.clone();
    cancelled["request_id"] = json!(Uuid::new_v4());
    cancelled["action"] = allow(&current)["action"].clone();
    // Wrong-session scope still cannot target another actor's root.
    post(
        &client(),
        &policy(&server, &other),
        &cancelled,
        StatusCode::NOT_FOUND,
    )
    .await;
    value(
        client()
            .get(format!(
                "{}/v1/harness/sessions/{}/turns/{}/permissions",
                server.url,
                SessionId::new(),
                first["task_id"].as_str().unwrap()
            ))
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn held_execution_and_changed_bindings_cannot_use_saved_test_permission() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("held.db");
    let model = Model::start(vec![tests(), tests(), finish()]).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let current = view(&server, &receipt).await;
    let request = allow(&current);
    let controls = get(&client(), &format!("{}/controls", goal(&server, &receipt))).await;
    post(
        &client(),
        &format!("{}/pause", goal(&server, &receipt)),
        &json!({"expected_revision":controls["pause"]["revision"]}),
        StatusCode::OK,
    )
    .await;
    post(
        &client(),
        &policy(&server, &receipt),
        &request,
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let controls = get(&client(), &format!("{}/controls", goal(&server, &receipt))).await;
    post(
        &client(),
        &format!("{}/resume", goal(&server, &receipt)),
        &json!({"expected_revision":controls["pause"]["revision"]}),
        StatusCode::OK,
    )
    .await;
    let id: RunId = current["eligible_wait"]["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    // Stop immediately after the next model decision creates its test wait, before automatic authorization.
    for _ in 0..30 {
        if engine.status(id).await.unwrap().record().wait.is_some() {
            break;
        }
        assert!(worker.step().await.unwrap());
    }
    let saved = engine.status(id).await.unwrap();
    assert!(saved.record().wait.is_some());
    let pool = summary::pool(&db).await;
    let session = receipt["session_id"].as_str().unwrap();
    let original: Value = serde_json::from_str(
        &sqlx::query_scalar::<_, String>(
            "SELECT payload FROM harness_session_permissions WHERE session_id=?",
        )
        .bind(session)
        .fetch_one(&pool)
        .await
        .unwrap(),
    )
    .unwrap();
    for path in ["workspace", "timeout", "contract", "actor", "session"] {
        let mut changed = original.clone();
        let grant = &mut changed["test_command"];
        match path {
            "workspace" => grant["request"]["workspace"] = json!("different-workspace"),
            "timeout" => grant["request"]["test_command"]["timeout_ms"] = json!(1),
            "contract" => grant["request"]["definition"]["id"]["version"] = json!(2),
            "actor" => grant["actor_id"] = json!(act0r::domain::AgentId::new()),
            _ => grant["session_id"] = json!(SessionId::new()),
        }
        sqlx::query("UPDATE harness_session_permissions SET payload=? WHERE session_id=?")
            .bind(changed.to_string())
            .bind(session)
            .execute(&pool)
            .await
            .unwrap();
        worker.advance_work().await.unwrap();
        assert_eq!(
            engine
                .status(id)
                .await
                .unwrap()
                .record()
                .wait
                .as_ref()
                .unwrap()
                .id,
            saved.record().wait.as_ref().unwrap().id,
            "mismatched {path} authorized tests"
        );
    }
    sqlx::query("UPDATE harness_session_permissions SET payload=? WHERE session_id=?")
        .bind(original.to_string())
        .bind(session)
        .execute(&pool)
        .await
        .unwrap();
    // Revoke before the worker's authorization transaction: this pending test stays a human wait.
    post(
        &client(),
        &policy(&server, &receipt),
        &revoke(&view(&server, &receipt).await),
        StatusCode::OK,
    )
    .await;
    worker.advance_work().await.unwrap();
    assert_eq!(
        engine
            .status(id)
            .await
            .unwrap()
            .record()
            .wait
            .as_ref()
            .unwrap()
            .id,
        saved.record().wait.as_ref().unwrap().id
    );
    drop(worker);
    approve(&server, &receipt).await;
    settle(&engine).await;
    assert_eq!(
        get(&client(), &format!("{}/changes", base(&server, &receipt))).await["completion"]["status"],
        "verified"
    );
    pool.close().await;
    server.close().await;
}
