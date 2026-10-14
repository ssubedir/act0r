//! Real saved model requests, sibling progress and exact human decisions through HTTP.
use super::*;
use act0r::domain::{RunId, RunStatus};
use std::sync::Mutex;

struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl Model {
    async fn start() -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let saved = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let p: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                saved.lock().unwrap().push(body.clone());
                let goal = p["goal"].as_str().unwrap();
                let turn = p["turn"].as_u64().unwrap();
                let guided = p["clarifications"].as_array().is_some_and(|a| a.iter().any(|c| c["guidance"] == true));
                let observed = p["tool_observations"].as_array().is_some_and(|a| !a.is_empty());
                let action = if goal == "Worker 0" {
                    if observed { json!({"kind":"finish_incomplete","summary":"Saved evidence inspected"}) }
                    else if guided { json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}}) }
                    else { json!({"kind":"ask_human","question":format!("Confirm I should inspect the permitted file? {turn}")}) }
                } else if goal == "Worker 1" {
                    json!({"kind":"finish_incomplete","summary":"Independent sibling settled"})
                } else if turn == 0 {
                    json!({"kind":"delegate_tasks","tasks":[{"worker":"coder","goal":"Worker 0","writable_paths":["src/"]},{"worker":"coder2","goal":"Worker 1","writable_paths":["src/"]}]})
                } else { json!({"kind":"finish_incomplete","summary":"Original group joined once"}) };
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
fn goal(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn turn(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
async fn work(server: &Server, receipt: &Value) -> Value {
    get(
        &client(),
        &format!("{}/delegated-work", turn(server, receipt)),
    )
    .await
}
async fn start(server: &Server, model: &Model, root: &Path) -> Value {
    let project = root.join("project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(project.join("src/lib.rs"), "Saved source evidence\n").unwrap();
    post(&client(), &format!("{}/v1/harness/sessions", server.url), &json!({"message_id":MessageId::new(),"prompt":"Inspect two independent assignments","workspace":fs::canonicalize(project).unwrap(),"target":{"provider":"openai","model":"clarification-fixture","endpoint":model.endpoint,"protocol":"chat_completions"},"coding":{"writable_paths":["src/"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000},"coder_count":2}}), StatusCode::ACCEPTED).await
}
async fn waiting(server: &Server, receipt: &Value) -> Value {
    let inbox = get(&client(), &format!("{}/inbox", goal(server, receipt))).await;
    let items = inbox["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    items[0].clone()
}
async fn stall(engine: &Engine, server: &Server, receipt: &Value) -> Value {
    settle(engine).await;
    for _ in 0..2 {
        let item = waiting(server, receipt).await;
        assert_eq!(item["kind"], "question");
        post(&client(), &format!("{}/waits/{}/responses", goal(server, receipt), item["wait"]["id"].as_str().unwrap()), &json!({"kind":"answer","answer":"Inspect the permitted file; the scope is already supplied"}), StatusCode::OK).await;
        settle(engine).await;
    }
    let item = waiting(server, receipt).await;
    assert_eq!(item["kind"], "guidance");
    assert_eq!(item["wait"]["payload"]["code"], "repeated_questions");
    assert_eq!(item["wait"]["payload"]["consecutive_questions"], 3);
    item
}
fn run(item: &Value) -> RunId {
    item["run_id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn stalled_worker_survives_restart_while_sibling_settles_and_guidance_replays_once_under_a_hold()
 {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("clarifications.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    let item = stall(&engine, &server, &receipt).await;
    let id = run(&item);
    let before = engine.status(id).await.unwrap();
    let children = work(&server, &receipt).await;
    assert_eq!(
        children["groups"][0]["children"][0]["guidance_wait_id"],
        item["wait"]["id"]
    );
    assert_eq!(children["groups"][0]["children"][1]["status"], "failed");
    assert_eq!(children["groups"][0]["finished"], 1);
    assert_eq!(children["groups"][0]["joined"], false);
    assert_eq!(before.record().usage.admitted_model_calls, 3);
    let model_calls = model.requests.lock().unwrap().len();
    assert_eq!(model_calls, 5);
    for body in model.requests.lock().unwrap().iter().filter(|b| {
        let p: Value = serde_json::from_str(b["messages"][1]["content"].as_str().unwrap()).unwrap();
        p["goal"] == "Worker 0"
    }) {
        let p: Value =
            serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
        let turn = p["turn"].as_u64().unwrap();
        assert_eq!(p["clarifications"].as_array().unwrap().len() as u64, turn);
        if turn > 0 {
            assert_eq!(
                p["clarifications"][turn as usize - 1]["response"]["answer"],
                "Inspect the permitted file; the scope is already supplied"
            );
        }
        assert!(
            body["messages"][0]["content"]
                .as_str()
                .unwrap()
                .starts_with("You are a coding agent")
        );
    }
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    assert_eq!(waiting(&server, &receipt).await, item);
    assert_eq!(
        engine.status(id).await.unwrap().record().state,
        before.record().state
    );
    settle(&engine).await;
    assert_eq!(model.requests.lock().unwrap().len(), model_calls);
    let pause_url = format!(
        "{}/workers/{}/pause",
        turn(&server, &receipt),
        item["task_id"].as_str().unwrap()
    );
    post(
        &client(),
        &pause_url,
        &json!({"request_id":MessageId::new(),"expected_revision":0,"paused":true}),
        StatusCode::ACCEPTED,
    )
    .await;
    let url = format!(
        "{}/waits/{}/responses",
        goal(&server, &receipt),
        item["wait"]["id"].as_str().unwrap()
    );
    let guidance = json!({"kind":"answer","answer":"Use read_file src/lib.rs; continue within the pinned scope"});
    let c = client();
    let (a, b) = tokio::join!(
        post(&c, &url, &guidance, StatusCode::OK),
        post(&c, &url, &guidance, StatusCode::OK)
    );
    assert_ne!(a["replayed"], b["replayed"]);
    settle(&engine).await;
    assert_eq!(model.requests.lock().unwrap().len(), model_calls);
    assert_eq!(
        engine.status(id).await.unwrap().record().state["state"]["clarification"]["needs_guidance"],
        false
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let url = format!(
        "{}/waits/{}/responses",
        goal(&server, &receipt),
        item["wait"]["id"].as_str().unwrap()
    );
    assert_eq!(
        post(&client(), &url, &guidance, StatusCode::OK).await["replayed"],
        true
    );
    post(
        &client(),
        &url,
        &json!({"kind":"answer","answer":"Different guidance"}),
        StatusCode::CONFLICT,
    )
    .await;
    let pause_url = format!(
        "{}/workers/{}/pause",
        turn(&server, &receipt),
        item["task_id"].as_str().unwrap()
    );
    post(
        &client(),
        &pause_url,
        &json!({"request_id":MessageId::new(),"expected_revision":1,"paused":false}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let completed = engine.status(id).await.unwrap();
    assert_eq!(completed.record().status, RunStatus::Failed);
    assert_eq!(
        completed.record().error.as_ref().unwrap().code,
        "coding_incomplete"
    );
    assert_eq!(completed.record().usage.admitted_model_calls, 5);
    let page = work(&server, &receipt).await;
    assert_eq!(page["groups"][0]["joined"], true);
    assert!(
        page["groups"][0]["children"][0]
            .get("guidance_wait_id")
            .is_none()
    );
    let pool = summary::pool(&db).await;
    let joins: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM history WHERE json_extract(payload,'$.kind')='task_group_joined'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(joins, 1);
    assert_eq!(
        completed
            .record()
            .usage
            .budget
            .as_ref()
            .unwrap()
            .max_model_calls,
        None
    );
    assert_eq!(
        completed.record().usage.budget.as_ref().unwrap().max_tokens,
        None
    );
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn guidance_cannot_approve_tools_cross_scope_or_restart_a_stopped_assignment() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("stopped.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    let item = stall(&engine, &server, &receipt).await;
    let url = format!(
        "{}/waits/{}/responses",
        goal(&server, &receipt),
        item["wait"]["id"].as_str().unwrap()
    );
    post(
        &client(),
        &url,
        &json!({"kind":"approval","approved":true}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    post(
        &client(),
        &url,
        &json!({"kind":"answer","answer":" "}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let before = engine.status(run(&item)).await.unwrap();
    // Even a valid answer that cannot fit preserves the exact saved wait.
    let response = client()
        .post(&url)
        .json(&json!({"kind":"answer","answer":"x".repeat(65_536)}))
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
    assert_eq!(
        engine.status(run(&item)).await.unwrap().record().state,
        before.record().state
    );
    assert_eq!(waiting(&server, &receipt).await, item);
    let other = post(&client(), &format!("{}/v1/harness/sessions", server.url), &json!({"message_id":MessageId::new(),"prompt":"Separate scope","target":{"provider":"openai","model":"clarification-fixture","endpoint":model.endpoint,"protocol":"chat_completions"}}), StatusCode::ACCEPTED).await;
    post(
        &client(),
        &format!(
            "{}/waits/{}/responses",
            goal(&server, &other),
            item["wait"]["id"].as_str().unwrap()
        ),
        &json!({"kind":"answer","answer":"Cross-scope guidance"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    post(
        &client(),
        &format!(
            "{}/workers/{}/cancellation",
            turn(&server, &receipt),
            item["task_id"].as_str().unwrap()
        ),
        &json!({"request_id":MessageId::new()}),
        StatusCode::ACCEPTED,
    )
    .await;
    let response = client()
        .post(&url)
        .json(&json!({"kind":"answer","answer":"Continue"}))
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
    server.close().await;
}
