use super::*;
use act0r::{
    Worker,
    domain::{Outcome, RunId, RunStatus},
};
use std::sync::Mutex;

struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
    parallel: bool,
}
impl Model {
    async fn start() -> Self {
        Self::mode(false, false).await
    }
    async fn mode(parallel: bool, helper: bool) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let router = axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let prompt: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                seen.lock().unwrap().push(prompt.clone());
                let turn = prompt["turn"].as_u64().unwrap();
                let action = if prompt["goal"].as_str().unwrap().starts_with("Worker") {
                    match turn {
                        0 | 2 => json!({"kind":"ask_human","question":"Continue this worker?"}),
                        1 => json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}}),
                        _ => json!({"kind":"finish_incomplete","summary":"Worker stopped with saved instructions"}),
                    }
                } else if parallel {
                    if turn == 0 {
                        json!({"kind":"delegate_tasks","tasks":[{"worker":"coder","goal":"Worker 0","writable_paths":["src/lib.rs"]},{"worker":"coder2","goal":"Worker 1","writable_paths":["src/lib.rs"]}]})
                    } else {
                        json!({"kind":"finish_incomplete","summary":"Parallel assignments retained"})
                    }
                } else {
                    match turn {
                        0 | 1 => {
                            let mut action=json!({"kind":"delegate_task","worker":if helper {"helper"} else {"coder"},"goal":format!("Worker {turn}")});
                            if !helper {action["writable_paths"]=json!(["src/"]);}
                            action
                        },
                        _ => json!({"kind":"finish_incomplete","summary":"Assignments retained"}),
                    }
                };
                async move {axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
            }));
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            endpoint,
            requests,
            task,
            parallel,
        }
    }
}
impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn base(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn goal(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn url(server: &Server, receipt: &Value, child: &Value) -> String {
    format!(
        "{}/workers/{}/steering",
        base(server, receipt),
        child["task_id"].as_str().unwrap()
    )
}
fn request(view: &Value, text: &str) -> Value {
    json!({"message_id":MessageId::new(),"run_id":view["run_id"],"text":text})
}
async fn start(server: &Server, model: &Model, root: &Path, budget: Option<Value>) -> Value {
    let project = root.join("project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(project.join("src/lib.rs"), "Saved source evidence\n").unwrap();
    let mut input = json!({
        "message_id":MessageId::new(),"prompt":"Guide delegated coding","workspace":fs::canonicalize(project).unwrap(),
        "target":{"provider":"openai","model":"worker-steering-fixture","endpoint":model.endpoint,"protocol":"chat_completions"},
        "coding":{"writable_paths":["src/"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000}}
    });
    if let Some(budget) = budget {
        input["budget"] = budget;
    }
    if model.parallel {
        input["coding"]["coder_count"] = json!(2);
    }
    post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await
}
async fn children(server: &Server, receipt: &Value) -> Vec<Value> {
    get(
        &client(),
        &format!("{}/delegated-work", base(server, receipt)),
    )
    .await["groups"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|g| g["children"].as_array().unwrap().clone())
        .collect()
}
async fn answer(server: &Server, receipt: &Value, child: &Value) {
    let inbox = get(&client(), &format!("{}/inbox", goal(server, receipt))).await;
    let wait = inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["task_id"] == child["task_id"] && i["actionable"] == true)
        .unwrap();
    post(
        &client(),
        &format!(
            "{}/waits/{}/responses",
            goal(server, receipt),
            wait["wait"]["id"].as_str().unwrap()
        ),
        &json!({"kind":"answer","answer":"Continue"}),
        StatusCode::OK,
    )
    .await;
}
async fn admit(worker: &mut Worker) -> act0r::domain::ActivityExecution {
    for _ in 0..100 {
        if worker.advance_work().await.unwrap() || worker.fire_due_timer().await.unwrap() {
            continue;
        }
        if let Some(execution) = worker.admit().await.unwrap() {
            return execution;
        }
    }
    panic!("model request did not become ready");
}

#[tokio::test]
async fn ordered_worker_instructions_survive_pause_restart_and_do_not_reach_later_assignments() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("worker.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), None).await;
    settle(&engine).await;
    let child = children(&server, &receipt).await.remove(0);
    let view = get(&client(), &url(&server, &receipt, &child)).await;
    assert_eq!(view, child["steering"]);
    assert_eq!(view["can_send"], true);
    let id: RunId = view["run_id"].as_str().unwrap().parse().unwrap();
    let before = engine.status(id).await.unwrap();
    let first = request(&view, "Focus on durable recovery 🦀");
    let second = request(&view, "Keep the existing API");
    let path = url(&server, &receipt, &child);
    let c = client();
    let (a, b) = tokio::join!(
        post(&c, &path, &first, StatusCode::ACCEPTED),
        post(&c, &path, &first, StatusCode::ACCEPTED)
    );
    assert_eq!(a["entry"], b["entry"]);
    assert_ne!(a["replayed"], b["replayed"]);
    post(&c, &path, &second, StatusCode::ACCEPTED).await;
    let saved = engine.status(id).await.unwrap();
    assert_eq!(saved.record().input, before.record().input);
    assert_eq!(saved.record().state, before.record().state);
    assert_eq!(json!(saved.record().usage), json!(before.record().usage));
    post(
        &c,
        &format!("{}/pause", goal(&server, &receipt)),
        &json!({"expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    answer(&server, &receipt, &child).await;
    settle(&engine).await;
    assert!(get(&c, &path).await["items"][0]["application"].is_null());
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let path = url(&server, &receipt, &child);
    assert_eq!(
        post(&c, &path, &first, StatusCode::ACCEPTED).await["replayed"],
        true
    );
    settle(&engine).await;
    assert!(get(&c, &path).await["items"][0]["application"].is_null());
    post(
        &c,
        &format!("{}/resume", goal(&server, &receipt)),
        &json!({"expected_revision":1}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let applied = get(&c, &path).await;
    assert!(applied["items"][0]["application"].is_object());
    assert_eq!(
        applied["items"][0]["application"],
        applied["items"][1]["application"]
    );
    let third = request(&applied, "Explain the saved evidence");
    post(&c, &path, &third, StatusCode::ACCEPTED).await;
    answer(&server, &receipt, &child).await;
    settle(&engine).await;
    let done = get(&c, &path).await;
    assert_eq!(done["terminal"], true);
    assert_eq!(done["can_send"], false);
    assert_eq!(
        done["items"][0]["application"],
        applied["items"][0]["application"]
    );
    assert!(done["items"][2]["application"].is_object());
    assert_eq!(
        post(&c, &path, &first, StatusCode::ACCEPTED).await["replayed"],
        true
    );
    post(&c, &path, &request(&done, "Too late"), StatusCode::CONFLICT).await;
    let all = children(&server, &receipt).await;
    let later = all
        .iter()
        .find(|c| c["task_id"] != child["task_id"])
        .unwrap();
    assert_eq!(later["actor_id"], child["actor_id"]);
    assert_ne!(later["run_id"], child["run_id"]);
    assert_eq!(later["steering"]["items"], json!([]));
    let prompts = model.requests.lock().unwrap().clone();
    let first_prompts: Vec<_> = prompts.iter().filter(|p| p["goal"] == "Worker 0").collect();
    assert_eq!(first_prompts.len(), 4);
    assert!(first_prompts[0].get("steering").is_none());
    assert_eq!(first_prompts[1]["steering"][0]["text"], first["text"]);
    assert_eq!(first_prompts[1]["steering"], first_prompts[2]["steering"]);
    assert_eq!(first_prompts[3]["steering"].as_array().unwrap().len(), 3);
    assert!(
        first_prompts[2]["tool_observations"]
            .to_string()
            .contains("Saved source evidence")
    );
    assert!(
        prompts
            .iter()
            .filter(|p| p["goal"] != "Worker 0")
            .all(|p| p.get("steering").is_none())
    );
    assert_eq!(
        engine.status(id).await.unwrap().record().state["state"]["config"],
        before.record().state["state"]["config"]
    );
    assert_eq!(
        fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
        "Saved source evidence\n"
    );
    server.close().await;
}

#[tokio::test]
async fn worker_steering_preserves_admitted_inputs_and_automatic_retries() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("admitted.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), None).await;
    settle(&engine).await;
    let child = children(&server, &receipt).await.remove(0);
    answer(&server, &receipt, &child).await;
    let mut worker = engine.worker().await.unwrap();
    let admitted = admit(&mut worker).await;
    assert_eq!(json!(admitted.run_id), child["run_id"]);
    let frozen = admitted.input.clone();
    let path = url(&server, &receipt, &child);
    let view = get(&client(), &path).await;
    let instruction = request(&view, "Use this after the current request");
    post(&client(), &path, &instruction, StatusCode::ACCEPTED).await;
    worker
        .complete(
            admitted,
            Outcome::Failure(act0r::domain::Failure::new(
                "model_timeout",
                "fixture timeout",
                true,
            )),
        )
        .await
        .unwrap();
    let retry = admit(&mut worker).await;
    assert_eq!(retry.ordinal, 2);
    assert_eq!(retry.input, frozen);
    assert!(get(&client(), &path).await["items"][0]["application"].is_null());
    let result = worker.execute(retry.clone()).await.unwrap();
    worker.complete(retry, result).await.unwrap();
    drop(worker);
    settle(&engine).await;
    let view = get(&client(), &path).await;
    assert!(view["items"][0]["application"].is_object());
    let id: RunId = child["run_id"].as_str().unwrap().parse().unwrap();
    let run = engine.status(id).await.unwrap();
    assert_eq!(run.record().usage.admitted_model_calls, 4);
    let requests = model.requests.lock().unwrap().clone();
    let prompts: Vec<_> = requests
        .iter()
        .filter(|p| p["goal"] == "Worker 0")
        .collect();
    assert!(prompts[1].get("steering").is_none());
    assert_eq!(prompts[2]["steering"][0]["text"], instruction["text"]);
    server.close().await;
}

#[tokio::test]
async fn worker_scope_bounds_identities_and_sql_rollback_are_enforced() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("scope.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), None).await;
    settle(&engine).await;
    let child = children(&server, &receipt).await.remove(0);
    let path = url(&server, &receipt, &child);
    let view = get(&client(), &path).await;
    let input = request(&view, "Keep edits within the assignment");
    let id: RunId = view["run_id"].as_str().unwrap().parse().unwrap();
    let before = json!(engine.status(id).await.unwrap());
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_worker_steering BEFORE INSERT ON harness_worker_steering BEGIN SELECT RAISE(ABORT,'rollback receipt'); END;").execute(&pool).await.unwrap();
    post(&client(), &path, &input, StatusCode::INTERNAL_SERVER_ERROR).await;
    assert_eq!(json!(engine.status(id).await.unwrap()), before);
    sqlx::query("DROP TRIGGER reject_worker_steering")
        .execute(&pool)
        .await
        .unwrap();
    for field in ["workspace", "writable_paths", "tools", "model", "budget"] {
        let mut altered = input.clone();
        altered[field] = json!("override");
        post(&client(), &path, &altered, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
    for text in [" ".into(), "🦀".repeat(1025)] {
        post(
            &client(),
            &path,
            &request(&view, &text),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let mut stale = input.clone();
    stale["run_id"] = json!(RunId::new());
    post(&client(), &path, &stale, StatusCode::CONFLICT).await;
    let mut nil = input.clone();
    nil["message_id"] = json!(uuid::Uuid::nil());
    post(&client(), &path, &nil, StatusCode::BAD_REQUEST).await;
    let root_path = format!(
        "{}/workers/{}/steering",
        base(&server, &receipt),
        receipt["task_id"].as_str().unwrap()
    );
    post(&client(), &root_path, &input, StatusCode::NOT_FOUND).await;
    let unknown = format!(
        "{}/workers/{}/steering",
        base(&server, &receipt),
        MessageId::new()
    );
    post(&client(), &unknown, &input, StatusCode::NOT_FOUND).await;
    let other = start(&server, &model, root.path(), None).await;
    post(
        &client(),
        &url(&server, &other, &child),
        &input,
        StatusCode::NOT_FOUND,
    )
    .await;
    post(&client(), &path, &input, StatusCode::ACCEPTED).await;
    let mut changed = input.clone();
    changed["text"] = json!("Different");
    post(&client(), &path, &changed, StatusCode::CONFLICT).await;
    post(
        &client(),
        &format!("{}/steering", base(&server, &receipt)),
        &input,
        StatusCode::CONFLICT,
    )
    .await;
    for n in 1..16 {
        post(
            &client(),
            &path,
            &request(&view, &format!("Instruction {n}")),
            StatusCode::ACCEPTED,
        )
        .await;
    }
    post(
        &client(),
        &path,
        &request(&view, "Seventeenth"),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(
        get(&client(), &path).await["items"]
            .as_array()
            .unwrap()
            .len(),
        16
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    server.close().await;
}

#[tokio::test]
async fn shared_budget_refusal_leaves_worker_instruction_unapplied() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("budget.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(
        &server,
        &model,
        root.path(),
        Some(json!({"max_model_calls":2,"max_tokens":null})),
    )
    .await;
    settle(&engine).await;
    let child = children(&server, &receipt).await.remove(0);
    let path = url(&server, &receipt, &child);
    let view = get(&client(), &path).await;
    post(
        &client(),
        &path,
        &request(&view, "No calls remain"),
        StatusCode::ACCEPTED,
    )
    .await;
    answer(&server, &receipt, &child).await;
    settle(&engine).await;
    let done = get(&client(), &path).await;
    assert_eq!(done["terminal"], true);
    assert!(done["items"][0]["application"].is_null());
    let run = engine
        .status(view["run_id"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap();
    assert_eq!(run.record().status, RunStatus::BudgetExhausted);
    assert!(run.record().state["state"].get("steering").is_none());
    assert_eq!(model.requests.lock().unwrap().len(), 2);
    server.close().await;
}

#[tokio::test]
async fn read_only_helpers_do_not_accept_coding_worker_guidance() {
    let model = Model::mode(false, true).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("helper.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), None).await;
    settle(&engine).await;
    let child = children(&server, &receipt).await.remove(0);
    let path = url(&server, &receipt, &child);
    let view = get(&client(), &path).await;
    assert_eq!(view["can_send"], false);
    assert_eq!(view["terminal"], false);
    assert_eq!(view["items"], json!([]));
    post(
        &client(),
        &path,
        &request(&view, "Give this helper coding authority"),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(get(&client(), &path).await["items"], json!([]));
    server.close().await;
}

#[tokio::test]
async fn parallel_worker_guidance_is_isolated_and_cancellation_retains_pending_history() {
    let model = Model::mode(true, false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("parallel.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), None).await;
    settle(&engine).await;
    let children = children(&server, &receipt).await;
    assert_eq!(children.len(), 2);
    assert_ne!(children[0]["actor_id"], children[1]["actor_id"]);
    let first = &children[0];
    let second = &children[1];
    let path = url(&server, &receipt, first);
    let view = get(&client(), &path).await;
    let instruction = request(&view, "Only guide the first worker");
    post(&client(), &path, &instruction, StatusCode::ACCEPTED).await;
    post(
        &client(),
        &url(&server, &receipt, second),
        &instruction,
        StatusCode::CONFLICT,
    )
    .await;
    answer(&server, &receipt, first).await;
    settle(&engine).await;
    assert!(get(&client(), &path).await["items"][0]["application"].is_object());
    let prompts = model.requests.lock().unwrap().clone();
    assert!(
        prompts
            .iter()
            .filter(|p| p["goal"] == second["goal"])
            .all(|p| p.get("steering").is_none())
    );
    let late = request(&get(&client(), &path).await, "Pending at cancellation");
    post(&client(), &path, &late, StatusCode::ACCEPTED).await;
    post(
        &client(),
        &format!("{}/cancel", goal(&server, &receipt)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let done = get(&client(), &path).await;
    assert_eq!(done["terminal"], true);
    assert_eq!(done["can_send"], false);
    assert!(done["items"][1]["application"].is_null());
    assert!(done["items"][0]["application"].is_object());
    assert_eq!(
        post(&client(), &path, &late, StatusCode::ACCEPTED).await["replayed"],
        true
    );
    post(
        &client(),
        &path,
        &request(&done, "Too late"),
        StatusCode::CONFLICT,
    )
    .await;
    server.close().await;
}
