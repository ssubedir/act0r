use super::*;
use act0r::{
    Worker,
    domain::{ActivityExecution, Failure, Outcome, RunStatus},
};
use std::sync::Mutex;

pub(super) struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl Model {
    pub(super) async fn start(parallel: bool, helper: bool) -> Self {
        Self::with_first_action(parallel, helper, None).await
    }
    pub(super) async fn with_first_action(
        parallel: bool,
        helper: bool,
        first_action: Option<Value>,
    ) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let app=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
                let prompt:Value=serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                seen.lock().unwrap().push(prompt.clone());
                let turn=prompt["turn"].as_u64().unwrap();
                let action=if prompt["goal"].as_str().unwrap().starts_with("Worker") {
                    match turn {
                        0 if first_action.is_some()=>first_action.clone().unwrap(),
                        0|2=>json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}}),
                        1|3=>json!({"kind":"ask_human","question":"Continue this assignment?"}),
                        _ if prompt.get("completion_policy").is_none()=>json!({"kind":"finish","summary":"Read-only helper finished with saved evidence"}),
                        _=>json!({"kind":"finish_incomplete","summary":"Worker finished with saved evidence"}),
                    }
                } else if turn==0 {
                    if parallel {json!({"kind":"delegate_tasks","tasks":[{"worker":"coder","goal":"Worker 0","writable_paths":["src/"]},{"worker":"coder2","goal":"Worker 1","writable_paths":["src/"]}]})}
                    else {json!({"kind":"delegate_task","worker":if helper {"helper"} else {"coder"},"goal":"Worker 0","writable_paths":if helper {Value::Null} else {json!(["src/"])}})}
                } else {json!({"kind":"finish_incomplete","summary":"Parent received saved outcomes"})};
                async move {axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
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
fn recovery(server: &Server, receipt: &Value, child: &Value) -> String {
    format!(
        "{}/workers/{}/recovery",
        base(server, receipt),
        child["task_id"].as_str().unwrap()
    )
}
fn request(view: &Value, decision: &str) -> Value {
    json!({"request_id":MessageId::new(),"run_id":view["run_id"],"activity_id":view["activity_id"],"attempt_id":view["attempt_id"],"expected_history_sequence":view["history_sequence"],"decision":decision})
}
pub(super) async fn start(
    server: &Server,
    model: &Model,
    root: &Path,
    parallel: bool,
    budget: Option<Value>,
) -> Value {
    let project = root.join("project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(project.join("src/lib.rs"), "Saved source evidence\n").unwrap();
    let mut input = json!({"message_id":MessageId::new(),"prompt":"Recover delegated coding","workspace":fs::canonicalize(project).unwrap(),"target":{"provider":"openai","model":"worker-recovery-fixture","endpoint":model.endpoint,"protocol":"chat_completions"},"coding":{"writable_paths":["src/"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000},"coder_count":if parallel {2} else {1}}});
    if let Some(budget) = budget {
        input["budget"] = budget;
    }
    post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await
}
pub(super) async fn work(server: &Server, receipt: &Value) -> Value {
    get(
        &client(),
        &format!("{}/delegated-work", base(server, receipt)),
    )
    .await
}
pub(super) async fn child(server: &Server, receipt: &Value, n: usize) -> Value {
    work(server, receipt).await["groups"][0]["children"][n].clone()
}
pub(super) async fn answer(server: &Server, receipt: &Value, child: &Value) {
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
pub(super) async fn admit(worker: &mut Worker) -> ActivityExecution {
    for _ in 0..100 {
        if worker.advance_work().await.unwrap() || worker.fire_due_timer().await.unwrap() {
            continue;
        }
        if let Some(execution) = worker.admit().await.unwrap() {
            return execution;
        }
    }
    panic!("model request not ready");
}
pub(super) async fn fail(worker: &mut Worker, execution: ActivityExecution, code: &str) {
    worker
        .complete(
            execution,
            Outcome::Failure(Failure::new(code, "Fixture saved model failure", false)),
        )
        .await
        .unwrap();
    for _ in 0..100 {
        if !worker.advance_work().await.unwrap() {
            break;
        }
    }
}

#[tokio::test]
async fn exact_worker_checkpoint_and_parent_join_survive_repeated_failure_retry_and_restart() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("recovery.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &assignment).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let frozen = execution.input.clone();
    let id = execution.run_id;
    let steering =
        json!({"message_id":MessageId::new(),"run_id":id,"text":"Keep the saved public API"});
    post(
        &client(),
        &format!(
            "{}/workers/{}/steering",
            base(&server, &receipt),
            assignment["task_id"].as_str().unwrap()
        ),
        &steering,
        StatusCode::ACCEPTED,
    )
    .await;
    fail(&mut worker, execution, "model_invalid_action").await;
    drop(worker);
    let held = engine.status(id).await.unwrap();
    let original_state = held.record().state.clone();
    let before_usage = held.record().usage.admitted_model_calls;
    assert_eq!(before_usage, 3);
    assert!(held.needs_worker_recovery());
    let view = get(&client(), &recovery(&server, &receipt, &assignment)).await;
    assert_eq!(child(&server, &receipt, 0).await["recovery"], view);
    assert_eq!(view["retry_allowed"], true);
    assert_eq!(
        child(&server, &receipt, 0).await["status"],
        "needs_recovery"
    );
    assert_eq!(work(&server, &receipt).await["groups"][0]["finished"], 0);
    assert_eq!(
        get(&client(), &goal(&server, &receipt)).await["task"]["summary"]["settled"],
        false
    );
    let decision = request(&view, "retry");
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    assert_eq!(
        get(&client(), &recovery(&server, &receipt, &assignment)).await,
        view
    );
    let c = client();
    let path = recovery(&server, &receipt, &assignment);
    let (a, b) = tokio::join!(
        post(&c, &path, &decision, StatusCode::ACCEPTED),
        post(&c, &path, &decision, StatusCode::ACCEPTED)
    );
    assert_eq!(a["resumed_activity_id"], b["resumed_activity_id"]);
    assert_ne!(a["replayed"], b["replayed"]);
    let mut worker = engine.worker().await.unwrap();
    let retry = admit(&mut worker).await;
    assert_eq!(retry.input, frozen);
    assert_eq!(retry.ordinal, 1);
    assert_eq!(
        engine.status(id).await.unwrap().record().state,
        original_state
    );
    fail(&mut worker, retry, "model_invalid_action").await;
    drop(worker);
    let newer = get(&c, &path).await;
    assert_ne!(newer["attempt_id"], view["attempt_id"]);
    let mut stale = decision.clone();
    stale["request_id"] = json!(MessageId::new());
    post(&c, &path, &stale, StatusCode::CONFLICT).await;
    assert_eq!(
        post(&c, &path, &decision, StatusCode::ACCEPTED).await["replayed"],
        true
    );
    post(&c, &path, &request(&newer, "retry"), StatusCode::ACCEPTED).await;
    let mut worker = engine.worker().await.unwrap();
    let retry = admit(&mut worker).await;
    assert_eq!(retry.input, frozen);
    let outcome = worker.execute(retry.clone()).await.unwrap();
    worker.complete(retry, outcome).await.unwrap();
    drop(worker);
    settle(&engine).await;
    let continued = engine.status(id).await.unwrap();
    assert_eq!(continued.record().usage.admitted_model_calls, 6);
    assert_eq!(
        continued.record().steering[0].request.text,
        "Keep the saved public API"
    );
    assert!(continued.record().steering[0].application.is_some());
    assert_eq!(
        continued
            .activities()
            .iter()
            .filter(|a| a.spec.handler.name == "native.tool"
                && a.status == act0r::domain::ActivityStatus::Succeeded)
            .count(),
        2
    );
    let prompts = model.requests.lock().unwrap().clone();
    let prompts: Vec<_> = prompts.iter().filter(|p| p["goal"] == "Worker 0").collect();
    assert!(prompts[2].get("steering").is_none());
    assert_eq!(prompts[3]["steering"][0]["text"], steering["text"]);
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    answer(&server, &receipt, &assignment).await;
    settle(&engine).await;
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    let settled = engine.status(id).await.unwrap();
    assert_eq!(settled.record().usage.admitted_model_calls, 7);
    assert_eq!(
        get(&c, &path).await["receipts"].as_array().unwrap().len(),
        2
    );
    assert!(!get(&c, &path).await["pending"].as_bool().unwrap());
    assert_eq!(
        fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
        "Saved source evidence\n"
    );
    let pool = summary::pool(&db).await;
    let completions:i64=sqlx::query_scalar("SELECT COUNT(*) FROM agent_mailbox WHERE json_extract(payload,'$.message.kind')='$run_completed' AND json_extract(payload,'$.message.payload.run_id')=?").bind(id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(completions, 1);
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn accepted_failure_releases_only_its_parallel_assignment_and_is_delivered_once() {
    let model = Model::start(true, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parallel.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let receipt = start(&server, &model, root.path(), true, None).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    let second = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &first).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let id = execution.run_id;
    fail(&mut worker, execution, "model_invalid_action").await;
    drop(worker);
    let actor_id = first["actor_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .status(actor_id)
            .await
            .unwrap()
            .record()
            .state["completed"],
        0
    );
    let path = recovery(&server, &receipt, &first);
    let view = get(&client(), &path).await;
    let decision = request(&view, "accept_failure");
    post(&client(), &path, &decision, StatusCode::ACCEPTED).await;
    settle(&engine).await;
    let group = work(&server, &receipt).await;
    assert_eq!(group["groups"][0]["finished"], 1);
    assert_eq!(group["groups"][0]["joined"], false);
    assert_eq!(
        engine.status(id).await.unwrap().record().status,
        RunStatus::Failed
    );
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .status(actor_id)
            .await
            .unwrap()
            .record()
            .state["completed"],
        1
    );
    for _ in 0..2 {
        answer(&server, &receipt, &second).await;
        settle(&engine).await;
    }
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    assert_eq!(
        post(&client(), &path, &decision, StatusCode::ACCEPTED).await["replayed"],
        true
    );
    let mut changed = decision.clone();
    changed["decision"] = json!("retry");
    post(&client(), &path, &changed, StatusCode::CONFLICT).await;
    let pool = summary::pool(&db).await;
    let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM history WHERE run_id=? AND json_extract(payload,'$.kind')='worker_recovery_decided'").bind(id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(count, 1);
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn recovery_scope_checkpoint_validation_pause_and_receipt_rollback_are_atomic() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("scope.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &assignment).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let id = execution.run_id;
    fail(&mut worker, execution, "model_invalid_action").await;
    drop(worker);
    let path = recovery(&server, &receipt, &assignment);
    let view = get(&client(), &path).await;
    let decision = request(&view, "retry");
    let before = json!(engine.status(id).await.unwrap());
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_worker_recovery BEFORE INSERT ON harness_worker_recoveries BEGIN SELECT RAISE(ABORT,'rollback receipt'); END;").execute(&pool).await.unwrap();
    post(
        &client(),
        &path,
        &decision,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(json!(engine.status(id).await.unwrap()), before);
    sqlx::query("DROP TRIGGER reject_worker_recovery")
        .execute(&pool)
        .await
        .unwrap();
    for field in ["run_id", "activity_id", "attempt_id"] {
        let mut wrong = decision.clone();
        wrong[field] = json!(uuid::Uuid::new_v4());
        post(&client(), &path, &wrong, StatusCode::CONFLICT).await;
    }
    let mut stale = decision.clone();
    stale["expected_history_sequence"] = json!(1);
    post(&client(), &path, &stale, StatusCode::CONFLICT).await;
    let mut nil = decision.clone();
    nil["request_id"] = json!(uuid::Uuid::nil());
    post(&client(), &path, &nil, StatusCode::BAD_REQUEST).await;
    for field in ["workspace", "writable_paths", "target", "budget", "result"] {
        let mut wrong = decision.clone();
        wrong[field] = json!("override");
        post(&client(), &path, &wrong, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
    for decision_name in ["accept", "fail", "complete"] {
        let mut wrong = decision.clone();
        wrong["decision"] = json!(decision_name);
        post(&client(), &path, &wrong, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
    for target in [receipt["task_id"].clone(), json!(MessageId::new())] {
        post(
            &client(),
            &format!(
                "{}/workers/{}/recovery",
                base(&server, &receipt),
                target.as_str().unwrap()
            ),
            &decision,
            StatusCode::NOT_FOUND,
        )
        .await;
    }
    let other = start(&server, &model, root.path(), false, None).await;
    post(
        &client(),
        &recovery(&server, &other, &assignment),
        &decision,
        StatusCode::NOT_FOUND,
    )
    .await;
    post(
        &client(),
        &format!("{}/pause", goal(&server, &receipt)),
        &json!({"expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    let paused = get(&client(), &path).await;
    assert_eq!(paused["retry_allowed"], false);
    assert_eq!(paused["accept_allowed"], false);
    post(&client(), &path, &decision, StatusCode::CONFLICT).await;
    post(
        &client(),
        &path,
        &request(&paused, "accept_failure"),
        StatusCode::CONFLICT,
    )
    .await;
    post(
        &client(),
        &format!("{}/resume", goal(&server, &receipt)),
        &json!({"expected_revision":1}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        get(&client(), &path).await["history_sequence"],
        view["history_sequence"]
    );
    post(&client(), &path, &decision, StatusCode::ACCEPTED).await;
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn exhausted_shared_or_attempt_allowance_blocks_retry_but_allows_accepting_failure() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("budget.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(
        &server,
        &model,
        root.path(),
        false,
        Some(json!({"max_model_calls":4,"max_tokens":null})),
    )
    .await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &assignment).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let id = execution.run_id;
    fail(&mut worker, execution, "model_invalid_action").await;
    drop(worker);
    let path = recovery(&server, &receipt, &assignment);
    let view = get(&client(), &path).await;
    assert_eq!(view["pending"], true);
    assert_eq!(view["retry_allowed"], false);
    assert_eq!(view["accept_allowed"], true);
    assert!(
        view["retry_blocked_reason"]
            .as_str()
            .unwrap()
            .contains("shared budget")
    );
    post(
        &client(),
        &path,
        &request(&view, "retry"),
        StatusCode::CONFLICT,
    )
    .await;
    let pool = summary::pool(&db).await;
    sqlx::query("UPDATE runs SET payload=json_set(payload,'$.allowance.limit',json_extract(payload,'$.allowance.admitted')) WHERE id=?").bind(id.to_string()).execute(&pool).await.unwrap();
    assert!(get(&client(), &path).await["retry_blocked_reason"].is_string());
    post(
        &client(),
        &path,
        &request(&view, "accept_failure"),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    assert_eq!(
        engine
            .status(id)
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        3
    );
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn stopping_a_held_assignment_clears_recovery_and_settles_the_parent() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("cancel.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &assignment).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let id = execution.run_id;
    fail(&mut worker, execution, "model_invalid_action").await;
    drop(worker);
    let path = recovery(&server, &receipt, &assignment);
    let view = get(&client(), &path).await;
    post(
        &client(),
        &format!("{}/cancel", goal(&server, &receipt)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    post(
        &client(),
        &path,
        &request(&view, "retry"),
        StatusCode::CONFLICT,
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        engine.status(id).await.unwrap().record().status,
        RunStatus::Cancelled
    );
    assert!(!engine.status(id).await.unwrap().needs_worker_recovery());
    let controls = get(&client(), &format!("{}/controls", goal(&server, &receipt))).await;
    assert_eq!(controls["cancellation"]["settled"], true);
    assert_eq!(
        get(&client(), &goal(&server, &receipt)).await["task"]["summary"]["settled"],
        true
    );
    server.close().await;
}

#[tokio::test]
async fn non_model_failures_and_read_only_helpers_do_not_enter_coding_recovery() {
    for (code, helper) in [
        ("model_configuration", false),
        ("model_refusal", false),
        ("tool_configuration", false),
        ("model_invalid_action", true),
    ] {
        let model = Model::start(false, helper).await;
        let root = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(root.path().join("excluded.db")).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let receipt = start(&server, &model, root.path(), false, None).await;
        settle(&engine).await;
        let assignment = child(&server, &receipt, 0).await;
        answer(&server, &receipt, &assignment).await;
        let mut worker = engine.worker().await.unwrap();
        let execution = admit(&mut worker).await;
        let id = execution.run_id;
        fail(&mut worker, execution, code).await;
        drop(worker);
        settle(&engine).await;
        assert!(!engine.status(id).await.unwrap().needs_worker_recovery());
        assert_eq!(
            get(&client(), &recovery(&server, &receipt, &assignment)).await["pending"],
            false
        );
        assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
        server.close().await;
    }
}

#[tokio::test]
async fn a_saved_delegation_deadline_cancels_pending_worker_recovery() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("deadline.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &assignment).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let id = execution.run_id;
    fail(&mut worker, execution, "model_invalid_action").await;
    drop(worker);
    let pool = summary::pool(&db).await;
    // Advance the durable deadline facts without sleeping or depending on real elapsed time.
    sqlx::query("UPDATE runs SET payload=json_set(payload,'$.delegations[0].deadline_at',0) WHERE id=(SELECT run_id FROM agent_tasks WHERE id=?)")
        .bind(receipt["task_id"].as_str().unwrap()).execute(&pool).await.unwrap();
    sqlx::query("UPDATE agent_delegations SET deadline_at=0 WHERE id=?")
        .bind(assignment["task_id"].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    settle(&engine).await;
    assert_eq!(
        engine.status(id).await.unwrap().record().status,
        RunStatus::Cancelled
    );
    assert_eq!(
        get(&client(), &recovery(&server, &receipt, &assignment)).await["pending"],
        false
    );
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    pool.close().await;
    server.close().await;
}
