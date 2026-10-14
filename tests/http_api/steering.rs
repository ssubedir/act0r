use super::*;
use act0r::{
    Worker,
    domain::{Outcome, RunId, RunStatus},
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    failures: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}
#[allow(deprecated)]
fn take_failure(count: &AtomicUsize) -> bool {
    count
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
}
impl Model {
    async fn start() -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let failures = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let fail = failures.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let router=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
                let prompt:Value=serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                seen.lock().unwrap().push(prompt.clone());
                let action=if prompt["goal"]=="Finish immediately" {json!({"kind":"finish","summary":"Done before the next decision"})}
                    else if prompt["turn"]==0 || prompt["goal"]=="With tool" && prompt["turn"]==2 || prompt["goal"]=="Pause twice" && prompt["turn"]==1 {json!({"kind":"ask_human","question":"May I continue?"})}
                    else if prompt["goal"]=="With tool" && prompt["turn"]==1 {json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}})}
                    else if prompt["goal"]=="Coding" && prompt["turn"]==1 {json!({"kind":"call_tool","tool":"run_tests","arguments":{}})}
                    else {json!({"kind":"finish","summary":"Instructions incorporated"})};
                let content=if take_failure(&fail) {"invalid structured response".to_string()} else {json!({"action":native_wire_action(action, &body)}).to_string()};
                async move {axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":content}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
            }));
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            endpoint,
            requests,
            failures,
            task,
        }
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
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
async fn start(
    server: &Server,
    model: &Model,
    prompt: &str,
    budget: Option<Value>,
    workspace: Option<String>,
) -> Value {
    let mut input = summary::input(&model.endpoint);
    input["prompt"] = json!(prompt);
    if let Some(budget) = budget {
        input["budget"] = budget;
    }
    if let Some(workspace) = workspace {
        input["workspace"] = json!(workspace);
    }
    post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await
}
fn request(view: &Value, text: &str) -> Value {
    json!({"message_id":MessageId::new(),"run_id":view["run_id"],"text":text})
}
async fn answer(server: &Server, receipt: &Value) {
    let url = goal(server, receipt);
    let inbox = get(&client(), &format!("{url}/inbox")).await;
    let wait = inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["actionable"] == true)
        .unwrap();
    post(
        &client(),
        &format!(
            "{url}/waits/{}/responses",
            wait["wait"]["id"].as_str().unwrap()
        ),
        &json!({"kind":"answer","answer":"Continue"}),
        StatusCode::OK,
    )
    .await;
}
async fn admit(worker: &mut Worker) -> act0r::domain::ActivityExecution {
    for _ in 0..100 {
        if worker.advance_work().await.unwrap() {
            continue;
        }
        if worker.fire_due_timer().await.unwrap() {
            continue;
        }
        if let Some(execution) = worker.admit().await.unwrap() {
            return execution;
        }
    }
    panic!("model request did not become ready");
}
async fn drain(worker: &mut Worker) {
    for _ in 0..100 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("worker did not settle");
}

#[tokio::test]
async fn ordered_steering_survives_restart_duplicates_and_stays_scoped_to_its_turn() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("steering.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, "Ask before finishing", None, None).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let queued = get(&client(), &url).await;
    assert_eq!(queued["can_send"], false);
    settle(&engine).await;
    let view = get(&client(), &url).await;
    assert_eq!(view["can_send"], true);
    let id: RunId = view["run_id"].as_str().unwrap().parse().unwrap();
    let before = engine.status(id).await.unwrap();
    let first = request(&view, "Focus on the API first 🦀");
    let second = request(&view, "Also document its recovery behavior");
    let c = client();
    let (a, b) = tokio::join!(
        post(&c, &url, &first, StatusCode::ACCEPTED),
        post(&c, &url, &first, StatusCode::ACCEPTED)
    );
    assert_eq!(a["entry"], b["entry"]);
    assert_ne!(a["replayed"], b["replayed"]);
    post(&c, &url, &second, StatusCode::ACCEPTED).await;
    let saved = engine.status(id).await.unwrap();
    assert_eq!(saved.record().input, before.record().input);
    assert_eq!(saved.record().state, before.record().state);
    assert_eq!(json!(saved.record().usage), json!(before.record().usage));
    assert_eq!(model.count(), 1);
    let follow = post(
        &c,
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            receipt["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"Finish immediately"}),
        StatusCode::ACCEPTED,
    )
    .await;
    let later = get(&c, &format!("{}/steering", base(&server, &follow))).await;
    assert_eq!(later["can_send"], false);
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let url = format!("{}/steering", base(&server, &receipt));
    assert_eq!(
        post(&c, &url, &first, StatusCode::ACCEPTED).await["replayed"],
        true
    );
    answer(&server, &receipt).await;
    settle(&engine).await;
    let view = get(&c, &url).await;
    assert_eq!(view["terminal"], true);
    assert_eq!(view["can_send"], false);
    assert_eq!(view["items"][0]["sequence"], 1);
    assert_eq!(view["items"][1]["sequence"], 2);
    assert_eq!(
        view["items"][0]["application"]["activity_id"],
        view["items"][1]["application"]["activity_id"]
    );
    let requests = model.requests.lock().unwrap().clone();
    assert_eq!(requests[1]["steering"][0]["text"], first["text"]);
    assert_eq!(requests[1]["steering"][1]["text"], second["text"]);
    assert!(requests[2].get("steering").is_none());
    let run = engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().usage.admitted_model_calls, 2);
    assert_eq!(
        post(&c, &url, &first, StatusCode::ACCEPTED).await["replayed"],
        true
    );
    post(&c, &url, &request(&view, "Too late"), StatusCode::CONFLICT).await;
    server.close().await;
}

#[tokio::test]
async fn steering_does_not_mutate_admitted_inputs_or_automatic_retries() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("admitted.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, "Ask before finishing", None, None).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let frozen = execution.input.clone();
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    let instruction = request(&view, "Keep the current checkpoint");
    post(&client(), &url, &instruction, StatusCode::ACCEPTED).await;
    let run = engine.status(execution.run_id).await.unwrap();
    assert_eq!(run.activities()[0].spec.input, frozen);
    worker
        .complete(
            execution,
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
    assert!(get(&client(), &url).await["items"][0]["application"].is_null());
    let result = worker.execute(retry.clone()).await.unwrap();
    worker.complete(retry, result).await.unwrap();
    drain(&mut worker).await;
    answer(&server, &receipt).await;
    let next = admit(&mut worker).await;
    let prompt: Value = serde_json::from_str(next.input["prompt"].as_str().unwrap()).unwrap();
    assert_eq!(prompt["steering"][0]["text"], instruction["text"]);
    let view = get(&client(), &url).await;
    assert_eq!(
        view["items"][0]["application"]["activity_id"],
        json!(next.activity_id)
    );
    let result = worker.execute(next.clone()).await.unwrap();
    worker.complete(next, result).await.unwrap();
    drain(&mut worker).await;
    assert_eq!(
        engine
            .status(view["run_id"].as_str().unwrap().parse().unwrap())
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        3
    );
    server.close().await;
}

#[tokio::test]
async fn steering_waits_through_pause_and_saved_answer_across_restart() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("paused.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, "Ask before finishing", None, None).await;
    settle(&engine).await;
    let controls = get(&client(), &format!("{}/controls", goal(&server, &receipt))).await;
    post(
        &client(),
        &format!("{}/pause", goal(&server, &receipt)),
        &json!({"expected_revision":controls["pause"]["revision"]}),
        StatusCode::OK,
    )
    .await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    post(
        &client(),
        &url,
        &request(&view, "Use Rust ports and adapters"),
        StatusCode::ACCEPTED,
    )
    .await;
    answer(&server, &receipt).await;
    settle(&engine).await;
    assert_eq!(model.count(), 1);
    assert!(get(&client(), &url).await["items"][0]["application"].is_null());
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(model.count(), 1);
    let controls = get(&client(), &format!("{}/controls", goal(&server, &receipt))).await;
    post(
        &client(),
        &format!("{}/resume", goal(&server, &receipt)),
        &json!({"expected_revision":controls["pause"]["revision"]}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let view = get(&client(), &format!("{}/steering", base(&server, &receipt))).await;
    assert!(view["items"][0]["application"].is_object());
    assert_eq!(
        model.requests.lock().unwrap()[1]["steering"][0]["text"],
        "Use Rust ports and adapters"
    );
    server.close().await;
}

#[tokio::test]
async fn finishing_and_cancellation_leave_unconsumed_messages_visible() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("terminal.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, "Finish immediately", None, None).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    let instruction = request(&view, "Arrived during the final request");
    post(&client(), &url, &instruction, StatusCode::ACCEPTED).await;
    let result = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, result).await.unwrap();
    drain(&mut worker).await;
    let finished = get(&client(), &url).await;
    assert_eq!(finished["terminal"], true);
    assert!(finished["items"][0]["application"].is_null());
    post(
        &client(),
        &url,
        &request(&finished, "After completion"),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(
        post(&client(), &url, &instruction, StatusCode::ACCEPTED).await["replayed"],
        true
    );
    let cancelled = start(&server, &model, "Ask before finishing", None, None).await;
    drain(&mut worker).await;
    let url = format!("{}/steering", base(&server, &cancelled));
    let view = get(&client(), &url).await;
    let instruction = request(&view, "Pending at cancellation");
    post(&client(), &url, &instruction, StatusCode::ACCEPTED).await;
    post(
        &client(),
        &format!("{}/cancel", goal(&server, &cancelled)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    drain(&mut worker).await;
    let view = get(&client(), &url).await;
    assert_eq!(view["can_send"], false);
    assert_eq!(view["terminal"], true);
    assert!(view["items"][0]["application"].is_null());
    post(
        &client(),
        &url,
        &request(&view, "Too late"),
        StatusCode::CONFLICT,
    )
    .await;
    server.close().await;
}

#[tokio::test]
async fn steering_has_strict_bounds_scope_and_exact_identity_checks() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("limits.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, "Ask before finishing", None, None).await;
    settle(&engine).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    let instruction = request(&view, "Bounded instruction");
    post(&client(), &url, &instruction, StatusCode::ACCEPTED).await;
    let mut changed = instruction.clone();
    changed["text"] = json!("Different instruction");
    post(&client(), &url, &changed, StatusCode::CONFLICT).await;
    changed = instruction.clone();
    changed["run_id"] = json!(RunId::new());
    post(&client(), &url, &changed, StatusCode::CONFLICT).await;
    changed = request(&view, "Wrong Run");
    changed["run_id"] = json!(RunId::new());
    post(&client(), &url, &changed, StatusCode::CONFLICT).await;
    for text in [" ".to_string(), "🦀".repeat(1025)] {
        post(
            &client(),
            &url,
            &request(&view, &text),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    changed = request(&view, "Extra field");
    changed["extra"] = json!(true);
    post(&client(), &url, &changed, StatusCode::UNPROCESSABLE_ENTITY).await;
    changed = request(&view, "Nil identity");
    changed["message_id"] = json!("00000000-0000-0000-0000-000000000000");
    post(&client(), &url, &changed, StatusCode::BAD_REQUEST).await;
    let other = start(&server, &model, "Ask before finishing", None, None).await;
    settle(&engine).await;
    post(
        &client(),
        &format!("{}/steering", base(&server, &other)),
        &instruction,
        StatusCode::CONFLICT,
    )
    .await;
    let wrong = format!(
        "{}/v1/harness/sessions/{}/turns/{}/steering",
        server.url,
        other["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    value(
        client().get(&wrong).send().await.unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    for i in 1..16 {
        post(
            &client(),
            &url,
            &request(&view, &format!("Instruction {i}")),
            StatusCode::ACCEPTED,
        )
        .await;
    }
    post(
        &client(),
        &url,
        &request(&view, "Instruction 17"),
        StatusCode::CONFLICT,
    )
    .await;
    let other_url = format!("{}/steering", base(&server, &other));
    let other_view = get(&client(), &other_url).await;
    for _ in 0..3 {
        post(
            &client(),
            &other_url,
            &request(&other_view, &"x".repeat(4096)),
            StatusCode::ACCEPTED,
        )
        .await;
    }
    post(
        &client(),
        &other_url,
        &request(&other_view, &"x".repeat(4096)),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        get(&client(), &other_url).await["items"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    server.close().await;
}

#[tokio::test]
async fn steering_receipt_failure_rolls_back_the_queue_and_history() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("rollback.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, "Ask before finishing", None, None).await;
    settle(&engine).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    let id = view["run_id"].as_str().unwrap().parse().unwrap();
    let before = engine.status(id).await.unwrap();
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_steering BEFORE INSERT ON harness_turn_steering BEGIN SELECT RAISE(ABORT,'steering receipt fixture'); END;").execute(&pool).await.unwrap();
    let instruction = request(&view, "Roll back everything");
    post(
        &client(),
        &url,
        &instruction,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    let after = engine.status(id).await.unwrap();
    assert_eq!(json!(after.record()), json!(before.record()));
    sqlx::raw_sql("DROP TRIGGER reject_steering;")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        post(&client(), &url, &instruction, StatusCode::ACCEPTED).await["entry"]["sequence"],
        1
    );
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn budget_refusal_does_not_mark_steering_applied_or_charge_an_extra_call() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("budget.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(
        &server,
        &model,
        "Ask before finishing",
        Some(json!({"max_model_calls":1,"max_tokens":null})),
        None,
    )
    .await;
    settle(&engine).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    post(
        &client(),
        &url,
        &request(&view, "No budget remains"),
        StatusCode::ACCEPTED,
    )
    .await;
    answer(&server, &receipt).await;
    settle(&engine).await;
    let view = get(&client(), &url).await;
    assert_eq!(view["terminal"], true);
    assert!(view["items"][0]["application"].is_null());
    let run = engine
        .status(view["run_id"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap();
    assert_eq!(run.record().status, RunStatus::BudgetExhausted);
    assert_eq!(run.record().usage.admitted_model_calls, 1);
    assert_eq!(model.count(), 1);
    assert!(run.record().state.get("steering").is_none());
    server.close().await;
}

#[tokio::test]
async fn applied_instructions_survive_tools_and_new_instructions_append_to_future_context() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "Saved tool evidence").unwrap();
    let (engine, agents) = local_app(root.path().join("tools.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(
        &server,
        &model,
        "With tool",
        None,
        Some(
            fs::canonicalize(source)
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        ),
    )
    .await;
    settle(&engine).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    let first = request(&view, "Inspect the README");
    post(&client(), &url, &first, StatusCode::ACCEPTED).await;
    answer(&server, &receipt).await;
    settle(&engine).await;
    let view = get(&client(), &url).await;
    let application = view["items"][0]["application"].clone();
    let second = request(&view, "Explain the result");
    post(&client(), &url, &second, StatusCode::ACCEPTED).await;
    answer(&server, &receipt).await;
    settle(&engine).await;
    let prompts = model.requests.lock().unwrap().clone();
    assert_eq!(prompts.len(), 4);
    assert_eq!(prompts[1]["steering"], prompts[2]["steering"]);
    assert_eq!(prompts[3]["steering"].as_array().unwrap().len(), 2);
    assert!(
        prompts[3]["observations"]
            .to_string()
            .contains("Saved tool evidence")
    );
    let view = get(&client(), &url).await;
    assert_eq!(view["items"][0]["application"], application);
    let run = engine
        .status(view["run_id"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap();
    assert_eq!(
        run.activities()
            .iter()
            .filter(|a| a.spec.handler.name == "native.tool")
            .count(),
        1
    );
    server.close().await;
}

#[tokio::test]
async fn explicit_failed_turn_recovery_keeps_pending_steering_out_of_the_saved_request() {
    let model = Model::start().await;
    model.failures.store(2, Ordering::SeqCst);
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("recover.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, "Ask before finishing", None, None).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let frozen = execution.input.clone();
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    post(
        &client(),
        &url,
        &request(&view, "Apply after checkpoint recovery"),
        StatusCode::ACCEPTED,
    )
    .await;
    let result = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, result).await.unwrap();
    drain(&mut worker).await;
    let recovery_url = format!("{}/recovery", base(&server, &receipt));
    let recovery = get(&client(), &recovery_url).await;
    assert_eq!(recovery["retry_allowed"], true);
    let decision = json!({"request_id":MessageId::new(),"run_id":recovery["run_id"],"activity_id":recovery["activity_id"],"attempt_id":recovery["attempt_id"],"expected_history_sequence":recovery["history_sequence"]});
    post(&client(), &recovery_url, &decision, StatusCode::ACCEPTED).await;
    let resumed = admit(&mut worker).await;
    assert_eq!(resumed.input, frozen);
    let result = worker.execute(resumed.clone()).await.unwrap();
    worker.complete(resumed, result).await.unwrap();
    drain(&mut worker).await;
    assert!(get(&client(), &url).await["items"][0]["application"].is_null());
    answer(&server, &receipt).await;
    drain(&mut worker).await;
    assert!(get(&client(), &url).await["items"][0]["application"].is_object());
    server.close().await;
}

#[tokio::test]
async fn restart_after_steered_admission_replays_exact_input_before_later_instruction() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("admission-restart.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, "Pause twice", None, None).await;
    settle(&engine).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    let first = request(&view, "Already in the saved request");
    post(&client(), &url, &first, StatusCode::ACCEPTED).await;
    answer(&server, &receipt).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let frozen = execution.input.clone();
    let second = request(&view, "Arrived after admission");
    post(&client(), &url, &second, StatusCode::ACCEPTED).await;
    drop(worker);
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let mut worker = engine.worker().await.unwrap();
    let replay = admit(&mut worker).await;
    assert_eq!(replay.activity_id, execution.activity_id);
    assert_eq!(replay.ordinal, 2);
    assert_eq!(replay.input, frozen);
    let result = worker.execute(replay.clone()).await.unwrap();
    worker.complete(replay, result).await.unwrap();
    drain(&mut worker).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    assert!(view["items"][0]["application"].is_object());
    assert!(view["items"][1]["application"].is_null());
    answer(&server, &receipt).await;
    drain(&mut worker).await;
    let view = get(&client(), &url).await;
    assert!(view["items"][1]["application"].is_object());
    let prompts = model.requests.lock().unwrap().clone();
    assert_eq!(prompts[1]["steering"].as_array().unwrap().len(), 1);
    assert_eq!(prompts[2]["steering"].as_array().unwrap().len(), 2);
    let run = engine.status(execution.run_id).await.unwrap();
    assert_eq!(run.record().usage.admitted_model_calls, 4);
    assert!(
        run.attempts()
            .iter()
            .any(|a| a.status == act0r::domain::AttemptStatus::Interrupted)
    );
    server.close().await;
}

#[tokio::test]
async fn coding_steering_retains_review_baseline_permissions_and_verified_completion() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "Unchanged source").unwrap();
    let (engine, agents) = local_app(root.path().join("coding-steering.db"))
        .await
        .unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let mut input = summary::input(&model.endpoint);
    input["prompt"] = json!("Coding");
    input["workspace"] = json!(fs::canonicalize(&source).unwrap().to_string_lossy());
    input["coding"] = json!({"writable_paths":["README.md"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000}});
    let receipt = post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    let id = view["run_id"].as_str().unwrap().parse().unwrap();
    let before = engine.status(id).await.unwrap();
    let checkpoint = before.record().state["state"]["change_review_state"].clone();
    assert!(checkpoint["baseline"].is_object());
    post(
        &client(),
        &url,
        &request(&view, "Verify with the pinned command"),
        StatusCode::ACCEPTED,
    )
    .await;
    answer(&server, &receipt).await;
    settle(&engine).await;
    let after = engine.status(id).await.unwrap();
    assert_eq!(after.record().input, before.record().input);
    assert_eq!(
        after.record().state["state"]["change_review_state"],
        checkpoint
    );
    let inbox = get(&client(), &format!("{}/inbox", goal(&server, &receipt))).await;
    let approval = wait(&inbox, "tool_approval");
    super::answer(
        &client(),
        &goal(&server, &receipt),
        &approval,
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let changes = get(&client(), &format!("{}/changes", base(&server, &receipt))).await;
    assert_eq!(changes["run_status"], "succeeded");
    assert_eq!(changes["completion"]["status"], "verified");
    assert_eq!(
        fs::read_to_string(source.join("README.md")).unwrap(),
        "Unchanged source"
    );
    assert_eq!(
        model.requests.lock().unwrap()[2]["steering"][0]["text"],
        "Verify with the pinned command"
    );
    server.close().await;
}

#[tokio::test]
async fn completion_racing_steering_commits_a_pending_message_or_refuses_it() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("race.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, "Finish immediately", None, None).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let url = format!("{}/steering", base(&server, &receipt));
    let view = get(&client(), &url).await;
    let instruction = request(&view, "Racing final completion");
    let result = worker.execute(execution.clone()).await.unwrap();
    let c = client();
    let (completion, response) = tokio::join!(
        worker.complete(execution, result),
        c.post(&url).json(&instruction).send()
    );
    completion.unwrap();
    let response = response.unwrap();
    let status = response.status();
    assert!(matches!(
        status,
        StatusCode::ACCEPTED | StatusCode::CONFLICT
    ));
    drain(&mut worker).await;
    let view = get(&c, &url).await;
    assert_eq!(view["terminal"], true);
    assert_eq!(
        view["items"].as_array().unwrap().len(),
        usize::from(status == StatusCode::ACCEPTED)
    );
    if status == StatusCode::ACCEPTED {
        assert!(view["items"][0]["application"].is_null());
        assert_eq!(
            post(&c, &url, &instruction, StatusCode::ACCEPTED).await["replayed"],
            true
        );
    }
    server.close().await;
}
