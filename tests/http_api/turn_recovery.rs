use super::*;
use act0r::domain::{RunId, RunStatus};
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
// Keep the test fixture compatible with the repository's Rust 1.94 minimum.
#[allow(deprecated)]
fn take_failure(count: &AtomicUsize) -> bool {
    count
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
}
impl Model {
    async fn start(tool: bool) -> Self {
        Self::start_mode(tool, false).await
    }
    async fn start_mode(tool: bool, delegate: bool) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let failures = Arc::new(AtomicUsize::new(2));
        let seen = requests.clone();
        let fail = failures.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            axum::serve(listener,axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
                let prompt:Value=serde_json::from_str(body["messages"].as_array().unwrap().last().unwrap()["content"].as_str().unwrap()).unwrap();
                let helper=delegate && prompt.get("workers").is_none();
                let index={let mut requests=seen.lock().unwrap();let n=requests.len();requests.push(body.clone());n};
                let content=if delegate && index==0 {json!({"action":{"kind":"delegate_task","worker":"helper","goal":"Inspect the checkpoint"}}).to_string()}
                else if helper {json!({"action":{"kind":"finish","summary":"Completed child evidence"}}).to_string()}
                else if tool && index==0 {json!({"action":native_wire_action(json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),&body)}).to_string()}
                else if take_failure(&fail) {"not a structured action".into()}
                else {json!({"action":{"kind":"finish","summary":"Recovered from the saved checkpoint."}}).to_string()};
                async move {axum::Json(json!({"id":"recovery-fixture","choices":[{"finish_reason":"stop","message":{"content":content}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
            }))).await.unwrap();
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
fn decision(view: &Value) -> Value {
    json!({"request_id":MessageId::new(),"run_id":view["run_id"],"activity_id":view["activity_id"],"attempt_id":view["attempt_id"],"expected_history_sequence":view["history_sequence"]})
}
fn recovery_url(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}/recovery",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
async fn start(
    server: &Server,
    model: &Model,
    budget: Option<Value>,
    workspace: Option<String>,
) -> Value {
    let mut input = summary::input(&model.endpoint);
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

#[tokio::test]
async fn recovery_reuses_tools_and_exact_request_survives_restart_and_duplicate_clicks() {
    let model = Model::start(true).await;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("project");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("README.md"), "Original checkpoint evidence").unwrap();
    let db = root.path().join("recovery.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(
        &server,
        &model,
        None,
        Some(
            fs::canonicalize(source)
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        ),
    )
    .await;
    let url = recovery_url(&server, &receipt);
    settle(&engine).await;
    let view = get(&client(), &url).await;
    assert_eq!(view["retry_allowed"], true, "{view}");
    assert_eq!(model.count(), 3);
    let id: RunId = view["run_id"].as_str().unwrap().parse().unwrap();
    let before = engine.status(id).await.unwrap();
    assert_eq!(before.record().status, RunStatus::Failed);
    let tool = before
        .activities()
        .iter()
        .find(|a| a.spec.input["definition"]["id"]["name"] == "read_file")
        .unwrap();
    let tool_id = tool.id;
    let old_state = before.record().state.clone();
    let old_input = before.record().input.clone();
    let frozen = before
        .activities()
        .iter()
        .find(|a| a.id.to_string() == view["activity_id"].as_str().unwrap())
        .unwrap()
        .spec
        .input
        .clone();
    // A replay of the read would return different evidence. Recovery must use its saved result.
    let workspace = tool.spec.input["workspace"].as_str().unwrap();
    fs::write(
        Path::new(workspace).join("README.md"),
        "Changed after failed turn",
    )
    .unwrap();
    let request = decision(&view);
    let c = client();
    let (a, b) = tokio::join!(
        post(&c, &url, &request, StatusCode::ACCEPTED),
        post(&c, &url, &request, StatusCode::ACCEPTED)
    );
    assert_eq!(a["resumed_activity_id"], b["resumed_activity_id"]);
    assert_ne!(a["replayed"], b["replayed"]);
    let saved = engine.status(id).await.unwrap();
    assert_eq!(saved.record().state, old_state);
    assert_eq!(saved.record().input, old_input);
    assert_eq!(json!(saved.record().usage), json!(before.record().usage));
    assert_eq!(saved.activities().last().unwrap().spec.input, frozen);
    assert_eq!(
        saved.activities().last().unwrap().spec.retry.max_attempts,
        1
    );
    assert_eq!(model.count(), 3, "HTTP admission does not execute inline");
    let mut competing = request.clone();
    competing["request_id"] = json!(MessageId::new());
    post(&c, &url, &competing, StatusCode::CONFLICT).await;
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let url = recovery_url(&server, &receipt);
    let replay = post(&c, &url, &request, StatusCode::ACCEPTED).await;
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["resumed_activity_id"], a["resumed_activity_id"]);
    settle(&engine).await;
    let recovered = engine.status(id).await.unwrap();
    assert_eq!(recovered.record().status, RunStatus::Succeeded);
    assert_eq!(recovered.record().usage.admitted_model_calls, 4);
    assert_eq!(recovered.record().usage.actual_input_tokens, 40);
    assert_eq!(
        recovered
            .activities()
            .iter()
            .filter(|a| a.spec.input["definition"]["id"]["name"] == "read_file")
            .count(),
        1
    );
    assert_eq!(
        recovered
            .activities()
            .iter()
            .find(|a| a.id == tool_id)
            .unwrap()
            .result,
        tool.result
    );
    assert_eq!(recovered.attempts().len(), before.attempts().len() + 1);
    {
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests[1], requests[3], "model payload must remain frozen");
        assert!(
            requests[3]
                .to_string()
                .contains("Original checkpoint evidence")
        );
        assert!(
            !requests[3]
                .to_string()
                .contains("Changed after failed turn")
        );
    }
    assert_eq!(get(&c, &url).await["receipts"].as_array().unwrap().len(), 1);
    let session = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    let memory = get(&c, &format!("{session}/memory")).await;
    assert_eq!(
        memory["actors"][0]["memory"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "successful recovery is remembered once"
    );
    server.close().await;
}

#[tokio::test]
async fn failed_recovery_needs_new_decision_and_preserves_all_charges() {
    let model = Model::start(false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("recovery.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, None, None).await;
    let url = recovery_url(&server, &receipt);
    settle(&engine).await;
    let request = decision(&get(&client(), &url).await);
    model.failures.store(1, Ordering::SeqCst);
    post(&client(), &url, &request, StatusCode::ACCEPTED).await;
    settle(&engine).await;
    assert_eq!(model.count(), 3, "manual decision allows only one attempt");
    let again = get(&client(), &url).await;
    assert_eq!(again["retry_allowed"], true);
    assert_ne!(again["activity_id"], request["activity_id"]);
    post(&client(), &url, &request, StatusCode::ACCEPTED).await;
    assert_eq!(model.count(), 3);
    let mut stale = request.clone();
    stale["request_id"] = json!(MessageId::new());
    post(&client(), &url, &stale, StatusCode::CONFLICT).await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    let memory = get(&client(), &format!("{base}/memory")).await;
    let planner = memory["actors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "planner")
        .unwrap();
    post(&client(),&format!("{base}/actors/{}/memory/clear",planner["agent_id"].as_str().unwrap()),&json!({"request_id":MessageId::new(),"expected_revision":planner["memory"]["revision"],"expected_generation":planner["memory"]["generation"]}),StatusCode::OK).await;
    post(&client(), &url, &decision(&again), StatusCode::ACCEPTED).await;
    settle(&engine).await;
    let run = engine
        .status(request["run_id"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().usage.admitted_model_calls, 4);
    let memory = get(&client(), &format!("{base}/memory")).await;
    let planner = memory["actors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "planner")
        .unwrap();
    assert!(
        planner["memory"]["entries"].as_array().unwrap().is_empty(),
        "recovery retains the original launch generation after a memory clear"
    );
    let pool = summary::pool(&root.path().join("recovery.db")).await;
    let completed: i64 = sqlx::query_scalar(
        "SELECT json_extract(payload,'$.state.completed') FROM agent_actors WHERE id=?",
    )
    .bind(planner["agent_id"].as_str().unwrap())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        completed, 1,
        "repeated terminal receipts still represent one completed task"
    );
    pool.close().await;
    assert_eq!(
        get(&client(), &url).await["receipts"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    server.close().await;
}

#[tokio::test]
async fn recovery_blocks_later_turn_until_completion_and_refuses_consumed_history() {
    let model = Model::start(false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("recovery.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, None, None).await;
    let url = recovery_url(&server, &receipt);
    settle(&engine).await;
    let request = decision(&get(&client(), &url).await);
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    let next = post(
        &client(),
        &format!("{base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Next turn"}),
        StatusCode::ACCEPTED,
    )
    .await;
    post(&client(), &url, &request, StatusCode::ACCEPTED).await;
    let worker = engine.worker().await.unwrap();
    // Recovery is already queued; the later root task cannot start during it.
    let pool = summary::pool(&root.path().join("recovery.db")).await;
    let ready: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_goal_ready_mailbox WHERE id=?")
        .bind(next["task_id"].as_str().unwrap())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ready, 0);
    drop(worker);
    settle(&engine).await;
    assert_eq!(model.count(), 4);
    let mut stale = request;
    stale["request_id"] = json!(MessageId::new());
    post(&client(), &url, &stale, StatusCode::CONFLICT).await;
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn recovery_refuses_exhausted_budget_archival_and_other_session_scope() {
    let model = Model::start(false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("recovery.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let finite = start(
        &server,
        &model,
        Some(json!({"max_model_calls":2,"max_tokens":null})),
        None,
    )
    .await;
    settle(&engine).await;
    let finite_url = recovery_url(&server, &finite);
    let view = get(&client(), &finite_url).await;
    assert_eq!(view["retry_allowed"], false);
    assert!(view["blocked_reason"].as_str().unwrap().contains("budget"));
    post(
        &client(),
        &finite_url,
        &decision(&view),
        StatusCode::CONFLICT,
    )
    .await;
    model.failures.store(2, Ordering::SeqCst);
    let second = start(&server, &model, None, None).await;
    settle(&engine).await;
    let second_url = recovery_url(&server, &second);
    let second_view = get(&client(), &second_url).await;
    let request = decision(&second_view);
    let wrong = format!(
        "{}/v1/harness/sessions/{}/turns/{}/recovery",
        server.url,
        finite["session_id"].as_str().unwrap(),
        second["task_id"].as_str().unwrap()
    );
    value(
        client().get(&wrong).send().await.unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    post(&client(), &finite_url, &request, StatusCode::CONFLICT).await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        second["session_id"].as_str().unwrap()
    );
    let metadata = get(&client(), &format!("{base}/metadata")).await;
    post(
        &client(),
        &format!("{base}/metadata"),
        &json!({"kind":"archive","expected_revision":metadata["revision"]}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(get(&client(), &second_url).await["retry_allowed"], false);
    post(&client(), &second_url, &request, StatusCode::CONFLICT).await;
    let metadata = get(&client(), &format!("{base}/metadata")).await;
    post(
        &client(),
        &format!("{base}/metadata"),
        &json!({"kind":"restore","expected_revision":metadata["revision"]}),
        StatusCode::OK,
    )
    .await;
    let goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        second["team_id"].as_str().unwrap(),
        second["task_id"].as_str().unwrap()
    );
    post(
        &client(),
        &format!("{goal}/cancel"),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(get(&client(), &second_url).await["retry_allowed"], false);
    post(&client(), &second_url, &request, StatusCode::CONFLICT).await;
    server.close().await;
}

#[tokio::test]
async fn recovery_refuses_a_failed_turn_after_later_context_launch_or_summary() {
    let model = Model::start(false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("recovery.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let first = start(&server, &model, None, None).await;
    settle(&engine).await;
    let url = recovery_url(&server, &first);
    let request = decision(&get(&client(), &url).await);
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        first["session_id"].as_str().unwrap()
    );
    post(
        &client(),
        &format!("{base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Follow up"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert!(
        get(&client(), &url).await["blocked_reason"]
            .as_str()
            .unwrap()
            .contains("later turn")
    );
    post(&client(), &url, &request, StatusCode::CONFLICT).await;
    model.failures.store(2, Ordering::SeqCst);
    let second = start(&server, &model, None, None).await;
    settle(&engine).await;
    let url = recovery_url(&server, &second);
    let request = decision(&get(&client(), &url).await);
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        second["session_id"].as_str().unwrap()
    );
    let sources = get(&client(), &format!("{base}/summary")).await;
    post(
        &client(),
        &format!("{base}/summary"),
        &summary::decision(&sources),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(get(&client(), &url).await["retry_allowed"], false);
    post(&client(), &url, &request, StatusCode::CONFLICT).await;
    settle(&engine).await;
    assert!(
        get(&client(), &url).await["blocked_reason"]
            .as_str()
            .unwrap()
            .contains("summarized")
    );
    post(&client(), &url, &request, StatusCode::CONFLICT).await;
    server.close().await;
}

#[tokio::test]
async fn recovery_transaction_rolls_back_actor_task_run_and_receipt() {
    let model = Model::start(false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("recovery.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, None, None).await;
    settle(&engine).await;
    let url = recovery_url(&server, &receipt);
    let view = get(&client(), &url).await;
    let request = decision(&view);
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_recovery BEFORE INSERT ON harness_turn_recoveries BEGIN SELECT RAISE(ABORT,'rollback recovery'); END;").execute(&pool).await.unwrap();
    post(&client(), &url, &request, StatusCode::INTERNAL_SERVER_ERROR).await;
    assert_eq!(get(&client(), &url).await, view);
    sqlx::raw_sql("DROP TRIGGER reject_recovery")
        .execute(&pool)
        .await
        .unwrap();
    post(&client(), &url, &request, StatusCode::ACCEPTED).await;
    settle(&engine).await;
    assert_eq!(model.count(), 3);
    let mut conflict = request.clone();
    conflict["expected_history_sequence"] = json!(9999);
    post(&client(), &url, &conflict, StatusCode::CONFLICT).await;
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn recovery_preserves_shared_child_charges_and_reuses_completed_delegation() {
    for limit in [Some(4), None] {
        let model = Model::start_mode(false, true).await;
        let root = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(root.path().join("recovery.db")).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let receipt = start(
            &server,
            &model,
            Some(json!({"max_model_calls":limit,"max_tokens":null})),
            None,
        )
        .await;
        settle(&engine).await;
        assert_eq!(model.count(), 4);
        let url = recovery_url(&server, &receipt);
        let view = get(&client(), &url).await;
        assert_eq!(view["usage"]["admitted_model_calls"], 3);
        if limit.is_some() {
            assert_eq!(view["retry_allowed"], false);
            assert!(
                view["blocked_reason"]
                    .as_str()
                    .unwrap()
                    .contains("shared budget")
            );
            post(&client(), &url, &decision(&view), StatusCode::CONFLICT).await;
        } else {
            assert_eq!(view["retry_allowed"], true, "{view}");
            let id = view["run_id"].as_str().unwrap().parse().unwrap();
            let before = engine.status(id).await.unwrap();
            assert_eq!(before.record().delegations.len(), 1);
            post(&client(), &url, &decision(&view), StatusCode::ACCEPTED).await;
            settle(&engine).await;
            let after = engine.status(id).await.unwrap();
            assert_eq!(after.record().status, RunStatus::Succeeded);
            assert_eq!(
                json!(before.record().delegations),
                json!(after.record().delegations)
            );
            assert_eq!(model.count(), 5);
        }
        server.close().await;
    }
}

#[tokio::test]
async fn recovery_retains_provider_deadline_and_excludes_authentication_failures() {
    for status in [StatusCode::TOO_MANY_REQUESTS, StatusCode::UNAUTHORIZED] {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let provider = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().route(
                    "/v1/chat/completions",
                    axum::routing::post(move || {
                        counted.fetch_add(1, Ordering::SeqCst);
                        async move { (status, [("retry-after", "60")], "provider failure") }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(root.path().join("recovery.db")).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let mut input = summary::input(&endpoint);
        input["target"]["policy"]["max_attempts"] = json!(1);
        let receipt = post(
            &client(),
            &format!("{}/v1/harness/sessions", server.url),
            &input,
            StatusCode::ACCEPTED,
        )
        .await;
        settle(&engine).await;
        let url = recovery_url(&server, &receipt);
        let view = get(&client(), &url).await;
        assert_eq!(view["error"]["code"], "model_http_status");
        if status == StatusCode::UNAUTHORIZED {
            assert_eq!(view["retry_allowed"], false);
        } else {
            assert_eq!(view["retry_allowed"], true);
            let id = view["run_id"].as_str().unwrap().parse().unwrap();
            let before = engine.status(id).await.unwrap();
            let attempt = before.attempts().last().unwrap();
            let minimum = before
                .record()
                .error
                .as_ref()
                .unwrap()
                .retry_after
                .as_ref()
                .unwrap()
                .deadline(attempt.ended_at.unwrap());
            post(&client(), &url, &decision(&view), StatusCode::ACCEPTED).await;
            let saved = engine.status(id).await.unwrap();
            assert!(saved.activities().last().unwrap().ready_at >= minimum);
            let mut worker = engine.worker().await.unwrap();
            worker.step().await.unwrap();
            drop(worker);
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "provider advice prevents immediate manual dispatch"
            );
        }
        server.close().await;
        provider.abort();
    }
}

#[tokio::test]
async fn recovery_refuses_newer_advanced_work_on_the_same_planner() {
    let model = Model::start(false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("recovery.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, None, None).await;
    settle(&engine).await;
    let url = recovery_url(&server, &receipt);
    let request = decision(&get(&client(), &url).await);
    let tasks = format!(
        "{}/v1/teams/{}/tasks",
        server.url,
        receipt["team_id"].as_str().unwrap()
    );
    post(
        &client(),
        &tasks,
        &json!({"message_id":MessageId::new(),"goal":"New advanced work"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let view = get(&client(), &url).await;
    assert_eq!(view["retry_allowed"], false);
    assert!(
        view["blocked_reason"]
            .as_str()
            .unwrap()
            .contains("other work")
    );
    post(&client(), &url, &request, StatusCode::CONFLICT).await;
    assert_eq!(model.count(), 3);
    server.close().await;
}
