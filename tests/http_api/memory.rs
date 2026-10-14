use super::*;
use act0r::domain::{AgentId, SessionId};
use std::sync::atomic::{AtomicUsize, Ordering};

fn input(endpoint: &str) -> Value {
    json!({"message_id":MessageId::new(),"prompt":"Remember durable recovery","target":{"provider":"openai","model":"memory-fixture","endpoint":endpoint,"protocol":"chat_completions"}})
}
fn clear(memory: &Value) -> Value {
    json!({"request_id":MessageId::new(),"expected_revision":memory["revision"],"expected_generation":memory["generation"]})
}
fn actor<'a>(view: &'a Value, name: &str) -> &'a Value {
    view["actors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == name)
        .unwrap()
}

#[tokio::test]
async fn memory_clear_survives_restart_without_erasing_later_results_or_launch_snapshots() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let model=tokio::spawn(axum::serve(listener,axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(_):axum::Json<Value>| {
        let call=counted.fetch_add(1,Ordering::SeqCst);
        async move {
            let action=match call {
                0=>json!({"kind":"delegate_task","worker":"helper","goal":"Explain recovery"}),
                1|2=>json!({"kind":"finish","summary":"Saved recovery result"}),
                3=>json!({"kind":"ask_human","question":"Continue with recovery?"}),
                4=>json!({"kind":"finish","summary":"Pre-clear work completed"}),
                5=>json!({"kind":"finish","summary":"New generation result"}),
                _=>panic!("unexpected call {call}"),
            };
            axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":action}).to_string()}}],"usage":{"prompt_tokens":80,"completion_tokens":20}}))
        }
    }))).into_future());
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("memory.db");
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let session = receipt["session_id"].as_str().unwrap();
    let first = receipt["task_id"].as_str().unwrap();
    let base = format!("{}/v1/harness/sessions/{session}", server.url);
    let queued = get(&client, &format!("{base}/turns/{first}/memory")).await;
    assert!(queued["runs"].as_array().unwrap().is_empty());
    settle(&engine).await;
    let current = get(&client, &format!("{base}/memory")).await;
    assert_eq!(
        actor(&current, "planner")["memory"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        actor(&current, "helper")["memory"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let first_snapshots = get(&client, &format!("{base}/turns/{first}/memory")).await;
    assert_eq!(first_snapshots["runs"].as_array().unwrap().len(), 2);
    assert!(
        first_snapshots["runs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["memory"]["entries"].as_array().unwrap().is_empty())
    );
    let second = post(
        &client,
        &format!("{base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Use previous recovery result"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let turn = second["task_id"].as_str().unwrap();
    let snapshots = get(&client, &format!("{base}/turns/{turn}/memory")).await;
    assert_eq!(
        snapshots["runs"][0]["memory"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let planner = actor(&current, "planner");
    let agent = planner["agent_id"].as_str().unwrap();
    let decision = clear(&planner["memory"]);
    let clear_url = format!("{base}/actors/{agent}/memory/clear");
    let cleared = post(&client, &clear_url, &decision, StatusCode::OK).await;
    assert_eq!(cleared["replayed"], false);
    assert_eq!(cleared["generation"], 1);
    let changed = get(&client, &format!("{base}/memory")).await;
    assert!(
        actor(&changed, "planner")["memory"]["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        actor(&changed, "helper")["memory"],
        actor(&current, "helper")["memory"]
    );
    let task_base = format!(
        "{}/v1/teams/{}/tasks/{turn}",
        server.url,
        receipt["team_id"].as_str().unwrap()
    );
    let question = wait(
        &get(&client, &format!("{task_base}/inbox")).await,
        "question",
    );
    answer(
        &client,
        &task_base,
        &question,
        json!({"kind":"answer","answer":"Continue"}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert!(
        actor(&get(&client, &format!("{base}/memory")).await, "planner")["memory"]["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        get(&client, &format!("{base}/turns/{turn}/memory")).await["runs"][0]["memory"],
        snapshots["runs"][0]["memory"]
    );
    let third = post(
        &client,
        &format!("{base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Remember a new result"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let accumulated = get(&client, &format!("{base}/memory")).await;
    assert_eq!(
        actor(&accumulated, "planner")["memory"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let third_snapshot = get(
        &client,
        &format!("{base}/turns/{}/memory", third["task_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(third_snapshot["runs"][0]["memory"]["generation"], 1);
    assert!(
        third_snapshot["runs"][0]["memory"]["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    server.close().await;
    drop(engine);
    let (_engine, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let base = format!("{}/v1/harness/sessions/{session}", server.url);
    let clear_url = format!("{base}/actors/{agent}/memory/clear");
    let replay = post(&client, &clear_url, &decision, StatusCode::OK).await;
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["revision"], cleared["revision"]);
    assert_eq!(get(&client, &format!("{base}/memory")).await, accumulated);
    let mut stale = decision.clone();
    stale["request_id"] = json!(MessageId::new());
    post(&client, &clear_url, &stale, StatusCode::CONFLICT).await;
    let helper = actor(&current, "helper")["agent_id"].as_str().unwrap();
    post(
        &client,
        &format!("{base}/actors/{helper}/memory/clear"),
        &decision,
        StatusCode::CONFLICT,
    )
    .await;
    post(
        &client,
        &format!("{base}/metadata"),
        &json!({"expected_revision":0,"kind":"archive"}),
        StatusCode::OK,
    )
    .await;
    post(
        &client,
        &clear_url,
        &clear(&actor(&accumulated, "planner")["memory"]),
        StatusCode::OK,
    )
    .await;
    let conversation = get(&client, &base).await;
    assert_eq!(conversation["turns"].as_array().unwrap().len(), 3);
    assert_eq!(
        conversation["turns"][0]["response"],
        "Saved recovery result"
    );
    assert_eq!(
        get(&client, &format!("{base}/turns/{first}/memory")).await,
        first_snapshots
    );
    assert_eq!(
        post(&client, &clear_url, &decision, StatusCode::OK).await["replayed"],
        true
    );
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    server.close().await;
    model.abort();
}

#[tokio::test]
async fn memory_routes_scope_validate_rollback_and_serialize_concurrent_clears() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("scope.db");
    let (_, agents) = local_app(&path).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input("http://127.0.0.1:1234/v1/chat/completions"),
        StatusCode::ACCEPTED,
    )
    .await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    let current = get(&client, &format!("{base}/memory")).await;
    let planner = actor(&current, "planner");
    let agent = planner["agent_id"].as_str().unwrap();
    let clear_url = format!("{base}/actors/{agent}/memory/clear");
    for url in [
        format!(
            "{}/v1/harness/sessions/{}/memory",
            server.url,
            SessionId::new()
        ),
        format!("{base}/turns/{}/memory", MessageId::new()),
    ] {
        assert_eq!(
            client.get(url).send().await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }
    let foreign = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input("http://127.0.0.1:1234/v1/chat/completions"),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(
        client
            .get(format!(
                "{base}/turns/{}/memory",
                foreign["task_id"].as_str().unwrap()
            ))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    post(
        &client,
        &format!("{base}/actors/{}/memory/clear", AgentId::new()),
        &clear(&planner["memory"]),
        StatusCode::NOT_FOUND,
    )
    .await;
    let foreign_memory = get(
        &client,
        &format!(
            "{}/v1/harness/sessions/{}/memory",
            server.url,
            foreign["session_id"].as_str().unwrap()
        ),
    )
    .await;
    post(
        &client,
        &format!(
            "{base}/actors/{}/memory/clear",
            actor(&foreign_memory, "planner")["agent_id"]
                .as_str()
                .unwrap()
        ),
        &clear(&planner["memory"]),
        StatusCode::NOT_FOUND,
    )
    .await;
    let mut extra = clear(&planner["memory"]);
    extra["secret"] = json!(true);
    post(
        &client,
        &clear_url,
        &extra,
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    for change in [
        json!({"request_id":"bad","expected_revision":0,"expected_generation":0}),
        json!({"request_id":MessageId::new(),"expected_revision":-1,"expected_generation":0}),
    ] {
        post(
            &client,
            &clear_url,
            &change,
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await;
    }
    let nil = json!({"request_id":"00000000-0000-0000-0000-000000000000","expected_revision":0,"expected_generation":0});
    post(&client, &clear_url, &nil, StatusCode::BAD_REQUEST).await;
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&path))
            .await
            .unwrap();
    sqlx::query("CREATE TRIGGER reject_memory_clear BEFORE INSERT ON harness_memory_clears BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END").execute(&pool).await.unwrap();
    let decision = clear(&planner["memory"]);
    post(
        &client,
        &clear_url,
        &decision,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(get(&client, &format!("{base}/memory")).await, current);
    sqlx::query("DROP TRIGGER reject_memory_clear")
        .execute(&pool)
        .await
        .unwrap();
    let (one, two) = tokio::join!(
        post(&client, &clear_url, &decision, StatusCode::OK),
        post(&client, &clear_url, &decision, StatusCode::OK)
    );
    assert_ne!(one["replayed"], two["replayed"]);
    assert_eq!(one["revision"], 1);
    assert_eq!(two["generation"], 1);
    let memory = get(&client, &format!("{base}/memory")).await;
    let first = clear(&actor(&memory, "planner")["memory"]);
    let second = clear(&actor(&memory, "planner")["memory"]);
    let (one, two) = tokio::join!(
        client.post(&clear_url).json(&first).send(),
        client.post(&clear_url).json(&second).send()
    );
    let statuses = [one.unwrap().status(), two.unwrap().status()];
    assert!(statuses.contains(&StatusCode::OK));
    assert!(statuses.contains(&StatusCode::CONFLICT));
    pool.close().await;
    server.close().await;
}
