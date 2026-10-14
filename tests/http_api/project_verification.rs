use super::*;
use act0r::domain::{RunId, project_verification as contract};
use std::sync::atomic::Ordering;
#[test]
#[ignore = "real source-project verifier invoked by the parent HTTP tests"]
fn verifier_child() {
    assert!(std::env::var("OPENAI_API_KEY").is_err());
    assert_eq!(fs::read_to_string("logic.txt").unwrap(), "fixed\n");
    let marker = Path::new(".act0r-tools/verification-count");
    let count = fs::read_to_string(marker)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    fs::write(marker, (count + 1).to_string()).unwrap();
    match fs::read_to_string("mode.txt").unwrap().as_str() {
        "fail" => panic!("source-project verification failed"),
        "mutate" => fs::write("README.md", "changed during tests\n").unwrap(),
        "slow" => std::thread::sleep(Duration::from_secs(2)),
        _ => {}
    }
    println!("original project verified");
}
fn request(endpoint: &str, source: &Path) -> Value {
    json!({"message_id":MessageId::new(),"prompt":"Repair logic and verify it","workspace":source,"target":{"provider":"openai","model":"verification-fixture","endpoint":endpoint,"protocol":"chat_completions"},"coding":{"writable_paths":["logic.txt"],"test_command":{"argv":[std::env::current_exe().unwrap(),"--ignored","--exact","project_verification::verifier_child","--nocapture"],"timeout_ms":500}}})
}
async fn setup(
    root: &Path,
) -> (
    Engine,
    Arc<AgentService>,
    Server,
    Client,
    Value,
    Value,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
    JoinHandle<()>,
) {
    let source = root.join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("logic.txt"), "broken\n").unwrap();
    fs::write(source.join("mode.txt"), "pass").unwrap();
    fs::write(source.join("README.md"), "source verifier\n").unwrap();
    let (endpoint,count,provider)=super::coding::provider(vec![json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"logic.txt","old":"broken\n","new":"fixed\n"}}),json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),json!({"kind":"finish","summary":"repaired and tested"})]).await;
    let (engine, agents) = local_app(root.join("verify.db")).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &request(&endpoint, &source),
        StatusCode::ACCEPTED,
    )
    .await;
    let goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    for _ in 0..2 {
        settle(&engine).await;
        let item = wait(
            &get(&client, &format!("{goal}/inbox")).await,
            "tool_approval",
        );
        answer(
            &client,
            &goal,
            &item,
            json!({"kind":"approval","approved":true}),
            StatusCode::OK,
        )
        .await;
    }
    settle(&engine).await;
    let base = format!(
        "{}/v1/harness/sessions/{}/turns/{}/applications",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    let app_id = RunId::new();
    post(
        &client,
        &base,
        &json!({"application_id":app_id}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let app = get(&client, &format!("{base}/{app_id}")).await;
    let refused = client
        .post(format!("{base}/{app_id}/verifications"))
        .json(&json!({"verification_id":RunId::new()}))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
    post(
        &client,
        &format!("{base}/{app_id}/responses"),
        &json!({"wait_id":app["wait"]["id"],"approved":true}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let app = get(&client, &format!("{base}/{app_id}")).await;
    assert_eq!(app["report"]["status"], "applied");
    assert_eq!(count.load(Ordering::SeqCst), 3);
    (
        engine, agents, server, client, receipt, app, count, provider,
    )
}
fn base(server: &Server, receipt: &Value, app: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}/applications/{}/verifications",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap(),
        app["run_id"].as_str().unwrap()
    )
}
async fn prepared(engine: &Engine, client: &Client, base: &str) -> (Value, Value) {
    let body = json!({"verification_id":RunId::new()});
    let receipt = post(client, base, &body, StatusCode::ACCEPTED).await;
    settle(engine).await;
    let view = get(
        client,
        &format!("{base}/{}", receipt["run_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(view["wait"]["key"], contract::APPROVAL_KEY);
    (body, view)
}
async fn approve(client: &Client, base: &str, view: &Value, approved: bool) {
    let url = format!("{base}/{}/responses", view["run_id"].as_str().unwrap());
    let body = json!({"wait_id":view["wait"]["id"],"approved":approved});
    post(client, &url, &body, StatusCode::OK).await;
    post(client, &url, &body, StatusCode::OK).await;
}
#[tokio::test]
async fn applied_project_verification_pins_scope_recovers_approval_and_replays_once() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, server, client, receipt, app, count, provider) = setup(root.path()).await;
    let route = base(&server, &receipt, &app);
    let body = json!({"verification_id":RunId::new()});
    let (a, b) = tokio::join!(
        post(&client, &route, &body, StatusCode::ACCEPTED),
        post(&client, &route, &body, StatusCode::ACCEPTED)
    );
    assert_eq!(a, b);
    settle(&engine).await;
    let id = a["run_id"].as_str().unwrap();
    let before = get(&client, &format!("{route}/{id}")).await;
    assert_eq!(before["input"]["target"], app["preview"]["target"]);
    assert_eq!(
        before["input"]["command"],
        app["preview"]["tests"]["command"]
    );
    let bad = client
        .post(&route)
        .json(&json!({"verification_id":RunId::new(),"command":{"argv":["changed"]}}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let another = client
        .post(&route)
        .json(&json!({"verification_id":RunId::new()}))
        .send()
        .await
        .unwrap();
    assert_eq!(another.status(), StatusCode::CONFLICT);
    assert_eq!(
        get(&client, &route).await["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let metadata = format!(
        "{}/v1/harness/sessions/{}/metadata",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    let meta = get(&client, &metadata).await;
    assert!(
        meta["archive_blocked_reason"]
            .as_str()
            .unwrap()
            .contains("verification")
    );
    post(
        &client,
        &metadata,
        &json!({"kind":"archive","expected_revision":meta["revision"]}),
        StatusCode::CONFLICT,
    )
    .await;
    server.close().await;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(root.path().join("verify.db")).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let route = base(&server, &receipt, &app);
    assert_eq!(get(&client, &format!("{route}/{id}")).await, before);
    approve(&client, &route, &before, true).await;
    settle(&engine).await;
    let saved = get(&client, &format!("{route}/{id}")).await;
    assert_eq!(saved["report"]["status"], "verified");
    assert_eq!(saved["report"]["output"]["ok"], true);
    assert_eq!(
        saved["report"]["before"]["digest"],
        saved["report"]["after"]["digest"]
    );
    assert!(
        saved["report"]["output"]["stdout"]
            .as_str()
            .unwrap()
            .contains("original project verified")
    );
    assert_eq!(
        fs::read_to_string(root.path().join("project/.act0r-tools/verification-count")).unwrap(),
        "1"
    );
    let other = format!(
        "{}/v1/harness/sessions/{}/turns/{}/applications/{}/verifications/{id}",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        MessageId::new(),
        app["run_id"].as_str().unwrap()
    );
    assert_eq!(
        client.get(other).send().await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    let metadata = format!(
        "{}/v1/harness/sessions/{}/metadata",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    let meta = get(&client, &metadata).await;
    post(
        &client,
        &metadata,
        &json!({"kind":"archive","expected_revision":meta["revision"]}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(post(&client, &route, &body, StatusCode::ACCEPTED).await, a);
    post(
        &client,
        &route,
        &json!({"verification_id":RunId::new()}),
        StatusCode::CONFLICT,
    )
    .await;
    settle(&engine).await;
    assert_eq!(get(&client, &format!("{route}/{id}")).await, saved);
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert_eq!(
        engine
            .status(id.parse().unwrap())
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        0
    );
    server.close().await;
    provider.abort();
}
#[tokio::test]
async fn failed_changed_and_rejected_source_verifications_keep_real_evidence() {
    let root = tempfile::tempdir().unwrap();
    let (engine, _agents, server, client, receipt, app, count, provider) = setup(root.path()).await;
    let route = base(&server, &receipt, &app);
    let source = root.path().join("project");
    fs::write(source.join("mode.txt"), "fail").unwrap();
    let (_, view) = prepared(&engine, &client, &route).await;
    approve(&client, &route, &view, true).await;
    settle(&engine).await;
    let failed = get(
        &client,
        &format!("{route}/{}", view["run_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(failed["status"], "succeeded");
    assert_eq!(failed["report"]["status"], "failed");
    assert_eq!(failed["report"]["output"]["ok"], false);
    fs::write(source.join("mode.txt"), "pass").unwrap();
    let (_, view) = prepared(&engine, &client, &route).await;
    fs::write(source.join("README.md"), "changed after review\n").unwrap();
    approve(&client, &route, &view, true).await;
    settle(&engine).await;
    let changed = get(
        &client,
        &format!("{route}/{}", view["run_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(changed["report"]["status"], "changed");
    assert!(changed["report"]["output"].is_null());
    assert_eq!(
        fs::read_to_string(source.join(".act0r-tools/verification-count")).unwrap(),
        "1"
    );
    fs::write(source.join("mode.txt"), "mutate").unwrap();
    let (_, view) = prepared(&engine, &client, &route).await;
    approve(&client, &route, &view, true).await;
    settle(&engine).await;
    let changed = get(
        &client,
        &format!("{route}/{}", view["run_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(changed["report"]["status"], "changed");
    assert_eq!(changed["report"]["output"]["ok"], true);
    assert_ne!(
        changed["report"]["before"]["digest"],
        changed["report"]["after"]["digest"]
    );
    let (_, view) = prepared(&engine, &client, &route).await;
    approve(&client, &route, &view, false).await;
    settle(&engine).await;
    assert_eq!(
        get(
            &client,
            &format!("{route}/{}", view["run_id"].as_str().unwrap())
        )
        .await["report"]["status"],
        "rejected"
    );
    assert_eq!(
        fs::read_to_string(source.join(".act0r-tools/verification-count")).unwrap(),
        "2"
    );
    assert_eq!(count.load(Ordering::SeqCst), 3);
    server.close().await;
    provider.abort();
}
#[tokio::test]
async fn published_verification_receipt_recovers_without_second_process_and_timeout_needs_reconciliation()
 {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, server, client, receipt, app, count, provider) = setup(root.path()).await;
    let route = base(&server, &receipt, &app);
    let (_, view) = prepared(&engine, &client, &route).await;
    approve(&client, &route, &view, true).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.handler, contract::handler("run"));
    let outcome = worker.execute(execution.clone()).await.unwrap();
    assert!(matches!(outcome, act0r::domain::Outcome::Success(_)));
    drop(worker);
    server.close().await;
    drop(agents);
    drop(engine);
    fs::write(root.path().join("project/README.md"), "later user edit\n").unwrap();
    let (engine, agents) = local_app(root.path().join("verify.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let route = base(&server, &receipt, &app);
    let recovery_url = format!(
        "{route}/{}/reconciliation",
        view["run_id"].as_str().unwrap()
    );
    let worker = engine.worker().await.unwrap();
    drop(worker);
    let blocked = get(
        &client,
        &format!("{route}/{}", view["run_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(blocked["status"], "blocked");
    assert_eq!(blocked["recovery"]["inspection"]["saved_report"], true);
    assert_eq!(blocked["recovery"]["inspection"]["project_matches"], false);
    assert_eq!(blocked["recovery"]["retry_allowed"], true);
    let mut decision = json!({"activity_id":execution.activity_id,"attempt_id":execution.attempt_id,"decision":{"action":"retry"},"evidence":"Stopped previous worker; completion receipt exists","operation_stopped":false});
    post(&client, &recovery_url, &decision, StatusCode::BAD_REQUEST).await;
    decision["operation_stopped"] = json!(true);
    post(&client, &recovery_url, &decision, StatusCode::OK).await;
    settle(&engine).await;
    post(&client, &recovery_url, &decision, StatusCode::OK).await;
    let done = get(
        &client,
        &format!("{route}/{}", view["run_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(done["report"]["status"], "verified");
    assert_eq!(
        fs::read_to_string(root.path().join("project/.act0r-tools/verification-count")).unwrap(),
        "1"
    );
    fs::write(root.path().join("project/mode.txt"), "slow").unwrap();
    let (_, view) = prepared(&engine, &client, &route).await;
    approve(&client, &route, &view, true).await;
    settle(&engine).await;
    let blocked = get(
        &client,
        &format!("{route}/{}", view["run_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(blocked["status"], "blocked");
    assert!(blocked["report"].is_null());
    assert_eq!(blocked["recovery"]["inspection"]["saved_report"], false);
    let decision = json!({"activity_id":blocked["recovery"]["activity_id"],"attempt_id":blocked["recovery"]["attempt_id"],"decision":{"action":"fail","message":"Timed out; do not rerun"},"evidence":"Verified prior test process was terminated","operation_stopped":true});
    post(
        &client,
        &format!(
            "{route}/{}/reconciliation",
            view["run_id"].as_str().unwrap()
        ),
        &decision,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        get(
            &client,
            &format!("{route}/{}", view["run_id"].as_str().unwrap())
        )
        .await["status"],
        "failed"
    );
    assert_eq!(count.load(Ordering::SeqCst), 3);
    server.close().await;
    provider.abort();
}
