use super::*;

#[tokio::test]
async fn api_serves_json_routes_without_frontend_assets() {
    let root = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(root.path().join("api.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    assert!(
        get(&client, &format!("{}/v1/harness/config", server.url))
            .await
            .is_object()
    );
    for path in [
        "/",
        "/console.css",
        "/console.mjs",
        "/core.mjs",
        "/transport.mjs",
    ] {
        let response = value(
            client
                .get(format!("{}{path}", server.url))
                .send()
                .await
                .unwrap(),
            StatusCode::NOT_FOUND,
        )
        .await;
        assert_eq!(response["error"]["code"], "not_found");
    }
    server.close().await;
}

async fn until(client: &Client, url: &str, ready: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let value = get(client, url).await;
            if ready(&value) {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn hosted_worker_drains_and_restores_paused_human_waits_before_finishing() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("host.db");
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::host(&engine, Arc::new(agents)).await;
    assert!(engine.worker().await.is_err());
    let client = client();
    let team = post(
        &client,
        &format!("{}/v1/teams", server.url),
        &spec(root.path()),
        StatusCode::OK,
    )
    .await;
    let task = MessageId::new();
    submit(
        &client,
        &server.url,
        team["id"].as_str().unwrap(),
        task,
        "Reviewed edit",
    )
    .await;
    let base = base(&server.url, &team, task);
    let before = until(&client, &format!("{base}/inbox"), |v| {
        v["items"].as_array().unwrap().len() == 2
    })
    .await;
    post(
        &client,
        &format!("{base}/pause"),
        &json!({"expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    // An idle SSE subscriber must not hold shutdown open.
    let stream = client
        .get(format!("{base}/events/stream"))
        .send()
        .await
        .unwrap();
    server.close().await;
    drop(stream);
    drop(engine.worker().await.unwrap()); // Shutdown released ownership.
    drop(engine);
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::host(&engine, Arc::new(agents)).await;
    let base = super::base(&server.url, &team, task);
    let after = get(&client, &format!("{base}/inbox")).await;
    for (before, after) in before["items"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after["items"].as_array().unwrap())
    {
        assert_eq!(before["wait"]["id"], after["wait"]["id"]);
    }
    assert_eq!(after["goal_paused"], true);
    for item in after["items"].as_array().unwrap() {
        let response = if item["kind"] == "question" {
            json!({"kind":"answer","answer":"Durability"})
        } else {
            json!({"kind":"approval","approved":true})
        };
        answer(&client, &base, item, response, StatusCode::OK).await;
    }
    assert_eq!(
        get(&client, &base).await["task"]["summary"]["settled"],
        false
    );
    post(
        &client,
        &format!("{base}/resume"),
        &json!({"expected_revision":1}),
        StatusCode::OK,
    )
    .await;
    let done = until(&client, &base, |v| v["task"]["summary"]["settled"] == true).await;
    assert_eq!(done["task"]["summary"]["succeeded"], 3);
    server.close().await;
    drop(engine.worker().await.unwrap());
}

#[tokio::test]
async fn host_startup_failure_stops_worker_and_releases_ownership() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("host.db")).await.unwrap();
    let listener = http_api::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let result = http_api::serve_with_worker(
        listener,
        Arc::new(agents),
        engine.worker().await.unwrap(),
        http_api::HarnessHostOptions {
            api: ApiOptions {
                max_event_streams: 0,
                ..Default::default()
            },
            ..Default::default()
        },
        std::future::pending(),
    )
    .await;
    assert!(result.is_err());
    drop(engine.worker().await.unwrap());
}

#[tokio::test]
async fn api_cli_with_worker_runs_submitted_work_without_a_separate_process() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("cli.db");
    let process = Process::start_with(&database, &["--with-worker"]).await;
    let client = client();
    let team = post(
        &client,
        &format!("{}/v1/teams", process.url),
        &spec(root.path()),
        StatusCode::OK,
    )
    .await;
    let task = MessageId::new();
    submit(
        &client,
        &process.url,
        team["id"].as_str().unwrap(),
        task,
        "Reviewed edit",
    )
    .await;
    let base = base(&process.url, &team, task);
    let inbox = until(&client, &format!("{base}/inbox"), |v| {
        v["items"].as_array().unwrap().len() == 2
    })
    .await;
    assert!(wait(&inbox, "question")["wait"]["id"].is_string());
    drop(process);
    let process = Process::start_with(&database, &["--with-worker"]).await;
    let restored = get(
        &client,
        &format!(
            "{}/v1/teams/{}/tasks/{task}/inbox",
            process.url,
            team["id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(inbox["items"], restored["items"]);
    let duplicate = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&database)
        .args(["api", "serve", "--bind", "127.0.0.1:0", "--with-worker"])
        .output()
        .unwrap();
    assert!(!duplicate.status.success());
}
