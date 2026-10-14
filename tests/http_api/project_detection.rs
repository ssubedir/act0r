use super::*;
use act0r::domain::ProjectProfileId;

#[tokio::test]
async fn detection_is_read_only_and_reviewed_profile_settings_pin_sessions_across_restart() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/lib.rs"), "pub fn value() -> u8 { 1 }\n").unwrap();
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname='detected'\nversion='0.1.0'\n[workspace]\n",
    )
    .unwrap();
    fs::write(source.join("Cargo.lock"), "version = 4\n").unwrap();
    let workspace = source.to_string_lossy().into_owned();
    let db = root.path().join("detect.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let detection = post(
        &client,
        &format!("{}/v1/harness/project-detection", server.url),
        &json!({"workspace":workspace}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(detection["kind"], "rust_package");
    let catalog = format!("{}/v1/harness/project-profiles", server.url);
    assert!(
        get(&client, &catalog).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        get(&client, &format!("{}/v1/harness/sessions", server.url)).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(!source.join("target").exists());
    let mut settings = detection["suggested"].clone();
    settings["coding"]["writable_paths"] = json!(["src/lib.rs"]);
    settings["coding"]["coder_count"] = json!(2);
    let id = ProjectProfileId::new();
    post(&client,&format!("{catalog}/{id}"),&json!({"request_id":MessageId::new(),"expected_revision":0,"action":{"kind":"save","settings":settings}}),StatusCode::OK).await;
    let request = json!({"message_id":MessageId::new(),"prompt":"Inspect","workspace":workspace,"coding":settings["coding"],"target":{"provider":"openai","model":"fixture","endpoint":"http://127.0.0.1:1/v1/chat/completions","protocol":"chat_completions"}});
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    fs::write(source.join("Cargo.toml"), "[package]\nname='different'\n").unwrap();
    let newer = post(
        &client,
        &format!("{}/v1/harness/project-detection", server.url),
        &json!({"workspace":workspace}),
        StatusCode::OK,
    )
    .await;
    assert_ne!(newer["manifest_sha256"], detection["manifest_sha256"]);
    assert_eq!(
        get(&client, &format!("{catalog}/{id}")).await["settings"],
        settings
    );
    server.close().await;
    drop(engine);
    let (_, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let saved = get(
        &client,
        &format!(
            "{}/v1/harness/sessions/{}",
            server.url,
            receipt["session_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(saved["coding"], settings["coding"]);
    assert_eq!(saved["workspace"], workspace);
    assert_eq!(
        get(
            &client,
            &format!("{}/v1/harness/project-profiles/{id}", server.url)
        )
        .await["settings"],
        settings
    );
    server.close().await;
}

#[tokio::test]
async fn detection_http_rejects_invalid_paths_and_unknown_request_fields() {
    let root = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(root.path().join("detect-errors.db"))
        .await
        .unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let url = format!("{}/v1/harness/project-detection", server.url);
    post(
        &client,
        &url,
        &json!({"workspace":"relative"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    post(
        &client,
        &url,
        &json!({"workspace":root.path().join("missing")}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    post(
        &client,
        &url,
        &json!({"workspace":root.path(),"execute":true}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    fs::write(root.path().join("Cargo.toml"), "[package\n").unwrap();
    post(
        &client,
        &url,
        &json!({"workspace":root.path()}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    server.close().await;
}
