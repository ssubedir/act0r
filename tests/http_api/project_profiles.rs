use super::*;
use act0r::domain::ProjectProfileId;

fn decision(settings: Value, revision: u64) -> Value {
    json!({"request_id":MessageId::new(),"expected_revision":revision,"action":{"kind":"save","settings":settings}})
}

#[tokio::test]
async fn project_profiles_retain_session_pins_through_edit_removal_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "Source\n").unwrap();
    let source = fs::canonicalize(source)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let db = root.path().join("profiles-http.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let catalog = format!("{}/v1/harness/project-profiles", server.url);
    let id = ProjectProfileId::new();
    let profile = format!("{catalog}/{id}");
    let coding = json!({"writable_paths":["README.md"],"test_command":{"argv":["cargo","test"],"timeout_ms":60000},"project_commands":{"format":{"argv":["cargo","fmt"],"timeout_ms":60000}}});
    let create = decision(json!({"name":"Rust","workspace":source,"coding":coding}), 0);
    let saved = post(&client, &profile, &create, StatusCode::OK).await;
    assert_eq!(saved["profile"]["revision"], 1);
    assert_eq!(
        get(&client, &catalog).await["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let selected = get(&client, &profile).await;
    let settings = &selected["settings"];
    let input = json!({"message_id":MessageId::new(),"prompt":"Inspect the project","target":{"provider":"openai","model":"fixture","endpoint":"http://127.0.0.1:1/v1/chat/completions","protocol":"chat_completions"},"workspace":settings["workspace"],"coding":settings["coding"]});
    let start = format!("{}/v1/harness/sessions", server.url);
    let session = post(&client, &start, &input, StatusCode::ACCEPTED).await;
    let changed = decision(json!({"name":"Read only","workspace":source}), 1);
    post(&client, &profile, &changed, StatusCode::OK).await;
    let pinned = get(
        &client,
        &format!("{start}/{}", session["session_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(pinned["coding"], coding);
    assert_eq!(pinned["workspace"], source);
    let team = agents
        .teams()
        .unwrap()
        .get(serde_json::from_value(session["team_id"].clone()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        team.members[0].pinned_config["input"]["project_commands"],
        coding["project_commands"]
    );
    assert!(
        team.members[0].pinned_config["input"]["tool_definitions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["requires_approval"] == true)
            .count()
            >= 4
    );
    let permission = get(
        &client,
        &format!(
            "{start}/{}/turns/{}/permissions",
            session["session_id"].as_str().unwrap(),
            session["task_id"].as_str().unwrap()
        ),
    )
    .await;
    assert!(permission["state"]["test_command"].is_null());
    let replay = post(&client, &profile, &create, StatusCode::OK).await;
    assert!(replay["replayed"].as_bool().unwrap());
    assert_eq!(replay["profile"]["revision"], 1);
    assert_eq!(get(&client, &profile).await["revision"], 2);
    let remove =
        json!({"request_id":MessageId::new(),"expected_revision":2,"action":{"kind":"remove"}});
    post(&client, &profile, &remove, StatusCode::OK).await;
    assert!(
        get(&client, &catalog).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    value(
        client.get(&profile).send().await.unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    server.close().await;
    drop(agents);
    drop(engine);
    let (_, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let profile = format!("{}/v1/harness/project-profiles/{id}", server.url);
    assert_eq!(
        post(&client, &profile, &remove, StatusCode::OK).await["replayed"],
        true
    );
    let sessions = format!("{}/v1/harness/sessions", server.url);
    assert_eq!(
        post(&client, &sessions, &input, StatusCode::ACCEPTED).await,
        session
    );
    let pinned = get(
        &client,
        &format!("{sessions}/{}", session["session_id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(pinned["coding"], coding);
    assert_eq!(pinned["workspace"], source);
    post(
        &client,
        &format!(
            "{sessions}/{}/turns",
            session["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"Continue with the saved commands"}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(
        fs::read_to_string(std::path::Path::new(&source).join("README.md")).unwrap(),
        "Source\n"
    );
    server.close().await;
}

#[tokio::test]
async fn project_profile_http_refuses_invalid_configuration_and_stale_decisions() {
    let root = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(root.path().join("invalid.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let catalog = format!("{}/v1/harness/project-profiles", server.url);
    let profile = format!("{catalog}/{}", ProjectProfileId::new());
    let workspace = root.path().to_string_lossy().into_owned();
    for invalid in [
        json!({"name":"","workspace":workspace}),
        json!({"name":"Bad","workspace":"relative"}),
        json!({"name":"Bad","workspace":workspace,"coding":{"writable_paths":["../secret"],"test_command":{"argv":["cargo"],"timeout_ms":60000}}}),
    ] {
        post(
            &client,
            &profile,
            &decision(invalid, 0),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    post(
        &client,
        &profile,
        &decision(json!({"name":"Bad","workspace":workspace,"target":{}}), 0),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    let create = decision(json!({"name":"Project","workspace":workspace}), 0);
    post(&client, &profile, &create, StatusCode::OK).await;
    post(
        &client,
        &profile,
        &decision(json!({"name":"Edit","workspace":workspace}), 0),
        StatusCode::CONFLICT,
    )
    .await;
    post(
        &client,
        &format!("{catalog}/{}", ProjectProfileId::new()),
        &decision(json!({"name":"PROJECT","workspace":workspace}), 0),
        StatusCode::CONFLICT,
    )
    .await;
    let mut changed = create.clone();
    changed["action"]["settings"]["name"] = json!("Reused identity");
    post(&client, &profile, &changed, StatusCode::CONFLICT).await;
    assert_eq!(get(&client, &profile).await["settings"]["name"], "Project");
    server.close().await;
}
