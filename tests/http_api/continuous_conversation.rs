use super::project_setup::{proposal, provider, start_body};
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn continuation_link_failure_rolls_back_turn_and_reuses_prepared_coding_on_retry() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "Use the custom verifier.\n").unwrap();
    let (endpoint, count, handle) = provider(vec![
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        json!({"kind":"propose_project_setup","proposal":proposal()}),
    ])
    .await;
    let db = root.path().join("rollback.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let original = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &start_body(&endpoint, &source, true),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let session = original["session_id"].as_str().unwrap();
    let setup = format!(
        "/v1/harness/sessions/{session}/turns/{}/project-setup",
        original["task_id"].as_str().unwrap()
    );
    let view = get(&client, &format!("{}{setup}", server.url)).await;
    let coding = serde_json::from_value::<act0r::domain::project_setup::ProjectSetupProposal>(
        view["proposal"].clone(),
    )
    .unwrap()
    .coding()
    .unwrap();
    let body = json!({"coding":coding});
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    sqlx::raw_sql("CREATE TRIGGER forced_continuation_failure BEFORE INSERT ON harness_project_continuations BEGIN SELECT RAISE(ABORT,'forced continuation failure'); END;").execute(&pool).await.unwrap();
    let failed = client
        .post(format!("{}{setup}", server.url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM harness_sessions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM harness_turns")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let partial: (String,String)=sqlx::query_as("SELECT id,payload FROM agent_teams WHERE json_type(payload,'$.spec.planner.config.discovery_context')='object'").fetch_one(&pool).await.unwrap();
    let list = get(&client, &format!("{}/v1/harness/conversations", server.url)).await;
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        get(
            &client,
            &format!("{}/v1/harness/sessions/{session}/conversation", server.url)
        )
        .await["stage"],
        "setup_review"
    );
    post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/metadata", server.url),
        &json!({"kind":"archive","expected_revision":0}),
        StatusCode::CONFLICT,
    )
    .await;
    sqlx::raw_sql("DROP TRIGGER forced_continuation_failure")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    server.close().await;
    drop(engine);
    let (_, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let accepted = post(
        &client,
        &format!("{}{setup}", server.url),
        &body,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(accepted["team_id"], partial.0);
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT payload FROM agent_teams WHERE id=?")
            .bind(&partial.0)
            .fetch_one(&pool)
            .await
            .unwrap(),
        partial.1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM harness_project_continuations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        post(
            &client,
            &format!("{}{setup}", server.url),
            &body,
            StatusCode::ACCEPTED
        )
        .await,
        accepted
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    pool.close().await;
    server.close().await;
    handle.abort();
}

#[tokio::test]
async fn conversation_pages_keep_scope_and_reopening_preserves_accepted_links() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "Use the custom verifier.\n").unwrap();
    let (endpoint, count, handle) = provider(vec![
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        json!({"kind":"propose_project_setup","proposal":proposal()}),
    ])
    .await;
    let db = root.path().join("pages.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let original = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &start_body(&endpoint, &source, true),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let session = original["session_id"].as_str().unwrap();
    let setup = format!(
        "/v1/harness/sessions/{session}/turns/{}/project-setup",
        original["task_id"].as_str().unwrap()
    );
    let view = get(&client, &format!("{}{setup}", server.url)).await;
    let coding = serde_json::from_value::<act0r::domain::project_setup::ProjectSetupProposal>(
        view["proposal"].clone(),
    )
    .unwrap()
    .coding()
    .unwrap();
    let accepted = post(
        &client,
        &format!("{}{setup}", server.url),
        &json!({"coding":coding}),
        StatusCode::ACCEPTED,
    )
    .await;
    for index in 0..40 {
        post(
            &client,
            &format!("{}/v1/harness/sessions/{session}/turns", server.url),
            &json!({"message_id":MessageId::new(),"prompt":format!("Follow-up {index}")}),
            StatusCode::ACCEPTED,
        )
        .await;
    }
    let path = format!("/v1/harness/sessions/{session}/conversation");
    let page = get(&client, &format!("{}{path}", server.url)).await;
    assert_eq!(page["turns"].as_array().unwrap().len(), 32);
    assert_eq!(page["active"], accepted);
    let cursor = page["next_before"].as_str().unwrap();
    let older = get(&client, &format!("{}{path}?before={cursor}", server.url)).await;
    assert_eq!(older["turns"].as_array().unwrap().len(), 10);
    assert_eq!(older["turns"][0]["task_id"], original["task_id"]);
    let seen = page["turns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|turn| turn["task_id"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert!(
        older["turns"]
            .as_array()
            .unwrap()
            .iter()
            .all(|turn| !seen.contains(turn["task_id"].as_str().unwrap()))
    );
    assert_eq!(older["next_before"], Value::Null);
    let mut manual = start_body(&endpoint, &source, true);
    manual["message_id"] = json!(MessageId::new());
    manual["discover_project"] = json!(false);
    let separate = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &manual,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(
        client
            .get(format!(
                "{}/v1/harness/sessions/{}/conversation?before={cursor}",
                server.url,
                separate["session_id"].as_str().unwrap()
            ))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(format!(
                "{}{path}?task={}",
                server.url,
                separate["task_id"].as_str().unwrap()
            ))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let list = get(
        &client,
        &format!("{}/v1/harness/conversations?limit=1", server.url),
    )
    .await;
    assert_eq!(list["items"][0]["session_id"], separate["session_id"]);
    let cursor = list["next_after"].as_str().unwrap();
    let list2 = get(
        &client,
        &format!(
            "{}/v1/harness/conversations?limit=1&after={cursor}",
            server.url
        ),
    )
    .await;
    assert_eq!(list2["items"][0]["session_id"], original["session_id"]);
    assert_eq!(list2["next_after"], Value::Null);
    assert_eq!(
        client
            .get(format!("{}/v1/harness/sessions?after={cursor}", server.url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    // Reopening preserves accepted links and actor state without more model calls.
    server.close().await;
    drop(engine);
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    let before: Vec<(String, String)> =
        sqlx::query_as("SELECT id,payload FROM agent_actors ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    pool.close().await;
    let (_, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let restored = get(&client, &format!("{}{path}", server.url)).await;
    assert_eq!(restored["active"], accepted);
    assert_eq!(restored["turns"], page["turns"]);
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    assert_eq!(
        sqlx::query_as::<_, (String, String)>("SELECT id,payload FROM agent_actors ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await
            .unwrap(),
        62
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    pool.close().await;
    server.close().await;
    handle.abort();
}

#[tokio::test]
async fn auto_conversation_keeps_answers_stages_followups_and_verification_over_restart() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("custom");
    fs::create_dir(&source).unwrap();
    fs::write(
        source.join("README.md"),
        "Repair logic.txt with the custom verifier.\n",
    )
    .unwrap();
    fs::write(source.join("logic.txt"), "broken\n").unwrap();
    let mut proposed = proposal();
    proposed["test_command"]["argv"] = json!([
        std::env::current_exe().unwrap(),
        "--ignored",
        "--exact",
        "project_setup::custom_verifier_child"
    ]);
    let (endpoint,count,handle)=provider(vec![
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        json!({"kind":"ask_human","question":"Which compatibility constraint applies?"}),
        json!({"kind":"propose_project_setup","proposal":proposed}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"logic.txt","old":"broken\n","new":"fixed\n"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Repaired and verified the custom project"}),
        json!({"kind":"ask_human","question":"Follow-up received by the coding planner"}),
        json!({"kind":"finish_incomplete","summary":"Follow-up context verified"}),
    ]).await;
    let db = root.path().join("conversation.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let request = start_body(&endpoint, &source, true);
    let original = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    let session = original["session_id"].as_str().unwrap();
    let path = format!("/v1/harness/sessions/{session}/conversation");
    let view = get(&client, &format!("{}{path}", server.url)).await;
    assert_eq!(view["stage"], "inspecting");
    assert_eq!(view["can_continue"], false);
    let followup = json!({"message_id":MessageId::new(),"prompt":"Explain the verified change"});
    post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/turns", server.url),
        &followup,
        StatusCode::CONFLICT,
    )
    .await;
    settle(&engine).await;
    let goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        original["team_id"].as_str().unwrap(),
        original["task_id"].as_str().unwrap()
    );
    let question = wait(&get(&client, &format!("{goal}/inbox")).await, "question");
    answer(
        &client,
        &goal,
        &question,
        json!({"kind":"answer","answer":"Keep API v2 unchanged"}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let view = get(&client, &format!("{}{path}", server.url)).await;
    assert_eq!(view["stage"], "setup_review");
    assert_eq!(
        view["turns"][0]["clarifications"][0]["answer"]["text"],
        "Keep API v2 unchanged"
    );
    post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/turns", server.url),
        &followup,
        StatusCode::CONFLICT,
    )
    .await;
    let setup_path = format!(
        "/v1/harness/sessions/{session}/turns/{}/project-setup",
        original["task_id"].as_str().unwrap()
    );
    let view = get(&client, &format!("{}{setup_path}", server.url)).await;
    let coding = serde_json::from_value::<act0r::domain::project_setup::ProjectSetupProposal>(
        view["proposal"].clone(),
    )
    .unwrap()
    .coding()
    .unwrap();
    let accepted = post(
        &client,
        &format!("{}{setup_path}", server.url),
        &json!({"coding":coding}),
        StatusCode::ACCEPTED,
    )
    .await;
    let view = get(&client, &format!("{}{path}", server.url)).await;
    assert_eq!(view["session_id"], original["session_id"]);
    assert_eq!(view["active"], accepted);
    assert_eq!(view["stage"], "coding");
    assert_eq!(view["turns"][1]["repeated_request"], true);
    let list = get(&client, &format!("{}/v1/harness/conversations", server.url)).await;
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    assert_eq!(list["items"][0]["session_id"], original["session_id"]);
    assert_eq!(list["items"][0]["team_id"], accepted["team_id"]);
    settle(&engine).await;
    let coding_goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        accepted["team_id"].as_str().unwrap(),
        accepted["task_id"].as_str().unwrap()
    );
    let edit = wait(
        &get(&client, &format!("{coding_goal}/inbox")).await,
        "tool_approval",
    );
    answer(
        &client,
        &coding_goal,
        &edit,
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        get(&client, &format!("{}{path}", server.url)).await["stage"],
        "verifying"
    );
    let verification = wait(
        &get(&client, &format!("{coding_goal}/inbox")).await,
        "tool_approval",
    );
    // Queue from the original identity while verification waits. Current work
    // must remain the earlier test approval, with its exact controls and scope.
    let queued = post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/turns", server.url),
        &followup,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(queued["session_id"], accepted["session_id"]);
    assert_eq!(queued["sequence"], 2);
    let view = get(&client, &format!("{}{path}", server.url)).await;
    assert_eq!(view["active"], accepted);
    assert_eq!(view["stage"], "verifying");
    assert_eq!(count.load(Ordering::SeqCst), 5);
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let view = get(&client, &format!("{}{path}", server.url)).await;
    assert_eq!(view["active"], accepted);
    assert_eq!(
        view["turns"][0]["clarifications"][0]["wait_id"],
        question["wait"]["id"]
    );
    let from_child = get(
        &client,
        &format!(
            "{}/v1/harness/sessions/{}/conversation",
            server.url,
            accepted["session_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(from_child["session_id"], original["session_id"]);
    assert_eq!(
        post(
            &client,
            &format!("{}/v1/harness/sessions/{session}/turns", server.url),
            &followup,
            StatusCode::ACCEPTED
        )
        .await,
        queued
    );
    // Root metadata protects all the linked work.
    post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/metadata", server.url),
        &json!({"kind":"archive","expected_revision":0}),
        StatusCode::CONFLICT,
    )
    .await;
    let coding_goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        accepted["team_id"].as_str().unwrap(),
        accepted["task_id"].as_str().unwrap()
    );
    answer(
        &client,
        &coding_goal,
        &verification,
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let view = get(&client, &format!("{}{path}", server.url)).await;
    assert_eq!(view["active"], queued);
    let changes = get(
        &client,
        &format!(
            "{}/v1/harness/sessions/{}/turns/{}/changes",
            server.url,
            accepted["session_id"].as_str().unwrap(),
            accepted["task_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(changes["completion"]["status"], "verified");
    let next_goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        queued["team_id"].as_str().unwrap(),
        queued["task_id"].as_str().unwrap()
    );
    let question = wait(
        &get(&client, &format!("{next_goal}/inbox")).await,
        "question",
    );
    answer(
        &client,
        &next_goal,
        &question,
        json!({"kind":"answer","answer":"Done"}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let view = get(&client, &format!("{}{path}", server.url)).await;
    assert_eq!(
        view["stage"],
        "incomplete",
        "{}; provider calls {}",
        view,
        count.load(Ordering::SeqCst)
    );
    assert_eq!(view["turns"].as_array().unwrap().len(), 3);
    assert_eq!(count.load(Ordering::SeqCst), 8);
    assert_eq!(
        fs::read_to_string(source.join("logic.txt")).unwrap(),
        "broken\n"
    );
    // Archiving the visible conversation refuses new child turns too, while an
    // exact already-admitted message still recovers its original receipt.
    post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/metadata", server.url),
        &json!({"kind":"archive","expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        get(&client, &format!("{}{path}", server.url)).await["can_continue"],
        false
    );
    post(
        &client,
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            accepted["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"New request"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(
        post(
            &client,
            &format!("{}/v1/harness/sessions/{session}/turns", server.url),
            &followup,
            StatusCode::ACCEPTED
        )
        .await,
        queued
    );
    server.close().await;
    handle.abort();
}
