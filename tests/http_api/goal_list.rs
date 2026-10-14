use super::*;

#[tokio::test]
async fn same_planner_follow_up_is_visible_in_the_goal_but_not_a_second_goal_row() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("api.db")).await.unwrap();
    let mut configuration = spec(root.path());
    configuration.planner.config["follow_up"] =
        json!({"max_follow_ups":1,"min_delay_ms":50,"max_delay_ms":1000});
    configuration.planner.config["script"] = json!([
        {"kind":"ask_human","question":"Which review?"},
        {"kind":"schedule_follow_up","summary":"Review selected","goal":"Continue","after_ms":50}
    ]);
    let team = agents.teams().unwrap().create(configuration).await.unwrap();
    let planner = team.planner().unwrap().agent_id;
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let task = MessageId::new();
    submit(&client, &server.url, &team.id.to_string(), task, "Review").await;
    settle(&engine).await;
    let base = format!("{}/v1/teams/{}/tasks/{task}", server.url, team.id);
    let inbox = get(&client, &format!("{base}/inbox")).await;
    let question = wait(&inbox, "question");
    post(
        &client,
        &format!(
            "{base}/waits/{}/responses",
            question["wait"]["id"].as_str().unwrap()
        ),
        &json!({"kind":"answer","answer":"Durability"}),
        StatusCode::OK,
    )
    .await;
    agents.actors().unwrap().pause(planner).await.unwrap();
    settle(&engine).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    agents.actors().unwrap().resume(planner).await.unwrap();
    settle(&engine).await;
    let page = get(
        &client,
        &format!("{}/v1/teams/{}/tasks", server.url, team.id),
    )
    .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["task_id"], task.to_string());
    assert_eq!(page["items"][0]["root_status"], "succeeded");
    let detail = get(&client, &base).await;
    assert_eq!(detail["task"]["summary"]["settled"], false);
    assert!(
        detail["task"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["actor_id"] == planner.to_string()
                && node["task_id"] != task.to_string())
    );
    server.close().await;
}

#[tokio::test]
async fn empty_lists_and_preview_byte_boundaries_preserve_unicode_and_nuls() {
    let root = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(root.path().join("api.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let team = post(
        &client,
        &format!("{}/v1/teams", server.url),
        &spec(root.path()),
        StatusCode::OK,
    )
    .await;
    let id = team["id"].as_str().unwrap();
    let endpoint = format!("{}/v1/teams/{id}/tasks", server.url);
    let empty = get(&client, &endpoint).await;
    assert!(empty["items"].as_array().unwrap().is_empty());
    assert_eq!(empty["has_more"], false);
    assert!(empty["next_cursor"].is_null());
    let inputs = [
        ("a".repeat(1023) + "🦀", "a".repeat(1023), true),
        ("🦀".repeat(256), "🦀".repeat(256), false),
        ("ab\0cd".into(), "ab\0cd".into(), false),
    ];
    for (goal, expected, truncated) in inputs {
        let task = MessageId::new();
        submit(&client, &server.url, id, task, &goal).await;
        let page = get(&client, &format!("{endpoint}?limit=1")).await;
        assert_eq!(page["items"][0]["task_id"], task.to_string());
        assert_eq!(page["items"][0]["goal_preview"], expected);
        assert_eq!(page["items"][0]["goal_truncated"], truncated);
    }
    server.close().await;
}

#[tokio::test]
async fn goals_are_scoped_bounded_ordered_and_survive_restart_with_frozen_membership() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, server, client, team, first) = setup(root.path(), 2).await;
    let team_id = team["id"].as_str().unwrap();
    let endpoint = format!("{}/v1/teams/{team_id}/tasks", server.url);
    let second = MessageId::new();
    let third = MessageId::new();
    let long_goal = "🦀".repeat(500);
    submit(&client, &server.url, team_id, second, &long_goal).await;
    submit(&client, &server.url, team_id, third, "Third").await;
    // Exact submission retry has no new entry or ordering effect.
    submit(&client, &server.url, team_id, first, "Reviewed edit").await;
    let page = get(&client, &format!("{endpoint}?limit=1")).await;
    assert_eq!(page["items"][0]["task_id"], third.to_string());
    assert_eq!(page["items"][0]["root_status"], "queued");
    assert_eq!(page["has_more"], true);
    let cursor = page["next_cursor"].as_str().unwrap().to_string();
    let fourth = MessageId::new();
    submit(&client, &server.url, team_id, fourth, "New after snapshot").await;
    let other = {
        let mut s = spec(root.path());
        s.name = "other-team".into();
        post(
            &client,
            &format!("{}/v1/teams", server.url),
            &s,
            StatusCode::OK,
        )
        .await
    };
    submit(
        &client,
        &server.url,
        other["id"].as_str().unwrap(),
        MessageId::new(),
        "Other team",
    )
    .await;
    // Worker-created child tasks must not become goal rows.
    settle(&engine).await;
    let control = format!("{}/pause", base(&server.url, &team, second));
    post(
        &client,
        &control,
        &json!({"expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    server.close().await;
    drop(agents);
    drop(engine);
    let (_, agents) = local_app(root.path().join("api.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let endpoint = format!("{}/v1/teams/{team_id}/tasks", server.url);
    let next = get(&client, &format!("{endpoint}?limit=1&after={cursor}")).await;
    assert_eq!(next["snapshot_position"], page["snapshot_position"]);
    assert_eq!(next["items"][0]["task_id"], second.to_string());
    assert_eq!(
        next["items"][0]["goal_preview"].as_str().unwrap().len(),
        1024
    );
    assert_eq!(next["items"][0]["goal_truncated"], true);
    assert_eq!(next["items"][0]["goal_paused"], true);
    let final_page = get(
        &client,
        &format!(
            "{endpoint}?limit=1&after={}",
            next["next_cursor"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(final_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(final_page["items"][0]["task_id"], first.to_string());
    assert_eq!(final_page["has_more"], false);
    assert!(final_page["next_cursor"].is_null());
    let fresh = get(&client, &endpoint).await;
    assert_eq!(fresh["items"].as_array().unwrap().len(), 4);
    assert_eq!(fresh["items"][0]["task_id"], fourth.to_string());
    let detail = get(&client, &base(&server.url, &team, first)).await;
    assert!(detail["task"]["nodes"].as_array().unwrap().len() > 1);
    let cancelled = post(
        &client,
        &format!("{}/cancel", base(&server.url, &team, fourth)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled["settled"], true);
    assert_eq!(
        get(&client, &endpoint).await["items"][0]["goal_cancelled"],
        true
    );
    server.close().await;
}

#[tokio::test]
async fn goal_list_rejects_bad_queries_scope_and_future_cursors() {
    let root = tempfile::tempdir().unwrap();
    let (_, _, server, client, team, _) = setup(root.path(), 2).await;
    let id = team["id"].as_str().unwrap();
    submit(&client, &server.url, id, MessageId::new(), "Second").await;
    let endpoint = format!("{}/v1/teams/{id}/tasks", server.url);
    for query in [
        "limit=0",
        "limit=101",
        "limit=-1",
        "limit=1&limit=2",
        "after=bad",
        "unknown=1",
    ] {
        value(
            client
                .get(format!("{endpoint}?{query}"))
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let page = get(&client, &format!("{endpoint}?limit=1")).await;
    let fields: Vec<_> = page["next_cursor"].as_str().unwrap().split(':').collect();
    for cursor in [
        format!(
            "g1:{}:{}:{}:{}",
            act0r::domain::EventStreamId::new(),
            id,
            fields[3],
            fields[4]
        ),
        format!(
            "g1:{}:{}:{}:{}",
            fields[1],
            TeamId::new(),
            fields[3],
            fields[4]
        ),
        format!("g1:{}:{id}:9999999:1", fields[1]),
        format!("g1:{}:{id}:1:2", fields[1]),
        format!("g1:{}:{id}:9223372036854775808:1", fields[1]),
    ] {
        value(
            client
                .get(format!("{endpoint}?after={cursor}"))
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    value(
        client
            .get(format!("{}/v1/teams/{}/tasks", server.url, TeamId::new()))
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    server.close().await;
}
