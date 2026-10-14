use super::*;
use act0r::domain::{SessionId, agent::*, harness::HarnessRequest};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn conversation_routes_reject_invalid_scope_queries_and_changed_turns() {
    let root = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(root.path().join("conversation-routes.db"))
        .await
        .unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let input = json!({"message_id":MessageId::new(),"prompt":"Initial request","target":{"provider":"openai","model":"fixture","endpoint":"http://127.0.0.1:1234/v1/chat/completions","protocol":"chat_completions"}});
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await;
    let session = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    let work = get(
        &client,
        &format!(
            "{session}/turns/{}/delegated-work",
            receipt["task_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(work["groups"], json!([]));
    assert_eq!(work["truncated"], false);
    let turn = json!({"message_id":MessageId::new(),"prompt":"Continue"});
    let saved = post(
        &client,
        &format!("{session}/turns"),
        &turn,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(
        saved,
        post(
            &client,
            &format!("{session}/turns"),
            &turn,
            StatusCode::ACCEPTED
        )
        .await
    );
    let mut changed = turn.clone();
    changed["prompt"] = json!("Different");
    post(
        &client,
        &format!("{session}/turns"),
        &changed,
        StatusCode::CONFLICT,
    )
    .await;
    let mut extra = turn;
    extra["target"] = json!({"model":"replacement"});
    post(
        &client,
        &format!("{session}/turns"),
        &extra,
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    for suffix in [
        "?before=0",
        "?before=18446744073709551615",
        "?unknown=1",
        "?before=1&before=2",
    ] {
        assert_eq!(
            client
                .get(format!("{session}{suffix}"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        client
            .get(format!(
                "{}/v1/harness/sessions/{}",
                server.url,
                SessionId::new()
            ))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        client
            .get(format!("{}/v1/harness/sessions/not-a-uuid", server.url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let page = get(&client, &session).await;
    assert_eq!(page["turns"].as_array().unwrap().len(), 2);
    assert_eq!(page["turns"][1]["sequence"], 2);
    server.close().await;
}

#[tokio::test]
async fn harness_http_runs_a_prompt_and_recovers_a_delegated_question_after_restart() {
    // Exercise the compatible HTTP model protocol, not the scripted model adapter.
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let model = tokio::spawn(axum::serve(listener, axum::Router::new().route(
        "/v1/chat/completions",
        axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let turn = counted.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(body["model"], "harness-fixture");
                assert!(body["response_format"]["json_schema"]["schema"].is_object());
                let action = match turn {
                    0 => json!({"kind":"delegate_task","worker":"helper","goal":"Explain recovery"}),
                    1 => json!({"kind":"ask_human","question":"Which failure should I explain?"}),
                    2 | 3 => json!({"kind":"finish","summary":"A saved mailbox and durable wait survive a restart."}),
                    4 => {
                        let context = body["messages"].to_string();
                        assert!(context.contains("A saved mailbox and durable wait survive a restart."));
                        assert!(context.contains("Explain that previous answer in one sentence"));
                        json!({"kind":"finish","summary":"Recovery resumes saved work instead of losing the conversation."})
                    },
                    5 => {
                        let context = body["messages"].to_string();
                        assert!(context.contains("Recovery resumes saved work instead of losing the conversation."));
                        json!({"kind":"finish","summary":"The same saved conversation is still available after another restart."})
                    },
                    _ => panic!("unexpected model call {turn}"),
                };
                // Some compatible servers wrap valid action JSON despite the requested format.
                let content = format!("```json\n{}\n```", json!({"action":action}));
                axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":content}}],"usage":{"prompt_tokens":80,"completion_tokens":20}}))
            }
        }),
    )).into_future());
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("harness.db");
    let (engine, agents) = local_app(&database).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let config = get(&client, &format!("{}/v1/harness/config", server.url)).await;
    assert!(config["api_key_available"].is_boolean());
    assert!(config.get("api_key").is_none());
    assert!(config.get("credential_availability").is_none());
    let input = HarnessRequest {
        auto_execute: false,
        discover_project: false,
        budget: None,
        coding: None,
        message_id: MessageId::new(),
        prompt: "Explain durable recovery".into(),
        workspace: None,
        target: ModelTarget {
            provider: "openai".into(),
            model: "harness-fixture".into(),
            endpoint,
            protocol: ModelProtocol::ChatCompletions,
            action_format: Default::default(),
            policy: Default::default(),
        },
    };
    let url = format!("{}/v1/harness/sessions", server.url);
    let receipt = post(&client, &url, &input, StatusCode::ACCEPTED).await;
    assert_eq!(
        receipt,
        post(&client, &url, &input, StatusCode::ACCEPTED).await
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0); // Admission does not run the request.
    assert_eq!(
        get(&client, &url).await["items"].as_array().unwrap().len(),
        1
    );
    let base = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        input.message_id
    );
    settle(&engine).await;
    let question = wait(&get(&client, &format!("{base}/inbox")).await, "question");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let session = receipt["session_id"].as_str().unwrap();
    let panel = format!(
        "{}/v1/harness/sessions/{session}/turns/{}/delegated-work",
        server.url, input.message_id
    );
    let delegation = get(&client, &panel).await;
    assert_eq!(delegation["groups"][0]["total"], 1);
    assert_eq!(delegation["groups"][0]["finished"], 0);
    assert_eq!(delegation["groups"][0]["joined"], false);
    assert!(delegation["groups"][0]["group_id"].is_null());
    assert_eq!(
        delegation["groups"][0]["children"][0]["goal"],
        "Explain recovery"
    );
    assert!(delegation["groups"][0]["children"][0]["changes"].is_null());
    let follow_up = json!({"message_id":MessageId::new(),"prompt":"Explain that previous answer in one sentence"});
    let follow_url = format!("{}/v1/harness/sessions/{session}/turns", server.url);
    let archive_refused = post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/metadata", server.url),
        &json!({"expected_revision":0,"kind":"archive"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(archive_refused["error"]["code"], "session_conflict");
    let second = post(&client, &follow_url, &follow_up, StatusCode::ACCEPTED).await;
    assert_eq!(second["sequence"], 2);
    assert_eq!(second["team_id"], receipt["team_id"]);
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2); // Queued follow-up cannot pass a durable question.
    let second_base = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        second["team_id"].as_str().unwrap(),
        second["task_id"].as_str().unwrap()
    );
    assert_eq!(
        get(&client, &second_base).await["task"]["nodes"][0]["status"],
        "queued"
    );
    server.close().await;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let follow_url = format!("{}/v1/harness/sessions/{session}/turns", server.url);
    assert_eq!(
        post(&client, &follow_url, &follow_up, StatusCode::ACCEPTED).await,
        second
    );
    let base = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        input.message_id
    );
    let restored = wait(&get(&client, &format!("{base}/inbox")).await, "question");
    assert_eq!(restored["wait"]["id"], question["wait"]["id"]);
    let response = json!({"kind":"answer","answer":"An app restart"});
    assert_eq!(
        answer(&client, &base, &restored, response.clone(), StatusCode::OK).await["replayed"],
        false
    );
    assert_eq!(
        answer(&client, &base, &restored, response, StatusCode::OK).await["replayed"],
        true
    );
    settle(&engine).await;
    let view = get(&client, &base).await;
    assert_eq!(view["task"]["summary"]["settled"], true);
    assert_eq!(view["task"]["summary"]["succeeded"], 2);
    let root = view["task"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["task_id"] == input.message_id.to_string())
        .unwrap();
    assert_eq!(
        root["result"]["answer"],
        "A saved mailbox and durable wait survive a restart."
    );
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    let panel = format!(
        "{}/v1/harness/sessions/{session}/turns/{}/delegated-work",
        server.url, input.message_id
    );
    let delegated = get(&client, &panel).await;
    assert_eq!(delegated["groups"][0]["finished"], 1);
    assert_eq!(delegated["groups"][0]["joined"], true);
    assert_eq!(
        delegated["groups"][0]["children"][0]["result"],
        "A saved mailbox and durable wait survive a restart."
    );
    assert!(delegated["groups"][0]["children"][0]["review"].is_null());
    let allowance = &view["task"]["budgets"][0];
    assert_eq!(allowance["member_count"], 2);
    assert_eq!(allowance["record"]["usage"]["admitted_model_calls"], 4);
    assert!(allowance["remaining_model_calls"].is_null());
    assert!(allowance["remaining_tokens"].is_null());
    let saved = get(
        &client,
        &format!("{}/v1/harness/sessions/{session}", server.url),
    )
    .await;
    assert_eq!(saved["turns"].as_array().unwrap().len(), 2);
    assert_eq!(
        saved["turns"][0]["response"],
        "A saved mailbox and durable wait survive a restart."
    );
    assert_eq!(saved["turns"][1]["settled"], true);
    assert_eq!(
        get(&client, &format!("{}/v1/teams", server.url)).await["teams"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        get(&client, &format!("{}/v1/harness/sessions", server.url)).await["items"][0]["root_status"],
        "succeeded"
    );
    let mut invalid = serde_json::to_value(input).unwrap();
    invalid["message_id"] = json!(MessageId::new());
    invalid["target"]["endpoint"] = json!("http://example.com/v1/chat/completions");
    post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &invalid,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        get(&client, &format!("{}/v1/harness/sessions", server.url)).await["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let third = post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/turns", server.url),
        &json!({"message_id":MessageId::new(),"prompt":"Continue after the restart"}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(third["sequence"], 3);
    assert_eq!(third["team_id"], receipt["team_id"]);
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    let saved = get(
        &client,
        &format!("{}/v1/harness/sessions/{session}", server.url),
    )
    .await;
    assert_eq!(saved["turns"][2]["settled"], true);
    server.close().await;
    model.abort();
}
