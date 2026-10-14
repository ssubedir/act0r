use super::*;
use act0r::domain::SessionId;

#[tokio::test]
async fn session_metadata_http_requires_fresh_decisions_and_preserves_archived_reads() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("session-http.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let list = format!("{}/v1/harness/sessions", server.url);
    let input = json!({"message_id":MessageId::new(),"prompt":"Saved work","target":{"provider":"openai","model":"fixture","endpoint":"http://127.0.0.1:1/v1/chat/completions","protocol":"chat_completions"}});
    let receipt = post(&client, &list, &input, StatusCode::ACCEPTED).await;
    let session = format!("{list}/{}", receipt["session_id"].as_str().unwrap());
    let metadata = format!("{session}/metadata");
    let initial = get(&client, &metadata).await;
    assert_eq!(initial["revision"], 0);
    assert!(initial["archive_blocked_reason"].is_string());
    for body in [
        json!({"expected_revision":0,"kind":"rename","title":"Valid","extra":true}),
        json!({"expected_revision":0,"kind":"archive","title":"Unexpected"}),
        json!({"expected_revision":0,"kind":"delete"}),
    ] {
        post(&client, &metadata, &body, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
    for title in [" ".to_string(), "Invalid\nTitle".into(), "🦀".repeat(65)] {
        post(
            &client,
            &metadata,
            &json!({"expected_revision":0,"kind":"rename","title":title}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    post(
        &client,
        &metadata,
        &json!({"expected_revision":0,"kind":"archive"}),
        StatusCode::CONFLICT,
    )
    .await;
    let rename = json!({"expected_revision":0,"kind":"rename","title":"My saved work 🦀"});
    let renamed = post(&client, &metadata, &rename, StatusCode::OK).await;
    assert_eq!(renamed["revision"], 1);
    assert_eq!(renamed["replayed"], false);
    assert_eq!(
        post(&client, &metadata, &rename, StatusCode::OK).await["replayed"],
        true
    );
    let task = serde_json::from_value(receipt["task_id"].clone()).unwrap();
    agents.actors().unwrap().cancel_goal(task).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..100 {
        if !worker.step().await.unwrap() {
            break;
        }
    }
    drop(worker);
    let archive = json!({"expected_revision":1,"kind":"archive"});
    post(&client, &metadata, &archive, StatusCode::OK).await;
    assert!(
        get(&client, &list).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let archived = get(&client, &format!("{list}?archived=true")).await;
    assert_eq!(archived["items"][0]["title"], "My saved work 🦀");
    assert_eq!(get(&client, &session).await["archived"], true);
    post(
        &client,
        &format!("{session}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Archived work"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(
        post(&client, &list, &input, StatusCode::ACCEPTED).await,
        receipt
    );
    server.close().await;
    drop(agents);
    drop(engine);
    let (_, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let metadata = format!(
        "{}/v1/harness/sessions/{}/metadata",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    assert_eq!(
        post(&client, &metadata, &archive, StatusCode::OK).await["replayed"],
        true
    );
    post(
        &client,
        &metadata,
        &json!({"expected_revision":2,"kind":"restore"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        post(&client, &metadata, &archive, StatusCode::CONFLICT).await["error"]["code"],
        "session_conflict"
    );
    value(
        client
            .get(format!(
                "{}/v1/harness/sessions/{}/metadata",
                server.url,
                SessionId::new()
            ))
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    for query in [
        "unknown=true",
        "limit=0",
        "limit=51",
        "limit=-1",
        "archived=maybe",
        "after=broken",
    ] {
        value(
            client
                .get(format!("{}/v1/harness/sessions?{query}", server.url))
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    server.close().await;
}
