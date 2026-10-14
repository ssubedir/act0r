use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn unlimited_and_independent_limits_keep_usage_and_recover_across_restart() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let model = tokio::spawn(axum::serve(listener, axum::Router::new().route(
        "/v1/chat/completions",
        axum::routing::post(move || {
            let n = counted.fetch_add(1, Ordering::SeqCst);
            async move {
                let action = if n == 39 {
                    json!({"kind":"finish","summary":"Forty saved model calls"})
                } else {
                    json!({"kind":"ask_human","question":"Continue?"})
                };
                // Missing provider usage must retain reservations even without a cap.
                axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":action}).to_string()}}]}))
            }
        })
    )).into_future());
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("budgets.db");
    let (mut engine, agents) = local_app(&database).await.unwrap();
    let mut server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let config = get(&client, &format!("{}/v1/harness/config", server.url)).await;
    assert_eq!(
        config["budget_defaults"],
        json!({"max_model_calls":null,"max_tokens":null})
    );
    let mut input = json!({"message_id":MessageId::new(),"prompt":"x".repeat(30000),"target":{"provider":"openai","model":"fixture","endpoint":endpoint,"protocol":"chat_completions"}});
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await;
    for n in 0..40 {
        settle(&engine).await;
        let base = format!(
            "{}/v1/teams/{}/tasks/{}",
            server.url,
            receipt["team_id"].as_str().unwrap(),
            receipt["task_id"].as_str().unwrap()
        );
        if n == 39 {
            break;
        }
        if n == 19 {
            server.close().await;
            drop(engine);
            let reopened = local_app(&database).await.unwrap();
            engine = reopened.0;
            server = Server::start(Arc::new(reopened.1), 2).await;
            assert_eq!(
                post(
                    &client,
                    &format!("{}/v1/harness/sessions", server.url),
                    &input,
                    StatusCode::ACCEPTED
                )
                .await,
                receipt
            );
        }
        let base = if n == 19 {
            format!(
                "{}/v1/teams/{}/tasks/{}",
                server.url,
                receipt["team_id"].as_str().unwrap(),
                receipt["task_id"].as_str().unwrap()
            )
        } else {
            base
        };
        let question = wait(&get(&client, &format!("{base}/inbox")).await, "question");
        answer(
            &client,
            &base,
            &question,
            json!({"kind":"answer","answer":"Continue"}),
            StatusCode::OK,
        )
        .await;
    }
    let base = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    let view = get(&client, &base).await;
    assert_eq!(view["task"]["summary"]["settled"], true);
    assert_eq!(view["task"]["summary"]["succeeded"], 1);
    let budget = &view["task"]["budgets"][0];
    assert_eq!(budget["record"]["usage"]["admitted_model_calls"], 40);
    assert_eq!(budget["record"]["usage"]["uncertain_calls"], 40);
    assert!(budget["record"]["usage"]["held_tokens"].as_u64().unwrap() > 800000);
    assert!(budget["remaining_model_calls"].is_null());
    assert!(budget["remaining_tokens"].is_null());
    assert!(budget["record"]["exhaustion"].is_null());

    for (limits, expected_calls) in [
        (json!({"max_model_calls":2,"max_tokens":null}), 2),
        (json!({"max_model_calls":null,"max_tokens":1}), 0),
    ] {
        let before = calls.load(Ordering::SeqCst);
        input["message_id"] = json!(MessageId::new());
        input["budget"] = limits;
        let saved = post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &input,
            StatusCode::ACCEPTED,
        )
        .await;
        let base = format!(
            "{}/v1/teams/{}/tasks/{}",
            server.url,
            saved["team_id"].as_str().unwrap(),
            saved["task_id"].as_str().unwrap()
        );
        for _ in 0..=expected_calls {
            settle(&engine).await;
            let view = get(&client, &base).await;
            if view["task"]["summary"]["settled"] == true {
                break;
            }
            let question = wait(&get(&client, &format!("{base}/inbox")).await, "question");
            answer(
                &client,
                &base,
                &question,
                json!({"kind":"answer","answer":"Continue"}),
                StatusCode::OK,
            )
            .await;
        }
        let view = get(&client, &base).await;
        assert_eq!(view["task"]["nodes"][0]["status"], "budget_exhausted");
        assert_eq!(calls.load(Ordering::SeqCst) - before, expected_calls);
        assert_eq!(
            post(
                &client,
                &format!("{}/v1/harness/sessions", server.url),
                &input,
                StatusCode::ACCEPTED
            )
            .await,
            saved
        );
        let mut changed = input.clone();
        changed["budget"] = json!({"max_model_calls":null,"max_tokens":null});
        post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &changed,
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    for (limits, status) in [
        (json!({"max_model_calls":0}), StatusCode::BAD_REQUEST),
        (json!({"max_tokens":0}), StatusCode::BAD_REQUEST),
        (json!({"max_tokens":-1}), StatusCode::UNPROCESSABLE_ENTITY),
        (
            json!({"max_model_calls":1.5}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            json!({"max_tokens":"unlimited"}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (json!({"unexpected":true}), StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        input["message_id"] = json!(MessageId::new());
        input["budget"] = limits;
        post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &input,
            status,
        )
        .await;
    }
    server.close().await;
    model.abort();
}
