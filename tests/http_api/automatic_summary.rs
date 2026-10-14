use super::summary::{Model, decision, input, pool};
use super::*;
use std::sync::atomic::Ordering;

async fn enable(client: &Client, base: &str, revision: u64, enabled: bool) -> Value {
    post(
        client,
        &format!("{base}/summary/policy"),
        &json!({"request_id":MessageId::new(),"expected_revision":revision,"enabled":enabled}),
        StatusCode::OK,
    )
    .await
}
async fn turns(client: &Client, base: &str, count: u64) -> Vec<Value> {
    let mut receipts = Vec::new();
    for i in 0..count {
        receipts.push(post(client, &format!("{base}/turns"), &json!({"message_id":MessageId::new(),"prompt":format!("Turn {}: remember Rust",i+2)}), StatusCode::ACCEPTED).await);
    }
    receipts
}
async fn pending(engine: &Engine, client: &Client, base: &str) -> Value {
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..200 {
        let view = get(client, &format!("{base}/summary")).await;
        if view["pending"] == true {
            return view;
        }
        assert!(
            worker.step().await.unwrap(),
            "no automatic admission: {view}"
        );
    }
    panic!("automatic admission did not settle");
}

#[tokio::test]
async fn opt_in_policy_has_exact_receipts_and_default_off_preserves_queued_work() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("off.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        first["session_id"].as_str().unwrap()
    );
    assert_eq!(
        get(&client, &format!("{base}/summary")).await["automatic"]["enabled"],
        false
    );
    turns(&client, &base, 6).await;
    settle(&engine).await;
    assert_eq!(model.count(), 7);
    assert!(get(&client, &format!("{base}/summary")).await["summary"].is_null());
    let request = json!({"request_id":MessageId::new(),"expected_revision":0,"enabled":true});
    let original = post(
        &client,
        &format!("{base}/summary/policy"),
        &request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(original["revision"], 1);
    enable(&client, &base, 1, false).await;
    let replay = post(
        &client,
        &format!("{base}/summary/policy"),
        &request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay["revision"], 1);
    assert_eq!(replay["replayed"], true);
    assert_eq!(
        get(&client, &format!("{base}/summary")).await["automatic"]["enabled"],
        false
    );
    let mut changed = request.clone();
    changed["enabled"] = json!(false);
    post(
        &client,
        &format!("{base}/summary/policy"),
        &changed,
        StatusCode::CONFLICT,
    )
    .await;
    changed["request_id"] = json!(MessageId::new());
    post(
        &client,
        &format!("{base}/summary/policy"),
        &changed,
        StatusCode::CONFLICT,
    )
    .await;
    changed["request_id"] = json!(uuid::Uuid::nil());
    post(
        &client,
        &format!("{base}/summary/policy"),
        &changed,
        StatusCode::BAD_REQUEST,
    )
    .await;
    let other = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    post(
        &client,
        &format!(
            "{}/v1/harness/sessions/{}/summary/policy",
            server.url,
            other["session_id"].as_str().unwrap()
        ),
        &request,
        StatusCode::CONFLICT,
    )
    .await;
    server.close().await;
}

#[tokio::test]
async fn automatic_gate_survives_restart_and_compacts_only_covered_launch_memory() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("automatic.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let session = first["session_id"].as_str().unwrap();
    let base = format!("{}/v1/harness/sessions/{session}", server.url);
    enable(&client, &base, 0, true).await;
    let queued = turns(&client, &base, 6).await;
    let gate = pending(&engine, &client, &base).await;
    assert_eq!(model.count(), 6);
    assert_eq!(gate["automatic"]["held_turn"], queued[5]["task_id"]);
    assert_eq!(gate["automatic"]["through_sequence"], 4);
    let held = get(
        &client,
        &format!(
            "{base}/turns/{}/memory",
            queued[5]["task_id"].as_str().unwrap()
        ),
    )
    .await;
    assert!(held["runs"].as_array().unwrap().is_empty());
    let before = get(
        &client,
        &format!(
            "{base}/turns/{}/memory",
            queued[4]["task_id"].as_str().unwrap()
        ),
    )
    .await;
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let base = format!("{}/v1/harness/sessions/{session}", server.url);
    let restarted = get(&client, &format!("{base}/summary")).await;
    assert_eq!(restarted["automatic"], gate["automatic"]);
    assert_eq!(
        restarted["last_attempt"]["run_id"],
        gate["last_attempt"]["run_id"]
    );
    settle(&engine).await;
    assert_eq!(model.count(), 8);
    let saved = get(&client, &format!("{base}/summary")).await;
    assert_eq!(saved["summary"]["through_sequence"], 4);
    assert!(saved["automatic"]["held_turn"].is_null());
    let future = get(
        &client,
        &format!(
            "{base}/turns/{}/memory",
            queued[5]["task_id"].as_str().unwrap()
        ),
    )
    .await;
    let memory = &future["runs"][0]["memory"];
    assert_eq!(memory["entries"].as_array().unwrap().len(), 2);
    assert_eq!(memory["entries"][0]["task_id"], queued[3]["task_id"]);
    assert_eq!(memory["entries"][1]["task_id"], queued[4]["task_id"]);
    assert_eq!(memory["omitted_entries"], 4);
    assert_eq!(future["runs"][0]["session_summary"], saved["summary"]);
    assert_eq!(
        get(
            &client,
            &format!(
                "{base}/turns/{}/memory",
                queued[4]["task_id"].as_str().unwrap()
            )
        )
        .await,
        before
    );
    let conn = pool(&db).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM harness_automatic_summary_attempts")
            .fetch_one(&conn)
            .await
            .unwrap(),
        1
    );
    let delegate = post(
        &client,
        &format!("{base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Delegate a history check"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let snapshots = get(
        &client,
        &format!(
            "{base}/turns/{}/memory",
            delegate["task_id"].as_str().unwrap()
        ),
    )
    .await;
    for run in snapshots["runs"].as_array().unwrap() {
        assert_eq!(run["session_summary"], saved["summary"]);
    }
    conn.close().await;
    server.close().await;
}

#[tokio::test]
async fn failed_automatic_summary_holds_work_allows_other_sessions_and_explicit_exact_retry() {
    let model = Model::start().await;
    model.fail.store(true, Ordering::SeqCst);
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("retry.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        first["session_id"].as_str().unwrap()
    );
    enable(&client, &base, 0, true).await;
    let queued = turns(&client, &base, 6).await;
    settle(&engine).await;
    let failed = get(&client, &format!("{base}/summary")).await;
    assert_eq!(failed["pending"], false);
    assert_eq!(failed["last_attempt"]["status"], "failed");
    assert_eq!(failed["automatic"]["held_turn"], queued[5]["task_id"]);
    assert_eq!(failed["next_through_sequence"], 4);
    assert_eq!(model.count(), 7);
    settle(&engine).await;
    assert_eq!(model.count(), 7, "no automatic retry loop");
    post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(model.count(), 8, "unrelated session remains runnable");
    model.fail.store(false, Ordering::SeqCst);
    let retry = decision(&failed);
    let receipt = post(
        &client,
        &format!("{base}/summary"),
        &retry,
        StatusCode::ACCEPTED,
    )
    .await;
    let replay = post(
        &client,
        &format!("{base}/summary"),
        &retry,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(receipt["run_id"], replay["run_id"]);
    settle(&engine).await;
    assert_eq!(model.count(), 10);
    let prompts = model.requests.lock().unwrap().clone();
    assert_eq!(prompts[6]["source_turns"], prompts[8]["source_turns"]);
    assert_eq!(
        get(&client, &format!("{base}/summary")).await["summary"]["through_sequence"],
        4
    );
    let conn = pool(&db).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM harness_automatic_summary_attempts")
            .fetch_one(&conn)
            .await
            .unwrap(),
        2
    );
    conn.close().await;
    server.close().await;
}

#[tokio::test]
async fn disabling_pending_or_failed_policy_releases_only_after_admitted_run_settles() {
    let model = Model::start().await;
    model.fail.store(true, Ordering::SeqCst);
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("disable.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        first["session_id"].as_str().unwrap()
    );
    enable(&client, &base, 0, true).await;
    let queued = turns(&client, &base, 6).await;
    pending(&engine, &client, &base).await;
    enable(&client, &base, 1, false).await;
    let off = get(&client, &format!("{base}/summary")).await;
    assert_eq!(off["pending"], true);
    assert!(!off["automatic"]["held_turn"].is_null());
    settle(&engine).await;
    assert_eq!(model.count(), 8);
    let off = get(&client, &format!("{base}/summary")).await;
    assert!(off["automatic"]["held_turn"].is_null());
    assert!(off["summary"].is_null());
    let future = get(
        &client,
        &format!(
            "{base}/turns/{}/memory",
            queued[5]["task_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(
        future["runs"][0]["memory"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        6
    );
    enable(&client, &base, 2, true).await;
    turns(&client, &base, 1).await;
    settle(&engine).await;
    assert_eq!(model.count(), 9);
    assert!(
        get(&client, &format!("{base}/summary")).await["automatic"]["blocked_reason"].is_string()
    );
    enable(&client, &base, 3, false).await;
    settle(&engine).await;
    assert_eq!(model.count(), 10);
    server.close().await;
}

#[tokio::test]
async fn automatic_admission_and_publication_rollback_preserve_mailbox_boundary() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("atomic.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let base = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        first["session_id"].as_str().unwrap()
    );
    turns(&client, &base, 5).await;
    settle(&engine).await;
    enable(&client, &base, 0, true).await;
    let queued = turns(&client, &base, 1).await;
    let conn = pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_gate BEFORE INSERT ON harness_summary_gates BEGIN SELECT RAISE(ABORT,'injected gate failure'); END;").execute(&conn).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.step().await.is_err());
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM runs WHERE json_extract(payload,'$.workflow.name')='session_summary'").fetch_one(&conn).await.unwrap(),0);
    assert!(
        get(
            &client,
            &format!(
                "{base}/turns/{}/memory",
                queued[0]["task_id"].as_str().unwrap()
            )
        )
        .await["runs"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    sqlx::raw_sql("DROP TRIGGER reject_gate;")
        .execute(&conn)
        .await
        .unwrap();
    drop(worker);
    let gate = pending(&engine, &client, &base).await;
    let mut worker = engine.worker().await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_gate_publish BEFORE DELETE ON harness_summary_gates BEGIN SELECT RAISE(ABORT,'injected gate publication failure'); END;").execute(&conn).await.unwrap();
    assert!(worker.step().await.is_err());
    let run = engine
        .status(
            gate["automatic"]["run_id"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(run.record().result.is_none());
    assert_eq!(run.record().usage.actual_input_tokens, 0);
    assert_eq!(
        get(&client, &format!("{base}/summary")).await["pending"],
        true
    );
    sqlx::raw_sql("DROP TRIGGER reject_gate_publish;")
        .execute(&conn)
        .await
        .unwrap();
    drop(worker);
    settle(&engine).await;
    assert_eq!(
        get(&client, &format!("{base}/summary")).await["summary"]["through_sequence"],
        4
    );
    conn.close().await;
    server.close().await;
}

#[tokio::test]
async fn unicode_byte_threshold_and_oversized_source_holds_keep_complete_turns() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("bytes.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    for (size, oversized) in [(12500, false), (25000, true)] {
        let text = "é".repeat(size);
        let mut request = input(&model.endpoint);
        request["prompt"] = json!(text);
        let first = post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &request,
            StatusCode::ACCEPTED,
        )
        .await;
        let base = format!(
            "{}/v1/harness/sessions/{}",
            server.url,
            first["session_id"].as_str().unwrap()
        );
        enable(&client, &base, 0, true).await;
        let queued = turns(&client, &base, 3).await;
        settle(&engine).await;
        let view = get(&client, &format!("{base}/summary")).await;
        if oversized {
            assert!(view["summary"].is_null());
            assert!(view["automatic"]["run_id"].is_null());
            assert_eq!(view["automatic"]["held_turn"], queued[2]["task_id"]);
            assert!(
                view["automatic"]["blocked_reason"]
                    .as_str()
                    .unwrap()
                    .contains("48 KiB")
            );
            let before = model.count();
            settle(&engine).await;
            assert_eq!(model.count(), before);
            enable(&client, &base, 1, false).await;
            settle(&engine).await;
            assert_eq!(model.count(), before + 1);
        } else {
            assert_eq!(view["summary"]["through_sequence"], 1);
            let prompts = model.requests.lock().unwrap().clone();
            let summary = prompts
                .iter()
                .find(|p| p.get("source_turns").is_some())
                .unwrap();
            assert_eq!(summary["source_turns"][0]["prompt"], request["prompt"]);
        }
    }
    server.close().await;
}

#[tokio::test]
async fn durable_question_blocks_compaction_until_the_entire_prior_turn_settles() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("wait.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let first = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&model.endpoint),
        StatusCode::ACCEPTED,
    )
    .await;
    let session = first["session_id"].as_str().unwrap();
    let base = format!("{}/v1/harness/sessions/{session}", server.url);
    enable(&client, &base, 0, true).await;
    turns(&client, &base, 4).await;
    let question = post(
        &client,
        &format!("{base}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Ask before completing this task"}),
        StatusCode::ACCEPTED,
    )
    .await;
    turns(&client, &base, 1).await;
    settle(&engine).await;
    assert_eq!(model.count(), 6);
    let view = get(&client, &format!("{base}/summary")).await;
    assert_eq!(view["pending"], false);
    assert!(view["automatic"]["held_turn"].is_null());
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        first["team_id"].as_str().unwrap(),
        question["task_id"].as_str().unwrap()
    );
    let wait = wait(&get(&client, &format!("{goal}/inbox")).await, "question");
    answer(
        &client,
        &goal,
        &wait,
        json!({"kind":"answer","answer":"Yes"}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(model.count(), 9);
    let base = format!("{}/v1/harness/sessions/{session}", server.url);
    assert_eq!(
        get(&client, &format!("{base}/summary")).await["summary"]["through_sequence"],
        4
    );
    server.close().await;
}
