use super::*;
use act0r::{
    adapters::sqlite::SqliteStore,
    application::ports::ExecutionStore,
    domain::{RunId, RunStatus},
};

async fn control(
    client: &Client,
    root: &str,
    action: &str,
    revision: u64,
    status: StatusCode,
) -> Value {
    post(
        client,
        &format!("{root}/{action}"),
        &json!({"expected_revision":revision}),
        status,
    )
    .await
}
async fn pool(db: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(db))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_human_answers_stay_held_across_api_and_worker_restart_until_resume() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("api.db");
    let server = Process::start(&db).await;
    let client = client();
    let team = post(
        &client,
        &format!("{}/v1/teams", server.url),
        &spec(dir.path()),
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
    worker_process(&db);
    let root = base(&server.url, &team, task);
    let initial = get(&client, &format!("{root}/controls")).await;
    assert_eq!(initial["pause"]["revision"], 0);
    assert!(initial["cancellation"].is_null());
    let inbox = get(&client, &format!("{root}/inbox")).await;
    let ask = wait(&inbox, "question");
    let edit = wait(&inbox, "tool_approval");
    let child: MessageId = serde_json::from_value(ask["task_id"].clone()).unwrap();
    let paused = control(
        &client,
        &base(&server.url, &team, child),
        "pause",
        0,
        StatusCode::OK,
    )
    .await;
    assert_eq!(paused["root_task_id"], task.to_string());
    assert_eq!(paused["control"]["revision"], 1);
    assert_eq!(
        paused["control"]["pause"]["requested_task_id"],
        child.to_string()
    );
    let before = get(&client, &root).await["task"]["summary"]["visible_usage"].clone();
    for (item, response) in [
        (&ask, json!({"kind":"answer","answer":"durability"})),
        (&edit, json!({"kind":"approval","approved":true})),
    ] {
        answer(&client, &root, item, response, StatusCode::OK).await;
    }
    drop(server);
    // A fresh worker recovers the database while the API is down; the hold still gates admission.
    worker_process(&db);
    let server = Process::start(&db).await;
    let root = base(&server.url, &team, task);
    let restored = get(&client, &format!("{root}/controls")).await;
    assert_eq!(restored["pause"], paused["control"]);
    assert_eq!(
        get(&client, &root).await["task"]["summary"]["visible_usage"],
        before
    );
    assert!(
        get(&client, &format!("{root}/inbox")).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let retry = control(&client, &root, "pause", 0, StatusCode::OK).await;
    assert_eq!(retry["replayed"], true);
    assert_eq!(retry["control"], paused["control"]);
    let stale = control(&client, &root, "resume", 0, StatusCode::CONFLICT).await;
    assert_eq!(stale["error"]["code"], "goal_revision_conflict");
    assert_eq!(stale["error"]["details"]["current_revision"], 1);
    let resumed = control(&client, &root, "resume", 1, StatusCode::OK).await;
    assert_eq!(resumed["control"]["revision"], 2);
    assert_eq!(resumed["control"]["paused"], false);
    assert_eq!(
        control(&client, &root, "resume", 1, StatusCode::OK).await["control"],
        resumed["control"]
    );
    control(&client, &root, "pause", 0, StatusCode::CONFLICT).await;
    worker_process(&db);
    let done = get(&client, &root).await;
    assert_eq!(done["task"]["summary"]["succeeded"], 3);
    assert_eq!(
        done["task"]["summary"]["visible_usage"]["admitted_model_calls"],
        6
    );
    assert_eq!(
        answer(
            &client,
            &root,
            &ask,
            json!({"kind":"answer","answer":"durability"}),
            StatusCode::OK
        )
        .await["replayed"],
        true
    );
    drop(server);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_receipts_survive_restart_and_leave_independent_goals_runnable() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("api.db");
    let server = Process::start(&db).await;
    let client = client();
    let team = post(
        &client,
        &format!("{}/v1/teams", server.url),
        &spec(dir.path()),
        StatusCode::OK,
    )
    .await;
    let task = MessageId::new();
    submit(
        &client,
        &server.url,
        team["id"].as_str().unwrap(),
        task,
        "Cancel this review",
    )
    .await;
    worker_process(&db);
    let root = base(&server.url, &team, task);
    let inbox = get(&client, &format!("{root}/inbox")).await;
    let ask = wait(&inbox, "question");
    let edit = wait(&inbox, "tool_approval");
    control(&client, &root, "pause", 0, StatusCode::OK).await;
    answer(
        &client,
        &root,
        &ask,
        json!({"kind":"answer","answer":"durability"}),
        StatusCode::OK,
    )
    .await;
    let before = get(&client, &root).await["task"]["summary"]["visible_usage"].clone();
    let child: MessageId = serde_json::from_value(edit["task_id"].clone()).unwrap();
    let cancelled = post(
        &client,
        &format!("{}/cancel", base(&server.url, &team, child)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled["receipt"]["root_task_id"], task.to_string());
    assert_eq!(cancelled["receipt"]["requested_task_id"], child.to_string());
    drop(server);
    worker_process(&db);
    let server = Process::start(&db).await;
    let root = base(&server.url, &team, task);
    let controls = get(&client, &format!("{root}/controls")).await;
    assert_eq!(controls["cancellation"]["receipt"], cancelled["receipt"]);
    assert_eq!(controls["cancellation"]["settled"], true);
    assert_eq!(controls["pause"]["paused"], true);
    assert_eq!(
        get(&client, &root).await["task"]["summary"]["visible_usage"],
        before
    );
    assert_eq!(
        post(
            &client,
            &format!("{root}/cancel"),
            &json!({}),
            StatusCode::OK
        )
        .await["receipt"],
        cancelled["receipt"]
    );
    for action in ["pause", "resume"] {
        control(&client, &root, action, 1, StatusCode::CONFLICT).await;
    }
    answer(
        &client,
        &root,
        &edit,
        json!({"kind":"approval","approved":true}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(
        answer(
            &client,
            &root,
            &ask,
            json!({"kind":"answer","answer":"durability"}),
            StatusCode::OK
        )
        .await["replayed"],
        true
    );
    let independent = MessageId::new();
    submit(
        &client,
        &server.url,
        team["id"].as_str().unwrap(),
        independent,
        "Independent review",
    )
    .await;
    worker_process(&db);
    let other = base(&server.url, &team, independent);
    let waiting = get(&client, &format!("{other}/inbox")).await;
    assert_eq!(waiting["goal_cancelled"], false);
    assert_eq!(waiting["goal_paused"], false);
    answer(
        &client,
        &other,
        &wait(&waiting, "question"),
        json!({"kind":"answer","answer":"independent"}),
        StatusCode::OK,
    )
    .await;
    answer(
        &client,
        &other,
        &wait(&waiting, "tool_approval"),
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    worker_process(&db);
    assert_eq!(
        get(&client, &other).await["task"]["summary"]["succeeded"],
        3
    );
    assert_eq!(
        get(&client, &root).await["task"]["summary"]["visible_usage"],
        before
    );
    drop(server);
}

#[tokio::test]
async fn team_ownership_typed_preconditions_and_concurrent_controls_preserve_newer_decisions() {
    let dir = tempfile::tempdir().unwrap();
    let (_, agents, server, client, team, task) = setup(dir.path(), 32).await;
    let root = base(&server.url, &team, task);
    let baseline = get(&client, &format!("{root}/controls")).await;
    let mut other_spec = spec(dir.path());
    other_spec.name = "other-team".into();
    let other = post(
        &client,
        &format!("{}/v1/teams", server.url),
        &other_spec,
        StatusCode::OK,
    )
    .await;
    let wrong = base(&server.url, &other, task);
    value(
        client
            .get(format!("{wrong}/controls"))
            .send()
            .await
            .unwrap(),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    for action in ["pause", "resume", "cancel"] {
        post(
            &client,
            &format!("{wrong}/{action}"),
            &json!({"expected_revision":0}),
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .await;
    }
    for input in [
        json!({}),
        json!({"expected_revision":-1}),
        json!({"expected_revision":0,"extra":true}),
    ] {
        post(
            &client,
            &format!("{root}/pause"),
            &input,
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await;
    }
    value(
        client
            .post(format!("{root}/pause"))
            .body("{}")
            .send()
            .await
            .unwrap(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
    )
    .await;
    assert_eq!(get(&client, &format!("{root}/controls")).await, baseline);
    assert_eq!(
        control(&client, &root, "resume", 0, StatusCode::OK).await["control"]["revision"],
        0
    );
    let (a, b) = tokio::join!(
        control(&client, &root, "pause", 0, StatusCode::OK),
        control(&client, &root, "pause", 0, StatusCode::OK)
    );
    assert_ne!(a["replayed"], b["replayed"]);
    assert_eq!(a["control"], b["control"]);
    // A Rust/CLI operator also increments the same revision, so HTTP cannot undo it accidentally.
    agents.actors().unwrap().resume_goal(task).await.unwrap();
    control(&client, &root, "pause", 0, StatusCode::CONFLICT).await;
    control(&client, &root, "pause", 2, StatusCode::OK).await;
    for (action, revision) in [("pause", 0), ("resume", 1), ("resume", 99)] {
        control(&client, &root, action, revision, StatusCode::CONFLICT).await;
    }
    assert_eq!(
        get(&client, &format!("{root}/controls")).await["pause"]["revision"],
        3
    );
    server.close().await;
}

#[tokio::test]
async fn failed_control_writes_roll_back_but_failed_cleanup_retains_the_cancellation_guard() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _, server, client, team, task) = setup(dir.path(), 32).await;
    settle(&engine).await;
    let root = base(&server.url, &team, task);
    let sql = pool(&dir.path().join("api.db")).await;
    sqlx::raw_sql("CREATE TRIGGER reject_http_hold BEFORE INSERT ON agent_goal_pauses BEGIN SELECT RAISE(ABORT,'private pause failure'); END;").execute(&sql).await.unwrap();
    let failed = control(
        &client,
        &root,
        "pause",
        0,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert!(!failed.to_string().contains("private pause failure"));
    assert_eq!(
        get(&client, &format!("{root}/controls")).await["pause"]["revision"],
        0
    );
    sqlx::query("DROP TRIGGER reject_http_hold")
        .execute(&sql)
        .await
        .unwrap();
    control(&client, &root, "pause", 0, StatusCode::OK).await;
    sqlx::raw_sql("CREATE TRIGGER reject_http_release BEFORE UPDATE ON agent_goal_pauses BEGIN SELECT RAISE(ABORT,'resume rollback'); END;").execute(&sql).await.unwrap();
    control(
        &client,
        &root,
        "resume",
        1,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(
        get(&client, &format!("{root}/controls")).await["pause"]["revision"],
        1
    );
    sqlx::query("DROP TRIGGER reject_http_release")
        .execute(&sql)
        .await
        .unwrap();
    control(&client, &root, "resume", 1, StatusCode::OK).await;
    let before = get(&client, &root).await["task"]["summary"]["visible_usage"].clone();
    sqlx::raw_sql("CREATE TRIGGER reject_http_cancellation BEFORE INSERT ON agent_goal_cancellations BEGIN SELECT RAISE(ABORT,'cancellation rollback'); END;").execute(&sql).await.unwrap();
    post(
        &client,
        &format!("{root}/cancel"),
        &json!({}),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert!(get(&client, &format!("{root}/controls")).await["cancellation"].is_null());
    sqlx::query("DROP TRIGGER reject_http_cancellation")
        .execute(&sql)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_http_cleanup BEFORE UPDATE ON runs WHEN NEW.status='cancelled' BEGIN SELECT RAISE(ABORT,'cleanup rollback'); END;").execute(&sql).await.unwrap();
    post(
        &client,
        &format!("{root}/cancel"),
        &json!({}),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    let saved = get(&client, &format!("{root}/controls")).await["cancellation"].clone();
    assert_eq!(saved["settled"], false);
    assert!(saved["pending_tasks"].as_u64().unwrap() > 0);
    control(&client, &root, "resume", 2, StatusCode::CONFLICT).await;
    sqlx::query("DROP TRIGGER reject_http_cleanup")
        .execute(&sql)
        .await
        .unwrap();
    settle(&engine).await;
    let settled = get(&client, &format!("{root}/controls")).await["cancellation"].clone();
    assert_eq!(settled["receipt"], saved["receipt"]);
    assert_eq!(settled["settled"], true);
    assert_eq!(
        get(&client, &root).await["task"]["summary"]["visible_usage"],
        before
    );
    sql.close().await;
    server.close().await;
}

#[tokio::test]
async fn resuming_a_goal_does_not_clear_an_individual_run_pause() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _, server, client, team, task) = setup(dir.path(), 32).await;
    settle(&engine).await;
    let root = base(&server.url, &team, task);
    let inbox = get(&client, &format!("{root}/inbox")).await;
    let ask = wait(&inbox, "question");
    let run: RunId = serde_json::from_value(ask["run_id"].clone()).unwrap();
    engine.pause(run).await.unwrap();
    control(&client, &root, "pause", 0, StatusCode::OK).await;
    answer(
        &client,
        &root,
        &ask,
        json!({"kind":"answer","answer":"durability"}),
        StatusCode::OK,
    )
    .await;
    answer(
        &client,
        &root,
        &wait(&inbox, "tool_approval"),
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    control(&client, &root, "resume", 1, StatusCode::OK).await;
    settle(&engine).await;
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Paused
    );
    engine.resume(run).await.unwrap();
    settle(&engine).await;
    assert_eq!(get(&client, &root).await["task"]["summary"]["succeeded"], 3);
    server.close().await;
}

#[tokio::test]
async fn cancellation_status_preserves_blocked_work_and_reports_exact_pending_counts() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _, server, client, team, task) = setup(dir.path(), 32).await;
    settle(&engine).await;
    let root = base(&server.url, &team, task);
    let inbox = get(&client, &format!("{root}/inbox")).await;
    let ask = wait(&inbox, "question");
    let run: RunId = serde_json::from_value(ask["run_id"].clone()).unwrap();
    let store = SqliteStore::open(dir.path().join("api.db")).await.unwrap();
    store
        .mutate(
            run,
            Box::new(|run| {
                run.block("operator reconciliation required".into(), 1000);
                Ok(None)
            }),
        )
        .await
        .unwrap();
    let before = json!(engine.status(run).await.unwrap());
    let cancelled = post(
        &client,
        &format!("{root}/cancel"),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled["settled"], false);
    assert_eq!(cancelled["blocked_tasks"], 1);
    settle(&engine).await;
    let exact = get(&client, &format!("{root}/controls")).await["cancellation"].clone();
    assert_eq!(exact["blocked_tasks"], 1);
    assert!(exact["pending_tasks"].as_u64().unwrap() >= 1);
    assert_eq!(exact["settled"], false);
    assert_eq!(json!(engine.status(run).await.unwrap()), before);
    server.close().await;
}
