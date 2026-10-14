//! Assignment holds reuse durable scheduler authority and keep exact reversible decisions.
use super::worker_recovery::{Model, admit, answer, child, fail, start, work};
use super::*;
use act0r::domain::{Outcome, ReconciliationDecision, ReconciliationRequest, RunId, RunStatus};

fn base(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn goal(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn url(server: &Server, receipt: &Value, task: &Value) -> String {
    format!(
        "{}/workers/{}/pause",
        base(server, receipt),
        task["task_id"].as_str().unwrap()
    )
}
fn request(revision: u64, paused: bool) -> Value {
    json!({"request_id":MessageId::new(),"expected_revision":revision,"paused":paused})
}
fn run(task: &Value) -> RunId {
    task["run_id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn an_assignment_pause_keeps_the_original_delegation_deadline_and_timeout_can_still_stop_it()
{
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("deadline.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    let pool = summary::pool(&db).await;
    // Pin a future absolute deadline, then advance that saved deadline without wall-clock sleeps.
    for deadline in [i64::MAX, 0] {
        sqlx::query("UPDATE runs SET payload=json_set(payload,'$.delegations[0].deadline_at',?) WHERE id=(SELECT run_id FROM agent_tasks WHERE id=?)")
            .bind(deadline).bind(receipt["task_id"].as_str().unwrap()).execute(&pool).await.unwrap();
        sqlx::query("UPDATE agent_delegations SET deadline_at=? WHERE id=?")
            .bind(deadline)
            .bind(assignment["task_id"].as_str().unwrap())
            .execute(&pool)
            .await
            .unwrap();
        if deadline == i64::MAX {
            control(&server, &receipt, &assignment, 0, true).await;
            control(&server, &receipt, &assignment, 1, false).await;
            let saved: i64 =
                sqlx::query_scalar("SELECT deadline_at FROM agent_delegations WHERE id=?")
                    .bind(assignment["task_id"].as_str().unwrap())
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(saved, deadline);
            control(&server, &receipt, &assignment, 2, true).await;
        }
    }
    settle(&engine).await;
    assert_eq!(child(&server, &receipt, 0).await["status"], "cancelled");
    assert_eq!(
        child(&server, &receipt, 0).await["pause"]["can_resume"],
        false
    );
    assert_eq!(child(&server, &receipt, 0).await["execution_held"], false);
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    pool.close().await;
    server.close().await;
}
async fn control(
    server: &Server,
    receipt: &Value,
    task: &Value,
    revision: u64,
    paused: bool,
) -> Value {
    post(
        &client(),
        &url(server, receipt, task),
        &request(revision, paused),
        StatusCode::ACCEPTED,
    )
    .await
}

#[tokio::test]
async fn a_parallel_assignment_stays_paused_across_restart_while_its_sibling_finishes() {
    let model = Model::start(true, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parallel.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), true, None).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    let second = child(&server, &receipt, 1).await;
    let before = engine.status(run(&first)).await.unwrap();
    let packet = request(0, true);
    let path = url(&server, &receipt, &first);
    let c = client();
    let (a, b) = tokio::join!(
        post(&c, &path, &packet, StatusCode::ACCEPTED),
        post(&c, &path, &packet, StatusCode::ACCEPTED)
    );
    assert_ne!(a["replayed"], b["replayed"]);
    assert_eq!(a["control"], b["control"]);
    assert_eq!(a["control"]["root_task_id"], first["task_id"]);
    assert_eq!(
        get(&c, &format!("{}/controls", goal(&server, &receipt))).await["pause"]["paused"],
        false
    );
    post(&c,&format!("{}/workers/{}/steering",base(&server,&receipt),first["task_id"].as_str().unwrap()),&json!({"message_id":MessageId::new(),"run_id":first["run_id"],"text":"Keep the public API"}),StatusCode::ACCEPTED).await;
    answer(&server, &receipt, &first).await;
    answer(&server, &receipt, &second).await;
    settle(&engine).await;
    answer(&server, &receipt, &second).await;
    settle(&engine).await;
    // The fixture intentionally finishes incomplete; this is a saved terminal result.
    assert_eq!(child(&server, &receipt, 1).await["status"], "failed");
    assert_eq!(
        engine
            .status(run(&first))
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        before.record().usage.admitted_model_calls
    );
    let page = work(&server, &receipt).await;
    assert_eq!(page["groups"][0]["finished"], 1);
    assert_eq!(page["groups"][0]["joined"], false);
    assert_eq!(child(&server, &receipt, 0).await["execution_held"], true);
    assert_eq!(
        child(&server, &receipt, 0).await["steering"]["items"][0]["application"],
        Value::Null
    );
    let inspected = get(&c, &goal(&server, &receipt)).await;
    for node in inspected["task"]["nodes"].as_array().unwrap() {
        assert_eq!(node["execution_held"], node["task_id"] == first["task_id"]);
    }
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    let path = url(&server, &receipt, &first);
    assert_eq!(
        post(&c, &path, &packet, StatusCode::ACCEPTED).await["control"],
        a["control"]
    );
    assert_eq!(
        child(&server, &receipt, 0).await["pause"]["can_resume"],
        true
    );
    post(&c, &path, &request(0, false), StatusCode::CONFLICT).await;
    let resumed = control(&server, &receipt, &first, 1, false).await;
    assert_eq!(resumed["control"]["revision"], 2);
    // An old pause acknowledgement cannot re-pause the resumed assignment.
    assert_eq!(
        post(&c, &path, &packet, StatusCode::ACCEPTED).await["replayed"],
        true
    );
    assert_eq!(get(&c, &path).await["control"]["paused"], false);
    settle(&engine).await;
    assert!(child(&server, &receipt, 0).await["steering"]["items"][0]["application"].is_object());
    answer(&server, &receipt, &first).await;
    settle(&engine).await;
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    assert_eq!(
        child(&server, &receipt, 0).await["pause"]["can_pause"],
        false
    );
    assert_eq!(
        post(&c, &path, &resumed["request"], StatusCode::ACCEPTED).await["replayed"],
        true
    );
    post(&c, &path, &request(2, true), StatusCode::CONFLICT).await;
    let pool = summary::pool(&db).await;
    let joins: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM history WHERE json_extract(payload,'$.kind')='task_group_joined'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(joins, 1);
    pool.close().await;
    assert_eq!(
        fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
        "Saved source evidence\n"
    );
    server.close().await;
}

#[tokio::test]
async fn queued_pause_keeps_mailbox_capacity_and_resume_does_not_unpause_the_actor() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("queued.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
    let mut assignment = None;
    for _ in 0..30 {
        assert!(worker.step().await.unwrap());
        let page = work(&server, &receipt).await;
        if let Some(task) = page["groups"].as_array().unwrap().first().and_then(|g| {
            g["children"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["pause"].is_object())
        }) {
            assert!(task["run_id"].is_null());
            assignment = Some(task.clone());
            break;
        }
    }
    let assignment = assignment.unwrap();
    drop(worker);
    let actor = assignment["actor_id"].as_str().unwrap().parse().unwrap();
    let before = agents
        .actors()
        .unwrap()
        .queue(actor)
        .await
        .unwrap()
        .outstanding;
    let saved = control(&server, &receipt, &assignment, 0, true).await;
    assert!(saved["run_id"].is_null());
    settle(&engine).await;
    assert!(child(&server, &receipt, 0).await["run_id"].is_null());
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .queue(actor)
            .await
            .unwrap()
            .outstanding,
        before
    );
    agents.actors().unwrap().pause(actor).await.unwrap();
    let queued = child(&server, &receipt, 0).await;
    assert_eq!(queued["status"], "paused");
    assert_eq!(queued["pause"]["status"], "paused");
    control(&server, &receipt, &assignment, 1, false).await;
    settle(&engine).await;
    assert!(child(&server, &receipt, 0).await["run_id"].is_null());
    assert_eq!(
        json!(
            agents
                .actors()
                .unwrap()
                .status(actor)
                .await
                .unwrap()
                .record()
                .status
        ),
        "paused"
    );
    agents.actors().unwrap().resume(actor).await.unwrap();
    settle(&engine).await;
    assert_eq!(child(&server, &receipt, 0).await["status"], "waiting");
    server.close().await;
}

#[tokio::test]
async fn admitted_model_settles_under_pause_without_admitting_the_next_step() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("admitted.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &assignment).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    assert_eq!(execution.run_id, run(&assignment));
    let frozen = execution.input.clone();
    let before = engine.status(execution.run_id).await.unwrap();
    control(&server, &receipt, &assignment, 0, true).await;
    assert_eq!(
        get(&client(), &url(&server, &receipt, &assignment)).await["admitted_operations"],
        1
    );
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution.clone(), outcome).await.unwrap();
    drop(worker);
    settle(&engine).await;
    let held = engine.status(execution.run_id).await.unwrap();
    assert_eq!(
        held.record().usage.admitted_model_calls,
        before.record().usage.admitted_model_calls
    );
    assert_eq!(
        held.record().usage.actual_input_tokens,
        before.record().usage.actual_input_tokens + 10
    );
    assert_eq!(
        held.activities()
            .iter()
            .find(|a| a.id == execution.activity_id)
            .unwrap()
            .spec
            .input,
        frozen
    );
    assert_eq!(held.attempts().len(), before.attempts().len());
    assert_eq!(
        get(&client(), &url(&server, &receipt, &assignment)).await["admitted_operations"],
        0
    );
    control(&server, &receipt, &assignment, 1, false).await;
    settle(&engine).await;
    assert_eq!(child(&server, &receipt, 0).await["status"], "waiting");
    assert!(
        engine
            .status(execution.run_id)
            .await
            .unwrap()
            .attempts()
            .len()
            > held.attempts().len()
    );
    server.close().await;
}

#[tokio::test]
async fn goal_and_assignment_holds_release_independently_and_stop_takes_precedence() {
    let model = Model::start(true, false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("holds.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), true, None).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    let second = child(&server, &receipt, 1).await;
    control(&server, &receipt, &first, 0, true).await;
    post(
        &client(),
        &format!("{}/pause", goal(&server, &receipt)),
        &json!({"expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    control(&server, &receipt, &first, 1, false).await;
    let view = child(&server, &receipt, 0).await;
    assert_eq!(view["pause"]["control"]["paused"], false);
    assert_eq!(view["pause"]["execution_held"], true);
    assert_eq!(view["pause"]["inherited_held"], true);
    control(&server, &receipt, &first, 2, true).await;
    post(
        &client(),
        &format!("{}/resume", goal(&server, &receipt)),
        &json!({"expected_revision":1}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        child(&server, &receipt, 0).await["pause"]["execution_held"],
        true
    );
    assert_eq!(
        child(&server, &receipt, 1).await["pause"]["execution_held"],
        false
    );
    post(
        &client(),
        &url(&server, &receipt, &first).replace("/pause", "/cancellation"),
        &json!({"request_id":MessageId::new()}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(child(&server, &receipt, 0).await["status"], "cancelled");
    assert_eq!(child(&server, &receipt, 0).await["execution_held"], false);
    assert_eq!(child(&server, &receipt, 1).await["status"], "waiting");
    post(
        &client(),
        &url(&server, &receipt, &first),
        &request(3, false),
        StatusCode::CONFLICT,
    )
    .await;
    control(&server, &receipt, &second, 0, true).await;
    post(
        &client(),
        &format!("{}/cancel", goal(&server, &receipt)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(child(&server, &receipt, 1).await["status"], "cancelled");
    assert_eq!(child(&server, &receipt, 1).await["execution_held"], false);
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    server.close().await;
}

#[tokio::test]
async fn strict_scope_revision_and_identity_checks_and_receipt_rollback_preserve_authority() {
    let model = Model::start(true, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("strict.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), true, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    let other = child(&server, &receipt, 1).await;
    let path = url(&server, &receipt, &assignment);
    for extra in [
        "run_id",
        "actor_id",
        "pause_goal",
        "workspace",
        "permissions",
    ] {
        let mut input = request(0, true);
        input[extra] = json!(true);
        post(&client(), &path, &input, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
    for body in [
        json!({}),
        json!({"request_id":MessageId::new(),"expected_revision":-1,"paused":true}),
        json!({"request_id":MessageId::new(),"expected_revision":0,"paused":"true"}),
    ] {
        post(&client(), &path, &body, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
    for body in [
        json!({"request_id":"00000000-0000-0000-0000-000000000000","expected_revision":0,"paused":true}),
        request(9_007_199_254_740_991, true),
    ] {
        post(&client(), &path, &body, StatusCode::BAD_REQUEST).await;
    }
    for bad in [
        path.replace(
            assignment["task_id"].as_str().unwrap(),
            receipt["task_id"].as_str().unwrap(),
        ),
        path.replace(
            assignment["task_id"].as_str().unwrap(),
            &MessageId::new().to_string(),
        ),
        path.replace(
            receipt["session_id"].as_str().unwrap(),
            &act0r::domain::SessionId::new().to_string(),
        ),
        path.replace(
            receipt["task_id"].as_str().unwrap(),
            &MessageId::new().to_string(),
        ),
    ] {
        assert!(
            !client()
                .post(bad)
                .json(&request(0, true))
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }
    let before = engine.status(run(&assignment)).await.unwrap();
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER refuse_pause BEFORE INSERT ON harness_worker_pauses BEGIN SELECT RAISE(ABORT,'fixture rollback'); END;").execute(&pool).await.unwrap();
    post(
        &client(),
        &path,
        &request(0, true),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(
        json!(engine.status(run(&assignment)).await.unwrap().record()),
        json!(before.record())
    );
    assert_eq!(get(&client(), &path).await["control"]["revision"], 0);
    assert_eq!(get(&client(), &path).await["execution_held"], false);
    sqlx::raw_sql("DROP TRIGGER refuse_pause")
        .execute(&pool)
        .await
        .unwrap();
    let packet = request(0, true);
    post(&client(), &path, &packet, StatusCode::ACCEPTED).await;
    post(
        &client(),
        &url(&server, &receipt, &other),
        &packet,
        StatusCode::CONFLICT,
    )
    .await;
    let mut changed = packet.clone();
    changed["paused"] = json!(false);
    post(&client(), &path, &changed, StatusCode::CONFLICT).await;
    post(&client(), &path, &request(1, true), StatusCode::CONFLICT).await;
    control(&server, &receipt, &assignment, 1, false).await;
    control(&server, &receipt, &assignment, 2, true).await;
    post(&client(), &path, &request(0, true), StatusCode::CONFLICT).await;
    post(&client(), &path, &request(1, false), StatusCode::CONFLICT).await;
    post(&client(), &path, &packet, StatusCode::ACCEPTED).await;
    assert_eq!(get(&client(), &path).await["control"]["revision"], 3);
    assert_eq!(
        child(&server, &receipt, 1).await["pause"]["control"]["revision"],
        0
    );
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn interrupted_edit_remains_reconcilable_and_resume_never_bypasses_its_block() {
    let model=Model::with_first_action(false,false,Some(json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"Saved source evidence","new":"Edited evidence"}}))).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("unsafe.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    let inbox = get(&client(), &format!("{}/inbox", goal(&server, &receipt))).await;
    let wait = inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["task_id"] == assignment["task_id"] && i["actionable"] == true)
        .unwrap();
    post(
        &client(),
        &format!(
            "{}/waits/{}/responses",
            goal(&server, &receipt),
            wait["wait"]["id"].as_str().unwrap()
        ),
        &json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let before = engine.status(execution.run_id).await.unwrap();
    let copy = before.record().input["input"]["workspace"]
        .as_str()
        .unwrap()
        .to_owned();
    control(&server, &receipt, &assignment, 0, true).await;
    assert!(matches!(
        worker.execute(execution.clone()).await.unwrap(),
        Outcome::Success(_) | Outcome::SuccessWithUsage { .. }
    ));
    worker
        .complete(
            execution.clone(),
            Outcome::Uncertain("edit acknowledgement lost after write".into()),
        )
        .await
        .unwrap();
    drop(worker);
    settle(&engine).await;
    assert_eq!(
        engine
            .status(execution.run_id)
            .await
            .unwrap()
            .record()
            .status,
        RunStatus::Blocked
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    control(&server, &receipt, &assignment, 1, false).await;
    settle(&engine).await;
    assert_eq!(
        engine
            .status(execution.run_id)
            .await
            .unwrap()
            .record()
            .status,
        RunStatus::Blocked
    );
    assert_eq!(
        engine
            .status(execution.run_id)
            .await
            .unwrap()
            .attempts()
            .len(),
        before.attempts().len()
    );
    control(&server, &receipt, &assignment, 2, true).await;
    engine
        .reconcile(
            execution.run_id,
            ReconciliationRequest {
                activity_id: execution.activity_id,
                attempt_id: execution.attempt_id,
                evidence: "Inspected the journal and file; the old operation has stopped".into(),
                decision: ReconciliationDecision::Retry {},
            },
        )
        .await
        .unwrap();
    settle(&engine).await;
    assert_eq!(
        engine
            .status(execution.run_id)
            .await
            .unwrap()
            .attempts()
            .len(),
        before.attempts().len()
    );
    assert_eq!(
        fs::read_to_string(Path::new(&copy).join("src/lib.rs")).unwrap(),
        "Edited evidence\n"
    );
    control(&server, &receipt, &assignment, 3, false).await;
    settle(&engine).await;
    assert_eq!(child(&server, &receipt, 0).await["status"], "waiting");
    assert_eq!(
        fs::read_to_string(Path::new(&copy).join("src/lib.rs")).unwrap(),
        "Edited evidence\n"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
        "Saved source evidence\n"
    );
    server.close().await;
}

#[tokio::test]
async fn a_paused_model_recovery_checkpoint_waits_for_resume_before_retry() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("recovery.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &assignment).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    fail(&mut worker, execution, "model_invalid_action").await;
    drop(worker);
    let before = engine.status(run(&assignment)).await.unwrap();
    assert!(before.needs_worker_recovery());
    control(&server, &receipt, &assignment, 0, true).await;
    let view = child(&server, &receipt, 0).await;
    assert_eq!(view["pause"]["can_resume"], true);
    assert_eq!(view["recovery"]["retry_allowed"], false);
    assert_eq!(view["recovery"]["accept_allowed"], false);
    let checkpoint = view["recovery"].clone();
    let packet = json!({"request_id":MessageId::new(),"run_id":checkpoint["run_id"],"activity_id":checkpoint["activity_id"],"attempt_id":checkpoint["attempt_id"],"expected_history_sequence":checkpoint["history_sequence"],"decision":"retry"});
    let path = url(&server, &receipt, &assignment).replace("/pause", "/recovery");
    post(&client(), &path, &packet, StatusCode::CONFLICT).await;
    control(&server, &receipt, &assignment, 1, false).await;
    assert_eq!(
        json!(engine.status(run(&assignment)).await.unwrap().record()),
        json!(before.record())
    );
    post(&client(), &path, &packet, StatusCode::ACCEPTED).await;
    settle(&engine).await;
    assert_eq!(child(&server, &receipt, 0).await["status"], "waiting");
    server.close().await;
}
