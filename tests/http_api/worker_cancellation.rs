//! Assignment-scoped cancellation uses the existing ownership/admission contract.
use super::worker_recovery::{Model, admit, answer, child, fail, start, work};
use super::*;
use act0r::domain::{
    Outcome, ReconciliationDecision, ReconciliationRequest, RecoveryPolicy, RunId, RunStatus,
};

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
fn url(server: &Server, receipt: &Value, assignment: &Value) -> String {
    format!(
        "{}/workers/{}/cancellation",
        base(server, receipt),
        assignment["task_id"].as_str().unwrap()
    )
}
fn request() -> Value {
    json!({"request_id":MessageId::new()})
}
fn run(assignment: &Value) -> RunId {
    assignment["run_id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn one_parallel_assignment_stops_durably_while_its_sibling_continues_and_joins_once() {
    let model = Model::start(true, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("stop.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), true, None).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    let second = child(&server, &receipt, 1).await;
    let sibling = engine.status(run(&second)).await.unwrap();
    let previous = engine.status(run(&first)).await.unwrap();
    assert_eq!(first["cancellation"]["can_stop"], true);
    let packet = request();
    let path = url(&server, &receipt, &first);
    let c = client();
    let (a, b) = tokio::join!(
        post(&c, &path, &packet, StatusCode::ACCEPTED),
        post(&c, &path, &packet, StatusCode::ACCEPTED)
    );
    assert_ne!(a["replayed"], b["replayed"]);
    assert_eq!(a["saved_at"], b["saved_at"]);
    assert_eq!(a["status"], "cancelled");
    assert_eq!(
        json!(engine.status(run(&second)).await.unwrap().record()),
        json!(sibling.record())
    );
    let stopped = engine.status(run(&first)).await.unwrap();
    assert_eq!(stopped.record().status, RunStatus::Cancelled);
    assert_eq!(
        json!(stopped.record().usage),
        json!(previous.record().usage)
    );
    assert_eq!(
        get(&c, &format!("{}/controls", goal(&server, &receipt))).await["cancellation"],
        Value::Null
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let replay = post(
        &c,
        &url(&server, &receipt, &first),
        &packet,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["saved_at"], a["saved_at"]);
    settle(&engine).await;
    let page = work(&server, &receipt).await;
    assert_eq!(page["groups"][0]["finished"], 1);
    assert_eq!(page["groups"][0]["joined"], false);
    assert_eq!(child(&server, &receipt, 1).await["status"], "waiting");
    answer(&server, &receipt, &second).await;
    settle(&engine).await;
    answer(&server, &receipt, &second).await;
    settle(&engine).await;
    // This fixture deliberately finishes incomplete after two questions; its final
    // result still proves that the sibling kept executing and returned to the parent.
    assert_eq!(child(&server, &receipt, 1).await["status"], "failed");
    assert!(
        child(&server, &receipt, 1).await["result"]
            .as_str()
            .unwrap()
            .contains("Worker finished")
    );
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    let pool = summary::pool(&db).await;
    let id: String = sqlx::query_scalar("SELECT run_id FROM agent_tasks WHERE id=?")
        .bind(receipt["task_id"].as_str().unwrap())
        .fetch_one(&pool)
        .await
        .unwrap();
    let parent_id: RunId = id.parse().unwrap();
    let parent = engine.status(parent_id).await.unwrap();
    assert_eq!(
        parent.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "cancelled"
    );
    for (kind, expected) in [("task_group_joined", 1_i64), ("delegation_completed", 2)] {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM history WHERE run_id=? AND json_extract(payload,'$.kind')=?",
        )
        .bind(parent_id.to_string())
        .bind(kind)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, expected);
    }
    pool.close().await;
    assert_eq!(
        fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
        "Saved source evidence\n"
    );
    post(
        &c,
        &url(&server, &receipt, &first),
        &request(),
        StatusCode::CONFLICT,
    )
    .await;
    server.close().await;
}

#[tokio::test]
async fn queued_assignment_can_stop_on_a_paused_actor_without_launching_a_run() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("queued.db")).await.unwrap();
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
                .find(|c| c["cancellation"].is_object())
        }) {
            assert!(task["run_id"].is_null());
            assignment = Some(task.clone());
            break;
        }
    }
    let assignment = assignment.unwrap();
    assert_eq!(assignment["cancellation"]["can_stop"], true);
    let actor_id = assignment["actor_id"].as_str().unwrap().parse().unwrap();
    agents.actors().unwrap().pause(actor_id).await.unwrap();
    let paused = child(&server, &receipt, 0).await;
    assert_eq!(paused["status"], "paused");
    assert_eq!(paused["cancellation"]["status"], "paused");
    assert_eq!(paused["cancellation"]["can_stop"], true);
    post(
        &client(),
        &url(&server, &receipt, &assignment),
        &request(),
        StatusCode::ACCEPTED,
    )
    .await;
    drop(worker);
    settle(&engine).await;
    let current = child(&server, &receipt, 0).await;
    assert_eq!(current["status"], "cancelled");
    assert!(current["run_id"].is_null());
    let actor = agents.actors().unwrap().status(actor_id).await.unwrap();
    assert_eq!(json!(actor.record().status), "paused");
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .queue(actor_id)
            .await
            .unwrap()
            .outstanding,
        0
    );
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    server.close().await;
}

#[tokio::test]
async fn admitted_model_finishes_before_cancellation_and_cannot_start_more_work() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("running.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &assignment).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    assert_eq!(execution.run_id, run(&assignment));
    let before = engine.status(execution.run_id).await.unwrap();
    let saved = post(
        &client(),
        &url(&server, &receipt, &assignment),
        &request(),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(saved["status"], "cancelling");
    assert_eq!(work(&server, &receipt).await["groups"][0]["finished"], 0);
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution.clone(), outcome).await.unwrap();
    drop(worker);
    settle(&engine).await;
    let after = engine.status(execution.run_id).await.unwrap();
    assert_eq!(after.record().status, RunStatus::Cancelled);
    assert_eq!(
        after.record().usage.admitted_model_calls,
        before.record().usage.admitted_model_calls
    );
    assert_eq!(after.activities().len(), before.activities().len());
    assert_eq!(
        get(&client(), &url(&server, &receipt, &assignment)).await["settled"],
        true
    );
    server.close().await;
}

#[tokio::test]
async fn held_model_recovery_can_be_stopped_while_the_goal_is_paused() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("held.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &assignment).await;
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    fail(&mut worker, execution, "model_invalid_action").await;
    drop(worker);
    assert!(
        engine
            .status(run(&assignment))
            .await
            .unwrap()
            .needs_worker_recovery()
    );
    post(
        &client(),
        &format!("{}/pause", goal(&server, &receipt)),
        &json!({"expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    post(
        &client(),
        &url(&server, &receipt, &assignment),
        &request(),
        StatusCode::ACCEPTED,
    )
    .await;
    let stopped = engine.status(run(&assignment)).await.unwrap();
    assert!(!stopped.needs_worker_recovery());
    assert_eq!(stopped.record().status, RunStatus::Cancelled);
    let controls = get(&client(), &format!("{}/controls", goal(&server, &receipt))).await;
    assert_eq!(controls["pause"]["paused"], true);
    assert!(controls["cancellation"].is_null());
    post(
        &client(),
        &format!("{}/resume", goal(&server, &receipt)),
        &json!({"expected_revision":1}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    server.close().await;
}

#[tokio::test]
async fn wrong_scope_changed_identity_terminal_races_and_transaction_rollback_cannot_stop_other_tasks()
 {
    let model = Model::start(true, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("scope.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), true, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    let other = child(&server, &receipt, 1).await;
    let path = url(&server, &receipt, &assignment);
    for input in [
        json!({}),
        json!({"request_id":MessageId::new(),"run_id":run(&other)}),
        json!({"request_id":"bad"}),
    ] {
        post(&client(), &path, &input, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
    post(
        &client(),
        &path,
        &json!({"request_id":"00000000-0000-0000-0000-000000000000"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let before = engine.status(run(&assignment)).await.unwrap();
    for bad in [
        format!(
            "{}/workers/{}/cancellation",
            base(&server, &receipt),
            receipt["task_id"].as_str().unwrap()
        ),
        format!(
            "{}/workers/{}/cancellation",
            base(&server, &receipt),
            MessageId::new()
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
                .json(&request())
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER refuse_stop BEFORE INSERT ON harness_worker_cancellations BEGIN SELECT RAISE(ABORT,'fixture rollback'); END;").execute(&pool).await.unwrap();
    post(
        &client(),
        &path,
        &request(),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(
        json!(engine.status(run(&assignment)).await.unwrap().record()),
        json!(before.record())
    );
    assert_eq!(get(&client(), &path).await["requested"], false);
    sqlx::raw_sql("DROP TRIGGER refuse_stop")
        .execute(&pool)
        .await
        .unwrap();
    let packet = request();
    post(&client(), &path, &packet, StatusCode::ACCEPTED).await;
    post(
        &client(),
        &url(&server, &receipt, &other),
        &packet,
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(
        child(&server, &receipt, 1).await["cancellation"]["requested"],
        false
    );
    // A whole-request stop after a scoped stop must still settle both members once.
    post(
        &client(),
        &format!("{}/cancel", goal(&server, &receipt)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(child(&server, &receipt, 1).await["status"], "cancelled");
    assert_eq!(
        get(&client(), &format!("{}/controls", goal(&server, &receipt))).await["cancellation"]["settled"],
        true
    );
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn unsafe_edit_stop_waits_for_reconciliation_across_restart_and_never_repeats_the_edit() {
    for stop_before_uncertainty in [true, false] {
        let model=Model::with_first_action(false,false,Some(json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"Saved source evidence","new":"Edited evidence"}}))).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("unsafe.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let receipt = start(&server, &model, root.path(), false, None).await;
        settle(&engine).await;
        let assignment = child(&server, &receipt, 0).await;
        let inbox = get(&client(), &format!("{}/inbox", goal(&server, &receipt))).await;
        let approval = inbox["items"]
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
                approval["wait"]["id"].as_str().unwrap()
            ),
            &json!({"kind":"approval","approved":true}),
            StatusCode::OK,
        )
        .await;
        let mut worker = engine.worker().await.unwrap();
        let execution = admit(&mut worker).await;
        assert_eq!(execution.run_id, run(&assignment));
        let before = engine.status(execution.run_id).await.unwrap();
        assert_eq!(
            before
                .activities()
                .iter()
                .find(|a| a.id == execution.activity_id)
                .unwrap()
                .spec
                .recovery,
            RecoveryPolicy::RequiresReconciliation
        );
        let copy = before.record().input["input"]["workspace"]
            .as_str()
            .unwrap()
            .to_owned();
        let outcome = worker.execute(execution.clone()).await.unwrap();
        assert!(matches!(
            outcome,
            Outcome::Success(_) | Outcome::SuccessWithUsage { .. }
        ));
        assert_eq!(
            fs::read_to_string(Path::new(&copy).join("src/lib.rs")).unwrap(),
            "Edited evidence\n"
        );
        let packet = request();
        let path = url(&server, &receipt, &assignment);
        if stop_before_uncertainty {
            post(&client(), &path, &packet, StatusCode::ACCEPTED).await;
        }
        assert!(
            !engine
                .status(execution.run_id)
                .await
                .unwrap()
                .record()
                .controls
                .cancel_requested
        );
        worker
            .complete(
                execution.clone(),
                Outcome::Uncertain("edit acknowledgement lost after write".into()),
            )
            .await
            .unwrap();
        drop(worker);
        if !stop_before_uncertainty {
            post(&client(), &path, &packet, StatusCode::ACCEPTED).await;
        }
        let view = get(&client(), &path).await;
        assert_eq!(view["requested"], true);
        assert_eq!(view["settled"], false);
        assert_eq!(view["status"], "blocked");
        settle(&engine).await;
        assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
        server.close().await;
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        assert_eq!(
            get(&client(), &url(&server, &receipt, &assignment)).await["requested"],
            true
        );
        assert_eq!(
            post(
                &client(),
                &url(&server, &receipt, &assignment),
                &packet,
                StatusCode::ACCEPTED
            )
            .await["replayed"],
            true
        );
        engine
            .reconcile(
                execution.run_id,
                ReconciliationRequest {
                    activity_id: execution.activity_id,
                    attempt_id: execution.attempt_id,
                    evidence:
                        "Inspected the journal and edited file; the old operation has stopped"
                            .into(),
                    decision: ReconciliationDecision::Retry {},
                },
            )
            .await
            .unwrap();
        settle(&engine).await;
        let after = engine.status(execution.run_id).await.unwrap();
        assert_eq!(after.record().status, RunStatus::Cancelled);
        assert_eq!(after.attempts().len(), before.attempts().len());
        assert_eq!(
            after.record().usage.admitted_model_calls,
            before.record().usage.admitted_model_calls
        );
        assert_eq!(after.record().reconciliation_receipts.len(), 1);
        assert_eq!(
            fs::read_to_string(Path::new(&copy).join("src/lib.rs")).unwrap(),
            "Edited evidence\n"
        );
        assert_eq!(
            fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
            "Saved source evidence\n"
        );
        assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
        assert!(
            get(&client(), &format!("{}/controls", goal(&server, &receipt))).await["cancellation"]
                .is_null()
        );
        server.close().await;
    }
}

#[tokio::test]
async fn an_old_stop_cannot_cancel_a_later_assignment_on_the_same_actor() {
    let model = Model::start(false, false).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("reuse.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    let packet = request();
    post(
        &client(),
        &url(&server, &receipt, &assignment),
        &packet,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let next = post(
        &client(),
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            receipt["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"Another assignment"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let next_child = child(&server, &next, 0).await;
    assert_eq!(next_child["actor_id"], assignment["actor_id"]);
    assert_ne!(next_child["task_id"], assignment["task_id"]);
    post(
        &client(),
        &url(&server, &receipt, &assignment),
        &packet,
        StatusCode::ACCEPTED,
    )
    .await;
    post(
        &client(),
        &url(&server, &next, &next_child),
        &packet,
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(child(&server, &next, 0).await["status"], "waiting");
    assert_eq!(
        child(&server, &next, 0).await["cancellation"]["requested"],
        false
    );
    server.close().await;
}

#[tokio::test]
async fn read_only_helpers_can_be_stopped_without_a_coding_recovery_contract() {
    let model = Model::start(false, true).await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("helper.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path(), false, None).await;
    settle(&engine).await;
    let assignment = child(&server, &receipt, 0).await;
    assert_eq!(assignment["cancellation"]["can_stop"], true);
    post(
        &client(),
        &url(&server, &receipt, &assignment),
        &request(),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(child(&server, &receipt, 0).await["status"], "cancelled");
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], true);
    server.close().await;
}
