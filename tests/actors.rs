use act0r::{
    Worker,
    adapters::sqlite::SqliteStore,
    application::{ActorService, ports::*},
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

fn message(recipient: AgentId) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":"Do the delegated task"}),
    }
}
fn worker_config(limit: u32) -> Value {
    json!({"workflow":{"name":"demo","version":1},"input":{"message":"Actor task"},"attempt_limit":limit})
}
async fn pair(actors: &ActorService) -> (AgentId, AgentId) {
    let worker = actors
        .create(
            AgentId::new(),
            DefinitionId::new("workflow_worker", 1),
            worker_config(10),
        )
        .await
        .unwrap();
    let coordinator = actors
        .create(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":worker}),
        )
        .await
        .unwrap();
    (coordinator, worker)
}
async fn drain(worker: &mut Worker) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline, "actor work did not finish");
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
async fn pool(db: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(db)
            .foreign_keys(true),
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn duplicate_envelopes_are_idempotent_and_conflicts_missing_recipients_and_forged_events_fail()
 {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("messages.db")).await.unwrap();
    let actors = agents.actors().unwrap();
    let (coordinator, worker_id) = pair(actors).await;
    let original = message(coordinator);
    let (a, b) = tokio::join!(actors.send(original.clone()), actors.send(original.clone()));
    assert_eq!(a.unwrap(), original.id);
    assert_eq!(b.unwrap(), original.id);
    let mut conflict = original.clone();
    conflict.payload = json!({"goal":"different"});
    assert!(matches!(
        actors.send(conflict).await,
        Err(act0r::Error::Domain(DomainError::ConflictingMessage))
    ));
    assert!(actors.send(message(AgentId::new())).await.is_err());
    let mut forged = message(worker_id);
    forged.kind = RUN_COMPLETED.into();
    assert!(actors.send(forged).await.is_err());
    let mut too_large = message(worker_id);
    too_large.payload = json!("x".repeat(65_537));
    assert!(actors.send(too_large).await.is_err());
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    actors.send(original).await.unwrap(); // Redelivery after processing also does nothing.
    assert!(!worker.step().await.unwrap());
    assert_eq!(actors.runs(worker_id).await.unwrap().len(), 1);
    assert_eq!(
        actors.status(worker_id).await.unwrap().record().state["completed"],
        1
    );
    assert_eq!(actors.mailbox(coordinator).await.unwrap().len(), 2);
}

#[tokio::test]
async fn stable_agents_process_multiple_tasks_in_fifo_order_without_overlapping_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("fifo.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let (coordinator, worker_id) = pair(actors).await;
    let requests: Vec<_> = (0..4).map(|_| message(coordinator)).collect();
    for request in &requests {
        actors.send(request.clone()).await.unwrap();
    }
    let mut executor = engine.worker().await.unwrap();
    for _ in 0..8 {
        assert!(executor.advance_work().await.unwrap());
    }
    // All incoming work was recorded, but only the first task can launch a run.
    assert_eq!(actors.runs(worker_id).await.unwrap().len(), 1);
    assert_eq!(
        actors.status(worker_id).await.unwrap().record().state["pending"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    drop(executor);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let mut executor = engine.worker().await.unwrap();
    drain(&mut executor).await;
    let runs = actors.runs(worker_id).await.unwrap();
    assert_eq!(runs.len(), 4);
    assert!(runs.iter().all(|r| r.notified));
    assert_eq!(
        actors.status(worker_id).await.unwrap().record().state["completed"],
        4
    );
    let coordinator_state = actors
        .status(coordinator)
        .await
        .unwrap()
        .record()
        .state
        .clone();
    let jobs = coordinator_state["jobs"].as_array().unwrap();
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(jobs[index]["correlation"], json!(request.id));
        assert_eq!(jobs[index]["result"]["run_id"], json!(runs[index].run_id));
        assert_eq!(jobs[index]["result"]["status"], "succeeded");
        assert_eq!(
            engine
                .status(runs[index].run_id)
                .await
                .unwrap()
                .attempts()
                .len(),
            3
        );
    }
    let mailbox = actors.mailbox(worker_id).await.unwrap();
    assert!(mailbox.iter().all(|m| m.status == MessageStatus::Processed));
    assert!(mailbox.windows(2).all(
        |m| m[0].sequence < m[1].sequence && m[0].processed_revision < m[1].processed_revision
    ));
    let saved = serde_json::to_value(actors.status(worker_id).await.unwrap().record()).unwrap();
    actors
        .create(
            worker_id,
            DefinitionId::new("workflow_worker", 1),
            worker_config(10),
        )
        .await
        .unwrap();
    assert_eq!(
        saved,
        serde_json::to_value(actors.status(worker_id).await.unwrap().record()).unwrap()
    );
    assert!(
        actors
            .create(
                worker_id,
                DefinitionId::new("workflow_worker", 1),
                worker_config(2)
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn paused_and_incompatible_agents_release_the_worker_and_rejected_messages_do_not_mutate_state()
 {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("controls.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let (coordinator, worker_id) = pair(actors).await;
    actors.pause(coordinator).await.unwrap();
    actors.send(message(coordinator)).await.unwrap();
    let mut executor = engine.worker().await.unwrap();
    assert!(!executor.has_active_runs().await.unwrap());
    // Another actor can finish while the coordinator's mailbox is paused.
    let other = actors
        .create(
            AgentId::new(),
            DefinitionId::new("workflow_worker", 1),
            worker_config(1),
        )
        .await
        .unwrap();
    actors.send(message(other)).await.unwrap();
    drain(&mut executor).await;
    assert_eq!(
        actors.status(other).await.unwrap().record().state["last_result"]["status"],
        "budget_exhausted"
    );
    assert_eq!(
        actors.mailbox(coordinator).await.unwrap()[0].status,
        MessageStatus::Queued
    );
    actors.resume(coordinator).await.unwrap();
    let empty_registry = ActorService::new(
        engine.clone(),
        Arc::new(SqliteStore::open(&db).await.unwrap()),
        vec![],
    )
    .unwrap();
    assert!(empty_registry.advance().await.unwrap());
    assert_eq!(
        actors.status(coordinator).await.unwrap().record().status,
        AgentStatus::Blocked
    );
    assert!(!empty_registry.has_pending().await.unwrap());
    assert_eq!(
        actors.mailbox(coordinator).await.unwrap()[0].status,
        MessageStatus::Queued
    );
    actors.resume(coordinator).await.unwrap();
    drain(&mut executor).await;
    let state = actors
        .status(worker_id)
        .await
        .unwrap()
        .record()
        .state
        .clone();
    let mut invalid = message(worker_id);
    invalid.kind = "unsupported".into();
    actors.send(invalid).await.unwrap();
    drain(&mut executor).await;
    assert_eq!(
        state,
        actors.status(worker_id).await.unwrap().record().state
    );
    assert_eq!(
        actors
            .mailbox(worker_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .status,
        MessageStatus::Rejected
    );
}

#[tokio::test]
async fn message_state_outgoing_run_notification_and_reply_transactions_roll_back_together() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let (coordinator, worker_id) = pair(actors).await;
    actors.send(message(coordinator)).await.unwrap();
    let connection = pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_turn BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'rollback turn'); END;").execute(&connection).await.unwrap();
    assert!(actors.advance().await.is_err());
    assert_eq!(
        actors.status(coordinator).await.unwrap().record().revision,
        0
    );
    assert!(actors.mailbox(worker_id).await.unwrap().is_empty());
    assert_eq!(
        actors.mailbox(coordinator).await.unwrap()[0].status,
        MessageStatus::Queued
    );
    sqlx::raw_sql("DROP TRIGGER reject_turn;")
        .execute(&connection)
        .await
        .unwrap();
    actors.advance().await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_run BEFORE INSERT ON agent_runs BEGIN SELECT RAISE(ABORT,'rollback run'); END;").execute(&connection).await.unwrap();
    assert!(actors.advance().await.is_err());
    assert_eq!(actors.status(worker_id).await.unwrap().record().revision, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs")
            .fetch_one(&connection)
            .await
            .unwrap(),
        0
    );
    sqlx::raw_sql("DROP TRIGGER reject_run;")
        .execute(&connection)
        .await
        .unwrap();
    actors.advance().await.unwrap();
    let link = actors.runs(worker_id).await.unwrap()[0].clone();
    let mut executor = engine.worker().await.unwrap();
    while engine.status(link.run_id).await.unwrap().record().status != RunStatus::Succeeded {
        let execution = executor.admit().await.unwrap().unwrap();
        let outcome = executor.execute(execution.clone()).await.unwrap();
        executor.complete(execution, outcome).await.unwrap();
    }
    sqlx::raw_sql("CREATE TRIGGER reject_notify BEFORE UPDATE ON agent_runs WHEN NEW.notified=1 BEGIN SELECT RAISE(ABORT,'rollback notify'); END;").execute(&connection).await.unwrap();
    assert!(actors.advance().await.is_err());
    assert!(!actors.runs(worker_id).await.unwrap()[0].notified);
    assert_eq!(actors.mailbox(worker_id).await.unwrap().len(), 1);
    sqlx::raw_sql("DROP TRIGGER reject_notify;")
        .execute(&connection)
        .await
        .unwrap();
    actors.advance().await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_reply BEFORE INSERT ON agent_mailbox WHEN json_extract(NEW.payload,'$.message.kind')='task_result' BEGIN SELECT RAISE(ABORT,'rollback reply'); END;").execute(&connection).await.unwrap();
    assert!(actors.advance().await.is_err());
    assert_eq!(
        actors.status(worker_id).await.unwrap().record().state["completed"],
        0
    );
    assert_eq!(
        actors.mailbox(worker_id).await.unwrap()[1].message.id,
        link.completion_message_id
    );
    assert_eq!(
        actors.mailbox(worker_id).await.unwrap()[1].status,
        MessageStatus::Queued
    );
    assert_eq!(actors.mailbox(coordinator).await.unwrap().len(), 1);
    sqlx::raw_sql("DROP TRIGGER reject_reply;")
        .execute(&connection)
        .await
        .unwrap();
    drain(&mut executor).await;
    assert_eq!(
        actors.status(worker_id).await.unwrap().record().state["completed"],
        1
    );
    assert_eq!(actors.mailbox(coordinator).await.unwrap().len(), 2);
    assert_eq!(actors.runs(worker_id).await.unwrap().len(), 1);
}

fn examples() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples")
}

#[tokio::test]
async fn rejected_delegated_tasks_return_a_saved_failure_reply() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("rejected.db")).await.unwrap();
    let actors = agents.actors().unwrap();
    // A pinned workflow with invalid input rejects the task before creating a run.
    let mut invalid = worker_config(10);
    invalid["input"] = json!({"unknown":true});
    let worker_id = actors
        .create(
            AgentId::new(),
            DefinitionId::new("workflow_worker", 1),
            invalid,
        )
        .await
        .unwrap();
    let coordinator = actors
        .create(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":worker_id}),
        )
        .await
        .unwrap();
    actors.send(message(coordinator)).await.unwrap();
    let mut executor = engine.worker().await.unwrap();
    drain(&mut executor).await;
    assert!(actors.runs(worker_id).await.unwrap().is_empty());
    assert_eq!(
        actors.status(worker_id).await.unwrap().record().state["active"],
        Value::Null
    );
    assert_eq!(
        actors.mailbox(worker_id).await.unwrap()[0].status,
        MessageStatus::Rejected
    );
    assert_eq!(
        actors.status(coordinator).await.unwrap().record().state["jobs"][0]["result"]["status"],
        "rejected"
    );
    assert!(!executor.has_active_runs().await.unwrap());
}

#[test]
fn cli_creates_agents_delivers_idempotent_messages_and_inspects_saved_replies() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cli.db");
    let run_cli = |args: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_act0r"))
            .arg("--db")
            .arg(&db)
            .arg("--json")
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let config = examples().join("configs/actor-worker.json");
    let worker = run_cli(&[
        "actor",
        "create",
        "workflow_worker",
        "--config-file",
        config.to_str().unwrap(),
    ])["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let coordinator_file = dir.path().join("coordinator.json");
    fs::write(
        &coordinator_file,
        serde_json::to_vec(&json!({"worker":worker})).unwrap(),
    )
    .unwrap();
    let coordinator = run_cli(&[
        "actor",
        "create",
        "coordinator",
        "--config-file",
        coordinator_file.to_str().unwrap(),
    ])["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let task = examples().join("configs/actor-task.json");
    let id = MessageId::new().to_string();
    for _ in 0..2 {
        assert_eq!(
            run_cli(&[
                "actor",
                "send",
                &coordinator,
                "--input-file",
                task.to_str().unwrap(),
                "--message-id",
                &id
            ])["message_id"],
            id
        );
    }
    run_cli(&["worker", "run", "--until-idle"]);
    let status = run_cli(&["actor", "status", &coordinator]);
    assert_eq!(
        status["agent"]["state"]["jobs"][0]["result"]["status"],
        "succeeded"
    );
    assert_eq!(
        run_cli(&["actor", "inbox", &coordinator])
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        run_cli(&["actor", "status", &worker])["runs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}
#[tokio::test]
async fn invalid_queued_tasks_are_rejected_without_losing_active_completion_or_later_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queued-validation.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let task: act0r::domain::task::TaskConfig =
        serde_json::from_str(include_str!("../examples/configs/task-scripted.json")).unwrap();
    let coding = agents
        .create_coding_actor(
            examples().join("benchmark-cases/addition/source"),
            task.agent,
            30,
        )
        .await
        .unwrap();
    let actors = agents.actors().unwrap();
    let coordinator = actors
        .create(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":coding}),
        )
        .await
        .unwrap();
    let first = message(coordinator);
    let mut invalid = message(coordinator);
    invalid.payload = json!({"goal":" "});
    let mut later = message(coordinator);
    later.payload = json!({"goal":"Keep processing after the invalid task"});
    for request in [&first, &invalid, &later] {
        actors.send(request.clone()).await.unwrap();
    }
    let mut executor = engine.worker().await.unwrap();
    for _ in 0..20 {
        if !executor.advance_work().await.unwrap() {
            break;
        }
    }
    let state = actors.status(coding).await.unwrap().record().state.clone();
    assert_eq!(state["pending"].as_array().unwrap().len(), 1);
    assert_eq!(
        actors.mailbox(coding).await.unwrap()[1].status,
        MessageStatus::Rejected
    );
    assert_eq!(
        actors.status(coordinator).await.unwrap().record().state["jobs"][1]["result"]["status"],
        "rejected"
    );
    let first_run = actors.runs(coding).await.unwrap()[0].run_id;
    engine.cancel(first_run).await.unwrap();
    drop(executor);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let mut executor = engine.worker().await.unwrap();
    for _ in 0..20 {
        if !executor.advance_work().await.unwrap() {
            break;
        }
    }
    let runs = actors.runs(coding).await.unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].run_id, first_run);
    let run = engine.status(runs[1].run_id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Queued);
    assert_eq!(run.record().input["input"]["goal"], later.payload["goal"]);
    assert!(run.attempts().is_empty());
    engine.cancel(runs[1].run_id).await.unwrap();
    for _ in 0..20 {
        if !executor.advance_work().await.unwrap() {
            break;
        }
    }
    let state = actors.status(coding).await.unwrap().record().state.clone();
    assert_eq!(state["completed"], 2);
    assert_eq!(state["active"], Value::Null);
    assert!(state["pending"].as_array().unwrap().is_empty());
    let jobs = actors.status(coordinator).await.unwrap().record().state["jobs"].clone();
    assert_eq!(jobs[0]["correlation"], json!(first.id));
    assert_eq!(jobs[0]["result"]["status"], "cancelled");
    assert_eq!(jobs[1]["correlation"], json!(invalid.id));
    assert_eq!(jobs[1]["result"]["status"], "rejected");
    assert_eq!(jobs[2]["correlation"], json!(later.id));
    assert_eq!(jobs[2]["result"]["status"], "cancelled");
    assert!(
        actors
            .runs(coding)
            .await
            .unwrap()
            .iter()
            .all(|r| r.notified)
    );
    assert!(!executor.has_active_runs().await.unwrap());
}
#[tokio::test]
async fn a_coding_agent_runs_through_its_mailbox_and_keeps_approval_waits_in_the_execution_core() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("coding.db")).await.unwrap();
    let task: act0r::domain::task::TaskConfig =
        serde_json::from_str(include_str!("../examples/configs/task-scripted.json")).unwrap();
    let mut config = task.agent;
    config.require_approval = true;
    let coding = agents
        .create_coding_actor(
            examples().join("benchmark-cases/addition/source"),
            config,
            30,
        )
        .await
        .unwrap();
    let actors = agents.actors().unwrap();
    let coordinator = actors
        .create(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":coding}),
        )
        .await
        .unwrap();
    let request = message(coordinator);
    actors.send(request.clone()).await.unwrap();
    let mut executor = engine.worker().await.unwrap();
    drain(&mut executor).await;
    let link = actors.runs(coding).await.unwrap()[0].clone();
    assert!(!link.notified);
    assert_eq!(
        engine.status(link.run_id).await.unwrap().record().status,
        RunStatus::Waiting
    );
    assert_eq!(actors.mailbox(coordinator).await.unwrap().len(), 1);
    for _ in 0..8 {
        let run = engine.status(link.run_id).await.unwrap();
        if let Some(wait) = &run.record().wait {
            engine
                .signal(link.run_id, wait.id, json!({"approved":true}))
                .await
                .unwrap();
            drain(&mut executor).await;
        } else {
            break;
        }
    }
    let run = engine.status(link.run_id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().input["input"]["goal"], request.payload["goal"]);
    assert_eq!(run.record().result.as_ref().unwrap()["tests_passed"], true);
    assert_eq!(
        actors.status(coordinator).await.unwrap().record().state["jobs"][0]["result"]["run_id"],
        json!(link.run_id)
    );
    assert_eq!(
        actors.status(coordinator).await.unwrap().record().state["jobs"][0]["correlation"],
        json!(request.id)
    );
    assert_eq!(
        actors.status(coding).await.unwrap().record().state["completed"],
        1
    );
}

#[tokio::test]
#[ignore = "subprocess durable actor crash fixture"]
async fn actor_crash_child() {
    let db = std::env::var("ACTOR_CRASH_DB").unwrap();
    let coordinator: AgentId = std::env::var("ACTOR_COORDINATOR").unwrap().parse().unwrap();
    let worker_id: AgentId = std::env::var("ACTOR_WORKER").unwrap().parse().unwrap();
    let marker = std::env::var("ACTOR_MARKER").unwrap();
    let boundary = std::env::var("ACTOR_BOUNDARY").unwrap();
    let (engine, agents) = local_app(db).await.unwrap();
    let actors = agents.actors().unwrap();
    let mut executor = engine.worker().await.unwrap();
    actors.advance().await.unwrap();
    if boundary != "forwarded" {
        actors.advance().await.unwrap();
        let link = actors.runs(worker_id).await.unwrap()[0].clone();
        if boundary == "executed" {
            let execution = executor.admit().await.unwrap().unwrap();
            let _ = executor.execute(execution).await.unwrap();
        }
        if matches!(boundary.as_str(), "completed" | "notified" | "reply") {
            while engine.status(link.run_id).await.unwrap().record().status != RunStatus::Succeeded
            {
                let execution = executor.admit().await.unwrap().unwrap();
                let outcome = executor.execute(execution.clone()).await.unwrap();
                executor.complete(execution, outcome).await.unwrap();
            }
        }
        if matches!(boundary.as_str(), "notified" | "reply") {
            actors.advance().await.unwrap();
        }
        if boundary == "reply" {
            actors.advance().await.unwrap();
        }
    }
    fs::write(marker,serde_json::to_vec(&json!({"coordinator":actors.status(coordinator).await.unwrap().record(),"worker":actors.status(worker_id).await.unwrap().record(),"runs":actors.runs(worker_id).await.unwrap()})).unwrap()).unwrap();
    loop {
        std::thread::park();
    }
}

#[tokio::test]
async fn killed_actor_workers_keep_message_run_and_reply_identities_without_reprocessing_turns() {
    for boundary in [
        "forwarded",
        "run_started",
        "executed",
        "completed",
        "notified",
        "reply",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("crash.db");
        let marker = dir.path().join("marker.json");
        let (engine, agents) = local_app(&db).await.unwrap();
        let actors = agents.actors().unwrap();
        let (coordinator, worker_id) = pair(actors).await;
        let request = message(coordinator);
        actors.send(request.clone()).await.unwrap();
        drop(agents);
        drop(engine);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "actor_crash_child", "--nocapture"])
            .env("ACTOR_CRASH_DB", &db)
            .env("ACTOR_COORDINATOR", coordinator.to_string())
            .env("ACTOR_WORKER", worker_id.to_string())
            .env("ACTOR_MARKER", &marker)
            .env("ACTOR_BOUNDARY", boundary)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !marker.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited at {boundary}"
            );
            if Instant::now() >= deadline {
                child.kill().unwrap();
                panic!("child timeout at {boundary}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value = serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let actors = agents.actors().unwrap();
        let mut executor = engine.worker().await.unwrap();
        drain(&mut executor).await;
        let runs = actors.runs(worker_id).await.unwrap();
        assert_eq!(runs.len(), 1, "boundary {boundary}");
        if boundary != "forwarded" {
            assert_eq!(json!(runs[0].run_id), saved["runs"][0]["run_id"]);
            assert_eq!(
                json!(runs[0].completion_message_id),
                saved["runs"][0]["completion_message_id"]
            );
        }
        assert_eq!(
            actors.status(coordinator).await.unwrap().record().revision,
            2
        );
        assert_eq!(actors.status(worker_id).await.unwrap().record().revision, 2);
        assert_eq!(actors.mailbox(coordinator).await.unwrap().len(), 2);
        assert_eq!(actors.mailbox(worker_id).await.unwrap().len(), 2);
        assert_eq!(
            actors.status(coordinator).await.unwrap().record().state["jobs"][0]["correlation"],
            json!(request.id)
        );
        let run = engine.status(runs[0].run_id).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Succeeded);
        assert_eq!(
            run.attempts().len(),
            if boundary == "executed" { 4 } else { 3 }
        );
        assert!(!executor.step().await.unwrap());
    }
}
