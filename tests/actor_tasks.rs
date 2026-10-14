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
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn request(agent: AgentId) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient: agent,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":"Do this task"}),
    }
}
async fn pair(actors: &ActorService) -> (AgentId, AgentId) {
    let worker = actors.create(AgentId::new(), DefinitionId::new("workflow_worker", 1),
        json!({"workflow":{"name":"demo","version":1},"input":{"message":"Task lifecycle"},"attempt_limit":10})).await.unwrap();
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
    let deadline = Instant::now() + Duration::from_secs(60);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline);
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
async fn turns(worker: &mut Worker) {
    for _ in 0..30 {
        if !worker.advance_work().await.unwrap() {
            return;
        }
    }
    panic!("mailbox turns did not quiesce");
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
async fn cancelling_b_after_it_enters_the_pending_queue_survives_restart_and_runs_only_a_and_c() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("abc.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let (coordinator, worker_id) = pair(actors).await;
    let messages: Vec<_> = (0..3).map(|_| request(coordinator)).collect();
    for message in &messages {
        actors.send(message.clone()).await.unwrap();
    }
    let mut worker = engine.worker().await.unwrap();
    turns(&mut worker).await;
    assert_eq!(
        actors.task(messages[0].id).await.unwrap().status,
        AgentTaskStatus::Running
    );
    assert_eq!(
        actors.task(messages[1].id).await.unwrap().status,
        AgentTaskStatus::Queued
    );
    let cancelled = actors.cancel_task(messages[1].id).await.unwrap();
    assert_eq!(cancelled.status, AgentTaskStatus::Cancelled);
    assert!(cancelled.run_id.is_none());
    assert_eq!(cancelled.admitted_attempts, 0);
    assert!(cancelled.usage.is_none());
    assert_eq!(
        actors.status(worker_id).await.unwrap().record().state["pending"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        json!(cancelled),
        json!(actors.cancel_task(messages[1].id).await.unwrap())
    );
    actors.send(messages[1].clone()).await.unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let tasks = actors.tasks(coordinator).await.unwrap();
    assert_eq!(tasks.len(), 3);
    assert_eq!(
        tasks.iter().map(|t| t.status).collect::<Vec<_>>(),
        vec![
            AgentTaskStatus::Succeeded,
            AgentTaskStatus::Cancelled,
            AgentTaskStatus::Succeeded
        ]
    );
    assert_eq!(actors.runs(worker_id).await.unwrap().len(), 2);
    assert_eq!(json!(tasks[1]), json!(cancelled));
    for index in [0, 2] {
        assert_eq!(tasks[index].task.id, messages[index].id);
        assert_eq!(tasks[index].execution_agent_id, Some(worker_id));
        assert_eq!(tasks[index].admitted_attempts, 3);
        assert_eq!(tasks[index].reply.as_ref().unwrap()["status"], "succeeded");
    }
    let jobs = actors.status(coordinator).await.unwrap().record().state["jobs"].clone();
    assert_eq!(jobs[1]["result"]["status"], "cancelled");
    assert_eq!(jobs[2]["result"]["status"], "succeeded");
    assert!(!worker.step().await.unwrap());
}

#[tokio::test]
async fn cancelling_a_child_handle_keeps_equal_correlations_in_other_agents_independent() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("scopes.db")).await.unwrap();
    let actors = agents.actors().unwrap();
    let (first, worker_id) = pair(actors).await;
    let second = actors
        .create(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":worker_id}),
        )
        .await
        .unwrap();
    let correlation = MessageId::new();
    let mut a = request(first);
    a.correlation_id = Some(correlation);
    let mut b = request(second);
    b.correlation_id = Some(correlation);
    actors.send(a.clone()).await.unwrap();
    actors.send(b.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    turns(&mut worker).await;
    let child = actors.tasks(worker_id).await.unwrap()[1].task.id;
    assert_eq!(actors.task(child).await.unwrap().task.parent_id, Some(b.id));
    actors.cancel_task(child).await.unwrap();
    drain(&mut worker).await;
    assert_eq!(
        actors.task(a.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(
        actors.task(b.id).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
    assert_eq!(actors.runs(worker_id).await.unwrap().len(), 1);
    assert_eq!(
        actors.status(first).await.unwrap().record().state["jobs"][0]["result"]["status"],
        "succeeded"
    );
    assert_eq!(
        actors.status(second).await.unwrap().record().state["jobs"][0]["result"]["status"],
        "cancelled"
    );
}

#[tokio::test]
async fn cancellation_before_consumption_and_after_forwarding_never_creates_a_run() {
    for forwarded in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("queued.db")).await.unwrap();
        let actors = agents.actors().unwrap();
        let (coordinator, worker_id) = pair(actors).await;
        let message = request(coordinator);
        actors.send(message.clone()).await.unwrap();
        actors.pause(coordinator).await.unwrap();
        assert_eq!(
            actors.task(message.id).await.unwrap().status,
            AgentTaskStatus::Paused
        );
        actors.resume(coordinator).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        if forwarded {
            assert!(worker.advance_work().await.unwrap());
        }
        let (left, right) = tokio::join!(
            actors.cancel_task(message.id),
            actors.cancel_task(message.id)
        );
        assert_eq!(json!(left.unwrap()), json!(right.unwrap()));
        drain(&mut worker).await;
        assert!(actors.runs(worker_id).await.unwrap().is_empty());
        assert_eq!(
            actors.task(message.id).await.unwrap().status,
            AgentTaskStatus::Cancelled
        );
        let inbox = actors
            .mailbox(if forwarded { worker_id } else { coordinator })
            .await
            .unwrap();
        assert_eq!(inbox[0].status, MessageStatus::Rejected);
        assert!(inbox[0].error.as_ref().unwrap().contains("cancelled"));
        assert!(matches!(
            actors.task(MessageId::new()).await,
            Err(act0r::Error::AgentTaskNotFound(_))
        ));
        assert!(matches!(
            actors.cancel_task(MessageId::new()).await,
            Err(act0r::Error::AgentTaskNotFound(_))
        ));
    }
}

#[tokio::test]
async fn cancellation_of_an_admitted_run_is_cooperative_and_keeps_interrupted_charges() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("admitted.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let (coordinator, _) = pair(actors).await;
    let message = request(coordinator);
    actors.send(message.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    turns(&mut worker).await;
    let run_id = actors.task(message.id).await.unwrap().run_id.unwrap();
    let store = SqliteStore::open(&db).await.unwrap();
    store
        .mutate(run_id, Box::new(|run| run.admit(run.record().updated_at)))
        .await
        .unwrap()
        .unwrap();
    let cancelled = actors.cancel_task(message.id).await.unwrap();
    assert_eq!(cancelled.status, AgentTaskStatus::Cancelling);
    assert_eq!(cancelled.admitted_attempts, 1);
    assert!(
        engine
            .status(run_id)
            .await
            .unwrap()
            .record()
            .controls
            .cancel_requested
    );
    assert_eq!(
        json!(cancelled),
        json!(actors.cancel_task(message.id).await.unwrap())
    );
    store.close().await;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let saved = actors.task(message.id).await.unwrap();
    assert_eq!(saved.status, AgentTaskStatus::Cancelled);
    assert_eq!(saved.run_id, Some(run_id));
    assert_eq!(saved.admitted_attempts, 1);
    assert_eq!(
        engine.status(run_id).await.unwrap().attempts()[0].status,
        AttemptStatus::Interrupted
    );
    assert_eq!(saved.reply.unwrap()["status"], "cancelled");
}

#[tokio::test]
async fn task_status_exposes_approval_waits_run_pauses_and_saved_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("waiting.db")).await.unwrap();
    let config =
        serde_json::from_str(include_str!("../examples/configs/agent-scripted.json")).unwrap();
    let coding = agents
        .create_coding_actor(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/benchmark-cases/addition/source"),
            config,
            30,
        )
        .await
        .unwrap();
    let actors = agents.actors().unwrap();
    let message = request(coding);
    actors.send(message.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let waiting = actors.task(message.id).await.unwrap();
    assert_eq!(waiting.status, AgentTaskStatus::Waiting);
    let run_id = waiting.run_id.unwrap();
    let wait_id = waiting.wait.unwrap().id;
    engine.pause(run_id).await.unwrap();
    assert_eq!(
        actors.task(message.id).await.unwrap().status,
        AgentTaskStatus::Paused
    );
    engine.resume(run_id).await.unwrap();
    assert_eq!(
        actors.task(message.id).await.unwrap().status,
        AgentTaskStatus::Waiting
    );
    let cancelled = actors.cancel_task(message.id).await.unwrap();
    assert_eq!(cancelled.status, AgentTaskStatus::Cancelled);
    assert_eq!(cancelled.admitted_attempts, waiting.admitted_attempts);
    assert!(
        engine
            .signal(run_id, wait_id, json!({"approved":true}))
            .await
            .is_err()
    );
    drain(&mut worker).await;
    assert_eq!(
        json!(cancelled),
        json!(actors.task(message.id).await.unwrap())
    );
}

#[tokio::test]
async fn a_saved_terminal_run_wins_over_late_cancellation_before_notification() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("late.db")).await.unwrap();
    let actors = agents.actors().unwrap();
    let (coordinator, worker_id) = pair(actors).await;
    let message = request(coordinator);
    actors.send(message.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    turns(&mut worker).await;
    let run_id = actors.task(message.id).await.unwrap().run_id.unwrap();
    while !AgentTaskStatus::from_run(engine.status(run_id).await.unwrap().record()).is_terminal() {
        worker.step().await.unwrap();
    }
    assert!(!actors.runs(worker_id).await.unwrap()[0].notified);
    let before = actors.task(message.id).await.unwrap();
    assert_eq!(before.status, AgentTaskStatus::Succeeded);
    assert_eq!(
        json!(before),
        json!(actors.cancel_task(message.id).await.unwrap())
    );
    assert!(
        !engine
            .status(run_id)
            .await
            .unwrap()
            .record()
            .controls
            .cancel_requested
    );
    drain(&mut worker).await;
    assert_eq!(actors.task(message.id).await.unwrap().reply, before.reply);
}

#[tokio::test]
async fn cancellation_rolls_back_run_controls_queue_changes_task_receipts_and_outgoing_replies() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let (coordinator, worker_id) = pair(actors).await;
    let a = request(coordinator);
    let b = request(coordinator);
    actors.send(a.clone()).await.unwrap();
    actors.send(b.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    turns(&mut worker).await;
    let connection = pool(&db).await;
    let before = json!({"tasks":actors.tasks(coordinator).await.unwrap(),"state":actors.status(worker_id).await.unwrap(),"coordinator":actors.status(coordinator).await.unwrap()});
    sqlx::raw_sql("CREATE TRIGGER fail_queue_cancel BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'queue rollback'); END;").execute(&connection).await.unwrap();
    assert!(actors.cancel_task(b.id).await.is_err());
    assert_eq!(
        before,
        json!({"tasks":actors.tasks(coordinator).await.unwrap(),"state":actors.status(worker_id).await.unwrap(),"coordinator":actors.status(coordinator).await.unwrap()})
    );
    sqlx::raw_sql("DROP TRIGGER fail_queue_cancel; CREATE TRIGGER fail_run_cancel BEFORE UPDATE ON agent_tasks BEGIN SELECT RAISE(ABORT,'run rollback'); END;").execute(&connection).await.unwrap();
    let run_id = actors.task(a.id).await.unwrap().run_id.unwrap();
    let before_run = json!(engine.status(run_id).await.unwrap());
    let before_history = json!(engine.history(run_id).await.unwrap());
    assert!(actors.cancel_task(a.id).await.is_err());
    assert_eq!(before_run, json!(engine.status(run_id).await.unwrap()));
    assert_eq!(before_history, json!(engine.history(run_id).await.unwrap()));
    sqlx::raw_sql("DROP TRIGGER fail_run_cancel;")
        .execute(&connection)
        .await
        .unwrap();
    let mut direct = request(worker_id);
    direct.sender = Some(coordinator);
    actors.send(direct.clone()).await.unwrap();
    let inbox_before = actors.mailbox(coordinator).await.unwrap().len();
    sqlx::raw_sql("CREATE TRIGGER fail_cancel_reply BEFORE INSERT ON agent_mailbox WHEN NEW.sender IS NOT NULL BEGIN SELECT RAISE(ABORT,'reply rollback'); END;").execute(&connection).await.unwrap();
    assert!(actors.cancel_task(direct.id).await.is_err());
    assert_eq!(
        actors.task(direct.id).await.unwrap().status,
        AgentTaskStatus::Queued
    );
    assert_eq!(
        actors.mailbox(coordinator).await.unwrap().len(),
        inbox_before
    );
    assert_eq!(
        actors
            .mailbox(worker_id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .status,
        MessageStatus::Queued
    );
    sqlx::raw_sql("DROP TRIGGER fail_cancel_reply;")
        .execute(&connection)
        .await
        .unwrap();
    actors.cancel_task(direct.id).await.unwrap();
    actors.cancel_task(direct.id).await.unwrap();
    assert_eq!(
        actors.mailbox(coordinator).await.unwrap().len(),
        inbox_before + 1
    );
    actors.cancel_task(b.id).await.unwrap();
    actors.cancel_task(a.id).await.unwrap();
    drain(&mut worker).await;
    assert_eq!(
        actors.task(a.id).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
    assert_eq!(
        actors.task(b.id).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
    connection.close().await;
}

#[test]
fn cli_task_handles_inspect_list_and_cancel_without_starting_a_run() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cli.db");
    let run = |args: &[&str]| {
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
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let config = examples.join("configs/actor-worker.json");
    let input = examples.join("configs/actor-task.json");
    let agent = run(&[
        "actor",
        "create",
        "workflow_worker",
        "--config-file",
        config.to_str().unwrap(),
    ])["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let id = run(&[
        "actor",
        "send",
        &agent,
        "--input-file",
        input.to_str().unwrap(),
    ])["message_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(run(&["actor", "task", "status", &id])["status"], "queued");
    assert_eq!(
        run(&["actor", "task", "cancel", &id])["status"],
        "cancelled"
    );
    run(&["worker", "run", "--until-idle"]);
    let tasks = run(&["actor", "task", "list", &agent]);
    assert_eq!(tasks.as_array().unwrap().len(), 1);
    assert_eq!(tasks[0]["status"], "cancelled");
    assert_eq!(tasks[0]["admitted_attempts"], 0);
    assert_eq!(tasks[0]["run_id"], Value::Null);
}

#[tokio::test]
#[ignore = "subprocess actor task cancellation fixture"]
async fn actor_task_crash_child() {
    let Ok(db) = std::env::var("ACTOR_TASK_CRASH_DB") else {
        return;
    };
    let id: MessageId = std::env::var("ACTOR_TASK_HANDLE").unwrap().parse().unwrap();
    let (engine, agents) = local_app(&db).await.unwrap();
    if std::env::var("ACTOR_TASK_BOUNDARY").unwrap() == "active" {
        let run_id = agents
            .actors()
            .unwrap()
            .task(id)
            .await
            .unwrap()
            .run_id
            .unwrap();
        let store = SqliteStore::open(&db).await.unwrap();
        store
            .mutate(run_id, Box::new(|run| run.admit(run.record().updated_at)))
            .await
            .unwrap()
            .unwrap();
    }
    let receipt = agents.actors().unwrap().cancel_task(id).await.unwrap();
    fs::write(
        std::env::var("ACTOR_TASK_MARKER").unwrap(),
        serde_json::to_vec(&receipt).unwrap(),
    )
    .unwrap();
    let _keep_alive = engine;
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_cancellation_processes_preserve_queued_and_admitted_task_decisions() {
    for boundary in ["queued", "active"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("kill.db");
        let marker = dir.path().join("receipt.json");
        let (engine, agents) = local_app(&db).await.unwrap();
        let actors = agents.actors().unwrap();
        let (coordinator, worker_id) = pair(actors).await;
        let messages: Vec<_> = (0..3).map(|_| request(coordinator)).collect();
        for message in &messages {
            actors.send(message.clone()).await.unwrap();
        }
        let mut worker = engine.worker().await.unwrap();
        turns(&mut worker).await;
        let id = messages[usize::from(boundary == "queued")].id;
        drop(worker);
        drop(agents);
        drop(engine);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "actor_task_crash_child",
                "--nocapture",
            ])
            .env("ACTOR_TASK_CRASH_DB", &db)
            .env("ACTOR_TASK_HANDLE", id.to_string())
            .env("ACTOR_TASK_BOUNDARY", boundary)
            .env("ACTOR_TASK_MARKER", &marker)
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
            assert!(child.try_wait().unwrap().is_none());
            if Instant::now() > deadline {
                child.kill().unwrap();
                panic!("cancellation fixture timeout");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value = serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let actors = agents.actors().unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let receipt = actors.task(id).await.unwrap();
        assert_eq!(receipt.status, AgentTaskStatus::Cancelled);
        assert_eq!(json!(receipt.run_id), saved["run_id"]);
        assert_eq!(json!(receipt.admitted_attempts), saved["admitted_attempts"]);
        assert_eq!(
            actors.runs(worker_id).await.unwrap().len(),
            if boundary == "queued" { 2 } else { 3 }
        );
        assert_eq!(
            actors.task(messages[2].id).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
    }
}
