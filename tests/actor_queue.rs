use act0r::{
    Error, Worker,
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
    sync::Arc,
    time::{Duration, Instant},
};

fn request(recipient: AgentId) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":"Capacity test"}),
    }
}
fn config() -> Value {
    json!({"workflow":{"name":"demo","version":1},"input":{"message":"Capacity test"},"attempt_limit":10})
}
async fn actor(actors: &ActorService, capacity: u32) -> AgentId {
    actors
        .create_with_queue_policy(
            AgentId::new(),
            DefinitionId::new("workflow_worker", 1),
            config(),
            AgentQueuePolicy {
                task_capacity: capacity,
            },
        )
        .await
        .unwrap()
}
async fn coordinator(actors: &ActorService, worker: AgentId, capacity: u32) -> AgentId {
    actors
        .create_with_queue_policy(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":worker}),
            AgentQueuePolicy {
                task_capacity: capacity,
            },
        )
        .await
        .unwrap()
}
async fn turns(worker: &mut Worker) {
    for _ in 0..150 {
        if !worker.advance_work().await.unwrap() {
            return;
        }
    }
    panic!("mailboxes did not quiesce");
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
async fn concurrent_admission_is_bounded_duplicates_are_free_and_cancelled_slots_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("admission.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let id = actor(actors, 2).await;
    actors.pause(id).await.unwrap();
    let mut sends = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let actors = actors.clone();
        let message = request(id);
        sends.spawn(async move {
            let result = actors.send(message.clone()).await;
            (message, result)
        });
    }
    let mut accepted = vec![];
    let mut rejected = vec![];
    while let Some(result) = sends.join_next().await {
        let (message, result) = result.unwrap();
        match result {
            Ok(handle) => {
                assert_eq!(handle, message.id);
                accepted.push(message);
            }
            Err(Error::QueueFull {
                agent_id,
                capacity: 2,
                outstanding: 2,
                requested: 1,
            }) => {
                assert_eq!(agent_id, id);
                rejected.push(message);
            }
            other => panic!("unexpected admission: {other:?}"),
        }
    }
    assert_eq!(accepted.len(), 2);
    assert_eq!(rejected.len(), 14);
    assert_eq!(actors.mailbox(id).await.unwrap().len(), 2);
    assert_eq!(actors.tasks(id).await.unwrap().len(), 2);
    assert!(actors.runs(id).await.unwrap().is_empty());
    assert!(matches!(
        actors.task(rejected[0].id).await,
        Err(Error::AgentTaskNotFound(_))
    ));
    actors.send(accepted[0].clone()).await.unwrap();
    let mut conflict = accepted[0].clone();
    conflict.payload = json!("conflict");
    assert!(matches!(
        actors.send(conflict).await,
        Err(Error::Domain(DomainError::ConflictingMessage))
    ));
    actors.cancel_task(accepted[0].id).await.unwrap();
    let queue = actors.queue(id).await.unwrap();
    assert_eq!(
        (queue.outstanding, queue.available, queue.mailbox_tasks),
        (1, 1, 1)
    );
    let snapshot = json!(queue);
    drop(agents);
    drop(engine);
    let (_engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    assert_eq!(json!(actors.queue(id).await.unwrap()), snapshot);
    // A failed admission saved no receipt, so its original ID can be retried.
    actors.send(rejected[0].clone()).await.unwrap();
    actors.send(accepted[0].clone()).await.unwrap(); // Terminal redelivery also reserves nothing.
    assert_eq!(actors.queue(id).await.unwrap().outstanding, 2);

    actors.cancel_task(accepted[1].id).await.unwrap();
    let connection = pool(&db).await;
    sqlx::query("CREATE TRIGGER reject_task_insert BEFORE INSERT ON agent_tasks BEGIN SELECT RAISE(ABORT,'task insert failure'); END").execute(&connection).await.unwrap();
    let retry = request(id);
    assert!(actors.send(retry.clone()).await.is_err());
    assert_eq!(actors.queue(id).await.unwrap().outstanding, 1);
    assert!(
        !actors
            .mailbox(id)
            .await
            .unwrap()
            .iter()
            .any(|m| m.message.id == retry.id)
    );
    sqlx::query("DROP TRIGGER reject_task_insert")
        .execute(&connection)
        .await
        .unwrap();
    actors.send(retry).await.unwrap();
    assert_eq!(actors.queue(id).await.unwrap().available, 0);
    connection.close().await;
}

#[tokio::test]
async fn consumed_pending_work_and_cooperative_cancellation_retain_capacity_until_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("pending.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let worker_id = actor(actors, 2).await;
    let coordinator = coordinator(actors, worker_id, 2).await;
    let a = request(coordinator);
    let b = request(coordinator);
    let c = request(coordinator);
    actors.send(a.clone()).await.unwrap();
    actors.send(b.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    turns(&mut worker).await;
    for id in [worker_id, coordinator] {
        let queue = actors.queue(id).await.unwrap();
        assert_eq!(
            (
                queue.outstanding,
                queue.mailbox_tasks,
                queue.pending_tasks,
                queue.executing_tasks,
                queue.available
            ),
            (2, 0, 1, 1, 0)
        );
    }
    assert!(matches!(
        actors.send(c.clone()).await,
        Err(Error::QueueFull { .. })
    ));
    actors.cancel_task(b.id).await.unwrap();
    assert_eq!(actors.queue(worker_id).await.unwrap().available, 1);
    assert_eq!(actors.queue(coordinator).await.unwrap().available, 1);
    actors.send(c.clone()).await.unwrap();
    turns(&mut worker).await;
    let run = actors.task(a.id).await.unwrap().run_id.unwrap();
    let store = SqliteStore::open(&db).await.unwrap();
    store
        .mutate(run, Box::new(|run| run.admit(run.record().updated_at)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        actors.cancel_task(a.id).await.unwrap().status,
        AgentTaskStatus::Cancelling
    );
    for id in [worker_id, coordinator] {
        assert_eq!(actors.queue(id).await.unwrap().outstanding, 2);
    }
    drop(store);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    assert_eq!(
        actors.task(a.id).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
    assert_eq!(actors.task(a.id).await.unwrap().admitted_attempts, 1);
    assert_eq!(
        actors.task(c.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    for id in [worker_id, coordinator] {
        assert_eq!(actors.queue(id).await.unwrap().available, 2);
    }
    assert_eq!(actors.runs(worker_id).await.unwrap().len(), 2);
}

#[tokio::test]
async fn terminal_runs_free_capacity_before_notification_and_replies_flow_while_full() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("completion.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let worker_id = actor(actors, 1).await;
    let coordinator = coordinator(actors, worker_id, 1).await;
    let a = request(coordinator);
    actors.send(a.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    turns(&mut worker).await;
    let run_id = actors.task(a.id).await.unwrap().run_id.unwrap();
    let store = SqliteStore::open(&db).await.unwrap();
    store
        .mutate(
            run_id,
            Box::new(|run| {
                run.cancel(run.record().updated_at)?;
                Ok(None)
            }),
        )
        .await
        .unwrap();
    assert!(!actors.runs(worker_id).await.unwrap()[0].notified);
    for id in [worker_id, coordinator] {
        assert_eq!(actors.queue(id).await.unwrap().available, 1);
    }
    let b = request(coordinator);
    actors.send(b.clone()).await.unwrap();
    // Notify A, then forward B, then deliver A's completion and reply with both queues full.
    assert!(worker.advance_work().await.unwrap());
    assert!(worker.advance_work().await.unwrap());
    for id in [worker_id, coordinator] {
        assert_eq!(actors.queue(id).await.unwrap().available, 0);
    }
    turns(&mut worker).await;
    assert_eq!(
        actors.status(coordinator).await.unwrap().record().state["jobs"][0]["result"]["status"],
        "cancelled"
    );
    drain(&mut worker).await;
    assert_eq!(
        actors.task(b.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(actors.queue(coordinator).await.unwrap().outstanding, 0);
    assert_eq!(actors.queue(worker_id).await.unwrap().outstanding, 0);
}

#[tokio::test]
async fn saturated_delegation_saves_rejection_without_state_changes_children_or_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("delegation.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let worker_id = actor(actors, 1).await;
    actors.pause(worker_id).await.unwrap();
    let existing = request(worker_id);
    actors.send(existing.clone()).await.unwrap();
    let coordinator = coordinator(actors, worker_id, 1).await;
    let forwarded = request(coordinator);
    actors.send(forwarded.clone()).await.unwrap();
    let before = actors
        .status(coordinator)
        .await
        .unwrap()
        .record()
        .state
        .clone();
    let connection = pool(&db).await;
    sqlx::query("CREATE TRIGGER reject_task_update BEFORE UPDATE ON agent_tasks BEGIN SELECT RAISE(ABORT,'rejection save failure'); END").execute(&connection).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.advance_work().await.is_err());
    assert_eq!(
        actors.task(forwarded.id).await.unwrap().status,
        AgentTaskStatus::Queued
    );
    assert_eq!(actors.queue(coordinator).await.unwrap().outstanding, 1);
    assert_eq!(
        actors.status(coordinator).await.unwrap().record().state,
        before
    );
    sqlx::query("DROP TRIGGER reject_task_update")
        .execute(&connection)
        .await
        .unwrap();
    turns(&mut worker).await;
    let rejected = actors.task(forwarded.id).await.unwrap();
    assert_eq!(rejected.status, AgentTaskStatus::Rejected);
    let reply = rejected.reply.unwrap();
    assert_eq!(reply["admission"]["code"], "queue_full");
    assert_eq!(reply["admission"]["agent_id"], json!(worker_id));
    assert_eq!(reply["admission"]["capacity"], 1);
    assert!(rejected.run_id.is_none());
    assert_eq!(
        actors.status(coordinator).await.unwrap().record().state,
        before
    );
    assert_eq!(actors.mailbox(worker_id).await.unwrap().len(), 1);
    assert_eq!(actors.tasks(worker_id).await.unwrap().len(), 1);
    assert_eq!(actors.queue(coordinator).await.unwrap().available, 1);
    actors.send(forwarded.clone()).await.unwrap();
    assert!(!worker.advance_work().await.unwrap());
    actors.cancel_task(existing.id).await.unwrap();
    actors.resume(worker_id).await.unwrap();
    let fresh = request(coordinator);
    actors.send(fresh.clone()).await.unwrap();
    drain(&mut worker).await;
    assert_eq!(
        actors.task(fresh.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(actors.runs(worker_id).await.unwrap().len(), 1);
    connection.close().await;
}

struct BatchSender;
impl AgentDefinition for BatchSender {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("batch_sender", 1)
    }
    fn initialize(&self, _config: &Value) -> Result<Value, DomainError> {
        Ok(json!({"emitted":0}))
    }
    fn transition(
        &self,
        config: &Value,
        _state: &Value,
        message: &AgentMessage,
    ) -> Result<AgentTransition, DomainError> {
        let recipients: Vec<AgentId> = serde_json::from_value(config.clone()).unwrap();
        Ok(AgentTransition {
            state: json!({"emitted":recipients.len()}),
            commands: recipients
                .into_iter()
                .map(|recipient| AgentCommand::Send {
                    recipient,
                    correlation_id: message.correlation(),
                    kind: "task".into(),
                    payload: json!({}),
                })
                .collect(),
        })
    }
}

#[tokio::test]
async fn outgoing_batches_reserve_capacity_as_a_unit_and_do_not_partially_send() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("batch.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let target = actor(agents.actors().unwrap(), 1).await;
    let free = actor(agents.actors().unwrap(), 1).await;
    let store = Arc::new(SqliteStore::open(&db).await.unwrap());
    let actors = ActorService::new(engine, store, vec![Arc::new(BatchSender)]).unwrap();
    let source = actors
        .create(
            AgentId::new(),
            DefinitionId::new("batch_sender", 1),
            json!([free, target, target]),
        )
        .await
        .unwrap();
    let mut message = request(source);
    message.kind = "broadcast".into();
    actors.send(message).await.unwrap();
    assert!(actors.advance().await.unwrap());
    assert_eq!(
        actors.status(source).await.unwrap().record().state,
        json!({"emitted":0})
    );
    assert_eq!(
        actors.mailbox(source).await.unwrap()[0].status,
        MessageStatus::Rejected
    );
    for id in [free, target] {
        assert!(actors.mailbox(id).await.unwrap().is_empty());
        assert!(actors.tasks(id).await.unwrap().is_empty());
        assert_eq!(actors.queue(id).await.unwrap().available, 1);
    }
}

#[tokio::test]
async fn policy_is_pinned_and_missing_policy_actors_drain_overfull_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("minimal.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    assert!(
        actors
            .create_with_queue_policy(
                AgentId::new(),
                DefinitionId::new("workflow_worker", 1),
                config(),
                AgentQueuePolicy { task_capacity: 0 }
            )
            .await
            .is_err()
    );
    let id = actor(actors, 66).await;
    actors
        .create_with_queue_policy(
            id,
            DefinitionId::new("workflow_worker", 1),
            config(),
            AgentQueuePolicy { task_capacity: 66 },
        )
        .await
        .unwrap();
    assert!(
        actors
            .create(id, DefinitionId::new("workflow_worker", 1), config())
            .await
            .is_err()
    );
    let messages: Vec<_> = (0..66).map(|_| request(id)).collect();
    for message in &messages {
        actors.send(message.clone()).await.unwrap();
    }
    assert!(matches!(
        actors.send(request(id)).await,
        Err(Error::QueueFull {
            capacity: 66,
            outstanding: 66,
            ..
        })
    ));
    // Configured capacities above 65 replace the old behavior's hardcoded pending limit.
    let mut worker = engine.worker().await.unwrap();
    turns(&mut worker).await;
    assert_eq!(
        actors.status(id).await.unwrap().record().state["pending"]
            .as_array()
            .unwrap()
            .len(),
        65
    );
    assert_eq!(actors.queue(id).await.unwrap().pending_tasks, 65);
    let connection = pool(&db).await;
    sqlx::query("UPDATE agent_actors SET payload=json_remove(payload,'$.queue_policy') WHERE id=?")
        .bind(id.to_string())
        .execute(&connection)
        .await
        .unwrap();
    let before: String = sqlx::query_scalar("SELECT payload FROM agent_actors WHERE id=?")
        .bind(id.to_string())
        .fetch_one(&connection)
        .await
        .unwrap();
    connection.close().await;
    drop(worker);
    drop(agents);
    drop(engine);
    let (_engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let queue = actors.queue(id).await.unwrap();
    assert_eq!(
        (queue.capacity, queue.outstanding, queue.available),
        (DEFAULT_TASK_CAPACITY, 66, 0)
    );
    assert!(matches!(
        actors.send(request(id)).await,
        Err(Error::QueueFull {
            capacity: 65,
            outstanding: 66,
            ..
        })
    ));
    let connection = pool(&db).await;
    let after: String = sqlx::query_scalar("SELECT payload FROM agent_actors WHERE id=?")
        .bind(id.to_string())
        .fetch_one(&connection)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&connection)
            .await
            .unwrap(),
        62
    );
    assert!(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='agent_tasks_open_agent')").fetch_one(&connection).await.unwrap());
    assert_eq!(before, after); // Reading old payloads does not rewrite pinned state.
    actors
        .create(id, DefinitionId::new("workflow_worker", 1), config())
        .await
        .unwrap();
    actors.cancel_task(messages[1].id).await.unwrap();
    assert_eq!(actors.queue(id).await.unwrap().available, 0);
    actors.cancel_task(messages[2].id).await.unwrap();
    assert_eq!(actors.queue(id).await.unwrap().available, 1);
    actors.send(request(id)).await.unwrap();
    assert_eq!(actors.queue(id).await.unwrap().outstanding, 65);
    connection.close().await;
}

#[test]
fn cli_sets_capacity_reports_depth_and_returns_queue_full_without_accepting_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cli.db");
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_act0r"))
            .arg("--db")
            .arg(&db)
            .arg("--json")
            .args(args)
            .output()
            .unwrap()
    };
    let success = |args: &[&str]| {
        let output = run(args);
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
    assert!(
        !run(&[
            "actor",
            "create",
            "workflow_worker",
            "--config-file",
            config.to_str().unwrap(),
            "--task-capacity",
            "0"
        ])
        .status
        .success()
    );
    let agent = success(&[
        "actor",
        "create",
        "workflow_worker",
        "--config-file",
        config.to_str().unwrap(),
        "--task-capacity",
        "1",
    ])["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let message = success(&[
        "actor",
        "send",
        &agent,
        "--input-file",
        input.to_str().unwrap(),
    ])["message_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let queue = success(&["actor", "status", &agent])["queue"].clone();
    assert_eq!(queue["capacity"], 1);
    assert_eq!(queue["outstanding"], 1);
    assert_eq!(queue["mailbox_tasks"], 1);
    assert_eq!(queue["available"], 0);
    let full = run(&[
        "actor",
        "send",
        &agent,
        "--input-file",
        input.to_str().unwrap(),
    ]);
    assert!(!full.status.success());
    assert!(String::from_utf8_lossy(&full.stderr).contains("QueueFull"));
    assert_eq!(
        success(&["actor", "inbox", &agent])
            .as_array()
            .unwrap()
            .len(),
        1
    );
    success(&["actor", "task", "cancel", &message]);
    assert_eq!(
        success(&["actor", "status", &agent])["queue"]["available"],
        1
    );
    let source = examples.join("benchmark-cases/addition/source");
    let config = examples.join("configs/agent-scripted.json");
    let coding = success(&[
        "actor",
        "coding-worker",
        "--source",
        source.to_str().unwrap(),
        "--config-file",
        config.to_str().unwrap(),
        "--task-capacity",
        "3",
    ])["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        success(&["actor", "status", &coding])["queue"]["capacity"],
        3
    );
}

#[tokio::test]
#[ignore = "subprocess admission and capacity-release fixture"]
async fn actor_queue_crash_child() {
    let Ok(db) = std::env::var("ACTOR_QUEUE_DB") else {
        return;
    };
    let (engine, agents) = local_app(db).await.unwrap();
    let message: AgentMessage =
        serde_json::from_str(&std::env::var("ACTOR_QUEUE_MESSAGE").unwrap()).unwrap();
    let actors = agents.actors().unwrap();
    if std::env::var("ACTOR_QUEUE_BOUNDARY").unwrap() == "admission" {
        actors.send(message.clone()).await.unwrap();
    } else {
        actors.cancel_task(message.id).await.unwrap();
    }
    fs::write(
        std::env::var("ACTOR_QUEUE_MARKER").unwrap(),
        serde_json::to_vec(&actors.queue(message.recipient).await.unwrap()).unwrap(),
    )
    .unwrap();
    let _keep_alive = engine;
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn killed_admission_and_cancellation_processes_preserve_capacity_and_handles() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("kill.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let id = actor(agents.actors().unwrap(), 1).await;
    let message = request(id);
    drop(agents);
    drop(engine);
    for boundary in ["admission", "cancellation"] {
        let marker = dir.path().join(format!("{boundary}.json"));
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "actor_queue_crash_child",
                "--nocapture",
            ])
            .env("ACTOR_QUEUE_DB", &db)
            .env(
                "ACTOR_QUEUE_MESSAGE",
                serde_json::to_string(&message).unwrap(),
            )
            .env("ACTOR_QUEUE_BOUNDARY", boundary)
            .env("ACTOR_QUEUE_MARKER", &marker)
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
                panic!("queue fixture timeout");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value = serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let actors = agents.actors().unwrap();
        assert_eq!(json!(actors.queue(id).await.unwrap()), saved);
        actors.send(message.clone()).await.unwrap();
        if boundary == "admission" {
            assert!(matches!(
                actors.send(request(id)).await,
                Err(Error::QueueFull { .. })
            ));
            assert_eq!(
                actors.task(message.id).await.unwrap().status,
                AgentTaskStatus::Queued
            );
        } else {
            assert_eq!(
                actors.task(message.id).await.unwrap().status,
                AgentTaskStatus::Cancelled
            );
            actors.send(request(id)).await.unwrap();
            assert_eq!(actors.queue(id).await.unwrap().outstanding, 1);
        }
        drop(agents);
        drop(engine);
    }
}
