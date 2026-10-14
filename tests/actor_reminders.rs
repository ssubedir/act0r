use act0r::{
    Engine, Error, Worker,
    adapters::{
        agent_executor::AgentExecutor,
        agents::AnswerAgentAdapter,
        demo::DemoExecutor,
        file_lock::FileOwnership,
        model::ModelRouter,
        sqlite::SqliteStore,
        tokio_worker::{self, WorkerOptions},
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{ActorService, AgentRegistry, AgentService, ports::*},
    domain::{actors::*, agent::*, *},
};
use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Command as ProcessCommand, Stdio},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

struct ManualClock(AtomicI64);
impl ManualClock {
    fn new(now: i64) -> Arc<Self> {
        Arc::new(Self(AtomicI64::new(now)))
    }
    fn set(&self, now: i64) {
        self.0.store(now, Ordering::SeqCst);
    }
}
impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}
struct App {
    engine: Engine,
    agents: AgentService,
    store: Arc<SqliteStore>,
    clock: Arc<ManualClock>,
}
async fn open(db: &Path, clock: Arc<ManualClock>) -> App {
    open_models(db, clock, Arc::new(ModelRouter::new(None).unwrap())).await
}
async fn open_models(db: &Path, clock: Arc<ManualClock>, models: Arc<dyn ModelClient>) -> App {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let root = db.parent().unwrap().join(".act0r/workspaces");
    let workspaces = Arc::new(LocalWorkspaces::new(root.clone()));
    let registry = Arc::new(AgentRegistry::new(vec![Arc::new(AnswerAgentAdapter)]).unwrap());
    let engine = Engine::new(
        store.clone(),
        Arc::new(AgentExecutor::new(
            models,
            Arc::new(LocalTools::new(root)),
            Arc::new(DemoExecutor),
        )),
        clock.clone(),
        Arc::new(FileOwnership::new(db)),
        registry.workflows(),
    )
    .unwrap();
    let actors = Arc::new(
        ActorService::new(
            engine.clone(),
            store.clone(),
            vec![
                Arc::new(RegisteredAgentDefinition),
                Arc::new(CoordinatorAgent),
            ],
        )
        .unwrap()
        .with_agent_registry(registry.clone()),
    );
    let engine = engine.with_coordinator(actors.clone());
    let agents = AgentService::new(engine.clone(), workspaces)
        .with_actors(actors)
        .with_agent_registry(registry);
    App {
        engine,
        agents,
        store,
        clock,
    }
}
fn config(human: bool) -> Value {
    json!({"goal":"Answer the task","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,"memory":{},
        "script":if human{json!([{"kind":"ask_human","question":"Confirm?"},{"kind":"finish","summary":"Remember blue."}])}else{json!([{"kind":"finish","summary":"Remember blue."}])}})
}
async fn create(app: &App, human: bool, capacity: u32) -> AgentId {
    app.agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            config(human),
            20,
            AgentQueuePolicy {
                task_capacity: capacity,
            },
        )
        .await
        .unwrap()
}
fn task(actor: AgentId, goal: &str) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient: actor,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":goal}),
    }
}
async fn drain(worker: &mut Worker) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline, "reminder fixture did not settle");
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
async fn run_for(app: &App, id: MessageId) -> Run {
    app.engine
        .status(
            app.agents
                .actors()
                .unwrap()
                .task(id)
                .await
                .unwrap()
                .run_id
                .unwrap(),
        )
        .await
        .unwrap()
}
async fn connection(db: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(db)
            .foreign_keys(true),
    )
    .await
    .unwrap()
}
async fn sql(db: &Path, statement: &'static str) {
    let conn = connection(db).await;
    sqlx::raw_sql(statement).execute(&conn).await.unwrap();
    conn.close().await;
}
async fn snapshot(app: &App, actor: AgentId) -> Value {
    let actors = app.agents.actors().unwrap();
    json!({"actor":actors.status(actor).await.unwrap(),"reminders":actors.reminders(actor).await.unwrap(),"mailbox":actors.mailbox(actor).await.unwrap(),"tasks":actors.tasks(actor).await.unwrap(),"runs":actors.runs(actor).await.unwrap()})
}

#[tokio::test]
async fn future_task_fires_once_after_restart_using_saved_actor_memory() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("restart.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let actor = create(&app, false, 2).await;
    let actors = app.agents.actors().unwrap();
    let seed = task(actor, "Remember blue");
    actors.send(seed).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    drain(&mut worker).await;
    let memory = actors.memory(actor).await.unwrap().unwrap();
    let second = task(actor, "Recall my preference");
    let reminder = actors
        .schedule_task(second.clone(), TimerDeadline::After { milliseconds: 500 })
        .await
        .unwrap();
    assert_eq!(reminder.due_at, 1500);
    assert!(matches!(
        actors.task(second.id).await,
        Err(Error::AgentTaskNotFound(_))
    ));
    assert_eq!(actors.queue(actor).await.unwrap().outstanding, 0);
    assert!(worker.has_active_runs().await.unwrap());
    app.clock.set(1499);
    assert!(!worker.step().await.unwrap());
    assert_eq!(actors.runs(actor).await.unwrap().len(), 1);
    drop(worker);
    drop(app);
    let app = open(&db, ManualClock::new(3000)).await;
    let actors = app.agents.actors().unwrap();
    assert_eq!(actors.reminder(second.id).await.unwrap(), reminder);
    assert_eq!(actors.memory(actor).await.unwrap().unwrap(), memory);
    let mut worker = app.engine.worker().await.unwrap();
    drain(&mut worker).await;
    let delivered = actors.reminder(second.id).await.unwrap();
    assert_eq!(delivered.status, ReminderStatus::Published);
    assert_eq!(delivered.published_at, Some(3000));
    let run = run_for(&app, second.id).await;
    assert_eq!(
        run.record().input["input"]["actor_memory"],
        json!(memory.snapshot(actor))
    );
    assert_eq!(
        actors
            .mailbox(actor)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.message.id == second.id)
            .count(),
        1
    );
    assert!(
        actors
            .status(actor)
            .await
            .unwrap()
            .record()
            .pending_reminders
            .is_empty()
    );
    assert_eq!(actors.runs(actor).await.unwrap().len(), 2);
    assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 2);
    assert!(!worker.step().await.unwrap());
    assert_eq!(actors.cancel_reminder(second.id).await.unwrap(), delivered);
    assert_eq!(
        actors
            .schedule_task(second, TimerDeadline::After { milliseconds: 500 })
            .await
            .unwrap(),
        delivered
    );
}

#[tokio::test]
async fn duplicate_registration_preserves_deadlines_and_reserved_ids_cannot_send_early_or_rearm() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("identity.db"), ManualClock::new(1000)).await;
    let actor = create(&app, false, 1).await;
    let actors = app.agents.actors().unwrap();
    let message = task(actor, "Later");
    let (a, b) = tokio::join!(
        actors.schedule_task(message.clone(), TimerDeadline::After { milliseconds: 500 }),
        actors.schedule_task(message.clone(), TimerDeadline::After { milliseconds: 500 })
    );
    assert_eq!(a.unwrap(), b.unwrap());
    let saved = actors.reminder(message.id).await.unwrap();
    let before = snapshot(&app, actor).await;
    app.clock.set(i64::MAX);
    assert_eq!(
        actors
            .schedule_task(message.clone(), TimerDeadline::After { milliseconds: 500 })
            .await
            .unwrap(),
        saved
    );
    assert_eq!(before, snapshot(&app, actor).await);
    assert!(matches!(
        actors.send(message.clone()).await,
        Err(Error::Domain(DomainError::ReservedMessageId))
    ));
    assert!(matches!(
        actors
            .schedule_task(message.clone(), TimerDeadline::At { unix_ms: 1500 })
            .await,
        Err(Error::Domain(DomainError::ConflictingMessage))
    ));
    let mut conflict = message.clone();
    conflict.payload = json!({"goal":"Changed"});
    assert!(matches!(
        actors
            .schedule_task(conflict, TimerDeadline::After { milliseconds: 500 })
            .await,
        Err(Error::Domain(DomainError::ConflictingMessage))
    ));
    let cancelled = actors.cancel_reminder(message.id).await.unwrap();
    assert_eq!(cancelled.status, ReminderStatus::Cancelled);
    assert_eq!(
        actors
            .schedule_task(message.clone(), TimerDeadline::After { milliseconds: 500 })
            .await
            .unwrap(),
        cancelled
    );
    assert_eq!(actors.cancel_reminder(message.id).await.unwrap(), cancelled);
    assert!(actors.send(message).await.is_err());
    assert!(
        !app.store
            .publish_due_actor_reminder(i64::MAX)
            .await
            .unwrap()
    );
    assert!(actors.tasks(actor).await.unwrap().is_empty());
    assert!(
        actors
            .status(actor)
            .await
            .unwrap()
            .record()
            .pending_reminders
            .is_empty()
    );
}

#[tokio::test]
async fn paused_and_blocked_actors_keep_overdue_reminders_until_resumed() {
    for blocked in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("controls.db");
        let app = open(&db, ManualClock::new(1000)).await;
        let actor = create(&app, false, 1).await;
        let actors = app.agents.actors().unwrap();
        actors.pause(actor).await.unwrap();
        let message = task(actor, "Wake after resume");
        actors
            .schedule_task(message.clone(), TimerDeadline::At { unix_ms: 1100 })
            .await
            .unwrap();
        if blocked {
            let conn = connection(&db).await;
            sqlx::query("UPDATE agent_actors SET status='blocked',payload=json_set(payload,'$.status','blocked','$.error','fixture block') WHERE id=?").bind(actor.to_string()).execute(&conn).await.unwrap();
            conn.close().await;
        }
        let mut worker = app.engine.worker().await.unwrap();
        app.clock.set(2000);
        assert!(!worker.has_active_runs().await.unwrap());
        assert!(!worker.advance_work().await.unwrap());
        assert_eq!(
            actors.reminder(message.id).await.unwrap().status,
            ReminderStatus::Scheduled
        );
        assert!(actors.mailbox(actor).await.unwrap().is_empty());
        let another = actors
            .schedule_task(
                task(actor, "Cancel while paused"),
                TimerDeadline::At { unix_ms: 1000 },
            )
            .await
            .unwrap();
        actors.cancel_reminder(another.id).await.unwrap();
        assert_eq!(
            actors.status(actor).await.unwrap().record().status,
            if blocked {
                AgentStatus::Blocked
            } else {
                AgentStatus::Paused
            }
        );
        actors.resume(actor).await.unwrap();
        drain(&mut worker).await;
        assert_eq!(
            actors.task(message.id).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        assert_eq!(
            actors.reminder(message.id).await.unwrap().published_at,
            Some(2000)
        );
    }
}

#[tokio::test]
async fn saturated_actor_keeps_due_work_without_blocking_other_actors_and_releases_on_completion() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("capacity.db"), ManualClock::new(1000)).await;
    let full = create(&app, true, 1).await;
    let ready = create(&app, false, 1).await;
    let actors = app.agents.actors().unwrap();
    let first = task(full, "Wait for clarification");
    actors.send(first.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    drain(&mut worker).await;
    let delayed = task(full, "Follow up");
    let other = task(ready, "Independent work");
    actors
        .schedule_task(delayed.clone(), TimerDeadline::At { unix_ms: 1100 })
        .await
        .unwrap();
    actors
        .schedule_task(other.clone(), TimerDeadline::At { unix_ms: 1200 })
        .await
        .unwrap();
    app.clock.set(2000);
    drain(&mut worker).await;
    assert_eq!(
        actors.reminder(delayed.id).await.unwrap().status,
        ReminderStatus::Scheduled
    );
    assert!(actors.task(delayed.id).await.is_err());
    assert_eq!(
        actors.task(other.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(actors.queue(full).await.unwrap().outstanding, 1);
    assert!(!worker.has_active_runs().await.unwrap());
    let waiting = run_for(&app, first.id).await;
    app.engine
        .signal(
            waiting.record().id,
            waiting.record().wait.as_ref().unwrap().id,
            json!("yes"),
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    assert_eq!(
        actors.reminder(delayed.id).await.unwrap().status,
        ReminderStatus::Published
    );
    let next = run_for(&app, delayed.id).await;
    assert_eq!(next.record().status, RunStatus::Waiting);
    assert_eq!(
        next.record().input["input"]["actor_memory"]["entries"][0]["goal"],
        "Wait for clarification"
    );
    app.engine
        .signal(
            next.record().id,
            next.record().wait.as_ref().unwrap().id,
            json!("yes"),
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    assert_eq!(actors.queue(full).await.unwrap().outstanding, 0);
}

#[tokio::test]
async fn schedule_cancel_and_publication_roll_back_actor_ids_receipts_mailbox_and_task_handles() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let actor = create(&app, false, 1).await;
    let actors = app.agents.actors().unwrap();
    let message = task(actor, "Atomic publication");
    let before = snapshot(&app, actor).await;
    for trigger in [
        "CREATE TRIGGER reject_reminder BEFORE INSERT ON agent_reminders BEGIN SELECT RAISE(ABORT,'register rollback'); END",
        "CREATE TRIGGER reject_reminder BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'actor rollback'); END",
    ] {
        sql(&db, trigger).await;
        assert!(
            actors
                .schedule_task(message.clone(), TimerDeadline::At { unix_ms: 1000 })
                .await
                .is_err()
        );
        assert_eq!(before, snapshot(&app, actor).await);
        sql(&db, "DROP TRIGGER reject_reminder").await;
    }
    actors
        .schedule_task(message.clone(), TimerDeadline::At { unix_ms: 1000 })
        .await
        .unwrap();
    let registered = snapshot(&app, actor).await;
    for trigger in [
        "CREATE TRIGGER reject_reminder BEFORE INSERT ON agent_mailbox BEGIN SELECT RAISE(ABORT,'mailbox rollback'); END",
        "CREATE TRIGGER reject_reminder BEFORE INSERT ON agent_tasks BEGIN SELECT RAISE(ABORT,'task rollback'); END",
        "CREATE TRIGGER reject_reminder BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'actor rollback'); END",
        "CREATE TRIGGER reject_reminder BEFORE UPDATE ON agent_reminders BEGIN SELECT RAISE(ABORT,'receipt rollback'); END",
    ] {
        sql(&db, trigger).await;
        assert!(app.store.publish_due_actor_reminder(1000).await.is_err());
        assert_eq!(registered, snapshot(&app, actor).await);
        sql(&db, "DROP TRIGGER reject_reminder").await;
    }
    for trigger in [
        "CREATE TRIGGER reject_reminder BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'cancel actor rollback'); END",
        "CREATE TRIGGER reject_reminder BEFORE UPDATE ON agent_reminders BEGIN SELECT RAISE(ABORT,'cancel receipt rollback'); END",
    ] {
        sql(&db, trigger).await;
        assert!(actors.cancel_reminder(message.id).await.is_err());
        assert_eq!(registered, snapshot(&app, actor).await);
        sql(&db, "DROP TRIGGER reject_reminder").await;
    }
    assert!(app.store.publish_due_actor_reminder(1000).await.unwrap());
    assert_eq!(actors.mailbox(actor).await.unwrap().len(), 1);
    assert_eq!(actors.tasks(actor).await.unwrap().len(), 1);
    assert!(!app.store.publish_due_actor_reminder(1000).await.unwrap());
    assert_eq!(actors.send(message.clone()).await.unwrap(), message.id);
    let mut worker = app.engine.worker().await.unwrap();
    drain(&mut worker).await;
    assert_eq!(actors.runs(actor).await.unwrap().len(), 1);
}

#[tokio::test]
async fn cancellation_and_due_publication_races_have_one_persisted_winner() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("races.db"), ManualClock::new(1000)).await;
    let actor = create(&app, false, 16).await;
    let actors = app.agents.actors().unwrap();
    for order in 0..12 {
        let message = task(actor, "Race the deadline");
        actors
            .schedule_task(message.clone(), TimerDeadline::At { unix_ms: 1000 })
            .await
            .unwrap();
        if order % 2 == 0 {
            let (delivery, cancel) = tokio::join!(
                app.store.publish_due_actor_reminder(1000),
                actors.cancel_reminder(message.id)
            );
            delivery.unwrap();
            cancel.unwrap();
        } else {
            let (cancel, delivery) = tokio::join!(
                actors.cancel_reminder(message.id),
                app.store.publish_due_actor_reminder(1000)
            );
            cancel.unwrap();
            delivery.unwrap();
        }
        let reminder = actors.reminder(message.id).await.unwrap();
        match reminder.status {
            ReminderStatus::Cancelled => {
                assert!(actors.task(message.id).await.is_err());
                assert!(
                    actors
                        .mailbox(actor)
                        .await
                        .unwrap()
                        .iter()
                        .all(|e| e.message.id != message.id)
                );
            }
            ReminderStatus::Published => {
                assert_eq!(
                    actors
                        .mailbox(actor)
                        .await
                        .unwrap()
                        .iter()
                        .filter(|e| e.message.id == message.id)
                        .count(),
                    1
                );
                actors.cancel_task(message.id).await.unwrap();
                assert_eq!(
                    actors.task(message.id).await.unwrap().status,
                    AgentTaskStatus::Cancelled
                );
                assert_eq!(
                    actors.cancel_reminder(message.id).await.unwrap().status,
                    ReminderStatus::Published
                );
            }
            ReminderStatus::Scheduled => panic!("race left the reminder open"),
        }
        assert!(
            !actors
                .status(actor)
                .await
                .unwrap()
                .record()
                .pending_reminders
                .contains(&message.id)
        );
    }
    assert!(!app.store.publish_due_actor_reminder(1000).await.unwrap());
    assert!(actors.runs(actor).await.unwrap().is_empty());
}

#[tokio::test]
async fn validation_and_reminder_limit_do_not_reserve_task_slots_and_clear_keeps_pending_ids() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("limits.db"), ManualClock::new(1000)).await;
    let actor = create(&app, false, 1).await;
    let actors = app.agents.actors().unwrap();
    let before = snapshot(&app, actor).await;
    let mut wrong = task(actor, "Wrong kind");
    wrong.kind = "task_result".into();
    assert!(
        actors
            .schedule_task(wrong, TimerDeadline::At { unix_ms: 0 })
            .await
            .is_err()
    );
    let mut large = task(actor, "Too large");
    large.payload = json!("x".repeat(65537));
    assert!(
        actors
            .schedule_task(large, TimerDeadline::At { unix_ms: 0 })
            .await
            .is_err()
    );
    assert!(
        actors
            .schedule_task(
                task(actor, "Overflow"),
                TimerDeadline::After {
                    milliseconds: u64::MAX
                }
            )
            .await
            .is_err()
    );
    let mut sender = task(actor, "Missing sender");
    sender.sender = Some(AgentId::new());
    assert!(
        actors
            .schedule_task(sender, TimerDeadline::At { unix_ms: 0 })
            .await
            .is_err()
    );
    assert!(
        actors
            .schedule_task(
                task(AgentId::new(), "Missing target"),
                TimerDeadline::At { unix_ms: 0 }
            )
            .await
            .is_err()
    );
    assert_eq!(before, snapshot(&app, actor).await);
    let immediate = task(actor, "Existing ID");
    actors.send(immediate.clone()).await.unwrap();
    assert!(
        actors
            .schedule_task(immediate.clone(), TimerDeadline::At { unix_ms: 2000 })
            .await
            .is_err()
    );
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    let completion = actors.runs(actor).await.unwrap()[0].completion_message_id;
    let mut reserved = task(actor, "Runtime ID");
    reserved.id = completion;
    assert!(
        actors
            .schedule_task(reserved, TimerDeadline::At { unix_ms: 2000 })
            .await
            .is_err()
    );
    drain(&mut worker).await;
    let mut messages = vec![];
    for i in 0..MAX_PENDING_REMINDERS {
        let message = task(actor, &format!("Scheduled {i}"));
        actors
            .schedule_task(message.clone(), TimerDeadline::At { unix_ms: 2000 })
            .await
            .unwrap();
        messages.push(message);
    }
    assert_eq!(actors.queue(actor).await.unwrap().outstanding, 0);
    assert_eq!(
        actors
            .status(actor)
            .await
            .unwrap()
            .record()
            .pending_reminders
            .len(),
        MAX_PENDING_REMINDERS
    );
    assert!(
        actors
            .schedule_task(
                task(actor, "Over limit"),
                TimerDeadline::At { unix_ms: 2000 }
            )
            .await
            .is_err()
    );
    assert_eq!(
        actors
            .schedule_task(messages[0].clone(), TimerDeadline::At { unix_ms: 2000 })
            .await
            .unwrap()
            .id,
        messages[0].id
    );
    actors.clear_memory(actor).await.unwrap();
    assert_eq!(
        actors
            .status(actor)
            .await
            .unwrap()
            .record()
            .pending_reminders
            .len(),
        MAX_PENDING_REMINDERS
    );
    actors.cancel_reminder(messages[0].id).await.unwrap();
    actors
        .schedule_task(task(actor, "New slot"), TimerDeadline::At { unix_ms: 2000 })
        .await
        .unwrap();
    assert_eq!(
        actors.reminders(actor).await.unwrap().len(),
        MAX_PENDING_REMINDERS + 1
    );
}

struct GatedModels {
    inner: ModelRouter,
    started: Semaphore,
    release: Semaphore,
}
impl ModelClient for GatedModels {
    fn generate(&self, request: ModelRequest, id: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            if request.target.model == "gated" {
                self.started.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            self.inner.generate(request, id).await
        })
    }
}
#[tokio::test]
async fn tokio_worker_waits_for_future_deadlines_and_publishes_while_execution_capacity_is_full() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(1000);
    let models = Arc::new(GatedModels {
        inner: ModelRouter::new(None).unwrap(),
        started: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let app = open_models(&dir.path().join("worker.db"), clock.clone(), models.clone()).await;
    let actors = app.agents.actors().unwrap();
    let mut cfg = config(false);
    cfg["target"]["model"] = json!("gated");
    let slow = app
        .agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            cfg,
            20,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let fast = create(&app, false, 1).await;
    let original = task(slow, "Wait in external I/O");
    actors.send(original.clone()).await.unwrap();
    let future = task(fast, "Wake while capacity is full");
    actors
        .schedule_task(future.clone(), TimerDeadline::At { unix_ms: 1500 })
        .await
        .unwrap();
    let worker = app.engine.worker().await.unwrap();
    let handle = tokio::spawn(async move {
        tokio_worker::run(
            worker,
            WorkerOptions {
                until_idle: true,
                poll_interval: Duration::from_millis(2),
                max_concurrent_activities: 1,
                ..WorkerOptions::default()
            },
        )
        .await
        .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), models.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(!handle.is_finished());
    assert_eq!(
        actors.reminder(future.id).await.unwrap().status,
        ReminderStatus::Scheduled
    );
    clock.set(1500);
    tokio::time::timeout(Duration::from_secs(5), async {
        while actors.reminder(future.id).await.unwrap().status != ReminderStatus::Published {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        run_for(&app, original.id).await.record().status,
        RunStatus::Running
    );
    if let Some(run_id) = actors.task(future.id).await.unwrap().run_id {
        // Mailbox turns may create a Run at capacity; external I/O still waits.
        assert_eq!(
            app.engine
                .status(run_id)
                .await
                .unwrap()
                .record()
                .allowance
                .admitted,
            0
        );
    }
    models.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        actors.task(future.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    // With no active execution, --until-idle must still wait for a future eligible reminder.
    let another = task(fast, "Future idle wake");
    actors
        .schedule_task(another.clone(), TimerDeadline::At { unix_ms: 2000 })
        .await
        .unwrap();
    let worker = app.engine.worker().await.unwrap();
    let handle = tokio::spawn(async move {
        tokio_worker::run(
            worker,
            WorkerOptions {
                until_idle: true,
                poll_interval: Duration::from_millis(2),
                ..WorkerOptions::default()
            },
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!handle.is_finished());
    clock.set(2000);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        actors.task(another.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
}

#[tokio::test]
async fn reminder_can_target_a_coordinator_and_keep_correlated_delegation_replies() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("coordinator.db"), ManualClock::new(1000)).await;
    let worker = create(&app, false, 2).await;
    let actors = app.agents.actors().unwrap();
    let coordinator = actors
        .create(
            AgentId::new(),
            CoordinatorAgent.id(),
            json!({"worker":worker}),
        )
        .await
        .unwrap();
    let message = task(coordinator, "A scheduled delegated goal");
    actors
        .schedule_task(message.clone(), TimerDeadline::At { unix_ms: 1000 })
        .await
        .unwrap();
    let mut executor = app.engine.worker().await.unwrap();
    drain(&mut executor).await;
    let handle = actors.task(message.id).await.unwrap();
    assert_eq!(handle.status, AgentTaskStatus::Succeeded);
    assert_eq!(handle.execution_agent_id, Some(worker));
    assert_eq!(
        actors
            .mailbox(coordinator)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.message.kind == "task_result")
            .count(),
        1
    );
    assert_eq!(
        actors.reminder(message.id).await.unwrap().status,
        ReminderStatus::Published
    );
}

#[tokio::test]
async fn killed_workers_preserve_scheduled_published_cancelled_and_admitted_reminders() {
    for phase in ["scheduled", "published", "started", "inflight", "cancelled"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("kill.db");
        let ready = dir.path().join("ready.json");
        let log = std::fs::File::create(dir.path().join("child.log")).unwrap();
        let mut child = ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "reminder_kill_fixture",
                "--nocapture",
            ])
            .env("ACT0R_REMINDER_DB", &db)
            .env("ACT0R_REMINDER_READY", &ready)
            .env("ACT0R_REMINDER_PHASE", phase)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(40);
        while !ready.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!(
                    "reminder child exited {status}: {}",
                    std::fs::read_to_string(dir.path().join("child.log")).unwrap()
                );
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("reminder child timed out");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value = serde_json::from_slice(&std::fs::read(ready).unwrap()).unwrap();
        let actor: AgentId = serde_json::from_value(saved["actor"].clone()).unwrap();
        let id: MessageId = serde_json::from_value(saved["id"].clone()).unwrap();
        let clock = ManualClock::new(5000);
        let app = open(&db, clock.clone()).await;
        let actors = app.agents.actors().unwrap();
        let mut worker = app.engine.worker().await.unwrap();
        clock.set(6000);
        drain(&mut worker).await;
        assert!(
            actors
                .status(actor)
                .await
                .unwrap()
                .record()
                .pending_reminders
                .is_empty()
        );
        let reminder = actors.reminder(id).await.unwrap();
        assert_eq!(reminder.due_at, 1500);
        if phase == "cancelled" {
            assert_eq!(reminder.status, ReminderStatus::Cancelled);
            assert!(actors.task(id).await.is_err());
            assert_eq!(actors.runs(actor).await.unwrap().len(), 1);
        } else {
            assert_eq!(reminder.status, ReminderStatus::Published);
            assert_eq!(actors.runs(actor).await.unwrap().len(), 2);
            let run = run_for(&app, id).await;
            assert_eq!(run.record().status, RunStatus::Succeeded);
            assert_eq!(
                run.record().input["input"]["actor_memory"]["entries"][0]["goal"],
                "Remember before scheduling"
            );
            if phase == "started" || phase == "inflight" {
                assert_eq!(run.record().input, saved["input"]);
            }
            assert_eq!(
                run.record().usage.admitted_model_calls,
                if phase == "inflight" { 2 } else { 1 }
            );
            assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 2);
            assert_eq!(
                actors
                    .mailbox(actor)
                    .await
                    .unwrap()
                    .iter()
                    .filter(|entry| entry.message.id == id)
                    .count(),
                1
            );
        }
        assert!(!worker.step().await.unwrap());
    }
}

#[tokio::test]
#[ignore = "subprocess fixture controlled by the reminder recovery test"]
async fn reminder_kill_fixture() {
    let db = std::env::var("ACT0R_REMINDER_DB").unwrap();
    let ready = std::path::PathBuf::from(std::env::var("ACT0R_REMINDER_READY").unwrap());
    let phase = std::env::var("ACT0R_REMINDER_PHASE").unwrap();
    let app = open(Path::new(&db), ManualClock::new(1000)).await;
    let actor = create(&app, false, 2).await;
    let actors = app.agents.actors().unwrap();
    actors
        .send(task(actor, "Remember before scheduling"))
        .await
        .unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    drain(&mut worker).await;
    let message = task(actor, "Wake after downtime");
    actors
        .schedule_task(message.clone(), TimerDeadline::After { milliseconds: 500 })
        .await
        .unwrap();
    app.clock.set(2000);
    if phase == "cancelled" {
        actors.cancel_reminder(message.id).await.unwrap();
    } else if phase != "scheduled" {
        app.store.publish_due_actor_reminder(2000).await.unwrap();
    }
    if phase == "started" || phase == "inflight" {
        worker.advance_work().await.unwrap();
    }
    if phase == "inflight" {
        worker.admit().await.unwrap().unwrap();
    }
    let input = if phase == "started" || phase == "inflight" {
        run_for(&app, message.id).await.record().input.clone()
    } else {
        Value::Null
    };
    let temporary = ready.with_extension("tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec(&json!({"actor":actor,"id":message.id,"input":input})).unwrap(),
    )
    .unwrap();
    std::fs::rename(temporary, ready).unwrap();
    std::future::pending::<()>().await;
}
