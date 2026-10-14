use act0r::{
    Engine, Worker,
    adapters::{
        agent_executor::AgentExecutor,
        agents::{AnswerAgentAdapter, PlannerAgentAdapter},
        demo::DemoExecutor,
        file_lock::FileOwnership,
        model::ModelRouter,
        sqlite::SqliteStore,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{ActorService, AgentRegistry, AgentService, ports::*},
    domain::{actors::*, agent::*, follow_up::*, *},
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

struct ManualClock(AtomicI64);
impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}
impl ManualClock {
    fn new(now: i64) -> Arc<Self> {
        Arc::new(Self(AtomicI64::new(now)))
    }
    fn set(&self, now: i64) {
        self.0.store(now, Ordering::SeqCst);
    }
}
struct Models(ModelRouter);
impl ModelClient for Models {
    fn generate(&self, request: ModelRequest, id: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
            if prompt["goal"] == "Hold" && request.turn == 0 {
                return Ok(ModelReply {
                    decision: Ok(ModelDecision {
                        action: AgentAction::AskHuman {
                            question: "Release?".into(),
                        },
                        request_id: None,
                        diagnostics: None,
                    }),
                    usage: Some(TokenUsage {
                        input_tokens: 10,
                        output_tokens: 10,
                    }),
                    retryable: false,
                    diagnostics: None,
                    retry_after: None,
                });
            }
            self.0.generate(request, id).await
        })
    }
}
struct App {
    engine: Engine,
    agents: AgentService,
    store: Arc<SqliteStore>,
    clock: Arc<ManualClock>,
}
async fn open(db: &Path, clock: Arc<ManualClock>) -> App {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let root = db.parent().unwrap().join(".act0r/workspaces");
    let workspaces = Arc::new(LocalWorkspaces::new(root.clone()));
    let registry = Arc::new(
        AgentRegistry::new(vec![
            Arc::new(AnswerAgentAdapter),
            Arc::new(PlannerAgentAdapter::default()),
        ])
        .unwrap(),
    );
    let engine = Engine::new(
        store.clone(),
        Arc::new(AgentExecutor::new(
            Arc::new(Models(ModelRouter::new(None).unwrap())),
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
            vec![Arc::new(RegisteredAgentDefinition)],
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
async fn answer(app: &App) -> AgentId {
    app.agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({"goal":"Answer","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"script":[{"kind":"finish","summary":"Blue"}]}),10,AgentQueuePolicy::default()).await.unwrap()
}
fn config(worker: AgentId, limit: u32) -> Value {
    json!({"goal":"Plan the task","target":{"provider":"scripted","model":"fixture","endpoint":""},"memory":{},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,"workers":[{"name":"assistant","agent_id":worker,"description":"A permitted assistant"}],
        "delegation":{"max_tasks":4,"max_depth":2},"follow_up":{"max_follow_ups":limit,"min_delay_ms":1,"max_delay_ms":1000},
        "script":[{"kind":"schedule_follow_up","summary":"The preferred color is blue.","goal":"Review the saved color","after_ms":500},{"kind":"finish","summary":"Released"}]})
}
async fn create(app: &App, limit: u32, capacity: u32) -> AgentId {
    let cfg = config(answer(app).await, limit);
    app.agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            cfg,
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
async fn settle(worker: &mut Worker) {
    // Future saved deadlines do not advance this manual clock.
    for _ in 0..100 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("follow-up fixture failed to quiesce");
}
async fn execute_one(worker: &mut Worker) {
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
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
async fn sql(db: &Path, statement: &'static str) {
    let conn = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(db)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    sqlx::raw_sql(statement).execute(&conn).await.unwrap();
    conn.close().await;
}
async fn snapshot(app: &App, actor: AgentId, root: MessageId) -> Value {
    let actors = app.agents.actors().unwrap();
    let run = run_for(app, root).await;
    json!({"actor":actors.status(actor).await.unwrap(),"tasks":actors.tasks(actor).await.unwrap(),"mailbox":actors.mailbox(actor).await.unwrap(),
        "links":actors.runs(actor).await.unwrap(),"reminders":actors.reminders(actor).await.unwrap(),"run":run,"history":app.engine.history(run.record().id).await.unwrap()})
}

#[tokio::test]
async fn scheduled_follow_up_recovers_once_with_saved_memory_and_parent_inspection() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("restart.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let actor = create(&app, 1, 1).await;
    let actors = app.agents.actors().unwrap();
    let root = task(actor, "Remember blue");
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let parent = actors.task(root.id).await.unwrap();
    assert_eq!(parent.status, AgentTaskStatus::Succeeded);
    let intent = parent.follow_up.clone().unwrap();
    assert_eq!(intent.status, FollowUpStatus::Registered);
    assert_eq!(intent.due_at, 1500);
    assert_eq!(parent.reply.as_ref().unwrap()["follow_up"], json!(intent));
    let reminder = actors.reminder(intent.reminder_id).await.unwrap();
    assert_eq!(reminder.origin, Some(intent.origin.clone()));
    assert_eq!(reminder.status, ReminderStatus::Scheduled);
    assert!(actors.task(reminder.id).await.is_err());
    assert_eq!(actors.queue(actor).await.unwrap().outstanding, 0);
    let memory = actors.memory(actor).await.unwrap().unwrap();
    assert_eq!(memory.revision, 1);
    assert!(worker.has_active_runs().await.unwrap());
    drop(worker);
    drop(app);
    let app = open(&db, ManualClock::new(5000)).await;
    let actors = app.agents.actors().unwrap();
    assert_eq!(actors.reminder(reminder.id).await.unwrap(), reminder);
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let child = actors.task(reminder.id).await.unwrap();
    assert_eq!(child.status, AgentTaskStatus::Succeeded);
    assert_eq!(child.task.follow_up_origin, Some(intent.origin));
    let run = run_for(&app, child.task.id).await;
    let context = run.record().follow_up_context.as_ref().unwrap();
    assert_eq!(context.depth, 1);
    assert_eq!(context.root_task_id, root.id);
    assert_eq!(context.remaining(), 0);
    assert_eq!(
        run.record().input["input"]["actor_memory"],
        json!(memory.snapshot(actor))
    );
    let request: ModelRequest =
        serde_json::from_value(run.activities()[0].spec.input.clone()).unwrap();
    assert!(!request.schema.to_string().contains("schedule_follow_up"));
    let refused = child.follow_up.unwrap();
    assert_eq!(refused.status, FollowUpStatus::Rejected);
    assert_eq!(refused.error.as_deref(), Some("chain_limit"));
    assert!(actors.reminder(refused.reminder_id).await.is_err());
    assert_eq!(actors.reminders(actor).await.unwrap().len(), 1);
    assert_eq!(actors.runs(actor).await.unwrap().len(), 2);
    assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 2);
    assert_eq!(
        actors.reminder(reminder.id).await.unwrap().status,
        ReminderStatus::Published
    );
    assert!(!worker.has_active_runs().await.unwrap());
    assert_eq!(actors.send(root.clone()).await.unwrap(), root.id);
    assert_eq!(actors.send(reminder.message).await.unwrap(), reminder.id);
    settle(&mut worker).await;
    assert_eq!(actors.runs(actor).await.unwrap().len(), 2);
}

#[tokio::test]
async fn chain_limits_survive_each_new_run_and_payloads_cannot_reset_lineage() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(&directory.path().join("chain.db"), ManualClock::new(1000)).await;
    let actor = create(&app, 3, 2).await;
    let actors = app.agents.actors().unwrap();
    let mut root = task(actor, "Root chain");
    root.payload["follow_up_context"] =
        json!({"depth":0,"root_task_id":MessageId::new(),"policy":{"max_follow_ups":64}});
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    let mut current = root.id;
    for depth in 0..=3 {
        settle(&mut worker).await;
        let run = run_for(&app, current).await;
        let context = run.record().follow_up_context.as_ref().unwrap();
        assert_eq!(context.task_id, current);
        assert_eq!(context.root_task_id, root.id);
        assert_eq!(context.depth, depth);
        assert_eq!(context.remaining(), 3 - depth);
        let intent = run.record().follow_up.as_ref().unwrap();
        if depth < 3 {
            assert_eq!(intent.status, FollowUpStatus::Registered);
            app.clock.set(intent.due_at + 10);
            current = intent.reminder_id;
        } else {
            assert_eq!(intent.error.as_deref(), Some("chain_limit"));
        }
    }
    assert_eq!(actors.reminders(actor).await.unwrap().len(), 3);
    assert_eq!(actors.tasks(actor).await.unwrap().len(), 4);
    assert!(!worker.has_active_runs().await.unwrap());
    // A separately submitted task has its own configured allowance, not an inherited chain.
    let other = task(actor, "Independent root");
    actors.send(other.clone()).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        run_for(&app, other.id)
            .await
            .record()
            .follow_up_context
            .as_ref()
            .unwrap()
            .depth,
        0
    );
    let next = actors.task(other.id).await.unwrap().follow_up.unwrap();
    assert_eq!(next.origin.root_task_id, other.id);
    actors.cancel_reminder(next.reminder_id).await.unwrap();
}

#[tokio::test]
async fn queued_follow_up_keeps_origin_and_selects_memory_at_launch() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(&directory.path().join("queued.db"), ManualClock::new(1000)).await;
    let actor = create(&app, 1, 2).await;
    let actors = app.agents.actors().unwrap();
    let root = task(actor, "Original result");
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let intent = actors.task(root.id).await.unwrap().follow_up.unwrap();
    let hold = task(actor, "Hold");
    actors.send(hold.clone()).await.unwrap();
    settle(&mut worker).await;
    app.clock.set(1500);
    settle(&mut worker).await;
    let child = actors.task(intent.reminder_id).await.unwrap();
    assert!(child.run_id.is_none());
    assert_eq!(child.task.follow_up_origin.as_ref(), Some(&intent.origin));
    assert_eq!(actors.queue(actor).await.unwrap().outstanding, 2);
    let held = run_for(&app, hold.id).await;
    app.engine
        .signal(
            held.record().id,
            held.record().wait.as_ref().unwrap().id,
            json!("Release"),
        )
        .await
        .unwrap();
    settle(&mut worker).await;
    let child = run_for(&app, intent.reminder_id).await;
    assert_eq!(child.record().follow_up_context.as_ref().unwrap().depth, 1);
    assert_eq!(
        child.record().input["input"]["actor_memory"]["entries"][1]["goal"],
        "Hold"
    );
    assert_eq!(
        child.record().follow_up.as_ref().unwrap().error.as_deref(),
        Some("chain_limit")
    );
    assert_eq!(actors.queue(actor).await.unwrap().outstanding, 0);
}

#[tokio::test]
async fn reminder_registration_and_parent_receipts_roll_back_as_one_unit() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("rollback.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let actor = create(&app, 1, 1).await;
    let actors = app.agents.actors().unwrap();
    let root = task(actor, "Atomic registration");
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    execute_one(&mut worker).await;
    let run = run_for(&app, root.id).await;
    let intent = run.record().follow_up.clone().unwrap();
    assert_eq!(intent.status, FollowUpStatus::Pending);
    let reserved = AgentMessage {
        id: intent.reminder_id,
        ..task(actor, "Early bypass")
    };
    assert!(matches!(
        actors.send(reserved.clone()).await,
        Err(act0r::Error::Domain(DomainError::ReservedMessageId))
    ));
    assert!(
        actors
            .schedule_task(reserved, TimerDeadline::At { unix_ms: 1000 })
            .await
            .is_err()
    );
    let before = snapshot(&app, actor, root.id).await;
    for trigger in [
        "CREATE TRIGGER refuse_follow_up BEFORE INSERT ON agent_reminders BEGIN SELECT RAISE(ABORT,'reminder'); END",
        "CREATE TRIGGER refuse_follow_up BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'actor'); END",
        "CREATE TRIGGER refuse_follow_up BEFORE UPDATE ON runs BEGIN SELECT RAISE(ABORT,'run'); END",
        "CREATE TRIGGER refuse_follow_up BEFORE INSERT ON history BEGIN SELECT RAISE(ABORT,'history'); END",
        "CREATE TRIGGER refuse_follow_up BEFORE UPDATE ON agent_tasks BEGIN SELECT RAISE(ABORT,'task'); END",
        "CREATE TRIGGER refuse_follow_up BEFORE INSERT ON agent_mailbox BEGIN SELECT RAISE(ABORT,'completion'); END",
        "CREATE TRIGGER refuse_follow_up BEFORE UPDATE ON agent_runs BEGIN SELECT RAISE(ABORT,'link'); END",
    ] {
        sql(&db, trigger).await;
        assert!(app.store.notify_run_completion(1000).await.is_err());
        assert_eq!(before, snapshot(&app, actor, root.id).await);
        sql(&db, "DROP TRIGGER refuse_follow_up").await;
    }
    app.clock.set(4000);
    assert!(app.store.notify_run_completion(4000).await.unwrap());
    assert!(!app.store.notify_run_completion(4000).await.unwrap());
    assert_eq!(
        actors.reminder(intent.reminder_id).await.unwrap().due_at,
        1500
    );
    assert_eq!(
        run.record().result,
        run_for(&app, root.id).await.record().result
    );
    settle(&mut worker).await;
    assert_eq!(actors.reminders(actor).await.unwrap().len(), 1);
    assert_eq!(actors.runs(actor).await.unwrap().len(), 2);
}

#[tokio::test]
async fn delayed_registration_does_not_turn_an_on_time_child_into_a_deadline_failure() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(
        &directory.path().join("completion-time.db"),
        ManualClock::new(1000),
    )
    .await;
    let child_actor = create(&app, 1, 1).await;
    let mut cfg = config(child_actor, 1);
    cfg.as_object_mut().unwrap().remove("follow_up");
    cfg["delegation"]["timeout_ms"] = json!(500);
    cfg["script"] = json!([{"kind":"delegate_task","worker":"assistant","goal":"Finish and schedule a review"},{"kind":"finish","summary":"The child finished on time"}]);
    let parent_actor = app
        .agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            cfg,
            20,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let actors = app.agents.actors().unwrap();
    let root = task(parent_actor, "Parent deadline");
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    execute_one(&mut worker).await;
    let parent = run_for(&app, root.id).await;
    let child_id = parent.record().delegations[0].id;
    worker.advance_work().await.unwrap(); // Publish the delegated request.
    worker.advance_work().await.unwrap(); // Start the child Run.
    execute_one(&mut worker).await;
    let child = run_for(&app, child_id).await;
    assert_eq!(child.record().status, RunStatus::Succeeded);
    assert_eq!(child.record().updated_at, 1000);
    app.clock.set(2000);
    assert!(app.store.notify_run_completion(2000).await.unwrap());
    let saved = run_for(&app, child_id).await;
    assert_eq!(saved.record().updated_at, 1000);
    assert_eq!(
        saved.record().follow_up.as_ref().unwrap().settled_at,
        Some(2000)
    );
    settle(&mut worker).await;
    let parent = run_for(&app, root.id).await;
    assert_eq!(parent.record().status, RunStatus::Succeeded);
    assert!(parent.record().delegations[0].timed_out_at.is_none());
    assert_eq!(
        parent.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "succeeded"
    );
}

#[tokio::test]
async fn a_pending_intent_can_be_cancelled_atomically_at_full_capacity_before_restart() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("pending-cancel.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let actor = create(&app, 1, 1).await;
    let actors = app.agents.actors().unwrap();
    let root = task(actor, "Cancel the saved intent");
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    execute_one(&mut worker).await;
    let intent = run_for(&app, root.id)
        .await
        .record()
        .follow_up
        .clone()
        .unwrap();
    for _ in 0..MAX_PENDING_REMINDERS {
        actors
            .schedule_task(
                task(actor, "Manual later"),
                TimerDeadline::At { unix_ms: 100000 },
            )
            .await
            .unwrap();
    }
    let before = snapshot(&app, actor, root.id).await;
    for trigger in [
        "CREATE TRIGGER refuse_cancel BEFORE INSERT ON agent_reminders BEGIN SELECT RAISE(ABORT,'reminder'); END",
        "CREATE TRIGGER refuse_cancel BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'actor'); END",
        "CREATE TRIGGER refuse_cancel BEFORE UPDATE ON runs BEGIN SELECT RAISE(ABORT,'run'); END",
        "CREATE TRIGGER refuse_cancel BEFORE INSERT ON history BEGIN SELECT RAISE(ABORT,'history'); END",
    ] {
        sql(&db, trigger).await;
        assert!(actors.cancel_reminder(intent.reminder_id).await.is_err());
        assert_eq!(before, snapshot(&app, actor, root.id).await);
        sql(&db, "DROP TRIGGER refuse_cancel").await;
    }
    let cancelled = actors.cancel_reminder(intent.reminder_id).await.unwrap();
    assert_eq!(cancelled.status, ReminderStatus::Cancelled);
    assert_eq!(cancelled.due_at, 1500);
    assert_eq!(
        cancelled,
        actors.cancel_reminder(intent.reminder_id).await.unwrap()
    );
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
    assert_eq!(
        run_for(&app, root.id)
            .await
            .record()
            .follow_up
            .as_ref()
            .unwrap()
            .error
            .as_deref(),
        Some("cancelled")
    );
    drop(worker);
    drop(app);
    let app = open(&db, ManualClock::new(5000)).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let actors = app.agents.actors().unwrap();
    assert_eq!(
        actors.reminder(intent.reminder_id).await.unwrap(),
        cancelled
    );
    assert_eq!(
        actors.task(root.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(
        actors.task(root.id).await.unwrap().reply.unwrap()["follow_up"]["error"],
        "cancelled"
    );
    assert!(actors.task(intent.reminder_id).await.is_err());
    assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 1);
    assert_eq!(actors.runs(actor).await.unwrap().len(), 1);
}

#[tokio::test]
async fn capacity_refusal_is_durable_and_does_not_change_the_finished_answer() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(
        &directory.path().join("capacity.db"),
        ManualClock::new(1000),
    )
    .await;
    let actor = create(&app, 1, 1).await;
    let actors = app.agents.actors().unwrap();
    let mut ids = vec![];
    for _ in 0..MAX_PENDING_REMINDERS {
        let message = task(actor, "Manual future task");
        ids.push(message.id);
        actors
            .schedule_task(message, TimerDeadline::At { unix_ms: 100000 })
            .await
            .unwrap();
    }
    let root = task(actor, "Finish despite capacity");
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let parent = actors.task(root.id).await.unwrap();
    let intent = parent.follow_up.unwrap();
    assert_eq!(parent.status, AgentTaskStatus::Succeeded);
    assert_eq!(
        parent.reply.unwrap()["result"]["answer"],
        "The preferred color is blue."
    );
    assert_eq!(intent.error.as_deref(), Some("reminder_capacity"));
    assert_eq!(intent.status, FollowUpStatus::Rejected);
    assert!(actors.reminder(intent.reminder_id).await.is_err());
    assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 1);
    actors.cancel_reminder(ids[0]).await.unwrap();
    assert!(!app.store.notify_run_completion(2000).await.unwrap());
    assert_eq!(
        run_for(&app, root.id).await.record().follow_up.as_ref(),
        Some(&intent)
    );
    assert_eq!(
        actors.reminders(actor).await.unwrap().len(),
        MAX_PENDING_REMINDERS
    );
}

#[tokio::test]
async fn cancellation_before_model_completion_prevents_any_follow_up() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(&directory.path().join("cancel.db"), ManualClock::new(1000)).await;
    let actor = create(&app, 1, 1).await;
    let actors = app.agents.actors().unwrap();
    let root = task(actor, "Cancelled decision");
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    actors.cancel_task(root.id).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
    settle(&mut worker).await;
    let run = run_for(&app, root.id).await;
    assert_eq!(run.record().status, RunStatus::Cancelled);
    assert!(run.record().follow_up.is_none());
    assert!(actors.reminders(actor).await.unwrap().is_empty());
    assert_eq!(run.record().usage.admitted_model_calls, 1);
    assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 0);
}

#[tokio::test]
async fn paused_publication_and_cancelled_follow_ups_keep_parent_success_and_identity() {
    for published in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let app = open(
            &directory.path().join("controls.db"),
            ManualClock::new(1000),
        )
        .await;
        let actor = create(&app, 1, 1).await;
        let actors = app.agents.actors().unwrap();
        let root = task(actor, "Controlled future");
        actors.send(root.clone()).await.unwrap();
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let intent = actors.task(root.id).await.unwrap().follow_up.unwrap();
        if published {
            app.clock.set(2000);
            app.store.publish_due_actor_reminder(2000).await.unwrap();
        }
        actors.pause(actor).await.unwrap();
        app.clock.set(2000);
        if published {
            actors.cancel_task(intent.reminder_id).await.unwrap();
        } else {
            actors.cancel_reminder(intent.reminder_id).await.unwrap();
        }
        assert!(!worker.has_active_runs().await.unwrap());
        assert_eq!(
            actors.task(root.id).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        actors.resume(actor).await.unwrap();
        settle(&mut worker).await;
        assert_eq!(actors.runs(actor).await.unwrap().len(), 1);
        assert!(matches!(
            actors
                .send(AgentMessage {
                    id: intent.reminder_id,
                    ..task(actor, "Retry cancelled")
                })
                .await,
            Err(act0r::Error::Domain(
                DomainError::ReservedMessageId | DomainError::ConflictingMessage
            ))
        ));
    }
}

#[tokio::test]
async fn policies_reserved_context_and_invalid_live_actions_are_enforced() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(
        &directory.path().join("validation.db"),
        ManualClock::new(1000),
    )
    .await;
    let permitted = answer(&app).await;
    for policy in [
        json!({"max_follow_ups":0}),
        json!({"max_follow_ups":65}),
        json!({"max_follow_ups":1,"min_delay_ms":0}),
        json!({"max_follow_ups":1,"min_delay_ms":100,"max_delay_ms":1}),
        json!({"max_follow_ups":1,"max_delay_ms":u64::MAX}),
    ] {
        let mut cfg = config(permitted, 1);
        cfg["follow_up"] = policy;
        assert!(
            app.agents
                .create_registered_actor(
                    DefinitionId::new("planner", 3),
                    None,
                    cfg,
                    20,
                    AgentQueuePolicy::default()
                )
                .await
                .is_err()
        );
    }
    let mut cfg = config(permitted, 1);
    cfg["follow_up_context"] = json!({"actor_id":AgentId::new(),"task_id":MessageId::new(),"root_task_id":MessageId::new(),"depth":0,"policy":cfg["follow_up"]});
    assert!(
        app.agents
            .create_registered_actor(
                DefinitionId::new("planner", 3),
                None,
                cfg,
                20,
                AgentQueuePolicy::default()
            )
            .await
            .is_err()
    );
    for delay in [0, 1001] {
        let mut cfg = config(permitted, 1);
        cfg["script"][0]["after_ms"] = json!(delay);
        assert!(
            app.agents
                .create_registered_actor(
                    DefinitionId::new("planner", 3),
                    None,
                    cfg,
                    20,
                    AgentQueuePolicy::default()
                )
                .await
                .is_err()
        );
    }
    let mut disabled = config(permitted, 1);
    disabled.as_object_mut().unwrap().remove("follow_up");
    assert!(
        app.agents
            .create_registered_actor(
                DefinitionId::new("planner", 3),
                None,
                disabled.clone(),
                20,
                AgentQueuePolicy::default()
            )
            .await
            .is_err()
    );
    disabled["script"] = json!([{"kind":"finish","summary":"Ordinary planner"}]);
    let actor = app
        .agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            disabled,
            20,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let actors = app.agents.actors().unwrap();
    let root = task(actor, "Disabled scheduling");
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    worker.complete(execution,Outcome::Success(json!({"action":{"kind":"schedule_follow_up","summary":"Done","goal":"Later","after_ms":500}}))).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        run_for(&app, root.id).await.record().status,
        RunStatus::Failed
    );
    assert!(actors.reminders(actor).await.unwrap().is_empty());
}

#[tokio::test]
async fn killed_workers_keep_intent_deadline_lineage_and_exactly_one_follow_up() {
    for phase in [
        "intent",
        "registered",
        "published",
        "started",
        "inflight",
        "cancelled",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("kill.db");
        let ready = directory.path().join("ready.json");
        let log = std::fs::File::create(directory.path().join("child.log")).unwrap();
        let mut child = ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "follow_up_kill_fixture",
                "--nocapture",
            ])
            .env("ACT0R_FOLLOW_UP_DB", &db)
            .env("ACT0R_FOLLOW_UP_READY", &ready)
            .env("ACT0R_FOLLOW_UP_PHASE", phase)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(40);
        while !ready.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!(
                    "follow-up child exited {status}: {}",
                    std::fs::read_to_string(directory.path().join("child.log")).unwrap()
                );
            }
            if Instant::now() > deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("follow-up child timed out");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value = serde_json::from_slice(&std::fs::read(ready).unwrap()).unwrap();
        let actor: AgentId = serde_json::from_value(saved["actor"].clone()).unwrap();
        let root: MessageId = serde_json::from_value(saved["root"].clone()).unwrap();
        let id: MessageId = serde_json::from_value(saved["id"].clone()).unwrap();
        let clock = ManualClock::new(5000);
        let app = open(&db, clock.clone()).await;
        let mut worker = app.engine.worker().await.unwrap();
        clock.set(6000);
        settle(&mut worker).await;
        let actors = app.agents.actors().unwrap();
        let parent = run_for(&app, root).await;
        assert_eq!(parent.record().follow_up.as_ref().unwrap().reminder_id, id);
        assert_eq!(parent.record().follow_up.as_ref().unwrap().due_at, 1500);
        assert_eq!(actors.reminders(actor).await.unwrap().len(), 1);
        if phase == "cancelled" {
            assert_eq!(
                actors.reminder(id).await.unwrap().status,
                ReminderStatus::Cancelled
            );
            assert!(actors.task(id).await.is_err());
        } else {
            let child = run_for(&app, id).await;
            assert_eq!(child.record().status, RunStatus::Succeeded);
            assert_eq!(child.record().follow_up_context.as_ref().unwrap().depth, 1);
            assert_eq!(
                child
                    .record()
                    .follow_up_context
                    .as_ref()
                    .unwrap()
                    .root_task_id,
                root
            );
            assert_eq!(
                child.record().follow_up.as_ref().unwrap().error.as_deref(),
                Some("chain_limit")
            );
            assert_eq!(
                child.record().input["input"]["actor_memory"]["entries"][0]["goal"],
                "Remember before downtime"
            );
            if phase == "started" || phase == "inflight" {
                assert_eq!(child.record().input, saved["input"]);
            }
            assert_eq!(
                child.record().usage.admitted_model_calls,
                if phase == "inflight" { 2 } else { 1 }
            );
            assert_eq!(
                actors
                    .mailbox(actor)
                    .await
                    .unwrap()
                    .iter()
                    .filter(|e| e.message.id == id)
                    .count(),
                1
            );
            assert_eq!(actors.runs(actor).await.unwrap().len(), 2);
        }
        assert!(!worker.has_active_runs().await.unwrap());
    }
}

#[tokio::test]
#[ignore = "subprocess fixture controlled by follow-up recovery test"]
async fn follow_up_kill_fixture() {
    let db = std::env::var("ACT0R_FOLLOW_UP_DB").unwrap();
    let ready = std::path::PathBuf::from(std::env::var("ACT0R_FOLLOW_UP_READY").unwrap());
    let phase = std::env::var("ACT0R_FOLLOW_UP_PHASE").unwrap();
    let app = open(Path::new(&db), ManualClock::new(1000)).await;
    let actor = create(&app, 1, 1).await;
    let actors = app.agents.actors().unwrap();
    let root = task(actor, "Remember before downtime");
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    execute_one(&mut worker).await;
    let id = run_for(&app, root.id)
        .await
        .record()
        .follow_up
        .as_ref()
        .unwrap()
        .reminder_id;
    if phase != "intent" {
        app.store.notify_run_completion(1000).await.unwrap();
        worker.advance_work().await.unwrap();
    }
    if phase == "cancelled" {
        actors.cancel_reminder(id).await.unwrap();
    }
    if ["published", "started", "inflight"].contains(&phase.as_str()) {
        app.clock.set(2000);
        app.store.publish_due_actor_reminder(2000).await.unwrap();
    }
    if phase == "started" || phase == "inflight" {
        worker.advance_work().await.unwrap();
    }
    if phase == "inflight" {
        worker.admit().await.unwrap().unwrap();
    }
    let input = if phase == "started" || phase == "inflight" {
        run_for(&app, id).await.record().input.clone()
    } else {
        Value::Null
    };
    let temporary = ready.with_extension("tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec(&json!({"actor":actor,"root":root.id,"id":id,"input":input})).unwrap(),
    )
    .unwrap();
    std::fs::rename(temporary, ready).unwrap();
    std::future::pending::<()>().await;
}
