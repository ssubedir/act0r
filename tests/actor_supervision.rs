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
    domain::{actors::*, agent::*, *},
};
use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Command as ProcessCommand, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

struct ClockAt(i64);
impl Clock for ClockAt {
    fn now_ms(&self) -> i64 {
        self.0
    }
}
struct Models(ModelRouter);
impl ModelClient for Models {
    fn generate(&self, request: ModelRequest, id: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
            let goal = prompt["goal"].as_str().unwrap();
            let decision = if goal.starts_with("Fail") {
                Err(format!("{goal}: {}", "🙂".repeat(600)))
            } else if goal == "Hold" && request.turn == 0 {
                Ok(ModelDecision {
                    action: AgentAction::AskHuman {
                        question: "Continue?".into(),
                    },
                    request_id: None,
                    diagnostics: None,
                })
            } else {
                return self.0.generate(request, id).await;
            };
            Ok(ModelReply {
                decision,
                usage: Some(TokenUsage {
                    input_tokens: 10,
                    output_tokens: 10,
                }),
                retryable: false,
                diagnostics: None,
                retry_after: None,
            })
        })
    }
}
struct App {
    engine: Engine,
    agents: AgentService,
    store: Arc<SqliteStore>,
}
async fn open(db: &Path, now: i64) -> App {
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
        Arc::new(ClockAt(now)),
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
                Arc::new(WorkflowWorkerAgent),
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
    }
}
fn config(limit: Option<u32>) -> Value {
    let mut config = json!({"goal":"Answer","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,"memory":{},
        "script":[{"kind":"finish","summary":"Saved answer"},{"kind":"finish","summary":"Released answer"}]});
    if let Some(limit) = limit {
        config["supervision"] = json!({"max_consecutive_failures":limit});
    }
    config
}
async fn create(app: &App, limit: Option<u32>) -> AgentId {
    app.agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            config(limit),
            20,
            AgentQueuePolicy { task_capacity: 8 },
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
    for _ in 0..300 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("supervision fixture did not quiesce");
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
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(db))
            .await
            .unwrap();
    sqlx::raw_sql(statement).execute(&pool).await.unwrap();
    pool.close().await;
}
async fn snapshot(db: &Path) -> Value {
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(db))
            .await
            .unwrap();
    let mut snapshot = serde_json::Map::new();
    for (table, query) in [
        (
            "agent_actors",
            "SELECT payload FROM agent_actors ORDER BY rowid",
        ),
        (
            "agent_mailbox",
            "SELECT payload FROM agent_mailbox ORDER BY rowid",
        ),
        (
            "agent_tasks",
            "SELECT payload FROM agent_tasks ORDER BY rowid",
        ),
        (
            "agent_runs",
            "SELECT payload FROM agent_runs ORDER BY rowid",
        ),
        (
            "agent_reminders",
            "SELECT payload FROM agent_reminders ORDER BY rowid",
        ),
        ("runs", "SELECT payload FROM runs ORDER BY rowid"),
        ("history", "SELECT payload FROM history ORDER BY rowid"),
    ] {
        let rows: Vec<String> = sqlx::query_scalar(query).fetch_all(&pool).await.unwrap();
        snapshot.insert(table.into(), json!(rows));
    }
    pool.close().await;
    Value::Object(snapshot)
}

#[tokio::test]
async fn threshold_pause_preserves_pending_work_reminders_fifo_and_memory_across_restart() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("queue.db");
    let app = open(&db, 1000).await;
    let actor = create(&app, Some(3)).await;
    let actors = app.agents.actors().unwrap();
    let tasks: Vec<_> = [
        "Fail A",
        "Fail B",
        "Fail C",
        "Oldest retained",
        "Cancel retained",
    ]
    .into_iter()
    .map(|g| task(actor, g))
    .collect();
    for task in &tasks {
        actors.send(task.clone()).await.unwrap();
    }
    let reminder = task(actor, "Due after pause");
    actors
        .schedule_task(reminder.clone(), TimerDeadline::At { unix_ms: 2000 })
        .await
        .unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    // Consume all task envelopes so the retained work lives in actor state.
    for _ in &tasks {
        assert!(worker.advance_work().await.unwrap());
    }
    settle(&mut worker).await;
    let saved = actors.supervision(actor).await.unwrap().unwrap();
    assert_eq!(saved.consecutive_failures, 3);
    assert_eq!(saved.failures.len(), 3);
    assert_eq!(saved.pause.as_ref().unwrap().task_id, tasks[2].id);
    assert_eq!(
        actors.status(actor).await.unwrap().record().status,
        AgentStatus::Paused
    );
    assert_eq!(actors.runs(actor).await.unwrap().len(), 3);
    for task in &tasks[3..] {
        assert!(actors.task(task.id).await.unwrap().run_id.is_none());
    }
    actors.send(tasks[0].clone()).await.unwrap();
    assert_eq!(actors.supervision(actor).await.unwrap().unwrap(), saved);
    assert!(actors.resume(actor).await.is_err());
    assert!(!worker.has_active_runs().await.unwrap());
    drop(worker);
    drop(app);

    let app = open(&db, 5000).await;
    let actors = app.agents.actors().unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(actors.supervision(actor).await.unwrap().unwrap(), saved);
    assert_eq!(
        actors.reminder(reminder.id).await.unwrap().status,
        ReminderStatus::Scheduled
    );
    actors.cancel_task(tasks[4].id).await.unwrap();
    let later = task(actor, "Mailbox task accepted while paused");
    actors.send(later.clone()).await.unwrap();
    let other = create(&app, Some(1)).await;
    let independent = task(other, "Healthy independent actor");
    actors.send(independent.clone()).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        actors.task(independent.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(actors.runs(actor).await.unwrap().len(), 3);
    let (first, second) = tokio::join!(
        actors.recover_supervision(actor),
        actors.recover_supervision(actor)
    );
    first.unwrap();
    second.unwrap();
    let recovered = actors.status(actor).await.unwrap();
    assert_eq!(
        recovered.record().supervision.as_ref().unwrap().recoveries,
        1
    );
    actors.recover_supervision(actor).await.unwrap();
    assert_eq!(
        actors.status(actor).await.unwrap().record().revision,
        recovered.record().revision
    );
    assert_eq!(
        actors
            .mailbox(actor)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.message.kind == SUPERVISION_RECOVERED)
            .count(),
        1
    );
    actors.pause(actor).await.unwrap();
    settle(&mut worker).await;
    assert!(actors.task(tasks[3].id).await.unwrap().run_id.is_none());
    actors.resume(actor).await.unwrap();
    settle(&mut worker).await;
    for id in [tasks[3].id, later.id, reminder.id] {
        assert_eq!(
            actors.task(id).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
    }
    assert_eq!(
        actors.task(tasks[4].id).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
    let memory = actors.memory(actor).await.unwrap().unwrap();
    assert_eq!(
        memory.entries.iter().map(|e| e.task_id).collect::<Vec<_>>(),
        vec![tasks[3].id, later.id, reminder.id]
    );
    assert_eq!(
        actors
            .supervision(actor)
            .await
            .unwrap()
            .unwrap()
            .consecutive_failures,
        0
    );
    assert_eq!(
        actors.reminder(reminder.id).await.unwrap().status,
        ReminderStatus::Published
    );
    assert_eq!(actors.queue(actor).await.unwrap().outstanding, 0);
    assert!(!worker.has_active_runs().await.unwrap());
}

#[tokio::test]
async fn success_resets_failures_while_waits_cancellation_budget_exhaustion_and_rejection_are_neutral()
 {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("outcomes.db");
    let app = open(&db, 1000).await;
    let actors = app.agents.actors().unwrap();
    let mut cfg = config(Some(2));
    cfg["max_model_calls"] = json!(1);
    let actor = app
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
    let mut worker = app.engine.worker().await.unwrap();
    for (goal, expected) in [("Fail first", 1), ("Success", 0), ("Fail again", 1)] {
        let task = task(actor, goal);
        actors.send(task).await.unwrap();
        settle(&mut worker).await;
        assert_eq!(
            actors
                .supervision(actor)
                .await
                .unwrap()
                .unwrap()
                .consecutive_failures,
            expected
        );
    }
    let rejected = task(actor, " ");
    actors.send(rejected.clone()).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        actors.task(rejected.id).await.unwrap().status,
        AgentTaskStatus::Rejected
    );
    let held = task(actor, "Hold");
    actors.send(held.clone()).await.unwrap();
    settle(&mut worker).await;
    let run = run_for(&app, held.id).await;
    assert_eq!(run.record().status, RunStatus::Waiting);
    actors.pause(actor).await.unwrap();
    actors.resume(actor).await.unwrap();
    assert_eq!(
        actors
            .supervision(actor)
            .await
            .unwrap()
            .unwrap()
            .consecutive_failures,
        1
    );
    actors.cancel_task(held.id).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        actors.task(held.id).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
    let budget = task(actor, "Hold");
    actors.send(budget.clone()).await.unwrap();
    settle(&mut worker).await;
    let run = run_for(&app, budget.id).await;
    app.engine
        .signal(
            run.record().id,
            run.record().wait.as_ref().unwrap().id,
            "yes".into(),
        )
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(
        actors.task(budget.id).await.unwrap().status,
        AgentTaskStatus::BudgetExhausted
    );
    assert_eq!(
        actors
            .supervision(actor)
            .await
            .unwrap()
            .unwrap()
            .consecutive_failures,
        1
    );
    let last = task(actor, "Fail threshold");
    actors.send(last.clone()).await.unwrap();
    settle(&mut worker).await;
    let health = actors.supervision(actor).await.unwrap().unwrap();
    assert_eq!(health.consecutive_failures, 2);
    assert_eq!(health.failures.len(), 3);
    for failure in &health.failures {
        assert!(failure.error.as_ref().unwrap().len() <= MAX_SUPERVISION_ERROR_BYTES);
    }
    assert!(
        run_for(&app, last.id)
            .await
            .record()
            .error
            .as_ref()
            .unwrap()
            .message
            .len()
            > MAX_SUPERVISION_ERROR_BYTES
    );
    assert_eq!(
        actors.memory(actor).await.unwrap().unwrap().entries.len(),
        1
    );
}

#[tokio::test]
async fn pause_and_failure_reply_roll_back_together_and_recovery_wakeup_is_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("rollback.db");
    let app = open(&db, 1000).await;
    let actor = create(&app, Some(1)).await;
    let actors = app.agents.actors().unwrap();
    let coordinator = actors
        .create(
            AgentId::new(),
            CoordinatorAgent.id(),
            json!({"worker":actor}),
        )
        .await
        .unwrap();
    let mut failed = task(actor, "Fail atomic");
    failed.sender = Some(coordinator);
    let next = task(actor, "Retained");
    actors.send(failed.clone()).await.unwrap();
    actors.send(next.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    worker.advance_work().await.unwrap();
    execute_one(&mut worker).await;
    app.store.notify_run_completion(1000).await.unwrap();
    let before = snapshot(&db).await;
    for trigger in [
        "CREATE TRIGGER refuse_supervision BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'actor'); END",
        "CREATE TRIGGER refuse_supervision BEFORE INSERT ON agent_mailbox WHEN json_extract(NEW.payload,'$.message.kind')='task_result' BEGIN SELECT RAISE(ABORT,'reply'); END",
        "CREATE TRIGGER refuse_supervision BEFORE UPDATE ON agent_mailbox BEGIN SELECT RAISE(ABORT,'receipt'); END",
    ] {
        sql(&db, trigger).await;
        assert!(worker.advance_work().await.is_err());
        assert_eq!(before, snapshot(&db).await);
        sql(&db, "DROP TRIGGER refuse_supervision").await;
    }
    worker.advance_work().await.unwrap();
    assert_eq!(
        actors
            .supervision(actor)
            .await
            .unwrap()
            .unwrap()
            .consecutive_failures,
        1
    );
    assert!(actors.task(next.id).await.unwrap().run_id.is_none());
    let before = snapshot(&db).await;
    for trigger in [
        "CREATE TRIGGER refuse_recovery BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'actor'); END",
        "CREATE TRIGGER refuse_recovery BEFORE INSERT ON agent_mailbox BEGIN SELECT RAISE(ABORT,'wakeup'); END",
    ] {
        sql(&db, trigger).await;
        assert!(actors.recover_supervision(actor).await.is_err());
        assert_eq!(before, snapshot(&db).await);
        sql(&db, "DROP TRIGGER refuse_recovery").await;
    }
    actors.recover_supervision(actor).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        actors.task(next.id).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(
        actors
            .supervision(actor)
            .await
            .unwrap()
            .unwrap()
            .failures
            .len(),
        1
    );
}

#[tokio::test]
async fn policy_is_pinned_opt_in_bounded_and_reserved_events_cannot_be_forged() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(&directory.path().join("policy.db"), 1000).await;
    let actors = app.agents.actors().unwrap();
    for limit in [0, 65] {
        assert!(
            app.agents
                .create_registered_actor(
                    DefinitionId::new("answer", 1),
                    None,
                    config(Some(limit)),
                    20,
                    AgentQueuePolicy::default()
                )
                .await
                .is_err()
        );
    }
    assert!(
        actors
            .create_with_policies(
                AgentId::new(),
                CoordinatorAgent.id(),
                json!({"worker":AgentId::new()}),
                AgentQueuePolicy::default(),
                Some(SupervisionPolicy {
                    max_consecutive_failures: 1
                })
            )
            .await
            .is_err()
    );
    let actor = create(&app, Some(1)).await;
    let mut message = task(actor, "Fail pinned");
    message.payload["supervision"] = json!({"max_consecutive_failures":64});
    actors.send(message.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let saved = actors.status(actor).await.unwrap();
    actors
        .create_with_policies(
            actor,
            saved.record().definition.clone(),
            saved.record().config.clone(),
            saved.record().queue_policy,
            Some(SupervisionPolicy {
                max_consecutive_failures: 1,
            }),
        )
        .await
        .unwrap();
    assert!(
        actors
            .create_with_policies(
                actor,
                saved.record().definition.clone(),
                saved.record().config.clone(),
                saved.record().queue_policy,
                Some(SupervisionPolicy {
                    max_consecutive_failures: 2
                })
            )
            .await
            .is_err()
    );
    for kind in [RUN_COMPLETED, SUPERVISION_RECOVERED] {
        let mut forged = task(actor, "forged");
        forged.kind = kind.into();
        assert!(actors.send(forged).await.is_err());
    }
    assert_eq!(
        actors.status(actor).await.unwrap().record().revision,
        saved.record().revision
    );
    let disabled = create(&app, None).await;
    for goal in ["Fail unsupervised", "Success"] {
        actors.send(task(disabled, goal)).await.unwrap();
    }
    settle(&mut worker).await;
    assert!(actors.supervision(disabled).await.unwrap().is_none());
    assert!(actors.recover_supervision(disabled).await.is_err());
    assert_eq!(actors.runs(disabled).await.unwrap().len(), 2);
}

#[tokio::test]
async fn failure_history_is_bounded_and_success_keeps_the_failure_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(&directory.path().join("history.db"), 1000).await;
    let actor = create(&app, Some(64)).await;
    let actors = app.agents.actors().unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    let mut ids = vec![];
    for ordinal in 0..20 {
        let task = task(actor, &format!("Fail {ordinal}"));
        ids.push(task.id);
        actors.send(task).await.unwrap();
        settle(&mut worker).await;
    }
    actors
        .send(task(actor, "Success after failures"))
        .await
        .unwrap();
    settle(&mut worker).await;
    let health = actors.supervision(actor).await.unwrap().unwrap();
    assert_eq!(health.consecutive_failures, 0);
    assert_eq!(health.failures.len(), MAX_SUPERVISION_FAILURES);
    assert_eq!(
        health
            .failures
            .iter()
            .map(|f| f.task_id)
            .collect::<Vec<_>>(),
        ids[4..]
    );
    assert!(health.pause.is_none());
}

#[tokio::test]
async fn manual_pause_defers_failure_observation_and_memory_clear_preserves_health() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(&directory.path().join("manual-pause.db"), 1000).await;
    let actor = create(&app, Some(1)).await;
    let actors = app.agents.actors().unwrap();
    let failed = task(actor, "Fail while manually paused");
    actors.send(failed.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    execute_one(&mut worker).await;
    actors.pause(actor).await.unwrap();
    app.store.notify_run_completion(1000).await.unwrap();
    let manual = actors.status(actor).await.unwrap();
    actors.recover_supervision(actor).await.unwrap();
    assert_eq!(
        actors.status(actor).await.unwrap().record().revision,
        manual.record().revision
    );
    assert_eq!(
        actors
            .supervision(actor)
            .await
            .unwrap()
            .unwrap()
            .consecutive_failures,
        0
    );
    assert!(!worker.step().await.unwrap());
    actors.resume(actor).await.unwrap();
    settle(&mut worker).await;
    let paused = actors.supervision(actor).await.unwrap().unwrap();
    assert_eq!(paused.consecutive_failures, 1);
    assert_eq!(paused.pause.as_ref().unwrap().task_id, failed.id);
    actors.clear_memory(actor).await.unwrap();
    assert_eq!(actors.supervision(actor).await.unwrap().unwrap(), paused);
    assert_eq!(
        actors.status(actor).await.unwrap().record().status,
        AgentStatus::Paused
    );
}

#[tokio::test]
async fn delegated_failure_still_reaches_the_parent_while_only_the_failed_child_pauses() {
    let directory = tempfile::tempdir().unwrap();
    let app = open(&directory.path().join("delegation.db"), 1000).await;
    let child = create(&app, Some(1)).await;
    let mut cfg = config(Some(1));
    cfg["workers"] = json!([{"name":"assistant","agent_id":child,"description":"Assistant"}]);
    cfg["delegation"] = json!({"max_tasks":2,"max_depth":2,"shared_budget":{"max_model_calls":4,"max_tokens":100000}});
    cfg["script"] = json!([{"kind":"delegate_task","worker":"assistant","goal":"Fail child"},{"kind":"finish","summary":"Handled failure"}]);
    let parent = app
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
    let root = task(parent, "Parent goal");
    let actors = app.agents.actors().unwrap();
    actors.send(root.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let run = run_for(&app, root.id).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "failed"
    );
    assert_eq!(
        actors.status(child).await.unwrap().record().status,
        AgentStatus::Paused
    );
    assert_eq!(
        actors
            .supervision(parent)
            .await
            .unwrap()
            .unwrap()
            .consecutive_failures,
        0
    );
}

#[tokio::test]
async fn killed_workers_count_completed_failures_once_and_recover_retained_work_once() {
    for phase in ["completed", "notified", "paused", "recovered"] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("kill.db");
        let ready = directory.path().join("ready.json");
        let log = std::fs::File::create(directory.path().join("child.log")).unwrap();
        let mut child = ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "supervision_kill_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("ACT0R_SUPERVISION_DB", &db)
            .env("ACT0R_SUPERVISION_READY", &ready)
            .env("ACT0R_SUPERVISION_PHASE", phase)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !ready.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!(
                    "fixture exited {status}: {}",
                    std::fs::read_to_string(directory.path().join("child.log")).unwrap()
                );
            }
            if Instant::now() > deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("fixture timed out");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value = serde_json::from_slice(&std::fs::read(ready).unwrap()).unwrap();
        let actor = serde_json::from_value(saved["actor"].clone()).unwrap();
        let failed = serde_json::from_value(saved["failed"].clone()).unwrap();
        let next = serde_json::from_value(saved["next"].clone()).unwrap();
        let app = open(&db, 5000).await;
        let actors = app.agents.actors().unwrap();
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        if phase != "recovered" {
            assert_eq!(
                actors
                    .supervision(actor)
                    .await
                    .unwrap()
                    .unwrap()
                    .consecutive_failures,
                1
            );
            assert_eq!(
                actors.status(actor).await.unwrap().record().status,
                AgentStatus::Paused
            );
            assert!(actors.task(next).await.unwrap().run_id.is_none());
            actors.recover_supervision(actor).await.unwrap();
            settle(&mut worker).await;
        }
        let health = actors.supervision(actor).await.unwrap().unwrap();
        assert_eq!(health.failures.len(), 1);
        assert_eq!(health.failures[0].task_id, failed);
        assert_eq!(health.recoveries, 1);
        assert_eq!(
            actors.task(next).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        assert_eq!(actors.runs(actor).await.unwrap().len(), 2);
        assert_eq!(
            run_for(&app, failed)
                .await
                .record()
                .usage
                .admitted_model_calls,
            1
        );
        assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 1);
        assert!(!worker.has_active_runs().await.unwrap());
    }
}
#[tokio::test]
#[ignore = "subprocess supervision checkpoint fixture"]
async fn supervision_kill_fixture() {
    let db = std::env::var("ACT0R_SUPERVISION_DB").unwrap();
    let ready = std::path::PathBuf::from(std::env::var("ACT0R_SUPERVISION_READY").unwrap());
    let phase = std::env::var("ACT0R_SUPERVISION_PHASE").unwrap();
    let app = open(Path::new(&db), 1000).await;
    let actor = create(&app, Some(1)).await;
    let actors = app.agents.actors().unwrap();
    let failed = task(actor, "Fail before kill");
    let next = task(actor, "Retained across kill");
    actors.send(failed.clone()).await.unwrap();
    actors.send(next.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    worker.advance_work().await.unwrap();
    execute_one(&mut worker).await;
    if phase != "completed" {
        app.store.notify_run_completion(1000).await.unwrap();
    }
    if phase == "paused" || phase == "recovered" {
        worker.advance_work().await.unwrap();
    }
    if phase == "recovered" {
        actors.recover_supervision(actor).await.unwrap();
    }
    std::fs::write(
        ready.with_extension("tmp"),
        serde_json::to_vec(&json!({"actor":actor,"failed":failed.id,"next":next.id})).unwrap(),
    )
    .unwrap();
    std::fs::rename(ready.with_extension("tmp"), ready).unwrap();
    std::future::pending::<()>().await;
}
