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
    domain::{actors::*, shared_budget::*, *},
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
            Arc::new(ModelRouter::new(None).unwrap()),
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
async fn create(app: &App, name: &str, config: Value) -> AgentId {
    app.agents
        .create_registered_actor(
            app.agents.latest_adapter(name).unwrap(),
            None,
            config,
            20,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap()
}
async fn answer(app: &App) -> AgentId {
    create(app,"answer",json!({"goal":"Answer","target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"script":[{"kind":"finish","summary":"Child result"}]})).await
}
fn follow() -> Value {
    json!({"kind":"schedule_follow_up","summary":"Saved progress","goal":"Continue the goal","after_ms":500})
}
fn config(worker: AgentId, calls: u32, script: Value) -> Value {
    json!({"goal":"Continue the goal","target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":8,"max_tokens":200000,"max_output_tokens":512,
        "workers":[{"name":"assistant","agent_id":worker,"description":"A permitted assistant"}],"delegation":{"max_tasks":8,"max_depth":4,"shared_budget":{"max_model_calls":20,"max_tokens":200000}},
        "goal_budget":{"max_model_calls":calls,"max_tokens":200000},"follow_up":{"max_follow_ups":20,"min_delay_ms":1,"max_delay_ms":1000},"script":script})
}
async fn send(app: &App, actor: AgentId, payload: Value) -> MessageId {
    app.agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload,
        })
        .await
        .unwrap()
}
async fn settle(worker: &mut Worker) {
    for _ in 0..200 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("fixture did not settle");
}
async fn run_for(app: &App, task: MessageId) -> Run {
    app.engine
        .status(
            app.agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .run_id
                .unwrap(),
        )
        .await
        .unwrap()
}
async fn scope(app: &App, run: RunId, root: RunId, kind: SharedBudgetKind) -> SharedBudgetView {
    app.engine
        .shared_budgets(run)
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.record.root_run_id == root && s.record.kind == kind)
        .unwrap()
}
async fn sql(db: &Path, statement: &'static str) {
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(db)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    sqlx::raw_sql(statement).execute(&pool).await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn follow_ups_and_their_children_exhaust_one_goal_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("goal.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let assistant = answer(&app).await;
    let mut cfg = config(
        assistant,
        5,
        json!([{"kind":"delegate_task","worker":"assistant","goal":"Help"},follow()]),
    );
    cfg["supervision"] = json!({"max_consecutive_failures":1});
    let actor = create(&app, "planner", cfg).await;
    let task = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let root = run_for(&app, task).await;
    let successor = root.record().follow_up.as_ref().unwrap().reminder_id;
    let saved = scope(
        &app,
        root.record().id,
        root.record().id,
        SharedBudgetKind::Goal,
    )
    .await;
    assert_eq!(saved.record.usage.admitted_model_calls, 3);
    assert_eq!(saved.remaining_model_calls, Some(2));
    assert_eq!(saved.members.len(), 2);
    assert!(saved.record.exhaustion.is_none());
    let original = json!(root);
    drop(worker);
    app.store.close().await;
    drop(app);
    let app = open(&db, ManualClock::new(5000)).await;
    assert_eq!(
        json!(
            scope(
                &app,
                root.record().id,
                root.record().id,
                SharedBudgetKind::Goal
            )
            .await
        ),
        json!(saved)
    );
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let next = run_for(&app, successor).await;
    assert_eq!(next.record().status, RunStatus::BudgetExhausted);
    assert_eq!(
        next.record().error.as_ref().unwrap().code,
        "goal_model_budget"
    );
    assert_eq!(next.attempts().len(), 1);
    assert!(next.record().follow_up.is_none());
    let total = scope(
        &app,
        next.record().id,
        root.record().id,
        SharedBudgetKind::Goal,
    )
    .await;
    assert_eq!(total.record.usage.admitted_model_calls, 5);
    assert_eq!(total.record.usage.charged_tokens(), 100);
    assert_eq!(total.remaining_model_calls, Some(0));
    assert_eq!(total.members.len(), 4);
    assert_eq!(
        total.record.exhaustion.as_ref().unwrap().run_id,
        next.record().id
    );
    assert_eq!(json!(run_for(&app, task).await), original);
    // Per-Run delegation scopes still renew; the continuing goal scope does not.
    assert_eq!(
        scope(
            &app,
            root.record().id,
            root.record().id,
            SharedBudgetKind::Delegation
        )
        .await
        .record
        .usage
        .admitted_model_calls,
        3
    );
    assert_eq!(
        scope(
            &app,
            next.record().id,
            next.record().id,
            SharedBudgetKind::Delegation
        )
        .await
        .record
        .usage
        .admitted_model_calls,
        2
    );
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .status(actor)
            .await
            .unwrap()
            .record()
            .status,
        AgentStatus::Ready
    );
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .supervision(actor)
            .await
            .unwrap()
            .unwrap()
            .consecutive_failures,
        0
    );
    // A new public task cannot forge continuation metadata or alter the pinned budget.
    let independent=send(&app,actor,json!({"goal_budget":{"max_model_calls":999,"max_tokens":999999},"follow_up_context":{"root_task_id":task},"goal":"Independent goal"})).await;
    settle(&mut worker).await;
    let independent_run = run_for(&app, independent).await;
    assert_eq!(independent_run.record().status, RunStatus::Succeeded);
    let independent_scope = scope(
        &app,
        independent_run.record().id,
        independent_run.record().id,
        SharedBudgetKind::Goal,
    )
    .await;
    assert_eq!(
        independent_scope
            .record
            .usage
            .budget
            .as_ref()
            .unwrap()
            .max_model_calls,
        Some(5)
    );
    assert_eq!(independent_scope.record.usage.admitted_model_calls, 3);
    assert_eq!(
        json!(
            scope(
                &app,
                root.record().id,
                root.record().id,
                SharedBudgetKind::Goal
            )
            .await
        ),
        json!(total)
    );
    app.agents
        .actors()
        .unwrap()
        .cancel_reminder(
            independent_run
                .record()
                .follow_up
                .as_ref()
                .unwrap()
                .reminder_id,
        )
        .await
        .unwrap();
    drop(worker);
    app.store.close().await;
    drop(app);
    let app = open(&db, ManualClock::new(9000)).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        json!(
            scope(
                &app,
                root.record().id,
                root.record().id,
                SharedBudgetKind::Goal
            )
            .await
        ),
        json!(total)
    );
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .tasks(actor)
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn a_delegated_planners_follow_ups_retain_ancestor_and_tighter_goal_scopes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("nested.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let leaf = answer(&app).await;
    let middle = create(&app, "planner", config(leaf, 3, json!([follow()]))).await;
    let mut cfg = config(
        middle,
        4,
        json!([{"kind":"delegate_task","worker":"assistant","goal":"Plan independently"},{"kind":"finish","summary":"Saved child result"}]),
    );
    cfg.as_object_mut().unwrap().remove("follow_up");
    let actor = create(&app, "planner", cfg).await;
    let task = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let root = run_for(&app, task).await;
    let child_task = root.record().delegations[0].id;
    let child = run_for(&app, child_task).await;
    let next_id = child.record().follow_up.as_ref().unwrap().reminder_id;
    assert_eq!(
        scope(
            &app,
            root.record().id,
            root.record().id,
            SharedBudgetKind::Goal
        )
        .await
        .record
        .usage
        .admitted_model_calls,
        3
    );
    app.clock.set(2000);
    settle(&mut worker).await;
    let next = run_for(&app, next_id).await;
    assert_eq!(next.record().status, RunStatus::Succeeded);
    let last_id = next.record().follow_up.as_ref().unwrap().reminder_id;
    drop(worker);
    app.store.close().await;
    drop(app);
    let app = open(&db, ManualClock::new(3000)).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let last = run_for(&app, last_id).await;
    assert_eq!(last.record().status, RunStatus::BudgetExhausted);
    assert_eq!(last.attempts().len(), 0);
    let ancestor = scope(
        &app,
        last.record().id,
        root.record().id,
        SharedBudgetKind::Goal,
    )
    .await;
    let subtree = scope(
        &app,
        last.record().id,
        child.record().id,
        SharedBudgetKind::Goal,
    )
    .await;
    assert_eq!(ancestor.record.usage.admitted_model_calls, 4);
    assert!(ancestor.record.exhaustion.is_some());
    assert_eq!(subtree.record.usage.admitted_model_calls, 2);
    assert_eq!(subtree.remaining_model_calls, Some(1));
    assert!(subtree.record.exhaustion.is_none());
    assert_eq!(subtree.members.len(), 3);
    assert_eq!(ancestor.members.len(), 4);
    assert_eq!(
        run_for(&app, task).await.record().status,
        RunStatus::Succeeded
    );
    assert!(
        app.engine
            .shared_budgets(last.record().id)
            .await
            .unwrap()
            .iter()
            .all(|s| s.record.kind == SharedBudgetKind::Goal
                || s.record.root_run_id == last.record().id)
    );
}

#[tokio::test]
async fn uncertain_usage_is_retained_until_tokens_stop_the_follow_up_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("tokens.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let assistant = answer(&app).await;
    let mut cfg = config(assistant, 50, json!([follow()]));
    cfg["goal_budget"]["max_tokens"] = json!(10000);
    let actor = create(&app, "planner", cfg).await;
    let task = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    for tick in 0..100 {
        if let Some(execution) = worker.admit().await.unwrap() {
            let outcome = worker.execute(execution.clone()).await.unwrap();
            let without_usage = Outcome::Success(outcome.result().unwrap().clone());
            worker
                .complete(execution.clone(), without_usage.clone())
                .await
                .unwrap();
            let saved = json!(app.engine.shared_budgets(execution.run_id).await.unwrap());
            worker
                .complete(execution.clone(), without_usage)
                .await
                .unwrap();
            assert_eq!(
                json!(app.engine.shared_budgets(execution.run_id).await.unwrap()),
                saved
            );
        } else if !worker.advance_work().await.unwrap() {
            app.clock.set(1000 + tick * 1000);
        }
        if app
            .agents
            .actors()
            .unwrap()
            .tasks(actor)
            .await
            .unwrap()
            .iter()
            .any(|t| t.status == AgentTaskStatus::BudgetExhausted)
        {
            break;
        }
    }
    settle(&mut worker).await;
    let root = run_for(&app, task).await;
    let total = scope(
        &app,
        root.record().id,
        root.record().id,
        SharedBudgetKind::Goal,
    )
    .await;
    assert_eq!(root.record().status, RunStatus::Succeeded);
    assert!(total.record.exhaustion.is_some());
    assert!(total.record.usage.held_tokens > 0);
    assert!(total.record.usage.charged_tokens() <= 10000);
    assert_eq!(total.record.usage.actual_input_tokens, 0);
    assert_eq!(total.record.usage.actual_output_tokens, 0);
    assert_eq!(
        total.record.usage.uncertain_calls,
        total.record.usage.admitted_model_calls
    );
    assert!(total.record.usage.admitted_model_calls < 50);
    assert!(total.members.len() >= 2);
    let sum: u64 = total.members.iter().map(|m| m.usage.charged_tokens()).sum();
    assert_eq!(sum, total.record.usage.charged_tokens());
    drop(worker);
    app.store.close().await;
    drop(app);
    let app = open(&db, ManualClock::new(100000)).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        json!(
            scope(
                &app,
                root.record().id,
                root.record().id,
                SharedBudgetKind::Goal
            )
            .await
        ),
        json!(total)
    );
}

#[tokio::test]
async fn follow_up_membership_and_goal_admission_roll_back_with_run_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let assistant = answer(&app).await;
    let actor = create(&app, "planner", config(assistant, 1, json!([follow()]))).await;
    let task = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    assert!(worker.advance_work().await.unwrap());
    let root = run_for(&app, task).await;
    let before = json!(root);
    let budget_before = json!(app.engine.shared_budgets(root.record().id).await.unwrap());
    sql(&db,"CREATE TRIGGER fail_goal BEFORE UPDATE ON agent_budget_scopes WHEN NEW.kind='goal' BEGIN SELECT RAISE(ABORT,'goal rollback'); END;").await;
    assert!(worker.admit().await.is_err());
    assert_eq!(json!(run_for(&app, task).await), before);
    assert_eq!(
        json!(app.engine.shared_budgets(root.record().id).await.unwrap()),
        budget_before
    );
    sql(&db, "DROP TRIGGER fail_goal").await;
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    let admitted = json!(run_for(&app, task).await);
    let reserved = json!(app.engine.shared_budgets(root.record().id).await.unwrap());
    sql(&db,"CREATE TRIGGER fail_goal BEFORE UPDATE ON agent_budget_scopes WHEN NEW.kind='goal' BEGIN SELECT RAISE(ABORT,'goal rollback'); END;").await;
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(json!(run_for(&app, task).await), admitted);
    assert_eq!(
        json!(app.engine.shared_budgets(root.record().id).await.unwrap()),
        reserved
    );
    sql(&db, "DROP TRIGGER fail_goal").await;
    worker.complete(execution, outcome).await.unwrap();
    settle(&mut worker).await;
    let root = run_for(&app, task).await;
    let next_id = root.record().follow_up.as_ref().unwrap().reminder_id;
    app.clock.set(2000);
    assert!(app.store.publish_due_actor_reminder(2000).await.unwrap());
    let actor_before = json!(app.agents.actors().unwrap().status(actor).await.unwrap());
    let total_before = json!(
        scope(
            &app,
            root.record().id,
            root.record().id,
            SharedBudgetKind::Goal
        )
        .await
    );
    sql(&db,"CREATE TRIGGER fail_membership BEFORE INSERT ON agent_budget_members WHEN NEW.kind='goal' BEGIN SELECT RAISE(ABORT,'membership rollback'); END;").await;
    assert!(worker.advance_work().await.is_err());
    assert!(
        app.agents
            .actors()
            .unwrap()
            .task(next_id)
            .await
            .unwrap()
            .run_id
            .is_none()
    );
    assert_eq!(
        json!(app.agents.actors().unwrap().status(actor).await.unwrap()),
        actor_before
    );
    assert_eq!(
        json!(
            scope(
                &app,
                root.record().id,
                root.record().id,
                SharedBudgetKind::Goal
            )
            .await
        ),
        total_before
    );
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .runs(actor)
            .await
            .unwrap()
            .len(),
        1
    );
    sql(&db, "DROP TRIGGER fail_membership").await;
    assert!(worker.advance_work().await.unwrap());
    let next = run_for(&app, next_id).await;
    let before = json!(next);
    let budget_before = json!(app.engine.shared_budgets(next.record().id).await.unwrap());
    sql(&db,"CREATE TRIGGER fail_goal BEFORE UPDATE ON agent_budget_scopes WHEN NEW.kind='goal' BEGIN SELECT RAISE(ABORT,'exhaustion rollback'); END;").await;
    assert!(worker.admit().await.is_err());
    assert_eq!(json!(run_for(&app, next_id).await), before);
    assert_eq!(
        json!(app.engine.shared_budgets(next.record().id).await.unwrap()),
        budget_before
    );
    sql(&db, "DROP TRIGGER fail_goal").await;
    assert!(worker.admit().await.unwrap().is_none());
    settle(&mut worker).await;
    assert_eq!(
        run_for(&app, next_id).await.record().status,
        RunStatus::BudgetExhausted
    );
    assert_eq!(
        app.engine
            .history(next.record().id)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "shared_budget_exhausted")
            .count(),
        1
    );
    assert_eq!(
        scope(
            &app,
            next.record().id,
            root.record().id,
            SharedBudgetKind::Goal
        )
        .await
        .record
        .usage
        .admitted_model_calls,
        1
    );
}

#[tokio::test]
async fn killed_workers_keep_goal_membership_reservations_and_exhaustion() {
    for phase in ["registered", "started", "inflight", "exhausted"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("kill.db");
        let ready = dir.path().join("ready.json");
        let log = std::fs::File::create(dir.path().join("child.log")).unwrap();
        let mut child = ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "goal_budget_kill_fixture",
                "--nocapture",
            ])
            .env("ACT0R_GOAL_DB", &db)
            .env("ACT0R_GOAL_READY", &ready)
            .env("ACT0R_GOAL_PHASE", phase)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(40);
        while !ready.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!(
                    "goal child exited {status}: {}",
                    std::fs::read_to_string(dir.path().join("child.log")).unwrap()
                )
            }
            if Instant::now() > deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("goal child timed out")
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value = serde_json::from_slice(&std::fs::read(ready).unwrap()).unwrap();
        let actor: AgentId = serde_json::from_value(saved["actor"].clone()).unwrap();
        let root: MessageId = serde_json::from_value(saved["root"].clone()).unwrap();
        let next_id: MessageId = serde_json::from_value(saved["next"].clone()).unwrap();
        let app = open(&db, ManualClock::new(10000)).await;
        let root_run = run_for(&app, root).await;
        assert_eq!(
            json!(
                app.engine
                    .shared_budgets(root_run.record().id)
                    .await
                    .unwrap()
            ),
            saved["scopes"]
        );
        let mut worker = app.engine.worker().await.unwrap();
        app.clock.set(11000);
        settle(&mut worker).await;
        let next = run_for(&app, next_id).await;
        let total = scope(
            &app,
            next.record().id,
            root_run.record().id,
            SharedBudgetKind::Goal,
        )
        .await;
        assert_eq!(total.members.len(), 2);
        assert_eq!(total.record.usage.admitted_model_calls, 2);
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .tasks(actor)
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            run_for(&app, root).await.record().status,
            RunStatus::Succeeded
        );
        if phase == "inflight" {
            assert_eq!(next.record().status, RunStatus::BudgetExhausted);
            assert_eq!(next.record().usage.admitted_model_calls, 1);
            assert_eq!(total.record.usage.uncertain_calls, 1);
            assert_eq!(
                total.record.usage.held_tokens,
                saved["held"].as_u64().unwrap()
            );
            assert!(total.record.exhaustion.is_some());
        } else if phase == "exhausted" {
            assert_eq!(next.record().status, RunStatus::BudgetExhausted);
            assert!(total.record.exhaustion.is_some());
        } else {
            assert_eq!(next.record().status, RunStatus::Succeeded);
            // Stop the newly registered successor; cancellation retains all spent usage.
            app.agents
                .actors()
                .unwrap()
                .cancel_reminder(next.record().follow_up.as_ref().unwrap().reminder_id)
                .await
                .unwrap();
            assert_eq!(
                json!(
                    scope(
                        &app,
                        next.record().id,
                        root_run.record().id,
                        SharedBudgetKind::Goal
                    )
                    .await
                ),
                json!(total)
            );
        }
    }
}

#[tokio::test]
async fn simultaneous_group_children_cannot_overspend_their_goal() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("concurrent.db");
    let app = open(&db, ManualClock::new(1000)).await;
    let first = answer(&app).await;
    let second = answer(&app).await;
    let mut cfg = config(
        first,
        2,
        json!([{"kind":"delegate_tasks","tasks":[{"worker":"assistant","goal":"First"},{"worker":"second","goal":"Second"}]},{"kind":"finish","summary":"Joined"}]),
    );
    cfg["workers"].as_array_mut().unwrap().push(
        json!({"name":"second","agent_id":second,"description":"Another permitted assistant"}),
    );
    cfg["delegation"]["max_parallel_tasks"] = json!(2);
    cfg.as_object_mut().unwrap().remove("follow_up");
    let actor = create(&app, "planner", cfg).await;
    let task = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    assert!(worker.advance_work().await.unwrap());
    assert!(worker.step().await.unwrap());
    for _ in 0..30 {
        let children_first = app.agents.actors().unwrap().tasks(first).await.unwrap();
        let children_second = app.agents.actors().unwrap().tasks(second).await.unwrap();
        if children_first.first().is_some_and(|t| t.run_id.is_some())
            && children_second.first().is_some_and(|t| t.run_id.is_some())
        {
            break;
        }
        assert!(worker.advance_work().await.unwrap());
    }
    let root = run_for(&app, task).await;
    assert_eq!(
        scope(
            &app,
            root.record().id,
            root.record().id,
            SharedBudgetKind::Goal
        )
        .await
        .members
        .len(),
        3
    );
    let (a, b) = tokio::join!(
        app.store.mutate_due(1000, Box::new(|r| r.admit(1000))),
        app.store.mutate_due(1000, Box::new(|r| r.admit(1000)))
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(usize::from(a.is_some()) + usize::from(b.is_some()), 1);
    let execution = a.or(b).unwrap();
    let total = scope(
        &app,
        root.record().id,
        root.record().id,
        SharedBudgetKind::Goal,
    )
    .await;
    assert_eq!(total.record.usage.admitted_model_calls, 2);
    assert!(total.record.exhaustion.is_some());
    assert!(total.record.usage.held_tokens > 0);
    let delegated = scope(
        &app,
        root.record().id,
        root.record().id,
        SharedBudgetKind::Delegation,
    )
    .await;
    assert!(delegated.record.exhaustion.is_none());
    assert_eq!(delegated.record.usage.admitted_model_calls, 2);
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        run_for(&app, task).await.record().status,
        RunStatus::BudgetExhausted
    );
    // Two admitted scripted replies each charge ten input and ten output tokens.
    assert_eq!(
        scope(
            &app,
            root.record().id,
            root.record().id,
            SharedBudgetKind::Goal
        )
        .await
        .record
        .usage
        .charged_tokens(),
        40
    );
}

#[tokio::test]
async fn goal_budget_is_opt_in_and_rejects_empty_allowances() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("config.db"), ManualClock::new(1000)).await;
    let assistant = answer(&app).await;
    for (calls, tokens) in [(0, 10000), (2, 0)] {
        let mut cfg = config(assistant, calls, json!([follow()]));
        cfg["goal_budget"]["max_tokens"] = json!(tokens);
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
    let mut cfg = config(
        assistant,
        2,
        json!([{"kind":"finish","summary":"Independent"}]),
    );
    cfg.as_object_mut().unwrap().remove("goal_budget");
    cfg.as_object_mut().unwrap().remove("follow_up");
    cfg["delegation"]
        .as_object_mut()
        .unwrap()
        .remove("shared_budget");
    let actor = create(&app, "planner", cfg).await;
    let task = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let run = run_for(&app, task).await;
    assert!(run.record().goal_budget.is_none());
    assert!(json!(run.record()).get("goal_budget").is_none());
    assert!(
        app.engine
            .shared_budgets(run.record().id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(run.record().status, RunStatus::Succeeded);
}

#[tokio::test]
#[ignore = "subprocess fixture controlled by goal budget recovery test"]
async fn goal_budget_kill_fixture() {
    let db = std::env::var("ACT0R_GOAL_DB").unwrap();
    let ready = std::path::PathBuf::from(std::env::var("ACT0R_GOAL_READY").unwrap());
    let phase = std::env::var("ACT0R_GOAL_PHASE").unwrap();
    let app = open(Path::new(&db), ManualClock::new(1000)).await;
    let assistant = answer(&app).await;
    let mut cfg = config(assistant, 2, json!([follow()]));
    if phase == "exhausted" {
        cfg["script"] = json!([{"kind":"ask_human","question":"Continue?"},follow()]);
    }
    let actor = create(&app, "planner", cfg).await;
    let root = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    if phase == "exhausted" {
        let run = run_for(&app, root).await;
        app.engine
            .signal(
                run.record().id,
                run.record().wait.as_ref().unwrap().id,
                json!("yes"),
            )
            .await
            .unwrap();
        settle(&mut worker).await;
    }
    let root_run = run_for(&app, root).await;
    let next = root_run.record().follow_up.as_ref().unwrap().reminder_id;
    if phase != "registered" {
        app.clock.set(2000);
        app.store.publish_due_actor_reminder(2000).await.unwrap();
        worker.advance_work().await.unwrap();
        if phase == "inflight" {
            worker.admit().await.unwrap().unwrap();
        }
        if phase == "exhausted" {
            settle(&mut worker).await;
        }
    }
    let total = scope(
        &app,
        root_run.record().id,
        root_run.record().id,
        SharedBudgetKind::Goal,
    )
    .await;
    let temporary = ready.with_extension("tmp");
    std::fs::write(&temporary,serde_json::to_vec(&json!({"actor":actor,"root":root,"next":next,"scopes":app.engine.shared_budgets(root_run.record().id).await.unwrap(),"held":total.record.usage.held_tokens})).unwrap()).unwrap();
    std::fs::rename(temporary, ready).unwrap();
    std::future::pending::<()>().await;
}
