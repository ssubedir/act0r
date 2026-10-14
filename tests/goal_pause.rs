use act0r::{
    Engine, Error, Worker,
    adapters::{
        agent_executor::AgentExecutor,
        agents::{AnswerAgentAdapter, PlannerAgentAdapter},
        demo::{DemoExecutor, TimerDemoWorkflow},
        file_lock::FileOwnership,
        model::ModelRouter,
        sqlite::SqliteStore,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{
        ActorService, AgentRegistry, AgentService, TaskInspection, TaskInspectionOptions, ports::*,
    },
    domain::{actors::*, *},
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
struct App {
    engine: Engine,
    agents: AgentService,
    store: Arc<SqliteStore>,
    clock: Arc<ManualClock>,
}
struct UnsafeWorkflow;
impl WorkflowDefinition for UnsafeWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("pause_unsafe", 1)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let commands = match event {
            WorkflowEvent::Started(_) => vec![
                Command::ConfigureUsage(UsageBudget {
                    max_model_calls: Some(4),
                    max_tokens: Some(10000),
                }),
                Command::Schedule(ActivitySpec {
                    handler: DefinitionId::new("echo_tool", 1),
                    input: json!({"arguments":{"text":"effect"}}),
                    retry: RetryPolicy::default(),
                    recovery: RecoveryPolicy::RequiresReconciliation,
                    usage: Some(ModelReservation {
                        input_tokens: 100,
                        output_tokens: 100,
                    }),
                }),
            ],
            WorkflowEvent::ActivityCompleted { result, .. } => {
                vec![Command::Complete(result.clone())]
            }
            _ => return Err(DomainError::InvalidState),
        };
        Ok(Transition {
            state: state.clone(),
            commands,
        })
    }
}
async fn open(db: &Path) -> App {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let clock = Arc::new(ManualClock(AtomicI64::new(1000)));
    let root = db.parent().unwrap().join("workspaces");
    let registry = Arc::new(
        AgentRegistry::new(vec![
            Arc::new(AnswerAgentAdapter),
            Arc::new(PlannerAgentAdapter::default()),
        ])
        .unwrap(),
    );
    let mut workflows = registry.workflows();
    workflows.push(Arc::new(TimerDemoWorkflow));
    workflows.push(Arc::new(UnsafeWorkflow));
    let engine = Engine::new(
        store.clone(),
        Arc::new(AgentExecutor::new(
            Arc::new(ModelRouter::new(None).unwrap()),
            Arc::new(LocalTools::new(root.clone())),
            Arc::new(DemoExecutor),
        )),
        clock.clone(),
        Arc::new(FileOwnership::new(db)),
        workflows,
    )
    .unwrap();
    let actors = Arc::new(
        ActorService::new(
            engine.clone(),
            store.clone(),
            vec![
                Arc::new(RegisteredAgentDefinition),
                Arc::new(CoordinatorAgent),
                Arc::new(WorkflowWorkerAgent),
            ],
        )
        .unwrap()
        .with_agent_registry(registry.clone())
        .with_inspection_reader(store.clone())
        .with_goal_control(store.clone()),
    );
    let engine = engine.with_coordinator(actors.clone());
    let agents = AgentService::new(engine.clone(), Arc::new(LocalWorkspaces::new(root)))
        .with_actors(actors)
        .with_agent_registry(registry);
    App {
        engine,
        agents,
        store,
        clock,
    }
}
fn answer(human: bool) -> Value {
    json!({"goal":"Assist", "target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":8,"max_tokens":200000,"max_output_tokens":512,
        "script": if human { json!([{"kind":"ask_human","question":"Continue?"},{"kind":"finish","summary":"Reviewed"}]) }
        else { json!([{"kind":"finish","summary":"Done"}]) }})
}
fn planner(workers: &[(&str, AgentId)], script: Value) -> Value {
    json!({"goal":"Plan", "target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":8,"max_tokens":200000,"max_output_tokens":512,
        "workers": workers.iter().map(|(name,id)| json!({"name":name,"agent_id":id,"description":name})).collect::<Vec<_>>(),
        "delegation":{"max_tasks":8,"max_depth":4,"max_parallel_tasks":4},
        "goal_budget":{"max_model_calls":20,"max_tokens":200000},
        "follow_up":{"max_follow_ups":1,"min_delay_ms":1,"max_delay_ms":1000},"script":script})
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
async fn special(app: &App, workflow: &str) -> AgentId {
    app.agents.actors().unwrap().create(AgentId::new(), DefinitionId::new("workflow_worker", 1),
        json!({"workflow":{"name":workflow,"version":1},"input":{"message":"Timer","wait_ms":100},"attempt_limit":8})).await.unwrap()
}
fn message(actor: AgentId) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient: actor,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({}),
    }
}
async fn send(app: &App, actor: AgentId) -> MessageId {
    app.agents
        .actors()
        .unwrap()
        .send(message(actor))
        .await
        .unwrap()
}
async fn settle(worker: &mut Worker) {
    for _ in 0..300 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("fixture did not settle");
}
async fn inspect(app: &App, id: MessageId) -> TaskInspection {
    app.agents
        .actors()
        .unwrap()
        .inspect_task(id, TaskInspectionOptions::default())
        .await
        .unwrap()
}
async fn run(app: &App, id: MessageId) -> Run {
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
async fn a_whole_goal_holds_its_human_child_and_overdue_follow_up_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("goal.db");
    let app = open(&db).await;
    let human = create(&app, "answer", answer(true)).await;
    let assistant = create(&app, "answer", answer(false)).await;
    let scheduler = create(&app,"planner",planner(&[("assistant",assistant)],json!([
        {"kind":"schedule_follow_up","summary":"Saved","goal":"Continue later","after_ms":500}
    ]))).await;
    let parent = create(&app,"planner",planner(&[("human",human),("scheduler",scheduler)],json!([
        {"kind":"delegate_tasks","tasks":[{"worker":"human","goal":"Review"},{"worker":"scheduler","goal":"Schedule"}]},
        {"kind":"finish","summary":"Combined"}
    ]))).await;
    let root = send(&app, parent).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let before = inspect(&app, root).await;
    let child = before
        .nodes
        .iter()
        .find(|n| n.wait.is_some())
        .unwrap()
        .task_id;
    let reminder = before
        .nodes
        .iter()
        .find(|n| n.status == "scheduled")
        .unwrap()
        .task_id;
    let source = before
        .nodes
        .iter()
        .find(|n| n.status == "succeeded")
        .unwrap()
        .task_id;
    let source_json = json!(run(&app, source).await);
    let deadline = app
        .agents
        .actors()
        .unwrap()
        .reminder(reminder)
        .await
        .unwrap()
        .due_at;
    let paused = app
        .agents
        .actors()
        .unwrap()
        .pause_goal(reminder)
        .await
        .unwrap();
    assert_eq!(paused.root_task_id, root);
    assert_eq!(paused.revision, 1);
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .pause_goal(child)
            .await
            .unwrap(),
        paused
    );
    let paused_view = inspect(&app, root).await;
    assert!(
        paused_view
            .nodes
            .iter()
            .filter(|n| n.status != "succeeded")
            .all(|n| n.execution_held)
    );
    assert_eq!(json!(run(&app, source).await), source_json);
    drop(worker);
    app.store.close().await;
    drop(app);

    let app = open(&db).await;
    app.clock.0.store(5000, Ordering::SeqCst);
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(app.store.goal_pause(child).await.unwrap(), paused);
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .reminder(reminder)
            .await
            .unwrap()
            .due_at,
        deadline
    );
    assert!(matches!(
        app.agents.actors().unwrap().task(reminder).await,
        Err(Error::AgentTaskNotFound(_))
    ));
    let wait = run(&app, child).await.record().wait.clone().unwrap();
    let child_run = run(&app, child).await.record().id;
    app.engine
        .signal(child_run, wait.id, json!({"answer":"Yes"}))
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(
        run(&app, child).await.record().usage.admitted_model_calls,
        1
    );
    assert_eq!(run(&app, root).await.record().usage.admitted_model_calls, 1);
    assert!(!worker.has_active_runs().await.unwrap());
    // Same correlation and forged parent fields cannot join this ownership tree.
    let mut independent = message(assistant);
    independent.correlation_id = Some(root);
    independent.payload = json!({"parent_id":root,"root_task_id":root});
    let other = app
        .agents
        .actors()
        .unwrap()
        .send(independent)
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(run(&app, other).await.record().status, RunStatus::Succeeded);
    assert!(inspect(&app, other).await.goal_pause.is_none());
    let resumed = app
        .agents
        .actors()
        .unwrap()
        .resume_goal(child)
        .await
        .unwrap();
    assert!(!resumed.paused);
    assert_eq!(resumed.pause, paused.pause);
    assert_eq!(resumed.revision, 2);
    settle(&mut worker).await;
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .resume_goal(root)
            .await
            .unwrap(),
        resumed
    );
    let finished = inspect(&app, root).await;
    assert!(finished.summary.settled);
    assert_eq!(finished.summary.succeeded, 4);
    assert!(finished.nodes.iter().all(|n| !n.execution_held));
    assert_eq!(finished.summary.visible_usage.admitted_model_calls, 6);
    assert_eq!(json!(run(&app, source).await), source_json);
    assert_eq!(run(&app, reminder).await.attempts().len(), 1);
}

#[tokio::test]
async fn held_mailboxes_preserve_task_fifo_and_cancel_overrides_the_hold() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let app = open(&dir.path().join("fifo.db")).await;
        let actor = create(&app, "answer", answer(false)).await;
        let other_actor = create(&app, "answer", answer(false)).await;
        let root = send(&app, actor).await;
        let next = send(&app, actor).await;
        let other = send(&app, other_actor).await;
        let paused = app.agents.actors().unwrap().pause_goal(root).await.unwrap();
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        assert_eq!(run(&app, other).await.record().status, RunStatus::Succeeded);
        assert!(
            app.agents
                .actors()
                .unwrap()
                .task(root)
                .await
                .unwrap()
                .run_id
                .is_none()
        );
        assert!(
            app.agents
                .actors()
                .unwrap()
                .task(next)
                .await
                .unwrap()
                .run_id
                .is_none()
        );
        assert!(!worker.has_active_runs().await.unwrap());
        assert_eq!(app.store.goal_pause(root).await.unwrap(), paused);
        if cancel {
            app.agents
                .actors()
                .unwrap()
                .cancel_goal(root)
                .await
                .unwrap();
            assert!(
                app.agents
                    .actors()
                    .unwrap()
                    .resume_goal(root)
                    .await
                    .is_err()
            );
            assert!(app.agents.actors().unwrap().pause_goal(root).await.is_err());
        } else {
            app.agents
                .actors()
                .unwrap()
                .resume_goal(root)
                .await
                .unwrap();
        }
        settle(&mut worker).await;
        assert_eq!(run(&app, next).await.record().status, RunStatus::Succeeded);
        if cancel {
            assert_eq!(inspect(&app, root).await.nodes[0].status, "cancelled");
        } else {
            assert_eq!(run(&app, root).await.record().status, RunStatus::Succeeded);
        }
    }
}

#[tokio::test]
async fn direct_admission_rolls_back_and_in_flight_outcomes_keep_individual_controls() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("admit.db")).await;
    let actor = create(&app, "answer", answer(true)).await;
    let root = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    let queued = run(&app, root).await;
    app.agents.actors().unwrap().pause_goal(root).await.unwrap();
    assert!(
        app.store
            .mutate(queued.record().id, Box::new(|r| r.admit(1000)))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(json!(run(&app, root).await), json!(queued));
    assert!(!worker.has_active_runs().await.unwrap());
    app.agents
        .actors()
        .unwrap()
        .resume_goal(root)
        .await
        .unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    app.agents.actors().unwrap().pause_goal(root).await.unwrap();
    app.engine.pause(execution.run_id).await.unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    let saved = run(&app, root).await;
    assert_eq!(saved.record().status, RunStatus::Paused);
    assert!(saved.record().wait.is_some());
    assert_eq!(saved.record().usage.charged_tokens(), 20);
    worker.complete(execution.clone(), outcome).await.unwrap();
    assert_eq!(json!(run(&app, root).await), json!(saved));
    app.agents.actors().unwrap().pause(actor).await.unwrap();
    app.agents
        .actors()
        .unwrap()
        .resume_goal(root)
        .await
        .unwrap();
    assert_eq!(json!(run(&app, root).await), json!(saved));
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .status(actor)
            .await
            .unwrap()
            .record()
            .status,
        AgentStatus::Paused
    );
    app.engine.resume(execution.run_id).await.unwrap();
    let wait = run(&app, root).await.record().wait.clone().unwrap();
    app.engine
        .signal(execution.run_id, wait.id, json!({"answer":"Yes"}))
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(run(&app, root).await.record().status, RunStatus::Succeeded);
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .status(actor)
            .await
            .unwrap()
            .record()
            .status,
        AgentStatus::Paused
    );
    app.agents.actors().unwrap().resume(actor).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(run(&app, root).await.attempts().len(), 2);
}

#[tokio::test]
async fn paused_timers_keep_absolute_deadlines_and_individual_run_pause() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("timer.db")).await;
    let actor = special(&app, "timer_demo").await;
    let root = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let saved = run(&app, root).await;
    let timer = saved.timers()[0].clone();
    app.agents.actors().unwrap().pause_goal(root).await.unwrap();
    app.clock.0.store(5000, Ordering::SeqCst);
    assert!(!worker.fire_due_timer().await.unwrap());
    assert_eq!(json!(run(&app, root).await), json!(saved));
    assert!(!worker.has_active_runs().await.unwrap());
    app.engine.pause(saved.record().id).await.unwrap();
    app.agents
        .actors()
        .unwrap()
        .resume_goal(root)
        .await
        .unwrap();
    assert!(!worker.fire_due_timer().await.unwrap());
    app.engine.resume(saved.record().id).await.unwrap();
    settle(&mut worker).await;
    let done = run(&app, root).await;
    assert_eq!(done.record().status, RunStatus::Succeeded);
    assert_eq!(done.timers()[0].id, timer.id);
    assert_eq!(done.timers()[0].due_at, timer.due_at);
    assert_eq!(
        app.engine
            .history(done.record().id)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "timer_fired")
            .count(),
        1
    );
}

#[tokio::test]
async fn unpublished_children_and_delegation_deadlines_wait_until_resume() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("intent.db");
    let app = open(&db).await;
    let child = create(&app, "answer", answer(false)).await;
    let mut config = planner(
        &[("assistant", child)],
        json!([
            {"kind":"delegate_task","worker":"assistant","goal":"Help"},{"kind":"finish","summary":"Handled"}
        ]),
    );
    config["delegation"]["timeout_ms"] = json!(100);
    let actor = create(&app, "planner", config).await;
    let root = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    worker.step().await.unwrap();
    let intent = run(&app, root).await.record().delegations[0].clone();
    app.agents
        .actors()
        .unwrap()
        .pause_goal(intent.id)
        .await
        .unwrap();
    drop(worker);
    app.store.close().await;
    drop(app);
    let app = open(&db).await;
    app.clock.0.store(5000, Ordering::SeqCst);
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let held = run(&app, root).await;
    assert_eq!(held.record().delegations[0].deadline_at, intent.deadline_at);
    assert!(held.record().delegations[0].timed_out_at.is_none());
    assert!(matches!(
        app.agents.actors().unwrap().task(intent.id).await,
        Err(Error::AgentTaskNotFound(_))
    ));
    assert!(!worker.has_active_runs().await.unwrap());
    app.agents
        .actors()
        .unwrap()
        .resume_goal(root)
        .await
        .unwrap();
    settle(&mut worker).await;
    let done = run(&app, root).await;
    assert_eq!(done.record().status, RunStatus::Succeeded);
    assert_eq!(done.record().delegations[0].deadline_at, intent.deadline_at);
    assert_eq!(done.record().delegations[0].timed_out_at, Some(5000));
    assert!(matches!(
        app.agents.actors().unwrap().task(intent.id).await,
        Err(Error::AgentTaskNotFound(_))
    ));
}

#[tokio::test]
async fn unsafe_outcomes_stay_blocked_through_goal_resume_and_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("unsafe.db")).await;
    let actor = special(&app, "pause_unsafe").await;
    let root = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    app.agents.actors().unwrap().pause_goal(root).await.unwrap();
    worker
        .complete(
            execution.clone(),
            Outcome::Uncertain("Unverified effect".into()),
        )
        .await
        .unwrap();
    let blocked = run(&app, root).await;
    assert_eq!(blocked.record().status, RunStatus::Blocked);
    assert_eq!(blocked.record().usage.charged_tokens(), 200);
    app.agents
        .actors()
        .unwrap()
        .resume_goal(root)
        .await
        .unwrap();
    assert_eq!(json!(run(&app, root).await), json!(blocked));
    assert!(!worker.step().await.unwrap());
    app.agents.actors().unwrap().pause_goal(root).await.unwrap();
    app.engine
        .reconcile(
            execution.run_id,
            ReconciliationRequest {
                activity_id: execution.activity_id,
                attempt_id: execution.attempt_id,
                evidence: "Verified repetition is appropriate".into(),
                decision: ReconciliationDecision::Retry {},
            },
        )
        .await
        .unwrap();
    assert!(!worker.step().await.unwrap());
    assert_eq!(run(&app, root).await.attempts().len(), 1);
    assert_eq!(run(&app, root).await.record().usage.charged_tokens(), 200);
    app.agents
        .actors()
        .unwrap()
        .resume_goal(root)
        .await
        .unwrap();
    settle(&mut worker).await;
    let done = run(&app, root).await;
    assert_eq!(done.record().status, RunStatus::Succeeded);
    assert_eq!(done.attempts().len(), 2);
    assert_eq!(done.record().reconciliation_receipts.len(), 1);
    assert_eq!(done.record().usage.charged_tokens(), 400);
}

#[tokio::test]
async fn completed_child_receipts_are_saved_while_its_parent_join_is_held() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("join.db")).await;
    let child = create(&app, "answer", answer(false)).await;
    let parent=create(&app,"planner",planner(&[("assistant",child)],json!([
        {"kind":"delegate_task","worker":"assistant","goal":"Help"},{"kind":"finish","summary":"Combined"}
    ]))).await;
    let root = send(&app, parent).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    worker.step().await.unwrap();
    let child_task = run(&app, root).await.record().delegations[0].id;
    worker.advance_work().await.unwrap();
    worker.advance_work().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    app.agents
        .actors()
        .unwrap()
        .pause_goal(child_task)
        .await
        .unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        run(&app, child_task).await.record().status,
        RunStatus::Succeeded
    );
    assert!(
        run(&app, root).await.record().delegations[0]
            .outcome
            .is_none()
    );
    let independent = send(&app, child).await;
    settle(&mut worker).await;
    assert_eq!(
        run(&app, independent).await.record().status,
        RunStatus::Succeeded
    );
    let held = inspect(&app, root).await;
    assert_eq!(held.summary.visible_usage.admitted_model_calls, 2);
    assert!(
        !held
            .nodes
            .iter()
            .find(|n| n.task_id == child_task)
            .unwrap()
            .execution_held
    );
    assert!(!worker.has_active_runs().await.unwrap());
    app.agents
        .actors()
        .unwrap()
        .resume_goal(root)
        .await
        .unwrap();
    settle(&mut worker).await;
    let done = inspect(&app, root).await;
    assert!(done.summary.settled);
    assert_eq!(done.summary.succeeded, 2);
    assert_eq!(done.summary.visible_usage.admitted_model_calls, 3);
    assert_eq!(run(&app, child_task).await.attempts().len(), 1);
}

#[tokio::test]
async fn a_late_follow_up_can_register_while_held_and_does_not_block_an_independent_reminder() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("late-follow.db")).await;
    let helper = create(&app, "answer", answer(false)).await;
    let actor = create(
        &app,
        "planner",
        planner(
            &[("helper", helper)],
            json!([
                {"kind":"schedule_follow_up","summary":"Saved","goal":"Later","after_ms":500}
            ]),
        ),
    )
    .await;
    let root = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    app.agents.actors().unwrap().pause_goal(root).await.unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
    settle(&mut worker).await;
    let source = run(&app, root).await;
    assert_eq!(source.record().status, RunStatus::Succeeded);
    let reminder = source.record().follow_up.as_ref().unwrap().reminder_id;
    let saved = json!(source);
    app.clock.0.store(5000, Ordering::SeqCst);
    assert!(!worker.step().await.unwrap());
    assert!(!worker.has_active_runs().await.unwrap());
    let manual = app
        .agents
        .actors()
        .unwrap()
        .schedule_task(message(actor), TimerDeadline::At { unix_ms: 5000 })
        .await
        .unwrap();
    assert!(app.store.publish_due_actor_reminder(5000).await.unwrap());
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .reminder(manual.id)
            .await
            .unwrap()
            .status,
        ReminderStatus::Published
    );
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .reminder(reminder)
            .await
            .unwrap()
            .status,
        ReminderStatus::Scheduled
    );
    assert!(inspect(&app, manual.id).await.goal_pause.is_none());
    app.agents
        .actors()
        .unwrap()
        .cancel_goal(manual.id)
        .await
        .unwrap();
    app.agents
        .actors()
        .unwrap()
        .resume_goal(reminder)
        .await
        .unwrap();
    settle(&mut worker).await;
    assert!(inspect(&app, root).await.summary.settled);
    assert_eq!(inspect(&app, root).await.summary.succeeded, 2);
    assert_eq!(json!(run(&app, root).await), saved);
    assert_eq!(run(&app, reminder).await.attempts().len(), 1);
}

#[tokio::test]
async fn goal_holds_are_not_limited_by_the_inspection_graph() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("deep.db")).await;
    let mut actor = create(&app, "answer", answer(true)).await;
    for _ in 0..130 {
        actor = app
            .agents
            .actors()
            .unwrap()
            .create(
                AgentId::new(),
                DefinitionId::new("coordinator", 1),
                json!({"worker":actor}),
            )
            .await
            .unwrap();
    }
    let root = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let task = app.agents.actors().unwrap().task(root).await.unwrap();
    let leaf_run = task.run_id.unwrap();
    let sql = pool(app.store.database_path()).await;
    let leaf: String = sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
        .bind(leaf_run.to_string())
        .fetch_one(&sql)
        .await
        .unwrap();
    sql.close().await;
    let leaf: MessageId = leaf.parse().unwrap();
    app.agents.actors().unwrap().pause_goal(leaf).await.unwrap();
    let wait = run(&app, root).await.record().wait.clone().unwrap();
    app.engine
        .signal(leaf_run, wait.id, json!({"answer":"Yes"}))
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(run(&app, root).await.record().usage.admitted_model_calls, 1);
    let bounded = inspect(&app, root).await;
    assert!(!bounded.truncations.is_empty());
    assert!(bounded.nodes.iter().all(|n| n.execution_held));
    assert!(!worker.has_active_runs().await.unwrap());
    app.agents
        .actors()
        .unwrap()
        .resume_goal(root)
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(run(&app, root).await.record().status, RunStatus::Succeeded);
    assert_eq!(run(&app, root).await.attempts().len(), 2);
}

#[tokio::test]
async fn failed_pause_and_resume_preserve_durable_records_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("upgrade.db");
    let app = open(&db).await;
    let actor = create(&app, "answer", answer(true)).await;
    let root = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let original = json!(run(&app, root).await);
    let other_actor = create(&app, "answer", answer(false)).await;
    let other = send(&app, other_actor).await;
    let cancelled = app
        .agents
        .actors()
        .unwrap()
        .cancel_goal(other)
        .await
        .unwrap()
        .receipt;
    drop(worker);
    app.store.close().await;
    drop(app);
    let sql = pool(&db).await;
    sql.close().await;
    let app = open(&db).await;
    assert_eq!(json!(run(&app, root).await), original);
    assert_eq!(
        app.store.goal_cancellation(other).await.unwrap().receipt,
        cancelled
    );
    let sql = pool(&db).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&sql)
            .await
            .unwrap(),
        62
    );
    sqlx::raw_sql("CREATE TRIGGER reject_pause BEFORE INSERT ON agent_goal_pauses BEGIN SELECT RAISE(ABORT,'pause rollback'); END;")
        .execute(&sql).await.unwrap();
    assert!(app.agents.actors().unwrap().pause_goal(root).await.is_err());
    assert_eq!(app.store.goal_pause(root).await.unwrap().revision, 0);
    assert_eq!(json!(run(&app, root).await), original);
    sqlx::raw_sql("DROP TRIGGER reject_pause;")
        .execute(&sql)
        .await
        .unwrap();
    let paused = app.agents.actors().unwrap().pause_goal(root).await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_resume BEFORE UPDATE ON agent_goal_pauses BEGIN SELECT RAISE(ABORT,'resume rollback'); END;")
        .execute(&sql).await.unwrap();
    assert!(
        app.agents
            .actors()
            .unwrap()
            .resume_goal(root)
            .await
            .is_err()
    );
    assert_eq!(app.store.goal_pause(root).await.unwrap(), paused);
    let mut worker = app.engine.worker().await.unwrap();
    assert!(!worker.step().await.unwrap());
    sqlx::raw_sql("DROP TRIGGER reject_resume;")
        .execute(&sql)
        .await
        .unwrap();
    let resumed = app
        .agents
        .actors()
        .unwrap()
        .resume_goal(root)
        .await
        .unwrap();
    assert_eq!(resumed.revision, 2);
    assert_eq!(resumed.pause, paused.pause);
    assert_eq!(json!(run(&app, root).await), original);
    assert!(
        app.agents
            .actors()
            .unwrap()
            .pause_goal(MessageId::new())
            .await
            .is_err()
    );
    sqlx::query(
        "UPDATE agent_tasks SET parent_id=id,payload=json_set(payload,'$.parent_id',id) WHERE id=?",
    )
    .bind(root.to_string())
    .execute(&sql)
    .await
    .unwrap();
    assert!(
        matches!(app.agents.actors().unwrap().pause_goal(root).await,Err(Error::Storage(message)) if message.contains("cyclic"))
    );
    sql.close().await;
}

#[tokio::test]
#[ignore = "subprocess goal pause persistence fixture"]
async fn goal_pause_crash_child() {
    let db = std::path::PathBuf::from(std::env::var_os("ACT0R_GOAL_PAUSE_DB").unwrap());
    let stage = std::env::var("ACT0R_GOAL_PAUSE_STAGE").unwrap();
    let app = open(&db).await;
    let actor = match stage.as_str() {
        "timer" => special(&app, "timer_demo").await,
        "unsafe" => special(&app, "pause_unsafe").await,
        _ => create(&app, "answer", answer(true)).await,
    };
    let root = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    if stage == "admitted" || stage == "unsafe" {
        worker.advance_work().await.unwrap();
        worker.admit().await.unwrap().unwrap();
    } else {
        settle(&mut worker).await;
    }
    let mut control = app.agents.actors().unwrap().pause_goal(root).await.unwrap();
    if stage == "resume" {
        control = app
            .agents
            .actors()
            .unwrap()
            .resume_goal(root)
            .await
            .unwrap();
    }
    let other_actor = create(&app, "answer", answer(false)).await;
    let other = send(&app, other_actor).await;
    std::fs::write(
        db.with_extension("ready"),
        serde_json::to_vec(&json!({"root":root,"other":other,"control":control})).unwrap(),
    )
    .unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[tokio::test]
async fn killed_workers_keep_holds_resumes_and_interrupted_safe_and_unsafe_operations() {
    for stage in ["wait", "admitted", "timer", "unsafe", "resume"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("crash.db");
        let mut process = ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "goal_pause_crash_child",
                "--nocapture",
            ])
            .env("ACT0R_GOAL_PAUSE_DB", &db)
            .env("ACT0R_GOAL_PAUSE_STAGE", stage)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !db.with_extension("ready").exists() {
            assert!(Instant::now() < deadline, "fixture timed out");
            assert!(process.try_wait().unwrap().is_none(), "fixture exited");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let saved: Value =
            serde_json::from_slice(&std::fs::read(db.with_extension("ready")).unwrap()).unwrap();
        let root: MessageId = serde_json::from_value(saved["root"].clone()).unwrap();
        let other: MessageId = serde_json::from_value(saved["other"].clone()).unwrap();
        let app = open(&db).await;
        app.clock.0.store(5000, Ordering::SeqCst);
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        assert_eq!(
            json!(app.store.goal_pause(root).await.unwrap()),
            saved["control"]
        );
        assert_eq!(run(&app, other).await.record().status, RunStatus::Succeeded);
        let held = run(&app, root).await;
        if stage == "admitted" || stage == "unsafe" {
            assert_eq!(held.attempts().len(), 1);
            assert_eq!(held.attempts()[0].status, AttemptStatus::Interrupted);
            assert!(held.record().usage.held_tokens > 0);
            assert_eq!(held.record().usage.uncertain_calls, 1);
        }
        if stage == "timer" {
            assert_eq!(held.timers()[0].status, TimerStatus::Scheduled);
            assert_eq!(held.timers()[0].due_at, 1100);
        }
        if stage == "unsafe" {
            assert_eq!(held.record().status, RunStatus::Blocked);
        }
        assert!(!worker.has_active_runs().await.unwrap());
        app.agents
            .actors()
            .unwrap()
            .resume_goal(root)
            .await
            .unwrap();
        if stage == "unsafe" {
            assert_eq!(json!(run(&app, root).await), json!(held));
            let attempt = &held.attempts()[0];
            app.engine
                .reconcile(
                    held.record().id,
                    ReconciliationRequest {
                        activity_id: attempt.activity_id,
                        attempt_id: attempt.id,
                        evidence: "Verified interrupted external effect".into(),
                        decision: ReconciliationDecision::Retry {},
                    },
                )
                .await
                .unwrap();
        }
        // Recovery keeps the safe retry deadline; resume does not make it immediately due.
        app.clock.0.store(10000, Ordering::SeqCst);
        settle(&mut worker).await;
        if stage != "timer" && stage != "unsafe" {
            let ready = run(&app, root).await;
            let wait = ready.record().wait.clone().unwrap();
            app.engine
                .signal(ready.record().id, wait.id, json!({"answer":"Yes"}))
                .await
                .unwrap();
            settle(&mut worker).await;
        }
        let done = run(&app, root).await;
        assert_eq!(done.record().status, RunStatus::Succeeded);
        if stage == "admitted" {
            assert_eq!(done.attempts().len(), 3);
            assert_eq!(done.record().usage.uncertain_calls, 1);
        }
        if stage == "unsafe" {
            assert_eq!(done.attempts().len(), 2);
            assert_eq!(done.record().usage.charged_tokens(), 400);
        }
    }
}
