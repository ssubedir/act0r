use act0r::{
    Engine, Worker,
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
        ActorService, AgentRegistry, AgentService, TaskInspection, TaskInspectionNode,
        TaskInspectionOptions, ports::*,
    },
    domain::{actors::*, integration::*, *},
};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
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
async fn open(db: &Path) -> App {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let clock = Arc::new(ManualClock(AtomicI64::new(1000)));
    let root = db.parent().unwrap().join("workspaces");
    let registry = Arc::new(
        AgentRegistry::new(vec![
            Arc::new(AnswerAgentAdapter),
            Arc::new(PlannerAgentAdapter::default()),
            Arc::new(UnsafeWorkflow),
        ])
        .unwrap(),
    );
    let mut workflows = registry.workflows();
    workflows.push(Arc::new(TimerDemoWorkflow));
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
fn answer_config(human: bool) -> Value {
    json!({"goal":"Assist","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":8,"max_tokens":200000,"max_output_tokens":512,
        "script":if human {json!([{"kind":"ask_human","question":"May I continue?"},{"kind":"finish","summary":"Child result"}])}
        else {json!([{"kind":"finish","summary":"Child result"}])}})
}
fn planner_config(workers: &[(&str, AgentId)], script: Value) -> Value {
    json!({"goal":"Complete the original goal","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":8,"max_tokens":200000,"max_output_tokens":512,
        "goal_budget":{"max_model_calls":20,"max_tokens":200000},
        "workers":workers.iter().map(|(name,id)|json!({"name":name,"agent_id":id,"description":name})).collect::<Vec<_>>(),
        "delegation":{"max_tasks":8,"max_depth":3,"max_parallel_tasks":4,"shared_budget":{"max_model_calls":20,"max_tokens":200000}},
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
fn message(actor: AgentId, payload: Value) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient: actor,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload,
    }
}
async fn send(app: &App, actor: AgentId, payload: Value) -> MessageId {
    app.agents
        .actors()
        .unwrap()
        .send(message(actor, payload))
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
async fn inspect(app: &App, id: MessageId) -> TaskInspection {
    app.agents
        .actors()
        .unwrap()
        .inspect_task(id, TaskInspectionOptions::default())
        .await
        .unwrap()
}
fn node(view: &TaskInspection, id: MessageId) -> &TaskInspectionNode {
    view.nodes.iter().find(|n| n.task_id == id).unwrap()
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

use std::{
    process::{Command as ProcessCommand, Stdio},
    time::{Duration, Instant},
};
struct UnsafeWorkflow;
impl WorkflowDefinition for UnsafeWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("unsafe_fixture", 1)
    }
    fn transition(&self, _state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let commands = match event {
            WorkflowEvent::Started(_) => vec![
                Command::ConfigureUsage(UsageBudget {
                    max_model_calls: Some(4),
                    max_tokens: Some(10000),
                }),
                Command::Schedule(ActivitySpec {
                    handler: DefinitionId::new("echo_tool", 1),
                    input: json!({"arguments":{"text":"Unsafe"}}),
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
            state: json!({}),
            commands,
        })
    }
}
impl AgentAdapter for UnsafeWorkflow {
    fn registration(&self) -> AgentRegistration {
        AgentRegistration {
            contract_version: 1,
            adapter: WorkflowDefinition::id(self),
            definition: WorkflowDefinition::id(self),
            capabilities: AgentCapabilities {
                checkpoints: CheckpointGranularity::TaskCompletion,
                cooperative_cancellation: true,
                human_input_waits: false,
                usage: UsageReporting::ReportedOrReserved,
                branching: false,
                delegation: false,
                task_groups: false,
            },
        }
    }
    fn definition(&self) -> Arc<dyn WorkflowDefinition> {
        Arc::new(Self)
    }
    fn prepare(&self, config: Value, _: Option<std::path::PathBuf>) -> PortFuture<'_, Value> {
        Box::pin(async move { Ok(config) })
    }
}
async fn saved_run(app: &App, task: MessageId) -> Run {
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
async fn statement(db: &Path, sql: &'static str) {
    let db = pool(db).await;
    sqlx::raw_sql(sql).execute(&db).await.unwrap();
    db.close().await;
}
fn follow() -> Value {
    json!({"kind":"schedule_follow_up","summary":"Saved progress","goal":"Continue later","after_ms":500})
}

#[tokio::test]
async fn cancels_a_whole_goal_from_a_reminder_across_restart_without_affecting_other_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("goal.db");
    let app = open(&db).await;
    let human = create(&app, "answer", answer_config(true)).await;
    let assistant = create(&app, "answer", answer_config(false)).await;
    let scheduler = create(
        &app,
        "planner",
        planner_config(&[("assistant", assistant)], json!([follow()])),
    )
    .await;
    let parent=create(&app,"planner",planner_config(&[("human",human),("scheduler",scheduler)],json!([
        {"kind":"delegate_tasks","tasks":[{"worker":"human","goal":"Review"},{"worker":"scheduler","goal":"Schedule"}]},
        {"kind":"finish","summary":"Combined"}]))).await;
    let root = send(&app, parent, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let waiting = inspect(&app, root).await;
    assert_eq!(waiting.nodes.len(), 4);
    let child = waiting
        .nodes
        .iter()
        .find(|n| n.wait.is_some())
        .unwrap()
        .task_id;
    let next = waiting
        .nodes
        .iter()
        .find(|n| n.status == "scheduled")
        .unwrap()
        .task_id;
    let success = waiting
        .nodes
        .iter()
        .find(|n| n.status == "succeeded")
        .unwrap()
        .task_id;
    let completed = json!(saved_run(&app, success).await);
    let usage = json!(waiting.summary.visible_usage);
    let independent = send(&app, human, json!({"goal":"An independent goal"})).await;
    let cancelled = app
        .agents
        .actors()
        .unwrap()
        .cancel_goal(next)
        .await
        .unwrap();
    assert_eq!(cancelled.receipt.root_task_id, root);
    assert_eq!(cancelled.receipt.requested_task_id, next);
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .reminder(next)
            .await
            .unwrap()
            .status,
        ReminderStatus::Cancelled
    );
    assert_eq!(node(&inspect(&app, root).await, child).status, "cancelled");
    assert_eq!(json!(saved_run(&app, success).await), completed);
    drop(worker);
    app.store.close().await;
    drop(app);
    let app = open(&db).await;
    app.clock.0.store(5000, Ordering::SeqCst);
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let done = inspect(&app, child).await;
    assert_eq!(done.root_task_id, root);
    assert!(done.summary.settled);
    assert_eq!(done.summary.stopped, 3);
    assert_eq!(done.summary.succeeded, 1);
    assert_eq!(json!(done.summary.visible_usage), usage);
    assert_eq!(json!(saved_run(&app, success).await), completed);
    assert_eq!(done.goal_cancellation.as_ref().unwrap(), &cancelled.receipt);
    assert!(matches!(
        app.agents.actors().unwrap().task(next).await,
        Err(act0r::Error::AgentTaskNotFound(_))
    ));
    let unrelated = inspect(&app, independent).await;
    assert!(unrelated.goal_cancellation.is_none());
    let other = &unrelated.nodes[0];
    assert_eq!(other.reason.code, "human_input");
    app.engine
        .signal(
            other.run_id.unwrap(),
            other.wait.as_ref().unwrap().id,
            json!({"answer":"Yes"}),
        )
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(
        inspect(&app, independent).await.nodes[0].status,
        "succeeded"
    );
    let repeated = app
        .agents
        .actors()
        .unwrap()
        .cancel_goal(child)
        .await
        .unwrap();
    assert_eq!(repeated.receipt, cancelled.receipt);
    assert!(repeated.settled);
}

#[tokio::test]
async fn receipt_stops_native_publication_even_before_any_cleanup() {
    for exhausted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("intent.db");
        let app = open(&db).await;
        let child = create(&app, "answer", answer_config(false)).await;
        let parent=create(&app,"planner",planner_config(&[("assistant",child)],json!([
        {"kind":"delegate_task","worker":"assistant","goal":"Help"},{"kind":"finish","summary":"Handled"}]))).await;
        let root = send(&app, parent, json!({})).await;
        let mut worker = app.engine.worker().await.unwrap();
        worker.advance_work().await.unwrap();
        worker.step().await.unwrap();
        let intent = inspect(&app, root).await.edges[0].child_task_id;
        if exhausted {
            // Restore a terminal parent with an unsettled intent: cancellation must
            // settle that intent without invoking an invalid terminal Run control.
            let run = saved_run(&app, root).await;
            let sql = pool(&db).await;
            sqlx::query("UPDATE runs SET status='budget_exhausted',payload=json_set(payload,'$.status','budget_exhausted') WHERE id=?")
            .bind(run.record().id.to_string()).execute(&sql).await.unwrap();
            sql.close().await;
        }
        app.store
            .request_goal_cancellation(intent, 1000)
            .await
            .unwrap();
        app.store
            .advance_delegation(
                1000,
                Box::new(|run, id, event| {
                    if let DelegationEvent::ChildCompleted(outcome) = event {
                        run.task_completed(id, outcome, None, 1000)?;
                    }
                    Ok(())
                }),
            )
            .await
            .unwrap();
        assert!(matches!(
            app.agents.actors().unwrap().task(intent).await,
            Err(act0r::Error::AgentTaskNotFound(_))
        ));
        let run = saved_run(&app, root).await;
        assert_eq!(
            run.record().status,
            if exhausted {
                RunStatus::BudgetExhausted
            } else {
                RunStatus::Cancelled
            }
        );
        assert_eq!(run.record().usage.admitted_model_calls, 1);
        assert_eq!(
            run.record().delegations[0].outcome.as_ref().unwrap()["error"]["code"],
            "cancelled"
        );
        settle(&mut worker).await;
        assert!(app.store.goal_cancellation(root).await.unwrap().settled);
    }
}

#[tokio::test]
async fn receipt_stops_activity_timer_and_reminder_admission_and_pending_registration() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("guards.db")).await;
    let actor = create(&app, "answer", answer_config(false)).await;
    let root = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    app.store
        .request_goal_cancellation(root, 1000)
        .await
        .unwrap();
    assert!(worker.admit().await.unwrap().is_none());
    let run = saved_run(&app, root).await;
    assert_eq!(run.record().status, RunStatus::Cancelled);
    assert!(run.attempts().is_empty());
    let actors = app.agents.actors().unwrap();
    let timer_actor=actors.create(AgentId::new(),DefinitionId::new("workflow_worker",1),
        json!({"workflow":{"name":"timer_demo","version":1},"input":{"message":"Timer","wait_ms":500},"attempt_limit":3})).await.unwrap();
    let timer = send(&app, timer_actor, json!({})).await;
    settle(&mut worker).await;
    app.store
        .request_goal_cancellation(timer, 1000)
        .await
        .unwrap();
    app.clock.0.store(2000, Ordering::SeqCst);
    assert!(worker.fire_due_timer().await.unwrap());
    let saved = saved_run(&app, timer).await;
    assert_eq!(saved.record().status, RunStatus::Cancelled);
    assert_eq!(saved.timers()[0].status, TimerStatus::Scheduled);
    assert!(
        !app.engine
            .history(saved.record().id)
            .await
            .unwrap()
            .iter()
            .any(|e| e.kind == "timer_fired")
    );
    assert!(
        !saved
            .attempts()
            .iter()
            .any(|a| a.status == AttemptStatus::Running)
    );
    let reminder = actors
        .schedule_task(
            message(actor, json!({})),
            TimerDeadline::After { milliseconds: 1 },
        )
        .await
        .unwrap();
    app.store
        .request_goal_cancellation(reminder.id, 2000)
        .await
        .unwrap();
    assert!(app.store.publish_due_actor_reminder(3000).await.unwrap());
    assert_eq!(
        actors.reminder(reminder.id).await.unwrap().status,
        ReminderStatus::Cancelled
    );
    assert!(matches!(
        actors.task(reminder.id).await,
        Err(act0r::Error::AgentTaskNotFound(_))
    ));
    settle(&mut worker).await;
    let planner = create(
        &app,
        "planner",
        planner_config(&[("assistant", actor)], json!([follow()])),
    )
    .await;
    let task = send(&app, planner, json!({})).await;
    worker.advance_work().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
    let finished = saved_run(&app, task).await;
    let next = finished.record().follow_up.as_ref().unwrap().reminder_id;
    app.store
        .request_goal_cancellation(next, 3000)
        .await
        .unwrap();
    app.store.notify_run_completion(3000).await.unwrap();
    let after = saved_run(&app, task).await;
    assert_eq!(after.record().status, RunStatus::Succeeded);
    assert_eq!(after.record().result, finished.record().result);
    assert_eq!(after.record().updated_at, finished.record().updated_at);
    assert_eq!(
        after.record().follow_up.as_ref().unwrap().error.as_deref(),
        Some("cancelled")
    );
    assert_eq!(
        actors.reminder(next).await.unwrap().status,
        ReminderStatus::Cancelled
    );
}

#[tokio::test]
async fn pending_tasks_are_removed_while_mailbox_is_paused_and_completion_cannot_launch_them() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("pending.db")).await;
    let actor = create(&app, "answer", answer_config(true)).await;
    let first = send(&app, actor, json!({"goal":"First"})).await;
    let stopped = send(&app, actor, json!({"goal":"Stop this"})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let active = saved_run(&app, first).await;
    let wait = active.record().wait.as_ref().unwrap().id;
    app.agents.actors().unwrap().pause(actor).await.unwrap();
    app.store
        .request_goal_cancellation(stopped, 1000)
        .await
        .unwrap();
    app.engine
        .signal(active.record().id, wait, json!({"answer":"Yes"}))
        .await
        .unwrap();
    // Bypass coordinator cleanup: a raw completion turn still cannot launch the stopped task.
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
    app.store.notify_run_completion(1000).await.unwrap();
    app.agents.actors().unwrap().resume(actor).await.unwrap();
    let actors = app.agents.actors().unwrap();
    let deferred = Run::start(
        &TimerDemoWorkflow,
        json!({"message":"Pending launch","wait_ms":1}),
        3,
        1000,
    )
    .unwrap();
    let link = AgentRun {
        agent_id: actor,
        run_id: deferred.record().id,
        request_id: stopped,
        correlation_id: stopped,
        completion_message_id: MessageId::new(),
        notified: false,
    };
    let before_state = json!(actors.status(actor).await.unwrap());
    assert!(
        !app.store
            .process_message(
                1000,
                Box::new(move |_, _, _| ActorDecisionOutcome::Apply(ActorEffects {
                    state: json!({}),
                    messages: vec![],
                    runs: vec![(deferred, link)]
                })),
                None,
            )
            .await
            .unwrap()
    );
    assert_eq!(json!(actors.status(actor).await.unwrap()), before_state);
    // Actor service normally removes stopped pending requests before this turn is retried.
    actors.cancel_goal(stopped).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(inspect(&app, stopped).await.nodes[0].status, "cancelled");
    assert!(inspect(&app, stopped).await.nodes[0].run_id.is_none());
    assert_eq!(inspect(&app, first).await.nodes[0].status, "succeeded");
    assert_eq!(actors.queue(actor).await.unwrap().outstanding, 0);
}

#[tokio::test]
async fn in_flight_results_and_charges_survive_goal_cancellation_without_another_decision() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("flight.db")).await;
    let actor = create(&app, "answer", answer_config(true)).await;
    let root = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let accepted = app
        .agents
        .actors()
        .unwrap()
        .cancel_goal(root)
        .await
        .unwrap();
    assert!(!accepted.settled);
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    let once = json!(saved_run(&app, root).await);
    worker.complete(execution, outcome).await.unwrap();
    assert_eq!(json!(saved_run(&app, root).await), once);
    settle(&mut worker).await;
    let run = saved_run(&app, root).await;
    assert_eq!(run.record().status, RunStatus::Cancelled);
    assert_eq!(run.attempts().len(), 1);
    assert!(run.record().wait.is_none());
    assert_eq!(run.record().usage.admitted_model_calls, 1);
    assert_eq!(run.record().usage.charged_tokens(), 20);
    assert!(run.activities()[0].result.is_some());
    assert!(app.store.goal_cancellation(root).await.unwrap().settled);
}

#[tokio::test]
async fn unsafe_uncertainty_requires_reconciliation_and_retry_cannot_restart_a_cancelled_goal() {
    for accept in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let app = open(&dir.path().join("unsafe.db")).await;
        let actor = create(&app, "unsafe_fixture", json!({})).await;
        let root = send(&app, actor, json!({})).await;
        let mut worker = app.engine.worker().await.unwrap();
        worker.advance_work().await.unwrap();
        let execution = worker.admit().await.unwrap().unwrap();
        let receipt = app
            .agents
            .actors()
            .unwrap()
            .cancel_goal(root)
            .await
            .unwrap()
            .receipt;
        assert!(
            !saved_run(&app, root)
                .await
                .record()
                .controls
                .cancel_requested
        );
        worker
            .complete(
                execution.clone(),
                Outcome::Uncertain("Unverified external effect".into()),
            )
            .await
            .unwrap();
        let blocked = saved_run(&app, root).await;
        assert_eq!(blocked.record().status, RunStatus::Blocked);
        assert_eq!(
            blocked.record().error.as_ref().unwrap().code,
            "uncertain_outcome"
        );
        assert_eq!(blocked.record().usage.charged_tokens(), 200);
        assert_eq!(blocked.record().usage.uncertain_calls, 1);
        let view = app.store.goal_cancellation(root).await.unwrap();
        assert_eq!(view.blocked_tasks, 1);
        assert!(!view.settled);
        assert!(app.engine.cancel(execution.run_id).await.is_err());
        assert!(!worker.step().await.unwrap());
        app.engine
            .reconcile(
                execution.run_id,
                ReconciliationRequest {
                    activity_id: execution.activity_id,
                    attempt_id: execution.attempt_id,
                    evidence: "Checked the external operation".into(),
                    decision: if accept {
                        ReconciliationDecision::Accept {
                            result: json!({"verified":true}),
                        }
                    } else {
                        ReconciliationDecision::Retry {}
                    },
                },
            )
            .await
            .unwrap();
        settle(&mut worker).await;
        let done = saved_run(&app, root).await;
        assert_eq!(
            done.record().status,
            if accept {
                RunStatus::Succeeded
            } else {
                RunStatus::Cancelled
            }
        );
        assert_eq!(done.attempts().len(), 1);
        assert_eq!(done.record().usage.charged_tokens(), 200);
        assert_eq!(done.record().reconciliation_receipts.len(), 1);
        let final_view = app.store.goal_cancellation(root).await.unwrap();
        assert!(final_view.settled);
        assert_eq!(final_view.receipt, receipt);
    }
}

#[tokio::test]
async fn a_cancelled_planner_keeps_its_unsafe_child_open_until_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("unsafe-child.db")).await;
    let effect = create(&app, "unsafe_fixture", json!({})).await;
    let planner = create(
        &app,
        "planner",
        planner_config(
            &[("effect", effect)],
            json!([
                {"kind":"delegate_task","worker":"effect","goal":"External operation"},
                {"kind":"finish","summary":"Done"}
            ]),
        ),
    )
    .await;
    let root = send(&app, planner, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    let mut admitted = None;
    for _ in 0..20 {
        worker.advance_work().await.unwrap();
        if let Some(execution) = worker.admit().await.unwrap() {
            if execution.handler == DefinitionId::new("echo_tool", 1) {
                admitted = Some(execution);
                break;
            }
            let outcome = worker.execute(execution.clone()).await.unwrap();
            worker.complete(execution, outcome).await.unwrap();
        }
    }
    let execution = admitted.expect("unsafe child admitted");
    let child = app.engine.status(execution.run_id).await.unwrap();
    assert_eq!(child.record().usage.charged_tokens(), 200);
    app.agents
        .actors()
        .unwrap()
        .cancel_goal(root)
        .await
        .unwrap();
    // The ordinary planner CancelChild path must neither set the unsafe child's
    // Run control nor keep reporting progress while it waits for an outcome.
    assert!(!worker.advance_work().await.unwrap());
    assert!(!app.store.actor_work_pending().await.unwrap());
    assert!(
        !app.engine
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
            Outcome::Uncertain("Unknown child effect".into()),
        )
        .await
        .unwrap();
    assert!(!worker.step().await.unwrap());
    let view = app.store.goal_cancellation(root).await.unwrap();
    assert_eq!(view.blocked_tasks, 1);
    assert!(!view.settled);
    app.engine
        .reconcile(
            execution.run_id,
            ReconciliationRequest {
                activity_id: execution.activity_id,
                attempt_id: execution.attempt_id,
                evidence: "Verified external state".into(),
                decision: ReconciliationDecision::Retry {},
            },
        )
        .await
        .unwrap();
    settle(&mut worker).await;
    let cancelled = inspect(&app, root).await;
    assert!(cancelled.summary.settled);
    assert_eq!(cancelled.summary.stopped, 2);
    let child = app.engine.status(execution.run_id).await.unwrap();
    assert_eq!(child.attempts().len(), 1);
    assert_eq!(child.record().usage.charged_tokens(), 200);
    assert_eq!(child.record().reconciliation_receipts.len(), 1);
    assert!(app.store.goal_cancellation(root).await.unwrap().settled);
}

#[tokio::test]
async fn cancellation_is_not_limited_by_the_inspection_graph_and_does_not_merge_correlations() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("deep.db")).await;
    let mut recipient = create(&app, "answer", answer_config(true)).await;
    let leaf_actor = recipient;
    for _ in 0..130 {
        recipient = app
            .agents
            .actors()
            .unwrap()
            .create(
                AgentId::new(),
                DefinitionId::new("coordinator", 1),
                json!({"worker":recipient}),
            )
            .await
            .unwrap();
    }
    let mut request = message(recipient, json!({}));
    request.correlation_id = Some(MessageId::new());
    let correlation = request.correlation_id;
    let root = app.agents.actors().unwrap().send(request).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let partial = app
        .agents
        .actors()
        .unwrap()
        .inspect_task(
            root,
            TaskInspectionOptions {
                max_nodes: 128,
                max_depth: 128,
            },
        )
        .await
        .unwrap();
    assert_eq!(partial.nodes.len(), 128);
    assert!(!partial.truncations.is_empty());
    let mut other = message(leaf_actor, json!({"parent_id":root,"root_task_id":root}));
    other.correlation_id = correlation;
    let independent = app.agents.actors().unwrap().send(other).await.unwrap();
    app.agents
        .actors()
        .unwrap()
        .cancel_goal(root)
        .await
        .unwrap();
    for _ in 0..400 {
        if !worker.step().await.unwrap() {
            break;
        }
    }
    assert!(app.store.goal_cancellation(root).await.unwrap().settled);
    assert_eq!(
        saved_run(&app, root).await.record().status,
        RunStatus::Cancelled
    );
    let full = app
        .agents
        .actors()
        .unwrap()
        .inspect_task(
            root,
            TaskInspectionOptions {
                max_nodes: 512,
                max_depth: 128,
            },
        )
        .await
        .unwrap();
    // Depth bounds remain inspection-only; even the omitted leaf's execution is cancelled.
    assert!(full.nodes.iter().all(|n| n.status == "cancelled"));
    let unrelated = inspect(&app, independent).await;
    assert_eq!(unrelated.root_task_id, independent);
    assert!(unrelated.goal_cancellation.is_none());
    assert_eq!(unrelated.nodes[0].reason.code, "human_input");
}

#[tokio::test]
async fn cancellation_receipt_and_cleanup_roll_back_independently_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("migration.db");
    let app = open(&db).await;
    let actor = create(&app, "answer", answer_config(true)).await;
    let root = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let before = json!(saved_run(&app, root).await);
    drop(worker);
    app.store.close().await;
    drop(app);
    let app = open(&db).await;
    assert_eq!(json!(saved_run(&app, root).await), before);
    let sql = pool(&db).await;
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&sql)
        .await
        .unwrap();
    sql.close().await;
    assert_eq!(version, 62);
    statement(&db,"CREATE TRIGGER reject_goal BEFORE INSERT ON agent_goal_cancellations BEGIN SELECT RAISE(ABORT,'rollback receipt'); END;").await;
    assert!(
        app.agents
            .actors()
            .unwrap()
            .cancel_goal(root)
            .await
            .is_err()
    );
    assert_eq!(json!(saved_run(&app, root).await), before);
    assert!(inspect(&app, root).await.goal_cancellation.is_none());
    statement(&db,"DROP TRIGGER reject_goal; CREATE TRIGGER reject_cleanup BEFORE UPDATE ON agent_tasks BEGIN SELECT RAISE(ABORT,'rollback cleanup'); END;").await;
    assert!(
        app.agents
            .actors()
            .unwrap()
            .cancel_goal(root)
            .await
            .is_err()
    );
    let latched = inspect(&app, root).await;
    assert!(latched.goal_cancellation.is_some());
    assert_eq!(json!(saved_run(&app, root).await), before);
    statement(&db, "DROP TRIGGER reject_cleanup;").await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        saved_run(&app, root).await.record().status,
        RunStatus::Cancelled
    );
    assert!(app.store.goal_cancellation(root).await.unwrap().settled);
}

#[tokio::test]
async fn unknown_and_cyclic_ownership_reject_cancellation_without_a_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("invalid.db");
    let app = open(&db).await;
    assert!(matches!(
        app.agents
            .actors()
            .unwrap()
            .cancel_goal(MessageId::new())
            .await,
        Err(act0r::Error::AgentTaskNotFound(_))
    ));
    let actor = create(&app, "answer", answer_config(false)).await;
    let root = send(&app, actor, json!({})).await;
    let sql = pool(&db).await;
    sqlx::query(
        "UPDATE agent_tasks SET parent_id=id,payload=json_set(payload,'$.parent_id',id) WHERE id=?",
    )
    .bind(root.to_string())
    .execute(&sql)
    .await
    .unwrap();
    assert!(
        matches!(app.agents.actors().unwrap().cancel_goal(root).await,Err(act0r::Error::Storage(message)) if message.contains("cyclic"))
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_goal_cancellations")
        .fetch_one(&sql)
        .await
        .unwrap();
    assert_eq!(count, 0);
    sql.close().await;
}

#[tokio::test]
async fn a_restored_blocked_run_cannot_resume_execution_after_goal_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("restored.db")).await;
    let actor = create(&app, "answer", answer_config(false)).await;
    let root = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    let run = saved_run(&app, root).await;
    app.store
        .mutate(
            run.record().id,
            Box::new(|r| {
                r.block("missing implementation".into(), 1000);
                Ok(None)
            }),
        )
        .await
        .unwrap();
    let request = app
        .agents
        .actors()
        .unwrap()
        .cancel_goal(root)
        .await
        .unwrap();
    assert_eq!(request.blocked_tasks, 1);
    assert_eq!(
        saved_run(&app, root).await.record().status,
        RunStatus::Blocked
    );
    app.engine.resume(run.record().id).await.unwrap();
    settle(&mut worker).await;
    let done = saved_run(&app, root).await;
    assert_eq!(done.record().status, RunStatus::Cancelled);
    assert!(done.attempts().is_empty());
    assert!(app.store.goal_cancellation(root).await.unwrap().settled);
}

#[tokio::test]
#[ignore = "subprocess goal cancellation persistence fixture"]
async fn goal_cancel_crash_child() {
    let db = std::path::PathBuf::from(std::env::var_os("ACT0R_GOAL_CANCEL_DB").unwrap());
    let stage = std::env::var("ACT0R_GOAL_CANCEL_STAGE").unwrap();
    let app = open(&db).await;
    let actor = if stage == "unsafe" {
        create(&app, "unsafe_fixture", json!({})).await
    } else {
        create(&app, "answer", answer_config(true)).await
    };
    let root = send(&app, actor, json!({})).await;
    let independent_actor = if stage == "unsafe" {
        create(&app, "answer", answer_config(true)).await
    } else {
        actor
    };
    let independent = send(&app, independent_actor, json!({"goal":"Independent"})).await;
    let mut worker = app.engine.worker().await.unwrap();
    if stage == "admitted" || stage == "unsafe" {
        worker.advance_work().await.unwrap();
        worker.admit().await.unwrap().unwrap();
    } else {
        settle(&mut worker).await;
    }
    let receipt = if stage == "cleanup" {
        app.agents
            .actors()
            .unwrap()
            .cancel_goal(root)
            .await
            .unwrap()
            .receipt
    } else {
        app.store
            .request_goal_cancellation(root, 1000)
            .await
            .unwrap()
    };
    std::fs::write(
        db.with_extension("ready"),
        serde_json::to_vec(&json!({"root":root,"independent":independent,"receipt":receipt}))
            .unwrap(),
    )
    .unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}
#[tokio::test]
async fn killed_workers_resume_receipt_cleanup_and_keep_interrupted_charges() {
    for stage in ["receipt", "admitted", "cleanup", "unsafe"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("crash.db");
        let mut process = ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "goal_cancel_crash_child",
                "--nocapture",
            ])
            .env("ACT0R_GOAL_CANCEL_DB", &db)
            .env("ACT0R_GOAL_CANCEL_STAGE", stage)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !db.with_extension("ready").exists() {
            assert!(Instant::now() < deadline, "crash fixture timed out");
            assert!(
                process.try_wait().unwrap().is_none(),
                "crash fixture exited"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let saved: Value =
            serde_json::from_slice(&std::fs::read(db.with_extension("ready")).unwrap()).unwrap();
        let root = serde_json::from_value(saved["root"].clone()).unwrap();
        let independent = serde_json::from_value(saved["independent"].clone()).unwrap();
        let app = open(&db).await;
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let stopped = saved_run(&app, root).await;
        assert_eq!(
            stopped.record().status,
            if stage == "unsafe" {
                RunStatus::Blocked
            } else {
                RunStatus::Cancelled
            }
        );
        assert_eq!(stopped.attempts().len(), 1);
        assert_eq!(stopped.record().usage.admitted_model_calls, 1);
        if stage == "admitted" || stage == "unsafe" {
            assert!(stopped.record().usage.held_tokens > 0);
            assert_eq!(stopped.record().usage.uncertain_calls, 1);
            assert_eq!(stopped.attempts()[0].status, AttemptStatus::Interrupted);
        } else {
            assert_eq!(stopped.record().usage.charged_tokens(), 20);
        }
        if stage == "unsafe" {
            let attempt = &stopped.attempts()[0];
            app.engine
                .reconcile(
                    stopped.record().id,
                    ReconciliationRequest {
                        activity_id: attempt.activity_id,
                        attempt_id: attempt.id,
                        evidence: "Checked the interrupted effect".into(),
                        decision: ReconciliationDecision::Retry {},
                    },
                )
                .await
                .unwrap();
            settle(&mut worker).await;
            let final_run = saved_run(&app, root).await;
            assert_eq!(final_run.record().status, RunStatus::Cancelled);
            assert_eq!(final_run.attempts().len(), 1);
            assert_eq!(final_run.record().usage.charged_tokens(), 200);
        }
        let done = app.store.goal_cancellation(root).await.unwrap();
        assert!(done.settled);
        assert_eq!(json!(done.receipt), saved["receipt"]);
        assert_eq!(
            inspect(&app, independent).await.nodes[0].reason.code,
            "human_input"
        );
    }
}
