use act0r::{
    Engine, Error, Worker,
    adapters::{
        agent_executor::AgentExecutor,
        agents::{AnswerAgentAdapter, PlannerAgentAdapter},
        demo::DemoExecutor,
        file_lock::FileOwnership,
        model::ModelRouter,
        sqlite::SqliteStore,
        tokio_worker::{self, WorkerOptions},
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{ActorService, AgentRegistry, AgentService, ports::*},
    domain::{actors::*, agent::*, delegation::*, integration::*, planner::*, *},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::Path,
    process::{Command as ProcessCommand, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, mpsc};

struct TestClock(AtomicI64);
impl TestClock {
    fn set(&self, now: i64) {
        self.0.store(now, Ordering::SeqCst);
    }
}
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}
struct Models {
    inner: ModelRouter,
    gates: Mutex<HashMap<String, Arc<Semaphore>>>,
    started: mpsc::UnboundedSender<String>,
}
impl Models {
    fn release(&self, name: &str, n: usize) {
        self.gates
            .lock()
            .unwrap()
            .entry(name.into())
            .or_insert_with(|| Arc::new(Semaphore::new(0)))
            .add_permits(n);
    }
}
impl ModelClient for Models {
    fn generate(&self, request: ModelRequest, activity: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            if request.target.model.starts_with("child") {
                let gate = self
                    .gates
                    .lock()
                    .unwrap()
                    .entry(request.target.model.clone())
                    .or_insert_with(|| Arc::new(Semaphore::new(0)))
                    .clone();
                self.started.send(request.target.model.clone()).unwrap();
                gate.acquire().await.unwrap().forget();
            }
            self.inner.generate(request, activity).await
        })
    }
}
struct App {
    engine: Engine,
    agents: AgentService,
    store: Arc<SqliteStore>,
    clock: Arc<TestClock>,
    models: Arc<Models>,
    started: mpsc::UnboundedReceiver<String>,
}
// An injected non-repeatable effect keeps the join open until an operator resolves it.
struct UnsafeAgent;
impl WorkflowDefinition for UnsafeAgent {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("unsafe_fixture", 1)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let command = match event {
            WorkflowEvent::Started(_) => Command::Schedule(ActivitySpec {
                handler: self.id(),
                input: Value::Null,
                retry: RetryPolicy::default(),
                recovery: RecoveryPolicy::RequiresReconciliation,
                usage: None,
            }),
            WorkflowEvent::ActivityCompleted { result, .. } => Command::Complete(result.clone()),
            _ => return Err(DomainError::InvalidState),
        };
        Ok(Transition {
            state: state.clone(),
            commands: vec![command],
        })
    }
}
impl AgentAdapter for UnsafeAgent {
    fn registration(&self) -> AgentRegistration {
        AgentRegistration {
            contract_version: 1,
            adapter: DefinitionId::new("unsafe_fixture", 1),
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
struct UnsafeExecutor;
impl ActivityExecutor for UnsafeExecutor {
    fn supports(&self, handler: &DefinitionId) -> bool {
        handler == &WorkflowDefinition::id(&UnsafeAgent) || DemoExecutor.supports(handler)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if execution.handler == WorkflowDefinition::id(&UnsafeAgent) {
                Ok(Outcome::Uncertain(
                    "Injected uncertain external effect".into(),
                ))
            } else {
                DemoExecutor.execute(execution).await
            }
        })
    }
}
async fn open_app(db: &Path, planner_installed: bool) -> App {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let root = db.parent().unwrap().join(".act0r/workspaces");
    let workspaces = Arc::new(LocalWorkspaces::new(root.clone()));
    let mut adapters: Vec<Arc<dyn AgentAdapter>> =
        vec![Arc::new(AnswerAgentAdapter), Arc::new(UnsafeAgent)];
    if planner_installed {
        adapters.push(Arc::new(PlannerAgentAdapter::default()));
    }
    let registry = Arc::new(AgentRegistry::new(adapters).unwrap());
    let clock = Arc::new(TestClock(AtomicI64::new(1000)));
    let (sender, started) = mpsc::unbounded_channel();
    let models = Arc::new(Models {
        inner: ModelRouter::new(None).unwrap(),
        gates: Mutex::new(HashMap::new()),
        started: sender,
    });
    let engine = Engine::new(
        store.clone(),
        Arc::new(AgentExecutor::new(
            models.clone(),
            Arc::new(LocalTools::new(root)),
            Arc::new(UnsafeExecutor),
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
        models,
        started,
    }
}
fn answer_config(name: &str, human: bool) -> Value {
    let script = if human {
        json!([{"kind":"ask_human","question":"Continue?"},{"kind":"finish","summary":name}])
    } else {
        json!([{"kind":"finish","summary":name}])
    };
    json!({"goal":"Explain the assigned subtask.","target":{"provider":"scripted","model":name,"endpoint":""},
        "max_model_calls":8,"max_tokens":200000,"max_output_tokens":512,"script":script})
}
fn planner_config(workers: &[(&str, AgentId)], script: Value) -> Value {
    json!({"goal":"Combine the workers' results.","target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":10,"max_tokens":200000,"max_output_tokens":512,
        "workers":workers.iter().map(|(name,id)|json!({"name":name,"agent_id":id,"description":name})).collect::<Vec<_>>(),
        "delegation":{"max_tasks":8,"max_depth":3,"max_parallel_tasks":4,"shared_budget":{"max_model_calls":12,"max_tokens":200000}},
        "script":script})
}
fn group_script(names: &[&str]) -> Value {
    json!([{"kind":"delegate_tasks","tasks":names.iter().map(|name|json!({"worker":name,"goal":format!("Work on {name}.")})).collect::<Vec<_>>()},
        {"kind":"finish","summary":"Combined the ordered child results."}])
}
async fn create(app: &App, name: &str, version: u32, config: Value) -> AgentId {
    app.agents
        .create_registered_actor(
            DefinitionId::new(name, version),
            None,
            config,
            20,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap()
}
async fn submit(app: &App, actor: AgentId) -> MessageId {
    app.agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({}),
        })
        .await
        .unwrap()
}
async fn fixture(
    app: &App,
    human: bool,
    budget: u32,
    deadline: Option<u64>,
) -> (MessageId, AgentId) {
    let a = create(app, "answer", 1, answer_config("child-a", human)).await;
    let b = create(app, "answer", 1, answer_config("child-b", false)).await;
    let mut cfg = planner_config(&[("a", a), ("b", b)], group_script(&["a", "b"]));
    cfg["delegation"]["shared_budget"]["max_model_calls"] = json!(budget);
    if let Some(deadline) = deadline {
        cfg["delegation"]["timeout_ms"] = json!(deadline);
    }
    let planner = create(app, "planner", 3, cfg).await;
    (submit(app, planner).await, planner)
}
async fn intent(worker: &mut Worker, app: &App, task: MessageId) -> RunId {
    for _ in 0..40 {
        if let Some(id) = app
            .agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .run_id
            && !app
                .engine
                .status(id)
                .await
                .unwrap()
                .record()
                .task_groups
                .is_empty()
        {
            return id;
        }
        worker.step().await.unwrap();
    }
    panic!("no durable group");
}
fn options(limit: usize) -> WorkerOptions {
    WorkerOptions {
        until_idle: true,
        poll_interval: Duration::from_millis(1),
        max_concurrent_activities: limit,
        ..WorkerOptions::default()
    }
}
async fn start_runner(app: &App, limit: usize) -> tokio::task::JoinHandle<Result<(), Error>> {
    tokio::spawn(tokio_worker::run(
        app.engine.worker().await.unwrap(),
        options(limit),
    ))
}
async fn finish(task: tokio::task::JoinHandle<Result<(), Error>>) {
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
async fn started(app: &mut App) -> String {
    tokio::time::timeout(Duration::from_secs(5), app.started.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn wait_for(app: &App, run: RunId, predicate: impl Fn(&Run) -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if predicate(&app.engine.status(run).await.unwrap()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
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
async fn parallel_children_join_in_request_order_once_and_a_paused_join_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("join.db");
    let mut app = open_app(&db, true).await;
    let (task, _) = fixture(&app, false, 4, None).await;
    let mut worker = app.engine.worker().await.unwrap();
    let root = intent(&mut worker, &app, task).await;
    let initial = app.engine.status(root).await.unwrap();
    let group = initial.record().task_groups[0].clone();
    drop(worker);
    let runner = start_runner(&app, 2).await;
    let mut started_names = vec![started(&mut app).await, started(&mut app).await];
    started_names.sort();
    assert_eq!(started_names, vec!["child-a", "child-b"]);
    for child in &group.children {
        let child = app.agents.actors().unwrap().task(*child).await.unwrap();
        assert_eq!(child.status, AgentTaskStatus::Running);
    }
    app.engine.pause(root).await.unwrap();
    app.models.release("child-b", 1);
    wait_for(&app, root, |r| r.record().delegations[1].outcome.is_some()).await;
    let partial = app.engine.status(root).await.unwrap();
    assert!(partial.record().task_groups[0].joined_at.is_none());
    assert_eq!(partial.record().usage.admitted_model_calls, 1);
    assert_eq!(partial.record().status, RunStatus::Paused);
    app.models.release("child-a", 1);
    finish(runner).await;
    let joined = app.engine.status(root).await.unwrap();
    assert!(joined.record().task_groups[0].joined_at.is_some());
    assert_eq!(joined.record().status, RunStatus::Paused);
    assert_eq!(joined.record().usage.admitted_model_calls, 1);
    let mut worker = app.engine.worker().await.unwrap();
    let history = app.engine.history(root).await.unwrap();
    let outcome = joined.record().delegations[1].outcome.clone().unwrap();
    let duplicate = group.children[1];
    app.store
        .mutate(
            root,
            Box::new(move |r| {
                r.task_completed(duplicate, outcome, None, 1000)?;
                Ok(None)
            }),
        )
        .await
        .unwrap();
    assert_eq!(app.engine.history(root).await.unwrap().len(), history.len());
    let conflict = joined.record().delegations[0].id;
    assert!(
        app.store
            .mutate(
                root,
                Box::new(move |r| {
                    r.task_completed(conflict, json!({"status":"failed"}), None, 1000)?;
                    Ok(None)
                })
            )
            .await
            .is_err()
    );
    assert!(!worker.step().await.unwrap());
    drop(worker);
    drop(app);
    let reopened = open_app(&db, true).await;
    assert_eq!(
        reopened
            .engine
            .status(root)
            .await
            .unwrap()
            .record()
            .task_groups[0]
            .id,
        group.id
    );
    reopened.engine.resume(root).await.unwrap();
    finish(start_runner(&reopened, 2).await).await;
    let run = reopened.engine.status(root).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    let model = run
        .activities()
        .iter()
        .rev()
        .find(|a| a.spec.handler.name == "agent.model")
        .unwrap();
    let prompt: Value = serde_json::from_str(model.spec.input["prompt"].as_str().unwrap()).unwrap();
    let results = &prompt["observations"][0]["results"];
    assert_eq!(results[0]["task_id"], json!(group.children[0]));
    assert_eq!(results[1]["task_id"], json!(group.children[1]));
    assert_eq!(
        reopened
            .engine
            .history(root)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    let view = reopened.agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(view.task_groups.len(), 1);
    assert_eq!(view.group_children.len(), 2);
    assert_eq!(view.shared_budgets[0].record.usage.admitted_model_calls, 4);
    assert_eq!(view.shared_budgets[0].record.usage.held_tokens, 0);
}

#[tokio::test]
async fn human_waits_keep_the_join_durable_and_do_not_occupy_execution_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("human.db");
    let app = open_app(&db, true).await;
    let (task, _) = fixture(&app, true, 5, None).await;
    app.models.release("child-a", 1);
    app.models.release("child-b", 1);
    finish(start_runner(&app, 1).await).await;
    let parent = app.agents.actors().unwrap().task(task).await.unwrap();
    let root = parent.run_id.unwrap();
    let group = parent.task_groups[0].clone();
    assert!(group.joined_at.is_none());
    let child = app
        .agents
        .actors()
        .unwrap()
        .task(group.children[0])
        .await
        .unwrap();
    assert_eq!(child.status, AgentTaskStatus::Waiting);
    assert!(parent.group_children[1].outcome.is_some());
    let child_run = child.run_id.unwrap();
    let wait = child.wait.unwrap().id;
    drop(app);
    let reopened = open_app(&db, true).await;
    reopened
        .engine
        .signal(child_run, wait, json!({"answer":"continue"}))
        .await
        .unwrap();
    reopened.models.release("child-a", 1);
    finish(start_runner(&reopened, 2).await).await;
    assert_eq!(
        reopened.engine.status(root).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    assert_eq!(
        reopened
            .engine
            .status(root)
            .await
            .unwrap()
            .record()
            .task_groups[0]
            .children,
        group.children
    );
}

#[tokio::test]
async fn failed_and_rejected_children_are_terminal_join_results() {
    let dir = tempfile::tempdir().unwrap();
    let app = open_app(&dir.path().join("failures.db"), true).await;
    let a = create(&app, "answer", 1, answer_config("child-a", false)).await;
    let missing = AgentId::new();
    let mut cfg = planner_config(
        &[("a", a), ("missing", missing)],
        group_script(&["a", "missing"]),
    );
    cfg["delegation"]["shared_budget"]["max_model_calls"] = json!(10);
    assert!(
        app.agents
            .create_registered_actor(
                DefinitionId::new("planner", 3),
                None,
                cfg.clone(),
                20,
                AgentQueuePolicy::default(),
            )
            .await
            .is_err()
    );
    // A stored actor can outlive its permitted worker; exercise runtime rejection too.
    let planner = AgentId::new();
    app.agents
        .actors()
        .unwrap()
        .create(
            planner,
            RegisteredAgentDefinition.id(),
            json!(RegisteredAgentConfig {
                registration: PlannerAgentAdapter::default().registration(),
                input: cfg,
                attempt_limit: 20,
                memory: None
            }),
        )
        .await
        .unwrap();
    let task = submit(&app, planner).await;
    let mut worker = app.engine.worker().await.unwrap();
    let root = intent(&mut worker, &app, task).await;
    while app
        .agents
        .actors()
        .unwrap()
        .task(app.engine.status(root).await.unwrap().record().delegations[0].id)
        .await
        .is_err()
        || app
            .agents
            .actors()
            .unwrap()
            .runs(a)
            .await
            .unwrap()
            .is_empty()
    {
        worker.advance_work().await.unwrap();
    }
    let execution = worker.admit().await.unwrap().unwrap();
    worker
        .complete(
            execution,
            Outcome::Failure(Failure::new("fixture_failure", "failed child", false)),
        )
        .await
        .unwrap();
    for _ in 0..20 {
        if !worker.has_active_runs().await.unwrap() {
            break;
        }
        worker.step().await.unwrap();
    }
    let run = app.engine.status(root).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "failed"
    );
    assert_eq!(
        run.record().delegations[1].outcome.as_ref().unwrap()["status"],
        "rejected"
    );
    assert!(run.record().task_groups[0].joined_at.is_some());
}

#[tokio::test]
async fn cancellation_and_group_deadlines_wait_for_every_in_flight_child_and_retain_usage() {
    for timeout in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut app = open_app(&dir.path().join("cancel.db"), true).await;
        let (task, planner) = fixture(&app, false, 10, timeout.then_some(100)).await;
        let mut worker = app.engine.worker().await.unwrap();
        let root = intent(&mut worker, &app, task).await;
        drop(worker);
        let runner = start_runner(&app, 2).await;
        started(&mut app).await;
        started(&mut app).await;
        if timeout {
            app.clock.set(1100);
        } else {
            app.agents
                .actors()
                .unwrap()
                .cancel_task(task)
                .await
                .unwrap();
        }
        let children = app.engine.status(root).await.unwrap().record().task_groups[0]
            .children
            .clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut cancelled = true;
                for child in &children {
                    cancelled &= app
                        .agents
                        .actors()
                        .unwrap()
                        .task(*child)
                        .await
                        .unwrap()
                        .task
                        .cancel_requested;
                }
                if cancelled {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .queue(planner)
                .await
                .unwrap()
                .outstanding,
            1
        );
        app.models.release("child-a", 1);
        app.models.release("child-b", 1);
        finish(runner).await;
        let run = app.engine.status(root).await.unwrap();
        assert_eq!(
            run.record().status,
            if timeout {
                RunStatus::Succeeded
            } else {
                RunStatus::Cancelled
            }
        );
        assert!(run.record().task_groups[0].joined_at.is_some());
        for child in &run.record().delegations {
            let outcome = child.outcome.as_ref().unwrap();
            assert_eq!(outcome["status"], "cancelled");
            if timeout {
                assert_eq!(outcome["error"]["code"], "delegation_timeout");
                assert_eq!(outcome["child_outcome"]["status"], "cancelled");
            }
            assert_eq!(outcome["usage"]["actual_input_tokens"], 10);
        }
        let scope = &app.engine.shared_budgets(root).await.unwrap()[0];
        assert_eq!(
            scope.record.usage.admitted_model_calls,
            if timeout { 4 } else { 3 }
        );
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .queue(planner)
                .await
                .unwrap()
                .outstanding,
            0
        );
    }
}

#[tokio::test]
async fn parallel_children_share_atomic_call_and_token_allowances() {
    for calls in [2, 3, 4] {
        let dir = tempfile::tempdir().unwrap();
        let app = open_app(&dir.path().join("budget.db"), true).await;
        let (task, _) = fixture(&app, false, calls, None).await;
        app.models.release("child-a", 1);
        app.models.release("child-b", 1);
        finish(start_runner(&app, 3).await).await;
        let parent = app.agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(
            parent.status,
            if calls == 4 {
                AgentTaskStatus::Succeeded
            } else {
                AgentTaskStatus::BudgetExhausted
            }
        );
        assert!(parent.task_groups[0].joined_at.is_some());
        let scope = &parent.shared_budgets[0];
        assert_eq!(scope.record.usage.admitted_model_calls, calls);
        assert_eq!(scope.members.len(), 3);
        assert_eq!(scope.record.usage.held_tokens, 0);
        if calls == 2 {
            assert!(
                parent
                    .group_children
                    .iter()
                    .any(|c| c.outcome.as_ref().unwrap()["status"] == "budget_exhausted")
            );
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let mut app = open_app(&dir.path().join("tokens.db"), true).await;
    let mut answer = answer_config("child-a", false);
    answer["max_output_tokens"] = json!(8000);
    let a = create(&app, "answer", 1, answer.clone()).await;
    answer["target"]["model"] = json!("child-b");
    let b = create(&app, "answer", 1, answer).await;
    let mut cfg = planner_config(&[("a", a), ("b", b)], group_script(&["a", "b"]));
    cfg["delegation"]["shared_budget"]["max_tokens"] = json!(12000);
    let planner = create(&app, "planner", 3, cfg).await;
    let task = submit(&app, planner).await;
    let runner = start_runner(&app, 2).await;
    started(&mut app).await;
    let root = app
        .agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .run_id
        .unwrap();
    wait_for(&app, root, |r| {
        r.record().delegations.iter().any(|d| {
            d.outcome
                .as_ref()
                .is_some_and(|o| o["status"] == "budget_exhausted")
        })
    })
    .await;
    let scopes = app.engine.shared_budgets(root).await.unwrap();
    assert_eq!(scopes[0].record.usage.admitted_model_calls, 2);
    assert!(scopes[0].record.usage.held_tokens > 8000);
    assert!(scopes[0].record.usage.charged_tokens() <= 12000);
    app.models.release("child-a", 1);
    app.models.release("child-b", 1);
    finish(runner).await;
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(view.status, AgentTaskStatus::BudgetExhausted);
    assert!(view.task_groups[0].joined_at.is_some());
    assert_eq!(view.shared_budgets[0].record.usage.held_tokens, 0);
    assert_eq!(view.shared_budgets[0].record.usage.charged_tokens(), 40);
}

#[tokio::test]
async fn group_creation_partial_delivery_and_final_join_rollback_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let app = open_app(&db, true).await;
    let (task, _) = fixture(&app, false, 4, None).await;
    let mut worker = app.engine.worker().await.unwrap();
    while app
        .agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .run_id
        .is_none()
    {
        worker.advance_work().await.unwrap();
    }
    let root = app
        .agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .run_id
        .unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    let conn = pool(&db).await;
    sqlx::query("CREATE TRIGGER reject_group BEFORE INSERT ON agent_task_groups BEGIN SELECT RAISE(ABORT,'group failure'); END").execute(&conn).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    let run = app.engine.status(root).await.unwrap();
    assert!(run.record().task_groups.is_empty());
    assert!(run.record().delegations.is_empty());
    assert!(run.attempts()[0].outcome.is_none());
    sqlx::query("DROP TRIGGER reject_group")
        .execute(&conn)
        .await
        .unwrap();
    worker.complete(execution, outcome).await.unwrap();
    let children = app.engine.status(root).await.unwrap().record().task_groups[0]
        .children
        .clone();
    for _ in 0..50 {
        let mut published = true;
        for child in &children {
            published &= app
                .agents
                .actors()
                .unwrap()
                .task(*child)
                .await
                .ok()
                .and_then(|v| v.run_id)
                .is_some();
        }
        if published {
            break;
        }
        worker.advance_work().await.unwrap();
    }
    app.models.release("child-a", 1);
    app.models.release("child-b", 1);
    for child in &children {
        let execution = worker.admit().await.unwrap().unwrap();
        let outcome = worker.execute(execution.clone()).await.unwrap();
        worker.complete(execution, outcome).await.unwrap();
        let before = json!(app.engine.status(root).await.unwrap());
        sqlx::query("CREATE TRIGGER reject_group_save BEFORE UPDATE ON agent_task_groups BEGIN SELECT RAISE(ABORT,'join failure'); END").execute(&conn).await.unwrap();
        let mut failed = false;
        for _ in 0..20 {
            if worker.advance_work().await.is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed);
        assert_eq!(json!(app.engine.status(root).await.unwrap()), before);
        sqlx::query("DROP TRIGGER reject_group_save")
            .execute(&conn)
            .await
            .unwrap();
        for _ in 0..30 {
            if app
                .engine
                .status(root)
                .await
                .unwrap()
                .record()
                .delegations
                .iter()
                .find(|d| d.id == *child)
                .unwrap()
                .outcome
                .is_some()
            {
                break;
            }
            worker.advance_work().await.unwrap();
        }
    }
    drop(worker);
    finish(start_runner(&app, 2).await).await;
    assert_eq!(
        app.engine.status(root).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    conn.close().await;
}

#[test]
fn one_planner_supports_single_children_and_bounded_distinct_groups() {
    let a = AgentId::new();
    let b = AgentId::new();
    let cfg = planner_config(&[("a", a), ("b", b)], group_script(&["a", "b"]));
    let config: PlannerConfig = serde_json::from_value(cfg.clone()).unwrap();
    assert!(config.validate().is_ok());
    let mut single = cfg.clone();
    single["delegation"]
        .as_object_mut()
        .unwrap()
        .remove("max_parallel_tasks");
    assert!(
        serde_json::from_value::<PlannerConfig>(single.clone())
            .unwrap()
            .validate()
            .is_err()
    );
    single["script"] = json!([{"kind":"delegate_task","worker":"a","goal":"One child"}]);
    assert!(
        serde_json::from_value::<PlannerConfig>(single)
            .unwrap()
            .validate()
            .is_ok()
    );
    for tasks in [
        json!([]),
        json!([{"worker":"a","goal":"one"}]),
        json!([{"worker":"a","goal":"one"},{"worker":"a","goal":"duplicate"}]),
        json!([{"worker":"a","goal":"one"},{"worker":"missing","goal":"unknown"}]),
    ] {
        let mut invalid = cfg.clone();
        invalid["script"][0]["tasks"] = tasks;
        assert!(
            serde_json::from_value::<PlannerConfig>(invalid)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    for limit in [0, 1, 17, 9] {
        let mut invalid = cfg.clone();
        invalid["delegation"]["max_parallel_tasks"] = json!(limit);
        assert!(
            serde_json::from_value::<PlannerConfig>(invalid)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let registration = json!(PlannerAgentAdapter::default().registration());
    assert_eq!(registration["capabilities"]["task_groups"], true);
    assert_eq!(
        PlannerAgentAdapter::default().registration().adapter,
        DefinitionId::new("planner", 3)
    );
    let old = json!({"max_tasks":1,"max_depth":1});
    let budget: DelegationBudget = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(json!(budget), old);
}

#[tokio::test]
async fn nested_groups_keep_ancestor_depth_and_shared_budgets() {
    let dir = tempfile::tempdir().unwrap();
    let app = open_app(&dir.path().join("nested.db"), true).await;
    let a = create(&app, "answer", 1, answer_config("child-a", false)).await;
    let b = create(&app, "answer", 1, answer_config("child-b", false)).await;
    let c = create(&app, "answer", 1, answer_config("child-c", false)).await;
    let nested = create(
        &app,
        "planner",
        3,
        planner_config(&[("a", a), ("b", b)], group_script(&["a", "b"])),
    )
    .await;
    let mut cfg = planner_config(
        &[("nested", nested), ("c", c)],
        group_script(&["nested", "c"]),
    );
    cfg["delegation"]["shared_budget"]["max_model_calls"] = json!(7);
    cfg["delegation"]["max_tasks"] = json!(4);
    let planner = create(&app, "planner", 3, cfg).await;
    let task = submit(&app, planner).await;
    for name in ["child-a", "child-b", "child-c"] {
        app.models.release(name, 1);
    }
    finish(start_runner(&app, 4).await).await;
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(view.status, AgentTaskStatus::Succeeded);
    assert_eq!(view.shared_budgets[0].record.usage.admitted_model_calls, 7);
    assert_eq!(view.shared_budgets[0].members.len(), 5);
    let nested_task = app
        .agents
        .actors()
        .unwrap()
        .task(view.task_groups[0].children[0])
        .await
        .unwrap();
    assert_eq!(nested_task.task_groups.len(), 1);
    assert_eq!(nested_task.shared_budgets.len(), 2);
    assert!(
        nested_task
            .shared_budgets
            .iter()
            .any(|s| s.record.usage.admitted_model_calls == 4)
    );
}

#[tokio::test]
async fn cancelling_before_publication_creates_no_child_and_joins_every_saved_intent() {
    let dir = tempfile::tempdir().unwrap();
    let app = open_app(&dir.path().join("before.db"), true).await;
    let (task, _) = fixture(&app, false, 8, None).await;
    let mut worker = app.engine.worker().await.unwrap();
    let root = intent(&mut worker, &app, task).await;
    let children = app.engine.status(root).await.unwrap().record().task_groups[0]
        .children
        .clone();
    app.agents
        .actors()
        .unwrap()
        .cancel_task(task)
        .await
        .unwrap();
    for _ in 0..30 {
        if !worker.has_active_runs().await.unwrap() {
            break;
        }
        worker.advance_work().await.unwrap();
    }
    let run = app.engine.status(root).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Cancelled);
    assert!(run.record().task_groups[0].joined_at.is_some());
    for id in children {
        assert!(matches!(
            app.agents.actors().unwrap().task(id).await,
            Err(Error::AgentTaskNotFound(_))
        ));
    }
}

#[tokio::test]
async fn missing_group_planner_blocks_continuation_and_restores_without_duplicate_children() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("restore.db");
    let app = open_app(&db, true).await;
    let (task, _) = fixture(&app, false, 8, None).await;
    let mut worker = app.engine.worker().await.unwrap();
    let root = intent(&mut worker, &app, task).await;
    let group = app.engine.status(root).await.unwrap().record().task_groups[0].clone();
    drop(worker);
    drop(app);
    let missing = open_app(&db, false).await;
    finish(start_runner(&missing, 2).await).await;
    assert_eq!(
        missing.engine.status(root).await.unwrap().record().status,
        RunStatus::Blocked
    );
    for id in &group.children {
        assert!(missing.agents.actors().unwrap().task(*id).await.is_err());
    }
    drop(missing);
    let restored = open_app(&db, true).await;
    restored.engine.resume(root).await.unwrap();
    restored.models.release("child-a", 1);
    restored.models.release("child-b", 1);
    finish(start_runner(&restored, 2).await).await;
    assert_eq!(
        restored
            .engine
            .status(root)
            .await
            .unwrap()
            .record()
            .task_groups[0]
            .children,
        group.children
    );
    assert_eq!(
        restored
            .agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status,
        AgentTaskStatus::Succeeded
    );
}

#[tokio::test]
async fn killed_workers_preserve_group_intents_partial_outcomes_and_join_receipts() {
    for phase in [
        "intent",
        "publication",
        "running",
        "partial",
        "joined",
        "expired",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("crash.db");
        let marker = dir.path().join("checkpoint.json");
        let app = open_app(&db, true).await;
        let (task, _) = fixture(&app, false, 8, (phase == "expired").then_some(100)).await;
        app.store.close().await;
        drop(app);
        let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "group_crash_child"])
            .env("ACT0R_GROUP_CRASH_DB", &db)
            .env("ACT0R_GROUP_CRASH_MARKER", &marker)
            .env("ACT0R_GROUP_CRASH_TASK", task.to_string())
            .env("ACT0R_GROUP_CRASH_PHASE", phase)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !marker.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "group crash fixture exited: {phase}"
            );
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("missing group checkpoint: {phase}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value = serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
        let root: RunId = serde_json::from_value(saved["root"].clone()).unwrap();
        let group: TaskGroup = serde_json::from_value(saved["group"].clone()).unwrap();
        let recovered = open_app(&db, true).await;
        recovered.clock.set(2000);
        if phase == "joined" {
            recovered.engine.resume(root).await.unwrap();
        }
        let worker = recovered.engine.worker().await.unwrap();
        recovered.clock.set(3000);
        recovered.models.release("child-a", 3);
        recovered.models.release("child-b", 3);
        tokio::time::timeout(
            Duration::from_secs(10),
            tokio_worker::run(worker, options(2)),
        )
        .await
        .unwrap()
        .unwrap();
        let run = recovered.engine.status(root).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Succeeded, "{phase}");
        assert_eq!(run.record().task_groups[0].id, group.id);
        assert_eq!(run.record().task_groups[0].children, group.children);
        assert_eq!(
            recovered
                .engine
                .history(root)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.kind == "task_group_created")
                .count(),
            1
        );
        assert_eq!(
            recovered
                .engine
                .history(root)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.kind == "task_group_joined")
                .count(),
            1
        );
        let scope = &recovered.engine.shared_budgets(root).await.unwrap()[0];
        let (calls, uncertain) = match phase {
            "running" => (6, 2),
            "partial" => (5, 1),
            "expired" => (2, 0),
            _ => (4, 0),
        };
        assert_eq!(scope.record.usage.admitted_model_calls, calls);
        assert_eq!(scope.record.usage.uncertain_calls, uncertain);
        for id in &group.children {
            if phase == "expired" {
                assert!(recovered.agents.actors().unwrap().task(*id).await.is_err());
            } else {
                let child = recovered.agents.actors().unwrap().task(*id).await.unwrap();
                assert_eq!(child.status, AgentTaskStatus::Succeeded);
                assert_eq!(
                    recovered
                        .agents
                        .actors()
                        .unwrap()
                        .runs(child.task.agent_id)
                        .await
                        .unwrap()
                        .len(),
                    1
                );
            }
        }
        if phase == "partial" {
            assert_eq!(
                run.record().delegations[1].outcome,
                saved["second_outcome"]
                    .as_object()
                    .map(|_| saved["second_outcome"].clone())
            );
        }
        if phase == "joined" {
            assert_eq!(run.record().task_groups[0].joined_at, group.joined_at);
        }
    }
}

#[tokio::test]
async fn an_uncertain_child_keeps_the_join_open_through_deadline_cancellation_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("uncertain.db");
    let app = open_app(&db, true).await;
    let unsafe_actor = create(&app, "unsafe_fixture", 1, json!({})).await;
    let answer = create(&app, "answer", 1, answer_config("child-b", false)).await;
    let mut cfg = planner_config(
        &[("effect", unsafe_actor), ("answer", answer)],
        group_script(&["effect", "answer"]),
    );
    cfg["delegation"]["timeout_ms"] = json!(100);
    let planner = create(&app, "planner", 3, cfg).await;
    let task = submit(&app, planner).await;
    app.models.release("child-b", 1);
    let runner = start_runner(&app, 2).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(root) = app
                .agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .run_id
            {
                let run = app.engine.status(root).await.unwrap();
                if run.record().delegations.len() == 2
                    && run.record().delegations[1].outcome.is_some()
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    // A deadline cannot manufacture a terminal result for an uncertain effect.
    app.clock.set(1100);
    finish(runner).await;
    let root = app
        .agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .run_id
        .unwrap();
    let pending = app.engine.status(root).await.unwrap();
    assert!(pending.record().task_groups[0].joined_at.is_none());
    assert!(pending.record().delegations[0].timed_out_at.is_some());
    let child = pending.record().delegations[0].id;
    let child_run = app
        .agents
        .actors()
        .unwrap()
        .task(child)
        .await
        .unwrap()
        .run_id
        .unwrap();
    let blocked = app.engine.status(child_run).await.unwrap();
    assert_eq!(blocked.record().status, RunStatus::Blocked);
    let attempt = blocked.attempts()[0].clone();
    let blocked_json = json!(blocked);
    app.store.close().await;
    drop(app);

    let restored = open_app(&db, true).await;
    restored.clock.set(1200);
    restored
        .agents
        .actors()
        .unwrap()
        .cancel_task(task)
        .await
        .unwrap();
    finish(start_runner(&restored, 2).await).await;
    assert_eq!(
        json!(restored.engine.status(child_run).await.unwrap()),
        blocked_json
    );
    assert!(
        restored
            .engine
            .status(root)
            .await
            .unwrap()
            .record()
            .task_groups[0]
            .joined_at
            .is_none()
    );
    assert_eq!(
        restored
            .agents
            .actors()
            .unwrap()
            .queue(planner)
            .await
            .unwrap()
            .outstanding,
        1
    );
    restored
        .engine
        .reconcile(
            child_run,
            ReconciliationRequest {
                activity_id: attempt.activity_id,
                attempt_id: attempt.id,
                decision: ReconciliationDecision::Fail {
                    message: "Operator verified the uncertain effect and stopped it.".into(),
                },
                evidence: "Inspected the effect's external record.".into(),
            },
        )
        .await
        .unwrap();
    finish(start_runner(&restored, 2).await).await;
    let done = restored.engine.status(root).await.unwrap();
    assert_eq!(done.record().status, RunStatus::Cancelled);
    assert!(done.record().task_groups[0].joined_at.is_some());
    assert_eq!(
        done.record().delegations[0].outcome.as_ref().unwrap()["child_outcome"]["status"],
        "failed"
    );
    assert_eq!(
        done.record().delegations[1].outcome.as_ref().unwrap()["status"],
        "succeeded"
    );
    assert_eq!(
        restored
            .agents
            .actors()
            .unwrap()
            .queue(planner)
            .await
            .unwrap()
            .outstanding,
        0
    );
}

#[tokio::test]
async fn independent_wait_cycles_are_detected_through_the_second_group_branch() {
    let dir = tempfile::tempdir().unwrap();
    let app = open_app(&dir.path().join("cycles.db"), true).await;
    let a = AgentId::new();
    let c = AgentId::new();
    let b = create(&app, "answer", 1, answer_config("child-b", false)).await;
    let actors = app.agents.actors().unwrap();
    let mut c_config = planner_config(
        &[("a", a)],
        json!([
            {"kind":"delegate_task","worker":"a","goal":"Ask A."}, {"kind":"finish","summary":"C finished."}
        ]),
    );
    c_config["delegation"]
        .as_object_mut()
        .unwrap()
        .remove("max_parallel_tasks");
    for (id, adapter, config) in [
        (
            a,
            PlannerAgentAdapter::default().registration(),
            planner_config(&[("b", b), ("c", c)], group_script(&["b", "c"])),
        ),
        (c, PlannerAgentAdapter::default().registration(), c_config),
    ] {
        actors
            .create(
                id,
                RegisteredAgentDefinition.id(),
                json!(RegisteredAgentConfig {
                    registration: adapter,
                    input: config,
                    attempt_limit: 20,
                    memory: None
                }),
            )
            .await
            .unwrap();
    }
    actors.pause(c).await.unwrap();
    // C's independent task precedes the delegated request. Its ancestry is empty.
    let independent = submit(&app, c).await;
    let root_task = submit(&app, a).await;
    let mut worker = app.engine.worker().await.unwrap();
    let root = intent(&mut worker, &app, root_task).await;
    assert_eq!(
        app.engine.status(root).await.unwrap().record().delegations[1].recipient,
        c
    );
    actors.resume(c).await.unwrap();
    app.models.release("child-b", 2);
    drop(worker);
    finish(start_runner(&app, 3).await).await;
    let independent_run = actors.task(independent).await.unwrap().run_id.unwrap();
    let run = app.engine.status(independent_run).await.unwrap();
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["error"]["code"],
        "delegation_cycle"
    );
    assert_eq!(
        app.engine.status(root).await.unwrap().record().status,
        RunStatus::Succeeded
    );
}

#[tokio::test]
#[ignore = "child process fixture invoked by the parent group recovery test"]
async fn group_crash_child() {
    let db = std::env::var_os("ACT0R_GROUP_CRASH_DB").unwrap();
    let marker = std::env::var_os("ACT0R_GROUP_CRASH_MARKER").unwrap();
    let phase = std::env::var("ACT0R_GROUP_CRASH_PHASE").unwrap();
    let task = std::env::var("ACT0R_GROUP_CRASH_TASK")
        .unwrap()
        .parse()
        .unwrap();
    let mut app = open_app(Path::new(&db), true).await;
    let mut worker = app.engine.worker().await.unwrap();
    let root = intent(&mut worker, &app, task).await;
    if phase == "publication" {
        worker.advance_work().await.unwrap();
    }
    if matches!(phase.as_str(), "running" | "partial" | "joined") {
        drop(worker);
        let runner = start_runner(&app, 2).await;
        started(&mut app).await;
        started(&mut app).await;
        if phase == "partial" {
            app.models.release("child-b", 1);
            wait_for(&app, root, |r| r.record().delegations[1].outcome.is_some()).await;
        }
        if phase == "joined" {
            app.engine.pause(root).await.unwrap();
            app.models.release("child-a", 1);
            app.models.release("child-b", 1);
            finish(runner).await;
        }
    }
    let run = app.engine.status(root).await.unwrap();
    let marker = Path::new(&marker);
    let temporary = marker.with_extension("tmp");
    std::fs::write(&temporary,serde_json::to_vec(&json!({"root":root,"group":run.record().task_groups[0],"second_outcome":run.record().delegations[1].outcome})).unwrap()).unwrap();
    std::fs::rename(temporary, marker).unwrap();
    std::future::pending::<()>().await;
}
