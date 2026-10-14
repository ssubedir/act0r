use act0r::{
    Engine, Error,
    adapters::{
        file_lock::FileOwnership,
        sqlite::SqliteStore,
        system_clock::SystemClock,
        tokio_worker::{self, WorkerOptions},
    },
    application::{ActorService, ports::*},
    domain::{actors::*, *},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    process::{Command as ProcessCommand, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, mpsc};

struct ProbeWorkflow;
impl WorkflowDefinition for ProbeWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("probe", 1)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        match event {
            WorkflowEvent::Started(input) => {
                let mut commands = vec![Command::ConfigureUsage(UsageBudget {
                    max_model_calls: Some(10),
                    max_tokens: Some(500),
                })];
                if let Some(budget) = input.get("shared_budget") {
                    commands.push(Command::ConfigureDelegation(delegation::DelegationBudget {
                        max_tasks: 4,
                        max_depth: 2,
                        timeout_ms: None,
                        shared_budget: Some(serde_json::from_value(budget.clone()).unwrap()),
                        max_parallel_tasks: None,
                    }));
                }
                let total = input["total"].as_u64().unwrap_or(2);
                for step in 0..total {
                    commands.push(Command::Schedule(ActivitySpec {
                        handler: DefinitionId::new("probe_step", 1),
                        input: json!({"step":step, "panic":input["panic"]}),
                        retry: RetryPolicy {
                            max_attempts: 3,
                            delay_ms: 1,
                            max_delay_ms: 1,
                        },
                        recovery: if input["unsafe"].as_bool().unwrap_or(false) {
                            RecoveryPolicy::RequiresReconciliation
                        } else {
                            RecoveryPolicy::SafeToRepeat
                        },
                        usage: Some(ModelReservation {
                            input_tokens: 10,
                            output_tokens: 20,
                        }),
                    }));
                }
                if input["timer"].as_bool().unwrap_or(false) {
                    commands.push(Command::StartTimer(TimerSpec::after(0, Value::Null)));
                }
                Ok(Transition {
                    state: json!({"total":total,"done":0}),
                    commands,
                })
            }
            WorkflowEvent::ActivityCompleted { .. } => {
                let done = state["done"].as_u64().unwrap() + 1;
                let mut state = state.clone();
                state["done"] = json!(done);
                let commands = if done == state["total"].as_u64().unwrap() {
                    vec![Command::Complete(json!({"done":done}))]
                } else {
                    vec![]
                };
                Ok(Transition { state, commands })
            }
            WorkflowEvent::TimerFired { .. } => {
                let mut state = state.clone();
                state["timer_fired"] = json!(true);
                Ok(Transition {
                    state,
                    commands: vec![],
                })
            }
            _ => Err(DomainError::InvalidWorkflow(
                "unexpected probe event".into(),
            )),
        }
    }
}

#[derive(Default)]
struct Telemetry {
    active: HashMap<RunId, usize>,
    max: usize,
    calls: Vec<ActivityExecution>,
}
struct ActiveCall(Arc<Mutex<Telemetry>>, RunId);
impl Drop for ActiveCall {
    fn drop(&mut self) {
        let mut stats = self.0.lock().unwrap();
        *stats.active.get_mut(&self.1).unwrap() -= 1;
    }
}
struct GateExecutor {
    gate: Arc<Semaphore>,
    stats: Arc<Mutex<Telemetry>>,
    started: mpsc::UnboundedSender<ActivityExecution>,
}
impl ActivityExecutor for GateExecutor {
    fn supports(&self, id: &DefinitionId) -> bool {
        *id == DefinitionId::new("probe_step", 1)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            {
                let mut stats = self.stats.lock().unwrap();
                let active = stats.active.entry(execution.run_id).or_default();
                *active += 1;
                assert_eq!(*active, 1, "same Run overlapped");
                stats.max = stats.max.max(stats.active.values().sum());
                stats.calls.push(execution.clone());
            }
            let _active = ActiveCall(self.stats.clone(), execution.run_id);
            self.started.send(execution.clone()).unwrap();
            assert!(
                !execution.input["panic"].as_bool().unwrap_or(false),
                "injected executor panic"
            );
            self.gate.clone().acquire_owned().await.unwrap().forget();
            Ok(Outcome::SuccessWithUsage {
                result: json!({"step":execution.input["step"]}),
                usage: TokenUsage {
                    input_tokens: 2,
                    output_tokens: 2,
                },
            })
        })
    }
}
struct App {
    engine: Engine,
    store: Arc<SqliteStore>,
    actors: Arc<ActorService>,
    executor: Arc<GateExecutor>,
    started: mpsc::UnboundedReceiver<ActivityExecution>,
}
async fn app(db: &Path) -> App {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let (sender, started) = mpsc::unbounded_channel();
    let executor = Arc::new(GateExecutor {
        gate: Arc::new(Semaphore::new(0)),
        stats: Arc::new(Mutex::new(Telemetry::default())),
        started: sender,
    });
    let engine = Engine::new(
        store.clone(),
        executor.clone(),
        Arc::new(SystemClock),
        Arc::new(FileOwnership::new(db)),
        vec![Arc::new(ProbeWorkflow)],
    )
    .unwrap();
    let actors = Arc::new(
        ActorService::new(
            engine.clone(),
            store.clone(),
            vec![Arc::new(WorkflowWorkerAgent)],
        )
        .unwrap(),
    );
    let engine = engine.with_coordinator(actors.clone());
    App {
        engine,
        store,
        actors,
        executor,
        started,
    }
}
fn options(limit: usize) -> WorkerOptions {
    WorkerOptions {
        until_idle: true,
        poll_interval: Duration::from_millis(1),
        activity_timeout: Duration::from_secs(30),
        shutdown_grace: Duration::ZERO,
        max_concurrent_activities: limit,
    }
}
async fn started(app: &mut App) -> ActivityExecution {
    tokio::time::timeout(Duration::from_secs(5), app.started.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn finish(task: tokio::task::JoinHandle<Result<(), Error>>) {
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
async fn start(app: &App, input: Value) -> RunId {
    app.engine
        .start(ProbeWorkflow.id(), input, 10)
        .await
        .unwrap()
}
async fn runner(app: &App, limit: usize) -> tokio::task::JoinHandle<Result<(), Error>> {
    let worker = app.engine.worker().await.unwrap();
    tokio::spawn(tokio_worker::run(worker, options(limit)))
}

#[tokio::test]
async fn owned_execution_keeps_database_ownership_until_the_future_stops() {
    let dir = tempfile::tempdir().unwrap();
    let mut app = app(&dir.path().join("ownership.db")).await;
    let id = start(&app, json!({"total":1})).await;
    let mut worker = app.engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let operation = worker.execute(execution);
    drop(worker);
    assert!(matches!(
        app.engine.worker().await,
        Err(Error::Ownership(_))
    ));
    let task = tokio::spawn(operation);
    assert_eq!(started(&mut app).await.run_id, id);
    assert!(matches!(
        app.engine.worker().await,
        Err(Error::Ownership(_))
    ));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let _recovered_worker = app.engine.worker().await.unwrap();
    let run = app.engine.status(id).await.unwrap();
    assert_eq!(run.record().usage.admitted_model_calls, 1);
    assert_eq!(run.record().usage.held_tokens, 30);
    assert_eq!(run.record().usage.uncertain_calls, 1);
    assert!(matches!(
        run.attempts()[0].outcome,
        Some(Outcome::Uncertain(_))
    ));
}

#[tokio::test]
async fn activities_overlap_up_to_the_limit_and_each_run_remains_serial() {
    for limit in [1, 2, 3] {
        let dir = tempfile::tempdir().unwrap();
        let mut app = app(&dir.path().join("overlap.db")).await;
        let mut ids = vec![];
        for _ in 0..5 {
            ids.push(start(&app, json!({})).await);
        }
        let task = runner(&app, limit).await;
        let mut active = HashSet::new();
        for _ in 0..limit {
            assert!(active.insert(started(&mut app).await.run_id));
        }
        let mut running = 0;
        for id in &ids {
            running += app
                .engine
                .status(*id)
                .await
                .unwrap()
                .activities()
                .iter()
                .filter(|a| a.status == ActivityStatus::Running)
                .count();
        }
        assert_eq!(running, limit);
        app.executor.gate.add_permits(10);
        finish(task).await;
        {
            let stats = app.executor.stats.lock().unwrap();
            assert_eq!(stats.max, limit);
            assert_eq!(stats.calls.len(), 10);
            assert_eq!(
                stats
                    .calls
                    .iter()
                    .map(|e| e.attempt_id)
                    .collect::<HashSet<_>>()
                    .len(),
                10
            );
            assert!(stats.active.values().all(|count| *count == 0));
        }
        for id in ids {
            let run = app.engine.status(id).await.unwrap();
            assert_eq!(run.record().status, RunStatus::Succeeded);
            assert_eq!(run.record().usage.admitted_model_calls, 2);
            assert_eq!(run.record().usage.charged_tokens(), 8);
            assert_eq!(run.record().usage.held_tokens, 0);
            assert!(run.activities().iter().all(|a| a.attempts == 1));
        }
    }
}

async fn actor(app: &App) -> AgentId {
    let id = AgentId::new();
    app.actors
        .create(
            id,
            WorkflowWorkerAgent.id(),
            json!({
                "workflow":ProbeWorkflow.id(), "input":{}, "attempt_limit":10,
            }),
        )
        .await
        .unwrap();
    id
}
async fn submit(app: &App, actor: AgentId) -> MessageId {
    app.actors
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

#[tokio::test]
async fn different_actors_overlap_but_tasks_on_one_actor_wait_for_the_saved_reply() {
    let dir = tempfile::tempdir().unwrap();
    let mut app = app(&dir.path().join("actors.db")).await;
    let a = actor(&app).await;
    let b = actor(&app).await;
    let tasks = [
        submit(&app, a).await,
        submit(&app, a).await,
        submit(&app, b).await,
        submit(&app, b).await,
    ];
    let task = runner(&app, 4).await;
    let first = started(&mut app).await;
    let second = started(&mut app).await;
    assert_ne!(first.run_id, second.run_id);
    // The runtime consumes queued messages while both actors' external I/O is pending.
    tokio::time::timeout(Duration::from_secs(5), async {
        while app
            .actors
            .mailbox(a)
            .await
            .unwrap()
            .iter()
            .any(|m| m.status == MessageStatus::Queued)
            || app
                .actors
                .mailbox(b)
                .await
                .unwrap()
                .iter()
                .any(|m| m.status == MessageStatus::Queued)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(app.actors.runs(a).await.unwrap().len(), 1);
    assert_eq!(app.actors.runs(b).await.unwrap().len(), 1);
    assert!(app.actors.task(tasks[1]).await.unwrap().run_id.is_none());
    assert!(app.actors.task(tasks[3]).await.unwrap().run_id.is_none());
    app.executor.gate.add_permits(8);
    finish(task).await;
    assert_eq!(app.executor.stats.lock().unwrap().max, 2);
    for task in tasks {
        let view = app.actors.task(task).await.unwrap();
        assert_eq!(view.task.status, AgentTaskStatus::Succeeded);
        assert_eq!(view.usage.unwrap().admitted_model_calls, 2);
    }
    assert_eq!(app.actors.runs(a).await.unwrap().len(), 2);
    assert_eq!(app.actors.runs(b).await.unwrap().len(), 2);
}

#[tokio::test]
async fn cancellation_preserves_the_in_flight_receipt_and_other_runs_continue() {
    let dir = tempfile::tempdir().unwrap();
    let mut app = app(&dir.path().join("cancel.db")).await;
    let cancelled = start(&app, json!({})).await;
    let normal = start(&app, json!({})).await;
    let task = runner(&app, 2).await;
    started(&mut app).await;
    started(&mut app).await;
    app.engine.cancel(cancelled).await.unwrap();
    app.executor.gate.add_permits(3);
    finish(task).await;
    let run = app.engine.status(cancelled).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Cancelled);
    assert_eq!(run.record().usage.admitted_model_calls, 1);
    assert_eq!(run.record().usage.charged_tokens(), 4);
    assert!(matches!(
        run.attempts()[0].outcome,
        Some(Outcome::SuccessWithUsage { .. })
    ));
    assert_eq!(
        app.engine.status(normal).await.unwrap().record().status,
        RunStatus::Succeeded
    );
}

#[tokio::test]
async fn task_panics_are_saved_as_uncertain_and_other_slots_keep_running() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(&dir.path().join("panic.db")).await;
    let failed = start(&app, json!({"total":1,"panic":true,"unsafe":true})).await;
    let normal = start(&app, json!({"total":1})).await;
    app.executor.gate.add_permits(1);
    finish(runner(&app, 2).await).await;
    let run = app.engine.status(failed).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Blocked);
    assert!(matches!(&run.attempts()[0].outcome,
        Some(Outcome::Uncertain(reason)) if reason.contains("activity task failed")));
    assert_eq!(run.record().usage.held_tokens, 30);
    assert_eq!(run.record().usage.uncertain_calls, 1);
    assert_eq!(
        app.engine.status(normal).await.unwrap().record().status,
        RunStatus::Succeeded
    );
}

#[tokio::test]
async fn every_timed_out_slot_retains_charges_and_obeys_retry_limits() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(&dir.path().join("timeouts.db")).await;
    let ids = [
        start(&app, json!({"total":1})).await,
        start(&app, json!({"total":1})).await,
    ];
    let mut options = options(2);
    options.activity_timeout = Duration::from_millis(30);
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio_worker::run(app.engine.worker().await.unwrap(), options),
    )
    .await
    .unwrap()
    .unwrap();
    for id in ids {
        let run = app.engine.status(id).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Failed);
        assert_eq!(run.record().usage.admitted_model_calls, 3);
        assert_eq!(run.record().usage.held_tokens, 90);
        assert_eq!(run.record().usage.uncertain_calls, 3);
        assert!(
            run.attempts()
                .iter()
                .all(|a| matches!(a.outcome, Some(Outcome::Uncertain(_))))
        );
    }
}

#[tokio::test]
async fn storage_admission_and_timers_exclude_only_their_own_running_run() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(&dir.path().join("guards.db")).await;
    let busy = start(&app, json!({"timer":true})).await;
    let other = start(&app, json!({"timer":true})).await;
    let mut worker = app.engine.worker().await.unwrap();
    assert_eq!(worker.admit().await.unwrap().unwrap().run_id, busy);
    assert!(worker.fire_due_timer().await.unwrap());
    assert_eq!(
        app.engine.status(busy).await.unwrap().timers()[0].status,
        TimerStatus::Scheduled
    );
    assert_eq!(
        app.engine.status(other).await.unwrap().timers()[0].status,
        TimerStatus::Fired
    );
    assert_eq!(worker.admit().await.unwrap().unwrap().run_id, other);
    assert!(worker.admit().await.unwrap().is_none());
    assert!(!worker.fire_due_timer().await.unwrap());
}

#[tokio::test]
async fn shared_call_and_token_caps_remain_atomic_across_simultaneous_admissions() {
    for budget in [
        json!({"max_model_calls":1,"max_tokens":500}),
        json!({"max_model_calls":10,"max_tokens":59}),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("budget.db");
        let app = app(&db).await;
        let root = start(&app, json!({"total":1,"shared_budget":budget})).await;
        let child = start(&app, json!({"total":1})).await;
        // Store-contract fixture: attach a second member to exercise overlapping reservations.
        // Native planner fan-out remains outside the runtime's public workflow contract.
        let pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&db)
                .foreign_keys(true),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent_budget_members(root_run_id,run_id) VALUES(?,?)")
            .bind(root.to_string())
            .bind(child.to_string())
            .execute(&pool)
            .await
            .unwrap();
        let mut worker = app.engine.worker().await.unwrap();
        let now = SystemClock.now_ms();
        let (a, b) = tokio::join!(
            app.store.mutate_due(now, Box::new(move |r| r.admit(now))),
            app.store.mutate_due(now, Box::new(move |r| r.admit(now)))
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(usize::from(a.is_some()) + usize::from(b.is_some()), 1);
        let execution = a.or(b).unwrap();
        let view = &app.engine.shared_budgets(root).await.unwrap()[0];
        assert_eq!(view.record.usage.admitted_model_calls, 1);
        assert_eq!(view.record.usage.held_tokens, 30);
        assert!(view.record.exhaustion.is_some());
        assert_eq!(view.members.len(), 2);
        worker
            .complete(
                execution.clone(),
                Outcome::SuccessWithUsage {
                    result: json!({"step":0}),
                    usage: TokenUsage {
                        input_tokens: 2,
                        output_tokens: 2,
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(
            app.engine
                .status(execution.run_id)
                .await
                .unwrap()
                .record()
                .status,
            RunStatus::Succeeded
        );
        let denied = if execution.run_id == root {
            child
        } else {
            root
        };
        assert_eq!(
            app.engine.status(denied).await.unwrap().record().status,
            RunStatus::BudgetExhausted
        );
        assert!(
            app.engine
                .status(denied)
                .await
                .unwrap()
                .attempts()
                .is_empty()
        );
        let view = &app.engine.shared_budgets(root).await.unwrap()[0];
        assert_eq!(view.record.usage.charged_tokens(), 4);
        assert!(view.record.exhaustion.is_some());
        pool.close().await;
    }
}

#[tokio::test]
async fn killing_a_worker_recovers_all_in_flight_attempts_and_keeps_uncertain_charges() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("crash.db");
    let marker = dir.path().join("started.json");
    let initial = app(&db).await;
    let safe = start(&initial, json!({"total":1})).await;
    let unsafe_run = start(&initial, json!({"total":1,"unsafe":true})).await;
    initial.store.close().await;
    drop(initial);
    let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
    command
        .args(["--ignored", "--exact", "concurrent_crash_child"])
        .env("ACT0R_CONCURRENT_CRASH_DB", &db)
        .env("ACT0R_CONCURRENT_CRASH_MARKER", &marker)
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
        if child.try_wait().unwrap().is_some() {
            panic!("crash fixture exited early");
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("missing in-flight checkpoint");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let saved: Vec<ActivityExecution> =
        serde_json::from_slice(&std::fs::read(marker).unwrap()).unwrap();
    assert_eq!(
        saved.iter().map(|e| e.run_id).collect::<HashSet<_>>(),
        HashSet::from([safe, unsafe_run])
    );
    let recovered = app(&db).await;
    let worker = recovered.engine.worker().await.unwrap();
    for id in [safe, unsafe_run] {
        let run = recovered.engine.status(id).await.unwrap();
        assert_eq!(run.record().usage.admitted_model_calls, 1);
        assert_eq!(run.record().usage.held_tokens, 30);
        assert_eq!(run.record().usage.uncertain_calls, 1);
        assert!(matches!(
            run.attempts()[0].outcome,
            Some(Outcome::Uncertain(_))
        ));
        assert_eq!(
            run.attempts()[0].id,
            saved.iter().find(|e| e.run_id == id).unwrap().attempt_id
        );
    }
    assert_eq!(
        recovered
            .engine
            .status(unsafe_run)
            .await
            .unwrap()
            .record()
            .status,
        RunStatus::Blocked
    );
    recovered.executor.gate.add_permits(1);
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio_worker::run(worker, options(2)),
    )
    .await
    .unwrap()
    .unwrap();
    let run = recovered.engine.status(safe).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().usage.admitted_model_calls, 2);
    assert_eq!(run.record().usage.held_tokens, 30);
    assert_eq!(run.record().usage.charged_tokens(), 34);
    assert_eq!(run.attempts()[1].ordinal, 2);
    assert_eq!(recovered.executor.stats.lock().unwrap().calls.len(), 1);
    let before = json!(run);
    drop(recovered);
    let reopened = app(&db).await;
    assert_eq!(json!(reopened.engine.status(safe).await.unwrap()), before);
}

#[tokio::test]
#[ignore = "child process fixture invoked by the parent crash test"]
async fn concurrent_crash_child() {
    let db = std::env::var_os("ACT0R_CONCURRENT_CRASH_DB").unwrap();
    let marker = std::env::var_os("ACT0R_CONCURRENT_CRASH_MARKER").unwrap();
    let mut app = app(Path::new(&db)).await;
    let _runner = runner(&app, 2).await;
    let first = started(&mut app).await;
    let second = started(&mut app).await;
    let marker = Path::new(&marker);
    let temporary = marker.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec(&[first, second]).unwrap()).unwrap();
    std::fs::rename(temporary, marker).unwrap();
    std::future::pending::<()>().await;
}
