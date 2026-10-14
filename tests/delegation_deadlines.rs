use act0r::{
    Engine, Worker,
    adapters::{
        agent_executor::AgentExecutor,
        agents::{AnswerAgentAdapter, CodingAgentAdapter, PlannerAgentAdapter},
        demo::DemoExecutor,
        file_lock::FileOwnership,
        model::ModelRouter,
        sqlite::SqliteStore,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{
        ActorService, AgentRegistry, AgentService,
        ports::{AgentAdapter, Clock},
    },
    domain::{
        actors::*,
        delegation::DelegationBudget,
        integration::{AgentInvocation, RegisteredAgentWorkflow},
        planner::PlannerWorkflow,
        *,
    },
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
impl ManualClock {
    fn new(now: i64) -> Self {
        Self(AtomicI64::new(now))
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
async fn app(db: &Path, clock: Arc<ManualClock>, with_planner: bool) -> (Engine, AgentService) {
    app_adapters(db, clock, with_planner, true).await
}
async fn app_adapters(
    db: &Path,
    clock: Arc<ManualClock>,
    with_planner: bool,
    with_answer: bool,
) -> (Engine, AgentService) {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let root = db.parent().unwrap().join(".act0r/workspaces");
    let workspaces = Arc::new(LocalWorkspaces::new(root.clone()));
    let mut adapters: Vec<Arc<dyn AgentAdapter>> =
        vec![Arc::new(CodingAgentAdapter::new(workspaces.clone()))];
    if with_answer {
        adapters.push(Arc::new(AnswerAgentAdapter));
    }
    if with_planner {
        adapters.push(Arc::new(PlannerAgentAdapter::default()));
    }
    let registry = Arc::new(AgentRegistry::new(adapters).unwrap());
    let engine = Engine::new(
        store.clone(),
        Arc::new(AgentExecutor::new(
            Arc::new(ModelRouter::new(None).unwrap()),
            Arc::new(LocalTools::new(root)),
            Arc::new(DemoExecutor),
        )),
        clock,
        Arc::new(FileOwnership::new(db)),
        registry.workflows(),
    )
    .unwrap();
    let actors = Arc::new(
        ActorService::new(
            engine.clone(),
            store,
            vec![Arc::new(RegisteredAgentDefinition)],
        )
        .unwrap()
        .with_agent_registry(registry.clone()),
    );
    let engine = engine.with_coordinator(actors.clone());
    let agents = AgentService::new(engine.clone(), workspaces)
        .with_actors(actors)
        .with_agent_registry(registry);
    (engine, agents)
}
fn config(worker: AgentId, timeout: Option<u64>) -> Value {
    json!({"goal":"Use an assistant and handle its outcome.","target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":6,"max_tokens":100000,"max_output_tokens":512,
        "workers":[{"name":"assistant","agent_id":worker,"description":"A permitted worker"}],"delegation":{"max_tasks":4,"max_depth":4,"timeout_ms":timeout},
        "script":[{"kind":"delegate_task","worker":"assistant","goal":"Explain durable execution."},{"kind":"finish","summary":"Handled the saved child outcome."}]})
}
async fn planner(agents: &AgentService, worker: AgentId, timeout: Option<u64>) -> AgentId {
    agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            config(worker, timeout),
            10,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap()
}
async fn answer(agents: &AgentService, human: bool) -> AgentId {
    let mut config: Value =
        serde_json::from_str(include_str!("../examples/configs/answer-scripted.json")).unwrap();
    if !human {
        config["script"] = json!([{"kind":"finish","summary":"Child finished."}]);
    }
    agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            config,
            10,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap()
}
async fn submit(agents: &AgentService, actor: AgentId) -> MessageId {
    agents
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
// Drain work that can progress at the manual clock's current time.
async fn settle(worker: &mut Worker) {
    for _ in 0..150 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("worker did not settle");
}
async fn intent(worker: &mut Worker, agents: &AgentService, task: MessageId) -> RunId {
    for _ in 0..30 {
        let view = agents.actors().unwrap().task(task).await.unwrap();
        if view.delegation.is_some() {
            return view.run_id.unwrap();
        }
        assert!(worker.step().await.unwrap());
    }
    panic!("delegation not saved");
}
async fn timeout_receipt(engine: &Engine, run: RunId) -> Value {
    let saved = engine.status(run).await.unwrap();
    let delegation = &saved.record().delegations[0];
    assert_eq!(delegation.deadline_at, Some(1100));
    assert!(delegation.timed_out_at.is_some());
    let outcome = delegation.outcome.clone().unwrap();
    assert_eq!(outcome["status"], "cancelled");
    assert_eq!(outcome["error"]["code"], "delegation_timeout");
    assert_eq!(
        engine
            .history(run)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "delegation_timed_out")
            .count(),
        1
    );
    assert_eq!(
        engine
            .history(run)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "delegation_completed")
            .count(),
        1
    );
    outcome
}

#[tokio::test]
async fn offline_expiry_keeps_the_deadline_and_published_identity() {
    for published in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("offline.db");
        let clock = Arc::new(ManualClock::new(1000));
        let (engine, agents) = app(&db, clock.clone(), true).await;
        let assistant = answer(&agents, true).await;
        let actor = planner(&agents, assistant, Some(100)).await;
        let task = submit(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let run = intent(&mut worker, &agents, task).await;
        let child = engine
            .status(run)
            .await
            .unwrap()
            .pending_delegation()
            .unwrap()
            .id;
        if published {
            settle(&mut worker).await;
        }
        assert!(worker.has_active_runs().await.unwrap());
        drop(worker);
        drop(agents);
        drop(engine);
        clock.set(900); // A backward clock change does not recompute the deadline.
        let (engine, agents) = app(&db, clock.clone(), true).await;
        let mut worker = engine.worker().await.unwrap();
        assert_eq!(
            engine
                .status(run)
                .await
                .unwrap()
                .pending_delegation()
                .unwrap()
                .deadline_at,
            Some(1100)
        );
        // Do not publish a previously unsubmitted child while checking the clock.
        if published {
            assert!(!worker.step().await.unwrap());
        }
        clock.set(9000);
        settle(&mut worker).await;
        let outcome = timeout_receipt(&engine, run).await;
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        assert_eq!(
            engine
                .status(run)
                .await
                .unwrap()
                .record()
                .usage
                .admitted_model_calls,
            2
        );
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .queue(actor)
                .await
                .unwrap()
                .outstanding,
            0
        );
        if published {
            assert_eq!(
                agents
                    .actors()
                    .unwrap()
                    .tasks(assistant)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                agents.actors().unwrap().task(child).await.unwrap().status,
                AgentTaskStatus::Cancelled
            );
            assert_eq!(outcome["child_outcome"]["usage"]["admitted_model_calls"], 1);
        } else {
            assert!(agents.actors().unwrap().task(child).await.is_err());
            assert!(
                agents
                    .actors()
                    .unwrap()
                    .tasks(assistant)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        assert!(!worker.has_active_runs().await.unwrap());
    }
}

#[tokio::test]
async fn saved_completion_wins_by_the_deadline_and_committed_expiry_wins_after_it() {
    for boundary in ["before", "equal", "late", "expiry_first"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("race.db");
        let clock = Arc::new(ManualClock::new(1000));
        let (engine, agents) = app(&db, clock.clone(), true).await;
        let assistant = answer(&agents, false).await;
        let actor = planner(&agents, assistant, Some(100)).await;
        let task = submit(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let run = intent(&mut worker, &agents, task).await;
        let child = engine
            .status(run)
            .await
            .unwrap()
            .pending_delegation()
            .unwrap()
            .id;
        assert!(worker.advance_work().await.unwrap()); // Publish.
        assert!(worker.advance_work().await.unwrap()); // Create child Run.
        let execution = worker.admit().await.unwrap().unwrap();
        let outcome = worker.execute(execution.clone()).await.unwrap();
        clock.set(match boundary {
            "before" => 1099,
            "late" => 1101,
            _ => 1100,
        });
        if boundary == "expiry_first" {
            assert!(worker.advance_work().await.unwrap()); // Persist expiry before completion.
            assert_eq!(
                agents.actors().unwrap().task(task).await.unwrap().status,
                AgentTaskStatus::Cancelling
            );
        }
        worker.complete(execution, outcome).await.unwrap();
        drop(worker);
        drop(agents);
        drop(engine);
        clock.set(9000);
        let (engine, agents) = app(&db, clock.clone(), true).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        assert_eq!(
            agents.actors().unwrap().task(child).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        if matches!(boundary, "before" | "equal") {
            let saved = engine.status(run).await.unwrap();
            assert!(saved.record().delegations[0].timed_out_at.is_none());
            assert_eq!(
                saved.record().delegations[0].outcome.as_ref().unwrap()["status"],
                "succeeded"
            );
            assert!(
                !engine
                    .history(run)
                    .await
                    .unwrap()
                    .iter()
                    .any(|e| e.kind == "delegation_timed_out")
            );
        } else {
            let outcome = timeout_receipt(&engine, run).await;
            assert_eq!(outcome["child_outcome"]["status"], "succeeded");
            assert_eq!(
                outcome["child_outcome"]["result"]["answer"],
                "Child finished."
            );
        }
    }
}

#[tokio::test]
async fn paused_parents_expire_without_publication_and_resume_from_the_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("pause.db");
    let clock = Arc::new(ManualClock::new(1000));
    let (engine, agents) = app(&db, clock.clone(), true).await;
    let assistant = answer(&agents, true).await;
    let actor = planner(&agents, assistant, Some(100)).await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let run = intent(&mut worker, &agents, task).await;
    engine.pause(run).await.unwrap();
    assert!(worker.has_active_runs().await.unwrap());
    assert!(!worker.step().await.unwrap());
    clock.set(1100);
    settle(&mut worker).await;
    timeout_receipt(&engine, run).await;
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Paused
    );
    assert_eq!(
        engine
            .status(run)
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        1
    );
    assert!(
        agents
            .actors()
            .unwrap()
            .tasks(assistant)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!worker.has_active_runs().await.unwrap());
    engine.resume(run).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
}

#[tokio::test]
async fn expiry_cancels_nested_children_and_keeps_in_flight_charges() {
    for nested in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("cancel.db");
        let clock = Arc::new(ManualClock::new(1000));
        let (engine, agents) = app(&db, clock.clone(), true).await;
        let assistant = answer(&agents, nested).await;
        let target = if nested {
            planner(&agents, assistant, None).await
        } else {
            assistant
        };
        let actor = planner(&agents, target, Some(100)).await;
        let task = submit(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let run = intent(&mut worker, &agents, task).await;
        let child = engine
            .status(run)
            .await
            .unwrap()
            .pending_delegation()
            .unwrap()
            .id;
        let execution = if nested {
            settle(&mut worker).await;
            None
        } else {
            assert!(worker.advance_work().await.unwrap());
            assert!(worker.advance_work().await.unwrap());
            Some(worker.admit().await.unwrap().unwrap())
        };
        clock.set(1100);
        assert!(worker.advance_work().await.unwrap()); // Expiry marker.
        assert!(worker.advance_work().await.unwrap()); // Child cancellation.
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .queue(actor)
                .await
                .unwrap()
                .outstanding,
            1
        );
        if let Some(execution) = execution {
            assert_eq!(
                agents.actors().unwrap().task(child).await.unwrap().status,
                AgentTaskStatus::Cancelling
            );
            let outcome = worker.execute(execution.clone()).await.unwrap();
            worker.complete(execution, outcome).await.unwrap();
        }
        settle(&mut worker).await;
        let outcome = timeout_receipt(&engine, run).await;
        assert_eq!(outcome["child_outcome"]["status"], "cancelled");
        assert_eq!(
            outcome["usage"]["actual_input_tokens"]
                .as_u64()
                .unwrap_or(0)
                + outcome["usage"]["actual_output_tokens"]
                    .as_u64()
                    .unwrap_or(0),
            20
        );
        for target in [target, assistant] {
            for task in agents.actors().unwrap().tasks(target).await.unwrap() {
                assert_eq!(task.status, AgentTaskStatus::Cancelled);
            }
            assert_eq!(
                agents
                    .actors()
                    .unwrap()
                    .queue(target)
                    .await
                    .unwrap()
                    .outstanding,
                0
            );
        }
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
    }
}

#[tokio::test]
async fn expiry_and_cancel_transaction_failures_retry_the_same_receipt() {
    use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let clock = Arc::new(ManualClock::new(1000));
    let (engine, agents) = app(&db, clock.clone(), true).await;
    let assistant = answer(&agents, true).await;
    let actor = planner(&agents, assistant, Some(100)).await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    let view = agents.actors().unwrap().task(task).await.unwrap();
    let run = view.run_id.unwrap();
    let child = view.delegation.unwrap().id;
    let before = json!(engine.status(run).await.unwrap());
    let child_before = json!(agents.actors().unwrap().task(child).await.unwrap());
    let mut connection = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&db))
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_expiry BEFORE INSERT ON history WHEN json_extract(NEW.payload,'$.kind')='delegation_timed_out' BEGIN SELECT RAISE(ABORT,'expiry rollback'); END;").execute(&mut connection).await.unwrap();
    clock.set(1100);
    assert!(worker.advance_work().await.is_err());
    assert_eq!(json!(engine.status(run).await.unwrap()), before);
    assert_eq!(
        json!(agents.actors().unwrap().task(child).await.unwrap()),
        child_before
    );
    let expired: Option<i64> =
        sqlx::query_scalar("SELECT timed_out_at FROM agent_delegations WHERE id=?")
            .bind(child.to_string())
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert!(expired.is_none());
    sqlx::raw_sql("DROP TRIGGER reject_expiry; CREATE TRIGGER reject_deadline_cancel BEFORE UPDATE ON agent_tasks BEGIN SELECT RAISE(ABORT,'cancel rollback'); END;").execute(&mut connection).await.unwrap();
    assert!(worker.advance_work().await.unwrap()); // Commit expiry first.
    let expired = json!(engine.status(run).await.unwrap());
    assert!(worker.advance_work().await.is_err());
    assert_eq!(json!(engine.status(run).await.unwrap()), expired);
    assert_eq!(
        json!(agents.actors().unwrap().task(child).await.unwrap()),
        child_before
    );
    sqlx::raw_sql("DROP TRIGGER reject_deadline_cancel;")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    clock.set(900); // The committed expiry cannot be undone by a clock change.
    let (engine, agents) = app(&db, clock.clone(), true).await;
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    timeout_receipt(&engine, run).await;
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .tasks(assistant)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn missing_planner_still_expires_and_cancels_but_retains_continuation_until_restored() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("missing.db");
    let clock = Arc::new(ManualClock::new(1000));
    let (engine, agents) = app(&db, clock.clone(), true).await;
    let assistant = answer(&agents, true).await;
    let actor = planner(&agents, assistant, Some(100)).await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    let view = agents.actors().unwrap().task(task).await.unwrap();
    let run = view.run_id.unwrap();
    let child = view.delegation.unwrap().id;
    drop(worker);
    drop(agents);
    drop(engine);
    clock.set(1100);
    let (engine, agents) = app(&db, clock.clone(), false).await;
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        agents.actors().unwrap().task(child).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Blocked
    );
    assert_eq!(
        engine
            .status(run)
            .await
            .unwrap()
            .pending_delegation()
            .unwrap()
            .timed_out_at,
        Some(1100)
    );
    assert!(!worker.has_active_runs().await.unwrap());
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = app(&db, clock.clone(), true).await;
    engine.resume(run).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    timeout_receipt(&engine, run).await;
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
}

#[test]
fn deadline_configuration_and_domain_receipts_are_bounded_and_idempotent() {
    for timeout_ms in [Some(0), Some(u64::MAX)] {
        assert!(
            DelegationBudget {
                max_tasks: 1,
                max_depth: 1,
                timeout_ms,
                shared_budget: None,
                max_parallel_tasks: None,
            }
            .validate()
            .is_err()
        );
    }
    let adapter = PlannerAgentAdapter::default();
    let registration = adapter.registration();
    let workflow =
        RegisteredAgentWorkflow::new(registration.clone(), Arc::new(PlannerWorkflow::default()))
            .unwrap();
    let input = json!(AgentInvocation {
        registration,
        input: config(AgentId::new(), Some(100))
    });
    let mut run = Run::start(&workflow, input, 10, 1000).unwrap();
    let execution = run.admit(1000).unwrap().unwrap();
    run.complete(&execution, Outcome::SuccessWithUsage { result:json!({"action":{"kind":"delegate_task","worker":"assistant","goal":"Explain recovery."},"request_id":null}), usage:TokenUsage { input_tokens:10, output_tokens:10 } }, &workflow, 1000).unwrap();
    let child = run.pending_delegation().unwrap().id;
    let before = json!(run);
    assert!(run.expire_delegation(child, None, 1099).is_err());
    assert!(run.expire_delegation(child, Some(1100), 9000).is_err());
    assert_eq!(json!(run), before);
    run.expire_delegation(child, None, 1100).unwrap();
    let expired = json!(run);
    run.expire_delegation(child, None, 900).unwrap();
    assert_eq!(json!(run), expired);
    assert!(
        run.task_completed(child, json!({"status":"running"}), Some(&workflow), 1101)
            .is_err()
    );
    assert_eq!(json!(run), expired);
    let outcome = json!({"task_id":child,"run_id":null,"status":"cancelled","result":null,"error":null,"usage":null});
    run.task_completed(child, outcome.clone(), Some(&workflow), 1101)
        .unwrap();
    let delivered = json!(run);
    run.task_completed(child, outcome, None, 9000).unwrap();
    assert_eq!(json!(run), delivered);
    assert!(
        run.task_completed(child, json!({"status":"failed"}), None, 9000)
            .is_err()
    );
}

#[tokio::test]
async fn deadline_can_cancel_an_unconsumed_child_with_a_missing_integration() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("missing-child.db");
    let clock = Arc::new(ManualClock::new(1000));
    let (engine, agents) = app(&db, clock.clone(), true).await;
    let assistant = answer(&agents, false).await;
    let actor = planner(&agents, assistant, Some(100)).await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let run = intent(&mut worker, &agents, task).await;
    let child = engine
        .status(run)
        .await
        .unwrap()
        .pending_delegation()
        .unwrap()
        .id;
    assert!(worker.advance_work().await.unwrap());
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = app_adapters(&db, clock.clone(), true, false).await;
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    let blocked = agents.actors().unwrap().task(child).await.unwrap();
    assert_eq!(blocked.status, AgentTaskStatus::Blocked);
    assert!(blocked.run_id.is_none());
    clock.set(1100);
    settle(&mut worker).await;
    timeout_receipt(&engine, run).await;
    assert_eq!(
        agents.actors().unwrap().task(child).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
}

#[tokio::test]
async fn deadline_cancellation_never_admits_a_queued_child_and_explicit_cancel_keeps_its_reason() {
    for explicit in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("queued.db");
        let clock = Arc::new(ManualClock::new(1000));
        let (engine, agents) = app(&db, clock.clone(), true).await;
        let assistant = answer(&agents, false).await;
        let actor = planner(&agents, assistant, Some(100)).await;
        let task = submit(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let run = intent(&mut worker, &agents, task).await;
        let child = engine
            .status(run)
            .await
            .unwrap()
            .pending_delegation()
            .unwrap()
            .id;
        assert!(worker.advance_work().await.unwrap()); // Published, no child Run yet.
        clock.set(1100);
        if explicit {
            agents.actors().unwrap().cancel_task(task).await.unwrap();
        }
        settle(&mut worker).await;
        let view = agents.actors().unwrap().task(child).await.unwrap();
        assert_eq!(view.status, AgentTaskStatus::Cancelled);
        assert!(view.run_id.is_none());
        assert!(view.usage.is_none());
        let parent = agents.actors().unwrap().task(task).await.unwrap();
        if explicit {
            assert_eq!(parent.status, AgentTaskStatus::Cancelled);
            assert!(
                engine.status(run).await.unwrap().record().delegations[0]
                    .timed_out_at
                    .is_none()
            );
            assert_eq!(parent.usage.unwrap().admitted_model_calls, 1);
        } else {
            timeout_receipt(&engine, run).await;
            assert_eq!(parent.status, AgentTaskStatus::Succeeded);
        }
    }
}

#[tokio::test]
async fn expiry_waits_for_uncertain_child_reconciliation_without_spinning_or_resetting_usage() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("uncertain.db");
    let clock = Arc::new(ManualClock::new(1000));
    let (engine, agents) = app(&db, clock.clone(), true).await;
    let task_config: Value =
        serde_json::from_str(include_str!("../examples/configs/task-scripted.json")).unwrap();
    let mut config = task_config["agent"].clone();
    config["repeat_safe_tests"] = json!(false);
    config["script"] = json!([{"kind":"run_tests"}]);
    let coding = agents
        .create_registered_actor(
            DefinitionId::new("coding", 1),
            Some(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("examples/benchmark-cases/addition/source"),
            ),
            config,
            10,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let actor = planner(&agents, coding, Some(100)).await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let run = intent(&mut worker, &agents, task).await;
    let child = engine
        .status(run)
        .await
        .unwrap()
        .pending_delegation()
        .unwrap()
        .id;
    assert!(worker.advance_work().await.unwrap());
    assert!(worker.advance_work().await.unwrap());
    assert!(worker.step().await.unwrap()); // Save run_tests decision.
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.handler, DefinitionId::new("agent.tool", 1));
    drop(worker);
    drop(agents);
    drop(engine);
    clock.set(1100);
    let (engine, agents) = app(&db, clock.clone(), true).await;
    let mut worker = engine.worker().await.unwrap();
    let child_before = json!(engine.status(execution.run_id).await.unwrap());
    assert_eq!(child_before["record"]["status"], "blocked");
    let charges = engine
        .status(execution.run_id)
        .await
        .unwrap()
        .record()
        .usage
        .charged_tokens();
    settle(&mut worker).await;
    assert_eq!(
        json!(engine.status(execution.run_id).await.unwrap()),
        child_before
    );
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Cancelling
    );
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .queue(actor)
            .await
            .unwrap()
            .outstanding,
        1
    );
    assert!(!worker.has_active_runs().await.unwrap());
    engine
        .reconcile(
            execution.run_id,
            ReconciliationRequest {
                activity_id: execution.activity_id,
                attempt_id: execution.attempt_id,
                decision: ReconciliationDecision::Fail {
                    message: "Stopped after inspecting uncertain test effects.".into(),
                },
                evidence: "No saved test result; operator inspected the workspace.".into(),
            },
        )
        .await
        .unwrap();
    settle(&mut worker).await;
    let outcome = timeout_receipt(&engine, run).await;
    assert_eq!(outcome["child_outcome"]["status"], "failed");
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .task(child)
            .await
            .unwrap()
            .usage
            .unwrap()
            .charged_tokens(),
        charges
    );
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .queue(actor)
            .await
            .unwrap()
            .outstanding,
        0
    );
}

#[tokio::test]
#[ignore = "subprocess delegation deadline checkpoint fixture"]
async fn deadline_crash_child() {
    let Ok(db) = std::env::var("DEADLINE_DB") else {
        return;
    };
    let clock = Arc::new(ManualClock::new(1000));
    let (engine, agents) = app(Path::new(&db), clock.clone(), true).await;
    let task: MessageId = std::env::var("DEADLINE_TASK").unwrap().parse().unwrap();
    let boundary = std::env::var("DEADLINE_BOUNDARY").unwrap();
    let mut worker = engine.worker().await.unwrap();
    let run = intent(&mut worker, &agents, task).await;
    if boundary != "requested" {
        settle(&mut worker).await;
    }
    if boundary == "expired" {
        clock.set(1100);
        assert!(worker.advance_work().await.unwrap());
    }
    std::fs::write(
        std::env::var("DEADLINE_MARKER").unwrap(),
        serde_json::to_vec(&engine.status(run).await.unwrap()).unwrap(),
    )
    .unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn killed_workers_recover_before_publication_during_wait_and_after_committed_expiry() {
    use std::{
        fs,
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    for boundary in ["requested", "published", "expired"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("kill.db");
        let marker = dir.path().join("marker.json");
        let clock = Arc::new(ManualClock::new(1000));
        let (engine, agents) = app(&db, clock.clone(), true).await;
        let assistant = answer(&agents, true).await;
        let actor = planner(&agents, assistant, Some(100)).await;
        let task = submit(&agents, actor).await;
        drop(agents);
        drop(engine);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "deadline_crash_child",
                "--nocapture",
            ])
            .env("DEADLINE_DB", &db)
            .env("DEADLINE_TASK", task.to_string())
            .env("DEADLINE_BOUNDARY", boundary)
            .env("DEADLINE_MARKER", &marker)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut process = command.spawn().unwrap();
        let until = Instant::now() + Duration::from_secs(30);
        loop {
            if fs::read(&marker)
                .ok()
                .is_some_and(|bytes| serde_json::from_slice::<Value>(&bytes).is_ok())
            {
                break;
            }
            assert!(
                process.try_wait().unwrap().is_none(),
                "checkpoint process failed"
            );
            if Instant::now() >= until {
                process.kill().unwrap();
                panic!("checkpoint timeout");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let saved: Value = serde_json::from_slice(&fs::read(&marker).unwrap()).unwrap();
        let run: RunId = serde_json::from_value(saved["record"]["id"].clone()).unwrap();
        let child: MessageId =
            serde_json::from_value(saved["record"]["delegations"][0]["id"].clone()).unwrap();
        clock.set(if boundary == "expired" { 900 } else { 9000 });
        let (engine, agents) = app(&db, clock.clone(), true).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        timeout_receipt(&engine, run).await;
        let children = agents.actors().unwrap().tasks(assistant).await.unwrap();
        if boundary == "requested" {
            assert!(children.is_empty());
        } else {
            assert_eq!(children.len(), 1);
            assert_eq!(children[0].task.id, child);
            assert_eq!(children[0].status, AgentTaskStatus::Cancelled);
        }
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
    }
}
