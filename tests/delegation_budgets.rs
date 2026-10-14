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
    domain::{actors::*, *},
};
use serde_json::{Value, json};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

struct ManualClock(AtomicI64);
impl ManualClock {
    fn new() -> Self {
        Self(AtomicI64::new(1000))
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
async fn app(db: &Path, clock: Arc<ManualClock>) -> (Engine, AgentService) {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let root = db.parent().unwrap().join(".act0r/workspaces");
    let workspaces = Arc::new(LocalWorkspaces::new(root.clone()));
    let adapters: Vec<Arc<dyn AgentAdapter>> = vec![
        Arc::new(CodingAgentAdapter::new(workspaces.clone())),
        Arc::new(AnswerAgentAdapter),
        Arc::new(PlannerAgentAdapter::default()),
    ];
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
fn planner_config(worker: AgentId, shared: Option<(u32, u64)>) -> Value {
    let mut config = json!({"goal":"Ask the assistant and handle its result.","target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":8,"max_tokens":200000,"max_output_tokens":512,
        "workers":[{"name":"assistant","agent_id":worker,"description":"A permitted worker"}],"delegation":{"max_tasks":4,"max_depth":4},
        "script":[{"kind":"delegate_task","worker":"assistant","goal":"Explain durable recovery."},{"kind":"finish","summary":"Handled the saved child result."}]});
    if let Some((calls, tokens)) = shared {
        config["delegation"]["shared_budget"] =
            json!({"max_model_calls":calls,"max_tokens":tokens});
    }
    config
}
async fn create(agents: &AgentService, name: &str, config: Value) -> AgentId {
    agents
        .create_registered_actor(
            agents.latest_adapter(name).unwrap(),
            None,
            config,
            15,
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
    create(agents, "answer", config).await
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
async fn drain(worker: &mut Worker) {
    for _ in 0..200 {
        if !worker.has_active_runs().await.unwrap() {
            return;
        }
        worker.step().await.unwrap();
    }
    panic!("work did not settle");
}
async fn root_run(worker: &mut Worker, agents: &AgentService, task: MessageId) -> RunId {
    for _ in 0..25 {
        if let Some(run) = agents.actors().unwrap().task(task).await.unwrap().run_id {
            return run;
        }
        assert!(worker.advance_work().await.unwrap());
    }
    panic!("Run not created");
}
async fn child_execution(
    worker: &mut Worker,
    agents: &AgentService,
    task: MessageId,
) -> (RunId, ActivityExecution) {
    let root = root_run(worker, agents, task).await;
    assert!(worker.step().await.unwrap()); // Parent chooses delegation.
    assert!(worker.advance_work().await.unwrap()); // Publish child.
    assert!(worker.advance_work().await.unwrap()); // Create child Run and inherit membership.
    let execution = worker.admit().await.unwrap().unwrap();
    assert_ne!(execution.run_id, root);
    (root, execution)
}
async fn sql(db: &Path, statement: &'static str) {
    let mut conn = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(db))
        .await
        .unwrap();
    sqlx::raw_sql(statement).execute(&mut conn).await.unwrap();
    conn.close().await.unwrap();
}
async fn scope(engine: &Engine, run: RunId, root: RunId) -> Value {
    let scopes = engine.shared_budgets(run).await.unwrap();
    json!(
        scopes
            .into_iter()
            .find(|s| s.record.root_run_id == root)
            .unwrap()
    )
}

#[tokio::test]
async fn nested_descendants_share_ancestor_caps_and_can_add_tighter_subtree_caps() {
    for (outer, inner, status, calls, inner_calls) in [
        (5, 3, AgentTaskStatus::Succeeded, 5, 3),
        (3, 100, AgentTaskStatus::BudgetExhausted, 3, 2),
        (20, 2, AgentTaskStatus::Succeeded, 4, 2),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("nested.db");
        let (engine, agents) = app(&db, Arc::new(ManualClock::new())).await;
        let leaf = answer(&agents, false).await;
        let middle = create(
            &agents,
            "planner",
            planner_config(leaf, Some((inner, 100000))),
        )
        .await;
        let actor = create(
            &agents,
            "planner",
            planner_config(middle, Some((outer, 100000))),
        )
        .await;
        let task = submit(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let parent = agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(parent.status, status);
        let root = parent.run_id.unwrap();
        let total = scope(&engine, root, root).await;
        assert_eq!(total["record"]["usage"]["admitted_model_calls"], calls);
        assert_eq!(total["record"]["usage"]["actual_input_tokens"], calls * 10);
        assert_eq!(total["members"].as_array().unwrap().len(), 3);
        let middle_task = agents.actors().unwrap().tasks(middle).await.unwrap()[0].clone();
        let middle_run = middle_task.run_id.unwrap();
        let subtree = scope(&engine, middle_run, middle_run).await;
        assert_eq!(
            subtree["record"]["usage"]["admitted_model_calls"],
            inner_calls
        );
        assert_eq!(subtree["members"].as_array().unwrap().len(), 2);
        if outer == 3 {
            assert_eq!(total["record"]["exhaustion"]["run_id"], json!(middle_run));
        }
        if inner == 2 {
            assert_eq!(middle_task.status, AgentTaskStatus::BudgetExhausted);
            assert!(total["record"]["exhaustion"].is_null());
            assert_eq!(
                subtree["record"]["exhaustion"]["reason"],
                "admission_denied"
            );
        }
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
        let saved = total.clone();
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, _) = app(&db, Arc::new(ManualClock::new())).await;
        assert_eq!(scope(&engine, root, root).await, saved);
    }
}

#[tokio::test]
async fn oversized_child_reservations_are_denied_without_charging_or_dispatching() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("tokens.db");
    let (engine, agents) = app(&db, Arc::new(ManualClock::new())).await;
    let mut config: Value =
        serde_json::from_str(include_str!("../examples/configs/answer-scripted.json")).unwrap();
    config["max_output_tokens"] = json!(20000);
    let assistant = create(&agents, "answer", config).await;
    let actor = create(
        &agents,
        "planner",
        planner_config(assistant, Some((20, 10000))),
    )
    .await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let parent = agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(parent.status, AgentTaskStatus::BudgetExhausted);
    let child = agents.actors().unwrap().tasks(assistant).await.unwrap()[0].clone();
    assert_eq!(child.status, AgentTaskStatus::BudgetExhausted);
    assert_eq!(child.admitted_attempts, 0);
    assert_eq!(child.usage.unwrap().admitted_model_calls, 0);
    let root = parent.run_id.unwrap();
    let total = scope(&engine, root, root).await;
    assert_eq!(total["record"]["usage"]["admitted_model_calls"], 1);
    assert_eq!(total["record"]["usage"]["held_tokens"], 0);
    assert_eq!(total["record"]["usage"]["actual_input_tokens"], 10);
    assert_eq!(
        total["record"]["exhaustion"]["run_id"],
        json!(child.run_id.unwrap())
    );
    let saved_run = engine.status(root).await.unwrap();
    let receipt = &saved_run.record().delegations[0];
    assert_eq!(
        receipt.outcome.as_ref().unwrap()["error"]["code"],
        "shared_model_budget"
    );
}

#[tokio::test]
async fn paused_waits_reopen_with_the_same_charges_and_exhaust_the_shared_allowance() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("wait.db");
    let clock = Arc::new(ManualClock::new());
    let (engine, agents) = app(&db, clock.clone()).await;
    let assistant = answer(&agents, true).await;
    let actor = create(
        &agents,
        "planner",
        planner_config(assistant, Some((3, 100000))),
    )
    .await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let root = agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .run_id
        .unwrap();
    engine.pause(root).await.unwrap();
    let saved = scope(&engine, root, root).await;
    assert_eq!(saved["record"]["usage"]["admitted_model_calls"], 2);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = app(&db, clock).await;
    let mut worker = engine.worker().await.unwrap();
    assert_eq!(scope(&engine, root, root).await, saved);
    let child = agents.actors().unwrap().tasks(assistant).await.unwrap()[0].clone();
    engine
        .signal(
            child.run_id.unwrap(),
            child.wait.unwrap().id,
            json!({"answer":"short"}),
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    assert_eq!(
        engine.status(root).await.unwrap().record().status,
        RunStatus::Paused
    );
    engine.resume(root).await.unwrap();
    drain(&mut worker).await;
    let total = scope(&engine, root, root).await;
    assert_eq!(total["record"]["usage"]["admitted_model_calls"], 3);
    assert_eq!(total["record"]["usage"]["actual_input_tokens"], 30);
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::BudgetExhausted
    );
}

#[tokio::test]
async fn retries_keep_uncertain_reservations_and_duplicate_outcomes_never_double_charge() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("retry.db");
    let clock = Arc::new(ManualClock::new());
    let (engine, agents) = app(&db, clock.clone()).await;
    let assistant = answer(&agents, false).await;
    let actor = create(
        &agents,
        "planner",
        planner_config(assistant, Some((1, 100000))),
    )
    .await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let root = root_run(&mut worker, &agents, task).await;
    let execution = worker.admit().await.unwrap().unwrap();
    let held = scope(&engine, root, root).await["record"]["usage"]["held_tokens"]
        .as_u64()
        .unwrap();
    assert!(held > 0);
    let outcome = Outcome::Failure(Failure::new(
        "fixture_http",
        "retryable failure without usage",
        true,
    ));
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    clock.set(2000);
    assert!(worker.admit().await.unwrap().is_none());
    let saved = scope(&engine, root, root).await;
    assert_eq!(saved["record"]["usage"]["held_tokens"], held);
    assert_eq!(saved["record"]["usage"]["uncertain_calls"], 1);
    assert_eq!(saved["record"]["usage"]["admitted_model_calls"], 1);
    worker.complete(execution, outcome).await.unwrap();
    assert_eq!(scope(&engine, root, root).await, saved);
    assert_eq!(engine.status(root).await.unwrap().attempts().len(), 1);
}

#[tokio::test]
async fn membership_admission_completion_and_exhaustion_failures_roll_back_together() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let (engine, agents) = app(&db, Arc::new(ManualClock::new())).await;
    let assistant = answer(&agents, false).await;
    let actor = create(
        &agents,
        "planner",
        planner_config(assistant, Some((1, 100000))),
    )
    .await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let root = root_run(&mut worker, &agents, task).await;
    let before = json!(engine.status(root).await.unwrap());
    let budget_before = scope(&engine, root, root).await;
    sql(&db,"CREATE TRIGGER fail_scope BEFORE UPDATE ON agent_budget_scopes BEGIN SELECT RAISE(ABORT,'scope rollback'); END;").await;
    assert!(worker.admit().await.is_err());
    assert_eq!(json!(engine.status(root).await.unwrap()), before);
    assert_eq!(scope(&engine, root, root).await, budget_before);
    sql(&db, "DROP TRIGGER fail_scope;").await;
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    let admitted = json!(engine.status(root).await.unwrap());
    let reserved = scope(&engine, root, root).await;
    sql(&db,"CREATE TRIGGER fail_scope BEFORE UPDATE ON agent_budget_scopes BEGIN SELECT RAISE(ABORT,'completion rollback'); END;").await;
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(json!(engine.status(root).await.unwrap()), admitted);
    assert_eq!(scope(&engine, root, root).await, reserved);
    sql(&db, "DROP TRIGGER fail_scope;").await;
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    let completed = scope(&engine, root, root).await;
    worker.complete(execution, outcome).await.unwrap();
    assert_eq!(scope(&engine, root, root).await, completed);
    assert!(worker.advance_work().await.unwrap()); // Publish child.
    let child_id = engine
        .status(root)
        .await
        .unwrap()
        .pending_delegation()
        .unwrap()
        .id;
    sql(&db,"CREATE TRIGGER fail_membership BEFORE INSERT ON agent_budget_members BEGIN SELECT RAISE(ABORT,'membership rollback'); END;").await;
    assert!(worker.advance_work().await.is_err());
    assert!(
        agents
            .actors()
            .unwrap()
            .task(child_id)
            .await
            .unwrap()
            .run_id
            .is_none()
    );
    assert_eq!(
        scope(&engine, root, root).await["members"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    sql(&db, "DROP TRIGGER fail_membership;").await;
    assert!(worker.advance_work().await.unwrap());
    let child_run = agents
        .actors()
        .unwrap()
        .task(child_id)
        .await
        .unwrap()
        .run_id
        .unwrap();
    let child_before = json!(engine.status(child_run).await.unwrap());
    let total_before = scope(&engine, root, root).await;
    sql(&db,"CREATE TRIGGER fail_scope BEFORE UPDATE ON agent_budget_scopes BEGIN SELECT RAISE(ABORT,'exhaustion rollback'); END;").await;
    assert!(worker.admit().await.is_err());
    assert_eq!(json!(engine.status(child_run).await.unwrap()), child_before);
    assert_eq!(scope(&engine, root, root).await, total_before);
    sql(&db, "DROP TRIGGER fail_scope;").await;
    assert!(worker.admit().await.unwrap().is_none());
    drain(&mut worker).await;
    let total = scope(&engine, root, root).await;
    assert_eq!(total["record"]["usage"]["admitted_model_calls"], 1);
    assert_eq!(total["record"]["usage"]["held_tokens"], 0);
    assert_eq!(total["record"]["exhaustion"]["run_id"], json!(child_run));
    assert_eq!(
        engine
            .history(child_run)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "shared_budget_exhausted")
            .count(),
        1
    );
}

#[tokio::test]
async fn cancellation_and_deadlines_retain_in_flight_child_charges() {
    for deadline in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("cancel.db");
        let clock = Arc::new(ManualClock::new());
        let (engine, agents) = app(&db, clock.clone()).await;
        let assistant = answer(&agents, true).await;
        let mut config = planner_config(assistant, Some((3, 100000)));
        if deadline {
            config["delegation"]["timeout_ms"] = json!(100);
        }
        let actor = create(&agents, "planner", config).await;
        let task = submit(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let (root, execution) = child_execution(&mut worker, &agents, task).await;
        let admitted = scope(&engine, root, root).await;
        assert_eq!(admitted["record"]["usage"]["admitted_model_calls"], 2);
        assert!(admitted["record"]["usage"]["held_tokens"].as_u64().unwrap() > 0);
        if deadline {
            clock.set(1100);
        } else {
            agents.actors().unwrap().cancel_task(task).await.unwrap();
        }
        for _ in 0..10 {
            let child_id = engine
                .status(root)
                .await
                .unwrap()
                .pending_delegation()
                .unwrap()
                .id;
            if agents
                .actors()
                .unwrap()
                .task(child_id)
                .await
                .unwrap()
                .task
                .cancel_requested
            {
                break;
            }
            assert!(worker.advance_work().await.unwrap());
        }
        let held = scope(&engine, root, root).await;
        assert_eq!(
            held["record"]["usage"]["held_tokens"],
            admitted["record"]["usage"]["held_tokens"]
        );
        let outcome = worker.execute(execution.clone()).await.unwrap();
        worker.complete(execution, outcome).await.unwrap();
        drain(&mut worker).await;
        let total = scope(&engine, root, root).await;
        let calls = if deadline { 3 } else { 2 };
        assert_eq!(total["record"]["usage"]["admitted_model_calls"], calls);
        assert_eq!(total["record"]["usage"]["actual_input_tokens"], calls * 10);
        assert_eq!(total["record"]["usage"]["held_tokens"], 0);
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            if deadline {
                AgentTaskStatus::Succeeded
            } else {
                AgentTaskStatus::Cancelled
            }
        );
        assert_eq!(
            agents.actors().unwrap().tasks(assistant).await.unwrap()[0].status,
            AgentTaskStatus::Cancelled
        );
    }
}

#[tokio::test]
async fn reported_overruns_remain_visible_and_latch_the_scope_without_refunds() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("overrun.db");
    let (engine, agents) = app(&db, Arc::new(ManualClock::new())).await;
    let assistant = answer(&agents, false).await;
    let actor = create(
        &agents,
        "planner",
        planner_config(assistant, Some((4, 20000))),
    )
    .await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let root = root_run(&mut worker, &agents, task).await;
    let execution = worker.admit().await.unwrap().unwrap();
    let result =
        json!({"action":{"kind":"finish","summary":"Provider completed."},"request_id":null});
    worker
        .complete(
            execution,
            Outcome::SuccessWithUsage {
                result,
                usage: TokenUsage {
                    input_tokens: 20001,
                    output_tokens: 1,
                },
            },
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    let total = scope(&engine, root, root).await;
    assert_eq!(total["record"]["usage"]["actual_input_tokens"], 20001);
    assert_eq!(total["record"]["usage"]["held_tokens"], 0);
    assert_eq!(
        total["record"]["exhaustion"]["reason"],
        "reported_usage_exceeded"
    );
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::BudgetExhausted
    );
}

#[tokio::test]
async fn separate_goals_on_the_same_actor_have_independent_shared_ledgers() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("goals.db");
    let (engine, agents) = app(&db, Arc::new(ManualClock::new())).await;
    let assistant = answer(&agents, false).await;
    let actor = create(
        &agents,
        "planner",
        planner_config(assistant, Some((3, 100000))),
    )
    .await;
    let first = submit(&agents, actor).await;
    let second = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let mut roots = vec![];
    for task in [first, second] {
        let view = agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(view.status, AgentTaskStatus::Succeeded);
        assert_eq!(view.shared_budgets.len(), 1);
        let root = view.run_id.unwrap();
        roots.push(root);
        let total = scope(&engine, root, root).await;
        assert_eq!(total["record"]["usage"]["admitted_model_calls"], 3);
        assert_eq!(total["record"]["usage"]["actual_input_tokens"], 30);
        assert_eq!(total["members"].as_array().unwrap().len(), 2);
        assert!(total["record"]["exhaustion"].is_null());
    }
    assert_ne!(roots[0], roots[1]);
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .tasks(assistant)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn invalid_shared_configuration_is_rejected_and_omission_preserves_the_pin() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("validation.db");
    let (_, agents) = app(&db, Arc::new(ManualClock::new())).await;
    let assistant = answer(&agents, false).await;
    for budget in [(0, 10000), (1, 0)] {
        assert!(
            agents
                .create_registered_actor(
                    DefinitionId::new("planner", 3),
                    None,
                    planner_config(assistant, Some(budget)),
                    10,
                    AgentQueuePolicy::default()
                )
                .await
                .is_err()
        );
    }
    let original = planner_config(assistant, None);
    let decoded: act0r::domain::planner::PlannerConfig =
        serde_json::from_value(original.clone()).unwrap();
    let pinned = serde_json::to_value(decoded).unwrap();
    assert_eq!(original["delegation"], pinned["delegation"]);
    assert!(pinned["delegation"].get("shared_budget").is_none());
}

#[tokio::test]
async fn sequential_children_keep_charging_the_same_goal_after_saved_replies() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sequential.db");
    let (engine, agents) = app(&db, Arc::new(ManualClock::new())).await;
    let assistant = answer(&agents, false).await;
    let mut config = planner_config(assistant, Some((4, 100000)));
    config["script"] = json!([
        {"kind":"delegate_task","worker":"assistant","goal":"First task."},
        {"kind":"delegate_task","worker":"assistant","goal":"Second task."},
        {"kind":"finish","summary":"Both tasks handled."}
    ]);
    let actor = create(&agents, "planner", config).await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let root = agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .run_id
        .unwrap();
    let run = engine.status(root).await.unwrap();
    assert_eq!(run.record().status, RunStatus::BudgetExhausted);
    assert_eq!(run.record().usage.admitted_model_calls, 2);
    assert_eq!(run.record().delegations.len(), 2);
    assert!(
        run.record()
            .delegations
            .iter()
            .all(|d| d.outcome.as_ref().unwrap()["status"] == "succeeded")
    );
    let total = scope(&engine, root, root).await;
    assert_eq!(total["record"]["usage"]["admitted_model_calls"], 4);
    assert_eq!(total["record"]["usage"]["actual_input_tokens"], 40);
    assert_eq!(total["members"].as_array().unwrap().len(), 3);
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .tasks(assistant)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn uncertain_child_tools_require_reconciliation_and_never_reset_the_shared_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("uncertain.db");
    let clock = Arc::new(ManualClock::new());
    let (engine, agents) = app(&db, clock.clone()).await;
    let mut config: Value =
        serde_json::from_str(include_str!("../examples/configs/agent-scripted.json")).unwrap();
    config["require_approval"] = json!(false);
    config["repeat_safe_tests"] = json!(false);
    config["script"] = json!([{"kind":"run_tests"},{"kind":"finish","summary":"Done."}]);
    let coding = agents
        .create_registered_actor(
            DefinitionId::new("coding", 1),
            Some(
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("examples/benchmark-cases/addition/source"),
            ),
            config,
            15,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let actor = create(
        &agents,
        "planner",
        planner_config(coding, Some((3, 100000))),
    )
    .await;
    let task = submit(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let (root, model) = child_execution(&mut worker, &agents, task).await;
    let outcome = worker.execute(model.clone()).await.unwrap();
    worker.complete(model, outcome).await.unwrap();
    let tool = worker.admit().await.unwrap().unwrap();
    assert_eq!(tool.handler, DefinitionId::new("agent.tool", 1));
    let charged = scope(&engine, root, root).await["record"]["usage"].clone();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = app(&db, clock).await;
    let mut worker = engine.worker().await.unwrap();
    assert_eq!(
        engine.status(tool.run_id).await.unwrap().record().status,
        RunStatus::Blocked
    );
    assert_eq!(scope(&engine, root, root).await["record"]["usage"], charged);
    agents.actors().unwrap().cancel_task(task).await.unwrap();
    assert!(!worker.has_active_runs().await.unwrap());
    engine
        .reconcile(
            tool.run_id,
            ReconciliationRequest {
                activity_id: tool.activity_id,
                attempt_id: tool.attempt_id,
                decision: ReconciliationDecision::Fail {
                    message: "Stopped after checking uncertain effects.".into(),
                },
                evidence: "Operator inspected the workspace; no saved tool outcome.".into(),
            },
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    assert_eq!(scope(&engine, root, root).await["record"]["usage"], charged);
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
}

#[tokio::test]
#[ignore = "subprocess shared budget admission checkpoint fixture"]
async fn budget_crash_child() {
    let Ok(db) = std::env::var("BUDGET_DB") else {
        return;
    };
    let (engine, agents) = app(Path::new(&db), Arc::new(ManualClock::new())).await;
    let task: MessageId = std::env::var("BUDGET_TASK").unwrap().parse().unwrap();
    let boundary = std::env::var("BUDGET_BOUNDARY").unwrap();
    let mut worker = engine.worker().await.unwrap();
    let root = if boundary == "root" {
        let root = root_run(&mut worker, &agents, task).await;
        assert!(worker.admit().await.unwrap().is_some());
        root
    } else {
        child_execution(&mut worker, &agents, task).await.0
    };
    std::fs::write(
        std::env::var("BUDGET_MARKER").unwrap(),
        serde_json::to_vec(&scope(&engine, root, root).await).unwrap(),
    )
    .unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn killed_workers_preserve_root_and_child_reservations_and_reject_unaffordable_retries() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for boundary in ["root", "child"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("kill.db");
        let marker = dir.path().join("marker.json");
        let clock = Arc::new(ManualClock::new());
        let (engine, agents) = app(&db, clock.clone()).await;
        let assistant = answer(&agents, false).await;
        let calls = if boundary == "root" { 1 } else { 2 };
        let actor = create(
            &agents,
            "planner",
            planner_config(assistant, Some((calls, 100000))),
        )
        .await;
        let task = submit(&agents, actor).await;
        drop(agents);
        drop(engine);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "budget_crash_child", "--nocapture"])
            .env("BUDGET_DB", &db)
            .env("BUDGET_TASK", task.to_string())
            .env("BUDGET_BOUNDARY", boundary)
            .env("BUDGET_MARKER", &marker)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = ChildGuard(command.spawn().unwrap());
        let until = Instant::now() + Duration::from_secs(30);
        let saved: Value = loop {
            if let Ok(bytes) = std::fs::read(&marker)
                && let Ok(saved) = serde_json::from_slice::<Value>(&bytes)
            {
                break saved;
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "checkpoint process exited"
            );
            assert!(Instant::now() < until, "checkpoint timeout");
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let root: RunId = serde_json::from_value(saved["record"]["root_run_id"].clone()).unwrap();
        clock.set(2000);
        let (engine, agents) = app(&db, clock.clone()).await;
        let mut worker = engine.worker().await.unwrap();
        let recovered = scope(&engine, root, root).await;
        assert_eq!(recovered["record"]["usage"]["admitted_model_calls"], calls);
        assert_eq!(
            recovered["record"]["usage"]["held_tokens"],
            saved["record"]["usage"]["held_tokens"]
        );
        assert_eq!(recovered["record"]["usage"]["uncertain_calls"], 1);
        clock.set(3000);
        drain(&mut worker).await;
        let total = scope(&engine, root, root).await;
        assert_eq!(total["record"]["usage"], recovered["record"]["usage"]);
        assert_eq!(total["members"].as_array().unwrap().len(), calls as usize);
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::BudgetExhausted
        );
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .tasks(assistant)
                .await
                .unwrap()
                .len(),
            if boundary == "root" { 0 } else { 1 }
        );
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, _) = app(&db, clock).await;
        assert_eq!(scope(&engine, root, root).await, total);
    }
}
