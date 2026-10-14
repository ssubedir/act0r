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
        TaskInspectionOptions, TaskInspectionService, TaskRelation, ports::*,
    },
    domain::{actors::*, shared_budget::*, *},
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
        .with_inspection_reader(store.clone()),
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
// Compare every column of every application table, including admission receipts and history.
async fn contents(db: &Path) -> Value {
    let db = pool(db).await;
    let tables:Vec<String>=sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != '_sqlx_migrations' ORDER BY name").fetch_all(&db).await.unwrap();
    let mut contents = serde_json::Map::new();
    for table in tables {
        let columns: Vec<String> =
            sqlx::query_scalar("SELECT name FROM pragma_table_info(?) ORDER BY cid")
                .bind(&table)
                .fetch_all(&db)
                .await
                .unwrap();
        let fields = columns
            .iter()
            .map(|c| format!("'{c}',\"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        assert!(
            std::iter::once(&table)
                .chain(columns.iter())
                .all(|s| s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        );
        let sql = format!("SELECT json_object({fields}) AS row FROM \"{table}\" ORDER BY row");
        // Identifiers come only from this fixture's migrations and are validated above.
        let rows: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(&db)
            .await
            .unwrap();
        contents.insert(table, json!(rows));
    }
    db.close().await;
    json!(contents)
}

#[tokio::test]
async fn delegated_human_wait_survives_restart_and_inspection_is_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("inspect.db");
    let app = open(&db).await;
    let child = create(&app, "answer", answer_config(true)).await;
    let actor=create(&app,"planner",planner_config(&[("assistant",child)],json!([
        {"kind":"delegate_task","worker":"assistant","goal":"Review this"},{"kind":"finish","summary":"Goal complete"}]))).await;
    let root = send(&app, actor, json!({"goal":"Original request"})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let before = contents(&db).await;
    let view = inspect(&app, root).await;
    let child_id = view.edges[0].child_task_id;
    assert_eq!(view.root_task_id, root);
    assert_eq!(view.nodes.len(), 2);
    assert_eq!(node(&view, root).goal.as_deref(), Some("Original request"));
    assert_eq!(node(&view, root).reason.code, "child_tasks");
    assert_eq!(node(&view, root).reason.related_tasks, vec![child_id]);
    let leaf = node(&view, child_id);
    assert_eq!(leaf.actor_id, child);
    assert_eq!(leaf.status, "waiting");
    assert_eq!(leaf.reason.code, "human_input");
    assert_eq!(leaf.wait.as_ref().unwrap().prompt, "May I continue?");
    assert_eq!(view.summary.pending, 2);
    assert!(!view.summary.settled);
    assert_eq!(view.summary.visible_runs, 2);
    assert_eq!(view.summary.visible_usage.admitted_model_calls, 2);
    assert_eq!(view.summary.visible_usage.charged_tokens(), 40);
    assert_eq!(view.budgets.len(), 2);
    for b in &view.budgets {
        assert_eq!(b.member_count, 2);
        assert_eq!(b.record.usage.admitted_model_calls, 2);
    }
    let from_child = inspect(&app, child_id).await;
    assert_eq!(from_child.root_task_id, root);
    assert_eq!(json!(from_child.nodes), json!(view.nodes));
    assert_eq!(before, contents(&db).await);
    let run = leaf.run_id.unwrap();
    let wait = leaf.wait.as_ref().unwrap().id;
    drop(worker);
    app.store.close().await;
    drop(app);
    let app = open(&db).await;
    // The read service also works with no installed workflow or worker definitions.
    let reopened = TaskInspectionService::new(app.store.clone())
        .inspect(root, TaskInspectionOptions::default())
        .await
        .unwrap();
    assert_eq!(json!(reopened), json!(view));
    app.engine
        .signal(run, wait, json!({"answer":"Yes"}))
        .await
        .unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let done = inspect(&app, child_id).await;
    assert!(done.summary.settled);
    assert_eq!(done.summary.succeeded, 2);
    assert_eq!(done.summary.pending, 0);
    assert_eq!(done.summary.visible_usage.admitted_model_calls, 4);
    assert_eq!(done.summary.visible_usage.charged_tokens(), 80);
    assert_eq!(
        node(&done, root).result.as_ref().unwrap()["answer"],
        "Goal complete"
    );
    assert_eq!(
        node(&done, child_id).result.as_ref().unwrap()["answer"],
        "Child result"
    );
}

#[tokio::test]
async fn unpublished_group_children_partial_results_and_limits_preserve_exact_budgets() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("groups.db")).await;
    let human = create(&app, "answer", answer_config(true)).await;
    let fast = create(&app, "answer", answer_config(false)).await;
    let actor=create(&app,"planner",planner_config(&[("human",human),("fast",fast)],json!([
        {"kind":"delegate_tasks","tasks":[{"worker":"human","goal":"Review"},{"worker":"fast","goal":"Calculate"}]},
        {"kind":"finish","summary":"Combined"}]))).await;
    let root = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    assert!(worker.advance_work().await.unwrap());
    assert!(worker.step().await.unwrap());
    let pending = inspect(&app, root).await;
    assert_eq!(pending.edges.len(), 2);
    assert!(
        pending
            .nodes
            .iter()
            .skip(1)
            .all(|n| !n.accepted && n.reason.code == "delegation_unpublished")
    );
    assert_eq!(pending.edges[0].group_id, pending.edges[1].group_id);
    assert!(pending.edges[0].group_id.is_some());
    settle(&mut worker).await;
    let view = inspect(&app, root).await;
    let child = view.edges[0].child_task_id;
    assert_eq!(node(&view, child).actor_id, human);
    assert_eq!(node(&view, view.edges[1].child_task_id).status, "succeeded");
    assert_eq!(node(&view, root).reason.related_tasks, vec![child]);
    assert_eq!(view.summary.visible_usage.admitted_model_calls, 3);
    for (nodes, depth, reason) in [(1, 16, "max_nodes"), (128, 0, "max_depth")] {
        let limited = app
            .agents
            .actors()
            .unwrap()
            .inspect_task(
                child,
                TaskInspectionOptions {
                    max_nodes: nodes,
                    max_depth: depth,
                },
            )
            .await
            .unwrap();
        assert_eq!(limited.nodes.len(), 1);
        assert!(!limited.requested_task_visible);
        assert!(!limited.summary.settled);
        assert_eq!(limited.truncations[0].omitted_children, 2);
        assert_eq!(limited.truncations[0].reason, reason);
        assert_eq!(limited.summary.visible_usage.admitted_model_calls, 1);
        assert_eq!(limited.budgets.len(), 2);
        for b in &limited.budgets {
            assert_eq!(b.member_count, 3);
            assert_eq!(b.record.usage.admitted_model_calls, 3);
        }
    }
    let leaf = node(&view, child);
    app.engine
        .signal(
            leaf.run_id.unwrap(),
            leaf.wait.as_ref().unwrap().id,
            json!({"answer":"Yes"}),
        )
        .await
        .unwrap();
    settle(&mut worker).await;
    let done = inspect(&app, root).await;
    assert!(done.summary.settled);
    assert_eq!(done.summary.succeeded, 3);
    assert_eq!(done.summary.visible_usage.admitted_model_calls, 5);
}

#[tokio::test]
async fn follow_up_intents_reminders_and_refusals_remain_in_the_goal() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("follow.db")).await;
    let child = create(&app, "answer", answer_config(false)).await;
    let actor = create(
        &app,
        "planner",
        planner_config(
            &[("assistant", child)],
            json!([
        {"kind":"schedule_follow_up","summary":"Progress saved","goal":"Continue","after_ms":500}]),
        ),
    )
    .await;
    let root = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    worker.step().await.unwrap();
    let pending = inspect(&app, root).await;
    let next = pending.edges[0].child_task_id;
    assert_eq!(pending.edges[0].relation, TaskRelation::FollowUp);
    assert_eq!(node(&pending, root).status, "succeeded");
    assert_eq!(node(&pending, next).reason.code, "follow_up_unregistered");
    assert_eq!(inspect(&app, next).await.root_task_id, root);
    settle(&mut worker).await;
    let scheduled = inspect(&app, next).await;
    assert_eq!(node(&scheduled, next).status, "scheduled");
    assert_eq!(node(&scheduled, next).reason.deadline_at, Some(1500));
    app.agents
        .actors()
        .unwrap()
        .cancel_reminder(next)
        .await
        .unwrap();
    let cancelled = inspect(&app, next).await;
    assert_eq!(node(&cancelled, next).status, "cancelled");
    assert!(cancelled.summary.settled);
    let independent = send(&app, actor, json!({"goal":"Another goal"})).await;
    settle(&mut worker).await;
    app.clock.0.store(2000, Ordering::SeqCst);
    settle(&mut worker).await;
    let chain = inspect(&app, independent).await;
    assert_eq!(chain.nodes.len(), 3);
    assert_eq!(chain.edges.len(), 2);
    assert_eq!(chain.summary.succeeded, 2);
    assert_eq!(chain.summary.stopped, 1);
    assert!(chain.summary.settled);
    let refused = chain.nodes.last().unwrap();
    assert_eq!(refused.status, "refused");
    assert_eq!(refused.error.as_ref().unwrap()["code"], "chain_limit");
    assert!(!refused.accepted);
    assert_eq!(
        inspect(&app, refused.task_id).await.root_task_id,
        independent
    );
    assert_eq!(chain.summary.visible_usage.admitted_model_calls, 2);
    assert_eq!(
        chain
            .budgets
            .iter()
            .filter(|b| b.record.kind == SharedBudgetKind::Goal)
            .count(),
        1
    );
}

#[tokio::test]
async fn actor_pause_run_pause_and_supervision_have_distinct_reasons() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("controls.db")).await;
    let actor = create(&app, "answer", answer_config(true)).await;
    let root = send(&app, actor, json!({})).await;
    let queued = send(&app, actor, json!({"goal":"Next task"})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let actors = app.agents.actors().unwrap();
    actors.pause(actor).await.unwrap();
    let waiting = inspect(&app, root).await;
    assert_eq!(waiting.nodes[0].reason.code, "human_input");
    assert_eq!(waiting.nodes[0].status, "waiting");
    assert_eq!(
        waiting.nodes[0].actor.as_ref().unwrap().status,
        AgentStatus::Paused
    );
    let behind = inspect(&app, queued).await;
    assert_eq!(behind.nodes[0].reason.code, "actor_paused");
    assert!(behind.nodes[0].run_id.is_none());
    app.engine
        .pause(waiting.nodes[0].run_id.unwrap())
        .await
        .unwrap();
    let paused = inspect(&app, root).await;
    assert_eq!(paused.nodes[0].reason.code, "run_paused");
    assert!(paused.nodes[0].wait.is_some());
    let mut cfg = answer_config(false);
    cfg["script"] = json!([{"kind":"finish","summary":""}]);
    cfg["supervision"] = json!({"max_consecutive_failures":1});
    let unhealthy = create(&app, "answer", cfg).await;
    let failed = send(&app, unhealthy, json!({})).await;
    let stopped = send(&app, unhealthy, json!({})).await;
    settle(&mut worker).await;
    let failure = inspect(&app, failed).await;
    assert_eq!(failure.nodes[0].status, "failed");
    assert!(failure.nodes[0].error.is_some());
    assert!(failure.summary.settled);
    let stopped = inspect(&app, stopped).await;
    assert_eq!(stopped.nodes[0].status, "paused");
    assert_eq!(stopped.nodes[0].reason.code, "actor_supervision");
    assert_eq!(
        stopped.nodes[0]
            .actor
            .as_ref()
            .unwrap()
            .supervision_pause
            .as_ref()
            .unwrap()
            .task_id,
        failed
    );
}

#[tokio::test]
async fn exhausted_goal_and_rejected_delegation_show_saved_errors_without_fake_usage() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("exhausted.db")).await;
    let child = create(&app, "answer", answer_config(false)).await;
    let mut cfg = planner_config(
        &[("assistant", child)],
        json!([{"kind":"schedule_follow_up","summary":"Progress","goal":"Continue","after_ms":500}]),
    );
    cfg["goal_budget"]["max_model_calls"] = json!(1);
    let actor = create(&app, "planner", cfg).await;
    let root = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    app.clock.0.store(2000, Ordering::SeqCst);
    settle(&mut worker).await;
    let view = inspect(&app, root).await;
    let next = view.nodes.last().unwrap();
    assert!(view.summary.settled);
    assert_eq!(view.summary.succeeded, 1);
    assert_eq!(view.summary.stopped, 1);
    assert_eq!(next.status, "budget_exhausted");
    assert_eq!(next.error.as_ref().unwrap()["code"], "goal_model_budget");
    assert_eq!(next.usage.as_ref().unwrap().admitted_model_calls, 0);
    assert_eq!(view.summary.visible_usage.admitted_model_calls, 1);
    assert_eq!(view.budgets.len(), 3);
    let goal = view
        .budgets
        .iter()
        .find(|b| b.record.kind == SharedBudgetKind::Goal)
        .unwrap();
    assert_eq!(goal.member_count, 2);
    assert_eq!(
        goal.record.exhaustion.as_ref().unwrap().run_id,
        next.run_id.unwrap()
    );
    let actor=create(&app,"planner",planner_config(&[("assistant",child)],json!([
        {"kind":"delegate_task","worker":"assistant","goal":"Help"},{"kind":"finish","summary":"Handled"}]))).await;
    // Native publication refuses a full recipient without inventing an accepted task.
    app.agents.actors().unwrap().pause(child).await.unwrap();
    let capacity = app
        .agents
        .actors()
        .unwrap()
        .queue(child)
        .await
        .unwrap()
        .capacity;
    for _ in 0..capacity {
        send(&app, child, json!({})).await;
    }
    let root = send(&app, actor, json!({})).await;
    settle(&mut worker).await;
    let view = inspect(&app, root).await;
    let refused = view.nodes.last().unwrap();
    assert_eq!(refused.reason.code, "delegation_refused");
    assert!(!refused.accepted);
    assert!(refused.run_id.is_none());
    assert!(refused.error.is_some());
    assert!(view.summary.settled);
    assert_eq!(view.summary.visible_usage.admitted_model_calls, 2);
    assert_eq!(inspect(&app, refused.task_id).await.root_task_id, root);
}

#[tokio::test]
async fn forwarding_is_physical_and_payloads_and_correlations_cannot_merge_goals() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(&dir.path().join("forward.db")).await;
    let worker_id = create(&app, "answer", answer_config(true)).await;
    let actors = app.agents.actors().unwrap();
    let coordinator = actors
        .create(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":worker_id}),
        )
        .await
        .unwrap();
    let mut original = message(coordinator, json!({"goal":"Original"}));
    original.correlation_id = Some(MessageId::new());
    let root = actors.send(original.clone()).await.unwrap();
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let view = inspect(&app, root).await;
    let child = view.edges[0].child_task_id;
    assert_eq!(view.edges[0].relation, TaskRelation::Forwarded);
    assert!(node(&view, root).run_id.is_none());
    assert_eq!(view.summary.visible_runs, 1);
    assert_eq!(view.summary.visible_usage.admitted_model_calls, 1);
    assert_eq!(inspect(&app, child).await.root_task_id, root);
    let bounded = actors
        .inspect_task(
            root,
            TaskInspectionOptions {
                max_nodes: 1,
                max_depth: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(bounded.nodes[0].reason.code, "forwarded_task");
    assert_eq!(bounded.nodes[0].reason.related_tasks, vec![child]);
    let mut spoof = message(
        worker_id,
        json!({"goal":"Independent","parent_id":root,"root_task_id":root}),
    );
    spoof.correlation_id = original.correlation_id;
    let other = actors.send(spoof).await.unwrap();
    let independent = inspect(&app, other).await;
    assert_eq!(independent.root_task_id, other);
    assert_eq!(independent.nodes.len(), 1);
    assert!(independent.edges.is_empty());
    assert_eq!(inspect(&app, root).await.nodes.len(), 2);
}

#[tokio::test]
async fn activity_admission_durable_timers_and_standalone_reminders_are_observational() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("work.db");
    let app = open(&db).await;
    let actor = create(&app, "answer", answer_config(false)).await;
    let root = send(&app, actor, json!({})).await;
    let mut worker = app.engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    assert_eq!(
        inspect(&app, root).await.nodes[0].reason.code,
        "activity_queued"
    );
    let execution = worker.admit().await.unwrap().unwrap();
    let before = contents(&db).await;
    let admitted = inspect(&app, root).await;
    assert_eq!(admitted.nodes[0].reason.code, "activity_in_flight");
    assert_eq!(admitted.summary.visible_usage.admitted_model_calls, 1);
    assert!(admitted.summary.visible_usage.held_tokens > 0);
    assert_eq!(before, contents(&db).await);
    worker
        .complete(
            execution,
            Outcome::Failure(Failure::new("busy", "Try later", true)),
        )
        .await
        .unwrap();
    let retry = inspect(&app, root).await;
    assert_eq!(retry.nodes[0].reason.code, "activity_retry");
    let due = retry.nodes[0].reason.deadline_at.unwrap();
    assert!(due > 1000);
    app.clock.0.store(due, Ordering::SeqCst);
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
    settle(&mut worker).await;
    let actors = app.agents.actors().unwrap();
    let timer_actor=actors.create(AgentId::new(),DefinitionId::new("workflow_worker",1),
        json!({"workflow":{"name":"timer_demo","version":1},"input":{"message":"Timer","wait_ms":500},"attempt_limit":3})).await.unwrap();
    let timer_task = send(&app, timer_actor, json!({})).await;
    settle(&mut worker).await;
    let timer = inspect(&app, timer_task).await;
    assert_eq!(timer.nodes[0].reason.code, "durable_timer");
    assert_eq!(timer.nodes[0].reason.deadline_at, Some(due + 500));
    let reminder = actors
        .schedule_task(
            message(actor, json!({"goal":"Later"})),
            TimerDeadline::After { milliseconds: 800 },
        )
        .await
        .unwrap();
    let saved = inspect(&app, reminder.id).await;
    assert_eq!(saved.root_task_id, reminder.id);
    assert_eq!(saved.nodes.len(), 1);
    assert_eq!(saved.nodes[0].reason.code, "reminder_scheduled");
    assert!(!saved.nodes[0].accepted);
    assert_eq!(saved.summary.visible_runs, 0);
}

#[tokio::test]
async fn invalid_limits_unknown_handles_and_corrupt_ownership_fail_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("invalid.db");
    let app = open(&db).await;
    let actors = app.agents.actors().unwrap();
    for (nodes, depth) in [(0, 16), (513, 16), (128, 129)] {
        assert!(matches!(
            actors
                .inspect_task(
                    MessageId::new(),
                    TaskInspectionOptions {
                        max_nodes: nodes,
                        max_depth: depth
                    }
                )
                .await,
            Err(act0r::Error::Domain(DomainError::InvalidConfiguration(_)))
        ));
    }
    assert!(matches!(
        actors
            .inspect_task(MessageId::new(), TaskInspectionOptions::default())
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
    sql.close().await;
    assert!(
        matches!(actors.inspect_task(root,TaskInspectionOptions::default()).await,Err(act0r::Error::Storage(message)) if message.contains("cyclic"))
    );
}
