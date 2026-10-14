use act0r::{
    Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, integration::AgentInvocation, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn request(actor: AgentId) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient: actor,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":"Solve the submitted goal using an assistant."}),
    }
}
fn config(worker: AgentId, script: Value, tasks: u32, depth: u32) -> Value {
    json!({"goal":"Planner goal","target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":10,"max_tokens":100000,"max_output_tokens":512,"max_input_bytes":65536,
        "workers":[{"name":"assistant","agent_id":worker,"description":"A permitted assistant"}],"delegation":{"max_tasks":tasks,"max_depth":depth},"script":script})
}
fn script() -> Value {
    json!([{"kind":"delegate_task","worker":"assistant","goal":"Explain durable recovery."},{"kind":"finish","summary":"Used the saved assistant result."}])
}
async fn answer(agents: &AgentService, human: bool) -> AgentId {
    let mut config: Value =
        serde_json::from_str(include_str!("../examples/configs/answer-scripted.json")).unwrap();
    if !human {
        config["script"] = json!([{"kind":"finish","summary":"The recorded child answer."}]);
    }
    agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            config,
            10,
            AgentQueuePolicy { task_capacity: 2 },
        )
        .await
        .unwrap()
}
async fn planner(
    agents: &AgentService,
    worker: AgentId,
    script: Value,
    tasks: u32,
    depth: u32,
) -> AgentId {
    agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            config(worker, script, tasks, depth),
            20,
            AgentQueuePolicy { task_capacity: 2 },
        )
        .await
        .unwrap()
}
async fn drain(worker: &mut Worker) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline, "worker did not settle");
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
async fn intent(worker: &mut Worker, agents: &AgentService, task: MessageId) -> RunId {
    for _ in 0..50 {
        if let Some(id) = agents.actors().unwrap().task(task).await.unwrap().run_id
            && agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .delegation
                .is_some()
        {
            return id;
        }
        assert!(worker.step().await.unwrap());
    }
    panic!("no delegation intent");
}

#[tokio::test]
async fn saved_waits_resume_once_and_parent_prompts_include_the_child_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("restart.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let assistant = answer(&agents, true).await;
    let actor = planner(&agents, assistant, script(), 2, 1).await;
    let message = request(actor);
    let task = message.id;
    agents
        .actors()
        .unwrap()
        .send(message.clone())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let parent = agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(parent.status, AgentTaskStatus::Waiting);
    assert!(parent.wait.is_none());
    let run = parent.run_id.unwrap();
    let child = parent.delegation.unwrap().id;
    let waiting = agents.actors().unwrap().task(child).await.unwrap();
    let child_run = waiting.run_id.unwrap();
    let wait = waiting.wait.unwrap().id;
    assert!(
        engine
            .signal(run, wait, json!({"answer":"forged"}))
            .await
            .is_err()
    );
    engine.pause(run).await.unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .delegation
            .unwrap()
            .id,
        child
    );
    engine
        .signal(child_run, wait, json!({"answer":"short"}))
        .await
        .unwrap();
    drain(&mut worker).await;
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Paused
    );
    let paused = engine.status(run).await.unwrap();
    assert!(paused.pending_delegation().is_none());
    assert_eq!(
        paused.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "succeeded"
    );
    assert!(
        paused.activities().last().unwrap().spec.input["prompt"]
            .as_str()
            .unwrap()
            .contains("durable agent saves")
    );
    engine.resume(run).await.unwrap();
    drain(&mut worker).await;
    let done = agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(done.status, AgentTaskStatus::Succeeded);
    assert_eq!(done.usage.unwrap().admitted_model_calls, 2);
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
        engine
            .history(run)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "delegation_completed")
            .count(),
        1
    );
    let before = json!(engine.status(run).await.unwrap());
    agents.actors().unwrap().send(message).await.unwrap();
    assert!(!worker.step().await.unwrap());
    assert_eq!(json!(engine.status(run).await.unwrap()), before);
}

#[tokio::test]
async fn cancellation_before_publication_or_during_child_admission_keeps_charges_and_slots() {
    for boundary in ["requested", "queued", "admitted"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("cancel.db")).await.unwrap();
        let assistant = answer(&agents, false).await;
        let actor = planner(&agents, assistant, script(), 2, 1).await;
        let task = agents.actors().unwrap().send(request(actor)).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        let run = intent(&mut worker, &agents, task).await;
        let child = engine
            .status(run)
            .await
            .unwrap()
            .pending_delegation()
            .unwrap()
            .id;
        let execution = if boundary == "requested" {
            None
        } else {
            assert!(worker.advance_work().await.unwrap()); // Publish the child.
            if boundary == "admitted" {
                assert!(worker.advance_work().await.unwrap()); // Create its Run.
                Some(worker.admit().await.unwrap().unwrap())
            } else {
                None
            }
        };
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .cancel_task(task)
                .await
                .unwrap()
                .status,
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
        if let Some(execution) = execution {
            assert!(worker.advance_work().await.unwrap()); // Propagate cancellation.
            assert_eq!(
                agents.actors().unwrap().task(child).await.unwrap().status,
                AgentTaskStatus::Cancelling
            );
            let outcome = worker.execute(execution.clone()).await.unwrap();
            worker.complete(execution, outcome).await.unwrap();
        }
        drain(&mut worker).await;
        let parent = agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(parent.status, AgentTaskStatus::Cancelled);
        assert_eq!(parent.usage.unwrap().admitted_model_calls, 1);
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .queue(actor)
                .await
                .unwrap()
                .available,
            2
        );
        if boundary == "requested" {
            assert!(agents.actors().unwrap().task(child).await.is_err());
        } else {
            let child = agents.actors().unwrap().task(child).await.unwrap();
            assert_eq!(child.status, AgentTaskStatus::Cancelled);
            if boundary == "admitted" {
                assert_eq!(child.usage.unwrap().charged_tokens(), 20);
            } else {
                assert!(child.run_id.is_none());
            }
        }
    }
}

#[tokio::test]
async fn queue_rejections_are_saved_without_child_admission_and_budgets_cannot_be_reset() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("limits.db")).await.unwrap();
    let assistant = answer(&agents, true).await;
    agents
        .actors()
        .unwrap()
        .send(request(assistant))
        .await
        .unwrap();
    agents
        .actors()
        .unwrap()
        .send(request(assistant))
        .await
        .unwrap();
    let actor = planner(&agents, assistant, script(), 2, 1).await;
    let task = agents.actors().unwrap().send(request(actor)).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let parent = agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(parent.status, AgentTaskStatus::Succeeded);
    let run = engine.status(parent.run_id.unwrap()).await.unwrap();
    let rejected = &run.record().delegations[0];
    assert_eq!(
        rejected.outcome.as_ref().unwrap()["error"]["code"],
        "queue_full"
    );
    assert!(agents.actors().unwrap().task(rejected.id).await.is_err());
    let assistant = answer(&agents, false).await;
    let actor=planner(&agents,assistant,json!([{"kind":"delegate_task","worker":"assistant","goal":"one"},{"kind":"delegate_task","worker":"assistant","goal":"two"}]),1,1).await;
    let task = agents.actors().unwrap().send(request(actor)).await.unwrap();
    drain(&mut worker).await;
    let parent = agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(parent.status, AgentTaskStatus::Failed);
    let run = engine.status(parent.run_id.unwrap()).await.unwrap();
    assert_eq!(run.record().delegations.len(), 1);
    assert_eq!(run.record().usage.admitted_model_calls, 2);
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
async fn descendant_requests_share_the_root_task_and_depth_limits() {
    for limit in ["count", "depth"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("nested.db")).await.unwrap();
        let assistant = answer(&agents, false).await;
        let nested = planner(&agents, assistant, script(), 4, 4).await;
        let root = planner(
            &agents,
            nested,
            script(),
            if limit == "count" { 1 } else { 4 },
            if limit == "depth" { 1 } else { 4 },
        )
        .await;
        let task = agents.actors().unwrap().send(request(root)).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        let nested_task = agents
            .actors()
            .unwrap()
            .tasks(nested)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let nested_run = engine.status(nested_task.run_id.unwrap()).await.unwrap();
        assert_eq!(
            nested_run.record().delegations[0].outcome.as_ref().unwrap()["error"]["code"],
            if limit == "count" {
                "delegation_budget"
            } else {
                "delegation_depth"
            }
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
    }
}

async fn raw_planner(agents: &AgentService, id: AgentId, target: AgentId) {
    let registration = agents
        .registrations()
        .unwrap()
        .into_iter()
        .find(|r| r.adapter.name == "planner")
        .unwrap();
    agents
        .actors()
        .unwrap()
        .create(
            id,
            RegisteredAgentDefinition.id(),
            json!(RegisteredAgentConfig {
                registration,
                input: config(target, script(), 4, 4),
                attempt_limit: 20,
                memory: None
            }),
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn nested_delegation_resumes_or_cancels_through_every_saved_child_link() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("nested-restart.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let assistant = answer(&agents, true).await;
        let nested = planner(&agents, assistant, script(), 4, 4).await;
        let root = planner(&agents, nested, script(), 2, 2).await;
        let task = agents.actors().unwrap().send(request(root)).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let nested_task = agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .delegation
            .unwrap()
            .id;
        let child = agents
            .actors()
            .unwrap()
            .task(nested_task)
            .await
            .unwrap()
            .delegation
            .unwrap()
            .id;
        let waiting = agents.actors().unwrap().task(child).await.unwrap();
        assert_eq!(waiting.status, AgentTaskStatus::Waiting);
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        if cancel {
            assert_eq!(
                agents
                    .actors()
                    .unwrap()
                    .cancel_task(task)
                    .await
                    .unwrap()
                    .status,
                AgentTaskStatus::Cancelling
            );
        } else {
            engine
                .signal(
                    waiting.run_id.unwrap(),
                    waiting.wait.unwrap().id,
                    json!({"answer":"short"}),
                )
                .await
                .unwrap();
        }
        drain(&mut worker).await;
        for task in [task, nested_task, child] {
            let view = agents.actors().unwrap().task(task).await.unwrap();
            assert_eq!(
                view.status,
                if cancel {
                    AgentTaskStatus::Cancelled
                } else {
                    AgentTaskStatus::Succeeded
                }
            );
            assert_eq!(
                view.usage.unwrap().admitted_model_calls,
                if cancel { 1 } else { 2 }
            );
        }
        for actor in [root, nested, assistant] {
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
    }
}

#[tokio::test]
async fn self_ancestry_and_independent_actor_wait_cycles_are_rejected() {
    for independent in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("cycles.db")).await.unwrap();
        let a = AgentId::new();
        let b = if independent { AgentId::new() } else { a };
        raw_planner(&agents, a, b).await;
        if independent {
            raw_planner(&agents, b, a).await;
        }
        let task = agents.actors().unwrap().send(request(a)).await.unwrap();
        let second = if independent {
            Some(agents.actors().unwrap().send(request(b)).await.unwrap())
        } else {
            None
        };
        let mut worker = engine.worker().await.unwrap();
        let run = intent(&mut worker, &agents, task).await;
        let cycle_run = if let Some(second) = second {
            intent(&mut worker, &agents, second).await
        } else {
            run
        };
        drain(&mut worker).await;
        let cycle = engine.status(cycle_run).await.unwrap();
        assert_eq!(
            cycle.record().delegations[0].outcome.as_ref().unwrap()["error"]["code"],
            "delegation_cycle"
        );
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Succeeded
        );
    }
}

#[tokio::test]
async fn child_coding_results_and_failed_continuations_remain_inspectable() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("coding.db")).await.unwrap();
    let task: Value =
        serde_json::from_str(include_str!("../examples/configs/task-scripted.json")).unwrap();
    let coding = agents
        .create_registered_actor(
            DefinitionId::new("coding", 1),
            Some(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("examples/benchmark-cases/addition/source"),
            ),
            task["agent"].clone(),
            40,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let actor = planner(&agents, coding, script(), 2, 1).await;
    let task = agents.actors().unwrap().send(request(actor)).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let run = engine
        .status(
            agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .run_id
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["result"]["tests_passed"],
        true
    );
    let assistant = answer(&agents, false).await;
    let mut config = config(assistant, script(), 2, 1);
    config["context"] = json!(act0r::domain::agent::ContextPolicy::default());
    // Fit the initial request exactly; the child's result then exceeds that pinned bound.
    let registration = agents
        .registrations()
        .unwrap()
        .into_iter()
        .find(|r| r.adapter.name == "planner")
        .unwrap();
    let native = crate_run(&registration, config.clone());
    let model_request: act0r::domain::agent::ModelRequest =
        serde_json::from_value(native.activities()[0].spec.input.clone()).unwrap();
    config["max_input_bytes"] = json!(model_request.context_input_bytes());
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            config,
            20,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let task = agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            payload: json!({}),
            ..request(actor)
        })
        .await
        .unwrap();
    drain(&mut worker).await;
    let run = engine
        .status(
            agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .run_id
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(run.record().status, RunStatus::Failed);
    assert_eq!(run.record().error.as_ref().unwrap().code, "context_limit");
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "succeeded"
    );
}
fn crate_run(registration: &act0r::domain::integration::AgentRegistration, input: Value) -> Run {
    let definition = act0r::domain::integration::RegisteredAgentWorkflow::new(
        registration.clone(),
        std::sync::Arc::new(act0r::domain::planner::PlannerWorkflow::default()),
    )
    .unwrap();
    Run::start(
        &definition,
        json!(AgentInvocation {
            registration: registration.clone(),
            input
        }),
        20,
        0,
    )
    .unwrap()
}

#[tokio::test]
async fn publication_and_child_reply_failures_roll_back_before_retry() {
    use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let assistant = answer(&agents, false).await;
    let actor = planner(&agents, assistant, script(), 2, 1).await;
    let task = agents.actors().unwrap().send(request(actor)).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    let run = intent(&mut worker, &agents, task).await;
    let saved = engine.status(run).await.unwrap();
    let child = saved.pending_delegation().unwrap().id;
    let mut connection = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&db)
            .busy_timeout(Duration::from_secs(5)),
    )
    .await
    .unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_child BEFORE INSERT ON agent_tasks BEGIN SELECT RAISE(ABORT,'child rollback'); END;")
        .execute(&mut connection).await.unwrap();
    assert!(worker.advance_work().await.is_err());
    assert_eq!(json!(engine.status(run).await.unwrap()), json!(saved));
    assert!(agents.actors().unwrap().task(child).await.is_err());
    let mailbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_mailbox WHERE id=?")
        .bind(child.to_string())
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(mailbox_count, 0);
    let publication: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT root_run_id,child_task_id FROM agent_delegations WHERE id=?")
            .bind(child.to_string())
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(publication, (None, None));
    sqlx::raw_sql("DROP TRIGGER reject_child;")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(worker.advance_work().await.unwrap());
    for _ in 0..30 {
        if agents
            .actors()
            .unwrap()
            .task(child)
            .await
            .unwrap()
            .status
            .is_terminal()
        {
            break;
        }
        assert!(worker.step().await.unwrap());
    }
    assert_eq!(
        agents.actors().unwrap().task(child).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    let saved = json!(engine.status(run).await.unwrap());
    let history = json!(engine.history(run).await.unwrap());
    sqlx::raw_sql("CREATE TRIGGER reject_child_reply BEFORE INSERT ON history WHEN json_extract(NEW.payload,'$.kind')='delegation_completed' BEGIN SELECT RAISE(ABORT,'reply rollback'); END;")
        .execute(&mut connection).await.unwrap();
    let mut rejected = false;
    for _ in 0..10 {
        if worker.advance_work().await.is_err() {
            rejected = true;
            break;
        }
    }
    assert!(
        rejected,
        "child reply did not reach the failing transaction"
    );
    assert_eq!(json!(engine.status(run).await.unwrap()), saved);
    assert_eq!(json!(engine.history(run).await.unwrap()), history);
    sqlx::raw_sql("DROP TRIGGER reject_child_reply;")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let done = engine.status(run).await.unwrap();
    assert_eq!(done.record().status, RunStatus::Succeeded);
    assert_eq!(done.record().usage.admitted_model_calls, 2);
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
        engine
            .history(run)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "delegation_completed")
            .count(),
        1
    );
}

#[tokio::test]
async fn cancellation_waits_for_child_reconciliation_and_retains_uncertain_charges() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("uncertain.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let task_config: Value =
        serde_json::from_str(include_str!("../examples/configs/task-scripted.json")).unwrap();
    let mut coding_config = task_config["agent"].clone();
    coding_config["repeat_safe_tests"] = json!(false);
    coding_config["script"] = json!([{"kind":"run_tests"},{"kind":"finish","summary":"Finished"}]);
    let coding = agents
        .create_registered_actor(
            DefinitionId::new("coding", 1),
            Some(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("examples/benchmark-cases/addition/source"),
            ),
            coding_config,
            20,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let actor = planner(&agents, coding, script(), 2, 1).await;
    let task = agents.actors().unwrap().send(request(actor)).await.unwrap();
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
    assert!(worker.step().await.unwrap()); // Save the child's run_tests decision.
    let child_run = agents
        .actors()
        .unwrap()
        .task(child)
        .await
        .unwrap()
        .run_id
        .unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.run_id, child_run);
    assert_eq!(
        engine
            .status(child_run)
            .await
            .unwrap()
            .activities()
            .iter()
            .find(|a| a.id == execution.activity_id)
            .unwrap()
            .spec
            .recovery,
        RecoveryPolicy::RequiresReconciliation
    );
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert_eq!(
        engine.status(child_run).await.unwrap().record().status,
        RunStatus::Blocked
    );
    let before = json!(engine.status(child_run).await.unwrap());
    let charges = engine
        .status(child_run)
        .await
        .unwrap()
        .record()
        .usage
        .charged_tokens();
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .cancel_task(task)
            .await
            .unwrap()
            .status,
        AgentTaskStatus::Cancelling
    );
    drain(&mut worker).await;
    assert_eq!(json!(engine.status(child_run).await.unwrap()), before);
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
    engine
        .reconcile(
            child_run,
            ReconciliationRequest {
                activity_id: execution.activity_id,
                attempt_id: execution.attempt_id,
                decision: ReconciliationDecision::Fail {
                    message: "Operator chose to stop after inspecting uncertain tests.".into(),
                },
                evidence: "No saved test result; workspace inspected.".into(),
            },
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Cancelled
    );
    assert_eq!(
        agents.actors().unwrap().task(child).await.unwrap().status,
        AgentTaskStatus::Failed
    );
    assert_eq!(
        engine
            .status(child_run)
            .await
            .unwrap()
            .record()
            .usage
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
async fn pause_before_publication_survives_restart_without_creating_a_child() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("paused.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let assistant = answer(&agents, false).await;
    let actor = planner(&agents, assistant, script(), 2, 1).await;
    let task = agents.actors().unwrap().send(request(actor)).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    let run = intent(&mut worker, &agents, task).await;
    let child = engine
        .status(run)
        .await
        .unwrap()
        .pending_delegation()
        .unwrap()
        .id;
    engine.pause(run).await.unwrap();
    drain(&mut worker).await;
    assert!(agents.actors().unwrap().task(child).await.is_err());
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.has_active_runs().await.unwrap());
    engine.resume(run).await.unwrap();
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Waiting
    );
    drain(&mut worker).await;
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(
        agents.actors().unwrap().tasks(assistant).await.unwrap()[0]
            .task
            .id,
        child
    );
}

#[tokio::test]
async fn missing_planner_definition_retains_unpublished_intents_and_saved_child_results() {
    use act0r::{
        Engine,
        adapters::{
            agent_executor::AgentExecutor, agents::AnswerAgentAdapter, demo::DemoExecutor,
            file_lock::FileOwnership, model::ModelRouter, sqlite::SqliteStore,
            system_clock::SystemClock, workspace::LocalTools,
        },
        application::{ActorService, AgentRegistry},
    };
    use std::sync::Arc;
    for published in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("missing.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let assistant = answer(&agents, false).await;
        let actor = planner(&agents, assistant, script(), 2, 1).await;
        let task = agents.actors().unwrap().send(request(actor)).await.unwrap();
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
            assert!(worker.advance_work().await.unwrap());
            for _ in 0..20 {
                if agents
                    .actors()
                    .unwrap()
                    .task(child)
                    .await
                    .unwrap()
                    .status
                    .is_terminal()
                {
                    break;
                }
                assert!(worker.step().await.unwrap());
            }
            assert_eq!(
                agents.actors().unwrap().task(child).await.unwrap().status,
                AgentTaskStatus::Succeeded
            );
        }
        drop(worker);
        drop(agents);
        drop(engine);
        let store = Arc::new(SqliteStore::open(&db).await.unwrap());
        let registry = Arc::new(AgentRegistry::new(vec![Arc::new(AnswerAgentAdapter)]).unwrap());
        let engine = Engine::new(
            store.clone(),
            Arc::new(AgentExecutor::new(
                Arc::new(ModelRouter::new(None).unwrap()),
                Arc::new(LocalTools::new(dir.path().join(".act0r/workspaces"))),
                Arc::new(DemoExecutor),
            )),
            Arc::new(SystemClock),
            Arc::new(FileOwnership::new(&db)),
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
            .with_agent_registry(registry),
        );
        let engine = engine.with_coordinator(actors.clone());
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let blocked = engine.status(run).await.unwrap();
        assert_eq!(blocked.record().status, RunStatus::Blocked);
        assert_eq!(blocked.pending_delegation().unwrap().id, child);
        assert_eq!(blocked.record().usage.admitted_model_calls, 1);
        if !published {
            assert!(actors.task(child).await.is_err());
        }
        drop(worker);
        drop(actors);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        engine.resume(run).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
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
            engine
                .status(run)
                .await
                .unwrap()
                .record()
                .usage
                .admitted_model_calls,
            2
        );
    }
}

#[tokio::test]
#[ignore = "subprocess native delegation checkpoint fixture"]
async fn delegation_crash_child() {
    let Ok(db) = std::env::var("DELEGATION_DB") else {
        return;
    };
    let task: MessageId = std::env::var("DELEGATION_TASK").unwrap().parse().unwrap();
    let (engine, agents) = local_app(db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    let run = intent(&mut worker, &agents, task).await;
    if std::env::var("DELEGATION_BOUNDARY").unwrap() == "published" {
        assert!(worker.advance_work().await.unwrap());
    }
    fs::write(
        std::env::var("DELEGATION_MARKER").unwrap(),
        serde_json::to_vec(&engine.status(run).await.unwrap()).unwrap(),
    )
    .unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn killed_workers_reuse_the_committed_intent_and_published_child_identity() {
    for boundary in ["requested", "published"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("kill.db");
        let marker = dir.path().join("marker.json");
        let (engine, agents) = local_app(&db).await.unwrap();
        let assistant = answer(&agents, false).await;
        let actor = planner(&agents, assistant, script(), 2, 1).await;
        let task = agents.actors().unwrap().send(request(actor)).await.unwrap();
        drop(agents);
        drop(engine);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "delegation_crash_child",
                "--nocapture",
            ])
            .env("DELEGATION_DB", &db)
            .env("DELEGATION_TASK", task.to_string())
            .env("DELEGATION_BOUNDARY", boundary)
            .env("DELEGATION_MARKER", &marker)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !marker.exists() {
            assert!(child.try_wait().unwrap().is_none());
            if Instant::now() > deadline {
                child.kill().unwrap();
                panic!("checkpoint timeout");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value = serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
        let id: MessageId =
            serde_json::from_value(saved["record"]["delegations"][0]["id"].clone()).unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        let tasks = agents.actors().unwrap().tasks(assistant).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task.id, id);
        let run = engine
            .status(
                agents
                    .actors()
                    .unwrap()
                    .task(task)
                    .await
                    .unwrap()
                    .run_id
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(run.record().usage.admitted_model_calls, 2);
    }
}
