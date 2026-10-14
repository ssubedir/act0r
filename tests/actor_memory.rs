use act0r::{
    Engine, Worker,
    adapters::{
        agents::{AnswerAgentAdapter, CodingAgentAdapter, PlannerAgentAdapter},
        workspace::LocalWorkspaces,
    },
    application::{ActorService, AgentService, ports::*},
    bootstrap::local_app,
    domain::{actors::*, agent::*, integration::*, *},
};
use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Command as ProcessCommand, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

fn config() -> Value {
    json!({"goal":"Answer this task.","target":{"provider":"scripted","model":"memory","endpoint":"","protocol":"chat_completions"},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "memory":{"max_entries":8,"max_bytes":8192},"script":[{"kind":"finish","summary":"The preferred color is blue."}]})
}
#[test]
fn memory_clear_counter_overflow_leaves_the_original_memory_intact() {
    let mut memory = ActorMemory::new(MemoryPolicy::default()).unwrap();
    memory
        .remember(MemoryEntry {
            task_id: MessageId::new(),
            goal: "Saved task".into(),
            result: json!({"answer":"saved"}),
        })
        .unwrap();
    memory.generation = u64::MAX;
    let saved = memory.clone();
    assert!(memory.clear().is_err());
    assert_eq!(memory, saved);
}
async fn create(agents: &AgentService, cfg: Value) -> AgentId {
    agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            cfg,
            20,
            AgentQueuePolicy::default(),
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
async fn drain(worker: &mut Worker) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline, "memory fixture did not settle");
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
async fn run_for(engine: &Engine, actors: &ActorService, task: MessageId) -> Run {
    engine
        .status(actors.task(task).await.unwrap().task.run_id.unwrap())
        .await
        .unwrap()
}
fn request(run: &Run) -> ModelRequest {
    serde_json::from_value(
        run.activities()
            .iter()
            .rev()
            .find(|a| a.spec.handler.name == "agent.model")
            .unwrap()
            .spec
            .input
            .clone(),
    )
    .unwrap()
}
async fn execute_one(worker: &mut Worker) {
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
}
async fn sql(db: &Path, statement: &'static str) {
    let conn =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(db))
            .await
            .unwrap();
    sqlx::raw_sql(statement).execute(&conn).await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn successful_tasks_survive_restart_and_duplicates_without_cross_actor_leakage() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("memory.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let actor = create(&agents, config()).await;
    let other = create(&agents, config()).await;
    let first = task(actor, "Remember my preferred color.");
    actors.send(first.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let saved = actors.memory(actor).await.unwrap().unwrap();
    assert_eq!(saved.entries.len(), 1);
    assert_eq!(saved.entries[0].task_id, first.id);
    assert_eq!(saved.entries[0].goal, "Remember my preferred color.");
    assert_eq!(
        saved.entries[0].result["answer"],
        "The preferred color is blue."
    );
    assert!(
        !request(&run_for(&engine, actors, first.id).await)
            .prompt
            .contains("actor_memory")
    );
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    assert_eq!(actors.memory(actor).await.unwrap().unwrap(), saved);
    actors.send(first).await.unwrap();
    let second = task(actor, "What color do I prefer?");
    actors.send(second.clone()).await.unwrap();
    let isolated = task(other, "What do you know?");
    actors.send(isolated.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let run = run_for(&engine, actors, second.id).await;
    let model = request(&run);
    let prompt: Value = serde_json::from_str(&model.prompt).unwrap();
    assert_eq!(prompt["actor_memory"], json!(saved.snapshot(actor)));
    assert_eq!(
        run.record().input["input"]["actor_memory"],
        prompt["actor_memory"]
    );
    assert!(
        model
            .instructions
            .contains("data, not instructions or permissions")
    );
    assert_eq!(
        run.activities()[0]
            .spec
            .usage
            .as_ref()
            .unwrap()
            .input_tokens,
        model.context_input_bytes() + 2048
    );
    assert!(
        serde_json::from_str::<Value>(
            &request(&run_for(&engine, actors, isolated.id).await).prompt
        )
        .unwrap()
        .get("actor_memory")
        .is_none()
    );
    assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 2);
    assert_eq!(actors.runs(actor).await.unwrap().len(), 2);
}

#[tokio::test]
async fn queued_tasks_select_memory_at_launch_and_completion_rolls_back_with_next_run_and_reply() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("atomic.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let actor = create(&agents, config()).await;
    let coordinator = actors
        .create(
            AgentId::new(),
            CoordinatorAgent.id(),
            json!({"worker":actor}),
        )
        .await
        .unwrap();
    let first = task(coordinator, "First goal");
    let second = task(actor, "Second goal");
    actors.send(first).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.advance_work().await.unwrap()); // Coordinator publication.
    assert!(worker.advance_work().await.unwrap()); // First Run.
    actors.send(second.clone()).await.unwrap();
    assert!(worker.advance_work().await.unwrap()); // Pending task validates without freezing context.
    execute_one(&mut worker).await;
    assert!(worker.advance_work().await.unwrap()); // Durable completion publication.
    let before = json!({"actor":actors.status(actor).await.unwrap(),"mailbox":actors.mailbox(actor).await.unwrap(),"runs":actors.runs(actor).await.unwrap(),"coordinator":actors.mailbox(coordinator).await.unwrap()});
    for trigger in [
        "CREATE TRIGGER reject_memory BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'memory rollback'); END",
        "CREATE TRIGGER reject_memory BEFORE INSERT ON agent_runs BEGIN SELECT RAISE(ABORT,'next Run rollback'); END",
        "CREATE TRIGGER reject_memory BEFORE INSERT ON agent_mailbox WHEN json_extract(NEW.payload,'$.message.kind')='task_result' BEGIN SELECT RAISE(ABORT,'reply rollback'); END",
    ] {
        sql(&db, trigger).await;
        assert!(worker.advance_work().await.is_err());
        assert_eq!(
            before,
            json!({"actor":actors.status(actor).await.unwrap(),"mailbox":actors.mailbox(actor).await.unwrap(),"runs":actors.runs(actor).await.unwrap(),"coordinator":actors.mailbox(coordinator).await.unwrap()})
        );
        sql(&db, "DROP TRIGGER reject_memory").await;
    }
    assert!(worker.advance_work().await.unwrap());
    let run = run_for(&engine, actors, second.id).await;
    assert_eq!(
        run.record().input["input"]["actor_memory"]["entries"][0]["goal"],
        "First goal"
    );
    assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 1);
    drain(&mut worker).await;
    assert_eq!(actors.memory(actor).await.unwrap().unwrap().revision, 2);
    assert_eq!(actors.mailbox(coordinator).await.unwrap().len(), 2);
}

#[tokio::test]
async fn clear_preserves_saved_wait_and_prompt_and_fences_old_completions_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("clear.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let mut cfg = config();
    cfg["script"] = json!([{"kind":"ask_human","question":"Confirm?"},{"kind":"finish","summary":"Confirmed blue."}]);
    let actor = create(&agents, cfg).await;
    let first = task(actor, "Store a preference");
    actors.send(first.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let run = run_for(&engine, actors, first.id).await;
    engine
        .signal(
            run.record().id,
            run.record().wait.as_ref().unwrap().id,
            json!("yes"),
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    let second = task(actor, "Use the saved preference");
    let third = task(actor, "Start fresh");
    actors.send(second.clone()).await.unwrap();
    actors.send(third.clone()).await.unwrap();
    drain(&mut worker).await;
    let saved = run_for(&engine, actors, second.id).await;
    assert_eq!(saved.record().status, RunStatus::Waiting);
    assert_eq!(
        saved.record().input["input"]["actor_memory"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    actors.pause(actor).await.unwrap();
    let actor_before = json!(actors.status(actor).await.unwrap());
    sql(&db,"CREATE TRIGGER reject_clear BEFORE UPDATE ON agent_actors BEGIN SELECT RAISE(ABORT,'clear rollback'); END").await;
    assert!(actors.clear_memory(actor).await.is_err());
    assert_eq!(actor_before, json!(actors.status(actor).await.unwrap()));
    sql(&db, "DROP TRIGGER reject_clear").await;
    actors.clear_memory(actor).await.unwrap();
    assert_eq!(
        actors.status(actor).await.unwrap().record().status,
        AgentStatus::Paused
    );
    assert!(
        actors
            .memory(actor)
            .await
            .unwrap()
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(
        json!(saved),
        json!(run_for(&engine, actors, second.id).await)
    );
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    assert_eq!(
        json!(saved),
        json!(run_for(&engine, actors, second.id).await)
    );
    actors.resume(actor).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    engine
        .signal(
            saved.record().id,
            saved.record().wait.as_ref().unwrap().id,
            json!("yes"),
        )
        .await
        .unwrap();
    let resumed = run_for(&engine, actors, second.id).await;
    assert_eq!(
        serde_json::from_str::<Value>(&request(&resumed).prompt).unwrap()["actor_memory"],
        saved.record().input["input"]["actor_memory"]
    );
    drain(&mut worker).await;
    let fresh = run_for(&engine, actors, third.id).await;
    assert_eq!(
        fresh.record().input["input"]["actor_memory"]["generation"],
        1
    );
    assert!(
        fresh.record().input["input"]["actor_memory"]["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        actors
            .memory(actor)
            .await
            .unwrap()
            .unwrap()
            .entries
            .is_empty()
    );
    engine
        .signal(
            fresh.record().id,
            fresh.record().wait.as_ref().unwrap().id,
            json!("yes"),
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    let memory = actors.memory(actor).await.unwrap().unwrap();
    assert_eq!(memory.entries.len(), 1);
    assert_eq!(memory.entries[0].task_id, third.id);
}

#[tokio::test]
async fn failed_cancelled_and_exhausted_tasks_never_enter_memory() {
    for terminal in ["failed", "cancelled", "budget_exhausted"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("terminal.db")).await.unwrap();
        let mut cfg = config();
        if terminal == "budget_exhausted" {
            cfg["max_tokens"] = json!(1);
        }
        let actor = create(&agents, cfg).await;
        let actors = agents.actors().unwrap();
        let message = task(actor, "Do a task");
        actors.send(message.clone()).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        worker.advance_work().await.unwrap();
        if terminal == "cancelled" {
            actors.cancel_task(message.id).await.unwrap();
        }
        if terminal == "failed" {
            let execution = worker.admit().await.unwrap().unwrap();
            worker
                .complete(
                    execution,
                    Outcome::Failure(Failure::new("fixture", "permanent fixture failure", false)),
                )
                .await
                .unwrap();
        }
        drain(&mut worker).await;
        assert_eq!(
            json!(run_for(&engine, actors, message.id).await.record().status),
            terminal
        );
        let memory = actors.memory(actor).await.unwrap().unwrap();
        assert!(memory.entries.is_empty());
        assert_eq!(memory.revision, 0);
    }
}

#[test]
fn retention_bounds_complete_unicode_entries_and_skips_oversized_results() {
    let mut memory = ActorMemory::new(MemoryPolicy {
        max_entries: 2,
        max_bytes: 600,
    })
    .unwrap();
    let entry = |goal: &str, text: &str| MemoryEntry {
        task_id: MessageId::new(),
        goal: goal.into(),
        result: json!({"answer":text}),
    };
    let first = entry("First", "🙂".repeat(35).as_str());
    memory.remember(first.clone()).unwrap();
    let second = entry("Second", "Small");
    memory.remember(second.clone()).unwrap();
    let third = entry("Third", "Small");
    memory.remember(third.clone()).unwrap();
    assert_eq!(memory.entries, vec![second, third.clone()]);
    assert_eq!(memory.omitted_entries, 1);
    let saved = memory.entries.clone();
    memory.remember(entry("Huge", &"🙂".repeat(1000))).unwrap();
    assert_eq!(memory.entries, saved);
    assert_eq!(memory.omitted_entries, 2);
    let revision = memory.revision;
    memory.remember(third.clone()).unwrap();
    assert_eq!(memory.revision, revision);
    let mut conflicting = third;
    conflicting.result = json!("changed result");
    assert!(matches!(
        memory.remember(conflicting),
        Err(DomainError::ConflictingMessage)
    ));
    assert_eq!(memory.revision, revision);
    assert!(memory.retained_bytes() <= 600);
    let mut bytes = ActorMemory::new(MemoryPolicy {
        max_entries: 64,
        max_bytes: 400,
    })
    .unwrap();
    bytes.remember(first).unwrap();
    bytes.remember(entry("Later", &"🙂".repeat(35))).unwrap();
    assert_eq!(bytes.entries.len(), 1);
    assert_eq!(bytes.omitted_entries, 1);
    assert!(
        MemoryPolicy {
            max_entries: 0,
            max_bytes: 8192
        }
        .validate()
        .is_err()
    );
    assert!(
        MemoryPolicy {
            max_entries: 8,
            max_bytes: 127
        }
        .validate()
        .is_err()
    );
}

#[test]
fn each_native_workflow_pins_a_prompt_fitting_suffix_and_charges_its_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let adapters: Vec<Arc<dyn AgentAdapter>> = vec![
        Arc::new(AnswerAgentAdapter),
        Arc::new(PlannerAgentAdapter::default()),
        Arc::new(CodingAgentAdapter::new(Arc::new(LocalWorkspaces::new(
            directory.path().into(),
        )))),
    ];
    for adapter in adapters {
        for protocol in ["responses", "chat_completions"] {
            let registration = adapter.registration();
            let mut cfg = config();
            cfg.as_object_mut().unwrap().remove("memory");
            cfg["target"]["protocol"] = json!(protocol);
            if registration.adapter.name == "planner" {
                cfg["workers"] =
                    json!([{"name":"worker","agent_id":AgentId::new(),"description":"Assistant"}]);
                cfg["delegation"] = json!({"max_tasks":2,"max_depth":2});
            }
            if registration.adapter.name == "coding" {
                cfg = serde_json::from_str(include_str!("../examples/configs/agent-scripted.json"))
                    .unwrap();
                cfg["workspace"] = json!("managed");
                cfg["target"]["protocol"] = json!(protocol);
            }
            let workflow =
                RegisteredAgentWorkflow::new(registration.clone(), adapter.definition()).unwrap();
            let actor = AgentId::new();
            let mut memory = ActorMemory::new(MemoryPolicy::default()).unwrap();
            for goal in ["Old goal", "Recent goal"] {
                memory
                    .remember(MemoryEntry {
                        task_id: MessageId::new(),
                        goal: goal.into(),
                        result: json!({"answer":"🙂".repeat(120)}),
                    })
                    .unwrap();
            }
            let mut selected = memory.snapshot(actor);
            selected.omit_oldest().unwrap();
            let invocation = |cfg: Value| {
                json!(AgentInvocation {
                    registration: registration.clone(),
                    input: cfg
                })
            };
            cfg["actor_memory"] = json!(selected);
            let fit = Run::start(&workflow, invocation(cfg.clone()), 20, 0).unwrap();
            let exact = request(&fit).context_input_bytes() as u32;
            if registration.adapter.name == "coding" {
                cfg["context"]["max_input_bytes"] = json!(exact);
            } else {
                cfg["max_input_bytes"] = json!(exact);
            }
            cfg["actor_memory"] = json!(memory.snapshot(actor));
            let run = Run::start(&workflow, invocation(cfg.clone()), 20, 0).unwrap();
            assert_eq!(run.record().input["input"]["actor_memory"], json!(selected));
            let model = request(&run);
            assert_eq!(model.context_input_bytes(), u64::from(exact));
            assert_eq!(
                run.activities()[0]
                    .spec
                    .usage
                    .as_ref()
                    .unwrap()
                    .input_tokens,
                u64::from(exact) + 2048
            );
            // All memory can be omitted; an oversized essential goal still fails.
            cfg.as_object_mut().unwrap().remove("actor_memory");
            let base = Run::start(&workflow, invocation(cfg.clone()), 20, 0).unwrap();
            let base_bytes = request(&base).context_input_bytes() as u32;
            if registration.adapter.name == "coding" {
                cfg["context"]["max_input_bytes"] = json!(base_bytes);
            } else {
                cfg["max_input_bytes"] = json!(base_bytes);
            }
            cfg["actor_memory"] = json!(memory.snapshot(actor));
            let empty = Run::start(&workflow, invocation(cfg.clone()), 20, 0).unwrap();
            assert!(
                empty.record().input["input"]["actor_memory"]["entries"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(request(&empty).prompt, request(&base).prompt);
            cfg["goal"] = json!("x".repeat(10000));
            assert!(matches!(
                Run::start(&workflow, invocation(cfg), 20, 0),
                Err(DomainError::ContextLimit { .. })
            ));
            assert_eq!(memory.entries.len(), 2);
        }
    }
}

#[tokio::test]
async fn runtime_snapshots_and_invalid_retention_are_rejected_before_workspace_preparation() {
    let dir = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(dir.path().join("validation.db")).await.unwrap();
    for supplied in [
        json!({"actor_memory":{}}),
        json!({"memory":{"max_entries":0}}),
        json!({"memory":null}),
    ] {
        let result = agents
            .create_registered_actor(
                DefinitionId::new("coding", 1),
                None,
                supplied,
                20,
                AgentQueuePolicy::default(),
            )
            .await;
        assert!(result.is_err());
        assert!(!result.unwrap_err().to_string().contains("source workspace"));
    }
}

#[tokio::test]
async fn durable_retry_reuses_pinned_memory_even_after_clear_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("retry.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actor = create(&agents, config()).await;
    let actors = agents.actors().unwrap();
    actors
        .send(task(actor, "Remember a preference"))
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let second = task(actor, "Recall it");
    actors.send(second.clone()).await.unwrap();
    worker.advance_work().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let saved_input = execution.input.clone();
    worker
        .complete(
            execution,
            Outcome::Failure(Failure::new("fixture", "retryable provider failure", true)),
        )
        .await
        .unwrap();
    let saved = run_for(&engine, actors, second.id).await;
    assert_eq!(saved.activities()[0].status, ActivityStatus::Queued);
    assert!(saved.activities()[0].ready_at > saved.record().updated_at);
    actors.clear_memory(actor).await.unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert_eq!(
        run_for(&engine, actors, second.id).await.record().input,
        saved.record().input
    );
    tokio::time::sleep(Duration::from_millis(550)).await;
    let retry = worker.admit().await.unwrap().unwrap();
    assert_eq!(retry.input, saved_input);
    let result = worker.execute(retry.clone()).await.unwrap();
    worker.complete(retry, result).await.unwrap();
    drain(&mut worker).await;
    assert_eq!(
        run_for(&engine, actors, second.id)
            .await
            .record()
            .usage
            .admitted_model_calls,
        2
    );
    assert!(
        actors
            .memory(actor)
            .await
            .unwrap()
            .unwrap()
            .entries
            .is_empty()
    );
}

#[tokio::test]
async fn native_coding_and_planner_actors_reuse_their_own_completed_task_history() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("native.db")).await.unwrap();
    let assistant = create(&agents, config()).await;
    let source = dir.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("README.md"), "memory fixture").unwrap();
    for name in ["coding", "planner"] {
        let mut cfg = config();
        let origin = if name == "coding" {
            cfg = serde_json::from_str(include_str!("../examples/configs/agent-scripted.json"))
                .unwrap();
            cfg["memory"] = json!({});
            cfg["test_command"] = json!(["rustc", "--version"]);
            cfg["require_approval"] = json!(false);
            cfg["script"] = json!([{"kind":"run_tests"},{"kind":"finish","summary":"Checked the fixed tool command."}]);
            Some(source.clone())
        } else {
            cfg["workers"] =
                json!([{"name":"assistant","agent_id":assistant,"description":"Answering worker"}]);
            cfg["delegation"] = json!({"max_tasks":2,"max_depth":2});
            None
        };
        let actor = agents
            .create_registered_actor(
                agents.latest_adapter(name).unwrap(),
                origin,
                cfg,
                20,
                AgentQueuePolicy::default(),
            )
            .await
            .unwrap();
        let first = task(actor, "First native task");
        let second = task(actor, "Second native task");
        let actors = agents.actors().unwrap();
        actors.send(first.clone()).await.unwrap();
        actors.send(second.clone()).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let saved = actors.memory(actor).await.unwrap().unwrap();
        assert_eq!(saved.entries.len(), 2, "{name}");
        assert_eq!(saved.entries[0].task_id, first.id);
        assert_eq!(saved.entries[1].task_id, second.id);
        let next = run_for(&engine, actors, second.id).await;
        assert_eq!(
            next.record().input["input"]["actor_memory"]["entries"][0]["goal"],
            "First native task"
        );
    }
    assert!(agents.actors().unwrap().create(AgentId::new(),WorkflowWorkerAgent.id(),json!({
        "workflow":{"name":"demo","version":1},"input":{"message":"Generic workflow"},"attempt_limit":10,"memory":{}
    })).await.is_err());
}

#[tokio::test]
async fn process_kill_recovers_memory_once_at_each_completion_boundary() {
    for phase in ["completed", "notified", "remembered", "inflight"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("kill.db");
        let ready = dir.path().join("ready.json");
        let log = std::fs::File::create(dir.path().join("child.log")).unwrap();
        let mut child = ProcessCommand::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "memory_kill_fixture", "--nocapture"])
            .env("ACT0R_MEMORY_DB", &db)
            .env("ACT0R_MEMORY_READY", &ready)
            .env("ACT0R_MEMORY_PHASE", phase)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(45);
        while !ready.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!(
                    "fixture exited {status}: {}",
                    std::fs::read_to_string(dir.path().join("child.log")).unwrap()
                );
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("memory kill fixture timed out");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let ids: Value = serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
        let actor: AgentId = serde_json::from_value(ids["actor"].clone()).unwrap();
        let second: MessageId = serde_json::from_value(ids["second"].clone()).unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let actors = agents.actors().unwrap();
        if phase == "remembered" || phase == "inflight" {
            assert_eq!(
                run_for(&engine, actors, second).await.record().input,
                ids["second_input"]
            );
        }
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let memory = actors.memory(actor).await.unwrap().unwrap();
        assert_eq!(memory.revision, 2);
        assert_eq!(memory.entries.len(), 2);
        assert_eq!(memory.entries[0].goal, "First goal");
        assert_eq!(memory.entries[1].goal, "Second goal");
        let next = run_for(&engine, actors, second).await;
        assert_eq!(
            next.record().input["input"]["actor_memory"]["entries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            next.record().usage.admitted_model_calls,
            if phase == "inflight" { 2 } else { 1 }
        );
        assert_eq!(actors.runs(actor).await.unwrap().len(), 2);
        assert_eq!(
            actors
                .mailbox(actor)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.message.kind == RUN_COMPLETED)
                .count(),
            2
        );
        assert!(!worker.step().await.unwrap());
        assert_eq!(actors.memory(actor).await.unwrap().unwrap(), memory);
    }
}

#[tokio::test]
#[ignore = "subprocess fixture controlled by the recovery test"]
async fn memory_kill_fixture() {
    let db = std::env::var("ACT0R_MEMORY_DB").unwrap();
    let ready = std::env::var("ACT0R_MEMORY_READY").unwrap();
    let phase = std::env::var("ACT0R_MEMORY_PHASE").unwrap();
    let (engine, agents) = local_app(&db).await.unwrap();
    let actors = agents.actors().unwrap();
    let actor = create(&agents, config()).await;
    let first = task(actor, "First goal");
    let second = task(actor, "Second goal");
    actors.send(first).await.unwrap();
    actors.send(second.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    worker.advance_work().await.unwrap();
    execute_one(&mut worker).await;
    if phase != "completed" {
        worker.advance_work().await.unwrap();
    }
    if phase == "remembered" || phase == "inflight" {
        worker.advance_work().await.unwrap();
    }
    if phase == "inflight" {
        worker.admit().await.unwrap().unwrap();
    }
    let second_input = if phase == "remembered" || phase == "inflight" {
        run_for(&engine, actors, second.id)
            .await
            .record()
            .input
            .clone()
    } else {
        Value::Null
    };
    std::fs::write(
        ready,
        serde_json::to_vec(&json!({"actor":actor,"second":second.id,"second_input":second_input}))
            .unwrap(),
    )
    .unwrap();
    std::future::pending::<()>().await;
}
