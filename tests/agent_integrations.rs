use act0r::{
    Engine, Worker,
    adapters::{
        agent_executor::AgentExecutor, agents::AnswerAgentAdapter, demo::DemoExecutor,
        file_lock::FileOwnership, model::ModelRouter, sqlite::SqliteStore,
        system_clock::SystemClock, workspace::LocalTools,
    },
    application::{ActorService, AgentRegistry, AgentService, ports::*},
    bootstrap::local_app,
    domain::{actors::*, integration::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

fn config(kind: &str, calls: u32) -> Value {
    if kind == "answer" {
        let mut value: Value =
            serde_json::from_str(include_str!("../examples/configs/answer-scripted.json")).unwrap();
        value["max_model_calls"] = json!(calls);
        value
    } else {
        let task: Value =
            serde_json::from_str(include_str!("../examples/configs/task-scripted.json")).unwrap();
        let mut value = task["agent"].clone();
        value["max_model_calls"] = json!(calls);
        value["script"].as_array_mut().unwrap().insert(
            0,
            json!({"kind":"ask_human","question":"Please confirm the coding goal."}),
        );
        value
    }
}
fn source(kind: &str) -> Option<PathBuf> {
    (kind == "coding").then(|| {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/benchmark-cases/addition/source")
    })
}
fn request(recipient: AgentId) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":"The submitted goal overrides the actor's initial goal."}),
    }
}
async fn create(agents: &AgentService, kind: &str, calls: u32) -> AgentId {
    agents
        .create_registered_actor(
            DefinitionId::new(kind, 1),
            source(kind),
            config(kind, calls),
            40,
            AgentQueuePolicy { task_capacity: 2 },
        )
        .await
        .unwrap()
}
async fn turns(worker: &mut Worker) {
    for _ in 0..50 {
        if !worker.advance_work().await.unwrap() {
            return;
        }
    }
    panic!("mailboxes did not quiesce");
}
async fn drain(worker: &mut Worker) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline);
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

#[tokio::test]
async fn both_integrations_preserve_identity_waits_checkpoint_results_and_usage_across_restart() {
    for kind in ["coding", "answer"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("conformance.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let id = create(&agents, kind, 10).await;
        let actors = agents.actors().unwrap();
        let message = request(id);
        actors.send(message.clone()).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let waiting = actors.task(message.id).await.unwrap();
        assert_eq!(waiting.status, AgentTaskStatus::Waiting);
        assert_eq!(waiting.usage.as_ref().unwrap().admitted_model_calls, 1);
        let run_id = waiting.run_id.unwrap();
        let wait_id = waiting.wait.as_ref().unwrap().id;
        let run = engine.status(run_id).await.unwrap();
        assert_eq!(
            run.record().workflow,
            DefinitionId::new(format!("registered_agent.{kind}"), 1)
        );
        assert_eq!(run.record().input["input"]["goal"], message.payload["goal"]);
        assert_eq!(
            run.record().input["registration"],
            run.record().state["registration"]
        );
        let registration = agents
            .registrations()
            .unwrap()
            .into_iter()
            .find(|r| r.adapter.name == kind)
            .unwrap();
        assert_eq!(run.record().input["registration"], json!(registration));
        assert_eq!(actors.queue(id).await.unwrap().executing_tasks, 1);
        engine.pause(run_id).await.unwrap();
        assert_eq!(
            actors.task(message.id).await.unwrap().status,
            AgentTaskStatus::Paused
        );
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let actors = agents.actors().unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert_eq!(
            actors
                .task(message.id)
                .await
                .unwrap()
                .wait
                .as_ref()
                .unwrap()
                .id,
            wait_id
        );
        engine.resume(run_id).await.unwrap();
        engine
            .signal(run_id, wait_id, json!({"answer":"Yes, proceed."}))
            .await
            .unwrap();
        engine
            .signal(run_id, wait_id, json!({"answer":"Yes, proceed."}))
            .await
            .unwrap();
        assert!(
            engine
                .signal(run_id, wait_id, json!({"answer":"different"}))
                .await
                .is_err()
        );
        drain(&mut worker).await;
        let result = actors.task(message.id).await.unwrap();
        assert_eq!(result.status, AgentTaskStatus::Succeeded);
        assert_eq!(result.run_id, Some(run_id));
        assert_eq!(actors.queue(id).await.unwrap().available, 2);
        assert_eq!(actors.runs(id).await.unwrap().len(), 1);
        let run = engine.status(run_id).await.unwrap();
        assert_eq!(run.record().input_receipts.len(), 1);
        assert_eq!(
            run.attempts()
                .iter()
                .filter(|a| a.activity_id == run.activities()[0].id)
                .count(),
            1
        );
        assert_eq!(run.record().usage.uncertain_calls, 0);
        if kind == "coding" {
            assert_eq!(run.record().result.as_ref().unwrap()["tests_passed"], true);
        } else {
            assert!(
                run.record().result.as_ref().unwrap()["answer"]
                    .as_str()
                    .unwrap()
                    .contains("durable")
            );
            assert!(
                run.activities()
                    .iter()
                    .all(|a| a.spec.handler == DefinitionId::new("agent.model", 1))
            );
        }
        let snapshot = json!(result);
        actors.send(message).await.unwrap();
        assert!(!worker.step().await.unwrap());
        assert_eq!(json!(actors.task(result.task.id).await.unwrap()), snapshot);
    }
}

#[tokio::test]
async fn both_integrations_apply_budget_and_cancellation_without_resetting_charges() {
    for kind in ["coding", "answer"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("controls.db")).await.unwrap();
        let id = create(&agents, kind, 1).await;
        let actors = agents.actors().unwrap();
        let message = request(id);
        actors.send(message.clone()).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let waiting = actors.task(message.id).await.unwrap();
        engine
            .signal(
                waiting.run_id.unwrap(),
                waiting.wait.as_ref().unwrap().id,
                json!({"answer":"Proceed"}),
            )
            .await
            .unwrap();
        drain(&mut worker).await;
        let exhausted = actors.task(message.id).await.unwrap();
        assert_eq!(exhausted.status, AgentTaskStatus::BudgetExhausted);
        assert_eq!(exhausted.usage.as_ref().unwrap().admitted_model_calls, 1);
        assert_eq!(exhausted.admitted_attempts, 1);

        let id = create(&agents, kind, 10).await;
        let active = request(id);
        let queued = request(id);
        actors.send(active.clone()).await.unwrap();
        actors.send(queued.clone()).await.unwrap();
        actors.cancel_task(queued.id).await.unwrap();
        turns(&mut worker).await;
        let execution = worker.admit().await.unwrap().unwrap();
        assert_eq!(
            actors.cancel_task(active.id).await.unwrap().status,
            AgentTaskStatus::Cancelling
        );
        assert_eq!(actors.queue(id).await.unwrap().outstanding, 1);
        let outcome = worker.execute(execution.clone()).await.unwrap();
        worker.complete(execution, outcome).await.unwrap();
        drain(&mut worker).await;
        let cancelled = actors.task(active.id).await.unwrap();
        assert_eq!(cancelled.status, AgentTaskStatus::Cancelled);
        assert_eq!(cancelled.usage.as_ref().unwrap().admitted_model_calls, 1);
        assert_eq!(cancelled.usage.as_ref().unwrap().charged_tokens(), 20);
        let queued = actors.task(queued.id).await.unwrap();
        assert!(queued.run_id.is_none());
        assert_eq!(queued.admitted_attempts, 0);
        assert_eq!(actors.queue(id).await.unwrap().available, 2);
    }
}

struct ChangedAnswer {
    contract: u32,
    wrong_definition: bool,
}
impl AgentAdapter for ChangedAnswer {
    fn registration(&self) -> AgentRegistration {
        let mut r = AnswerAgentAdapter.registration();
        r.contract_version = self.contract;
        r.capabilities.human_input_waits = false;
        if self.wrong_definition {
            r.definition = DefinitionId::new("wrong", 1);
        }
        r
    }
    fn definition(&self) -> Arc<dyn WorkflowDefinition> {
        AnswerAgentAdapter.definition()
    }
    fn prepare(&self, config: Value, source: Option<PathBuf>) -> PortFuture<'_, Value> {
        AnswerAgentAdapter.prepare(config, source)
    }
}
async fn custom_app(db: &Path, adapters: Vec<Arc<dyn AgentAdapter>>) -> (Engine, ActorService) {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let workspaces = db.parent().unwrap().join(".act0r/workspaces");
    let registry = Arc::new(AgentRegistry::new(adapters).unwrap());
    let engine = Engine::new(
        store.clone(),
        Arc::new(AgentExecutor::new(
            Arc::new(ModelRouter::new(None).unwrap()),
            Arc::new(LocalTools::new(workspaces)),
            Arc::new(DemoExecutor),
        )),
        Arc::new(SystemClock),
        Arc::new(FileOwnership::new(db)),
        registry.workflows(),
    )
    .unwrap();
    let actors = ActorService::new(
        engine.clone(),
        store,
        vec![Arc::new(RegisteredAgentDefinition)],
    )
    .unwrap()
    .with_agent_registry(registry);
    (engine.with_coordinator(Arc::new(actors.clone())), actors)
}

#[tokio::test]
async fn missing_or_changed_integrations_block_before_execution_and_can_resume_when_restored() {
    for missing in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("versions.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let id = create(&agents, "answer", 4).await;
        let actors = agents.actors().unwrap();
        let a = request(id);
        let b = request(id);
        actors.send(a.clone()).await.unwrap();
        actors.send(b.clone()).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert!(worker.advance_work().await.unwrap());
        let run_id = actors.task(a.id).await.unwrap().run_id.unwrap();
        let state = actors.status(id).await.unwrap().record().state.clone();
        drop(worker);
        drop(agents);
        drop(engine);
        let adapters: Vec<Arc<dyn AgentAdapter>> = if missing {
            vec![]
        } else {
            vec![Arc::new(ChangedAnswer {
                contract: 1,
                wrong_definition: false,
            })]
        };
        let (engine, actors) = custom_app(&db, adapters).await;
        let mut worker = engine.worker().await.unwrap();
        assert_eq!(
            engine.status(run_id).await.unwrap().record().status,
            RunStatus::Blocked
        );
        assert_eq!(
            engine
                .status(run_id)
                .await
                .unwrap()
                .record()
                .usage
                .admitted_model_calls,
            0
        );
        assert!(engine.resume(run_id).await.is_err());
        assert!(worker.advance_work().await.unwrap());
        assert_eq!(
            actors.status(id).await.unwrap().record().status,
            AgentStatus::Blocked
        );
        assert_eq!(actors.status(id).await.unwrap().record().state, state);
        assert_eq!(
            actors.mailbox(id).await.unwrap()[1].status,
            MessageStatus::Queued
        );
        assert!(!worker.has_active_runs().await.unwrap());
        drop(worker);
        drop(actors);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let actors = agents.actors().unwrap();
        actors.cancel_task(b.id).await.unwrap();
        actors.resume(id).await.unwrap();
        engine.resume(run_id).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let waiting = actors.task(a.id).await.unwrap();
        engine
            .signal(run_id, waiting.wait.unwrap().id, json!({"answer":"short"}))
            .await
            .unwrap();
        drain(&mut worker).await;
        assert_eq!(
            actors.task(a.id).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        assert_eq!(actors.runs(id).await.unwrap().len(), 1);
        assert_eq!(
            engine
                .history(run_id)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.kind == "implementation_restored")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn registry_rejects_duplicate_or_invalid_contracts_and_invalid_agent_inputs() {
    assert!(
        AgentRegistry::new(vec![
            Arc::new(AnswerAgentAdapter),
            Arc::new(AnswerAgentAdapter)
        ])
        .is_err()
    );
    assert!(
        AgentRegistry::new(vec![Arc::new(ChangedAnswer {
            contract: 2,
            wrong_definition: false
        })])
        .is_err()
    );
    assert!(
        AgentRegistry::new(vec![Arc::new(ChangedAnswer {
            contract: 1,
            wrong_definition: true
        })])
        .is_err()
    );
    let dir = tempfile::tempdir().unwrap();
    let (_engine, agents) = local_app(dir.path().join("invalid.db")).await.unwrap();
    assert!(
        agents
            .create_registered_actor(
                DefinitionId::new("answer", 2),
                None,
                config("answer", 3),
                30,
                AgentQueuePolicy::default()
            )
            .await
            .is_err()
    );
    assert!(
        agents
            .create_registered_actor(
                DefinitionId::new("answer", 1),
                Some(dir.path().into()),
                config("answer", 3),
                30,
                AgentQueuePolicy::default()
            )
            .await
            .is_err()
    );
    let mut invalid = config("answer", 3);
    invalid["script"] = json!([{"kind":"read_file","path":"secret"}]);
    assert!(
        agents
            .create_registered_actor(
                DefinitionId::new("answer", 1),
                None,
                invalid,
                30,
                AgentQueuePolicy::default()
            )
            .await
            .is_err()
    );
    let mut invalid = config("answer", 3);
    invalid["max_input_bytes"] = json!(1);
    let id = agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            invalid,
            30,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let actors = agents.actors().unwrap();
    let message = request(id);
    actors.send(message.clone()).await.unwrap();
    assert!(actors.advance().await.unwrap());
    assert_eq!(
        actors.task(message.id).await.unwrap().status,
        AgentTaskStatus::Rejected
    );
    assert!(actors.runs(id).await.unwrap().is_empty());
}

#[tokio::test]
async fn undeclared_human_waits_fail_without_creating_a_wait() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("capabilities.db");
    let adapter = ChangedAnswer {
        contract: 1,
        wrong_definition: false,
    };
    let registration = adapter.registration();
    let (engine, actors) = custom_app(&db, vec![Arc::new(adapter)]).await;
    let id = actors
        .create(
            AgentId::new(),
            RegisteredAgentDefinition.id(),
            json!(RegisteredAgentConfig {
                registration,
                input: config("answer", 4),
                attempt_limit: 10,
                memory: None
            }),
        )
        .await
        .unwrap();
    let message = request(id);
    actors.send(message.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let task = actors.task(message.id).await.unwrap();
    assert_eq!(task.status, AgentTaskStatus::Failed);
    assert!(task.wait.is_none());
    assert_eq!(task.usage.unwrap().admitted_model_calls, 1);
    assert!(
        engine
            .status(task.run_id.unwrap())
            .await
            .unwrap()
            .record()
            .error
            .as_ref()
            .unwrap()
            .message
            .contains("without declaring")
    );
}

#[tokio::test]
async fn restoring_an_adapter_cannot_resume_an_uncertain_external_effect() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("uncertain.db")).await.unwrap();
    let mut config = config("coding", 10);
    config["repeat_safe_tests"] = json!(false);
    config["script"] = json!([{"kind":"run_tests"}]);
    let id = agents
        .create_registered_actor(
            DefinitionId::new("coding", 1),
            source("coding"),
            config,
            30,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let actors = agents.actors().unwrap();
    let message = request(id);
    actors.send(message.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    turns(&mut worker).await;
    assert!(worker.step().await.unwrap()); // The saved model decision schedules the unsafe test.
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.handler, DefinitionId::new("agent.tool", 1));
    drop(worker);
    let _worker = engine.worker().await.unwrap();
    let run = engine.status(execution.run_id).await.unwrap();
    assert_eq!(
        run.record().error.as_ref().unwrap().code,
        "uncertain_outcome"
    );
    let before = json!(run);
    assert!(engine.resume(execution.run_id).await.is_err());
    assert_eq!(
        json!(engine.status(execution.run_id).await.unwrap()),
        before
    );
    assert_eq!(
        actors.task(message.id).await.unwrap().status,
        AgentTaskStatus::Blocked
    );
}

#[tokio::test]
async fn answer_context_limits_preserve_completed_decisions_and_refuse_tool_actions() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("answer.db")).await.unwrap();
    let id = create(&agents, "answer", 4).await;
    let actors = agents.actors().unwrap();
    let message = request(id);
    actors.send(message.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let waiting = actors.task(message.id).await.unwrap();
    let run_id = waiting.run_id.unwrap();
    assert!(
        engine
            .signal(
                run_id,
                waiting.wait.as_ref().unwrap().id,
                json!({"answer":"x".repeat(70_000)})
            )
            .await
            .is_err()
    );
    assert_eq!(
        actors.task(message.id).await.unwrap().status,
        AgentTaskStatus::Waiting
    );
    assert_eq!(
        actors
            .task(message.id)
            .await
            .unwrap()
            .usage
            .unwrap()
            .admitted_model_calls,
        1
    );
    engine
        .signal(run_id, waiting.wait.unwrap().id, json!({"answer":"short"}))
        .await
        .unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    worker
        .complete(
            execution,
            Outcome::SuccessWithUsage {
                result: json!({"action":{"kind":"read_file","path":"secret"},"request_id":null}),
                usage: TokenUsage {
                    input_tokens: 10,
                    output_tokens: 10,
                },
            },
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    assert_eq!(
        actors.task(message.id).await.unwrap().status,
        AgentTaskStatus::Failed
    );
    assert_eq!(engine.status(run_id).await.unwrap().activities().len(), 2);
    assert_eq!(
        actors
            .task(message.id)
            .await
            .unwrap()
            .usage
            .unwrap()
            .admitted_model_calls,
        2
    );
}

#[test]
fn cli_discovers_capabilities_and_creates_both_integrations() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cli.db");
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_act0r"))
            .arg("--db")
            .arg(&db)
            .arg("--json")
            .args(args)
            .output()
            .unwrap()
    };
    let success = |args: &[&str]| {
        let output = run(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let registrations = success(&["actor", "agent", "list"]);
    assert_eq!(registrations[0]["adapter"]["name"], "answer");
    assert_eq!(
        registrations[0]["capabilities"]["checkpoints"],
        "model_decision"
    );
    assert_eq!(
        registrations[1]["capabilities"]["checkpoints"],
        "model_and_tool"
    );
    assert_eq!(registrations[1]["capabilities"]["branching"], false);
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let config = examples.join("configs/answer-scripted.json");
    let id = success(&[
        "actor",
        "agent",
        "create",
        "answer",
        "--config-file",
        config.to_str().unwrap(),
        "--task-capacity",
        "1",
    ])["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        !run(&[
            "actor",
            "agent",
            "create",
            "answer",
            "--adapter-version",
            "2",
            "--config-file",
            config.to_str().unwrap()
        ])
        .status
        .success()
    );
    let input = dir.path().join("question.json");
    fs::write(&input, br#"{"goal":"How do agents resume?"}"#).unwrap();
    let message = success(&[
        "actor",
        "send",
        &id,
        "--input-file",
        input.to_str().unwrap(),
    ])["message_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(run(&["worker", "run", "--until-idle"]).status.success());
    let waiting = success(&["actor", "task", "status", &message]);
    let run_id = waiting["run_id"].as_str().unwrap();
    let wait_id = waiting["wait"]["id"].as_str().unwrap();
    let response = dir.path().join("response.json");
    fs::write(&response, br#"{"answer":"yes"}"#).unwrap();
    assert!(
        run(&[
            "run",
            "signal",
            run_id,
            wait_id,
            "--input-file",
            response.to_str().unwrap()
        ])
        .status
        .success()
    );
    assert!(run(&["worker", "run", "--until-idle"]).status.success());
    assert_eq!(
        success(&["actor", "task", "status", &message])["status"],
        "succeeded"
    );
    let config = examples.join("configs/agent-scripted.json");
    let source = examples.join("benchmark-cases/addition/source");
    let coding = success(&[
        "actor",
        "agent",
        "create",
        "coding",
        "--source",
        source.to_str().unwrap(),
        "--config-file",
        config.to_str().unwrap(),
    ])["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        success(&["actor", "status", &coding])["agent"]["config"]["registration"]["adapter"]["name"],
        "coding"
    );
    let message = success(&[
        "actor",
        "send",
        &coding,
        "--input-file",
        input.to_str().unwrap(),
    ])["message_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(run(&["worker", "run", "--until-idle"]).status.success());
    let task = success(&["actor", "task", "status", &message]);
    let status = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&db)
        .args(["run", "status", task["run_id"].as_str().unwrap()])
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(String::from_utf8_lossy(&status.stdout).contains("Context:"));
}

#[tokio::test]
#[ignore = "subprocess registered-agent checkpoint fixture"]
async fn integration_crash_child() {
    let Ok(db) = std::env::var("INTEGRATION_DB") else {
        return;
    };
    let (engine, agents) = local_app(&db).await.unwrap();
    let id: MessageId = std::env::var("INTEGRATION_TASK").unwrap().parse().unwrap();
    let mut worker = engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    if std::env::var("INTEGRATION_BOUNDARY").unwrap() == "waiting" {
        let outcome = worker.execute(execution.clone()).await.unwrap();
        worker.complete(execution, outcome).await.unwrap();
    }
    fs::write(
        std::env::var("INTEGRATION_MARKER").unwrap(),
        serde_json::to_vec(&agents.actors().unwrap().task(id).await.unwrap()).unwrap(),
    )
    .unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn both_integrations_recover_after_process_kills_at_admission_and_saved_wait_boundaries() {
    for kind in ["coding", "answer"] {
        for boundary in ["admitted", "waiting"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("kill.db");
            let marker = dir.path().join("checkpoint.json");
            let (engine, agents) = local_app(&db).await.unwrap();
            let id = create(&agents, kind, 10).await;
            let message = request(id);
            agents
                .actors()
                .unwrap()
                .send(message.clone())
                .await
                .unwrap();
            let mut worker = engine.worker().await.unwrap();
            turns(&mut worker).await;
            let run_id = agents
                .actors()
                .unwrap()
                .task(message.id)
                .await
                .unwrap()
                .run_id
                .unwrap();
            drop(worker);
            drop(agents);
            drop(engine);
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--ignored",
                    "--exact",
                    "integration_crash_child",
                    "--nocapture",
                ])
                .env("INTEGRATION_DB", &db)
                .env("INTEGRATION_TASK", message.id.to_string())
                .env("INTEGRATION_BOUNDARY", boundary)
                .env("INTEGRATION_MARKER", &marker)
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
                    panic!("integration checkpoint timeout");
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            child.kill().unwrap();
            child.wait().unwrap();
            let saved: Value = serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
            let (engine, agents) = local_app(&db).await.unwrap();
            let actors = agents.actors().unwrap();
            let mut worker = engine.worker().await.unwrap();
            drain(&mut worker).await;
            let waiting = actors.task(message.id).await.unwrap();
            assert_eq!(waiting.run_id, Some(run_id));
            assert_eq!(waiting.status, AgentTaskStatus::Waiting);
            let usage = waiting.usage.as_ref().unwrap();
            assert_eq!(
                usage.admitted_model_calls,
                if boundary == "admitted" { 2 } else { 1 }
            );
            assert_eq!(
                usage.uncertain_calls,
                if boundary == "admitted" { 1 } else { 0 }
            );
            if boundary == "waiting" {
                assert_eq!(
                    json!(waiting.wait.as_ref().unwrap().id),
                    saved["wait"]["id"]
                );
            }
            engine
                .signal(
                    run_id,
                    waiting.wait.unwrap().id,
                    json!({"answer":"Proceed"}),
                )
                .await
                .unwrap();
            drain(&mut worker).await;
            assert_eq!(
                actors.task(message.id).await.unwrap().status,
                AgentTaskStatus::Succeeded
            );
            assert_eq!(actors.runs(id).await.unwrap().len(), 1);
            assert_eq!(
                engine
                    .status(run_id)
                    .await
                    .unwrap()
                    .attempts()
                    .iter()
                    .filter(|a| a.status == AttemptStatus::Interrupted)
                    .count(),
                usize::from(boundary == "admitted")
            );
        }
    }
}
