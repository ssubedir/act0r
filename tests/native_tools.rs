use act0r::{
    Engine, Error, Worker,
    adapters::{
        agent_executor::AgentExecutor,
        agents::{AnswerAgentAdapter, PlannerAgentAdapter, ToolAgentAdapter},
        demo::DemoExecutor,
        file_lock::FileOwnership,
        model::ModelRouter,
        native_tools::{EchoTool, SumTool},
        sqlite::SqliteStore,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{ActorService, AgentRegistry, AgentService, ToolRegistry, ports::*},
    domain::{actors::*, agent::*, tools::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

struct ManualClock(AtomicI64);
impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}
#[derive(Clone, Copy)]
enum Behavior {
    Success,
    RetryOnce,
    Unknown,
    Oversized,
    Rejected,
}
struct CountingTool {
    definition: ToolDefinition,
    behavior: Behavior,
    calls: AtomicUsize,
    keys: Mutex<Vec<(ActivityId, AttemptId)>>,
    file: Option<PathBuf>,
}
impl CountingTool {
    fn new(approval: bool, recovery: RecoveryPolicy) -> Self {
        let mut definition = EchoTool.definition();
        definition.requires_approval = approval;
        definition.recovery = recovery;
        Self {
            definition,
            behavior: Behavior::Success,
            calls: AtomicUsize::new(0),
            keys: Mutex::new(vec![]),
            file: None,
        }
    }
    fn count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}
impl NativeTool for CountingTool {
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.keys
                .lock()
                .unwrap()
                .push((call.activity_id, call.attempt_id));
            if let Some(file) = &self.file {
                let mut saved = fs::read_to_string(file).unwrap_or_default();
                saved.push_str(&format!("{} {}\n", call.activity_id, call.attempt_id));
                fs::write(file, saved)?;
            }
            match self.behavior {
                Behavior::Rejected => Ok(NativeToolResult::Rejected(ToolError::new(
                    "request_refused",
                    "choose another permitted action",
                ))),
                Behavior::Unknown => Err(Error::Unavailable(
                    "test operation completed but receipt is uncertain".into(),
                )),
                Behavior::RetryOnce if count == 1 => Ok(NativeToolResult::Failure(Failure::new(
                    "busy",
                    "known transient refusal",
                    true,
                ))),
                Behavior::Oversized => {
                    Ok(NativeToolResult::Success(json!({"text":"x".repeat(9000)})))
                }
                _ => Ok(NativeToolResult::Success(
                    json!({"text":call.arguments["text"]}),
                )),
            }
        })
    }
}
struct App {
    engine: Engine,
    agents: AgentService,
    clock: Arc<ManualClock>,
}
async fn open(
    db: &Path,
    tools: Vec<Arc<dyn NativeTool>>,
    models: Option<Arc<dyn ModelClient>>,
) -> App {
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let clock = Arc::new(ManualClock(AtomicI64::new(1000)));
    let tools = Arc::new(ToolRegistry::new(tools).unwrap());
    let registry = Arc::new(
        AgentRegistry::new(vec![
            Arc::new(AnswerAgentAdapter),
            Arc::new(PlannerAgentAdapter::new(tools.clone())),
            Arc::new(ToolAgentAdapter::new(tools.clone())),
        ])
        .unwrap(),
    );
    let root = db.parent().unwrap().join("workspaces");
    let engine = Engine::new(
        store.clone(),
        Arc::new(
            AgentExecutor::new(
                models.unwrap_or_else(|| Arc::new(ModelRouter::new(None).unwrap())),
                Arc::new(LocalTools::new(root.clone())),
                Arc::new(DemoExecutor),
            )
            .with_native_tools(tools.clone()),
        ),
        clock.clone(),
        Arc::new(FileOwnership::new(db)),
        registry.workflows(),
    )
    .unwrap();
    let actors = Arc::new(
        ActorService::new(
            engine.clone(),
            store.clone(),
            vec![
                Arc::new(RegisteredAgentDefinition),
                Arc::new(CoordinatorAgent),
            ],
        )
        .unwrap()
        .with_agent_registry(registry.clone())
        .with_inspection_reader(store.clone())
        .with_goal_control(store),
    );
    let engine = engine.with_coordinator(actors.clone());
    let agents = AgentService::new(engine.clone(), Arc::new(LocalWorkspaces::new(root)))
        .with_actors(actors)
        .with_agent_registry(registry)
        .with_tool_registry(tools);
    App {
        engine,
        agents,
        clock,
    }
}
fn config() -> Value {
    json!({"goal":"Use the echo tool then answer.","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "tools":[{"name":"echo","version":1}],
        "script":[{"kind":"call_tool","tool":"echo","arguments":{"text":"saved"}},{"kind":"finish","summary":"Done."}]})
}

struct FixedFeedback {
    definition: ToolDefinition,
    error: ToolError,
}
impl NativeTool for FixedFeedback {
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }
    fn execute(&self, _: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async { Ok(NativeToolResult::Rejected(self.error.clone())) })
    }
}

#[tokio::test]
async fn feedback_requires_a_pinned_contract_and_bounded_valid_output() {
    for mode in ["valid", "undeclared", "code", "empty", "message", "output"] {
        let mut definition = EchoTool.definition();
        definition.reports_errors = mode != "undeclared";
        if mode == "output" {
            definition.max_output_bytes = 32;
        }
        let error = ToolError::new(
            if mode == "code" {
                "invalid code"
            } else {
                "request_refused"
            },
            match mode {
                "empty" => " ".into(),
                "message" => "😀".repeat(1025),
                _ => "choose another permitted action".into(),
            },
        );
        let tool = Arc::new(FixedFeedback {
            definition: definition.clone(),
            error: error.clone(),
        });
        let registry = ToolRegistry::new(vec![tool]).unwrap();
        let Command::Schedule(spec) = definition
            .schedule_call(&json!({"text":"request"}))
            .unwrap()
        else {
            panic!("expected intent")
        };
        assert_eq!(
            spec.handler.version,
            if mode == "undeclared" { 1 } else { 3 }
        );
        let result = registry
            .execute(ActivityExecution {
                run_id: RunId::new(),
                activity_id: ActivityId::new(),
                attempt_id: AttemptId::new(),
                ordinal: 1,
                handler: spec.handler,
                input: spec.input,
            })
            .await
            .unwrap();
        if mode == "valid" {
            assert_eq!(result.result(), Some(&json!({"ok":false,"error":error})));
        } else {
            let Outcome::Failure(failure) = result else {
                panic!("invalid feedback was admitted")
            };
            assert!(!failure.retryable);
            assert_eq!(
                failure.code,
                if mode == "output" {
                    "tool_output_limit"
                } else {
                    "tool_feedback_contract"
                }
            );
        }
    }
    assert!(json!(EchoTool.definition()).get("reports_errors").is_none());
    assert!(
        EchoTool
            .definition()
            .prompt_metadata()
            .get("reports_errors")
            .is_none()
    );
}

#[tokio::test]
async fn expected_feedback_retains_results_when_context_or_model_budget_stops_correction() {
    for policy in ["tool", "planner"] {
        for mode in ["context", "model"] {
            let dir = tempfile::tempdir().unwrap();
            let mut definition = EchoTool.definition();
            definition.reports_errors = true;
            let tool = Arc::new(FixedFeedback {
                definition,
                error: ToolError::new(
                    "request_refused",
                    if mode == "context" {
                        "x".repeat(3500)
                    } else {
                        "choose another permitted action".into()
                    },
                ),
            });
            let app = open(
                &dir.path().join("feedback-limits.db"),
                vec![tool.clone()],
                None,
            )
            .await;
            let mut cfg = config();
            if mode == "context" {
                // Admit the initial request, then overflow only with saved feedback.
                // Derive the boundary from the pinned format rather than prompt wording.
                let registry = Arc::new(ToolRegistry::new(vec![tool]).unwrap());
                let (pinned, workflow): (Value, Arc<dyn WorkflowDefinition>) = if policy == "tool" {
                    let adapter = ToolAgentAdapter::new(registry);
                    (
                        adapter.pin_config(cfg.clone(), None).unwrap(),
                        adapter.definition(),
                    )
                } else {
                    let adapter = PlannerAgentAdapter::new(registry);
                    let mut seed = planner_config(AgentId::new());
                    seed["goal"] = json!("Submitted goal");
                    (
                        adapter.pin_config(seed, None).unwrap(),
                        adapter.definition(),
                    )
                };
                let seed = Run::start(workflow.as_ref(), pinned, 10, 0).unwrap();
                let request: ModelRequest =
                    serde_json::from_value(seed.activities()[0].spec.input.clone()).unwrap();
                cfg["max_input_bytes"] = json!(request.context_input_bytes() + 100);
            } else {
                cfg["max_model_calls"] = json!(1);
            }
            let actor = create_policy(&app, policy, cfg, 10).await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            settle(&mut worker).await;
            let view = app.agents.actors().unwrap().task(task).await.unwrap();
            let run = app.engine.status(view.run_id.unwrap()).await.unwrap();
            assert_eq!(run.activities()[1].result.as_ref().unwrap()["ok"], false);
            assert_eq!(run.record().usage.admitted_model_calls, 1);
            if mode == "context" {
                assert_eq!(run.record().error.as_ref().unwrap().code, "context_limit");
            } else {
                assert_eq!(view.status, AgentTaskStatus::BudgetExhausted);
            }
        }
    }
}
async fn create(app: &App, config: Value, attempts: u32) -> AgentId {
    app.agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            None,
            config,
            attempts,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap()
}
async fn answer_worker(app: &App, wait: bool) -> AgentId {
    let mut script = vec![];
    if wait {
        script.push(json!({"kind":"ask_human","question":"Continue?"}));
    }
    script.push(json!({"kind":"finish","summary":"Worker answered."}));
    app.agents.create_registered_actor(DefinitionId::new("answer",1), None,
        json!({"goal":"Answer the assigned task","target":{"provider":"scripted","model":"worker","endpoint":""},
            "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"script":script}),
        10, AgentQueuePolicy::default()).await.unwrap()
}
fn planner_config(worker: AgentId) -> Value {
    let mut cfg = config();
    cfg["target"]["model"] = json!("planner");
    cfg["workers"] = json!([{"name":"assistant","agent_id":worker,"description":"Answers a delegated question"}]);
    cfg["delegation"] = json!({"max_tasks":2,"max_depth":1});
    cfg
}
async fn create_policy(app: &App, policy: &str, mut cfg: Value, attempts: u32) -> AgentId {
    if policy == "tool" {
        return create(app, cfg, attempts).await;
    }
    let worker = answer_worker(app, false).await;
    let base = planner_config(worker);
    cfg["workers"] = base["workers"].clone();
    cfg["delegation"] = base["delegation"].clone();
    app.agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            cfg,
            attempts,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap()
}
async fn send(app: &App, id: AgentId) -> MessageId {
    let message = AgentMessage {
        id: MessageId::new(),
        sender: None,
        recipient: id,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":"Submitted goal","tools":[{"name":"sum","version":1}]}),
    };
    app.agents
        .actors()
        .unwrap()
        .send(message.clone())
        .await
        .unwrap();
    message.id
}
async fn settle(worker: &mut Worker) {
    for _ in 0..100 {
        if !worker.step().await.unwrap() && !worker.has_active_runs().await.unwrap() {
            return;
        }
    }
    panic!("work did not settle");
}
async fn mailbox(worker: &mut Worker) {
    for _ in 0..100 {
        if !worker.advance_work().await.unwrap() {
            return;
        }
    }
    panic!("mailbox did not settle");
}
async fn quiesce(worker: &mut Worker) {
    // A future reminder remains runnable work, but this manual clock does not advance.
    for _ in 0..100 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("worker did not reach its next durable wait");
}
async fn model_once(worker: &mut Worker) -> ActivityExecution {
    mailbox(worker).await;
    let model = worker.admit().await.unwrap().unwrap();
    assert_eq!(model.handler, DefinitionId::new("agent.model", 1));
    let outcome = worker.execute(model.clone()).await.unwrap();
    worker.complete(model.clone(), outcome).await.unwrap();
    model
}

#[test]
fn typed_contracts_validate_definitions_arguments_optional_fields_and_registry_duplicates() {
    let mut tool = EchoTool.definition();
    tool.parameters.insert(
        "count".into(),
        ToolParameter {
            value_type: ToolValueType::Integer,
            description: "Optional count".into(),
            required: false,
        },
    );
    tool.parameters.insert(
        "enabled".into(),
        ToolParameter {
            value_type: ToolValueType::Boolean,
            description: "Flag".into(),
            required: true,
        },
    );
    tool.parameters.insert(
        "ratio".into(),
        ToolParameter {
            value_type: ToolValueType::Number,
            description: "Ratio".into(),
            required: true,
        },
    );
    assert!(
        tool.validate_arguments(&json!({"text":"✓","enabled":true,"ratio":0.5}))
            .is_ok()
    );
    assert!(
        tool.validate_arguments(&json!({"text":"✓","enabled":true,"ratio":0.5,"count":null}))
            .is_ok()
    );
    for bad in [
        json!({"text":1}),
        json!({"text":"ok","enabled":1,"ratio":1}),
        json!({"text":"ok","enabled":true,"ratio":1,"count":0.5}),
        json!({"text":"ok","enabled":true,"ratio":1,"extra":0}),
        json!([]),
        json!({"text":null,"enabled":true,"ratio":1}),
    ] {
        assert!(tool.validate_arguments(&bad).is_err());
    }
    assert_eq!(tool.input_schema()["additionalProperties"], false);
    tool.max_input_bytes = 2;
    assert!(
        tool.validate_arguments(&json!({"text":"too large"}))
            .is_err()
    );
    for bad in [0, 1_048_577] {
        tool.max_input_bytes = bad;
        assert!(tool.validate().is_err());
    }
    assert!(ToolRegistry::new(vec![Arc::new(EchoTool), Arc::new(EchoTool)]).is_err());
}

#[tokio::test]
async fn tool_result_and_exact_next_model_request_survive_restart_without_repeating_effect() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("tools.db");
    let tool = Arc::new(CountingTool::new(false, RecoveryPolicy::SafeToRepeat));
    let app = open(&db, vec![tool.clone(), Arc::new(SumTool)], None).await;
    let actor = create(&app, config(), 10).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    let first = model_once(&mut worker).await;
    let input: ModelRequest = serde_json::from_value(first.input.clone()).unwrap();
    assert!(input.native_tool_flattened);
    assert_eq!(input.native_tool_names, vec!["echo"]);
    assert_eq!(
        input.schema["properties"]["action"]["anyOf"][0]["properties"]["kind"]["enum"][0],
        "echo"
    );
    assert!(
        !input.schema["properties"]["action"]["anyOf"]
            .as_array()
            .unwrap()
            .iter()
            .any(|variant| variant["properties"]["kind"]["enum"] == json!(["sum"]))
    );
    let call = worker.admit().await.unwrap().unwrap();
    let id = call.activity_id;
    assert_eq!(call.input["definition"], json!(tool.definition));
    worker
        .complete(call.clone(), worker.execute(call.clone()).await.unwrap())
        .await
        .unwrap();
    let before = app.engine.status(call.run_id).await.unwrap();
    let next = before.activities().last().unwrap().spec.input.clone();
    assert!(next["prompt"].as_str().unwrap().contains("saved"));
    assert_eq!(before.record().input["input"]["tools"], config()["tools"]);
    let usage = before.record().usage.clone();
    drop(worker);
    drop(app);
    let app = open(&db, vec![tool.clone(), Arc::new(SumTool)], None).await;
    let mut worker = app.engine.worker().await.unwrap();
    assert_eq!(
        app.engine
            .status(call.run_id)
            .await
            .unwrap()
            .activities()
            .last()
            .unwrap()
            .spec
            .input,
        next
    );
    assert_eq!(
        json!(app.engine.status(call.run_id).await.unwrap().record().usage),
        json!(usage)
    );
    settle(&mut worker).await;
    assert_eq!(tool.count(), 1);
    let run = app.engine.status(call.run_id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(
        run.attempts()
            .iter()
            .filter(|a| a.activity_id == id)
            .count(),
        1
    );
    assert_eq!(run.record().usage.admitted_model_calls, 2);
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .queue(actor)
            .await
            .unwrap()
            .outstanding,
        0
    );
    assert!(
        worker
            .complete(call, Outcome::Success(json!({"text":"different"})))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn approval_wait_survives_restart_and_answers_remain_durable_during_goal_pause() {
    for policy in ["tool", "planner"] {
        for approved in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("approval.db");
            let tool = Arc::new(CountingTool::new(
                true,
                RecoveryPolicy::RequiresReconciliation,
            ));
            let app = open(&db, vec![tool.clone()], None).await;
            let actor = create_policy(&app, policy, config(), 10).await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            settle(&mut worker).await;
            let view = app.agents.actors().unwrap().task(task).await.unwrap();
            let run = view.run_id.unwrap();
            let wait = view.wait.unwrap();
            assert_eq!(wait.key, "native_tool_approval");
            assert_eq!(tool.count(), 0);
            app.agents.actors().unwrap().pause_goal(task).await.unwrap();
            app.engine.pause(run).await.unwrap();
            drop(worker);
            drop(app);
            let app = open(&db, vec![tool.clone()], None).await;
            let mut worker = app.engine.worker().await.unwrap();
            assert_eq!(
                app.engine
                    .status(run)
                    .await
                    .unwrap()
                    .record()
                    .wait
                    .as_ref()
                    .unwrap()
                    .id,
                wait.id
            );
            app.engine
                .signal(run, wait.id, json!({"approved":approved}))
                .await
                .unwrap();
            app.engine
                .signal(run, wait.id, json!({"approved":approved}))
                .await
                .unwrap();
            assert!(
                app.engine
                    .signal(run, wait.id, json!({"approved":!approved}))
                    .await
                    .is_err()
            );
            settle(&mut worker).await;
            assert_eq!(tool.count(), 0);
            app.agents
                .actors()
                .unwrap()
                .resume_goal(task)
                .await
                .unwrap();
            settle(&mut worker).await;
            assert_eq!(
                app.engine.status(run).await.unwrap().record().status,
                RunStatus::Paused
            );
            app.engine.resume(run).await.unwrap();
            settle(&mut worker).await;
            assert_eq!(tool.count(), usize::from(approved));
            assert_eq!(
                app.engine.status(run).await.unwrap().record().status,
                RunStatus::Succeeded
            );
        }
    }
}

#[tokio::test]
async fn changed_or_missing_contract_blocks_before_admission_and_exact_restoration_preserves_pins()
{
    for (policy, feedback) in [
        ("tool", false),
        ("planner", false),
        ("tool", true),
        ("planner", true),
    ] {
        for change in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("pins.db");
            let mut tool = CountingTool::new(false, RecoveryPolicy::SafeToRepeat);
            tool.definition.reports_errors = feedback;
            let tool = Arc::new(tool);
            let app = open(&db, vec![tool.clone()], None).await;
            let actor = create_policy(&app, policy, config(), 10).await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            mailbox(&mut worker).await;
            let run = app
                .agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .run_id
                .unwrap();
            let original = app.engine.status(run).await.unwrap().record().input.clone();
            drop(worker);
            drop(app);
            let mut changed = CountingTool::new(false, RecoveryPolicy::SafeToRepeat);
            changed.definition.description = "Changed contract".into();
            let installed: Vec<Arc<dyn NativeTool>> = if change {
                vec![Arc::new(changed)]
            } else {
                vec![]
            };
            let app = open(&db, installed, None).await;
            let mut worker = app.engine.worker().await.unwrap();
            settle(&mut worker).await;
            let blocked = app.engine.status(run).await.unwrap();
            assert_eq!(blocked.record().status, RunStatus::Blocked);
            assert_eq!(blocked.record().allowance.admitted, 0);
            assert_eq!(blocked.record().input, original);
            assert!(app.engine.resume(run).await.is_err());
            drop(worker);
            drop(app);
            let app = open(&db, vec![tool.clone()], None).await;
            let mut worker = app.engine.worker().await.unwrap();
            app.engine.resume(run).await.unwrap();
            settle(&mut worker).await;
            assert_eq!(tool.count(), 1);
            assert_eq!(
                app.engine.status(run).await.unwrap().record().input,
                original
            );
        }
    }
}

#[tokio::test]
async fn missing_tool_retains_queued_mailbox_work_until_its_exact_contract_is_restored() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("queued-pin.db");
        let tool = Arc::new(CountingTool::new(false, RecoveryPolicy::SafeToRepeat));
        let app = open(&db, vec![tool.clone()], None).await;
        let actor = create_policy(&app, policy, config(), 10).await;
        let task = send(&app, actor).await;
        let state = app
            .agents
            .actors()
            .unwrap()
            .status(actor)
            .await
            .unwrap()
            .record()
            .state
            .clone();
        drop(app);
        let app = open(&db, vec![], None).await;
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let actors = app.agents.actors().unwrap();
        assert_eq!(
            actors.status(actor).await.unwrap().record().status,
            AgentStatus::Blocked
        );
        assert_eq!(actors.status(actor).await.unwrap().record().state, state);
        assert_eq!(
            actors.mailbox(actor).await.unwrap()[0].status,
            MessageStatus::Queued
        );
        assert!(actors.task(task).await.unwrap().run_id.is_none());
        assert_eq!(tool.count(), 0);
        drop(worker);
        drop(app);
        let app = open(&db, vec![tool.clone()], None).await;
        app.agents.actors().unwrap().resume(actor).await.unwrap();
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        assert_eq!(tool.count(), 1);
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .status,
            AgentTaskStatus::Succeeded
        );
    }
}
struct ChosenAction(AgentAction);
impl ModelClient for ChosenAction {
    fn generate(&self, _: ModelRequest, _: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            Ok(ModelReply {
                decision: Ok(ModelDecision {
                    action: self.0.clone(),
                    request_id: None,
                    diagnostics: None,
                }),
                usage: Some(TokenUsage {
                    input_tokens: 10,
                    output_tokens: 10,
                }),
                retryable: false,
                diagnostics: None,
                retry_after: None,
            })
        })
    }
}
struct ToolThenDelegate {
    wait: bool,
}
impl ModelClient for ToolThenDelegate {
    fn generate(&self, request: ModelRequest, id: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
            let action = if request.target.model == "worker" {
                if self.wait && request.turn == 0 {
                    AgentAction::AskHuman {
                        question: "Continue?".into(),
                    }
                } else {
                    AgentAction::Finish {
                        summary: prompt["goal"].as_str().unwrap().into(),
                    }
                }
            } else {
                match request.turn {
                    0 => AgentAction::CallTool {
                        tool: "echo".into(),
                        arguments: json!({"text":"durable evidence"}),
                    },
                    1 => AgentAction::DelegateTask {
                        writable_paths: None,
                        worker: "assistant".into(),
                        goal: format!(
                            "Explain {}",
                            prompt["observations"][0]["result"]["text"]
                                .as_str()
                                .unwrap()
                        ),
                    },
                    _ => {
                        assert_eq!(prompt["observations"][1]["result"]["status"], "succeeded");
                        AgentAction::Finish {
                            summary: "Used saved tool evidence and the worker reply.".into(),
                        }
                    }
                }
            };
            ChosenAction(action).generate(request, id).await
        })
    }
}
#[tokio::test]
async fn planner_uses_saved_tool_data_for_delegation_across_two_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("mixed-planner.db");
    let tool = Arc::new(CountingTool::new(false, RecoveryPolicy::SafeToRepeat));
    let models = Arc::new(ToolThenDelegate { wait: true });
    let app = open(
        &db,
        vec![tool.clone(), Arc::new(SumTool)],
        Some(models.clone()),
    )
    .await;
    let child_actor = answer_worker(&app, true).await;
    let mut cfg = planner_config(child_actor);
    cfg["goal_budget"] = json!({"max_model_calls":5,"max_tokens":100000});
    let actor = app
        .agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            cfg,
            10,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    let first = model_once(&mut worker).await;
    let request: ModelRequest = serde_json::from_value(first.input).unwrap();
    let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
    let mut metadata = EchoTool.definition().prompt_metadata();
    metadata["action_kind"] = json!("echo");
    assert_eq!(prompt["tools"], json!([metadata]));
    assert!(
        !request.schema["properties"]["action"]["anyOf"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["properties"]["kind"]["enum"] == json!(["sum"]))
    );
    let call = worker.admit().await.unwrap().unwrap();
    assert_eq!(call.handler, DefinitionId::new("native.tool", 1));
    worker
        .complete(call.clone(), worker.execute(call.clone()).await.unwrap())
        .await
        .unwrap();
    let before = app.engine.status(call.run_id).await.unwrap();
    let next = before.activities().last().unwrap().spec.input.clone();
    let usage = json!(before.record().usage);
    let pins = before.record().input.clone();
    drop(worker);
    drop(app);
    let app = open(
        &db,
        vec![tool.clone(), Arc::new(SumTool)],
        Some(models.clone()),
    )
    .await;
    let mut worker = app.engine.worker().await.unwrap();
    let saved = app.engine.status(call.run_id).await.unwrap();
    assert_eq!(saved.activities().last().unwrap().spec.input, next);
    assert_eq!(json!(saved.record().usage), usage);
    assert_eq!(saved.record().input, pins);
    settle(&mut worker).await;
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    let child = view.delegation.unwrap();
    assert_eq!(child.goal, "Explain durable evidence");
    let waiting = app.agents.actors().unwrap().task(child.id).await.unwrap();
    let wait = waiting.wait.unwrap();
    assert_eq!(tool.count(), 1);
    drop(worker);
    drop(app);
    let app = open(&db, vec![tool.clone()], Some(models)).await;
    let mut worker = app.engine.worker().await.unwrap();
    let saved_child = app.agents.actors().unwrap().task(child.id).await.unwrap();
    assert_eq!(saved_child.wait.as_ref().unwrap().id, wait.id);
    app.engine
        .signal(
            saved_child.run_id.unwrap(),
            wait.id,
            json!({"answer":"Continue"}),
        )
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(tool.count(), 1);
    let parent = app.engine.status(call.run_id).await.unwrap();
    assert_eq!(parent.record().status, RunStatus::Succeeded);
    assert_eq!(parent.record().usage.admitted_model_calls, 3);
    assert_eq!(
        parent
            .attempts()
            .iter()
            .filter(|a| a.activity_id == call.activity_id)
            .count(),
        1
    );
    let inspection = app
        .agents
        .actors()
        .unwrap()
        .inspect_task(task, Default::default())
        .await
        .unwrap();
    assert_eq!(inspection.nodes.len(), 2);
    assert_eq!(inspection.summary.visible_usage.admitted_model_calls, 5);
}

#[tokio::test]
async fn planner_configuration_rejects_forged_missing_duplicate_and_unallowed_tools() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(
        &dir.path().join("planner-config.db"),
        vec![Arc::new(EchoTool)],
        None,
    )
    .await;
    let worker = answer_worker(&app, false).await;
    for mode in ["forged", "missing", "duplicate", "unallowed", "types"] {
        let mut cfg = planner_config(worker);
        match mode {
            "forged" => cfg["tool_definitions"] = json!([EchoTool.definition()]),
            "missing" => cfg["tools"][0]["version"] = json!(2),
            "duplicate" => cfg["tools"]
                .as_array_mut()
                .unwrap()
                .push(json!({"name":"echo","version":1})),
            "unallowed" => cfg["script"][0]["tool"] = json!("sum"),
            _ => cfg["script"][0]["arguments"] = json!({"text":1}),
        }
        assert!(
            app.agents
                .create_registered_actor(
                    DefinitionId::new("planner", 3),
                    None,
                    cfg,
                    10,
                    AgentQueuePolicy::default()
                )
                .await
                .is_err(),
            "accepted {mode}"
        );
    }
}

#[tokio::test]
async fn planner_follow_up_retains_tool_pins_memory_and_goal_hold_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("tool-follow-up.db");
    let tool = Arc::new(CountingTool::new(false, RecoveryPolicy::SafeToRepeat));
    let app = open(&db, vec![tool.clone()], None).await;
    let mut cfg = planner_config(answer_worker(&app, false).await);
    cfg["memory"] = json!({"max_entries":8,"max_bytes":8192});
    cfg["follow_up"] = json!({"max_follow_ups":1,"min_delay_ms":1,"max_delay_ms":1000});
    cfg["goal_budget"] = json!({"max_model_calls":4,"max_tokens":100000});
    cfg["script"] = json!([{"kind":"call_tool","tool":"echo","arguments":{"text":"remember"}},
        {"kind":"schedule_follow_up","summary":"Saved evidence","goal":"Review it","after_ms":500}]);
    let actor = app
        .agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            cfg,
            10,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    quiesce(&mut worker).await;
    let parent = app.agents.actors().unwrap().task(task).await.unwrap();
    let intent = parent.follow_up.unwrap();
    let reminder = app
        .agents
        .actors()
        .unwrap()
        .reminder(intent.reminder_id)
        .await
        .unwrap();
    let pins = app
        .engine
        .status(parent.run_id.unwrap())
        .await
        .unwrap()
        .record()
        .input["input"]["tool_definitions"]
        .clone();
    app.agents.actors().unwrap().pause_goal(task).await.unwrap();
    drop(worker);
    drop(app);
    let app = open(&db, vec![tool.clone()], None).await;
    app.clock.0.store(2000, Ordering::SeqCst);
    let mut worker = app.engine.worker().await.unwrap();
    quiesce(&mut worker).await;
    assert_eq!(tool.count(), 1);
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .reminder(intent.reminder_id)
            .await
            .unwrap(),
        reminder
    );
    app.agents
        .actors()
        .unwrap()
        .resume_goal(task)
        .await
        .unwrap();
    quiesce(&mut worker).await;
    let child = app
        .agents
        .actors()
        .unwrap()
        .task(intent.reminder_id)
        .await
        .unwrap();
    let run = app.engine.status(child.run_id.unwrap()).await.unwrap();
    assert_eq!(run.record().input["input"]["tool_definitions"], pins);
    assert!(run.record().input["input"]["actor_memory"].is_object());
    assert_eq!(child.status, AgentTaskStatus::Succeeded);
    assert_eq!(
        child.follow_up.unwrap().status,
        act0r::domain::follow_up::FollowUpStatus::Rejected
    );
    assert_eq!(tool.count(), 2); // A new task intentionally requests a new tool call.
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .reminders(actor)
            .await
            .unwrap()
            .len(),
        1
    );
    let inspection = app
        .agents
        .actors()
        .unwrap()
        .inspect_task(task, Default::default())
        .await
        .unwrap();
    assert_eq!(inspection.summary.visible_usage.admitted_model_calls, 4);
}

#[tokio::test]
async fn planner_direct_tool_results_precede_an_ordered_group_with_one_goal_allowance() {
    for allowance in [4, 5] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("tool-group.db");
        let tool = Arc::new(CountingTool::new(false, RecoveryPolicy::SafeToRepeat));
        let app = open(&db, vec![tool.clone()], None).await;
        let a = answer_worker(&app, false).await;
        let b = answer_worker(&app, false).await;
        let mut cfg = planner_config(a);
        cfg["workers"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"second","agent_id":b,"description":"Second specialist"}));
        cfg["delegation"]["max_parallel_tasks"] = json!(2);
        cfg["goal_budget"] = json!({"max_model_calls":allowance,"max_tokens":100000});
        cfg["script"] = json!([{"kind":"call_tool","tool":"echo","arguments":{"text":"group evidence"}},
        {"kind":"delegate_tasks","tasks":[{"worker":"second","goal":"Second"},{"worker":"assistant","goal":"First"}]},
        {"kind":"finish","summary":"Combined."}]);
        let actor = app
            .agents
            .create_registered_actor(
                DefinitionId::new("planner", 3),
                None,
                cfg,
                10,
                AgentQueuePolicy::default(),
            )
            .await
            .unwrap();
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(
            view.status,
            if allowance == 5 {
                AgentTaskStatus::Succeeded
            } else {
                AgentTaskStatus::BudgetExhausted
            }
        );
        let run = app.engine.status(view.run_id.unwrap()).await.unwrap();
        let final_request: ModelRequest =
            serde_json::from_value(run.activities().last().unwrap().spec.input.clone()).unwrap();
        let prompt: Value = serde_json::from_str(&final_request.prompt).unwrap();
        assert_eq!(
            prompt["observations"][0]["result"]["text"],
            "group evidence"
        );
        assert_eq!(
            prompt["observations"][1]["action"]["tasks"][0]["worker"],
            "second"
        );
        let ids: Vec<_> = run.record().task_groups[0]
            .children
            .iter()
            .map(|id| json!(id))
            .collect();
        assert_eq!(prompt["observations"][1]["results"][0]["task_id"], ids[0]);
        assert_eq!(prompt["observations"][1]["results"][1]["task_id"], ids[1]);
        assert_eq!(tool.count(), 1);
        let inspection = app
            .agents
            .actors()
            .unwrap()
            .inspect_task(task, Default::default())
            .await
            .unwrap();
        assert_eq!(
            inspection.summary.visible_usage.admitted_model_calls,
            allowance
        );
    }
}
#[tokio::test]
async fn malformed_or_unallowed_model_calls_never_dispatch_a_tool() {
    for policy in ["tool", "planner"] {
        for action in [
            AgentAction::CallTool {
                tool: "sum".into(),
                arguments: json!({"left":1,"right":2}),
            },
            AgentAction::CallTool {
                tool: "echo".into(),
                arguments: json!({"text":4}),
            },
            AgentAction::CallTool {
                tool: "echo".into(),
                arguments: json!({"text":"ok","extra":true}),
            },
        ] {
            let dir = tempfile::tempdir().unwrap();
            let tool = Arc::new(CountingTool::new(false, RecoveryPolicy::SafeToRepeat));
            let app = open(
                &dir.path().join("refuse.db"),
                vec![tool.clone(), Arc::new(SumTool)],
                Some(Arc::new(ChosenAction(action))),
            )
            .await;
            let actor = create_policy(&app, policy, config(), 10).await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            settle(&mut worker).await;
            assert_eq!(tool.count(), 0);
            let view = app.agents.actors().unwrap().task(task).await.unwrap();
            assert_eq!(view.status, AgentTaskStatus::Failed);
            let run = app.engine.status(view.run_id.unwrap()).await.unwrap();
            assert_eq!(run.attempts().len(), 1);
            assert_eq!(run.record().usage.admitted_model_calls, 1);
        }
    }
}

#[tokio::test]
async fn retry_deadline_and_stable_activity_key_survive_restart() {
    for (policy, feedback) in [
        ("tool", false),
        ("planner", false),
        ("tool", true),
        ("planner", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("retry.db");
        let mut tool = CountingTool::new(false, RecoveryPolicy::SafeToRepeat);
        tool.behavior = Behavior::RetryOnce;
        tool.definition.reports_errors = feedback;
        let tool = Arc::new(tool);
        let app = open(&db, vec![tool.clone()], None).await;
        let actor = create_policy(&app, policy, config(), 10).await;
        send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let call = worker.admit().await.unwrap().unwrap();
        assert_eq!(call.handler.version, if feedback { 3 } else { 1 });
        worker
            .complete(call.clone(), worker.execute(call.clone()).await.unwrap())
            .await
            .unwrap();
        assert!(worker.admit().await.unwrap().is_none());
        drop(worker);
        drop(app);
        let app = open(&db, vec![tool.clone()], None).await;
        let mut worker = app.engine.worker().await.unwrap();
        assert!(worker.admit().await.unwrap().is_none());
        app.clock.0.store(1100, Ordering::SeqCst);
        settle(&mut worker).await;
        let keys = tool.keys.lock().unwrap().clone();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].0, keys[1].0);
        assert_ne!(keys[0].1, keys[1].1);
        assert_eq!(
            app.engine
                .status(call.run_id)
                .await
                .unwrap()
                .record()
                .status,
            RunStatus::Succeeded
        );
    }
}

#[tokio::test]
async fn uncertain_tool_effect_requires_reconciliation_and_remains_protected_by_goal_controls() {
    for (policy, feedback) in [
        ("tool", false),
        ("planner", false),
        ("tool", true),
        ("planner", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("uncertain.db");
        let mut tool = CountingTool::new(false, RecoveryPolicy::RequiresReconciliation);
        tool.behavior = Behavior::Unknown;
        tool.definition.reports_errors = feedback;
        let tool = Arc::new(tool);
        let app = open(&db, vec![tool.clone()], None).await;
        let actor = create_policy(&app, policy, config(), 10).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let run = app
            .agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .run_id
            .unwrap();
        let blocked = app.engine.status(run).await.unwrap();
        assert_eq!(blocked.record().status, RunStatus::Blocked);
        let interrupted = blocked.attempts().last().unwrap().clone();
        app.agents.actors().unwrap().pause_goal(task).await.unwrap();
        app.agents
            .actors()
            .unwrap()
            .resume_goal(task)
            .await
            .unwrap();
        settle(&mut worker).await;
        assert_eq!(tool.count(), 1);
        assert!(app.engine.resume(run).await.is_err());
        app.agents.actors().unwrap().pause_goal(task).await.unwrap();
        let request = ReconciliationRequest {
            activity_id: interrupted.activity_id,
            attempt_id: interrupted.id,
            decision: ReconciliationDecision::Accept {
                result: json!({"text":"verified"}),
            },
            evidence: "Inspected the external operation and verified its result.".into(),
        };
        app.engine.reconcile(run, request.clone()).await.unwrap();
        app.engine.reconcile(run, request).await.unwrap();
        settle(&mut worker).await;
        assert_eq!(tool.count(), 1);
        app.agents
            .actors()
            .unwrap()
            .resume_goal(task)
            .await
            .unwrap();
        settle(&mut worker).await;
        assert_eq!(
            app.engine.status(run).await.unwrap().record().status,
            RunStatus::Succeeded
        );
        assert_eq!(tool.count(), 1);
    }
}

#[tokio::test]
async fn attempt_model_context_and_output_limits_retain_work_without_extra_dispatch() {
    for policy in ["tool", "planner"] {
        for mode in ["attempt", "model", "context", "output", "cancel"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("limits.db");
            let mut tool = CountingTool::new(false, RecoveryPolicy::SafeToRepeat);
            if mode == "output" {
                tool.behavior = Behavior::Oversized;
            }
            let tool = Arc::new(tool);
            let app = open(&db, vec![tool.clone()], None).await;
            let mut cfg = config();
            if mode == "model" {
                cfg["max_model_calls"] = json!(1);
            }
            if mode == "context" {
                // Fit fixed window metadata/instructions, then overflow on a complete result.
                cfg["max_input_bytes"] = json!(4000);
                cfg["script"][0]["arguments"]["text"] = json!("x".repeat(3500));
            }
            let actor =
                create_policy(&app, policy, cfg, if mode == "attempt" { 1 } else { 10 }).await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            if mode == "cancel" {
                model_once(&mut worker).await;
                app.agents
                    .actors()
                    .unwrap()
                    .cancel_goal(task)
                    .await
                    .unwrap();
            }
            settle(&mut worker).await;
            let view = app.agents.actors().unwrap().task(task).await.unwrap();
            assert_eq!(
                tool.count(),
                usize::from(!matches!(mode, "attempt" | "cancel"))
            );
            let run = app.engine.status(view.run_id.unwrap()).await.unwrap();
            if mode == "context" {
                assert_eq!(run.record().error.as_ref().unwrap().code, "context_limit");
                assert!(
                    run.activities()
                        .iter()
                        .any(|a| a.spec.handler == DefinitionId::new("native.tool", 1)
                            && a.status == ActivityStatus::Succeeded)
                );
            }
            if mode == "output" {
                assert_eq!(
                    run.record().error.as_ref().unwrap().code,
                    "tool_output_limit"
                );
            }
            if matches!(mode, "attempt" | "model") {
                assert_eq!(run.record().status, RunStatus::BudgetExhausted);
            }
            if mode == "cancel" {
                assert_eq!(run.record().status, RunStatus::Cancelled);
            }
        }
    }
}

#[tokio::test]
async fn allowlist_configuration_cannot_forge_pins_or_bypass_types_and_new_tools_do_not_change_answer_pins()
 {
    let dir = tempfile::tempdir().unwrap();
    let app = open(
        &dir.path().join("config.db"),
        vec![Arc::new(EchoTool)],
        None,
    )
    .await;
    for mode in ["missing", "duplicate", "forged", "bad-script"] {
        let mut cfg = config();
        match mode {
            "missing" => cfg["tools"][0]["version"] = json!(2),
            "duplicate" => cfg["tools"]
                .as_array_mut()
                .unwrap()
                .push(json!({"name":"echo","version":1})),
            "forged" => cfg["tool_definitions"] = json!([EchoTool.definition()]),
            _ => cfg["script"][0]["arguments"] = json!({"text":true}),
        }
        assert!(
            app.agents
                .create_registered_actor(
                    DefinitionId::new("tool", 1),
                    None,
                    cfg,
                    10,
                    AgentQueuePolicy::default()
                )
                .await
                .is_err()
        );
    }
    assert!(
        app.agents
            .create_registered_actor(
                DefinitionId::new("answer", 1),
                None,
                config(),
                10,
                AgentQueuePolicy::default()
            )
            .await
            .is_err()
    );
    assert_eq!(
        AnswerAgentAdapter.registration().capabilities.checkpoints,
        act0r::domain::integration::CheckpointGranularity::ModelDecision
    );
    let source: Value =
        serde_json::from_str(include_str!("../examples/configs/answer-scripted.json")).unwrap();
    assert!(
        app.agents
            .create_registered_actor(
                DefinitionId::new("answer", 1),
                None,
                source,
                10,
                AgentQueuePolicy::default()
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn delegated_tool_actor_uses_shared_goal_allowance_and_retains_saved_results() {
    let dir = tempfile::tempdir().unwrap();
    let tool = Arc::new(CountingTool::new(false, RecoveryPolicy::SafeToRepeat));
    let app = open(&dir.path().join("goal.db"), vec![tool.clone()], None).await;
    let actor = create(&app, config(), 10).await;
    let planner = json!({"goal":"Delegate", "target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "goal_budget":{"max_model_calls":2,"max_tokens":100000},"workers":[{"name":"worker","agent_id":actor,"description":"Native tools"}],"delegation":{"max_tasks":1,"max_depth":2},
        "script":[{"kind":"delegate_task","worker":"worker","goal":"Use a tool"},{"kind":"finish","summary":"Done"}]});
    let root = app
        .agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            planner,
            10,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap();
    let task = send(&app, root).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(tool.count(), 1);
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(view.status, AgentTaskStatus::BudgetExhausted);
    let inspection = app
        .agents
        .actors()
        .unwrap()
        .inspect_task(task, Default::default())
        .await
        .unwrap();
    assert_eq!(inspection.nodes.len(), 2);
    assert_eq!(inspection.summary.visible_usage.admitted_model_calls, 2);
}

#[tokio::test]
async fn tool_intent_and_completion_transactions_roll_back_without_losing_the_saved_outcome() {
    for (policy, feedback) in [
        ("tool", false),
        ("planner", false),
        ("tool", true),
        ("planner", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("rollback.db");
        let mut tool = CountingTool::new(false, RecoveryPolicy::SafeToRepeat);
        if feedback {
            tool.definition.reports_errors = true;
            tool.behavior = Behavior::Rejected;
        }
        let tool = Arc::new(tool);
        let app = open(&db, vec![tool.clone()], None).await;
        let actor = create_policy(&app, policy, config(), 10).await;
        send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        mailbox(&mut worker).await;
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
        let model = worker.admit().await.unwrap().unwrap();
        let outcome = worker.execute(model.clone()).await.unwrap();
        let before = json!(app.engine.status(model.run_id).await.unwrap());
        let history = json!(app.engine.history(model.run_id).await.unwrap());
        sqlx::query("CREATE TRIGGER reject_tool_intent BEFORE INSERT ON activities WHEN json_extract(NEW.payload,'$.spec.handler.name')='native.tool' BEGIN SELECT RAISE(ABORT,'test intent rollback'); END").execute(&pool).await.unwrap();
        assert!(
            worker
                .complete(model.clone(), outcome.clone())
                .await
                .is_err()
        );
        assert_eq!(
            json!(app.engine.status(model.run_id).await.unwrap()),
            before
        );
        assert_eq!(
            json!(app.engine.history(model.run_id).await.unwrap()),
            history
        );
        assert_eq!(tool.count(), 0);
        sqlx::query("DROP TRIGGER reject_tool_intent")
            .execute(&pool)
            .await
            .unwrap();
        worker.complete(model, outcome).await.unwrap();
        let call = worker.admit().await.unwrap().unwrap();
        let outcome = worker.execute(call.clone()).await.unwrap();
        let before = json!(app.engine.status(call.run_id).await.unwrap());
        sqlx::query("CREATE TRIGGER reject_tool_continuation BEFORE INSERT ON activities WHEN json_extract(NEW.payload,'$.spec.handler.name')='agent.model' BEGIN SELECT RAISE(ABORT,'test result rollback'); END").execute(&pool).await.unwrap();
        assert!(
            worker
                .complete(call.clone(), outcome.clone())
                .await
                .is_err()
        );
        assert_eq!(json!(app.engine.status(call.run_id).await.unwrap()), before);
        assert_eq!(tool.count(), 1);
        sqlx::query("DROP TRIGGER reject_tool_continuation")
            .execute(&pool)
            .await
            .unwrap();
        worker
            .complete(call.clone(), outcome.clone())
            .await
            .unwrap();
        worker.complete(call.clone(), outcome).await.unwrap();
        settle(&mut worker).await;
        assert_eq!(tool.count(), 1);
        assert_eq!(
            app.engine
                .status(call.run_id)
                .await
                .unwrap()
                .record()
                .status,
            RunStatus::Succeeded
        );
    }
}

#[tokio::test]
async fn tool_actors_retain_memory_without_changing_their_pinned_tool_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let tool = Arc::new(CountingTool::new(false, RecoveryPolicy::SafeToRepeat));
    let app = open(&dir.path().join("memory.db"), vec![tool.clone()], None).await;
    let mut cfg = config();
    cfg["memory"] = json!({"max_entries":4,"max_bytes":8192});
    let actor = create(&app, cfg, 10).await;
    let first = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .memory(actor)
            .await
            .unwrap()
            .unwrap()
            .entries[0]
            .task_id,
        first
    );
    send(&app, actor).await;
    let request = model_once(&mut worker).await;
    assert!(request.input["prompt"].as_str().unwrap().contains("Done."));
    settle(&mut worker).await;
    assert_eq!(tool.count(), 2);
}

#[tokio::test]
async fn responses_and_chat_models_select_native_tools_and_receive_saved_observations() {
    for (policy, feedback) in [
        ("tool", false),
        ("planner", false),
        ("tool", true),
        ("planner", true),
    ] {
        use std::{
            io::{Read, Write},
            net::TcpListener,
        };
        for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let mut requests = vec![];
                for action in [
                    json!({"kind":"echo","text":"wire-result"}),
                    json!({"kind":"finish","summary":"Verified wire result."}),
                ] {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    let mut bytes = vec![];
                    let mut buffer = [0; 4096];
                    let (offset, length) = loop {
                        let count = stream.read(&mut buffer).unwrap();
                        assert!(count > 0);
                        bytes.extend_from_slice(&buffer[..count]);
                        if let Some(position) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                            let header = String::from_utf8_lossy(&bytes[..position]).to_lowercase();
                            let length = header
                                .lines()
                                .find_map(|line| {
                                    line.strip_prefix("content-length:")
                                        .map(|value| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap();
                            break (position + 4, length);
                        }
                    };
                    while bytes.len() < offset + length {
                        let count = stream.read(&mut buffer).unwrap();
                        assert!(count > 0);
                        bytes.extend_from_slice(&buffer[..count]);
                    }
                    requests.push(
                        serde_json::from_slice::<Value>(&bytes[offset..offset + length]).unwrap(),
                    );
                    let text = json!({"action":action}).to_string();
                    let body=match protocol {
                    ModelProtocol::Responses=>json!({"status":"completed","usage":{"input_tokens":20,"output_tokens":10},"output":[{"content":[{"type":"output_text","text":text}]}]}),
                    ModelProtocol::ChatCompletions=>json!({"usage":{"prompt_tokens":20,"completion_tokens":10},"choices":[{"finish_reason":"stop","message":{"content":text}}]}),
                }.to_string();
                    write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
                }
                requests
            });
            let dir = tempfile::tempdir().unwrap();
            let mut tool = CountingTool::new(false, RecoveryPolicy::SafeToRepeat);
            if feedback {
                tool.definition.reports_errors = true;
                tool.behavior = Behavior::Rejected;
            }
            let tool = Arc::new(tool);
            let app = open(&dir.path().join("wire.db"), vec![tool.clone()], None).await;
            let mut cfg = config();
            cfg["target"] = json!({"provider":"openai","model":"fixture","endpoint":format!("http://{address}/v1/{}",if matches!(protocol, ModelProtocol::Responses) {"responses"}else{"chat/completions"}),"protocol":protocol});
            cfg["script"] = json!([]);
            let actor = create_policy(&app, policy, cfg, 10).await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            settle(&mut worker).await;
            assert_eq!(tool.count(), 1);
            assert_eq!(
                app.agents
                    .actors()
                    .unwrap()
                    .task(task)
                    .await
                    .unwrap()
                    .status,
                AgentTaskStatus::Succeeded
            );
            let requests = server.join().unwrap();
            assert_eq!(requests.len(), 2);
            let schema = if matches!(protocol, ModelProtocol::Responses) {
                &requests[0]["text"]["format"]["schema"]
            } else {
                &requests[0]["response_format"]["json_schema"]["schema"]
            };
            assert!(!schema.to_string().contains("call_tool"));
            assert!(requests[1].to_string().contains("wire-result"));
            assert_eq!(
                requests[1].to_string().contains("request_refused"),
                feedback
            );
        }
    }
}

#[tokio::test]
async fn admitted_tool_outcomes_commit_under_goal_pause_or_cancellation() {
    for policy in ["tool", "planner"] {
        for cancel in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let tool = Arc::new(CountingTool::new(false, RecoveryPolicy::SafeToRepeat));
            let app = open(&dir.path().join("controls.db"), vec![tool.clone()], None).await;
            let actor = create_policy(&app, policy, config(), 10).await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            model_once(&mut worker).await;
            let call = worker.admit().await.unwrap().unwrap();
            let outcome = worker.execute(call.clone()).await.unwrap();
            if cancel {
                app.agents
                    .actors()
                    .unwrap()
                    .cancel_goal(task)
                    .await
                    .unwrap();
            } else {
                app.agents.actors().unwrap().pause_goal(task).await.unwrap();
            }
            worker.complete(call.clone(), outcome).await.unwrap();
            settle(&mut worker).await;
            let saved = app.engine.status(call.run_id).await.unwrap();
            assert_eq!(
                saved
                    .activities()
                    .iter()
                    .find(|a| a.id == call.activity_id)
                    .unwrap()
                    .result,
                Some(json!({"text":"saved"}))
            );
            assert_eq!(saved.record().usage.admitted_model_calls, 1);
            assert_eq!(tool.count(), 1);
            if cancel {
                assert_eq!(saved.record().status, RunStatus::Cancelled);
            } else {
                app.agents
                    .actors()
                    .unwrap()
                    .resume_goal(task)
                    .await
                    .unwrap();
                settle(&mut worker).await;
                assert_eq!(
                    app.engine
                        .status(call.run_id)
                        .await
                        .unwrap()
                        .record()
                        .status,
                    RunStatus::Succeeded
                );
            }
            assert_eq!(tool.count(), 1);
        }
    }
}

#[tokio::test]
#[ignore]
async fn tool_crash_fixture() {
    let Ok(root) = std::env::var("ACT0R_TOOL_CRASH_DIR") else {
        return;
    };
    let root = PathBuf::from(root);
    let stage = std::env::var("ACT0R_TOOL_CRASH_STAGE").unwrap();
    let policy = std::env::var("ACT0R_TOOL_CRASH_POLICY").unwrap_or_else(|_| "tool".into());
    let mut tool = CountingTool::new(
        stage == "approval",
        if stage == "unsafe" {
            RecoveryPolicy::RequiresReconciliation
        } else {
            RecoveryPolicy::SafeToRepeat
        },
    );
    tool.file = Some(root.join("effects.log"));
    let app = open(&root.join("crash.db"), vec![Arc::new(tool)], None).await;
    let actor = create_policy(&app, &policy, config(), 10).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    if matches!(stage.as_str(), "saved" | "unsafe") {
        let call = worker.admit().await.unwrap().unwrap();
        let outcome = worker.execute(call.clone()).await.unwrap();
        if stage == "saved" {
            worker.complete(call, outcome).await.unwrap();
        }
    }
    fs::write(
        root.join("ready.json"),
        serde_json::to_vec(&json!({"task":task,"actor":actor})).unwrap(),
    )
    .unwrap();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
#[tokio::test]
async fn killed_workers_recover_tool_intent_saved_result_approval_and_uncertain_effect() {
    for policy in ["tool", "planner"] {
        for stage in ["intent", "saved", "approval", "unsafe"] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let mut child = ProcessCommand::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", "tool_crash_fixture", "--nocapture"])
                .env("ACT0R_TOOL_CRASH_DIR", root)
                .env("ACT0R_TOOL_CRASH_STAGE", stage)
                .env("ACT0R_TOOL_CRASH_POLICY", policy)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(30);
            while !root.join("ready.json").exists() {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "fixture exited: {stage}"
                );
                assert!(Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            child.kill().unwrap();
            child.wait().unwrap();
            let ready: Value =
                serde_json::from_slice(&fs::read(root.join("ready.json")).unwrap()).unwrap();
            let task: MessageId = serde_json::from_value(ready["task"].clone()).unwrap();
            let mut tool = CountingTool::new(
                stage == "approval",
                if stage == "unsafe" {
                    RecoveryPolicy::RequiresReconciliation
                } else {
                    RecoveryPolicy::SafeToRepeat
                },
            );
            tool.file = Some(root.join("effects.log"));
            let tool = Arc::new(tool);
            let app = open(&root.join("crash.db"), vec![tool.clone()], None).await;
            let mut worker = app.engine.worker().await.unwrap();
            let view = app.agents.actors().unwrap().task(task).await.unwrap();
            let run = view.run_id.unwrap();
            if stage == "approval" {
                assert_eq!(tool.count(), 0);
                app.engine
                    .signal(run, view.wait.unwrap().id, json!({"approved":true}))
                    .await
                    .unwrap();
            }
            if stage == "unsafe" {
                assert_eq!(
                    app.engine.status(run).await.unwrap().record().status,
                    RunStatus::Blocked
                );
                settle(&mut worker).await;
                assert_eq!(tool.count(), 0);
                let snapshot = app.engine.status(run).await.unwrap();
                let interrupted = snapshot.attempts().last().unwrap();
                app.engine
                    .reconcile(
                        run,
                        ReconciliationRequest {
                            activity_id: interrupted.activity_id,
                            attempt_id: interrupted.id,
                            decision: ReconciliationDecision::Accept {
                                result: json!({"text":"saved"}),
                            },
                            evidence: "Verified the fixture effect in effects.log.".into(),
                        },
                    )
                    .await
                    .unwrap();
            }
            settle(&mut worker).await;
            assert_eq!(
                app.engine.status(run).await.unwrap().record().status,
                RunStatus::Succeeded
            );
            assert_eq!(
                fs::read_to_string(root.join("effects.log"))
                    .unwrap()
                    .lines()
                    .count(),
                1,
                "effect duplicated: {stage}"
            );
            assert_eq!(
                tool.count(),
                usize::from(matches!(stage, "intent" | "approval"))
            );
        }
    }
}
