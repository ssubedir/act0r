//! Consumer tests use public bootstrap/trait APIs and the application-owned example tool.
#[path = "../examples/custom_tools/word_count.rs"]
mod word_count;
use act0r::{
    Worker,
    adapters::native_tools::{EchoTool, SumTool},
    application::{AgentService, ToolRegistry, ports::*},
    bootstrap::{local_app, local_app_with_tools},
    domain::{actors::*, tools::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use word_count::WordCountTool;

#[test]
fn bounded_string_lists_have_matching_runtime_and_strict_model_schemas() {
    let definition = ToolDefinition::builder("batch", 1, RecoveryPolicy::SafeToRepeat)
        .description("Bounded lists")
        .required_parameter(
            "paths",
            ToolValueType::StringArray { max_items: 8 },
            "Paths",
        )
        .optional_parameter(
            "optional",
            ToolValueType::StringArray { max_items: 2 },
            "Optional paths",
        )
        .build()
        .unwrap();
    definition
        .validate_arguments(&json!({"paths":["a","b"],"optional":null}))
        .unwrap();
    definition
        .validate_arguments(&json!({"paths":["a"]}))
        .unwrap();
    for args in [
        json!({"paths":[]}),
        json!({"paths":["a",42]}),
        json!({"paths":vec!["a";9]}),
        json!({"paths":"a"}),
        json!({"paths":[["a"]]}),
        json!({"paths":["a"],"optional":[]}),
    ] {
        assert!(definition.validate_arguments(&args).is_err());
    }
    let schema = definition.action_schema();
    let paths = &schema["properties"]["arguments"]["properties"]["paths"];
    assert_eq!(paths["type"], "array");
    assert_eq!(paths["items"]["type"], "string");
    assert_eq!(paths["minItems"], 1);
    assert_eq!(paths["maxItems"], 8);
    assert_eq!(
        schema["properties"]["arguments"]["properties"]["optional"]["anyOf"][0]["maxItems"],
        2
    );
    assert_eq!(
        serde_json::from_value::<ToolDefinition>(json!(definition)).unwrap(),
        definition
    );
    assert!(
        serde_json::from_value::<ToolValueType>(
            json!({"string_array":{"max_items":8,"unknown":true}})
        )
        .is_err()
    );
    for max_items in [0, 33] {
        assert!(
            ToolDefinition::builder("invalid", 1, RecoveryPolicy::SafeToRepeat)
                .description("Invalid list bound")
                .required_parameter("paths", ToolValueType::StringArray { max_items }, "Paths")
                .build()
                .is_err()
        );
    }
}

#[test]
fn builder_preserves_existing_pins_and_supports_optional_primitive_schemas_and_scopes() {
    let echo = ToolDefinition::builder("echo", 1, RecoveryPolicy::SafeToRepeat)
        .description("Return the supplied text.")
        .required_parameter("text", ToolValueType::String, "Text to return.")
        .build()
        .unwrap();
    assert_eq!(echo, EchoTool.definition());
    assert_eq!(json!(echo), json!(EchoTool.definition()));
    let definition = ToolDefinition::builder("custom", 2, RecoveryPolicy::RequiresReconciliation)
        .description("A scoped custom operation")
        .required_parameter("text", ToolValueType::String, "Text")
        .optional_parameter("count", ToolValueType::Integer, "Count")
        .optional_parameter("enabled", ToolValueType::Boolean, "Enabled")
        .optional_parameter("ratio", ToolValueType::Number, "Ratio")
        .byte_limits(2048, 4096)
        .retry_policy(RetryPolicy {
            max_attempts: 2,
            delay_ms: 5,
            max_delay_ms: 20,
        })
        .requires_approval(true)
        .requires_workspace(true)
        .requires_writable_paths(true)
        .requires_test_command(true)
        .reports_errors(true)
        .build()
        .unwrap();
    definition
        .validate_arguments(&json!({"text":"x","count":null,"enabled":true,"ratio":0.5}))
        .unwrap();
    definition.validate_arguments(&json!({"text":"x"})).unwrap();
    assert!(
        definition
            .validate_arguments(&json!({"text":"x","count":1.5}))
            .is_err()
    );
    assert!(
        definition
            .validate_arguments(&json!({"text":"x","unknown":1}))
            .is_err()
    );
    assert_eq!(definition.input_schema()["required"], json!(["text"]));
    assert_eq!(
        definition.action_schema()["properties"]["arguments"]["required"],
        json!(["count", "enabled", "ratio", "text"])
    );
    assert_eq!(definition.handler(), DefinitionId::new("native.tool", 5));
    let command = WorkspaceTestCommand {
        argv: vec!["fixed-tests".into()],
        timeout_ms: 1000,
    };
    let Command::WaitForInput { payload, .. } = definition
        .request_call_with_context(
            &json!({"text":"x"}),
            Some("pinned-workspace"),
            &["src/".into()],
            Some(&command),
        )
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(payload["recovery"], "RequiresReconciliation");
    assert_eq!(payload["writable_paths"], json!(["src/"]));
    assert_eq!(payload["test_command"], json!(command));
}

#[test]
fn builder_rejects_duplicate_parameters_invalid_contracts_limits_retries_and_scope_dependencies() {
    let builder = || {
        ToolDefinition::builder("custom", 1, RecoveryPolicy::SafeToRepeat)
            .description("Custom tool")
    };
    let bad = vec![
        ToolDefinition::builder("bad name", 1, RecoveryPolicy::SafeToRepeat)
            .description("Bad name"),
        ToolDefinition::builder("custom", 0, RecoveryPolicy::SafeToRepeat)
            .description("Bad version"),
        ToolDefinition::builder("custom", 1, RecoveryPolicy::SafeToRepeat),
        builder().description(" "),
        builder().description("x".repeat(4097)),
        builder().required_parameter("bad name", ToolValueType::String, "Bad parameter"),
        builder()
            .required_parameter("text", ToolValueType::String, "One")
            .optional_parameter("text", ToolValueType::Integer, "Two"),
        builder().byte_limits(0, 8192),
        builder().byte_limits(4096, 0),
        builder().byte_limits(1_048_577, 8192),
        builder().byte_limits(4096, 1_048_577),
        builder().retry_policy(RetryPolicy {
            max_attempts: 0,
            delay_ms: 0,
            max_delay_ms: 0,
        }),
        builder().retry_policy(RetryPolicy {
            max_attempts: 1,
            delay_ms: 2,
            max_delay_ms: 1,
        }),
        builder().requires_writable_paths(true),
        builder().requires_test_command(true),
    ];
    for invalid in bad {
        assert!(invalid.build().is_err());
    }
    let mut many = builder();
    for i in 0..33 {
        many = many.required_parameter(format!("field_{i}"), ToolValueType::String, "Text");
    }
    assert!(many.build().is_err());
}

struct WrappedTool {
    definition: ToolDefinition,
    inner: Arc<WordCountTool>,
    ledger: Option<PathBuf>,
}
impl NativeTool for WrappedTool {
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }
    fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            if let Some(path) = &self.ledger {
                use std::io::Write;
                let mut ledger = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                writeln!(ledger, "{} {}", call.activity_id, call.attempt_id)?;
            }
            self.inner.execute(call).await
        })
    }
}

#[tokio::test]
async fn bootstrap_retains_all_builtins_and_rejects_duplicate_or_invalid_custom_contracts() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("catalog.db");
    let (default_engine, default_agents) = local_app(&db).await.unwrap();
    let builtins = default_agents.tool_definitions().unwrap();
    let identities: std::collections::HashSet<_> =
        builtins.iter().map(|tool| tool.id.clone()).collect();
    assert_eq!(identities.len(), builtins.len());
    assert!(
        builtins
            .iter()
            .any(|tool| tool.id.name == "run_command" && tool.requires_project_command)
    );
    assert!(!builtins.iter().any(|d| d.id.name == "word_count"));
    drop(default_agents);
    drop(default_engine);
    let custom = Arc::new(WordCountTool::new().unwrap());
    let (engine, agents) = local_app_with_tools(&db, vec![custom.clone()])
        .await
        .unwrap();
    let extended = agents.tool_definitions().unwrap();
    assert_eq!(extended.len(), builtins.len() + 1);
    assert_eq!(
        extended
            .iter()
            .filter(|d| d.id.name != "word_count")
            .cloned()
            .collect::<Vec<_>>(),
        builtins
    );
    assert!(extended.contains(&custom.definition()));
    drop(agents);
    drop(engine);
    assert!(
        local_app_with_tools(&db, vec![Arc::new(SumTool)])
            .await
            .is_err()
    );
    assert!(
        local_app_with_tools(&db, vec![custom.clone(), custom.clone()])
            .await
            .is_err()
    );
    let mut invalid = custom.definition();
    invalid.max_output_bytes = 0;
    assert!(
        local_app_with_tools(
            &db,
            vec![Arc::new(WrappedTool {
                definition: invalid,
                inner: custom.clone(),
                ledger: None
            })]
        )
        .await
        .is_err()
    );
    assert_eq!(custom.executions(), 0);
    let mut v2 = custom.definition();
    v2.id.version = 2;
    let registry = ToolRegistry::new(vec![custom.clone()])
        .unwrap()
        .with_tools(vec![Arc::new(WrappedTool {
            definition: v2.clone(),
            inner: custom.clone(),
            ledger: None,
        })])
        .unwrap();
    assert_eq!(registry.definitions(), vec![custom.definition(), v2]);
    assert!(registry.with_tools(vec![custom]).is_err());
    let (_, agents) = local_app(&db).await.unwrap();
    assert_eq!(agents.tool_definitions().unwrap(), builtins);
}

fn config() -> Value {
    json!({"goal":"Use custom and built-in tools","target":{"provider":"scripted","model":"custom","endpoint":""},
        "max_model_calls":5,"max_tokens":300000,"max_output_tokens":512,
        "tools":[{"name":"word_count","version":1},{"name":"sum","version":1}],
        "script":[{"kind":"call_tool","tool":"word_count","arguments":{"text":"Durable actors 🦀"}},
            {"kind":"call_tool","tool":"sum","arguments":{"left":19,"right":23}},
            {"kind":"finish","summary":"Used saved custom and built-in results."}]})
}
async fn create(agents: &AgentService, policy: &str, mut cfg: Value) -> AgentId {
    if policy == "planner" {
        let child = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
            "goal":"Review supplied results","target":{"provider":"scripted","model":"reviewer","endpoint":""},
            "max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,
            "script":[{"kind":"finish","summary":"Reviewed."}]
        }),4,Default::default()).await.unwrap();
        cfg["workers"] =
            json!([{"name":"reviewer","agent_id":child,"description":"Review supplied results"}]);
        cfg["delegation"] = json!({"max_tasks":1,"max_depth":1});
        cfg["script"] = json!([
            {"kind":"call_tool","tool":"word_count","arguments":{"text":"Durable actors 🦀"}},
            {"kind":"call_tool","tool":"sum","arguments":{"left":19,"right":23}},
            {"kind":"delegate_task","worker":"reviewer","goal":"Review the saved word count 3 and sum 42."},
            {"kind":"finish","summary":"Reviewed the saved custom and built-in results."}]);
        cfg["goal_budget"] = json!({"max_model_calls":5,"max_tokens":300000});
    }
    agents
        .create_registered_actor(
            DefinitionId::new(policy, if policy == "planner" { 3 } else { 1 }),
            None,
            cfg,
            16,
            Default::default(),
        )
        .await
        .unwrap()
}
async fn send(agents: &AgentService, actor: AgentId) -> MessageId {
    agents.actors().unwrap().send(AgentMessage {id:MessageId::new(),recipient:actor,sender:None,correlation_id:None,
        kind:"task".into(),payload:json!({"goal":"Use allowed tools","tools":[{"name":"echo","version":1}],"tool_definitions":[],"workspace":"forged"})}).await.unwrap()
}
async fn model_once(worker: &mut Worker) -> ActivityExecution {
    for _ in 0..100 {
        if !worker.advance_work().await.unwrap() {
            break;
        }
    }
    let model = worker.admit().await.unwrap().unwrap();
    assert_eq!(model.handler, DefinitionId::new("agent.model", 1));
    worker
        .complete(model.clone(), worker.execute(model.clone()).await.unwrap())
        .await
        .unwrap();
    model
}
async fn settle(worker: &mut Worker) {
    for _ in 0..500 {
        if !worker.step().await.unwrap() && !worker.has_active_runs().await.unwrap() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("worker did not settle");
}

#[tokio::test]
async fn custom_tool_results_and_builtin_continuations_survive_restart_for_both_actor_policies() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("restart.db");
        let tool = Arc::new(WordCountTool::new().unwrap());
        let (engine, agents) = local_app_with_tools(&db, vec![tool.clone()]).await.unwrap();
        let actor = create(&agents, policy, config()).await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let model = model_once(&mut worker).await;
        let request = model.input.clone();
        assert!(request["schema"].to_string().contains("word_count"));
        assert!(!request["schema"].to_string().contains("echo"));
        let call = worker.admit().await.unwrap().unwrap();
        let result = worker.execute(call.clone()).await.unwrap();
        assert_eq!(result.result(), Some(&json!({"words":3})));
        worker.complete(call.clone(), result.clone()).await.unwrap();
        let saved = json!(engine.status(call.run_id).await.unwrap());
        let history = json!(engine.history(call.run_id).await.unwrap());
        assert_eq!(tool.executions(), 1);
        drop(worker);
        drop(agents);
        drop(engine);
        let fresh = Arc::new(WordCountTool::new().unwrap());
        let (engine, agents) = local_app_with_tools(&db, vec![fresh.clone()])
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert_eq!(json!(engine.status(call.run_id).await.unwrap()), saved);
        worker.complete(call.clone(), result).await.unwrap();
        assert_eq!(json!(engine.history(call.run_id).await.unwrap()), history);
        settle(&mut worker).await;
        assert_eq!(fresh.executions(), 0);
        let saved = engine.status(call.run_id).await.unwrap();
        assert_eq!(saved.record().status, RunStatus::Succeeded);
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == call.activity_id)
                .count(),
            1
        );
        assert_eq!(
            saved
                .activities()
                .iter()
                .find(|a| a.spec.input["definition"]["id"]["name"] == "sum")
                .unwrap()
                .result,
            Some(json!({"sum":42}))
        );
        let inspection = agents
            .actors()
            .unwrap()
            .inspect_task(task, Default::default())
            .await
            .unwrap();
        assert_eq!(
            inspection.summary.visible_usage.admitted_model_calls,
            if policy == "planner" { 5 } else { 3 }
        );
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
    }
}

#[tokio::test]
async fn missing_or_changed_custom_contract_blocks_and_exact_restoration_keeps_saved_intent() {
    for changed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("pins.db");
        let tool = Arc::new(WordCountTool::new().unwrap());
        let (engine, agents) = local_app_with_tools(&db, vec![tool.clone()]).await.unwrap();
        let actor = create(&agents, "tool", config()).await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let run = agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .run_id
            .unwrap();
        let original = engine.status(run).await.unwrap();
        let intent = original.activities().last().unwrap().spec.input.clone();
        let pins = original.record().input.clone();
        let usage = json!(original.record().usage);
        drop(worker);
        drop(agents);
        drop(engine);
        let mut definition = tool.definition();
        definition.description.push_str(" Changed contract.");
        let installed: Vec<Arc<dyn NativeTool>> = if changed {
            vec![Arc::new(WrappedTool {
                definition,
                inner: tool.clone(),
                ledger: None,
            })]
        } else {
            vec![]
        };
        let (engine, agents) = local_app_with_tools(&db, installed).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Blocked
        );
        assert!(engine.resume(run).await.is_err());
        assert_eq!(tool.executions(), 0);
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app_with_tools(&db, vec![tool.clone()]).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        engine.resume(run).await.unwrap();
        let restored = engine.status(run).await.unwrap();
        assert_eq!(restored.activities().last().unwrap().spec.input, intent);
        assert_eq!(restored.record().input, pins);
        assert_eq!(json!(restored.record().usage), usage);
        settle(&mut worker).await;
        assert_eq!(tool.executions(), 1);
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
    }
}

#[tokio::test]
async fn custom_approval_waits_and_declared_recovery_survive_restart_and_denial() {
    for approved in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("approval.db");
        let inner = Arc::new(WordCountTool::new().unwrap());
        let definition =
            ToolDefinition::builder("word_count", 1, RecoveryPolicy::RequiresReconciliation)
                .description("Count words after approval")
                .required_parameter("text", ToolValueType::String, "Text")
                .byte_limits(4096, 256)
                .requires_approval(true)
                .build()
                .unwrap();
        let tool = Arc::new(WrappedTool {
            definition: definition.clone(),
            inner: inner.clone(),
            ledger: None,
        });
        let (engine, agents) = local_app_with_tools(&db, vec![tool.clone()]).await.unwrap();
        let actor = create(&agents, "tool", config()).await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        let view = agents.actors().unwrap().task(task).await.unwrap();
        let run = view.run_id.unwrap();
        let wait = view.wait.unwrap();
        assert_eq!(wait.key, "native_tool_approval");
        assert_eq!(wait.payload["recovery"], "RequiresReconciliation");
        assert_eq!(inner.executions(), 0);
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app_with_tools(&db, vec![tool]).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert_eq!(
            json!(engine.status(run).await.unwrap().record().wait),
            json!(Some(wait.clone()))
        );
        engine
            .signal(run, wait.id, json!({"approved":approved}))
            .await
            .unwrap();
        engine
            .signal(run, wait.id, json!({"approved":approved}))
            .await
            .unwrap();
        if approved {
            let call = worker.admit().await.unwrap().unwrap();
            assert_eq!(call.input["definition"], json!(definition));
            worker
                .complete(
                    call,
                    Outcome::Uncertain("Stopped before a receipt was saved".into()),
                )
                .await
                .unwrap();
            assert_eq!(
                engine.status(run).await.unwrap().record().status,
                RunStatus::Blocked
            );
            assert!(worker.admit().await.unwrap().is_none());
            assert_eq!(inner.executions(), 0);
        } else {
            settle(&mut worker).await;
            assert_eq!(
                agents.actors().unwrap().task(task).await.unwrap().status,
                AgentTaskStatus::Succeeded
            );
            assert_eq!(inner.executions(), 0);
        }
    }
}

#[tokio::test]
#[ignore = "invoked and killed by the custom-tool bootstrap recovery test"]
async fn tool_extension_crash_fixture() {
    let root = PathBuf::from(std::env::var("ACT0R_EXTENSION_CRASH_DIR").unwrap());
    let stage = std::env::var("ACT0R_EXTENSION_CRASH_STAGE").unwrap();
    let inner = Arc::new(WordCountTool::new().unwrap());
    let tool = Arc::new(WrappedTool {
        definition: inner.definition(),
        inner,
        ledger: Some(root.join("calls.log")),
    });
    let (engine, agents) = local_app_with_tools(root.join("crash.db"), vec![tool])
        .await
        .unwrap();
    let actor = create(&agents, "tool", config()).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let call = worker.admit().await.unwrap().unwrap();
    if stage == "saved" {
        worker
            .complete(call.clone(), worker.execute(call.clone()).await.unwrap())
            .await
            .unwrap();
    }
    let saved = engine.status(call.run_id).await.unwrap();
    fs::write(root.join("ready.json"),serde_json::to_vec(&json!({"task":task,"call":call,"next":saved.activities().last().unwrap().spec.input,"usage":saved.record().usage})).unwrap()).unwrap();
    let _keep = (engine, agents, worker);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn killed_custom_hosts_recover_uncommitted_calls_and_reuse_saved_results_through_public_bootstrap()
 {
    for stage in ["admitted", "saved"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "tool_extension_crash_fixture"])
            .env("ACT0R_EXTENSION_CRASH_DIR", root)
            .env("ACT0R_EXTENSION_CRASH_STAGE", stage)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let start = Instant::now();
        while !root.join("ready.json").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "fixture exited {stage}"
            );
            if start.elapsed() > Duration::from_secs(30) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("fixture timeout {stage}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let ready: Value =
            serde_json::from_slice(&fs::read(root.join("ready.json")).unwrap()).unwrap();
        let task: MessageId = serde_json::from_value(ready["task"].clone()).unwrap();
        let original: ActivityExecution = serde_json::from_value(ready["call"].clone()).unwrap();
        let inner = Arc::new(WordCountTool::new().unwrap());
        let tool = Arc::new(WrappedTool {
            definition: inner.definition(),
            inner: inner.clone(),
            ledger: Some(root.join("calls.log")),
        });
        let (engine, agents) = local_app_with_tools(root.join("crash.db"), vec![tool])
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        let restored = engine.status(original.run_id).await.unwrap();
        assert_eq!(
            restored.activities().last().unwrap().spec.input,
            ready["next"]
        );
        assert_eq!(json!(restored.record().usage), ready["usage"]);
        assert_ne!(restored.record().status, RunStatus::Blocked);
        settle(&mut worker).await;
        let saved = engine.status(original.run_id).await.unwrap();
        assert_eq!(saved.activities()[1].id, original.activity_id);
        assert_eq!(saved.activities()[1].result, Some(json!({"words":3})));
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == original.activity_id)
                .count(),
            if stage == "admitted" { 2 } else { 1 }
        );
        assert_eq!(inner.executions(), if stage == "admitted" { 1 } else { 0 });
        assert_eq!(
            fs::read_to_string(root.join("calls.log"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            agents.actors().unwrap().task(task).await.unwrap().status,
            AgentTaskStatus::Succeeded
        );
        assert_eq!(saved.record().usage.admitted_model_calls, 3);
    }
}
