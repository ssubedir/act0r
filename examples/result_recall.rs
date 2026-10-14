//! A deterministic planner recalls evicted real test output across two restarts.
use act0r::{
    Engine, Error, Worker,
    adapters::{
        agent_executor::AgentExecutor,
        agents::{AnswerAgentAdapter, PlannerAgentAdapter},
        demo::DemoExecutor,
        file_lock::FileOwnership,
        native_tools::agent_tool_registry,
        sqlite::SqliteStore,
        system_clock::SystemClock,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{ActorService, AgentRegistry, AgentService, ports::*},
    domain::{actors::*, agent::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
type DemoResult<T> = Result<T, Box<dyn std::error::Error>>;

struct RecallModel;
impl ModelClient for RecallModel {
    fn generate(&self, request: ModelRequest, _: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            let call = |tool: &str, arguments: Value| AgentAction::CallTool {
                tool: tool.into(),
                arguments,
            };
            let action = if request.target.model == "reviewer" {
                assert!(request.prompt.contains("fixed_contract"));
                AgentAction::Finish {
                    summary: "Reviewed the recalled failure; no new tests ran.".into(),
                }
            } else {
                let selection = request.context.as_ref().expect("bounded context");
                assert!(selection.input_bytes <= 16000);
                assert!(selection.included_observations <= 1);
                let prompt: Value = serde_json::from_str(&request.prompt)?;
                let result = prompt["observations"]
                    .as_array()
                    .and_then(|v| v.last())
                    .map(|v| v["result"].clone())
                    .unwrap_or(Value::Null);
                match request.turn {
                    0 => call("run_tests", json!({})),
                    1..=6 => call(
                        "echo",
                        json!({"text":format!("later observation {}",request.turn)}),
                    ),
                    7 => {
                        assert!(!request.prompt.contains("fixed_contract"));
                        call("list_results", json!({"tool":"run_tests","limit":1}))
                    }
                    8 => {
                        assert_eq!(result["results"][0]["ok"], false);
                        call(
                            "read_result",
                            json!({"activity_id":result["results"][0]["activity_id"],"max_bytes":4096}),
                        )
                    }
                    9 => {
                        assert_eq!(result["complete"], true);
                        let original: Value = serde_json::from_str(
                            result["text"].as_str().expect("saved JSON page"),
                        )?;
                        assert_eq!(original["ok"], false);
                        assert_eq!(original["exit_code"], 101);
                        AgentAction::DelegateTask {
                            writable_paths: None,
                            worker: "reviewer".into(),
                            goal: format!("Review historical test failure: {}", original["stdout"]),
                        }
                    }
                    10 => AgentAction::Finish {
                        summary: "Recalled and reviewed historical failure; no new test execution."
                            .into(),
                    },
                    _ => return Err(Error::Unavailable("unexpected demo model turn".into())),
                }
            };
            Ok(ModelReply {
                decision: Ok(ModelDecision {
                    action,
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

async fn open(db: &Path) -> DemoResult<(Engine, AgentService)> {
    let store = Arc::new(SqliteStore::open(db).await?);
    let root = store
        .database_path()
        .parent()
        .ok_or("missing database parent")?
        .join(".act0r/workspaces");
    let workspaces = Arc::new(LocalWorkspaces::new(root.clone()));
    let local = Arc::new(LocalTools::new(root));
    let tools = Arc::new(agent_tool_registry(
        local.clone(),
        local.clone(),
        local.clone(),
        store.clone(),
    )?);
    let registry = Arc::new(AgentRegistry::new(vec![
        Arc::new(AnswerAgentAdapter),
        Arc::new(PlannerAgentAdapter::new(tools.clone()).with_workspaces(workspaces.clone())),
    ])?);
    let engine = Engine::new(
        store.clone(),
        Arc::new(
            AgentExecutor::new(Arc::new(RecallModel), local, Arc::new(DemoExecutor))
                .with_native_tools(tools.clone()),
        ),
        Arc::new(SystemClock),
        Arc::new(FileOwnership::new(db)),
        registry.workflows(),
    )?;
    let actors = Arc::new(
        ActorService::new(
            engine.clone(),
            store.clone(),
            vec![Arc::new(RegisteredAgentDefinition)],
        )?
        .with_agent_registry(registry.clone())
        .with_inspection_reader(store.clone())
        .with_goal_control(store),
    );
    let engine = engine.with_coordinator(actors.clone());
    let agents = AgentService::new(engine.clone(), workspaces)
        .with_actors(actors)
        .with_agent_registry(registry)
        .with_tool_registry(tools);
    Ok((engine, agents))
}

async fn through_tool(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
    name: &str,
    status: ActivityStatus,
) -> DemoResult<RunId> {
    for _ in 0..100 {
        worker.step().await?;
        let view = agents.actors()?.task(task).await?;
        if let Some(wait) = view.wait {
            assert_eq!(wait.key, "native_tool_approval");
            // This example owns the fixture and approves its fixed test command.
            engine
                .signal(
                    view.run_id.ok_or("missing Run")?,
                    wait.id,
                    json!({"approved":true}),
                )
                .await?;
        }
        if let Some(run) = view.run_id {
            let saved = engine.status(run).await?;
            if saved
                .activities()
                .iter()
                .any(|a| a.spec.input["definition"]["id"]["name"] == name && a.status == status)
            {
                return Ok(run);
            }
            if !saved.record().status.is_unfinished() {
                return Err(format!("demo ended early: {:?}", saved.record().error).into());
            }
        }
    }
    Err("demo boundary was not reached".into())
}

#[tokio::main]
async fn main() -> DemoResult<()> {
    let db = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/result-recall-demo/demo.db".into()),
    );
    let parent = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let source = parent.join(format!("recall-source-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(source.join("src"))?;
    fs::create_dir(source.join("tests"))?;
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"recall_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )?;
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"recall_fixture\"\nversion = \"0.1.0\"\n",
    )?;
    fs::write(source.join("src/lib.rs"), "pub fn value() -> u32 { 0 }\n")?;
    fs::write(
        source.join("tests/contract.rs"),
        r#"#[test]
fn fixed_contract() {
    use std::io::Write;
    let mut ledger = std::fs::OpenOptions::new().create(true).append(true).open(".act0r-tools/test-runs").unwrap();
    writeln!(ledger, "executed").unwrap();
    assert_eq!(recall_fixture::value(), 2);
}
"#,
    )?;
    let (engine, agents) = open(&db).await?;
    let reviewer = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
        "goal":"Review historical evidence","target":{"provider":"scripted","model":"reviewer","endpoint":""},
        "max_model_calls":1,"max_tokens":100000,"max_output_tokens":512
    }),4,Default::default()).await?;
    let actor = agents.create_registered_actor(DefinitionId::new("planner",3),Some(source.clone()),json!({
        "goal":"Recall and review earlier test output","target":{"provider":"scripted","model":"recall","endpoint":""},
        "max_model_calls":11,"max_tokens":1000000,"max_output_tokens":512,
        "context":{"max_input_bytes":16000,"max_observations":1},
        "goal_budget":{"max_model_calls":12,"max_tokens":1000000},
        "workers":[{"name":"reviewer","agent_id":reviewer,"description":"Review saved evidence"}],
        "delegation":{"max_tasks":1,"max_depth":1},
        "tools":[{"name":"run_tests","version":1},{"name":"echo","version":1},{"name":"list_results","version":1},{"name":"read_result","version":1}],
        "test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000}
    }),40,Default::default()).await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Recall and review earlier test output."}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let run = through_tool(
        &engine,
        &agents,
        &mut worker,
        task,
        "list_results",
        ActivityStatus::Queued,
    )
    .await?;
    let saved = engine.status(run).await?;
    assert!(
        !saved.record().state["state"]["context"]["observations"]
            .to_string()
            .contains("fixed_contract")
    );
    let original = saved
        .activities()
        .iter()
        .find(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
        .ok_or("missing tests")?;
    let source_activity = original.id;
    let result = original.result.clone().ok_or("missing test result")?;
    let workspace = PathBuf::from(
        original.spec.input["workspace"]
            .as_str()
            .ok_or("missing workspace")?,
    );
    // Current files now differ; recall must continue to return the historical failure.
    fs::write(
        workspace.join("src/lib.rs"),
        "pub fn value() -> u32 { 2 }\n",
    )?;
    let pending = saved.activities().last().unwrap().spec.input.clone();
    let usage = json!(saved.record().usage);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = open(&db).await?;
    let restored = engine.status(run).await?;
    assert_eq!(restored.activities().last().unwrap().spec.input, pending);
    assert_eq!(json!(restored.record().usage), usage);
    let mut worker = engine.worker().await?;
    through_tool(
        &engine,
        &agents,
        &mut worker,
        task,
        "read_result",
        ActivityStatus::Succeeded,
    )
    .await?;
    let saved = engine.status(run).await?;
    let recalled = saved
        .activities()
        .iter()
        .find(|a| a.spec.input["definition"]["id"]["name"] == "read_result")
        .unwrap();
    let page = recalled.result.as_ref().ok_or("missing recalled output")?;
    assert_eq!(
        serde_json::from_str::<Value>(page["text"].as_str().unwrap())?,
        result
    );
    assert_eq!(page["source"]["activity_id"], json!(source_activity));
    let pending = saved.activities().last().unwrap().spec.input.clone();
    let usage = json!(saved.record().usage);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = open(&db).await?;
    let restored = engine.status(run).await?;
    assert_eq!(restored.activities().last().unwrap().spec.input, pending);
    assert_eq!(json!(restored.record().usage), usage);
    let mut worker = engine.worker().await?;
    for _ in 0..100 {
        if !worker.step().await? && !worker.has_active_runs().await? {
            break;
        }
    }
    let view = agents.actors()?.task(task).await?;
    assert_eq!(view.status, AgentTaskStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(workspace.join(".act0r-tools/test-runs"))?
            .lines()
            .count(),
        1
    );
    let saved = engine.status(run).await?;
    assert_eq!(
        saved
            .activities()
            .iter()
            .filter(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
            .count(),
        1
    );
    assert_eq!(
        saved
            .attempts()
            .iter()
            .filter(|a| a.activity_id == source_activity)
            .count(),
        1
    );
    assert_eq!(
        fs::read_to_string(source.join("src/lib.rs"))?,
        "pub fn value() -> u32 { 0 }\n"
    );
    let tree = agents
        .actors()?
        .inspect_task(task, Default::default())
        .await?;
    assert_eq!(tree.summary.visible_usage.admitted_model_calls, 12);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"task":task,"run":run,"status":view.status,
        "restart_count":2,"test_executions":1,"recalled_activity":source_activity,"recalled_exit_code":result["exit_code"],
        "model_calls":12,"max_input_bytes":16000,"max_observations":1,"exact_saved_result":true,"review_tasks":1}))?
    );
    Ok(())
}
