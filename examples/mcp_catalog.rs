//! Enable get_file_info in host JSON, save its result, and resume without calling it again.
use act0r::{
    adapters::mcp::McpConfig,
    bootstrap::local_app_with_mcp_tools,
    domain::{actors::*, *},
};
use serde_json::json;
use std::{fs, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::args_os()
        .nth(1)
        .ok_or("pass a host MCP configuration JSON path enabling get_file_info")?;
    let config: McpConfig = serde_json::from_slice(&fs::read(config_path)?)?;
    let root = PathBuf::from(".build/mcp-catalog-demo").join(uuid::Uuid::new_v4().to_string());
    let source = root.join("source");
    fs::create_dir_all(&source)?;
    fs::write(
        source.join("README.md"),
        "Metadata obtained from a configured MCP tool.\n",
    )?;
    let original_size = fs::metadata(source.join("README.md"))?.len();
    let database = root.join("demo.db");
    let (engine, agents) = local_app_with_mcp_tools(&database, config.clone()).await?;
    let tools = agents.tool_definitions()?;
    let tool = tools
        .iter()
        .find(|tool| tool.external.is_some())
        .ok_or("configure a read-only get_file_info tool")?
        .id
        .clone();
    let actor = agents.create_registered_actor(DefinitionId::new("tool",1),Some(source),json!({
        "goal":"Inspect project metadata", "target":{"provider":"scripted","model":"mcp-catalog-demo","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,"tools":[],
        "script":[{"kind":"call_tool","tool":tool.name,"arguments":{"path":"README.md"}},
            {"kind":"finish","summary":"Used the saved metadata."}]
    }),10,Default::default()).await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            sender: None,
            recipient: actor,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Inspect project metadata"}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let mut checkpoint = None;
    for _ in 0..100 {
        worker.step().await?;
        if let Some(run) = agents.actors()?.task(task).await?.run_id {
            let saved = engine.status(run).await?;
            if let Some(activity) = saved
                .activities()
                .iter()
                .find(|a| a.spec.input["definition"]["id"] == json!(tool) && a.result.is_some())
            {
                let result = activity.result.clone().ok_or("missing tool result")?;
                assert_eq!(result["ok"], true);
                let text = result["content"][0]["text"]
                    .as_str()
                    .ok_or("metadata server returned no text")?;
                assert!(
                    text.lines()
                        .any(|line| line == format!("size: {original_size}"))
                );
                let workspace = activity.spec.input["workspace"]
                    .as_str()
                    .ok_or("missing managed workspace")?;
                fs::write(
                    PathBuf::from(workspace).join("README.md"),
                    "Changed after the committed metadata read.",
                )?;
                checkpoint = Some((
                    run,
                    activity.id,
                    result,
                    saved
                        .activities()
                        .last()
                        .ok_or("missing continuation")?
                        .spec
                        .input
                        .clone(),
                ));
                break;
            }
        }
    }
    let (run, activity, result, next) = checkpoint.ok_or("configured MCP tool did not complete")?;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app_with_mcp_tools(&database, config).await?;
    assert_eq!(
        engine
            .status(run)
            .await?
            .activities()
            .last()
            .ok_or("missing saved continuation")?
            .spec
            .input,
        next
    );
    let mut worker = engine.worker().await?;
    for _ in 0..100 {
        if !worker.step().await? && !worker.has_active_runs().await? {
            break;
        }
    }
    let saved = engine.status(run).await?;
    assert_eq!(saved.record().status, RunStatus::Succeeded);
    assert_eq!(
        agents.actors()?.task(task).await?.status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(
        saved
            .activities()
            .iter()
            .find(|a| a.id == activity)
            .ok_or("missing saved metadata")?
            .result
            .as_ref(),
        Some(&result)
    );
    assert_eq!(
        saved
            .attempts()
            .iter()
            .filter(|a| a.activity_id == activity)
            .count(),
        1
    );
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"tool":tool,"status":"succeeded","restart_verified":true,
        "saved_tool_attempts":1,"original_size":original_size,"result":result,"database":database})
        )?
    );
    Ok(())
}
