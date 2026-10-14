//! Read through a real MCP filesystem server, then reuse the saved result on restart.
use act0r::{
    adapters::mcp::McpFilesystemConfig,
    bootstrap::local_app_with_mcp,
    domain::{actors::*, *},
};
use serde_json::json;
use std::{fs, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::args_os()
        .nth(1)
        .ok_or("pass an MCP filesystem configuration JSON path")?;
    let config: McpFilesystemConfig = serde_json::from_slice(&fs::read(config_path)?)?;
    let root = PathBuf::from(".build/mcp-reads-demo").join(uuid::Uuid::new_v4().to_string());
    let source = root.join("source");
    fs::create_dir_all(source.join("src"))?;
    fs::write(
        source.join("README.md"),
        "Evidence read by the MCP filesystem server.\r\n😀\n",
    )?;
    fs::write(source.join("src/example.txt"), "Durable actor evidence.\n")?;
    fs::write(source.join(".env"), "excluded source file")?;
    let database = root.join("demo.db");
    let (engine, agents) = local_app_with_mcp(&database, config.clone()).await?;
    let actor = agents.create_registered_actor(DefinitionId::new("tool",1),Some(source),json!({
        "goal":"Read project files through MCP", "target":{"provider":"scripted","model":"mcp-demo","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
        "tools":[{"name":"read_files","version":1}],
        "script":[{"kind":"call_tool","tool":"read_files","arguments":{"paths":["README.md","missing.md","src/example.txt",".env"]}},
            {"kind":"finish","summary":"Used the saved MCP file evidence."}]
    }),10,Default::default()).await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            sender: None,
            recipient: actor,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Read project files through MCP"}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let mut checkpoint = None;
    for _ in 0..100 {
        worker.step().await?;
        if let Some(run) = agents.actors()?.task(task).await?.run_id {
            let saved = engine.status(run).await?;
            if let Some(activity) = saved.activities().iter().find(|a| {
                a.spec.input["definition"]["id"]["name"] == "read_files" && a.result.is_some()
            }) {
                let result = activity.result.clone().ok_or("missing read result")?;
                assert_eq!(
                    result["files"][0]["text"],
                    "Evidence read by the MCP filesystem server.\r\n😀\n"
                );
                assert_eq!(result["files"][1]["error"]["code"], "file_not_found");
                assert_eq!(result["files"][2]["text"], "Durable actor evidence.\n");
                assert_eq!(result["files"][3]["error"]["code"], "path_refused");
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
    let (run, activity, result, next) = checkpoint.ok_or("MCP read did not complete")?;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app_with_mcp(&database, config).await?;
    let saved = engine.status(run).await?;
    assert_eq!(
        saved
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
            .ok_or("missing saved MCP activity")?
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
        serde_json::to_string_pretty(&json!({"status":"succeeded","restart_verified":true,
        "saved_tool_attempts":1,"result":result,"database":database}))?
    );
    Ok(())
}
