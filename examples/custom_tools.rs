//! Register a custom tool, preserve its saved result, and use a built-in after restart.
#[path = "custom_tools/word_count.rs"]
mod word_count;
use act0r::{
    bootstrap::local_app_with_tools,
    domain::{actors::*, *},
};
use serde_json::json;
use std::{fs, path::PathBuf, sync::Arc};
use word_count::WordCountTool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/custom-tools-demo/demo.db".into()),
    );
    if let Some(parent) = database.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let tool = Arc::new(WordCountTool::new()?);
    let (engine, agents) = local_app_with_tools(&database, vec![tool.clone()]).await?;
    let actor = agents.create_registered_actor(DefinitionId::new("tool",1),None,json!({
        "goal":"Count words, then add numbers","target":{"provider":"scripted","model":"custom","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
        "tools":[{"name":"word_count","version":1},{"name":"sum","version":1}],
        "script":[{"kind":"call_tool","tool":"word_count","arguments":{"text":"Durable agents count saved words. 🦀"}},
            {"kind":"call_tool","tool":"sum","arguments":{"left":19,"right":23}},
            {"kind":"finish","summary":"Counted six words and added 19 and 23."}]
    }),10,Default::default()).await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Use the allowed custom and built-in tools."}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let mut checkpoint = None;
    for _ in 0..100 {
        worker.step().await?;
        if let Some(run) = agents.actors()?.task(task).await?.run_id {
            let saved = engine.status(run).await?;
            if let Some(activity) = saved.activities().iter().find(|a| {
                a.spec.input["definition"]["id"]["name"] == "word_count" && a.result.is_some()
            }) {
                assert_eq!(activity.result, Some(json!({"words":6})));
                checkpoint = Some((
                    run,
                    activity.id,
                    saved
                        .activities()
                        .last()
                        .ok_or("missing continuation")?
                        .spec
                        .input
                        .clone(),
                    json!(saved.record().usage),
                ));
                break;
            }
        }
    }
    let (run, activity, next, usage) = checkpoint.ok_or("custom tool result was not saved")?;
    assert_eq!(tool.executions(), 1);
    drop(worker);
    drop(agents);
    drop(engine);
    let restored_tool = Arc::new(WordCountTool::new()?);
    let (engine, agents) = local_app_with_tools(&database, vec![restored_tool.clone()]).await?;
    let mut worker = engine.worker().await?;
    let restored = engine.status(run).await?;
    assert_eq!(
        restored
            .activities()
            .last()
            .ok_or("missing saved continuation")?
            .spec
            .input,
        next
    );
    assert_eq!(json!(restored.record().usage), usage);
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
    assert_eq!(restored_tool.executions(), 0);
    let attempts = saved
        .attempts()
        .iter()
        .filter(|a| a.activity_id == activity)
        .count();
    assert_eq!(attempts, 1);
    let sum = saved
        .activities()
        .iter()
        .find(|a| a.spec.input["definition"]["id"]["name"] == "sum")
        .ok_or("built-in sum did not run")?;
    assert_eq!(sum.result, Some(json!({"sum":42})));
    let installed = agents.tool_definitions()?;
    assert!(
        installed
            .iter()
            .any(|d| d.id == DefinitionId::new("word_count", 1))
    );
    assert!(
        installed
            .iter()
            .any(|d| d.id == DefinitionId::new("search_text", 1))
    );
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"restart_verified":true,"words":6,"sum":42,
        "custom_executions":tool.executions(),"custom_executions_after_restart":restored_tool.executions(),
        "saved_tool_attempts":attempts,"model_calls":saved.record().usage.admitted_model_calls,"task":task,"run":run,
        "installed_tools":installed.len(),"status":saved.record().status}))?
    );
    Ok(())
}
