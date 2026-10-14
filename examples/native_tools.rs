//! Save a native tool result, close the runtime, and resume the saved model continuation.
use act0r::{
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "native-tools.db".into());
    if let Some(parent) = std::path::Path::new(&database)
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&database).await?;
    let config: Value = serde_json::from_str(include_str!("configs/tool-agent-scripted.json"))?;
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            None,
            config,
            10,
            AgentQueuePolicy::default(),
        )
        .await?;
    let message = AgentMessage {
        id: MessageId::new(),
        recipient: actor,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":"Add 19 and 23 using the sum tool."}),
    };
    agents.actors()?.send(message.clone()).await?;
    let mut worker = engine.worker().await?;
    let mut saved = None;
    for _ in 0..100 {
        worker.step().await?;
        let task = agents.actors()?.task(message.id).await?;
        if let Some(run) = task.run_id {
            let state = engine.status(run).await?;
            if let Some(tool) = state.activities().iter().find(|a| {
                a.spec.handler == DefinitionId::new("native.tool", 1)
                    && a.status == ActivityStatus::Succeeded
            }) {
                assert_eq!(tool.result, Some(json!({"sum":42})));
                saved = Some((
                    run,
                    tool.id,
                    state.activities().last().unwrap().spec.input.clone(),
                ));
                break;
            }
        }
    }
    let (run, tool, next_request) = saved.ok_or("tool result was not saved")?;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await?;
    let mut worker = engine.worker().await?;
    assert_eq!(
        engine
            .status(run)
            .await?
            .activities()
            .last()
            .unwrap()
            .spec
            .input,
        next_request
    );
    for _ in 0..100 {
        if !worker.step().await? && !worker.has_active_runs().await? {
            break;
        }
    }
    let final_run = engine.status(run).await?;
    assert_eq!(final_run.record().status, RunStatus::Succeeded);
    let attempts = final_run
        .attempts()
        .iter()
        .filter(|a| a.activity_id == tool)
        .count();
    assert_eq!(attempts, 1);
    let task = agents.actors()?.task(message.id).await?;
    assert_eq!(task.status, AgentTaskStatus::Succeeded);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"restart_verified":true,"saved_tool_attempts":attempts,"tools":agents.tool_definitions()?,"task":task})
        )?
    );
    Ok(())
}
