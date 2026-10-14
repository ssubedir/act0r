//! Two tasks on one actor, separated by a database reopen; no credentials required.
use act0r::{
    Worker,
    bootstrap::local_app,
    domain::{actors::*, agent::ModelRequest, *},
};
use serde_json::{Value, json};

async fn drain(worker: &mut Worker) -> Result<(), act0r::Error> {
    while worker.step().await? {}
    Ok(())
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
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/actor-memory-demo/demo.db".into());
    if let Some(parent) = std::path::Path::new(&database).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&database).await?;
    let config = json!({
        "goal":"Answer the assigned question.","target":{"provider":"scripted","model":"memory-demo","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,
        "memory":{"max_entries":8,"max_bytes":8192},"script":[{"kind":"finish","summary":"The preferred color is blue."}]
    });
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            config,
            10,
            AgentQueuePolicy::default(),
        )
        .await?;
    agents
        .actors()?
        .send(task(actor, "Remember my preferred color: blue."))
        .await?;
    let mut worker = engine.worker().await?;
    drain(&mut worker).await?;
    let remembered = agents.actors()?.memory(actor).await?.unwrap();
    drop(worker);
    drop(agents);
    drop(engine);

    let (engine, agents) = local_app(&database).await?;
    assert_eq!(agents.actors()?.memory(actor).await?.unwrap(), remembered);
    let second = task(actor, "What color did I ask you to remember?");
    agents.actors()?.send(second.clone()).await?;
    let mut worker = engine.worker().await?;
    drain(&mut worker).await?;
    let handle = agents.actors()?.task(second.id).await?;
    let run = engine.status(handle.run_id.unwrap()).await?;
    let model: ModelRequest = serde_json::from_value(run.activities()[0].spec.input.clone())?;
    let prompt: Value = serde_json::from_str(&model.prompt)?;
    assert_eq!(prompt["actor_memory"], json!(remembered.snapshot(actor)));
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"agent_id":actor,"restart_verified":true,
        "second_task_context":prompt,"memory":agents.actors()?.memory(actor).await?,"result":handle.reply}))?
    );
    Ok(())
}
