//! A future task survives downtime and uses saved actor memory; no credentials required.
use act0r::{
    Worker,
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::json;

async fn drain(worker: &mut Worker) -> Result<(), act0r::Error> {
    while worker.has_active_runs().await? {
        if !worker.step().await? {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
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
        .unwrap_or_else(|| ".build/actor-reminders-demo/demo.db".into());
    if let Some(parent) = std::path::Path::new(&database).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&database).await?;
    let actor = agents.create_registered_actor(
        DefinitionId::new("answer", 1), None,
        json!({"goal":"Answer the assigned question.","target":{"provider":"scripted","model":"reminders-demo","endpoint":""},
            "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,
            "memory":{},"script":[{"kind":"finish","summary":"The preferred color is blue."}]}),
        10, AgentQueuePolicy::default(),
    ).await?;
    agents
        .actors()?
        .send(task(actor, "Remember my preferred color: blue."))
        .await?;
    let mut worker = engine.worker().await?;
    drain(&mut worker).await?;
    let memory = agents.actors()?.memory(actor).await?.unwrap();
    let message = task(actor, "What color did I ask you to remember?");
    let deadline = TimerDeadline::After { milliseconds: 100 };
    let scheduled = agents
        .actors()?
        .schedule_task(message.clone(), deadline.clone())
        .await?;
    assert_eq!(scheduled.status, ReminderStatus::Scheduled);
    assert_eq!(agents.actors()?.queue(actor).await?.outstanding, 0);
    drop(worker);
    drop(agents);
    drop(engine);

    // Downtime is longer than the delay. The saved UTC deadline remains authoritative.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let (engine, agents) = local_app(&database).await?;
    assert_eq!(agents.actors()?.reminder(message.id).await?, scheduled);
    let mut worker = engine.worker().await?;
    drain(&mut worker).await?;
    let published = agents.actors()?.reminder(message.id).await?;
    assert_eq!(published.status, ReminderStatus::Published);
    assert_eq!(published.due_at, scheduled.due_at);
    assert_eq!(
        agents
            .actors()?
            .schedule_task(message.clone(), deadline)
            .await?,
        published
    );
    let handle = agents.actors()?.task(message.id).await?;
    assert_eq!(handle.status, AgentTaskStatus::Succeeded);
    let run = engine.status(handle.run_id.unwrap()).await?;
    assert_eq!(
        run.record().input["input"]["actor_memory"],
        json!(memory.snapshot(actor))
    );
    assert_eq!(agents.actors()?.tasks(actor).await?.len(), 2);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "agent_id":actor,"restart_verified":true,"reminder":published,"task":handle,
            "memory":agents.actors()?.memory(actor).await?
        }))?
    );
    Ok(())
}
