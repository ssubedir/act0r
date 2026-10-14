//! A planner finishes, schedules itself, and recovers with memory after downtime.
use act0r::{
    bootstrap::local_app,
    domain::{actors::*, follow_up::*, *},
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/planner-follow-ups-demo/demo.db".into());
    if let Some(parent) = std::path::Path::new(&database).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&database).await?;
    let worker_actor=agents.create_registered_actor(DefinitionId::new("answer",1),None,
        json!({"goal":"Assist the planner","target":{"provider":"scripted","model":"follow-up-demo","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"script":[{"kind":"finish","summary":"Blue"}]}),10,AgentQueuePolicy::default()).await?;
    let actor=agents.create_registered_actor(DefinitionId::new("planner",3),None,
        json!({"goal":"Remember the result and schedule a review","target":{"provider":"scripted","model":"follow-up-demo","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"memory":{},
        "workers":[{"name":"assistant","agent_id":worker_actor,"description":"A permitted assistant"}],
        "delegation":{"max_tasks":2,"max_depth":2},"follow_up":{"max_follow_ups":1,"min_delay_ms":1,"max_delay_ms":60000},
        "script":[{"kind":"schedule_follow_up","summary":"The preferred color is blue.","goal":"Review the saved color","after_ms":1000}]}),
        10,AgentQueuePolicy {task_capacity:1}).await?;
    let root = AgentMessage {
        id: MessageId::new(),
        recipient: actor,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":"Remember my preferred color: blue."}),
    };
    agents.actors()?.send(root.clone()).await?;
    let mut worker = engine.worker().await?;
    while agents.actors()?.memory(actor).await?.unwrap().revision == 0 {
        worker.step().await?;
    }
    let parent = agents.actors()?.task(root.id).await?;
    let intent = parent.follow_up.clone().unwrap();
    assert_eq!(intent.status, FollowUpStatus::Registered);
    let memory = agents.actors()?.memory(actor).await?.unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let (engine, agents) = local_app(&database).await?;
    assert_eq!(
        agents.actors()?.reminder(intent.reminder_id).await?.due_at,
        intent.due_at
    );
    let mut worker = engine.worker().await?;
    while worker.has_active_runs().await? {
        if !worker.step().await? {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    let child = agents.actors()?.task(intent.reminder_id).await?;
    assert_eq!(child.status, AgentTaskStatus::Succeeded);
    let run = engine.status(child.run_id.unwrap()).await?;
    assert_eq!(
        run.record().input["input"]["actor_memory"],
        json!(memory.snapshot(actor))
    );
    assert_eq!(run.record().follow_up_context.as_ref().unwrap().depth, 1);
    // The script deliberately asks again; the runtime refuses another link.
    assert_eq!(
        child.follow_up.as_ref().unwrap().error.as_deref(),
        Some("chain_limit")
    );
    assert_eq!(agents.actors()?.reminders(actor).await?.len(), 1);
    assert_eq!(agents.actors()?.tasks(actor).await?.len(), 2);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"agent_id":actor,"restart_verified":true,"parent":parent,"follow_up_task":child,"memory":agents.actors()?.memory(actor).await?})
        )?
    );
    Ok(())
}
