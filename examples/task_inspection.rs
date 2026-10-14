//! Inspect an entire goal across a delegated human wait, restart, and completion.
use act0r::{
    application::TaskInspectionOptions,
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/task-inspection-demo/demo.db".into());
    if let Some(parent) = std::path::Path::new(&database).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&database).await?;
    let assistant=agents.create_registered_actor(DefinitionId::new("answer",1),None,
        json!({"goal":"Review","target":{"provider":"scripted","model":"inspection-demo","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"ask_human","question":"May I continue with the review?"},{"kind":"finish","summary":"Review complete"}]}),10,AgentQueuePolicy::default()).await?;
    let planner=agents.create_registered_actor(DefinitionId::new("planner",3),None,
        json!({"goal":"Complete a reviewed goal","target":{"provider":"scripted","model":"inspection-demo","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "goal_budget":{"max_model_calls":8,"max_tokens":100000},
        "workers":[{"name":"reviewer","agent_id":assistant,"description":"Reviews the goal"}],
        "delegation":{"max_tasks":2,"max_depth":2,"shared_budget":{"max_model_calls":8,"max_tokens":100000}},
        "script":[{"kind":"delegate_task","worker":"reviewer","goal":"Review this goal"},{"kind":"finish","summary":"Goal complete"}]}),10,AgentQueuePolicy::default()).await?;
    let root = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: planner,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"A goal with a human review"}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    while worker.step().await? {}
    let waiting = agents
        .actors()?
        .inspect_task(root, TaskInspectionOptions::default())
        .await?;
    assert_eq!(waiting.nodes.len(), 2);
    assert_eq!(waiting.summary.visible_usage.admitted_model_calls, 2);
    let leaf = waiting.nodes.iter().find(|n| n.wait.is_some()).unwrap();
    let child = leaf.task_id;
    let run = leaf.run_id.unwrap();
    let wait = leaf.wait.as_ref().unwrap().id;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await?;
    let restored = agents
        .actors()?
        .inspect_task(root, TaskInspectionOptions::default())
        .await?;
    assert_eq!(
        serde_json::to_value(&waiting)?,
        serde_json::to_value(&restored)?
    );
    assert_eq!(
        agents
            .actors()?
            .inspect_task(child, TaskInspectionOptions::default())
            .await?
            .root_task_id,
        root
    );
    engine
        .signal(run, wait, json!({"answer":"Yes, continue"}))
        .await?;
    let mut worker = engine.worker().await?;
    while worker.step().await? {}
    let completed = agents
        .actors()?
        .inspect_task(child, TaskInspectionOptions::default())
        .await?;
    assert!(completed.summary.settled);
    assert_eq!(completed.summary.succeeded, 2);
    assert_eq!(completed.summary.visible_usage.admitted_model_calls, 4);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"restart_verified":true,"waiting":waiting,"completed":completed})
        )?
    );
    Ok(())
}
