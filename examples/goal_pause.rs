//! Hold a delegated human wait and future follow-up across downtime, then resume the goal.
use act0r::{
    Error, Worker,
    application::{AgentService, TaskInspectionOptions},
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::{Value, json};
async fn create(agents: &AgentService, name: &str, config: Value) -> Result<AgentId, Error> {
    agents
        .create_registered_actor(
            agents.latest_adapter(name)?,
            None,
            config,
            10,
            AgentQueuePolicy::default(),
        )
        .await
}
async fn send(agents: &AgentService, actor: AgentId, goal: &str) -> Result<MessageId, Error> {
    agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":goal}),
        })
        .await
}
async fn settle(worker: &mut Worker) -> Result<(), Error> {
    for _ in 0..256 {
        if !worker.step().await? {
            return Ok(());
        }
    }
    Err(Error::Unavailable("demo did not settle".into()))
}
fn config(script: Value) -> Value {
    json!({"goal":"Help with this goal","target":{"provider":"scripted","model":"pause-demo","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,"script":script})
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/goal-pause-demo/demo.db".into());
    if let Some(parent) = std::path::Path::new(&database).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&database).await?;
    let human=create(&agents,"answer",config(json!([
        {"kind":"ask_human","question":"May I continue?"},{"kind":"finish","summary":"Review complete"}
    ]))).await?;
    let assistant = create(
        &agents,
        "answer",
        config(json!([{"kind":"finish","summary":"Independent result"}])),
    )
    .await?;
    let mut scheduler_config = config(json!([
        {"kind":"schedule_follow_up","summary":"Progress saved","goal":"Review later","after_ms":1000}
    ]));
    scheduler_config["workers"] =
        json!([{"name":"reviewer","agent_id":human,"description":"Reviews"}]);
    scheduler_config["delegation"] = json!({"max_tasks":2,"max_depth":2});
    scheduler_config["follow_up"] =
        json!({"max_follow_ups":1,"min_delay_ms":1,"max_delay_ms":1000});
    let scheduler = create(&agents, "planner", scheduler_config).await?;
    let mut planner_config = config(json!([
        {"kind":"delegate_tasks","tasks":[{"worker":"human","goal":"Review now"},{"worker":"scheduler","goal":"Schedule review"}]},
        {"kind":"finish","summary":"Combined result"}
    ]));
    planner_config["workers"] = json!([
        {"name":"human","agent_id":human,"description":"Human review"},
        {"name":"scheduler","agent_id":scheduler,"description":"Future review"}
    ]);
    planner_config["delegation"] = json!({"max_tasks":2,"max_depth":2,"max_parallel_tasks":2});
    planner_config["goal_budget"] = json!({"max_model_calls":8,"max_tokens":100000});
    let planner = create(&agents, "planner", planner_config).await?;
    let root = send(&agents, planner, "Review now and later").await?;
    let mut worker = engine.worker().await?;
    settle(&mut worker).await?;
    let waiting = agents
        .actors()?
        .inspect_task(root, TaskInspectionOptions::default())
        .await?;
    assert_eq!(waiting.summary.visible_usage.admitted_model_calls, 3);
    let leaf = waiting.nodes.iter().find(|n| n.wait.is_some()).unwrap();
    let child = leaf.task_id;
    let child_run = leaf.run_id.unwrap();
    let wait = leaf.wait.as_ref().unwrap().id;
    let reminder = waiting
        .nodes
        .iter()
        .find(|n| n.status == "scheduled")
        .unwrap()
        .task_id;
    let due = agents.actors()?.reminder(reminder).await?.due_at;
    let paused = agents.actors()?.pause_goal(reminder).await?;
    drop(worker);
    drop(agents);
    drop(engine);
    // This sleep simulates worker downtime; the actual deadline remains in SQLite.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let (engine, agents) = local_app(&database).await?;
    let mut worker = engine.worker().await?;
    assert_eq!(agents.actors()?.pause_goal(child).await?, paused);
    assert_eq!(agents.actors()?.reminder(reminder).await?.due_at, due);
    engine
        .signal(child_run, wait, json!({"answer":"Continue"}))
        .await?;
    let other = send(
        &agents,
        assistant,
        "Independent work while the other goal is held",
    )
    .await?;
    settle(&mut worker).await?;
    let held = agents
        .actors()?
        .inspect_task(root, TaskInspectionOptions::default())
        .await?;
    assert_eq!(held.summary.visible_usage.admitted_model_calls, 3);
    assert!(agents.actors()?.task(reminder).await.is_err());
    assert!(!worker.has_active_runs().await?);
    let independent = agents
        .actors()?
        .inspect_task(other, TaskInspectionOptions::default())
        .await?;
    assert_eq!(independent.summary.succeeded, 1);
    let resumed = agents.actors()?.resume_goal(child).await?;
    assert!(!resumed.paused);
    assert_eq!(resumed.pause, paused.pause);
    settle(&mut worker).await?;
    let completed = agents
        .actors()?
        .inspect_task(root, TaskInspectionOptions::default())
        .await?;
    assert!(completed.summary.settled);
    assert_eq!(completed.summary.succeeded, 4);
    assert_eq!(completed.summary.visible_usage.admitted_model_calls, 6);
    assert!(completed.nodes.iter().all(|n| !n.execution_held));
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"restart_verified":true,"overdue_reminder_held":true,
        "paused":paused,"held":held,"resumed":resumed,"completed":completed,"independent":independent})
        )?
    );
    Ok(())
}
