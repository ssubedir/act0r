//! Cancel a delegated human wait and future follow-up, then restart without reviving either.
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
    json!({"goal":"Help with this goal", "target":{"provider":"scripted","model":"cancellation-demo","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,"script":script})
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/goal-cancellation-demo/demo.db".into());
    if let Some(parent) = std::path::Path::new(&database).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&database).await?;
    let human = create(&agents, "answer", config(json!([
        {"kind":"ask_human","question":"May I continue?"}, {"kind":"finish","summary":"Review complete"}
    ]))).await?;
    let mut scheduler_config = config(json!([
        {"kind":"schedule_follow_up","summary":"Progress saved","goal":"Review later","after_ms":3600000}
    ]));
    scheduler_config["workers"] =
        json!([{"name":"reviewer","agent_id":human,"description":"Reviews"}]);
    scheduler_config["delegation"] = json!({"max_tasks":2,"max_depth":2});
    scheduler_config["follow_up"] =
        json!({"max_follow_ups":1,"min_delay_ms":1,"max_delay_ms":3600000});
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
    assert_eq!(waiting.nodes.len(), 4);
    assert_eq!(waiting.summary.visible_usage.admitted_model_calls, 3);
    assert!(waiting.nodes.iter().any(|n| n.wait.is_some()));
    let reminder = waiting
        .nodes
        .iter()
        .find(|n| n.status == "scheduled")
        .unwrap()
        .task_id;
    let completed_run = waiting
        .nodes
        .iter()
        .find(|n| n.status == "succeeded")
        .unwrap()
        .run_id
        .unwrap();
    let saved_result = serde_json::to_value(engine.status(completed_run).await?)?;
    // This separate submission shares the human actor, but has independent ownership.
    let other = send(&agents, human, "An independent review").await?;
    let cancellation = agents.actors()?.cancel_goal(reminder).await?;
    assert_eq!(cancellation.receipt.root_task_id, root);
    drop(worker);
    drop(agents);
    drop(engine);

    let (engine, agents) = local_app(&database).await?;
    let mut worker = engine.worker().await?;
    settle(&mut worker).await?;
    let cancelled = agents
        .actors()?
        .inspect_task(root, TaskInspectionOptions::default())
        .await?;
    assert!(cancelled.summary.settled);
    assert_eq!(cancelled.summary.stopped, 3);
    assert_eq!(cancelled.summary.succeeded, 1);
    assert_eq!(
        serde_json::to_value(&cancelled.summary.visible_usage)?,
        serde_json::to_value(&waiting.summary.visible_usage)?
    );
    assert_eq!(
        serde_json::to_value(engine.status(completed_run).await?)?,
        saved_result
    );
    let receipt = agents.actors()?.cancel_goal(root).await?;
    assert!(receipt.settled);
    assert_eq!(receipt.receipt, cancellation.receipt);
    assert!(agents.actors()?.task(reminder).await.is_err());
    let independent = agents
        .actors()?
        .inspect_task(other, TaskInspectionOptions::default())
        .await?;
    assert!(independent.goal_cancellation.is_none());
    let leaf = &independent.nodes[0];
    engine
        .signal(
            leaf.run_id.unwrap(),
            leaf.wait.as_ref().unwrap().id,
            json!({"answer":"Continue"}),
        )
        .await?;
    settle(&mut worker).await?;
    let independent = agents
        .actors()?
        .inspect_task(other, TaskInspectionOptions::default())
        .await?;
    assert_eq!(independent.summary.succeeded, 1);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "restart_verified":true,"waiting":waiting,"cancellation":receipt,"cancelled":cancelled,"independent":independent
        }))?
    );
    Ok(())
}
