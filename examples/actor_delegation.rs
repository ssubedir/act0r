//! Native model-selected delegation, saved child waits, and restart.
use act0r::{
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

async fn drain(worker: &mut act0r::Worker) -> Result<(), act0r::Error> {
    let deadline = Instant::now() + Duration::from_secs(90);
    while worker.has_active_runs().await? {
        assert!(Instant::now() < deadline, "delegation demo timed out");
        if !worker.step().await? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".build/actor-delegation-demo/demo.db"));
    if let Some(parent) = db.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&db).await?;
    let answer: Value = serde_json::from_str(include_str!("configs/answer-scripted.json"))?;
    let assistant = agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            answer,
            10,
            AgentQueuePolicy::default(),
        )
        .await?;
    let planner_config = json!({
        "goal":"Explain durable execution with help from the assistant.",
        "target":{"provider":"scripted","model":"planner-fixture","endpoint":""},
        "max_model_calls":4,"max_tokens":50000,"max_output_tokens":512,"max_input_bytes":65536,
        "workers":[{"name":"assistant","agent_id":assistant,"description":"Answers questions about durable execution."}],
        "delegation":{"max_tasks":2,"max_depth":1},
        "script":[{"kind":"delegate_task","worker":"assistant","goal":"Explain how durable agents recover."},
                  {"kind":"finish","summary":"The assistant explained durable execution after clarification and a restart."}]
    });
    let config_path = db.parent().unwrap().join("planner.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&planner_config)?)?;
    let planner = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            planner_config,
            10,
            AgentQueuePolicy::default(),
        )
        .await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: planner,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Explain durable recovery through the assistant."}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    drain(&mut worker).await?;
    let waiting = agents.actors()?.task(task).await?;
    assert_eq!(waiting.status, AgentTaskStatus::Waiting);
    let child = waiting.delegation.ok_or("missing delegated task")?.id;
    let child_wait = agents.actors()?.task(child).await?;
    let child_run = child_wait.run_id.ok_or("missing child Run")?;
    let wait = child_wait.wait.ok_or("missing child human wait")?.id;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    assert_eq!(
        agents.actors()?.task(task).await?.delegation.unwrap().id,
        child
    );
    engine
        .signal(
            child_run,
            wait,
            json!({"answer":"A short explanation, please."}),
        )
        .await?;
    let mut worker = engine.worker().await?;
    drain(&mut worker).await?;
    let parent = agents.actors()?.task(task).await?;
    let child = agents.actors()?.task(child).await?;
    assert_eq!(parent.status, AgentTaskStatus::Succeeded);
    assert_eq!(child.status, AgentTaskStatus::Succeeded);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"parent":parent,"child":child,"planner_config":config_path})
        )?
    );
    Ok(())
}
