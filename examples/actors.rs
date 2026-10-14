//! cargo run --locked --example actors -- .build/actors-demo/demo.db
use act0r::{
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::json;
use std::{path::Path, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/actors-demo/demo.db".into());
    if let Some(parent) = Path::new(&db)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&db).await?;
    let actors = agents.actors()?;
    let worker = actors.create(AgentId::new(),DefinitionId::new("workflow_worker",1),json!({"workflow":{"name":"demo","version":1},"input":{"message":"Durable actor delegation"},"attempt_limit":10})).await?;
    let coordinator = actors
        .create(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":worker}),
        )
        .await?;
    let request = actors
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: coordinator,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Delegate this task and keep its reply"}),
        })
        .await?;
    let mut executor = engine.worker().await?;
    executor.advance_work().await?; // Coordinator commits its outgoing task.
    executor.advance_work().await?; // Worker commits a run intent.
    drop(executor);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let mut executor = engine.worker().await?;
    while executor.has_active_runs().await? {
        if !executor.step().await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"request_id":request,"coordinator":agents.actors()?.status(coordinator).await?.record(),"worker":agents.actors()?.status(worker).await?.record(),"runs":agents.actors()?.runs(worker).await?})
        )?
    );
    Ok(())
}
