//! cargo run --locked --example actor_tasks -- .build/actor-tasks-demo/demo.db
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
        .unwrap_or_else(|| ".build/actor-tasks-demo/demo.db".into());
    if let Some(parent) = Path::new(&db)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&db).await?;
    let actors = agents.actors()?;
    let worker = actors.create(AgentId::new(), DefinitionId::new("workflow_worker", 1), json!({"workflow":{"name":"demo","version":1},"input":{"message":"Durable task lifecycle"},"attempt_limit":10})).await?;
    let coordinator = actors
        .create(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":worker}),
        )
        .await?;
    let mut handles = vec![];
    for name in ["A", "B", "C"] {
        handles.push(
            actors
                .send(AgentMessage {
                    id: MessageId::new(),
                    recipient: coordinator,
                    sender: None,
                    correlation_id: None,
                    kind: "task".into(),
                    payload: json!({"goal":format!("Task {name}")}),
                })
                .await?,
        );
    }
    let mut executor = engine.worker().await?;
    while executor.advance_work().await? {}
    actors.cancel_task(handles[1]).await?;
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
    let actors = agents.actors()?;
    let tasks = actors.tasks(coordinator).await?;
    assert_eq!(
        tasks.iter().map(|t| t.status).collect::<Vec<_>>(),
        vec![
            AgentTaskStatus::Succeeded,
            AgentTaskStatus::Cancelled,
            AgentTaskStatus::Succeeded
        ]
    );
    assert!(tasks[1].run_id.is_none());
    assert_eq!(tasks[1].admitted_attempts, 0);
    assert!(tasks[1].usage.is_none());
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"coordinator":coordinator,"worker":worker,"tasks":tasks})
        )?
    );
    Ok(())
}
