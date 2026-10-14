//! Two-slot admission, pending cancellation, retry, and restart without a model server.
use act0r::{
    Error,
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::json;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".build/actor-queue-demo/demo.db"));
    if let Some(parent) = db.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&db).await?;
    let actors = agents.actors()?;
    let policy = AgentQueuePolicy { task_capacity: 2 };
    let worker_id = actors.create_with_queue_policy(AgentId::new(), DefinitionId::new("workflow_worker", 1), json!({"workflow":{"name":"demo","version":1},"input":{"message":"Bounded actor task"},"attempt_limit":10}), policy).await?;
    let coordinator_id = actors
        .create_with_queue_policy(
            AgentId::new(),
            DefinitionId::new("coordinator", 1),
            json!({"worker":worker_id}),
            policy,
        )
        .await?;
    let tasks: Vec<_> = ["A", "B", "C"]
        .into_iter()
        .map(|name| AgentMessage {
            id: MessageId::new(),
            recipient: coordinator_id,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":name}),
        })
        .collect();
    actors.send(tasks[0].clone()).await?;
    actors.send(tasks[1].clone()).await?;
    let rejected = match actors.send(tasks[2].clone()).await {
        Err(error @ Error::QueueFull { .. }) => error.to_string(),
        other => return Err(format!("expected QueueFull, got {other:?}").into()),
    };
    let mut worker = engine.worker().await?;
    while worker.advance_work().await? {}
    let full_queue = actors.queue(worker_id).await?;
    assert_eq!(
        (
            full_queue.outstanding,
            full_queue.pending_tasks,
            full_queue.executing_tasks
        ),
        (2, 1, 1)
    );
    actors.cancel_task(tasks[1].id).await?;
    assert_eq!(actors.queue(worker_id).await?.available, 1);
    actors.send(tasks[2].clone()).await?; // Retry the unaccepted request using its original ID.
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let actors = agents.actors()?;
    let mut worker = engine.worker().await?;
    let deadline = Instant::now() + Duration::from_secs(60);
    while worker.has_active_runs().await? {
        if Instant::now() > deadline {
            return Err("actor queue demo timed out".into());
        }
        if !worker.step().await? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let results = actors.tasks(coordinator_id).await?;
    assert_eq!(
        results.iter().map(|t| t.status).collect::<Vec<_>>(),
        [
            AgentTaskStatus::Succeeded,
            AgentTaskStatus::Cancelled,
            AgentTaskStatus::Succeeded
        ]
    );
    assert!(results[1].run_id.is_none());
    assert_eq!(actors.runs(worker_id).await?.len(), 2);
    assert_eq!(actors.queue(worker_id).await?.available, 2);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"queue_full":rejected,"full_queue":full_queue,"final_queue":actors.queue(worker_id).await?,"tasks":results})
        )?
    );
    Ok(())
}
