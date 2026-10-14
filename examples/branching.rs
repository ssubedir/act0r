//! cargo run --example branching -- .build/branch-demo/demo.db
use act0r::{
    bootstrap::local_app,
    domain::{CheckpointId, ForkId, task::TaskConfig},
};
use std::{path::Path, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/branch-demo/demo.db".into());
    if let Some(parent) = Path::new(&database)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(database).await?;
    let mut config: TaskConfig = serde_json::from_str(include_str!("configs/task-scripted.json"))?;
    config.candidates = 1;
    let task_id = agents
        .tasks()
        .unwrap()
        .start(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/coding-task"),
            config,
        )
        .await?;
    let parent = agents
        .tasks()
        .unwrap()
        .status(task_id)
        .await?
        .record()
        .candidates[0]
        .run_id;
    let mut worker = engine.worker().await?;
    // Three model/tool pairs complete the list, read, and edit steps.
    for _ in 0..6 {
        worker.step().await?;
    }
    engine.pause(parent).await?;
    let branches = agents.branches().unwrap();
    let checkpoint_id = CheckpointId::new();
    branches.create_checkpoint(checkpoint_id, parent).await?;
    worker.advance_work().await?;
    let fork_id = ForkId::new();
    let fork = branches.create_fork(fork_id, checkpoint_id, 2).await?;
    worker.advance_work().await?;
    // Keep the two continuations and retire the paused parent.
    engine.cancel(parent).await?;
    while worker.has_active_runs().await? {
        if !worker.step().await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let task = agents.tasks().unwrap().status(task_id).await?;
    println!("Task: {task_id}\nCheckpoint: {checkpoint_id}\nFork: {fork_id}");
    println!("Children: {}", serde_json::to_string(&fork.run_ids)?);
    println!("{}", serde_json::to_string_pretty(task.record())?);
    Ok(())
}
