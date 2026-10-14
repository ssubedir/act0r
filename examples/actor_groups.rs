//! Two native worker actors, an ordered durable join, and restart with a partial result.
use act0r::{
    adapters::tokio_worker::{self, WorkerOptions},
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::json;
use std::path::PathBuf;

async fn drain(engine: &act0r::Engine) -> Result<(), act0r::Error> {
    tokio_worker::run(
        engine.worker().await?,
        WorkerOptions {
            until_idle: true,
            max_concurrent_activities: 2,
            ..WorkerOptions::default()
        },
    )
    .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".build/actor-groups-demo/demo.db"));
    let directory = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(directory)?;
    let (engine, agents) = local_app(&db).await?;
    let mut workers = vec![];
    for (name, script) in [
        (
            "recovery",
            json!([
                {"kind":"ask_human","question":"Should the explanation focus on crash recovery?"},
                {"kind":"finish","summary":"Completed decisions and results are saved; interrupted work follows its pinned recovery policy."}
            ]),
        ),
        (
            "budgets",
            json!([
                {"kind":"finish","summary":"The planner and its children reserve model calls and tokens against one inherited allowance."}
            ]),
        ),
    ] {
        let actor = agents
            .create_registered_actor(
                DefinitionId::new("answer", 1),
                None,
                json!({"goal":"Answer your assigned subtask.",
                "target":{"provider":"scripted","model":name,"endpoint":""},
                "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
                "scripted_delay_ms":150,"script":script}),
                10,
                AgentQueuePolicy::default(),
            )
            .await?;
        workers
            .push(json!({"name":name,"agent_id":actor,"description":format!("Explains {name}.")}));
    }
    let planner_config = json!({
        "goal":"Explain durable recovery and shared budgets with both specialists.",
        "target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "workers":workers,
        "delegation":{"max_tasks":2,"max_depth":1,"max_parallel_tasks":2,
            "shared_budget":{"max_model_calls":5,"max_tokens":200000}},
        "script":[
            {"kind":"delegate_tasks","tasks":[
                {"worker":"recovery","goal":"Explain crash recovery."},
                {"worker":"budgets","goal":"Explain shared accounting."}]},
            {"kind":"finish","summary":"Combined both specialists' saved results after a restart."}]
    });
    let config_path = directory.join("planner.json");
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
            payload: json!({}),
        })
        .await?;
    drain(&engine).await?;
    let partial = agents.actors()?.task(task).await?;
    assert_eq!(partial.status, AgentTaskStatus::Waiting);
    let group = partial.task_groups.first().ok_or("missing group")?.clone();
    assert!(group.joined_at.is_none());
    assert!(partial.group_children[0].outcome.is_none());
    assert_eq!(
        partial.group_children[1].outcome.as_ref().unwrap()["status"],
        "succeeded"
    );
    let child = agents.actors()?.task(group.children[0]).await?;
    let child_run = child.run_id.ok_or("missing child Run")?;
    let wait = child.wait.ok_or("missing child wait")?.id;
    println!(
        "Saved group {}: budgets completed; recovery awaits clarification. Reopening the database.",
        group.id
    );
    drop(agents);
    drop(engine);

    let (engine, agents) = local_app(&db).await?;
    let restored = agents.actors()?.task(task).await?;
    assert_eq!(restored.task_groups[0].id, group.id);
    assert_eq!(restored.task_groups[0].children, group.children);
    engine
        .signal(
            child_run,
            wait,
            json!({"answer":"Yes, focus on crash recovery."}),
        )
        .await?;
    drain(&engine).await?;
    let done = agents.actors()?.task(task).await?;
    assert_eq!(done.status, AgentTaskStatus::Succeeded);
    assert!(done.task_groups[0].joined_at.is_some());
    assert_eq!(done.shared_budgets[0].record.usage.admitted_model_calls, 5);
    assert_eq!(
        engine
            .history(done.run_id.unwrap())
            .await?
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"parent":done,"planner_config":config_path}))?
    );
    Ok(())
}
