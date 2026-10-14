//! Portable file inspection, saved results, restart, and native delegation.
use act0r::{
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::json;
use std::{fs, path::Path};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/workspace-tools-demo/demo.db".into());
    let path = Path::new(&database);
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let source = path.with_extension("source");
    fs::create_dir_all(source.join("src"))?;
    fs::write(
        source.join("README.md"),
        "Project evidence: durable actors in Rust.\n",
    )?;
    fs::write(
        source.join("src/lib.rs"),
        "pub fn durable() -> bool { true }\n",
    )?;
    fs::write(source.join(".env"), "excluded fixture")?;
    let (engine, agents) = local_app(&database).await?;
    let reviewer = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
        "goal":"Review supplied project evidence", "target":{"provider":"scripted","model":"reviewer","endpoint":""},
        "max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"finish","summary":"Reviewed the saved evidence about durable Rust actors."}]
    }),10,AgentQueuePolicy::default()).await?;
    let config = json!({
        "goal":"Inspect the project and delegate its review.",
        "target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "goal_budget":{"max_model_calls":5,"max_tokens":100000},
        "tools":[{"name":"list_files","version":2},{"name":"read_file","version":2}],
        "workers":[{"name":"reviewer","agent_id":reviewer,"description":"Reviews supplied file evidence"}],
        "delegation":{"max_tasks":1,"max_depth":1},
        "script":[{"kind":"call_tool","tool":"list_files","arguments":{}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
            {"kind":"delegate_task","worker":"reviewer","goal":"Review the saved evidence: durable actors in Rust."},
            {"kind":"finish","summary":"The saved project evidence was inspected and reviewed."}]
    });
    let config_path = path.with_extension("planner.json");
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    let planner = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source.clone()),
            config,
            10,
            AgentQueuePolicy::default(),
        )
        .await?;
    let workspace = agents.actors()?.status(planner).await?.record().config["input"]["workspace"]
        .as_str()
        .ok_or("missing pinned workspace")?
        .to_owned();
    assert!(!Path::new(&workspace).join(".env").exists());
    fs::write(
        source.join("README.md"),
        "Changed source after actor creation",
    )?;
    let task = agents.actors()?.send(AgentMessage {
        id:MessageId::new(),recipient:planner,sender:None,correlation_id:None,kind:"task".into(),
        payload:json!({"goal":"Inspect the copied project, then ask the reviewer to review it."}),
    }).await?;
    let mut worker = engine.worker().await?;
    let mut checkpoint = None;
    for _ in 0..100 {
        worker.step().await?;
        if let Some(run) = agents.actors()?.task(task).await?.run_id {
            let saved = engine.status(run).await?;
            if let Some(read) = saved.activities().iter().find(|a| {
                a.spec.handler == DefinitionId::new("native.tool", 3)
                    && a.spec.input["definition"]["id"]["name"] == "read_file"
                    && a.status == ActivityStatus::Succeeded
            }) {
                assert_eq!(
                    read.result.as_ref().unwrap()["text"],
                    "Project evidence: durable actors in Rust.\n"
                );
                let next = saved
                    .activities()
                    .last()
                    .ok_or("missing next request")?
                    .spec
                    .input
                    .clone();
                assert!(
                    next["prompt"]
                        .as_str()
                        .unwrap()
                        .contains("Project evidence: durable actors in Rust.")
                );
                checkpoint = Some((run, read.id, next));
                break;
            }
        }
    }
    let (run, read, next) = checkpoint.ok_or("read result was not saved")?;
    fs::write(
        Path::new(&workspace).join("README.md"),
        "Changed file after saved read",
    )?;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await?;
    let mut worker = engine.worker().await?;
    assert_eq!(
        engine
            .status(run)
            .await?
            .activities()
            .last()
            .unwrap()
            .spec
            .input,
        next
    );
    for _ in 0..100 {
        if !worker.step().await? && !worker.has_active_runs().await? {
            break;
        }
    }
    let parent = agents.actors()?.task(task).await?;
    let saved = engine.status(run).await?;
    let child = agents
        .actors()?
        .task(
            saved
                .record()
                .delegations
                .first()
                .ok_or("missing child")?
                .id,
        )
        .await?;
    assert_eq!(parent.status, AgentTaskStatus::Succeeded);
    assert_eq!(child.status, AgentTaskStatus::Succeeded);
    let file_activities: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.handler == DefinitionId::new("native.tool", 3))
        .collect();
    assert_eq!(file_activities.len(), 2);
    for activity in &file_activities {
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == activity.id)
                .count(),
            1
        );
    }
    let inspection = agents
        .actors()?
        .inspect_task(task, Default::default())
        .await?;
    assert_eq!(inspection.summary.visible_usage.admitted_model_calls, 5);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"restart_verified":true,"saved_read":read,
        "file_activities":file_activities.len(),"file_attempts":2,"workspace":workspace,
        "model_calls":inspection.summary.visible_usage.admitted_model_calls,"planner_config":config_path,
        "parent":parent,"child":child}))?
    );
    Ok(())
}
