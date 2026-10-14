//! Expected read error, saved correction request, restart, and delegated review.
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
        .unwrap_or_else(|| ".build/tool-feedback-demo/demo.db".into());
    let path = Path::new(&database);
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let source = path.with_extension("source");
    fs::create_dir_all(&source)?;
    fs::write(
        source.join("README.md"),
        "Project evidence: durable actors in Rust.\n",
    )?;
    let (engine, agents) = local_app(&database).await?;
    let reviewer = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
        "goal":"Review supplied evidence", "target":{"provider":"scripted","model":"reviewer","endpoint":""},
        "max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"finish","summary":"Reviewed the saved evidence about durable Rust actors."}]
    }),10,AgentQueuePolicy::default()).await?;
    let config = json!({
        "goal":"Inspect the project and request a review.",
        "target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "goal_budget":{"max_model_calls":5,"max_tokens":100000},
        "tools":[{"name":"read_file","version":2}],
        "workers":[{"name":"reviewer","agent_id":reviewer,"description":"Reviews supplied file evidence"}],
        "delegation":{"max_tasks":1,"max_depth":1},
        "script":[{"kind":"call_tool","tool":"read_file","arguments":{"path":"missing.md"}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
            {"kind":"delegate_task","worker":"reviewer","goal":"Review the saved evidence: durable actors in Rust."},
            {"kind":"finish","summary":"Recovered from the missing file and reviewed the saved evidence."}]
    });
    let config_path = path.with_extension("planner.json");
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    let planner = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source),
            config,
            10,
            AgentQueuePolicy::default(),
        )
        .await?;
    let workspace = agents.actors()?.status(planner).await?.record().config["input"]["workspace"]
        .as_str()
        .ok_or("missing workspace")?
        .to_owned();
    let task = agents.actors()?.send(AgentMessage { id:MessageId::new(),recipient:planner,sender:None,correlation_id:None,
        kind:"task".into(),payload:json!({"goal":"Inspect the project, correct any missing path, and request a review."}) }).await?;
    let mut worker = engine.worker().await?;
    let mut checkpoint = None;
    for _ in 0..100 {
        worker.step().await?;
        if let Some(run) = agents.actors()?.task(task).await?.run_id {
            let saved = engine.status(run).await?;
            if let Some(activity) = saved.activities().iter().find(|a| {
                a.spec.handler == DefinitionId::new("native.tool", 3)
                    && a.status == ActivityStatus::Succeeded
            }) {
                let result = activity.result.clone().ok_or("missing saved feedback")?;
                assert_eq!(result["ok"], false);
                assert_eq!(result["error"]["code"], "file_not_found");
                let next = saved
                    .activities()
                    .last()
                    .ok_or("missing correction request")?
                    .spec
                    .input
                    .clone();
                assert!(next["prompt"].as_str().unwrap().contains("file_not_found"));
                checkpoint = Some((run, activity.id, result, next));
                break;
            }
        }
    }
    let (run, error_id, feedback, next) = checkpoint.ok_or("error feedback was not saved")?;
    fs::write(
        Path::new(&workspace).join("missing.md"),
        "new evidence must not replace the saved error",
    )?;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await?;
    let mut worker = engine.worker().await?;
    let restored = engine.status(run).await?;
    assert_eq!(restored.activities().last().unwrap().spec.input, next);
    assert_eq!(restored.activities()[1].result, Some(feedback.clone()));
    for _ in 0..100 {
        if !worker.step().await? && !worker.has_active_runs().await? {
            break;
        }
    }
    let parent = agents.actors()?.task(task).await?;
    assert_eq!(parent.status, AgentTaskStatus::Succeeded);
    let saved = engine.status(run).await?;
    let child_id = saved
        .record()
        .delegations
        .first()
        .ok_or("missing delegated review")?
        .id;
    let child = agents.actors()?.task(child_id).await?;
    assert_eq!(child.status, AgentTaskStatus::Succeeded);
    let file_activities: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.handler == DefinitionId::new("native.tool", 3))
        .collect();
    assert_eq!(file_activities.len(), 2);
    assert_eq!(
        file_activities[1].result.as_ref().unwrap()["text"],
        "Project evidence: durable actors in Rust.\n"
    );
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
        serde_json::to_string_pretty(
            &json!({"restart_verified":true,"saved_error":feedback,"error_activity":error_id,
        "file_attempts":2,"model_calls":inspection.summary.visible_usage.admitted_model_calls,
        "parent":{"task":task,"status":parent.status},"child":{"task":child_id,"status":child.status},"planner_config":config_path})
        )?
    );
    Ok(())
}
