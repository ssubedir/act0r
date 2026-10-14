//! Direct tool call, saved continuation, restart, and delegation under one goal budget.
use act0r::{
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/planner-tools-demo/demo.db".into());
    let path = std::path::Path::new(&database);
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&database).await?;
    let assistant = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
        "goal":"Explain the result", "target":{"provider":"scripted","model":"assistant","endpoint":""},
        "max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"finish","summary":"19 + 23 = 42."}]
    }),10,AgentQueuePolicy::default()).await?;
    let config = json!({
        "goal":"Calculate and ask the assistant to explain the result.",
        "target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
        "goal_budget":{"max_model_calls":4,"max_tokens":100000},
        "tools":[{"name":"sum","version":1}],
        "workers":[{"name":"assistant","agent_id":assistant,"description":"Explains the calculation"}],
        "delegation":{"max_tasks":1,"max_depth":1},
        "script":[{"kind":"call_tool","tool":"sum","arguments":{"left":19,"right":23}},
            {"kind":"delegate_task","worker":"assistant","goal":"Explain why 19 + 23 = 42."},
            {"kind":"finish","summary":"The saved calculation is 42, confirmed by the assistant."}]
    });
    let config_path = path.with_extension("planner.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    let planner = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            config,
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
            payload: json!({"goal":"Compute 19 + 23, then ask the assistant to explain it."}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let mut checkpoint = None;
    for _ in 0..100 {
        worker.step().await?;
        if let Some(run) = agents.actors()?.task(task).await?.run_id {
            let saved = engine.status(run).await?;
            if let Some(tool) = saved.activities().iter().find(|a| {
                a.spec.handler == DefinitionId::new("native.tool", 1)
                    && a.status == ActivityStatus::Succeeded
            }) {
                assert_eq!(tool.result, Some(json!({"sum":42})));
                checkpoint = Some((
                    run,
                    tool.id,
                    saved.activities().last().unwrap().spec.input.clone(),
                ));
                break;
            }
        }
    }
    let (run, tool, next) = checkpoint.ok_or("tool result was not saved")?;
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
    let attempts = saved
        .attempts()
        .iter()
        .filter(|a| a.activity_id == tool)
        .count();
    assert_eq!(attempts, 1);
    let inspection = agents
        .actors()?
        .inspect_task(task, Default::default())
        .await?;
    assert_eq!(inspection.summary.visible_usage.admitted_model_calls, 4);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"restart_verified":true,"saved_tool_attempts":attempts,
        "model_calls":inspection.summary.visible_usage.admitted_model_calls,"planner_config":config_path,"parent":parent,"child":child})
        )?
    );
    Ok(())
}
