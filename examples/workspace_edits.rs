//! Native planner edit, uncommitted effect, explicit reconciliation, and delegated review.
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
        .unwrap_or_else(|| ".build/workspace-edits-demo/demo.db".into());
    let path = Path::new(&database);
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let source = path.with_extension("source");
    fs::create_dir_all(&source)?;
    fs::write(source.join("README.md"), "Original project evidence.\n")?;
    let (engine, agents) = local_app(&database).await?;
    let reviewer = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
        "goal":"Review supplied evidence","target":{"provider":"scripted","model":"reviewer","endpoint":""},
        "max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"finish","summary":"Reviewed the updated project evidence."}]
    }),10,Default::default()).await?;
    let config = json!({
        "goal":"Read, edit, inspect, and review the README.","target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":5,"max_tokens":300000,"max_output_tokens":512,"goal_budget":{"max_model_calls":6,"max_tokens":300000},
        "tools":[{"name":"read_file","version":2},{"name":"replace_text","version":1}],"writable_paths":["README.md"],
        "workers":[{"name":"reviewer","agent_id":reviewer,"description":"Reviews supplied file evidence"}],"delegation":{"max_tasks":1,"max_depth":1},
        "script":[{"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"README.md","old":"Original project evidence.","new":"Updated project evidence."}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
            {"kind":"delegate_task","worker":"reviewer","goal":"Review the saved updated project evidence."},
            {"kind":"finish","summary":"Edited and reviewed the README."}]
    });
    let config_path = path.with_extension("planner.json");
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source.clone()),
            config,
            14,
            Default::default(),
        )
        .await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Update and review the README."}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let mut pending = None;
    for _ in 0..100 {
        worker.step().await?;
        let view = agents.actors()?.task(task).await?;
        if let Some(wait) = view.wait {
            assert_eq!(wait.key, "native_tool_approval");
            assert_eq!(wait.payload["writable_paths"], json!(["README.md"]));
            pending = Some((view.run_id.ok_or("missing Run")?, wait));
            break;
        }
    }
    let (run, wait) = pending.ok_or("edit approval was not reached")?;
    engine
        .signal(run, wait.id, json!({"approved":true}))
        .await?;
    let edit = worker.admit().await?.ok_or("missing approved edit")?;
    assert_eq!(edit.handler, DefinitionId::new("native.tool", 4));
    let result = worker.execute(edit.clone()).await?;
    assert_eq!(
        result.result().ok_or("missing edit result")?["already_applied"],
        false
    );
    let workspace = edit.input["workspace"]
        .as_str()
        .ok_or("missing edit workspace")?
        .to_owned();
    // Deliberately stop without saving the edit's result, reproducing an uncertain effect.
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await?;
    let mut worker = engine.worker().await?;
    assert_eq!(
        engine.status(run).await?.record().status,
        RunStatus::Blocked
    );
    assert!(worker.admit().await?.is_none());
    assert_eq!(
        fs::read_to_string(Path::new(&workspace).join("README.md"))?,
        "Updated project evidence.\n"
    );
    assert_eq!(
        fs::read_to_string(source.join("README.md"))?,
        "Original project evidence.\n"
    );
    // This deterministic fixture has inspected the file and owns the stopped worker.
    // A real operator must inspect the external effect before authorizing this decision.
    engine.reconcile(run,ReconciliationRequest {activity_id:edit.activity_id,attempt_id:edit.attempt_id,
        decision:ReconciliationDecision::Retry {},evidence:"Stopped worker; verified the expected updated README in the managed workspace. Authorize journal verification.".into()}).await?;
    let retry = worker.admit().await?.ok_or("missing reconciled retry")?;
    assert_eq!(retry.activity_id, edit.activity_id);
    assert_eq!(retry.input, edit.input);
    assert_ne!(retry.attempt_id, edit.attempt_id);
    let outcome = worker.execute(retry.clone()).await?;
    assert_eq!(
        outcome.result().ok_or("missing recovered edit result")?["already_applied"],
        true
    );
    worker.complete(retry, outcome).await?;
    let saved = engine.status(run).await?;
    let next = saved
        .activities()
        .last()
        .ok_or("missing next request")?
        .spec
        .input
        .clone();
    let usage = json!(saved.record().usage);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await?;
    let mut worker = engine.worker().await?;
    let saved = engine.status(run).await?;
    assert_eq!(saved.activities().last().unwrap().spec.input, next);
    assert_eq!(json!(saved.record().usage), usage);
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
        .ok_or("missing review")?
        .id;
    assert_eq!(
        agents.actors()?.task(child_id).await?.status,
        AgentTaskStatus::Succeeded
    );
    let edit_attempts = saved
        .attempts()
        .iter()
        .filter(|a| a.activity_id == edit.activity_id)
        .count();
    assert_eq!(edit_attempts, 2);
    let reads: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.input["definition"]["id"]["name"] == "read_file")
        .collect();
    assert_eq!(reads.len(), 2);
    assert_eq!(
        reads[1].result.as_ref().unwrap()["text"],
        "Updated project evidence.\n"
    );
    let tree = agents
        .actors()?
        .inspect_task(task, Default::default())
        .await?;
    assert_eq!(tree.summary.visible_usage.admitted_model_calls, 6);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"restart_verified":true,"reconciliation_verified":true,"already_applied":true,
        "edit_activity":edit.activity_id,"edit_attempts":edit_attempts,"model_calls":tree.summary.visible_usage.admitted_model_calls,
        "task":task,"review_task":child_id,"status":parent.status,"workspace":workspace,"planner_config":config_path})
        )?
    );
    Ok(())
}
