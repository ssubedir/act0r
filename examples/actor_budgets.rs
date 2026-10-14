//! Compare a shared allowance of three and four model calls across restart.
use act0r::{
    Worker,
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant},
};

async fn drain(worker: &mut Worker) -> Result<(), act0r::Error> {
    let until = Instant::now() + Duration::from_secs(90);
    while worker.has_active_runs().await? {
        assert!(Instant::now() < until, "shared budget demo did not settle");
        if !worker.step().await? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    Ok(())
}
async fn scenario(db: &Path, calls: u32) -> Result<Value, Box<dyn std::error::Error>> {
    let (engine, agents) = local_app(db).await?;
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
    let planner = json!({"goal":"Get an explanation from the assistant.","target":{"provider":"scripted","model":"budget-fixture","endpoint":""},"max_model_calls":8,"max_tokens":100000,"max_output_tokens":512,
        "workers":[{"name":"assistant","agent_id":assistant,"description":"Explains durable recovery."}],
        "delegation":{"max_tasks":2,"max_depth":1,"shared_budget":{"max_model_calls":calls,"max_tokens":20000}},
        "script":[{"kind":"delegate_task","worker":"assistant","goal":"Explain durable recovery."},{"kind":"finish","summary":"The assistant explained recovery after clarification."}]});
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            planner,
            10,
            AgentQueuePolicy::default(),
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
            payload: json!({}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    drain(&mut worker).await?;
    let parent = agents.actors()?.task(task).await?;
    let root = parent.run_id.ok_or("missing parent Run")?;
    let child_id = parent.delegation.ok_or("missing saved delegation")?.id;
    let saved = serde_json::to_value(&engine.shared_budgets(root).await?[0].record.usage)?;
    assert_eq!(saved["admitted_model_calls"], 2);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(db).await?;
    let mut worker = engine.worker().await?;
    assert_eq!(
        serde_json::to_value(&engine.shared_budgets(root).await?[0].record.usage)?,
        saved
    );
    assert_eq!(
        engine.status(root).await?.pending_delegation().unwrap().id,
        child_id
    );
    let child = agents.actors()?.task(child_id).await?;
    assert_eq!(child.shared_budgets[0].record.root_run_id, root);
    engine
        .signal(
            child.run_id.ok_or("missing child Run")?,
            child.wait.ok_or("missing clarification wait")?.id,
            json!({"answer":"Short explanation, please."}),
        )
        .await?;
    drain(&mut worker).await?;
    let parent = agents.actors()?.task(task).await?;
    let child = agents.actors()?.task(child_id).await?;
    assert_eq!(child.status, AgentTaskStatus::Succeeded);
    assert_eq!(
        parent.status,
        if calls == 3 {
            AgentTaskStatus::BudgetExhausted
        } else {
            AgentTaskStatus::Succeeded
        }
    );
    assert_eq!(
        parent.shared_budgets[0].record.usage.admitted_model_calls,
        calls
    );
    assert_eq!(parent.shared_budgets[0].record.usage.held_tokens, 0);
    assert_eq!(parent.shared_budgets[0].members.len(), 2);
    assert_eq!(agents.actors()?.queue(actor).await?.outstanding, 0);
    Ok(json!({"shared_model_call_limit":calls,"parent":parent,"child":child}))
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| ".build/actor-budgets-demo".into());
    std::fs::create_dir_all(&root)?;
    let mut scenarios = vec![];
    for calls in [3, 4] {
        scenarios.push(scenario(&root.join(format!("budget-{calls}.db")), calls).await?);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"scenarios":scenarios}))?
    );
    Ok(())
}
