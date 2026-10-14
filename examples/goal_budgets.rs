//! One goal allowance survives restart and bounds an entire follow-up chain.
use act0r::{
    bootstrap::local_app,
    domain::{actors::*, shared_budget::*, *},
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/goal-budgets-demo/demo.db".into());
    if let Some(parent) = std::path::Path::new(&database).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&database).await?;
    let assistant=agents.create_registered_actor(DefinitionId::new("answer",1),None,
        json!({"goal":"Assist","target":{"provider":"scripted","model":"goal-demo","endpoint":""},"max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"finish","summary":"Saved child result"}]}),10,AgentQueuePolicy::default()).await?;
    let actor=agents.create_registered_actor(DefinitionId::new("planner",3),None,
        json!({"goal":"Continue durable work","target":{"provider":"scripted","model":"goal-demo","endpoint":""},"max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "goal_budget":{"max_model_calls":5,"max_tokens":100000},"memory":{},
        "workers":[{"name":"assistant","agent_id":assistant,"description":"A permitted assistant"}],"delegation":{"max_tasks":4,"max_depth":2},
        "follow_up":{"max_follow_ups":4,"min_delay_ms":1,"max_delay_ms":60000},
        "script":[{"kind":"delegate_task","worker":"assistant","goal":"Help with this step"},{"kind":"schedule_follow_up","summary":"Saved progress","goal":"Continue the goal","after_ms":1000}]}),20,AgentQueuePolicy::default()).await?;
    let root = agents
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
    while !agents.actors()?.task(root).await?.status.is_terminal() {
        worker.step().await?;
    }
    while agents
        .actors()?
        .task(root)
        .await?
        .follow_up
        .as_ref()
        .is_none_or(|f| f.status != act0r::domain::follow_up::FollowUpStatus::Registered)
    {
        worker.advance_work().await?;
    }
    let original = agents.actors()?.task(root).await?;
    let run = original.run_id.unwrap();
    let next = original.follow_up.as_ref().unwrap().reminder_id;
    let before = engine
        .shared_budgets(run)
        .await?
        .into_iter()
        .find(|s| s.record.kind == SharedBudgetKind::Goal)
        .unwrap();
    assert_eq!(before.record.usage.admitted_model_calls, 3);
    assert_eq!(before.remaining_model_calls, Some(2));
    drop(worker);
    drop(agents);
    drop(engine);
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let (engine, agents) = local_app(&database).await?;
    let restored = engine
        .shared_budgets(run)
        .await?
        .into_iter()
        .find(|s| s.record.kind == SharedBudgetKind::Goal)
        .unwrap();
    assert_eq!(
        serde_json::to_value(&restored)?,
        serde_json::to_value(&before)?
    );
    let mut worker = engine.worker().await?;
    while worker.has_active_runs().await? {
        if !worker.step().await? {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    let follow_up = agents.actors()?.task(next).await?;
    assert_eq!(follow_up.status, AgentTaskStatus::BudgetExhausted);
    let total = engine
        .shared_budgets(run)
        .await?
        .into_iter()
        .find(|s| s.record.kind == SharedBudgetKind::Goal)
        .unwrap();
    assert_eq!(total.record.usage.admitted_model_calls, 5);
    assert_eq!(total.members.len(), 4);
    assert!(total.record.exhaustion.is_some());
    assert_eq!(
        agents.actors()?.task(root).await?.status,
        AgentTaskStatus::Succeeded
    );
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"restart_verified":true,"agent_id":actor,"original_task":original,"follow_up_task":follow_up,"goal_budget":total})
        )?
    );
    Ok(())
}
