//! Stop while a child waits, restart after its persisted deadline, and handle timeout.
use act0r::{
    Worker,
    adapters::system_clock::SystemClock,
    application::ports::Clock,
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

async fn drain(worker: &mut Worker) -> Result<(), act0r::Error> {
    let until = Instant::now() + Duration::from_secs(90);
    while worker.has_active_runs().await? {
        assert!(Instant::now() < until, "deadline demo did not settle");
        if !worker.step().await? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".build/actor-deadlines-demo/demo.db"));
    if let Some(parent) = db.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&db).await?;
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
    let config = json!({"goal":"Ask the assistant and report its timeout.","target":{"provider":"scripted","model":"deadline-fixture","endpoint":""},"max_model_calls":4,"max_tokens":50000,"max_output_tokens":512,
        "workers":[{"name":"assistant","agent_id":assistant,"description":"Needs human clarification."}],"delegation":{"max_tasks":2,"max_depth":1,"timeout_ms":3000},
        "script":[{"kind":"delegate_task","worker":"assistant","goal":"Explain durable recovery."},{"kind":"finish","summary":"The assistant exceeded its durable deadline; its saved timeout was handled."}]});
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
            payload: json!({}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let mut waiting = None;
    for _ in 0..100 {
        let parent = agents.actors()?.task(task).await?;
        if let Some(intent) = parent.delegation
            && let Ok(child) = agents.actors()?.task(intent.id).await
            && child.wait.is_some()
        {
            waiting = Some((
                parent.run_id.unwrap(),
                intent.id,
                intent.deadline_at.unwrap(),
            ));
            break;
        }
        assert!(worker.step().await?, "no saved child wait");
    }
    let (run, child_id, deadline) = waiting.ok_or("child never reached its human wait")?;
    drop(worker);
    drop(agents);
    drop(engine);
    // This sleep simulates downtime; the SQLite deadline is the authoritative timer.
    let remaining = u64::try_from(deadline.saturating_sub(SystemClock.now_ms())).unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(remaining.saturating_add(50))).await;
    let (engine, agents) = local_app(&db).await?;
    let intent = engine
        .status(run)
        .await?
        .pending_delegation()
        .cloned()
        .ok_or("missing saved wait")?;
    assert_eq!(intent.id, child_id);
    assert_eq!(intent.deadline_at, Some(deadline));
    let mut worker = engine.worker().await?;
    drain(&mut worker).await?;
    let parent = agents.actors()?.task(task).await?;
    let child = agents.actors()?.task(child_id).await?;
    let intent = engine.status(run).await?.record().delegations[0].clone();
    assert_eq!(parent.status, AgentTaskStatus::Succeeded);
    assert_eq!(child.status, AgentTaskStatus::Cancelled);
    assert_eq!(
        intent.outcome.as_ref().unwrap()["error"]["code"],
        "delegation_timeout"
    );
    assert_eq!(
        engine
            .history(run)
            .await?
            .iter()
            .filter(|e| e.kind == "delegation_timed_out")
            .count(),
        1
    );
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"parent":parent,"child":child,"delegation":intent}))?
    );
    Ok(())
}
