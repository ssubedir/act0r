//! Coding and answering agents share actor tasks, durable waits, and recovery.
use act0r::{
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".build/agent-integrations-demo/demo.db"));
    if let Some(parent) = db.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(&db).await?;
    let coding: Value = serde_json::from_str(include_str!("configs/task-scripted.json"))?;
    let coding_id = agents
        .create_registered_actor(
            DefinitionId::new("coding", 1),
            Some(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("examples/benchmark-cases/addition/source"),
            ),
            coding["agent"].clone(),
            40,
            AgentQueuePolicy::default(),
        )
        .await?;
    let answer: Value = serde_json::from_str(include_str!("configs/answer-scripted.json"))?;
    let answer_id = agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            answer,
            10,
            AgentQueuePolicy::default(),
        )
        .await?;
    let mut handles = vec![];
    for (id, goal) in [
        (coding_id, "Fix addition and run the fixed tests."),
        (answer_id, "Explain how durable agents recover."),
    ] {
        handles.push(
            agents
                .actors()?
                .send(AgentMessage {
                    id: MessageId::new(),
                    recipient: id,
                    sender: None,
                    correlation_id: None,
                    kind: "task".into(),
                    payload: json!({"goal":goal}),
                })
                .await?,
        );
    }
    let mut worker = engine.worker().await?;
    let deadline = Instant::now() + Duration::from_secs(90);
    while worker.has_active_runs().await? {
        if Instant::now() > deadline {
            return Err("integration demo timed out".into());
        }
        if !worker.step().await? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if agents.actors()?.task(handles[1]).await?.status == AgentTaskStatus::Waiting {
            break;
        }
    }
    let waiting = agents.actors()?.task(handles[1]).await?;
    let run_id = waiting.run_id.ok_or("missing answer run")?;
    let wait_id = waiting.wait.ok_or("missing answer wait")?.id;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    assert_eq!(
        agents
            .actors()?
            .task(handles[1])
            .await?
            .wait
            .as_ref()
            .map(|w| w.id),
        Some(wait_id)
    );
    engine
        .signal(
            run_id,
            wait_id,
            json!({"answer":"A short explanation, please."}),
        )
        .await?;
    let mut worker = engine.worker().await?;
    let deadline = Instant::now() + Duration::from_secs(90);
    while worker.has_active_runs().await? {
        if Instant::now() > deadline {
            return Err("integration demo timed out after restart".into());
        }
        if !worker.step().await? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let coding = agents.actors()?.task(handles[0]).await?;
    let answer = agents.actors()?.task(handles[1]).await?;
    assert_eq!(coding.status, AgentTaskStatus::Succeeded);
    assert_eq!(answer.status, AgentTaskStatus::Succeeded);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"registrations":agents.registrations()?,"coding":coding,"answer":answer})
        )?
    );
    Ok(())
}
