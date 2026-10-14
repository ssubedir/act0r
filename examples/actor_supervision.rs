//! Three failed tasks pause an actor; explicit recovery resumes its durable queue.
use act0r::{
    Engine,
    adapters::{
        agent_executor::AgentExecutor,
        agents::AnswerAgentAdapter,
        demo::DemoExecutor,
        file_lock::FileOwnership,
        model::ModelRouter,
        sqlite::SqliteStore,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{ActorService, AgentRegistry, AgentService, ports::*},
    domain::{actors::*, agent::*, *},
};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};
struct DemoClock(i64);
impl Clock for DemoClock {
    fn now_ms(&self) -> i64 {
        self.0
    }
}
struct Faults(ModelRouter);
impl ModelClient for Faults {
    fn generate(&self, request: ModelRequest, id: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            let prompt: Value = serde_json::from_str(&request.prompt)?;
            if prompt["goal"]
                .as_str()
                .is_some_and(|g| g.starts_with("Fail"))
            {
                return Ok(ModelReply {
                    decision: Err("Scripted terminal model failure".into()),
                    usage: Some(TokenUsage {
                        input_tokens: 10,
                        output_tokens: 10,
                    }),
                    retryable: false,
                    diagnostics: None,
                    retry_after: None,
                });
            }
            self.0.generate(request, id).await
        })
    }
}
async fn app(db: &Path, now: i64) -> Result<(Engine, AgentService), act0r::Error> {
    let store = Arc::new(SqliteStore::open(db).await?);
    let root = db
        .parent()
        .unwrap_or(Path::new("."))
        .join(".act0r/workspaces");
    let workspaces = Arc::new(LocalWorkspaces::new(root.clone()));
    let registry = Arc::new(AgentRegistry::new(vec![Arc::new(AnswerAgentAdapter)])?);
    let engine = Engine::new(
        store.clone(),
        Arc::new(AgentExecutor::new(
            Arc::new(Faults(ModelRouter::new(None)?)),
            Arc::new(LocalTools::new(root)),
            Arc::new(DemoExecutor),
        )),
        Arc::new(DemoClock(now)),
        Arc::new(FileOwnership::new(db)),
        registry.workflows(),
    )?;
    let actors = Arc::new(
        ActorService::new(
            engine.clone(),
            store,
            vec![Arc::new(RegisteredAgentDefinition)],
        )?
        .with_agent_registry(registry.clone()),
    );
    let engine = engine.with_coordinator(actors.clone());
    let agents = AgentService::new(engine.clone(), workspaces)
        .with_actors(actors)
        .with_agent_registry(registry);
    Ok((engine, agents))
}
fn task(actor: AgentId, goal: &str) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient: actor,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":goal}),
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/actor-supervision-demo/demo.db".into());
    let db = Path::new(&database);
    if let Some(parent) = db.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = app(db, 1000).await?;
    let actor=agents.create_registered_actor(DefinitionId::new("answer",1),None,
        json!({"goal":"Answer","target":{"provider":"scripted","model":"supervision-demo","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"memory":{},
        "supervision":{"max_consecutive_failures":3},"script":[{"kind":"finish","summary":"Completed retained work"}]}),
        20,AgentQueuePolicy {task_capacity:8}).await?;
    let mut tasks = vec![];
    for goal in ["Fail one", "Fail two", "Fail three", "Retained goal"] {
        let task = task(actor, goal);
        agents.actors()?.send(task.clone()).await?;
        tasks.push(task);
    }
    let reminder = task(actor, "Review after downtime");
    agents
        .actors()?
        .schedule_task(reminder.clone(), TimerDeadline::At { unix_ms: 1500 })
        .await?;
    let mut worker = engine.worker().await?;
    for _ in &tasks {
        worker.advance_work().await?;
    }
    while worker.step().await? {}
    let paused = agents.actors()?.supervision(actor).await?.unwrap();
    assert_eq!(paused.consecutive_failures, 3);
    assert_eq!(
        agents.actors()?.status(actor).await?.record().status,
        AgentStatus::Paused
    );
    assert!(agents.actors()?.task(tasks[3].id).await?.run_id.is_none());
    drop(worker);
    drop(agents);
    drop(engine);
    // The demo clock advances across downtime; no in-memory sleep drives durability.
    let (engine, agents) = app(db, 5000).await?;
    let mut worker = engine.worker().await?;
    assert!(!worker.step().await?);
    assert_eq!(agents.actors()?.supervision(actor).await?.unwrap(), paused);
    assert_eq!(
        agents.actors()?.reminder(reminder.id).await?.status,
        ReminderStatus::Scheduled
    );
    agents.actors()?.recover_supervision(actor).await?;
    while worker.step().await? {}
    assert_eq!(
        agents.actors()?.task(tasks[3].id).await?.status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(
        agents.actors()?.task(reminder.id).await?.status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(agents.actors()?.runs(actor).await?.len(), 5);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"agent_id":actor,"restart_verified":true,"paused":paused,
        "supervision":agents.actors()?.supervision(actor).await?,"retained_task":agents.actors()?.task(tasks[3].id).await?,
        "reminder_task":agents.actors()?.task(reminder.id).await?,"memory":agents.actors()?.memory(actor).await?})
        )?
    );
    Ok(())
}
