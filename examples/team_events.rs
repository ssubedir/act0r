//! Disconnect at two durable waits, restart execution, and recover all missed goal events.
use act0r::{
    adapters::tokio_worker::{self, WorkerOptions},
    application::{AgentService, TaskInspectionOptions, TeamTimelineEvent},
    bootstrap::local_app,
    domain::{teams::TeamSpec, timeline::*, *},
};
use serde_json::json;
use std::{collections::HashSet, path::PathBuf};

async fn drain(
    agents: &AgentService,
    team: TeamId,
    task: MessageId,
    mut after: Option<EventCursor>,
) -> Result<(Vec<TeamTimelineEvent>, EventCursor), act0r::Error> {
    let mut events = Vec::new();
    loop {
        let page = agents
            .teams()?
            .events()?
            .read(
                team,
                task,
                EventQuery {
                    after,
                    limit: 7,
                    ..Default::default()
                },
            )
            .await?;
        assert!(page.oversized_event.is_none());
        events.extend(page.events);
        after = Some(page.next_cursor);
        if !page.has_more {
            return Ok((events, page.next_cursor));
        }
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| ".build/team-events-demo/demo.db".into()),
    );
    if let Some(parent) = db.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut spec: TeamSpec = serde_json::from_str(include_str!("configs/team-inbox.json"))?;
    spec.name = "timeline-restart-demo".into();
    let source = std::fs::canonicalize(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/team-inbox-source"),
    )?;
    let original = std::fs::read_to_string(source.join("README.md"))?;
    spec.source = Some(source.to_string_lossy().into_owned());
    let (engine, agents) = local_app(&db).await?;
    let team = agents.teams()?.create(spec).await?;
    let task = agents
        .teams()?
        .submit(
            team.id,
            MessageId::new(),
            "Coordinate a reviewed edit".into(),
        )
        .await?;
    tokio_worker::run(
        engine.worker().await?,
        WorkerOptions {
            until_idle: true,
            max_concurrent_activities: 2,
            ..Default::default()
        },
    )
    .await?;
    let waits = agents
        .teams()?
        .tasks()?
        .inbox(team.id, task, TaskInspectionOptions::default())
        .await?;
    assert_eq!(waits.items.len(), 2);
    let (mut delivered, checkpoint) = drain(&agents, team.id, task, None).await?;
    let before = delivered.len();
    let cursor_file = db.with_extension("cursor");
    std::fs::write(&cursor_file, format!("{checkpoint}\n"))?;
    drop(agents);
    drop(engine);

    let (engine, agents) = local_app(&db).await?;
    for item in &waits.items {
        agents
            .teams()?
            .tasks()?
            .respond(
                team.id,
                task,
                item.wait.id,
                match item.kind {
                    HumanWaitKind::Question => HumanResponse::Answer {
                        answer: "Review durability".into(),
                    },
                    HumanWaitKind::ToolApproval => HumanResponse::Approval { approved: true },
                    _ => panic!("unexpected wait"),
                },
            )
            .await?;
    }
    tokio_worker::run(
        engine.worker().await?,
        WorkerOptions {
            until_idle: true,
            max_concurrent_activities: 2,
            ..Default::default()
        },
    )
    .await?;
    let saved: EventCursor = std::fs::read_to_string(&cursor_file)?.trim().parse()?;
    assert_eq!(saved, checkpoint);
    let (missed, cursor) = drain(&agents, team.id, task, Some(saved)).await?;
    let recovered = missed.len();
    delivered.extend(missed);
    let (full, _) = drain(&agents, team.id, task, None).await?;
    assert_eq!(json!(delivered), json!(full));
    assert_eq!(
        delivered
            .iter()
            .map(|e| e.event.id)
            .collect::<HashSet<_>>()
            .len(),
        delivered.len()
    );
    let completed = agents
        .teams()?
        .tasks()?
        .status(team.id, task, TaskInspectionOptions::default())
        .await?;
    assert!(completed.task.summary.settled);
    assert_eq!(completed.task.summary.succeeded, 3);
    assert_eq!(completed.task.summary.visible_usage.admitted_model_calls, 6);
    let editor = team.members.iter().find(|m| m.name == "editor").unwrap();
    assert_eq!(
        std::fs::read_to_string(
            PathBuf::from(&editor.workspace.as_ref().unwrap().path).join("README.md")
        )?,
        original.replace("Draft", "Reviewed")
    );
    assert_eq!(std::fs::read_to_string(source.join("README.md"))?, original);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "team_id":team.id,"task_id":task,"restart_verified":true,"recovered_exact_history":true,
            "events_before_disconnect":before,"missed_events_recovered":recovered,"events_total":delivered.len(),
            "successful_tasks":completed.task.summary.succeeded,"model_calls":completed.task.summary.visible_usage.admitted_model_calls,
            "cursor_file":cursor_file,"next_cursor":cursor,
        }))?
    );
    Ok(())
}
