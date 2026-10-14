//! Two named workers pause, survive restart, and finish through the team inbox.
use act0r::{
    application::TaskInspectionOptions,
    bootstrap::local_app,
    domain::{teams::TeamSpec, *},
};
use serde_json::json;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| ".build/team-inbox-demo/demo.db".into()),
    );
    if let Some(parent) = db.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut spec: TeamSpec = serde_json::from_str(include_str!("configs/team-inbox.json"))?;
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
            "Coordinate the reviewed edit".into(),
        )
        .await?;
    let mut worker = engine.worker().await?;
    while worker.step().await? {}
    let waiting = agents
        .teams()?
        .tasks()?
        .inbox(team.id, task, TaskInspectionOptions::default())
        .await?;
    assert_eq!(waiting.items.len(), 2);
    assert!(waiting.items.iter().all(|i| i.actionable));
    drop(worker);
    drop(agents);
    drop(engine);

    let (engine, agents) = local_app(&db).await?;
    let restored = agents
        .teams()?
        .tasks()?
        .inbox(team.id, task, TaskInspectionOptions::default())
        .await?;
    assert_eq!(json!(waiting), json!(restored));
    for item in &restored.items {
        let response = match item.kind {
            HumanWaitKind::Question => HumanResponse::Answer {
                answer: "Review durability".into(),
            },
            HumanWaitKind::ToolApproval => HumanResponse::Approval { approved: true },
            _ => panic!("unexpected demo wait"),
        };
        let receipt = agents
            .teams()?
            .tasks()?
            .respond(team.id, task, item.wait.id, response.clone())
            .await?;
        assert!(!receipt.replayed);
        assert!(
            agents
                .teams()?
                .tasks()?
                .respond(team.id, task, item.wait.id, response)
                .await?
                .replayed
        );
    }
    let mut worker = engine.worker().await?;
    while worker.step().await? {}
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
        serde_json::to_string_pretty(
            &json!({"restart_verified":true,"waiting":waiting,"completed":completed})
        )?
    );
    Ok(())
}
