use act0r::{
    Engine, Error,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::AgentMessage, agent::*, harness::*, sessions::*, *},
};
use serde_json::json;
use std::{fs, path::Path};

fn request(prompt: &str) -> HarnessRequest {
    HarnessRequest {
        auto_execute: false,
        discover_project: false,
        budget: None,
        message_id: MessageId::new(),
        prompt: prompt.into(),
        workspace: None,
        coding: None,
        target: ModelTarget {
            provider: "openai".into(),
            model: "session-fixture".into(),
            endpoint: "http://127.0.0.1:1/v1/chat/completions".into(),
            protocol: ModelProtocol::ChatCompletions,
            action_format: Default::default(),
            policy: Default::default(),
        },
    }
}
fn change(revision: u64, change: SessionChange) -> SessionUpdate {
    SessionUpdate {
        expected_revision: revision,
        change,
    }
}
async fn settle(engine: &Engine, agents: &AgentService, receipt: &HarnessReceipt) {
    agents
        .actors()
        .unwrap()
        .cancel_goal(receipt.task_id)
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..100 {
        if !worker.step().await.unwrap() {
            break;
        }
    }
}
async fn pool(database: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(database))
        .await
        .unwrap()
}

#[test]
fn strict_metadata_requests_and_latest_receipts_protect_newer_decisions() {
    let rename: SessionUpdate =
        serde_json::from_value(json!({"expected_revision":0,"kind":"rename","title":"Project 🦀"}))
            .unwrap();
    let mut metadata = SessionMetadata::default();
    assert!(!metadata.apply(&rename, None).unwrap());
    assert!(metadata.apply(&rename, None).unwrap());
    assert!(
        !metadata
            .apply(&change(1, SessionChange::Archive), None)
            .unwrap()
    );
    assert!(metadata.apply(&rename, None).is_err());
    assert!(
        !metadata
            .apply(&change(2, SessionChange::Restore), None)
            .unwrap()
    );
    assert!(
        metadata
            .apply(&change(1, SessionChange::Archive), None)
            .is_err()
    );
    for value in [
        json!({"expected_revision":0,"kind":"rename","title":"Title","extra":true}),
        json!({"expected_revision":0,"kind":"archive","title":"extra"}),
        json!({"expected_revision":0,"kind":"delete"}),
    ] {
        assert!(serde_json::from_value::<SessionUpdate>(value).is_err());
    }
    for title in [" ".to_string(), "bad\nname".into(), "🦀".repeat(65)] {
        assert!(
            change(0, SessionChange::Rename { title })
                .validate()
                .is_err()
        );
    }
    assert!(
        change(i64::MAX as u64, SessionChange::Restore)
            .validate()
            .is_err()
    );
}

#[tokio::test]
async fn organization_preserves_pins_evidence_and_receipts_across_restart() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("sessions.db");
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("keep.txt"), "Original").unwrap();
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut input = request("Original request");
    input.workspace = Some(fs::canonicalize(&source).unwrap().to_string_lossy().into());
    let harness = agents.teams().unwrap().harness().unwrap();
    let receipt = harness.start(input.clone()).await.unwrap();
    let pins =
        serde_json::to_value(agents.teams().unwrap().get(receipt.team_id).await.unwrap()).unwrap();
    let rename = change(
        0,
        SessionChange::Rename {
            title: "  Project 🦀  ".into(),
        },
    );
    harness
        .update_session(receipt.session_id, rename.clone())
        .await
        .unwrap();
    assert!(
        harness
            .session_metadata(receipt.session_id)
            .await
            .unwrap()
            .archive_blocked_reason
            .is_some()
    );
    assert!(matches!(
        harness
            .update_session(receipt.session_id, change(1, SessionChange::Archive))
            .await,
        Err(Error::Domain(DomainError::SessionConflict(_)))
    ));
    settle(&engine, &agents, &receipt).await;
    let archive = change(1, SessionChange::Archive);
    let saved = harness
        .update_session(receipt.session_id, archive.clone())
        .await
        .unwrap();
    assert!(!saved.replayed);
    assert!(saved.view.metadata.archived);
    assert!(harness.sessions().await.unwrap().items.is_empty());
    assert_eq!(
        harness
            .sessions_page(SessionListQuery {
                archived: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .items[0]
            .title
            .as_deref(),
        Some("Project 🦀")
    );
    let conversation = harness
        .conversation(receipt.session_id, None)
        .await
        .unwrap();
    assert!(conversation.archived);
    assert_eq!(conversation.turns[0].prompt, input.prompt);
    // Lost start acknowledgements remain readable without reopening or duplicating work.
    assert_eq!(harness.start(input).await.unwrap().task_id, receipt.task_id);
    assert!(
        harness
            .session_metadata(receipt.session_id)
            .await
            .unwrap()
            .metadata
            .archived
    );
    let follow = HarnessTurnRequest {
        message_id: MessageId::new(),
        prompt: "Continue".into(),
    };
    assert!(
        harness
            .append(receipt.session_id, follow.clone())
            .await
            .is_err()
    );
    assert_eq!(
        serde_json::to_value(agents.teams().unwrap().get(receipt.team_id).await.unwrap()).unwrap(),
        pins
    );
    let workspace = pins["members"][0]["workspace"]["path"].as_str().unwrap();
    assert_eq!(
        fs::read_to_string(Path::new(workspace).join("keep.txt")).unwrap(),
        "Original"
    );
    assert_eq!(
        fs::read_to_string(source.join("keep.txt")).unwrap(),
        "Original"
    );
    drop(agents);
    drop(engine);
    let (_, agents) = local_app(&db).await.unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    assert!(
        harness
            .update_session(receipt.session_id, archive.clone())
            .await
            .unwrap()
            .replayed
    );
    harness
        .update_session(receipt.session_id, change(2, SessionChange::Restore))
        .await
        .unwrap();
    assert!(
        harness
            .update_session(receipt.session_id, archive)
            .await
            .is_err()
    );
    let next = harness.append(receipt.session_id, follow).await.unwrap();
    assert_eq!(next.team_id, receipt.team_id);
    assert_eq!(next.sequence, 2);
    let metadata = harness
        .session_metadata(receipt.session_id)
        .await
        .unwrap()
        .metadata;
    assert_eq!(metadata.title.as_deref(), Some("Project 🦀"));
    assert!(!metadata.archived);
    assert_eq!(metadata.revision, 3);
}

#[tokio::test]
async fn archive_and_follow_up_admission_share_one_atomic_boundary() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("race.db")).await.unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    let receipt = harness.start(request("Race")).await.unwrap();
    settle(&engine, &agents, &receipt).await;
    let (archived, appended) = tokio::join!(
        harness.update_session(receipt.session_id, change(0, SessionChange::Archive)),
        harness.append(
            receipt.session_id,
            HarnessTurnRequest {
                message_id: MessageId::new(),
                prompt: "New work".into()
            }
        )
    );
    assert_ne!(archived.is_ok(), appended.is_ok());
    let view = harness.session_metadata(receipt.session_id).await.unwrap();
    assert_eq!(view.metadata.archived, archived.is_ok());
    assert_eq!(
        harness
            .conversation(receipt.session_id, None)
            .await
            .unwrap()
            .turns
            .len(),
        if appended.is_ok() { 2 } else { 1 }
    );
}

#[tokio::test]
async fn reminders_and_advanced_admission_cannot_hide_work_in_archived_sessions() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("wake.db")).await.unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    let receipt = harness.start(request("Wake")).await.unwrap();
    settle(&engine, &agents, &receipt).await;
    let planner = agents
        .teams()
        .unwrap()
        .get(receipt.team_id)
        .await
        .unwrap()
        .members[0]
        .agent_id;
    harness
        .update_session(receipt.session_id, change(0, SessionChange::Archive))
        .await
        .unwrap();
    let message = AgentMessage {
        id: MessageId::new(),
        recipient: planner,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload: json!({"goal":"Later"}),
    };
    agents
        .actors()
        .unwrap()
        .schedule_task(
            message.clone(),
            TimerDeadline::After {
                milliseconds: 60000,
            },
        )
        .await
        .unwrap();
    let metadata = harness.session_metadata(receipt.session_id).await.unwrap();
    assert!(!metadata.metadata.archived);
    assert_eq!(metadata.metadata.revision, 2);
    assert!(metadata.archive_blocked_reason.is_some());
    assert!(
        harness
            .update_session(receipt.session_id, change(2, SessionChange::Archive))
            .await
            .is_err()
    );
    agents
        .actors()
        .unwrap()
        .cancel_reminder(message.id)
        .await
        .unwrap();
    harness
        .update_session(receipt.session_id, change(2, SessionChange::Archive))
        .await
        .unwrap();
    let task = MessageId::new();
    agents
        .teams()
        .unwrap()
        .submit(receipt.team_id, task, "Advanced work".into())
        .await
        .unwrap();
    let metadata = harness.session_metadata(receipt.session_id).await.unwrap();
    assert!(!metadata.metadata.archived);
    assert_eq!(metadata.metadata.revision, 4);
    assert!(metadata.archive_blocked_reason.is_some());
    assert!(
        harness
            .update_session(receipt.session_id, change(2, SessionChange::Archive))
            .await
            .is_err()
    );
    assert_eq!(harness.sessions().await.unwrap().items.len(), 1);
}

#[tokio::test]
async fn whole_session_checks_refuse_pending_applications_and_uncertain_workspace_runs() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("protected.db");
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    let (engine, agents) = local_app(&db).await.unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    let mut input = request("Protected");
    input.workspace = Some(fs::canonicalize(source).unwrap().to_string_lossy().into());
    let receipt = harness.start(input).await.unwrap();
    settle(&engine, &agents, &receipt).await;
    let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
    let workspace = &team.members[0].workspace.as_ref().unwrap().path;
    let pool = pool(&db).await;
    for (status, workflow, input) in [
        (
            "waiting",
            "workspace.apply",
            json!({"task_id":receipt.task_id}),
        ),
        (
            "blocked",
            "fixture",
            json!({"nested":{"workspace":workspace}}),
        ),
    ] {
        let id = RunId::new();
        sqlx::query("INSERT INTO runs(id,status,created_at,payload) VALUES(?,?,0,?)")
            .bind(id.to_string())
            .bind(status)
            .bind(json!({"workflow":{"name":workflow},"input":input}).to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            harness
                .session_metadata(receipt.session_id)
                .await
                .unwrap()
                .archive_blocked_reason
                .is_some()
        );
        assert!(
            harness
                .update_session(receipt.session_id, change(0, SessionChange::Archive))
                .await
                .is_err()
        );
        sqlx::query("DELETE FROM runs WHERE id=?")
            .bind(id.to_string())
            .execute(&pool)
            .await
            .unwrap();
    }
    harness
        .update_session(receipt.session_id, change(0, SessionChange::Archive))
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs(id,status,created_at,payload) VALUES(?,'blocked',0,?)")
        .bind(RunId::new().to_string())
        .bind(json!({"input":{"workspace":workspace}}).to_string())
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        !harness
            .session_metadata(receipt.session_id)
            .await
            .unwrap()
            .metadata
            .archived
    );
    assert_eq!(harness.sessions().await.unwrap().items.len(), 1);
    pool.close().await;
}

#[tokio::test]
async fn pagination_keeps_creation_order_after_followups_new_sessions_reopen_and_vacuum() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("pages.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    let mut receipts = Vec::new();
    for n in 0..55 {
        receipts.push(
            harness
                .start(request(&format!("Request {n}")))
                .await
                .unwrap(),
        );
    }
    let first = harness
        .sessions_page(SessionListQuery {
            limit: 7,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(first.items.len(), 7);
    assert!(first.truncated);
    let cursor = first.next_after.unwrap();
    let newest = harness
        .start(request("Inserted after the page"))
        .await
        .unwrap();
    harness
        .append(
            receipts[0].session_id,
            HarnessTurnRequest {
                message_id: MessageId::new(),
                prompt: "A recent follow-up must not reorder creation pagination".into(),
            },
        )
        .await
        .unwrap();
    assert!(
        harness
            .sessions_page(SessionListQuery {
                archived: true,
                after: Some(cursor),
                limit: 7
            })
            .await
            .is_err()
    );
    let bad =
        SessionListCursor::new(cursor.stream, false, cursor.snapshot + 100, cursor.before).unwrap();
    assert!(
        harness
            .sessions_page(SessionListQuery {
                after: Some(bad),
                ..Default::default()
            })
            .await
            .is_err()
    );
    drop(agents);
    drop(engine);
    let pool = pool(&db).await;
    sqlx::query("VACUUM").execute(&pool).await.unwrap();
    pool.close().await;
    let (_, agents) = local_app(&db).await.unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    let mut seen: Vec<_> = first.items.iter().map(|s| s.session_id).collect();
    let mut after = Some(cursor);
    while let Some(cursor) = after {
        let page = harness
            .sessions_page(SessionListQuery {
                after: Some(cursor),
                limit: 7,
                ..Default::default()
            })
            .await
            .unwrap();
        seen.extend(page.items.iter().map(|s| s.session_id));
        after = page.next_after;
    }
    assert_eq!(seen.len(), 55);
    let mut unique = seen.clone();
    unique.sort_by_key(|id| id.to_string());
    unique.dedup();
    assert_eq!(unique.len(), 55);
    assert!(!seen.contains(&newest.session_id));
    assert_eq!(seen.last(), Some(&receipts[0].session_id));
    let other = root.path().join("other.db");
    let (_, other_agents) = local_app(other).await.unwrap();
    assert!(
        other_agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .sessions_page(SessionListQuery {
                after: Some(cursor),
                ..Default::default()
            })
            .await
            .is_err()
    );
}
