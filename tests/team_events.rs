//! A timeline is saved Run history scoped by real goal ownership, with reconnectable order.
use act0r::{
    Engine,
    application::{AgentService, TaskInspectionOptions, TeamEventPage},
    bootstrap::local_app,
    domain::{actors::AgentMessage, teams::*, timeline::*, *},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

fn manifest(root: &Path) -> TeamSpec {
    let source = root.join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("README.md"), "Draft\n").unwrap();
    let mut spec: TeamSpec =
        serde_json::from_str(include_str!("../examples/configs/team-inbox.json")).unwrap();
    spec.source = Some(
        fs::canonicalize(source)
            .unwrap()
            .to_string_lossy()
            .into_owned(),
    );
    spec
}
async fn setup(root: &Path) -> (Engine, AgentService, TeamRecord, MessageId) {
    let (engine, agents) = local_app(root.join("events.db")).await.unwrap();
    let team = agents
        .teams()
        .unwrap()
        .create(manifest(root))
        .await
        .unwrap();
    let task = agents
        .teams()
        .unwrap()
        .submit(team.id, MessageId::new(), "Reviewed edit".into())
        .await
        .unwrap();
    (engine, agents, team, task)
}
async fn settle(engine: &Engine) {
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..300 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("fixture did not settle");
}
async fn page(
    agents: &AgentService,
    team: TeamId,
    task: MessageId,
    after: Option<EventCursor>,
    limit: u32,
) -> TeamEventPage {
    agents
        .teams()
        .unwrap()
        .events()
        .unwrap()
        .read(
            team,
            task,
            EventQuery {
                after,
                limit,
                ..Default::default()
            },
        )
        .await
        .unwrap()
}
async fn pool(db: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(db)
            .foreign_keys(true),
    )
    .await
    .unwrap()
}
async fn respond_all(agents: &AgentService, team: TeamId, task: MessageId) {
    let tasks = agents.teams().unwrap().tasks().unwrap();
    let inbox = tasks
        .inbox(team, task, TaskInspectionOptions::default())
        .await
        .unwrap();
    for item in inbox.items {
        tasks
            .respond(
                team,
                task,
                item.wait.id,
                match item.kind {
                    HumanWaitKind::Question => HumanResponse::Answer {
                        answer: "durability".into(),
                    },
                    HumanWaitKind::ToolApproval => HumanResponse::Approval { approved: true },
                    _ => panic!("unexpected wait"),
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn pagination_recovers_every_actor_event_once_after_restart_and_vacuum() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(dir.path()).await;
    assert!(
        page(&agents, team.id, task, None, 512)
            .await
            .events
            .is_empty()
    );
    settle(&engine).await;
    let first = page(&agents, team.id, task, None, 3).await;
    assert!(first.has_more);
    assert_eq!(first.events.len(), 3);
    let replay = page(&agents, team.id, task, None, 3).await;
    assert_eq!(json!(first), json!(replay));
    let mut delivered = first.events;
    let mut cursor = first.next_cursor;
    drop(agents);
    drop(engine);
    let p = pool(&dir.path().join("events.db")).await;
    sqlx::query("VACUUM").execute(&p).await.unwrap();
    p.close().await;
    let (engine, agents) = local_app(dir.path().join("events.db")).await.unwrap();
    respond_all(&agents, team.id, task).await;
    settle(&engine).await;
    loop {
        let batch = page(&agents, team.id, task, Some(cursor), 3).await;
        assert!(batch.event_bytes <= batch.query.max_bytes);
        cursor = batch.next_cursor;
        delivered.extend(batch.events);
        if !batch.has_more {
            break;
        }
    }
    let full = page(&agents, team.id, task, None, 512).await;
    assert!(!full.has_more);
    assert_eq!(json!(delivered), json!(full.events));
    assert_eq!(cursor, full.next_cursor);
    assert!(
        page(&agents, team.id, task, Some(cursor), 2)
            .await
            .events
            .is_empty()
    );
    let mut sequences = HashMap::new();
    let mut ids = HashSet::new();
    let mut members = HashSet::new();
    for item in &delivered {
        assert!(ids.insert(item.event.id));
        members.insert(item.member.as_deref().unwrap());
        let last = sequences.entry(item.event.run_id).or_insert(0);
        assert_eq!(item.event.sequence, *last + 1);
        *last = item.event.sequence;
    }
    assert_eq!(members, HashSet::from(["planner", "editor", "reviewer"]));
    assert!(delivered.windows(2).all(|w| w[0].position < w[1].position));
    assert_eq!(
        delivered
            .iter()
            .filter(|e| e.event.kind == "input_received")
            .count(),
        2
    );
    assert_eq!(
        delivered
            .iter()
            .filter(|e| e.event.kind == "run_succeeded")
            .count(),
        3
    );
    let child = delivered
        .iter()
        .find(|e| e.task_id != task)
        .unwrap()
        .task_id;
    let by_child = page(&agents, team.id, child, None, 512).await;
    assert_eq!(by_child.root_task_id, task);
    assert_eq!(json!(by_child.events), json!(full.events));
    assert!(
        page(&agents, team.id, child, Some(cursor), 512)
            .await
            .events
            .is_empty()
    );
}

#[tokio::test]
async fn cursor_before_admission_skips_unrelated_runs_without_losing_new_child_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(dir.path()).await;
    let planner = team.planner().unwrap().agent_id;
    agents.actors().unwrap().pause(planner).await.unwrap();
    let worker = team
        .members
        .iter()
        .find(|m| m.name == "reviewer")
        .unwrap()
        .agent_id;
    let unrelated = MessageId::new();
    agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            id: unrelated,
            recipient: worker,
            sender: Some(planner),
            correlation_id: Some(task),
            kind: "task".into(),
            payload: json!({"goal":"Independent, despite correlation"}),
        })
        .await
        .unwrap();
    settle(&engine).await;
    let empty = page(&agents, team.id, task, None, 512).await;
    assert!(empty.events.is_empty());
    assert!(empty.next_cursor.position() > 0);
    agents.actors().unwrap().resume(planner).await.unwrap();
    settle(&engine).await;
    // The unrelated review occupies the same actor's mailbox; answering it permits the owned child.
    let unrelated_status = agents.actors().unwrap().task(unrelated).await.unwrap();
    let unrelated_run = unrelated_status.run_id.unwrap();
    let wait = engine
        .status(unrelated_run)
        .await
        .unwrap()
        .record()
        .wait
        .clone()
        .unwrap();
    engine
        .signal(unrelated_run, wait.id, json!({"answer":"continue"}))
        .await
        .unwrap();
    settle(&engine).await;
    let owned = page(&agents, team.id, task, Some(empty.next_cursor), 512).await;
    assert!(!owned.events.is_empty());
    assert!(owned.events.iter().all(|e| e.task_id != unrelated));
    assert_eq!(
        json!(owned.events),
        json!(page(&agents, team.id, task, None, 512).await.events)
    );
    assert!(
        agents
            .teams()
            .unwrap()
            .events()
            .unwrap()
            .read(team.id, unrelated, EventQuery::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cursors_reject_other_goals_teams_streams_and_future_positions() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(dir.path()).await;
    settle(&engine).await;
    let first = page(&agents, team.id, task, None, 512).await;
    let second = agents
        .teams()
        .unwrap()
        .submit(team.id, MessageId::new(), "Another goal".into())
        .await
        .unwrap();
    let service = agents.teams().unwrap().events().unwrap();
    let query = EventQuery {
        after: Some(first.next_cursor),
        ..Default::default()
    };
    assert!(service.read(team.id, second, query).await.is_err());
    assert!(service.read(TeamId::new(), task, query).await.is_err());
    assert!(
        service
            .read(team.id, MessageId::new(), query)
            .await
            .is_err()
    );
    let text = first.next_cursor.to_string();
    let mut fields: Vec<_> = text.split(':').map(str::to_owned).collect();
    fields[1] = EventStreamId::new().to_string();
    assert!(
        service
            .read(
                team.id,
                task,
                EventQuery {
                    after: Some(fields.join(":").parse().unwrap()),
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    fields = text.split(':').map(str::to_owned).collect();
    fields[4] = (first.snapshot_position + 1).to_string();
    assert!(
        service
            .read(
                team.id,
                task,
                EventQuery {
                    after: Some(fields.join(":").parse().unwrap()),
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    let p = pool(&dir.path().join("events.db")).await;
    sqlx::query("UPDATE history_stream SET id=?")
        .bind(EventStreamId::new().to_string())
        .execute(&p)
        .await
        .unwrap();
    assert!(service.read(team.id, task, query).await.is_err());
    p.close().await;
}

#[tokio::test]
async fn byte_boundaries_and_oversized_events_never_skip_history() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(dir.path()).await;
    settle(&engine).await;
    let service = agents.teams().unwrap().events().unwrap();
    let first = page(&agents, team.id, task, None, 1).await;
    let too_small = service
        .read(
            team.id,
            task,
            EventQuery {
                max_bytes: first.event_bytes - 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(too_small.events.is_empty());
    assert!(too_small.has_more);
    assert_eq!(too_small.next_cursor.position(), 0);
    assert_eq!(
        too_small.oversized_event.as_ref().unwrap().event_id,
        first.events[0].event.id
    );
    assert_eq!(
        too_small.oversized_event.unwrap().required_bytes,
        u64::from(first.event_bytes)
    );
    let exact = service
        .read(
            team.id,
            task,
            EventQuery {
                max_bytes: first.event_bytes,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(exact.events.len(), 1);
    assert_eq!(exact.next_cursor, first.next_cursor);
    // A later oversized event returns earlier deliverable events and checkpoints only those.
    let second = page(&agents, team.id, task, Some(first.next_cursor), 1).await;
    let p = pool(&dir.path().join("events.db")).await;
    sqlx::query("UPDATE history SET payload=json_set(payload,'$.payload.large',?) WHERE id=?")
        .bind("é".repeat(4096))
        .bind(second.events[0].event.id.to_string())
        .execute(&p)
        .await
        .unwrap();
    let partial = service
        .read(
            team.id,
            task,
            EventQuery {
                max_bytes: 4096,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(partial.events.len(), 1);
    assert_eq!(partial.next_cursor, first.next_cursor);
    let blocked = service
        .read(
            team.id,
            task,
            EventQuery {
                after: Some(partial.next_cursor),
                max_bytes: 4096,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(blocked.events.is_empty());
    assert_eq!(blocked.next_cursor, partial.next_cursor);
    assert_eq!(
        blocked.oversized_event.unwrap().event_id,
        second.events[0].event.id
    );
    assert!(
        !service
            .read(
                team.id,
                task,
                EventQuery {
                    after: Some(partial.next_cursor),
                    max_bytes: 16384,
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .events
            .is_empty()
    );
    p.close().await;
}

#[tokio::test]
async fn index_failure_rolls_back_input_receipts_run_transition_and_all_events() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(dir.path()).await;
    settle(&engine).await;
    let before = page(&agents, team.id, task, None, 512).await;
    let tasks = agents.teams().unwrap().tasks().unwrap();
    let inbox = tasks
        .inbox(team.id, task, TaskInspectionOptions::default())
        .await
        .unwrap();
    let ask = inbox
        .items
        .iter()
        .find(|i| i.kind == HumanWaitKind::Question)
        .unwrap();
    let run_before = json!(engine.status(ask.run_id).await.unwrap().record());
    let p = pool(&dir.path().join("events.db")).await;
    sqlx::raw_sql("CREATE TRIGGER reject_index BEFORE INSERT ON history_positions BEGIN SELECT RAISE(ABORT,'index fault'); END;").execute(&p).await.unwrap();
    let response = HumanResponse::Answer {
        answer: "durability".into(),
    };
    assert!(
        tasks
            .respond(team.id, task, ask.wait.id, response.clone())
            .await
            .is_err()
    );
    assert_eq!(
        run_before,
        json!(engine.status(ask.run_id).await.unwrap().record())
    );
    assert_eq!(
        json!(before),
        json!(page(&agents, team.id, task, None, 512).await)
    );
    sqlx::query("DROP TRIGGER reject_index")
        .execute(&p)
        .await
        .unwrap();
    tasks
        .respond(team.id, task, ask.wait.id, response.clone())
        .await
        .unwrap();
    let after = page(&agents, team.id, task, Some(before.next_cursor), 512).await;
    assert_eq!(
        after
            .events
            .iter()
            .filter(|e| e.event.kind == "input_received")
            .count(),
        1
    );
    assert!(
        tasks
            .respond(team.id, task, ask.wait.id, response)
            .await
            .unwrap()
            .replayed
    );
    assert!(
        page(&agents, team.id, task, Some(after.next_cursor), 512)
            .await
            .events
            .is_empty()
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM history h LEFT JOIN history_positions p ON h.id=p.event_id WHERE p.event_id IS NULL").fetch_one(&p).await.unwrap(),0);
    p.close().await;
}

#[tokio::test]
async fn follow_up_runs_append_to_the_original_goal_after_an_empty_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("events.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut spec = manifest(dir.path());
    spec.planner.config["follow_up"] =
        json!({"max_follow_ups":1,"min_delay_ms":50,"max_delay_ms":1000});
    spec.planner.config["script"] = json!([
        {"kind":"ask_human","question":"Which review?"},
        {"kind":"schedule_follow_up","summary":"Review selected","goal":"Continue","after_ms":50}
    ]);
    let team = agents.teams().unwrap().create(spec).await.unwrap();
    let task = agents
        .teams()
        .unwrap()
        .submit(team.id, MessageId::new(), "Review".into())
        .await
        .unwrap();
    settle(&engine).await;
    respond_all(&agents, team.id, task).await;
    // Hold mailbox admission so a slow host cannot start the follow-up before our checkpoint.
    agents
        .actors()
        .unwrap()
        .pause(team.planner().unwrap().agent_id)
        .await
        .unwrap();
    settle(&engine).await;
    let before = page(&agents, team.id, task, None, 512).await;
    assert!(
        page(&agents, team.id, task, Some(before.next_cursor), 512)
            .await
            .events
            .is_empty()
    );
    drop(agents);
    drop(engine);
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    agents
        .actors()
        .unwrap()
        .resume(team.planner().unwrap().agent_id)
        .await
        .unwrap();
    settle(&engine).await;
    let after = page(&agents, team.id, task, Some(before.next_cursor), 512).await;
    assert!(after.events.iter().any(|e| e.task_id != task
        && e.member.as_deref() == Some("planner")
        && e.event.kind == "input_wait_created"));
    assert!(
        after
            .events
            .iter()
            .all(|e| e.position > before.snapshot_position)
    );
    let child = after
        .events
        .iter()
        .find(|e| e.task_id != task)
        .unwrap()
        .task_id;
    assert_eq!(
        json!(after.events),
        json!(
            page(&agents, team.id, child, Some(before.next_cursor), 512)
                .await
                .events
        )
    );
}

#[test]
fn cursor_contract_is_versioned_and_query_limits_are_validated() {
    let cursor =
        EventCursor::new(EventStreamId::new(), TeamId::new(), MessageId::new(), 7).unwrap();
    assert_eq!(cursor, cursor.to_string().parse().unwrap());
    assert_eq!(cursor, serde_json::from_value(json!(cursor)).unwrap());
    for malformed in ["", "v1", "v2:a:b:c:1", "v1:a:b:c:1"] {
        assert!(malformed.parse::<EventCursor>().is_err());
    }
    let text = cursor.to_string();
    let prefix = text.rsplit_once(':').unwrap().0;
    for position in [
        "-1",
        "+1",
        "",
        "1.0",
        "9223372036854775808",
        "18446744073709551616",
        "1:extra",
    ] {
        assert!(
            format!("{prefix}:{position}")
                .parse::<EventCursor>()
                .is_err()
        );
    }
    assert!(
        EventCursor::new(
            EventStreamId::new(),
            TeamId::new(),
            MessageId::new(),
            u64::MAX
        )
        .is_err()
    );
    for query in [
        EventQuery {
            limit: 0,
            ..Default::default()
        },
        EventQuery {
            limit: 513,
            ..Default::default()
        },
        EventQuery {
            max_bytes: 0,
            ..Default::default()
        },
        EventQuery {
            max_bytes: MAX_EVENT_PAGE_BYTES + 1,
            ..Default::default()
        },
    ] {
        assert!(query.validate().is_err());
    }
}

#[tokio::test]
async fn concurrent_execution_and_snapshot_pages_preserve_committed_order() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(dir.path()).await;
    let worker = engine.worker().await.unwrap();
    let execution = act0r::adapters::tokio_worker::run(
        worker,
        act0r::adapters::tokio_worker::WorkerOptions {
            until_idle: true,
            max_concurrent_activities: 2,
            ..Default::default()
        },
    );
    let reads = async {
        let mut cursor = None;
        let mut events = Vec::new();
        for _ in 0..30 {
            let batch = page(&agents, team.id, task, cursor, 2).await;
            assert!(
                batch
                    .events
                    .iter()
                    .all(|e| e.position <= batch.snapshot_position)
            );
            cursor = Some(batch.next_cursor);
            events.extend(batch.events);
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        loop {
            let batch = page(&agents, team.id, task, cursor, 2).await;
            cursor = Some(batch.next_cursor);
            events.extend(batch.events);
            if !batch.has_more {
                break;
            }
        }
        (cursor, events)
    };
    let (result, (cursor, mut seen)) = tokio::join!(execution, reads);
    result.unwrap();
    seen.extend(page(&agents, team.id, task, cursor, 512).await.events);
    let full = page(&agents, team.id, task, None, 512).await;
    assert_eq!(json!(seen), json!(full.events));
    // Clock corrections cannot reorder this stream.
    let p = pool(&dir.path().join("events.db")).await;
    sqlx::query("UPDATE history SET payload=json_set(payload,'$.at',-sequence)")
        .execute(&p)
        .await
        .unwrap();
    let altered = page(&agents, team.id, task, None, 512).await;
    assert_eq!(
        full.events.iter().map(|e| e.event.id).collect::<Vec<_>>(),
        altered
            .events
            .iter()
            .map(|e| e.event.id)
            .collect::<Vec<_>>()
    );
    p.close().await;
}

fn cli(db: &Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(db)
        .arg("--json")
        .args(args)
        .output()
        .unwrap()
}
struct Watch {
    child: std::process::Child,
    pages: std::sync::mpsc::Receiver<String>,
}
impl Watch {
    fn start(db: &Path, team: TeamId, task: MessageId, cursor: &Path) -> Self {
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"))
            .arg("--db")
            .arg(db)
            .arg("--json")
            .args(["team", "task", "watch"])
            .arg(team.to_string())
            .arg(task.to_string())
            .arg("--cursor-file")
            .arg(cursor)
            .args(["--poll-ms", "10"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, pages) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else {
                    break;
                };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self { child, pages }
    }
    fn next(&self, cursor: &Path) -> Value {
        let line = self
            .pages
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("watch must flush a page");
        let page: Value = serde_json::from_str(&line).unwrap();
        let expected = page["next_cursor"].as_str().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while fs::read_to_string(cursor).ok().as_deref().map(str::trim) != Some(expected) {
            assert!(
                std::time::Instant::now() < deadline,
                "checkpoint not committed after output"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        page
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_watcher_reconnects_after_kill_without_owning_or_running_the_worker() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("events.db");
    let cursor = dir.path().join("watch.cursor");
    let (engine, agents, team, task) = setup(dir.path()).await;
    settle(&engine).await;
    let first = page(&agents, team.id, task, None, 512).await;
    fs::write(&cursor, format!("{}\n", first.next_cursor)).unwrap();
    let worker = engine.worker().await.unwrap(); // A watcher must coexist with the executor lease.
    let watch = Watch::start(&db, team.id, task, &cursor);
    let initial = watch.next(&cursor);
    assert!(initial["events"].as_array().unwrap().is_empty());
    let conflict = cli(
        &db,
        &[
            "team",
            "task",
            "watch",
            &team.id.to_string(),
            &task.to_string(),
            "--cursor-file",
            cursor.to_str().unwrap(),
        ],
    );
    assert!(!conflict.status.success());
    assert!(String::from_utf8_lossy(&conflict.stderr).contains("another watcher"));
    let waiting = agents
        .teams()
        .unwrap()
        .tasks()
        .unwrap()
        .inbox(team.id, task, TaskInspectionOptions::default())
        .await
        .unwrap();
    let ask = waiting
        .items
        .iter()
        .find(|i| i.kind == HumanWaitKind::Question)
        .unwrap();
    agents
        .teams()
        .unwrap()
        .tasks()
        .unwrap()
        .respond(
            team.id,
            task,
            ask.wait.id,
            HumanResponse::Answer {
                answer: "durability".into(),
            },
        )
        .await
        .unwrap();
    let response = watch.next(&cursor);
    assert!(
        response["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["event"]["kind"] == "input_received")
    );
    let saved = fs::read_to_string(&cursor).unwrap();
    drop(watch);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    respond_all(&agents, team.id, task).await;
    settle(&engine).await;
    assert_eq!(fs::read_to_string(&cursor).unwrap(), saved);
    let watch = Watch::start(&db, team.id, task, &cursor);
    let missed = watch.next(&cursor);
    assert!(
        missed["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["event"]["kind"] == "run_succeeded")
    );
    let combined = first
        .events
        .iter()
        .map(|e| json!(e))
        .chain(response["events"].as_array().unwrap().iter().cloned())
        .chain(missed["events"].as_array().unwrap().iter().cloned())
        .collect::<Vec<_>>();
    assert_eq!(
        json!(combined),
        json!(page(&agents, team.id, task, None, 512).await.events)
    );
    drop(watch);
    let output = cli(
        &db,
        &[
            "team",
            "task",
            "events",
            &team.id.to_string(),
            &task.to_string(),
            "--after",
            saved.trim(),
            "--limit",
            "2",
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["events"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    // Bad/cross-goal cursors are preserved for inspection instead of silently replacing them.
    fs::write(&cursor, "broken").unwrap();
    let bad = cli(
        &db,
        &[
            "team",
            "task",
            "watch",
            &team.id.to_string(),
            &task.to_string(),
            "--cursor-file",
            cursor.to_str().unwrap(),
        ],
    );
    assert!(!bad.status.success());
    assert!(bad.stdout.is_empty());
    assert_eq!(fs::read_to_string(&cursor).unwrap(), "broken");
    let second = agents
        .teams()
        .unwrap()
        .submit(team.id, MessageId::new(), "Another goal".into())
        .await
        .unwrap();
    fs::write(&cursor, &saved).unwrap();
    let bad = cli(
        &db,
        &[
            "team",
            "task",
            "watch",
            &team.id.to_string(),
            &second.to_string(),
            "--cursor-file",
            cursor.to_str().unwrap(),
        ],
    );
    assert!(!bad.status.success());
    assert!(bad.stdout.is_empty());
    assert_eq!(fs::read_to_string(&cursor).unwrap(), saved);
    let bad = cli(
        &db,
        &[
            "team",
            "task",
            "events",
            &team.id.to_string(),
            &task.to_string(),
            "--limit",
            "0",
        ],
    );
    assert!(!bad.status.success());
}
