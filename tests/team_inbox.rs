//! Team/goal authority, exact native requests, and atomic durable human decisions.
use act0r::{
    Engine, Error,
    application::{AgentService, TaskInspectionOptions, TeamInboxItem, TeamTaskInbox},
    bootstrap::local_app,
    domain::{actors::*, teams::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
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
    let (engine, agents) = local_app(root.join("inbox.db")).await.unwrap();
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
async fn inbox(agents: &AgentService, team: TeamId, task: MessageId) -> TeamTaskInbox {
    agents
        .teams()
        .unwrap()
        .tasks()
        .unwrap()
        .inbox(team, task, TaskInspectionOptions::default())
        .await
        .unwrap()
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
fn question(view: &TeamTaskInbox) -> &TeamInboxItem {
    view.items
        .iter()
        .find(|i| i.kind == HumanWaitKind::Question)
        .unwrap()
}
fn approval(view: &TeamTaskInbox) -> &TeamInboxItem {
    view.items
        .iter()
        .find(|i| i.kind == HumanWaitKind::ToolApproval)
        .unwrap()
}
fn answer(text: &str) -> HumanResponse {
    HumanResponse::Answer {
        answer: text.into(),
    }
}
fn approve(approved: bool) -> HumanResponse {
    HumanResponse::Approval { approved }
}

#[tokio::test]
async fn parallel_native_waits_survive_restart_and_receipts_survive_goal_completion() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(root.path()).await;
    settle(&engine).await;
    let waiting = inbox(&agents, team.id, task).await;
    assert_eq!(waiting.items.len(), 2);
    assert!(waiting.items.iter().all(|i| i.actionable));
    let edit = approval(&waiting);
    let ask = question(&waiting);
    assert_eq!(edit.member.as_deref(), Some("editor"));
    assert_eq!(ask.member.as_deref(), Some("reviewer"));
    assert_eq!(
        edit.wait.payload["arguments"],
        json!({"path":"README.md","old":"Draft","new":"Reviewed"})
    );
    assert_eq!(edit.wait.payload["writable_paths"], json!(["README.md"]));
    assert_eq!(
        edit.wait.payload["workspace"],
        team.members[1].workspace.as_ref().unwrap().path
    );
    let edit_wait = edit.wait.id;
    let ask_wait = ask.wait.id;
    let ask_run = ask.run_id;
    let child = ask.task_id;
    drop(agents);
    drop(engine);

    let (engine, agents) = local_app(root.path().join("inbox.db")).await.unwrap();
    assert_eq!(json!(waiting), json!(inbox(&agents, team.id, task).await));
    let child_view = agents
        .teams()
        .unwrap()
        .tasks()
        .unwrap()
        .status(team.id, child, TaskInspectionOptions::default())
        .await
        .unwrap();
    assert_eq!(child_view.task.root_task_id, task);
    let tasks = agents.teams().unwrap().tasks().unwrap();
    assert!(
        tasks
            .respond(team.id, task, ask_wait, approve(true))
            .await
            .is_err()
    );
    assert!(
        tasks
            .respond(team.id, task, edit_wait, answer("yes"))
            .await
            .is_err()
    );
    assert!(
        tasks
            .respond(team.id, task, ask_wait, answer("  "))
            .await
            .is_err()
    );
    assert!(
        tasks
            .respond(team.id, task, ask_wait, answer(&"é".repeat(32769)))
            .await
            .is_err()
    );
    assert!(
        !tasks
            .respond(team.id, task, ask_wait, answer("Review durability — café"))
            .await
            .unwrap()
            .replayed
    );
    assert!(
        !tasks
            .respond(team.id, child, edit_wait, approve(true))
            .await
            .unwrap()
            .replayed
    );
    let answered = engine.status(ask_run).await.unwrap();
    assert_eq!(answered.record().input_receipts.len(), 1);
    assert!(matches!(
        tasks
            .respond(team.id, task, ask_wait, answer("different"))
            .await,
        Err(Error::Domain(DomainError::ConflictingOutcome))
    ));
    assert!(matches!(
        tasks
            .respond(team.id, task, WaitId::new(), answer("stale"))
            .await,
        Err(Error::Domain(DomainError::UnknownWait))
    ));
    settle(&engine).await;
    let done = tasks
        .status(team.id, task, TaskInspectionOptions::default())
        .await
        .unwrap();
    assert!(done.task.summary.settled);
    assert_eq!(done.task.summary.succeeded, 3);
    assert_eq!(done.task.summary.visible_usage.admitted_model_calls, 6);
    assert!(inbox(&agents, team.id, task).await.items.is_empty());
    let saved = json!(engine.status(ask_run).await.unwrap());
    let events = engine.history(ask_run).await.unwrap().len();
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(root.path().join("inbox.db")).await.unwrap();
    let receipt = agents
        .teams()
        .unwrap()
        .tasks()
        .unwrap()
        .respond(team.id, task, ask_wait, answer("Review durability — café"))
        .await
        .unwrap();
    assert!(receipt.replayed);
    assert_eq!(receipt.task_id, child);
    assert_eq!(saved, json!(engine.status(ask_run).await.unwrap()));
    assert_eq!(events, engine.history(ask_run).await.unwrap().len());
    assert_eq!(
        fs::read_to_string(root.path().join("source/README.md")).unwrap(),
        "Draft\n"
    );
    assert_eq!(
        fs::read_to_string(
            Path::new(&team.members[1].workspace.as_ref().unwrap().path).join("README.md")
        )
        .unwrap(),
        "Reviewed\n"
    );
}

#[tokio::test]
async fn goal_authority_ignores_correlation_and_display_truncation() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(root.path()).await;
    settle(&engine).await;
    let waiting = inbox(&agents, team.id, task).await;
    let ask = question(&waiting);
    let tasks = agents.teams().unwrap().tasks().unwrap();
    let partial = tasks
        .inbox(
            team.id,
            task,
            TaskInspectionOptions {
                max_nodes: 1,
                max_depth: 0,
            },
        )
        .await
        .unwrap();
    assert!(partial.items.is_empty());
    assert_eq!(partial.truncations.len(), 1);
    let mut other = manifest(root.path());
    other.name = "other-team".into();
    let other = agents.teams().unwrap().create(other).await.unwrap();
    assert!(
        tasks
            .inbox(other.id, task, TaskInspectionOptions::default())
            .await
            .is_err()
    );
    assert!(
        tasks
            .respond(other.id, task, ask.wait.id, answer("forged"))
            .await
            .is_err()
    );
    let second = agents
        .teams()
        .unwrap()
        .submit(team.id, MessageId::new(), "Other goal".into())
        .await
        .unwrap();
    assert!(matches!(
        tasks
            .respond(team.id, second, ask.wait.id, answer("forged"))
            .await,
        Err(Error::Domain(DomainError::UnknownWait))
    ));
    let outsider = agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: team.members[2].agent_id,
            sender: None,
            correlation_id: Some(task),
            kind: "task".into(),
            payload: json!({"goal":"Unrelated worker task"}),
        })
        .await
        .unwrap();
    assert!(
        tasks
            .status(team.id, outsider, TaskInspectionOptions::default())
            .await
            .is_err()
    );
    let before = json!(engine.status(ask.run_id).await.unwrap());
    assert_eq!(
        waiting.items.len(),
        inbox(&agents, team.id, task).await.items.len()
    );
    assert_eq!(before, json!(engine.status(ask.run_id).await.unwrap()));
    // Reply lookup is exact and independent of an operator's bounded display.
    assert!(
        !tasks
            .respond(team.id, task, ask.wait.id, answer("bounded view reply"))
            .await
            .unwrap()
            .replayed
    );
}

#[tokio::test]
async fn concurrent_duplicate_and_conflicting_responses_record_only_one_transition() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(root.path()).await;
    settle(&engine).await;
    let waiting = inbox(&agents, team.id, task).await;
    let tasks = agents.teams().unwrap().tasks().unwrap();
    let ask = question(&waiting);
    let (a, b) = tokio::join!(
        tasks.respond(team.id, task, ask.wait.id, answer("same")),
        tasks.respond(team.id, task, ask.wait.id, answer("same"))
    );
    assert_ne!(a.unwrap().replayed, b.unwrap().replayed);
    let edit = approval(&waiting);
    let (a, b) = tokio::join!(
        tasks.respond(team.id, task, edit.wait.id, approve(true)),
        tasks.respond(team.id, task, edit.wait.id, approve(false))
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let err = a.err().or(b.err()).unwrap();
    assert!(matches!(
        err,
        Error::Domain(DomainError::ConflictingOutcome)
    ));
    for item in &waiting.items {
        assert_eq!(
            engine
                .status(item.run_id)
                .await
                .unwrap()
                .record()
                .input_receipts
                .len(),
            1
        );
        assert_eq!(
            engine
                .history(item.run_id)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.kind == "input_received")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn response_and_continuation_roll_back_together_and_direct_signals_replay() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(root.path()).await;
    settle(&engine).await;
    let waiting = inbox(&agents, team.id, task).await;
    let ask = question(&waiting);
    let before = json!(engine.status(ask.run_id).await.unwrap());
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(root.path().join("inbox.db")),
    )
    .await
    .unwrap();
    sqlx::raw_sql("CREATE TRIGGER refuse_input BEFORE INSERT ON history WHEN json_extract(NEW.payload,'$.kind')='input_received' BEGIN SELECT RAISE(ABORT,'receipt refused'); END;").execute(&pool).await.unwrap();
    let tasks = agents.teams().unwrap().tasks().unwrap();
    assert!(
        tasks
            .respond(team.id, task, ask.wait.id, answer("record me"))
            .await
            .is_err()
    );
    assert_eq!(before, json!(engine.status(ask.run_id).await.unwrap()));
    sqlx::raw_sql("DROP TRIGGER refuse_input;")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    engine
        .signal(ask.run_id, ask.wait.id, json!({"answer":"record me"}))
        .await
        .unwrap();
    assert!(
        tasks
            .respond(team.id, task, ask.wait.id, answer("record me"))
            .await
            .unwrap()
            .replayed
    );
    assert!(
        tasks
            .respond(team.id, task, ask.wait.id, answer("changed"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn goal_pause_preserves_decisions_and_cancellation_refuses_new_input() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(root.path()).await;
    settle(&engine).await;
    let waiting = inbox(&agents, team.id, task).await;
    agents.actors().unwrap().pause_goal(task).await.unwrap();
    let paused = inbox(&agents, team.id, task).await;
    assert!(paused.goal_paused);
    assert!(
        paused
            .items
            .iter()
            .all(|i| i.actionable && i.execution_held)
    );
    let tasks = agents.teams().unwrap().tasks().unwrap();
    let ask = question(&waiting);
    tasks
        .respond(team.id, task, ask.wait.id, answer("keep paused"))
        .await
        .unwrap();
    let calls = tasks
        .status(team.id, task, TaskInspectionOptions::default())
        .await
        .unwrap()
        .task
        .summary
        .visible_usage
        .admitted_model_calls;
    settle(&engine).await;
    assert_eq!(
        calls,
        tasks
            .status(team.id, task, TaskInspectionOptions::default())
            .await
            .unwrap()
            .task
            .summary
            .visible_usage
            .admitted_model_calls
    );
    assert!(inbox(&agents, team.id, task).await.goal_paused);
    agents.actors().unwrap().cancel_goal(task).await.unwrap();
    let cancelled = inbox(&agents, team.id, task).await;
    assert!(cancelled.goal_cancelled);
    assert!(cancelled.items.iter().all(|i| !i.actionable));
    assert!(
        tasks
            .respond(team.id, task, approval(&waiting).wait.id, approve(true))
            .await
            .is_err()
    );
    assert!(
        tasks
            .respond(team.id, task, ask.wait.id, answer("keep paused"))
            .await
            .unwrap()
            .replayed
    );
}

#[tokio::test]
async fn denials_do_not_execute_the_tool_and_individual_task_cancellation_is_respected() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(root.path()).await;
    settle(&engine).await;
    let waiting = inbox(&agents, team.id, task).await;
    let tasks = agents.teams().unwrap().tasks().unwrap();
    let edit = approval(&waiting);
    tasks
        .respond(team.id, task, edit.wait.id, approve(false))
        .await
        .unwrap();
    let ask = question(&waiting);
    agents
        .actors()
        .unwrap()
        .cancel_task(ask.task_id)
        .await
        .unwrap();
    assert!(
        tasks
            .respond(team.id, task, ask.wait.id, answer("too late"))
            .await
            .is_err()
    );
    settle(&engine).await;
    assert_eq!(
        fs::read_to_string(
            Path::new(&team.members[1].workspace.as_ref().unwrap().path).join("README.md")
        )
        .unwrap(),
        "Draft\n"
    );
    let edit_run = engine.status(edit.run_id).await.unwrap();
    assert!(
        edit_run
            .activities()
            .iter()
            .all(|a| a.spec.handler.name != "replace_text")
    );
}

#[tokio::test]
async fn planner_import_approvals_keep_exact_artifacts_through_restart_and_denial() {
    for approved in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("inbox.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut spec = manifest(root.path());
        spec.planner.config["max_model_calls"] = json!(4);
        spec.planner.config["change_review"] = json!(true);
        spec.planner.config["child_changes"] = json!(true);
        spec.planner.config["writable_paths"] = json!(["README.md"]);
        spec.planner.config["script"] = json!([
            {"kind":"delegate_task","worker":"editor","goal":"Edit README"},
            {"kind":"inspect_child_changes","worker":"editor","offset":0},
            {"kind":"import_child_changes","worker":"editor"},
            {"kind":"finish","summary":"Import decision handled"}
        ]);
        spec.workers[0].config["change_review"] = json!(true);
        let team = agents.teams().unwrap().create(spec).await.unwrap();
        let task = agents
            .teams()
            .unwrap()
            .submit(team.id, MessageId::new(), "Import approved patch".into())
            .await
            .unwrap();
        settle(&engine).await;
        let waiting = inbox(&agents, team.id, task).await;
        assert_eq!(waiting.items.len(), 1);
        agents
            .teams()
            .unwrap()
            .tasks()
            .unwrap()
            .respond(team.id, task, approval(&waiting).wait.id, approve(true))
            .await
            .unwrap();
        settle(&engine).await;
        let import = inbox(&agents, team.id, task).await;
        assert_eq!(import.items.len(), 1, "{}", json!(import));
        let item = &import.items[0];
        assert_eq!(item.kind, HumanWaitKind::ChildImportApproval);
        assert_eq!(item.member.as_deref(), Some("planner"));
        assert_eq!(
            item.wait.payload["inspection"]["preview"]["files"][0]["path"],
            "README.md"
        );
        assert!(
            item.wait.payload["inspection"]["patch"]
                .as_str()
                .unwrap()
                .contains("Reviewed")
        );
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        assert_eq!(json!(import), json!(inbox(&agents, team.id, task).await));
        let tasks = agents.teams().unwrap().tasks().unwrap();
        assert!(
            tasks
                .respond(team.id, task, item.wait.id, answer("not an approval"))
                .await
                .is_err()
        );
        tasks
            .respond(team.id, task, item.wait.id, approve(approved))
            .await
            .unwrap();
        settle(&engine).await;
        let done = tasks
            .status(team.id, task, TaskInspectionOptions::default())
            .await
            .unwrap();
        assert!(done.task.summary.settled);
        assert_eq!(done.task.summary.succeeded, 2);
        assert_eq!(
            fs::read_to_string(
                Path::new(&team.members[0].workspace.as_ref().unwrap().path).join("README.md")
            )
            .unwrap(),
            if approved { "Reviewed\n" } else { "Draft\n" }
        );
        assert!(
            tasks
                .respond(team.id, task, item.wait.id, approve(approved))
                .await
                .unwrap()
                .replayed
        );
    }
}

#[tokio::test]
async fn scheduled_planner_follow_up_questions_stay_in_the_original_goal_inbox() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inbox.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut spec = manifest(root.path());
    spec.planner.config["follow_up"] =
        json!({"max_follow_ups":1,"min_delay_ms":1,"max_delay_ms":1000});
    spec.planner.config["script"] = json!([
        {"kind":"ask_human","question":"Which review should I schedule?"},
        {"kind":"schedule_follow_up","summary":"Review selected","goal":"Continue the review","after_ms":1}
    ]);
    let team = agents.teams().unwrap().create(spec).await.unwrap();
    let task = agents
        .teams()
        .unwrap()
        .submit(team.id, MessageId::new(), "Schedule a review".into())
        .await
        .unwrap();
    settle(&engine).await;
    let waiting = inbox(&agents, team.id, task).await;
    assert_eq!(question(&waiting).member.as_deref(), Some("planner"));
    agents
        .teams()
        .unwrap()
        .tasks()
        .unwrap()
        .respond(
            team.id,
            task,
            question(&waiting).wait.id,
            answer("durability"),
        )
        .await
        .unwrap();
    settle(&engine).await;
    drop(agents);
    drop(engine);
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    settle(&engine).await;
    let continued = inbox(&agents, team.id, task).await;
    assert_eq!(continued.items.len(), 1, "{}", json!(continued));
    let ask = question(&continued);
    assert_ne!(ask.task_id, task);
    assert_eq!(continued.root_task_id, task);
    let by_child = inbox(&agents, team.id, ask.task_id).await;
    assert_eq!(by_child.root_task_id, task);
    let receipt = agents
        .teams()
        .unwrap()
        .tasks()
        .unwrap()
        .respond(team.id, ask.task_id, ask.wait.id, answer("continue"))
        .await
        .unwrap();
    assert_eq!(receipt.root_task_id, task);
    assert_eq!(receipt.task_id, ask.task_id);
}

#[tokio::test]
async fn unsupported_waits_and_blocked_runs_are_visible_but_cannot_be_approved() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, team, task) = setup(root.path()).await;
    settle(&engine).await;
    let waiting = inbox(&agents, team.id, task).await;
    let ask = question(&waiting);
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(root.path().join("inbox.db")),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE runs SET payload=json_set(payload,'$.wait.key','future_input') WHERE id=?")
        .bind(ask.run_id.to_string())
        .execute(&pool)
        .await
        .unwrap();
    let future = inbox(&agents, team.id, task).await;
    let future = future
        .items
        .iter()
        .find(|i| i.wait.id == ask.wait.id)
        .unwrap();
    assert_eq!(future.kind, HumanWaitKind::Unsupported);
    assert!(!future.actionable);
    let tasks = agents.teams().unwrap().tasks().unwrap();
    assert!(
        tasks
            .respond(team.id, task, ask.wait.id, approve(true))
            .await
            .is_err()
    );
    assert!(
        tasks
            .respond(team.id, task, ask.wait.id, answer("future"))
            .await
            .is_err()
    );
    let edit = approval(&waiting);
    sqlx::query("UPDATE runs SET status='blocked',payload=json_set(payload,'$.status','blocked') WHERE id=?").bind(edit.run_id.to_string()).execute(&pool).await.unwrap();
    let blocked = inbox(&agents, team.id, task).await;
    assert!(
        !blocked
            .items
            .iter()
            .find(|i| i.wait.id == edit.wait.id)
            .unwrap()
            .actionable
    );
    assert!(
        tasks
            .respond(team.id, task, edit.wait.id, approve(true))
            .await
            .is_err()
    );
    assert!(
        engine
            .status(edit.run_id)
            .await
            .unwrap()
            .record()
            .input_receipts
            .is_empty()
    );
    pool.close().await;
}

fn cli(db: &Path, args: &[&str], json_output: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_act0r"));
    command.arg("--db").arg(db);
    if json_output {
        command.arg("--json");
    }
    command.args(args).output().unwrap()
}
fn cli_json(db: &Path, args: &[&str]) -> Value {
    let output = cli(db, args, true);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
#[test]
fn cli_inbox_answers_and_approvals_complete_a_goal_across_process_restarts() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inbox.db");
    let config = root.path().join("team.json");
    let mut spec = json!(manifest(root.path()));
    spec["source"] = json!("source");
    fs::write(&config, serde_json::to_vec(&spec).unwrap()).unwrap();
    let team = cli_json(
        &db,
        &["team", "create", "--config-file", config.to_str().unwrap()],
    );
    let team = team["id"].as_str().unwrap();
    let submitted = cli_json(&db, &["team", "submit", team, "--goal", "Reviewed edit"]);
    let task = submitted["task_id"].as_str().unwrap();
    assert!(
        cli(
            &db,
            &[
                "worker",
                "run",
                "--until-idle",
                "--max-concurrent-activities",
                "2"
            ],
            false
        )
        .status
        .success()
    );
    let waiting = cli_json(&db, &["team", "task", "inbox", team, task]);
    assert_eq!(waiting["items"].as_array().unwrap().len(), 2);
    let text = cli(&db, &["team", "task", "inbox", team, task], false);
    let text = String::from_utf8(text.stdout).unwrap();
    assert!(text.contains("Saved request:"));
    assert!(text.contains("Reviewed"));
    assert!(text.contains("editor"));
    let partial = cli(
        &db,
        &["team", "task", "inbox", team, task, "--max-nodes", "1"],
        false,
    );
    assert!(
        String::from_utf8(partial.stdout)
            .unwrap()
            .contains("PARTIAL INBOX")
    );
    let answer_path = root.path().join("answer.txt");
    fs::write(&answer_path, "Review durability — café\n").unwrap();
    for item in waiting["items"].as_array().unwrap() {
        let wait = item["wait"]["id"].as_str().unwrap();
        let response = if item["kind"] == "question" {
            let file = answer_path.to_str().unwrap();
            assert!(
                !cli(&db, &["team", "task", "approve", team, task, wait], true)
                    .status
                    .success()
            );
            assert!(
                !cli(&db, &["team", "task", "answer", team, task, wait], true)
                    .status
                    .success()
            );
            cli_json(
                &db,
                &[
                    "team",
                    "task",
                    "answer",
                    team,
                    task,
                    wait,
                    "--answer-file",
                    file,
                ],
            )
        } else {
            cli_json(&db, &["team", "task", "approve", team, task, wait])
        };
        assert_eq!(response["replayed"], false);
    }
    assert!(
        cli(
            &db,
            &[
                "worker",
                "run",
                "--until-idle",
                "--max-concurrent-activities",
                "2"
            ],
            false
        )
        .status
        .success()
    );
    let status = cli_json(&db, &["team", "task", "status", team, task]);
    assert_eq!(status["task"]["summary"]["settled"], true);
    assert_eq!(status["task"]["summary"]["succeeded"], 3);
    assert!(
        cli_json(&db, &["team", "task", "inbox", team, task])["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let human = cli(&db, &["team", "task", "status", team, task], false);
    let human = String::from_utf8(human.stdout).unwrap();
    assert!(human.contains("Member: editor"));
    assert!(human.contains("settled: true"));
}
