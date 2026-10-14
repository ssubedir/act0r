use act0r::{
    adapters::workspace::{LocalTools, LocalWorkspaces},
    application::ports::*,
    bootstrap::local_app,
    domain::{agent::*, *},
};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    time::{Duration, Instant},
};

fn config() -> AgentConfig {
    serde_json::from_str(include_str!("../examples/configs/agent-scripted.json")).unwrap()
}
fn source() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/coding-task")
}
fn replacement() -> AgentAction {
    AgentAction::ReplaceText {
        path: "src/lib.rs".into(),
        old: "left - right".into(),
        new: "left + right".into(),
    }
}

#[tokio::test]
async fn coding_agent_edits_isolated_project_passes_tests_and_reopens_without_replay() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("agent.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = config();
    cfg.require_approval = false;
    let id = agents.start(source(), cfg, 30).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..4 {
        assert!(worker.step().await.unwrap());
    }
    let before = engine.status(id).await.unwrap();
    let finished: Vec<_> = before
        .activities()
        .iter()
        .filter(|a| a.status == ActivityStatus::Succeeded)
        .map(|a| (a.id, a.result.clone()))
        .collect();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, _) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..20 {
        if !worker.step().await.unwrap() {
            break;
        }
    }
    let run = engine.status(id).await.unwrap();
    assert_eq!(
        run.record().status,
        RunStatus::Succeeded,
        "{:?}",
        run.record().error
    );
    assert_eq!(run.record().usage.admitted_model_calls, 5);
    assert_eq!(run.record().usage.actual_input_tokens, 50);
    assert_eq!(run.record().usage.actual_output_tokens, 50);
    assert_eq!(run.record().usage.held_tokens, 0);
    assert_eq!(run.record().result.as_ref().unwrap()["tests_passed"], true);
    assert_eq!(run.attempts().len(), 9);
    for (id, result) in finished {
        let activity = run.activities().iter().find(|a| a.id == id).unwrap();
        assert_eq!(activity.result, result);
        assert_eq!(activity.attempts, 1);
    }
    let workspace = run.record().state["config"]["workspace"].as_str().unwrap();
    assert!(
        std::fs::read_to_string(Path::new(workspace).join("src/lib.rs"))
            .unwrap()
            .contains("left + right")
    );
    assert!(
        std::fs::read_to_string(source().join("src/lib.rs"))
            .unwrap()
            .contains("left - right")
    );
}

#[tokio::test]
async fn approvals_and_human_answers_survive_restart_and_duplicate_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("wait.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = config();
    cfg.script = vec![
        replacement(),
        AgentAction::AskHuman {
            question: "Continue?".into(),
        },
        AgentAction::RunTests,
        AgentAction::Finish {
            summary: "done".into(),
        },
    ];
    let id = agents.start(source(), cfg, 30).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    worker.step().await.unwrap();
    let before = engine.status(id).await.unwrap();
    let wait = before.record().wait.clone().unwrap();
    assert_eq!(before.record().status, RunStatus::Waiting);
    assert!(!worker.has_active_runs().await.unwrap());
    assert!(!worker.step().await.unwrap());
    let workspace = before.record().state["config"]["workspace"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        std::fs::read_to_string(Path::new(&workspace).join("src/lib.rs"))
            .unwrap()
            .contains("left - right")
    );
    drop(worker);
    drop(engine);
    drop(agents);
    let (engine, _) = local_app(&db).await.unwrap();
    assert_eq!(
        engine
            .status(id)
            .await
            .unwrap()
            .record()
            .wait
            .as_ref()
            .unwrap()
            .id,
        wait.id
    );
    engine.pause(id).await.unwrap();
    engine
        .signal(id, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    let history = engine.history(id).await.unwrap().len();
    engine
        .signal(id, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    assert_eq!(engine.history(id).await.unwrap().len(), history);
    assert!(
        engine
            .signal(id, wait.id, json!({"approved":false}))
            .await
            .is_err()
    );
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.step().await.unwrap());
    engine.resume(id).await.unwrap();
    worker.step().await.unwrap();
    worker.step().await.unwrap();
    let wait = engine
        .status(id)
        .await
        .unwrap()
        .record()
        .wait
        .clone()
        .unwrap();
    assert_eq!(wait.key, "agent_answer");
    engine
        .signal(id, wait.id, json!({"answer":"yes"}))
        .await
        .unwrap();
    worker.step().await.unwrap();
    let wait = engine
        .status(id)
        .await
        .unwrap()
        .record()
        .wait
        .clone()
        .unwrap();
    assert_eq!(wait.key, "tool_approval");
    engine
        .signal(id, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    worker.step().await.unwrap();
    worker.step().await.unwrap();
    assert_eq!(
        engine.status(id).await.unwrap().record().status,
        RunStatus::Succeeded
    );
}

#[tokio::test]
async fn pause_and_cancel_during_activity_preserve_result_and_usage_at_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("controls.db"))
        .await
        .unwrap();
    for cancel in [false, true] {
        let id = agents.start(source(), config(), 30).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        let execution = worker.admit().await.unwrap().unwrap();
        if cancel {
            engine.cancel(id).await.unwrap();
        } else {
            engine.pause(id).await.unwrap();
        }
        assert_eq!(
            engine.status(id).await.unwrap().record().status,
            RunStatus::Running
        );
        let outcome = worker.execute(execution.clone()).await.unwrap();
        worker.complete(execution, outcome).await.unwrap();
        let run = engine.status(id).await.unwrap();
        assert_eq!(
            run.record().status,
            if cancel {
                RunStatus::Cancelled
            } else {
                RunStatus::Paused
            }
        );
        assert_eq!(run.record().usage.actual_input_tokens, 10);
        assert!(run.activities()[0].result.is_some());
        assert_eq!(run.activities().len(), if cancel { 1 } else { 2 });
        assert!(!worker.step().await.unwrap());
        if !cancel {
            engine.resume(id).await.unwrap();
            assert!(worker.step().await.unwrap());
            engine.cancel(id).await.unwrap();
        }
    }
}

#[test]
fn model_budgets_reserve_before_dispatch_and_keep_interrupted_charge() {
    let mut cfg = config();
    cfg.workspace = "managed".into();
    cfg.max_model_calls = Some(1);
    let workflow = AgentWorkflow;
    let mut run = Run::start(&workflow, serde_json::to_value(&cfg).unwrap(), 30, 0).unwrap();
    let attempt = run.admit(0).unwrap().unwrap();
    let held = run.record().usage.held_tokens;
    assert!(held > 0);
    assert_eq!(run.record().usage.admitted_model_calls, 1);
    run.recover(1);
    assert_eq!(run.record().usage.held_tokens, held);
    assert_eq!(run.record().usage.uncertain_calls, 1);
    assert!(run.admit(501).unwrap().is_none());
    assert_eq!(run.record().status, RunStatus::BudgetExhausted);
    assert_eq!(run.attempts()[0].id, attempt.attempt_id);
    cfg.max_model_calls = Some(10);
    cfg.max_tokens = Some(1);
    let mut run = Run::start(&workflow, serde_json::to_value(&cfg).unwrap(), 30, 0).unwrap();
    assert!(run.admit(0).unwrap().is_none());
    assert!(run.attempts().is_empty());
    cfg.max_tokens = Some(100000);
    let mut run = Run::start(&workflow, serde_json::to_value(cfg).unwrap(), 30, 0).unwrap();
    let execution = run.admit(0).unwrap().unwrap();
    let reserved = run.record().usage.held_tokens;
    run.complete(
        &execution,
        Outcome::SuccessWithUsage {
            result: json!({"action":{"kind":"list_files"},"request_id":null}),
            usage: TokenUsage {
                input_tokens: reserved + 1,
                output_tokens: 1,
            },
        },
        &workflow,
        1,
    )
    .unwrap();
    assert_eq!(run.record().status, RunStatus::BudgetExhausted);
    assert_eq!(run.record().usage.held_tokens, 0);
    assert_eq!(run.record().usage.charged_tokens(), reserved + 2);
    assert_eq!(run.activities().len(), 1);
}

#[tokio::test]
async fn workspace_edits_recover_by_hash_and_reject_traversal_and_protected_files() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("workspaces");
    let workspace = LocalWorkspaces::new(root.clone())
        .prepare(source())
        .await
        .unwrap();
    let tools = LocalTools::new(root);
    let id = ActivityId::new();
    let request = ToolRequest {
        workspace: workspace.clone(),
        action: replacement(),
        test_command: vec!["cargo".into(), "test".into(), "--offline".into()],
        writable_paths: vec!["src/lib.rs".into()],
    };
    assert_eq!(
        tools.execute(request.clone(), id).await.unwrap()["already_applied"],
        false
    );
    // Same activity delivered after the edit but before recording its outcome.
    assert_eq!(
        tools.execute(request.clone(), id).await.unwrap()["already_applied"],
        true
    );
    // A persisted intent whose publication was interrupted can publish on retry.
    std::fs::write(
        Path::new(&workspace).join("src/lib.rs"),
        std::fs::read(source().join("src/lib.rs")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        tools.execute(request.clone(), id).await.unwrap()["already_applied"],
        false
    );
    let mut changed = request.clone();
    changed.action = AgentAction::ReplaceText {
        path: "src/lib.rs".into(),
        old: "left + right".into(),
        new: "0".into(),
    };
    assert_eq!(tools.execute(changed, id).await.unwrap()["ok"], false);
    std::fs::write(
        Path::new(&workspace).join("src/lib.rs"),
        "externally changed",
    )
    .unwrap();
    // Both adapters share the edit journal and preserve coding error observations.
    for old in ["", "missing text"] {
        let mut unmatched = request.clone();
        unmatched.action = AgentAction::ReplaceText {
            path: "src/lib.rs".into(),
            old: old.into(),
            new: "ignored".into(),
        };
        assert_eq!(
            tools.execute(unmatched, ActivityId::new()).await.unwrap(),
            json!({"ok":false,"error":"implementation unavailable: old text must match exactly once"})
        );
    }
    assert_eq!(
        tools.execute(request.clone(), id).await.unwrap()["ok"],
        false
    );
    for path in [
        "../outside",
        ".act0r-tools/managed",
        "C:/outside",
        "tests/add.rs",
    ] {
        let mut request = request.clone();
        request.action = AgentAction::ReplaceText {
            path: path.into(),
            old: "adds_integers".into(),
            new: "ignored".into(),
        };
        assert_eq!(
            tools.execute(request, ActivityId::new()).await.unwrap()["ok"],
            false,
            "{path}"
        );
    }
    for path in [
        ".ACT0R-TOOLS/managed",
        ".act0r-tools./managed",
        ".act0r-tools/managed",
    ] {
        let mut private_read = request.clone();
        private_read.action = AgentAction::ReadFile { path: path.into() };
        assert_eq!(
            tools
                .execute(private_read, ActivityId::new())
                .await
                .unwrap()["ok"],
            false
        );
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            source().join("src/lib.rs"),
            Path::new(&workspace).join("escape.rs"),
        )
        .unwrap();
        let mut request = request;
        request.action = AgentAction::ReadFile {
            path: "escape.rs".into(),
        };
        assert_eq!(
            tools.execute(request, ActivityId::new()).await.unwrap()["ok"],
            false
        );
    }
}

#[tokio::test]
async fn recovery_honors_controls_before_any_retry_is_dispatched() {
    for cancel in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("recovery.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let id = agents.start(source(), config(), 30).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        worker.admit().await.unwrap().unwrap();
        if cancel {
            engine.cancel(id).await.unwrap();
        } else {
            engine.pause(id).await.unwrap();
        }
        drop(worker);
        drop(engine);
        drop(agents);
        let (engine, _) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        let run = engine.status(id).await.unwrap();
        assert_eq!(
            run.record().status,
            if cancel {
                RunStatus::Cancelled
            } else {
                RunStatus::Paused
            }
        );
        assert_eq!(run.record().usage.admitted_model_calls, 1);
        assert_eq!(run.record().usage.uncertain_calls, 1);
        assert_eq!(run.attempts().len(), 1);
        assert!(!worker.step().await.unwrap());
        assert!(!worker.has_active_runs().await.unwrap());
    }
}

#[tokio::test]
async fn approval_transaction_failure_leaves_wait_receipt_and_continuation_unmodified() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("atomic.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = config();
    cfg.script = vec![replacement()];
    let id = agents.start(source(), cfg, 30).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    worker.step().await.unwrap();
    let before = engine.status(id).await.unwrap();
    let history = engine.history(id).await.unwrap().len();
    let wait = before.record().wait.as_ref().unwrap().id;
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    sqlx::query("CREATE TRIGGER reject_approval BEFORE INSERT ON activities WHEN json_extract(NEW.payload,'$.spec.handler.name')='agent.tool' BEGIN SELECT RAISE(ABORT,'fixture failure'); END").execute(&pool).await.unwrap();
    assert!(
        engine
            .signal(id, wait, json!({"approved":true}))
            .await
            .is_err()
    );
    assert_eq!(
        serde_json::to_value(engine.status(id).await.unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    assert_eq!(engine.history(id).await.unwrap().len(), history);
    sqlx::query("DROP TRIGGER reject_approval")
        .execute(&pool)
        .await
        .unwrap();
    engine
        .signal(id, wait, json!({"approved":false}))
        .await
        .unwrap();
    let run = engine.status(id).await.unwrap();
    assert!(run.record().wait.is_none());
    assert_eq!(run.activities().len(), 2);
    assert_eq!(run.activities()[1].spec.handler.name, "agent.model");
    assert_eq!(
        run.record().state["observations"][0]["result"]["approved"],
        false
    );
    pool.close().await;
}

#[tokio::test]
async fn unsafe_tests_block_after_interruption_instead_of_automatically_running_again() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("tests.db")).await.unwrap();
    let mut cfg = config();
    cfg.require_approval = false;
    cfg.repeat_safe_tests = false;
    cfg.script = vec![AgentAction::RunTests];
    let id = agents.start(source(), cfg, 30).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    worker.step().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.handler.name, "agent.tool");
    drop(worker);
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.step().await.unwrap());
    let run = engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Blocked);
    assert_eq!(run.activities()[1].status, ActivityStatus::Indeterminate);
    assert_eq!(run.activities()[1].attempts, 1);
}

#[tokio::test]
async fn killed_agent_worker_keeps_completed_steps_and_charges_interrupted_model() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("crash.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = config();
    cfg.require_approval = false;
    cfg.scripted_delay_ms = 1000;
    let id = agents.start(source(), cfg, 30).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    worker.step().await.unwrap();
    worker.step().await.unwrap();
    drop(worker);
    let first = engine.status(id).await.unwrap().activities()[0].id;
    let mut command = ProcessCommand::new(env!("CARGO_BIN_EXE_act0r"));
    command
        .arg("--db")
        .arg(&db)
        .args(["worker", "run", "--until-idle"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let interrupted = loop {
        let run = engine.status(id).await.unwrap();
        if let Some(a) = run
            .activities()
            .iter()
            .find(|a| a.status == ActivityStatus::Running)
        {
            break (a.id, a.spec.input.clone());
        }
        assert!(Instant::now() < deadline);
        assert!(child.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    child.kill().unwrap();
    child.wait().unwrap();
    let mut worker = engine.worker().await.unwrap();
    let run = engine.status(id).await.unwrap();
    assert_eq!(run.record().usage.uncertain_calls, 1);
    assert_eq!(
        run.activities()
            .iter()
            .find(|a| a.id == interrupted.0)
            .unwrap()
            .spec
            .input,
        interrupted.1
    );
    let held = run.record().usage.held_tokens;
    assert!(held > 0);
    let deadline = Instant::now() + Duration::from_secs(20);
    while worker.has_active_runs().await.unwrap() {
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(Instant::now() < deadline);
    }
    let run = engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().usage.held_tokens, held);
    assert_eq!(
        run.activities()
            .iter()
            .find(|a| a.id == first)
            .unwrap()
            .attempts,
        1
    );
    assert_eq!(
        run.activities()
            .iter()
            .find(|a| a.id == interrupted.0)
            .unwrap()
            .attempts,
        2
    );
    assert_eq!(run.record().usage.admitted_model_calls, 6);
}
