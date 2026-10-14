use act0r::{
    adapters::{candidates::LocalCandidates, sqlite::SqliteStore},
    application::ports::*,
    bootstrap::local_app,
    domain::{task::*, *},
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

fn config() -> TaskConfig {
    serde_json::from_str(include_str!("../examples/configs/task-scripted.json")).unwrap()
}
fn source() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/coding-task")
}
async fn drain(worker: &mut act0r::Worker) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline, "worker did not reach idle");
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

#[tokio::test]
async fn three_candidates_use_one_snapshot_resume_and_select_the_smallest_verified_patch() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("tasks.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let id = agents
        .tasks()
        .unwrap()
        .start(source(), config())
        .await
        .unwrap();
    let task = agents.tasks().unwrap().status(id).await.unwrap();
    let candidates = task.record().candidates.clone();
    assert_eq!(candidates.len(), 3);
    for candidate in &candidates {
        assert!(
            fs::read_to_string(Path::new(&candidate.workspace).join("src/lib.rs"))
                .unwrap()
                .contains("left - right")
        );
    }
    assert_ne!(candidates[0].workspace, candidates[1].workspace);
    assert_ne!(candidates[1].workspace, candidates[2].workspace);
    let first = Path::new(&candidates[0].workspace).join("src/lib.rs");
    let original = fs::read_to_string(&first).unwrap();
    fs::write(
        &first,
        format!("{original}\n// Additional candidate-local change.\n"),
    )
    .unwrap();
    assert_eq!(
        fs::read_to_string(Path::new(&candidates[1].workspace).join("src/lib.rs")).unwrap(),
        original
    );
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..6 {
        assert!(worker.step().await.unwrap());
    }
    let before = engine.status(candidates[0].run_id).await.unwrap();
    let completed: Vec<_> = before
        .activities()
        .iter()
        .filter(|a| a.status == ActivityStatus::Succeeded)
        .map(|a| (a.id, a.result.clone()))
        .collect();
    drop(worker);
    drop(engine);
    drop(agents);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let task = agents.tasks().unwrap().status(id).await.unwrap();
    let record = task.record();
    assert_eq!(record.status, TaskStatus::Succeeded);
    assert_eq!(record.selected, Some(candidates[1].run_id));
    assert_eq!(record.attempts, 30);
    assert_eq!(record.usage.admitted_model_calls, 15);
    assert_eq!(record.usage.actual_input_tokens, 150);
    assert_eq!(record.usage.actual_output_tokens, 150);
    assert_eq!(record.usage.held_tokens, 0);
    for candidate in &record.candidates {
        assert_eq!(candidate.status, RunStatus::Succeeded);
        let evaluation = candidate.evaluation.as_ref().unwrap();
        assert!(evaluation.passed, "{:?}", evaluation.error);
        assert_eq!(evaluation.tests["exit_code"], 0);
        assert_eq!(evaluation.snapshot_digest, record.snapshot.digest);
        assert!(evaluation.diff[0].patch.contains("+    left + right"));
        assert_eq!(
            candidate.workspace,
            candidates[candidate.ordinal as usize].workspace
        );
        assert!(
            fs::read_to_string(Path::new(&candidate.workspace).join("src/lib.rs"))
                .unwrap()
                .contains("left + right")
        );
    }
    let run = engine.status(candidates[0].run_id).await.unwrap();
    for (id, result) in completed {
        let activity = run.activities().iter().find(|a| a.id == id).unwrap();
        assert_eq!(activity.result, result);
        assert_eq!(activity.attempts, 1);
    }
    assert!(
        fs::read_to_string(source().join("src/lib.rs"))
            .unwrap()
            .contains("left - right")
    );
    assert!(!worker.step().await.unwrap());
}

#[tokio::test]
async fn task_waits_preserve_controls_and_require_approval_for_final_verification() {
    for approve_verification in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(directory.path().join("approvals.db"))
            .await
            .unwrap();
        let mut cfg = config();
        cfg.candidates = 1;
        cfg.agent.require_approval = true;
        let id = agents.tasks().unwrap().start(source(), cfg).await.unwrap();
        let run_id = agents
            .tasks()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .candidates[0]
            .run_id;
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        assert_eq!(
            agents
                .tasks()
                .unwrap()
                .status(id)
                .await
                .unwrap()
                .record()
                .status,
            TaskStatus::Waiting
        );
        let wait = engine
            .status(run_id)
            .await
            .unwrap()
            .record()
            .wait
            .clone()
            .unwrap();
        engine.pause(run_id).await.unwrap();
        engine
            .signal(run_id, wait.id, serde_json::json!({"approved": true}))
            .await
            .unwrap();
        engine
            .signal(run_id, wait.id, serde_json::json!({"approved": true}))
            .await
            .unwrap();
        assert_eq!(
            engine.status(run_id).await.unwrap().record().status,
            RunStatus::Paused
        );
        assert_eq!(
            agents
                .tasks()
                .unwrap()
                .status(id)
                .await
                .unwrap()
                .record()
                .attempts,
            5
        );
        engine.resume(run_id).await.unwrap();
        drain(&mut worker).await;
        let wait = engine
            .status(run_id)
            .await
            .unwrap()
            .record()
            .wait
            .clone()
            .unwrap();
        assert_eq!(wait.key, "tool_approval");
        engine
            .signal(run_id, wait.id, serde_json::json!({"approved": true}))
            .await
            .unwrap();
        drain(&mut worker).await;
        let wait = engine
            .status(run_id)
            .await
            .unwrap()
            .record()
            .wait
            .clone()
            .unwrap();
        assert_eq!(wait.key, "candidate_verification");
        assert!(
            engine
                .signal(run_id, wait.id, serde_json::json!({"approved": "yes"}))
                .await
                .is_err()
        );
        assert_eq!(
            engine
                .status(run_id)
                .await
                .unwrap()
                .record()
                .wait
                .as_ref()
                .unwrap()
                .id,
            wait.id
        );
        engine
            .signal(
                run_id,
                wait.id,
                serde_json::json!({"approved": approve_verification}),
            )
            .await
            .unwrap();
        drain(&mut worker).await;
        let task = agents.tasks().unwrap().status(id).await.unwrap();
        assert_eq!(
            task.record().status,
            if approve_verification {
                TaskStatus::Succeeded
            } else {
                TaskStatus::NoSolution
            }
        );
        assert_eq!(task.record().usage.admitted_model_calls, 5);
        assert_eq!(
            task.record().attempts,
            if approve_verification { 10 } else { 9 }
        );
    }
}

#[tokio::test]
async fn failed_creation_does_not_publish_a_partial_task_or_candidate_runs() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("creation.db");
    let (_, agents) = local_app(&db).await.unwrap();
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    sqlx::query("CREATE TRIGGER reject_second_candidate BEFORE INSERT ON task_candidates WHEN NEW.ordinal=1 BEGIN SELECT RAISE(ABORT,'reject candidate'); END").execute(&pool).await.unwrap();
    assert!(
        agents
            .tasks()
            .unwrap()
            .start(source(), config())
            .await
            .is_err()
    );
    for query in [
        "SELECT COUNT(*) FROM coding_tasks",
        "SELECT COUNT(*) FROM task_candidates",
        "SELECT COUNT(*) FROM runs",
        "SELECT COUNT(*) FROM activities",
        "SELECT COUNT(*) FROM history",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(query)
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }
    pool.close().await;
}

#[tokio::test]
async fn aggregate_model_token_and_attempt_allowances_stop_candidates_before_execution() {
    for kind in ["model", "token", "attempt"] {
        let directory = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(directory.path().join("budget.db")).await.unwrap();
        let mut cfg = config();
        match kind {
            "model" => cfg.budget.max_model_calls = 3,
            "token" => cfg.budget.max_tokens = 1,
            _ => cfg.budget.max_attempts = 3,
        }
        let id = agents
            .tasks()
            .unwrap()
            .start(source(), cfg.clone())
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let task = agents.tasks().unwrap().status(id).await.unwrap();
        assert_eq!(task.record().status, TaskStatus::NoSolution);
        assert!(task.record().selected.is_none());
        assert!(task.record().attempts <= cfg.budget.max_attempts);
        assert!(task.record().usage.admitted_model_calls <= cfg.budget.max_model_calls);
        assert!(task.record().usage.charged_tokens() <= cfg.budget.max_tokens);
        let mut attempts = 0;
        for candidate in &task.record().candidates {
            let run = engine.status(candidate.run_id).await.unwrap();
            attempts += run.record().allowance.admitted;
            assert_eq!(run.record().status, RunStatus::BudgetExhausted);
            assert_eq!(run.record().error.as_ref().unwrap().code, "task_budget");
        }
        assert_eq!(task.record().attempts, attempts);
        if kind == "token" {
            assert_eq!(attempts, 0);
        }
    }
}

#[tokio::test]
async fn snapshot_forks_ignore_later_source_changes_and_reject_snapshot_tampering() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/main.rs"), "original\n").unwrap();
    let adapter = LocalCandidates::new(
        directory.path().join(".act0r/snapshots"),
        directory.path().join(".act0r/workspaces"),
    );
    let snapshot = adapter
        .snapshot(source.clone(), TaskId::new())
        .await
        .unwrap();
    fs::write(source.join("src/main.rs"), "changed source\n").unwrap();
    let id = RunId::new();
    let first = adapter.fork(snapshot.clone(), id).await.unwrap();
    assert_eq!(adapter.fork(snapshot.clone(), id).await.unwrap(), first);
    let second = adapter.fork(snapshot.clone(), RunId::new()).await.unwrap();
    assert_eq!(
        fs::read_to_string(Path::new(&first).join("src/main.rs")).unwrap(),
        "original\n"
    );
    assert_eq!(
        fs::read_to_string(Path::new(&second).join("src/main.rs")).unwrap(),
        "original\n"
    );
    fs::write(
        Path::new(&snapshot.workspace).join("src/main.rs"),
        "tampered\n",
    )
    .unwrap();
    assert!(adapter.fork(snapshot, RunId::new()).await.is_err());
}

#[tokio::test]
async fn final_verification_rejects_changed_tests_and_retests_the_exact_candidate_patch() {
    for tamper_tests in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(directory.path().join("verify.db")).await.unwrap();
        let mut cfg = config();
        cfg.candidates = 1;
        let id = agents.tasks().unwrap().start(source(), cfg).await.unwrap();
        let candidate = agents
            .tasks()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .candidates[0]
            .clone();
        let mut worker = engine.worker().await.unwrap();
        loop {
            let execution = worker.admit().await.unwrap().unwrap();
            if execution.handler == DefinitionId::new("coding.evaluate", 1) {
                let path = Path::new(&candidate.workspace).join(if tamper_tests {
                    "tests/add.rs"
                } else {
                    "src/lib.rs"
                });
                if tamper_tests {
                    fs::write(path, "#[test] fn skip_checks() {}\n").unwrap();
                } else {
                    fs::write(
                        path,
                        "pub fn add(left: i32, right: i32) -> i32 { left - right }\n",
                    )
                    .unwrap();
                }
                let outcome = worker.execute(execution.clone()).await.unwrap();
                worker.complete(execution, outcome).await.unwrap();
                break;
            }
            let outcome = worker.execute(execution.clone()).await.unwrap();
            worker.complete(execution, outcome).await.unwrap();
        }
        let task = agents.tasks().unwrap().status(id).await.unwrap();
        assert_eq!(task.record().status, TaskStatus::NoSolution);
        let evaluation = task.record().candidates[0].evaluation.as_ref().unwrap();
        assert!(!evaluation.passed);
        if tamper_tests {
            assert!(evaluation.error.as_ref().unwrap().contains("protected"));
            assert!(evaluation.tests.is_null());
        } else {
            assert_eq!(evaluation.tests["ok"], false);
            assert_ne!(evaluation.tests["exit_code"], 0);
            assert!(!evaluation.diff.is_empty());
        }
        assert_eq!(
            engine
                .status(candidate.run_id)
                .await
                .unwrap()
                .record()
                .result
                .as_ref()
                .unwrap()["tests_passed"],
            false
        );
    }
}

#[tokio::test]
async fn task_accounting_and_attempt_records_rollback_together_and_concurrent_admissions_share_a_budget()
 {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("atomic.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = config();
    cfg.budget.max_model_calls = 1;
    let id = agents.tasks().unwrap().start(source(), cfg).await.unwrap();
    let task = agents.tasks().unwrap().status(id).await.unwrap();
    let store = Arc::new(SqliteStore::open(&db).await.unwrap());
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    let mut worker = engine.worker().await.unwrap();
    sqlx::query("CREATE TRIGGER reject_task_update BEFORE UPDATE ON coding_tasks BEGIN SELECT RAISE(ABORT,'reject task update'); END").execute(&pool).await.unwrap();
    assert!(worker.admit().await.is_err());
    for candidate in &task.record().candidates {
        let run = engine.status(candidate.run_id).await.unwrap();
        assert!(run.attempts().is_empty());
        assert_eq!(run.record().usage.held_tokens, 0);
    }
    assert_eq!(
        agents
            .tasks()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .attempts,
        0
    );
    sqlx::query("DROP TRIGGER reject_task_update")
        .execute(&pool)
        .await
        .unwrap();
    let now = i64::MAX / 2;
    let (a, b) = tokio::join!(
        store.mutate_due(now, Box::new(move |run| run.admit(now))),
        store.mutate_due(now, Box::new(move |run| run.admit(now)))
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(usize::from(a.is_some()) + usize::from(b.is_some()), 1);
    let execution = a.or(b).unwrap();
    let task = agents.tasks().unwrap().status(id).await.unwrap();
    assert_eq!(task.record().attempts, 1);
    assert_eq!(task.record().usage.admitted_model_calls, 1);
    assert!(task.record().usage.held_tokens > 0);
    let outcome = worker.execute(execution.clone()).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_task_update BEFORE UPDATE ON coding_tasks BEGIN SELECT RAISE(ABORT,'reject task update'); END").execute(&pool).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(
        engine.status(execution.run_id).await.unwrap().attempts()[0].status,
        AttemptStatus::Running
    );
    assert_eq!(
        agents
            .tasks()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .usage
            .actual_input_tokens,
        0
    );
    sqlx::query("DROP TRIGGER reject_task_update")
        .execute(&pool)
        .await
        .unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    worker.complete(execution, outcome).await.unwrap();
    let task = agents.tasks().unwrap().status(id).await.unwrap();
    assert_eq!(task.record().attempts, 1);
    assert_eq!(task.record().usage.actual_input_tokens, 10);
    assert_eq!(task.record().usage.held_tokens, 0);
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn killed_workers_recover_shared_reservations_and_final_verification_without_mixing_candidates()
 {
    for phase in ["model", "evaluation", "evaluation-unsafe"] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("killed.db");
        let marker = directory.path().join("marker.json");
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut cfg = config();
        cfg.agent.repeat_safe_tests = phase != "evaluation-unsafe";
        let id = agents.tasks().unwrap().start(source(), cfg).await.unwrap();
        let candidates = agents
            .tasks()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .candidates
            .clone();
        drop(engine);
        drop(agents);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "task_crash_child", "--ignored", "--nocapture"])
            .env("ACT0R_TASK_CRASH_DB", &db)
            .env("ACT0R_TASK_CRASH_MARKER", &marker)
            .env("ACT0R_TASK_CRASH_PHASE", phase)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        let execution: ActivityExecution = loop {
            if let Ok(bytes) = fs::read(&marker)
                && let Ok(execution) = serde_json::from_slice(&bytes)
            {
                break execution;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "crash child exited before checkpoint"
            );
            assert!(
                Instant::now() < deadline,
                "crash child did not reach checkpoint"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        child.kill().unwrap();
        child.wait().unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        let interrupted = engine.status(execution.run_id).await.unwrap();
        assert_eq!(
            interrupted
                .attempts()
                .iter()
                .find(|a| a.id == execution.attempt_id)
                .unwrap()
                .status,
            AttemptStatus::Interrupted
        );
        if phase == "evaluation-unsafe" {
            assert_eq!(interrupted.record().status, RunStatus::Blocked);
            drain(&mut worker).await;
            assert_eq!(
                agents
                    .tasks()
                    .unwrap()
                    .status(id)
                    .await
                    .unwrap()
                    .record()
                    .status,
                TaskStatus::Waiting
            );
            engine.reconcile(execution.run_id, ReconciliationRequest {
                activity_id: execution.activity_id, attempt_id: execution.attempt_id,
                decision: ReconciliationDecision::Retry {},
                evidence: "Killed the worker, checked the fixed fixture has no remaining test process, and authorize rerunning verification.".into(),
            }).await.unwrap();
        }
        drain(&mut worker).await;
        let task = agents.tasks().unwrap().status(id).await.unwrap();
        assert_eq!(task.record().status, TaskStatus::Succeeded);
        assert_eq!(task.record().attempts, 31);
        assert_eq!(
            task.record().usage.admitted_model_calls,
            if phase == "model" { 16 } else { 15 }
        );
        assert_eq!(
            task.record().usage.uncertain_calls,
            u32::from(phase == "model")
        );
        if phase == "model" {
            assert!(task.record().usage.held_tokens > 0);
        }
        for candidate in &task.record().candidates {
            assert_eq!(
                candidate.workspace,
                candidates[candidate.ordinal as usize].workspace
            );
            assert!(candidate.evaluation.as_ref().unwrap().passed);
        }
        let run = engine.status(execution.run_id).await.unwrap();
        let activity = run
            .activities()
            .iter()
            .find(|a| a.id == execution.activity_id)
            .unwrap();
        assert_eq!(activity.attempts, 2);
        assert_eq!(activity.spec.input, execution.input);
        assert!(!worker.step().await.unwrap());
    }
}

#[tokio::test]
#[ignore = "subprocess crash fixture"]
async fn task_crash_child() {
    let db = PathBuf::from(std::env::var("ACT0R_TASK_CRASH_DB").unwrap());
    let marker = PathBuf::from(std::env::var("ACT0R_TASK_CRASH_MARKER").unwrap());
    let phase = std::env::var("ACT0R_TASK_CRASH_PHASE").unwrap();
    let (engine, _) = local_app(db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    loop {
        let Some(execution) = worker.admit().await.unwrap() else {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        };
        let checkpoint = if phase == "model" {
            execution.handler.name == "agent.model"
        } else {
            execution.handler.name == "coding.evaluate"
        };
        if phase == "model" && checkpoint {
            fs::write(&marker, serde_json::to_vec(&execution).unwrap()).unwrap();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        let outcome = worker.execute(execution.clone()).await.unwrap();
        if checkpoint {
            fs::write(&marker, serde_json::to_vec(&execution).unwrap()).unwrap();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        worker.complete(execution, outcome).await.unwrap();
    }
}
