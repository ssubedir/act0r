use super::*;
use crate::{
    Engine,
    adapters::{
        demo::{DemoExecutor, DemoWorkflow, TimerDemoWorkflow},
        file_lock::FileOwnership,
        system_clock::SystemClock,
    },
};
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn checkpoint(marker: &Path) -> ! {
    std::fs::write(marker, "ready").unwrap();
    loop {
        std::thread::park();
    }
}

/// Launched only by the parent test. Fault hooks are absent from production builds.
#[test]
#[ignore = "child-process crash fixture; invoked by recovery tests"]
fn crash_fixture() {
    let Some(database) = std::env::var_os("ACT0R_TEST_DATABASE") else {
        return;
    };
    let marker = PathBuf::from(std::env::var_os("ACT0R_TEST_MARKER").unwrap());
    let stage = std::env::var("ACT0R_TEST_STAGE").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut store = SqliteStore::open(database).await.unwrap();
        let target = stage.clone();
        let probe_marker = marker.clone();
        store.probe = Some(Arc::new(move |point, run| {
            // The second activity's completion changes revision 2 -> 3.
            if point == target
                && run.record().revision == 3
                && run
                    .pending_events
                    .iter()
                    .any(|e| e.kind == "attempt_finished")
            {
                checkpoint(&probe_marker);
            }
        }));
        let ownership = Arc::new(FileOwnership::new(store.database_path()));
        let engine = Engine::new(
            Arc::new(store),
            Arc::new(DemoExecutor),
            Arc::new(SystemClock),
            ownership,
            vec![Arc::new(DemoWorkflow)],
        )
        .unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert!(worker.step().await.unwrap()); // Save PrepareInput before crashing later work.
        let execution = worker.admit().await.unwrap().unwrap();
        if stage == "after_intent" {
            checkpoint(&marker);
        }
        let outcome = worker.execute(execution.clone()).await.unwrap();
        if stage == "after_execution" {
            checkpoint(&marker);
        }
        worker.complete(execution, outcome).await.unwrap();
        panic!("fixture failed to reach requested checkpoint");
    });
}

#[tokio::test]
async fn killed_workers_recover_at_four_commit_boundaries() {
    for stage in [
        "after_intent",
        "after_execution",
        "during_completion",
        "after_commit",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("recovery.db");
        let marker = directory.path().join("checkpoint");
        let engine = crate::bootstrap::local_engine(&database).await.unwrap();
        let id = engine
            .start(
                DefinitionId::new("demo", 1),
                json!({"message": "recover", "delay_ms": 0}),
                10,
            )
            .await
            .unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "adapters::sqlite::recovery_tests::crash_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("ACT0R_TEST_DATABASE", &database)
            .env("ACT0R_TEST_MARKER", &marker)
            .env("ACT0R_TEST_STAGE", stage)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut child = ChildGuard(child);
        let deadline = Instant::now() + Duration::from_secs(15);
        while !marker.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "fixture exited before {stage}"
            );
            assert!(Instant::now() < deadline, "fixture timed out at {stage}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            matches!(engine.worker().await, Err(Error::Ownership(_))),
            "second process acquired ownership at {stage}"
        );
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let reopened = crate::bootstrap::local_engine(&database).await.unwrap();
        let mut worker = reopened.worker().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while worker.has_active_runs().await.unwrap() {
            assert!(
                Instant::now() < deadline,
                "recovery did not finish at {stage}"
            );
            if !worker.step().await.unwrap() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let run = reopened.status(id).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Succeeded, "{stage}");
        assert_eq!(
            run.activities().len(),
            3,
            "continuation duplicated at {stage}"
        );
        assert_eq!(
            run.activities()[0].attempts,
            1,
            "saved PrepareInput replayed at {stage}"
        );
        assert_eq!(
            run.activities()[1].attempts,
            if stage == "after_commit" { 1 } else { 2 },
            "{stage}"
        );
        assert_eq!(
            run.record().allowance.admitted,
            if stage == "after_commit" { 3 } else { 4 }
        );
        assert_eq!(
            run.record().result,
            Some(json!({"text": "recover", "tool": "echo"}))
        );
        let events = reopened.history(id).await.unwrap();
        assert!(
            events
                .iter()
                .enumerate()
                .all(|(i, e)| e.sequence == i as u64 + 1)
        );
    }
}

#[tokio::test]
async fn adapter_connections_enforce_durability_and_foreign_keys() {
    let directory = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(directory.path().join("settings.db"))
        .await
        .unwrap();
    let mut connections = vec![];
    for _ in 0..4 {
        connections.push(store.pool.acquire().await.unwrap());
    }
    for mut connection in connections {
        let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        let sync: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        let foreign: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        let busy: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(journal, "wal");
        assert_eq!(sync, 2);
        assert_eq!(foreign, 1);
        assert_eq!(busy, 5_000);
    }
}

#[test]
#[ignore = "child-process durable timer fixture; invoked by recovery tests"]
fn timer_crash_fixture() {
    let Some(database) = std::env::var_os("ACT0R_TIMER_TEST_DATABASE") else {
        return;
    };
    let marker = PathBuf::from(std::env::var_os("ACT0R_TIMER_TEST_MARKER").unwrap());
    let stage = std::env::var("ACT0R_TIMER_TEST_STAGE").unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let mut store = SqliteStore::open(database).await.unwrap();
        let target = stage.clone();
        let probe_marker = marker.clone();
        store.probe = Some(Arc::new(move |point, run| {
            if point == target && run.pending_events.iter().any(|e| e.kind == "timer_fired") {
                checkpoint(&probe_marker);
            }
        }));
        let ownership = Arc::new(FileOwnership::new(store.database_path()));
        let engine = Engine::new(
            Arc::new(store),
            Arc::new(DemoExecutor),
            Arc::new(SystemClock),
            ownership,
            vec![Arc::new(TimerDemoWorkflow)],
        )
        .unwrap();
        let mut worker = engine.worker().await.unwrap();
        if stage == "waiting" {
            checkpoint(&marker);
        }
        assert!(worker.fire_due_timer().await.unwrap());
        panic!("timer fixture failed to reach checkpoint");
    });
}

#[tokio::test]
async fn killed_workers_preserve_timer_and_apply_firing_once() {
    for stage in ["waiting", "during_completion", "after_commit"] {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("timer.db");
        let marker = directory.path().join("timer_checkpoint");
        let engine = crate::bootstrap::local_engine(&database).await.unwrap();
        let id = engine
            .start(
                DefinitionId::new("timer_demo", 1),
                json!({"message": "durable timer", "wait_ms": 0}),
                1,
            )
            .await
            .unwrap();
        let due_at = engine.status(id).await.unwrap().timers()[0].due_at;
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "adapters::sqlite::recovery_tests::timer_crash_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("ACT0R_TIMER_TEST_DATABASE", &database)
            .env("ACT0R_TIMER_TEST_MARKER", &marker)
            .env("ACT0R_TIMER_TEST_STAGE", stage)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut child = ChildGuard(child);
        let deadline = Instant::now() + Duration::from_secs(15);
        while !marker.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "timer fixture exited before {stage}"
            );
            assert!(
                Instant::now() < deadline,
                "timer fixture timed out at {stage}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let reopened = crate::bootstrap::local_engine(&database).await.unwrap();
        let prior = reopened.status(id).await.unwrap();
        assert_eq!(prior.timers()[0].due_at, due_at);
        assert_eq!(
            prior.timers()[0].status,
            if stage == "after_commit" {
                TimerStatus::Fired
            } else {
                TimerStatus::Scheduled
            }
        );
        assert_eq!(
            prior.activities().len(),
            if stage == "after_commit" { 1 } else { 0 }
        );
        let mut worker = reopened.worker().await.unwrap();
        for _ in 0..3 {
            worker.step().await.unwrap();
        }
        let run = reopened.status(id).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Succeeded, "{stage}");
        assert_eq!(
            run.activities().len(),
            1,
            "continuation duplicated at {stage}"
        );
        assert_eq!(run.record().allowance.admitted, 1);
        assert_eq!(
            reopened
                .history(id)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.kind == "timer_fired")
                .count(),
            1
        );
    }
}
