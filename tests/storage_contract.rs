mod common;
use act0r::{
    Engine, Error,
    adapters::{file_lock::FileOwnership, sqlite::SqliteStore},
    application::ports::*,
    domain::*,
};
use common::*;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn duplicate_completion_and_continuation_survive_reopening() {
    let f = Fixture::new(TestWorkflow::default()).await;
    let id = f.start(4).await;
    let mut worker = f.engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    let before = f.engine.history(id).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
    assert_eq!(before.len(), f.engine.history(id).await.unwrap().len());
    drop(worker);
    let reopened = SqliteStore::open(f.store.database_path()).await.unwrap();
    let run = reopened.get(id).await.unwrap();
    assert_eq!(run.activities().len(), 2);
    assert_eq!(run.activities()[0].status, ActivityStatus::Succeeded);
    assert_eq!(run.activities()[1].status, ActivityStatus::Queued);
    let mut worker = f.engine.worker().await.unwrap();
    assert!(worker.step().await.unwrap());
    assert_eq!(
        f.engine.status(id).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    assert_eq!(f.executor.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn failed_completion_commit_rolls_back_result_history_and_continuation() {
    let f = Fixture::new(TestWorkflow::default()).await;
    let id = f.start(4).await;
    let mut worker = f.engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let before = f.engine.history(id).await.unwrap().len();
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(f.store.database_path())
            .foreign_keys(true),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER reject_continuation BEFORE INSERT ON activities WHEN NEW.scheduling_revision > 1 BEGIN SELECT RAISE(ABORT, 'injected failure'); END").execute(&pool).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), Outcome::Success(json!("ok")))
            .await
            .is_err()
    );
    let run = f.engine.status(id).await.unwrap();
    assert_eq!(run.activities().len(), 1);
    assert_eq!(run.activities()[0].status, ActivityStatus::Running);
    assert!(run.activities()[0].result.is_none());
    assert!(run.attempts()[0].outcome.is_none());
    assert_eq!(before, f.engine.history(id).await.unwrap().len());
    sqlx::query("DROP TRIGGER reject_continuation")
        .execute(&pool)
        .await
        .unwrap();
    worker
        .complete(execution, Outcome::Success(json!("ok")))
        .await
        .unwrap();
    assert_eq!(f.engine.status(id).await.unwrap().activities().len(), 2);
    pool.close().await;
}

#[tokio::test]
async fn persisted_retry_deadline_and_charges_survive_restart() {
    let f = Fixture::new(TestWorkflow::default()).await;
    f.executor
        .outcomes
        .lock()
        .unwrap()
        .push_back(Outcome::Failure(Failure::new("transient", "later", true)));
    let id = f.start(5).await;
    let mut worker = f.engine.worker().await.unwrap();
    worker.step().await.unwrap();
    drop(worker);
    let mut worker = f.engine.worker().await.unwrap();
    f.clock.set(1_099);
    assert!(worker.admit().await.unwrap().is_none());
    f.clock.set(1_100);
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.ordinal, 2);
    assert_eq!(
        f.engine
            .status(id)
            .await
            .unwrap()
            .record()
            .allowance
            .admitted,
        2
    );
}

#[tokio::test]
async fn provider_deadline_receipt_and_retry_commit_are_atomic() {
    let f = Fixture::new(TestWorkflow::default()).await;
    let id = f.start(5).await;
    let mut worker = f.engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let failure = Failure::new(
        "model_http_status",
        "model endpoint returned HTTP 429",
        true,
    )
    .with_request(
        Some(RequestDiagnostics {
            category: RequestCategory::HttpStatus,
            client_request_id: execution.activity_id,
            elapsed_ms: 25,
            http_status: Some(429),
            request_id: Some("req_retry".into()),
        }),
        Some(RetryAfter::DelayMs(5_000)),
    );
    let outcome = Outcome::Failure(failure.clone());
    let before = serde_json::to_value(f.engine.status(id).await.unwrap()).unwrap();
    let history = f.engine.history(id).await.unwrap().len();
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(f.store.database_path()),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER reject_retry BEFORE UPDATE ON activities WHEN NEW.ready_at=6000 BEGIN SELECT RAISE(ABORT,'injected retry failure'); END")
        .execute(&pool).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(
        before,
        serde_json::to_value(f.engine.status(id).await.unwrap()).unwrap()
    );
    assert_eq!(history, f.engine.history(id).await.unwrap().len());
    sqlx::query("DROP TRIGGER reject_retry")
        .execute(&pool)
        .await
        .unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    let committed = serde_json::to_value(f.engine.status(id).await.unwrap()).unwrap();
    worker.complete(execution, outcome.clone()).await.unwrap();
    assert_eq!(
        committed,
        serde_json::to_value(f.engine.status(id).await.unwrap()).unwrap()
    );
    pool.close().await;
    drop(worker);
    let reopened = SqliteStore::open(f.store.database_path()).await.unwrap();
    let saved = reopened.get(id).await.unwrap();
    assert_eq!(saved.activities()[0].ready_at, 6_000);
    assert_eq!(saved.activities()[0].error.as_ref(), Some(&failure));
    assert_eq!(saved.attempts()[0].outcome.as_ref(), Some(&outcome));
    let mut worker = f.engine.worker().await.unwrap();
    for now in [900, 1_100, 5_999] {
        f.clock.set(now);
        assert!(worker.admit().await.unwrap().is_none());
    }
    f.clock.set(6_000);
    assert_eq!(worker.admit().await.unwrap().unwrap().ordinal, 2);
}

#[tokio::test]
#[ignore = "subprocess provider retry fixture; invoked by parent test"]
async fn provider_retry_crash_child() {
    let db = std::env::var("ACT0R_PROVIDER_CRASH_DB").unwrap();
    let marker = std::env::var("ACT0R_PROVIDER_CRASH_MARKER").unwrap();
    let store = Arc::new(SqliteStore::open(&db).await.unwrap());
    let engine = Engine::new(
        store,
        Arc::new(TestExecutor::default()),
        Arc::new(ManualClock::new(1_000)),
        Arc::new(FileOwnership::new(std::path::Path::new(&db))),
        vec![Arc::new(TestWorkflow::default())],
    )
    .unwrap();
    let mut worker = engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    worker
        .complete(
            execution,
            Outcome::Failure(
                Failure::new("limited", "later", true)
                    .with_request(None, Some(RetryAfter::DelayMs(5_000))),
            ),
        )
        .await
        .unwrap();
    std::fs::write(marker, b"retry committed").unwrap();
    loop {
        std::thread::park();
    }
}

#[tokio::test]
async fn killed_worker_keeps_committed_provider_deadline_without_resetting_it() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let f = Fixture::new(TestWorkflow::default()).await;
    let id = f.start(5).await;
    let marker = f.directory.path().join("retry-committed");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--ignored",
            "--exact",
            "provider_retry_crash_child",
            "--nocapture",
        ])
        .env("ACT0R_PROVIDER_CRASH_DB", f.store.database_path())
        .env("ACT0R_PROVIDER_CRASH_MARKER", &marker)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "child exited before commit"
        );
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("child timed out before commit");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    f.clock.set(5_999);
    let mut worker = f.engine.worker().await.unwrap();
    assert!(worker.admit().await.unwrap().is_none());
    let saved = f.engine.status(id).await.unwrap();
    assert_eq!(saved.activities()[0].ready_at, 6_000);
    assert_eq!(saved.attempts().len(), 1);
    assert_eq!(saved.record().allowance.admitted, 1);
    f.clock.set(6_000);
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.ordinal, 2);
    assert_eq!(saved.activities()[0].id, execution.activity_id);
}

#[tokio::test]
async fn startup_recovers_safe_attempt_and_rejects_late_completion() {
    let f = Fixture::new(TestWorkflow::default()).await;
    let id = f.start(5).await;
    let mut worker = f.engine.worker().await.unwrap();
    let old = worker.admit().await.unwrap().unwrap();
    drop(worker);
    let mut worker = f.engine.worker().await.unwrap();
    f.clock.set(1_100);
    let current = worker.admit().await.unwrap().unwrap();
    assert_eq!(current.activity_id, old.activity_id);
    assert!(matches!(
        worker.complete(old, Outcome::Success(json!("old"))).await,
        Err(Error::Domain(DomainError::StaleAttempt))
    ));
    worker
        .complete(current, Outcome::Success(json!("new")))
        .await
        .unwrap();
    assert_eq!(
        f.engine.status(id).await.unwrap().activities()[0].result,
        Some(json!("new"))
    );
}

#[tokio::test]
async fn unsafe_interruption_blocks_and_budget_is_not_refunded() {
    let f = Fixture::new(TestWorkflow {
        recovery: RecoveryPolicy::RequiresReconciliation,
        total: 1,
    })
    .await;
    let id = f.start(2).await;
    let mut worker = f.engine.worker().await.unwrap();
    worker.admit().await.unwrap().unwrap();
    drop(worker);
    let mut worker = f.engine.worker().await.unwrap();
    assert!(worker.admit().await.unwrap().is_none());
    let run = f.engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Blocked);
    assert_eq!(run.activities()[0].status, ActivityStatus::Indeterminate);
    assert_eq!(run.record().allowance.admitted, 1);
}

#[tokio::test]
async fn exhausted_run_budget_prevents_continuation_dispatch() {
    let f = Fixture::new(TestWorkflow::default()).await;
    let id = f.start(1).await;
    let mut worker = f.engine.worker().await.unwrap();
    worker.step().await.unwrap();
    assert!(worker.admit().await.unwrap().is_none());
    let run = f.engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::BudgetExhausted);
    assert_eq!(run.record().allowance.admitted, 1);
    assert_eq!(f.executor.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn missing_workflow_or_handler_blocks_with_diagnostic() {
    for missing_workflow in [true, false] {
        let f = Fixture::new(TestWorkflow::default()).await;
        let id = f.start(5).await;
        let definitions: Vec<Arc<dyn WorkflowDefinition>> = if missing_workflow {
            vec![]
        } else {
            vec![Arc::new(TestWorkflow::default())]
        };
        let executor = Arc::new(TestExecutor {
            unsupported: !missing_workflow,
            ..Default::default()
        });
        let engine = Engine::new(
            f.store.clone(),
            executor,
            f.clock.clone(),
            Arc::new(FileOwnership::new(f.store.database_path())),
            definitions,
        )
        .unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert!(worker.admit().await.unwrap().is_none());
        let run = engine.status(id).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Blocked);
        assert!(
            run.record()
                .error
                .as_ref()
                .unwrap()
                .message
                .contains(if missing_workflow {
                    "missing workflow"
                } else {
                    "missing handler"
                })
        );
    }
}

#[tokio::test]
async fn one_worker_owns_database_and_can_admit_different_runs() {
    let f = Fixture::new(TestWorkflow::default()).await;
    f.start(5).await;
    f.start(5).await;
    let mut first = f.engine.worker().await.unwrap();
    assert!(matches!(f.engine.worker().await, Err(Error::Ownership(_))));
    let a = first.admit().await.unwrap().unwrap();
    let b = first.admit().await.unwrap().unwrap();
    assert_ne!(a.run_id, b.run_id);
    assert!(first.admit().await.unwrap().is_none());
    drop(first);
    assert!(f.engine.worker().await.is_ok());
}

#[tokio::test]
async fn newer_schema_is_rejected_and_storage_settings_are_explicit() {
    let f = Fixture::new(TestWorkflow::default()).await;
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(f.store.database_path())
            .foreign_keys(true)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Full),
    )
    .await
    .unwrap();
    let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(journal, "wal");
    // Inspect the adapter's own connections in its unit tests; independent connections have separate settings.
    let current_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    let future_version = current_version + 1;
    // PRAGMA assignments cannot bind values; this value comes only from an integer.
    let future_pragma = format!("PRAGMA user_version={future_version}");
    sqlx::query(sqlx::AssertSqlSafe(future_pragma.as_str()))
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        matches!(SqliteStore::open(f.store.database_path()).await, Err(Error::Storage(message)) if message.contains(&format!("unsupported database schema {future_version}")))
    );
    pool.close().await;
}
