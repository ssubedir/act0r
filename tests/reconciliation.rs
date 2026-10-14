mod common;
use act0r::{
    Engine, Error,
    adapters::{file_lock::FileOwnership, sqlite::SqliteStore},
    application::ports::*,
    domain::*,
};
use common::*;
use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Child, Command as ProcessCommand, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

fn workflow(total: u64) -> TestWorkflow {
    TestWorkflow {
        recovery: RecoveryPolicy::RequiresReconciliation,
        total,
    }
}
fn request(
    execution: &ActivityExecution,
    decision: ReconciliationDecision,
) -> ReconciliationRequest {
    ReconciliationRequest {
        activity_id: execution.activity_id,
        attempt_id: execution.attempt_id,
        decision,
        evidence: "Inspected the operation's persisted output and external effects.".into(),
    }
}

#[test]
fn retry_decisions_are_attempt_specific_and_do_not_authorize_future_retries() {
    let wf = workflow(1);
    let mut run = Run::start(&wf, Value::Null, 4, 0).unwrap();
    let first = run.admit(0).unwrap().unwrap();
    run.recover(1);
    let retry = request(&first, ReconciliationDecision::Retry {});
    run.reconcile(retry.clone(), Some(&wf), 2).unwrap();
    let snapshot = serde_json::to_value(&run).unwrap();
    run.reconcile(retry.clone(), None, 3).unwrap();
    assert_eq!(snapshot, serde_json::to_value(&run).unwrap());
    let second = run.admit(3).unwrap().unwrap();
    assert_eq!(second.activity_id, first.activity_id);
    assert_ne!(second.attempt_id, first.attempt_id);
    assert_eq!(second.ordinal, 2);
    run.recover(4);
    run.reconcile(retry.clone(), None, 5).unwrap();
    assert_eq!(run.record().status, RunStatus::Blocked);
    assert!(run.admit(5).unwrap().is_none());
    let mut conflict = retry;
    conflict.decision = ReconciliationDecision::Accept {
        result: json!("different"),
    };
    assert!(matches!(
        run.reconcile(conflict, Some(&wf), 5),
        Err(DomainError::ConflictingReconciliation)
    ));
    run.reconcile(
        request(
            &second,
            ReconciliationDecision::Accept {
                result: json!("verified"),
            },
        ),
        Some(&wf),
        6,
    )
    .unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().allowance.admitted, 2);
    assert_eq!(run.record().reconciliation_receipts.len(), 2);
    assert!(
        run.attempts()
            .iter()
            .all(|a| matches!(a.outcome, Some(Outcome::Uncertain(_))))
    );
}

#[test]
fn reconciliation_rejects_invalid_targets_and_never_resets_budgets() {
    let wf = workflow(1);
    let mut run = Run::start(&wf, Value::Null, 1, 0).unwrap();
    let execution = run.admit(0).unwrap().unwrap();
    let accept = request(
        &execution,
        ReconciliationDecision::Accept {
            result: json!("verified"),
        },
    );
    assert!(matches!(
        run.reconcile(accept.clone(), Some(&wf), 1),
        Err(DomainError::InvalidState)
    ));
    run.recover(1);
    let before = serde_json::to_value(&run).unwrap();
    let mut empty = accept.clone();
    empty.evidence = " ".into();
    assert!(matches!(
        run.reconcile(empty, Some(&wf), 2),
        Err(DomainError::InvalidReconciliation(_))
    ));
    let mut unknown = accept.clone();
    unknown.attempt_id = AttemptId::new();
    assert!(matches!(
        run.reconcile(unknown, Some(&wf), 2),
        Err(DomainError::UnknownAttempt)
    ));
    assert!(matches!(
        run.reconcile(accept.clone(), None, 2),
        Err(DomainError::InvalidWorkflow(_))
    ));
    assert!(matches!(
        run.reconcile(
            request(&execution, ReconciliationDecision::Retry {}),
            Some(&wf),
            2
        ),
        Err(DomainError::InvalidReconciliation(_))
    ));
    assert_eq!(before, serde_json::to_value(&run).unwrap());
    // Accepting a verified final result needs no additional admission.
    run.reconcile(accept, Some(&wf), 2).unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().allowance.admitted, 1);

    let mut run = Run::start(&wf, Value::Null, 10, 0).unwrap();
    for now in 0..3 {
        let execution = run.admit(now).unwrap().unwrap();
        run.recover(now);
        let decision = request(&execution, ReconciliationDecision::Retry {});
        if now < 2 {
            run.reconcile(decision, Some(&wf), now).unwrap();
        } else {
            assert!(matches!(
                run.reconcile(decision, Some(&wf), now),
                Err(DomainError::InvalidReconciliation(_))
            ));
        }
    }
    assert_eq!(run.record().status, RunStatus::Blocked);
    assert_eq!(run.activities()[0].attempts, 3);
    run.block("missing implementation".into(), 4);
    let last = run.attempts().last().unwrap();
    let decision = ReconciliationRequest {
        activity_id: run.activities()[0].id,
        attempt_id: last.id,
        decision: ReconciliationDecision::Retry {},
        evidence: "Inspected interrupted work.".into(),
    };
    assert!(matches!(
        run.reconcile(decision, Some(&wf), 5),
        Err(DomainError::InvalidState)
    ));
}

#[test]
fn accepted_results_honor_pause_and_retain_uncertain_model_spend() {
    struct Metered;
    impl WorkflowDefinition for Metered {
        fn id(&self) -> DefinitionId {
            workflow(2).id()
        }
        fn transition(
            &self,
            state: &Value,
            event: &WorkflowEvent,
        ) -> Result<Transition, DomainError> {
            let mut transition = workflow(2).transition(state, event)?;
            if matches!(event, WorkflowEvent::Started(_)) {
                if let Command::Schedule(spec) = &mut transition.commands[0] {
                    spec.usage = Some(ModelReservation {
                        input_tokens: 20,
                        output_tokens: 10,
                    });
                }
                transition.commands.insert(
                    0,
                    Command::ConfigureUsage(UsageBudget {
                        max_model_calls: Some(1),
                        max_tokens: Some(30),
                    }),
                );
            }
            Ok(transition)
        }
    }
    let wf = Metered;
    let mut run = Run::start(&wf, Value::Null, 4, 0).unwrap();
    let execution = run.admit(0).unwrap().unwrap();
    run.pause(1).unwrap();
    run.recover(2);
    let before = serde_json::to_value(&run.record().usage).unwrap();
    assert!(matches!(
        run.reconcile(
            request(&execution, ReconciliationDecision::Retry {}),
            Some(&wf),
            3
        ),
        Err(DomainError::InvalidReconciliation(_))
    ));
    run.reconcile(
        request(
            &execution,
            ReconciliationDecision::Accept {
                result: json!("verified"),
            },
        ),
        Some(&wf),
        3,
    )
    .unwrap();
    assert_eq!(run.record().status, RunStatus::Paused);
    assert_eq!(before, serde_json::to_value(&run.record().usage).unwrap());
    assert_eq!(run.record().usage.held_tokens, 30);
    assert_eq!(run.record().usage.uncertain_calls, 1);
    assert!(run.admit(4).unwrap().is_none());
    run.resume(4).unwrap();
    let next = run.admit(4).unwrap().unwrap();
    run.complete(&next, Outcome::Success(Value::Null), &wf, 5)
        .unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
}

#[tokio::test]
async fn receipt_result_and_continuation_roll_back_together_and_replay_after_restart() {
    let f = Fixture::new(workflow(2)).await;
    let id = f.start(4).await;
    let mut worker = f.engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    drop(worker);
    let mut worker = f.engine.worker().await.unwrap();
    let before = serde_json::to_value(f.engine.status(id).await.unwrap()).unwrap();
    let history = serde_json::to_value(f.engine.history(id).await.unwrap()).unwrap();
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(f.store.database_path()),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER reject_reconciliation BEFORE INSERT ON history WHEN json_extract(NEW.payload,'$.kind')='activity_reconciled' BEGIN SELECT RAISE(ABORT,'fixture failure'); END").execute(&pool).await.unwrap();
    let accept = request(
        &execution,
        ReconciliationDecision::Accept {
            result: json!("verified"),
        },
    );
    assert!(f.engine.reconcile(id, accept.clone()).await.is_err());
    assert_eq!(
        before,
        serde_json::to_value(f.engine.status(id).await.unwrap()).unwrap()
    );
    assert_eq!(
        history,
        serde_json::to_value(f.engine.history(id).await.unwrap()).unwrap()
    );
    sqlx::query("DROP TRIGGER reject_reconciliation")
        .execute(&pool)
        .await
        .unwrap();
    let (first, second) = tokio::join!(
        f.engine.reconcile(id, accept.clone()),
        f.engine.reconcile(id, accept.clone())
    );
    first.unwrap();
    second.unwrap();
    assert!(worker.step().await.unwrap());
    assert_eq!(
        f.engine.status(id).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    assert_eq!(f.executor.calls.lock().unwrap().len(), 1);
    drop(worker);
    let store = Arc::new(SqliteStore::open(f.store.database_path()).await.unwrap());
    let reopened = Engine::new(
        store.clone(),
        f.executor.clone(),
        f.clock.clone(),
        Arc::new(FileOwnership::new(store.database_path())),
        vec![Arc::new(workflow(2))],
    )
    .unwrap();
    let saved = serde_json::to_value(reopened.status(id).await.unwrap()).unwrap();
    let events = serde_json::to_value(reopened.history(id).await.unwrap()).unwrap();
    reopened.reconcile(id, accept.clone()).await.unwrap();
    assert_eq!(
        saved,
        serde_json::to_value(reopened.status(id).await.unwrap()).unwrap()
    );
    assert_eq!(
        events,
        serde_json::to_value(reopened.history(id).await.unwrap()).unwrap()
    );
    let mut conflict = accept;
    conflict.evidence.push_str(" changed");
    assert!(matches!(
        reopened.reconcile(id, conflict).await,
        Err(Error::Domain(DomainError::ConflictingReconciliation))
    ));
    pool.close().await;
    store.close().await;
}

#[test]
fn acceptance_requires_the_pinned_definition_and_preserves_invalid_continuation_evidence() {
    struct BadContinuation;
    impl WorkflowDefinition for BadContinuation {
        fn id(&self) -> DefinitionId {
            workflow(2).id()
        }
        fn transition(
            &self,
            state: &Value,
            event: &WorkflowEvent,
        ) -> Result<Transition, DomainError> {
            if matches!(event, WorkflowEvent::ActivityCompleted { .. }) {
                Ok(Transition {
                    state: Value::Null,
                    commands: vec![],
                })
            } else {
                workflow(2).transition(state, event)
            }
        }
    }
    let wf = BadContinuation;
    let mut run = Run::start(&wf, Value::Null, 3, 0).unwrap();
    let execution = run.admit(0).unwrap().unwrap();
    run.recover(1);
    let accept = request(
        &execution,
        ReconciliationDecision::Accept {
            result: json!("verified"),
        },
    );
    let before = serde_json::to_value(&run).unwrap();
    assert!(matches!(
        run.reconcile(
            accept.clone(),
            Some(&act0r::adapters::demo::DemoWorkflow),
            2
        ),
        Err(DomainError::InvalidWorkflow(_))
    ));
    assert_eq!(before, serde_json::to_value(&run).unwrap());
    run.reconcile(accept.clone(), Some(&wf), 2).unwrap();
    assert_eq!(run.record().status, RunStatus::Failed);
    assert_eq!(
        run.record().error.as_ref().unwrap().code,
        "invalid_workflow"
    );
    assert_eq!(run.activities()[0].result, Some(json!("verified")));
    assert_eq!(run.activities()[0].status, ActivityStatus::Succeeded);
    assert!(matches!(
        run.attempts()[0].outcome,
        Some(Outcome::Uncertain(_))
    ));
    let before = serde_json::to_value(&run).unwrap();
    run.reconcile(accept, None, 3).unwrap();
    assert_eq!(before, serde_json::to_value(&run).unwrap());
}

#[test]
fn decision_files_require_explicit_payloads() {
    let base = json!({"activity_id":ActivityId::new(),"attempt_id":AttemptId::new(),"decision":{"action":"retry"},"evidence":"Inspected the operation."});
    for bad in [
        json!({"action":"accept"}),
        json!({"action":"retry","result":{"ok":true}}),
        json!({"action":"fail"}),
    ] {
        let mut input = base.clone();
        input["decision"] = bad;
        assert!(
            serde_json::from_value::<ReconciliationRequest>(input.clone()).is_err(),
            "accepted incomplete or unexpected fields: {input}"
        );
    }
    let mut input = base;
    input["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ReconciliationRequest>(input).is_err());
}

#[tokio::test]
async fn failure_decision_is_terminal_and_conflicting_concurrent_decisions_cannot_both_commit() {
    let f = Fixture::new(workflow(2)).await;
    let id = f.start(4).await;
    let mut worker = f.engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    worker
        .complete(
            execution.clone(),
            Outcome::Uncertain("executor timed out".into()),
        )
        .await
        .unwrap();
    let fail = request(
        &execution,
        ReconciliationDecision::Fail {
            message: "Unable to establish the external result.".into(),
        },
    );
    let retry = request(&execution, ReconciliationDecision::Retry {});
    let (a, b) = tokio::join!(f.engine.reconcile(id, fail), f.engine.reconcile(id, retry));
    assert_ne!(a.is_ok(), b.is_ok());
    let rejected = if a.is_err() { a } else { b };
    assert!(matches!(
        rejected,
        Err(Error::Domain(DomainError::ConflictingReconciliation))
    ));
    let run = f.engine.status(id).await.unwrap();
    assert_eq!(run.record().reconciliation_receipts.len(), 1);
    if matches!(
        run.record().reconciliation_receipts[0].request.decision,
        ReconciliationDecision::Retry {}
    ) {
        // Exercise the fail path deterministically even if retry won the concurrent race.
        let next = worker.admit().await.unwrap().unwrap();
        worker
            .complete(next.clone(), Outcome::Uncertain("still unknown".into()))
            .await
            .unwrap();
        f.engine
            .reconcile(
                id,
                request(
                    &next,
                    ReconciliationDecision::Fail {
                        message: "Unable to verify completion.".into(),
                    },
                ),
            )
            .await
            .unwrap();
    }
    let run = f.engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Failed);
    assert_eq!(
        run.record().error.as_ref().unwrap().code,
        "reconciliation_failed"
    );
    assert_eq!(run.activities()[0].status, ActivityStatus::Failed);
    assert!(worker.admit().await.unwrap().is_none());
}

struct EffectExecutor {
    root: std::path::PathBuf,
    interrupt: bool,
}
impl ActivityExecutor for EffectExecutor {
    fn supports(&self, handler: &DefinitionId) -> bool {
        handler == &DefinitionId::new("test_step", 1)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if execution.input["step"] == 0 {
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.root.join("effect"))?;
                writeln!(file, "applied")?;
                file.sync_all()?;
                if self.interrupt {
                    std::fs::write(self.root.join("checkpoint"), "ready")?;
                    std::future::pending::<()>().await;
                }
            }
            Ok(Outcome::Success(json!({"ok": true})))
        })
    }
}
async fn effect_engine(root: &Path, interrupt: bool) -> Engine {
    let store = Arc::new(SqliteStore::open(root.join("kill.db")).await.unwrap());
    let ownership = Arc::new(FileOwnership::new(store.database_path()));
    Engine::new(
        store,
        Arc::new(EffectExecutor {
            root: root.into(),
            interrupt,
        }),
        Arc::new(ManualClock::new(1000)),
        ownership,
        vec![Arc::new(workflow(2))],
    )
    .unwrap()
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
#[ignore = "child-process uncertain-effect fixture; invoked by parent test"]
fn reconciliation_crash_fixture() {
    let Some(root) = std::env::var_os("ACT0R_RECONCILIATION_ROOT") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let engine = effect_engine(Path::new(&root), true).await;
        engine.worker().await.unwrap().step().await.unwrap();
    });
}
#[tokio::test]
async fn killed_worker_resumes_from_verified_external_effect_without_repeating_it() {
    let directory = tempfile::tempdir().unwrap();
    let engine = effect_engine(directory.path(), false).await;
    let id = engine
        .start(workflow(2).id(), Value::Null, 4)
        .await
        .unwrap();
    let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "reconciliation_crash_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("ACT0R_RECONCILIATION_ROOT", directory.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = ChildGuard(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(15);
    while !directory.path().join("checkpoint").exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "fixture exited early"
        );
        assert!(Instant::now() < deadline, "fixture timed out");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let engine = effect_engine(directory.path(), false).await;
    let mut worker = engine.worker().await.unwrap();
    let run = engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Blocked);
    assert!(!worker.step().await.unwrap());
    assert_eq!(
        std::fs::read_to_string(directory.path().join("effect")).unwrap(),
        "applied\n"
    );
    let execution = ActivityExecution {
        run_id: id,
        activity_id: run.activities()[0].id,
        attempt_id: run.attempts()[0].id,
        ordinal: 1,
        handler: DefinitionId::new("test_step", 1),
        input: json!({"step": 0}),
    };
    let accept = request(
        &execution,
        ReconciliationDecision::Accept {
            result: json!({"ok":true}),
        },
    );
    engine.reconcile(id, accept.clone()).await.unwrap();
    engine.reconcile(id, accept).await.unwrap();
    assert!(worker.step().await.unwrap());
    let run = engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.activities().len(), 2);
    assert_eq!(run.activities()[0].attempts, 1);
    assert!(matches!(
        run.attempts()[0].outcome,
        Some(Outcome::Uncertain(_))
    ));
    assert_eq!(
        std::fs::read_to_string(directory.path().join("effect")).unwrap(),
        "applied\n"
    );
    assert_eq!(
        engine
            .history(id)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "activity_reconciled")
            .count(),
        1
    );
}
