mod common;
use act0r::{Engine, adapters::file_lock::FileOwnership, domain::*};
use common::*;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Clone)]
struct TimerWorkflow {
    spec: TimerSpec,
    follow_up: bool,
}
impl WorkflowDefinition for TimerWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("timer_test", 1)
    }
    fn transition(&self, _state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let command = match event {
            WorkflowEvent::Started(_) => Command::StartTimer(self.spec.clone()),
            WorkflowEvent::TimerFired { payload, .. } if !self.follow_up => {
                Command::Complete(payload.clone())
            }
            WorkflowEvent::TimerFired { .. } => Command::Schedule(ActivitySpec {
                handler: DefinitionId::new("test_step", 1),
                input: Value::Null,
                retry: RetryPolicy::default(),
                recovery: RecoveryPolicy::SafeToRepeat,
                usage: None,
            }),
            WorkflowEvent::ActivityCompleted { result, .. } => Command::Complete(result.clone()),
            WorkflowEvent::InputReceived { .. }
            | WorkflowEvent::TaskCompleted { .. }
            | WorkflowEvent::TaskGroupCompleted { .. }
            | WorkflowEvent::PlannerQuestionClosed { .. }
            | WorkflowEvent::CompletionRejected { .. } => {
                return Err(DomainError::InvalidState);
            }
        };
        Ok(Transition {
            state: json!({"continued": !matches!(event, WorkflowEvent::Started(_))}),
            commands: vec![command],
        })
    }
}
async fn fixture(spec: TimerSpec, follow_up: bool) -> Fixture {
    let mut f = Fixture::new(TestWorkflow::default()).await;
    f.engine = Engine::new(
        f.store.clone(),
        f.executor.clone(),
        f.clock.clone(),
        Arc::new(FileOwnership::new(f.store.database_path())),
        vec![
            Arc::new(TestWorkflow::default()),
            Arc::new(TimerWorkflow { spec, follow_up }),
        ],
    )
    .unwrap();
    f
}
async fn start(f: &Fixture) -> RunId {
    f.engine
        .start(DefinitionId::new("timer_test", 1), Value::Null, 1)
        .await
        .unwrap()
}

#[tokio::test]
async fn timer_deadline_survives_restart_and_backward_clock_change() {
    let f = fixture(TimerSpec::after(100, json!("awake")), false).await;
    let id = start(&f).await;
    let timer_id = f.engine.status(id).await.unwrap().timers()[0].id;
    let mut worker = f.engine.worker().await.unwrap();
    f.clock.set(1_099);
    assert!(!worker.step().await.unwrap());
    drop(worker);
    let mut worker = f.engine.worker().await.unwrap();
    f.clock.set(900);
    assert!(!worker.step().await.unwrap());
    let run = f.engine.status(id).await.unwrap();
    assert_eq!(run.timers()[0].id, timer_id);
    assert_eq!(run.timers()[0].due_at, 1_100);
    f.clock.set(1_100);
    assert!(worker.step().await.unwrap());
    let run = f.engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().allowance.admitted, 0);
    assert_eq!(run.timers()[0].status, TimerStatus::Fired);
    assert_eq!(run.timers()[0].fired_at, Some(1_100));
}

#[tokio::test]
async fn timer_that_expired_while_offline_fires_without_restarting_wait() {
    let f = fixture(TimerSpec::after(100, json!("overdue")), false).await;
    let id = start(&f).await;
    drop(f.engine.worker().await.unwrap());
    f.clock.set(99_000);
    let mut worker = f.engine.worker().await.unwrap();
    assert!(worker.step().await.unwrap());
    let run = f.engine.status(id).await.unwrap();
    assert_eq!(run.timers()[0].due_at, 1_100);
    assert_eq!(run.record().result, Some(json!("overdue")));
}

#[tokio::test]
async fn firing_result_history_and_continuation_commit_together() {
    let f = fixture(TimerSpec::after(0, Value::Null), true).await;
    let id = start(&f).await;
    let mut worker = f.engine.worker().await.unwrap();
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(f.store.database_path()),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER reject_timer_continuation BEFORE INSERT ON activities BEGIN SELECT RAISE(ABORT, 'timer commit failure'); END").execute(&pool).await.unwrap();
    let before = f.engine.history(id).await.unwrap().len();
    assert!(worker.fire_due_timer().await.is_err());
    let run = f.engine.status(id).await.unwrap();
    assert_eq!(run.timers()[0].status, TimerStatus::Scheduled);
    assert!(run.timers()[0].fired_at.is_none());
    assert!(run.activities().is_empty());
    assert_eq!(before, f.engine.history(id).await.unwrap().len());
    sqlx::query("DROP TRIGGER reject_timer_continuation")
        .execute(&pool)
        .await
        .unwrap();
    assert!(worker.fire_due_timer().await.unwrap());
    assert!(!worker.fire_due_timer().await.unwrap());
    assert_eq!(f.engine.status(id).await.unwrap().activities().len(), 1);
    assert_eq!(
        f.engine
            .history(id)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "timer_fired")
            .count(),
        1
    );
    worker.step().await.unwrap();
    assert_eq!(
        f.engine.status(id).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    pool.close().await;
}

#[tokio::test]
async fn waiting_timer_releases_execution_for_other_runs() {
    let f = fixture(TimerSpec::after(10_000, Value::Null), false).await;
    let waiting = start(&f).await;
    let working = f.start(5).await;
    let mut worker = f.engine.worker().await.unwrap();
    assert!(worker.step().await.unwrap());
    assert!(worker.step().await.unwrap());
    assert_eq!(
        f.engine.status(working).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    let run = f.engine.status(waiting).await.unwrap();
    assert_eq!(run.timers()[0].status, TimerStatus::Scheduled);
    assert_eq!(run.record().allowance.admitted, 0);
    assert!(worker.has_active_runs().await.unwrap());
}

#[test]
fn absolute_and_zero_delay_timers_fire_once_and_overflow_is_rejected() {
    for spec in [
        TimerSpec::at(500, json!("done")),
        TimerSpec::after(0, json!("done")),
    ] {
        let workflow = TimerWorkflow {
            spec,
            follow_up: false,
        };
        let mut run = Run::start(&workflow, Value::Null, 1, 1_000).unwrap();
        let id = run.timers()[0].id;
        assert!(run.fire_timer(id, &workflow, 1_000).unwrap());
        let before = serde_json::to_value(&run).unwrap();
        assert!(!run.fire_timer(id, &workflow, 1_001).unwrap());
        assert_eq!(before, serde_json::to_value(&run).unwrap());
    }
    let workflow = TimerWorkflow {
        spec: TimerSpec::after(u64::MAX, Value::Null),
        follow_up: false,
    };
    assert!(Run::start(&workflow, Value::Null, 1, 1_000).is_err());
}

#[test]
fn parallel_timers_allow_intermediate_transition_and_complete_after_both() {
    struct TwoTimers;
    impl WorkflowDefinition for TwoTimers {
        fn id(&self) -> DefinitionId {
            DefinitionId::new("two_timers", 1)
        }
        fn transition(
            &self,
            state: &Value,
            event: &WorkflowEvent,
        ) -> Result<Transition, DomainError> {
            match event {
                WorkflowEvent::Started(_) => Ok(Transition {
                    state: json!(0),
                    commands: vec![
                        Command::StartTimer(TimerSpec::after(10, Value::Null)),
                        Command::StartTimer(TimerSpec::after(20, Value::Null)),
                    ],
                }),
                WorkflowEvent::TimerFired { .. } => {
                    let count = state.as_u64().unwrap() + 1;
                    Ok(Transition {
                        state: json!(count),
                        commands: if count == 2 {
                            vec![Command::Complete(json!("done"))]
                        } else {
                            vec![]
                        },
                    })
                }
                _ => Err(DomainError::InvalidState),
            }
        }
    }
    let mut run = Run::start(&TwoTimers, Value::Null, 1, 0).unwrap();
    assert!(run.fire_timer(run.timers()[0].id, &TwoTimers, 10).unwrap());
    assert!(run.record().status.is_active());
    assert!(run.fire_timer(run.timers()[1].id, &TwoTimers, 20).unwrap());
    assert_eq!(run.record().status, RunStatus::Succeeded);
}

#[tokio::test]
async fn missing_timer_workflow_blocks_run_instead_of_firing() {
    let f = fixture(TimerSpec::after(0, Value::Null), false).await;
    let id = start(&f).await;
    let engine = Engine::new(
        f.store.clone(),
        f.executor.clone(),
        f.clock.clone(),
        Arc::new(FileOwnership::new(f.store.database_path())),
        vec![],
    )
    .unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.fire_due_timer().await.unwrap());
    assert_eq!(
        engine.status(id).await.unwrap().record().status,
        RunStatus::Blocked
    );
}
