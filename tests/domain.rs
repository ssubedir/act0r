mod common;
use act0r::domain::*;
use common::TestWorkflow;
use serde_json::{Value, json};

#[test]
fn deliberate_failure_is_terminal_and_requires_a_quiescent_boundary() {
    struct FailureWorkflow(u8);
    impl WorkflowDefinition for FailureWorkflow {
        fn id(&self) -> DefinitionId {
            DefinitionId::new("failure_boundary", 1)
        }
        fn transition(
            &self,
            state: &Value,
            event: &WorkflowEvent,
        ) -> Result<Transition, DomainError> {
            if matches!(event, WorkflowEvent::Started(_)) {
                let mut transition = TestWorkflow::default().transition(state, event)?;
                if self.0 == 2 {
                    let Command::Schedule(spec) = &transition.commands[0] else {
                        panic!("missing fixture activity");
                    };
                    transition.commands.push(Command::Schedule(spec.clone()));
                }
                return Ok(transition);
            }
            let mut commands = vec![Command::Fail(Failure::new(
                "coding_incomplete",
                "Saved partial work",
                self.0 == 3,
            ))];
            if self.0 == 1 {
                commands.push(Command::Complete(json!("wrong success")));
            }
            Ok(Transition {
                state: json!({"partial":true}),
                commands,
            })
        }
    }
    for mode in 0..4 {
        let workflow = FailureWorkflow(mode);
        let mut run = Run::start(&workflow, Value::Null, 10, 0).unwrap();
        let execution = run.admit(0).unwrap().unwrap();
        run.complete(
            &execution,
            Outcome::Success(json!("saved activity")),
            &workflow,
            1,
        )
        .unwrap();
        assert_eq!(run.record().status, RunStatus::Failed);
        assert!(run.record().result.is_none());
        assert_eq!(
            run.record().error.as_ref().unwrap().code,
            if mode == 0 {
                "coding_incomplete"
            } else {
                "invalid_workflow"
            }
        );
        assert_eq!(run.activities()[0].result, Some(json!("saved activity")));
    }
}

#[test]
fn provider_advice_never_shortens_local_delay_or_overflows_and_cannot_bypass_caps() {
    let workflow = TestWorkflow::default();
    for (advice, ready_at) in [
        (RetryAfter::DelayMs(0), 1_100),
        (RetryAfter::At(900), 1_100),
        (RetryAfter::At(7_000), 7_000),
        (RetryAfter::DelayMs(u64::MAX), i64::MAX),
    ] {
        let mut run = Run::start(&workflow, Value::Null, 10, 1_000).unwrap();
        let execution = run.admit(1_000).unwrap().unwrap();
        run.complete(
            &execution,
            Outcome::Failure(
                Failure::new("limited", "later", true).with_request(None, Some(advice)),
            ),
            &workflow,
            1_000,
        )
        .unwrap();
        assert_eq!(run.activities()[0].ready_at, ready_at);
    }
    let mut run = Run::start(&workflow, Value::Null, 1, 1_000).unwrap();
    let execution = run.admit(1_000).unwrap().unwrap();
    run.complete(
        &execution,
        Outcome::Failure(
            Failure::new("limited", "later", true).with_request(None, Some(RetryAfter::At(7_000))),
        ),
        &workflow,
        1_000,
    )
    .unwrap();
    assert_eq!(run.record().status, RunStatus::BudgetExhausted);
    assert!(run.admit(7_000).unwrap().is_none());
}

#[test]
fn duplicate_success_is_a_noop_and_conflicting_success_is_rejected() {
    let workflow = TestWorkflow::default();
    let mut run = Run::start(&workflow, Value::Null, 4, 0).unwrap();
    let execution = run.admit(0).unwrap().unwrap();
    let outcome = Outcome::Success(json!("first"));
    run.complete(&execution, outcome.clone(), &workflow, 1)
        .unwrap();
    let before = serde_json::to_value(&run).unwrap();
    run.complete(&execution, outcome, &workflow, 2).unwrap();
    assert_eq!(before, serde_json::to_value(&run).unwrap());
    assert!(matches!(
        run.complete(
            &execution,
            Outcome::Success(json!("different")),
            &workflow,
            2
        ),
        Err(DomainError::ConflictingOutcome)
    ));
}

#[test]
fn recovery_preserves_charge_and_stable_operation_identity() {
    let workflow = TestWorkflow::default();
    let mut run = Run::start(&workflow, Value::Null, 4, 0).unwrap();
    let first = run.admit(0).unwrap().unwrap();
    run.recover(10);
    assert!(run.admit(109).unwrap().is_none());
    let second = run.admit(110).unwrap().unwrap();
    assert_eq!(first.activity_id, second.activity_id);
    assert_ne!(first.attempt_id, second.attempt_id);
    assert_eq!(run.record().allowance.admitted, 2);
    assert!(matches!(
        run.complete(&first, Outcome::Success(json!("late")), &workflow, 111),
        Err(DomainError::StaleAttempt)
    ));
}

#[test]
fn uncertain_irreversible_work_blocks_even_with_exhausted_budget() {
    let workflow = TestWorkflow {
        recovery: RecoveryPolicy::RequiresReconciliation,
        total: 1,
    };
    let mut run = Run::start(&workflow, Value::Null, 1, 0).unwrap();
    run.admit(0).unwrap().unwrap();
    run.recover(1);
    assert_eq!(run.record().status, RunStatus::Blocked);
    assert_eq!(run.activities()[0].status, ActivityStatus::Indeterminate);
    assert!(run.admit(1_000).unwrap().is_none());
}

#[test]
fn attempt_and_retry_limits_include_initial_and_interrupted_attempts() {
    let workflow = TestWorkflow::default();
    let mut run = Run::start(&workflow, Value::Null, 1, 0).unwrap();
    run.admit(0).unwrap();
    run.recover(1);
    assert_eq!(run.record().status, RunStatus::BudgetExhausted);
    assert_eq!(run.record().allowance.admitted, 1);
    let mut run = Run::start(&workflow, Value::Null, 100, 0).unwrap();
    for now in [0, 100, 300] {
        let execution = run.admit(now).unwrap().unwrap();
        run.complete(
            &execution,
            Outcome::Failure(Failure::new("transient", "try again", true)),
            &workflow,
            now,
        )
        .unwrap();
    }
    assert_eq!(run.record().status, RunStatus::Failed);
    assert_eq!(run.attempts().len(), 3);
    assert!(run.admit(1_000).unwrap().is_none());
}

#[test]
fn invalid_continuation_preserves_completed_work_and_stops_run() {
    struct BadContinuation;
    impl WorkflowDefinition for BadContinuation {
        fn id(&self) -> DefinitionId {
            TestWorkflow::default().id()
        }
        fn transition(
            &self,
            state: &Value,
            event: &WorkflowEvent,
        ) -> Result<Transition, DomainError> {
            match event {
                WorkflowEvent::Started(_) => TestWorkflow::default().transition(state, event),
                WorkflowEvent::ActivityCompleted { .. } => Ok(Transition {
                    state: Value::Null,
                    commands: vec![],
                }),
                WorkflowEvent::TimerFired { .. }
                | WorkflowEvent::InputReceived { .. }
                | WorkflowEvent::TaskCompleted { .. }
                | WorkflowEvent::TaskGroupCompleted { .. }
                | WorkflowEvent::PlannerQuestionClosed { .. }
                | WorkflowEvent::CompletionRejected { .. } => {
                    Err(DomainError::InvalidWorkflow("unexpected event".into()))
                }
            }
        }
    }
    let workflow = BadContinuation;
    let mut run = Run::start(&workflow, Value::Null, 5, 0).unwrap();
    let execution = run.admit(0).unwrap().unwrap();
    run.complete(&execution, Outcome::Success(json!("saved")), &workflow, 1)
        .unwrap();
    assert_eq!(run.record().status, RunStatus::Failed);
    assert_eq!(
        run.record().error.as_ref().unwrap().code,
        "invalid_workflow"
    );
    assert_eq!(run.activities()[0].result, Some(json!("saved")));
    assert_eq!(run.activities()[0].status, ActivityStatus::Succeeded);
    assert!(run.admit(2).unwrap().is_none());
}
