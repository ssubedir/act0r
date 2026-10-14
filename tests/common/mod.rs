#![allow(dead_code)]
use act0r::{
    Engine,
    adapters::{file_lock::FileOwnership, sqlite::SqliteStore},
    application::ports::*,
    domain::*,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
};

pub struct ManualClock(pub AtomicI64);
impl ManualClock {
    pub fn new(now: i64) -> Self {
        Self(AtomicI64::new(now))
    }
    pub fn set(&self, now: i64) {
        self.0.store(now, Ordering::SeqCst);
    }
}
impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

pub struct TestWorkflow {
    pub recovery: RecoveryPolicy,
    pub total: u64,
}
impl Default for TestWorkflow {
    fn default() -> Self {
        Self {
            recovery: RecoveryPolicy::SafeToRepeat,
            total: 2,
        }
    }
}
impl WorkflowDefinition for TestWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("test", 1)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        let step = match event {
            WorkflowEvent::Started(_) => 0,
            WorkflowEvent::ActivityCompleted { .. } => state["step"].as_u64().unwrap() + 1,
            WorkflowEvent::TimerFired { .. }
            | WorkflowEvent::InputReceived { .. }
            | WorkflowEvent::TaskCompleted { .. }
            | WorkflowEvent::TaskGroupCompleted { .. }
            | WorkflowEvent::PlannerQuestionClosed { .. }
            | WorkflowEvent::CompletionRejected { .. } => {
                return Err(DomainError::InvalidWorkflow("unexpected test event".into()));
            }
        };
        let command = if step >= self.total {
            Command::Complete(json!({"done": true}))
        } else {
            Command::Schedule(ActivitySpec {
                handler: DefinitionId::new("test_step", 1),
                input: json!({"step": step}),
                retry: RetryPolicy {
                    max_attempts: 3,
                    delay_ms: 100,
                    max_delay_ms: 200,
                },
                recovery: self.recovery,
                usage: None,
            })
        };
        Ok(Transition {
            state: json!({"step": step}),
            commands: vec![command],
        })
    }
}

#[derive(Default)]
pub struct TestExecutor {
    pub outcomes: Mutex<VecDeque<Outcome>>,
    pub calls: Mutex<Vec<ActivityExecution>>,
    pub unsupported: bool,
}
impl ActivityExecutor for TestExecutor {
    fn supports(&self, handler: &DefinitionId) -> bool {
        !self.unsupported && handler == &DefinitionId::new("test_step", 1)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(execution);
            Ok(self
                .outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Outcome::Success(json!({"ok": true}))))
        })
    }
}
pub struct Fixture {
    pub directory: tempfile::TempDir,
    pub store: Arc<SqliteStore>,
    pub clock: Arc<ManualClock>,
    pub executor: Arc<TestExecutor>,
    pub engine: Engine,
}
impl Fixture {
    pub async fn new(workflow: TestWorkflow) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteStore::open(directory.path().join("test.db"))
                .await
                .unwrap(),
        );
        let clock = Arc::new(ManualClock::new(1_000));
        let executor = Arc::new(TestExecutor::default());
        let engine = Engine::new(
            store.clone(),
            executor.clone(),
            clock.clone(),
            Arc::new(FileOwnership::new(store.database_path())),
            vec![Arc::new(workflow)],
        )
        .unwrap();
        Self {
            directory,
            store,
            clock,
            executor,
            engine,
        }
    }
    pub async fn start(&self, limit: u32) -> RunId {
        self.engine
            .start(DefinitionId::new("test", 1), Value::Null, limit)
            .await
            .unwrap()
    }
}
