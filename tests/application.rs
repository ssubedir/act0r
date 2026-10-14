mod common;
use act0r::{Engine, adapters::file_lock::FileOwnership, application::ports::*, domain::*};
use common::*;
use serde_json::json;
use std::sync::Arc;

struct InspectingExecutor {
    store: Arc<dyn ExecutionStore>,
}
impl ActivityExecutor for InspectingExecutor {
    fn supports(&self, handler: &DefinitionId) -> bool {
        handler == &DefinitionId::new("test_step", 1)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            let run = self.store.get(execution.run_id).await?;
            let activity = run
                .activities()
                .iter()
                .find(|a| a.id == execution.activity_id)
                .unwrap();
            assert_eq!(activity.active_attempt, Some(execution.attempt_id));
            assert_eq!(activity.status, ActivityStatus::Running);
            assert_eq!(
                run.record().allowance.admitted,
                execution.input["step"].as_u64().unwrap() as u32 + 1
            );
            // A second writer can transact while the external handler is executing.
            self.store
                .mutate(execution.run_id, Box::new(|_| Ok(None)))
                .await?;
            Ok(Outcome::Success(json!("observed committed intent")))
        })
    }
}

#[tokio::test]
async fn application_commits_intent_before_io_and_completion_before_return() {
    let f = Fixture::new(TestWorkflow::default()).await;
    let engine = Engine::new(
        f.store.clone(),
        Arc::new(InspectingExecutor {
            store: f.store.clone(),
        }),
        f.clock.clone(),
        Arc::new(FileOwnership::new(f.store.database_path())),
        vec![Arc::new(TestWorkflow::default())],
    )
    .unwrap();
    let id = f.start(5).await;
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.step().await.unwrap());
    let run = engine.status(id).await.unwrap();
    assert_eq!(run.activities()[0].status, ActivityStatus::Succeeded);
    assert_eq!(run.activities()[1].status, ActivityStatus::Queued);
    assert!(worker.step().await.unwrap());
    assert_eq!(
        engine.status(id).await.unwrap().record().status,
        RunStatus::Succeeded
    );
}
