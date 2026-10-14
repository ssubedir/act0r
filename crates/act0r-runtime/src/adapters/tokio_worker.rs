use crate::{
    Error, Worker,
    domain::{ActivityExecution, DomainError, Outcome},
};
use std::{collections::HashMap, time::Duration};
use tokio::task::{Id, JoinError, JoinSet};

pub struct WorkerOptions {
    pub until_idle: bool,
    pub poll_interval: Duration,
    pub activity_timeout: Duration,
    pub shutdown_grace: Duration,
    pub max_concurrent_activities: usize,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            until_idle: false,
            poll_interval: Duration::from_millis(100),
            activity_timeout: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(5),
            max_concurrent_activities: 1,
        }
    }
}

type Finished = Result<(Id, Outcome), JoinError>;

#[derive(Default)]
struct InFlight {
    operations: JoinSet<Outcome>,
    executions: HashMap<Id, ActivityExecution>,
}
impl InFlight {
    fn start(&mut self, worker: &Worker, execution: ActivityExecution, base_timeout: Duration) {
        let timeout_ms = worker.execution_timeout_ms(
            &execution,
            u64::try_from(base_timeout.as_millis()).unwrap_or(u64::MAX),
        );
        let operation = worker.execute(execution.clone());
        let handle = self.operations.spawn(async move {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), operation).await {
                Ok(result) => result.unwrap_or_else(|e| Outcome::Uncertain(e.to_string())),
                Err(_) => Outcome::Uncertain(
                    "activity timed out; external effects may still be running".into(),
                ),
            }
        });
        self.executions.insert(handle.id(), execution);
    }

    async fn complete(
        &mut self,
        worker: &mut Worker,
        finished: Finished,
        aborted: bool,
    ) -> Result<(), Error> {
        let (id, outcome) = match finished {
            Ok(result) => result,
            Err(error) => {
                let reason = if aborted && error.is_cancelled() {
                    "shutdown grace expired".into()
                } else {
                    format!("activity task failed; external effects may still be running: {error}")
                };
                (error.id(), Outcome::Uncertain(reason))
            }
        };
        let execution = self.executions.remove(&id).expect("tracked execution task");
        loop {
            match worker.complete(execution.clone(), outcome.clone()).await {
                Err(Error::StorageBusy(_)) => tokio::time::sleep(Duration::from_millis(100)).await,
                result => {
                    result?;
                    break;
                }
            }
        }
        tracing::info!("activity outcome committed");
        Ok(())
    }

    async fn drain(&mut self, worker: &mut Worker, grace: Duration) -> Result<(), Error> {
        let deadline = tokio::time::sleep(grace);
        tokio::pin!(deadline);
        while !self.operations.is_empty() {
            tokio::select! {
                biased;
                finished = self.operations.join_next_with_id() => {
                    self.complete(worker, finished.expect("in-flight operation"), false).await?;
                }
                _ = &mut deadline => {
                    // Stop local futures, then persist every remaining attempt conservatively.
                    // Aborting a future cannot undo an external effect.
                    self.operations.abort_all();
                    while let Some(finished) = self.operations.join_next_with_id().await {
                        self.complete(worker, finished, true).await?;
                    }
                }
            }
        }
        Ok(())
    }
}

pub async fn run(mut worker: Worker, options: WorkerOptions) -> Result<(), Error> {
    run_until(&mut worker, options, shutdown_signal()).await
}

/// Hosts supply a lifecycle signal; shutdown still drains and persists uncertain attempts.
pub async fn run_with_shutdown(
    mut worker: Worker,
    options: WorkerOptions,
    shutdown: impl std::future::Future<Output = std::io::Result<()>>,
) -> Result<(), Error> {
    run_until(&mut worker, options, shutdown).await
}

async fn run_until(
    worker: &mut Worker,
    options: WorkerOptions,
    shutdown: impl std::future::Future<Output = std::io::Result<()>>,
) -> Result<(), Error> {
    if options.max_concurrent_activities == 0 {
        return Err(DomainError::InvalidConfiguration(
            "maximum concurrent activities must be positive".into(),
        )
        .into());
    }
    tokio::pin!(shutdown);
    let mut in_flight = InFlight::default();
    loop {
        // Observe shutdown and commit ready outcomes before making new admissions.
        tokio::select! {
            biased;
            signal = &mut shutdown => {
                signal?;
                return in_flight.drain(worker, options.shutdown_grace).await;
            }
            finished = in_flight.operations.join_next_with_id(), if !in_flight.operations.is_empty() => {
                in_flight.complete(worker, finished.expect("in-flight operation"), false).await?;
                continue;
            }
            _ = std::future::ready(()) => {}
        }
        let advanced = match worker.advance_work().await {
            Err(Error::StorageBusy(_)) => false,
            result => result?,
        };
        let fired = match worker.fire_due_timer().await {
            Err(Error::StorageBusy(_)) => false,
            result => result?,
        };
        // Mailboxes, cancellation propagation and durable timers progress even at capacity.
        tokio::select! {
            biased;
            signal = &mut shutdown => {
                signal?;
                return in_flight.drain(worker, options.shutdown_grace).await;
            }
            _ = std::future::ready(()) => {}
        }
        if in_flight.operations.len() < options.max_concurrent_activities
            && let Some(execution) = match worker.admit().await {
                Err(Error::StorageBusy(_)) => None,
                result => result?,
            }
        {
            tracing::info!(activity = %execution.activity_id, attempt = %execution.attempt_id, "execution intent committed");
            in_flight.start(worker, execution, options.activity_timeout);
            continue;
        }
        if advanced || fired {
            continue;
        }
        if options.until_idle
            && in_flight.operations.is_empty()
            && !worker.has_active_runs().await?
        {
            return Ok(());
        }
        tokio::select! {
            biased;
            signal = &mut shutdown => {
                signal?;
                return in_flight.drain(worker, options.shutdown_grace).await;
            }
            finished = in_flight.operations.join_next_with_id(), if !in_flight.operations.is_empty() => {
                in_flight.complete(worker, finished.expect("in-flight operation"), false).await?;
            }
            _ = tokio::time::sleep(options.poll_interval) => {}
        }
    }
}

/// Wait for the platform shutdown signal shared by the worker and product transports.
pub async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result, _ = terminate.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ActivityStatus, DefinitionId, RunStatus};
    use serde_json::json;

    fn options() -> WorkerOptions {
        WorkerOptions {
            until_idle: true,
            poll_interval: Duration::from_millis(1),
            activity_timeout: Duration::from_secs(10),
            shutdown_grace: Duration::ZERO,
            max_concurrent_activities: 1,
        }
    }
    #[tokio::test]
    async fn finished_outcome_is_retained_while_its_commit_waits_for_maintenance() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("completion-busy.db");
        let (engine, _) = crate::bootstrap::local_app(&database).await.unwrap();
        let run = engine
            .start(
                DefinitionId::new("demo", 1),
                json!({"message":"exact saved outcome"}),
                5,
            )
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        let execution = worker.admit().await.unwrap().unwrap();
        let mut in_flight = InFlight::default();
        in_flight.start(&worker, execution, Duration::from_secs(5));
        let finished = in_flight.operations.join_next_with_id().await.unwrap();
        let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}", database.display()))
            .await
            .unwrap();
        let guard = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let commit =
            tokio::spawn(async move { in_flight.complete(&mut worker, finished, false).await });
        tokio::time::sleep(Duration::from_millis(5400)).await;
        let waiting = !commit.is_finished();
        guard.commit().await.unwrap();
        assert!(
            waiting,
            "finished outcome was discarded after a transient busy response"
        );
        tokio::time::timeout(Duration::from_secs(12), commit)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let saved = engine.status(run).await.unwrap();
        assert_eq!(saved.record().allowance.admitted, 1);
        assert_eq!(saved.activities()[0].status, ActivityStatus::Succeeded);
        assert_eq!(saved.attempts().len(), 1);
        assert_eq!(saved.record().state["stage"], "deciding");
        pool.close().await;
    }

    async fn wait_for_admissions(engine: &crate::Engine, ids: &[crate::domain::RunId]) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut ready = true;
                for id in ids {
                    ready &= engine
                        .status(*id)
                        .await
                        .unwrap()
                        .record()
                        .allowance
                        .admitted
                        == 1;
                }
                if ready {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn zero_concurrency_is_rejected_before_admission() {
        let directory = tempfile::tempdir().unwrap();
        let engine = crate::bootstrap::local_engine(directory.path().join("invalid.db"))
            .await
            .unwrap();
        let id = engine
            .start(DefinitionId::new("demo", 1), json!({"message":"stop"}), 5)
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        let mut options = options();
        options.max_concurrent_activities = 0;
        assert!(matches!(
            run_until(&mut worker, options, std::future::pending()).await,
            Err(Error::Domain(DomainError::InvalidConfiguration(_)))
        ));
        assert_eq!(
            engine.status(id).await.unwrap().record().allowance.admitted,
            0
        );
        assert_eq!(WorkerOptions::default().max_concurrent_activities, 1);
    }

    #[tokio::test]
    async fn shutdown_drains_all_slots_with_one_grace_and_stops_admission() {
        let directory = tempfile::tempdir().unwrap();
        let engine = crate::bootstrap::local_engine(directory.path().join("drain.db"))
            .await
            .unwrap();
        let mut ids = vec![];
        // The first finishes during grace; the second stays uncertain; the third is never admitted.
        for delay_ms in [250, 5_000, 0] {
            ids.push(
                engine
                    .start(
                        DefinitionId::new("demo", 1),
                        json!({"message":"drain", "delay_ms":delay_ms}),
                        5,
                    )
                    .await
                    .unwrap(),
            );
        }
        let mut worker = engine.worker().await.unwrap();
        let shutdown = async {
            wait_for_admissions(&engine, &ids[..2]).await;
            Ok(())
        };
        let mut options = options();
        options.max_concurrent_activities = 2;
        options.shutdown_grace = Duration::from_millis(600);
        run_until(&mut worker, options, shutdown).await.unwrap();
        let fast = engine.status(ids[0]).await.unwrap();
        assert!(matches!(
            fast.attempts()[0].outcome,
            Some(Outcome::Success(_))
        ));
        assert_eq!(fast.record().allowance.admitted, 1);
        assert_eq!(fast.activities()[1].status, ActivityStatus::Queued);
        let slow = engine.status(ids[1]).await.unwrap();
        assert!(matches!(&slow.attempts()[0].outcome,
            Some(Outcome::Uncertain(reason)) if reason == "shutdown grace expired"));
        assert_eq!(slow.record().allowance.admitted, 1);
        assert_eq!(
            engine
                .status(ids[2])
                .await
                .unwrap()
                .record()
                .allowance
                .admitted,
            0
        );
    }

    #[tokio::test]
    async fn zero_grace_records_every_in_flight_attempt() {
        let directory = tempfile::tempdir().unwrap();
        let engine = crate::bootstrap::local_engine(directory.path().join("abort.db"))
            .await
            .unwrap();
        let mut ids = vec![];
        for _ in 0..3 {
            ids.push(
                engine
                    .start(
                        DefinitionId::new("demo", 1),
                        json!({"message":"abort", "delay_ms":5_000}),
                        5,
                    )
                    .await
                    .unwrap(),
            );
        }
        let mut worker = engine.worker().await.unwrap();
        let shutdown = async {
            wait_for_admissions(&engine, &ids).await;
            Ok(())
        };
        let mut options = options();
        options.max_concurrent_activities = 3;
        run_until(&mut worker, options, shutdown).await.unwrap();
        for id in ids {
            let run = engine.status(id).await.unwrap();
            assert_eq!(run.attempts().len(), 1);
            assert!(matches!(
                run.attempts()[0].outcome,
                Some(Outcome::Uncertain(_))
            ));
            assert!(
                run.activities()
                    .iter()
                    .all(|a| a.status != ActivityStatus::Running)
            );
        }
    }

    #[tokio::test]
    async fn durable_timer_fires_while_another_run_fills_execution_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let engine = crate::bootstrap::local_engine(directory.path().join("timer.db"))
            .await
            .unwrap();
        let busy = engine
            .start(
                DefinitionId::new("demo", 1),
                json!({"message":"busy", "delay_ms":5_000}),
                5,
            )
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        let timer_engine = engine.clone();
        // Poll this writer independently: a shutdown future holding a transaction
        // would stop progressing while the runner awaits another SQLite writer.
        let timer_check = tokio::spawn(async move {
            wait_for_admissions(&timer_engine, &[busy]).await;
            let timer = timer_engine
                .start(
                    DefinitionId::new("timer_demo", 1),
                    json!({"message":"due", "wait_ms":0}),
                    5,
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let run = timer_engine.status(timer).await.unwrap();
                    if run.timers()[0].status == crate::domain::TimerStatus::Fired {
                        assert_eq!(run.record().allowance.admitted, 0);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        });
        let shutdown = async {
            timer_check.await.unwrap();
            Ok(())
        };
        run_until(&mut worker, options(), shutdown).await.unwrap();
        assert_eq!(
            engine
                .status(busy)
                .await
                .unwrap()
                .record()
                .allowance
                .admitted,
            1
        );
    }

    #[tokio::test]
    async fn pending_shutdown_does_not_admit_work() {
        let directory = tempfile::tempdir().unwrap();
        let engine = crate::bootstrap::local_engine(directory.path().join("shutdown.db"))
            .await
            .unwrap();
        let id = engine
            .start(DefinitionId::new("demo", 1), json!({"message": "stop"}), 5)
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        run_until(&mut worker, options(), std::future::ready(Ok(())))
            .await
            .unwrap();
        let run = engine.status(id).await.unwrap();
        assert_eq!(run.record().allowance.admitted, 0);
        assert_eq!(run.record().status, RunStatus::Queued);
    }

    #[tokio::test]
    async fn graceful_shutdown_records_uncertainty_after_drain_expires() {
        let directory = tempfile::tempdir().unwrap();
        let engine = crate::bootstrap::local_engine(directory.path().join("shutdown.db"))
            .await
            .unwrap();
        let id = engine
            .start(
                DefinitionId::new("demo", 1),
                json!({"message": "stop", "delay_ms": 5_000}),
                5,
            )
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        let shutdown = async {
            loop {
                if engine.status(id).await.unwrap().activities()[0].status
                    == ActivityStatus::Running
                {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        run_until(&mut worker, options(), shutdown).await.unwrap();
        let run = engine.status(id).await.unwrap();
        assert_eq!(run.activities()[0].status, ActivityStatus::Queued);
        assert_eq!(run.record().allowance.admitted, 1);
        assert!(matches!(
            run.attempts()[0].outcome,
            Some(Outcome::Uncertain(_))
        ));
    }

    #[tokio::test]
    async fn timeout_retries_are_bounded_and_charged() {
        let directory = tempfile::tempdir().unwrap();
        let engine = crate::bootstrap::local_engine(directory.path().join("timeout.db"))
            .await
            .unwrap();
        let id = engine
            .start(
                DefinitionId::new("demo", 1),
                json!({"message": "timeout", "delay_ms": 5_000}),
                5,
            )
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        let mut options = options();
        options.activity_timeout = Duration::from_millis(1);
        run_until(&mut worker, options, std::future::pending())
            .await
            .unwrap();
        let run = engine.status(id).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Failed);
        assert_eq!(run.record().allowance.admitted, 3);
        assert!(
            run.attempts()
                .iter()
                .all(|a| matches!(a.outcome, Some(Outcome::Uncertain(_))))
        );
    }
}
