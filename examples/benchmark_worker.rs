//! A serial worker with request pacing for small live-provider benchmark pilots.
//! cargo run --locked --example benchmark_worker -- ./pilot.db 10000
use act0r::{Error, bootstrap::local_engine, domain::Outcome};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let database = args.next().ok_or_else(|| {
        Error::Unavailable("usage: benchmark_worker DATABASE [MODEL_INTERVAL_MS]".into())
    })?;
    let interval: u64 = args
        .next()
        .unwrap_or_else(|| "10000".into())
        .parse()
        .map_err(|_| {
            Error::Unavailable("model interval must be an integer in 0..60000 milliseconds".into())
        })?;
    if interval > 60_000 || args.next().is_some() {
        return Err(Error::Unavailable(
            "usage: benchmark_worker DATABASE [MODEL_INTERVAL_MS in 0..60000]".into(),
        ));
    }
    let engine = local_engine(database).await?;
    let mut worker = engine.worker().await?;
    // Also wait before the first request, so restarting does not create an immediate burst.
    let mut last_model_started = Instant::now();
    while worker.has_active_runs().await? {
        if worker.advance_work().await? || worker.fire_due_timer().await? {
            continue;
        }
        let Some(execution) = worker.admit().await? else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        if matches!(
            execution.handler.name.as_str(),
            "agent.model" | "search.propose"
        ) {
            let wait = Duration::from_millis(interval).saturating_sub(last_model_started.elapsed());
            tokio::time::sleep(wait).await;
            last_model_started = Instant::now();
        }
        eprintln!(
            "Executing {} for run {}",
            execution.handler.name, execution.run_id
        );
        let outcome = match worker.execute(execution.clone()).await {
            Ok(outcome) => outcome,
            Err(error) => Outcome::Uncertain(format!("executor error: {error}")),
        };
        worker.complete(execution, outcome).await?;
    }
    Ok(())
}
