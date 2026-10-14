//! cargo run --example library -- ./library.db
use act0r::{bootstrap::local_engine, domain::DefinitionId};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), act0r::Error> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "library.db".into());
    let engine = local_engine(database).await?;
    let run_id = engine
        .start(
            DefinitionId::new("demo", 1),
            json!({"message": "From the Rust API"}),
            10,
        )
        .await?;
    let mut worker = engine.worker().await?;
    while worker.has_active_runs().await? {
        if !worker.step().await? {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(engine.status(run_id).await?.record())?
    );
    Ok(())
}
