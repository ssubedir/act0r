//! cargo run --locked --example search -- .build/search-demo/demo.db
use act0r::{bootstrap::local_app, domain::search::SearchConfig};
use std::{path::Path, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".build/search-demo/demo.db".into());
    if let Some(parent) = Path::new(&database)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let (engine, agents) = local_app(database).await?;
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let config: SearchConfig = serde_json::from_str(include_str!("configs/search-scripted.json"))?;
    let id = agents
        .search()?
        .start(
            examples.join("benchmark-cases/addition/source"),
            config,
            &examples.join("configs"),
        )
        .await?;
    let mut worker = engine.worker().await?;
    while worker.has_active_runs().await? {
        if !worker.step().await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let search = agents.search()?.status(id).await?;
    let task = agents.search()?.task(id).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"search":search.record(),"task":task.record()})
        )?
    );
    Ok(())
}
