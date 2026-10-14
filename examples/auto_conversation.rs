//! Run the normal Auto API with an isolated JavaScript project.
#[path = "support/auto_conversation.rs"]
mod demo;
use clap::Parser;

#[tokio::main]
async fn main() -> demo::Result<()> {
    let report = demo::run(demo::Options::parse()).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "status":report["status"],"phase":report["phase"],"directory":report["directory"],
            "mode":report["mode"],"model_calls":report["model_calls"],
            "applied_tests_ok":report["applied_tests_ok"],"restarts":report["restarts"],
            "session":report["session"]
        }))?
    );
    Ok(())
}
