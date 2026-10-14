//! Compare native and standalone coding contracts with the same small repair.
#[path = "support/tool_dispatch_probe.rs"]
mod probe;
use clap::Parser;

#[tokio::main]
async fn main() -> probe::Result<()> {
    let report = probe::run(probe::Options::parse()).await?;
    let arms: Vec<_> = report["arms"].as_array().unwrap().iter().map(|arm|
        serde_json::json!({"name":arm["name"],"status":arm["status"],"stop":arm["stop"],"model_calls":arm["model_calls"],"tool_sequence":arm["tool_sequence"]})
    ).collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "status":report["status"],"directory":report["directory"],"arms":arms
        }))?
    );
    if !matches!(report["status"].as_str(), Some("verified" | "prepared")) {
        return Err(
            "probe did not verify both tool-dispatch contracts; inspect report.json".into(),
        );
    }
    Ok(())
}
