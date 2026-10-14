//! A focused planner import -> parent test -> verified finish trial.
#[path = "support/planner_dispatch_probe.rs"]
mod probe;
use clap::Parser;

#[tokio::main]
async fn main() -> probe::Result<()> {
    let report = probe::run(probe::Options::parse()).await?;
    let restarts: Vec<_> = report["restarts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| serde_json::json!({"kind":r["kind"],"verified":r["verified_after_restart"]}))
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "status":report["status"],"directory":report["directory"],"stop":report["stop"],
            "model_calls":report["model_calls"],"parent_tests_passed":report["parent_tests_passed"],
            "worker_mode":report["worker_mode"],"worker_model_calls":report["worker_model_calls"],
            "refusal_recovered":report["refusal_recovered"],"restarts":restarts
        }))?
    );
    if !matches!(
        report["status"].as_str(),
        Some("verified" | "prepared" | "checkpoint")
    ) {
        return Err("planner probe did not verify completion; inspect report.json".into());
    }
    Ok(())
}
