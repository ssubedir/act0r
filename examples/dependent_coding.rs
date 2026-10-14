//! cargo run --locked --offline --example dependent_coding
#[path = "support/dependent_coding.rs"]
mod demo;
use clap::Parser;

#[tokio::main]
async fn main() -> demo::Result<()> {
    let options = demo::Options::parse();
    let report = demo::run(options).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
