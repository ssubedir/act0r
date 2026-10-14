use crate::{
    Engine,
    adapters::{
        http_api::{self, ApiOptions, HarnessHostOptions},
        tokio_worker::WorkerOptions,
    },
    application::{AgentService, Error},
};
use clap::Subcommand;
use std::{io::Write, net::SocketAddr, num::NonZeroUsize, sync::Arc};

#[derive(Subcommand)]
pub(super) enum ApiCommand {
    /// Listen on loopback, optionally hosting the durable worker in the same process.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: SocketAddr,
        /// Own and run the durable worker alongside the API.
        #[arg(long)]
        with_worker: bool,
        #[arg(long, default_value = "2", requires = "with_worker")]
        max_concurrent_activities: NonZeroUsize,
    },
}
pub(super) async fn execute(
    command: ApiCommand,
    engine: Engine,
    agents: AgentService,
    json_output: bool,
) -> Result<(), Error> {
    let ApiCommand::Serve {
        bind,
        with_worker,
        max_concurrent_activities,
    } = command;
    let listener = http_api::bind(bind).await?;
    let address = listener.local_addr()?;
    let worker = if with_worker {
        Some(engine.worker().await?)
    } else {
        None
    };
    if json_output {
        println!(
            "{}",
            serde_json::json!({"api":"listening","url":format!("http://{address}"),"worker":if with_worker {"running"} else {"external"}})
        );
    } else {
        println!(
            "API listening at http://{address}{}",
            if with_worker {
                "; durable worker running."
            } else {
                "; run the worker against the same database."
            }
        );
    }
    std::io::stdout().flush()?;
    let agents = Arc::new(agents);
    if let Some(worker) = worker {
        return http_api::serve_with_worker(
            listener,
            agents,
            worker,
            HarnessHostOptions {
                worker: WorkerOptions {
                    max_concurrent_activities: max_concurrent_activities.get(),
                    ..Default::default()
                },
                ..Default::default()
            },
            crate::adapters::tokio_worker::shutdown_signal(),
        )
        .await;
    }
    http_api::serve(
        listener,
        agents,
        ApiOptions::default(),
        crate::adapters::tokio_worker::shutdown_signal(),
    )
    .await
}
