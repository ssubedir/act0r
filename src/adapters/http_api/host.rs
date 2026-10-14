//! One local process hosts the HTTP adapter and the existing durable worker.
use super::*;
use crate::{
    Worker,
    adapters::tokio_worker::{self, WorkerOptions},
};

#[derive(Default)]
pub struct HarnessHostOptions {
    pub api: ApiOptions,
    pub worker: WorkerOptions,
}

async fn stopped(mut signal: watch::Receiver<bool>) -> std::io::Result<()> {
    while !*signal.borrow_and_update() {
        if signal.changed().await.is_err() {
            break;
        }
    }
    Ok(())
}

/// Drain both adapters on shutdown or if either adapter exits. The caller acquires ownership first.
pub async fn serve_with_worker(
    listener: TcpListener,
    agents: Arc<AgentService>,
    worker: Worker,
    options: HarnessHostOptions,
    shutdown: impl std::future::Future<Output = std::io::Result<()>> + Send + 'static,
) -> Result<(), Error> {
    let (stop, signal) = watch::channel(false);
    let api = serve(listener, agents, options.api, stopped(signal.clone()));
    let worker = tokio_worker::run_with_shutdown(worker, options.worker, stopped(signal));
    tokio::pin!(api, worker, shutdown);
    let (api_result, worker_result, signal_result) = tokio::select! {
        result = &mut api => (Some(result), None, Ok(())),
        result = &mut worker => (None, Some(result), Ok(())),
        result = &mut shutdown => (None, None, result),
    };
    let _ = stop.send(true);
    let (api_result, worker_result) = tokio::join!(
        async {
            match api_result {
                Some(result) => result,
                None => api.await,
            }
        },
        async {
            match worker_result {
                Some(result) => result,
                None => worker.await,
            }
        },
    );
    api_result?;
    worker_result?;
    signal_result?;
    Ok(())
}
