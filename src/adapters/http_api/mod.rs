//! Local HTTP transport for native team services. Execution remains in the existing worker.
use crate::{
    application::{AgentService, Error},
    domain::DomainError,
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, oneshot, watch},
};
mod coding;
mod controls;
mod errors;
mod events;
mod harness;
mod host;
mod memory;
mod permissions;
mod project_profiles;
mod project_refresh;
mod project_verification;
mod routes;
mod steering;
mod storage;
mod summary;
mod turn_recovery;
mod worker_cancellation;
mod worker_pause;
use errors::ApiError;
pub use host::{HarnessHostOptions, serve_with_worker};

pub const MAX_JSON_BODY_BYTES: usize = 1024 * 1024;
#[derive(Clone, Copy, Debug)]
pub struct ApiOptions {
    pub event_poll_interval: Duration,
    pub max_event_streams: usize,
    pub request_timeout: Duration,
}
impl Default for ApiOptions {
    fn default() -> Self {
        Self {
            event_poll_interval: Duration::from_millis(250),
            max_event_streams: 32,
            request_timeout: Duration::from_secs(30),
        }
    }
}
struct ApiState {
    agents: Arc<AgentService>,
    port: u16,
    options: ApiOptions,
    stopping: watch::Receiver<bool>,
    streams: Arc<Semaphore>,
}
pub async fn bind(address: SocketAddr) -> Result<TcpListener, Error> {
    check_address(address)?;
    Ok(TcpListener::bind(address).await?)
}
fn check_address(address: SocketAddr) -> Result<(), Error> {
    if !address.ip().is_loopback() {
        return Err(DomainError::InvalidConfiguration(
            "the local API requires a loopback bind address".into(),
        )
        .into());
    }
    Ok(())
}

/// Signal shutdown to live streams, stop accepting connections, and drain accepted requests.
pub async fn serve(
    listener: TcpListener,
    agents: Arc<AgentService>,
    options: ApiOptions,
    shutdown: impl std::future::Future<Output = std::io::Result<()>> + Send + 'static,
) -> Result<(), Error> {
    let address = listener.local_addr()?;
    check_address(address)?;
    if !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&options.event_poll_interval)
        || !(1..=128).contains(&options.max_event_streams)
        || !(Duration::from_millis(1)..=Duration::from_secs(300)).contains(&options.request_timeout)
    {
        return Err(DomainError::InvalidConfiguration(
            "API polling requires 1..60000 ms, 1..128 event streams, and request deadlines of 1..300000 ms".into(),
        )
        .into());
    }
    let (stop_streams, stopping) = watch::channel(false);
    let state = Arc::new(ApiState {
        agents,
        port: address.port(),
        options,
        stopping,
        streams: Arc::new(Semaphore::new(options.max_event_streams)),
    });
    let app = Router::new()
        .route("/v1/harness/config", get(harness::config))
        .route("/v1/harness/project-profiles", get(project_profiles::list))
        .route("/v1/harness/project-detection", post(project_profiles::detect))
        .route("/v1/harness/project-profiles/{id}", get(project_profiles::get).post(project_profiles::change))
        .route("/v1/harness/storage", get(storage::report))
        .route("/v1/harness/storage/cleanup", post(storage::clean))
        .route(
            "/v1/harness/sessions",
            get(harness::sessions).post(harness::start),
        )
        .route("/v1/harness/conversations", get(harness::conversations))
        .route("/v1/harness/sessions/{session}/conversation", get(harness::continuous))
        .route("/v1/harness/sessions/{session}/metadata",get(harness::metadata).post(harness::update_metadata))
        .route("/v1/harness/sessions/{session}/memory", get(memory::current))
        .route("/v1/harness/sessions/{session}/permissions", post(permissions::change))
        .route("/v1/harness/sessions/{session}/turns/{turn}/permissions", get(permissions::current))
        .route("/v1/harness/sessions/{session}/summary", get(summary::current).post(summary::start))
        .route("/v1/harness/sessions/{session}/summary/policy", post(summary::policy))
        .route("/v1/harness/sessions/{session}/turns/{task}/recovery", get(turn_recovery::current).post(turn_recovery::retry))
        .route("/v1/harness/sessions/{session}/turns/{task}/steering", get(steering::current).post(steering::send))
        .route("/v1/harness/sessions/{session}/turns/{task}/project-setup", get(harness::project_setup).post(harness::accept_project_setup))
        .route("/v1/harness/sessions/{session}/turns/{task}/workers/{worker}/steering", get(steering::worker_current).post(steering::worker_send))
        .route("/v1/harness/sessions/{session}/turns/{task}/workers/{worker}/recovery", get(turn_recovery::worker_current).post(turn_recovery::worker_decide))
        .route("/v1/harness/sessions/{session}/turns/{task}/workers/{worker}/cancellation", get(worker_cancellation::current).post(worker_cancellation::stop))
        .route("/v1/harness/sessions/{session}/turns/{task}/workers/{worker}/pause", get(worker_pause::current).post(worker_pause::control))
        .route("/v1/harness/sessions/{session}/turns/{turn}/memory", get(memory::turn))
        .route("/v1/harness/sessions/{session}/actors/{agent}/memory/clear", post(memory::clear))
        .route(
            "/v1/harness/sessions/{session_id}",
            get(harness::conversation),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns",
            post(harness::append),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/changes",
            get(coding::changes),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/delegated-work",
            get(coding::delegated_work),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/tool-recovery",
            get(coding::tool_recoveries),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/child-import-recovery",
            get(coding::child_import_recoveries),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/runs/{run_id}/child-import-reconciliation",
            post(coding::reconcile_child_import),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/runs/{run_id}/reconciliation",
            post(coding::reconcile_tool),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/applications",
            get(coding::applications).post(coding::preview),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/applications/{run_id}",
            get(coding::application),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/applications/{run_id}/responses",
            post(coding::respond),
        )
        .route(
            "/v1/harness/sessions/{session_id}/turns/{task_id}/applications/{run_id}/reconciliation",
            post(coding::reconcile),
        )
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/applications/{application_id}/verifications",get(project_verification::list).post(project_verification::start))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/applications/{application_id}/verifications/{verification_id}",get(project_verification::status))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/applications/{application_id}/verifications/{verification_id}/responses",post(project_verification::respond))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/applications/{application_id}/verifications/{verification_id}/reconciliation",post(project_verification::reconcile))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/project-refresh",get(project_refresh::status).post(project_refresh::start))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/project-refresh/inspection",get(project_refresh::inspect))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/project-refresh/cancellation",post(project_refresh::cancel))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/project-setup-review",get(project_refresh::status).post(project_refresh::update_setup))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/project-setup-review/inspection",get(project_refresh::inspect))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/project-setup-review/cancellation",post(project_refresh::cancel))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/applications/{application_id}/verifications/{verification_id}/repair",get(project_verification::repair).post(project_verification::start_repair))
        .route("/v1/harness/sessions/{session_id}/turns/{task_id}/applications/{application_id}/verifications/{verification_id}/repair/cancellation",post(project_verification::cancel_repair))
        .route(
            "/v1/health",
            get(|| async { Json(serde_json::json!({"status":"ok","api_version":1})) }),
        )
        .route("/v1/teams", get(routes::list).post(routes::create))
        .route("/v1/teams/{team_id}", get(routes::team_status))
        .route("/v1/teams/{team_id}/resume", post(routes::resume))
        .route(
            "/v1/teams/{team_id}/tasks",
            get(routes::goals).post(routes::submit),
        )
        .route(
            "/v1/teams/{team_id}/tasks/{task_id}",
            get(routes::task_status),
        )
        .route(
            "/v1/teams/{team_id}/tasks/{task_id}/inbox",
            get(routes::inbox),
        )
        .route(
            "/v1/teams/{team_id}/tasks/{task_id}/controls",
            get(controls::status),
        )
        .route(
            "/v1/teams/{team_id}/tasks/{task_id}/pause",
            post(controls::pause),
        )
        .route(
            "/v1/teams/{team_id}/tasks/{task_id}/resume",
            post(controls::resume),
        )
        .route(
            "/v1/teams/{team_id}/tasks/{task_id}/cancel",
            post(controls::cancel),
        )
        .route(
            "/v1/teams/{team_id}/tasks/{task_id}/waits/{wait_id}/responses",
            post(routes::respond),
        )
        .route(
            "/v1/teams/{team_id}/tasks/{task_id}/events",
            get(events::read),
        )
        .route(
            "/v1/teams/{team_id}/tasks/{task_id}/events/stream",
            get(events::stream),
        )
        .fallback(|| async {
            ApiError::new(StatusCode::NOT_FOUND, "not_found", "unknown API route")
        })
        .method_not_allowed_fallback(|| async {
            ApiError::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "method is not supported for this route",
            )
        })
        .layer(DefaultBodyLimit::max(MAX_JSON_BODY_BYTES))
        .layer(middleware::from_fn_with_state(state.clone(), local_request))
        .with_state(state);
    let (stop_http, stopped) = oneshot::channel();
    let server = axum::serve(listener, app).with_graceful_shutdown(async {
        let _ = stopped.await;
    });
    use std::future::IntoFuture;
    let server = server.into_future();
    tokio::pin!(server);
    tokio::select! {
        result=&mut server => {result?;Ok(())}
        signal=shutdown => {
            let _=stop_streams.send(true);
            let _=stop_http.send(());
            server.await?;
            signal?;
            Ok(())
        }
    }
}

async fn local_request(
    State(state): State<Arc<ApiState>>,
    request: Request,
    next: Next,
) -> Response {
    let mut response = match validate_request(&state, &request) {
        Ok(()) => match tokio::time::timeout(state.options.request_timeout,next.run(request)).await {
            Ok(response)=>response,
            Err(_)=>ApiError::new(StatusCode::REQUEST_TIMEOUT,"request_timeout","API request deadline exceeded; retry using the same team name, message ID or wait response").into_response(),
        },
        Err(error) => error.into_response(),
    };
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static header"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        "nosniff".parse().expect("static header"),
    );
    response
}

fn validate_request(state: &ApiState, request: &Request) -> Result<(), ApiError> {
    if *state.stopping.borrow() {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "shutting_down",
            "API is shutting down",
        ));
    }
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_host",
                "Host must identify this loopback listener",
            )
        })?;
    let valid = host
        .parse::<SocketAddr>()
        .is_ok_and(|a| a.ip().is_loopback() && a.port() == state.port)
        || host.eq_ignore_ascii_case(&format!("localhost:{}", state.port));
    if !valid {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_host",
            "Host must identify this loopback listener",
        ));
    }
    if let Some(origin) = request.headers().get(header::ORIGIN)
        && !origin
            .to_str()
            .is_ok_and(|o| o.eq_ignore_ascii_case(&format!("http://{host}")))
    {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "origin_refused",
            "request Origin must match this API's origin",
        ));
    }
    if let Some(site) = request.headers().get("sec-fetch-site")
        && !site
            .to_str()
            .is_ok_and(|s| s == "same-origin" || s == "none")
    {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "origin_refused",
            "cross-origin browser requests are not accepted",
        ));
    }
    if request.uri().to_string().len() > 8192 {
        return Err(ApiError::new(
            StatusCode::URI_TOO_LONG,
            "uri_too_long",
            "API request URI exceeds 8192 bytes",
        ));
    }
    Ok(())
}
