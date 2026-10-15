//! Bounded HTTP I/O beneath the SDK's MCP lifecycle and Streamable HTTP transport.
use super::*;
use futures_util::{StreamExt, stream::BoxStream};
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use rmcp::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::{
        StreamableHttpClientTransport,
        common::client_side_sse::ExponentialBackoff,
        streamable_http_client::{
            SseError, StreamableHttpClient, StreamableHttpClientTransportConfig,
            StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use sse_stream::{Sse, SseStream};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};

const MAX_RESPONSE: usize = 1_048_576;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpHttpConfig {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_token_env: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}
impl McpHttpConfig {
    pub fn validate(&self) -> Result<(), Error> {
        let url = reqwest::Url::parse(&self.url).map_err(|_| config_error())?;
        let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if self.url.len() > 4096
            || url.host_str().is_none()
            || !(url.scheme() == "https" || (loopback && url.scheme() == "http"))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(1..=60_000).contains(&self.timeout_ms)
            || self.bearer_token_env.as_ref().is_some_and(|name| {
                name.is_empty()
                    || name.len() > 64
                    || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            })
        {
            return Err(config_error());
        }
        Ok(())
    }
    fn token(&self) -> Result<Option<String>, Error> {
        self.bearer_token_env
            .as_ref()
            .map(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|value| {
                        !value.is_empty()
                            && value.len() <= 8192
                            && value.bytes().all(|b| b.is_ascii_graphic())
                    })
                    .ok_or_else(|| {
                        Error::Unavailable(
                            "MCP HTTP bearer token environment variable is missing, blank or invalid".into(),
                        )
                    })
            })
            .transpose()
    }
    /// Local diagnostics check presence/shape only; never contact the endpoint.
    pub fn check_credentials(&self) -> Result<(), Error> {
        self.token().map(|_| ())
    }
    pub(super) async fn connect(&self) -> Result<Session, Error> {
        self.validate()?;
        let mut headers = HeaderMap::new();
        if let Some(token) = self.token()? {
            let mut value =
                HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| config_error())?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(self.timeout_ms.min(10_000)))
            .timeout(Duration::from_millis(self.timeout_ms))
            .build()
            .map_err(|_| config_error())?;
        let mut config = StreamableHttpClientTransportConfig::with_uri(self.url.clone())
            .reinit_on_expired_session(false)
            .max_concurrent_requests(1)
            .max_sse_event_size(MAX_RESPONSE)
            .control_request_timeout(Duration::from_secs(1));
        // Only the durable runtime may authorize another tool POST. SSE loss is
        // also surfaced instead of silently reconnecting behind the activity journal.
        let mut retry = ExponentialBackoff::default();
        retry.max_times = Some(0);
        config.retry_config = Arc::new(retry);
        let status = Arc::new(AtomicU16::new(0));
        let transport = StreamableHttpClientTransport::with_client(
            HttpClient {
                client,
                status: status.clone(),
            },
            config,
        );
        let client = ().serve(transport).await.map_err(|_| {
            // The SDK may close its worker while sending notifications/initialized,
            // obscuring the HTTP error. Keep only its status, never the response body.
            let status = status.load(Ordering::Relaxed);
            let message = if status == 0 {
                "MCP HTTP handshake interrupted; check endpoint and Streamable HTTP support".into()
            } else {
                format!(
                    "MCP HTTP handshake refused (HTTP {status}); check endpoint and bearer token"
                )
            };
            if status == 0 || matches!(status, 408 | 425 | 429 | 500..=599) {
                // No tools have been dispatched during initialization. Let the
                // durable activity policy schedule a retry rather than the SDK.
                std::io::Error::new(std::io::ErrorKind::Interrupted, message).into()
            } else {
                Error::Unavailable(message)
            }
        })?;
        Ok(Session {
            client,
            process: None,
        })
    }
}
fn config_error() -> Error {
    Error::Unavailable("HTTP MCP requires an HTTPS URL (HTTP on loopback), no embedded credentials/query/fragment, a bounded token environment name and a 1–60000 ms deadline".into())
}

#[derive(Debug, thiserror::Error)]
enum HttpError {
    #[error("MCP HTTP {0}")]
    Protocol(&'static str),
    #[error("MCP HTTP endpoint returned status {0}")]
    Status(u16),
}
type HttpResult<T> = Result<T, StreamableHttpError<HttpError>>;
fn failure(reason: &'static str) -> StreamableHttpError<HttpError> {
    StreamableHttpError::Client(HttpError::Protocol(reason))
}
#[derive(Clone)]
struct HttpClient {
    client: reqwest::Client,
    status: Arc<AtomicU16>,
}
impl HttpClient {
    fn request(
        &self,
        method: Method,
        uri: &str,
        session: Option<&str>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> HttpResult<reqwest::RequestBuilder> {
        let mut request = self
            .client
            .request(method, uri)
            .header("accept", "application/json, text/event-stream");
        for (name, value) in headers {
            if matches!(
                name.as_str(),
                "authorization"
                    | "cookie"
                    | "host"
                    | "proxy-authorization"
                    | "content-length"
                    | "accept"
                    | "content-type"
                    | "mcp-session-id"
                    | "last-event-id"
            ) {
                return Err(failure("reserved header refused"));
            }
            request = request.header(name, value);
        }
        if let Some(session) = session {
            request = request.header("mcp-session-id", session);
        }
        Ok(request)
    }
    fn sse(response: reqwest::Response) -> BoxStream<'static, Result<Sse, SseError>> {
        let mut received = 0usize;
        let bytes = response.bytes_stream().map(move |chunk| {
            let chunk = chunk.map_err(|_| HttpError::Protocol("response stream failed"))?;
            received = received.saturating_add(chunk.len());
            if received > MAX_RESPONSE {
                return Err(HttpError::Protocol("response exceeded 1 MiB"));
            }
            Ok(chunk)
        });
        SseStream::from_bytes_stream(bytes).boxed()
    }
}
impl StreamableHttpClient for HttpClient {
    type Error = HttpError;
    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        _auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> HttpResult<StreamableHttpPostResponse> {
        let notification = !matches!(message, ClientJsonRpcMessage::Request(_));
        self.status.store(0, Ordering::Relaxed);
        let mut response = self
            .request(Method::POST, &uri, session.as_deref(), headers)?
            .json(&message)
            .send()
            .await
            .map_err(|_| failure("request failed"))?;
        let status = response.status();
        if !status.is_success() {
            self.status.store(status.as_u16(), Ordering::Relaxed);
        }
        if status == reqwest::StatusCode::NOT_FOUND && session.is_some() {
            return Err(StreamableHttpError::SessionExpired);
        }
        // Error bodies can contain credentials or arbitrary HTML. Never include them
        // in persisted diagnostics, model context, or transport tracing.
        if !status.is_success() {
            return Err(StreamableHttpError::Client(HttpError::Status(
                status.as_u16(),
            )));
        }
        if status == reqwest::StatusCode::ACCEPTED
            || status == reqwest::StatusCode::NO_CONTENT
            || (notification && response.content_length() == Some(0))
        {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        let session = response
            .headers()
            .get("mcp-session-id")
            .map(|value| {
                value
                    .to_str()
                    .ok()
                    .filter(|value| {
                        !value.is_empty()
                            && value.len() <= 1024
                            && value.bytes().all(|byte| byte.is_ascii_graphic())
                    })
                    .map(str::to_owned)
                    .ok_or_else(|| failure("invalid session identifier"))
            })
            .transpose()?;
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        if content_type == "text/event-stream" {
            return Ok(StreamableHttpPostResponse::Sse(
                Self::sse(response),
                session,
            ));
        }
        if content_type != "application/json" {
            return Err(failure("unsupported response content type"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| failure("response read failed"))?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE {
                return Err(failure("response exceeded 1 MiB"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let message: ServerJsonRpcMessage =
            serde_json::from_slice(&bytes).map_err(|_| failure("invalid JSON-RPC response"))?;
        Ok(StreamableHttpPostResponse::Json(message, session))
    }
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last: Option<String>,
        _auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> HttpResult<BoxStream<'static, Result<Sse, SseError>>> {
        let mut request = self.request(Method::GET, &uri, session.as_deref(), headers)?;
        if let Some(last) = last {
            request = request.header("last-event-id", last);
        }
        let response = request
            .send()
            .await
            .map_err(|_| failure("stream request failed"))?;
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            return Err(StreamableHttpError::ServerDoesNotSupportSse);
        }
        if !response.status().is_success() {
            return Err(failure("stream request refused"));
        }
        if !response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(';').next().unwrap_or("").trim() == "text/event-stream")
        {
            return Err(failure("invalid stream content type"));
        }
        Ok(Self::sse(response))
    }
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        _auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> HttpResult<()> {
        let response = self
            .request(Method::DELETE, &uri, Some(&session), headers)?
            .send()
            .await
            .map_err(|_| failure("session cleanup failed"))?;
        if !response.status().is_success()
            && response.status() != reqwest::StatusCode::METHOD_NOT_ALLOWED
        {
            return Err(failure("session cleanup refused"));
        }
        Ok(())
    }
}
