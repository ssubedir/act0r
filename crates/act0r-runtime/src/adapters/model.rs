use crate::{
    application::{Error, ports::*},
    domain::{
        ActivityId, RequestCategory, RequestDiagnostics, RetryAfter, TokenUsage, agent::*,
        search::*,
    },
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant, UNIX_EPOCH},
};

/// One request per activity; this adapter never drives an internal agent/tool loop.
pub struct ModelRouter {
    clients: Mutex<HashMap<u32, reqwest::Client>>,
    key: Option<String>,
}
struct RawReply {
    text: Result<String, String>,
    request_id: Option<String>,
    usage: Option<TokenUsage>,
    retryable: bool,
    diagnostics: RequestDiagnostics,
    retry_after: Option<RetryAfter>,
}
impl RawReply {
    fn fail(
        mut diagnostics: RequestDiagnostics,
        category: RequestCategory,
        message: impl Into<String>,
        retryable: bool,
        retry_after: Option<RetryAfter>,
    ) -> Self {
        diagnostics.category = category;
        Self {
            text: Err(message.into()),
            request_id: diagnostics.request_id.clone(),
            usage: None,
            retryable,
            diagnostics,
            retry_after,
        }
    }
}
impl ModelRouter {
    /// Exact JSON body used by the transport; excludes authentication and headers.
    /// Inspection of saved requests must use this same serializer, not a second contract.
    pub fn request_body(request: &ModelRequest) -> Value {
        if request.target.action_format == ModelActionFormat::FunctionCalling {
            let tools: Vec<_> = request.function_actions().unwrap_or_default().into_iter().map(|function| {
                json!({"type":"function","function":{"name":function.name,"description":format!("Select the saved {} action.",function.name),"parameters":function.parameters}})
            }).collect();
            return json!({"model":request.target.model,"messages":[{"role":"system","content":request.function_instructions()},{"role":"user","content":request.prompt}],"max_tokens":request.max_output_tokens,"stream":false,"n":1,"tools":tools,"tool_choice":"required","parallel_tool_calls":false});
        }
        match request.target.protocol {
            ModelProtocol::Responses => {
                json!({"model":request.target.model,"instructions":request.instructions,"input":request.prompt,"max_output_tokens":request.max_output_tokens,"store":false,"text":{"format":{"type":"json_schema","name":"act0r_action","strict":true,"schema":request.schema}}})
            }
            ModelProtocol::ChatCompletions => {
                json!({"model":request.target.model,"messages":[{"role":"system","content":request.chat_instructions()},{"role":"user","content":request.prompt}],"max_tokens":request.max_output_tokens,"stream":false,"n":1,"response_format":{"type":"json_schema","json_schema":{"name":"act0r_action","strict":true,"schema":request.schema}}})
            }
        }
    }
    pub fn from_environment() -> Result<Self, Error> {
        Self::new(environment_key())
    }
    /// Availability only; never expose credentials through the operator transport.
    pub fn environment_key_available() -> bool {
        environment_key().is_some()
    }
    pub fn new(key: Option<String>) -> Result<Self, Error> {
        let client = Self::client(45_000)?;
        Ok(Self {
            clients: Mutex::new(HashMap::from([(45_000, client)])),
            key,
        })
    }
    fn client(connect_timeout_ms: u32) -> Result<reqwest::Client, Error> {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(u64::from(connect_timeout_ms)))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Unavailable("cannot create model HTTP client".into()))
    }
    fn policy_client(&self, policy: &ModelPolicy) -> Result<reqwest::Client, Error> {
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| Error::Unavailable("model client cache unavailable".into()))?;
        if let std::collections::hash_map::Entry::Vacant(entry) =
            clients.entry(policy.connect_timeout_ms)
        {
            entry.insert(Self::client(policy.connect_timeout_ms)?);
        }
        Ok(clients[&policy.connect_timeout_ms].clone())
    }
    async fn openai(&self, request: ModelRequest, activity: ActivityId) -> RawReply {
        let started = Instant::now();
        let mut reply = self.send(request, activity).await;
        reply.diagnostics.elapsed_ms =
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        reply
    }
    async fn send(&self, request: ModelRequest, activity: ActivityId) -> RawReply {
        let mut diagnostics = RequestDiagnostics {
            category: RequestCategory::Success,
            client_request_id: activity,
            elapsed_ms: 0,
            http_status: None,
            request_id: None,
        };
        let validation = request.target.validate().and_then(|_| {
            if request.target.action_format == ModelActionFormat::FunctionCalling {
                request.function_actions().map(|_| ())
            } else {
                Ok(())
            }
        });
        if let Err(error) = validation {
            return RawReply::fail(
                diagnostics,
                RequestCategory::Configuration,
                error.to_string(),
                false,
                None,
            );
        }
        let Ok(url) = reqwest::Url::parse(&request.target.endpoint) else {
            return RawReply::fail(
                diagnostics,
                RequestCategory::Configuration,
                "invalid model endpoint",
                false,
                None,
            );
        };
        let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if !(url.scheme() == "https" || (local && url.scheme() == "http"))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return RawReply::fail(
                diagnostics,
                RequestCategory::Configuration,
                "endpoint must use HTTPS (HTTP allowed on loopback) without embedded credentials or query parameters",
                false,
                None,
            );
        }
        let key = self.key.as_deref();
        if !local && key.is_none() {
            return RawReply::fail(
                diagnostics,
                RequestCategory::Configuration,
                "OPENAI_API_KEY is missing",
                false,
                None,
            );
        }
        let protocol = request.target.protocol;
        let body = Self::request_body(&request);
        let client = match self.policy_client(&request.target.policy) {
            Ok(client) => client,
            Err(_) => {
                return RawReply::fail(
                    diagnostics,
                    RequestCategory::Configuration,
                    "model HTTP client unavailable",
                    false,
                    None,
                );
            }
        };
        let mut builder = client
            .post(url)
            .timeout(Duration::from_millis(u64::from(
                request.target.policy.request_timeout_ms,
            )))
            .header("X-Client-Request-Id", activity.to_string())
            .json(&body);
        if let Some(key) = key {
            builder = builder.bearer_auth(key);
        }
        let mut response = match builder.send().await {
            Ok(response) => response,
            Err(error) => {
                let (category, message, retryable) = if error.is_timeout() {
                    (
                        RequestCategory::Timeout,
                        "model request timed out before a response was recorded",
                        true,
                    )
                } else if error.is_connect() {
                    (
                        RequestCategory::Connection,
                        "model connection failed (DNS, TCP, proxy, or TLS)",
                        true,
                    )
                } else if error.is_builder() {
                    (
                        RequestCategory::Configuration,
                        "model request could not be built",
                        false,
                    )
                } else {
                    (
                        RequestCategory::Transport,
                        "model transport failed before a response was recorded",
                        true,
                    )
                };
                return RawReply::fail(diagnostics, category, message, retryable, None);
            }
        };
        let status = response.status();
        diagnostics.http_status = Some(status.as_u16());
        diagnostics.request_id = ["x-request-id", "request-id", "x-correlation-id"]
            .iter()
            .find_map(|name| {
                response
                    .headers()
                    .get(*name)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| safe_id(v, key))
            });
        let retryable_http = status.as_u16() == 429 || status.is_server_error();
        let retry_after = if retryable_http {
            response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_retry_after)
        } else {
            None
        };
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if bytes.len() + chunk.len() <= 512 * 1024 => {
                    bytes.extend_from_slice(&chunk)
                }
                Ok(None) => break,
                Ok(Some(_)) => {
                    return RawReply::fail(
                        diagnostics,
                        RequestCategory::ResponseTooLarge,
                        "model response exceeded the size limit",
                        retryable_http,
                        retry_after,
                    );
                }
                Err(error) => {
                    let (category, message) = if error.is_timeout() {
                        (
                            RequestCategory::Timeout,
                            "model request timed out while reading its response",
                        )
                    } else {
                        (
                            RequestCategory::ResponseRead,
                            "model response body could not be read",
                        )
                    };
                    return RawReply::fail(
                        diagnostics,
                        category,
                        message,
                        status.is_success() || retryable_http,
                        retry_after,
                    );
                }
            }
        }
        let value = serde_json::from_slice::<Value>(&bytes);
        if !status.is_success() {
            let mut reply = RawReply::fail(
                diagnostics,
                RequestCategory::HttpStatus,
                format!("model endpoint returned HTTP {}", status.as_u16()),
                retryable_http,
                retry_after,
            );
            if let Ok(value) = value {
                reply.usage = token_usage(&value, protocol);
            }
            return reply;
        }
        let Ok(value) = value else {
            return RawReply::fail(
                diagnostics,
                RequestCategory::InvalidJson,
                "invalid model response JSON",
                false,
                None,
            );
        };
        if diagnostics.request_id.is_none() {
            diagnostics.request_id = value["id"].as_str().and_then(|v| safe_id(v, key));
        }
        let usage = token_usage(&value, protocol);
        let text = match protocol {
            ModelProtocol::Responses => parse_responses(&value),
            ModelProtocol::ChatCompletions
                if request.target.action_format == ModelActionFormat::FunctionCalling =>
            {
                parse_chat_function(&value, &request)
            }
            ModelProtocol::ChatCompletions => parse_chat(&value),
        };
        if let Err(message) = &text {
            diagnostics.category = if message.contains("refused") {
                RequestCategory::Refusal
            } else if message.contains("complete") || message.contains("token limit") {
                RequestCategory::Incomplete
            } else {
                RequestCategory::InvalidAction
            };
        }
        let retryable = text.is_err()
            && request.target.action_format == ModelActionFormat::FunctionCalling
            && diagnostics.category == RequestCategory::InvalidAction;
        RawReply {
            text,
            request_id: diagnostics.request_id.clone(),
            usage,
            retryable,
            diagnostics,
            retry_after: None,
        }
    }
}
fn environment_key() -> Option<String> {
    std::env::var("OPENAI_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty())
}
fn token_usage(value: &Value, protocol: ModelProtocol) -> Option<TokenUsage> {
    let (input_field, output_field) = match protocol {
        ModelProtocol::Responses => ("input_tokens", "output_tokens"),
        ModelProtocol::ChatCompletions => ("prompt_tokens", "completion_tokens"),
    };
    value["usage"][input_field]
        .as_u64()
        .zip(value["usage"][output_field].as_u64())
        .map(|(input_tokens, output_tokens)| TokenUsage {
            input_tokens,
            output_tokens,
        })
}
fn safe_id(value: &str, key: Option<&str>) -> Option<String> {
    (value.len() <= 128
        && !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
        && !key.is_some_and(|key| !key.is_empty() && value.contains(key)))
    .then(|| value.to_owned())
}
fn parse_retry_after(value: &str) -> Option<RetryAfter> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        let seconds = value.bytes().fold(0u64, |n, b| {
            n.saturating_mul(10).saturating_add(u64::from(b - b'0'))
        });
        return Some(RetryAfter::DelayMs(seconds.saturating_mul(1000)));
    }
    let time = httpdate::parse_http_date(value)
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()?;
    Some(RetryAfter::At(i64::try_from(time.as_millis()).ok()?))
}
fn parse_responses(value: &Value) -> Result<String, String> {
    if value["status"] != "completed" {
        return Err("model response did not complete".into());
    }
    let mut texts = vec![];
    for item in value["output"]
        .as_array()
        .ok_or("model response has no output")?
    {
        if let Some(content) = item["content"].as_array() {
            for part in content {
                if part["type"] == "refusal" {
                    return Err("model refused the request".into());
                }
                if part["type"] == "output_text" {
                    texts.push(part["text"].as_str().ok_or("missing output text")?);
                }
            }
        }
    }
    if texts.len() != 1 {
        return Err("expected one structured model action".into());
    }
    Ok(texts[0].into())
}
fn parse_chat(value: &Value) -> Result<String, String> {
    let choices = value["choices"]
        .as_array()
        .ok_or("model response has no choices")?;
    if choices.len() != 1 {
        return Err("expected one model choice".into());
    }
    let choice = &choices[0];
    if choice["finish_reason"] != "stop" {
        if choice["finish_reason"] == "length" {
            return Err("model output token limit reached before a complete action; increase max_output_tokens or reduce model reasoning".into());
        }
        return Err("model response did not complete with a structured action".into());
    }
    let message = &choice["message"];
    if message["refusal"].as_str().is_some_and(|s| !s.is_empty()) {
        return Err("model refused the request".into());
    }
    if message["tool_calls"]
        .as_array()
        .is_some_and(|a| !a.is_empty())
        || message["function_call"].is_object()
    {
        return Err("expected a structured action rather than provider tool dispatch".into());
    }
    let text = message["content"].as_str().ok_or_else(|| {
        if message["content"].is_null() {
            "model response did not complete with final action text; provider returned empty or reasoning-only content"
        } else {
            "structured output text must be a string"
        }
    })?;
    if text.trim().is_empty() {
        return Err("model response did not complete with final action text; provider returned empty or reasoning-only content".into());
    }
    Ok(text.into())
}
fn parse_chat_function(value: &Value, request: &ModelRequest) -> Result<String, String> {
    let choices = value["choices"]
        .as_array()
        .ok_or("model response has no choices")?;
    if choices.len() != 1 {
        return Err("expected one model choice".into());
    }
    let choice = &choices[0];
    if choice["finish_reason"] == "length" {
        return Err("model output token limit reached before a complete function call; increase max_output_tokens or reduce model reasoning".into());
    }
    if !matches!(
        choice["finish_reason"].as_str(),
        Some("tool_calls" | "stop")
    ) {
        return Err("model response did not complete with a function call".into());
    }
    let message = &choice["message"];
    if message["refusal"].as_str().is_some_and(|s| !s.is_empty()) {
        return Err("model refused the request".into());
    }
    if message["function_call"].is_object() {
        return Err("expected one modern function tool call".into());
    }
    let calls = message["tool_calls"]
        .as_array()
        .ok_or("expected one supplied function call")?;
    if calls.len() != 1 {
        return Err("expected exactly one function call; parallel dispatch is unsupported".into());
    }
    let call = &calls[0];
    if call["type"] != "function"
        || call["id"]
            .as_str()
            .is_none_or(|s| s.is_empty() || s.len() > 256)
    {
        return Err("invalid function call identity or type".into());
    }
    let name = call["function"]["name"]
        .as_str()
        .ok_or("missing function name")?;
    let arguments = call["function"]["arguments"]
        .as_str()
        .ok_or("function arguments must be a JSON string")?;
    // Do not collapse duplicate parameter keys before validating the saved contract.
    struct UniqueArguments(serde_json::Map<String, Value>);
    impl<'de> serde::Deserialize<'de> for UniqueArguments {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct ObjectVisitor;
            impl<'de> serde::de::Visitor<'de> for ObjectVisitor {
                type Value = UniqueArguments;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("one object with unique parameter names")
                }
                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> Result<Self::Value, A::Error> {
                    let mut fields = serde_json::Map::new();
                    while let Some((key, value)) = map.next_entry::<String, Value>()? {
                        if fields.insert(key, value).is_some() {
                            return Err(serde::de::Error::custom("duplicate function parameter"));
                        }
                    }
                    Ok(UniqueArguments(fields))
                }
            }
            deserializer.deserialize_map(ObjectVisitor)
        }
    }
    let UniqueArguments(fields) = serde_json::from_str(arguments).map_err(
        |_| "invalid function arguments: expected one JSON object with unique parameter names",
    )?;
    function_action_text(request, name, json!(fields))
}
fn parse_action(
    text: &str,
    value: &Value,
    native_tool_names: &[String],
    flattened: bool,
    schema: &Value,
) -> Result<ModelDecision, String> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ActionEnvelope {
        action: AgentAction,
    }
    #[derive(serde::Deserialize)]
    struct FlatToolAction {
        kind: String,
        #[serde(flatten)]
        arguments: serde_json::Map<String, Value>,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct FlatEnvelope {
        action: FlatToolAction,
    }
    let action = if flattened {
        let parsed: FlatEnvelope = parse_structured(text, "invalid structured model action")?;
        let action = parsed.action;
        let saved = &schema["properties"]["action"];
        let variants = saved["anyOf"]
            .as_array()
            .map_or_else(|| vec![saved], |v| v.iter().collect());
        // Match a complete saved alternative: scoped delegate_task has two variants.
        // Single-action coordination has no anyOf. Absent actions remain unavailable.
        if !variants.iter().any(|variant| {
            variant["properties"]["kind"]["enum"][0] == action.kind
                && variant["properties"].as_object().is_some_and(|properties| {
                    action
                        .arguments
                        .keys()
                        .all(|key| properties.contains_key(key))
                })
                && variant["required"].as_array().is_some_and(|required| {
                    required.iter().all(|key| {
                        key.as_str()
                            .is_some_and(|key| key == "kind" || action.arguments.contains_key(key))
                    })
                })
        }) {
            return Err("invalid structured model action: action kind or fields differ from the saved schema".into());
        }
        if native_tool_names.contains(&action.kind) {
            AgentAction::CallTool {
                tool: action.kind,
                arguments: json!(action.arguments),
            }
        } else {
            let parsed: ActionEnvelope = parse_structured(text, "invalid structured model action")?;
            match parsed.action {
                AgentAction::CallTool { .. } => {
                    return Err(
                        "invalid structured model action: expected a flat native tool".into(),
                    );
                }
                action => action,
            }
        }
    } else {
        if !native_tool_names.is_empty() {
            return Err(
                "invalid structured model action: native tools require the flat contract".into(),
            );
        }
        let parsed: ActionEnvelope = parse_structured(text, "invalid structured model action")?;
        parsed.action
    };
    Ok(ModelDecision {
        action,
        request_id: value["id"].as_str().map(String::from),
        diagnostics: None,
    })
}
/// Remove only a complete outer JSON fence; never extract actions from prose or reasoning.
fn parse_structured<T: serde::de::DeserializeOwned>(text: &str, label: &str) -> Result<T, String> {
    let text = text.trim();
    let json = text
        .strip_prefix("```")
        .and_then(|fenced| fenced.split_once('\n'))
        .filter(|(language, _)| {
            language.trim().is_empty() || language.trim().eq_ignore_ascii_case("json")
        })
        .and_then(|(_, content)| content.strip_suffix("```"))
        .map_or(text, str::trim);
    serde_json::from_str(json).map_err(|error| match error.classify() {
        serde_json::error::Category::Data => format!(
            "{label}: JSON does not match the required envelope and action fields; check missing fields, extra fields, and action kind"
        ),
        _ => format!(
            "{label}: expected one complete JSON object (invalid JSON at line {}, column {})",
            error.line(), error.column()
        ),
    })
}
impl ModelClient for ModelRouter {
    fn generate(&self, request: ModelRequest, activity: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            if request.target.provider == "scripted" {
                tokio::time::sleep(Duration::from_millis(u64::from(request.scripted_delay_ms)))
                    .await;
                return Ok(ModelReply {
                    decision: request
                        .script
                        .get(request.turn as usize)
                        .cloned()
                        .map(|action| ModelDecision {
                            action,
                            request_id: None,
                            diagnostics: None,
                        })
                        .ok_or_else(|| "script exhausted".into()),
                    usage: Some(TokenUsage {
                        input_tokens: 10,
                        output_tokens: 10,
                    }),
                    retryable: false,
                    diagnostics: None,
                    retry_after: None,
                });
            }
            if request.target.provider != "openai" {
                return Err(Error::Unavailable("unsupported model provider".into()));
            }
            let native_tool_names = request.native_tool_names.clone();
            let flattened = request.native_tool_flattened;
            let schema = request.schema.clone();
            let mut reply = self.openai(request, activity).await;
            let decision = reply.text.and_then(|text| {
                parse_action(
                    &text,
                    &json!({"id":reply.request_id}),
                    &native_tool_names,
                    flattened,
                    &schema,
                )
            });
            if decision.is_err() && reply.diagnostics.category == RequestCategory::Success {
                reply.diagnostics.category = RequestCategory::InvalidAction;
                // Reject the response without dispatching an action. The durable
                // activity policy owns any new request, delay and usage admission.
                reply.retryable = true;
            }
            let diagnostics = reply.diagnostics;
            Ok(ModelReply {
                decision: decision.map(|mut decision| {
                    decision.diagnostics = Some(diagnostics.clone());
                    decision
                }),
                usage: reply.usage,
                retryable: reply.retryable,
                diagnostics: Some(diagnostics),
                retry_after: reply.retry_after,
            })
        })
    }
}
impl ProposalClient for ModelRouter {
    fn propose(
        &self,
        request: ProposalRequest,
        activity: ActivityId,
    ) -> PortFuture<'_, ProposalReply> {
        Box::pin(async move {
            if request.model.target.provider == "scripted" {
                tokio::time::sleep(Duration::from_millis(u64::from(
                    request.model.scripted_delay_ms,
                )))
                .await;
                return Ok(ProposalReply {
                    actions: Ok(request.scripted_proposals),
                    usage: Some(TokenUsage {
                        input_tokens: 10,
                        output_tokens: 10,
                    }),
                    retryable: false,
                    diagnostics: None,
                    retry_after: None,
                });
            }
            if request.model.target.provider != "openai" {
                return Err(Error::Unavailable("unsupported model provider".into()));
            }
            let mut reply = self.openai(request.model, activity).await;
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Envelope {
                actions: Vec<AgentAction>,
            }
            let actions = reply.text.and_then(|text| {
                parse_structured::<Envelope>(&text, "invalid structured action proposals")
                    .map(|e| e.actions)
            });
            if actions.is_err() && reply.diagnostics.category == RequestCategory::Success {
                reply.diagnostics.category = RequestCategory::InvalidAction;
                reply.retryable = true;
            }
            Ok(ProposalReply {
                actions,
                usage: reply.usage,
                retryable: reply.retryable,
                diagnostics: Some(reply.diagnostics),
                retry_after: reply.retry_after,
            })
        })
    }
}
