use super::{ActivityId, AttemptId, ModelReservation, RunId, TokenUsage};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DefinitionId {
    pub name: String,
    pub version: u32,
}
impl DefinitionId {
    pub fn new(name: impl Into<String>, version: u32) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryPolicy {
    SafeToRepeat,
    RequiresReconciliation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub delay_ms: u32,
    pub max_delay_ms: u32,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay_ms: 100,
            max_delay_ms: 5_000,
        }
    }
}
impl RetryPolicy {
    pub fn delay(&self, attempts: u32) -> i64 {
        let multiplier = 1_u64 << attempts.saturating_sub(1).min(32);
        (u64::from(self.delay_ms)
            .saturating_mul(multiplier)
            .min(u64::from(self.max_delay_ms))) as i64
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivitySpec {
    pub handler: DefinitionId,
    pub input: Value,
    pub retry: RetryPolicy,
    pub recovery: RecoveryPolicy,
    #[serde(default)]
    pub usage: Option<ModelReservation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Indeterminate,
}
impl ActivityStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Indeterminate => "indeterminate",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Activity {
    pub id: ActivityId,
    pub run_id: RunId,
    pub scheduling_revision: u64,
    pub command_ordinal: u32,
    pub spec: ActivitySpec,
    pub status: ActivityStatus,
    pub ready_at: i64,
    pub attempts: u32,
    pub active_attempt: Option<AttemptId>,
    pub result: Option<Value>,
    pub error: Option<Failure>,
    /// Explicit model recovery reuses the original input, even before its new attempt.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub preserve_input: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum RetryAfter {
    DelayMs(u64),
    At(i64),
}
impl RetryAfter {
    pub fn deadline(&self, now: i64) -> i64 {
        match self {
            Self::DelayMs(delay) => now.saturating_add(i64::try_from(*delay).unwrap_or(i64::MAX)),
            Self::At(deadline) => (*deadline).max(now),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestCategory {
    Success,
    Configuration,
    Timeout,
    Connection,
    Transport,
    HttpStatus,
    ResponseRead,
    ResponseTooLarge,
    InvalidJson,
    InvalidAction,
    Incomplete,
    Refusal,
}
impl RequestCategory {
    pub fn failure_code(self) -> &'static str {
        match self {
            Self::Success => "model_success",
            Self::Configuration => "model_configuration",
            Self::Timeout => "model_timeout",
            Self::Connection => "model_connection",
            Self::Transport => "model_transport",
            Self::HttpStatus => "model_http_status",
            Self::ResponseRead => "model_response_read",
            Self::ResponseTooLarge => "model_response_size",
            Self::InvalidJson => "model_invalid_json",
            Self::InvalidAction => "model_invalid_action",
            Self::Incomplete => "model_incomplete",
            Self::Refusal => "model_refusal",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestDiagnostics {
    pub category: RequestCategory,
    pub client_request_id: ActivityId,
    pub elapsed_ms: u64,
    pub http_status: Option<u16>,
    pub request_id: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Failure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<RetryAfter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<RequestDiagnostics>,
}
impl Failure {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
            retry_after: None,
            request: None,
        }
    }
    pub fn with_request(
        mut self,
        request: Option<RequestDiagnostics>,
        retry_after: Option<RetryAfter>,
    ) -> Self {
        self.request = request;
        self.retry_after = retry_after;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Outcome {
    Success(Value),
    Failure(Failure),
    Uncertain(String),
    SuccessWithUsage { result: Value, usage: TokenUsage },
    FailureWithUsage { failure: Failure, usage: TokenUsage },
}
impl Outcome {
    pub fn result(&self) -> Option<&Value> {
        match self {
            Self::Success(value) | Self::SuccessWithUsage { result: value, .. } => Some(value),
            _ => None,
        }
    }
    pub fn usage(&self) -> Option<&TokenUsage> {
        match self {
            Self::SuccessWithUsage { usage, .. } | Self::FailureWithUsage { usage, .. } => {
                Some(usage)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Running,
    Succeeded,
    Failed,
    Interrupted,
}
impl AttemptStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attempt {
    pub id: AttemptId,
    pub activity_id: ActivityId,
    pub ordinal: u32,
    pub status: AttemptStatus,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub outcome: Option<Outcome>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityExecution {
    pub run_id: RunId,
    /// Stable idempotency key for the logical operation, including retries.
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub ordinal: u32,
    pub handler: DefinitionId,
    pub input: Value,
}
