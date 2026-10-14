use super::{DomainError, RunId, TimerId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerDeadline {
    /// Resolve once from the scheduling transition's timestamp, then persist.
    After { milliseconds: u64 },
    /// Absolute UTC Unix timestamp in milliseconds.
    At { unix_ms: i64 },
}
impl TimerDeadline {
    pub fn resolve(&self, now: i64) -> Result<i64, DomainError> {
        match self {
            Self::At { unix_ms } => Ok(*unix_ms),
            Self::After { milliseconds } => i64::try_from(*milliseconds)
                .ok()
                .and_then(|delay| now.checked_add(delay))
                .ok_or_else(|| {
                    DomainError::InvalidConfiguration(
                        "timer deadline exceeds supported timestamp range".into(),
                    )
                }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimerSpec {
    pub deadline: TimerDeadline,
    pub payload: Value,
}
impl TimerSpec {
    pub fn after(milliseconds: u64, payload: Value) -> Self {
        Self {
            deadline: TimerDeadline::After { milliseconds },
            payload,
        }
    }
    pub fn at(unix_ms: i64, payload: Value) -> Self {
        Self {
            deadline: TimerDeadline::At { unix_ms },
            payload,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerStatus {
    Scheduled,
    Fired,
}
impl TimerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Fired => "fired",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DurableTimer {
    pub id: TimerId,
    pub run_id: RunId,
    pub scheduling_revision: u64,
    pub command_ordinal: u32,
    pub due_at: i64,
    pub status: TimerStatus,
    pub fired_at: Option<i64>,
    pub payload: Value,
}
