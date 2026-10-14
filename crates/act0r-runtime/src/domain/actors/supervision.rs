//! Actor-owned health, updated only by accepted terminal Run notifications.
use super::*;

pub const MAX_SUPERVISION_FAILURES: usize = 16;
pub const MAX_SUPERVISION_ERROR_BYTES: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisionPolicy {
    pub max_consecutive_failures: u32,
}
impl SupervisionPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if !(1..=64).contains(&self.max_consecutive_failures) {
            return Err(DomainError::InvalidConfiguration(
                "supervision requires 1..64 consecutive failures".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorFailure {
    pub task_id: MessageId,
    pub run_id: RunId,
    pub error: Option<String>,
    pub observed_at: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisionPause {
    pub task_id: MessageId,
    pub run_id: RunId,
    pub at: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorSupervision {
    pub policy: SupervisionPolicy,
    pub consecutive_failures: u32,
    pub failures: Vec<ActorFailure>,
    pub pause: Option<SupervisionPause>,
    pub recoveries: u64,
    pub recovered_at: Option<i64>,
}
impl ActorSupervision {
    pub fn new(policy: SupervisionPolicy) -> Result<Self, DomainError> {
        policy.validate()?;
        Ok(Self {
            policy,
            consecutive_failures: 0,
            failures: vec![],
            pause: None,
            recoveries: 0,
            recovered_at: None,
        })
    }
    pub fn pauses_on(&self, message: &AgentMessage) -> bool {
        message.kind == RUN_COMPLETED
            && message.payload["status"] == "failed"
            && self.consecutive_failures.saturating_add(1) >= self.policy.max_consecutive_failures
    }
    pub(super) fn observe(&mut self, message: &AgentMessage, now: i64) -> Result<(), DomainError> {
        if message.kind != RUN_COMPLETED {
            return Ok(());
        }
        let status: RunStatus = serde_json::from_value(message.payload["status"].clone())
            .map_err(|_| DomainError::InvalidState)?;
        if status.is_unfinished() || message.sender.is_some() || self.pause.is_some() {
            return Err(DomainError::InvalidState);
        }
        match status {
            RunStatus::Succeeded => self.consecutive_failures = 0,
            RunStatus::Failed => {
                let task_id = serde_json::from_value(message.payload["request_id"].clone())
                    .map_err(|_| DomainError::InvalidState)?;
                let run_id = serde_json::from_value(message.payload["run_id"].clone())
                    .map_err(|_| DomainError::InvalidState)?;
                let error = message.payload["error"]["message"].as_str().map(|error| {
                    let mut end = error.len().min(MAX_SUPERVISION_ERROR_BYTES);
                    while !error.is_char_boundary(end) {
                        end -= 1;
                    }
                    error[..end].to_string()
                });
                self.consecutive_failures = self
                    .consecutive_failures
                    .checked_add(1)
                    .ok_or(DomainError::InvalidState)?;
                self.failures.push(ActorFailure {
                    task_id,
                    run_id,
                    error,
                    observed_at: now,
                });
                if self.failures.len() > MAX_SUPERVISION_FAILURES {
                    self.failures.remove(0);
                }
                if self.consecutive_failures >= self.policy.max_consecutive_failures {
                    self.pause = Some(SupervisionPause {
                        task_id,
                        run_id,
                        at: now,
                    });
                }
            }
            // These outcomes neither punish an actor nor erase prior failures.
            RunStatus::Cancelled | RunStatus::BudgetExhausted => {}
            _ => return Err(DomainError::InvalidState),
        }
        Ok(())
    }
}
impl Agent {
    /// Reset a latched failure pause; the store also saves a durable queue wakeup.
    pub(crate) fn recover_supervision(&mut self, now: i64) -> Result<bool, DomainError> {
        let health = self.record.supervision.as_mut().ok_or_else(|| {
            DomainError::InvalidConfiguration("actor supervision is disabled".into())
        })?;
        if health.pause.is_none() {
            return Ok(false);
        }
        if self.record.status != AgentStatus::Paused {
            return Err(DomainError::InvalidState);
        }
        health.recoveries = health
            .recoveries
            .checked_add(1)
            .ok_or(DomainError::InvalidState)?;
        health.consecutive_failures = 0;
        health.pause = None;
        health.recovered_at = Some(now);
        self.record.status = AgentStatus::Ready;
        self.record.error = None;
        self.bump(now)?;
        Ok(true)
    }
}
