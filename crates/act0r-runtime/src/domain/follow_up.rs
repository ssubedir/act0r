//! A bounded chain of self-addressed tasks; each Run owns at most one future intent.
use super::*;
use serde::{Deserialize, Serialize};

fn min_delay() -> u64 {
    1
}
fn max_delay() -> u64 {
    86_400_000
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowUpPolicy {
    pub max_follow_ups: u32,
    #[serde(default = "min_delay")]
    pub min_delay_ms: u64,
    #[serde(default = "max_delay")]
    pub max_delay_ms: u64,
}
impl FollowUpPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if !(1..=64).contains(&self.max_follow_ups)
            || self.min_delay_ms == 0
            || self.min_delay_ms > self.max_delay_ms
            || i64::try_from(self.max_delay_ms).is_err()
        {
            return Err(DomainError::InvalidConfiguration(
                "follow-ups require 1..64 tasks and positive supported delay bounds".into(),
            ));
        }
        Ok(())
    }
    pub fn validate_request(&self, goal: &str, milliseconds: u64) -> Result<(), DomainError> {
        self.validate()?;
        if goal.trim().is_empty()
            || goal.len() > 60_000
            || milliseconds < self.min_delay_ms
            || milliseconds > self.max_delay_ms
        {
            return Err(DomainError::InvalidConfiguration(
                "follow-up requires a bounded nonempty goal and a delay within the pinned policy"
                    .into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowUpContext {
    pub actor_id: AgentId,
    pub task_id: MessageId,
    pub root_task_id: MessageId,
    /// Root task has depth zero; each successful follow-up consumes one position.
    pub depth: u32,
    pub policy: FollowUpPolicy,
}
impl FollowUpContext {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.policy.validate()?;
        if self.depth > self.policy.max_follow_ups
            || (self.depth == 0) != (self.task_id == self.root_task_id)
        {
            return Err(DomainError::InvalidConfiguration(
                "invalid follow-up chain context".into(),
            ));
        }
        Ok(())
    }
    pub fn remaining(&self) -> u32 {
        self.policy.max_follow_ups.saturating_sub(self.depth)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowUpOrigin {
    pub run_id: RunId,
    pub task_id: MessageId,
    pub root_task_id: MessageId,
    pub depth: u32,
    pub policy: FollowUpPolicy,
}
impl FollowUpOrigin {
    pub fn context(
        &self,
        actor_id: AgentId,
        task_id: MessageId,
    ) -> Result<FollowUpContext, DomainError> {
        if self.depth == 0 || self.task_id == task_id {
            return Err(DomainError::InvalidState);
        }
        let context = FollowUpContext {
            actor_id,
            task_id,
            root_task_id: self.root_task_id,
            depth: self.depth,
            policy: self.policy.clone(),
        };
        context.validate()?;
        Ok(context)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FollowUpStatus {
    Pending,
    Registered,
    Rejected,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowUpIntent {
    pub reminder_id: MessageId,
    pub goal: String,
    pub after_ms: u64,
    pub due_at: i64,
    pub origin: FollowUpOrigin,
    pub status: FollowUpStatus,
    pub error: Option<String>,
    pub created_at: i64,
    pub settled_at: Option<i64>,
}
impl FollowUpIntent {
    pub fn new(
        run_id: RunId,
        context: &FollowUpContext,
        goal: String,
        milliseconds: u64,
        now: i64,
    ) -> Result<Self, DomainError> {
        context.validate()?;
        context.policy.validate_request(&goal, milliseconds)?;
        let exhausted = context.remaining() == 0;
        Ok(Self {
            reminder_id: MessageId::new(),
            goal,
            after_ms: milliseconds,
            due_at: TimerDeadline::After { milliseconds }.resolve(now)?,
            origin: FollowUpOrigin {
                run_id,
                task_id: context.task_id,
                root_task_id: context.root_task_id,
                depth: context.depth + 1,
                policy: context.policy.clone(),
            },
            status: if exhausted {
                FollowUpStatus::Rejected
            } else {
                FollowUpStatus::Pending
            },
            error: exhausted.then(|| "chain_limit".into()),
            created_at: now,
            settled_at: exhausted.then_some(now),
        })
    }
}
impl Run {
    /// Registration receipt is committed with the reminder and completion notification.
    pub fn settle_follow_up(&mut self, error: Option<String>, now: i64) -> Result<(), DomainError> {
        if self.record.status != RunStatus::Succeeded {
            return Err(DomainError::InvalidState);
        }
        let intent = self
            .record
            .follow_up
            .as_mut()
            .ok_or(DomainError::InvalidState)?;
        if intent.status != FollowUpStatus::Pending {
            return Err(DomainError::InvalidState);
        }
        intent.status = if error.is_some() {
            FollowUpStatus::Rejected
        } else {
            FollowUpStatus::Registered
        };
        intent.error = error;
        intent.settled_at = Some(now);
        let saved = intent.clone();
        self.record.revision = self
            .record
            .revision
            .checked_add(1)
            .ok_or(DomainError::InvalidState)?;
        // Terminal updated_at is the completion time used by delegation deadlines.
        // Receipt time belongs to the intent and its history event.
        let completed_at = self.record.updated_at;
        self.event(
            "follow_up_settled",
            serde_json::json!({"follow_up":saved}),
            None,
            None,
            now,
        );
        self.record.updated_at = completed_at;
        Ok(())
    }
}
