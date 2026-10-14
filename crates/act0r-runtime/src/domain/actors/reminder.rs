//! One-shot tasks owned by an actor, published into its mailbox when due.
use super::*;

pub const MAX_PENDING_REMINDERS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReminderStatus {
    Scheduled,
    Published,
    Cancelled,
}
impl ReminderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Published => "published",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorReminder {
    /// Also the message ID and, after publication, the task handle.
    pub id: MessageId,
    pub message: AgentMessage,
    pub deadline: TimerDeadline,
    pub due_at: i64,
    pub status: ReminderStatus,
    pub created_at: i64,
    pub published_at: Option<i64>,
    pub cancelled_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<crate::domain::follow_up::FollowUpOrigin>,
}
impl ActorReminder {
    pub fn new(
        message: AgentMessage,
        deadline: TimerDeadline,
        now: i64,
    ) -> Result<Self, DomainError> {
        Self::validate_message(&message)?;
        let due_at = deadline.resolve(now)?;
        Ok(Self {
            id: message.id,
            message,
            deadline,
            due_at,
            status: ReminderStatus::Scheduled,
            created_at: now,
            published_at: None,
            cancelled_at: None,
            origin: None,
        })
    }
    pub fn validate_message(message: &AgentMessage) -> Result<(), DomainError> {
        message.validate_public()?;
        if message.kind != "task" {
            return Err(DomainError::InvalidConfiguration(
                "actor reminders schedule task messages only".into(),
            ));
        }
        Ok(())
    }
    pub fn matches(&self, message: &AgentMessage, deadline: &TimerDeadline) -> bool {
        &self.message == message && &self.deadline == deadline
    }
    pub fn publish(&mut self, now: i64) -> Result<(), DomainError> {
        if self.status != ReminderStatus::Scheduled || now < self.due_at {
            return Err(DomainError::InvalidState);
        }
        self.status = ReminderStatus::Published;
        self.published_at = Some(now);
        Ok(())
    }
    pub fn cancel(&mut self, now: i64) -> bool {
        if self.status != ReminderStatus::Scheduled {
            return false;
        }
        self.status = ReminderStatus::Cancelled;
        self.cancelled_at = Some(now);
        true
    }
}
