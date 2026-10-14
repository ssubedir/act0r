//! Cursor and bounded query contracts for a committed goal history stream.
use super::{DomainError, EventStreamId, MessageId, TeamId};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

pub const MAX_EVENT_PAGE_BYTES: u32 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EventCursor {
    stream: EventStreamId,
    team: TeamId,
    root: MessageId,
    position: u64,
}
impl EventCursor {
    pub fn new(
        stream: EventStreamId,
        team: TeamId,
        root: MessageId,
        position: u64,
    ) -> Result<Self, DomainError> {
        if position > i64::MAX as u64 {
            return Err(invalid_cursor());
        }
        Ok(Self {
            stream,
            team,
            root,
            position,
        })
    }
    pub fn position(self) -> u64 {
        self.position
    }
    pub fn validate_scope(
        self,
        stream: EventStreamId,
        team: TeamId,
        root: MessageId,
    ) -> Result<(), DomainError> {
        if (self.stream, self.team, self.root) != (stream, team, root) {
            return Err(DomainError::InvalidConfiguration(
                "event cursor belongs to another database stream, team or goal".into(),
            ));
        }
        Ok(())
    }
}
fn invalid_cursor() -> DomainError {
    DomainError::InvalidConfiguration(
        "invalid event cursor; expected v1:STREAM:TEAM:GOAL:POSITION".into(),
    )
}
impl fmt::Display for EventCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "v1:{}:{}:{}:{}",
            self.stream, self.team, self.root, self.position
        )
    }
}
impl FromStr for EventCursor {
    type Err = DomainError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > 256 {
            return Err(invalid_cursor());
        }
        let fields: Vec<_> = value.split(':').collect();
        let ["v1", stream, team, root, position] = fields.as_slice() else {
            return Err(invalid_cursor());
        };
        if position.is_empty() || !position.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid_cursor());
        }
        Self::new(
            stream.parse().map_err(|_| invalid_cursor())?,
            team.parse().map_err(|_| invalid_cursor())?,
            root.parse().map_err(|_| invalid_cursor())?,
            position.parse().map_err(|_| invalid_cursor())?,
        )
    }
}
impl From<EventCursor> for String {
    fn from(value: EventCursor) -> Self {
        value.to_string()
    }
}
impl TryFrom<String> for EventCursor {
    type Error = DomainError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EventQuery {
    pub after: Option<EventCursor>,
    pub limit: u32,
    pub max_bytes: u32,
}
impl Default for EventQuery {
    fn default() -> Self {
        Self {
            after: None,
            limit: 100,
            max_bytes: 256 * 1024,
        }
    }
}
impl EventQuery {
    pub fn validate(self) -> Result<(), DomainError> {
        if !(1..=512).contains(&self.limit) || !(1..=MAX_EVENT_PAGE_BYTES).contains(&self.max_bytes)
        {
            return Err(DomainError::InvalidConfiguration(
                "event pages require 1..512 events and 1..16777216 bytes".into(),
            ));
        }
        Ok(())
    }
}
