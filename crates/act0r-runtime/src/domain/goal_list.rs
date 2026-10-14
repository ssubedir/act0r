//! Stable membership for a descending page of persisted planner roots.
use super::{DomainError, EventStreamId, TeamId};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct GoalListCursor {
    stream: EventStreamId,
    team: TeamId,
    snapshot: u64,
    before: u64,
}
impl GoalListCursor {
    pub fn new(
        stream: EventStreamId,
        team: TeamId,
        snapshot: u64,
        before: u64,
    ) -> Result<Self, DomainError> {
        if snapshot > i64::MAX as u64 || before == 0 || before > snapshot {
            return Err(invalid_cursor());
        }
        Ok(Self {
            stream,
            team,
            snapshot,
            before,
        })
    }
    pub fn snapshot(self) -> u64 {
        self.snapshot
    }
    pub fn before(self) -> u64 {
        self.before
    }
    pub fn validate_scope(self, stream: EventStreamId, team: TeamId) -> Result<(), DomainError> {
        if (self.stream, self.team) != (stream, team) {
            return Err(DomainError::InvalidConfiguration(
                "goal cursor belongs to another database or team".into(),
            ));
        }
        Ok(())
    }
}
fn invalid_cursor() -> DomainError {
    DomainError::InvalidConfiguration(
        "invalid goal cursor; expected g1:STREAM:TEAM:SNAPSHOT:BEFORE".into(),
    )
}
impl fmt::Display for GoalListCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "g1:{}:{}:{}:{}",
            self.stream, self.team, self.snapshot, self.before
        )
    }
}
impl FromStr for GoalListCursor {
    type Err = DomainError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > 256 {
            return Err(invalid_cursor());
        }
        let fields: Vec<_> = value.split(':').collect();
        let ["g1", stream, team, snapshot, before] = fields.as_slice() else {
            return Err(invalid_cursor());
        };
        if [snapshot, before]
            .iter()
            .any(|v| v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(invalid_cursor());
        }
        Self::new(
            stream.parse().map_err(|_| invalid_cursor())?,
            team.parse().map_err(|_| invalid_cursor())?,
            snapshot.parse().map_err(|_| invalid_cursor())?,
            before.parse().map_err(|_| invalid_cursor())?,
        )
    }
}
impl From<GoalListCursor> for String {
    fn from(value: GoalListCursor) -> Self {
        value.to_string()
    }
}
impl TryFrom<String> for GoalListCursor {
    type Error = DomainError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GoalListQuery {
    pub after: Option<GoalListCursor>,
    pub limit: u32,
}
impl Default for GoalListQuery {
    fn default() -> Self {
        Self {
            after: None,
            limit: 25,
        }
    }
}
impl GoalListQuery {
    pub fn validate(self) -> Result<(), DomainError> {
        if !(1..=100).contains(&self.limit) {
            return Err(DomainError::InvalidConfiguration(
                "goal pages require 1..100 items".into(),
            ));
        }
        Ok(())
    }
}
