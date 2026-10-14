//! Session organization is independent of the actors and their execution history.
use super::{DomainError, EventStreamId, SessionId};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionChange {
    Rename { title: String },
    Archive,
    Restore,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SessionUpdate {
    pub expected_revision: u64,
    #[serde(flatten)]
    pub change: SessionChange,
}
// A strict wire enum avoids Serde's unsupported flatten + deny_unknown_fields combination.
impl<'de> Deserialize<'de> for SessionUpdate {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Rename {
                expected_revision: u64,
                title: String,
            },
            Archive {
                expected_revision: u64,
            },
            Restore {
                expected_revision: u64,
            },
        }
        Ok(match Wire::deserialize(deserializer)? {
            Wire::Rename {
                expected_revision,
                title,
            } => Self {
                expected_revision,
                change: SessionChange::Rename { title },
            },
            Wire::Archive { expected_revision } => Self {
                expected_revision,
                change: SessionChange::Archive,
            },
            Wire::Restore { expected_revision } => Self {
                expected_revision,
                change: SessionChange::Restore,
            },
        })
    }
}
impl SessionUpdate {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.expected_revision >= i64::MAX as u64 {
            return Err(DomainError::InvalidConfiguration(
                "session revision is outside its supported range".into(),
            ));
        }
        if let SessionChange::Rename { title } = &self.change
            && (title.trim().is_empty() || title.len() > 256 || title.chars().any(char::is_control))
        {
            return Err(DomainError::InvalidConfiguration(
                "session titles require 1..256 UTF-8 bytes without control characters".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub title: Option<String>,
    pub archived: bool,
    pub revision: u64,
    #[serde(skip_serializing)]
    pub last_request: Option<SessionUpdate>,
}
impl SessionMetadata {
    pub fn apply(
        &mut self,
        request: &SessionUpdate,
        blocked: Option<&str>,
    ) -> Result<bool, DomainError> {
        request.validate()?;
        if self.revision == request.expected_revision + 1
            && self.last_request.as_ref() == Some(request)
        {
            return Ok(true);
        }
        if self.revision != request.expected_revision {
            return Err(DomainError::SessionConflict(format!(
                "refresh session metadata: expected revision {}, current {}",
                request.expected_revision, self.revision
            )));
        }
        match &request.change {
            SessionChange::Rename { title } => self.title = Some(title.trim().into()),
            SessionChange::Archive => {
                if let Some(reason) = blocked {
                    return Err(DomainError::SessionConflict(reason.into()));
                }
                self.archived = true;
            }
            SessionChange::Restore => self.archived = false,
        }
        self.revision += 1;
        self.last_request = Some(request.clone());
        Ok(false)
    }
}
#[derive(Debug, Serialize)]
pub struct SessionMetadataView {
    pub session_id: SessionId,
    #[serde(flatten)]
    pub metadata: SessionMetadata,
    pub archive_blocked_reason: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct SessionUpdateReceipt {
    #[serde(flatten)]
    pub view: SessionMetadataView,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SessionListCursor {
    pub stream: EventStreamId,
    pub archived: bool,
    pub snapshot: u64,
    pub before: u64,
}
fn invalid_cursor() -> DomainError {
    DomainError::InvalidConfiguration("invalid session cursor or scope".into())
}
impl SessionListCursor {
    pub fn new(
        stream: EventStreamId,
        archived: bool,
        snapshot: u64,
        before: u64,
    ) -> Result<Self, DomainError> {
        if snapshot > i64::MAX as u64 || before == 0 || before > snapshot {
            return Err(invalid_cursor());
        }
        Ok(Self {
            stream,
            archived,
            snapshot,
            before,
        })
    }
    pub fn validate_scope(self, stream: EventStreamId, archived: bool) -> Result<(), DomainError> {
        Self::new(self.stream, self.archived, self.snapshot, self.before)?;
        if self.stream != stream || self.archived != archived {
            return Err(invalid_cursor());
        }
        Ok(())
    }
}
impl fmt::Display for SessionListCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "s1:{}:{}:{}:{}",
            self.stream,
            if self.archived { "archived" } else { "active" },
            self.snapshot,
            self.before
        )
    }
}
impl FromStr for SessionListCursor {
    type Err = DomainError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > 256 {
            return Err(invalid_cursor());
        }
        let fields: Vec<_> = value.split(':').collect();
        let ["s1", stream, view, snapshot, before] = fields.as_slice() else {
            return Err(invalid_cursor());
        };
        let archived = match *view {
            "active" => false,
            "archived" => true,
            _ => return Err(invalid_cursor()),
        };
        if [snapshot, before]
            .iter()
            .any(|s| s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(invalid_cursor());
        }
        Self::new(
            stream.parse().map_err(|_| invalid_cursor())?,
            archived,
            snapshot.parse().map_err(|_| invalid_cursor())?,
            before.parse().map_err(|_| invalid_cursor())?,
        )
    }
}
impl From<SessionListCursor> for String {
    fn from(value: SessionListCursor) -> Self {
        value.to_string()
    }
}
impl TryFrom<String> for SessionListCursor {
    type Error = DomainError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SessionListQuery {
    pub archived: bool,
    pub after: Option<SessionListCursor>,
    pub limit: u32,
}
impl Default for SessionListQuery {
    fn default() -> Self {
        Self {
            archived: false,
            after: None,
            limit: 50,
        }
    }
}
impl SessionListQuery {
    pub fn validate(self) -> Result<(), DomainError> {
        if !(1..=50).contains(&self.limit) {
            return Err(DomainError::InvalidConfiguration(
                "session pages require 1..50 items".into(),
            ));
        }
        Ok(())
    }
}
