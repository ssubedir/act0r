//! A visible conversation links execution scopes without merging actor mailboxes.
use super::{harness::*, *};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectContinuation {
    pub session_id: SessionId,
    pub task_id: MessageId,
}
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationStage {
    Inspecting,
    SetupReview,
    Coding,
    Verifying,
    Completed,
    Incomplete,
    Failed,
    Stopped,
}
pub fn stage(
    discovery: bool,
    status: &str,
    result: &Value,
    settled: bool,
    verifying: bool,
) -> ConversationStage {
    match status {
        "incomplete" => ConversationStage::Incomplete,
        "cancelled" => ConversationStage::Stopped,
        "failed" | "rejected" | "budget_exhausted" | "blocked" => ConversationStage::Failed,
        "succeeded" if discovery && result.get("project_setup").is_some() => {
            ConversationStage::SetupReview
        }
        "succeeded" if settled && result["completion"]["status"] == "incomplete" => {
            ConversationStage::Incomplete
        }
        "succeeded" if settled => ConversationStage::Completed,
        _ if discovery => ConversationStage::Inspecting,
        _ if verifying => ConversationStage::Verifying,
        _ => ConversationStage::Coding,
    }
}
/// Stable paging across linked physical sessions. Later coding admission cannot
/// renumber earlier source turns or change the scope of a captured page cursor.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ConversationCursor {
    pub session_id: SessionId,
    pub source_sequence: u64,
    pub phase: u8,
    pub sequence: u64,
}
impl ConversationCursor {
    pub fn validate(&self, session: SessionId) -> Result<(), DomainError> {
        if self.session_id != session
            || self.phase > 64
            || self.source_sequence == 0
            || self.source_sequence > i64::MAX as u64
            || self.sequence == 0
            || self.sequence > i64::MAX as u64
        {
            return Err(DomainError::InvalidConfiguration(
                "invalid conversation cursor or scope".into(),
            ));
        }
        Ok(())
    }
}
impl fmt::Display for ConversationCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "a1:{}:{}:{}:{}",
            self.session_id, self.source_sequence, self.phase, self.sequence
        )
    }
}
impl FromStr for ConversationCursor {
    type Err = DomainError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || DomainError::InvalidConfiguration("invalid conversation cursor".into());
        if value.len() > 160 {
            return Err(invalid());
        }
        let parts = value.split(':').collect::<Vec<_>>();
        let ["a1", session, source, phase, sequence] = parts.as_slice() else {
            return Err(invalid());
        };
        if [*source, *phase, *sequence]
            .iter()
            .any(|s| s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(invalid());
        }
        let cursor = Self {
            session_id: session.parse().map_err(|_| invalid())?,
            source_sequence: source.parse().map_err(|_| invalid())?,
            phase: phase.parse().map_err(|_| invalid())?,
            sequence: sequence.parse().map_err(|_| invalid())?,
        };
        cursor.validate(cursor.session_id)?;
        Ok(cursor)
    }
}
impl From<ConversationCursor> for String {
    fn from(value: ConversationCursor) -> Self {
        value.to_string()
    }
}
impl TryFrom<String> for ConversationCursor {
    type Error = DomainError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[derive(Debug, Serialize)]
pub struct ConversationTurn {
    pub session_id: SessionId,
    pub team_id: TeamId,
    pub phase: String,
    pub repeated_request: bool,
    pub cursor: ConversationCursor,
    pub clarifications: Vec<SavedClarification>,
    pub omitted_clarifications: usize,
    #[serde(flatten)]
    pub turn: HarnessTurn,
}
#[derive(Debug, Serialize)]
pub struct SavedClarification {
    pub wait_id: WaitId,
    pub question: discovery_context::Excerpt,
    pub answer: discovery_context::Excerpt,
}
#[derive(Debug, Serialize)]
pub struct ContinuousConversation {
    pub auto_execute: bool,
    pub setup_error: Option<String>,
    pub requested_session_id: SessionId,
    pub session_id: SessionId,
    pub title: Option<String>,
    pub archived: bool,
    pub automatic: bool,
    pub stage: ConversationStage,
    pub active: HarnessReceipt,
    pub selected: HarnessReceipt,
    pub can_continue: bool,
    pub model: String,
    pub workspace: Option<String>,
    pub coding: Option<HarnessCoding>,
    pub turns: Vec<ConversationTurn>,
    pub next_before: Option<ConversationCursor>,
}
/// Give the filtered sidebar its own cursor scope.
pub fn list_stream(stream: EventStreamId) -> EventStreamId {
    let bytes = Sha256::digest(format!("act0r-continuous-list-v1:{stream}"));
    uuid::Uuid::from_slice(&bytes[..16])
        .expect("digest UUID")
        .to_string()
        .parse()
        .expect("UUID")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn stages_require_actual_verification_and_settled_completion() {
        assert_eq!(
            stage(true, "waiting", &Value::Null, false, false),
            ConversationStage::Inspecting
        );
        assert_eq!(
            stage(true, "succeeded", &json!({"project_setup":{}}), true, false),
            ConversationStage::SetupReview
        );
        assert_eq!(
            stage(false, "waiting", &Value::Null, false, true),
            ConversationStage::Verifying
        );
        assert_eq!(
            stage(false, "succeeded", &json!({"summary":"done"}), false, false),
            ConversationStage::Coding
        );
        assert_eq!(
            stage(
                false,
                "succeeded",
                &json!({"completion":{"status":"incomplete"}}),
                true,
                false
            ),
            ConversationStage::Incomplete
        );
        assert_eq!(
            stage(
                false,
                "succeeded",
                &json!({"completion":{"status":"verified"}}),
                true,
                false
            ),
            ConversationStage::Completed
        );
        assert_eq!(
            stage(false, "cancelled", &Value::Null, true, false),
            ConversationStage::Stopped
        );
    }
    #[test]
    fn conversation_cursors_are_scoped_and_lists_have_distinct_streams() {
        let session = SessionId::new();
        let cursor = ConversationCursor {
            session_id: session,
            source_sequence: 2,
            phase: 1,
            sequence: 30,
        };
        let decoded: ConversationCursor = cursor.to_string().parse().unwrap();
        decoded.validate(session).unwrap();
        assert!(decoded.validate(SessionId::new()).is_err());
        for value in [
            format!("a1:{session}:0:1:1"),
            format!("a1:{session}:1:65:1"),
            format!("a1:{session}:1:1:0"),
            format!("a1:{session}:1:1:18446744073709551615"),
        ] {
            assert!(value.parse::<ConversationCursor>().is_err());
        }
        let stream = EventStreamId::new();
        assert_ne!(list_stream(stream), stream);
        assert_eq!(list_stream(stream), list_stream(stream));
    }
}
