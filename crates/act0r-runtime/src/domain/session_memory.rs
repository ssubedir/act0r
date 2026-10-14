//! Session-scoped views and optimistic, durably receipted memory decisions.
use super::{actors::*, teams::TeamRole, *};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClearMemoryRequest {
    pub request_id: MessageId,
    pub expected_revision: u64,
    pub expected_generation: u64,
}
impl ClearMemoryRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.request_id.to_string() == uuid::Uuid::nil().to_string()
            || self.expected_revision == u64::MAX
            || self.expected_generation == u64::MAX
        {
            return Err(DomainError::InvalidConfiguration(
                "memory clear requires a nonzero request ID and incrementable counters".into(),
            ));
        }
        Ok(())
    }
    pub fn check(&self, memory: &ActorMemory) -> Result<(), DomainError> {
        self.validate()?;
        memory.validate()?;
        if memory.revision != self.expected_revision
            || memory.generation != self.expected_generation
        {
            return Err(DomainError::SessionConflict(
                "actor memory changed; refresh it before clearing".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClearMemoryReceipt {
    pub session_id: SessionId,
    pub agent_id: AgentId,
    pub request: ClearMemoryRequest,
    pub revision: u64,
    pub generation: u64,
    pub cleared_at: i64,
    pub replayed: bool,
}
#[derive(Serialize)]
pub struct SessionActorMemory {
    pub agent_id: AgentId,
    pub name: String,
    pub role: TeamRole,
    pub memory: Option<ActorMemory>,
    pub retained_bytes: usize,
}
#[derive(Serialize)]
pub struct SessionMemory {
    pub session_id: SessionId,
    pub actors: Vec<SessionActorMemory>,
}
#[derive(Serialize)]
pub struct TurnMemoryRun {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_summary: Option<session_summary::SummarySnapshot>,
    pub run_id: RunId,
    pub task_id: MessageId,
    pub agent_id: AgentId,
    pub name: String,
    pub status: String,
    pub created_at: i64,
    pub memory: Option<MemorySnapshot>,
}
#[derive(Serialize)]
pub struct TurnMemory {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub runs: Vec<TurnMemoryRun>,
    pub truncated: bool,
}
