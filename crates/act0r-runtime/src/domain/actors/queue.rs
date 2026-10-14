use super::*;

pub const DEFAULT_TASK_CAPACITY: u32 = 65;

/// Pinned admission policy, independent of an agent's behavior configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentQueuePolicy {
    pub task_capacity: u32,
}
impl Default for AgentQueuePolicy {
    fn default() -> Self {
        Self {
            task_capacity: DEFAULT_TASK_CAPACITY,
        }
    }
}
impl AgentQueuePolicy {
    pub fn validate(self) -> Result<(), DomainError> {
        if self.task_capacity == 0 {
            return Err(DomainError::InvalidConfiguration(
                "task capacity must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Unfinished requests assigned to this actor, including delegated executions.
#[derive(Clone, Debug, Serialize)]
pub struct AgentQueue {
    pub agent_id: AgentId,
    pub capacity: u32,
    pub outstanding: u64,
    pub available: u64,
    pub mailbox_tasks: u64,
    pub pending_tasks: u64,
    /// Includes running, waiting, paused, blocked, and cooperatively cancelling Runs.
    pub executing_tasks: u64,
}
impl AgentQueue {
    pub fn can_admit(&self, requested: u64) -> bool {
        requested <= self.available
    }
}
