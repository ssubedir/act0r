#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("project profile change refused: {0}")]
    ProjectProfileConflict(String),
    #[error("session change refused: {0}")]
    SessionConflict(String),
    #[error("message ID is reserved for durable reminder work")]
    ReservedMessageId,
    #[error("message ID already belongs to a different envelope")]
    ConflictingMessage,
    #[error("invalid workflow: {0}")]
    InvalidWorkflow(String),
    #[error("unknown activity or attempt")]
    UnknownAttempt,
    #[error("unknown durable timer")]
    UnknownTimer,
    #[error("unknown or no longer pending human-input wait")]
    UnknownWait,
    #[error("completion is from a superseded attempt")]
    StaleAttempt,
    #[error("conflicting outcome for an already recorded attempt")]
    ConflictingOutcome,
    #[error("conflicting reconciliation for an already resolved attempt")]
    ConflictingReconciliation,
    #[error(
        "goal control revision conflict: expected {expected_revision}, current {current_revision}"
    )]
    GoalRevisionConflict {
        expected_revision: u64,
        current_revision: u64,
    },
    #[error("invalid reconciliation: {0}")]
    InvalidReconciliation(String),
    #[error("run cannot accept this operation in its current state")]
    InvalidState,
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error(
        "essential agent context requires {required_bytes} input bytes but the limit is {limit_bytes}; increase context.max_input_bytes or reduce the goal, permissions, or newest result"
    )]
    ContextLimit {
        required_bytes: u64,
        limit_bytes: u32,
    },
}
