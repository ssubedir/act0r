mod actors;
mod agent;
mod agent_registry;
mod benchmarks;
mod branches;
mod child_import_recovery;
mod delegated_work;
mod harness;
mod patch_applications;
mod planner_coordination;
pub mod ports;
mod project_detection;
mod project_profiles;
mod project_setup;
mod project_verification;
mod search;
mod services;
mod storage;
mod task_inspection;
mod tasks;
mod team_controls;
mod team_events;
mod team_goals;
mod team_tasks;
mod teams;
mod tool_recovery;
mod tool_registry;
mod worker;

pub use actors::*;
pub use agent::*;
pub use agent_registry::*;
pub use benchmarks::*;
pub use branches::*;
pub use child_import_recovery::*;
pub use delegated_work::*;
pub use harness::*;
pub use patch_applications::*;
pub use planner_coordination::*;
pub use project_detection::*;
pub use project_profiles::*;
pub use project_verification::*;
pub use search::*;
pub use services::*;
pub use storage::*;
pub use task_inspection::*;
pub use tasks::*;
pub use team_controls::*;
pub use team_events::*;
pub use team_goals::*;
pub use team_tasks::*;
pub use teams::*;
pub use tool_recovery::*;
pub use tool_registry::*;
pub use worker::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("project profile {0} was not found")]
    ProjectProfileNotFound(crate::domain::ProjectProfileId),
    #[error("storage is temporarily busy: {0}")]
    StorageBusy(String),
    #[error("storage cleanup refused: {0}")]
    StorageConflict(String),
    #[error("session {0} was not found")]
    SessionNotFound(crate::domain::SessionId),
    #[error("team {0} was not found")]
    TeamNotFound(crate::domain::TeamId),
    #[error(transparent)]
    ToolRejected(#[from] crate::domain::tools::ToolError),
    #[error("actor reminder {0} was not found")]
    ActorReminderNotFound(crate::domain::MessageId),
    #[error(
        "QueueFull: agent {agent_id} has {outstanding}/{capacity} outstanding tasks; cannot admit {requested} more"
    )]
    QueueFull {
        agent_id: crate::domain::AgentId,
        capacity: u32,
        outstanding: u64,
        requested: u64,
    },
    #[error("agent task {0} was not found")]
    AgentTaskNotFound(crate::domain::MessageId),
    #[error("agent {0} was not found")]
    AgentNotFound(crate::domain::AgentId),
    #[error(transparent)]
    Domain(#[from] crate::domain::DomainError),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("run {0} was not found")]
    NotFound(crate::domain::RunId),
    #[error("task {0} was not found")]
    TaskNotFound(crate::domain::TaskId),
    #[error("benchmark {0} was not found")]
    BenchmarkNotFound(crate::domain::BenchmarkId),
    #[error("checkpoint {0} was not found")]
    CheckpointNotFound(crate::domain::CheckpointId),
    #[error("fork {0} was not found")]
    ForkNotFound(crate::domain::ForkId),
    #[error("search {0} was not found")]
    SearchNotFound(crate::domain::SearchId),
    #[error("implementation unavailable: {0}")]
    Unavailable(String),
    #[error("another worker owns this database: {0}")]
    Ownership(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}
