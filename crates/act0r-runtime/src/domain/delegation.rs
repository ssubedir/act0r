//! Durable child-task intent; delivery and continuation share the Run boundary.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationBudget {
    pub max_tasks: u32,
    pub max_depth: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_budget: Option<UsageBudget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallel_tasks: Option<u32>,
}
impl DelegationBudget {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self
            .max_parallel_tasks
            .is_some_and(|n| !(2..=16).contains(&n) || n > self.max_tasks)
        {
            return Err(DomainError::InvalidConfiguration(
                "parallel delegation requires 2..16 tasks within the task allowance".into(),
            ));
        }
        if self
            .shared_budget
            .as_ref()
            .is_some_and(|b| b.validate().is_err())
        {
            return Err(DomainError::InvalidConfiguration(
                "shared model-call and token limits must be positive or unlimited".into(),
            ));
        }
        if !(1..=64).contains(&self.max_tasks) || !(1..=8).contains(&self.max_depth) {
            return Err(DomainError::InvalidConfiguration(
                "delegation requires 1..64 tasks and depth 1..8".into(),
            ));
        }
        if self
            .timeout_ms
            .is_some_and(|ms| ms == 0 || i64::try_from(ms).is_err())
        {
            return Err(DomainError::InvalidConfiguration(
                "delegation timeout must be a positive supported millisecond duration".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskDelegation {
    pub id: MessageId,
    pub recipient: AgentId,
    pub goal: String,
    pub created_at: i64,
    pub outcome: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timed_out_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<TaskGroupId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_input: Option<workspace_handoff::PreparedWorkspace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_dependency: Option<Box<initial_dependencies::InitialDependency>>,
}

/// A durable all-terminal join. Child order is the order requested by the planner.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskGroup {
    pub id: TaskGroupId,
    pub children: Vec<MessageId>,
    pub created_at: i64,
    pub deadline_at: Option<i64>,
    pub joined_at: Option<i64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegatedWork {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<AgentId>,
    pub recipient: AgentId,
    pub goal: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writable_paths: Option<Vec<String>>,
}
impl TaskDelegation {
    pub fn deadline_pending(&self, child_completed_at: Option<i64>) -> bool {
        self.outcome.is_none()
            && self.timed_out_at.is_none()
            && self
                .deadline_at
                .is_some_and(|deadline| child_completed_at.is_none_or(|at| at > deadline))
    }
    pub fn deadline_due(&self, now: i64, child_completed_at: Option<i64>) -> bool {
        self.deadline_pending(child_completed_at) && self.deadline_at.is_some_and(|at| at <= now)
    }
    pub fn completed_outcome(&self, child: Value) -> Value {
        match self.timed_out_at {
            None => child,
            Some(at) => serde_json::json!({
                "task_id":self.id,"run_id":child["run_id"],"status":"cancelled","result":null,
                "error":{"code":"delegation_timeout","message":"delegated task exceeded its saved deadline"},
                "usage":child["usage"],"deadline_at":self.deadline_at,"timed_out_at":at,"child_outcome":child
            }),
        }
    }
}
