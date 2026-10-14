//! A bounded, agent-reported checklist. Execution remains owned by the Run.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;

pub const MAX_STEPS: usize = 16;
pub const MAX_TITLE_BYTES: usize = 256;
pub const MAX_EXPLANATION_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    InProgress,
    Completed,
    Blocked,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    pub id: String,
    pub title: String,
    pub status: StepStatus,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPlan {
    pub revision: u32,
    pub explanation: String,
    pub steps: Vec<PlanStep>,
}

pub(crate) fn validate_update(explanation: &str, steps: &[PlanStep]) -> Result<(), DomainError> {
    let mut ids = HashSet::new();
    if explanation.trim().is_empty()
        || explanation.len() > MAX_EXPLANATION_BYTES
        || explanation.chars().any(char::is_control)
        || steps.is_empty()
        || steps.len() > MAX_STEPS
        || steps
            .iter()
            .filter(|s| s.status == StepStatus::InProgress)
            .count()
            > 1
        || steps.iter().any(|s| {
            s.id.is_empty()
                || s.id.len() > 32
                || !s
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || !ids.insert(&s.id)
                || s.title.trim().is_empty()
                || s.title.len() > MAX_TITLE_BYTES
                || s.title.chars().any(char::is_control)
        })
    {
        return Err(DomainError::InvalidWorkflow(
            "plan requires an explanation (1..512 UTF-8 bytes), 1..16 steps with unique ASCII IDs (1..32 characters), titles (1..256 UTF-8 bytes), and at most one in_progress step; text must be single-line".into(),
        ));
    }
    Ok(())
}
impl TaskPlan {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.revision == 0 {
            return Err(DomainError::InvalidState);
        }
        validate_update(&self.explanation, &self.steps)
    }
    /// Replace remaining work using stable IDs; completed work cannot disappear or change.
    pub(crate) fn update(
        current: Option<&Self>,
        expected_revision: u32,
        explanation: String,
        steps: Vec<PlanStep>,
    ) -> Result<Self, DomainError> {
        validate_update(&explanation, &steps)?;
        let revision = current.map_or(0, |p| p.revision);
        if expected_revision != revision {
            return Err(DomainError::InvalidWorkflow(format!(
                "plan revision changed; expected_revision must be {revision}"
            )));
        }
        if let Some(current) = current {
            current.validate()?;
            if current.steps.iter().any(|old| {
                old.status == StepStatus::Completed && !steps.iter().any(|new| new == old)
            }) {
                return Err(DomainError::InvalidWorkflow(
                    "preserve completed steps with their saved ID, title and status; revise only unfinished work".into(),
                ));
            }
        }
        Ok(Self {
            revision: revision.checked_add(1).ok_or(DomainError::InvalidState)?,
            explanation,
            steps,
        })
    }
}

/// Project only the native planner checkpoint, including a pinned registration wrapper.
pub fn from_run(run: &RunRecord) -> Result<Option<TaskPlan>, DomainError> {
    let state = if run.workflow == planner::PlannerWorkflow::definition_id() {
        &run.state
    } else if run.workflow.name.starts_with("registered_agent.") {
        let registration: integration::AgentRegistration =
            serde_json::from_value(run.state["registration"].clone())
                .map_err(|_| DomainError::InvalidState)?;
        if registration.definition != planner::PlannerWorkflow::definition_id() {
            return Ok(None);
        }
        if registration.workflow_id() != run.workflow
            || run.input["registration"] != run.state["registration"]
        {
            return Err(DomainError::InvalidState);
        }
        &run.state["state"]
    } else {
        return Ok(None);
    };
    let plan: Option<TaskPlan> =
        serde_json::from_value(state.get("task_plan").cloned().unwrap_or(Value::Null))
            .map_err(|_| DomainError::InvalidState)?;
    if let Some(plan) = &plan {
        plan.validate()?;
    }
    Ok(plan)
}

pub(crate) fn action_schema() -> Value {
    json!({"type":"object","properties":{
        "kind":{"type":"string","enum":["update_plan"]},
        "expected_revision":{"type":"integer","minimum":0,"maximum":u32::MAX},
        "explanation":{"type":"string","minLength":1,"maxLength":MAX_EXPLANATION_BYTES},
        "steps":{"type":"array","minItems":1,"maxItems":MAX_STEPS,"items":{
            "type":"object","properties":{
                "id":{"type":"string","minLength":1,"maxLength":32,"pattern":"^[A-Za-z0-9_-]+$"},
                "title":{"type":"string","minLength":1,"maxLength":MAX_TITLE_BYTES},
                "status":{"type":"string","enum":["pending","in_progress","completed","blocked"]}
            },"required":["id","title","status"],"additionalProperties":false}
        }
    },"required":["kind","expected_revision","explanation","steps"],"additionalProperties":false})
}

pub(crate) const INSTRUCTIONS: &str = " For multi-step work, use update_plan to save a short checklist before doing the work and update it as steps finish, become blocked, or user steering changes the remaining work. Simple answers do not require a plan. Supply expected_revision from task_plan.revision (0 when task_plan is null), a brief single-line explanation, and the full steps array. Use stable step IDs, at most one in_progress step, and retain every completed step with its exact ID, title and completed status. Revising unfinished steps is allowed. Mark completed only after doing the work; a checklist is not test evidence or execution authority. The runtime saves the plan then requests your next action. If an update is refused, use the saved plan and feedback to correct it. Treat the saved plan as your own task bookkeeping, subject to current user instructions and pinned permissions.";
