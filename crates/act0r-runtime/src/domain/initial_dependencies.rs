//! Initial assignment ordering uses the same exact-task hold and dependency review.
use super::{agent::PlannerTask, delegation::DelegatedWork, *};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialDependency {
    pub activity_id: ActivityId,
    pub prerequisite_task_id: MessageId,
}

pub fn validate_names(tasks: &[PlannerTask], enabled: bool) -> Result<(), DomainError> {
    let edges: Vec<_> = tasks.iter().filter(|t| t.depends_on.is_some()).collect();
    if edges.is_empty() {
        return Ok(());
    }
    if !enabled || tasks.len() != 2 || edges.len() != 1 {
        return Err(invalid());
    }
    let waiting = edges[0];
    let prerequisite = waiting.depends_on.as_ref().unwrap();
    if !matches!(waiting.worker.as_str(), "coder" | "coder2")
        || !matches!(prerequisite.as_str(), "coder" | "coder2")
        || waiting.worker == *prerequisite
        || !tasks.iter().any(|t| &t.worker == prerequisite)
    {
        return Err(invalid());
    }
    Ok(())
}

pub fn validate_workers(tasks: &[DelegatedWork]) -> Result<(), DomainError> {
    let edges: Vec<_> = tasks.iter().filter(|t| t.depends_on.is_some()).collect();
    if edges.is_empty() {
        return Ok(());
    }
    if tasks.len() != 2 || edges.len() != 1 {
        return Err(invalid());
    }
    let waiting = edges[0];
    let source = waiting.depends_on.unwrap();
    if source == waiting.recipient || !tasks.iter().any(|t| t.recipient == source) {
        return Err(invalid());
    }
    Ok(())
}

/// Bind the declaration to prepared physical tasks before publication can admit either.
pub fn bind_tasks(
    tasks: &[(DelegatedWork, Option<workspace_handoff::PreparedWorkspace>)],
) -> Result<HashMap<AgentId, InitialDependency>, DomainError> {
    let mut bindings = HashMap::new();
    for (task, prepared) in tasks {
        if let Some(source) = task.depends_on {
            let receiver = prepared.as_ref().ok_or(DomainError::InvalidState)?;
            let prerequisite = tasks
                .iter()
                .find(|(t, _)| t.recipient == source)
                .and_then(|(_, p)| p.as_ref())
                .ok_or(DomainError::InvalidState)?;
            if receiver.activity_id != prerequisite.activity_id
                || receiver.task_id == prerequisite.task_id
            {
                return Err(DomainError::InvalidState);
            }
            bindings.insert(
                task.recipient,
                InitialDependency {
                    activity_id: receiver.activity_id,
                    prerequisite_task_id: prerequisite.task_id,
                },
            );
        }
    }
    Ok(bindings)
}

fn invalid() -> DomainError {
    DomainError::InvalidWorkflow("initial dependencies require one coder/coder2 edge between two distinct members of the same group; self, missing and circular prerequisites are refused".into())
}

impl Run {
    pub(crate) fn initial_dependency_bound(
        &mut self,
        dependency: &planner_pause::WorkerDependency,
        now: i64,
    ) -> Result<(), DomainError> {
        dependency.validate()?;
        let waiting = self
            .record
            .delegations
            .iter()
            .find(|d| d.id == dependency.task_id)
            .ok_or(DomainError::InvalidState)?;
        if dependency.planner_run_id != self.record.id
            || waiting.initial_dependency.as_ref().is_none_or(|i| {
                i.activity_id != dependency.activity_id
                    || i.prerequisite_task_id != dependency.prerequisite_task_id
            })
        {
            return Err(DomainError::InvalidState);
        }
        self.record.revision += 1;
        self.event(
            "initial_dependency_bound",
            json!({"dependency":dependency}),
            None,
            None,
            now,
        );
        Ok(())
    }
}

pub fn schema(config: &planner::PlannerConfig, schema: &mut Value, instructions: &mut String) {
    if !config.initial_dependencies {
        return;
    }
    let variants = schema["properties"]["action"]["anyOf"]
        .as_array_mut()
        .unwrap();
    for variant in variants {
        if variant["properties"]["kind"]["enum"][0] != "delegate_tasks" {
            continue;
        }
        let items = &mut variant["properties"]["tasks"]["items"];
        let mut alternatives = items["anyOf"]
            .as_array()
            .cloned()
            .unwrap_or_else(|| vec![items.clone()]);
        let dependent: Vec<_> = alternatives
            .iter()
            .map(|item| {
                let mut item = item.clone();
                item["properties"]["depends_on"] =
                    json!({"type":"string","enum":["coder","coder2"]});
                item["required"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!("depends_on"));
                item
            })
            .collect();
        alternatives.extend(dependent);
        *items = json!({"anyOf":alternatives});
    }
    instructions.push_str(" Initial dependencies: for two coding assignments, optionally set depends_on on one delegate_tasks member to the other coder/coder2. Independent tasks omit depends_on. Declare dependent work upfront so the receiver prepares its isolated workspace and original baseline, then waits before its first model call. The prerequisite continues. Its terminal outcome wakes this same planner for explicit review; inspect_worker_changes/import_worker_changes request the existing patch approval, then resume_worker and guide_workers. Failure, stop or timeout requires a decision to guide/resume or stop the waiting worker. Denial never releases a hold. Include prerequisite files in the receiver's writable_paths when a patch transfer is needed. Import the receiver's consolidated patch once and run fresh parent tests. Self, missing and circular dependencies are invalid. Existing limits and original deadlines apply.");
}
