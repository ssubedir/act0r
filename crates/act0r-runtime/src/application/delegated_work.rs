//! Bounded projections of durable delegation facts. No model or filesystem execution.
use super::{Error, TaskInspectionOptions, TaskInspectionSnapshot, task_inspection};
use crate::domain::{change_review::*, delegation::TaskDelegation, *};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Serialize)]
pub struct DelegatedWorkView {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub groups: Vec<DelegatedWorkGroup>,
    pub coordination: Option<super::PlannerCoordination>,
    pub truncated: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct DelegatedWorkGroup {
    pub parent_task_id: MessageId,
    pub group_id: Option<TaskGroupId>,
    pub total: usize,
    pub finished: usize,
    pub joined: bool,
    pub children: Vec<DelegatedWorkTask>,
}
#[derive(Clone, Debug, Serialize)]
pub struct DelegatedWorkTask {
    pub task_id: MessageId,
    pub actor_id: AgentId,
    pub run_id: Option<RunId>,
    pub goal: String,
    pub goal_truncated: bool,
    pub write_scope: Option<DelegatedWriteScope>,
    pub status: String,
    pub execution_held: bool,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guidance_wait_id: Option<WaitId>,
    pub result: Option<String>,
    pub result_truncated: bool,
    pub changes: Option<DelegatedWorkChanges>,
    pub review: Option<ChildPatchReview>,
    pub steering: Option<steering::TurnSteering>,
    pub recovery: Option<worker_recovery::WorkerRecovery>,
    pub cancellation: Option<worker_cancellation::WorkerCancellation>,
    pub pause: Option<worker_pause::WorkerPause>,
    pub messages: Option<worker_messages::WorkerMessages>,
    pub questions: Option<worker_questions::WorkerQuestions>,
}
#[derive(Clone, Debug, Serialize)]
pub struct DelegatedWriteScope {
    pub source: &'static str,
    pub paths: Vec<String>,
    pub paths_truncated: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct DelegatedWorkChanges {
    pub artifact_id: ActivityId,
    pub file_count: usize,
    pub files: Vec<String>,
    pub files_truncated: bool,
    pub test_status: TestEvidenceStatus,
    pub tests: Option<DelegatedWorkTests>,
    pub completion: Option<completion::CompletionStatus>,
}
#[derive(Clone, Debug, Serialize)]
pub struct DelegatedWorkTests {
    pub activity_id: ActivityId,
    pub command: tools::WorkspaceTestCommand,
    pub ok: Option<bool>,
    pub exit_code: Option<i64>,
}
#[derive(Clone, Debug, Serialize)]
pub struct ChildPatchReview {
    pub artifact_id: ActivityId,
    pub status: &'static str,
    pub inspection_id: Option<ActivityId>,
    pub import_activity_id: Option<ActivityId>,
    pub detail: Option<String>,
}
pub(crate) fn delegated_text(value: &str, max: usize) -> String {
    let mut end = value.len().min(max);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}
fn terminal(status: &str) -> bool {
    matches!(
        status,
        "succeeded" | "failed" | "rejected" | "refused" | "cancelled" | "budget_exhausted"
    )
}
pub(crate) fn assemble_delegated_work(
    session_id: SessionId,
    turn_id: MessageId,
    snapshot: TaskInspectionSnapshot,
    changes: HashMap<MessageId, TaskChanges>,
    reviews: HashMap<MessageId, ChildPatchReview>,
    scopes: HashMap<MessageId, Vec<String>>,
) -> Result<DelegatedWorkView, Error> {
    let parents: Vec<_> = snapshot
        .nodes
        .iter()
        .filter_map(|n| {
            n.run
                .as_ref()
                .filter(|r| !r.record.delegations.is_empty())
                .map(|r| (n.id, r.record.clone()))
        })
        .collect();
    let inspection =
        task_inspection::assemble(turn_id, TaskInspectionOptions::default(), snapshot)?;
    let nodes: HashMap<_, _> = inspection.nodes.iter().map(|n| (n.task_id, n)).collect();
    let mut groups = Vec::new();
    for (parent_task_id, parent) in parents {
        let mut seen = HashSet::new();
        for first in &parent.delegations {
            // Single delegations have no group ID; each is its own durable join.
            let key = first
                .group_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| first.id.to_string());
            if !seen.insert(key) {
                continue;
            }
            let tasks: Vec<&TaskDelegation> = parent
                .delegations
                .iter()
                .filter(|d| match first.group_id {
                    Some(id) => d.group_id == Some(id),
                    None => d.id == first.id,
                })
                .collect();
            let finished = tasks
                .iter()
                .filter(|d| {
                    d.outcome.is_some() || nodes.get(&d.id).is_some_and(|n| terminal(&n.status))
                })
                .count();
            let joined = first.group_id.map_or_else(
                || {
                    first.outcome.is_some()
                        && parent
                            .planner_question
                            .as_ref()
                            .is_none_or(|q| q.child_task_id != first.id)
                },
                |id| {
                    parent
                        .task_groups
                        .iter()
                        .any(|g| g.id == id && g.joined_at.is_some())
                },
            );
            let mut children = Vec::new();
            for delegation in &tasks {
                let Some(node) = nodes.get(&delegation.id) else {
                    continue;
                };
                let saved = changes.get(&delegation.id);
                let artifact =
                    saved
                        .and_then(|v| v.changes.as_ref())
                        .map(|v| DelegatedWorkChanges {
                            artifact_id: v.activity_id,
                            file_count: v.files.len(),
                            files: v.files.iter().take(100).map(|f| f.path.clone()).collect(),
                            files_truncated: v.files.len() > 100,
                            test_status: v.test_status,
                            tests: v.tests.as_ref().map(|t| DelegatedWorkTests {
                                activity_id: t.activity_id,
                                command: t.command.clone(),
                                ok: t.result["ok"].as_bool(),
                                exit_code: t.result["exit_code"].as_i64(),
                            }),
                            completion: saved.and_then(|s| s.completion.as_ref().map(|c| c.status)),
                        });
                let result = node.result.as_ref().or(node.error.as_ref()).map(|r| {
                    r.as_str()
                        .map(str::to_owned)
                        .or_else(|| {
                            ["answer", "summary", "message"]
                                .iter()
                                .find_map(|key| r[*key].as_str().map(str::to_owned))
                        })
                        .unwrap_or_else(|| serde_json::to_string_pretty(r).unwrap_or_default())
                });
                children.push(DelegatedWorkTask {
                    task_id: node.task_id,
                    actor_id: node.actor_id,
                    run_id: node.run_id,
                    goal: delegated_text(&delegation.goal, 8192),
                    goal_truncated: delegation.goal.len() > 8192,
                    write_scope: delegation
                        .workspace_input
                        .as_ref()
                        .and_then(|p| p.request.writable_paths.as_ref())
                        .map(|paths| ("task", paths))
                        .or_else(|| scopes.get(&delegation.id).map(|paths| ("worker", paths)))
                        .map(|(source, paths)| DelegatedWriteScope {
                            source,
                            paths: paths.iter().take(32).cloned().collect(),
                            paths_truncated: paths.len() > 32,
                        }),
                    status: node.status.clone(),
                    execution_held: node.execution_held,
                    reason: delegated_text(&node.reason.message, 2048),
                    guidance_wait_id: node
                        .wait
                        .as_ref()
                        .filter(|wait| wait.key == clarification::WAIT_KEY)
                        .map(|wait| wait.id),
                    result_truncated: result.as_ref().is_some_and(|s| s.len() > 16384),
                    result: result.map(|s| delegated_text(&s, 16384)),
                    changes: artifact,
                    review: reviews.get(&delegation.id).cloned(),
                    steering: None,
                    recovery: None,
                    cancellation: None,
                    pause: None,
                    messages: None,
                    questions: None,
                });
            }
            if children.is_empty() {
                continue;
            }
            groups.push(DelegatedWorkGroup {
                parent_task_id,
                group_id: first.group_id,
                total: tasks.len(),
                finished,
                joined,
                children,
            });
        }
    }
    Ok(DelegatedWorkView {
        session_id,
        turn_id,
        groups,
        coordination: None,
        truncated: !inspection.truncations.is_empty(),
    })
}
