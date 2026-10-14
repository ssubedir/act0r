//! Read models assembled from a consistent, bounded snapshot of durable task facts.
use super::{Error, ports::TaskInspectionReadPort};
use crate::domain::{actors::*, shared_budget::*, *};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, sync::Arc};

#[derive(Clone, Copy, Debug, Serialize)]
pub struct TaskInspectionOptions {
    pub max_nodes: u32,
    pub max_depth: u32,
}
impl Default for TaskInspectionOptions {
    fn default() -> Self {
        Self {
            max_nodes: 128,
            max_depth: 16,
        }
    }
}
impl TaskInspectionOptions {
    pub fn validate(self) -> Result<(), Error> {
        if !(1..=512).contains(&self.max_nodes) || self.max_depth > 128 {
            return Err(DomainError::InvalidConfiguration(
                "task inspection requires 1..512 nodes and depth 0..128".into(),
            )
            .into());
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRelation {
    Forwarded,
    Delegated,
    FollowUp,
}
impl TaskRelation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Forwarded => "forwarded",
            Self::Delegated => "delegated",
            Self::FollowUp => "follow_up",
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskInspectionEdge {
    pub parent_task_id: MessageId,
    pub child_task_id: MessageId,
    pub relation: TaskRelation,
    pub group_id: Option<TaskGroupId>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskInspectionTruncation {
    pub parent_task_id: MessageId,
    pub reason: &'static str,
    pub omitted_children: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskInspectionActor {
    pub status: AgentStatus,
    pub error: Option<String>,
    pub supervision_pause: Option<SupervisionPause>,
}
#[derive(Clone, Debug)]
pub enum TaskInspectionIntent {
    Delegation(delegation::TaskDelegation),
    FollowUp(follow_up::FollowUpIntent),
}
#[derive(Clone, Debug, Deserialize)]
pub struct TaskInspectionActivity {
    pub id: ActivityId,
    pub handler: DefinitionId,
    pub status: ActivityStatus,
    pub ready_at: i64,
    pub error: Option<Failure>,
}
#[derive(Clone, Debug)]
pub struct TaskInspectionRun {
    pub record: RunRecord,
    pub activity: Option<TaskInspectionActivity>,
    pub timer: Option<(TimerId, i64)>,
}
#[derive(Clone, Debug)]
pub struct TaskInspectionFacts {
    pub id: MessageId,
    pub actor_id: AgentId,
    pub actor: Option<TaskInspectionActor>,
    pub depth: u32,
    pub goal: Option<String>,
    pub task: Option<AgentTaskRecord>,
    pub run: Option<TaskInspectionRun>,
    pub reminder: Option<ActorReminder>,
    pub intent: Option<TaskInspectionIntent>,
    pub mailbox_status: Option<MessageStatus>,
    pub forwarded_task: Option<MessageId>,
    pub budgets: Vec<TaskInspectionBudget>,
    pub execution_held: bool,
}
#[derive(Clone, Debug)]
pub struct TaskInspectionSnapshot {
    pub root_task_id: MessageId,
    pub goal_cancellation: Option<goal::GoalCancellationReceipt>,
    pub goal_pause: Option<goal::GoalPauseRecord>,
    pub nodes: Vec<TaskInspectionFacts>,
    pub edges: Vec<TaskInspectionEdge>,
    pub truncations: Vec<TaskInspectionTruncation>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskInspectionBudgetKey {
    pub root_run_id: RunId,
    pub kind: SharedBudgetKind,
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskInspectionBudget {
    pub record: SharedBudgetRecord,
    pub member_count: u64,
    pub remaining_model_calls: Option<u32>,
    pub remaining_tokens: Option<u64>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskInspectionReason {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub related_tasks: Vec<MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_id: Option<WaitId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<i64>,
}
fn reason(code: &'static str, message: impl Into<String>) -> TaskInspectionReason {
    TaskInspectionReason {
        code,
        message: message.into(),
        related_tasks: vec![],
        wait_id: None,
        deadline_at: None,
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskInspectionNode {
    pub task_id: MessageId,
    pub actor_id: AgentId,
    pub actor: Option<TaskInspectionActor>,
    pub run_id: Option<RunId>,
    pub depth: u32,
    pub accepted: bool,
    pub status: String,
    pub execution_held: bool,
    pub goal: Option<String>,
    pub reason: TaskInspectionReason,
    pub wait: Option<HumanWait>,
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub usage: Option<UsageLedger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<task_plan::TaskPlan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_instructions: Option<project_instructions::ProjectInstructions>,
    pub budget_scopes: Vec<TaskInspectionBudgetKey>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskInspectionSummary {
    pub succeeded: u32,
    pub stopped: u32,
    pub pending: u32,
    /// True only when the entire graph was read and all requests are terminal.
    pub settled: bool,
    pub visible_runs: u32,
    /// Sum of unique visible Run ledgers, never a sum of overlapping budgets.
    pub visible_usage: UsageLedger,
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskInspection {
    pub requested_task_id: MessageId,
    pub requested_task_visible: bool,
    pub root_task_id: MessageId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_cancellation: Option<goal::GoalCancellationReceipt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_pause: Option<goal::GoalPauseRecord>,
    pub limits: TaskInspectionOptions,
    pub nodes: Vec<TaskInspectionNode>,
    pub edges: Vec<TaskInspectionEdge>,
    pub budgets: Vec<TaskInspectionBudget>,
    pub summary: TaskInspectionSummary,
    pub truncations: Vec<TaskInspectionTruncation>,
}
pub struct TaskInspectionService {
    reader: Arc<dyn TaskInspectionReadPort>,
}
impl TaskInspectionService {
    pub fn new(reader: Arc<dyn TaskInspectionReadPort>) -> Self {
        Self { reader }
    }
    pub async fn inspect(
        &self,
        id: MessageId,
        options: TaskInspectionOptions,
    ) -> Result<TaskInspection, Error> {
        options.validate()?;
        let snapshot = self.reader.task_inspection_snapshot(id, options).await?;
        assemble(id, options, snapshot)
    }
}
fn label(status: AgentTaskStatus) -> &'static str {
    match status {
        AgentTaskStatus::Queued => "queued",
        AgentTaskStatus::Running => "running",
        AgentTaskStatus::Waiting => "waiting",
        AgentTaskStatus::NeedsRecovery => "needs_recovery",
        AgentTaskStatus::Paused => "paused",
        AgentTaskStatus::Blocked => "blocked",
        AgentTaskStatus::Cancelling => "cancelling",
        AgentTaskStatus::Succeeded => "succeeded",
        AgentTaskStatus::Failed => "failed",
        AgentTaskStatus::Rejected => "rejected",
        AgentTaskStatus::BudgetExhausted => "budget_exhausted",
        AgentTaskStatus::Cancelled => "cancelled",
    }
}
fn explain(
    f: &TaskInspectionFacts,
) -> (String, TaskInspectionReason, Option<Value>, Option<Value>) {
    let run = f.run.as_ref().map(|r| &r.record);
    let reply = f.task.as_ref().and_then(|t| t.reply.as_ref());
    let result = run.and_then(|r| r.result.clone()).or_else(|| {
        reply
            .and_then(|r| r.get("result"))
            .filter(|v| !v.is_null())
            .cloned()
    });
    let error = run
        .and_then(|r| r.error.as_ref())
        .map(|e| serde_json::json!(e))
        .or_else(|| {
            reply
                .and_then(|r| r.get("error"))
                .filter(|v| !v.is_null())
                .cloned()
        });
    if let Some(task) = &f.task {
        let status = if task.status.is_terminal() {
            task.status
        } else if let Some(run) = run {
            AgentTaskStatus::from_run(run)
        } else {
            task.status
        };
        if status.is_terminal() {
            let r = reason(
                label(status),
                error
                    .as_ref()
                    .and_then(|e| e["message"].as_str())
                    .unwrap_or(match status {
                        AgentTaskStatus::Succeeded => "Task completed successfully",
                        AgentTaskStatus::Cancelled => "Task was cancelled",
                        AgentTaskStatus::Rejected => "Task was rejected",
                        AgentTaskStatus::BudgetExhausted => "A configured allowance is exhausted",
                        _ => "Task failed",
                    }),
            );
            return (label(status).into(), r, result, error);
        }
        if let Some(run) = run {
            let r = if status == AgentTaskStatus::Cancelling {
                reason(
                    "cancellation_requested",
                    "Cancellation is waiting for an admitted activity or child to settle",
                )
            } else if status == AgentTaskStatus::NeedsRecovery {
                reason(
                    "worker_recovery",
                    "A saved model failure needs a recovery decision; the parent is waiting",
                )
            } else if status == AgentTaskStatus::Blocked {
                reason(
                    "run_blocked",
                    run.error
                        .as_ref()
                        .map_or("Run is blocked", |e| e.message.as_str()),
                )
            } else if status == AgentTaskStatus::Paused {
                reason(
                    "run_paused",
                    if run.controls.boundary_pause {
                        "Run paused at a workflow boundary"
                    } else {
                        "Run is paused by an execution control"
                    },
                )
            } else if let Some(wait) = &run.wait {
                let mut r = reason("human_input", &wait.prompt);
                r.wait_id = Some(wait.id);
                r
            } else if run.delegations.iter().any(|d| d.outcome.is_none()) {
                let mut r = reason("child_tasks", "Waiting for delegated tasks to finish");
                r.related_tasks = run
                    .delegations
                    .iter()
                    .filter(|d| d.outcome.is_none())
                    .map(|d| d.id)
                    .collect();
                r.deadline_at = run
                    .delegations
                    .iter()
                    .filter(|d| d.outcome.is_none())
                    .filter_map(|d| d.deadline_at)
                    .min();
                r
            } else if let Some(activity) = f.run.as_ref().and_then(|r| r.activity.as_ref()) {
                if activity.status == ActivityStatus::Running {
                    reason(
                        "activity_in_flight",
                        format!(
                            "Activity {} ({} v{}) was admitted and has no saved outcome",
                            activity.id, activity.handler.name, activity.handler.version
                        ),
                    )
                } else {
                    let mut r = reason(
                        if activity.error.is_some() {
                            "activity_retry"
                        } else {
                            "activity_queued"
                        },
                        if activity.error.is_some() {
                            "An activity retry is queued"
                        } else {
                            "An activity is queued for worker admission"
                        },
                    );
                    r.deadline_at = Some(activity.ready_at);
                    r
                }
            } else if let Some((id, due)) = f.run.as_ref().and_then(|r| r.timer) {
                let mut r = reason("durable_timer", format!("Waiting for durable timer {id}"));
                r.deadline_at = Some(due);
                r
            } else {
                reason(
                    "run_pending",
                    "Run is waiting for a saved continuation or worker progress",
                )
            };
            return (label(status).into(), r, result, error);
        }
        if task.cancel_requested {
            return (
                "cancelling".into(),
                reason(
                    "cancellation_requested",
                    "Cancellation awaits the forwarded task or completion receipt",
                ),
                result,
                error,
            );
        }
        if let Some(actor) = &f.actor {
            if actor.status == AgentStatus::Blocked {
                return (
                    "blocked".into(),
                    reason(
                        "actor_blocked",
                        actor.error.as_deref().unwrap_or("Actor mailbox is blocked"),
                    ),
                    result,
                    error,
                );
            }
            if actor.status == AgentStatus::Paused {
                return (
                    "paused".into(),
                    reason(
                        if actor.supervision_pause.is_some() {
                            "actor_supervision"
                        } else {
                            "actor_paused"
                        },
                        actor.error.as_deref().unwrap_or("Actor mailbox is paused"),
                    ),
                    result,
                    error,
                );
            }
        }
        if let Some(forwarded) = f.forwarded_task {
            let mut r = reason(
                "forwarded_task",
                "Waiting for the forwarded task's completion receipt",
            );
            r.related_tasks = vec![forwarded];
            return ("waiting".into(), r, result, error);
        }
        return (
            label(status).into(),
            reason(
                if f.mailbox_status == Some(MessageStatus::Queued) {
                    "mailbox_queued"
                } else {
                    "actor_queue"
                },
                if f.mailbox_status == Some(MessageStatus::Queued) {
                    "Task is awaiting a mailbox turn"
                } else {
                    "Task is waiting for the actor's current task to finish"
                },
            ),
            result,
            error,
        );
    }
    if let Some(reminder) = &f.reminder {
        let mut r = reason(
            match reminder.status {
                ReminderStatus::Scheduled => "reminder_scheduled",
                ReminderStatus::Cancelled => "reminder_cancelled",
                ReminderStatus::Published => "reminder_published",
            },
            match reminder.status {
                ReminderStatus::Scheduled => {
                    "Future task is saved and awaits its deadline and actor admission"
                }
                ReminderStatus::Cancelled => "Future task was cancelled before publication",
                ReminderStatus::Published => "Reminder was published",
            },
        );
        r.deadline_at = Some(reminder.due_at);
        return (
            if reminder.status == ReminderStatus::Cancelled {
                "cancelled"
            } else {
                "scheduled"
            }
            .into(),
            r,
            None,
            None,
        );
    }
    match &f.intent {
        Some(TaskInspectionIntent::Delegation(d)) => {
            if let Some(outcome) = &d.outcome {
                let err = outcome.get("error").filter(|e| !e.is_null()).cloned();
                return (
                    outcome["status"].as_str().unwrap_or("refused").into(),
                    reason(
                        "delegation_refused",
                        err.as_ref()
                            .and_then(|e| e["message"].as_str())
                            .unwrap_or("Delegated request finished without an accepted task"),
                    ),
                    outcome.get("result").filter(|v| !v.is_null()).cloned(),
                    err,
                );
            }
            let mut r = reason(
                "delegation_unpublished",
                "Delegated request is saved and awaits publication",
            );
            r.deadline_at = d.deadline_at;
            ("pending_publication".into(), r, None, None)
        }
        Some(TaskInspectionIntent::FollowUp(f)) => {
            let mut r = reason(
                if f.status == follow_up::FollowUpStatus::Rejected {
                    "follow_up_refused"
                } else {
                    "follow_up_unregistered"
                },
                f.error
                    .as_deref()
                    .unwrap_or("Follow-up intent awaits reminder registration"),
            );
            r.deadline_at = Some(f.due_at);
            (
                if f.error.as_deref() == Some("cancelled") {
                    "cancelled"
                } else if f.status == follow_up::FollowUpStatus::Rejected {
                    "refused"
                } else {
                    "pending_publication"
                }
                .into(),
                r,
                None,
                f.error.as_ref().map(|e| serde_json::json!({"code":e})),
            )
        }
        None => (
            "pending_publication".into(),
            reason("task_unpublished", "Task intent has not been accepted"),
            None,
            None,
        ),
    }
}
pub(super) fn assemble(
    requested_task_id: MessageId,
    limits: TaskInspectionOptions,
    snapshot: TaskInspectionSnapshot,
) -> Result<TaskInspection, Error> {
    let mut nodes = vec![];
    let mut budgets = vec![];
    let mut seen_budgets = HashSet::new();
    let mut seen_runs = HashSet::new();
    let mut summary = TaskInspectionSummary {
        succeeded: 0,
        stopped: 0,
        pending: 0,
        settled: false,
        visible_runs: 0,
        visible_usage: UsageLedger::default(),
    };
    for facts in snapshot.nodes {
        let (status, reason, result, error) = explain(&facts);
        let execution_held = facts.execution_held
            && !matches!(
                status.as_str(),
                "succeeded" | "failed" | "rejected" | "refused" | "cancelled" | "budget_exhausted"
            );
        match status.as_str() {
            "succeeded" => summary.succeeded += 1,
            "failed" | "rejected" | "refused" | "cancelled" | "budget_exhausted" => {
                summary.stopped += 1
            }
            _ => summary.pending += 1,
        }
        let usage = facts.run.as_ref().map(|r| r.record.usage.clone());
        if let Some(run) = &facts.run
            && seen_runs.insert(run.record.id)
        {
            summary.visible_runs += 1;
            let u = &run.record.usage;
            let total = &mut summary.visible_usage;
            total.admitted_model_calls = total
                .admitted_model_calls
                .saturating_add(u.admitted_model_calls);
            total.actual_input_tokens = total
                .actual_input_tokens
                .saturating_add(u.actual_input_tokens);
            total.actual_output_tokens = total
                .actual_output_tokens
                .saturating_add(u.actual_output_tokens);
            total.held_tokens = total.held_tokens.saturating_add(u.held_tokens);
            total.uncertain_calls = total.uncertain_calls.saturating_add(u.uncertain_calls);
        }
        let keys = facts
            .budgets
            .iter()
            .map(|b| TaskInspectionBudgetKey {
                root_run_id: b.record.root_run_id,
                kind: b.record.kind,
            })
            .collect();
        for budget in facts.budgets {
            if seen_budgets.insert((budget.record.root_run_id, budget.record.kind.as_str())) {
                budgets.push(budget)
            }
        }
        nodes.push(TaskInspectionNode {
            task_id: facts.id,
            actor_id: facts.actor_id,
            actor: facts.actor,
            run_id: facts.run.as_ref().map(|r| r.record.id),
            depth: facts.depth,
            accepted: facts.task.is_some(),
            status,
            execution_held,
            goal: facts.goal,
            reason,
            wait: facts.run.as_ref().and_then(|r| r.record.wait.clone()),
            result,
            error,
            usage,
            plan: facts
                .run
                .as_ref()
                .map(|r| task_plan::from_run(&r.record))
                .transpose()?
                .flatten(),
            budget_scopes: keys,
            project_instructions: facts
                .run
                .as_ref()
                .map(|r| project_instructions::from_run(&r.record))
                .transpose()?
                .flatten(),
        });
    }
    summary.settled = summary.pending == 0 && snapshot.truncations.is_empty();
    budgets.sort_by_key(|b| {
        (
            b.record.created_at,
            b.record.root_run_id.to_string(),
            b.record.kind.as_str(),
        )
    });
    let requested_task_visible = nodes.iter().any(|n| n.task_id == requested_task_id);
    Ok(TaskInspection {
        requested_task_id,
        requested_task_visible,
        root_task_id: snapshot.root_task_id,
        goal_cancellation: snapshot.goal_cancellation,
        goal_pause: snapshot.goal_pause,
        limits,
        nodes,
        edges: snapshot.edges,
        budgets,
        summary,
        truncations: snapshot.truncations,
    })
}
