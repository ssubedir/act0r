use crate::{
    application::{TaskInspection, TaskInspectionNode},
    domain::MessageId,
};
use std::collections::{HashMap, HashSet};

fn preview(text: &str, limit: usize) -> String {
    let mut chars = text.chars();
    let mut result: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() {
        result.push('…')
    }
    serde_json::to_string(&result).expect("text is serializable")
}
fn visit(
    id: MessageId,
    depth: usize,
    relation: &str,
    view: &TaskInspection,
    nodes: &HashMap<MessageId, &TaskInspectionNode>,
    seen: &mut HashSet<MessageId>,
    out: &mut String,
) {
    if !seen.insert(id) {
        return;
    }
    let Some(node) = nodes.get(&id) else { return };
    let indent = "  ".repeat(depth);
    out.push_str(&format!(
        "{indent}[{relation}] {} — {}{}\n",
        node.task_id,
        node.status,
        if node.accepted { "" } else { " (saved intent)" }
    ));
    out.push_str(&format!(
        "{indent}  Actor: {}; Run: {}\n",
        node.actor_id,
        node.run_id.map_or_else(|| "none".into(), |r| r.to_string())
    ));
    if node.execution_held {
        out.push_str(&format!("{indent}  Admission: held by goal pause\n"));
    }
    if let Some(actor) = &node.actor
        && actor.status != crate::domain::actors::AgentStatus::Ready
    {
        out.push_str(&format!(
            "{indent}  Mailbox: {}{}\n",
            actor.status.as_str(),
            actor
                .supervision_pause
                .as_ref()
                .map_or_else(String::new, |p| format!(
                    " (supervision pause after task {})",
                    p.task_id
                ))
        ));
    }
    if let Some(goal) = &node.goal {
        out.push_str(&format!("{indent}  Goal: {}\n", preview(goal, 160)))
    }
    out.push_str(&format!(
        "{indent}  Why: {} — {}\n",
        node.reason.code,
        preview(&node.reason.message, 240)
    ));
    if let Some(wait) = &node.wait {
        out.push_str(&format!(
            "{indent}  Human wait: {} — {}\n",
            wait.id,
            preview(&wait.prompt, 240)
        ))
    }
    if let Some(due) = node.reason.deadline_at {
        out.push_str(&format!("{indent}  Saved deadline: {due} (UTC Unix ms)\n"))
    }
    if !node.reason.related_tasks.is_empty() {
        out.push_str(&format!(
            "{indent}  Awaiting tasks: {}\n",
            node.reason
                .related_tasks
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
    if let Some(result) = &node.result {
        out.push_str(&format!(
            "{indent}  Result: {}\n",
            preview(&result.to_string(), 240)
        ))
    }
    if let Some(error) = &node.error {
        out.push_str(&format!(
            "{indent}  Error: {}\n",
            preview(&error.to_string(), 240)
        ))
    }
    if let Some(usage) = &node.usage {
        out.push_str(&format!(
            "{indent}  Usage: {} model calls, {} charged tokens\n",
            usage.admitted_model_calls,
            usage.charged_tokens()
        ))
    }
    for edge in view.edges.iter().filter(|e| e.parent_task_id == id) {
        let relation = match edge.group_id {
            Some(group) => format!("{} group={group}", edge.relation.as_str()),
            None => edge.relation.as_str().into(),
        };
        visit(
            edge.child_task_id,
            depth + 1,
            &relation,
            view,
            nodes,
            seen,
            out,
        );
    }
    for truncated in view.truncations.iter().filter(|t| t.parent_task_id == id) {
        out.push_str(&format!(
            "{indent}  ... {} child requests omitted ({})\n",
            truncated.omitted_children, truncated.reason
        ));
    }
}
pub(super) fn render(view: &TaskInspection) -> String {
    let mut out = format!(
        "Goal: {}\nRequested task: {}\nWork: {} pending, {} succeeded, {} stopped; settled: {}\n",
        view.root_task_id,
        view.requested_task_id,
        view.summary.pending,
        view.summary.succeeded,
        view.summary.stopped,
        view.summary.settled
    );
    if !view.truncations.is_empty() {
        out.push_str(&format!("PARTIAL VIEW: limits are {} nodes and depth {}. Visible usage covers only shown Runs; budget scope totals remain exact.\n",view.limits.max_nodes,view.limits.max_depth));
    }
    if let Some(receipt) = &view.goal_cancellation {
        out.push_str(&format!(
            "Goal cancellation requested at {} (UTC Unix ms) through task {}.\n",
            receipt.requested_at, receipt.requested_task_id
        ));
    }
    if view.goal_cancellation.is_none()
        && let Some(pause) = &view.goal_pause
    {
        let receipt = if pause.paused {
            &pause.pause
        } else {
            &pause.resume
        };
        if let Some(receipt) = receipt {
            out.push_str(&format!(
                "Goal {} at {} (UTC Unix ms) through task {}.\n",
                if pause.paused { "paused" } else { "resumed" },
                receipt.requested_at,
                receipt.requested_task_id
            ));
        }
    }
    if !view.requested_task_visible {
        out.push_str("Requested task lies beyond the visible graph limits.\n")
    }
    let usage = &view.summary.visible_usage;
    out.push_str(&format!("Visible usage: {} unique Runs, {} model calls, {} charged tokens ({} held; {} uncertain calls)\n\n",view.summary.visible_runs,usage.admitted_model_calls,usage.charged_tokens(),usage.held_tokens,usage.uncertain_calls));
    let nodes = view.nodes.iter().map(|n| (n.task_id, n)).collect();
    visit(
        view.root_task_id,
        0,
        "root",
        view,
        &nodes,
        &mut HashSet::new(),
        &mut out,
    );
    if !view.budgets.is_empty() {
        out.push_str("\nShared budget scopes (each total is reported separately):\n")
    }
    for scope in &view.budgets {
        if let Some(budget) = &scope.record.usage.budget {
            out.push_str(&format!("  {} {}: {}/{} calls, {}/{} tokens; {} calls / {} tokens remaining; {} member Runs\n",scope.record.kind.as_str(),scope.record.root_run_id,scope.record.usage.admitted_model_calls,budget.max_model_calls.map_or_else(|| "unlimited".to_string(), |n| n.to_string()),scope.record.usage.charged_tokens(),budget.max_tokens.map_or_else(|| "unlimited".to_string(), |n| n.to_string()),scope.remaining_model_calls.map_or_else(|| "unlimited".to_string(), |n| n.to_string()),scope.remaining_tokens.map_or_else(|| "unlimited".to_string(), |n| n.to_string()),scope.member_count));
        }
        if let Some(exhaustion) = &scope.record.exhaustion {
            out.push_str(&format!(
                "    Exhausted: {} by Run {} at {} (UTC Unix ms)\n",
                exhaustion.reason, exhaustion.run_id, exhaustion.recorded_at
            ));
        }
    }
    out
}
