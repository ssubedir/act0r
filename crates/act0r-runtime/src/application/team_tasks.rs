//! Goal-scoped operator views and decisions; Runs retain ownership of durable waits.
use super::{ports::*, *};
use crate::domain::{teams::*, *};
use serde::Serialize;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize)]
pub struct TeamTaskMember {
    pub name: String,
    pub role: TeamRole,
    pub agent_id: AgentId,
}
pub struct TeamTaskSnapshot {
    pub team: TeamSummary,
    pub members: Vec<TeamTaskMember>,
    pub task: TaskInspectionSnapshot,
}
#[derive(Debug, Serialize)]
pub struct TeamTaskView {
    pub team: TeamSummary,
    pub members: Vec<TeamTaskMember>,
    pub task: TaskInspection,
}
#[derive(Debug, Serialize)]
pub struct TeamInboxItem {
    pub member: Option<String>,
    pub actor_id: AgentId,
    pub task_id: MessageId,
    pub run_id: RunId,
    pub kind: HumanWaitKind,
    pub wait: HumanWait,
    pub actionable: bool,
    pub unavailable_reason: Option<&'static str>,
    pub execution_held: bool,
}
#[derive(Debug, Serialize)]
pub struct TeamTaskInbox {
    pub team: TeamSummary,
    pub requested_task_id: MessageId,
    pub root_task_id: MessageId,
    pub limits: TaskInspectionOptions,
    pub goal_cancelled: bool,
    pub goal_paused: bool,
    pub items: Vec<TeamInboxItem>,
    pub truncations: Vec<TaskInspectionTruncation>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TeamWaitReceipt {
    pub team_id: TeamId,
    pub root_task_id: MessageId,
    pub task_id: MessageId,
    pub run_id: RunId,
    pub wait_id: WaitId,
    pub replayed: bool,
}
pub struct TeamTaskService {
    engine: Engine,
    port: Arc<dyn TeamTaskInboxPort>,
}
impl TeamTaskService {
    pub fn new(engine: Engine, port: Arc<dyn TeamTaskInboxPort>) -> Self {
        Self { engine, port }
    }
    pub async fn status(
        &self,
        team: TeamId,
        task: MessageId,
        options: TaskInspectionOptions,
    ) -> Result<TeamTaskView, Error> {
        options.validate()?;
        let snapshot = self.port.team_task_snapshot(team, task, options).await?;
        Ok(TeamTaskView {
            team: snapshot.team,
            members: snapshot.members,
            task: super::task_inspection::assemble(task, options, snapshot.task)?,
        })
    }
    pub async fn inbox(
        &self,
        team: TeamId,
        task: MessageId,
        options: TaskInspectionOptions,
    ) -> Result<TeamTaskInbox, Error> {
        let view = self.status(team, task, options).await?;
        let cancelled = view.task.goal_cancellation.is_some();
        let items = view
            .task
            .nodes
            .iter()
            .filter_map(|node| {
                let (Some(wait), Some(run_id)) = (&node.wait, node.run_id) else {
                    return None;
                };
                let kind = HumanWaitKind::for_key(&wait.key);
                let unavailable_reason = if cancelled {
                    Some("goal_cancelled")
                } else if !matches!(node.status.as_str(), "waiting" | "paused") {
                    Some("task_not_waiting")
                } else if kind == HumanWaitKind::Unsupported {
                    Some("unsupported_wait")
                } else {
                    None
                };
                Some(TeamInboxItem {
                    member: view
                        .members
                        .iter()
                        .find(|m| m.agent_id == node.actor_id)
                        .map(|m| m.name.clone()),
                    actor_id: node.actor_id,
                    task_id: node.task_id,
                    run_id,
                    kind,
                    wait: wait.clone(),
                    actionable: unavailable_reason.is_none(),
                    unavailable_reason,
                    execution_held: node.execution_held,
                })
            })
            .collect();
        Ok(TeamTaskInbox {
            team: view.team,
            requested_task_id: task,
            root_task_id: view.task.root_task_id,
            limits: options,
            goal_cancelled: cancelled,
            goal_paused: view.task.goal_pause.as_ref().is_some_and(|p| p.paused),
            items,
            truncations: view.task.truncations,
        })
    }
    pub async fn respond(
        &self,
        team: TeamId,
        task: MessageId,
        wait_id: WaitId,
        response: HumanResponse,
    ) -> Result<TeamWaitReceipt, Error> {
        response.validate()?;
        let definitions = self.engine.definitions.clone();
        let now = self.engine.clock.now_ms();
        self.port
            .respond_team_wait(
                team,
                task,
                wait_id,
                Box::new(move |run| {
                    let workflow = definitions.get(&run.record().workflow);
                    run.respond_to_human(wait_id, response, workflow.map(|w| w.as_ref()), now)
                }),
            )
            .await
    }
}
