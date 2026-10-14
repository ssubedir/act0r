use super::*;
use crate::{
    application::*,
    domain::{actors::AgentTaskRecord, teams::*},
};

impl SqliteStore {
    pub(super) async fn check_team_task(
        conn: &mut SqliteConnection,
        team: TeamId,
        task: MessageId,
    ) -> Result<(TeamRecord, MessageId), Error> {
        let record = Self::load_team(conn, team).await?;
        if record.status != TeamStatus::Ready {
            return Err(Error::Unavailable("team setup is not ready".into()));
        }
        Self::check_team_actors(conn, &record).await?;
        let root = Self::goal_root(conn, task).await?;
        let payload: String = sqlx::query_scalar("SELECT payload FROM agent_tasks WHERE id=?")
            .bind(root.to_string())
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
        let root_task: AgentTaskRecord = serde_json::from_str(&payload)?;
        if root_task.agent_id != record.planner()?.agent_id {
            return Err(Error::Unavailable(
                "task does not belong to this team's planner goal".into(),
            ));
        }
        Ok((record, root))
    }
}
impl TeamTaskInboxPort for SqliteStore {
    fn team_task_snapshot(
        &self,
        team: TeamId,
        task: MessageId,
        options: TaskInspectionOptions,
    ) -> PortFuture<'_, TeamTaskSnapshot> {
        Box::pin(async move {
            options.validate()?;
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let (record, root) = Self::check_team_task(&mut tx, team, task).await?;
            let snapshot = Self::inspection_snapshot(&mut tx, task, options).await?;
            if snapshot.root_task_id != root {
                return Err(Error::Storage(
                    "task inspection disagrees with authoritative goal ownership".into(),
                ));
            }
            tx.commit().await.map_err(storage)?;
            Ok(TeamTaskSnapshot {
                team: TeamSummary::from(&record),
                members: record
                    .members
                    .iter()
                    .map(|m| TeamTaskMember {
                        name: m.name.clone(),
                        role: m.role,
                        agent_id: m.agent_id,
                    })
                    .collect(),
                task: snapshot,
            })
        })
    }
    fn respond_team_wait(
        &self,
        team: TeamId,
        task: MessageId,
        wait: WaitId,
        decision: TeamWaitDecision,
    ) -> PortFuture<'_, TeamWaitReceipt> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let (_, root) = Self::check_team_task(&mut tx, team, task).await?;
            // This query is independent of display limits and follows only real ownership.
            // Input receipts keep retries discoverable after the wait and goal have completed.
            let matches: Vec<(String, String)> = sqlx::query_as(
                "WITH RECURSIVE owned(id) AS (SELECT ? UNION SELECT o.id FROM agent_goal_ownership o JOIN owned p ON o.parent_id=p.id)
                SELECT a.request_id,a.run_id FROM agent_runs a JOIN owned o ON o.id=a.request_id JOIN runs r ON r.id=a.run_id
                WHERE json_extract(r.payload,'$.wait.id')=? OR EXISTS (SELECT 1 FROM json_each(r.payload,'$.input_receipts') i WHERE json_extract(i.value,'$.wait_id')=?) LIMIT 2"
            ).bind(root.to_string()).bind(wait.to_string()).bind(wait.to_string())
                .fetch_all(&mut *tx).await.map_err(storage)?;
            let [(task_id, run_id)] = matches.as_slice() else {
                return if matches.is_empty() {
                    Err(DomainError::UnknownWait.into())
                } else {
                    Err(Error::Storage(
                        "wait identity is ambiguous within the goal".into(),
                    ))
                };
            };
            let task_id: MessageId = task_id
                .parse()
                .map_err(|_| Error::Storage("invalid task identity".into()))?;
            let run_id: RunId = run_id
                .parse()
                .map_err(|_| Error::Storage("invalid run identity".into()))?;
            let mut run = Self::load(&mut tx, run_id).await?;
            // A cancelled goal may acknowledge a saved receipt, but accepts no new input.
            let has_receipt = run
                .record()
                .input_receipts
                .iter()
                .any(|r| r.wait_id == wait);
            let task_payload: String =
                sqlx::query_scalar("SELECT payload FROM agent_tasks WHERE id=?")
                    .bind(task_id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let target: AgentTaskRecord = serde_json::from_str(&task_payload)?;
            if !has_receipt && (target.cancel_requested || target.status.is_terminal()) {
                return Err(Error::Unavailable(
                    "stopped task cannot accept new human input".into(),
                ));
            }
            if !has_receipt
                && Self::goal_receipt_for_task(&mut tx, task_id)
                    .await?
                    .is_some()
            {
                return Err(Error::Unavailable(
                    "cancelled goal cannot accept new human input".into(),
                ));
            }
            let before = run.clone();
            let replayed = decision(&mut run)?;
            if !replayed {
                self.save_change(&mut tx, &before, run, None).await?;
            }
            tx.commit().await.map_err(storage)?;
            Ok(TeamWaitReceipt {
                team_id: team,
                root_task_id: root,
                task_id,
                run_id,
                wait_id: wait,
                replayed,
            })
        })
    }
}
