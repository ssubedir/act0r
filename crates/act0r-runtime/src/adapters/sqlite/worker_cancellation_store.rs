use super::*;
use crate::domain::{actors::AgentTask, delegation::TaskDelegation, worker_cancellation::*};

impl SqliteStore {
    /// Authoritative ancestry plus the saved delegation bind a physical assignment.
    pub(super) async fn harness_worker_assignment(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
        task_id: MessageId,
    ) -> Result<(AgentTask, TaskDelegation, Option<Run>), Error> {
        Self::harness_context(conn, session, turn).await?;
        if task_id == turn || Self::goal_root(conn, task_id).await? != turn {
            return Err(Error::AgentTaskNotFound(task_id));
        }
        let task = Self::load_agent_task(conn, task_id).await?;
        let parent_id: Option<String> =
            sqlx::query_scalar("SELECT parent_id FROM agent_goal_ownership WHERE id=?")
                .bind(task_id.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
        let parent_id: MessageId = parent_id
            .ok_or(Error::AgentTaskNotFound(task_id))?
            .parse()
            .map_err(|_| DomainError::InvalidState)?;
        let parent_task = Self::load_agent_task(conn, parent_id).await?;
        let parent = Self::load(
            conn,
            parent_task
                .record()
                .run_id
                .ok_or(DomainError::InvalidState)?,
        )
        .await?;
        let delegation = parent
            .record()
            .delegations
            .iter()
            .find(|d| d.id == task_id && d.recipient == task.record().agent_id)
            .ok_or(Error::AgentTaskNotFound(task_id))?
            .clone();
        let run = match task.record().run_id {
            Some(id) => Some(Self::load(conn, id).await?),
            None => None,
        };
        Ok((task, delegation, run))
    }
    pub(super) async fn worker_cancellation_preview(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<WorkerCancellation, Error> {
        let (_, delegation, run) =
            Self::harness_worker_assignment(conn, session, turn, task).await?;
        let current = Self::agent_task_view(conn, task).await?;
        let requested = current.task.cancel_requested
            || run
                .as_ref()
                .is_some_and(|r| r.record().controls.cancel_requested)
            || Self::goal_receipt_for_task(conn, task).await?.is_some();
        let settled = current.status.is_terminal();
        let blocked_reason = if settled || delegation.outcome.is_some() {
            Some("this assignment has already finished".into())
        } else if requested
            || delegation.timed_out_at.is_some()
            || Self::goal_receipt_for_task(conn, task).await?.is_some()
        {
            Some("this assignment is already stopping".into())
        } else if Self::load_session_metadata(conn, session).await?.archived {
            Some("restore this session before stopping an assignment".into())
        } else {
            None
        };
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_worker_cancellations WHERE session_id=? AND turn_id=? AND task_id=? ORDER BY rowid DESC LIMIT 32")
            .bind(session.to_string()).bind(turn.to_string()).bind(task.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        Ok(WorkerCancellation {
            session_id: session,
            turn_id: turn,
            task_id: task,
            run_id: current.run_id,
            status: current.status,
            requested,
            settled,
            can_stop: blocked_reason.is_none(),
            blocked_reason,
            receipts: rows
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?,
        })
    }
    pub(super) async fn read_worker_cancellation(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<WorkerCancellation, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let view = Self::worker_cancellation_preview(&mut tx, session, turn, task).await?;
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    pub(super) async fn admit_assignment_stop(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: StopAssignmentRequest,
        now: i64,
        mut decision: ActorCancellation,
    ) -> Result<StopAssignmentReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<String> = sqlx::query_scalar(
            "SELECT payload FROM harness_worker_cancellations WHERE request_id=?",
        )
        .bind(request.request_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?;
        if let Some(saved) = saved {
            let mut receipt: StopAssignmentReceipt = serde_json::from_str(&saved)?;
            if receipt.session_id != session
                || receipt.turn_id != turn
                || receipt.task_id != task
                || receipt.request != request
            {
                return Err(DomainError::SessionConflict(
                    "stop request identity belongs to another assignment".into(),
                )
                .into());
            }
            receipt.replayed = true;
            tx.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let view = Self::worker_cancellation_preview(&mut tx, session, turn, task).await?;
        if let Some(reason) = view.blocked_reason {
            return Err(DomainError::SessionConflict(reason).into());
        }
        // Root the existing admission hold at this assignment, never at the parent goal.
        // It also protects unsafe work until it settles or is explicitly reconciled.
        Self::save_goal_cancellation(&mut tx, task, task, now).await?;
        let cancelled = self
            .cancel_agent_task_in(&mut tx, task, now, &mut decision)
            .await?;
        let receipt = StopAssignmentReceipt {
            session_id: session,
            turn_id: turn,
            task_id: task,
            request,
            run_id: cancelled.run_id,
            status: cancelled.status,
            saved_at: now,
            replayed: false,
        };
        Self::save_assignment_stop_receipt(&mut tx, &receipt).await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    pub(super) async fn save_assignment_stop_receipt(
        conn: &mut SqliteConnection,
        receipt: &StopAssignmentReceipt,
    ) -> Result<(), Error> {
        sqlx::query("INSERT INTO harness_worker_cancellations(request_id,session_id,turn_id,task_id,run_id,payload) VALUES(?,?,?,?,?,?)")
            .bind(receipt.request.request_id.to_string()).bind(receipt.session_id.to_string()).bind(receipt.turn_id.to_string()).bind(receipt.task_id.to_string())
            .bind(receipt.run_id.map(|id| id.to_string())).bind(serde_json::to_string(receipt)?).execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
}
