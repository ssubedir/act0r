use super::*;
use crate::domain::{goal::*, worker_pause::*};

impl SqliteStore {
    /// Read scheduling authority rather than inferring holds from physical Run status.
    pub(super) async fn task_execution_held(
        conn: &mut SqliteConnection,
        task: MessageId,
    ) -> Result<bool, Error> {
        sqlx::query_scalar("SELECT (EXISTS(SELECT 1 FROM agent_goal_paused_requests WHERE id=?) OR EXISTS(SELECT 1 FROM harness_initial_dependencies WHERE task_id=? AND status='pending')) AND NOT EXISTS(SELECT 1 FROM agent_goal_cancelled_requests WHERE id=?)")
            .bind(task.to_string()).bind(task.to_string()).bind(task.to_string()).fetch_one(&mut *conn).await.map_err(storage)
    }
    pub(super) async fn worker_pause_preview(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<WorkerPause, Error> {
        let (_, delegation, run) =
            Self::harness_worker_assignment(conn, session, turn, task).await?;
        let current = Self::agent_task_view(conn, task).await?;
        let control = Self::goal_pause_for_root(conn, task)
            .await?
            .unwrap_or_else(|| GoalPause::new(task).record);
        let settled = current.status.is_terminal();
        let blocked_reason = if settled || delegation.outcome.is_some() {
            Some("this assignment has already finished".into())
        } else if current.task.cancel_requested
            || run
                .as_ref()
                .is_some_and(|r| r.record().controls.cancel_requested)
            || delegation.timed_out_at.is_some()
            || Self::goal_receipt_for_task(conn, task).await?.is_some()
        {
            Some("this assignment is stopping".into())
        } else if Self::load_session_metadata(conn, session).await?.archived {
            Some("restore this session before pausing or resuming an assignment".into())
        } else {
            None
        };
        let execution_held = !settled && Self::task_execution_held(conn, task).await?;
        let inherited_held: bool = execution_held && sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM agent_goal_paused_requests WHERE id=? AND root_task_id<>?)")
            .bind(task.to_string()).bind(task.to_string()).bind(task.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        // Include running descendants, without depending on the bounded inspection graph.
        let admitted: i64 = sqlx::query_scalar("WITH RECURSIVE subtree(id) AS (SELECT ? UNION SELECT o.id FROM agent_goal_ownership o JOIN subtree s ON o.parent_id=s.id) SELECT COUNT(*) FROM activities a JOIN agent_runs r ON r.run_id=a.run_id JOIN subtree s ON s.id=r.request_id WHERE a.status='running'")
            .bind(task.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_worker_pauses WHERE session_id=? AND turn_id=? AND task_id=? ORDER BY rowid DESC LIMIT 32")
            .bind(session.to_string()).bind(turn.to_string()).bind(task.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        Ok(WorkerPause {
            session_id: session,
            turn_id: turn,
            task_id: task,
            run_id: current.run_id,
            status: current.status,
            can_pause: blocked_reason.is_none() && !control.paused,
            can_resume: blocked_reason.is_none() && control.paused,
            control,
            inherited_held,
            planner_hold: Self::planner_hold_for_task(conn, task).await?,
            execution_held,
            initial_dependency: Self::initial_prerequisite(conn, task).await?,
            admitted_operations: u64::try_from(admitted).map_err(|_| DomainError::InvalidState)?,
            settled,
            blocked_reason,
            receipts: rows
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?,
        })
    }
    pub(super) async fn read_worker_pause(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<WorkerPause, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let view = Self::worker_pause_preview(&mut tx, session, turn, task).await?;
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    pub(super) async fn admit_assignment_pause(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: AssignmentPauseRequest,
        now: i64,
    ) -> Result<AssignmentPauseReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<String> =
            sqlx::query_scalar("SELECT payload FROM harness_worker_pauses WHERE request_id=?")
                .bind(request.request_id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        if let Some(saved) = saved {
            let mut receipt: AssignmentPauseReceipt = serde_json::from_str(&saved)?;
            if receipt.session_id != session
                || receipt.turn_id != turn
                || receipt.task_id != task
                || receipt.request != request
            {
                return Err(DomainError::SessionConflict(
                    "assignment hold identity belongs to another request".into(),
                )
                .into());
            }
            receipt.replayed = true;
            tx.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let view = Self::worker_pause_preview(&mut tx, session, turn, task).await?;
        if let Some(reason) = view.blocked_reason {
            return Err(DomainError::SessionConflict(reason).into());
        }
        if view.control.revision != request.expected_revision
            || view.control.paused == request.paused
        {
            return Err(DomainError::SessionConflict(
                "assignment hold changed; refresh before deciding".into(),
            )
            .into());
        }
        let (control, _) = Self::save_goal_hold(
            &mut tx,
            task,
            task,
            request.paused,
            Some(GoalPausePrecondition {
                expected_revision: request.expected_revision,
            }),
            now,
        )
        .await?;
        let receipt = AssignmentPauseReceipt {
            session_id: session,
            turn_id: turn,
            task_id: task,
            request,
            run_id: view.run_id,
            status: view.status,
            control,
            saved_at: now,
            replayed: false,
        };
        sqlx::query("INSERT INTO harness_worker_pauses(request_id,session_id,turn_id,task_id,run_id,payload) VALUES(?,?,?,?,?,?)")
            .bind(receipt.request.request_id.to_string()).bind(session.to_string()).bind(turn.to_string()).bind(task.to_string())
            .bind(receipt.run_id.map(|id| id.to_string())).bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
}
