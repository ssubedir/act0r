use super::*;
use crate::domain::{actors::AgentRun, worker_recovery::*};

impl SqliteStore {
    /// This runs before any completion is published or joined, under the same writer lock.
    pub(super) async fn hold_coding_worker_failure(
        &self,
        conn: &mut SqliteConnection,
        link: &AgentRun,
        run: &mut Run,
        now: i64,
    ) -> Result<bool, Error> {
        if run.failed_model_checkpoint().is_err() {
            return Ok(false);
        }
        let session: Option<String> = sqlx::query_scalar("SELECT h.session_id FROM harness_turns h JOIN harness_turn_ownership o ON o.root_task_id=h.task_id WHERE o.id=? AND o.id<>h.task_id")
            .bind(link.request_id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        let Some(session) = session else {
            return Ok(false);
        };
        let session: SessionId = session.parse().map_err(|_| DomainError::InvalidState)?;
        let turn = Self::goal_root(conn, link.request_id).await?;
        if Self::harness_context(conn, session, turn)
            .await?
            .coding
            .is_none()
            || Self::accepted_task_binding(conn, session, link.request_id, true)
                .await?
                .is_none()
        {
            return Ok(false);
        }
        if !run.hold_worker_failure(now)? {
            return Ok(false);
        }
        self.save(conn, run, false).await?;
        Ok(true)
    }
    pub(super) async fn worker_recovery_preview(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<(WorkerRecovery, Option<Run>), Error> {
        // The existing worker projection verifies both ancestry and the saved parent delegation.
        let (_, run) = Self::worker_steering_preview(conn, session, turn, task).await?;
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_worker_recoveries WHERE session_id=? AND turn_id=? AND task_id=? ORDER BY rowid DESC LIMIT 32")
            .bind(session.to_string()).bind(turn.to_string()).bind(task.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let mut view = WorkerRecovery {
            session_id: session,
            turn_id: turn,
            task_id: task,
            run_id: run.as_ref().map(|r| r.record().id),
            activity_id: None,
            attempt_id: None,
            history_sequence: run.as_ref().map_or(0, |r| r.record().history_sequence),
            pending: run.as_ref().is_some_and(Run::needs_worker_recovery),
            error: None,
            target: None,
            usage: run.as_ref().map(|r| r.record().usage.clone()),
            retry_allowed: false,
            accept_allowed: false,
            blocked_reason: None,
            retry_blocked_reason: None,
            receipts: rows
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?,
        };
        if !view.pending {
            view.blocked_reason =
                Some("this assignment has no pending model recovery decision".into());
            return Ok((view, run));
        }
        let saved = run.as_ref().ok_or(DomainError::InvalidState)?;
        let (activity, attempt) = saved.failed_model_checkpoint()?;
        view.activity_id = Some(activity.id);
        view.attempt_id = Some(attempt.id);
        view.error = saved.record().error.clone();
        let model: agent::ModelRequest = serde_json::from_value(activity.spec.input.clone())?;
        view.target = Some(model.target);
        view.blocked_reason = if Self::load_session_metadata(conn, session).await?.archived {
            Some("restore this session before deciding".into())
        } else if Self::goal_receipt_for_task(conn, task).await?.is_some() {
            Some("this assignment is stopping".into())
        } else if Self::task_execution_held(conn, task).await? {
            Some("resume this assignment and its held ancestors before deciding".into())
        } else if Self::accepted_task_binding(conn, session, task, true)
            .await?
            .is_none()
        {
            Some("the accepted coding assignment is no longer active".into())
        } else {
            let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_runs WHERE run_id=? AND request_id=? AND notified=0)")
                .bind(saved.record().id.to_string()).bind(task.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            (!current).then(|| "the worker completion has already been published".into())
        };
        view.accept_allowed = view.blocked_reason.is_none();
        view.retry_blocked_reason = saved
            .validate_model_recovery_budget()
            .err()
            .map(|e| e.to_string());
        let scopes = Self::load_shared_scopes(conn, saved.record().id).await?;
        if activity.spec.usage.as_ref().is_some_and(|reservation| {
            scopes.iter().any(|s| {
                s.record().exhaustion.is_some() || !s.record().usage.can_reserve(reservation)
            })
        }) {
            view.retry_blocked_reason =
                Some("saved shared budget cannot admit another model request".into());
        }
        view.retry_allowed = view.accept_allowed && view.retry_blocked_reason.is_none();
        Ok((view, run))
    }
    pub(super) async fn read_worker_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<WorkerRecovery, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let (view, _) = Self::worker_recovery_preview(&mut tx, session, turn, task).await?;
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    pub(super) async fn admit_worker_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: WorkerRecoveryRequest,
        now: i64,
        check: RecoveryCheck,
    ) -> Result<WorkerRecoveryReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<String> =
            sqlx::query_scalar("SELECT payload FROM harness_worker_recoveries WHERE request_id=?")
                .bind(request.request_id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        if let Some(saved) = saved {
            let mut receipt: WorkerRecoveryReceipt = serde_json::from_str(&saved)?;
            if receipt.session_id != session
                || receipt.turn_id != turn
                || receipt.task_id != task
                || receipt.request != request
            {
                return Err(DomainError::SessionConflict(
                    "recovery decision identity belongs to another request".into(),
                )
                .into());
            }
            receipt.replayed = true;
            tx.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let (view, run) = Self::worker_recovery_preview(&mut tx, session, turn, task).await?;
        let reason = view.blocked_reason.or_else(|| {
            (request.decision == WorkerRecoveryDecision::Retry)
                .then_some(view.retry_blocked_reason)
                .flatten()
        });
        if let Some(reason) = reason {
            return Err(DomainError::SessionConflict(reason).into());
        }
        let mut run = run.ok_or(DomainError::InvalidState)?;
        let previous_error = run
            .record()
            .error
            .clone()
            .ok_or(DomainError::InvalidState)?;
        let resumed_activity_id = run
            .decide_worker_recovery(&request, now)
            .map_err(|e| DomainError::SessionConflict(e.to_string()))?;
        if resumed_activity_id.is_some() {
            check(&run)?;
        }
        // The actor still owns this request, and no completion has been sent to its parent.
        self.save(&mut tx, &run, false).await?;
        let receipt = WorkerRecoveryReceipt {
            session_id: session,
            turn_id: turn,
            task_id: task,
            request,
            resumed_activity_id,
            previous_error,
            saved_at: now,
            replayed: false,
        };
        sqlx::query("INSERT INTO harness_worker_recoveries(request_id,session_id,turn_id,task_id,run_id,payload) VALUES(?,?,?,?,?,?)")
            .bind(receipt.request.request_id.to_string()).bind(session.to_string()).bind(turn.to_string()).bind(task.to_string())
            .bind(run.record().id.to_string()).bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
}
