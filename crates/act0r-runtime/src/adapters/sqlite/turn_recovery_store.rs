use super::*;
use crate::domain::{actors::*, turn_recovery::*};

impl SqliteStore {
    async fn recovery_preview(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
    ) -> Result<(TurnRecovery, Option<Run>), Error> {
        let context = Self::harness_context(conn, session, turn).await?;
        let sequence: i64 = sqlx::query_scalar(
            "SELECT sequence FROM harness_turns WHERE session_id=? AND task_id=?",
        )
        .bind(session.to_string())
        .bind(turn.to_string())
        .fetch_one(&mut *conn)
        .await
        .map_err(storage)?;
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_turn_recoveries WHERE session_id=? AND task_id=? ORDER BY rowid DESC LIMIT 32")
            .bind(session.to_string()).bind(turn.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let receipts = rows
            .iter()
            .map(|s| serde_json::from_str(s))
            .collect::<Result<Vec<RetryTurnReceipt>, _>>()?;
        let id: Option<String> = sqlx::query_scalar("SELECT run_id FROM agent_tasks WHERE id=?")
            .bind(turn.to_string())
            .fetch_optional(&mut *conn)
            .await
            .map_err(storage)?
            .flatten();
        let run = match id {
            Some(id) => {
                Some(Self::load(conn, id.parse().map_err(|_| DomainError::InvalidState)?).await?)
            }
            None => None,
        };
        let mut view = TurnRecovery {
            session_id: session,
            task_id: turn,
            run_id: run.as_ref().map(|r| r.record().id),
            activity_id: None,
            attempt_id: None,
            history_sequence: run.as_ref().map_or(0, |r| r.record().history_sequence),
            error: run.as_ref().and_then(|r| r.record().error.clone()),
            target: None,
            usage: run.as_ref().map(|r| r.record().usage.clone()),
            retry_allowed: false,
            blocked_reason: None,
            receipts,
        };
        let blocked = |message: String| Error::from(DomainError::SessionConflict(message));
        let reason = async {
            let run = run.as_ref().ok_or_else(|| blocked("turn has no saved Run".into()))?;
            let (activity, attempt) = run.failed_model_checkpoint().map_err(|e| blocked(e.to_string()))?;
            view.activity_id = Some(activity.id);
            view.attempt_id = Some(attempt.id);
            let request: agent::ModelRequest = serde_json::from_value(activity.spec.input.clone()).map_err(|_| blocked("saved model request is invalid".into()))?;
            view.target = Some(request.target);
            run.validate_model_recovery_budget().map_err(|e| blocked(e.to_string()))?;
            // Shared charges include completed children and all previous model attempts.
            let scopes = Self::load_shared_scopes(conn, run.record().id).await?;
            if activity.spec.usage.as_ref().is_some_and(|reservation| scopes.iter().any(|s| s.record().exhaustion.is_some() || !s.record().usage.can_reserve(reservation))) {
                return Err(blocked("saved shared budget cannot admit another model request".into()));
            }
            let metadata = Self::load_session_metadata(conn, session).await?;
            if metadata.archived { return Err(blocked("restore this session before retrying".into())); }
            let cancelled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_goal_cancellations WHERE root_task_id=?)")
                .bind(turn.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            if cancelled { return Err(blocked("cancelled work cannot be retried".into())); }
            if context.paused { return Err(blocked("resume this goal before retrying".into())); }
            let unsettled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_unsettled_turns WHERE task_id=?)")
                .bind(turn.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            if unsettled { return Err(blocked("wait for this turn's owned work and completion receipts to settle".into())); }
            let later: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_turns h JOIN harness_turn_ownership o ON o.root_task_id=h.task_id JOIN agent_tasks t ON t.id=o.id WHERE h.session_id=? AND h.sequence>? AND t.run_id IS NOT NULL)")
                .bind(session.to_string()).bind(sequence).fetch_one(&mut *conn).await.map_err(storage)?;
            if later { return Err(blocked("a later turn has started; send a follow-up instead".into())); }
            let summary_busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_summary_heads WHERE session_id=? AND pending_run_id IS NOT NULL) OR EXISTS(SELECT 1 FROM harness_summary_gates WHERE session_id=?)")
                .bind(session.to_string()).bind(session.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            if summary_busy { return Err(blocked("a summary has already admitted this history; resolve the summary and send a follow-up".into())); }
            let latest: Option<String> = sqlx::query_scalar("SELECT latest_run_id FROM harness_summary_heads WHERE session_id=?")
                .bind(session.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?.flatten();
            if let Some(id) = latest {
                let snapshot = Self::summary_snapshot(conn, id.parse().map_err(|_| DomainError::InvalidState)?).await?;
                if snapshot.through_sequence >= sequence as u64 { return Err(blocked("this turn is already summarized; send a follow-up instead".into())); }
            }
            let task = Self::load_agent_task(conn, turn).await?;
            if task.record().cancel_requested { return Err(blocked("cancelled work cannot be retried".into())); }
            let payload: String = sqlx::query_scalar("SELECT payload FROM agent_mailbox WHERE id=?")
                .bind(turn.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            let entry: MailboxEntry = serde_json::from_str(&payload)?;
            let mut actor = Self::load_actor(conn, task.record().agent_id).await?;
            WorkflowWorkerAgent::resume_failed_turn(&mut actor, &entry.message, run.record(), run.record().updated_at).map_err(|_| blocked("the planner is held or has accepted other work; resolve that work first".into()))?;
            Ok::<(), Error>(())
        }.await;
        view.blocked_reason = match reason {
            Ok(()) => None,
            Err(Error::Domain(DomainError::SessionConflict(reason))) => Some(reason),
            Err(error) => return Err(error),
        };
        view.retry_allowed = view.blocked_reason.is_none();
        Ok((view, run))
    }
    pub(super) async fn read_turn_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<TurnRecovery, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let (view, _) = Self::recovery_preview(&mut tx, session, turn).await?;
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    pub(super) async fn admit_turn_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
        request: RetryTurnRequest,
        now: i64,
        check: RecoveryCheck,
    ) -> Result<RetryTurnReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<String> =
            sqlx::query_scalar("SELECT payload FROM harness_turn_recoveries WHERE request_id=?")
                .bind(request.request_id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        if let Some(saved) = saved {
            let mut receipt: RetryTurnReceipt = serde_json::from_str(&saved)?;
            if receipt.session_id != session
                || receipt.task_id != turn
                || receipt.request != request
            {
                return Err(DomainError::SessionConflict(
                    "recovery decision identity already belongs to another request".into(),
                )
                .into());
            }
            receipt.replayed = true;
            tx.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let (view, run) = Self::recovery_preview(&mut tx, session, turn).await?;
        if let Some(reason) = view.blocked_reason {
            return Err(DomainError::SessionConflict(reason).into());
        }
        let mut run = run.ok_or(DomainError::InvalidState)?;
        let previous_error = run
            .record()
            .error
            .clone()
            .ok_or(DomainError::InvalidState)?;
        let resumed_activity_id = run
            .retry_failed_model(&request, now)
            .map_err(|e| DomainError::SessionConflict(e.to_string()))?;
        check(&run)?;
        let mut task = Self::load_agent_task(&mut tx, turn).await?;
        task.resume_failed_turn(run.record().id, now)?;
        let payload: String = sqlx::query_scalar("SELECT payload FROM agent_mailbox WHERE id=?")
            .bind(turn.to_string())
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        let entry: MailboxEntry = serde_json::from_str(&payload)?;
        let mut actor = Self::load_actor(&mut tx, task.record().agent_id).await?;
        WorkflowWorkerAgent::resume_failed_turn(&mut actor, &entry.message, run.record(), now)?;
        let payload: String =
            sqlx::query_scalar("SELECT payload FROM agent_runs WHERE run_id=? AND notified=1")
                .bind(run.record().id.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
        let mut link: AgentRun = serde_json::from_str(&payload)?;
        if link.request_id != turn || link.agent_id != actor.record().id {
            return Err(DomainError::InvalidState.into());
        }
        link.notified = false;
        link.completion_message_id = MessageId::new();
        self.save(&mut tx, &run, false).await?;
        Self::save_agent_task(&mut tx, &task).await?;
        Self::save_actor(&mut tx, &actor).await?;
        sqlx::query("UPDATE agent_runs SET notified=0,payload=? WHERE run_id=?")
            .bind(serde_json::to_string(&link)?)
            .bind(run.record().id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let receipt = RetryTurnReceipt {
            session_id: session,
            task_id: turn,
            request,
            resumed_activity_id,
            previous_error,
            saved_at: now,
            replayed: false,
        };
        sqlx::query("INSERT INTO harness_turn_recoveries(request_id,session_id,task_id,run_id,activity_id,payload) VALUES(?,?,?,?,?,?)")
            .bind(receipt.request.request_id.to_string()).bind(session.to_string()).bind(turn.to_string()).bind(run.record().id.to_string())
            .bind(receipt.request.activity_id.to_string()).bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
}
