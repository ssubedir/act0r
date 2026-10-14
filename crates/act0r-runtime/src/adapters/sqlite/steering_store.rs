use super::*;
use crate::domain::steering::*;

impl SqliteStore {
    pub(super) async fn worker_steering_preview(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
        task_id: MessageId,
    ) -> Result<(TurnSteering, Option<Run>), Error> {
        let context = Self::harness_context(conn, session, turn).await?;
        if context.coding.is_none() {
            return Err(Error::AgentTaskNotFound(task_id));
        }
        let (task, delegation, run) =
            Self::harness_worker_assignment(conn, session, turn, task_id).await?;
        let terminal = task.record().status.is_terminal()
            || delegation.outcome.is_some()
            || delegation.timed_out_at.is_some()
            || run.as_ref().is_some_and(|r| {
                !r.needs_worker_recovery()
                    && !r.record().status.is_unfinished()
                    && r.record().status != RunStatus::Blocked
            });
        let reason = if run.as_ref().is_some_and(Run::needs_worker_recovery) {
            Some(
                "resolve this worker's saved model failure before sending more instructions".into(),
            )
        } else if terminal || task.record().cancel_requested {
            Some("this worker task has finished or is stopping".into())
        } else if Self::load_session_metadata(conn, session).await?.archived {
            Some("restore this archived session before sending instructions".into())
        } else if Self::goal_receipt_for_task(conn, task_id).await?.is_some() {
            Some("this worker task is cancelled".into())
        } else if let Some(run) = &run {
            if let Some((_, _, accepted, pinned)) =
                Self::accepted_task_permission(conn, session, task_id).await?
            {
                run.worker_steering_allowed(&accepted, &pinned)
                    .err()
                    .map(|e| match e {
                        DomainError::SessionConflict(reason) => reason,
                        other => other.to_string(),
                    })
            } else {
                Some("this worker needs an active, verified native coding assignment".into())
            }
        } else {
            Some("this worker has not started; wait for its assignment to launch".into())
        };
        Ok((
            TurnSteering {
                session_id: session,
                task_id,
                run_id: run.as_ref().map(|r| r.record().id),
                can_send: reason.is_none(),
                blocked_reason: reason,
                terminal,
                items: run
                    .as_ref()
                    .map_or_else(Vec::new, |r| r.record().steering.clone()),
            },
            run,
        ))
    }
    pub(super) async fn read_worker_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<TurnSteering, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let (view, _) = Self::worker_steering_preview(&mut tx, session, turn, task).await?;
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    async fn steering_preview(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
    ) -> Result<(TurnSteering, Option<Run>), Error> {
        Self::harness_context(conn, session, turn).await?;
        let task = Self::load_agent_task(conn, turn).await?;
        let run = match task.record().run_id {
            Some(id) => Some(Self::load(conn, id).await?),
            None => None,
        };
        let terminal = run.as_ref().is_some_and(|r| {
            !r.record().status.is_unfinished() && r.record().status != RunStatus::Blocked
        });
        let mut view = TurnSteering {
            session_id: session,
            task_id: turn,
            run_id: run.as_ref().map(|r| r.record().id),
            can_send: false,
            blocked_reason: None,
            terminal,
            items: run
                .as_ref()
                .map_or_else(Vec::new, |r| r.record().steering.clone()),
        };
        let reason = if Self::load_session_metadata(conn, session).await?.archived {
            Some("restore this archived session before sending instructions".into())
        } else if task.record().parent_id.is_some()
            || task.record().cancel_requested
            || task.record().status.is_terminal()
        {
            Some("this turn has finished or is stopping".into())
        } else if Self::goal_receipt_for_task(conn, turn).await?.is_some() {
            Some("this turn is cancelled".into())
        } else if let Some(run) = &run {
            run.steering_allowed().err().map(|e| match e {
                DomainError::SessionConflict(reason) => reason,
                other => other.to_string(),
            })
        } else {
            Some("this turn has not started; wait for its planner to launch".into())
        };
        view.can_send = reason.is_none();
        view.blocked_reason = reason;
        Ok((view, run))
    }
    pub(super) async fn read_turn_steering(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<TurnSteering, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let (view, _) = Self::steering_preview(&mut tx, session, turn).await?;
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    pub(super) async fn admit_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        request: SteeringRequest,
        now: i64,
    ) -> Result<SteeringReceipt, Error> {
        self.save_steering(session, turn, turn, request, now).await
    }
    pub(super) async fn admit_worker_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: SteeringRequest,
        now: i64,
    ) -> Result<SteeringReceipt, Error> {
        if task == turn {
            return Err(Error::AgentTaskNotFound(task));
        }
        self.save_steering(session, turn, task, request, now).await
    }
    async fn save_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: SteeringRequest,
        now: i64,
    ) -> Result<SteeringReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<(String, String)> =
            sqlx::query_as("SELECT payload,task_id FROM harness_turn_steering WHERE message_id=? UNION ALL SELECT payload,turn_id FROM harness_worker_steering WHERE message_id=?")
                .bind(request.message_id.to_string())
                .bind(request.message_id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        if let Some((saved, saved_turn)) = saved {
            let mut receipt: SteeringReceipt = serde_json::from_str(&saved)?;
            if receipt.session_id != session
                || receipt.task_id != task
                || saved_turn != turn.to_string()
                || receipt.entry.request != request
            {
                return Err(DomainError::SessionConflict(
                    "steering message identity belongs to another request".into(),
                )
                .into());
            }
            receipt.replayed = true;
            tx.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let (view, run) = if task == turn {
            Self::steering_preview(&mut tx, session, turn).await?
        } else {
            Self::worker_steering_preview(&mut tx, session, turn, task).await?
        };
        if let Some(reason) = view.blocked_reason {
            return Err(DomainError::SessionConflict(reason).into());
        }
        let mut run = run.ok_or(DomainError::InvalidState)?;
        let entry = run.queue_steering(request, now)?;
        let receipt = SteeringReceipt {
            session_id: session,
            task_id: task,
            entry,
            replayed: false,
        };
        self.save(&mut tx, &run, false).await?;
        if task == turn {
            sqlx::query("INSERT INTO harness_turn_steering(message_id,session_id,task_id,run_id,payload) VALUES(?,?,?,?,?)")
            .bind(receipt.entry.request.message_id.to_string()).bind(session.to_string()).bind(turn.to_string())
            .bind(run.record().id.to_string()).bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
        } else {
            sqlx::query("INSERT INTO harness_worker_steering(message_id,session_id,turn_id,task_id,run_id,payload) VALUES(?,?,?,?,?,?)")
                .bind(receipt.entry.request.message_id.to_string()).bind(session.to_string()).bind(turn.to_string()).bind(task.to_string())
                .bind(run.record().id.to_string()).bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
        }
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
}
