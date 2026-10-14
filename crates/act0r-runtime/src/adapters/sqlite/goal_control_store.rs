use super::*;
use crate::domain::{actors::AgentTaskView, goal::*};

impl SqliteStore {
    pub(super) async fn goal_root(
        conn: &mut SqliteConnection,
        requested: MessageId,
    ) -> Result<MessageId, Error> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_goal_ownership WHERE id=?)")
                .bind(requested.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
        if !exists {
            return Err(Error::AgentTaskNotFound(requested));
        }
        let roots: Vec<String> = sqlx::query_scalar("WITH RECURSIVE ancestors(id,parent_id) AS (SELECT id,parent_id FROM agent_goal_ownership WHERE id=? UNION SELECT o.id,o.parent_id FROM agent_goal_ownership o JOIN ancestors a ON o.id=a.parent_id) SELECT id FROM ancestors WHERE parent_id IS NULL")
            .bind(requested.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        if roots.len() != 1 {
            return Err(Error::Storage(
                "goal ownership is cyclic, missing, or ambiguous".into(),
            ));
        }
        roots[0]
            .parse()
            .map_err(|_| Error::Storage("invalid goal root identity".into()))
    }
    pub(super) async fn goal_pause_for_root(
        conn: &mut SqliteConnection,
        root: MessageId,
    ) -> Result<Option<GoalPauseRecord>, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_goal_pauses WHERE root_task_id=?")
                .bind(root.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        payload.map(|p| Ok(serde_json::from_str(&p)?)).transpose()
    }
    pub(super) async fn goal_run_paused(
        conn: &mut SqliteConnection,
        run: RunId,
    ) -> Result<bool, Error> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_runs a JOIN agent_goal_paused_requests h ON h.id=a.request_id WHERE a.run_id=?)")
            .bind(run.to_string()).fetch_one(&mut *conn).await.map_err(storage)
    }
    pub(super) async fn goal_receipt_for_task(
        conn: &mut SqliteConnection,
        id: MessageId,
    ) -> Result<Option<GoalCancellationReceipt>, Error> {
        let payload:Option<String>=sqlx::query_scalar("SELECT c.payload FROM agent_goal_cancellations c JOIN agent_goal_cancelled_requests s ON s.root_task_id=c.root_task_id WHERE s.id=?")
            .bind(id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        payload.map(|p| Ok(serde_json::from_str(&p)?)).transpose()
    }
    pub(super) async fn goal_receipt_for_run(
        conn: &mut SqliteConnection,
        run: RunId,
    ) -> Result<Option<GoalCancellationReceipt>, Error> {
        let task: Option<String> =
            sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
                .bind(run.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        match task {
            Some(task) => {
                Self::goal_receipt_for_task(
                    conn,
                    task.parse()
                        .map_err(|_| Error::Storage("invalid goal task identity".into()))?,
                )
                .await
            }
            None => Ok(None),
        }
    }
    pub(super) async fn guard_goal_run(
        conn: &mut SqliteConnection,
        run: &mut Run,
        now: i64,
    ) -> Result<bool, Error> {
        if let Some(receipt) = Self::goal_receipt_for_run(conn, run.record().id).await? {
            receipt.cancel_run(run, now)?;
            return Ok(true);
        }
        Ok(false)
    }
    pub(super) async fn goal_task_reconciliation_pending(
        conn: &mut SqliteConnection,
        task: &AgentTaskView,
    ) -> Result<bool, Error> {
        if let Some(run) = task.run_id
            && Self::goal_receipt_for_task(conn, task.task.id)
                .await?
                .is_some()
        {
            return Ok(GoalCancellationReceipt::protects_reconciliation(
                &Self::load(conn, run).await?,
            ));
        }
        Ok(false)
    }
    async fn goal_cleanup_candidates(
        conn: &mut SqliteConnection,
    ) -> Result<Vec<(String, String)>, Error> {
        sqlx::query_as("SELECT 'reminder',m.id FROM agent_reminders m JOIN agent_goal_cancelled_requests s ON s.id=m.id WHERE m.status='scheduled'
            UNION ALL SELECT 'follow_up',json_extract(r.payload,'$.follow_up.reminder_id') FROM runs r JOIN agent_runs a ON a.run_id=r.id JOIN agent_goal_cancelled_requests s ON s.id=a.request_id WHERE r.status='succeeded' AND a.notified=0 AND json_extract(r.payload,'$.follow_up.status')='pending'
            UNION ALL SELECT 'task',t.id FROM agent_tasks t JOIN agent_goal_cancelled_requests s ON s.id=t.id LEFT JOIN runs r ON r.id=t.run_id WHERE json_extract(t.payload,'$.status') NOT IN ('succeeded','failed','rejected','cancelled','budget_exhausted') AND json_extract(t.payload,'$.cancel_requested')=0 AND (r.id IS NULL OR r.status IN ('queued','running','waiting','paused') OR (json_type(r.payload,'$.worker_recovery')='object' AND json_extract(r.payload,'$.worker_recovery.decision') IS NULL))
            ORDER BY 1,2").fetch_all(&mut *conn).await.map_err(storage)
    }
    async fn actionable_goal_task(
        conn: &mut SqliteConnection,
        id: MessageId,
    ) -> Result<bool, Error> {
        let view = Self::agent_task_view(conn, id).await?;
        if view.status.is_terminal() || view.task.cancel_requested {
            return Ok(false);
        }
        if let Some(run) = view.run_id {
            return Ok(!GoalCancellationReceipt::protects_reconciliation(
                &Self::load(conn, run).await?,
            ));
        }
        Ok(true)
    }
    /// Internal assignment controls may hold a subtree; goal controls resolve the root first.
    pub(super) async fn save_goal_hold(
        conn: &mut SqliteConnection,
        root: MessageId,
        requested: MessageId,
        paused: bool,
        precondition: Option<GoalPausePrecondition>,
        now: i64,
    ) -> Result<(GoalPauseRecord, bool), Error> {
        let mut hold = match Self::goal_pause_for_root(conn, root).await? {
            Some(record) => GoalPause { record },
            None => GoalPause::new(root),
        };
        let changed = match precondition {
            Some(precondition) => hold.control_if_revision(requested, paused, precondition, now)?,
            None => hold.control(requested, paused, now)?,
        };
        if changed {
            sqlx::query("INSERT INTO agent_goal_pauses(root_task_id,paused,payload) VALUES(?,?,?) ON CONFLICT(root_task_id) DO UPDATE SET paused=excluded.paused,payload=excluded.payload")
                .bind(root.to_string()).bind(paused).bind(serde_json::to_string(hold.record())?)
                .execute(&mut *conn).await.map_err(storage)?;
        }
        Ok((hold.record, !changed))
    }
    /// Internal callers may root the cancellation at an assignment subtree.
    /// Public whole-goal controls still resolve the original root first.
    pub(super) async fn save_goal_cancellation(
        conn: &mut SqliteConnection,
        root: MessageId,
        requested: MessageId,
        now: i64,
    ) -> Result<GoalCancellationReceipt, Error> {
        let receipt = GoalCancellationReceipt::new(root, requested, now);
        sqlx::query("INSERT INTO agent_goal_cancellations(root_task_id,payload) VALUES(?,?) ON CONFLICT(root_task_id) DO NOTHING")
            .bind(root.to_string()).bind(serde_json::to_string(&receipt)?)
            .execute(&mut *conn).await.map_err(storage)?;
        Self::goal_receipt_for_task(conn, root)
            .await?
            .ok_or(DomainError::InvalidState.into())
    }
    async fn cancellation_view(
        conn: &mut SqliteConnection,
        receipt: GoalCancellationReceipt,
    ) -> Result<GoalCancellationView, Error> {
        let (tasks,blocked):(i64,i64)=sqlx::query_as("SELECT COUNT(*),COALESCE(SUM(CASE WHEN r.status='blocked' OR (r.id IS NULL AND a.status='blocked') THEN 1 ELSE 0 END),0) FROM agent_tasks t JOIN agent_goal_cancelled_requests s ON s.id=t.id JOIN agent_actors a ON a.id=t.agent_id LEFT JOIN runs r ON r.id=t.run_id WHERE s.root_task_id=? AND json_extract(t.payload,'$.status') NOT IN ('succeeded','failed','rejected','cancelled','budget_exhausted') AND (r.id IS NULL OR r.status NOT IN ('succeeded','failed','cancelled','budget_exhausted') OR (json_type(r.payload,'$.worker_recovery')='object' AND json_extract(r.payload,'$.worker_recovery.decision') IS NULL))")
            .bind(receipt.root_task_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        let reminders:i64=sqlx::query_scalar("SELECT COUNT(*) FROM agent_reminders m JOIN agent_goal_cancelled_requests s ON s.id=m.id WHERE s.root_task_id=? AND m.status='scheduled'").bind(receipt.root_task_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        let intents:i64=sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM agent_delegations d JOIN agent_goal_cancelled_requests s ON s.id=d.id WHERE s.root_task_id=? AND d.completed=0 AND d.child_task_id IS NULL) + (SELECT COUNT(*) FROM runs r JOIN agent_runs a ON a.run_id=r.id JOIN agent_goal_cancelled_requests s ON s.id=a.request_id WHERE s.root_task_id=? AND json_extract(r.payload,'$.follow_up.status')='pending')")
            .bind(receipt.root_task_id.to_string()).bind(receipt.root_task_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        Ok(GoalCancellationView {
            receipt,
            pending_tasks: tasks as u64,
            pending_reminders: reminders as u64,
            pending_intents: intents as u64,
            blocked_tasks: blocked as u64,
            settled: tasks + reminders + intents == 0,
        })
    }
}
impl GoalControlPort for SqliteStore {
    fn team_goal_controls(
        &self,
        team: TeamId,
        requested: MessageId,
    ) -> PortFuture<'_, crate::application::TeamGoalControls> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let (_, root) = Self::check_team_task(&mut tx, team, requested).await?;
            let pause = Self::goal_pause_for_root(&mut tx, root)
                .await?
                .unwrap_or_else(|| GoalPause::new(root).record);
            let cancellation = match Self::goal_receipt_for_task(&mut tx, root).await? {
                Some(receipt) => Some(Self::cancellation_view(&mut tx, receipt).await?),
                None => None,
            };
            tx.commit().await.map_err(storage)?;
            Ok(crate::application::TeamGoalControls {
                team_id: team,
                requested_task_id: requested,
                root_task_id: root,
                pause,
                cancellation,
            })
        })
    }
    fn control_team_goal_pause(
        &self,
        team: TeamId,
        requested: MessageId,
        paused: bool,
        precondition: GoalPausePrecondition,
        now: i64,
    ) -> PortFuture<'_, crate::application::TeamGoalPauseReceipt> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let (_, root) = Self::check_team_task(&mut tx, team, requested).await?;
            if Self::goal_receipt_for_task(&mut tx, root).await?.is_some() {
                return Err(DomainError::InvalidState.into());
            }
            let (control, replayed) =
                Self::save_goal_hold(&mut tx, root, requested, paused, Some(precondition), now)
                    .await?;
            tx.commit().await.map_err(storage)?;
            Ok(crate::application::TeamGoalPauseReceipt {
                team_id: team,
                requested_task_id: requested,
                root_task_id: root,
                control,
                replayed,
            })
        })
    }
    fn request_team_goal_cancellation(
        &self,
        team: TeamId,
        requested: MessageId,
        now: i64,
    ) -> PortFuture<'_, GoalCancellationReceipt> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let (_, root) = Self::check_team_task(&mut tx, team, requested).await?;
            let receipt = Self::save_goal_cancellation(&mut tx, root, requested, now).await?;
            tx.commit().await.map_err(storage)?;
            Ok(receipt)
        })
    }
    fn control_goal_pause(
        &self,
        requested: MessageId,
        paused: bool,
        now: i64,
    ) -> PortFuture<'_, GoalPauseRecord> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let root = Self::goal_root(&mut tx, requested).await?;
            if Self::goal_receipt_for_task(&mut tx, root).await?.is_some() {
                return Err(Error::Unavailable(
                    "a cancelled goal cannot be paused or resumed".into(),
                ));
            }
            let (record, _) =
                Self::save_goal_hold(&mut tx, root, requested, paused, None, now).await?;
            tx.commit().await.map_err(storage)?;
            Ok(record)
        })
    }
    fn goal_pause(&self, requested: MessageId) -> PortFuture<'_, GoalPauseRecord> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let root = Self::goal_root(&mut tx, requested).await?;
            let record = Self::goal_pause_for_root(&mut tx, root)
                .await?
                .unwrap_or_else(|| GoalPause::new(root).record);
            tx.commit().await.map_err(storage)?;
            Ok(record)
        })
    }
    fn request_goal_cancellation(
        &self,
        requested: MessageId,
        now: i64,
    ) -> PortFuture<'_, GoalCancellationReceipt> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let root = Self::goal_root(&mut tx, requested).await?;
            let saved = Self::save_goal_cancellation(&mut tx, root, requested, now).await?;
            tx.commit().await.map_err(storage)?;
            Ok(saved)
        })
    }
    fn goal_cancellation(&self, root: MessageId) -> PortFuture<'_, GoalCancellationView> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let receipt = Self::goal_receipt_for_task(&mut tx, root)
                .await?
                .ok_or_else(|| Error::Unavailable("goal has no cancellation request".into()))?;
            let view = Self::cancellation_view(&mut tx, receipt).await?;
            tx.commit().await.map_err(storage)?;
            Ok(view)
        })
    }
    fn advance_goal_cancellation(
        &self,
        now: i64,
        mut decision: ActorCancellation,
    ) -> PortFuture<'_, bool> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            for (kind, id) in Self::goal_cleanup_candidates(&mut tx).await? {
                let id: MessageId = id
                    .parse()
                    .map_err(|_| Error::Storage("invalid goal request identity".into()))?;
                match kind.as_str() {
                    "reminder" => {
                        self.cancel_reminder_in(&mut tx, id, now).await?;
                    }
                    "follow_up" => {
                        self.cancel_pending_follow_up(&mut tx, id, now).await?;
                    }
                    _ => {
                        if !Self::actionable_goal_task(&mut tx, id).await? {
                            continue;
                        }
                        self.cancel_agent_task_in(&mut tx, id, now, &mut decision)
                            .await?;
                    }
                }
                tx.commit().await.map_err(storage)?;
                return Ok(true);
            }
            tx.commit().await.map_err(storage)?;
            Ok(false)
        })
    }
    fn goal_cancellation_work_pending(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            for (kind, id) in Self::goal_cleanup_candidates(&mut tx).await? {
                let id = id
                    .parse()
                    .map_err(|_| Error::Storage("invalid goal request identity".into()))?;
                if kind != "task" || Self::actionable_goal_task(&mut tx, id).await? {
                    tx.commit().await.map_err(storage)?;
                    return Ok(true);
                }
            }
            tx.commit().await.map_err(storage)?;
            Ok(false)
        })
    }
}
