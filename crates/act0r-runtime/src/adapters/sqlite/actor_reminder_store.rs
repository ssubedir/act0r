use super::*;
use crate::domain::actors::*;

impl SqliteStore {
    async fn load_reminder(
        conn: &mut SqliteConnection,
        id: MessageId,
    ) -> Result<ActorReminder, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_reminders WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        Ok(serde_json::from_str(
            &payload.ok_or(Error::ActorReminderNotFound(id))?,
        )?)
    }
    async fn save_reminder(
        conn: &mut SqliteConnection,
        reminder: &ActorReminder,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE agent_reminders SET status=?,payload=? WHERE id=?")
            .bind(reminder.status.as_str())
            .bind(serde_json::to_string(reminder)?)
            .bind(reminder.id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Ok(())
    }
    /// Skip full actors without letting one overdue reminder block another actor.
    pub(super) async fn eligible_reminder(
        conn: &mut SqliteConnection,
        latest: i64,
    ) -> Result<Option<ActorReminder>, Error> {
        let actors:Vec<String>=sqlx::query_scalar("SELECT r.agent_id FROM agent_reminders r JOIN agent_actors a ON a.id=r.agent_id WHERE r.status='scheduled' AND r.due_at<=? AND a.status='ready' AND NOT EXISTS(SELECT 1 FROM agent_goal_paused_requests h WHERE h.id=r.id) GROUP BY r.agent_id ORDER BY MIN(r.due_at),MIN(r.rowid)")
            .bind(latest).fetch_all(&mut *conn).await.map_err(storage)?;
        for actor in actors {
            let id: AgentId = actor
                .parse()
                .map_err(|e: uuid::Error| Error::Storage(e.to_string()))?;
            if !Self::load_actor_queue(conn, id).await?.can_admit(1) {
                continue;
            }
            let payload:String=sqlx::query_scalar("SELECT payload FROM agent_reminders r WHERE agent_id=? AND status='scheduled' AND due_at<=? AND NOT EXISTS(SELECT 1 FROM agent_goal_paused_requests h WHERE h.id=r.id) ORDER BY due_at,rowid LIMIT 1")
                .bind(actor).bind(latest).fetch_one(&mut *conn).await.map_err(storage)?;
            return Ok(Some(serde_json::from_str(&payload)?));
        }
        Ok(None)
    }
    pub(super) fn schedule_reminder(
        &self,
        message: AgentMessage,
        deadline: TimerDeadline,
        now: i64,
    ) -> PortFuture<'_, ActorReminder> {
        Box::pin(async move {
            ActorReminder::validate_message(&message)?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            match Self::load_reminder(&mut tx, message.id).await {
                Ok(saved) => {
                    if !saved.matches(&message, &deadline) {
                        return Err(DomainError::ConflictingMessage.into());
                    }
                    tx.commit().await.map_err(storage)?;
                    return Ok(saved);
                }
                Err(Error::ActorReminderNotFound(_)) => {}
                Err(error) => return Err(error),
            }
            let taken:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_mailbox WHERE id=?) OR EXISTS(SELECT 1 FROM agent_runs WHERE json_extract(payload,'$.completion_message_id')=?) OR EXISTS(SELECT 1 FROM agent_delegations WHERE id=?) OR EXISTS(SELECT 1 FROM runs WHERE json_extract(payload,'$.follow_up.reminder_id')=?)")
                .bind(message.id.to_string()).bind(message.id.to_string()).bind(message.id.to_string()).bind(message.id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
            if taken {
                return Err(DomainError::ConflictingMessage.into());
            }
            let mut actor = Self::load_actor(&mut tx, message.recipient).await?;
            if let Some(sender) = message.sender {
                Self::load_actor(&mut tx, sender).await?;
            }
            let reminder = ActorReminder::new(message, deadline, now)?;
            actor.reminder_scheduled(&reminder, now)?;
            sqlx::query("INSERT INTO agent_reminders(id,agent_id,due_at,status,payload) VALUES(?,?,?,'scheduled',?)")
                .bind(reminder.id.to_string()).bind(reminder.message.recipient.to_string()).bind(reminder.due_at).bind(serde_json::to_string(&reminder)?)
                .execute(&mut *tx).await.map_err(storage)?;
            Self::save_actor(&mut tx, &actor).await?;
            tx.commit().await.map_err(storage)?;
            Ok(reminder)
        })
    }
    pub(super) fn reminder(&self, id: MessageId) -> PortFuture<'_, ActorReminder> {
        Box::pin(async move {
            let mut conn = self.pool.acquire().await.map_err(storage)?;
            Self::load_reminder(&mut conn, id).await
        })
    }
    pub(super) fn reminders(&self, id: AgentId) -> PortFuture<'_, Vec<ActorReminder>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            Self::load_actor(&mut tx, id).await?;
            let payloads: Vec<String> = sqlx::query_scalar(
                "SELECT payload FROM agent_reminders WHERE agent_id=? ORDER BY due_at,rowid",
            )
            .bind(id.to_string())
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
            let reminders = payloads
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?;
            tx.commit().await.map_err(storage)?;
            Ok(reminders)
        })
    }
    pub(super) fn cancel_reminder(&self, id: MessageId, now: i64) -> PortFuture<'_, ActorReminder> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let reminder = self.cancel_reminder_in(&mut tx, id, now).await?;
            tx.commit().await.map_err(storage)?;
            Ok(reminder)
        })
    }
    pub(super) async fn cancel_reminder_in(
        &self,
        conn: &mut SqliteConnection,
        id: MessageId,
        now: i64,
    ) -> Result<ActorReminder, Error> {
        let mut reminder = match Self::load_reminder(conn, id).await {
            Ok(reminder) => reminder,
            Err(Error::ActorReminderNotFound(_)) => {
                return self.cancel_pending_follow_up(conn, id, now).await;
            }
            Err(error) => return Err(error),
        };
        if reminder.cancel(now) {
            let mut actor = Self::load_actor(conn, reminder.message.recipient).await?;
            actor.reminder_settled(id, now)?;
            Self::save_actor(conn, &actor).await?;
            Self::save_reminder(conn, &reminder).await?;
        }
        Ok(reminder)
    }
    pub(super) fn publish_reminder(&self, now: i64) -> PortFuture<'_, bool> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let Some(mut reminder) = Self::eligible_reminder(&mut tx, now).await? else {
                tx.commit().await.map_err(storage)?;
                return Ok(false);
            };
            if Self::goal_receipt_for_task(&mut tx, reminder.id)
                .await?
                .is_some()
            {
                self.cancel_reminder_in(&mut tx, reminder.id, now).await?;
                tx.commit().await.map_err(storage)?;
                return Ok(true);
            }
            let mut actor = Self::load_actor(&mut tx, reminder.message.recipient).await?;
            reminder.publish(now)?;
            Self::insert_message_inner(&mut tx, reminder.message.clone(), now, true).await?;
            Self::insert_actor_task_with_origin(
                &mut tx,
                &reminder.message,
                None,
                reminder.origin.clone(),
                now,
            )
            .await?;
            actor.reminder_settled(reminder.id, now)?;
            Self::save_actor(&mut tx, &actor).await?;
            Self::save_reminder(&mut tx, &reminder).await?;
            tx.commit().await.map_err(storage)?;
            Ok(true)
        })
    }
}
