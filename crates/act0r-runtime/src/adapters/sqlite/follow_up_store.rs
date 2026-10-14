use super::*;
use crate::domain::{actors::*, follow_up::*};

impl SqliteStore {
    pub(super) async fn cancel_pending_follow_up(
        &self,
        conn: &mut SqliteConnection,
        id: MessageId,
        now: i64,
    ) -> Result<ActorReminder, Error> {
        let payload:Option<String>=sqlx::query_scalar("SELECT a.payload FROM agent_runs a JOIN runs r ON r.id=a.run_id WHERE a.notified=0 AND r.status='succeeded' AND json_extract(r.payload,'$.follow_up.reminder_id')=? AND json_extract(r.payload,'$.follow_up.status')='pending'")
            .bind(id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        let link: AgentRun =
            serde_json::from_str(&payload.ok_or(Error::ActorReminderNotFound(id))?)?;
        let mut run = Self::load(conn, link.run_id).await?;
        let context = run
            .record()
            .follow_up_context
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        let intent = run
            .record()
            .follow_up
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        if context.actor_id != link.agent_id
            || context.task_id != link.request_id
            || intent.origin.run_id != link.run_id
            || intent.origin.task_id != link.request_id
        {
            return Err(DomainError::InvalidState.into());
        }
        let mut reminder = ActorReminder::new(
            AgentMessage {
                id,
                recipient: link.agent_id,
                sender: None,
                correlation_id: None,
                kind: "task".into(),
                payload: serde_json::json!({"goal":intent.goal}),
            },
            TimerDeadline::At {
                unix_ms: intent.due_at,
            },
            now,
        )?;
        reminder.origin = Some(intent.origin.clone());
        reminder.cancel(now);
        let mut actor = Self::load_actor(conn, link.agent_id).await?;
        actor.reminder_cancelled_before_registration(&reminder, now)?;
        run.settle_follow_up(Some("cancelled".into()), now)?;
        sqlx::query("INSERT INTO agent_reminders(id,agent_id,due_at,status,payload) VALUES(?,?,?,'cancelled',?)")
            .bind(id.to_string()).bind(link.agent_id.to_string()).bind(reminder.due_at).bind(serde_json::to_string(&reminder)?)
            .execute(&mut *conn).await.map_err(storage)?;
        Self::save_actor(conn, &actor).await?;
        self.save(conn, &run, false).await?;
        Ok(reminder)
    }
    pub(super) async fn follow_up_task_origins(
        conn: &mut SqliteConnection,
        actor: AgentId,
    ) -> Result<ActorTaskOrigins, Error> {
        let payloads:Vec<String>=sqlx::query_scalar("SELECT payload FROM agent_tasks WHERE agent_id=? AND run_id IS NULL AND (json_extract(payload,'$.follow_up_origin') IS NOT NULL OR json_extract(payload,'$.workspace_input') IS NOT NULL) AND json_extract(payload,'$.status')='queued'")
            .bind(actor.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let mut origins = ActorTaskOrigins::default();
        for payload in payloads {
            let task: AgentTaskRecord = serde_json::from_str(&payload)?;
            if let Some(origin) = task.follow_up_origin {
                origins.follow_ups.insert(task.id, origin);
            }
            if let Some(input) = task.workspace_input {
                origins.workspace_inputs.insert(task.id, input);
            }
        }
        Self::task_session_summaries(conn, actor, &mut origins).await?;
        Ok(origins)
    }
    /// The completion receipt and reminder registration share this transaction.
    pub(super) async fn register_run_follow_up(
        &self,
        conn: &mut SqliteConnection,
        run: &mut Run,
        link: &AgentRun,
        now: i64,
    ) -> Result<(), Error> {
        let Some(intent) = run
            .record()
            .follow_up
            .as_ref()
            .filter(|f| f.status == FollowUpStatus::Pending)
            .cloned()
        else {
            return Ok(());
        };
        if Self::goal_receipt_for_task(conn, link.request_id)
            .await?
            .is_some()
        {
            self.cancel_pending_follow_up(conn, intent.reminder_id, now)
                .await?;
            *run = Self::load(conn, link.run_id).await?;
            return Ok(());
        }
        let context = run
            .record()
            .follow_up_context
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        if run.record().status != RunStatus::Succeeded
            || context.actor_id != link.agent_id
            || context.task_id != link.request_id
            || intent.origin.run_id != run.record().id
            || intent.origin.task_id != link.request_id
            || intent.origin.root_task_id != context.root_task_id
            || intent.origin.depth != context.depth + 1
            || intent.origin.policy != context.policy
            || context.remaining() == 0
        {
            return Err(DomainError::InvalidState.into());
        }
        let mut actor = Self::load_actor(conn, link.agent_id).await?;
        if actor.record().definition != RegisteredAgentDefinition.id() {
            return Err(DomainError::InvalidState.into());
        }
        let config: RegisteredAgentConfig = serde_json::from_value(actor.record().config.clone())?;
        if config.registration.definition != planner::PlannerWorkflow::definition_id()
            || serde_json::from_value::<planner::PlannerConfig>(config.input)?
                .follow_up
                .as_ref()
                != Some(&context.policy)
        {
            return Err(DomainError::InvalidState.into());
        }
        let mut reminder = ActorReminder::new(
            AgentMessage {
                id: intent.reminder_id,
                recipient: link.agent_id,
                sender: None,
                correlation_id: None,
                kind: "task".into(),
                payload: serde_json::json!({"goal":intent.goal}),
            },
            TimerDeadline::At {
                unix_ms: intent.due_at,
            },
            now,
        )?;
        reminder.origin = Some(intent.origin);
        match actor.reminder_scheduled(&reminder, now) {
            Ok(()) => {
                sqlx::query("INSERT INTO agent_reminders(id,agent_id,due_at,status,payload) VALUES(?,?,?,'scheduled',?)")
                    .bind(reminder.id.to_string()).bind(link.agent_id.to_string()).bind(reminder.due_at).bind(serde_json::to_string(&reminder)?)
                    .execute(&mut *conn).await.map_err(storage)?;
                Self::save_actor(conn, &actor).await?;
                run.settle_follow_up(None, now)?;
            }
            Err(DomainError::InvalidConfiguration(_)) => {
                run.settle_follow_up(Some("reminder_capacity".into()), now)?
            }
            Err(error) => return Err(error.into()),
        }
        self.save(conn, run, false).await
    }
}
