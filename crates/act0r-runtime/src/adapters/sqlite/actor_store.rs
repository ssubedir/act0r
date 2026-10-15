use super::*;
use crate::domain::actors::*;
use serde_json::json;
use std::collections::HashMap;

impl SqliteStore {
    pub(super) async fn load_actor(
        conn: &mut SqliteConnection,
        id: AgentId,
    ) -> Result<Agent, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_actors WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        Ok(Agent {
            record: serde_json::from_str(&payload.ok_or(Error::AgentNotFound(id))?)?,
        })
    }
    pub(super) async fn save_actor(
        conn: &mut SqliteConnection,
        actor: &Agent,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE agent_actors SET status=?,payload=? WHERE id=?")
            .bind(actor.record.status.as_str())
            .bind(serde_json::to_string(actor.record())?)
            .bind(actor.record.id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Ok(())
    }
    pub(super) async fn insert_message(
        conn: &mut SqliteConnection,
        message: AgentMessage,
        now: i64,
    ) -> Result<MessageId, Error> {
        Self::insert_message_inner(conn, message, now, false).await
    }
    pub(super) async fn insert_message_inner(
        conn: &mut SqliteConnection,
        message: AgentMessage,
        now: i64,
        reminder_publication: bool,
    ) -> Result<MessageId, Error> {
        message.validate()?;
        let existing: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_mailbox WHERE id=?")
                .bind(message.id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        if let Some(existing) = existing {
            let entry: MailboxEntry = serde_json::from_str(&existing)?;
            return if entry.message == message {
                Ok(message.id)
            } else {
                Err(DomainError::ConflictingMessage.into())
            };
        }
        if !reminder_publication {
            let reserved: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_reminders WHERE id=?) OR EXISTS(SELECT 1 FROM runs WHERE json_extract(payload,'$.follow_up.reminder_id')=?)")
                    .bind(message.id.to_string())
                    .bind(message.id.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            if reserved {
                return Err(DomainError::ReservedMessageId.into());
            }
        }
        Self::load_actor(conn, message.recipient).await?;
        if let Some(sender) = message.sender {
            Self::load_actor(conn, sender).await?;
        }
        if message.kind == "task" {
            Self::check_actor_capacity(conn, message.recipient, 1).await?;
        }
        let mut entry = MailboxEntry {
            sequence: 0,
            message,
            status: MessageStatus::Queued,
            error: None,
            processed_revision: None,
            received_at: now,
            processed_at: None,
        };
        let result = sqlx::query("INSERT INTO agent_mailbox(id,recipient,sender,status,payload) VALUES(?,?,?,'queued',?)")
            .bind(entry.message.id.to_string()).bind(entry.message.recipient.to_string()).bind(entry.message.sender.map(|id|id.to_string())).bind(serde_json::to_string(&entry)?).execute(&mut *conn).await.map_err(storage)?;
        entry.sequence = result.last_insert_rowid();
        sqlx::query("UPDATE agent_mailbox SET payload=? WHERE id=?")
            .bind(serde_json::to_string(&entry)?)
            .bind(entry.message.id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Ok(entry.message.id)
    }
    async fn validate_actor_effects(
        conn: &mut SqliteConnection,
        actor: &Agent,
        effects: &ActorEffects,
    ) -> Result<Option<String>, Error> {
        if effects.messages.len() > 16 || effects.runs.len() > 1 {
            return Ok(Some("too many actor effects".into()));
        }
        let mut reservations: HashMap<AgentId, u64> = HashMap::new();
        for message in &effects.messages {
            if message.sender != Some(actor.record.id) || message.kind.starts_with('$') {
                return Ok(Some(
                    "invalid outgoing sender or reserved message kind".into(),
                ));
            }
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_actors WHERE id=?)")
                    .bind(message.recipient.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            if !exists {
                return Ok(Some(format!(
                    "recipient {} does not exist",
                    message.recipient
                )));
            }
            if message.kind == "task" {
                let existing: bool =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_mailbox WHERE id=?)")
                        .bind(message.id.to_string())
                        .fetch_one(&mut *conn)
                        .await
                        .map_err(storage)?;
                if !existing {
                    *reservations.entry(message.recipient).or_default() += 1;
                }
            }
        }
        for (recipient, requested) in reservations {
            Self::check_actor_capacity(conn, recipient, requested).await?;
        }
        for (run, link) in &effects.runs {
            if link.agent_id != actor.record.id || link.run_id != run.record().id || link.notified {
                return Ok(Some("invalid actor run link".into()));
            }
            let available: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM agent_runs WHERE (agent_id=? AND notified=0) OR request_id=?) AND EXISTS(SELECT 1 FROM agent_mailbox WHERE id=? AND recipient=?) AND NOT EXISTS(SELECT 1 FROM agent_tasks WHERE parent_id=?)")
                .bind(link.agent_id.to_string()).bind(link.request_id.to_string()).bind(link.request_id.to_string()).bind(link.agent_id.to_string()).bind(link.request_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            if !available {
                return Ok(Some(
                    "agent already has a run or request does not belong to its mailbox".into(),
                ));
            }
            match Self::load_agent_task(conn, link.request_id).await {
                Ok(task) if task.record.cancel_requested || task.record.status.is_terminal() => {
                    return Ok(Some(
                        "cancelled or completed task cannot start a run".into(),
                    ));
                }
                Ok(_) | Err(Error::AgentTaskNotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }
}
impl ActorStore for SqliteStore {
    fn schedule_actor_reminder(
        &self,
        message: AgentMessage,
        deadline: TimerDeadline,
        now: i64,
    ) -> PortFuture<'_, ActorReminder> {
        self.schedule_reminder(message, deadline, now)
    }
    fn actor_reminder(&self, id: MessageId) -> PortFuture<'_, ActorReminder> {
        self.reminder(id)
    }
    fn actor_reminders(&self, id: AgentId) -> PortFuture<'_, Vec<ActorReminder>> {
        self.reminders(id)
    }
    fn cancel_actor_reminder(&self, id: MessageId, now: i64) -> PortFuture<'_, ActorReminder> {
        self.cancel_reminder(id, now)
    }
    fn publish_due_actor_reminder(&self, now: i64) -> PortFuture<'_, bool> {
        self.publish_reminder(now)
    }
    fn advance_delegation(
        &self,
        now: i64,
        decision: DelegationDecision,
    ) -> PortFuture<'_, DelegationProgress> {
        self.advance_native_delegation(now, decision)
    }
    fn actor_task(&self, id: MessageId) -> PortFuture<'_, AgentTaskView> {
        self.task_status(id)
    }
    fn actor_tasks(&self, id: AgentId) -> PortFuture<'_, Vec<AgentTaskView>> {
        self.list_agent_tasks(id)
    }
    fn cancel_actor_task(
        &self,
        id: MessageId,
        now: i64,
        decision: ActorCancellation,
    ) -> PortFuture<'_, AgentTaskView> {
        self.cancel_agent_task(id, now, decision)
    }
    fn create_actor(&self, agent: Agent) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let existing: Option<String> =
                sqlx::query_scalar("SELECT payload FROM agent_actors WHERE id=?")
                    .bind(agent.record.id.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            if let Some(existing) = existing {
                let existing: AgentRecord = serde_json::from_str(&existing)?;
                if existing.definition != agent.record.definition
                    || existing.config != agent.record.config
                    || existing.queue_policy != agent.record.queue_policy
                    || existing.supervision.as_ref().map(|h| &h.policy)
                        != agent.record.supervision.as_ref().map(|h| &h.policy)
                {
                    return Err(DomainError::InvalidConfiguration(
                        "agent ID already has a different pinned configuration".into(),
                    )
                    .into());
                }
            } else {
                let reserved: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM agent_team_members WHERE agent_id=?)",
                )
                .bind(agent.record.id.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
                if reserved {
                    return Err(Error::Unavailable(
                        "actor identity is reserved for atomic team publication".into(),
                    ));
                }
                sqlx::query("INSERT INTO agent_actors(id,status,payload) VALUES(?,?,?)")
                    .bind(agent.record.id.to_string())
                    .bind(agent.record.status.as_str())
                    .bind(serde_json::to_string(agent.record())?)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
            }
            tx.commit().await.map_err(storage)
        })
    }
    fn actor(&self, id: AgentId) -> PortFuture<'_, Agent> {
        Box::pin(async move {
            let mut conn = self.pool.acquire().await.map_err(storage)?;
            Self::load_actor(&mut conn, id).await
        })
    }
    fn actor_queue(&self, id: AgentId) -> PortFuture<'_, AgentQueue> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let queue = Self::load_actor_queue(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(queue)
        })
    }
    fn mailbox(&self, id: AgentId) -> PortFuture<'_, Vec<MailboxEntry>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            Self::load_actor(&mut tx, id).await?;
            let payloads: Vec<String> = sqlx::query_scalar(
                "SELECT payload FROM agent_mailbox WHERE recipient=? ORDER BY sequence",
            )
            .bind(id.to_string())
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
            let entries = payloads
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?;
            tx.commit().await.map_err(storage)?;
            Ok(entries)
        })
    }
    fn actor_runs(&self, id: AgentId) -> PortFuture<'_, Vec<AgentRun>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            Self::load_actor(&mut tx, id).await?;
            let payloads: Vec<String> = sqlx::query_scalar(
                "SELECT payload FROM agent_runs WHERE agent_id=? ORDER BY rowid",
            )
            .bind(id.to_string())
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
            let runs = payloads
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?;
            tx.commit().await.map_err(storage)?;
            Ok(runs)
        })
    }
    fn enqueue_message(&self, message: AgentMessage, now: i64) -> PortFuture<'_, MessageId> {
        Box::pin(async move {
            message.validate_public()?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let id = Self::insert_message(&mut tx, message, now).await?;
            let payload: String =
                sqlx::query_scalar("SELECT payload FROM agent_mailbox WHERE id=?")
                    .bind(id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let entry: MailboxEntry = serde_json::from_str(&payload)?;
            Self::insert_actor_task(&mut tx, &entry.message, None, now).await?;
            tx.commit().await.map_err(storage)?;
            Ok(id)
        })
    }
    fn process_message(
        &self,
        now: i64,
        decision: ActorDecision,
        summary_factory: Option<SummaryRunFactory>,
    ) -> PortFuture<'_, bool> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let payload: Option<String> = sqlx::query_scalar(
                "SELECT m.payload FROM agent_goal_ready_mailbox m ORDER BY m.sequence LIMIT 1",
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
            let Some(payload) = payload else {
                tx.commit().await.map_err(storage)?;
                return Ok(false);
            };
            let mut entry: MailboxEntry = serde_json::from_str(&payload)?;
            if let Some(factory) = summary_factory
                && self
                    .admit_automatic_summary(&mut tx, entry.message.id, now, factory)
                    .await?
            {
                tx.commit().await.map_err(storage)?;
                return Ok(true);
            }
            let mut agent = Self::load_actor(&mut tx, entry.message.recipient).await?;
            let origins = Self::follow_up_task_origins(&mut tx, agent.record().id).await?;
            let mut admission_rejection = None;
            let outcome = match decision(&agent, &entry.message, &origins) {
                ActorDecisionOutcome::Apply(effects) => {
                    // A completion turn may otherwise launch an already accepted pending task.
                    for (_, link) in &effects.runs {
                        if Self::goal_receipt_for_task(&mut tx, link.request_id)
                            .await?
                            .is_some()
                        {
                            tx.commit().await.map_err(storage)?;
                            return Ok(false);
                        }
                    }
                    let delegated: Vec<_> = effects
                        .messages
                        .iter()
                        .filter(|m| m.kind == "task")
                        .collect();
                    let error = if entry.message.kind == "task"
                        && (delegated.len() > 1
                            || (!delegated.is_empty()
                                && (!effects.runs.is_empty()
                                    || delegated[0].correlation() != entry.message.correlation())))
                    {
                        Some("a tracked task may delegate to one child or start one run".into())
                    } else {
                        match Self::validate_actor_effects(&mut tx, &agent, &effects).await {
                            Ok(error) => error,
                            Err(error @ Error::QueueFull { .. }) => {
                                if let Error::QueueFull {
                                    agent_id,
                                    capacity,
                                    outstanding,
                                    requested,
                                } = &error
                                {
                                    admission_rejection = Some(
                                        json!({"code":"queue_full","agent_id":agent_id,"capacity":capacity,"outstanding":outstanding,"requested":requested}),
                                    );
                                }
                                Some(error.to_string())
                            }
                            Err(error) => return Err(error),
                        }
                    };
                    match error {
                        Some(error) => ActorDecisionOutcome::Reject(error),
                        None => ActorDecisionOutcome::Apply(effects),
                    }
                }
                outcome => outcome,
            };
            match outcome {
                ActorDecisionOutcome::Block(reason) => {
                    agent.block(reason, now)?;
                }
                outcome => {
                    match outcome {
                        ActorDecisionOutcome::Apply(effects) => {
                            agent.processed(
                                Some(effects.state),
                                &entry.message,
                                entry.sequence,
                                now,
                            )?;
                            entry.status = MessageStatus::Processed;
                            for message in effects.messages {
                                Self::insert_message(&mut tx, message.clone(), now).await?;
                                let parent = (entry.message.kind == "task"
                                    && message.kind == "task")
                                    .then_some(entry.message.id);
                                Self::insert_actor_task(&mut tx, &message, parent, now).await?;
                            }
                            for (run, link) in effects.runs {
                                self.save(&mut tx, &run, true).await?;
                                sqlx::query("INSERT INTO agent_runs(run_id,agent_id,request_id,notified,payload) VALUES(?,?,?,0,?)").bind(link.run_id.to_string()).bind(link.agent_id.to_string()).bind(link.request_id.to_string()).bind(serde_json::to_string(&link)?).execute(&mut *tx).await.map_err(storage)?;
                                Self::attach_agent_task_run(&mut tx, &link, now).await?;
                                Self::inherit_shared_budgets(&mut tx, &link).await?;
                            }
                        }
                        ActorDecisionOutcome::Reject(reason) => {
                            agent.processed(None, &entry.message, entry.sequence, now)?;
                            entry.status = MessageStatus::Rejected;
                            entry.error = Some(reason.clone());
                            let mut reply = json!({"agent_id":agent.record.id,"request_id":entry.message.id,"status":"rejected","error":reason});
                            if let Some(admission) = admission_rejection {
                                reply["admission"] = admission;
                            }
                            if entry.message.kind == "task" {
                                Self::finish_agent_task(
                                    &mut tx,
                                    entry.message.id,
                                    reply.clone(),
                                    now,
                                )
                                .await?;
                            }
                            if let Some(recipient) = entry.message.sender {
                                Self::insert_message(
                                    &mut tx,
                                    AgentMessage {
                                        id: MessageId::new(),
                                        recipient,
                                        sender: None,
                                        correlation_id: Some(entry.message.correlation()),
                                        kind: MESSAGE_REJECTED.into(),
                                        payload: reply,
                                    },
                                    now,
                                )
                                .await?;
                            }
                        }
                        ActorDecisionOutcome::Block(_) => unreachable!(),
                    }
                    entry.processed_revision = Some(agent.record.revision);
                    entry.processed_at = Some(now);
                    sqlx::query("UPDATE agent_mailbox SET status=?,payload=? WHERE id=?")
                        .bind(if entry.status == MessageStatus::Processed {
                            "processed"
                        } else {
                            "rejected"
                        })
                        .bind(serde_json::to_string(&entry)?)
                        .bind(entry.message.id.to_string())
                        .execute(&mut *tx)
                        .await
                        .map_err(storage)?;
                }
            }
            Self::save_actor(&mut tx, &agent).await?;
            tx.commit().await.map_err(storage)?;
            Ok(true)
        })
    }
    fn notify_run_completion(&self, now: i64) -> PortFuture<'_, bool> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let payload: Option<String> = sqlx::query_scalar("SELECT a.payload FROM agent_runs a JOIN runs r ON r.id=a.run_id WHERE a.notified=0 AND r.status IN ('succeeded','failed','cancelled','budget_exhausted') AND NOT (COALESCE(json_type(r.payload,'$.worker_recovery'),'')='object' AND json_extract(r.payload,'$.worker_recovery.decision') IS NULL) ORDER BY a.rowid LIMIT 1").fetch_optional(&mut *tx).await.map_err(storage)?;
            let Some(payload) = payload else {
                tx.commit().await.map_err(storage)?;
                return Ok(false);
            };
            let mut link: AgentRun = serde_json::from_str(&payload)?;
            let mut run = Self::load(&mut tx, link.run_id).await?;
            if self
                .hold_coding_worker_failure(&mut tx, &link, &mut run, now)
                .await?
            {
                tx.commit().await.map_err(storage)?;
                return Ok(true);
            }
            self.register_run_follow_up(&mut tx, &mut run, &link, now)
                .await?;
            Self::finish_agent_task(
                &mut tx,
                link.request_id,
                task_run_reply(link.request_id, run.record()),
                now,
            )
            .await?;
            Self::insert_message(
                &mut tx,
                AgentMessage {
                    id: link.completion_message_id,
                    recipient: link.agent_id,
                    sender: None,
                    correlation_id: Some(link.correlation_id),
                    kind: RUN_COMPLETED.into(),
                    payload: task_run_reply(link.request_id, run.record()),
                },
                now,
            )
            .await?;
            link.notified = true;
            sqlx::query("UPDATE agent_runs SET notified=1,payload=? WHERE run_id=?")
                .bind(serde_json::to_string(&link)?)
                .bind(link.run_id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            tx.commit().await.map_err(storage)?;
            Ok(true)
        })
    }
    fn actor_work_pending(&self) -> PortFuture<'_, bool> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let mail: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_goal_ready_mailbox) OR EXISTS(SELECT 1 FROM agent_runs a JOIN runs r ON r.id=a.run_id WHERE a.notified=0 AND r.status IN ('succeeded','failed','cancelled','budget_exhausted') AND NOT (COALESCE(json_type(r.payload,'$.worker_recovery'),'')='object' AND json_extract(r.payload,'$.worker_recovery.decision') IS NULL))").fetch_one(&mut *tx).await.map_err(storage)?;
            // Future reminders keep --until-idle alive only when their actor can accept work.
            let ready = mail
                || Self::native_delegation_ready(&mut tx).await?
                || Self::eligible_reminder(&mut tx, i64::MAX).await?.is_some()
                || sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM timers t JOIN runs r ON r.id=t.run_id WHERE t.status='scheduled' AND r.status='waiting' AND (json_extract(r.payload,'$.worker_reply_wait.timer_id')=t.id OR json_extract(t.payload,'$.payload.key')='planner_worker_question') AND NOT EXISTS(SELECT 1 FROM agent_runs l JOIN agent_goal_paused_requests h ON h.id=l.request_id WHERE l.run_id=r.id))").fetch_one(&mut *tx).await.map_err(storage)?;
            tx.commit().await.map_err(storage)?;
            Ok(ready)
        })
    }
    fn control_actor(&self, id: AgentId, paused: bool, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut agent = Self::load_actor(&mut tx, id).await?;
            agent.control(paused, now)?;
            Self::save_actor(&mut tx, &agent).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn recover_actor_supervision(&self, id: AgentId, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut agent = Self::load_actor(&mut tx, id).await?;
            if agent.recover_supervision(now)? {
                Self::save_actor(&mut tx, &agent).await?;
                Self::insert_message(&mut tx, AgentMessage {
                    id: MessageId::new(), recipient: id, sender: None, correlation_id: None,
                    kind: SUPERVISION_RECOVERED.into(), payload: json!({"recovery":agent.record().supervision.as_ref().unwrap().recoveries}),
                }, now).await?;
            }
            tx.commit().await.map_err(storage)
        })
    }
    fn restore_actor_implementation(
        &self,
        id: AgentId,
        expected_revision: u64,
        now: i64,
    ) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut agent = Self::load_actor(&mut tx, id).await?;
            if agent.record.status == AgentStatus::Blocked
                && agent.record.revision == expected_revision
                && agent
                    .record
                    .supervision
                    .as_ref()
                    .is_none_or(|supervision| supervision.pause.is_none())
            {
                agent.control(false, now)?;
                Self::save_actor(&mut tx, &agent).await?;
            }
            tx.commit().await.map_err(storage)
        })
    }
    fn clear_actor_memory(
        &self,
        id: AgentId,
        now: i64,
        decision: ActorMemoryDecision,
    ) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut agent = Self::load_actor(&mut tx, id).await?;
            let state = decision(&agent)?;
            agent.memory_cleared(state, now)?;
            Self::save_actor(&mut tx, &agent).await?;
            tx.commit().await.map_err(storage)
        })
    }
}
