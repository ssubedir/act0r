use super::*;
use crate::domain::actors::*;
use serde_json::{Value, json};
use std::collections::HashMap;

impl SqliteStore {
    pub(super) async fn load_agent_task(
        conn: &mut SqliteConnection,
        id: MessageId,
    ) -> Result<AgentTask, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_tasks WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        Ok(AgentTask {
            record: serde_json::from_str(&payload.ok_or(Error::AgentTaskNotFound(id))?)?,
        })
    }
    pub(super) async fn save_agent_task(
        conn: &mut SqliteConnection,
        task: &AgentTask,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE agent_tasks SET run_id=?,payload=? WHERE id=?")
            .bind(task.record.run_id.map(|id| id.to_string()))
            .bind(serde_json::to_string(task.record())?)
            .bind(task.record.id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Ok(())
    }
    pub(super) async fn insert_actor_task(
        conn: &mut SqliteConnection,
        message: &AgentMessage,
        parent: Option<MessageId>,
        now: i64,
    ) -> Result<(), Error> {
        Self::insert_actor_task_with_origin(conn, message, parent, None, now).await
    }
    pub(super) async fn insert_actor_task_with_origin(
        conn: &mut SqliteConnection,
        message: &AgentMessage,
        parent: Option<MessageId>,
        origin: Option<follow_up::FollowUpOrigin>,
        now: i64,
    ) -> Result<(), Error> {
        if message.kind != "task" {
            return Ok(());
        }
        let mut task = AgentTask::new(message, parent, now)?;
        if let Some(origin) = &origin {
            origin.context(message.recipient, message.id)?;
        }
        task.record.follow_up_origin = origin;
        sqlx::query("INSERT INTO agent_tasks(id,agent_id,parent_id,run_id,payload) VALUES(?,?,?,NULL,?) ON CONFLICT(id) DO NOTHING")
            .bind(message.id.to_string()).bind(message.recipient.to_string()).bind(parent.map(|id| id.to_string()))
            .bind(serde_json::to_string(task.record())?).execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
    pub(super) async fn attach_agent_task_run(
        conn: &mut SqliteConnection,
        link: &AgentRun,
        now: i64,
    ) -> Result<(), Error> {
        let mut task = match Self::load_agent_task(conn, link.request_id).await {
            Ok(task) => task,
            Err(Error::AgentTaskNotFound(_)) => return Ok(()),
            Err(error) => return Err(error),
        };
        task.attach_run(link.run_id, now)?;
        Self::save_agent_task(conn, &task).await
    }
    pub(super) async fn finish_agent_task(
        conn: &mut SqliteConnection,
        id: MessageId,
        reply: Value,
        now: i64,
    ) -> Result<(), Error> {
        let mut next = Some(id);
        while let Some(id) = next {
            let mut task = match Self::load_agent_task(conn, id).await {
                Ok(task) => task,
                Err(Error::AgentTaskNotFound(_)) => return Ok(()),
                Err(error) => return Err(error),
            };
            task.finish(reply.clone(), now)?;
            next = task.record.parent_id;
            Self::save_agent_task(conn, &task).await?;
        }
        Ok(())
    }
    async fn agent_task_chain(
        conn: &mut SqliteConnection,
        id: MessageId,
    ) -> Result<Vec<AgentTask>, Error> {
        let mut tasks = vec![Self::load_agent_task(conn, id).await?];
        loop {
            let child: Option<String> =
                sqlx::query_scalar("SELECT id FROM agent_tasks WHERE parent_id=?")
                    .bind(tasks.last().unwrap().record.id.to_string())
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(storage)?;
            let Some(child) = child else {
                break;
            };
            let child = child
                .parse()
                .map_err(|_| Error::Storage("invalid agent task identity".into()))?;
            tasks.push(Self::load_agent_task(conn, child).await?);
        }
        Ok(tasks)
    }
    pub(super) async fn agent_task_view(
        conn: &mut SqliteConnection,
        id: MessageId,
    ) -> Result<AgentTaskView, Error> {
        let chain = Self::agent_task_chain(conn, id).await?;
        let task = chain[0].record.clone();
        let mut view = AgentTaskView {
            follow_up: None,
            status: task.status,
            reply: task.reply.clone(),
            task,
            run_id: None,
            execution_agent_id: None,
            wait: None,
            admitted_attempts: 0,
            usage: None,
            delegation: None,
            shared_budgets: vec![],
            task_groups: vec![],
            group_children: vec![],
        };
        let leaf = chain.last().unwrap();
        if let Some(run_id) = leaf.record.run_id {
            let run = Self::load(conn, run_id).await?;
            view.follow_up = run.record().follow_up.clone();
            view.run_id = Some(run_id);
            view.execution_agent_id = Some(leaf.record.agent_id);
            view.wait = run.record().wait.clone();
            view.delegation = run.pending_delegation().cloned();
            view.task_groups = run.record().task_groups.clone();
            view.group_children = run
                .record()
                .delegations
                .iter()
                .filter(|d| d.group_id.is_some())
                .cloned()
                .collect();
            view.admitted_attempts = run.record().allowance.admitted;
            view.usage = Some(run.record().usage.clone());
            view.shared_budgets = Self::shared_budget_views(conn, run_id).await?;
            if !view.task.status.is_terminal() {
                view.status = AgentTaskStatus::from_run(run.record());
                if view.status.is_terminal() {
                    view.reply = Some(task_run_reply(leaf.record.id, run.record()));
                }
            }
        } else if !view.status.is_terminal() {
            let actor = Self::load_actor(conn, leaf.record.agent_id).await?;
            view.status = match actor.record.status {
                AgentStatus::Ready => AgentTaskStatus::Queued,
                AgentStatus::Paused => AgentTaskStatus::Paused,
                AgentStatus::Blocked => AgentTaskStatus::Blocked,
            };
        }
        Ok(view)
    }
    pub(super) async fn load_actor_queue(
        conn: &mut SqliteConnection,
        id: AgentId,
    ) -> Result<AgentQueue, Error> {
        let actor = Self::load_actor(conn, id).await?;
        let mut queue = AgentQueue {
            agent_id: id,
            capacity: actor.record.queue_policy.task_capacity,
            outstanding: 0,
            available: 0,
            mailbox_tasks: 0,
            pending_tasks: 0,
            executing_tasks: 0,
        };
        // Saved terminal receipts are excluded without reading their historical Runs.
        // Open handles use the same authoritative execution view as task inspection.
        let entries: Vec<(String, String)> = sqlx::query_as("SELECT t.id,m.status FROM agent_tasks t JOIN agent_mailbox m ON m.id=t.id WHERE t.agent_id=? AND json_extract(t.payload,'$.status') NOT IN ('succeeded','failed','rejected','budget_exhausted','cancelled')")
            .bind(id.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        for (id, mailbox_status) in entries {
            let id = id
                .parse()
                .map_err(|_| Error::Storage("invalid agent task identity".into()))?;
            let task = Self::agent_task_view(conn, id).await?;
            if task.status.is_terminal() {
                continue;
            }
            queue.outstanding += 1;
            if task.run_id.is_some() {
                queue.executing_tasks += 1;
            } else if mailbox_status == "queued" {
                queue.mailbox_tasks += 1;
            } else {
                queue.pending_tasks += 1;
            }
        }
        queue.available = u64::from(queue.capacity).saturating_sub(queue.outstanding);
        Ok(queue)
    }
    pub(super) async fn check_actor_capacity(
        conn: &mut SqliteConnection,
        id: AgentId,
        requested: u64,
    ) -> Result<(), Error> {
        let queue = Self::load_actor_queue(conn, id).await?;
        if !queue.can_admit(requested) {
            return Err(Error::QueueFull {
                agent_id: id,
                capacity: queue.capacity,
                outstanding: queue.outstanding,
                requested,
            });
        }
        Ok(())
    }

    pub(super) fn task_status(&self, id: MessageId) -> PortFuture<'_, AgentTaskView> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let view = Self::agent_task_view(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(view)
        })
    }
    pub(super) fn list_agent_tasks(&self, id: AgentId) -> PortFuture<'_, Vec<AgentTaskView>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            Self::load_actor(&mut tx, id).await?;
            let ids: Vec<String> = sqlx::query_scalar("SELECT t.id FROM agent_tasks t JOIN agent_mailbox m ON m.id=t.id WHERE t.agent_id=? ORDER BY m.sequence")
                .bind(id.to_string()).fetch_all(&mut *tx).await.map_err(storage)?;
            let mut tasks = vec![];
            for id in ids {
                let id = id
                    .parse()
                    .map_err(|_| Error::Storage("invalid agent task identity".into()))?;
                tasks.push(Self::agent_task_view(&mut tx, id).await?);
            }
            tx.commit().await.map_err(storage)?;
            Ok(tasks)
        })
    }
    pub(super) fn cancel_agent_task(
        &self,
        id: MessageId,
        now: i64,
        mut decision: ActorCancellation,
    ) -> PortFuture<'_, AgentTaskView> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let view = self
                .cancel_agent_task_in(&mut tx, id, now, &mut decision)
                .await?;
            tx.commit().await.map_err(storage)?;
            Ok(view)
        })
    }
    pub(super) async fn cancel_agent_task_in(
        &self,
        conn: &mut SqliteConnection,
        id: MessageId,
        now: i64,
        decision: &mut ActorCancellation,
    ) -> Result<AgentTaskView, Error> {
        let initial = Self::agent_task_view(&mut *conn, id).await?;
        if initial.status.is_terminal() || initial.task.cancel_requested {
            return Ok(initial);
        }
        if Self::goal_task_reconciliation_pending(conn, &initial).await? {
            return Ok(initial);
        }
        let mut tasks = Self::agent_task_chain(&mut *conn, id).await?;
        let leaf = tasks.last().unwrap().record.clone();
        let reply = if let Some(run_id) = leaf.run_id {
            let mut run = Self::load(&mut *conn, run_id).await?;
            let before = run.clone();
            run.cancel(now)?;
            let (run, _) = self.save_change(&mut *conn, &before, run, None).await?;
            AgentTaskStatus::from_run(run.record())
                .is_terminal()
                .then(|| task_run_reply(leaf.id, run.record()))
        } else {
            Some(
                json!({"agent_id":leaf.agent_id,"request_id":leaf.id,"run_id":null,"status":"cancelled","result":null,"error":{"code":"cancelled","message":"task cancelled before execution","retryable":false}}),
            )
        };
        let mut requests: HashMap<AgentId, Vec<AgentMessage>> = HashMap::new();
        let mut root_message = None;
        for task in &mut tasks {
            let payload: String =
                sqlx::query_scalar("SELECT payload FROM agent_mailbox WHERE id=?")
                    .bind(task.record.id.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            let mut entry: MailboxEntry = serde_json::from_str(&payload)?;
            if task.record.id == id {
                root_message = Some(entry.message.clone());
            }
            task.request_cancel(now);
            Self::save_agent_task(&mut *conn, task).await?;
            if entry.status == MessageStatus::Queued {
                entry.status = MessageStatus::Rejected;
                entry.error = Some("task cancelled before execution".into());
                entry.processed_at = Some(now);
                sqlx::query("UPDATE agent_mailbox SET status='rejected',payload=? WHERE id=?")
                    .bind(serde_json::to_string(&entry)?)
                    .bind(entry.message.id.to_string())
                    .execute(&mut *conn)
                    .await
                    .map_err(storage)?;
            } else if entry.status == MessageStatus::Processed && leaf.run_id.is_none() {
                requests
                    .entry(task.record.agent_id)
                    .or_default()
                    .push(entry.message);
            }
        }
        for (agent_id, requests) in requests {
            let mut agent = Self::load_actor(&mut *conn, agent_id).await?;
            let state = decision(&agent, &requests, reply.as_ref())?;
            agent.cancelled_state(state, now)?;
            Self::save_actor(&mut *conn, &agent).await?;
        }
        if let Some(reply) = reply {
            Self::finish_agent_task(&mut *conn, leaf.id, reply.clone(), now).await?;
            if leaf.run_id.is_none() {
                let root = root_message.unwrap();
                if let Some(recipient) = root.sender {
                    Self::insert_message(
                        &mut *conn,
                        AgentMessage {
                            id: MessageId::new(),
                            recipient,
                            sender: Some(root.recipient),
                            correlation_id: Some(root.correlation()),
                            kind: "task_result".into(),
                            payload: reply,
                        },
                        now,
                    )
                    .await?;
                }
            }
        }
        Self::agent_task_view(conn, id).await
    }
}
