use super::*;
use crate::domain::{tools::*, worker_messages::*};
use serde_json::json;

fn result(receipt: WorkerMessageReceipt) -> Result<NativeToolResult, Error> {
    Ok(match receipt.error {
        Some(error) => NativeToolResult::Rejected(error),
        None => NativeToolResult::Success(json!({"ok":true,"receipt":{
            "message_id":receipt.message_id,"recipient_task_id":receipt.recipient_task_id,
            "recipient_run_id":receipt.recipient_run_id,"sequence":receipt.message.as_ref().map(|m|m.sequence),
            "saved_at":receipt.saved_at
        }})),
    })
}
impl WorkerMessagePort for SqliteStore {
    fn guide_workers(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move { self.execute_guidance(call).await })
    }
    fn worker_question(
        &self,
        call: NativeToolCall,
        reply: bool,
    ) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move { self.execute_worker_question(call, reply).await })
    }
    fn send_worker_message(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let request: SendMessage = serde_json::from_value(call.arguments.clone())?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            // The logical activity is the idempotency key; attempts can change after a crash.
            let saved: Option<String> = sqlx::query_scalar(
                "SELECT payload FROM harness_worker_messages WHERE message_id=?",
            )
            .bind(call.activity_id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
            if let Some(saved) = saved {
                let receipt: WorkerMessageReceipt = serde_json::from_str(&saved)?;
                if receipt.sender_run_id != call.run_id || receipt.request != request {
                    return Err(DomainError::ConflictingMessage.into());
                }
                tx.commit().await.map_err(storage)?;
                return result(receipt);
            }
            let source = Self::load(&mut tx, call.run_id).await?;
            let activity = source
                .activities()
                .iter()
                .find(|a| a.id == call.activity_id && a.status == ActivityStatus::Running)
                .ok_or(DomainError::InvalidState)?;
            let pinned: NativeToolRequest = serde_json::from_value(activity.spec.input.clone())?;
            if pinned.definition.id != DefinitionId::new("send_worker_message", 2)
                || pinned.arguments != call.arguments
                || activity.active_attempt != Some(call.attempt_id)
                || activity.spec.handler != pinned.definition.handler()
                || !source.attempts().iter().any(|a| {
                    a.id == call.attempt_id
                        && a.activity_id == call.activity_id
                        && a.status == AttemptStatus::Running
                })
            {
                return Err(DomainError::InvalidState.into());
            }
            let source_task: String =
                sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
                    .bind(call.run_id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let source_task: MessageId =
                source_task.parse().map_err(|_| DomainError::InvalidState)?;
            let turn = Self::goal_root(&mut tx, source_task).await?;
            let session: Option<String> =
                sqlx::query_scalar("SELECT session_id FROM harness_turns WHERE task_id=?")
                    .bind(turn.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            let Some(session) = session else {
                return Ok(NativeToolResult::Rejected(ToolError::new(
                    "message_scope",
                    "Worker messaging requires a harness coding assignment.",
                )));
            };
            let session: SessionId = session.parse().map_err(|_| DomainError::InvalidState)?;
            let (task, delegation, _) =
                Self::harness_worker_assignment(&mut tx, session, turn, source_task).await?;
            let team_id: String =
                sqlx::query_scalar("SELECT team_id FROM harness_sessions WHERE id=?")
                    .bind(session.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let team = Self::load_team(
                &mut tx,
                team_id.parse().map_err(|_| DomainError::InvalidState)?,
            )
            .await?;
            let sender = team
                .members
                .iter()
                .find(|m| {
                    m.agent_id == task.record().agent_id
                        && matches!(m.name.as_str(), "coder" | "coder2")
                })
                .ok_or(DomainError::InvalidState)?
                .name
                .clone();
            let parent_id: String =
                sqlx::query_scalar("SELECT parent_id FROM agent_goal_ownership WHERE id=?")
                    .bind(source_task.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let parent_id: MessageId = parent_id.parse().map_err(|_| DomainError::InvalidState)?;
            let parent_task = Self::load_agent_task(&mut tx, parent_id).await?;
            let parent = Self::load(
                &mut tx,
                parent_task
                    .record()
                    .run_id
                    .ok_or(DomainError::InvalidState)?,
            )
            .await?;
            let now = crate::adapters::system_clock::SystemClock.now_ms();
            let mut receipt = WorkerMessageReceipt {
                session_id: session,
                turn_id: turn,
                sender_task_id: source_task,
                sender_run_id: call.run_id,
                message_id: call.activity_id,
                request,
                recipient_task_id: None,
                recipient_run_id: None,
                message: None,
                error: None,
                saved_at: now,
            };
            let sent: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM harness_worker_messages WHERE session_id=? AND turn_id=? AND sender_task_id=?",
            )
            .bind(session.to_string())
            .bind(turn.to_string())
            .bind(source_task.to_string())
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
            let target = if receipt.request.recipient == "parent" {
                Some(parent_id)
            } else if let Some(group) = delegation.group_id {
                team.members
                    .iter()
                    .find(|m| {
                        m.name == receipt.request.recipient
                            && matches!(m.name.as_str(), "coder" | "coder2")
                            && m.agent_id != task.record().agent_id
                    })
                    .and_then(|m| {
                        parent
                            .record()
                            .delegations
                            .iter()
                            .find(|d| d.group_id == Some(group) && d.recipient == m.agent_id)
                    })
                    .map(|d| d.id)
            } else {
                None
            };
            let refusal = if let Err(error) = receipt.request.validate() {
                Some(error)
            } else if receipt.request.needs_coordination
                && (receipt.request.recipient != "parent"
                    || parent_id != turn
                    || parent.pending_delegation_by_id(source_task).is_none()
                    || !parent.findings_allowed())
            {
                Some(ToolError::new(
                    "message_coordination",
                    "Coordination findings require an enabled direct parent planner waiting on this exact coding assignment.",
                ))
            } else if sent >= 32 {
                Some(ToolError::new(
                    "message_capacity",
                    "This assignment has already sent 32 notes.",
                ))
            } else if Self::harness_context(&mut tx, session, turn)
                .await?
                .coding
                .is_none()
                || task.record().cancel_requested
                || source.record().controls.cancel_requested
                || Self::goal_receipt_for_task(&mut tx, source_task)
                    .await?
                    .is_some()
                || Self::load_session_metadata(&mut tx, session)
                    .await?
                    .archived
            {
                Some(ToolError::new(
                    "message_scope",
                    "This assignment is stopped or its session is unavailable.",
                ))
            } else if target.is_none() {
                Some(ToolError::new(
                    "message_recipient",
                    "Choose parent or another coder in this same parallel assignment group.",
                ))
            } else {
                None
            };
            receipt.error = refusal;
            if receipt.error.is_none() {
                let target = target.ok_or(DomainError::InvalidState)?;
                let target_task = Self::load_agent_task(&mut tx, target).await?;
                receipt.recipient_task_id = Some(target);
                receipt.recipient_run_id = target_task.record().run_id;
                let closed = target_task.record().cancel_requested
                    || target_task.record().status.is_terminal()
                    || Self::goal_receipt_for_task(&mut tx, target)
                        .await?
                        .is_some()
                    || parent
                        .record()
                        .delegations
                        .iter()
                        .find(|d| d.id == target)
                        .is_some_and(|d| d.outcome.is_some() || d.timed_out_at.is_some());
                if closed {
                    receipt.error = Some(ToolError::new(
                        "message_recipient_closed",
                        "This recipient has finished or is stopping; the note was not delivered.",
                    ));
                } else if let Some(run_id) = target_task.record().run_id {
                    let mut recipient = Self::load(&mut tx, run_id).await?;
                    match recipient.queue_peer_message(
                        PeerMessage {
                            question_id: None,
                            deadline_at: None,
                            message_id: call.activity_id,
                            sender_task_id: source_task,
                            sender,
                            text: receipt.request.text.clone(),
                            needs_coordination: receipt.request.needs_coordination,
                            sequence: 0,
                            saved_at: now,
                            application: None,
                        },
                        now,
                    ) {
                        Ok(entry) => {
                            if receipt.request.needs_coordination {
                                recipient.queue_finding_wake(entry.sequence, now)?;
                            }
                            receipt.message = Some(entry);
                            self.save(&mut tx, &recipient, false).await?;
                        }
                        Err(error) => {
                            receipt.error = Some(ToolError::new(
                                "message_recipient_unavailable",
                                error.to_string(),
                            ))
                        }
                    }
                } else {
                    receipt.error = Some(ToolError::new(
                        "message_recipient_not_started",
                        "This recipient has not started its assignment yet. Continue useful work before sending a new note.",
                    ));
                }
            }
            sqlx::query("INSERT INTO harness_worker_messages(message_id,session_id,turn_id,sender_task_id,sender_run_id,recipient_task_id,recipient_run_id,payload) VALUES(?,?,?,?,?,?,?,?)")
                .bind(call.activity_id.to_string()).bind(session.to_string()).bind(turn.to_string())
                .bind(source_task.to_string()).bind(call.run_id.to_string())
                .bind(receipt.recipient_task_id.map(|id| id.to_string())).bind(receipt.recipient_run_id.map(|id| id.to_string()))
                .bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
            tx.commit().await.map_err(storage)?;
            result(receipt)
        })
    }
}
impl SqliteStore {
    pub(super) async fn worker_message_history(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<WorkerMessages, Error> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM (SELECT payload,json_extract(payload,'$.saved_at') AS saved_at FROM harness_worker_messages WHERE session_id=? AND turn_id=? AND (sender_task_id=? OR recipient_task_id=?) UNION ALL SELECT d.value AS payload,json_extract(d.value,'$.saved_at') AS saved_at FROM harness_planner_guidance g,json_each(g.payload,'$.deliveries') d WHERE g.session_id=? AND g.turn_id=? AND (g.turn_id=? OR json_extract(d.value,'$.recipient_task_id')=?)) ORDER BY saved_at,payload LIMIT 65")
            .bind(session.to_string()).bind(turn.to_string()).bind(task.to_string()).bind(task.to_string())
            .bind(session.to_string()).bind(turn.to_string()).bind(task.to_string()).bind(task.to_string())
            .fetch_all(&mut *conn).await.map_err(storage)?;
        let mut items = Vec::new();
        let mut recipients = std::collections::HashMap::new();
        for row in rows.iter().take(64) {
            let receipt: WorkerMessageReceipt = serde_json::from_str(row)?;
            let (application, recipient_terminal) = if let Some(id) = receipt.recipient_run_id {
                if let std::collections::hash_map::Entry::Vacant(entry) = recipients.entry(id) {
                    entry.insert(Self::load(conn, id).await?);
                }
                let recipient = recipients.get(&id).ok_or(DomainError::InvalidState)?;
                (
                    recipient
                        .record()
                        .peer_messages
                        .iter()
                        .find(|e| e.message_id == receipt.message_id)
                        .and_then(|e| e.application.clone()),
                    !recipient.needs_worker_recovery()
                        && !recipient.record().status.is_unfinished()
                        && recipient.record().status != RunStatus::Blocked,
                )
            } else {
                (None, false)
            };
            items.push(WorkerMessageItem {
                receipt,
                application,
                recipient_terminal,
            });
        }
        Ok(WorkerMessages {
            session_id: session,
            turn_id: turn,
            task_id: task,
            items,
            truncated: rows.len() > 64,
        })
    }
}
