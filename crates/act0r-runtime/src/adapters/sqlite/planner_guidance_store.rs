use super::*;
use crate::domain::{planner_guidance::*, tools::*, worker_messages::*};
use serde_json::json;
fn result(receipt: &GuidanceReceipt) -> NativeToolResult {
    NativeToolResult::Success(json!({"ok":true,"receipt":{
        "activity_id":receipt.activity_id,"saved_at":receipt.saved_at,
        "deliveries":receipt.deliveries.iter().map(|d| json!({
            "recipient":d.request.recipient,"recipient_task_id":d.recipient_task_id,
            "recipient_run_id":d.recipient_run_id,"message_id":d.message_id,
            "sequence":d.message.as_ref().map(|m|m.sequence),"error":d.error
        })).collect::<Vec<_>>()
    }}))
}

impl SqliteStore {
    pub(super) async fn guidance_for_timer(
        conn: &mut SqliteConnection,
        run: &mut Run,
        id: TimerId,
    ) -> Result<(), Error> {
        if !run
            .timers()
            .iter()
            .any(|t| t.id == id && t.payload["key"] == WAKE_KEY)
        {
            return Ok(());
        }
        let root: String = sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
            .bind(run.record().id.to_string())
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
        let root: MessageId = root.parse().map_err(|_| DomainError::InvalidState)?;
        let session: String =
            sqlx::query_scalar("SELECT session_id FROM harness_turns WHERE task_id=?")
                .bind(root.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
        let session: SessionId = session.parse().map_err(|_| DomainError::InvalidState)?;
        Self::harness_context(conn, session, root).await?;
        let team_id: String = sqlx::query_scalar("SELECT team_id FROM harness_sessions WHERE id=?")
            .bind(session.to_string())
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
        let team = Self::load_team(
            conn,
            team_id.parse().map_err(|_| DomainError::InvalidState)?,
        )
        .await?;
        let mut targets = Vec::new();
        for delegation in &run.record().delegations {
            if delegation.outcome.is_some() || delegation.timed_out_at.is_some() {
                continue;
            }
            let task = Self::load_agent_task(conn, delegation.id).await?;
            let name = team.members.iter().find(|m| {
                m.agent_id == task.record().agent_id
                    && matches!(m.name.as_str(), "coder" | "coder2")
            });
            let Some(name) = name else {
                continue;
            };
            let (view, _) =
                Self::worker_steering_preview(conn, session, root, delegation.id).await?;
            if view.can_send
                && let Some(run_id) = view.run_id
            {
                targets.push(GuidanceTarget {
                    name: name.name.clone(),
                    task_id: delegation.id,
                    run_id,
                    pause: if run.worker_pauses_allowed() {
                        let hold =
                            Self::worker_pause_preview(conn, session, root, delegation.id).await?;
                        Some(planner_pause::GuidancePause {
                            planner_paused: hold.planner_hold.as_ref().is_some_and(|h| h.paused),
                            revision: hold.planner_hold.as_ref().map_or(0, |h| h.revision),
                            manual_paused: hold.control.paused,
                            inherited_held: hold.inherited_held,
                        })
                    } else {
                        None
                    },
                });
            }
        }
        let dependencies = Self::ready_dependencies(conn, run, id, &targets).await?;
        run.prepare_guidance_timer(id, targets, dependencies);
        Ok(())
    }
    pub(super) async fn execute_guidance(
        &self,
        call: NativeToolCall,
    ) -> Result<NativeToolResult, Error> {
        let request: GuideWorkers = serde_json::from_value(call.arguments.clone())?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<String> =
            sqlx::query_scalar("SELECT payload FROM harness_planner_guidance WHERE activity_id=?")
                .bind(call.activity_id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        if let Some(saved) = saved {
            let receipt: GuidanceReceipt = serde_json::from_str(&saved)?;
            if receipt.run_id != call.run_id || receipt.request != request {
                return Err(DomainError::ConflictingMessage.into());
            }
            tx.commit().await.map_err(storage)?;
            return Ok(result(&receipt));
        }
        let source = Self::load(&mut tx, call.run_id).await?;
        let activity = source
            .activities()
            .iter()
            .find(|a| a.id == call.activity_id && a.status == ActivityStatus::Running)
            .ok_or(DomainError::InvalidState)?;
        let pinned: NativeToolRequest = serde_json::from_value(activity.spec.input.clone())?;
        if pinned.definition.id != DefinitionId::new("guide_workers", 1)
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
        let guidance = source
            .record()
            .planner_guidance
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        let root: String = sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
            .bind(call.run_id.to_string())
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        let root: MessageId = root.parse().map_err(|_| DomainError::InvalidState)?;
        if Self::goal_root(&mut tx, root).await? != root {
            return Err(DomainError::InvalidState.into());
        }
        let session: String =
            sqlx::query_scalar("SELECT session_id FROM harness_turns WHERE task_id=?")
                .bind(root.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
        let session: SessionId = session.parse().map_err(|_| DomainError::InvalidState)?;
        let now = crate::adapters::system_clock::SystemClock.now_ms();
        let mut receipt = GuidanceReceipt {
            activity_id: call.activity_id,
            run_id: call.run_id,
            session_id: session,
            turn_id: root,
            request: request.clone(),
            deliveries: vec![],
            saved_at: now,
        };
        let validation = request.validate().err();
        for (name, text) in request.selected() {
            let target = guidance.targets.iter().find(|t| t.name == name);
            let mut delivery = WorkerMessageReceipt {
                session_id: session,
                turn_id: root,
                sender_task_id: root,
                sender_run_id: call.run_id,
                message_id: call.activity_id,
                request: SendMessage {
                    recipient: name.into(),
                    text: text.into(),
                    needs_coordination: false,
                },
                recipient_task_id: target.map(|t| t.task_id),
                recipient_run_id: target.map(|t| t.run_id),
                message: None,
                error: validation.clone(),
                saved_at: now,
            };
            if delivery.error.is_none() {
                if source.record().controls.cancel_requested
                    || Self::goal_receipt_for_task(&mut tx, root).await?.is_some()
                {
                    delivery.error = Some(ToolError::new(
                        "guidance_stopped",
                        "The goal is stopping; guidance was not delivered.",
                    ));
                } else if let Some(target) = target {
                    let (view, recipient) =
                        Self::worker_steering_preview(&mut tx, session, root, target.task_id)
                            .await?;
                    if !view.can_send
                        || view.run_id != Some(target.run_id)
                        || source.pending_delegation_by_id(target.task_id).is_none()
                    {
                        delivery.error = Some(ToolError::new(
                            "guidance_recipient_closed",
                            view.blocked_reason
                                .unwrap_or_else(|| "This exact assignment has closed.".into()),
                        ));
                    } else {
                        let mut recipient = recipient.ok_or(DomainError::InvalidState)?;
                        match recipient.queue_peer_message(
                            PeerMessage {
                                question_id: None,
                                deadline_at: None,
                                message_id: call.activity_id,
                                sender_task_id: root,
                                sender: "parent".into(),
                                text: text.into(),
                                needs_coordination: false,
                                sequence: 0,
                                saved_at: now,
                                application: None,
                            },
                            now,
                        ) {
                            Ok(message) => {
                                delivery.message = Some(message);
                                self.save(&mut tx, &recipient, false).await?;
                            }
                            Err(error) => {
                                delivery.error = Some(ToolError::new(
                                    "guidance_recipient_unavailable",
                                    error.to_string(),
                                ))
                            }
                        }
                    }
                } else {
                    delivery.error = Some(ToolError::new(
                        "guidance_recipient",
                        "Choose only a target from the saved guidance step.",
                    ));
                }
            }
            receipt.deliveries.push(delivery);
        }
        sqlx::query("INSERT INTO harness_planner_guidance(activity_id,run_id,session_id,turn_id,payload) VALUES(?,?,?,?,?)")
            .bind(call.activity_id.to_string()).bind(call.run_id.to_string()).bind(session.to_string()).bind(root.to_string())
            .bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(result(&receipt))
    }
}
