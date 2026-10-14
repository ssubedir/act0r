use super::*;
use crate::domain::{planner_pause::*, tools::*};
use serde_json::json;

fn refused(code: &str, message: &str) -> serde_json::Value {
    json!({"ok":false,"error":ToolError::new(code,message)})
}
fn output(receipt: PlannerPauseReceipt) -> Result<NativeToolResult, Error> {
    if receipt.result["ok"] == false {
        Ok(NativeToolResult::Rejected(serde_json::from_value(
            receipt.result["error"].clone(),
        )?))
    } else {
        Ok(NativeToolResult::Success(receipt.result))
    }
}
impl SqliteStore {
    pub(super) async fn planner_hold_for_task(
        conn: &mut SqliteConnection,
        task: MessageId,
    ) -> Result<Option<PlannerHoldRecord>, Error> {
        let saved: Option<String> =
            sqlx::query_scalar("SELECT payload FROM harness_planner_holds WHERE task_id=?")
                .bind(task.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        saved.map(|s| Ok(serde_json::from_str(&s)?)).transpose()
    }
}
impl PlannerPausePort for SqliteStore {
    fn control_worker_pause(
        &self,
        call: NativeToolCall,
        paused: bool,
    ) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let request: PauseWorker = serde_json::from_value(call.arguments.clone())?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let saved: Option<String> = sqlx::query_scalar(
                "SELECT payload FROM harness_planner_pauses WHERE activity_id=?",
            )
            .bind(call.activity_id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
            if let Some(saved) = saved {
                let receipt: PlannerPauseReceipt = serde_json::from_str(&saved)?;
                if receipt.run_id != call.run_id
                    || receipt.request != request
                    || receipt.paused != paused
                {
                    return Err(DomainError::ConflictingMessage.into());
                }
                tx.commit().await.map_err(storage)?;
                return output(receipt);
            }
            let mut source = Self::load(&mut tx, call.run_id).await?;
            let activity = source
                .activities()
                .iter()
                .find(|a| a.id == call.activity_id && a.status == ActivityStatus::Running)
                .ok_or(DomainError::InvalidState)?;
            let pinned: NativeToolRequest = serde_json::from_value(activity.spec.input.clone())?;
            let state = if source.record().state["config"].is_object() {
                &source.record().state
            } else {
                &source.record().state["state"]
            };
            if !planner::worker_pause_admitted(state, &pinned.definition, &call.arguments)
                || pinned.definition.id.name
                    != if request.dependency.is_some() {
                        "wait_for_worker"
                    } else if paused {
                        "pause_worker"
                    } else {
                        "resume_worker"
                    }
                || pinned.arguments != call.arguments
                || activity.active_attempt != Some(call.attempt_id)
                || activity.spec.handler != pinned.definition.handler()
                || call.workspace.is_some()
                || !call.writable_paths.is_empty()
                || call.test_command.is_some()
                || call.project_command.is_some()
                || pinned.definition.id.name == "wait_for_worker"
                    && (!paused || !source.dependencies_allowed())
                || !source.attempts().iter().any(|a| {
                    a.id == call.attempt_id
                        && a.activity_id == call.activity_id
                        && a.status == AttemptStatus::Running
                })
            {
                return Err(DomainError::InvalidState.into());
            }
            let turn: String =
                sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
                    .bind(call.run_id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let turn: MessageId = turn.parse().map_err(|_| DomainError::InvalidState)?;
            let session: String =
                sqlx::query_scalar("SELECT session_id FROM harness_turns WHERE task_id=?")
                    .bind(turn.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let session: SessionId = session.parse().map_err(|_| DomainError::InvalidState)?;
            let now = crate::adapters::system_clock::SystemClock.now_ms();
            let guidance = source
                .record()
                .planner_guidance
                .clone()
                .ok_or(DomainError::InvalidState)?;
            let target = guidance.targets.iter().find(|t| t.name == request.worker);
            let mut receipt = PlannerPauseReceipt {
                activity_id: call.activity_id,
                run_id: call.run_id,
                session_id: session,
                turn_id: turn,
                task_id: target.map(|t| t.task_id),
                target_run_id: target.map(|t| t.run_id),
                request,
                paused,
                saved_at: now,
                result: serde_json::Value::Null,
            };
            receipt.result = if let Err(error) = receipt.request.validate() {
                json!({"ok":false,"error":error})
            } else if source.record().controls.cancel_requested
                || Self::goal_receipt_for_task(&mut tx, turn).await?.is_some()
            {
                refused(
                    "worker_pause_goal",
                    "The goal is stopping; no assignment hold was changed.",
                )
            } else if let Some(target) = target {
                let (task, delegation, run) =
                    Self::harness_worker_assignment(&mut tx, session, turn, target.task_id).await?;
                if task.record().run_id != Some(target.run_id)
                    || run.as_ref().is_none_or(|r| r.record().id != target.run_id)
                    || !source
                        .record()
                        .delegations
                        .iter()
                        .any(|d| d.id == delegation.id && d.recipient == task.record().agent_id)
                {
                    return Err(DomainError::InvalidState.into());
                }
                let view =
                    Self::worker_pause_preview(&mut tx, session, turn, target.task_id).await?;
                if let Some(reason) = view.blocked_reason {
                    refused("worker_pause_unavailable", &reason)
                } else {
                    let dependency =
                        Self::dependency_for_pause(&mut tx, &source, &receipt, target, now).await?;
                    let mut hold = view.planner_hold.unwrap_or(PlannerHoldRecord {
                        session_id: session,
                        turn_id: turn,
                        task_id: target.task_id,
                        run_id: target.run_id,
                        planner_run_id: call.run_id,
                        activity_id: call.activity_id,
                        paused: false,
                        revision: 0,
                        reason: String::new(),
                        saved_at: now,
                    });
                    if hold.session_id != session
                        || hold.turn_id != turn
                        || hold.task_id != target.task_id
                        || hold.run_id != target.run_id
                        || hold.planner_run_id != call.run_id
                    {
                        refused(
                            "worker_pause_owner",
                            "This hold belongs to another planner; it was not changed.",
                        )
                    } else if hold.paused == paused {
                        refused(
                            "worker_pause_state",
                            if paused {
                                "This planner already paused this assignment."
                            } else {
                                "This planner has no active pause on this assignment; other holds were not changed."
                            },
                        )
                    } else if let Err(error) = dependency {
                        json!({"ok":false,"error":error})
                    } else {
                        hold.transition(
                            paused,
                            call.activity_id,
                            receipt.request.reason.clone(),
                            now,
                        )?;
                        sqlx::query("INSERT INTO harness_planner_holds(task_id,paused,payload) VALUES(?,?,?) ON CONFLICT(task_id) DO UPDATE SET paused=excluded.paused,payload=excluded.payload")
                            .bind(target.task_id.to_string()).bind(paused).bind(serde_json::to_string(&hold)?)
                            .execute(&mut *tx).await.map_err(storage)?;
                        Self::close_worker_dependencies(&mut tx, target.task_id).await?;
                        let mut dependency = dependency.expect("validated dependency");
                        if let Some(d) = &mut dependency {
                            d.hold_revision = hold.revision;
                            Self::save_dependency(&mut tx, d).await?;
                            if d.outcome.is_some() {
                                source.queue_dependency_wake(d.activity_id, now)?;
                                self.save(&mut tx, &source, false).await?;
                            }
                        }
                        // Omit the reason from the acknowledgement: escaping may expand it.
                        // The immutable receipt and current hold retain the full user-visible text.
                        let mut result = json!({"ok":true,"worker":receipt.request.worker,"control":{
                            "task_id":hold.task_id,"run_id":hold.run_id,"planner_run_id":hold.planner_run_id,
                            "activity_id":hold.activity_id,"paused":hold.paused,"revision":hold.revision
                        },"execution_held":Self::task_execution_held(&mut tx,target.task_id).await?,"manual_paused":view.control.paused,"inherited_held":view.inherited_held});
                        if let Some(d) = dependency {
                            result["dependency"] = json!({"activity_id":d.activity_id,"prerequisite":d.prerequisite,"prerequisite_task_id":d.prerequisite_task_id,"prerequisite_run_id":d.prerequisite_run_id,"outcome":d.outcome});
                        }
                        result
                    }
                }
            } else {
                refused(
                    "worker_pause_scope",
                    "Choose coder or coder2 captured in this guidance step; other assignments cannot be paused or resumed.",
                )
            };
            sqlx::query("INSERT INTO harness_planner_pauses(activity_id,run_id,session_id,turn_id,payload) VALUES(?,?,?,?,?)")
                .bind(call.activity_id.to_string()).bind(call.run_id.to_string()).bind(session.to_string()).bind(turn.to_string())
                .bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
            tx.commit().await.map_err(storage)?;
            output(receipt)
        })
    }
}
