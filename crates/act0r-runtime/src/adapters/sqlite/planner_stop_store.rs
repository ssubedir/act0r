use super::*;
use crate::domain::{planner_stop::*, tools::*, worker_cancellation::*};
use serde_json::json;

fn refused(code: &str, message: &str) -> serde_json::Value {
    json!({"ok":false,"error":ToolError::new(code,message)})
}
fn output(receipt: PlannerStopReceipt) -> Result<NativeToolResult, Error> {
    if receipt.result["ok"] == false {
        Ok(NativeToolResult::Rejected(serde_json::from_value(
            receipt.result["error"].clone(),
        )?))
    } else {
        Ok(NativeToolResult::Success(receipt.result))
    }
}
impl PlannerStopPort for SqliteStore {
    fn stop_worker(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let request: StopWorker = serde_json::from_value(call.arguments.clone())?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let saved: Option<String> =
                sqlx::query_scalar("SELECT payload FROM harness_planner_stops WHERE activity_id=?")
                    .bind(call.activity_id.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            if let Some(saved) = saved {
                let receipt: PlannerStopReceipt = serde_json::from_str(&saved)?;
                if receipt.run_id != call.run_id || receipt.request != request {
                    return Err(DomainError::ConflictingMessage.into());
                }
                tx.commit().await.map_err(storage)?;
                return output(receipt);
            }
            let source = Self::load(&mut tx, call.run_id).await?;
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
            if !planner::worker_stop_admitted(state, &pinned.definition, &call.arguments)
                || pinned.arguments != call.arguments
                || activity.active_attempt != Some(call.attempt_id)
                || activity.spec.handler != pinned.definition.handler()
                || call.workspace.is_some()
                || !call.writable_paths.is_empty()
                || call.test_command.is_some()
                || call.project_command.is_some()
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
            let session: Option<String> =
                sqlx::query_scalar("SELECT session_id FROM harness_turns WHERE task_id=?")
                    .bind(turn.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            let session: SessionId = session
                .ok_or(DomainError::InvalidState)?
                .parse()
                .map_err(|_| DomainError::InvalidState)?;
            let now = crate::adapters::system_clock::SystemClock.now_ms();
            let guidance = source
                .record()
                .planner_guidance
                .as_ref()
                .ok_or(DomainError::InvalidState)?;
            let target = guidance.targets.iter().find(|t| t.name == request.worker);
            let mut receipt = PlannerStopReceipt {
                activity_id: call.activity_id,
                run_id: call.run_id,
                session_id: session,
                turn_id: turn,
                task_id: target.map(|t| t.task_id),
                target_run_id: target.map(|t| t.run_id),
                request,
                saved_at: now,
                result: serde_json::Value::Null,
            };
            receipt.result = if let Err(error) = receipt.request.validate() {
                json!({"ok":false,"error":error})
            } else if source.record().controls.cancel_requested
                || Self::goal_receipt_for_task(&mut tx, turn).await?.is_some()
            {
                refused(
                    "worker_stop_goal",
                    "The goal is stopping; no assignment stop was added.",
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
                    Self::worker_cancellation_preview(&mut tx, session, turn, target.task_id)
                        .await?;
                if let Some(reason) = view.blocked_reason {
                    refused("worker_stop_unavailable", &reason)
                } else {
                    // Admission closes atomically with both receipts. Actor-specific cleanup is
                    // performed by the existing durable cancellation coordinator, including
                    // after restart, without changing this planner's saved join.
                    Self::save_goal_cancellation(&mut tx, target.task_id, target.task_id, now)
                        .await?;
                    let cancellation = StopAssignmentReceipt {
                        session_id: session,
                        turn_id: turn,
                        task_id: target.task_id,
                        request: StopAssignmentRequest {
                            request_id: call
                                .activity_id
                                .to_string()
                                .parse()
                                .map_err(|_| DomainError::InvalidState)?,
                        },
                        run_id: Some(target.run_id),
                        status: view.status,
                        saved_at: now,
                        replayed: false,
                    };
                    Self::save_assignment_stop_receipt(&mut tx, &cancellation).await?;
                    // Keep the acknowledgement compact even for heavily escaped reasons.
                    // The immutable request in the receipt retains the complete reason.
                    json!({"ok":true,"stop_requested":true,"worker":receipt.request.worker,"cancellation":cancellation})
                }
            } else {
                refused(
                    "worker_stop_scope",
                    "Choose coder or coder2 captured in this guidance step; other assignments cannot be stopped.",
                )
            };
            sqlx::query("INSERT INTO harness_planner_stops(activity_id,run_id,session_id,turn_id,payload) VALUES(?,?,?,?,?)")
                .bind(call.activity_id.to_string()).bind(call.run_id.to_string()).bind(session.to_string()).bind(turn.to_string())
                .bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
            tx.commit().await.map_err(storage)?;
            output(receipt)
        })
    }
}
