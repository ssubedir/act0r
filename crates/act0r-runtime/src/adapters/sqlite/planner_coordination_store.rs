//! Restore coordination from immutable timer captures and receipts, never the live event tail.
use super::*;
use crate::application::*;
use crate::domain::{
    assignment_inspection::InspectionReceipt,
    planner_guidance::*,
    planner_pause::{PlannerPauseReceipt, WorkerDependency},
    planner_stop::PlannerStopReceipt,
    worker_messages::*,
};

fn pending_status(run: &Run) -> &'static str {
    if run.record().controls.cancel_requested || run.record().status == RunStatus::Cancelled {
        "stopped"
    } else if run.needs_worker_recovery() {
        "needs_recovery"
    } else {
        match run.record().status {
            RunStatus::Failed => "failed",
            RunStatus::Blocked => "blocked",
            RunStatus::BudgetExhausted => "budget_exhausted",
            RunStatus::Succeeded => "closed",
            _ => "reviewing",
        }
    }
}

impl SqliteStore {
    pub(super) async fn planner_coordination(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
        run_id: RunId,
    ) -> Result<PlannerCoordination, Error> {
        let run = Self::load(conn, run_id).await?;
        let held = Self::task_execution_held(conn, turn).await?
            || (run.record().status.is_unfinished()
                && !run.record().controls.cancel_requested
                && (run.record().controls.pause_requested || run.record().controls.boundary_pause));
        // Only a real timer_fired event starts a batch. Coalesced/skipped wakes have no such event.
        // The first guidance_finished event closes its interval, even if the Run later coordinates
        // a worker question or another batch. Recovery before that event stays in the same interval.
        let rows = sqlx::query("SELECT t.payload,h.sequence,h.payload AS started,(SELECT MIN(e.sequence) FROM history e WHERE e.run_id=h.run_id AND e.sequence>h.sequence AND json_extract(e.payload,'$.kind')='planner_guidance_finished') AS end_sequence FROM timers t JOIN history h ON h.run_id=t.run_id AND json_extract(h.payload,'$.kind')='timer_fired' AND json_extract(h.payload,'$.payload.timer_id')=t.id WHERE t.run_id=? AND json_extract(t.payload,'$.payload.key')='planner_guidance' ORDER BY h.sequence DESC LIMIT ?")
            .bind(run_id.to_string()).bind((MAX_COORDINATION_BATCHES+1) as i64)
            .fetch_all(&mut *conn).await.map_err(storage)?;
        let mut batches = Vec::new();
        for row in rows.iter().take(MAX_COORDINATION_BATCHES) {
            let timer: DurableTimer =
                serde_json::from_str(&row.try_get::<String, _>("payload").map_err(storage)?)?;
            let start: HistoryEvent =
                serde_json::from_str(&row.try_get::<String, _>("started").map_err(storage)?)?;
            let end: Option<i64> = row.try_get("end_sequence").map_err(storage)?;
            let guidance: PlannerGuidance =
                serde_json::from_value(timer.payload["guidance"].clone())?;
            guidance.validate()?;
            if timer.run_id != run_id
                || guidance
                    .targets
                    .iter()
                    .any(|t| !run.record().delegations.iter().any(|d| d.id == t.task_id))
            {
                return Err(DomainError::InvalidState.into());
            }
            let finished_at: Option<i64> = if let Some(end) = end {
                sqlx::query_scalar("SELECT json_extract(payload,'$.at') FROM history WHERE run_id=? AND sequence=?")
                    .bind(run_id.to_string()).bind(end).fetch_optional(&mut *conn).await.map_err(storage)?
            } else {
                None
            };
            let mut batch = CoordinationBatch {
                timer_id: timer.id,
                started_at: start.at,
                finished_at,
                status: if end.is_some() {
                    "finished"
                } else {
                    pending_status(&run)
                },
                guidance,
                inspections: vec![],
                stops: vec![],
                pauses: vec![],
                decision: None,
            };
            let receipts = sqlx::query("SELECT a.payload,i.payload AS inspection,g.payload AS guidance,s.payload AS stop,p.payload AS pause FROM activities a JOIN history h ON h.run_id=a.run_id AND json_extract(h.payload,'$.kind')='activity_scheduled' AND json_extract(h.payload,'$.activity_id')=a.id LEFT JOIN harness_assignment_inspections i ON i.activity_id=a.id AND i.run_id=a.run_id LEFT JOIN harness_planner_stops s ON s.activity_id=a.id AND s.run_id=a.run_id LEFT JOIN harness_planner_pauses p ON p.activity_id=a.id AND p.run_id=a.run_id LEFT JOIN harness_planner_guidance g ON g.activity_id=a.id AND g.run_id=a.run_id AND g.session_id=? AND g.turn_id=? WHERE a.run_id=? AND h.sequence>? AND h.sequence<? AND (i.activity_id IS NOT NULL OR g.activity_id IS NOT NULL OR s.activity_id IS NOT NULL OR p.activity_id IS NOT NULL) ORDER BY h.sequence LIMIT 10")
                .bind(session.to_string()).bind(turn.to_string()).bind(run_id.to_string()).bind(start.sequence as i64)
                .bind(end.unwrap_or(i64::MAX)).fetch_all(&mut *conn).await.map_err(storage)?;
            for row in receipts {
                let activity: Activity =
                    serde_json::from_str(&row.try_get::<String, _>("payload").map_err(storage)?)?;
                if let Some(payload) = row.try_get::<Option<String>, _>("pause").map_err(storage)? {
                    let mut receipt: PlannerPauseReceipt = serde_json::from_str(&payload)?;
                    if receipt.activity_id != activity.id
                        || receipt.run_id != run_id
                        || receipt.session_id != session
                        || receipt.turn_id != turn
                        || receipt.task_id.is_some()
                            && !batch.guidance.targets.iter().any(|t| {
                                Some(t.task_id) == receipt.task_id
                                    && Some(t.run_id) == receipt.target_run_id
                                    && t.name == receipt.request.worker
                            })
                        || receipt.result["ok"] == true && receipt.task_id.is_none()
                    {
                        return Err(DomainError::InvalidState.into());
                    }
                    let current = if let Some(task) = receipt.task_id {
                        Some(Self::worker_pause_preview(conn, session, turn, task).await?)
                    } else {
                        None
                    };
                    if current.as_ref().is_some_and(|v| {
                        v.run_id != receipt.target_run_id
                            || v.planner_hold.as_ref().is_some_and(|h| {
                                h.planner_run_id != run_id
                                    || h.run_id != receipt.target_run_id.unwrap()
                                    || h.task_id != receipt.task_id.unwrap()
                                    || h.session_id != session
                                    || h.turn_id != turn
                            })
                    }) {
                        return Err(DomainError::InvalidState.into());
                    }
                    let text_truncated = receipt.request.reason.len()
                        > planner_pause::MAX_REASON_BYTES
                        || receipt
                            .request
                            .dependency
                            .as_ref()
                            .is_some_and(|s| s.len() > 128)
                        || receipt.request.worker.len() > 128;
                    if let Some(name) = &mut receipt.request.dependency {
                        *name = delegated_text(name, 128);
                    }
                    receipt.request.worker = delegated_text(&receipt.request.worker, 128);
                    receipt.request.reason =
                        delegated_text(&receipt.request.reason, planner_pause::MAX_REASON_BYTES);
                    batch.pauses.push(CoordinationPause {
                        receipt,
                        activity_status: activity.status,
                        text_truncated,
                        current_status: current.as_ref().map(|v| v.status),
                        current_revision: current
                            .as_ref()
                            .and_then(|v| v.planner_hold.as_ref().map(|h| h.revision)),
                        planner_paused: current
                            .as_ref()
                            .is_some_and(|v| v.planner_hold.as_ref().is_some_and(|h| h.paused)),
                        manual_paused: current.as_ref().is_some_and(|v| v.control.paused),
                        inherited_held: current.as_ref().is_some_and(|v| v.inherited_held),
                        execution_held: current.as_ref().is_some_and(|v| v.execution_held),
                        admitted_operations: current.as_ref().map_or(0, |v| v.admitted_operations),
                        settled: current.as_ref().is_some_and(|v| v.settled),
                    });
                }
                if let Some(payload) = row.try_get::<Option<String>, _>("stop").map_err(storage)? {
                    let mut receipt: PlannerStopReceipt = serde_json::from_str(&payload)?;
                    if receipt.activity_id != activity.id
                        || receipt.run_id != run_id
                        || receipt.session_id != session
                        || receipt.turn_id != turn
                        || receipt.task_id.is_some()
                            && !batch.guidance.targets.iter().any(|t| {
                                Some(t.task_id) == receipt.task_id
                                    && Some(t.run_id) == receipt.target_run_id
                                    && t.name == receipt.request.worker
                            })
                        || receipt.result["ok"] == true && receipt.task_id.is_none()
                    {
                        return Err(DomainError::InvalidState.into());
                    }
                    let current = if let Some(task) = receipt.task_id {
                        Some(Self::worker_cancellation_preview(conn, session, turn, task).await?)
                    } else {
                        None
                    };
                    if current
                        .as_ref()
                        .is_some_and(|v| v.run_id != receipt.target_run_id)
                    {
                        return Err(DomainError::InvalidState.into());
                    }
                    let text_truncated = receipt.request.reason.len()
                        > planner_stop::MAX_REASON_BYTES
                        || receipt.request.worker.len() > 128;
                    receipt.request.worker = delegated_text(&receipt.request.worker, 128);
                    receipt.request.reason =
                        delegated_text(&receipt.request.reason, planner_stop::MAX_REASON_BYTES);
                    batch.stops.push(CoordinationStop {
                        receipt,
                        activity_status: activity.status,
                        text_truncated,
                        current_status: current.as_ref().map(|v| v.status),
                        settled: current.as_ref().is_some_and(|v| v.settled),
                    });
                }
                if let Some(payload) = row
                    .try_get::<Option<String>, _>("inspection")
                    .map_err(storage)?
                {
                    let receipt: InspectionReceipt = serde_json::from_str(&payload)?;
                    if receipt.activity_id != activity.id
                        || receipt.run_id != run_id
                        || (receipt.result["ok"] == true
                            && !batch.guidance.targets.iter().any(|t| {
                                receipt.result["assignment"]["task_id"] == t.task_id.to_string()
                                    && receipt.result["assignment"]["run_id"]
                                        == t.run_id.to_string()
                                    && t.name == receipt.request.worker
                            })
                            && !batch.guidance.dependencies.iter().any(|d| {
                                receipt.result["assignment"]["task_id"]
                                    == d.prerequisite_task_id.to_string()
                                    && receipt.result["assignment"]["run_id"]
                                        == d.prerequisite_run_id.to_string()
                                    && d.prerequisite == receipt.request.worker
                            }))
                    {
                        return Err(DomainError::InvalidState.into());
                    }
                    batch.inspections.push(CoordinationInspection {
                        receipt,
                        activity_status: activity.status,
                    });
                }
                if let Some(payload) = row
                    .try_get::<Option<String>, _>("guidance")
                    .map_err(storage)?
                {
                    let receipt: GuidanceReceipt = serde_json::from_str(&payload)?;
                    if receipt.activity_id != activity.id
                        || receipt.run_id != run_id
                        || receipt.turn_id != turn
                        || receipt.session_id != session
                    {
                        return Err(DomainError::InvalidState.into());
                    }
                    let mut deliveries = Vec::new();
                    let mut text_truncated = false;
                    let mut request = receipt.request;
                    for text in [&mut request.coder, &mut request.coder2]
                        .into_iter()
                        .flatten()
                    {
                        text_truncated |= text.len() > MAX_TEXT_BYTES;
                        *text = delegated_text(text, MAX_TEXT_BYTES);
                    }
                    for mut delivery in receipt.deliveries {
                        if delivery.sender_task_id != turn
                            || delivery.sender_run_id != run_id
                            || delivery.message_id != activity.id
                            || delivery.session_id != session
                            || delivery.turn_id != turn
                            || (delivery.error.is_none()
                                && !batch.guidance.targets.iter().any(|t| {
                                    delivery.recipient_task_id == Some(t.task_id)
                                        && delivery.recipient_run_id == Some(t.run_id)
                                        && delivery.request.recipient == t.name
                                }))
                        {
                            return Err(DomainError::InvalidState.into());
                        }
                        text_truncated |= bounded_receipt_text(&mut delivery);
                        deliveries.push(Self::coordination_delivery(conn, delivery).await?);
                    }
                    batch.decision = Some(CoordinationDecision {
                        activity_id: activity.id,
                        activity_status: activity.status,
                        saved_at: receipt.saved_at,
                        request,
                        text_truncated,
                        deliveries,
                    });
                }
            }
            batches.push(batch);
        }
        let receipts: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_worker_messages WHERE session_id=? AND turn_id=? AND json_extract(payload,'$.request.recipient')='parent' AND json_extract(payload,'$.request.needs_coordination')=1 ORDER BY json_extract(payload,'$.saved_at') DESC,message_id DESC LIMIT ?")
            .bind(session.to_string()).bind(turn.to_string()).bind((MAX_COORDINATION_FINDINGS+1) as i64)
            .fetch_all(&mut *conn).await.map_err(storage)?;
        let skipped_through: Option<i64> = sqlx::query_scalar("SELECT MAX(json_extract(t.payload,'$.payload.findings_through')) FROM timers t WHERE t.run_id=? AND t.status='fired' AND json_extract(t.payload,'$.payload.key')='planner_guidance' AND NOT EXISTS(SELECT 1 FROM history h WHERE h.run_id=t.run_id AND json_extract(h.payload,'$.kind')='timer_fired' AND json_extract(h.payload,'$.payload.timer_id')=t.id)")
            .bind(run_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        let mut findings = Vec::new();
        for payload in receipts.iter().take(MAX_COORDINATION_FINDINGS) {
            let mut receipt: WorkerMessageReceipt = serde_json::from_str(payload)?;
            if !run
                .record()
                .delegations
                .iter()
                .any(|d| d.id == receipt.sender_task_id)
                || receipt.session_id != session
                || receipt.turn_id != turn
                || (receipt.error.is_none()
                    && (receipt.recipient_task_id != Some(turn)
                        || receipt.recipient_run_id != Some(run_id)))
            {
                continue;
            }
            let captured: Option<String> = sqlx::query_scalar("SELECT t.id FROM timers t,json_each(t.payload,'$.payload.guidance.findings') f WHERE t.run_id=? AND t.status='fired' AND json_extract(t.payload,'$.payload.key')='planner_guidance' AND json_extract(f.value,'$.message_id')=? AND EXISTS(SELECT 1 FROM history h WHERE h.run_id=t.run_id AND json_extract(h.payload,'$.kind')='timer_fired' AND json_extract(h.payload,'$.payload.timer_id')=t.id) LIMIT 1")
                .bind(run_id.to_string()).bind(receipt.message_id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
            let batch_id = captured
                .map(|id| id.parse::<TimerId>().map_err(|_| DomainError::InvalidState))
                .transpose()?;
            let status = if receipt.error.is_some() {
                "refused"
            } else if let Some(id) = batch_id {
                if let Some(batch) = batches.iter().find(|b| b.timer_id == id) {
                    if let Some(decision) = &batch.decision {
                        if decision.deliveries.is_empty() {
                            if batch.stops.iter().any(|s| s.receipt.result["ok"] == true) {
                                "stop_requested"
                            } else if batch.pauses.iter().any(|p| p.receipt.result["ok"] == true) {
                                "pause_changed"
                            } else {
                                "no_action"
                            }
                        } else if decision
                            .deliveries
                            .iter()
                            .all(|d| d.receipt.error.is_some())
                        {
                            "refused"
                        } else if decision
                            .deliveries
                            .iter()
                            .any(|d| d.receipt.error.is_some())
                        {
                            "partial"
                        } else {
                            "routed"
                        }
                    } else {
                        batch.status
                    }
                } else {
                    "history_omitted"
                }
            } else if receipt
                .message
                .as_ref()
                .is_some_and(|m| m.sequence <= skipped_through.unwrap_or(0) as u64)
            {
                "skipped"
            } else {
                match pending_status(&run) {
                    "reviewing" => "queued",
                    other => other,
                }
            };
            let text_truncated = bounded_receipt_text(&mut receipt);
            findings.push(CoordinationFinding {
                receipt,
                text_truncated,
                batch_id,
                status,
            });
        }
        let dependency_rows:Vec<String>=sqlx::query_scalar("SELECT payload FROM harness_worker_dependencies WHERE session_id=? AND turn_id=? AND planner_run_id=? ORDER BY rowid DESC LIMIT 65")
            .bind(session.to_string()).bind(turn.to_string()).bind(run_id.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let mut dependencies = Vec::new();
        for payload in dependency_rows.iter().take(64) {
            let d: WorkerDependency = serde_json::from_str(payload)?;
            d.validate()?;
            if d.session_id != session
                || d.turn_id != turn
                || d.planner_run_id != run_id
                || !run.record().delegations.iter().any(|p| p.id == d.task_id)
                || !run
                    .record()
                    .delegations
                    .iter()
                    .any(|p| p.id == d.prerequisite_task_id)
            {
                return Err(DomainError::InvalidState.into());
            }
            dependencies.push(d);
        }
        let (transfers, transfer_truncated) = Self::worker_transfer_summaries(conn, &run).await?;
        Ok(PlannerCoordination {
            session_id: session,
            turn_id: turn,
            run_id,
            execution_held: held,
            findings,
            dependencies,
            transfers,
            batches,
            truncated: transfer_truncated
                || rows.len() > MAX_COORDINATION_BATCHES
                || dependency_rows.len() > 64
                || receipts.len() > MAX_COORDINATION_FINDINGS,
        })
    }
    async fn coordination_delivery(
        conn: &mut SqliteConnection,
        receipt: WorkerMessageReceipt,
    ) -> Result<WorkerMessageItem, Error> {
        let (application, recipient_terminal) = if receipt.error.is_none() {
            let recipient = Self::load(
                conn,
                receipt.recipient_run_id.ok_or(DomainError::InvalidState)?,
            )
            .await?;
            let message = recipient
                .record()
                .peer_messages
                .iter()
                .find(|m| {
                    m.message_id == receipt.message_id && m.sender_task_id == receipt.sender_task_id
                })
                .ok_or(DomainError::InvalidState)?;
            (
                message.application.clone(),
                !recipient.needs_worker_recovery()
                    && !recipient.record().status.is_unfinished()
                    && recipient.record().status != RunStatus::Blocked,
            )
        } else {
            (None, false)
        };
        Ok(WorkerMessageItem {
            receipt,
            application,
            recipient_terminal,
        })
    }
}

fn bounded_receipt_text(receipt: &mut WorkerMessageReceipt) -> bool {
    let mut truncated = receipt.request.text.len() > MAX_TEXT_BYTES;
    receipt.request.text = delegated_text(&receipt.request.text, MAX_TEXT_BYTES);
    if let Some(error) = &mut receipt.error {
        truncated |= error.message.len() > MAX_TEXT_BYTES;
        error.message = delegated_text(&error.message, MAX_TEXT_BYTES);
    }
    truncated
}
