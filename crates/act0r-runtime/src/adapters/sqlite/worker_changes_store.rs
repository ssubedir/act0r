use super::*;
use crate::domain::{
    child_changes::{InspectRequest, Inspection, ManagedTarget},
    planner_pause::WorkerDependency,
    worker_changes::*,
};
use serde_json::json;

fn unavailable(message: &str) -> Error {
    Error::Unavailable(message.into())
}
impl SqliteStore {
    pub(super) async fn worker_transfer_summaries(
        conn: &mut SqliteConnection,
        run: &Run,
    ) -> Result<(Vec<crate::application::WorkerTransferSummary>, bool), Error> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM activities WHERE run_id=? AND json_extract(payload,'$.spec.handler.name') IN ('planner.worker_changes.inspect','planner.worker_changes.import') AND json_extract(payload,'$.spec.handler.version')=1 ORDER BY scheduling_revision DESC,command_ordinal DESC LIMIT 65")
            .bind(run.record().id.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let mut items = vec![];
        for row in rows.iter().take(64) {
            let a: Activity = serde_json::from_str(row)?;
            let import = a.spec.handler == handler("import");
            let inspection: Option<Inspection> = if import {
                Some(serde_json::from_value(a.spec.input.clone())?)
            } else {
                a.result
                    .as_ref()
                    .filter(|r| r["ok"] == true)
                    .map(|r| serde_json::from_value(r["inspection"].clone()))
                    .transpose()?
            };
            let d = if let Some(i) = &inspection {
                i.validate()?;
                i.request
                    .dependency
                    .clone()
                    .ok_or(DomainError::InvalidState)?
            } else {
                let r: InspectWorkerChanges = serde_json::from_value(a.spec.input.clone())?;
                r.validate()?;
                r.dependency
            };
            if d.planner_run_id != run.record().id
                || !run.record().delegations.iter().any(|p| p.id == d.task_id)
                || !run
                    .record()
                    .delegations
                    .iter()
                    .any(|p| p.id == d.prerequisite_task_id)
            {
                return Err(DomainError::InvalidState.into());
            }
            let status = match a.status {
                ActivityStatus::Succeeded
                    if a.result.as_ref().is_some_and(|r| r["ok"] == false) =>
                {
                    "refused".into()
                }
                ActivityStatus::Succeeded if import => a
                    .result
                    .as_ref()
                    .and_then(|r| r["status"].as_str())
                    .ok_or(DomainError::InvalidState)?
                    .to_string(),
                ActivityStatus::Succeeded
                    if run.record().wait.as_ref().is_some_and(|w| {
                        w.payload["inspection"]["preview"]["preparation_id"] == json!(a.id)
                    }) =>
                {
                    "awaiting_approval".into()
                }
                ActivityStatus::Succeeded => "reviewed".into(),
                ActivityStatus::Indeterminate => "interrupted".into(),
                ActivityStatus::Failed => "failed".into(),
                ActivityStatus::Queued => "queued".into(),
                ActivityStatus::Running => "running".into(),
            };
            let error = a
                .result
                .as_ref()
                .and_then(|r| r["error"]["message"].as_str())
                .map(|s| s.chars().take(512).collect());
            items.push(crate::application::WorkerTransferSummary {
                activity_id: a.id,
                dependency: d,
                phase: if import { "import" } else { "inspect" },
                status,
                artifact_id: inspection.as_ref().map(|i| i.artifact_id),
                files: inspection.as_ref().map_or(vec![], |i| {
                    i.preview.files.iter().take(16).cloned().collect()
                }),
                file_count: inspection.as_ref().map_or(0, |i| i.preview.files.len()),
                error,
            });
        }
        Ok((items, rows.len() > 64))
    }
    async fn worker_patch_source(
        conn: &mut SqliteConnection,
        parent: RunId,
        request: &InspectWorkerChanges,
    ) -> Result<(InspectRequest, change_review::TaskChanges), Error> {
        request.validate()?;
        let d = &request.dependency;
        let run = Self::load(conn, parent).await?;
        let input = run
            .record()
            .input
            .get("input")
            .unwrap_or(&run.record().input);
        let config: planner::PlannerConfig = serde_json::from_value(input.clone())?;
        let saved: Option<String> = sqlx::query_scalar(
            "SELECT payload FROM harness_worker_dependencies WHERE activity_id=? AND closed=0",
        )
        .bind(d.activity_id.to_string())
        .fetch_optional(&mut *conn)
        .await
        .map_err(storage)?;
        let saved: Option<WorkerDependency> =
            saved.map(|s| serde_json::from_str(&s)).transpose()?;
        if !config.worker_changes
            || parent != d.planner_run_id
            || saved.as_ref().is_none_or(|s| json!(s) != json!(d))
            || run
                .record()
                .planner_guidance
                .as_ref()
                .is_none_or(|g| !g.dependencies.iter().any(|p| json!(p) == json!(d)))
            || run.record().controls.cancel_requested
            || Self::goal_receipt_for_task(conn, d.turn_id)
                .await?
                .is_some()
            || Self::load_session_metadata(conn, d.session_id)
                .await?
                .archived
        {
            return Err(unavailable(
                "The saved dependency review is no longer available for this planner.",
            ));
        }
        let waiting = run
            .record()
            .delegations
            .iter()
            .find(|p| p.id == d.task_id)
            .ok_or(DomainError::InvalidState)?;
        let prerequisite = run
            .record()
            .delegations
            .iter()
            .find(|p| p.id == d.prerequisite_task_id)
            .ok_or(DomainError::InvalidState)?;
        let now = crate::adapters::system_clock::SystemClock.now_ms();
        if waiting.group_id.is_none()
            || waiting.group_id != prerequisite.group_id
            || waiting.outcome.is_some()
            || waiting.timed_out_at.is_some()
            || waiting.deadline_at.is_some_and(|at| at <= now)
            || prerequisite.timed_out_at.is_some()
            || prerequisite.outcome.as_ref().is_none_or(|o| {
                o["status"] != "succeeded"
                    || o["task_id"] != json!(d.prerequisite_task_id)
                    || o["run_id"] != json!(d.prerequisite_run_id)
            })
        {
            return Err(unavailable(
                "The original join must have a successful prerequisite and an unfinished receiving assignment.",
            ));
        }
        let hold = Self::planner_hold_for_task(conn, d.task_id).await?;
        if hold.is_none_or(|h| {
            !h.paused
                || h.revision != d.hold_revision
                || h.run_id != d.run_id
                || h.planner_run_id != parent
        }) {
            return Err(unavailable(
                "The exact planner-owned dependency hold must remain active.",
            ));
        }
        let (task, _, receiver) =
            Self::harness_worker_assignment(conn, d.session_id, d.turn_id, d.task_id).await?;
        let receiver = receiver.ok_or(DomainError::InvalidState)?;
        let pause = Self::worker_pause_preview(conn, d.session_id, d.turn_id, d.task_id).await?;
        let state = receiver
            .record()
            .state
            .get("state")
            .unwrap_or(&receiver.record().state);
        if task.record().run_id != Some(d.run_id)
            || receiver.record().id != d.run_id
            || pause.blocked_reason.is_some()
            || !pause.execution_held
            || pause.admitted_operations != 0
            || !receiver.record().status.is_unfinished()
            || receiver.activities().iter().any(|a| {
                matches!(
                    a.status,
                    ActivityStatus::Running | ActivityStatus::Indeterminate
                )
            })
            || !change_review::ready_for_worker_changes(state)
        {
            return Err(unavailable(
                "The receiving assignment must be held with admitted work settled and no pending test checkpoint or uncertain effect.",
            ));
        }
        let (source_task, _, source_run) =
            Self::harness_worker_assignment(conn, d.session_id, d.turn_id, d.prerequisite_task_id)
                .await?;
        if source_task.record().status != actors::AgentTaskStatus::Succeeded
            || source_task.record().run_id != Some(d.prerequisite_run_id)
            || source_run.as_ref().is_none_or(|r| {
                r.record().id != d.prerequisite_run_id || r.record().status != RunStatus::Succeeded
            })
            || !config
                .workers
                .iter()
                .any(|w| w.name == d.prerequisite && w.agent_id == source_task.record().agent_id)
            || !config
                .workers
                .iter()
                .any(|w| w.name == d.worker && w.agent_id == task.record().agent_id)
            || waiting.workspace_input.is_none()
        {
            return Err(unavailable(
                "The transfer differs from the pinned source or receiving task.",
            ));
        }
        let target: ManagedTarget = serde_json::from_value(
            json!({"workspace":state["config"]["workspace"],"writable_paths":state["config"]["writable_paths"]}),
        )?;
        target.validate()?;
        if waiting
            .workspace_input
            .as_ref()
            .is_none_or(|w| w.workspace != target.workspace)
        {
            return Err(unavailable(
                "The receiver workspace differs from its accepted task handoff.",
            ));
        }
        let view = Self::saved_task_changes(conn, d.prerequisite_task_id).await?;
        let changes = view
            .changes
            .as_ref()
            .ok_or_else(|| unavailable("The prerequisite has no committed change artifact."))?;
        if !view.enabled
            || view.run_status != Some(RunStatus::Succeeded)
            || changes.run_id != d.prerequisite_run_id
            || prerequisite
                .workspace_input
                .as_ref()
                .and_then(|w| w.request.writable_paths.as_ref())
                .is_some_and(|paths| {
                    changes
                        .files
                        .iter()
                        .any(|f| !tools::writable_path_permitted(paths, &f.path))
                })
        {
            return Err(unavailable(
                "The prerequisite artifact is unavailable or contains out-of-scope files.",
            ));
        }
        Ok((
            InspectRequest {
                worker: d.prerequisite.clone(),
                recipient: source_task.record().agent_id,
                target,
                offset: request.offset,
                dependency: Some(d.clone()),
            },
            view,
        ))
    }
    pub(super) async fn save_worker_transfer_changes(
        &self,
        conn: &mut SqliteConnection,
        before: &Run,
        run: &Run,
        execution: Option<&ActivityExecution>,
    ) -> Result<(), Error> {
        if let Some(call) = execution.filter(|c| c.handler == handler("import")) {
            let i: Inspection = serde_json::from_value(call.input.clone())?;
            i.validate()?;
            let d = i
                .request
                .dependency
                .as_ref()
                .ok_or(DomainError::InvalidState)?;
            // Only admission of the first attempt invalidates evidence. An old retry never
            // rewrites a later receiver checkpoint; the actual write rechecks authority.
            let done: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM history WHERE run_id=? AND json_extract(payload,'$.kind')='worker_changes_started' AND json_extract(payload,'$.payload.transfer_activity_id')=?)")
                .bind(d.run_id.to_string()).bind(call.activity_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            if !done {
                let validated = Self::worker_patch_source(
                    conn,
                    call.run_id,
                    &InspectWorkerChanges {
                        dependency: d.clone(),
                        offset: i.request.offset,
                    },
                )
                .await;
                match validated {
                    Ok((request, view)) => {
                        if request != i.request
                            || view
                                .changes
                                .as_ref()
                                .is_none_or(|c| c.activity_id != i.artifact_id)
                        {
                            return Err(DomainError::InvalidState.into());
                        }
                        let mut receiver = Self::load(conn, d.run_id).await?;
                        receiver.worker_changes_started(
                            call.activity_id,
                            &i,
                            run.record().updated_at,
                        )?;
                        self.save(conn, &receiver, false).await?;
                    }
                    Err(Error::Unavailable(_)) => {} // The executor saves refusal feedback.
                    Err(error) => return Err(error),
                }
            }
        }
        for a in run.activities().iter().filter(|a| {
            a.spec.handler == handler("import") && a.status == ActivityStatus::Succeeded
        }) {
            if before
                .activities()
                .iter()
                .any(|b| b.id == a.id && b.status == ActivityStatus::Succeeded)
            {
                continue;
            }
            let Some(result) = &a.result else {
                continue;
            };
            if result["ok"] == false {
                continue;
            }
            let i: Inspection = serde_json::from_value(a.spec.input.clone())?;
            let report: patch_application::ApplicationReport =
                serde_json::from_value(result.clone())?;
            let mut receiver = Self::load(
                conn,
                i.request
                    .dependency
                    .as_ref()
                    .ok_or(DomainError::InvalidState)?
                    .run_id,
            )
            .await?;
            receiver.worker_changes_received(a.id, &i, &report, run.record().updated_at)?;
            self.save(conn, &receiver, false).await?;
        }
        Ok(())
    }
}
impl WorkerChangesReadPort for SqliteStore {
    fn worker_changes(
        &self,
        parent: RunId,
        request: InspectWorkerChanges,
    ) -> PortFuture<'_, (InspectRequest, change_review::TaskChanges)> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let result = Self::worker_patch_source(&mut tx, parent, &request).await?;
            tx.commit().await.map_err(storage)?;
            Ok(result)
        })
    }
    fn validate_worker_import(&self, parent: RunId, inspection: Inspection) -> PortFuture<'_, ()> {
        Box::pin(async move {
            inspection.validate()?;
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let (request, source) = Self::worker_patch_source(
                &mut tx,
                parent,
                &InspectWorkerChanges {
                    dependency: inspection
                        .request
                        .dependency
                        .clone()
                        .ok_or(DomainError::InvalidState)?,
                    offset: inspection.request.offset,
                },
            )
            .await?;
            if request != inspection.request
                || source
                    .changes
                    .as_ref()
                    .is_none_or(|c| c.activity_id != inspection.artifact_id)
            {
                return Err(DomainError::InvalidState.into());
            }
            tx.commit().await.map_err(storage)?;
            Ok(())
        })
    }
}
