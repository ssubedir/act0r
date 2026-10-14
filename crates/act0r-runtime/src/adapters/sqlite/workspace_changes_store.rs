use super::*;
use crate::domain::change_review::*;
use serde_json::json;
impl WorkspaceChangesReadPort for SqliteStore {
    fn task_changes(&self, id: MessageId) -> PortFuture<'_, TaskChanges> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let view = Self::saved_task_changes(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(view)
        })
    }
}
impl SqliteStore {
    pub(super) async fn saved_task_changes(
        conn: &mut SqliteConnection,
        id: MessageId,
    ) -> Result<TaskChanges, Error> {
        let task = Self::load_agent_task(conn, id).await?;
        let mut view = TaskChanges {
            task_id: id,
            run_id: task.record().run_id,
            run_status: None,
            enabled: false,
            changes: None,
            error: None,
            completion: None,
        };
        if let Some(run_id) = view.run_id {
            let payload: String = sqlx::query_scalar("SELECT payload FROM runs WHERE id=?")
                .bind(run_id.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
            let run: RunRecord = serde_json::from_str(&payload)?;
            let input = run.input.get("input").unwrap_or(&run.input);
            view.enabled = input["change_review"].as_bool().unwrap_or(false);
            view.run_status = Some(run.status);
            if view.enabled {
                let state = run.state.get("state").unwrap_or(&run.state);
                view.completion = completion_report(state)?;
                let artifact: Option<(String,String)> = sqlx::query_as("SELECT id,json_extract(payload,'$.result') FROM activities WHERE run_id=? AND status='succeeded' AND json_extract(payload,'$.spec.handler.name')='workspace.changes' AND json_extract(payload,'$.spec.handler.version')=1 ORDER BY scheduling_revision DESC,command_ordinal DESC LIMIT 1")
                        .bind(run_id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
                if let Some((activity_id, result)) = artifact {
                    let changes: WorkspaceChangeSet = serde_json::from_str(&result)?;
                    changes.validate()?;
                    if changes.run_id != run_id || changes.activity_id.to_string() != activity_id {
                        return Err(Error::Storage(
                            "saved change-set identity differs from task execution".into(),
                        ));
                    }
                    // An admitted capture can complete after cancellation without a policy
                    // continuation. Never attach the assessment of an older candidate to it.
                    if view
                        .completion
                        .as_ref()
                        .is_some_and(|r| r.activity_id != changes.activity_id)
                    {
                        view.completion = None;
                    }
                    if let Some(report) = &view.completion {
                        let command = serde_json::from_value(input["test_command"].clone())?;
                        let expected = crate::domain::completion::assess(
                            &changes,
                            &command,
                            report.status
                                == crate::domain::completion::CompletionStatus::Incomplete,
                        );
                        if report != &expected {
                            return Err(Error::Storage(
                                "completion assessment differs from its saved change artifact"
                                    .into(),
                            ));
                        }
                    }
                    view.changes = Some(changes);
                }
                view.error = run.error.as_ref().map(|e| e.message.clone());
                if view.changes.is_none()
                    && view.error.is_none()
                    && matches!(
                        run.status,
                        RunStatus::Cancelled | RunStatus::BudgetExhausted
                    )
                {
                    view.error =
                        Some("task stopped before a final change set was committed".into());
                }
            }
        } else {
            let actor = Self::load_actor(conn, task.record().agent_id).await?;
            view.enabled = actor.record().config["input"]["change_review"]
                .as_bool()
                .unwrap_or(false);
        }
        Ok(view)
    }
}
impl ChildChangesReadPort for SqliteStore {
    fn child_changes(
        &self,
        parent: RunId,
        request: child_changes::InspectRequest,
    ) -> PortFuture<'_, TaskChanges> {
        Box::pin(async move {
            request.validate()?;
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let run = Self::load(&mut tx, parent).await?;
            let input = run
                .record()
                .input
                .get("input")
                .unwrap_or(&run.record().input);
            let config: planner::PlannerConfig = serde_json::from_value(input.clone())?;
            if !config.child_changes
                || config.workspace.as_deref() != Some(&request.target.workspace)
                || config.writable_paths != request.target.writable_paths
                || !config
                    .workers
                    .iter()
                    .any(|w| w.name == request.worker && w.agent_id == request.recipient)
            {
                return Err(Error::Unavailable(
                    "inspection differs from the parent's pinned worker or workspace permissions"
                        .into(),
                ));
            }
            let child = run
                .record()
                .delegations
                .iter()
                .rev()
                .find(|d| d.recipient == request.recipient)
                .ok_or_else(|| {
                    Error::Unavailable("no direct task has been delegated to this worker".into())
                })?;
            let outcome = child.outcome.as_ref().ok_or_else(|| {
                Error::Unavailable("child has not returned a terminal result".into())
            })?;
            if child.timed_out_at.is_some()
                || outcome["status"] != "succeeded"
                || outcome["task_id"] != json!(child.id)
            {
                return Err(Error::Unavailable(
                    "the latest direct child did not succeed".into(),
                ));
            }
            let task = Self::load_agent_task(&mut tx, child.id).await?;
            if task.record().agent_id != request.recipient
                || task.record().status != actors::AgentTaskStatus::Succeeded
                || outcome["run_id"] != json!(task.record().run_id)
            {
                return Err(Error::Storage(
                    "child outcome differs from its physical task".into(),
                ));
            }
            let view = Self::saved_task_changes(&mut tx, child.id).await?;
            if view.run_status != Some(RunStatus::Succeeded) || view.changes.is_none() {
                return Err(Error::Unavailable("child has no successful committed change artifact; enable change_review on the worker".into()));
            }
            if let Some(paths) = child
                .workspace_input
                .as_ref()
                .and_then(|p| p.request.writable_paths.as_ref())
                && view.changes.as_ref().is_some_and(|changes| {
                    changes
                        .files
                        .iter()
                        .any(|file| !tools::writable_path_permitted(paths, &file.path))
                })
            {
                return Err(Error::Unavailable("child patch contains files outside its task writable paths; it cannot be imported".into()));
            }
            tx.commit().await.map_err(storage)?;
            Ok(view)
        })
    }
}
