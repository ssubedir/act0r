//! Read only persisted facts under an exact session turn, in a single transaction.
use super::*;
use crate::application::{
    ChildPatchReview, DelegatedWorkView, TaskInspectionOptions, assemble_delegated_work,
    delegated_text,
};
use std::collections::HashMap;

impl SqliteStore {
    pub(super) async fn read_delegated_work(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
    ) -> Result<DelegatedWorkView, Error> {
        let snapshot =
            Self::inspection_subtree(conn, turn, TaskInspectionOptions::default()).await?;
        let accepted: std::collections::HashSet<_> = snapshot
            .nodes
            .iter()
            .filter(|n| n.task.is_some())
            .map(|n| n.id)
            .collect();
        let planner_run = snapshot
            .nodes
            .iter()
            .find(|n| n.id == turn)
            .and_then(|n| n.run.as_ref())
            .map(|r| r.record.id);
        let mut changes = HashMap::new();
        let mut reviews = HashMap::new();
        let mut scopes = HashMap::new();
        for parent in &snapshot.nodes {
            let Some(run) = &parent.run else {
                continue;
            };
            for child in &run.record.delegations {
                if !snapshot
                    .nodes
                    .iter()
                    .any(|n| n.id == child.id && n.task.is_some())
                {
                    continue;
                }
                let saved = Self::saved_task_changes(conn, child.id).await?;
                let node = snapshot
                    .nodes
                    .iter()
                    .find(|n| n.id == child.id)
                    .ok_or(DomainError::InvalidState)?;
                let input = if let Some(run) = &node.run {
                    run.record.input["input"].clone()
                } else {
                    Self::load_actor(conn, child.recipient)
                        .await?
                        .record()
                        .config["input"]
                        .clone()
                };
                let paths: Vec<String> = serde_json::from_value(
                    input
                        .get("writable_paths")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!([])),
                )?;
                tools::validate_writable_paths(&paths)?;
                scopes.insert(child.id, paths);
                if let Some(artifact) = &saved.changes {
                    reviews.insert(
                        child.id,
                        Self::child_patch_review(conn, &run.record, child.id, artifact.activity_id)
                            .await?,
                    );
                }
                changes.insert(child.id, saved);
            }
        }
        let mut view = assemble_delegated_work(session, turn, snapshot, changes, reviews, scopes)?;
        for group in &mut view.groups {
            for child in &mut group.children {
                if !accepted.contains(&child.task_id) {
                    continue;
                }
                child.cancellation = Some(
                    Self::worker_cancellation_preview(conn, session, turn, child.task_id).await?,
                );
                child.pause =
                    Some(Self::worker_pause_preview(conn, session, turn, child.task_id).await?);
            }
        }
        if Self::harness_context(conn, session, turn)
            .await?
            .coding
            .is_some()
        {
            if let Some(run_id) = planner_run {
                view.coordination =
                    Some(Self::planner_coordination(conn, session, turn, run_id).await?);
            }
            for group in &mut view.groups {
                for child in &mut group.children {
                    if !accepted.contains(&child.task_id) {
                        continue;
                    }
                    child.recovery = Some(
                        Self::worker_recovery_preview(conn, session, turn, child.task_id)
                            .await?
                            .0,
                    );
                    child.steering = Some(
                        Self::worker_steering_preview(conn, session, turn, child.task_id)
                            .await?
                            .0,
                    );
                    child.messages = Some(
                        Self::worker_message_history(conn, session, turn, child.task_id).await?,
                    );
                    child.questions = Some(
                        Self::worker_question_history(conn, session, turn, child.task_id).await?,
                    );
                }
            }
        }
        Ok(view)
    }
    async fn child_patch_review(
        conn: &mut SqliteConnection,
        parent: &RunRecord,
        child: MessageId,
        artifact: ActivityId,
    ) -> Result<ChildPatchReview, Error> {
        let mut review = ChildPatchReview {
            artifact_id: artifact,
            status: "not_reviewed",
            inspection_id: None,
            import_activity_id: None,
            detail: None,
        };
        let inspected: Option<String> = sqlx::query_scalar("SELECT json_extract(payload,'$.result.inspection') FROM activities WHERE run_id=? AND status='succeeded' AND json_extract(payload,'$.spec.handler.name')='planner.child_changes.inspect' AND json_extract(payload,'$.spec.handler.version')=1 AND json_extract(payload,'$.result.inspection.task_id')=? AND json_extract(payload,'$.result.inspection.artifact_id')=? ORDER BY scheduling_revision DESC,command_ordinal DESC LIMIT 1")
            .bind(parent.id.to_string()).bind(child.to_string()).bind(artifact.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        if let Some(value) = inspected {
            let inspection: child_changes::Inspection = serde_json::from_str(&value)?;
            inspection.validate()?;
            review.inspection_id = Some(inspection.preview.preparation_id);
            review.status = if inspection.preview.has_conflicts() {
                "conflicted"
            } else {
                "reviewed"
            };
            if inspection.preview.has_conflicts() {
                review.detail = Some("The saved preview found file conflicts. No import is approved by this preview.".into());
            }
        }
        let imported: Option<String> = sqlx::query_scalar("SELECT payload FROM activities WHERE run_id=? AND json_extract(payload,'$.spec.handler.name')='planner.child_changes.import' AND json_extract(payload,'$.spec.handler.version')=1 AND json_extract(payload,'$.spec.input.task_id')=? AND json_extract(payload,'$.spec.input.artifact_id')=? ORDER BY scheduling_revision DESC,command_ordinal DESC LIMIT 1")
            .bind(parent.id.to_string()).bind(child.to_string()).bind(artifact.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        let mut import_sequence = 0i64;
        if let Some(value) = imported {
            let activity: Activity = serde_json::from_str(&value)?;
            review.import_activity_id = Some(activity.id);
            let inspection: child_changes::Inspection =
                serde_json::from_value(activity.spec.input)?;
            inspection.validate()?;
            review.inspection_id = Some(inspection.preview.preparation_id);
            import_sequence = sqlx::query_scalar("SELECT sequence FROM history WHERE run_id=? AND json_extract(payload,'$.activity_id')=? AND json_extract(payload,'$.kind')='activity_scheduled' LIMIT 1")
                .bind(parent.id.to_string()).bind(activity.id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            review.status = match activity.status {
                ActivityStatus::Succeeded => {
                    let report: patch_application::ApplicationReport =
                        serde_json::from_value(activity.result.ok_or(DomainError::InvalidState)?)?;
                    if report.run_id != inspection.preview.run_id
                        || report.target != inspection.preview.target
                    {
                        return Err(DomainError::InvalidState.into());
                    }
                    match report.status {
                        patch_application::ApplicationStatus::Applied => "imported",
                        patch_application::ApplicationStatus::Conflicted => "conflicted",
                        patch_application::ApplicationStatus::Rejected => "rejected",
                    }
                }
                ActivityStatus::Indeterminate => "interrupted",
                ActivityStatus::Failed => "failed",
                ActivityStatus::Queued if parent.status == RunStatus::Cancelled => "cancelled",
                ActivityStatus::Queued if parent.status == RunStatus::Blocked => "blocked",
                _ => "importing",
            };
            review.detail = activity.error.map(|e| delegated_text(&e.message, 2048));
        }
        // Approval receipts bind to the exact wait and artifact, independent of the live event tail.
        let waited: Option<(i64, String)> = sqlx::query_as("SELECT sequence,json_extract(payload,'$.payload.wait') FROM history WHERE run_id=? AND json_extract(payload,'$.kind')='input_wait_created' AND json_extract(payload,'$.payload.wait.key')='child_changes_approval' AND COALESCE(json_type(payload,'$.payload.wait.payload.inspection.request.dependency'),'null')='null' AND json_extract(payload,'$.payload.wait.payload.inspection.task_id')=? AND json_extract(payload,'$.payload.wait.payload.inspection.artifact_id')=? ORDER BY sequence DESC LIMIT 1")
            .bind(parent.id.to_string()).bind(child.to_string()).bind(artifact.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        if let Some((sequence, value)) = waited
            && sequence > import_sequence
        {
            let wait: HumanWait = serde_json::from_str(&value)?;
            let inspection: child_changes::Inspection =
                serde_json::from_value(wait.payload["inspection"].clone())?;
            inspection.validate()?;
            review.inspection_id = Some(inspection.preview.preparation_id);
            review.detail = None;
            review.status = match parent
                .input_receipts
                .iter()
                .find(|r| r.wait_id == wait.id)
                .and_then(|r| r.payload["approved"].as_bool())
            {
                Some(false) => "declined",
                Some(true) => "approved",
                None if parent.wait.as_ref().is_some_and(|w| w.id == wait.id)
                    && parent.status.is_unfinished() =>
                {
                    "awaiting_approval"
                }
                None => "approval_closed",
            };
        }
        Ok(review)
    }
}
