use super::*;
use crate::domain::{harness::HarnessTurnContext, patch_application, project_verification::*};
use serde_json::json;
impl SqliteStore {
    pub(super) async fn verification_application(
        conn: &mut SqliteConnection,
        session: SessionId,
        task: MessageId,
        application: RunId,
    ) -> Result<(HarnessTurnContext, Run), Error> {
        let context = Self::harness_context(conn, session, task).await?;
        let applied = Self::load(conn, application).await?;
        if context.coding.is_none()
            || applied.record().workflow != patch_application::workflow()
            || applied.record().input["task_id"] != json!(task)
            || applied.record().input["target"] != json!(context.workspace)
        {
            return Err(Error::NotFound(application));
        }
        Ok((context, applied))
    }
    pub(super) async fn admit_project_verification(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
        run: Run,
    ) -> Result<RunId, Error> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let (context, applied) =
            Self::verification_application(&mut tx, session, task, application).await?;
        let preview: patch_application::ApplicationPreview =
            serde_json::from_value(applied.record().state["preview"].clone())?;
        let expected = VerificationInput {
            application_id: application,
            task_id: task,
            target: preview.target,
            root_identity: preview.root_identity,
            applied_files: preview
                .files
                .into_iter()
                .map(|f| AppliedFile {
                    path: f.path,
                    sha256: f.after_sha256,
                })
                .collect(),
            command: context
                .coding
                .ok_or(DomainError::InvalidState)?
                .test_command,
        };
        if run.record().workflow != workflow()
            || run.record().input != json!(expected)
            || applied.record().status != RunStatus::Succeeded
            || applied
                .record()
                .result
                .as_ref()
                .is_none_or(|r| r["status"] != "applied")
        {
            return Err(DomainError::InvalidConfiguration(
                "verification must pin the applied project and session's exact reviewed command"
                    .into(),
            )
            .into());
        }
        let id = run.record().id;
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE id=?)")
            .bind(id.to_string())
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        if exists {
            let saved = Self::load(&mut tx, id).await?;
            if saved.record().workflow != run.record().workflow
                || saved.record().input != run.record().input
            {
                return Err(DomainError::ConflictingMessage.into());
            }
        } else {
            if Self::conversation_archived(&mut tx, session).await? {
                return Err(DomainError::SessionConflict(
                    "restore the conversation before creating project verification".into(),
                )
                .into());
            }
            let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE json_extract(payload,'$.workflow.name')='project.verify' AND json_extract(payload,'$.input.application_id')=?").bind(application.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
            if count >= 20 {
                return Err(DomainError::InvalidConfiguration(
                    "this application already has 20 project verification runs".into(),
                )
                .into());
            }
            let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE json_extract(payload,'$.workflow.name')='project.verify' AND json_extract(payload,'$.input.application_id')=? AND status NOT IN ('succeeded','failed','cancelled','budget_exhausted'))").bind(application.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
            if pending {
                return Err(DomainError::SessionConflict(
                    "finish or recover the existing project verification first".into(),
                )
                .into());
            }
            self.save(&mut tx, &run, true).await?;
        }
        tx.commit().await.map_err(storage)?;
        Ok(id)
    }
    pub(super) async fn read_project_verifications(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
    ) -> Result<VerificationPage, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        Self::verification_application(&mut tx, session, task, application).await?;
        let rows:Vec<(String,String,Option<String>)>=sqlx::query_as("SELECT id,status,json_extract(payload,'$.result.status') FROM runs WHERE json_extract(payload,'$.workflow.name')='project.verify' AND json_extract(payload,'$.input.task_id')=? AND json_extract(payload,'$.input.application_id')=? ORDER BY created_at DESC,id DESC LIMIT 21").bind(task.to_string()).bind(application.to_string()).fetch_all(&mut *tx).await.map_err(storage)?;
        let truncated = rows.len() > 20;
        let mut items = Vec::new();
        for (id, status, outcome) in rows.into_iter().take(20) {
            items.push(VerificationSummary {
                run_id: id.parse().map_err(|_| DomainError::InvalidState)?,
                status: serde_json::from_value(json!(status))?,
                outcome: outcome
                    .map(|s| serde_json::from_value(json!(s)))
                    .transpose()?,
            });
        }
        tx.commit().await.map_err(storage)?;
        Ok(VerificationPage { items, truncated })
    }
}
