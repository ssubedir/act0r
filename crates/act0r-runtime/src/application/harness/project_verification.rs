use super::*;
use crate::domain::project_verification::*;
impl HarnessService<'_> {
    pub async fn verification_repair(
        &self,
        scope: verification_repair::RepairScope,
    ) -> Result<verification_repair::RepairView, Error> {
        self.reader.verification_repair(scope).await
    }
    pub async fn start_verification_repair(
        &self,
        scope: verification_repair::RepairScope,
        request: verification_repair::RepairRequest,
    ) -> Result<verification_repair::RepairAdmission, Error> {
        let intent = self
            .reader
            .begin_verification_repair(scope.clone(), request)
            .await?;
        if let Some(receipt) = intent.receipt.clone() {
            return Ok(verification_repair::RepairAdmission {
                conversation_id: intent.root,
                receipt,
            });
        }
        let result = async {
            self.verifier()?
                .require_current_project(scope.verification_id)
                .await?;
            let team = self.teams.create(intent.spec.clone()).await?;
            let _copies = self
                .verifier()?
                .lock_repair_copies(&team, &intent.snapshot_digest)
                .await?;
            self.verifier()?
                .require_current_project(scope.verification_id)
                .await?;
            self.reader
                .admit_verification_repair(intent.clone(), team.id, self.teams.now_ms())
                .await
        }
        .await;
        match result {
            Ok(receipt) => Ok(verification_repair::RepairAdmission {
                conversation_id: intent.root,
                receipt,
            }),
            Err(error) => {
                // A concurrent caller may have committed while this caller was
                // checking source/copy ownership. Its receipt is authoritative.
                if let Ok(saved) = self.reader.verification_repair(scope.clone()).await
                    && saved.request_id == Some(intent.request_id)
                    && let Some(receipt) = saved.receipt
                {
                    return Ok(verification_repair::RepairAdmission {
                        conversation_id: intent.root,
                        receipt,
                    });
                }
                let _ = self
                    .reader
                    .verification_repair_error(scope, intent.request_id, error.to_string())
                    .await;
                Err(error)
            }
        }
    }
    pub async fn cancel_verification_repair(
        &self,
        scope: verification_repair::RepairScope,
        request: verification_repair::RepairRequest,
    ) -> Result<(), Error> {
        self.reader.cancel_verification_repair(scope, request).await
    }
    pub(super) fn verifier(&self) -> Result<&super::super::ProjectVerificationService, Error> {
        self.project_verification
            .ok_or_else(|| Error::Unavailable("project verification is not installed".into()))
    }
    pub async fn verify_project(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
        id: RunId,
    ) -> Result<RunId, Error> {
        let view = self.application(session, task, application).await?;
        if view.status != RunStatus::Succeeded
            || view
                .report
                .as_ref()
                .is_none_or(|r| r.status != patch_application::ApplicationStatus::Applied)
        {
            return Err(DomainError::InvalidConfiguration(
                "project verification requires a completed applied patch".into(),
            )
            .into());
        }
        let context = self.coding_context(session, task).await?;
        let preview = view.preview.ok_or(DomainError::InvalidState)?;
        let input = VerificationInput {
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
        let run = self.verifier()?.prepare(id, input)?;
        self.reader
            .create_project_verification(session, task, application, run)
            .await
    }
    async fn owned_verification(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
        id: RunId,
    ) -> Result<Run, Error> {
        let applied = self.application(session, task, application).await?;
        let run = self.engine.status(id).await?;
        if run.record().workflow != workflow()
            || run.record().input["application_id"] != serde_json::json!(application)
            || run.record().input["task_id"] != serde_json::json!(task)
            || run.record().input["target"]
                != serde_json::json!(applied.preview.ok_or(DomainError::InvalidState)?.target)
        {
            return Err(Error::NotFound(id));
        }
        Ok(run)
    }
    pub async fn project_verifications(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
    ) -> Result<VerificationPage, Error> {
        self.application(session, task, application).await?;
        self.reader
            .project_verifications(session, task, application)
            .await
    }
    pub async fn project_verification(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
        id: RunId,
    ) -> Result<super::super::ProjectVerificationView, Error> {
        self.verifier()?
            .status(
                self.owned_verification(session, task, application, id)
                    .await?,
            )
            .await
    }
    pub async fn respond_project_verification(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
        id: RunId,
        wait: WaitId,
        approved: bool,
    ) -> Result<(), Error> {
        self.verifier()?
            .respond(
                self.owned_verification(session, task, application, id)
                    .await?,
                wait,
                approved,
            )
            .await
    }
    pub async fn reconcile_project_verification(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
        id: RunId,
        request: tool_recovery::ToolRecoveryRequest,
    ) -> Result<(), Error> {
        self.verifier()?
            .reconcile(
                self.owned_verification(session, task, application, id)
                    .await?,
                request,
            )
            .await
    }
}
