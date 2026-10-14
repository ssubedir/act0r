use super::*;
use crate::domain::project_refresh::*;

impl HarnessService<'_> {
    pub async fn project_refresh(&self, scope: RefreshScope) -> Result<RefreshView, Error> {
        self.reader.project_refresh(scope).await
    }
    pub async fn inspect_project_refresh(
        &self,
        scope: RefreshScope,
    ) -> Result<RefreshReview, Error> {
        let context = self.reader.refresh_context(scope.clone()).await?;
        let coding = project_setup::coding_settings(&context.original)?;
        let (snapshot, old) = self
            .verifier()?
            .inspect_refresh(
                context.original.source.ok_or(DomainError::InvalidState)?,
                context.workspace,
            )
            .await?;
        RefreshReview::new(scope, &snapshot, &old, coding).map_err(Into::into)
    }
    pub async fn start_project_refresh(
        &self,
        scope: RefreshScope,
        request: RefreshRequest,
    ) -> Result<RefreshAdmission, Error> {
        if request.coding.is_some() {
            return Err(DomainError::InvalidConfiguration(
                "use project setup review to change coding settings".into(),
            )
            .into());
        }
        self.start_workspace_stage(scope, request).await
    }
    pub async fn update_project_setup(
        &self,
        scope: RefreshScope,
        request: SetupUpdateRequest,
    ) -> Result<RefreshAdmission, Error> {
        self.start_workspace_stage(scope, request.into()).await
    }
    async fn start_workspace_stage(
        &self,
        scope: RefreshScope,
        request: RefreshRequest,
    ) -> Result<RefreshAdmission, Error> {
        request.validate()?;
        let intent = if let Some(intent) = self
            .reader
            .refresh_intent(scope.clone(), request.request_id)
            .await?
        {
            if intent.request != request {
                return Err(DomainError::ConflictingMessage.into());
            }
            intent
        } else {
            let context = self.reader.refresh_context(scope.clone()).await?;
            let (snapshot, _) = self
                .verifier()?
                .inspect_refresh(
                    context.original.source.ok_or(DomainError::InvalidState)?,
                    context.workspace,
                )
                .await?;
            if snapshot.review_digest(&scope)? != request.review_digest {
                return Err(DomainError::SessionConflict(
                    "the project or managed copy changed; review project changes again".into(),
                )
                .into());
            }
            self.reader
                .begin_project_refresh(scope.clone(), request.clone(), snapshot)
                .await?
        };
        if let Some(receipt) = intent.receipt.clone() {
            return Ok(RefreshAdmission {
                conversation_id: intent.root,
                receipt,
            });
        }
        if intent.cancelled {
            return Err(
                DomainError::SessionConflict("refresh preparation was cancelled".into()).into(),
            );
        }
        let result = async {
            let _source = self
                .verifier()?
                .lock_refresh(intent.snapshot.clone())
                .await?;
            let team = self.teams.create(intent.spec.clone()).await?;
            let _copies = self
                .verifier()?
                .lock_repair_copies(&team, &intent.snapshot.source_digest)
                .await?;
            // External editors do not participate in our lock: inspect the source again.
            let (observed, _) = self
                .verifier()?
                .inspect_refresh(
                    intent.snapshot.source.clone(),
                    intent.snapshot.workspace.clone(),
                )
                .await?;
            if observed.review_digest(&scope)? != intent.request.review_digest {
                return Err(DomainError::SessionConflict(
                    "project inputs changed while preparing refresh".into(),
                )
                .into());
            }
            self.reader
                .admit_project_refresh(intent.clone(), team.id, self.teams.now_ms())
                .await
        }
        .await;
        match result {
            Ok(receipt) => Ok(RefreshAdmission {
                conversation_id: intent.root,
                receipt,
            }),
            Err(error) => {
                if let Some(saved) = self
                    .reader
                    .refresh_intent(scope.clone(), request.request_id)
                    .await?
                    && saved.request == request
                    && let Some(receipt) = saved.receipt
                {
                    return Ok(RefreshAdmission {
                        conversation_id: saved.root,
                        receipt,
                    });
                }
                let _ = self
                    .reader
                    .project_refresh_error(scope, request.request_id, error.to_string())
                    .await;
                Err(error)
            }
        }
    }
    pub async fn cancel_project_refresh(
        &self,
        scope: RefreshScope,
        request: MessageId,
    ) -> Result<(), Error> {
        self.reader.cancel_project_refresh(scope, request).await
    }
}
