use super::{Error, harness::HarnessService};
use crate::domain::{harness::*, project_setup::*, *};

impl HarnessService<'_> {
    /// Read immutable Run evidence after verifying session/task ownership.
    pub async fn project_setup(
        &self,
        session: SessionId,
        task: MessageId,
    ) -> Result<ProjectSetupView, Error> {
        self.reader.harness_turn_context(session, task).await?;
        let conversation = self.reader.harness_conversation(session, None).await?;
        let team = self.teams.get(conversation.team_id).await?;
        if team.spec.planner.config["discover_project"] != true {
            return Err(DomainError::InvalidConfiguration(
                "this session is not discovering a project".into(),
            )
            .into());
        }
        let actor_task = self.actors()?.task(task).await?;
        let mut proposal = None;
        if let Some(run_id) = actor_task.run_id {
            let run = self.engine.status(run_id).await?;
            if run.record().status == RunStatus::Succeeded
                && let Some(value) = run
                    .record()
                    .result
                    .as_ref()
                    .and_then(|v| v.get("project_setup"))
            {
                let saved: ProjectSetupProposal = serde_json::from_value(value.clone())?;
                saved.coding()?;
                proposal = Some(saved);
            }
        }
        let coding_id = coding_start_id(session, task);
        let coding_session_id = coding_id
            .to_string()
            .parse()
            .map_err(|_| DomainError::InvalidState)?;
        let (accepted, accepted_coding) = match self
            .reader
            .harness_conversation(coding_session_id, None)
            .await
        {
            Ok(saved) => {
                // Locate the first turn independently of the conversation page limit.
                self.reader
                    .harness_turn_context(coding_session_id, coding_id)
                    .await?;
                (
                    Some(HarnessReceipt {
                        session_id: coding_session_id,
                        team_id: saved.team_id,
                        task_id: coding_id,
                        sequence: 1,
                    }),
                    saved.coding,
                )
            }
            Err(Error::SessionNotFound(_)) => (None, None),
            Err(error) => return Err(error),
        };
        let coding_team = self
            .teams
            .harness_start(format!("{SESSION_PREFIX}{coding_id}"))
            .await?;
        let discovery_context = if let Some(saved) = coding_team {
            // Restart recovery reuses the captured discovery evidence.
            Some(serde_json::from_value(
                saved.spec.planner.config["discovery_context"].clone(),
            )?)
        } else if proposal.is_some() {
            let run_id = actor_task.run_id.ok_or(DomainError::InvalidState)?;
            let run = self.engine.status(run_id).await?;
            Some(discovery_context::DiscoveryContext::capture(
                session,
                task,
                &run,
                &self.engine.history(run_id).await?,
                team.planner()?
                    .workspace
                    .as_ref()
                    .ok_or(DomainError::InvalidState)?
                    .snapshot_digest
                    .clone(),
            )?)
        } else {
            None
        };
        Ok(ProjectSetupView {
            auto_execute: conversation.auto_execute,
            session_id: session,
            task_id: task,
            run_id: actor_task.run_id,
            workspace: team.spec.source.ok_or(DomainError::InvalidState)?,
            can_accept: proposal.is_some() && !conversation.archived && accepted.is_none(),
            proposal,
            coding_session_id,
            accepted,
            accepted_coding,
            discovery_context,
        })
    }
    /// Exact reviewed settings admit one stable new coding session. Its Run uses
    /// the discovery goal/model and remaining allowance, never browser defaults.
    pub async fn accept_project_setup(
        &self,
        session: SessionId,
        task: MessageId,
        review: ReviewProjectSetup,
    ) -> Result<HarnessReceipt, Error> {
        review.coding.validate()?;
        let view = self.project_setup(session, task).await?;
        if view.auto_execute
            && view
                .proposal
                .as_ref()
                .map(ProjectSetupProposal::coding)
                .transpose()?
                .as_ref()
                != Some(&review.coding)
        {
            return Err(DomainError::ConflictingMessage.into());
        }
        if let Some(receipt) = view.accepted {
            if view.accepted_coding.as_ref() != Some(&review.coding) {
                return Err(DomainError::ConflictingMessage.into());
            }
            return Ok(receipt);
        }
        if view.proposal.is_none() {
            return Err(DomainError::InvalidConfiguration(
                "wait for a saved setup proposal before continuing".into(),
            )
            .into());
        }
        let conversation = self.reader.harness_conversation(session, None).await?;
        let team = self.teams.get(conversation.team_id).await?;
        let run = self
            .engine
            .status(view.run_id.ok_or(DomainError::InvalidState)?)
            .await?;
        let config: planner::PlannerConfig =
            serde_json::from_value(run.record().input["input"].clone())?;
        if !config.discover_project {
            return Err(DomainError::InvalidState.into());
        }
        let goal = config.goal;
        let target = config.target;
        let budget: UsageBudget =
            serde_json::from_value(team.spec.planner.config["goal_budget"].clone())?;
        let request = HarnessRequest {
            auto_execute: view.auto_execute,
            discover_project: false,
            message_id: coding_start_id(session, task),
            prompt: goal,
            target,
            workspace: Some(view.workspace),
            coding: Some(review.coding),
            budget: Some(remaining_budget(&budget, &run.record().usage)?),
        };
        let mut spec = request.team_spec()?;
        if let Some(saved) = self.teams.harness_start(spec.name.clone()).await? {
            let mut original = saved.spec.clone();
            original
                .planner
                .config
                .as_object_mut()
                .ok_or(DomainError::InvalidState)?
                .remove("discovery_context");
            for worker in &mut original.workers {
                worker
                    .config
                    .as_object_mut()
                    .ok_or(DomainError::InvalidState)?
                    .remove("discovery_context");
            }
            if original != spec {
                return Err(DomainError::ConflictingMessage.into());
            }
            spec = saved.spec;
        } else {
            if !view.can_accept {
                return Err(DomainError::InvalidConfiguration(
                    "restore the discovery session before accepting setup".into(),
                )
                .into());
            }
            let snapshot = view.discovery_context.ok_or(DomainError::InvalidState)?;
            snapshot.validate()?;
            let value = serde_json::to_value(snapshot)?;
            spec.planner.config["discovery_context"] = value.clone();
            for worker in &mut spec.workers {
                worker.config["discovery_context"] = value.clone();
            }
        }
        let team = self.teams.create(spec).await?;
        self.reader
            .start_project_coding_turn(
                team.id,
                HarnessTurnRequest {
                    message_id: request.message_id,
                    prompt: request.prompt,
                },
                conversation::ProjectContinuation {
                    session_id: session,
                    task_id: task,
                },
                self.teams.now_ms(),
            )
            .await
    }
}

/// Continue Auto requests through the same durable team setup and mailbox path.
/// The worker drives this even when every browser is closed.
impl super::ports::WorkCoordinator for super::TeamService {
    fn advance(&self) -> super::ports::PortFuture<'_, bool> {
        Box::pin(async move {
            let harness = self.harness()?;
            let Some(source) = harness.reader.pending_auto_setup().await? else {
                return Ok(false);
            };
            let view = harness
                .project_setup(source.session_id, source.task_id)
                .await?;
            let coding = view.proposal.ok_or(DomainError::InvalidState)?.coding()?;
            match harness
                .accept_project_setup(
                    source.session_id,
                    source.task_id,
                    ReviewProjectSetup { coding },
                )
                .await
            {
                Ok(_) => Ok(true),
                Err(error) => {
                    let context = harness
                        .reader
                        .harness_turn_context(source.session_id, source.task_id)
                        .await?;
                    if context.paused
                        || context.cancelled
                        || harness
                            .conversation(source.session_id, None)
                            .await?
                            .archived
                    {
                        return Ok(false);
                    }
                    if matches!(error, super::Error::StorageBusy(_)) {
                        return Ok(false);
                    }
                    harness
                        .reader
                        .auto_setup_failed(
                            source,
                            error.to_string().chars().take(4096).collect(),
                            self.now_ms(),
                        )
                        .await?;
                    Ok(true)
                }
            }
        })
    }
    fn has_pending(&self) -> super::ports::PortFuture<'_, bool> {
        Box::pin(async move { Ok(self.harness()?.reader.pending_auto_setup().await?.is_some()) })
    }
}
