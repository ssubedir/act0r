use super::{Error, TeamService, ports::HarnessStorePort};
use crate::domain::{harness::*, *};
mod project_refresh;
mod project_verification;

/// Assemble actors and submit through existing durable setup/mailbox boundaries.
pub struct HarnessService<'a> {
    pub(crate) teams: &'a TeamService,
    pub(crate) reader: &'a dyn HarnessStorePort,
    pub(crate) engine: &'a super::Engine,
    pub(crate) actors: Option<&'a super::ActorService>,
    pub(crate) recovery: Option<&'a super::ToolRecoveryService>,
    pub(crate) project_verification: Option<&'a super::ProjectVerificationService>,
    pub(crate) import_recovery: Option<&'a super::ChildImportRecoveryService>,
}
impl HarnessService<'_> {
    pub async fn continuous_conversation(
        &self,
        session: SessionId,
        before: Option<conversation::ConversationCursor>,
        task: Option<MessageId>,
    ) -> Result<conversation::ContinuousConversation, Error> {
        self.reader
            .continuous_conversation(session, before, task)
            .await
    }
    pub async fn conversation_sessions(
        &self,
        query: sessions::SessionListQuery,
    ) -> Result<HarnessSessions, Error> {
        query.validate()?;
        self.reader.conversation_sessions(query).await
    }
    pub async fn worker_pause(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<worker_pause::WorkerPause, Error> {
        self.reader.worker_pause(session, turn, task).await
    }
    pub async fn control_assignment_pause(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: worker_pause::AssignmentPauseRequest,
    ) -> Result<worker_pause::AssignmentPauseReceipt, Error> {
        request.validate()?;
        self.reader
            .control_assignment_pause(session, turn, task, request, self.teams.now_ms())
            .await
    }
    pub async fn worker_cancellation(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<worker_cancellation::WorkerCancellation, Error> {
        self.reader.worker_cancellation(session, turn, task).await
    }
    pub async fn stop_assignment(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: worker_cancellation::StopAssignmentRequest,
    ) -> Result<worker_cancellation::StopAssignmentReceipt, Error> {
        request.validate()?;
        self.reader
            .stop_assignment(
                session,
                turn,
                task,
                request,
                self.teams.now_ms(),
                self.actors()?.cancellation_decision(),
            )
            .await
    }
    pub async fn delegated_work(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<super::DelegatedWorkView, Error> {
        self.reader.harness_delegated_work(session, turn).await
    }
    pub async fn permissions(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<permissions::SessionPermissions, Error> {
        self.reader.session_permissions(session, turn).await
    }
    pub async fn change_permissions(
        &self,
        session: SessionId,
        request: permissions::PermissionRequest,
    ) -> Result<permissions::PermissionReceipt, Error> {
        request.validate()?;
        self.reader
            .change_permissions(
                session,
                request,
                self.teams.now_ms(),
                self.engine.test_permission_decision(),
                self.engine.task_edit_permission_decision(),
                self.engine.task_test_permission_decision(),
            )
            .await
    }
    pub async fn turn_steering(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<steering::TurnSteering, Error> {
        self.reader.turn_steering(session, turn).await
    }
    pub async fn send_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        request: steering::SteeringRequest,
    ) -> Result<steering::SteeringReceipt, Error> {
        request.validate()?;
        self.reader
            .send_steering(session, turn, request, self.teams.now_ms())
            .await
    }
    pub async fn worker_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<steering::TurnSteering, Error> {
        self.reader.worker_steering(session, turn, task).await
    }
    pub async fn send_worker_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: steering::SteeringRequest,
    ) -> Result<steering::SteeringReceipt, Error> {
        request.validate()?;
        self.reader
            .send_worker_steering(session, turn, task, request, self.teams.now_ms())
            .await
    }
    pub async fn turn_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<turn_recovery::TurnRecovery, Error> {
        self.reader.turn_recovery(session, turn).await
    }
    pub async fn retry_turn(
        &self,
        session: SessionId,
        turn: MessageId,
        request: turn_recovery::RetryTurnRequest,
    ) -> Result<turn_recovery::RetryTurnReceipt, Error> {
        request.validate()?;
        let engine = self.engine.clone();
        self.reader
            .retry_turn(
                session,
                turn,
                request,
                self.teams.now_ms(),
                Box::new(move |run| {
                    if let Some(reason) = engine.incompatibility(run) {
                        return Err(Error::Unavailable(reason));
                    }
                    Ok(())
                }),
            )
            .await
    }
    pub async fn worker_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<worker_recovery::WorkerRecovery, Error> {
        self.reader.worker_recovery(session, turn, task).await
    }
    pub async fn decide_worker_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: worker_recovery::WorkerRecoveryRequest,
    ) -> Result<worker_recovery::WorkerRecoveryReceipt, Error> {
        request.validate()?;
        let engine = self.engine.clone();
        self.reader
            .decide_worker_recovery(
                session,
                turn,
                task,
                request,
                self.teams.now_ms(),
                Box::new(move |run| {
                    if let Some(reason) = engine.incompatibility(run) {
                        return Err(Error::Unavailable(reason));
                    }
                    Ok(())
                }),
            )
            .await
    }
    pub async fn summary_policy(
        &self,
        session: SessionId,
        request: session_summary::SummaryPolicyRequest,
    ) -> Result<session_summary::SummaryPolicyReceipt, Error> {
        request.validate()?;
        self.reader.change_summary_policy(session, request).await
    }
    pub async fn summary(
        &self,
        session: SessionId,
    ) -> Result<session_summary::SessionSummary, Error> {
        self.reader.session_summary(session).await
    }
    pub async fn summarize(
        &self,
        session: SessionId,
        request: session_summary::SummaryRequest,
    ) -> Result<session_summary::SummaryReceipt, Error> {
        request.validate()?;
        let now = self.teams.now_ms();
        self.reader
            .start_session_summary(
                session,
                request,
                now,
                Box::new(move |input| {
                    input.validate()?;
                    Ok(Run::start(
                        &session_summary::SessionSummaryWorkflow,
                        serde_json::to_value(input)?,
                        8,
                        now,
                    )?)
                }),
            )
            .await
    }
    pub async fn memory(&self, session: SessionId) -> Result<session_memory::SessionMemory, Error> {
        let mut actors = Vec::new();
        for (member, actor) in self.reader.session_memory_actors(session).await? {
            let memory = self.actors()?.inspect_memory(&actor)?;
            let retained_bytes = memory.as_ref().map(|m| m.retained_bytes()).unwrap_or(0);
            actors.push(session_memory::SessionActorMemory {
                agent_id: member.agent_id,
                name: member.name,
                role: member.role,
                memory,
                retained_bytes,
            });
        }
        Ok(session_memory::SessionMemory {
            session_id: session,
            actors,
        })
    }
    pub async fn turn_memory(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<session_memory::TurnMemory, Error> {
        self.reader.turn_memory(session, turn).await
    }
    pub async fn clear_memory(
        &self,
        session: SessionId,
        agent: AgentId,
        request: session_memory::ClearMemoryRequest,
    ) -> Result<session_memory::ClearMemoryReceipt, Error> {
        request.validate()?;
        let decision = self.actors()?.session_memory_decision(request.clone());
        self.reader
            .clear_session_memory(session, agent, request, self.teams.now_ms(), decision)
            .await
    }
    fn recovery(&self) -> Result<&super::ToolRecoveryService, Error> {
        self.recovery
            .ok_or_else(|| Error::Unavailable("tool recovery is not installed".into()))
    }
    pub async fn tool_recoveries(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<super::ToolRecoveries, Error> {
        let context = self.coding_context(session, turn).await?;
        let goal_paused = context.paused;
        let (runs, truncated) = self.reader.harness_tool_runs(session, turn).await?;
        let mut items = Vec::new();
        for (task, run) in runs {
            let mut view = self.recovery()?.status(task, run).await?;
            if let Some(recovery) = &mut view.recovery {
                recovery.paused |= goal_paused;
            }
            if view.recovery.is_some() || !view.reconciliation_receipts.is_empty() {
                items.push(view);
            }
        }
        Ok(super::ToolRecoveries { items, truncated })
    }
    pub async fn reconcile_tool(
        &self,
        session: SessionId,
        turn: MessageId,
        id: RunId,
        request: tool_recovery::ToolRecoveryRequest,
    ) -> Result<(), Error> {
        self.coding_context(session, turn).await?;
        let (_, run) = self.reader.harness_tool_run(session, turn, id).await?;
        self.recovery()?.reconcile(run, request).await
    }
    fn import_recovery(&self) -> Result<&super::ChildImportRecoveryService, Error> {
        self.import_recovery
            .ok_or_else(|| Error::Unavailable("child import recovery is not installed".into()))
    }
    pub async fn child_import_recoveries(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<super::ChildImportRecoveries, Error> {
        let context = self.coding_context(session, turn).await?;
        let (runs, truncated) = self.reader.harness_tool_runs(session, turn).await?;
        let mut items = Vec::new();
        for (task, run) in runs {
            let mut view = self.import_recovery()?.status(task, run).await?;
            if let Some(recovery) = &mut view.recovery {
                recovery.paused |= context.paused;
                recovery.cancelled = context.cancelled;
                recovery.retry_allowed &= !context.cancelled;
            }
            if view.recovery.is_some() || !view.reconciliation_receipts.is_empty() {
                items.push(view);
            }
        }
        Ok(super::ChildImportRecoveries { items, truncated })
    }
    pub async fn reconcile_child_import(
        &self,
        session: SessionId,
        turn: MessageId,
        id: RunId,
        request: tool_recovery::ToolRecoveryRequest,
    ) -> Result<(), Error> {
        self.coding_context(session, turn).await?;
        let (_, run) = self.reader.harness_tool_run(session, turn, id).await?;
        self.import_recovery()?.reconcile(run, request).await
    }
    pub(crate) fn actors(&self) -> Result<&super::ActorService, Error> {
        self.actors
            .ok_or_else(|| Error::Unavailable("harness coding is not installed".into()))
    }
    async fn coding_context(
        &self,
        session: SessionId,
        task: MessageId,
    ) -> Result<HarnessTurnContext, Error> {
        let context = self.reader.harness_turn_context(session, task).await?;
        if context.coding.is_none() || context.workspace.is_none() {
            return Err(DomainError::InvalidConfiguration(
                "this session is read-only; start a coding session".into(),
            )
            .into());
        }
        Ok(context)
    }
    pub async fn changes(
        &self,
        session: SessionId,
        task: MessageId,
    ) -> Result<change_review::TaskChanges, Error> {
        self.coding_context(session, task).await?;
        self.actors()?.task_changes(task).await
    }
    pub async fn preview(
        &self,
        session: SessionId,
        task: MessageId,
        id: RunId,
    ) -> Result<RunId, Error> {
        let context = self.coding_context(session, task).await?;
        let changes = self.actors()?.task_changes(task).await?;
        validate_application_changes(&changes)?;
        let run = self
            .actors()?
            .patch_applications()?
            .prepare_preview(
                id,
                task,
                context.workspace.ok_or(DomainError::InvalidState)?.into(),
            )
            .await?;
        self.reader
            .create_harness_application(session, task, run)
            .await
    }
    pub async fn applications(
        &self,
        session: SessionId,
        task: MessageId,
    ) -> Result<HarnessApplications, Error> {
        self.coding_context(session, task).await?;
        self.reader.harness_applications(session, task).await
    }
    pub async fn application(
        &self,
        session: SessionId,
        task: MessageId,
        id: RunId,
    ) -> Result<super::patch_applications::ApplicationView, Error> {
        let context = self.coding_context(session, task).await?;
        let run = self.engine.status(id).await?;
        if run.record().workflow != patch_application::workflow()
            || run.record().input["task_id"] != serde_json::json!(task)
            || run.record().input["target"] != serde_json::json!(context.workspace)
        {
            return Err(Error::NotFound(id));
        }
        self.actors()?.patch_applications()?.status(id).await
    }
    pub async fn respond_application(
        &self,
        session: SessionId,
        task: MessageId,
        id: RunId,
        wait: WaitId,
        approved: bool,
    ) -> Result<(), Error> {
        self.application(session, task, id).await?;
        self.actors()?
            .patch_applications()?
            .respond(id, wait, approved)
            .await
    }
    pub async fn reconcile_application(
        &self,
        session: SessionId,
        task: MessageId,
        id: RunId,
        request: patch_application::ApplicationRecoveryRequest,
    ) -> Result<(), Error> {
        self.application(session, task, id).await?;
        self.actors()?
            .patch_applications()?
            .reconcile(id, request)
            .await
    }
    pub async fn start(&self, request: HarnessRequest) -> Result<HarnessReceipt, Error> {
        let spec = self.teams.harness_spec(&request)?;
        self.start_spec(request, spec).await
    }
    pub(crate) async fn start_spec(
        &self,
        request: HarnessRequest,
        spec: teams::TeamSpec,
    ) -> Result<HarnessReceipt, Error> {
        let team = self.teams.create(spec).await?;
        self.reader
            .start_harness_turn(
                team.id,
                HarnessTurnRequest {
                    message_id: request.message_id,
                    prompt: request.prompt,
                },
                self.teams.now_ms(),
            )
            .await
    }
    pub async fn sessions(&self) -> Result<HarnessSessions, Error> {
        self.sessions_page(Default::default()).await
    }
    pub async fn sessions_page(
        &self,
        query: sessions::SessionListQuery,
    ) -> Result<HarnessSessions, Error> {
        query.validate()?;
        self.reader.harness_sessions(query).await
    }
    pub async fn session_metadata(
        &self,
        session: SessionId,
    ) -> Result<sessions::SessionMetadataView, Error> {
        self.reader.session_metadata(session).await
    }
    pub async fn update_session(
        &self,
        session: SessionId,
        request: sessions::SessionUpdate,
    ) -> Result<sessions::SessionUpdateReceipt, Error> {
        request.validate()?;
        self.reader.update_session(session, request).await
    }
    pub async fn conversation(
        &self,
        session: SessionId,
        before: Option<u64>,
    ) -> Result<HarnessConversation, Error> {
        self.reader.harness_conversation(session, before).await
    }
    pub async fn append(
        &self,
        session: SessionId,
        turn: HarnessTurnRequest,
    ) -> Result<HarnessReceipt, Error> {
        turn.validate()?;
        self.reader
            .append_harness_turn(session, turn, self.teams.now_ms())
            .await
    }
}
