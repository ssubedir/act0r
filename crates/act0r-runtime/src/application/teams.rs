use super::{
    ActorService, AgentRegistry, Engine, Error, TeamEventService, TeamGoalControlService,
    TeamGoalListService, TeamTaskService, ports::*,
};
use crate::domain::{actors::*, teams::*, *};
use serde_json::json;
use std::{path::Path, sync::Arc};

pub struct TeamService {
    engine: Engine,
    store: Arc<dyn TeamStore>,
    workspaces: Arc<dyn TeamWorkspaces>,
    registry: Arc<AgentRegistry>,
    tasks: Option<TeamTaskService>,
    events: Option<TeamEventService>,
    controls: Option<TeamGoalControlService>,
    goals: Option<TeamGoalListService>,
    harness_reader: Option<Arc<dyn HarnessStorePort>>,
    harness_actors: Option<Arc<ActorService>>,
    harness_recovery: Option<super::ToolRecoveryService>,
    harness_import_recovery: Option<super::ChildImportRecoveryService>,
    project_verification: Option<super::ProjectVerificationService>,
    project_profiles: Option<super::ProjectProfileService>,
    project_detection: Option<super::ProjectDetectionService>,
}
impl TeamService {
    pub fn new(
        engine: Engine,
        store: Arc<dyn TeamStore>,
        workspaces: Arc<dyn TeamWorkspaces>,
        registry: Arc<AgentRegistry>,
    ) -> Self {
        Self {
            engine,
            store,
            workspaces,
            registry,
            tasks: None,
            events: None,
            controls: None,
            goals: None,
            harness_reader: None,
            harness_actors: None,
            harness_recovery: None,
            harness_import_recovery: None,
            project_verification: None,
            project_profiles: None,
            project_detection: None,
        }
    }
    pub fn with_task_inbox(mut self, port: Arc<dyn TeamTaskInboxPort>) -> Self {
        self.tasks = Some(TeamTaskService::new(self.engine.clone(), port));
        self
    }
    pub fn with_project_profiles(mut self, port: Arc<dyn ProjectProfileStorePort>) -> Self {
        self.project_profiles = Some(super::ProjectProfileService::new(port));
        self
    }
    pub fn project_profiles(&self) -> Result<&super::ProjectProfileService, Error> {
        self.project_profiles
            .as_ref()
            .ok_or_else(|| Error::Unavailable("project profiles are not installed".into()))
    }
    pub fn with_project_detection(mut self, port: Arc<dyn ProjectDetectionPort>) -> Self {
        self.project_detection = Some(super::ProjectDetectionService::new(port));
        self
    }
    pub fn project_detection(&self) -> Result<&super::ProjectDetectionService, Error> {
        self.project_detection
            .as_ref()
            .ok_or_else(|| Error::Unavailable("project detection is not installed".into()))
    }
    pub fn with_harness_store(mut self, port: Arc<dyn HarnessStorePort>) -> Self {
        self.harness_reader = Some(port);
        self
    }
    pub fn with_harness_coding(mut self, actors: Arc<ActorService>) -> Self {
        self.harness_actors = Some(actors);
        self
    }
    pub fn with_harness_recovery(
        mut self,
        tools: Arc<super::ToolRegistry>,
        inspector: Arc<dyn ToolRecoveryInspectionPort>,
    ) -> Self {
        self.harness_recovery = Some(super::ToolRecoveryService::new(
            self.engine.clone(),
            tools,
            inspector,
        ));
        self
    }
    pub fn with_harness_import_recovery(
        mut self,
        inspector: Arc<dyn ChildImportInspectionPort>,
    ) -> Self {
        self.harness_import_recovery = Some(super::ChildImportRecoveryService::new(
            self.engine.clone(),
            inspector,
        ));
        self
    }
    pub fn with_project_verification(
        mut self,
        inspector: Arc<dyn ProjectVerificationInspectionPort>,
    ) -> Self {
        self.project_verification = Some(super::ProjectVerificationService::new(
            self.engine.clone(),
            inspector,
        ));
        self
    }
    pub(crate) fn harness_spec(
        &self,
        request: &harness::HarnessRequest,
    ) -> Result<TeamSpec, Error> {
        Ok(request.team_spec()?)
    }
    pub(crate) fn now_ms(&self) -> i64 {
        self.engine.clock.now_ms()
    }
    pub fn harness(&self) -> Result<super::HarnessService<'_>, Error> {
        Ok(super::HarnessService {
            teams: self,
            engine: &self.engine,
            actors: self.harness_actors.as_deref(),
            recovery: self.harness_recovery.as_ref(),
            import_recovery: self.harness_import_recovery.as_ref(),
            project_verification: self.project_verification.as_ref(),
            reader: self
                .harness_reader
                .as_deref()
                .ok_or_else(|| Error::Unavailable("harness discovery is not installed".into()))?,
        })
    }
    pub fn with_goal_listing(mut self, port: Arc<dyn TeamGoalListPort>) -> Self {
        self.goals = Some(TeamGoalListService::new(port));
        self
    }
    pub fn goals(&self) -> Result<&TeamGoalListService, Error> {
        self.goals
            .as_ref()
            .ok_or_else(|| Error::Unavailable("team goal listing is not installed".into()))
    }
    pub fn tasks(&self) -> Result<&TeamTaskService, Error> {
        self.tasks
            .as_ref()
            .ok_or_else(|| Error::Unavailable("team task inbox is not installed".into()))
    }
    pub fn with_task_events(mut self, port: Arc<dyn TeamTaskEventsPort>) -> Self {
        self.events = Some(TeamEventService::new(port));
        self
    }
    pub fn events(&self) -> Result<&TeamEventService, Error> {
        self.events
            .as_ref()
            .ok_or_else(|| Error::Unavailable("team timeline is not installed".into()))
    }
    pub fn with_goal_controls(mut self, actors: Arc<ActorService>) -> Self {
        self.controls = Some(TeamGoalControlService::new(self.engine.clone(), actors));
        self
    }
    pub fn controls(&self) -> Result<&TeamGoalControlService, Error> {
        self.controls
            .as_ref()
            .ok_or_else(|| Error::Unavailable("team goal controls are not installed".into()))
    }
    /// Name is the durable creation key. Editing a saved configuration requires a new name.
    pub async fn create(&self, spec: TeamSpec) -> Result<TeamRecord, Error> {
        spec.validate()?;
        if spec
            .source
            .as_ref()
            .is_some_and(|p| !Path::new(p).is_absolute())
        {
            return Err(DomainError::InvalidConfiguration("team source must be an absolute directory (CLI resolves paths relative to the configuration file)".into()).into());
        }
        if let Some(saved) = self.store.team_named(spec.name.clone()).await? {
            if saved.spec != spec {
                return Err(conflicting_spec());
            }
            return self.resume(saved.id).await;
        }
        let record = self.compile(spec)?;
        let saved = self.store.register_team(record).await?;
        self.resume(saved.id).await
    }
    fn compile(&self, spec: TeamSpec) -> Result<TeamRecord, Error> {
        let mut members = Vec::with_capacity(spec.workers.len() + 1);
        for worker in &spec.workers {
            let source = if worker.adapter.name == "answer" {
                None
            } else {
                spec.source.clone()
            };
            members.push(self.pin_member(
                &worker.name,
                TeamRole::Worker,
                &worker.adapter,
                worker.actor_spec(),
                source,
            )?);
        }
        let mut planner = spec.planner.clone();
        planner.config["workers"] = json!(members.iter().zip(&spec.workers).map(|(member, worker)|
            json!({"name":member.name,"agent_id":member.agent_id,"description":worker.description})).collect::<Vec<_>>());
        members.insert(
            0,
            self.pin_member(
                "planner",
                TeamRole::Planner,
                &DefinitionId::new("planner", 3),
                planner,
                spec.source.clone(),
            )?,
        );
        let now = self.engine.clock.now_ms();
        Ok(TeamRecord {
            id: TeamId::new(),
            spec,
            status: TeamStatus::Preparing,
            members,
            error: None,
            created_at: now,
            updated_at: now,
        })
    }
    fn pin_member(
        &self,
        name: &str,
        role: TeamRole,
        adapter: &DefinitionId,
        spec: TeamActorSpec,
        workspace: Option<String>,
    ) -> Result<TeamMember, Error> {
        let mut input = spec.config;
        let object = input.as_object_mut().ok_or(DomainError::InvalidState)?;
        let supervision = object
            .remove("supervision")
            .map(serde_json::from_value::<SupervisionPolicy>)
            .transpose()?;
        if let Some(policy) = &supervision {
            policy.validate()?;
        }
        let memory = object
            .remove("memory")
            .map(serde_json::from_value::<MemoryPolicy>)
            .transpose()?;
        if let Some(policy) = &memory {
            policy.validate()?;
        }
        let registration = self.registry.registration(adapter)?.clone();
        if !self
            .engine
            .definitions
            .contains_key(&registration.workflow_id())
        {
            return Err(Error::Unavailable(
                "team agent workflow is not installed".into(),
            ));
        }
        let input = self.registry.pin_config(adapter, input, workspace)?;
        let config = serde_json::to_value(RegisteredAgentConfig {
            registration,
            input,
            attempt_limit: spec.attempt_limit,
            memory,
        })?;
        let agent_id = AgentId::new();
        // Validate initialization for every actor before a setup intent or workspace is allocated.
        Agent::new_with_policies(
            agent_id,
            &RegisteredAgentDefinition,
            config.clone(),
            spec.queue_policy,
            supervision.clone(),
            self.engine.clock.now_ms(),
        )?;
        Ok(TeamMember {
            name: name.into(),
            role,
            agent_id,
            pinned_config: config,
            queue_policy: spec.queue_policy,
            supervision,
            workspace: None,
        })
    }
    fn check_pins(&self, record: &TeamRecord) -> Result<(), Error> {
        for member in &record.members {
            let pinned: RegisteredAgentConfig =
                serde_json::from_value(member.pinned_config.clone())?;
            self.registry
                .check_config(&pinned.registration, &pinned.input)?;
            if !self
                .engine
                .definitions
                .contains_key(&pinned.registration.workflow_id())
            {
                return Err(Error::Unavailable(
                    "pinned team workflow is not installed".into(),
                ));
            }
        }
        Ok(())
    }
    /// Reuse saved contracts, IDs, snapshot and copies; never repin or reset a ready actor.
    pub async fn resume(&self, id: TeamId) -> Result<TeamRecord, Error> {
        self.store.team(id).await?;
        let _lease = self.workspaces.lock_setup(id).await?;
        let result = self.setup(id).await;
        if let Err(error) = &result {
            let mut message = error.to_string();
            let mut end = message.len().min(2048);
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
            let _ = self
                .store
                .team_setup_error(id, Some(message), self.engine.clock.now_ms())
                .await;
        }
        result
    }
    async fn setup(&self, id: TeamId) -> Result<TeamRecord, Error> {
        let mut record = self.store.team(id).await?;
        self.check_pins(&record)?;
        if record.status == TeamStatus::Ready {
            self.store.team_view(id).await?;
            return Ok(record);
        }
        self.store
            .team_setup_error(id, None, self.engine.clock.now_ms())
            .await?;
        for member in &mut record.members {
            if member.needs_workspace() {
                let workspace = self
                    .workspaces
                    .prepare_member(
                        id,
                        member.agent_id,
                        record
                            .spec
                            .source
                            .clone()
                            .ok_or(DomainError::InvalidState)?,
                    )
                    .await?;
                if member
                    .workspace
                    .as_ref()
                    .is_some_and(|saved| saved != &workspace)
                {
                    return Err(Error::Unavailable(
                        "prepared team workspace differs from its saved receipt".into(),
                    ));
                }
                self.store
                    .save_team_workspace(
                        id,
                        member.agent_id,
                        workspace.clone(),
                        self.engine.clock.now_ms(),
                    )
                    .await?;
                member.workspace = Some(workspace);
            }
        }
        let now = self.engine.clock.now_ms();
        let mut actors = Vec::with_capacity(record.members.len());
        for member in &record.members {
            let config = member.actor_config()?;
            let pinned: RegisteredAgentConfig = serde_json::from_value(config.clone())?;
            self.registry
                .check_config(&pinned.registration, &pinned.input)?;
            actors.push(Agent::new_with_policies(
                member.agent_id,
                &RegisteredAgentDefinition,
                config,
                member.queue_policy,
                member.supervision.clone(),
                now,
            )?);
        }
        self.store.publish_team(id, actors, now).await
    }
    pub async fn get(&self, id: TeamId) -> Result<TeamRecord, Error> {
        self.store.team(id).await
    }
    pub(crate) async fn harness_start(&self, name: String) -> Result<Option<TeamRecord>, Error> {
        self.store.team_named(name).await
    }
    pub async fn status(&self, id: TeamId) -> Result<TeamView, Error> {
        self.store.team_view(id).await
    }
    pub async fn list(&self) -> Result<TeamList, Error> {
        self.store.list_teams().await
    }
    /// Message identity makes an identical submission retry idempotent, including after completion.
    pub async fn submit(
        &self,
        id: TeamId,
        message_id: MessageId,
        goal: String,
    ) -> Result<MessageId, Error> {
        if goal.trim().is_empty() {
            return Err(
                DomainError::InvalidConfiguration("team goal must not be empty".into()).into(),
            );
        }
        let record = self.store.team(id).await?;
        self.check_pins(&record)?;
        let message = AgentMessage {
            id: message_id,
            recipient: record.planner()?.agent_id,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":goal}),
        };
        message.validate_public()?;
        self.store
            .submit_team(id, message, self.engine.clock.now_ms())
            .await
    }
}
fn conflicting_spec() -> Error {
    DomainError::InvalidConfiguration(
        "team name already has a different pinned configuration; choose a new name".into(),
    )
    .into()
}
