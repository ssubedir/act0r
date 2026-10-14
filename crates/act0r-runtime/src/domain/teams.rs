//! A pinned assembly of native actors. Execution and durable waits remain actor/Run concerns.
use super::{actors::*, *};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

pub const MAX_TEAM_SPEC_BYTES: usize = 1024 * 1024;
fn attempts() -> u32 {
    100
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamSpec {
    pub version: u32,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub planner: TeamActorSpec,
    pub workers: Vec<TeamWorkerSpec>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamActorSpec {
    pub config: Value,
    #[serde(default = "attempts")]
    pub attempt_limit: u32,
    #[serde(default)]
    pub queue_policy: AgentQueuePolicy,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamWorkerSpec {
    pub name: String,
    pub description: String,
    pub adapter: DefinitionId,
    pub config: Value,
    #[serde(default = "attempts")]
    pub attempt_limit: u32,
    #[serde(default)]
    pub queue_policy: AgentQueuePolicy,
}
impl TeamWorkerSpec {
    pub fn actor_spec(&self) -> TeamActorSpec {
        TeamActorSpec {
            config: self.config.clone(),
            attempt_limit: self.attempt_limit,
            queue_policy: self.queue_policy,
        }
    }
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
impl TeamSpec {
    pub fn validate(&self) -> Result<(), DomainError> {
        let invalid = |message: &str| DomainError::InvalidConfiguration(message.into());
        if self.version != 1 || !valid_name(&self.name) {
            return Err(invalid(
                "team requires version 1 and a name of 1..64 ASCII letters, digits, '-' or '_'",
            ));
        }
        if self
            .source
            .as_ref()
            .is_some_and(|p| p.is_empty() || p.len() > 4096)
            || self.workers.is_empty()
            || self.workers.len() > 16
            || serde_json::to_vec(self)
                .map_err(|e| invalid(&e.to_string()))?
                .len()
                > MAX_TEAM_SPEC_BYTES
        {
            return Err(invalid(
                "team requires 1..16 workers, a bounded source path, and at most 1 MiB of configuration",
            ));
        }
        let mut names = HashSet::from(["planner"]);
        self.planner.validate()?;
        if self.planner.config.get("workers").is_some() {
            return Err(invalid(
                "team planner worker identities are selected by the runtime",
            ));
        }
        for worker in &self.workers {
            if !valid_name(&worker.name)
                || !names.insert(&worker.name)
                || worker.description.len() > 4096
                || ![DefinitionId::new("tool", 1), DefinitionId::new("answer", 1)]
                    .contains(&worker.adapter)
            {
                return Err(invalid(
                    "workers require unique bounded names (excluding 'planner'), descriptions up to 4096 bytes, and tool v1 or answer v1",
                ));
            }
            worker.actor_spec().validate()?;
        }
        Ok(())
    }
}
impl TeamActorSpec {
    fn validate(&self) -> Result<(), DomainError> {
        self.queue_policy.validate()?;
        if self.attempt_limit == 0 || !self.config.is_object() {
            return Err(DomainError::InvalidConfiguration(
                "team actors require an object configuration and positive attempt limit".into(),
            ));
        }
        for key in [
            "workspace",
            "workspace_input",
            "actor_memory",
            "session_summary",
            "follow_up_context",
            "tool_definitions",
        ] {
            if self.config.get(key).is_some() {
                return Err(DomainError::InvalidConfiguration(format!(
                    "{key} is selected by the runtime"
                )));
            }
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamStatus {
    Preparing,
    Ready,
}
impl TeamStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Ready => "ready",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamRole {
    Planner,
    Worker,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamWorkspace {
    pub path: String,
    pub snapshot_digest: String,
}
impl TeamWorkspace {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.path.is_empty()
            || self.path.len() > 4096
            || self.snapshot_digest.len() != 64
            || !self.snapshot_digest.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(DomainError::InvalidConfiguration(
                "team workspace requires a bounded path and SHA-256 snapshot digest".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamMember {
    pub name: String,
    pub role: TeamRole,
    pub agent_id: AgentId,
    /// Immutable registration, contracts and provisional source path, captured before setup.
    pub pinned_config: Value,
    pub queue_policy: AgentQueuePolicy,
    pub supervision: Option<SupervisionPolicy>,
    pub workspace: Option<TeamWorkspace>,
}
impl TeamMember {
    pub fn needs_workspace(&self) -> bool {
        self.pinned_config["input"]["workspace"].is_string()
    }
    pub fn actor_config(&self) -> Result<Value, DomainError> {
        let mut config = self.pinned_config.clone();
        if self.needs_workspace() {
            let workspace = self.workspace.as_ref().ok_or(DomainError::InvalidState)?;
            config["input"]["workspace"] = Value::String(workspace.path.clone());
        }
        Ok(config)
    }
    pub fn matches_actor(&self, actor: &Agent) -> Result<bool, DomainError> {
        let record = actor.record();
        Ok(record.id == self.agent_id
            && record.definition == RegisteredAgentDefinition.id()
            && record.config == self.actor_config()?
            && record.queue_policy == self.queue_policy
            && record.supervision.as_ref().map(|s| &s.policy) == self.supervision.as_ref())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamRecord {
    pub id: TeamId,
    pub spec: TeamSpec,
    pub status: TeamStatus,
    pub members: Vec<TeamMember>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}
impl TeamRecord {
    /// Validate assembly invariants without installed adapters or filesystem access.
    pub fn validate_assembly(&self) -> Result<(), DomainError> {
        self.spec.validate()?;
        if self.members.len() != self.spec.workers.len() + 1
            || self.members[0].role != TeamRole::Planner
            || self.members[0].name != "planner"
        {
            return Err(DomainError::InvalidState);
        }
        let decode_error = |e: serde_json::Error| DomainError::InvalidConfiguration(e.to_string());
        let mut ids = HashSet::new();
        let mut paths = HashSet::new();
        let mut digest = None;
        for (index, member) in self.members.iter().enumerate() {
            if !ids.insert(member.agent_id) {
                return Err(DomainError::InvalidState);
            }
            let (adapter, spec) = if index == 0 {
                (DefinitionId::new("planner", 3), self.spec.planner.clone())
            } else {
                let worker = &self.spec.workers[index - 1];
                if member.role != TeamRole::Worker || member.name != worker.name {
                    return Err(DomainError::InvalidState);
                }
                (worker.adapter.clone(), worker.actor_spec())
            };
            let pinned: RegisteredAgentConfig =
                serde_json::from_value(member.pinned_config.clone()).map_err(decode_error)?;
            let supervision = spec
                .config
                .get("supervision")
                .cloned()
                .map(serde_json::from_value::<SupervisionPolicy>)
                .transpose()
                .map_err(decode_error)?;
            let memory = spec
                .config
                .get("memory")
                .cloned()
                .map(serde_json::from_value::<MemoryPolicy>)
                .transpose()
                .map_err(decode_error)?;
            let source = if adapter.name == "answer" {
                None
            } else {
                self.spec.source.as_ref()
            };
            if pinned.registration.adapter != adapter
                || pinned.attempt_limit != spec.attempt_limit
                || member.queue_policy != spec.queue_policy
                || member.supervision != supervision
                || pinned.memory != memory
                || pinned.input["workspace"].as_str() != source.map(String::as_str)
            {
                return Err(DomainError::InvalidConfiguration(
                    "team member differs from its pinned assembly".into(),
                ));
            }
            if let Some(workspace) = &member.workspace {
                workspace.validate()?;
                if !member.needs_workspace()
                    || !paths.insert(&workspace.path)
                    || digest.is_some_and(|d| d != &workspace.snapshot_digest)
                {
                    return Err(DomainError::InvalidState);
                }
                digest = Some(&workspace.snapshot_digest);
            } else if self.status == TeamStatus::Ready && member.needs_workspace() {
                return Err(DomainError::InvalidState);
            }
        }
        let planner: super::planner::PlannerConfig =
            serde_json::from_value(self.members[0].pinned_config["input"].clone())
                .map_err(decode_error)?;
        if planner.workers.len() != self.spec.workers.len()
            || planner
                .workers
                .iter()
                .zip(&self.members[1..])
                .zip(&self.spec.workers)
                .any(|((target, member), worker)| {
                    target.name != worker.name
                        || target.agent_id != member.agent_id
                        || target.description != worker.description
                })
        {
            return Err(DomainError::InvalidConfiguration(
                "team planner targets differ from saved member identities".into(),
            ));
        }
        Ok(())
    }
    pub fn validate_intent(&self) -> Result<(), DomainError> {
        self.validate_assembly()?;
        if self.status != TeamStatus::Preparing
            || self.members.iter().any(|m| m.workspace.is_some())
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
    pub fn planner(&self) -> Result<&TeamMember, DomainError> {
        self.members
            .iter()
            .find(|m| m.role == TeamRole::Planner)
            .ok_or(DomainError::InvalidState)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TeamSummary {
    pub id: TeamId,
    pub name: String,
    pub status: TeamStatus,
    pub error: Option<String>,
}
impl From<&TeamRecord> for TeamSummary {
    fn from(team: &TeamRecord) -> Self {
        Self {
            id: team.id,
            name: team.spec.name.clone(),
            status: team.status,
            error: team.error.clone(),
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct TeamMemberView {
    pub name: String,
    pub role: TeamRole,
    pub agent_id: AgentId,
    pub workspace: Option<TeamWorkspace>,
    pub status: Option<AgentStatus>,
    pub queue: Option<AgentQueue>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TeamView {
    #[serde(flatten)]
    pub team: TeamSummary,
    pub members: Vec<TeamMemberView>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TeamList {
    pub teams: Vec<TeamSummary>,
    pub truncated: bool,
}
