//! Task-scoped workspace inputs. Only accepted task records may supply these references.
use super::{actors::RegisteredAgentConfig, *};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub fn handler(name: &str) -> DefinitionId {
    DefinitionId::new(format!("workspace.handoff.{name}"), 1)
}
pub fn is_handler(id: &DefinitionId) -> bool {
    ["prepare", "prepare_group", "verify"]
        .iter()
        .any(|name| id == &handler(name))
}
pub fn activity(name: &str, input: Value) -> Command {
    Command::Schedule(ActivitySpec {
        handler: handler(name),
        input,
        retry: RetryPolicy::default(),
        recovery: RecoveryPolicy::SafeToRepeat,
        usage: None,
    })
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareRequest {
    pub workspace: String,
    pub recipient: AgentId,
    pub goal: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writable_paths: Option<Vec<String>>,
}
impl PrepareRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if let Some(paths) = &self.writable_paths {
            tools::validate_write_scope(paths, paths)?;
        }
        if self.workspace.trim().is_empty()
            || self.workspace.len() > 32768
            || self.goal.trim().is_empty()
            || self.goal.len() > 60000
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
}
/// Compact reference; the complete manifest stays in the immutable snapshot receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedWorkspace {
    pub parent_run_id: RunId,
    pub activity_id: ActivityId,
    pub task_id: MessageId,
    pub request: PrepareRequest,
    pub workspace: String,
    pub snapshot_id: TaskId,
    pub snapshot_digest: String,
    pub worker_pin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_index: Option<u32>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareGroupRequest {
    pub workspace: String,
    pub tasks: Vec<delegation::DelegatedWork>,
}
impl PrepareGroupRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        let mut recipients = std::collections::HashSet::new();
        if !(2..=16).contains(&self.tasks.len())
            || self.tasks.iter().map(|t| t.goal.len()).sum::<usize>() > 60_000
            || self.tasks.iter().any(|t| !recipients.insert(t.recipient))
        {
            return Err(DomainError::InvalidState);
        }
        initial_dependencies::validate_workers(&self.tasks)?;
        for task in &self.tasks {
            self.member_request(task).validate()?;
        }
        Ok(())
    }
    pub fn member_request(&self, task: &delegation::DelegatedWork) -> PrepareRequest {
        PrepareRequest {
            workspace: self.workspace.clone(),
            recipient: task.recipient,
            goal: task.goal.clone(),
            writable_paths: task.writable_paths.clone(),
        }
    }
}
/// One immutable input and an ordered set of independently owned child copies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedGroup {
    pub parent_run_id: RunId,
    pub activity_id: ActivityId,
    pub request: PrepareGroupRequest,
    pub workspaces: Vec<PreparedWorkspace>,
}
impl PreparedGroup {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.request.validate()?;
        if self.workspaces.len() != self.request.tasks.len() {
            return Err(DomainError::InvalidState);
        }
        let mut paths = std::collections::HashSet::new();
        for (index, (input, task)) in self.workspaces.iter().zip(&self.request.tasks).enumerate() {
            input.validate()?;
            if input.parent_run_id != self.parent_run_id
                || input.activity_id != self.activity_id
                || input.group_index != Some(index as u32)
                || input.request != self.request.member_request(task)
                || input.snapshot_digest != self.workspaces[0].snapshot_digest
                || !paths.insert(&input.workspace)
            {
                return Err(DomainError::InvalidState);
            }
        }
        Ok(())
    }
    pub fn id(&self) -> Result<TaskGroupId, DomainError> {
        self.activity_id
            .to_string()
            .parse()
            .map_err(|_| DomainError::InvalidState)
    }
}
pub fn member_task_id(activity: ActivityId, index: u32) -> Result<MessageId, DomainError> {
    if index >= 16 {
        return Err(DomainError::InvalidState);
    }
    let digest = Sha256::digest(format!("act0r.workspace.handoff.group:{activity}:{index}"));
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
        .to_string()
        .parse()
        .map_err(|_| DomainError::InvalidState)
}
pub fn worker_pin(config: &RegisteredAgentConfig) -> Result<String, DomainError> {
    let bytes = serde_json::to_vec(config).map_err(|_| DomainError::InvalidState)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
impl PreparedWorkspace {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.request.validate()?;
        let task_id = match self.group_index {
            Some(index) => member_task_id(self.activity_id, index)?,
            None => self
                .activity_id
                .to_string()
                .parse()
                .map_err(|_| DomainError::InvalidState)?,
        };
        if self.task_id != task_id
            || self.snapshot_id.to_string() != self.activity_id.to_string()
            || self.workspace.trim().is_empty()
            || self.workspace.len() > 32768
            || self.workspace == self.request.workspace
            || [&self.snapshot_digest, &self.worker_pin]
                .iter()
                .any(|s| s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
    pub fn check_worker(
        &self,
        actor: AgentId,
        config: &RegisteredAgentConfig,
    ) -> Result<(), DomainError> {
        self.validate()?;
        check_worker(config)?;
        if let Some(paths) = &self.request.writable_paths {
            let input: answer::AnswerConfig = serde_json::from_value(config.input.clone())
                .map_err(|_| DomainError::InvalidState)?;
            tools::validate_write_scope(paths, &input.writable_paths)?;
        }
        if actor != self.request.recipient || worker_pin(config)? != self.worker_pin {
            return Err(DomainError::InvalidConfiguration(
                "workspace handoff differs from the worker's pinned configuration".into(),
            ));
        }
        Ok(())
    }
}
pub fn check_worker(config: &RegisteredAgentConfig) -> Result<(), DomainError> {
    let input: answer::AnswerConfig = serde_json::from_value(config.input.clone())
        .map_err(|e| DomainError::InvalidConfiguration(e.to_string()))?;
    input.validate()?;
    if config.registration.adapter != DefinitionId::new("tool", 1)
        || config.registration.definition != answer::ToolAgentWorkflow::definition_id()
        || !input.accept_workspace_handoff
        || !input.change_review
        || input.workspace.is_none()
        || input.workspace_input.is_some()
    {
        return Err(DomainError::InvalidConfiguration("workspace handoff requires an opted-in native tool worker with change_review and a managed workspace".into()));
    }
    Ok(())
}

const KEY: &str = "workspace_handoff_state";
#[derive(Serialize, Deserialize)]
enum Phase {
    Verifying(ActivitySpec),
    Ready,
}
pub fn validate_state(state: &Value) -> Result<(), DomainError> {
    if state["config"].get("workspace_input").is_some() != state.get(KEY).is_some() {
        return Err(DomainError::InvalidState);
    }
    if let Some(value) = state.get(KEY) {
        let _: Phase =
            serde_json::from_value(value.clone()).map_err(|_| DomainError::InvalidState)?;
    }
    Ok(())
}
/// Verify the saved input before the child's baseline or first model call.
pub fn transition(
    state: &Value,
    event: &WorkflowEvent,
    inner: impl Fn(&Value, &WorkflowEvent) -> Result<Transition, DomainError>,
) -> Result<Transition, DomainError> {
    if let WorkflowEvent::Started(input) = event {
        let mut t = inner(state, event)?;
        if let Some(value) = input.get("workspace_input") {
            let prepared: PreparedWorkspace =
                serde_json::from_value(value.clone()).map_err(|_| DomainError::InvalidState)?;
            prepared.validate()?;
            let index = t
                .commands
                .iter()
                .position(|c| matches!(c, Command::Schedule(_)))
                .ok_or(DomainError::InvalidState)?;
            let Command::Schedule(first) = t.commands.remove(index) else {
                return Err(DomainError::InvalidState);
            };
            t.commands
                .insert(index, activity("verify", json!(prepared)));
            t.state[KEY] = json!(Phase::Verifying(first));
        }
        return Ok(t);
    }
    validate_state(state)?;
    if let Some(value) = state.get(KEY) {
        let phase: Phase =
            serde_json::from_value(value.clone()).map_err(|_| DomainError::InvalidState)?;
        if let Phase::Verifying(first) = phase {
            let WorkflowEvent::ActivityCompleted {
                handler: id,
                result,
                ..
            } = event
            else {
                return Err(DomainError::InvalidState);
            };
            if id != &handler("verify") || result != &json!({"verified":true}) {
                return Err(DomainError::InvalidState);
            }
            let mut state = state.clone();
            state[KEY] = json!(Phase::Ready);
            return Ok(Transition {
                state,
                commands: vec![Command::Schedule(first)],
            });
        }
        if let WorkflowEvent::ActivityCompleted {
            handler: id,
            result,
            ..
        } = event
            && id == &change_review::handler("baseline")
            && result["digest"] != state["config"]["workspace_input"]["snapshot_digest"]
        {
            return Err(DomainError::InvalidWorkflow(
                "child baseline differs from its handed-off snapshot".into(),
            ));
        }
        let mut t = inner(state, event)?;
        t.state[KEY] = json!(Phase::Ready);
        return Ok(t);
    }
    inner(state, event)
}
