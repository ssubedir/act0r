//! Session authority is separate from model instructions and immutable actor contracts.
use super::{tools::NativeToolRequest, *};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionRequest {
    pub request_id: Uuid,
    pub expected_revision: u64,
    pub action: PermissionAction,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PermissionAction {
    AllowTestCommand {
        task_id: MessageId,
        run_id: RunId,
        wait_id: WaitId,
    },
    RevokeTestCommand {
        grant_id: Uuid,
    },
    AllowTaskEdits {
        task_id: MessageId,
        run_id: RunId,
        wait_id: WaitId,
    },
    RevokeTaskEdits {
        grant_id: Uuid,
    },
    AllowTaskTests {
        task_id: MessageId,
        run_id: RunId,
        wait_id: WaitId,
    },
    RevokeTaskTests {
        grant_id: Uuid,
    },
}
impl PermissionRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.request_id.is_nil()
            || self.expected_revision >= i64::MAX as u64
            || matches!(self.action, PermissionAction::RevokeTestCommand { grant_id } if grant_id.is_nil())
            || matches!(self.action, PermissionAction::RevokeTaskEdits { grant_id } if grant_id.is_nil())
            || matches!(self.action, PermissionAction::RevokeTaskTests { grant_id } if grant_id.is_nil())
        {
            return Err(DomainError::InvalidConfiguration(
                "permission needs a request identity and a valid revision".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TestPermission {
    pub grant_id: Uuid,
    pub session_id: SessionId,
    pub actor_id: AgentId,
    pub request: NativeToolRequest,
    pub granted_at: i64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PermissionState {
    pub revision: u64,
    pub test_command: Option<TestPermission>,
}
impl PermissionState {
    pub fn check(&self, request: &PermissionRequest) -> Result<(), DomainError> {
        request.validate()?;
        if self.revision != request.expected_revision {
            return Err(DomainError::SessionConflict(
                "session permissions changed; refresh before deciding".into(),
            ));
        }
        match request.action {
            PermissionAction::AllowTestCommand { .. } if self.test_command.is_some() => Err(
                DomainError::SessionConflict("a test command permission is already saved".into()),
            ),
            PermissionAction::RevokeTestCommand { grant_id }
                if !self
                    .test_command
                    .as_ref()
                    .is_some_and(|g| g.grant_id == grant_id) =>
            {
                Err(DomainError::SessionConflict(
                    "the reviewed permission is no longer active".into(),
                ))
            }
            _ => Ok(()),
        }
    }
    pub fn apply(
        &mut self,
        request: &PermissionRequest,
        grant: Option<TestPermission>,
    ) -> Result<(), DomainError> {
        self.check(request)?;
        match (&request.action, &grant) {
            (PermissionAction::AllowTestCommand { .. }, Some(grant))
                if grant.grant_id == request.request_id => {}
            (PermissionAction::RevokeTestCommand { .. }, None) => {}
            (
                PermissionAction::AllowTaskEdits { .. }
                | PermissionAction::RevokeTaskEdits { .. }
                | PermissionAction::AllowTaskTests { .. }
                | PermissionAction::RevokeTaskTests { .. },
                None,
            ) => {
                self.revision += 1;
                return Ok(());
            }
            _ => return Err(DomainError::InvalidState),
        }
        self.test_command = grant;
        self.revision += 1;
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionReceipt {
    pub session_id: SessionId,
    pub request: PermissionRequest,
    pub state: PermissionState,
    pub saved_at: i64,
    pub replayed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_edit: Option<TaskEditPermission>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_test: Option<TaskTestPermission>,
}
#[derive(Clone, Debug, Serialize)]
pub struct SessionPermissions {
    pub session_id: SessionId,
    pub state: PermissionState,
    pub archived: bool,
    pub history: Vec<PermissionReceipt>,
    pub eligible_wait: Option<EligibleTestWait>,
    pub eligible_edit_waits: Vec<EligibleEditWait>,
    pub task_edits: Vec<TaskEditPermissionView>,
    pub task_edits_truncated: bool,
    pub eligible_task_test_waits: Vec<EligibleTaskTestWait>,
    pub task_tests: Vec<TaskTestPermissionView>,
    pub task_tests_truncated: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct EligibleTestWait {
    pub task_id: MessageId,
    pub run_id: RunId,
    pub wait_id: WaitId,
    pub request: NativeToolRequest,
}

/// Human authority for one accepted workspace handoff and its immutable native contracts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskEditPermission {
    pub grant_id: Uuid,
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub run_id: RunId,
    pub actor_id: AgentId,
    pub workspace_input: workspace_handoff::PreparedWorkspace,
    pub tools: Vec<tools::ToolDefinition>,
    pub granted_at: i64,
    pub revoked_at: Option<i64>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskEditPermissionView {
    pub grant: TaskEditPermission,
    pub status: &'static str,
}
#[derive(Clone, Debug, Serialize)]
pub struct EligibleEditWait {
    pub task_id: MessageId,
    pub run_id: RunId,
    pub wait_id: WaitId,
    pub actor_id: AgentId,
    pub workspace: String,
    pub writable_paths: Vec<String>,
    pub tools: Vec<tools::ToolDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskTestPermission {
    pub grant_id: Uuid,
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub run_id: RunId,
    pub actor_id: AgentId,
    pub workspace_input: workspace_handoff::PreparedWorkspace,
    pub request: NativeToolRequest,
    pub granted_at: i64,
    pub revoked_at: Option<i64>,
}
#[derive(Clone, Debug, Serialize)]
pub struct TaskTestPermissionView {
    pub grant: TaskTestPermission,
    pub status: &'static str,
}
#[derive(Clone, Debug, Serialize)]
pub struct EligibleTaskTestWait {
    pub task_id: MessageId,
    pub run_id: RunId,
    pub wait_id: WaitId,
    pub actor_id: AgentId,
    pub request: NativeToolRequest,
}

fn edit_contract(tool: &tools::ToolDefinition) -> bool {
    matches!(tool.id.name.as_str(), "replace_text" | "create_file")
        && tool.id.version == 1
        && tool.handler() == DefinitionId::new("native.tool", 4)
        && tool.requires_approval
        && tool.requires_workspace
        && tool.requires_writable_paths
        && !tool.requires_test_command
        && !tool.requires_project_command
        && tool.recovery == RecoveryPolicy::RequiresReconciliation
}
impl Run {
    pub fn task_permission_binding(
        &self,
        task: &actors::AgentTaskRecord,
        pinned: &actors::RegisteredAgentConfig,
    ) -> Result<(workspace_handoff::PreparedWorkspace, answer::AnswerConfig), DomainError> {
        if !(self.record.status.is_unfinished()
            || self.record.status == RunStatus::Blocked
            || self.needs_worker_recovery())
        {
            return Err(DomainError::InvalidState);
        }
        self.task_workspace_binding(task, pinned)
    }
    pub(crate) fn task_workspace_binding(
        &self,
        task: &actors::AgentTaskRecord,
        pinned: &actors::RegisteredAgentConfig,
    ) -> Result<(workspace_handoff::PreparedWorkspace, answer::AnswerConfig), DomainError> {
        self.native_task_binding(task, pinned, true)
    }
    /// Historical read authority retains immutable task pins after completion or stop.
    pub fn task_inspection_binding(
        &self,
        task: &actors::AgentTaskRecord,
        pinned: &actors::RegisteredAgentConfig,
    ) -> Result<(workspace_handoff::PreparedWorkspace, answer::AnswerConfig), DomainError> {
        self.native_task_binding(task, pinned, false)
    }
    fn native_task_binding(
        &self,
        task: &actors::AgentTaskRecord,
        pinned: &actors::RegisteredAgentConfig,
        active: bool,
    ) -> Result<(workspace_handoff::PreparedWorkspace, answer::AnswerConfig), DomainError> {
        let input = task
            .workspace_input
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        input.check_worker(task.agent_id, pinned)?;
        let invocation: integration::AgentInvocation =
            serde_json::from_value(self.record.input.clone())
                .map_err(|_| DomainError::InvalidState)?;
        let effective: answer::AnswerConfig = serde_json::from_value(invocation.input.clone())
            .map_err(|_| DomainError::InvalidState)?;
        let original: answer::AnswerConfig =
            serde_json::from_value(pinned.input.clone()).map_err(|_| DomainError::InvalidState)?;
        let scope = input
            .request
            .writable_paths
            .as_ref()
            .unwrap_or(&original.writable_paths);
        if input.request.writable_paths.is_some() {
            tools::validate_write_scope(scope, &original.writable_paths)?;
        }
        if task.id != input.task_id
            || task.run_id != Some(self.record.id)
            || active
                && (task.cancel_requested
                    || task.status.is_terminal()
                    || self.record.controls.cancel_requested)
            || invocation.registration != pinned.registration
            || invocation.registration.definition != answer::ToolAgentWorkflow::definition_id()
            || invocation.registration.workflow_id() != self.record.workflow
            || self.record.state["registration"] != json!(invocation.registration)
            || effective.workspace_input.as_ref() != Some(input)
            || effective.workspace.as_ref() != Some(&input.workspace)
            || &effective.writable_paths != scope
            || effective.tool_definitions != original.tool_definitions
            || effective.test_command != original.test_command
            || effective.project_commands != original.project_commands
            || self.record.state["state"]["config"] != invocation.input
        {
            return Err(DomainError::InvalidState);
        }
        Ok((input.clone(), effective))
    }
    pub fn task_edit_binding(
        &self,
        task: &actors::AgentTaskRecord,
        pinned: &actors::RegisteredAgentConfig,
    ) -> Result<
        (
            workspace_handoff::PreparedWorkspace,
            Vec<tools::ToolDefinition>,
        ),
        DomainError,
    > {
        let (input, config) = self.task_permission_binding(task, pinned)?;
        if input.request.writable_paths.is_none() {
            return Err(DomainError::InvalidState);
        }
        let tools: Vec<_> = config
            .tool_definitions
            .into_iter()
            .filter(edit_contract)
            .collect();
        if tools.is_empty() {
            return Err(DomainError::InvalidState);
        }
        Ok((input, tools))
    }
    pub fn task_test_binding(
        &self,
        task: &actors::AgentTaskRecord,
        pinned: &actors::RegisteredAgentConfig,
    ) -> Result<(workspace_handoff::PreparedWorkspace, NativeToolRequest), DomainError> {
        let (input, config) = self.task_permission_binding(task, pinned)?;
        let request = native_test_request(
            &config.tool_definitions,
            config.workspace.as_deref(),
            &config.writable_paths,
            config.test_command.as_ref(),
        )?;
        Ok((input, request))
    }
    pub fn pending_task_test_request(&self) -> Result<NativeToolRequest, DomainError> {
        if !self.record.status.is_unfinished() || self.record.controls.cancel_requested {
            return Err(DomainError::InvalidState);
        }
        let state = &self.record.state["state"];
        let config: answer::AnswerConfig = serde_json::from_value(state["config"].clone())
            .map_err(|_| DomainError::InvalidState)?;
        let action: agent::AgentAction = serde_json::from_value(state["pending_tool"].clone())
            .map_err(|_| DomainError::InvalidState)?;
        if !matches!(action, agent::AgentAction::CallTool { ref tool, ref arguments } if tool == "run_tests" && arguments == &json!({}))
        {
            return Err(DomainError::InvalidState);
        }
        let request = native_test_request(
            &config.tool_definitions,
            config.workspace.as_deref(),
            &config.writable_paths,
            config.test_command.as_ref(),
        )?;
        self.check_test_wait(&request)?;
        Ok(request)
    }
    pub fn authorize_task_tests(
        &mut self,
        grant: &TaskTestPermission,
        workflow: &dyn WorkflowDefinition,
        now: i64,
    ) -> Result<(), DomainError> {
        let request = self.pending_task_test_request()?;
        if grant.grant_id.is_nil()
            || grant.revoked_at.is_some()
            || grant.run_id != self.record.id
            || grant.task_id != grant.workspace_input.task_id
            || grant.actor_id != grant.workspace_input.request.recipient
            || self.record.state["state"]["config"]["workspace_input"]
                != json!(grant.workspace_input)
            || request.workspace.as_ref() != Some(&grant.workspace_input.workspace)
            || request != grant.request
        {
            return Err(DomainError::SessionConflict(
                "saved permission does not match this task test".into(),
            ));
        }
        let wait_id = self
            .record
            .wait
            .as_ref()
            .ok_or(DomainError::UnknownWait)?
            .id;
        self.respond_to_human(
            wait_id,
            HumanResponse::Approval { approved: true },
            Some(workflow),
            now,
        )?;
        self.event("task_test_authorized", json!({"session_id":grant.session_id,"grant_id":grant.grant_id,"task_id":grant.task_id,"actor_id":grant.actor_id,"wait_id":wait_id,"request":request}), None, None, now);
        Ok(())
    }
    pub fn pending_task_edit_request(&self) -> Result<NativeToolRequest, DomainError> {
        if !self.record.status.is_unfinished() || self.record.controls.cancel_requested {
            return Err(DomainError::InvalidState);
        }
        let state = &self.record.state["state"];
        let config: answer::AnswerConfig = serde_json::from_value(state["config"].clone())
            .map_err(|_| DomainError::InvalidState)?;
        let agent::AgentAction::CallTool { tool, arguments } =
            serde_json::from_value(state["pending_tool"].clone())
                .map_err(|_| DomainError::InvalidState)?
        else {
            return Err(DomainError::InvalidState);
        };
        let definition = tools::allowed_tool(&config.tool_definitions, &tool)?;
        if !edit_contract(definition)
            || !arguments["path"].as_str().is_some_and(|path| {
                tools::portable_file_path(path)
                    && tools::writable_path_permitted(&config.writable_paths, path)
            })
        {
            return Err(DomainError::InvalidState);
        }
        let Command::WaitForInput { key, payload, .. } = definition.request_call_with_permissions(
            &arguments,
            config.workspace.as_deref(),
            &config.writable_paths,
        )?
        else {
            return Err(DomainError::InvalidState);
        };
        let wait = self.record.wait.as_ref().ok_or(DomainError::UnknownWait)?;
        if wait.key != key || wait.payload != payload {
            return Err(DomainError::InvalidState);
        }
        let Command::Schedule(spec) = definition.schedule_call_with_permissions(
            &arguments,
            config.workspace.as_deref(),
            &config.writable_paths,
        )?
        else {
            return Err(DomainError::InvalidState);
        };
        serde_json::from_value(spec.input).map_err(|_| DomainError::InvalidState)
    }
    pub fn authorize_task_edits(
        &mut self,
        grant: &TaskEditPermission,
        workflow: &dyn WorkflowDefinition,
        now: i64,
    ) -> Result<(), DomainError> {
        let request = self.pending_task_edit_request()?;
        if grant.grant_id.is_nil()
            || grant.revoked_at.is_some()
            || grant.run_id != self.record.id
            || grant.task_id != grant.workspace_input.task_id
            || grant.actor_id != grant.workspace_input.request.recipient
            || self.record.state["state"]["config"]["workspace_input"]
                != json!(grant.workspace_input)
            || request.workspace.as_ref() != Some(&grant.workspace_input.workspace)
            || Some(&request.writable_paths)
                != grant.workspace_input.request.writable_paths.as_ref()
            || !grant.tools.contains(&request.definition)
        {
            return Err(DomainError::SessionConflict(
                "saved permission does not match this task edit".into(),
            ));
        }
        let wait_id = self
            .record
            .wait
            .as_ref()
            .ok_or(DomainError::UnknownWait)?
            .id;
        self.respond_to_human(
            wait_id,
            HumanResponse::Approval { approved: true },
            Some(workflow),
            now,
        )?;
        self.event("task_edit_authorized", json!({"session_id":grant.session_id,"grant_id":grant.grant_id,"task_id":grant.task_id,"actor_id":grant.actor_id,"wait_id":wait_id,"request":request}), None, None, now);
        Ok(())
    }
}

pub fn test_request(config: &planner::PlannerConfig) -> Result<NativeToolRequest, DomainError> {
    native_test_request(
        &config.tool_definitions,
        config.workspace.as_deref(),
        &config.writable_paths,
        config.test_command.as_ref(),
    )
}
fn native_test_request(
    definitions: &[tools::ToolDefinition],
    workspace: Option<&str>,
    writable_paths: &[String],
    test_command: Option<&tools::WorkspaceTestCommand>,
) -> Result<NativeToolRequest, DomainError> {
    let tool = tools::allowed_tool(definitions, "run_tests")?;
    if tool.id != DefinitionId::new("run_tests", 1)
        || !tool.requires_approval
        || !tool.requires_test_command
        || !tool.requires_workspace
        || tool.requires_writable_paths
        || tool.requires_project_command
        || tool.recovery != RecoveryPolicy::RequiresReconciliation
        || tool.handler() != DefinitionId::new("native.tool", 5)
    {
        return Err(DomainError::SessionConflict(
            "only the pinned native test command supports session permission".into(),
        ));
    }
    let Command::Schedule(spec) =
        tool.schedule_call_with_context(&json!({}), workspace, writable_paths, test_command)?
    else {
        return Err(DomainError::InvalidState);
    };
    serde_json::from_value(spec.input).map_err(|_| DomainError::InvalidState)
}
impl Run {
    pub fn pending_test_request(&self) -> Result<NativeToolRequest, DomainError> {
        if !self.record.status.is_unfinished() || self.record.controls.cancel_requested {
            return Err(DomainError::InvalidState);
        }
        let state: &Value = if self.record.workflow == planner::PlannerWorkflow::definition_id() {
            &self.record.state
        } else {
            let invocation: integration::AgentInvocation =
                serde_json::from_value(self.record.input.clone())
                    .map_err(|_| DomainError::InvalidState)?;
            if invocation.registration.definition != planner::PlannerWorkflow::definition_id()
                || invocation.registration.workflow_id() != self.record.workflow
                || self.record.state["registration"] != json!(invocation.registration)
            {
                return Err(DomainError::InvalidState);
            }
            &self.record.state["state"]
        };
        let config: planner::PlannerConfig = serde_json::from_value(state["config"].clone())
            .map_err(|_| DomainError::InvalidState)?;
        let action: agent::AgentAction = serde_json::from_value(state["pending"].clone())
            .map_err(|_| DomainError::InvalidState)?;
        if !matches!(action, agent::AgentAction::CallTool { ref tool, ref arguments } if tool == "run_tests" && arguments == &json!({}))
        {
            return Err(DomainError::InvalidState);
        }
        let request = test_request(&config)?;
        self.check_test_wait(&request)?;
        Ok(request)
    }
    fn check_test_wait(&self, request: &NativeToolRequest) -> Result<(), DomainError> {
        let Command::WaitForInput { key, payload, .. } =
            request.definition.request_call_with_context(
                &request.arguments,
                request.workspace.as_deref(),
                &request.writable_paths,
                request.test_command.as_ref(),
            )?
        else {
            return Err(DomainError::InvalidState);
        };
        let wait = self.record.wait.as_ref().ok_or(DomainError::UnknownWait)?;
        if wait.key != key || wait.payload != payload {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
    pub fn authorize_test_command(
        &mut self,
        grant: &TestPermission,
        workflow: &dyn WorkflowDefinition,
        now: i64,
    ) -> Result<(), DomainError> {
        if self.pending_test_request()? != grant.request {
            return Err(DomainError::SessionConflict(
                "saved permission does not match this test request".into(),
            ));
        }
        let wait_id = self
            .record
            .wait
            .as_ref()
            .ok_or(DomainError::UnknownWait)?
            .id;
        self.respond_to_human(
            wait_id,
            HumanResponse::Approval { approved: true },
            Some(workflow),
            now,
        )?;
        self.event("test_command_authorized", json!({"session_id":grant.session_id,"grant_id":grant.grant_id,"wait_id":wait_id,"request":grant.request}), None, None, now);
        Ok(())
    }
}
