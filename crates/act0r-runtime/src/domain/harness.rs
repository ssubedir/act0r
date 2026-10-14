//! A user's request pins a native actor assembly before execution starts.
use super::{
    agent::ModelTarget,
    teams::{TeamActorSpec, TeamSpec, TeamWorkerSpec},
    *,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
pub const SESSION_PREFIX: &str = "harness-v2-";

pub fn validate_workspace(path: &str) -> Result<(), DomainError> {
    if path.trim().is_empty()
        || path.len() > 4096
        || path.contains('\0')
        || !std::path::Path::new(path).is_absolute()
    {
        return Err(DomainError::InvalidConfiguration(
            "workspace must be a bounded absolute directory path".into(),
        ));
    }
    Ok(())
}

/// Explicit session permissions; models cannot choose paths or test commands.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessCoding {
    pub writable_paths: Vec<String>,
    pub test_command: tools::WorkspaceTestCommand,
    /// Fixed for the session. Two distinct actors can join independent coding tasks.
    #[serde(default = "one_coder", skip_serializing_if = "is_one_coder")]
    pub coder_count: u8,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub project_commands: tools::ProjectCommands,
}
fn one_coder() -> u8 {
    1
}
fn is_one_coder(count: &u8) -> bool {
    *count == 1
}
impl HarnessCoding {
    pub fn validate(&self) -> Result<(), DomainError> {
        if !matches!(self.coder_count, 1 | 2) {
            return Err(DomainError::InvalidConfiguration(
                "coding requires one or two coding workers".into(),
            ));
        }
        if self.writable_paths.is_empty() {
            return Err(DomainError::InvalidConfiguration(
                "coding requires writable paths".into(),
            ));
        }
        tools::validate_writable_paths(&self.writable_paths)?;
        self.test_command.validate()?;
        tools::validate_project_commands(&self.project_commands)
    }
}
/// Browser application admits only a successful, verified, nonempty immutable patch.
pub fn validate_application_changes(
    changes: &change_review::TaskChanges,
) -> Result<(), DomainError> {
    if changes.run_status != Some(RunStatus::Succeeded)
        || changes
            .completion
            .as_ref()
            .is_none_or(|report| report.status != completion::CompletionStatus::Verified)
        || changes
            .changes
            .as_ref()
            .is_none_or(|saved| saved.files.is_empty())
    {
        return Err(DomainError::InvalidConfiguration("application requires saved file changes and a successful turn with fresh passing tests".into()));
    }
    Ok(())
}
#[derive(Debug, Serialize)]
pub struct HarnessTurnContext {
    pub workspace: Option<String>,
    pub coding: Option<HarnessCoding>,
    pub paused: bool,
    pub cancelled: bool,
}
#[derive(Debug, Serialize)]
pub struct HarnessApplicationSummary {
    pub run_id: RunId,
    pub status: RunStatus,
    pub outcome: Option<patch_application::ApplicationStatus>,
}
#[derive(Debug, Serialize)]
pub struct HarnessApplications {
    pub items: Vec<HarnessApplicationSummary>,
    pub truncated: bool,
}

fn validate_prompt(prompt: &str) -> Result<(), DomainError> {
    if prompt.trim().is_empty()
        || serde_json::to_vec(&json!({"goal":prompt}))
            .map_err(|_| DomainError::InvalidState)?
            .len()
            > 65536
    {
        return Err(DomainError::InvalidConfiguration(
            "request must be nonblank and fit within 65536 UTF-8 payload bytes".into(),
        ));
    }
    Ok(())
}

/// A turn supplies only its identity and text; session configuration stays pinned.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessTurnRequest {
    pub message_id: MessageId,
    pub prompt: String,
}
impl HarnessTurnRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_prompt(&self.prompt)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessRequest {
    /// New Auto requests manage setup and routine work in their isolated copy.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub auto_execute: bool,
    /// Inspect a project before granting coding capabilities.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub discover_project: bool,
    pub message_id: MessageId,
    pub prompt: String,
    pub target: ModelTarget,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coding: Option<HarnessCoding>,
    /// A fresh allowance per turn, shared across the planner and delegated helpers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<UsageBudget>,
}

impl HarnessRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_prompt(&self.prompt)?;
        if let Some(budget) = &self.budget {
            budget.validate()?;
        }
        if self.target.provider != "openai"
            || self.target.model.trim().is_empty()
            || self.target.model.len() > 256
            || self.target.endpoint.is_empty()
            || self.target.endpoint.len() > 4096
        {
            return Err(DomainError::InvalidConfiguration(
                "configure a compatible endpoint and a model name before starting".into(),
            ));
        }
        self.target.validate()?;
        if let Some(path) = &self.workspace {
            validate_workspace(path)?;
        }
        if let Some(coding) = &self.coding {
            if self.workspace.is_none() {
                return Err(DomainError::InvalidConfiguration(
                    "coding requires a project folder".into(),
                ));
            }
            coding.validate()?;
        }
        if self.discover_project && (self.workspace.is_none() || self.coding.is_some()) {
            return Err(DomainError::InvalidConfiguration(
                "project discovery requires a folder and no pre-granted coding configuration"
                    .into(),
            ));
        }
        if self.auto_execute && !self.discover_project && self.coding.is_none() {
            return Err(DomainError::InvalidConfiguration(
                "automatic project work requires discovery or coding in a project folder".into(),
            ));
        }
        Ok(())
    }
    pub fn team_spec(&self) -> Result<TeamSpec, DomainError> {
        let mut spec = self.base_team_spec()?;
        if self.coding.is_some() {
            // Share only the explicit session permissions and model settings. The
            // coder is a tool actor, not another planner or budget owner.
            let mut coder = spec.planner.config.clone();
            let config = coder.as_object_mut().ok_or(DomainError::InvalidState)?;
            for key in [
                "task_planning",
                "goal_budget",
                "delegation",
                "workspace_handoff",
            ] {
                config.remove(key);
            }
            coder["accept_workspace_handoff"] = json!(true);
            coder["tools"]
                .as_array_mut()
                .ok_or(DomainError::InvalidState)?
                .push(json!(DefinitionId::new("send_worker_message", 2)));
            coder["tools"]
                .as_array_mut()
                .ok_or(DomainError::InvalidState)?
                .extend([
                    json!(DefinitionId::new("ask_worker", 2)),
                    json!(DefinitionId::new("reply_worker", 2)),
                ]);
            spec.planner.config["child_changes"] = json!(true);
            spec.planner.config["question_inspection_limit"] =
                json!(planner::MAX_QUESTION_INSPECTIONS);
            spec.planner.config["guidance_routing"] = json!(true);
            spec.planner.config["worker_findings"] = json!(true);
            spec.planner.config["worker_stop_limit"] = json!(planner_stop::MAX_STOPS);
            spec.planner.config["worker_pause_limit"] = json!(planner_pause::MAX_PAUSE_ACTIONS);
            spec.planner.config["worker_dependencies"] = json!(true);
            spec.planner.config["worker_changes"] = json!(true);
            spec.planner.config["assignment_inspection_limit"] =
                json!(assignment_inspection::MAX_INSPECTIONS);
            spec.planner.config["tools"]
                .as_array_mut()
                .ok_or(DomainError::InvalidState)?
                .extend([
                    json!(DefinitionId::new("reply_worker", 2)),
                    json!(DefinitionId::new("guide_workers", 1)),
                    json!(DefinitionId::new("inspect_assignment", 1)),
                    json!(DefinitionId::new("stop_worker", 1)),
                    json!(DefinitionId::new("pause_worker", 1)),
                    json!(DefinitionId::new("resume_worker", 1)),
                    json!(DefinitionId::new("wait_for_worker", 1)),
                ]);
            spec.planner.config["delegation_write_scopes"] = json!(true);
            spec.workers.push(TeamWorkerSpec {
                name: "coder".into(),
                description: "Delegate focused implementation and repair tasks here. Edit the isolated snapshot of the planner's current code within the pinned writable paths, run the fixed tests, and return a verified saved patch. The planner must inspect the patch, request approval to import it, and run combined tests before finishing.".into(),
                adapter: DefinitionId::new("tool", 1),
                config: coder,
                attempt_limit: 128,
                queue_policy: Default::default(),
            });
            if self
                .coding
                .as_ref()
                .is_some_and(|coding| coding.coder_count == 2)
            {
                spec.planner.config["delegation"]["max_parallel_tasks"] = json!(2);
                spec.planner.config["initial_dependencies"] = json!(true);
                let mut second = spec
                    .workers
                    .last()
                    .ok_or(DomainError::InvalidState)?
                    .clone();
                second.name = "coder2".into();
                let guidance = " In two-coder mode, use delegate_tasks with coder and coder2 for independent subtasks on distinct files. Name the assigned files in each goal and avoid overlapping edits. Both children receive the same starting snapshot; inspect and approve each successful patch separately, handle conflicts without overwriting changes, then run fresh combined tests. Declare one initial dependency with depends_on on the receiving assignment when the other coder must finish first.";
                second.description.push_str(guidance);
                spec.workers
                    .last_mut()
                    .ok_or(DomainError::InvalidState)?
                    .description
                    .push_str(guidance);
                spec.workers.push(second);
            }
        }
        for config in std::iter::once(&mut spec.planner.config)
            .chain(spec.workers.iter_mut().map(|worker| &mut worker.config))
        {
            if let Some(tools) = config["tools"].as_array_mut()
                && tools.contains(&json!(DefinitionId::new("read_file", 2)))
            {
                tools.push(json!(DefinitionId::new("read_files", 1)));
            }
        }
        spec.validate()?;
        Ok(spec)
    }
    fn base_team_spec(&self) -> Result<TeamSpec, DomainError> {
        self.validate()?;
        let tools = if self.workspace.is_some() {
            vec![
                DefinitionId::new("list_files", 2),
                DefinitionId::new("read_file", 2),
                DefinitionId::new("search_text", 1),
            ]
        } else {
            vec![]
        };
        let budget = self.budget.clone().unwrap_or_default();
        let common = json!({"goal":self.prompt,"target":self.target,"max_model_calls":budget.max_model_calls,"max_tokens":budget.max_tokens,"max_output_tokens":2048,"memory":{"max_entries":8,"max_bytes":32768}});
        let mut planner = common.clone();
        planner["task_planning"] = json!(true);
        planner["goal_budget"] = json!(budget);
        planner["delegation"] = json!({"max_tasks":4,"max_depth":2});
        planner["tools"] = json!(tools);
        if self.auto_execute {
            planner["auto_execute"] = json!(true);
        }
        if self.discover_project {
            planner["discover_project"] = json!(true);
            planner["task_planning"] = json!(false);
        }
        let mut helper = common;
        if self.workspace.is_some() {
            planner["load_project_instructions"] = json!(true);
            helper["load_project_instructions"] = json!(true);
        }
        helper["tools"] = json!(tools);
        if let Some(coding) = &self.coding {
            let mut coding_tools = tools;
            coding_tools.extend([
                DefinitionId::new("replace_text", 1),
                DefinitionId::new("create_file", 1),
                DefinitionId::new("run_tests", 1),
            ]);
            if !coding.project_commands.is_empty() {
                coding_tools.push(DefinitionId::new("run_command", 1));
                planner["project_commands"] = json!(coding.project_commands);
            }
            planner["tools"] = json!(coding_tools);
            planner["writable_paths"] = json!(coding.writable_paths);
            planner["test_command"] = json!(coding.test_command);
            planner["change_review"] = json!(true);
            planner["completion_policy"] = json!(completion::CompletionPolicy::RequirePassingTests);
            planner["workspace_handoff"] = json!(true);
            helper["change_review"] = json!(true);
            helper["accept_workspace_handoff"] = json!(true);
        }
        let spec = TeamSpec {
            version:1,
            name:format!("{SESSION_PREFIX}{}",self.message_id),
            source:self.workspace.clone(),
            planner:TeamActorSpec { config:planner,attempt_limit:if self.coding.is_some() {128} else {64},queue_policy:Default::default() },
            workers:vec![TeamWorkerSpec { name:"helper".into(), description:if self.workspace.is_some() { "Inspect the project copy and answer delegated questions. Source files are read-only." } else { "Analyze delegated questions and return a concise useful answer." }.into(),adapter:DefinitionId::new(if self.workspace.is_some() {"tool"} else {"answer"},1),config:helper,attempt_limit:64,queue_policy:Default::default() }],
        };
        spec.validate()?;
        Ok(spec)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HarnessReceipt {
    pub session_id: SessionId,
    pub team_id: TeamId,
    pub task_id: MessageId,
    pub sequence: u64,
}
#[derive(Debug, Serialize)]
pub struct HarnessSession {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_session_id: Option<SessionId>,
    pub title: Option<String>,
    pub archived: bool,
    pub metadata_revision: u64,
    pub session_id: SessionId,
    pub team_id: TeamId,
    pub task_id: MessageId,
    pub prompt_preview: String,
    pub prompt_truncated: bool,
    pub model: String,
    pub workspace: Option<String>,
    pub coding: Option<HarnessCoding>,
    pub created_at: i64,
    pub root_status: String,
    pub paused: bool,
    pub cancelled: bool,
    pub setup_error: Option<String>,
    pub turn_count: u64,
    pub can_continue: bool,
}
#[derive(Debug, Serialize)]
pub struct HarnessSessions {
    pub items: Vec<HarnessSession>,
    pub truncated: bool,
    pub next_after: Option<super::sessions::SessionListCursor>,
}

#[derive(Debug, Serialize)]
pub struct HarnessTurn {
    pub task_id: MessageId,
    pub sequence: u64,
    pub prompt: String,
    pub prompt_truncated: bool,
    pub response: Option<String>,
    pub response_truncated: bool,
    pub status: String,
    pub created_at: i64,
    pub settled: bool,
}
#[derive(Debug, Serialize)]
pub struct HarnessConversation {
    pub auto_execute: bool,
    pub discover_project: bool,
    pub title: Option<String>,
    pub archived: bool,
    pub session_id: SessionId,
    pub team_id: TeamId,
    pub model: String,
    pub workspace: Option<String>,
    pub coding: Option<HarnessCoding>,
    pub can_continue: bool,
    pub turns: Vec<HarnessTurn>,
    /// Fetch older turns with this exclusive sequence boundary.
    pub next_before: Option<u64>,
}
