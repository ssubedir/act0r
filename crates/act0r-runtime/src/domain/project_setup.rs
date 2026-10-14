//! Language-neutral proposals are data until reviewed into a pinned coding session.
use super::{
    DomainError, MessageId, RunId, SessionId, UsageBudget, UsageLedger, harness::HarnessCoding,
    tools::WorkspaceTestCommand,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
#[cfg(test)]
mod review_tests;

/// The editable coding settings of an existing assembly. Models and budgets stay pinned.
pub fn coding_settings(spec: &super::teams::TeamSpec) -> Result<HarnessCoding, DomainError> {
    let config = &spec.planner.config;
    let coding = HarnessCoding {
        writable_paths: serde_json::from_value(config["writable_paths"].clone())
            .map_err(|_| DomainError::InvalidState)?,
        test_command: serde_json::from_value(config["test_command"].clone())
            .map_err(|_| DomainError::InvalidState)?,
        project_commands: config
            .get("project_commands")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| DomainError::InvalidState)?
            .unwrap_or_default(),
        coder_count: config["delegation"]["max_parallel_tasks"]
            .as_u64()
            .unwrap_or(1)
            .try_into()
            .map_err(|_| DomainError::InvalidState)?,
    };
    coding.validate()?;
    Ok(coding)
}

/// Revise only human-reviewed paths and commands in fresh editable actors.
pub fn with_coding_settings(
    original: &super::teams::TeamSpec,
    coding: &HarnessCoding,
) -> Result<super::teams::TeamSpec, DomainError> {
    coding.validate()?;
    let previous = coding_settings(original)?;
    if coding.coder_count != previous.coder_count {
        return Err(DomainError::InvalidConfiguration(
            "project setup updates retain the existing coding worker count".into(),
        ));
    }
    if coding == &previous {
        return Err(DomainError::InvalidConfiguration(
            "no project setup changes were selected; use project refresh to keep the same settings"
                .into(),
        ));
    }
    let mut spec = original.clone();
    for config in std::iter::once(&mut spec.planner.config).chain(
        spec.workers
            .iter_mut()
            .filter(|worker| {
                worker.config["writable_paths"]
                    .as_array()
                    .is_some_and(|paths| !paths.is_empty())
            })
            .map(|worker| &mut worker.config),
    ) {
        config["writable_paths"] = json!(coding.writable_paths);
        config["test_command"] = json!(coding.test_command);
        let object = config.as_object_mut().ok_or(DomainError::InvalidState)?;
        if coding.project_commands.is_empty() {
            object.remove("project_commands");
        } else {
            object.insert("project_commands".into(), json!(coding.project_commands));
        }
        let tools = object
            .get_mut("tools")
            .and_then(Value::as_array_mut)
            .ok_or(DomainError::InvalidState)?;
        let command = json!(super::DefinitionId::new("run_command", 1));
        if coding.project_commands.is_empty() {
            tools.retain(|tool| tool["name"] != "run_command");
        } else if !tools.contains(&command) {
            tools.push(command);
        }
    }
    spec.validate()?;
    Ok(spec)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupCommand {
    pub name: String,
    pub argv: Vec<String>,
    pub timeout_ms: u32,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSetupProposal {
    pub summary: String,
    pub writable_paths: Vec<String>,
    pub test_command: WorkspaceTestCommand,
    pub commands: Vec<SetupCommand>,
}
impl ProjectSetupProposal {
    pub fn coding(&self) -> Result<HarnessCoding, DomainError> {
        if self.summary.trim().is_empty()
            || self.summary.len() > 4096
            || self.commands.len() > 16
            || serde_json::to_vec(self)
                .map_err(|_| DomainError::InvalidState)?
                .len()
                > 65536
        {
            return Err(DomainError::InvalidConfiguration(
                "project setup needs a bounded explanation and at most 16 commands".into(),
            ));
        }
        let mut commands = std::collections::BTreeMap::new();
        for command in &self.commands {
            if commands
                .insert(
                    command.name.clone(),
                    WorkspaceTestCommand {
                        argv: command.argv.clone(),
                        timeout_ms: command.timeout_ms,
                    },
                )
                .is_some()
            {
                return Err(DomainError::InvalidConfiguration(
                    "project setup command names must be distinct".into(),
                ));
            }
        }
        let coding = HarnessCoding {
            writable_paths: self.writable_paths.clone(),
            test_command: self.test_command.clone(),
            project_commands: commands,
            coder_count: 1,
        };
        coding.validate()?;
        Ok(coding)
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewProjectSetup {
    pub coding: HarnessCoding,
}
#[derive(Debug, Serialize)]
pub struct ProjectSetupView {
    pub auto_execute: bool,
    pub session_id: SessionId,
    pub task_id: MessageId,
    pub run_id: Option<RunId>,
    pub workspace: String,
    pub proposal: Option<ProjectSetupProposal>,
    pub coding_session_id: SessionId,
    pub accepted: Option<super::harness::HarnessReceipt>,
    pub accepted_coding: Option<HarnessCoding>,
    pub discovery_context: Option<super::discovery_context::DiscoveryContext>,
    pub can_accept: bool,
}
/// One coding start per physical discovery task, including concurrent/replayed review.
pub fn coding_start_id(session: SessionId, task: MessageId) -> MessageId {
    let digest = Sha256::digest(format!("act0r-project-setup-v1:{session}:{task}"));
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
        .to_string()
        .parse()
        .expect("derived UUID")
}
pub fn remaining_budget(
    budget: &UsageBudget,
    usage: &UsageLedger,
) -> Result<UsageBudget, DomainError> {
    let remaining = UsageBudget {
        max_model_calls: budget.remaining_model_calls(usage),
        max_tokens: budget.remaining_tokens(usage),
    };
    remaining.validate().map_err(|_| DomainError::InvalidConfiguration("project discovery exhausted the request budget; start a new request with sufficient limits".into()))?;
    Ok(remaining)
}
pub fn action_schema() -> Value {
    let command = json!({"type":"object","properties":{"argv":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":128},"timeout_ms":{"type":"integer","minimum":1,"maximum":600000}},"required":["argv","timeout_ms"],"additionalProperties":false});
    let mut named = command.clone();
    named["properties"]["name"] = json!({"type":"string","minLength":1,"maxLength":64});
    named["required"]
        .as_array_mut()
        .unwrap()
        .push(json!("name"));
    json!({"type":"object","properties":{"kind":{"type":"string","enum":["propose_project_setup"]},"proposal":{"type":"object","properties":{"summary":{"type":"string","minLength":1,"maxLength":4096},"writable_paths":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":128},"test_command":command,"commands":{"type":"array","items":named,"maxItems":16}},"required":["summary","writable_paths","test_command","commands"],"additionalProperties":false}},"required":["kind","proposal"],"additionalProperties":false})
}
pub const INSTRUCTIONS: &str = " You are inspecting the project to handle the user's goal. First use list_files and read_file/search_text to learn its instructions, documentation, structure, manifests and scripts. Support any language, mixed projects and custom tooling; no manifest type is required. For a request that only needs analysis, finish with the answer. If edits are needed, use propose_project_setup to return a focused writable scope, one meaningful verification command and optional named commands, with a summary explaining evidence from the files you inspected. Commands are direct executable/argument arrays, not shell strings; account for the host OS. Do not default to Cargo or invent commands. If the verification procedure or required executable is unclear, ask_human for the missing information. Use finish_incomplete for an actual blocker. You cannot edit, execute commands or delegate during discovery. Proposing setup grants no authority and executes nothing; the user reviews and may edit it before a separate durable coding session runs the original goal.";
