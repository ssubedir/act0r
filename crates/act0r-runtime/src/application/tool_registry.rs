use super::{Error, ports::*};
use crate::domain::{tools::*, *};
use std::{collections::HashMap, sync::Arc};

struct RegisteredTool {
    definition: ToolDefinition,
    implementation: Arc<dyn NativeTool>,
}
#[derive(Default)]
pub struct ToolRegistry {
    tools: HashMap<DefinitionId, RegisteredTool>,
    workspace_defaults: Vec<DefinitionId>,
    general_defaults: Vec<DefinitionId>,
    external_inspection: bool,
}
impl ToolRegistry {
    pub fn new(tools: Vec<Arc<dyn NativeTool>>) -> Result<Self, Error> {
        let mut registry = HashMap::new();
        for implementation in tools {
            let definition = implementation.definition();
            definition.validate()?;
            if registry
                .insert(
                    definition.id.clone(),
                    RegisteredTool {
                        definition,
                        implementation,
                    },
                )
                .is_some()
            {
                return Err(Error::Unavailable(
                    "duplicate native tool registration".into(),
                ));
            }
        }
        Ok(Self {
            tools: registry,
            workspace_defaults: vec![],
            general_defaults: vec![],
            external_inspection: false,
        })
    }
    /// Host-approved read-only tools are pinned only into newly created workspace actors.
    pub fn with_workspace_defaults(mut self, ids: Vec<DefinitionId>) -> Result<Self, Error> {
        if ids.len() > 8 {
            return Err(Error::Unavailable(
                "at most eight workspace tool defaults are supported".into(),
            ));
        }
        for id in &ids {
            let tool = self.definition(id)?;
            if tool.external.as_ref().is_none_or(|c| !c.read_only) || !tool.requires_workspace {
                return Err(Error::Unavailable(
                    "workspace tool defaults must be host-approved read-only external tools".into(),
                ));
            }
        }
        self.workspace_defaults = ids;
        Ok(self)
    }
    pub fn pin_workspace_defaults(&self, tools: &mut Vec<DefinitionId>) {
        if self.external_inspection {
            tools.retain(|id| {
                !matches!(
                    (id.name.as_str(), id.version),
                    ("read_file" | "list_files", 2) | ("read_files" | "search_text", 1)
                )
            });
        }
        for id in &self.workspace_defaults {
            if !tools.contains(id) {
                tools.push(id.clone());
            }
        }
    }
    /// Host-approved tools usable by fresh tool/planner actors with or without a project.
    pub fn with_general_defaults(mut self, ids: Vec<DefinitionId>) -> Result<Self, Error> {
        if ids.len() > 8 {
            return Err(Error::Unavailable(
                "at most eight general tool defaults are supported".into(),
            ));
        }
        for id in &ids {
            let tool = self.definition(id)?;
            if tool.external.is_none() || tool.requires_workspace {
                return Err(Error::Unavailable("general tool defaults must be host-selected external tools without workspace requirements".into()));
            }
        }
        self.general_defaults = ids;
        Ok(self)
    }
    pub fn pin_general_defaults(&self, tools: &mut Vec<DefinitionId>) {
        for id in &self.general_defaults {
            if !tools.contains(id) {
                tools.push(id.clone());
            }
        }
    }
    /// Fresh actors use the host's external inspection catalog directly. Existing
    /// registrations stay installed for exact recovery of previously pinned work.
    pub fn with_external_inspection(mut self, enabled: bool) -> Result<Self, Error> {
        if enabled && self.workspace_defaults.is_empty() {
            return Err(Error::Unavailable(
                "external inspection requires configured workspace tools".into(),
            ));
        }
        self.external_inspection = enabled;
        Ok(self)
    }
    /// Extend a startup catalog without replacing any installed versioned contract.
    /// The returned registry must be shared by actor adapters, execution, and discovery.
    pub fn with_tools(mut self, tools: Vec<Arc<dyn NativeTool>>) -> Result<Self, Error> {
        let additional = Self::new(tools)?;
        if additional
            .tools
            .keys()
            .any(|id| self.tools.contains_key(id))
        {
            return Err(Error::Unavailable(
                "duplicate native tool registration".into(),
            ));
        }
        self.tools.extend(additional.tools);
        Ok(self)
    }
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        let mut tools: Vec<_> = self.tools.values().map(|t| t.definition.clone()).collect();
        tools.sort_by(|a, b| (&a.id.name, a.id.version).cmp(&(&b.id.name, b.id.version)));
        tools
    }
    pub fn definition(&self, id: &DefinitionId) -> Result<&ToolDefinition, Error> {
        self.tools.get(id).map(|t| &t.definition).ok_or_else(|| {
            Error::Unavailable(format!(
                "native tool {} v{} is not registered",
                id.name, id.version
            ))
        })
    }
    pub fn pin(&self, ids: &[DefinitionId]) -> Result<Vec<ToolDefinition>, Error> {
        ids.iter().map(|id| self.definition(id).cloned()).collect()
    }
    pub fn check_pins(&self, pins: &[ToolDefinition]) -> Result<(), Error> {
        for pin in pins {
            if self.definition(&pin.id)? != pin {
                return Err(Error::Unavailable(format!(
                    "restore pinned native tool {} v{} and its exact contract",
                    pin.id.name, pin.id.version
                )));
            }
        }
        Ok(())
    }
    pub async fn execute(&self, execution: ActivityExecution) -> Result<Outcome, Error> {
        let request: NativeToolRequest = serde_json::from_value(execution.input)?;
        let tool = self
            .tools
            .get(&request.definition.id)
            .ok_or_else(|| Error::Unavailable("pinned native tool is not installed".into()))?;
        if tool.definition != request.definition {
            return Err(Error::Unavailable(
                "pinned native tool contract differs".into(),
            ));
        }
        let expected_handler = tool.definition.handler();
        if execution.handler != expected_handler
            || (!tool.definition.requires_workspace && request.workspace.is_some())
            || (!tool.definition.requires_writable_paths && !request.writable_paths.is_empty())
            || (!tool.definition.requires_test_command && request.test_command.is_some())
            || (!tool.definition.requires_project_command && request.project_command.is_some())
        {
            return Err(Error::Unavailable(
                "native tool handler or workspace context differs from its pinned contract".into(),
            ));
        }
        if let Err(error) = validate_tool_workspace(
            std::slice::from_ref(&tool.definition),
            request.workspace.as_deref(),
        ) {
            return Ok(Outcome::Failure(Failure::new(
                "tool_workspace",
                error.to_string(),
                false,
            )));
        }
        if let Err(error) = tool.definition.validate_arguments(&request.arguments) {
            return Ok(Outcome::Failure(Failure::new(
                "tool_input",
                error.to_string(),
                false,
            )));
        }
        if let Err(error) = validate_tool_permissions(
            std::slice::from_ref(&tool.definition),
            &request.writable_paths,
        ) {
            return Ok(Outcome::Failure(Failure::new(
                "tool_permissions",
                error.to_string(),
                false,
            )));
        }
        if let Err(error) = validate_tool_test_command(
            std::slice::from_ref(&tool.definition),
            request.test_command.as_ref(),
        ) {
            return Ok(Outcome::Failure(Failure::new(
                "tool_test_command",
                error.to_string(),
                false,
            )));
        }
        validate_project_command(
            &tool.definition,
            &request.arguments,
            request.project_command.as_ref(),
        )?;
        let result = tool
            .implementation
            .execute(NativeToolCall {
                run_id: execution.run_id,
                activity_id: execution.activity_id,
                attempt_id: execution.attempt_id,
                arguments: request.arguments,
                workspace: request.workspace,
                writable_paths: request.writable_paths,
                test_command: request.test_command,
                project_command: request.project_command,
            })
            .await?;
        let result = match result {
            NativeToolResult::Success(result) => result,
            NativeToolResult::Rejected(error) => {
                if !tool.definition.reports_errors {
                    return Ok(Outcome::Failure(Failure::new(
                        "tool_feedback_contract",
                        "this pinned tool does not declare error feedback",
                        false,
                    )));
                }
                match error.observation() {
                    Ok(result) => result,
                    Err(_) => {
                        return Ok(Outcome::Failure(Failure::new(
                            "tool_feedback_contract",
                            "tool returned invalid or oversized error feedback",
                            false,
                        )));
                    }
                }
            }
            NativeToolResult::Failure(failure) => return Ok(Outcome::Failure(failure)),
            NativeToolResult::Uncertain(reason) => return Ok(Outcome::Uncertain(reason)),
        };
        Ok(
            if result.to_string().len() <= tool.definition.max_output_bytes as usize {
                Outcome::Success(result)
            } else {
                Outcome::Failure(Failure::new(
                    "tool_output_limit",
                    "native tool output exceeded its pinned byte limit",
                    false,
                ))
            },
        )
    }
}
