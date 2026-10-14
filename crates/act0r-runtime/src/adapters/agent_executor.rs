use crate::{application::ports::*, domain::*};
use std::sync::Arc;

pub struct AgentExecutor {
    models: Arc<dyn ModelClient>,
    tools: Arc<dyn ToolRunner>,
    fallback: Arc<dyn ActivityExecutor>,
    native_tools: Option<Arc<crate::application::ToolRegistry>>,
    workspace_changes: Option<Arc<dyn WorkspaceChangesPort>>,
    patch_applications: Option<Arc<dyn PatchApplicationPort>>,
    project_verification: Option<Arc<dyn ProjectVerificationPort>>,
    child_changes: Option<Arc<dyn ChildChangesPort>>,
    workspace_handoff: Option<Arc<dyn WorkspaceHandoffPort>>,
}
impl AgentExecutor {
    pub fn new(
        models: Arc<dyn ModelClient>,
        tools: Arc<dyn ToolRunner>,
        fallback: Arc<dyn ActivityExecutor>,
    ) -> Self {
        Self {
            models,
            tools,
            fallback,
            native_tools: None,
            workspace_changes: None,
            patch_applications: None,
            project_verification: None,
            child_changes: None,
            workspace_handoff: None,
        }
    }
    pub fn with_native_tools(mut self, tools: Arc<crate::application::ToolRegistry>) -> Self {
        self.native_tools = Some(tools);
        self
    }
    pub fn with_workspace_changes(mut self, changes: Arc<dyn WorkspaceChangesPort>) -> Self {
        self.workspace_changes = Some(changes);
        self
    }
    pub fn with_patch_applications(mut self, applications: Arc<dyn PatchApplicationPort>) -> Self {
        self.patch_applications = Some(applications);
        self
    }
    pub fn with_project_verification(
        mut self,
        verification: Arc<dyn ProjectVerificationPort>,
    ) -> Self {
        self.project_verification = Some(verification);
        self
    }
    pub fn with_child_changes(mut self, changes: Arc<dyn ChildChangesPort>) -> Self {
        self.child_changes = Some(changes);
        self
    }
    pub fn with_workspace_handoff(mut self, handoff: Arc<dyn WorkspaceHandoffPort>) -> Self {
        self.workspace_handoff = Some(handoff);
        self
    }
}
impl ActivityExecutor for AgentExecutor {
    fn request_timeout_ms(&self, execution: &ActivityExecution) -> Option<u64> {
        if execution.handler == DefinitionId::new("agent.model", 1) {
            let request: agent::ModelRequest =
                serde_json::from_value(execution.input.clone()).ok()?;
            (request.target.provider == "openai")
                .then_some(u64::from(request.target.policy.request_timeout_ms))
        } else if matches!(execution.handler, DefinitionId { ref name, version: 5 | 6 } if name == "native.tool")
        {
            let request: tools::NativeToolRequest =
                serde_json::from_value(execution.input.clone()).ok()?;
            request
                .test_command
                .map(|command| u64::from(command.timeout_ms))
                .or_else(|| {
                    request
                        .project_command
                        .map(|selected| u64::from(selected.command.timeout_ms))
                })
                .map(|deadline| deadline.saturating_add(5000))
        } else if execution.handler == project_verification::handler("run") {
            serde_json::from_value::<project_verification::VerificationRequest>(
                execution.input.clone(),
            )
            .ok()
            .map(|r| u64::from(r.input.command.timeout_ms).saturating_add(5000))
        } else {
            self.fallback.request_timeout_ms(execution)
        }
    }
    fn supports(&self, handler: &DefinitionId) -> bool {
        handler == &DefinitionId::new("agent.model", 1)
            || handler == &DefinitionId::new("agent.tool", 1)
            || handler == &DefinitionId::new("search.feedback", 1)
            || (tools::is_native_tool_handler(handler) && self.native_tools.is_some())
            || (change_review::is_handler(handler) && self.workspace_changes.is_some())
            || (patch_application::is_handler(handler) && self.patch_applications.is_some())
            || (project_verification::is_handler(handler) && self.project_verification.is_some())
            || (child_changes::is_handler(handler) && self.child_changes.is_some())
            || (workspace_handoff::is_handler(handler) && self.workspace_handoff.is_some())
            || self.fallback.supports(handler)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if project_verification::is_handler(&execution.handler) {
                return self
                    .project_verification
                    .as_ref()
                    .ok_or_else(|| {
                        crate::Error::Unavailable("project verification is not configured".into())
                    })?
                    .execute(execution)
                    .await;
            }
            if workspace_handoff::is_handler(&execution.handler) {
                return self
                    .workspace_handoff
                    .as_ref()
                    .ok_or_else(|| {
                        crate::Error::Unavailable("workspace handoff is not configured".into())
                    })?
                    .execute(execution)
                    .await;
            }
            if child_changes::is_handler(&execution.handler) {
                return self
                    .child_changes
                    .as_ref()
                    .ok_or_else(|| {
                        crate::Error::Unavailable("child changes are not configured".into())
                    })?
                    .execute(execution)
                    .await;
            }
            if patch_application::is_handler(&execution.handler) {
                return self
                    .patch_applications
                    .as_ref()
                    .ok_or_else(|| {
                        crate::Error::Unavailable("patch application is not configured".into())
                    })?
                    .execute(execution)
                    .await;
            }
            if change_review::is_handler(&execution.handler) {
                let changes = self.workspace_changes.as_ref().ok_or_else(|| {
                    crate::Error::Unavailable("workspace change review is not configured".into())
                })?;
                return Ok(match changes.execute(execution).await {
                    Ok(result) => Outcome::Success(result),
                    Err(error) => Outcome::Failure(Failure::new(
                        "workspace_review",
                        error.to_string(),
                        matches!(error, crate::Error::Ownership(_) | crate::Error::Io(_)),
                    )),
                });
            }
            if tools::is_native_tool_handler(&execution.handler) {
                return self
                    .native_tools
                    .as_ref()
                    .ok_or_else(|| {
                        crate::Error::Unavailable("native tools are not configured".into())
                    })?
                    .execute(execution)
                    .await;
            }
            if execution.handler == DefinitionId::new("agent.model", 1) {
                let request = serde_json::from_value(execution.input)?;
                let reply = self.models.generate(request, execution.activity_id).await?;
                let code = reply
                    .diagnostics
                    .as_ref()
                    .map_or("model_response", |d| d.category.failure_code());
                return Ok(match (reply.decision, reply.usage) {
                    (Ok(decision), Some(usage)) => Outcome::SuccessWithUsage {
                        result: serde_json::to_value(decision)?,
                        usage,
                    },
                    (Ok(decision), None) => Outcome::Success(serde_json::to_value(decision)?),
                    (Err(message), Some(usage)) => Outcome::FailureWithUsage {
                        failure: Failure::new(code, message, reply.retryable)
                            .with_request(reply.diagnostics, reply.retry_after),
                        usage,
                    },
                    (Err(message), None) => Outcome::Failure(
                        Failure::new(code, message, reply.retryable)
                            .with_request(reply.diagnostics, reply.retry_after),
                    ),
                });
            }
            if matches!(execution.handler, ref h if *h == DefinitionId::new("agent.tool", 1) || *h == DefinitionId::new("search.feedback", 1))
            {
                let request = serde_json::from_value(execution.input)?;
                return Ok(Outcome::Success(
                    self.tools.execute(request, execution.activity_id).await?,
                ));
            }
            self.fallback.execute(execution).await
        })
    }
}
