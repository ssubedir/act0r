use super::{Engine, Error, ToolRegistry, ports::*};
use crate::domain::{tool_recovery::*, tools::NativeToolRequest, *};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct ToolRecoveryView {
    pub activity_id: ActivityId,
    pub attempt_id: AttemptId,
    pub request: NativeToolRequest,
    pub diagnostic: Option<Outcome>,
    pub interrupted_at: Option<i64>,
    pub paused: bool,
    pub retry_allowed: bool,
    pub inspection: Option<ToolInspection>,
    pub inspection_error: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct ToolRecoveryItem {
    pub task_id: MessageId,
    pub run_id: RunId,
    pub status: RunStatus,
    pub recovery: Option<ToolRecoveryView>,
    pub reconciliation_receipts: Vec<ReconciliationReceipt>,
}
#[derive(Debug, Serialize)]
pub struct ToolRecoveries {
    pub items: Vec<ToolRecoveryItem>,
    pub truncated: bool,
}
pub struct ToolRecoveryService {
    engine: Engine,
    tools: Arc<ToolRegistry>,
    inspector: Arc<dyn ToolRecoveryInspectionPort>,
}
impl ToolRecoveryService {
    pub fn new(
        engine: Engine,
        tools: Arc<ToolRegistry>,
        inspector: Arc<dyn ToolRecoveryInspectionPort>,
    ) -> Self {
        Self {
            engine,
            tools,
            inspector,
        }
    }
    fn request(&self, activity: &Activity) -> Result<NativeToolRequest, Error> {
        let request: NativeToolRequest = serde_json::from_value(activity.spec.input.clone())
            .map_err(|_| {
                DomainError::InvalidReconciliation(
                    "decision must identify a saved native tool request".into(),
                )
            })?;
        if !supported(&request, &activity.spec.handler)
            || activity.spec.recovery != RecoveryPolicy::RequiresReconciliation
        {
            return Err(DomainError::InvalidReconciliation(
                "only saved native edit/create/test/command calls and MCP actions are recoverable here".into(),
            )
            .into());
        }
        request.definition.validate()?;
        request.definition.validate_arguments(&request.arguments)?;
        crate::domain::tools::validate_tool_workspace(
            std::slice::from_ref(&request.definition),
            request.workspace.as_deref(),
        )?;
        crate::domain::tools::validate_tool_permissions(
            std::slice::from_ref(&request.definition),
            &request.writable_paths,
        )?;
        crate::domain::tools::validate_tool_test_command(
            std::slice::from_ref(&request.definition),
            request.test_command.as_ref(),
        )?;
        crate::domain::tools::validate_project_command(
            &request.definition,
            &request.arguments,
            request.project_command.as_ref(),
        )?;
        Ok(request)
    }
    pub async fn status(&self, task: MessageId, run: Run) -> Result<ToolRecoveryItem, Error> {
        let mut recovery = None;
        if run.record().status == RunStatus::Blocked
            && !run.record().controls.cancel_requested
            && run
                .record()
                .error
                .as_ref()
                .is_some_and(|error| error.code == "uncertain_outcome")
            && let Some(activity) = run.activities().iter().find(|activity| {
                activity.status == ActivityStatus::Indeterminate
                    && activity.active_attempt.is_none()
                    && serde_json::from_value::<NativeToolRequest>(activity.spec.input.clone())
                        .is_ok_and(|request| supported(&request, &activity.spec.handler))
            })
            && let Some(attempt) = run.attempts().iter().find(|attempt| {
                attempt.activity_id == activity.id
                    && attempt.ordinal == activity.attempts
                    && attempt.status == AttemptStatus::Interrupted
                    && matches!(attempt.outcome, Some(Outcome::Uncertain(_)))
            })
        {
            let request = self.request(activity)?;
            let compatible = self
                .tools
                .definition(&request.definition.id)
                .is_ok_and(|definition| *definition == request.definition);
            let external = external_action(&request);
            let observed = if compatible && external {
                Ok(None)
            } else if compatible {
                self.inspector
                    .inspect_tool(activity.id, request.clone())
                    .await
                    .map(Some)
            } else {
                Err(Error::Unavailable(
                    "restore this tool's exact pinned contract before retrying".into(),
                ))
            };
            let (inspection, inspection_error) = match observed {
                Ok(view) => (view, None),
                Err(error) => (None, Some(error.to_string())),
            };
            recovery = Some(ToolRecoveryView {
                activity_id: activity.id,
                attempt_id: attempt.id,
                request,
                diagnostic: attempt.outcome.clone(),
                interrupted_at: attempt.ended_at,
                paused: run.record().controls.pause_requested
                    || run.record().controls.boundary_pause,
                retry_allowed: run.record().allowance.admitted < run.record().allowance.limit
                    && activity.attempts < activity.spec.retry.max_attempts
                    && compatible
                    && (external
                        || inspection
                            .as_ref()
                            .is_some_and(|view| view.conflict.is_none())),
                inspection,
                inspection_error,
            });
        }
        let receipts = run
            .record()
            .reconciliation_receipts
            .iter()
            .filter(|receipt| {
                run.activities().iter().any(|activity| {
                    activity.id == receipt.request.activity_id && self.request(activity).is_ok()
                })
            })
            .cloned()
            .collect();
        Ok(ToolRecoveryItem {
            task_id: task,
            run_id: run.record().id,
            status: run.record().status,
            recovery,
            reconciliation_receipts: receipts,
        })
    }
    pub async fn reconcile(&self, run: Run, request: ToolRecoveryRequest) -> Result<(), Error> {
        request.validate()?;
        let activity = run
            .activities()
            .iter()
            .find(|activity| activity.id == request.activity_id)
            .ok_or(DomainError::UnknownAttempt)?;
        let saved_request = self.request(activity)?;
        if let ToolRecoveryDecision::Accept { result } = &request.decision {
            validate_external_result(&saved_request, result)?;
        }
        let recorded = run
            .record()
            .reconciliation_receipts
            .iter()
            .any(|receipt| receipt.request == request.clone().into());
        if external_action(&saved_request)
            && matches!(request.decision, ToolRecoveryDecision::Retry {})
            && !recorded
        {
            self.tools
                .check_pins(std::slice::from_ref(&saved_request.definition))?;
        }
        if !run.attempts().iter().any(|attempt| {
            attempt.id == request.attempt_id && attempt.activity_id == request.activity_id
        }) {
            return Err(DomainError::UnknownAttempt.into());
        }
        self.engine.reconcile(run.record().id, request.into()).await
    }
}
