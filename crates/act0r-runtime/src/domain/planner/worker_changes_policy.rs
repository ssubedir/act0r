use super::*;
use crate::domain::{planner_pause::WorkerDependency, worker_changes};

pub(super) fn allowed(value: &Value, command: &Command) -> bool {
    let Ok(state) = decode::<PlannerState>(value) else {
        return false;
    };
    if !state.config.worker_changes || state.coordinating_guidance.is_none() {
        return false;
    }
    match command {
        Command::Schedule(spec) if spec.handler == worker_changes::handler("inspect") => {
            decode::<worker_changes::InspectWorkerChanges>(&spec.input).is_ok_and(|r| {
                r.validate().is_ok()
                    && state
                        .dependency(&r.dependency.worker)
                        .is_ok_and(|d| d == r.dependency)
                    && state.worker_change_inspections < worker_changes::MAX_INSPECTIONS
            })
        }
        Command::Schedule(spec) if spec.handler == worker_changes::handler("import") => {
            decode::<child_changes::Inspection>(&spec.input).is_ok_and(|i| {
                state.validate_worker_inspection(&i).is_ok()
                    && state.inspection.as_ref() == Some(&i)
                    && state.worker_change_imports <= worker_changes::MAX_IMPORTS
                    && matches!(state.pending, Some(AgentAction::ImportWorkerChanges { .. }))
            })
        }
        Command::WaitForInput { key, payload, .. }
            if key == child_changes::APPROVAL_KEY && payload["worker_transfer"] == true =>
        {
            decode::<child_changes::Inspection>(&payload["inspection"]).is_ok_and(|i| {
                state.validate_worker_inspection(&i).is_ok()
                    && !i.preview.has_conflicts()
                    && state.inspection.as_ref() == Some(&i)
                    && state.worker_change_imports < worker_changes::MAX_IMPORTS
            })
        }
        _ => false,
    }
}

impl PlannerState {
    fn dependency(&self, worker: &str) -> Result<WorkerDependency, DomainError> {
        if !self.config.worker_changes {
            return Err(DomainError::InvalidWorkflow(
                "worker patch handoff is disabled".into(),
            ));
        }
        let d = self
            .coordinating_guidance
            .as_ref()
            .and_then(|g| g.dependencies.iter().find(|d| d.worker == worker))
            .ok_or_else(|| {
                DomainError::InvalidWorkflow(
                    "choose a receiving worker from the captured dependency review".into(),
                )
            })?;
        worker_changes::InspectWorkerChanges {
            dependency: d.clone(),
            offset: 0,
        }
        .validate()?;
        Ok(d.clone())
    }
    pub(super) fn validate_worker_inspection(
        &self,
        i: &child_changes::Inspection,
    ) -> Result<(), DomainError> {
        let d = i
            .request
            .dependency
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        if json!(d) != json!(self.dependency(&d.worker)?)
            || !self
                .config
                .workers
                .iter()
                .any(|w| w.name == d.prerequisite && w.agent_id == i.request.recipient)
        {
            return Err(DomainError::InvalidState);
        }
        i.validate()
    }
}
pub(super) fn select(
    state: &mut PlannerState,
    action: AgentAction,
) -> Result<Command, DomainError> {
    let command = match &action {
        AgentAction::InspectWorkerChanges { worker, offset } => {
            if state.worker_change_inspections >= worker_changes::MAX_INSPECTIONS {
                return Err(DomainError::InvalidWorkflow(
                    "worker patch inspection allowance exhausted".into(),
                ));
            }
            let request = worker_changes::InspectWorkerChanges {
                dependency: state.dependency(worker)?,
                offset: *offset,
            };
            request.validate()?;
            state.worker_change_inspections += 1;
            state.inspection = None;
            worker_changes::activity("inspect", json!(request))
        }
        AgentAction::ImportWorkerChanges { worker } => {
            if state.worker_change_imports >= worker_changes::MAX_IMPORTS {
                return Err(DomainError::InvalidWorkflow(
                    "worker patch import allowance exhausted".into(),
                ));
            }
            let inspection = state.inspection.as_ref().ok_or_else(|| {
                DomainError::InvalidWorkflow(
                    "inspect the prerequisite patch before requesting a transfer".into(),
                )
            })?;
            state.validate_worker_inspection(inspection)?;
            if inspection
                .request
                .dependency
                .as_ref()
                .is_none_or(|d| d.worker != *worker)
                || inspection.preview.has_conflicts()
            {
                return Err(DomainError::InvalidWorkflow(
                    "transfer requires this receiver's latest conflict-free inspection".into(),
                ));
            }
            state.worker_change_imports += 1;
            if state.config.auto_execute {
                worker_changes::activity("import", json!(inspection))
            } else {
                Command::WaitForInput {
                    key: child_changes::APPROVAL_KEY.into(),
                    prompt: format!(
                        "Import {}'s reviewed patch into paused {worker}'s assignment?",
                        inspection.request.worker
                    ),
                    payload: json!({"inspection":inspection,"worker_transfer":true}),
                }
            }
        }
        _ => return Err(DomainError::InvalidState),
    };
    state.pending = Some(action);
    Ok(command)
}
pub(super) fn event(
    state: &mut PlannerState,
    event: &WorkflowEvent,
) -> Result<Option<Command>, DomainError> {
    match event {
        WorkflowEvent::ActivityCompleted {
            activity_id,
            handler,
            result,
        } if handler == &worker_changes::handler("inspect") => {
            let action = state.pending.take().ok_or(DomainError::InvalidState)?;
            let AgentAction::InspectWorkerChanges { worker, offset } = &action else {
                return Err(DomainError::InvalidState);
            };
            let observation = if result["ok"] == true {
                let inspection: child_changes::Inspection = decode(&result["inspection"])?;
                state.validate_worker_inspection(&inspection)?;
                if inspection.preview.preparation_id != *activity_id
                    || inspection.request.offset != *offset
                    || inspection
                        .request
                        .dependency
                        .as_ref()
                        .is_none_or(|d| d.worker != *worker)
                {
                    return Err(DomainError::InvalidState);
                }
                let mut value = inspection.observation();
                value["receiving_worker"] = json!(worker);
                state.inspection = Some(inspection);
                value
            } else if result["ok"] == false {
                result.clone()
            } else {
                return Err(DomainError::InvalidState);
            };
            state.observe(
                ObservationKind::Tool,
                json!({"action":action,"result":observation}),
            )?;
            Ok(Some(model(state)?))
        }
        WorkflowEvent::ActivityCompleted {
            handler, result, ..
        } if handler == &worker_changes::handler("import") => {
            let action = state.pending.take().ok_or(DomainError::InvalidState)?;
            if !matches!(action, AgentAction::ImportWorkerChanges { .. }) {
                return Err(DomainError::InvalidState);
            }
            let inspection = state.inspection.take().ok_or(DomainError::InvalidState)?;
            state.validate_worker_inspection(&inspection)?;
            if result["ok"] == false {
                state.observe(
                    ObservationKind::Tool,
                    json!({"action":action,"result":result}),
                )?;
                return Ok(Some(model(state)?));
            }
            let report: patch_application::ApplicationReport = decode(result)?;
            let paths: Vec<_> = inspection
                .preview
                .files
                .iter()
                .map(|f| f.path.clone())
                .collect();
            if report.run_id != inspection.preview.run_id
                || report.target != inspection.preview.target
                || match report.status {
                    patch_application::ApplicationStatus::Applied => {
                        report.applied_files != paths || !report.conflicts.is_empty()
                    }
                    patch_application::ApplicationStatus::Conflicted => {
                        !report.applied_files.is_empty()
                            || report.conflicts.is_empty()
                            || report
                                .conflicts
                                .iter()
                                .any(|f| !paths.contains(&f.path) || f.conflict.is_none())
                    }
                    patch_application::ApplicationStatus::Rejected => true,
                }
            {
                return Err(DomainError::InvalidState);
            }
            state.observe(ObservationKind::Tool,json!({"action":action,"result":{"ok":report.status==patch_application::ApplicationStatus::Applied,"status":report.status,"dependency_id":inspection.request.dependency.as_ref().unwrap().activity_id,"task_id":inspection.request.dependency.as_ref().unwrap().task_id,"artifact_id":inspection.artifact_id,"applied_file_count":report.applied_files.len(),"conflicts":report.conflicts.iter().take(16).collect::<Vec<_>>()}}))?;
            Ok(Some(model(state)?))
        }
        WorkflowEvent::InputReceived { key, payload, .. }
            if key == child_changes::APPROVAL_KEY
                && matches!(state.pending, Some(AgentAction::ImportWorkerChanges { .. })) =>
        {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Approval {
                approved: bool,
            }
            let approval: Approval = decode(payload)?;
            let inspection = state.inspection.as_ref().ok_or(DomainError::InvalidState)?;
            state.validate_worker_inspection(inspection)?;
            if approval.approved {
                Ok(Some(worker_changes::activity("import", json!(inspection))))
            } else {
                let action = state.pending.take().ok_or(DomainError::InvalidState)?;
                state.inspection = None;
                state.observe(
                    ObservationKind::Tool,
                    json!({"action":action,"result":{"ok":false,"approved":false}}),
                )?;
                Ok(Some(model(state)?))
            }
        }
        _ => Ok(None),
    }
}
pub(super) fn schema(state: &PlannerState, schema: &mut Value, instructions: &mut String) {
    if !state.config.worker_changes || state.coordinating_guidance.is_none() {
        return;
    }
    let workers: Vec<_> = state
        .coordinating_guidance
        .as_ref()
        .unwrap()
        .dependencies
        .iter()
        .filter(|d| d.outcome == Some(actors::AgentTaskStatus::Succeeded))
        .map(|d| d.worker.clone())
        .collect();
    if workers.is_empty() {
        return;
    }
    let action = &mut schema["properties"]["action"];
    let mut variants = if let Some(v) = action["anyOf"].as_array() {
        v.clone()
    } else {
        vec![action.clone()]
    };
    if state.worker_change_inspections < worker_changes::MAX_INSPECTIONS {
        variants.push(json!({"type":"object","properties":{"kind":{"type":"string","enum":["inspect_worker_changes"]},"worker":{"type":"string","enum":workers},"offset":{"type":"integer","minimum":0,"maximum":4194304}},"required":["kind","worker","offset"],"additionalProperties":false}));
    }
    if state.worker_change_imports < worker_changes::MAX_IMPORTS
        && state
            .inspection
            .as_ref()
            .is_some_and(|i| i.request.dependency.is_some() && !i.preview.has_conflicts())
    {
        variants.push(json!({"type":"object","properties":{"kind":{"type":"string","enum":["import_worker_changes"]},"worker":{"type":"string","enum":[state.inspection.as_ref().unwrap().request.dependency.as_ref().unwrap().worker]}},"required":["kind","worker"],"additionalProperties":false}));
    }
    *action = json!({"anyOf":variants});
    *instructions = instructions.replace(
        "or create another wait here",
        "or create unrelated waits here",
    );
    instructions.push_str(" worker_changes allows up to three inspect_worker_changes actions and one import_worker_changes approval request per coordination step. worker selects the waiting assignment from a successful captured dependency. Inspect its prerequisite's immutable patch with offset:0 and follow next_offset. The target must remain held and have no admitted work or uncertain effects. Conflicting or out-of-scope files cannot transfer. import_worker_changes saves human approval for that exact artifact and receiver. After a successful transfer, explicitly resume_worker and guide the receiver to read the transferred files and run fresh tests. The receiver retains its original baseline: its final patch includes the prerequisite changes. Import that consolidated downstream patch into the parent once; separately importing the same prerequisite files will conflict. A transfer does not resume work, approve later edits, or verify combined tests.");
}

pub(super) fn prompt(prompt: &mut Value, config: &PlannerConfig, inspections: u32, imports: u32) {
    if config.worker_changes {
        prompt["worker_changes"] = json!({"inspections_used":inspections,"inspections_remaining":worker_changes::MAX_INSPECTIONS-inspections,"imports_used":imports,"imports_remaining":worker_changes::MAX_IMPORTS-imports});
    }
}

pub(super) fn handles(state: &PlannerState, event: &WorkflowEvent) -> bool {
    match event {
        WorkflowEvent::ActivityCompleted { handler, .. } => worker_changes::is_handler(handler),
        WorkflowEvent::InputReceived { key, .. } => {
            key == child_changes::APPROVAL_KEY
                && matches!(state.pending, Some(AgentAction::ImportWorkerChanges { .. }))
        }
        _ => false,
    }
}
