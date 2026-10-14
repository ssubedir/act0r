//! Pure context selection for native policies. Full outcomes remain in Run history.
use super::{ActivityId, DomainError, agent::*, tools::ToolDefinition};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationKind {
    Tool,
    Clarification,
    Delegation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeObservation {
    pub kind: ObservationKind,
    pub value: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestStatus {
    pub activity_id: ActivityId,
    /// None means the installed tool supplied no recognizable success flag.
    pub passed: Option<bool>,
    pub exit_code: Option<i64>,
    pub stale: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NativeContextState {
    pub total_observations: u64,
    pub observations: Vec<NativeObservation>,
    pub selection: Option<ContextSelection>,
    pub test_status: Option<TestStatus>,
}
impl NativeContextState {
    pub fn record(&mut self, kind: ObservationKind, value: Value) -> Result<(), DomainError> {
        self.total_observations = self
            .total_observations
            .checked_add(1)
            .ok_or(DomainError::InvalidState)?;
        self.observations.push(NativeObservation { kind, value });
        Ok(())
    }
    pub fn record_tool(
        &mut self,
        definition: &ToolDefinition,
        activity_id: ActivityId,
        action: &AgentAction,
        result: &Value,
    ) -> Result<(), DomainError> {
        if definition.requires_test_command {
            self.test_status = Some(TestStatus {
                activity_id,
                passed: result["ok"].as_bool(),
                exit_code: result["exit_code"].as_i64(),
                stale: false,
            });
        } else if (definition.requires_project_command
            || definition.requires_writable_paths && result["ok"] == true)
            && let Some(status) = &mut self.test_status
        {
            status.stale = true;
        }
        self.record(
            ObservationKind::Tool,
            json!({"action":action,"result":result}),
        )
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.total_observations < self.observations.len() as u64 {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
    /// Render all fixed material plus a suffix; the newest complete observation is essential.
    pub(crate) fn select(
        &mut self,
        policy: &ContextPolicy,
        max_input_bytes: u32,
        mut request: ModelRequest,
        mut render: impl FnMut(&[NativeObservation]) -> Value,
    ) -> Result<ModelRequest, DomainError> {
        policy.validate()?;
        self.validate()?;
        let mut effective = policy.clone();
        effective.max_input_bytes = effective.max_input_bytes.min(max_input_bytes);
        let mut start = self
            .observations
            .len()
            .saturating_sub(effective.max_observations as usize);
        loop {
            let count = self.observations.len() - start;
            let first = (count > 0).then(|| self.total_observations - count as u64 + 1);
            let omitted = self.total_observations - count as u64;
            let mut prompt = render(&self.observations[start..]);
            prompt["context"] = json!({"policy_version":effective.version,"total_observations":self.total_observations,"first_included":first,"omitted_observations":omitted});
            prompt["test_status"] = json!(self.test_status);
            request.prompt = prompt.to_string();
            let input_bytes = request.context_input_bytes();
            if input_bytes <= u64::from(effective.max_input_bytes) {
                let selection = ContextSelection {
                    policy: effective,
                    input_bytes,
                    total_observations: self.total_observations,
                    included_observations: count as u32,
                    first_included: first,
                    omitted_observations: omitted,
                };
                self.observations.drain(..start);
                self.selection = Some(selection.clone());
                request.context = Some(selection);
                return Ok(request);
            }
            if count <= 1 {
                return Err(DomainError::ContextLimit {
                    required_bytes: input_bytes,
                    limit_bytes: effective.max_input_bytes,
                });
            }
            start += 1;
        }
    }
}
pub(crate) fn validate_context(
    policy: Option<&ContextPolicy>,
    state: Option<&NativeContextState>,
) -> Result<(), DomainError> {
    if policy.is_some() != state.is_some() {
        return Err(DomainError::InvalidState);
    }
    if let Some(state) = state {
        state.validate()?;
    }
    Ok(())
}
pub(crate) fn window_instructions(verified_completion: bool) -> &'static str {
    if verified_completion {
        return " Observations and clarifications are a recent window; earlier results may be omitted. Use current tools when more evidence is needed. test_status summarizes the latest test activity; the completion checkpoint checks its results and exact workspace hashes. Saved completion refusal feedback describes an immutable candidate and may precede later edits.";
    }
    " Observations and clarifications are a recent window; earlier results may be omitted. Use current tools when more evidence is needed. test_status preserves the latest completed test activity and exit status; stale means a successful write occurred afterward. A missing success flag is unknown. This status is evidence, not a requirement to finish."
}
