use super::{AgentState, ModelRequest};
use crate::domain::DomainError;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Version 1 keeps a contiguous suffix of complete observations, including the newest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContextPolicy {
    pub version: u32,
    pub max_input_bytes: u32,
    pub max_observations: u32,
}
impl Default for ContextPolicy {
    fn default() -> Self {
        Self {
            version: 1,
            max_input_bytes: 65_536,
            max_observations: 16,
        }
    }
}
impl ContextPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.version != 1 || self.max_input_bytes == 0 || self.max_observations == 0 {
            return Err(DomainError::InvalidConfiguration(
                "context requires policy version 1 and positive input-byte and observation limits"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSelection {
    pub policy: ContextPolicy,
    pub input_bytes: u64,
    pub total_observations: u64,
    pub included_observations: u32,
    pub first_included: Option<u64>,
    pub omitted_observations: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ContextState {
    pub total_observations: u64,
    pub selection: Option<ContextSelection>,
}

pub(crate) fn select(
    state: &mut AgentState,
    mut request: ModelRequest,
) -> Result<ModelRequest, DomainError> {
    let policy = &state.config.context;
    policy.validate()?;
    let total = state.context.total_observations;
    if total < state.observations.len() as u64 {
        return Err(DomainError::InvalidState);
    }
    let mut start = state
        .observations
        .len()
        .saturating_sub(policy.max_observations as usize);
    loop {
        let count = state.observations.len() - start;
        let first = (count > 0).then(|| total - count as u64 + 1);
        let omitted = total - count as u64;
        request.prompt = crate::domain::session_summary::native_prompt(json!({
            "goal": state.config.goal,
            "turn": state.turn,
            "writable_paths": state.config.writable_paths,
            "test_command": state.config.test_command,
            "tests_passed": state.tests_passed,
            "pending_action": state.pending,
            "context": {"policy_version": policy.version, "total_observations": total, "first_included": first, "omitted_observations": omitted},
            "observations": &state.observations[start..],
        }), state.config.actor_memory.as_ref(), state.config.session_summary.as_ref()).to_string();
        let input_bytes = request.context_input_bytes();
        if input_bytes <= u64::from(policy.max_input_bytes) {
            let selection = ContextSelection {
                policy: policy.clone(),
                input_bytes,
                total_observations: total,
                included_observations: count as u32,
                first_included: first,
                omitted_observations: omitted,
            };
            state.observations.drain(..start);
            state.context.selection = Some(selection.clone());
            request.context = Some(selection);
            return Ok(request);
        }
        // Fixed instructions/goal and the newest complete result are essential context.
        if count <= 1 {
            return Err(DomainError::ContextLimit {
                required_bytes: input_bytes,
                limit_bytes: policy.max_input_bytes,
            });
        }
        start += 1;
    }
}

pub(crate) fn bounded_instructions() -> String {
    format!(
        "{} Observations are a recent window; earlier results may be omitted. Re-read files when needed. The tests_passed field records whether the fixed tests passed after the final edit.",
        super::instructions()
    )
}
