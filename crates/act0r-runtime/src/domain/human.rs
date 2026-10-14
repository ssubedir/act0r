use super::WaitId;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const MAX_HUMAN_ANSWER_BYTES: usize = 65_536;

/// The native inbox deliberately exposes only supported human decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanWaitKind {
    Question,
    Guidance,
    ToolApproval,
    ChildImportApproval,
    Unsupported,
}
impl HumanWaitKind {
    pub fn for_key(key: &str) -> Self {
        match key {
            "agent_answer" | "answer_clarification" | "planner_answer" => Self::Question,
            super::clarification::WAIT_KEY => Self::Guidance,
            "native_tool_approval" => Self::ToolApproval,
            super::child_changes::APPROVAL_KEY => Self::ChildImportApproval,
            _ => Self::Unsupported,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HumanResponse {
    Answer { answer: String },
    Approval { approved: bool },
}
impl HumanResponse {
    pub fn validate(&self) -> Result<(), super::DomainError> {
        if let Self::Answer { answer } = self
            && (answer.trim().is_empty() || answer.len() > MAX_HUMAN_ANSWER_BYTES)
        {
            return Err(super::DomainError::InvalidConfiguration(
                "human answer must contain 1..65536 UTF-8 bytes of nonblank text".into(),
            ));
        }
        Ok(())
    }
    pub fn payload(&self) -> Value {
        match self {
            Self::Answer { answer } => json!({"answer":answer}),
            Self::Approval { approved } => json!({"approved":approved}),
        }
    }
    pub fn validate_wait(&self, wait: &HumanWait) -> Result<(), super::DomainError> {
        self.validate()?;
        match (self, HumanWaitKind::for_key(&wait.key)) {
            (Self::Answer { .. }, HumanWaitKind::Question | HumanWaitKind::Guidance)
            | (
                Self::Approval { .. },
                HumanWaitKind::ToolApproval | HumanWaitKind::ChildImportApproval,
            ) => Ok(()),
            _ => Err(super::DomainError::InvalidConfiguration(
                "response does not match a supported question or approval wait".into(),
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HumanWait {
    pub id: WaitId,
    pub key: String,
    pub prompt: String,
    pub payload: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InputReceipt {
    pub wait_id: WaitId,
    pub payload: Value,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ControlFlags {
    pub pause_requested: bool,
    pub cancel_requested: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub boundary_pause: bool,
}
