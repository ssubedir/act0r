use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTaskStatus {
    Queued,
    Running,
    Waiting,
    NeedsRecovery,
    Paused,
    Blocked,
    Cancelling,
    Succeeded,
    Failed,
    Rejected,
    BudgetExhausted,
    Cancelled,
}
impl AgentTaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Failed
                | Self::Rejected
                | Self::BudgetExhausted
                | Self::Cancelled
        )
    }
    pub fn from_run(run: &RunRecord) -> Self {
        if run
            .worker_recovery
            .as_ref()
            .is_some_and(|r| r.decision.is_none())
        {
            return Self::NeedsRecovery;
        }
        if run.status.is_unfinished()
            && (run.controls.cancel_requested
                || run
                    .delegations
                    .iter()
                    .any(|d| d.outcome.is_none() && d.timed_out_at.is_some()))
        {
            return Self::Cancelling;
        }
        match run.status {
            RunStatus::Queued | RunStatus::Running => Self::Running,
            RunStatus::Waiting => Self::Waiting,
            RunStatus::Paused => Self::Paused,
            RunStatus::Blocked => Self::Blocked,
            RunStatus::Succeeded => Self::Succeeded,
            RunStatus::Failed => Self::Failed,
            RunStatus::BudgetExhausted => Self::BudgetExhausted,
            RunStatus::Cancelled => Self::Cancelled,
        }
    }
}

/// A request's identity is its original mailbox message ID, separate from CodingTask.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentTaskRecord {
    pub id: MessageId,
    pub agent_id: AgentId,
    pub correlation_id: MessageId,
    pub parent_id: Option<MessageId>,
    pub run_id: Option<RunId>,
    pub status: AgentTaskStatus,
    pub cancel_requested: bool,
    pub reply: Option<Value>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follow_up_origin: Option<crate::domain::follow_up::FollowUpOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_input: Option<crate::domain::workspace_handoff::PreparedWorkspace>,
}
#[derive(Clone, Debug, Serialize)]
pub struct AgentTask {
    pub(crate) record: AgentTaskRecord,
}
impl AgentTask {
    pub(crate) fn resume_failed_turn(&mut self, run: RunId, now: i64) -> Result<(), DomainError> {
        if self.record.status != AgentTaskStatus::Failed
            || self.record.cancel_requested
            || self.record.parent_id.is_some()
            || self.record.run_id != Some(run)
        {
            return Err(DomainError::InvalidState);
        }
        self.record.status = AgentTaskStatus::Running;
        self.record.reply = None;
        self.record.updated_at = now;
        Ok(())
    }
    pub fn new(
        message: &AgentMessage,
        parent_id: Option<MessageId>,
        now: i64,
    ) -> Result<Self, DomainError> {
        if message.kind != "task" || parent_id == Some(message.id) {
            return Err(DomainError::InvalidConfiguration(
                "task handle requires a task envelope and distinct parent".into(),
            ));
        }
        Ok(Self {
            record: AgentTaskRecord {
                id: message.id,
                agent_id: message.recipient,
                correlation_id: message.correlation(),
                parent_id,
                run_id: None,
                status: AgentTaskStatus::Queued,
                cancel_requested: false,
                reply: None,
                created_at: now,
                updated_at: now,
                follow_up_origin: None,
                workspace_input: None,
            },
        })
    }
    pub fn record(&self) -> &AgentTaskRecord {
        &self.record
    }
    pub(crate) fn attach_run(&mut self, id: RunId, now: i64) -> Result<(), DomainError> {
        if self.record.cancel_requested
            || self.record.status.is_terminal()
            || self.record.run_id.is_some()
        {
            return Err(DomainError::InvalidState);
        }
        self.record.run_id = Some(id);
        self.record.status = AgentTaskStatus::Running;
        self.record.updated_at = now;
        Ok(())
    }
    pub(crate) fn request_cancel(&mut self, now: i64) {
        if !self.record.status.is_terminal() && !self.record.cancel_requested {
            self.record.cancel_requested = true;
            self.record.status = AgentTaskStatus::Cancelling;
            self.record.updated_at = now;
        }
    }
    pub(crate) fn finish(&mut self, reply: Value, now: i64) -> Result<(), DomainError> {
        if self.record.status.is_terminal() {
            return Ok(());
        }
        let status: AgentTaskStatus =
            serde_json::from_value(reply["status"].clone()).map_err(|_| {
                DomainError::InvalidConfiguration("task reply needs a terminal status".into())
            })?;
        if !status.is_terminal() {
            return Err(DomainError::InvalidState);
        }
        self.record.status = status;
        self.record.reply = Some(reply);
        self.record.updated_at = now;
        Ok(())
    }
}

/// Execution lifecycle and charges are read from the authoritative saved Run.
#[derive(Clone, Debug, Serialize)]
pub struct AgentTaskView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub follow_up: Option<crate::domain::follow_up::FollowUpIntent>,
    pub task: AgentTaskRecord,
    pub status: AgentTaskStatus,
    pub run_id: Option<RunId>,
    pub execution_agent_id: Option<AgentId>,
    pub reply: Option<Value>,
    pub wait: Option<HumanWait>,
    pub admitted_attempts: u32,
    pub usage: Option<UsageLedger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegation: Option<crate::domain::delegation::TaskDelegation>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub shared_budgets: Vec<crate::domain::shared_budget::SharedBudgetView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_groups: Vec<crate::domain::delegation::TaskGroup>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_children: Vec<crate::domain::delegation::TaskDelegation>,
}
pub fn task_run_reply(request_id: MessageId, run: &RunRecord) -> Value {
    let mut reply = json!({"request_id":request_id,"run_id":run.id,"status":run.status,"result":run.result,"error":run.error});
    if let Some(intent) = &run.follow_up {
        reply["follow_up"] = json!(intent);
    }
    reply
}
