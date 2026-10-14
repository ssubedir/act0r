use super::{
    ActivityId, ActivitySpec, DefinitionId, DomainError, TimerId, TimerSpec, UsageBudget, WaitId,
};
use serde_json::Value;

pub enum WorkflowEvent {
    Started(Value),
    /// Internal continuation produced by the authoritative completion checkpoint.
    CompletionRejected {
        feedback: Value,
    },
    ActivityCompleted {
        activity_id: ActivityId,
        handler: DefinitionId,
        result: Value,
    },
    /// Leave a closed/expired coordination step, retaining any admitted read result.
    PlannerQuestionClosed {
        question_id: ActivityId,
        inspection: Option<(ActivityId, Value)>,
        model_completed: bool,
    },
    TimerFired {
        timer_id: TimerId,
        scheduled_for: i64,
        payload: Value,
    },
    InputReceived {
        wait_id: WaitId,
        key: String,
        payload: Value,
    },
    TaskCompleted {
        task_id: super::MessageId,
        outcome: Value,
    },
    TaskGroupCompleted {
        group_id: super::TaskGroupId,
        outcomes: Vec<Value>,
    },
}
pub struct Transition {
    pub state: Value,
    pub commands: Vec<Command>,
}
pub enum Command {
    /// Audit the checklist checkpoint in the same commit as its continuation.
    RecordTaskPlan(super::task_plan::TaskPlan),
    Schedule(ActivitySpec),
    StartTimer(TimerSpec),
    Complete(Value),
    /// Deliberate terminal failure at a quiescent workflow boundary.
    Fail(super::Failure),
    ConfigureFollowUps(super::follow_up::FollowUpContext),
    CompleteWithFollowUp {
        result: Value,
        goal: String,
        milliseconds: u64,
    },
    ConfigureUsage(UsageBudget),
    ConfigureGoalBudget(UsageBudget),
    ConfigureDelegation(super::delegation::DelegationBudget),
    DelegateTask {
        recipient: super::AgentId,
        goal: String,
    },
    DelegatePreparedTask(super::workspace_handoff::PreparedWorkspace),
    DelegatePreparedGroup(super::workspace_handoff::PreparedGroup),
    DelegateTasks {
        tasks: Vec<super::delegation::DelegatedWork>,
    },
    /// Yield a quiescent continuation to a planner without overriding user pause.
    PauseAtBoundary,
    WaitForWorkerReply {
        question_id: ActivityId,
        deadline_at: i64,
    },
    /// Return to the original child join after answering a child question.
    ResumeDelegationJoin,
    WaitForInput {
        key: String,
        prompt: String,
        payload: Value,
    },
}

/// A deterministic synchronous policy. No persistence or external execution here.
pub trait WorkflowDefinition: Send + Sync {
    fn id(&self) -> DefinitionId;
    /// Select deterministic input context once, before saving the Run.
    fn prepare_input(&self, input: Value) -> Result<Value, DomainError> {
        Ok(input)
    }
    /// Check pinned implementation metadata before admitting external work on recovery.
    fn validate_state(&self, _state: &Value) -> Result<(), DomainError> {
        Ok(())
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError>;
}
