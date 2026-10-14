//! Correlated sibling questions. Waits are Run checkpoints, never executing tool calls.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const WAIT_KEY: &str = "worker_reply";
pub const PLANNER_QUESTION_KEY: &str = "planner_worker_question";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlannerQuestion {
    pub question_id: ActivityId,
    pub child_task_id: MessageId,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub closed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AskWorker {
    pub recipient: String,
    pub text: String,
    pub timeout_ms: u32,
}
impl AskWorker {
    pub fn validate(&self) -> Result<(), tools::ToolError> {
        worker_messages::SendMessage {
            recipient: self.recipient.clone(),
            text: self.text.clone(),
            needs_coordination: false,
        }
        .validate()?;
        if !(1_000..=300_000).contains(&self.timeout_ms) {
            return Err(tools::ToolError::new(
                "question_timeout",
                "Choose a timeout of 1000..300000 milliseconds.",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyWorker {
    pub question_id: ActivityId,
    pub text: String,
}
impl ReplyWorker {
    pub fn validate(&self) -> Result<(), tools::ToolError> {
        worker_messages::SendMessage {
            recipient: "sibling".into(),
            text: self.text.clone(),
            needs_coordination: false,
        }
        .validate()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuestionOperation {
    Ask(AskWorker),
    Reply(ReplyWorker),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionStatus {
    Answered,
    TimedOut,
    RecipientClosed,
    RequesterClosed,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuestionOutcome {
    pub status: QuestionStatus,
    pub text: Option<String>,
    pub reply_id: Option<ActivityId>,
    pub resolved_at: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerQuestion {
    pub question_id: ActivityId,
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub requester_task_id: MessageId,
    pub requester_run_id: RunId,
    pub requester: String,
    pub recipient_task_id: MessageId,
    pub recipient_run_id: RunId,
    pub recipient: String,
    pub text: String,
    pub deadline_at: i64,
    pub saved_at: i64,
    pub outcome: Option<QuestionOutcome>,
}
impl WorkerQuestion {
    pub fn settle(&mut self, status: QuestionStatus, now: i64) {
        if self.outcome.is_none() {
            self.outcome = Some(QuestionOutcome {
                status,
                text: None,
                reply_id: None,
                resolved_at: now,
            });
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuestionReceipt {
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub operation: QuestionOperation,
    pub question_id: Option<ActivityId>,
    pub deadline_at: Option<i64>,
    pub error: Option<tools::ToolError>,
    pub saved_at: i64,
}
#[derive(Clone, Debug, Serialize)]
pub struct WorkerQuestions {
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub task_id: MessageId,
    pub items: Vec<WorkerQuestion>,
    pub receipts: Vec<QuestionReceipt>,
    pub truncated: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerReplyWait {
    pub question_id: ActivityId,
    pub timer_id: TimerId,
    pub deadline_at: i64,
}

/// Edges include accepted questions whose sending activity has not completed yet.
pub fn would_cycle(edges: &[(RunId, RunId)], requester: RunId, recipient: RunId) -> bool {
    let mut frontier = vec![recipient];
    let mut visited = std::collections::HashSet::new();
    while let Some(run) = frontier.pop() {
        if run == requester {
            return true;
        }
        if visited.insert(run) {
            frontier.extend(
                edges
                    .iter()
                    .filter(|(from, _)| *from == run)
                    .map(|(_, to)| *to),
            );
        }
    }
    false
}
impl Run {
    pub(crate) fn prepare_planner_question_status(
        &mut self,
        question: &WorkerQuestion,
        closed: bool,
    ) -> Result<(), DomainError> {
        let current = self
            .record
            .planner_question
            .as_mut()
            .ok_or(DomainError::InvalidState)?;
        if current.question_id != question.question_id
            || current.child_task_id != question.requester_task_id
            || question.recipient_run_id != self.record.id
        {
            return Err(DomainError::InvalidState);
        }
        current.closed = closed || question.outcome.is_some();
        Ok(())
    }
    pub(crate) fn accepts_planner_questions(&self) -> bool {
        self.steering_native_state()
            .is_ok_and(|(state, definition)| {
                definition == planner::PlannerWorkflow::definition_id()
                    && planner::accepts_worker_questions(state)
            })
    }
    pub(crate) fn queue_planner_question(
        &mut self,
        question: &WorkerQuestion,
        now: i64,
    ) -> Result<(), DomainError> {
        if !self.accepts_planner_questions()
            || question.recipient_run_id != self.record.id
            || question.recipient != "parent"
            || self
                .pending_delegation_by_id(question.requester_task_id)
                .is_none()
        {
            return Err(DomainError::InvalidState);
        }
        let (state, _) = self.steering_native_state()?;
        planner::check_question_context(
            state,
            question,
            steering::instructions(&self.record.steering),
            worker_messages::instructions(&self.record.peer_messages),
        )?;
        self.timers.push(DurableTimer {
            id: TimerId::new(),
            run_id: self.record.id,
            scheduling_revision: self.record.revision,
            command_ordinal: self.timers.len() as u32,
            due_at: now,
            status: TimerStatus::Scheduled,
            fired_at: None,
            payload: json!({"key":PLANNER_QUESTION_KEY,"question":question}),
        });
        self.event(
            "planner_question_queued",
            json!({"question_id":question.question_id,"task_id":question.requester_task_id}),
            None,
            None,
            now,
        );
        Ok(())
    }
    pub(crate) fn prepare_planner_question_timer(&mut self, question: &WorkerQuestion) {
        for timer in &mut self.timers {
            if timer.payload["key"] == PLANNER_QUESTION_KEY
                && timer.payload["question"]["question_id"] == json!(question.question_id)
            {
                timer.payload["question"] = json!(question);
            }
        }
    }
    pub(crate) fn prepare_worker_reply_timer(
        &mut self,
        question: &WorkerQuestion,
    ) -> Result<(), DomainError> {
        let wait = self
            .record
            .worker_reply_wait
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        if wait.question_id != question.question_id
            || self.record.id != question.requester_run_id
            || wait.deadline_at != question.deadline_at
        {
            return Err(DomainError::InvalidState);
        }
        if let Some(outcome) = &question.outcome {
            let timer = self
                .timers
                .iter_mut()
                .find(|t| t.id == wait.timer_id)
                .ok_or(DomainError::UnknownTimer)?;
            timer.payload =
                json!({"key":WAIT_KEY,"question_id":question.question_id,"outcome":outcome});
        }
        Ok(())
    }
}
pub(crate) fn wait_command(result: &Value) -> Result<Command, DomainError> {
    let question_id = serde_json::from_value(result["question_id"].clone())
        .map_err(|_| DomainError::InvalidState)?;
    let deadline_at = result["deadline_at"]
        .as_i64()
        .ok_or(DomainError::InvalidState)?;
    Ok(Command::WaitForWorkerReply {
        question_id,
        deadline_at,
    })
}
