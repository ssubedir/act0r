use super::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
fn guidance_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Blocked,
    BudgetExhausted,
    Waiting,
    Paused,
    Cancelled,
}
impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::BudgetExhausted => "budget_exhausted",
            Self::Waiting => "waiting",
            Self::Paused => "paused",
            Self::Cancelled => "cancelled",
        }
    }
    pub fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }
    pub fn is_unfinished(self) -> bool {
        self.is_active() || matches!(self, Self::Waiting | Self::Paused)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttemptAllowance {
    pub limit: u32,
    pub admitted: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: RunId,
    pub workflow: DefinitionId,
    pub input: Value,
    pub state: Value,
    pub revision: u64,
    pub status: RunStatus,
    pub allowance: AttemptAllowance,
    pub created_at: i64,
    pub updated_at: i64,
    pub result: Option<Value>,
    pub error: Option<Failure>,
    pub history_sequence: u64,
    #[serde(default)]
    pub usage: UsageLedger,
    #[serde(default)]
    pub controls: ControlFlags,
    #[serde(default)]
    pub wait: Option<HumanWait>,
    #[serde(default)]
    pub input_receipts: Vec<InputReceipt>,
    #[serde(default)]
    pub reconciliation_receipts: Vec<ReconciliationReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_budget: Option<delegation::DelegationBudget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_budget: Option<UsageBudget>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delegations: Vec<delegation::TaskDelegation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_groups: Vec<delegation::TaskGroup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follow_up_context: Option<follow_up::FollowUpContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follow_up: Option<follow_up::FollowUpIntent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steering: Vec<steering::SteeringEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peer_messages: Vec<worker_messages::PeerMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_reply_wait: Option<worker_questions::WorkerReplyWait>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner_question: Option<worker_questions::PlannerQuestion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner_guidance: Option<planner_guidance::PlannerGuidance>,
    #[serde(default, skip_serializing_if = "guidance_zero")]
    pub guidance_handled_through: u64,
    #[serde(default, skip_serializing_if = "guidance_zero")]
    pub findings_handled_through: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_recovery: Option<worker_recovery::WorkerRecoveryCheckpoint>,
}

/// Aggregate mutations go through domain methods; consumers receive immutable records.
#[derive(Clone, Debug, Serialize)]
pub struct Run {
    pub(crate) record: RunRecord,
    pub(crate) activities: Vec<Activity>,
    pub(crate) attempts: Vec<Attempt>,
    pub(crate) timers: Vec<DurableTimer>,
    #[serde(skip)]
    pub(crate) pending_events: Vec<HistoryEvent>,
}
impl Run {
    pub fn record(&self) -> &RunRecord {
        &self.record
    }
    pub fn activities(&self) -> &[Activity] {
        &self.activities
    }
    pub fn attempts(&self) -> &[Attempt] {
        &self.attempts
    }
    pub fn timers(&self) -> &[DurableTimer] {
        &self.timers
    }

    pub fn start(
        workflow: &dyn WorkflowDefinition,
        input: Value,
        limit: u32,
        now: i64,
    ) -> Result<Self, DomainError> {
        Self::start_with_id(RunId::new(), workflow, input, limit, now)
    }

    pub fn start_with_id(
        id: RunId,
        workflow: &dyn WorkflowDefinition,
        input: Value,
        limit: u32,
        now: i64,
    ) -> Result<Self, DomainError> {
        if limit == 0 {
            return Err(DomainError::InvalidConfiguration(
                "attempt limit must be positive".into(),
            ));
        }
        if workflow.id().name.is_empty() || workflow.id().version == 0 {
            return Err(DomainError::InvalidConfiguration(
                "workflow name and version are required".into(),
            ));
        }
        let input = workflow.prepare_input(input)?;
        let transition =
            workflow.transition(&Value::Null, &WorkflowEvent::Started(input.clone()))?;
        let mut run = Self {
            record: RunRecord {
                id,
                workflow: workflow.id(),
                input,
                state: Value::Null,
                revision: 0,
                status: RunStatus::Queued,
                allowance: AttemptAllowance { limit, admitted: 0 },
                created_at: now,
                updated_at: now,
                result: None,
                error: None,
                history_sequence: 0,
                steering: vec![],
                peer_messages: vec![],
                worker_reply_wait: None,
                planner_question: None,
                planner_guidance: None,
                guidance_handled_through: 0,
                findings_handled_through: 0,
                worker_recovery: None,
                usage: UsageLedger::default(),
                controls: ControlFlags::default(),
                wait: None,
                input_receipts: vec![],
                reconciliation_receipts: vec![],
                delegation_budget: None,
                goal_budget: None,
                delegations: vec![],
                task_groups: vec![],
                follow_up_context: None,
                follow_up: None,
            },
            activities: vec![],
            attempts: vec![],
            timers: vec![],
            pending_events: vec![],
        };
        run.event(
            "run_started",
            json!({"workflow": run.record.workflow}),
            None,
            None,
            now,
        );
        run.apply(transition, now)?;
        Ok(run)
    }

    pub fn next_due(&self, now: i64) -> Option<&Activity> {
        if !self.record.status.is_active()
            || self
                .activities
                .iter()
                .any(|a| a.status == ActivityStatus::Running)
        {
            return None;
        }
        self.activities
            .iter()
            .filter(|a| a.status == ActivityStatus::Queued && a.ready_at <= now)
            .min_by_key(|a| (a.ready_at, a.scheduling_revision, a.command_ordinal))
    }

    pub(crate) fn start_forked(
        id: RunId,
        input: Value,
        limit: u32,
        origin: &branch::BranchOrigin,
        now: i64,
    ) -> Result<Self, DomainError> {
        Self::start_forked_with(
            id,
            &task::BranchCandidateWorkflow,
            input,
            limit,
            origin,
            now,
        )
    }
    pub(crate) fn start_forked_with(
        id: RunId,
        workflow: &dyn WorkflowDefinition,
        input: Value,
        limit: u32,
        origin: &branch::BranchOrigin,
        now: i64,
    ) -> Result<Self, DomainError> {
        let mut run = Self::start_with_id(id, workflow, input, limit, now)?;
        if origin.attempts > limit
            || serde_json::to_value(&run.record.usage.budget).ok()
                != serde_json::to_value(&origin.usage.budget).ok()
        {
            return Err(DomainError::InvalidState);
        }
        run.record.allowance.admitted = origin.attempts;
        run.record.usage = origin.usage.clone();
        run.event("run_forked", json!({"origin": origin}), None, None, now);
        Ok(run)
    }

    pub(crate) fn begin_search_feedback(&mut self, now: i64) -> Result<(), DomainError> {
        if self.record.workflow != search::FeedbackSearchCandidateWorkflow.id()
            || self.record.status != RunStatus::Paused
            || self.record.controls.pause_requested
            || self.record.controls.cancel_requested
            || self.record.wait.is_some()
        {
            return Err(DomainError::InvalidState);
        }
        let mut state: search::SearchCandidateState =
            serde_json::from_value(self.record.state.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        let queued: Vec<_> = self
            .activities
            .iter()
            .filter(|a| a.status == ActivityStatus::Queued)
            .collect();
        if queued.len() != 1
            || queued[0].attempts != 0
            || queued[0].spec.handler != DefinitionId::new("agent.model", 1)
            || self.activities.iter().any(|a| {
                a.status == ActivityStatus::Running || a.status == ActivityStatus::Indeterminate
            })
            || self
                .timers
                .iter()
                .any(|t| t.status == TimerStatus::Scheduled)
        {
            return Err(DomainError::InvalidState);
        }
        let command = state.begin_feedback()?;
        for activity in &mut self.activities {
            if activity.status == ActivityStatus::Queued {
                activity.status = ActivityStatus::Failed;
                activity.error = Some(Failure::new(
                    "search_superseded",
                    "saved model request superseded by feedback evaluation",
                    false,
                ));
            }
        }
        self.record.controls.boundary_pause = false;
        self.record.status = RunStatus::Queued;
        self.event(
            "search_feedback_requested",
            json!({"search_id":state.input.search.as_ref().map(|s|s.search_id)}),
            None,
            None,
            now,
        );
        self.apply(
            Transition {
                state: serde_json::to_value(state).unwrap(),
                commands: vec![command],
            },
            now,
        )
    }
    pub(crate) fn select_search_for_verification(&mut self, now: i64) -> Result<(), DomainError> {
        let mut state: search::SearchCandidateState =
            serde_json::from_value(self.record.state.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        let agent: agent::AgentState = serde_json::from_value(state.agent.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        if !matches!(self.record.workflow, ref w if *w == search::SearchCandidateWorkflow.id() || *w == search::FeedbackSearchCandidateWorkflow.id())
            || self.record.status != RunStatus::Paused
            || self.record.controls.pause_requested
            || !agent.tests_passed
            || agent.pending.is_some()
            || state.verifying
        {
            return Err(DomainError::InvalidState);
        }
        let queued: Vec<_> = self
            .activities
            .iter()
            .filter(|a| a.status == ActivityStatus::Queued)
            .collect();
        let feedback_complete = self.record.workflow
            == search::FeedbackSearchCandidateWorkflow.id()
            && state.feedback_checked
            && state
                .feedback
                .as_ref()
                .is_some_and(|f| f.phase == search::FeedbackPhase::Completed);
        if !(feedback_complete && queued.is_empty())
            && (queued.len() != 1
                || queued[0].attempts != 0
                || queued[0].spec.handler != DefinitionId::new("agent.model", 1))
        {
            return Err(DomainError::InvalidState);
        }
        for activity in &mut self.activities {
            if activity.status == ActivityStatus::Queued {
                activity.status = ActivityStatus::Failed;
                activity.error = Some(Failure::new(
                    "search_superseded",
                    "saved model request superseded by final verification",
                    false,
                ));
            }
        }
        state.verifying = true;
        state.finish = Some(
            json!({"summary":"Selected bounded-search continuation", "workspace":state.input.config.workspace, "tests_passed":true, "model_turns":agent.turn}),
        );
        let candidate = task::CandidateState {
            agent: state.agent.clone(),
            input: state.input.clone(),
            finish: state.finish.clone(),
        };
        let command = if state.input.config.require_approval {
            Command::WaitForInput {
                key: "candidate_verification".into(),
                prompt: "Approve final fixed tests in a clean verification workspace?".into(),
                payload: json!({"workspace":state.input.config.workspace, "snapshot_digest":state.input.snapshot.digest, "test_command":state.input.final_tests.as_ref().map_or(&state.input.config.test_command,|t|&t.test_command)}),
            }
        } else {
            candidate.evaluate()?
        };
        self.record.controls.boundary_pause = false;
        self.record.status = RunStatus::Queued;
        self.event(
            "search_selected",
            json!({"search_id":state.input.search.as_ref().map(|s|s.search_id)}),
            None,
            None,
            now,
        );
        self.apply(
            Transition {
                state: serde_json::to_value(state).unwrap(),
                commands: vec![command],
            },
            now,
        )
    }

    pub fn admit(&mut self, now: i64) -> Result<Option<ActivityExecution>, DomainError> {
        self.admit_with_steering(now)
    }
    pub(super) fn admit_prepared_request(
        &mut self,
        now: i64,
    ) -> Result<Option<ActivityExecution>, DomainError> {
        let Some(activity) = self.next_due(now) else {
            return Ok(None);
        };
        let id = activity.id;
        if self.record.allowance.admitted >= self.record.allowance.limit {
            self.terminate(
                RunStatus::BudgetExhausted,
                Failure::new("attempt_budget", "total attempt allowance exhausted", false),
                now,
            );
            return Ok(None);
        }
        let index = self
            .activities
            .iter()
            .position(|a| a.id == id)
            .ok_or(DomainError::UnknownAttempt)?;
        if self.activities[index].attempts >= self.activities[index].spec.retry.max_attempts {
            self.terminate(
                RunStatus::Failed,
                Failure::new(
                    "retry_exhausted",
                    "activity attempt allowance exhausted",
                    false,
                ),
                now,
            );
            return Ok(None);
        }
        if let Some(reservation) = &self.activities[index].spec.usage {
            if !self.record.usage.can_reserve(reservation) {
                self.terminate(
                    RunStatus::BudgetExhausted,
                    Failure::new(
                        "model_budget",
                        "model-call or token allowance cannot admit this request",
                        false,
                    ),
                    now,
                );
                return Ok(None);
            }
            self.record.usage.reserve(reservation);
            self.event(
                "usage_reserved",
                json!({"reservation": self.activities[index].spec.usage}),
                Some(id),
                None,
                now,
            );
        }
        let attempt_id = AttemptId::new();
        let activity = &mut self.activities[index];
        activity.attempts += 1;
        activity.status = ActivityStatus::Running;
        activity.active_attempt = Some(attempt_id);
        let execution = ActivityExecution {
            run_id: self.record.id,
            activity_id: id,
            attempt_id,
            ordinal: activity.attempts,
            handler: activity.spec.handler.clone(),
            input: activity.spec.input.clone(),
        };
        self.attempts.push(Attempt {
            id: attempt_id,
            activity_id: id,
            ordinal: execution.ordinal,
            status: AttemptStatus::Running,
            started_at: now,
            ended_at: None,
            outcome: None,
        });
        self.record.allowance.admitted += 1;
        self.record.status = RunStatus::Running;
        self.event(
            "attempt_admitted",
            json!({"ordinal": execution.ordinal, "handler": execution.handler}),
            Some(id),
            Some(attempt_id),
            now,
        );
        Ok(Some(execution))
    }

    pub(crate) fn exhaust_task_budget(&mut self, now: i64) {
        self.terminate(
            RunStatus::BudgetExhausted,
            Failure::new(
                "task_budget",
                "shared task allowance cannot admit this attempt",
                false,
            ),
            now,
        );
    }

    pub(crate) fn exhaust_shared_budget(
        &mut self,
        root: RunId,
        kind: shared_budget::SharedBudgetKind,
        now: i64,
    ) {
        self.event(
            "shared_budget_exhausted",
            json!({"root_run_id": root, "kind":kind}),
            None,
            None,
            now,
        );
        let code = match kind {
            shared_budget::SharedBudgetKind::Delegation => "shared_model_budget",
            shared_budget::SharedBudgetKind::Goal => "goal_model_budget",
        };
        self.terminate(
            RunStatus::BudgetExhausted,
            Failure::new(
                code,
                format!(
                    "{} model-call or token allowance rooted at {root} cannot admit this request",
                    kind.as_str()
                ),
                false,
            ),
            now,
        );
    }

    pub fn complete(
        &mut self,
        execution: &ActivityExecution,
        outcome: Outcome,
        workflow: &dyn WorkflowDefinition,
        now: i64,
    ) -> Result<(), DomainError> {
        let coordinating = self.record.planner_question.clone();
        let guidance_join = self
            .record
            .planner_guidance
            .as_ref()
            .and_then(|g| g.targets.first())
            .map(|t| t.task_id);
        if execution.run_id != self.record.id {
            return Err(DomainError::UnknownAttempt);
        }
        let ai = self
            .activities
            .iter()
            .position(|a| a.id == execution.activity_id)
            .ok_or(DomainError::UnknownAttempt)?;
        let ti = self
            .attempts
            .iter()
            .position(|a| a.id == execution.attempt_id && a.activity_id == execution.activity_id)
            .ok_or(DomainError::UnknownAttempt)?;
        if let Some(saved) = &self.attempts[ti].outcome {
            if saved == &outcome {
                return Ok(());
            }
            return Err(
                if self.attempts[ti].ordinal < self.activities[ai].attempts {
                    DomainError::StaleAttempt
                } else {
                    DomainError::ConflictingOutcome
                },
            );
        }
        if self.activities[ai].active_attempt != Some(execution.attempt_id) {
            return Err(DomainError::StaleAttempt);
        }
        if !self.record.status.is_active() {
            return Err(DomainError::InvalidState);
        }
        if workflow.id() != self.record.workflow {
            return Err(DomainError::InvalidWorkflow(
                "definition version does not match run".into(),
            ));
        }
        let transition = outcome
            .result()
            .filter(|_| !self.record.controls.cancel_requested)
            .map(|result| self.completed_transition(ai, result, workflow, now));
        let attempt = &mut self.attempts[ti];
        attempt.status = match &outcome {
            Outcome::Success(_) | Outcome::SuccessWithUsage { .. } => AttemptStatus::Succeeded,
            Outcome::Failure(_) | Outcome::FailureWithUsage { .. } => AttemptStatus::Failed,
            Outcome::Uncertain(_) => AttemptStatus::Interrupted,
        };
        attempt.ended_at = Some(now);
        attempt.outcome = Some(outcome.clone());
        self.activities[ai].active_attempt = None;
        let exceeded = if let Some(reservation) = &self.activities[ai].spec.usage {
            let exceeded = self.record.usage.reconcile(reservation, outcome.usage());
            self.event(
                "usage_reconciled",
                json!({"usage": outcome.usage(), "ledger": self.record.usage}),
                Some(execution.activity_id),
                Some(execution.attempt_id),
                now,
            );
            exceeded
        } else {
            false
        };
        self.event(
            "attempt_finished",
            json!({"outcome": outcome}),
            Some(execution.activity_id),
            Some(execution.attempt_id),
            now,
        );
        match outcome {
            Outcome::Success(result) | Outcome::SuccessWithUsage { result, .. } => {
                self.activities[ai].status = ActivityStatus::Succeeded;
                self.activities[ai].result = Some(result);
                self.activities[ai].error = None;
                if self.record.controls.cancel_requested {
                    self.finish_cancel(now);
                    return Ok(());
                }
                if exceeded {
                    self.terminate(
                        RunStatus::BudgetExhausted,
                        Failure::new(
                            "usage_reservation_exceeded",
                            "provider usage exceeded its reservation or run allowance",
                            false,
                        ),
                        now,
                    );
                    return Ok(());
                }
                match transition.ok_or(DomainError::InvalidState)? {
                    Ok(transition) => {
                        self.apply(transition, now)?;
                        if let Some(task) = guidance_join
                            && self.record.planner_guidance.is_none()
                            && self.record.status.is_unfinished()
                        {
                            let index = self
                                .record
                                .delegations
                                .iter()
                                .position(|d| d.id == task)
                                .ok_or(DomainError::InvalidState)?;
                            self.deliver_delegation_join(index, Some(workflow), now)?;
                        }
                        if let Some(question) = &coordinating
                            && self.record.planner_question.is_none()
                            && self.record.planner_guidance.is_none()
                            && self.record.status.is_unfinished()
                        {
                            let index = self
                                .record
                                .delegations
                                .iter()
                                .position(|d| d.id == question.child_task_id)
                                .ok_or(DomainError::InvalidState)?;
                            self.deliver_delegation_join(index, Some(workflow), now)?;
                        }
                    }
                    Err(error) => {
                        self.terminate(RunStatus::Failed, Self::continuation_failure(error), now)
                    }
                }
            }
            Outcome::Failure(failure) | Outcome::FailureWithUsage { failure, .. } => {
                self.retry_or_finish(ai, failure, now)
            }
            Outcome::Uncertain(reason) => self.resolve_uncertain(ai, reason, now),
        }
        if self.record.controls.cancel_requested {
            self.finish_cancel(now);
        } else if exceeded {
            self.terminate(
                RunStatus::BudgetExhausted,
                Failure::new(
                    "usage_reservation_exceeded",
                    "provider usage exceeded its reservation or run allowance",
                    false,
                ),
                now,
            );
        }
        self.honour_pause();
        Ok(())
    }

    pub fn pause(&mut self, now: i64) -> Result<(), DomainError> {
        if !self.record.status.is_unfinished() || self.record.controls.cancel_requested {
            return Err(DomainError::InvalidState);
        }
        if !self.record.controls.pause_requested {
            self.record.controls.pause_requested = true;
            self.event("pause_requested", json!({}), None, None, now);
        }
        self.honour_pause();
        Ok(())
    }
    pub fn resume(&mut self, now: i64) -> Result<(), DomainError> {
        if !self.record.status.is_unfinished() || self.record.controls.cancel_requested {
            return Err(DomainError::InvalidState);
        }
        let feedback_boundary = self.record.workflow
            == search::FeedbackSearchCandidateWorkflow.id()
            && serde_json::from_value::<search::SearchCandidateState>(self.record.state.clone())
                .ok()
                .is_some_and(|s| !s.verifying && s.feedback.as_ref().is_some_and(|f| f.is_ready()));
        if feedback_boundary && !self.record.controls.pause_requested {
            return Ok(());
        }
        if self.record.controls.pause_requested || self.record.controls.boundary_pause {
            self.record.controls.pause_requested = false;
            self.record.controls.boundary_pause = feedback_boundary;
            if self.record.status == RunStatus::Paused {
                self.record.status = if self.record.wait.is_some()
                    || self.pending_delegation().is_some()
                        && self.record.planner_question.is_none()
                        && self.record.planner_guidance.is_none()
                    || self.record.worker_reply_wait.is_some()
                {
                    RunStatus::Waiting
                } else {
                    RunStatus::Queued
                };
            }
            self.event("run_resumed", json!({}), None, None, now);
            self.honour_pause();
        }
        Ok(())
    }
    pub fn cancel(&mut self, now: i64) -> Result<(), DomainError> {
        if self.record.status == RunStatus::Cancelled || self.record.controls.cancel_requested {
            return Ok(());
        }
        if self.needs_worker_recovery() {
            self.record.worker_recovery = None;
            self.record.status = RunStatus::Waiting;
            self.event("worker_recovery_cancelled", json!({}), None, None, now);
        }
        if !self.record.status.is_unfinished() {
            return Err(DomainError::InvalidState);
        }
        self.record.controls.cancel_requested = true;
        self.event("cancel_requested", json!({}), None, None, now);
        if !self
            .activities
            .iter()
            .any(|a| a.status == ActivityStatus::Running)
        {
            self.finish_cancel(now);
        }
        Ok(())
    }
    fn finish_cancel(&mut self, now: i64) {
        if self.pending_delegation().is_some()
            || self
                .activities
                .iter()
                .any(|a| a.status == ActivityStatus::Running)
        {
            return;
        }
        if self.record.status != RunStatus::Cancelled {
            self.record.wait = None;
            self.record.worker_reply_wait = None;
            self.record.planner_question = None;
            self.record.planner_guidance = None;
            self.terminate(RunStatus::Cancelled, Failure::new("cancelled", "cancelled at an activity boundary; inspect interrupted outcomes for uncertain effects", false), now);
        }
    }
    fn honour_pause(&mut self) {
        if (self.record.controls.pause_requested || self.record.controls.boundary_pause)
            && self.record.status.is_unfinished()
            && !self
                .activities
                .iter()
                .any(|a| a.status == ActivityStatus::Running)
        {
            self.record.status = RunStatus::Paused;
        }
    }
    pub fn pending_delegation(&self) -> Option<&delegation::TaskDelegation> {
        self.record.delegations.iter().find(|d| d.outcome.is_none())
    }
    pub fn pending_delegation_by_id(&self, id: MessageId) -> Option<&delegation::TaskDelegation> {
        self.record
            .delegations
            .iter()
            .find(|d| d.id == id && d.outcome.is_none())
    }
    pub fn delegation_clock_enabled(&self) -> bool {
        !self.record.controls.cancel_requested
            && (self.record.status.is_unfinished()
                || (self.record.status == RunStatus::Blocked
                    && self
                        .record
                        .error
                        .as_ref()
                        .is_some_and(|e| e.code == "implementation_unavailable")))
    }
    /// Persist expiry before cancellation; a saved receipt survives backward clock changes.
    pub fn expire_delegation(
        &mut self,
        id: MessageId,
        child_completed_at: Option<i64>,
        now: i64,
    ) -> Result<(), DomainError> {
        let intent = self
            .pending_delegation_by_id(id)
            .ok_or(DomainError::InvalidState)?;
        if intent.timed_out_at.is_some() {
            return Ok(());
        }
        if !self.delegation_clock_enabled() || !intent.deadline_due(now, child_completed_at) {
            return Err(DomainError::InvalidState);
        }
        let deadline = intent.deadline_at;
        self.record
            .delegations
            .iter_mut()
            .find(|d| d.id == id)
            .unwrap()
            .timed_out_at = Some(now);
        self.event(
            "delegation_timed_out",
            json!({"task_id":id,"deadline_at":deadline,"child_completed_at":child_completed_at}),
            None,
            None,
            now,
        );
        Ok(())
    }
    /// Runtime delivery only; human signals cannot impersonate a child result.
    pub fn task_completed(
        &mut self,
        id: MessageId,
        outcome: Value,
        workflow: Option<&dyn WorkflowDefinition>,
        now: i64,
    ) -> Result<(), DomainError> {
        let index = self
            .record
            .delegations
            .iter()
            .position(|d| d.id == id)
            .ok_or(DomainError::InvalidState)?;
        let status: actors::AgentTaskStatus = serde_json::from_value(outcome["status"].clone())
            .map_err(|_| {
                DomainError::InvalidConfiguration("child reply needs a terminal task status".into())
            })?;
        if !status.is_terminal() {
            return Err(DomainError::InvalidState);
        }
        let outcome = self.record.delegations[index].completed_outcome(outcome);
        if let Some(saved) = &self.record.delegations[index].outcome {
            return if saved == &outcome {
                Ok(())
            } else {
                Err(DomainError::ConflictingOutcome)
            };
        }
        if !self.record.controls.cancel_requested && self.record.status == RunStatus::Blocked {
            return Err(DomainError::InvalidState);
        }
        let workflow =
            if self.record.status.is_unfinished() && !self.record.controls.cancel_requested {
                Some(
                    workflow
                        .filter(|w| w.id() == self.record.workflow)
                        .ok_or_else(|| {
                            DomainError::InvalidWorkflow(
                                "definition unavailable for child continuation".into(),
                            )
                        })?,
                )
            } else {
                None
            };
        self.record.delegations[index].outcome = Some(outcome.clone());
        self.event(
            "delegation_completed",
            json!({"task_id":id,"outcome":outcome}),
            None,
            None,
            now,
        );
        // Keep terminal child receipts while the planner coordinates a question or guidance.
        // The original all-finished continuation is delivered when it returns.
        if (self.record.planner_question.is_some() || self.record.planner_guidance.is_some())
            && !self.record.controls.cancel_requested
        {
            return Ok(());
        }
        self.deliver_delegation_join(index, workflow, now)
    }

    fn deliver_delegation_join(
        &mut self,
        index: usize,
        workflow: Option<&dyn WorkflowDefinition>,
        now: i64,
    ) -> Result<(), DomainError> {
        let id = self.record.delegations[index].id;
        let Some(outcome) = self.record.delegations[index].outcome.clone() else {
            return Ok(());
        };
        let event = if let Some(group_id) = self.record.delegations[index].group_id {
            let group_index = self
                .record
                .task_groups
                .iter()
                .position(|g| g.id == group_id)
                .ok_or(DomainError::InvalidState)?;
            let outcomes: Option<Vec<Value>> = self.record.task_groups[group_index]
                .children
                .iter()
                .map(|id| {
                    self.record
                        .delegations
                        .iter()
                        .find(|d| &d.id == id)?
                        .outcome
                        .clone()
                })
                .collect();
            let Some(outcomes) = outcomes else {
                return Ok(());
            };
            if self.record.task_groups[group_index].joined_at.is_some() {
                return Err(DomainError::InvalidState);
            }
            self.record.task_groups[group_index].joined_at = Some(now);
            self.event(
                "task_group_joined",
                json!({"group_id":group_id,"outcomes":outcomes}),
                None,
                None,
                now,
            );
            WorkflowEvent::TaskGroupCompleted { group_id, outcomes }
        } else {
            WorkflowEvent::TaskCompleted {
                task_id: id,
                outcome,
            }
        };
        if self.record.controls.cancel_requested {
            self.finish_cancel(now);
            return Ok(());
        }
        if !self.record.status.is_unfinished() {
            return Ok(());
        }
        let workflow = workflow.ok_or(DomainError::InvalidState)?;
        let transition = workflow
            .transition(&self.record.state, &event)
            .and_then(|t| {
                self.validate_transition(&t, None, None, now)?;
                Ok(t)
            });
        self.record.status = RunStatus::Queued;
        match transition {
            Ok(t) => self.apply(t, now)?,
            Err(error) => self.terminate(RunStatus::Failed, Self::continuation_failure(error), now),
        }
        self.honour_pause();
        Ok(())
    }
    /// Typed native decisions preserve input idempotency even when a completed
    /// Run's workflow definition is no longer installed. New input needs it.
    pub fn respond_to_human(
        &mut self,
        id: WaitId,
        response: HumanResponse,
        workflow: Option<&dyn WorkflowDefinition>,
        now: i64,
    ) -> Result<bool, DomainError> {
        response.validate()?;
        let payload = response.payload();
        if let Some(receipt) = self.record.input_receipts.iter().find(|r| r.wait_id == id) {
            return if receipt.payload == payload {
                Ok(true)
            } else {
                Err(DomainError::ConflictingOutcome)
            };
        }
        let wait = self
            .record
            .wait
            .as_ref()
            .filter(|w| w.id == id)
            .ok_or(DomainError::UnknownWait)?;
        response.validate_wait(wait)?;
        let workflow = workflow
            .ok_or_else(|| DomainError::InvalidWorkflow("definition unavailable".into()))?;
        self.signal(id, payload, workflow, now)?;
        Ok(false)
    }

    pub fn signal(
        &mut self,
        id: WaitId,
        payload: Value,
        workflow: &dyn WorkflowDefinition,
        now: i64,
    ) -> Result<(), DomainError> {
        if let Some(receipt) = self.record.input_receipts.iter().find(|r| r.wait_id == id) {
            return if receipt.payload == payload {
                Ok(())
            } else {
                Err(DomainError::ConflictingOutcome)
            };
        }
        let wait = self
            .record
            .wait
            .as_ref()
            .filter(|w| w.id == id)
            .ok_or(DomainError::UnknownWait)?;
        if !self.record.status.is_unfinished() || self.record.controls.cancel_requested {
            return Err(DomainError::InvalidState);
        }
        if workflow.id() != self.record.workflow {
            return Err(DomainError::InvalidWorkflow(
                "definition version does not match run".into(),
            ));
        }
        let transition = workflow.transition(
            &self.record.state,
            &WorkflowEvent::InputReceived {
                wait_id: id,
                key: wait.key.clone(),
                payload: payload.clone(),
            },
        )?;
        self.validate_transition(&transition, None, None, now)?;
        self.record.wait = None;
        self.record.status = RunStatus::Queued;
        self.record.input_receipts.push(InputReceipt {
            wait_id: id,
            payload: payload.clone(),
        });
        self.event(
            "input_received",
            json!({"wait_id": id, "payload": payload}),
            None,
            None,
            now,
        );
        self.apply(transition, now)?;
        self.honour_pause();
        Ok(())
    }

    /// Resolve only a blocked, latest interrupted attempt. Preserve its uncertain outcome.
    /// The receipt, accepted result and continuation belong to the same unit of work.
    pub fn reconcile(
        &mut self,
        request: ReconciliationRequest,
        workflow: Option<&dyn WorkflowDefinition>,
        now: i64,
    ) -> Result<(), DomainError> {
        if let Some(receipt) = self
            .record
            .reconciliation_receipts
            .iter()
            .find(|r| r.request.attempt_id == request.attempt_id)
        {
            return if receipt.request == request {
                Ok(())
            } else {
                Err(DomainError::ConflictingReconciliation)
            };
        }
        if request.evidence.trim().is_empty() || request.evidence.len() > 16_384 {
            return Err(DomainError::InvalidReconciliation(
                "evidence must contain 1 to 16384 bytes of inspection notes".into(),
            ));
        }
        let ai = self
            .activities
            .iter()
            .position(|a| a.id == request.activity_id)
            .ok_or(DomainError::UnknownAttempt)?;
        let attempt = self
            .attempts
            .iter()
            .find(|a| a.id == request.attempt_id && a.activity_id == request.activity_id)
            .ok_or(DomainError::UnknownAttempt)?;
        if attempt.ordinal != self.activities[ai].attempts {
            return Err(DomainError::StaleAttempt);
        }
        if self.record.status != RunStatus::Blocked
            || self.record.error.as_ref().map(|e| e.code.as_str()) != Some("uncertain_outcome")
            || self.record.controls.cancel_requested
            || self.activities[ai].status != ActivityStatus::Indeterminate
            || self.activities[ai].active_attempt.is_some()
            || attempt.status != AttemptStatus::Interrupted
            || !matches!(attempt.outcome, Some(Outcome::Uncertain(_)))
        {
            return Err(DomainError::InvalidState);
        }
        let transition = match &request.decision {
            ReconciliationDecision::Accept { result } => {
                let workflow = workflow
                    .ok_or_else(|| DomainError::InvalidWorkflow("definition unavailable".into()))?;
                if workflow.id() != self.record.workflow {
                    return Err(DomainError::InvalidWorkflow(
                        "definition version does not match run".into(),
                    ));
                }
                Some(self.completed_transition(ai, result, workflow, now))
            }
            ReconciliationDecision::Retry {} => {
                if self.record.allowance.admitted >= self.record.allowance.limit
                    || self.activities[ai].attempts >= self.activities[ai].spec.retry.max_attempts
                    || self.activities[ai]
                        .spec
                        .usage
                        .as_ref()
                        .is_some_and(|r| !self.record.usage.can_reserve(r))
                {
                    return Err(DomainError::InvalidReconciliation(
                        "retry would exceed the existing attempt or model allowance".into(),
                    ));
                }
                None
            }
            ReconciliationDecision::Fail { message } => {
                if message.trim().is_empty() || message.len() > 16_384 {
                    return Err(DomainError::InvalidReconciliation(
                        "failure message must contain 1 to 16384 bytes".into(),
                    ));
                }
                None
            }
        };
        self.record
            .reconciliation_receipts
            .push(ReconciliationReceipt {
                request: request.clone(),
                recorded_at: now,
            });
        self.event(
            "activity_reconciled",
            json!({"request": request}),
            Some(request.activity_id),
            Some(request.attempt_id),
            now,
        );
        self.record.status = RunStatus::Queued;
        self.record.error = None;
        self.activities[ai].error = None;
        match request.decision {
            ReconciliationDecision::Accept { result } => {
                self.activities[ai].status = ActivityStatus::Succeeded;
                self.activities[ai].result = Some(result);
                match transition.ok_or(DomainError::InvalidState)? {
                    Ok(transition) => self.apply(transition, now)?,
                    Err(error) => {
                        self.terminate(RunStatus::Failed, Self::continuation_failure(error), now)
                    }
                }
            }
            ReconciliationDecision::Retry {} => {
                self.activities[ai].status = ActivityStatus::Queued;
                self.activities[ai].ready_at = now;
            }
            ReconciliationDecision::Fail { message } => {
                let failure = Failure::new("reconciliation_failed", message, false);
                self.activities[ai].status = ActivityStatus::Failed;
                self.activities[ai].error = Some(failure.clone());
                self.terminate(RunStatus::Failed, failure, now);
            }
        }
        // Operator assertions never refund uncertain usage or reset attempt allowances.
        self.honour_pause();
        Ok(())
    }

    fn continuation_failure(error: DomainError) -> Failure {
        let code = if matches!(&error, DomainError::ContextLimit { .. }) {
            "context_limit"
        } else {
            "invalid_workflow"
        };
        Failure::new(code, error.to_string(), false)
    }

    fn planner_question_closed(&self, now: i64) -> bool {
        self.record.planner_question.as_ref().is_some_and(|q| {
            self.steering_native_state()
                .is_ok_and(|(state, _)| planner::question_is_closed(state, now, q.closed))
        })
    }
    /// Fresh coordination work can be abandoned at its original deadline. Saved
    /// attempts/recovery inputs and admitted outcomes keep their normal lifecycle.
    pub(crate) fn finish_closed_planner_question(
        &mut self,
        workflow: &dyn WorkflowDefinition,
        now: i64,
    ) -> Result<(), DomainError> {
        if !self.planner_question_closed(now) || self.record.controls.cancel_requested {
            return Ok(());
        }
        let Some(activity) = self.next_due(now) else {
            return Ok(());
        };
        if activity.attempts != 0 || activity.preserve_input {
            return Ok(());
        }
        let question = self
            .record
            .planner_question
            .clone()
            .ok_or(DomainError::InvalidState)?;
        let transition = workflow.transition(
            &self.record.state,
            &WorkflowEvent::PlannerQuestionClosed {
                question_id: question.question_id,
                inspection: None,
                model_completed: false,
            },
        )?;
        for activity in &mut self.activities {
            if activity.status == ActivityStatus::Queued {
                activity.status = ActivityStatus::Failed;
                activity.error = Some(Failure::new(
                    "planner_question_closed",
                    "Coordination ended at its saved deadline or closed requester",
                    false,
                ));
            }
        }
        self.apply(transition, now)?;
        let index = self
            .record
            .delegations
            .iter()
            .position(|d| d.id == question.child_task_id)
            .ok_or(DomainError::InvalidState)?;
        self.deliver_delegation_join(index, Some(workflow), now)?;
        Ok(())
    }
    fn completed_transition(
        &self,
        ai: usize,
        result: &Value,
        workflow: &dyn WorkflowDefinition,
        now: i64,
    ) -> Result<Transition, DomainError> {
        let activity = &self.activities[ai];
        let event = if self.planner_question_closed(now)
            && activity.spec.input["definition"]["id"]["name"] != "reply_worker"
        {
            WorkflowEvent::PlannerQuestionClosed {
                question_id: self
                    .record
                    .planner_question
                    .as_ref()
                    .ok_or(DomainError::InvalidState)?
                    .question_id,
                inspection: tools::is_native_tool_handler(&activity.spec.handler)
                    .then(|| (activity.id, result.clone())),
                model_completed: activity.spec.handler == DefinitionId::new("agent.model", 1),
            }
        } else {
            WorkflowEvent::ActivityCompleted {
                activity_id: self.activities[ai].id,
                handler: self.activities[ai].spec.handler.clone(),
                result: result.clone(),
            }
        };
        workflow
            .transition(&self.record.state, &event)
            .and_then(|transition| {
                self.validate_transition(&transition, Some(self.activities[ai].id), None, now)?;
                Ok(transition)
            })
    }

    pub fn recover(&mut self, now: i64) {
        let running: Vec<usize> = self
            .activities
            .iter()
            .enumerate()
            .filter(|(_, a)| a.status == ActivityStatus::Running)
            .map(|(i, _)| i)
            .collect();
        for ai in running {
            let Some(id) = self.activities[ai].active_attempt.take() else {
                continue;
            };
            if let Some(attempt) = self.attempts.iter_mut().find(|a| a.id == id) {
                attempt.status = AttemptStatus::Interrupted;
                attempt.ended_at = Some(now);
                attempt.outcome = Some(Outcome::Uncertain(
                    "worker interrupted before outcome persistence".into(),
                ));
            }
            self.event(
                "attempt_interrupted",
                json!({"reason": "worker_restart"}),
                Some(self.activities[ai].id),
                Some(id),
                now,
            );
            if let Some(reservation) = &self.activities[ai].spec.usage {
                self.record.usage.reconcile(reservation, None);
                self.event(
                    "usage_reconciled",
                    json!({"usage": null, "ledger": self.record.usage}),
                    Some(self.activities[ai].id),
                    Some(id),
                    now,
                );
            }
            self.resolve_uncertain(
                ai,
                "worker interrupted before outcome persistence".into(),
                now,
            );
        }
        if self.record.controls.cancel_requested {
            self.finish_cancel(now);
        }
        self.honour_pause();
    }

    /// Commit deadline firing or a saved peer reply wake and its continuation together.
    pub fn fire_timer(
        &mut self,
        id: TimerId,
        workflow: &dyn WorkflowDefinition,
        now: i64,
    ) -> Result<bool, DomainError> {
        let index = self
            .timers
            .iter()
            .position(|t| t.id == id)
            .ok_or(DomainError::UnknownTimer)?;
        let timer = self.timers[index].clone();
        let peer_wait = self
            .record
            .worker_reply_wait
            .as_ref()
            .is_some_and(|w| w.timer_id == id);
        let planner_question = timer.payload["key"] == worker_questions::PLANNER_QUESTION_KEY;
        let planner_guidance = timer.payload["key"] == planner_guidance::WAKE_KEY;
        if timer.status == TimerStatus::Fired
            || (timer.due_at > now && !(peer_wait && timer.payload.get("outcome").is_some()))
            || !(self.record.status.is_active()
                || (peer_wait || planner_question || planner_guidance)
                    && self.record.status == RunStatus::Waiting)
        {
            return Ok(false);
        }
        if self
            .activities
            .iter()
            .any(|a| a.status == ActivityStatus::Running)
        {
            return Ok(false);
        }
        if workflow.id() != self.record.workflow {
            return Err(DomainError::InvalidWorkflow(
                "definition version does not match run".into(),
            ));
        }
        if planner_guidance {
            if self.record.status != RunStatus::Waiting
                || self.record.planner_question.is_some()
                || self.record.planner_guidance.is_some()
                || self.record.wait.is_some()
                || self
                    .activities
                    .iter()
                    .any(|a| matches!(a.status, ActivityStatus::Queued | ActivityStatus::Running))
            {
                return Ok(false);
            }
            let through = timer.payload["through"]
                .as_u64()
                .ok_or(DomainError::InvalidState)?;
            let guidance: planner_guidance::PlannerGuidance =
                serde_json::from_value(timer.payload["guidance"].clone())
                    .map_err(|_| DomainError::InvalidState)?;
            let findings_through = timer.payload["findings_through"].as_u64().unwrap_or(0);
            if (through <= self.record.guidance_handled_through
                && findings_through <= self.record.findings_handled_through
                && guidance.dependencies.is_empty())
                || guidance.targets.is_empty()
                || (guidance.instructions.is_empty()
                    && guidance.findings.is_empty()
                    && guidance.dependencies.is_empty())
                || self.pending_delegation().is_none()
            {
                self.timers[index].status = TimerStatus::Fired;
                self.timers[index].fired_at = Some(now);
                self.record.guidance_handled_through =
                    self.record.guidance_handled_through.max(through);
                self.record.findings_handled_through =
                    self.record.findings_handled_through.max(findings_through);
                self.event(
                    "planner_guidance_skipped",
                    json!({"through":through,"findings_through":findings_through}),
                    None,
                    None,
                    now,
                );
                return Ok(true);
            }
            self.record.guidance_handled_through =
                self.record.guidance_handled_through.max(through);
            self.record.findings_handled_through =
                self.record.findings_handled_through.max(findings_through);
            guidance.validate()?;
            if guidance.dependencies.iter().any(|d| {
                d.planner_run_id != self.record.id
                    || d.review_timer_id != Some(id)
                    || !self.record.delegations.iter().any(|p| {
                        p.id == d.prerequisite_task_id
                            && p.outcome
                                .as_ref()
                                .is_some_and(|o| o["status"] == serde_json::json!(d.outcome))
                    })
            }) {
                return Err(DomainError::InvalidState);
            }
            if guidance
                .targets
                .iter()
                .any(|t| self.pending_delegation_by_id(t.task_id).is_none())
            {
                return Err(DomainError::InvalidState);
            }
            self.record.planner_guidance = Some(guidance);
            self.record.status = RunStatus::Queued;
        }
        if planner_question {
            if self.record.status != RunStatus::Waiting
                || self.record.planner_question.is_some()
                || self.record.planner_guidance.is_some()
                || self.record.wait.is_some()
                || self
                    .activities
                    .iter()
                    .any(|a| matches!(a.status, ActivityStatus::Queued | ActivityStatus::Running))
            {
                return Ok(false);
            }
            let question: worker_questions::WorkerQuestion =
                serde_json::from_value(timer.payload["question"].clone())
                    .map_err(|_| DomainError::InvalidState)?;
            if question.recipient_run_id != self.record.id || question.recipient != "parent" {
                return Err(DomainError::InvalidState);
            }
            if question.outcome.is_some()
                || question.deadline_at <= now
                || self
                    .pending_delegation_by_id(question.requester_task_id)
                    .is_none()
            {
                self.timers[index].status = TimerStatus::Fired;
                self.timers[index].fired_at = Some(now);
                self.event(
                    "planner_question_skipped",
                    json!({"question_id":question.question_id}),
                    None,
                    None,
                    now,
                );
                return Ok(true);
            }
            self.record.planner_question = Some(worker_questions::PlannerQuestion {
                question_id: question.question_id,
                child_task_id: question.requester_task_id,
                closed: false,
            });
            self.record.status = RunStatus::Queued;
        }
        let transition = workflow
            .transition(
                &self.record.state,
                &WorkflowEvent::TimerFired {
                    timer_id: id,
                    scheduled_for: timer.due_at,
                    payload: timer.payload.clone(),
                },
            )
            .and_then(|transition| {
                self.validate_transition(&transition, None, Some(id), now)?;
                Ok(transition)
            });
        let timer = &mut self.timers[index];
        timer.status = TimerStatus::Fired;
        timer.fired_at = Some(now);
        let due_at = timer.due_at;
        if peer_wait {
            self.record.worker_reply_wait = None;
            self.record.status = RunStatus::Queued;
        }
        self.event(
            "timer_fired",
            json!({"timer_id": id, "due_at": due_at}),
            None,
            None,
            now,
        );
        match transition {
            Ok(transition) => self.apply(transition, now)?,
            Err(error) => self.terminate(RunStatus::Failed, Self::continuation_failure(error), now),
        }
        Ok(true)
    }

    pub fn block(&mut self, reason: String, now: i64) {
        self.terminate(
            RunStatus::Blocked,
            Failure::new("implementation_unavailable", reason, false),
            now,
        );
    }
    /// Only the application may establish that a missing implementation is restored.
    pub(crate) fn restore_implementation(&mut self, now: i64) -> Result<(), DomainError> {
        if self.record.status != RunStatus::Blocked
            || self.record.error.as_ref().map(|e| e.code.as_str())
                != Some("implementation_unavailable")
            || self.activities.iter().any(|a| {
                matches!(
                    a.status,
                    ActivityStatus::Running | ActivityStatus::Indeterminate
                )
            })
        {
            return Err(DomainError::InvalidState);
        }
        self.record.error = None;
        self.record.status = if self.record.wait.is_some()
            || self.pending_delegation().is_some()
                && self.record.planner_question.is_none()
                && self.record.planner_guidance.is_none()
            || self.record.worker_reply_wait.is_some()
        {
            RunStatus::Waiting
        } else {
            RunStatus::Queued
        };
        self.event("implementation_restored", json!({}), None, None, now);
        self.honour_pause();
        Ok(())
    }

    fn resolve_uncertain(&mut self, ai: usize, reason: String, now: i64) {
        let failure = Failure::new("uncertain_outcome", reason, true);
        if self.activities[ai].spec.recovery == RecoveryPolicy::RequiresReconciliation {
            self.activities[ai].status = ActivityStatus::Indeterminate;
            self.activities[ai].error = Some(failure.clone());
            self.terminate(RunStatus::Blocked, failure, now);
        } else {
            self.retry_or_finish(ai, failure, now);
        }
    }

    fn retry_or_finish(&mut self, ai: usize, failure: Failure, now: i64) {
        self.activities[ai].error = Some(failure.clone());
        if !failure.retryable
            || self.activities[ai].attempts >= self.activities[ai].spec.retry.max_attempts
        {
            self.activities[ai].status = ActivityStatus::Failed;
            self.terminate(RunStatus::Failed, failure, now);
        } else if self.record.allowance.admitted >= self.record.allowance.limit {
            self.activities[ai].status = ActivityStatus::Failed;
            self.terminate(
                RunStatus::BudgetExhausted,
                Failure::new("attempt_budget", "total attempt allowance exhausted", false),
                now,
            );
        } else {
            let a = &mut self.activities[ai];
            a.status = ActivityStatus::Queued;
            a.ready_at = now.saturating_add(a.spec.retry.delay(a.attempts));
            if let Some(advice) = &failure.retry_after {
                a.ready_at = a.ready_at.max(advice.deadline(now));
            }
            let (id, ready_at) = (a.id, a.ready_at);
            self.event(
                "retry_scheduled",
                json!({"ready_at": ready_at, "error": failure}),
                Some(id),
                None,
                now,
            );
        }
    }

    fn validate_transition(
        &self,
        transition: &Transition,
        completing: Option<ActivityId>,
        firing: Option<TimerId>,
        now: i64,
    ) -> Result<(), DomainError> {
        if self.record.planner_guidance.is_some() {
            let allowed = transition.commands.len() == 1
                && match &transition.commands[0] {
                    Command::ResumeDelegationJoin => true,
                    Command::Schedule(spec) => {
                        spec.handler == DefinitionId::new("agent.model", 1)
                            || self.steering_native_state().is_ok_and(|(state, _)| {
                                planner::worker_changes_allowed(state, &transition.commands[0])
                            })
                            || tools::is_native_tool_handler(&spec.handler)
                                && spec.input["definition"]["id"]
                                    == json!(DefinitionId::new("guide_workers", 1))
                            || tools::is_native_tool_handler(&spec.handler)
                                && self.steering_native_state().is_ok_and(|(state, _)| {
                                    planner::assignment_inspection_allowed(
                                        state,
                                        &spec.input["definition"],
                                        &spec.input["workspace"],
                                    ) || planner::worker_stop_allowed(
                                        state,
                                        &spec.input["definition"],
                                        &spec.input["workspace"],
                                    ) || planner::worker_pause_allowed(
                                        state,
                                        &spec.input["definition"],
                                        &spec.input["workspace"],
                                    )
                                })
                    }
                    Command::WaitForInput { .. } => {
                        self.steering_native_state().is_ok_and(|(state, _)| {
                            planner::worker_changes_allowed(state, &transition.commands[0])
                        })
                    }
                    _ => false,
                };
            if !allowed {
                return Err(DomainError::InvalidWorkflow(
                    "guidance can only route instructions and resume the saved join".into(),
                ));
            }
        } else if let Some(question) = &self.record.planner_question {
            let allowed = transition.commands.len() == 1
                && match &transition.commands[0] {
                    Command::ResumeDelegationJoin => true,
                    Command::Schedule(spec)
                        if spec.handler == DefinitionId::new("agent.model", 1) =>
                    {
                        true
                    }
                    Command::Schedule(spec) if tools::is_native_tool_handler(&spec.handler) => {
                        (spec.input["definition"]["id"]
                            == json!(DefinitionId::new("reply_worker", 2))
                            && spec.input["arguments"]["question_id"]
                                == json!(question.question_id))
                            || self.steering_native_state().is_ok_and(|(state, _)| {
                                planner::question_inspection_allowed(
                                    state,
                                    &spec.input["definition"],
                                    &spec.input["workspace"],
                                ) || planner::assignment_inspection_allowed(
                                    state,
                                    &spec.input["definition"],
                                    &spec.input["workspace"],
                                )
                            })
                    }
                    _ => false,
                };
            if !allowed {
                return Err(DomainError::InvalidWorkflow(
                    "a planner answering a child can only inspect permitted code, reply and return to its saved join"
                        .into(),
                ));
            }
        } else if self.pending_delegation().is_some() {
            return Err(DomainError::InvalidWorkflow(
                "complete the pending child task before continuing".into(),
            ));
        }
        let has_pending = self.activities.iter().any(|a| {
            Some(a.id) != completing
                && matches!(a.status, ActivityStatus::Queued | ActivityStatus::Running)
        }) || self.timers.iter().any(|t| {
            Some(t.id) != firing
                && t.status == TimerStatus::Scheduled
                && t.payload["key"] != worker_questions::PLANNER_QUESTION_KEY
                && t.payload["key"] != planner_guidance::WAKE_KEY
        });
        if transition.commands.is_empty() && !has_pending {
            return Err(DomainError::InvalidWorkflow(
                "transition must schedule work or complete".into(),
            ));
        }
        if transition.commands.len() > u32::MAX as usize {
            return Err(DomainError::InvalidWorkflow("too many commands".into()));
        }
        let completes = transition
            .commands
            .iter()
            .filter(|c| {
                matches!(
                    c,
                    Command::Complete(_) | Command::CompleteWithFollowUp { .. } | Command::Fail(_)
                )
            })
            .count();
        if completes > 0
            && (transition.commands.len() != 1
                || self.activities.iter().any(|a| {
                    Some(a.id) != completing
                        && matches!(a.status, ActivityStatus::Queued | ActivityStatus::Running)
                })
                || self.timers.iter().any(|t| {
                    Some(t.id) != firing
                        && t.status == TimerStatus::Scheduled
                        && t.payload["key"] != worker_questions::PLANNER_QUESTION_KEY
                        && t.payload["key"] != planner_guidance::WAKE_KEY
                }))
        {
            return Err(DomainError::InvalidWorkflow(
                "completion must be the only command and leave no pending activities or timers"
                    .into(),
            ));
        }
        let budgets = transition
            .commands
            .iter()
            .filter(|c| matches!(c, Command::ConfigureUsage(_)))
            .count();
        if budgets > 1
            || (budgets > 0
                && (self.record.revision != 0
                    || self.record.usage.budget.is_some()
                    || transition.commands.len() == 1))
        {
            return Err(DomainError::InvalidWorkflow(
                "usage budget must be configured once with initial work".into(),
            ));
        }
        for c in &transition.commands {
            if let Command::Fail(failure) = c
                && (failure.code.trim().is_empty()
                    || failure.code.len() > 128
                    || failure.message.trim().is_empty()
                    || failure.message.len() > 60_000
                    || failure.retryable)
            {
                return Err(DomainError::InvalidWorkflow(
                    "terminal failure needs a bounded code and message and cannot be retryable"
                        .into(),
                ));
            }
            if let Command::ConfigureGoalBudget(budget) = c
                && (budget.validate().is_err()
                    || self.record.revision != 0
                    || self.record.goal_budget.is_some()
                    || transition.commands.len() == 1
                    || transition
                        .commands
                        .iter()
                        .filter(|c| matches!(c, Command::ConfigureGoalBudget(_)))
                        .count()
                        != 1)
            {
                return Err(DomainError::InvalidWorkflow(
                    "a valid goal budget must be pinned once with initial work".into(),
                ));
            }
            if let Command::ConfigureFollowUps(context) = c {
                context.validate()?;
                if self.record.revision != 0
                    || self.record.follow_up_context.is_some()
                    || transition.commands.len() == 1
                    || transition
                        .commands
                        .iter()
                        .filter(|c| matches!(c, Command::ConfigureFollowUps(_)))
                        .count()
                        != 1
                {
                    return Err(DomainError::InvalidWorkflow(
                        "follow-up context must be pinned with initial work".into(),
                    ));
                }
            }
            if let Command::CompleteWithFollowUp {
                goal, milliseconds, ..
            } = c
            {
                let context = self.record.follow_up_context.as_ref().ok_or_else(|| {
                    DomainError::InvalidWorkflow("actor follow-up context required".into())
                })?;
                if self.record.follow_up.is_some() {
                    return Err(DomainError::InvalidState);
                }
                context.policy.validate_request(goal, *milliseconds)?;
                TimerDeadline::After {
                    milliseconds: *milliseconds,
                }
                .resolve(now)?;
            }
            if let Command::ConfigureDelegation(budget) = c {
                budget.validate()?;
                if self.record.revision != 0
                    || self.record.delegation_budget.is_some()
                    || transition
                        .commands
                        .iter()
                        .filter(|c| matches!(c, Command::ConfigureDelegation(_)))
                        .count()
                        != 1
                    || transition.commands.len() == 1
                {
                    return Err(DomainError::InvalidWorkflow(
                        "delegation budget must be pinned with initial work".into(),
                    ));
                }
            }
            if let Command::DelegatePreparedTask(prepared) = c {
                prepared.validate()?;
                if prepared.parent_run_id != self.record.id
                    || prepared.group_index.is_some()
                    || self
                        .record
                        .delegations
                        .iter()
                        .any(|d| d.id == prepared.task_id)
                    || !self.activities.iter().any(|a| {
                        a.id == prepared.activity_id
                            && a.spec.handler == workspace_handoff::handler("prepare")
                            && a.spec.input == json!(prepared.request)
                            && (completing == Some(a.id)
                                || (a.status == ActivityStatus::Succeeded
                                    && a.result
                                        .as_ref()
                                        .is_some_and(|r| r["prepared"] == json!(prepared))))
                    })
                {
                    return Err(DomainError::InvalidState);
                }
            }
            let goal = match c {
                Command::DelegateTask { goal, .. } => Some(goal),
                Command::DelegatePreparedTask(prepared) => Some(&prepared.request.goal),
                _ => None,
            };
            if let Some(goal) = goal {
                let budget = self.record.delegation_budget.as_ref().ok_or_else(|| {
                    DomainError::InvalidWorkflow("delegation budget required".into())
                })?;
                if transition.commands.len() != 1
                    || has_pending
                    || goal.trim().is_empty()
                    || goal.len() > 60_000
                {
                    return Err(DomainError::InvalidWorkflow(
                        "delegation requires one bounded goal and no other pending work".into(),
                    ));
                }
                if self.record.delegations.len() >= budget.max_tasks as usize {
                    return Err(DomainError::InvalidWorkflow(
                        "delegation task allowance exhausted".into(),
                    ));
                }
                if let Some(milliseconds) = budget.timeout_ms {
                    TimerDeadline::After { milliseconds }.resolve(now)?;
                }
            }
            if let Command::DelegatePreparedGroup(prepared) = c {
                prepared.validate()?;
                let group_id = prepared.id()?;
                if prepared.parent_run_id != self.record.id
                    || self.record.task_groups.iter().any(|g| g.id == group_id)
                    || prepared
                        .workspaces
                        .iter()
                        .any(|p| self.record.delegations.iter().any(|d| d.id == p.task_id))
                    || !self.activities.iter().any(|a| {
                        a.id == prepared.activity_id
                            && a.spec.handler == workspace_handoff::handler("prepare_group")
                            && a.spec.input == json!(prepared.request)
                            && (completing == Some(a.id)
                                || (a.status == ActivityStatus::Succeeded
                                    && a.result
                                        .as_ref()
                                        .is_some_and(|r| r["prepared"] == json!(prepared))))
                    })
                {
                    return Err(DomainError::InvalidState);
                }
            }
            let tasks = match c {
                Command::DelegateTasks { tasks } => Some(tasks),
                Command::DelegatePreparedGroup(prepared) => Some(&prepared.request.tasks),
                _ => None,
            };
            if let Some(tasks) = tasks {
                initial_dependencies::validate_workers(tasks)?;
                if matches!(c, Command::DelegateTasks { .. })
                    && tasks
                        .iter()
                        .any(|t| t.writable_paths.is_some() || t.depends_on.is_some())
                {
                    return Err(DomainError::InvalidWorkflow(
                        "task scopes and dependencies require a prepared workspace handoff".into(),
                    ));
                }
                let budget = self.record.delegation_budget.as_ref().ok_or_else(|| {
                    DomainError::InvalidWorkflow("delegation budget required".into())
                })?;
                let limit = budget.max_parallel_tasks.unwrap_or(1) as usize;
                let mut recipients = std::collections::HashSet::new();
                if transition.commands.len() != 1
                    || has_pending
                    || tasks.len() < 2
                    || tasks.len() > limit
                    || self.record.delegations.len().saturating_add(tasks.len())
                        > budget.max_tasks as usize
                    || tasks
                        .iter()
                        .any(|t| t.goal.trim().is_empty() || !recipients.insert(t.recipient))
                    || tasks.iter().map(|t| t.goal.len()).sum::<usize>() > 60_000
                {
                    return Err(DomainError::InvalidWorkflow(
                        "task group requires bounded distinct recipients and no other pending work"
                            .into(),
                    ));
                }
                if let Some(milliseconds) = budget.timeout_ms {
                    TimerDeadline::After { milliseconds }.resolve(now)?;
                }
            }
            if let Command::ConfigureUsage(budget) = c
                && budget.validate().is_err()
            {
                return Err(DomainError::InvalidConfiguration(
                    "usage limits must be positive or unlimited".into(),
                ));
            }
            if let Command::WaitForInput { key, prompt, .. } = c
                && (transition.commands.len() != 1
                    || has_pending
                    || key.is_empty()
                    || prompt.trim().is_empty())
            {
                return Err(DomainError::InvalidWorkflow(
                    "input wait must be the only command and leave no pending work".into(),
                ));
            }
            if let Command::StartTimer(spec) = c {
                spec.deadline.resolve(now)?;
            }
            if matches!(c, Command::WaitForWorkerReply { .. })
                && (transition.commands.len() != 1
                    || has_pending
                    || self.record.worker_reply_wait.is_some())
            {
                return Err(DomainError::InvalidWorkflow(
                    "worker reply wait must be the only command at a quiescent boundary".into(),
                ));
            }
            if let Command::RecordTaskPlan(plan) = c {
                plan.validate()?;
            }
            if let Command::Schedule(spec) = c
                && (spec.retry.max_attempts == 0
                    || spec.handler.name.is_empty()
                    || spec.handler.version == 0
                    || spec.retry.delay_ms > spec.retry.max_delay_ms)
            {
                return Err(DomainError::InvalidConfiguration(
                    "invalid activity definition or retry policy".into(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn apply(&mut self, transition: Transition, now: i64) -> Result<(), DomainError> {
        self.validate_transition(&transition, None, None, now)?;
        self.record.state = transition.state;
        self.record.revision += 1;
        for (ordinal, command) in transition.commands.into_iter().enumerate() {
            match command {
                Command::ConfigureFollowUps(context) => {
                    self.event(
                        "follow_up_context_configured",
                        json!({"context":context}),
                        None,
                        None,
                        now,
                    );
                    self.record.follow_up_context = Some(context);
                }
                Command::CompleteWithFollowUp {
                    result,
                    goal,
                    milliseconds,
                } => {
                    let context = self
                        .record
                        .follow_up_context
                        .as_ref()
                        .ok_or(DomainError::InvalidState)?;
                    let intent = follow_up::FollowUpIntent::new(
                        self.record.id,
                        context,
                        goal,
                        milliseconds,
                        now,
                    )?;
                    self.event(
                        "follow_up_requested",
                        json!({"follow_up":intent}),
                        None,
                        None,
                        now,
                    );
                    self.record.follow_up = Some(intent);
                    self.record.status = RunStatus::Succeeded;
                    self.record.result = Some(result.clone());
                    self.event("run_succeeded", json!({"result":result}), None, None, now);
                }
                Command::Schedule(spec) => {
                    let id = ActivityId::new();
                    self.event(
                        "activity_scheduled",
                        json!({"handler": spec.handler, "input": spec.input}),
                        Some(id),
                        None,
                        now,
                    );
                    self.activities.push(Activity {
                        id,
                        run_id: self.record.id,
                        scheduling_revision: self.record.revision,
                        command_ordinal: ordinal as u32,
                        spec,
                        status: ActivityStatus::Queued,
                        ready_at: now,
                        attempts: 0,
                        active_attempt: None,
                        result: None,
                        error: None,
                        preserve_input: false,
                    });
                }
                Command::ConfigureUsage(budget) => {
                    self.record.usage.budget = Some(budget);
                }
                Command::RecordTaskPlan(plan) => {
                    plan.validate()?;
                    self.event("task_plan_updated", json!({"plan":plan}), None, None, now);
                }
                Command::ConfigureGoalBudget(budget) => {
                    self.event(
                        "goal_budget_configured",
                        json!({"budget":budget}),
                        None,
                        None,
                        now,
                    );
                    self.record.goal_budget = Some(budget);
                }
                Command::ConfigureDelegation(budget) => {
                    self.record.delegation_budget = Some(budget)
                }
                command @ (Command::DelegateTask { .. } | Command::DelegatePreparedTask(_)) => {
                    let (recipient, goal, workspace_input) = match command {
                        Command::DelegateTask { recipient, goal } => (recipient, goal, None),
                        Command::DelegatePreparedTask(prepared) => (
                            prepared.request.recipient,
                            prepared.request.goal.clone(),
                            Some(prepared),
                        ),
                        _ => unreachable!(),
                    };
                    let delegation = delegation::TaskDelegation {
                        id: workspace_input
                            .as_ref()
                            .map_or_else(MessageId::new, |p| p.task_id),
                        recipient,
                        goal,
                        created_at: now,
                        outcome: None,
                        deadline_at: self
                            .record
                            .delegation_budget
                            .as_ref()
                            .and_then(|b| b.timeout_ms)
                            .map(|milliseconds| TimerDeadline::After { milliseconds }.resolve(now))
                            .transpose()?,
                        timed_out_at: None,
                        group_id: None,
                        initial_dependency: None,
                        workspace_input,
                    };
                    self.event(
                        "delegation_requested",
                        json!({"delegation":delegation}),
                        None,
                        None,
                        now,
                    );
                    self.record.delegations.push(delegation);
                    self.record.status = RunStatus::Waiting;
                }
                command @ (Command::DelegateTasks { .. } | Command::DelegatePreparedGroup(_)) => {
                    let (id, tasks): (_, Vec<_>) = match command {
                        Command::DelegateTasks { tasks } => (
                            TaskGroupId::new(),
                            tasks.into_iter().map(|t| (t, None)).collect(),
                        ),
                        Command::DelegatePreparedGroup(prepared) => (
                            prepared.id()?,
                            prepared
                                .workspaces
                                .into_iter()
                                .zip(prepared.request.tasks)
                                .map(|(p, task)| {
                                    (
                                        delegation::DelegatedWork {
                                            depends_on: task.depends_on,
                                            recipient: p.request.recipient,
                                            goal: p.request.goal.clone(),
                                            writable_paths: p.request.writable_paths.clone(),
                                        },
                                        Some(p),
                                    )
                                })
                                .collect(),
                        ),
                        _ => unreachable!(),
                    };
                    let deadline_at = self
                        .record
                        .delegation_budget
                        .as_ref()
                        .and_then(|b| b.timeout_ms)
                        .map(|milliseconds| TimerDeadline::After { milliseconds }.resolve(now))
                        .transpose()?;
                    let mut children = vec![];
                    let initial = initial_dependencies::bind_tasks(&tasks)?;
                    for (task, workspace_input) in tasks {
                        let child = delegation::TaskDelegation {
                            id: workspace_input
                                .as_ref()
                                .map_or_else(MessageId::new, |p| p.task_id),
                            recipient: task.recipient,
                            goal: task.goal,
                            created_at: now,
                            outcome: None,
                            deadline_at,
                            timed_out_at: None,
                            group_id: Some(id),
                            initial_dependency: initial.get(&task.recipient).cloned().map(Box::new),
                            workspace_input,
                        };
                        children.push(child.id);
                        self.event(
                            "delegation_requested",
                            json!({"delegation":child}),
                            None,
                            None,
                            now,
                        );
                        self.record.delegations.push(child);
                    }
                    let group = delegation::TaskGroup {
                        id,
                        children,
                        created_at: now,
                        deadline_at,
                        joined_at: None,
                    };
                    self.event(
                        "task_group_created",
                        json!({"group":group}),
                        None,
                        None,
                        now,
                    );
                    self.record.task_groups.push(group);
                    self.record.status = RunStatus::Waiting;
                }
                Command::PauseAtBoundary => {
                    self.record.controls.boundary_pause = true;
                    self.event("planner_boundary", json!({}), None, None, now);
                }
                Command::WaitForWorkerReply {
                    question_id,
                    deadline_at,
                } => {
                    let timer_id = TimerId::new();
                    self.timers.push(DurableTimer { id: timer_id, run_id: self.record.id, scheduling_revision: self.record.revision, command_ordinal: ordinal as u32, due_at: deadline_at, status: TimerStatus::Scheduled, fired_at: None, payload: json!({"key":worker_questions::WAIT_KEY,"question_id":question_id}) });
                    self.record.worker_reply_wait = Some(worker_questions::WorkerReplyWait {
                        question_id,
                        timer_id,
                        deadline_at,
                    });
                    self.record.status = RunStatus::Waiting;
                    self.event("worker_reply_wait_created", json!({"question_id":question_id,"timer_id":timer_id,"deadline_at":deadline_at}), None, None, now);
                }
                Command::ResumeDelegationJoin => {
                    if let Some(guidance) = self.record.planner_guidance.take() {
                        self.record.status = RunStatus::Waiting;
                        self.event(
                            "planner_guidance_finished",
                            json!({"guidance":guidance}),
                            None,
                            None,
                            now,
                        );
                        continue;
                    }
                    let question = self
                        .record
                        .planner_question
                        .take()
                        .ok_or(DomainError::InvalidState)?;
                    self.record.status = RunStatus::Waiting;
                    self.event(
                        "planner_question_finished",
                        json!({"question_id":question.question_id}),
                        None,
                        None,
                        now,
                    );
                }
                Command::WaitForInput {
                    key,
                    prompt,
                    payload,
                } => {
                    let wait = HumanWait {
                        id: WaitId::new(),
                        key,
                        prompt,
                        payload,
                    };
                    self.event("input_wait_created", json!({"wait": wait}), None, None, now);
                    self.record.wait = Some(wait);
                    self.record.status = RunStatus::Waiting;
                }
                Command::Complete(result) => {
                    self.record.status = RunStatus::Succeeded;
                    self.record.result = Some(result.clone());
                    self.event("run_succeeded", json!({"result": result}), None, None, now);
                }
                Command::Fail(failure) => self.terminate(RunStatus::Failed, failure, now),
                Command::StartTimer(spec) => {
                    let id = TimerId::new();
                    let due_at = spec.deadline.resolve(now)?;
                    self.event(
                        "timer_scheduled",
                        json!({"timer_id": id, "due_at": due_at, "payload": spec.payload}),
                        None,
                        None,
                        now,
                    );
                    self.timers.push(DurableTimer {
                        id,
                        run_id: self.record.id,
                        scheduling_revision: self.record.revision,
                        command_ordinal: ordinal as u32,
                        due_at,
                        status: TimerStatus::Scheduled,
                        fired_at: None,
                        payload: spec.payload,
                    });
                }
            }
        }
        self.honour_pause();
        Ok(())
    }

    fn terminate(&mut self, status: RunStatus, error: Failure, now: i64) {
        self.record.status = status;
        self.record.error = Some(error.clone());
        self.event(
            "run_stopped",
            json!({"status": status, "error": error}),
            None,
            None,
            now,
        );
    }

    pub(crate) fn event(
        &mut self,
        kind: &str,
        payload: Value,
        activity_id: Option<ActivityId>,
        attempt_id: Option<AttemptId>,
        now: i64,
    ) {
        self.record.updated_at = now;
        self.record.history_sequence += 1;
        self.pending_events.push(HistoryEvent {
            id: EventId::new(),
            run_id: self.record.id,
            sequence: self.record.history_sequence,
            kind: kind.into(),
            payload_version: 1,
            payload,
            activity_id,
            attempt_id,
            at: now,
        });
    }
}
