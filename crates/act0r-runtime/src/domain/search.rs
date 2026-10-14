//! A bounded, single-expansion search. Durable execution stays in Run and CodingTask.
use super::{agent::*, branch::*, task::*, *};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchConfig {
    pub version: u32,
    #[serde(default = "default_strategy")]
    pub strategy: DefinitionId,
    pub task: TaskConfig,
    pub width: u32,
    pub warmup_steps: u32,
    pub rollout_steps: u32,
    #[serde(default)]
    pub scripted_proposals: Vec<AgentAction>,
    #[serde(default)]
    pub final_tests: Option<FinalTestSource>,
}
fn default_strategy() -> DefinitionId {
    DefinitionId::new("feedback_cost", 1)
}
pub(crate) fn default_search_version() -> u32 {
    1
}

fn is_false(value: &bool) -> bool {
    !value
}
pub(crate) fn candidate_workflow(
    version: u32,
) -> Result<&'static dyn WorkflowDefinition, DomainError> {
    match version {
        1 => Ok(&SearchCandidateWorkflow),
        2 => Ok(&FeedbackSearchCandidateWorkflow),
        _ => Err(DomainError::InvalidConfiguration(
            "unsupported search version".into(),
        )),
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalTestSource {
    pub source: String,
    pub test_command: Vec<String>,
}
impl SearchConfig {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.task.validate()?;
        if self.task.agent.target.action_format == ModelActionFormat::FunctionCalling {
            return Err(DomainError::InvalidConfiguration(
                "search proposals require the JSON schema action format".into(),
            ));
        }
        if !matches!(self.version, 1 | 2)
            || self.strategy.name.trim().is_empty()
            || self.strategy.version == 0
            || self.task.candidates != 1
            || !(2..=4).contains(&self.width)
            || self.warmup_steps > 16
            || !(1..=16).contains(&self.rollout_steps)
            || (self.task.agent.target.provider != "scripted"
                && !self.scripted_proposals.is_empty())
        {
            return Err(DomainError::InvalidConfiguration("search v1 or v2 requires one root, width 2..4, warmup 0..16 and rollout 1..16; fixture proposals require the scripted provider".into()));
        }
        if self.task.agent.target.provider == "scripted" {
            validate_proposals(&self.scripted_proposals, self.width)?;
        }
        Ok(())
    }
}
pub fn validate_proposals(actions: &[AgentAction], width: u32) -> Result<(), DomainError> {
    if !(2..=width as usize).contains(&actions.len())
        || actions.iter().enumerate().any(|(i, a)| {
            matches!(a, AgentAction::Finish { .. } | AgentAction::AskHuman { .. })
                || actions[..i].contains(a)
        })
    {
        return Err(DomainError::InvalidConfiguration("expected 2..width distinct tool actions; finish and human waits are not expansion actions".into()));
    }
    Ok(())
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchRun {
    pub search_id: SearchId,
    pub initial_action: Option<AgentAction>,
    #[serde(default = "default_search_version")]
    pub version: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchFork {
    pub search_id: SearchId,
    pub proposal_run_id: RunId,
    pub actions: Vec<AgentAction>,
    #[serde(default = "default_search_version")]
    pub version: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProposalRequest {
    pub model: ModelRequest,
    pub width: u32,
    pub scripted_proposals: Vec<AgentAction>,
}
pub struct ProposalReply {
    pub actions: Result<Vec<AgentAction>, String>,
    pub usage: Option<TokenUsage>,
    pub retryable: bool,
    pub diagnostics: Option<RequestDiagnostics>,
    pub retry_after: Option<RetryAfter>,
}
impl ProposalRequest {
    pub fn from_checkpoint(cp: &Checkpoint, config: &SearchConfig) -> Result<Self, DomainError> {
        let mut model: ModelRequest =
            serde_json::from_value(cp.record().seed.next_model.input.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        model.instructions.push_str(&format!("\nPropose between 2 and {} distinct alternative next tool actions. Return an object with an actions array. Do not execute them. No finish or ask_human actions. Use the saved observations and fixed permissions.", config.width));
        let variants = action_schema()["properties"]["action"]["anyOf"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| {
                !matches!(
                    v["properties"]["kind"]["enum"][0].as_str(),
                    Some("finish" | "ask_human")
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        model.schema = json!({"type":"object","properties":{"actions":{"type":"array","minItems":2,"maxItems":config.width,"items":{"anyOf":variants}}},"required":["actions"],"additionalProperties":false});
        if model.context_input_bytes() > u64::from(config.task.agent.context.max_input_bytes) {
            return Err(DomainError::InvalidConfiguration(
                "proposal request exceeds the pinned context limit".into(),
            ));
        }
        let input_bytes = model.context_input_bytes();
        if let Some(context) = &mut model.context {
            context.input_bytes = input_bytes;
        }
        Ok(Self {
            model,
            width: config.width,
            scripted_proposals: config.scripted_proposals.clone(),
        })
    }
}
pub struct ProposalWorkflow;
impl WorkflowDefinition for ProposalWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("search.proposals", 1)
    }
    fn transition(&self, _: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        match event {
            WorkflowEvent::Started(input) => {
                let input: ProposalInput = serde_json::from_value(input.clone())
                    .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
                let retry = input.request.model.target.policy.retry();
                Ok(Transition {
                    state: Value::Null,
                    commands: vec![
                        Command::ConfigureUsage(input.budget),
                        Command::Schedule(ActivitySpec {
                            handler: DefinitionId::new("search.propose", 1),
                            usage: Some(input.request.model.reservation()),
                            input: serde_json::to_value(input.request).unwrap(),
                            retry,
                            recovery: RecoveryPolicy::SafeToRepeat,
                        }),
                    ],
                })
            }
            WorkflowEvent::ActivityCompleted {
                handler, result, ..
            } if *handler == DefinitionId::new("search.propose", 1) => Ok(Transition {
                state: Value::Null,
                commands: vec![Command::Complete(result.clone())],
            }),
            _ => Err(DomainError::InvalidState),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProposalInput {
    pub search_id: SearchId,
    pub task_id: TaskId,
    pub checkpoint_id: CheckpointId,
    pub request: ProposalRequest,
    pub budget: UsageBudget,
}

/// Each tool completion yields to the planner; feedback never runs held-out tests.
pub struct SearchCandidateWorkflow;
pub struct FeedbackSearchCandidateWorkflow;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackPhase {
    AwaitingApproval,
    Scheduled,
    Completed,
    Denied,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FeedbackEvaluation {
    pub phase: FeedbackPhase,
    pub activity_id: Option<ActivityId>,
    pub result: Option<Value>,
}
impl FeedbackEvaluation {
    pub(crate) fn is_ready(&self) -> bool {
        matches!(self.phase, FeedbackPhase::Completed | FeedbackPhase::Denied)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SearchCandidateState {
    pub agent: Value,
    pub input: CandidateInput,
    pub finish: Option<Value>,
    pub steps: u32,
    pub finished: bool,
    pub verifying: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub feedback_checked: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<FeedbackEvaluation>,
}
impl SearchCandidateState {
    pub(crate) fn begin_feedback(&mut self) -> Result<Command, DomainError> {
        let agent: AgentState = serde_json::from_value(self.agent.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        if self.input.search.as_ref().is_none_or(|s| s.version != 2)
            || self.feedback.is_some()
            || self.feedback_checked
            || self.verifying
            || agent.pending.is_some()
        {
            return Err(DomainError::InvalidState);
        }
        let approval = self.input.config.require_approval;
        self.feedback = Some(FeedbackEvaluation {
            phase: if approval {
                FeedbackPhase::AwaitingApproval
            } else {
                FeedbackPhase::Scheduled
            },
            activity_id: None,
            result: None,
        });
        Ok(if approval {
            Command::WaitForInput {
                key: "search_feedback".into(),
                prompt: "Approve the fixed feedback tests at this rollout boundary?".into(),
                payload: json!({"workspace":self.input.config.workspace,"test_command":self.input.config.test_command,"action":{"kind":"run_tests"}}),
            }
        } else {
            self.feedback_activity()?
        })
    }
    fn feedback_activity(&self) -> Result<Command, DomainError> {
        Ok(Command::Schedule(ActivitySpec {
            handler: DefinitionId::new("search.feedback", 1),
            input: serde_json::to_value(ToolRequest {
                workspace: self.input.config.workspace.clone(),
                action: AgentAction::RunTests,
                test_command: self.input.config.test_command.clone(),
                writable_paths: self.input.config.writable_paths.clone(),
            })
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?,
            retry: RetryPolicy::default(),
            recovery: if self.input.config.repeat_safe_tests {
                RecoveryPolicy::SafeToRepeat
            } else {
                RecoveryPolicy::RequiresReconciliation
            },
            usage: None,
        }))
    }
}
impl WorkflowDefinition for SearchCandidateWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("coding.search_candidate", 1)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        candidate_transition(state, event, 1)
    }
}
impl WorkflowDefinition for FeedbackSearchCandidateWorkflow {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("coding.search_candidate", 2)
    }
    fn transition(&self, state: &Value, event: &WorkflowEvent) -> Result<Transition, DomainError> {
        candidate_transition(state, event, 2)
    }
}
fn candidate_transition(
    state: &Value,
    event: &WorkflowEvent,
    version: u32,
) -> Result<Transition, DomainError> {
    let mut next: SearchCandidateState;
    let mut transition;
    if let WorkflowEvent::Started(input) = event {
        let input: CandidateInput = serde_json::from_value(input.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        let marker = input.search.as_ref().ok_or(DomainError::InvalidState)?;
        if marker.version != version {
            return Err(DomainError::InvalidState);
        }
        input.config.validate()?;
        if let Some(seed) = &input.branch {
            let action = marker
                .initial_action
                .clone()
                .ok_or(DomainError::InvalidState)?;
            next = SearchCandidateState {
                agent: serde_json::to_value(&seed.agent).unwrap(),
                input: input.clone(),
                finish: None,
                steps: 0,
                finished: false,
                verifying: false,
                feedback_checked: version == 2
                    && (seed.agent.tests_passed
                        || seed
                            .agent
                            .observations
                            .iter()
                            .rev()
                            .find(|o| {
                                matches!(
                                    o.action,
                                    AgentAction::ReplaceText { .. } | AgentAction::RunTests
                                )
                            })
                            .is_some_and(|o| {
                                matches!(o.action, AgentAction::RunTests)
                                    && o.result["approved"] != false
                            })),
                feedback: None,
            };
            transition = Transition {
                state: next.agent.clone(),
                commands: vec![
                    Command::ConfigureUsage(
                        seed.origin
                            .usage
                            .budget
                            .clone()
                            .ok_or(DomainError::InvalidState)?,
                    ),
                    Command::Schedule(ActivitySpec {
                        handler: DefinitionId::new("search.action", 1),
                        input: json!({"action":action, "request_id":null}),
                        retry: RetryPolicy::default(),
                        recovery: RecoveryPolicy::SafeToRepeat,
                        usage: None,
                    }),
                ],
            };
        } else {
            if marker.initial_action.is_some() {
                return Err(DomainError::InvalidState);
            }
            transition = AgentWorkflow.transition(
                &Value::Null,
                &WorkflowEvent::Started(serde_json::to_value(&input.config).unwrap()),
            )?;
            next = SearchCandidateState {
                agent: transition.state.clone(),
                input,
                finish: None,
                steps: 0,
                finished: false,
                verifying: false,
                feedback_checked: false,
                feedback: None,
            };
            transition.commands.push(Command::PauseAtBoundary);
        }
    } else {
        next = serde_json::from_value(state.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        if next
            .input
            .search
            .as_ref()
            .is_none_or(|s| s.version != version)
        {
            return Err(DomainError::InvalidState);
        }
        if version == 2 {
            match event {
                WorkflowEvent::InputReceived { key, payload, .. } if key == "search_feedback" => {
                    if next
                        .feedback
                        .as_ref()
                        .is_none_or(|f| f.phase != FeedbackPhase::AwaitingApproval)
                    {
                        return Err(DomainError::InvalidState);
                    }
                    let approved = payload["approved"].as_bool().ok_or_else(|| {
                        DomainError::InvalidConfiguration(
                            "feedback approval needs an approved boolean".into(),
                        )
                    })?;
                    let command = if approved {
                        next.feedback.as_mut().unwrap().phase = FeedbackPhase::Scheduled;
                        next.feedback_activity()?
                    } else {
                        let feedback = next.feedback.as_mut().unwrap();
                        feedback.phase = FeedbackPhase::Denied;
                        feedback.result = Some(json!({"approved":false}));
                        next.feedback_checked = true;
                        Command::PauseAtBoundary
                    };
                    return Ok(Transition {
                        state: serde_json::to_value(next).unwrap(),
                        commands: vec![command],
                    });
                }
                WorkflowEvent::ActivityCompleted {
                    activity_id,
                    handler,
                    result,
                } if *handler == DefinitionId::new("search.feedback", 1) => {
                    if next
                        .feedback
                        .as_ref()
                        .is_none_or(|f| f.phase != FeedbackPhase::Scheduled)
                    {
                        return Err(DomainError::InvalidState);
                    }
                    let mut agent: AgentState = serde_json::from_value(next.agent.clone())
                        .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
                    agent.tests_passed = result["ok"] == true && result["exit_code"] == 0;
                    super::agent::record_observation(
                        &mut agent,
                        AgentAction::RunTests,
                        result.clone(),
                    )?;
                    next.agent = serde_json::to_value(agent).unwrap();
                    next.feedback_checked = true;
                    next.feedback = Some(FeedbackEvaluation {
                        phase: FeedbackPhase::Completed,
                        activity_id: Some(*activity_id),
                        result: Some(result.clone()),
                    });
                    return Ok(Transition {
                        state: serde_json::to_value(next).unwrap(),
                        commands: vec![Command::PauseAtBoundary],
                    });
                }
                _ => {}
            }
        }
        if next.verifying {
            let transition = CandidateWorkflow.transition(state, event)?;
            let candidate: CandidateState = serde_json::from_value(transition.state)
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            next.agent = candidate.agent;
            next.input = candidate.input;
            next.finish = candidate.finish;
            return Ok(Transition {
                state: serde_json::to_value(next).unwrap(),
                commands: transition.commands,
            });
        }
        let mapped;
        let event = if let WorkflowEvent::ActivityCompleted {
            activity_id,
            handler,
            result,
        } = event
            && *handler == DefinitionId::new("search.action", 1)
        {
            mapped = WorkflowEvent::ActivityCompleted {
                activity_id: *activity_id,
                handler: DefinitionId::new("agent.model", 1),
                result: result.clone(),
            };
            &mapped
        } else {
            event
        };
        if version == 2
            && matches!(event, WorkflowEvent::ActivityCompleted { handler, .. } if *handler == DefinitionId::new("agent.tool",1))
        {
            let agent: AgentState = serde_json::from_value(next.agent.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            match agent.pending {
                Some(AgentAction::RunTests) => next.feedback_checked = true,
                Some(AgentAction::ReplaceText { .. }) => next.feedback_checked = false,
                _ => {}
            }
        }
        transition = AgentWorkflow.transition(&next.agent, event)?;
        if matches!(event, WorkflowEvent::ActivityCompleted { handler, .. } if *handler == DefinitionId::new("agent.tool",1))
        {
            next.steps += 1;
            transition.commands.push(Command::PauseAtBoundary);
        }
        if transition
            .commands
            .iter()
            .any(|c| matches!(c, Command::Complete(_)))
        {
            let mut agent: AgentState = serde_json::from_value(transition.state.clone())
                .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
            next.finished = true;
            transition.commands = vec![
                super::agent::next_model(&mut agent)?,
                Command::PauseAtBoundary,
            ];
            transition.state = serde_json::to_value(agent).unwrap();
        }
    }
    next.agent = transition.state;
    Ok(Transition {
        state: serde_json::to_value(next).unwrap(),
        commands: transition.commands,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchPhase {
    Warmup,
    Checkpoint,
    Proposing,
    RollingOut,
    Verifying,
    Completed,
    Failed,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RolloutScore {
    pub feedback_passed: bool,
    pub attempts: u32,
    pub model_calls: u32,
    pub charged_tokens: u64,
    pub steps: u32,
    pub terminal_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_feedback: Option<FeedbackEvaluation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchNode {
    pub run_id: RunId,
    pub parent: RunId,
    pub action: AgentAction,
    pub visits: u32,
    pub reward: u32,
    pub result: Option<RolloutScore>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchRecord {
    pub id: SearchId,
    pub version: u32,
    pub config: SearchConfig,
    pub task_id: TaskId,
    pub root: RunId,
    pub checkpoint_id: CheckpointId,
    pub proposal_run_id: RunId,
    pub fork_id: ForkId,
    pub phase: SearchPhase,
    pub nodes: Vec<SearchNode>,
    pub selected: Option<RunId>,
    pub verified: bool,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}
#[derive(Clone, Debug, Serialize)]
pub struct Search {
    pub(crate) record: SearchRecord,
}
impl Search {
    pub fn record(&self) -> &SearchRecord {
        &self.record
    }
    pub(crate) fn needs_feedback(&self, run: &Run) -> Result<bool, DomainError> {
        if self.record.config.version != 2
            || self.record.phase != SearchPhase::RollingOut
            || !self
                .record
                .nodes
                .iter()
                .any(|n| n.run_id == run.record().id && n.result.is_none())
            || run.record().status != RunStatus::Paused
            || run.record().controls.pause_requested
            || run.record().controls.cancel_requested
        {
            return Ok(false);
        }
        let state: SearchCandidateState = serde_json::from_value(run.record().state.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        Ok(
            (state.steps >= self.record.config.rollout_steps || state.finished)
                && !state.feedback_checked
                && state.feedback.is_none()
                && !state.verifying,
        )
    }
    pub(crate) fn new(
        id: SearchId,
        config: SearchConfig,
        task_id: TaskId,
        root: RunId,
        now: i64,
    ) -> Result<Self, DomainError> {
        config.validate()?;
        Ok(Self {
            record: SearchRecord {
                id,
                version: 1,
                config,
                task_id,
                root,
                checkpoint_id: CheckpointId::new(),
                proposal_run_id: RunId::new(),
                fork_id: ForkId::new(),
                phase: SearchPhase::Warmup,
                nodes: vec![],
                selected: None,
                verified: false,
                error: None,
                created_at: now,
                updated_at: now,
            },
        })
    }
    pub(crate) fn score(
        &mut self,
        run: &Run,
        prefix: &BranchOrigin,
        now: i64,
    ) -> Result<bool, DomainError> {
        let node = self
            .record
            .nodes
            .iter_mut()
            .find(|n| n.run_id == run.record().id)
            .ok_or(DomainError::InvalidState)?;
        if node.result.is_some() {
            return Ok(false);
        }
        let state: SearchCandidateState = serde_json::from_value(run.record().state.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        let agent: AgentState = serde_json::from_value(state.agent)
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        if self.record.phase != SearchPhase::RollingOut
            || run.record().status.is_active()
            || matches!(run.record().status, RunStatus::Waiting | RunStatus::Blocked)
            || run.record().controls.pause_requested
            || (self.record.config.version == 2
                && run.record().status == RunStatus::Paused
                && (!state.feedback_checked
                    || state.feedback.as_ref().is_some_and(|f| !f.is_ready())))
            || (run.record().status == RunStatus::Paused
                && state.steps < self.record.config.rollout_steps
                && !state.finished
                && !agent.tests_passed)
        {
            return Err(DomainError::InvalidState);
        }
        let usage = run
            .record()
            .usage
            .since(&prefix.usage)
            .ok_or(DomainError::InvalidState)?;
        let passed = run.record().status == RunStatus::Paused && agent.tests_passed;
        node.visits = 1;
        node.reward = u32::from(passed);
        node.result = Some(RolloutScore {
            feedback_passed: passed,
            steps: state.steps,
            attempts: run
                .record()
                .allowance
                .admitted
                .checked_sub(prefix.attempts)
                .ok_or(DomainError::InvalidState)?,
            model_calls: usage.admitted_model_calls,
            charged_tokens: usage.charged_tokens(),
            terminal_error: run.record().error.as_ref().map(|e| e.message.clone()),
            automatic_feedback: state.feedback,
        });
        self.record.updated_at = now;
        Ok(true)
    }
    pub(crate) fn checkpoint_requested(
        &mut self,
        root: &Run,
        now: i64,
    ) -> Result<bool, DomainError> {
        if self.record.phase != SearchPhase::Warmup {
            return Ok(false);
        }
        let state: SearchCandidateState = serde_json::from_value(root.record().state.clone())
            .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        if root.record().id != self.record.root
            || root.record().status != RunStatus::Paused
            || root.record().controls.pause_requested
            || (state.steps < self.record.config.warmup_steps && !state.finished)
        {
            return Err(DomainError::InvalidState);
        }
        self.record.phase = SearchPhase::Checkpoint;
        self.record.updated_at = now;
        Ok(true)
    }
    pub(crate) fn proposals_started(&mut self, now: i64) -> Result<(), DomainError> {
        if self.record.phase != SearchPhase::Checkpoint {
            return Err(DomainError::InvalidState);
        }
        self.record.phase = SearchPhase::Proposing;
        self.record.updated_at = now;
        Ok(())
    }
    pub(crate) fn proposals(&self, run: &Run) -> Result<Vec<AgentAction>, DomainError> {
        if self.record.phase != SearchPhase::Proposing
            || run.record().id != self.record.proposal_run_id
            || run.record().workflow != ProposalWorkflow.id()
            || run.record().status != RunStatus::Succeeded
        {
            return Err(DomainError::InvalidState);
        }
        let actions: Vec<AgentAction> = serde_json::from_value(
            run.record()
                .result
                .as_ref()
                .ok_or(DomainError::InvalidState)?["actions"]
                .clone(),
        )
        .map_err(|e| DomainError::InvalidWorkflow(e.to_string()))?;
        validate_proposals(&actions, self.record.config.width)?;
        Ok(actions)
    }
    pub(crate) fn rollouts_planned(
        &mut self,
        plan: &ForkRecord,
        actions: Vec<AgentAction>,
        now: i64,
    ) -> Result<(), DomainError> {
        if self.record.phase != SearchPhase::Proposing
            || plan.id != self.record.fork_id
            || plan.checkpoint_id != self.record.checkpoint_id
            || plan.run_ids.len() != actions.len()
        {
            return Err(DomainError::InvalidState);
        }
        self.record.nodes = plan
            .run_ids
            .iter()
            .zip(actions)
            .map(|(id, action)| SearchNode {
                run_id: *id,
                parent: self.record.root,
                action,
                visits: 0,
                reward: 0,
                result: None,
            })
            .collect();
        self.record.phase = SearchPhase::RollingOut;
        self.record.updated_at = now;
        Ok(())
    }
    pub(crate) fn choose(
        &mut self,
        selected: Option<RunId>,
        now: i64,
    ) -> Result<bool, DomainError> {
        if matches!(
            self.record.phase,
            SearchPhase::Verifying | SearchPhase::Completed
        ) {
            return if self.record.selected == selected {
                Ok(false)
            } else {
                Err(DomainError::ConflictingOutcome)
            };
        }
        if self.record.phase != SearchPhase::RollingOut
            || self.record.nodes.is_empty()
            || self.record.nodes.iter().any(|n| n.result.is_none())
            || selected.is_some_and(|id| {
                !self
                    .record
                    .nodes
                    .iter()
                    .any(|n| n.run_id == id && n.result.as_ref().unwrap().feedback_passed)
            })
        {
            return Err(DomainError::InvalidState);
        }
        self.record.selected = selected;
        self.record.phase = if selected.is_some() {
            SearchPhase::Verifying
        } else {
            SearchPhase::Completed
        };
        self.record.updated_at = now;
        if selected.is_none() {
            self.record.error = Some("no branch passed feedback within the bounded rollout".into());
        }
        Ok(true)
    }
    pub(crate) fn verification_observed(&mut self, run: &Run, now: i64) -> Result<(), DomainError> {
        if self.record.phase != SearchPhase::Verifying
            || Some(run.record().id) != self.record.selected
            || run.record().status.is_unfinished()
            || run.record().status == RunStatus::Blocked
        {
            return Err(DomainError::InvalidState);
        }
        self.record.verified = run
            .record()
            .result
            .as_ref()
            .is_some_and(|r| r["evaluation"]["passed"] == true);
        self.record.phase = SearchPhase::Completed;
        self.record.updated_at = now;
        if !self.record.verified {
            self.record.error = Some("selected branch did not pass final verification".into());
        }
        Ok(())
    }
    pub(crate) fn fail(&mut self, error: String, now: i64) {
        self.record.phase = SearchPhase::Failed;
        self.record.error = Some(error);
        self.record.updated_at = now;
    }
}
