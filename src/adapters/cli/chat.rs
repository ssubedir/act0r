//! A terminal transport for the existing harness, inbox and reviewed application services.
use crate::{
    Engine, Error,
    adapters::{operator, tokio_worker},
    application::{AgentService, TaskInspectionOptions, TeamInboxItem, TeamTaskView},
    domain::{
        HumanResponse, HumanWaitKind, MessageId, RunId, RunStatus, SessionId, WaitId,
        agent::{ModelActionFormat, ModelTarget},
        conversation::{ContinuousConversation, ConversationStage},
        goal::GoalPausePrecondition,
        harness::{HarnessReceipt, HarnessRequest, HarnessTurnRequest},
        steering::{SteeringRequest, TurnSteering},
        turn_recovery::{RetryTurnRequest, TurnRecovery},
    },
};
use clap::{Args, ValueEnum};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::oneshot;
mod activity;
mod input;

#[derive(Args)]
pub(super) struct ChatOptions {
    /// Project to inspect and work on automatically, in an isolated copy.
    #[arg(long, conflicts_with_all = ["resume", "list"])]
    project: Option<PathBuf>,
    /// Restore a saved conversation, including its original model and project settings.
    #[arg(long, conflicts_with_all = ["model", "endpoint", "action_format", "model_timeout_ms", "list"])]
    resume: Option<SessionId>,
    /// Show saved conversations without starting a worker or making model calls.
    #[arg(long, conflicts_with_all = ["prompt", "model", "endpoint", "action_format", "model_timeout_ms"])]
    list: bool,
    /// Model name for a new session; otherwise use ACT0R_MODEL or the harness default.
    #[arg(long)]
    model: Option<String>,
    /// Full compatible Chat Completions URL; otherwise use ACT0R_MODEL_ENDPOINT.
    #[arg(long)]
    endpoint: Option<String>,
    /// Override the new session's structured action format.
    #[arg(long, value_enum)]
    action_format: Option<ActionFormat>,
    /// Request deadline for a new session, in milliseconds (default 90000).
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=600_000))]
    model_timeout_ms: Option<u32>,
    /// First request, or a reply/follow-up when resuming; otherwise read from stdin.
    prompt: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum ActionFormat {
    JsonSchema,
    FunctionCalling,
}

pub(super) fn model_target(
    model: Option<&str>,
    endpoint: Option<&str>,
    format: Option<ActionFormat>,
    timeout_ms: Option<u32>,
) -> Result<ModelTarget, Error> {
    let mut target: ModelTarget =
        serde_json::from_value(operator::harness_model_target(model, endpoint))?;
    if let Some(format) = format {
        target.action_format = match format {
            ActionFormat::JsonSchema => ModelActionFormat::JsonSchema,
            ActionFormat::FunctionCalling => ModelActionFormat::FunctionCalling,
        };
    }
    if let Some(timeout_ms) = timeout_ms {
        target.policy.request_timeout_ms = timeout_ms;
        target.policy.connect_timeout_ms = target.policy.connect_timeout_ms.min(timeout_ms);
    }
    Ok(target)
}

pub(super) fn validate_database_project(database: &Path, project: &Path) -> Result<(), Error> {
    if let Ok(relative) = database.strip_prefix(project)
        && !relative.components().any(|component| {
            let name = component.as_os_str().to_string_lossy();
            name.eq_ignore_ascii_case(".act0r") || name.eq_ignore_ascii_case(".build")
        })
    {
        return Err(Error::Unavailable("keep --db outside the project or under .act0r/ so runtime state is excluded from project inspection".into()));
    }
    Ok(())
}

struct Output {
    json: bool,
    terminal: input::TerminalOutput,
}
impl Output {
    fn new(json: bool) -> Self {
        Self {
            json,
            terminal: Default::default(),
        }
    }
    fn emit(&self, event: &str, data: Value, text: &str) -> Result<(), Error> {
        if self.json {
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout, "{}", json!({"event":event,"data":data}))?;
            stdout.flush()?;
        } else {
            self.terminal.write(text)?;
        }
        Ok(())
    }
    fn note(&self, text: &str) -> Result<(), Error> {
        self.emit("notice", json!({"message":text}), text)
    }
}

pub(super) async fn execute(
    options: ChatOptions,
    engine: Engine,
    agents: &AgentService,
    database: &Path,
    json_output: bool,
) -> Result<(), Error> {
    let database = std::fs::canonicalize(database)?;
    let output = Output::new(json_output);
    if options.list {
        let sessions = agents
            .teams()?
            .harness()?
            .conversation_sessions(Default::default())
            .await?;
        let mut text = String::new();
        for session in &sessions.items {
            text.push_str(&format!(
                "{}  {}  {}\n",
                session.session_id, session.root_status, session.prompt_preview
            ));
        }
        if sessions.items.is_empty() {
            text.push_str("No saved conversations.");
        }
        if sessions.truncated {
            text.push_str("Only the newest conversations are shown.\n");
        }
        return output.emit("sessions", json!(sessions), text.trim_end());
    }
    // Acquire ownership before admitting a new request. A second CLI must not leave
    // orphaned work when another process already owns this database.
    let worker = engine.worker().await?;
    let (stop, stopped) = oneshot::channel();
    let worker = tokio_worker::run_with_shutdown(
        worker,
        tokio_worker::WorkerOptions {
            max_concurrent_activities: 2,
            ..Default::default()
        },
        async {
            let _ = stopped.await;
            Ok(())
        },
    );
    let conversation = interact(options, agents, &database, &output);
    let shutdown = tokio_worker::shutdown_signal();
    tokio::pin!(worker, conversation, shutdown);
    let (result, worker_result) = tokio::select! {
        biased;
        signal = &mut shutdown => (signal.map_err(Error::from), None),
        result = &mut worker => (Ok(()), Some(result)),
        result = &mut conversation => (result, None),
    };
    let _ = stop.send(());
    let worker_result = match worker_result {
        Some(result) => result,
        None => worker.await,
    };
    result?;
    worker_result
}

enum Pending {
    Human(HarnessReceipt, TeamInboxItem),
    Application(HarnessReceipt, RunId, WaitId),
}
impl Pending {
    fn id(&self) -> WaitId {
        match self {
            Self::Human(_, item) => item.wait.id,
            Self::Application(_, _, wait) => *wait,
        }
    }
    async fn respond(&self, agents: &AgentService, text: &str) -> Result<(), Error> {
        let teams = agents.teams()?;
        match self {
            Self::Human(scope, item) => {
                let response = match item.kind {
                    HumanWaitKind::Question | HumanWaitKind::Guidance => HumanResponse::Answer {
                        answer: text.into(),
                    },
                    _ => HumanResponse::Approval {
                        approved: approval(text)?,
                    },
                };
                teams
                    .tasks()?
                    .respond(scope.team_id, scope.task_id, item.wait.id, response)
                    .await?;
            }
            Self::Application(scope, run, wait) => {
                teams
                    .harness()?
                    .respond_application(
                        scope.session_id,
                        scope.task_id,
                        *run,
                        *wait,
                        approval(text)?,
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
fn approval(text: &str) -> Result<bool, Error> {
    match text.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => Ok(true),
        "n" | "no" => Ok(false),
        _ => Err(Error::Unavailable(
            "reply yes or no to this approval".into(),
        )),
    }
}

#[derive(Default)]
struct Displayed {
    activity: activity::ActivityFeed,
    prompts: HashSet<MessageId>,
    responses: HashSet<MessageId>,
    waits: HashSet<WaitId>,
    patches: HashSet<crate::domain::ActivityId>,
    applications: HashSet<String>,
    steering: HashMap<MessageId, &'static str>,
    steering_scope: Option<HarnessReceipt>,
    recovery: Option<String>,
    progress: String,
}
struct Snapshot {
    conversation: ContinuousConversation,
    task: TeamTaskView,
    pending: Option<Pending>,
    application_unfinished: bool,
    application_held: bool,
    steering: TurnSteering,
    activity_pending: bool,
    recovery: Option<TurnRecovery>,
}
fn stage_label(stage: ConversationStage) -> &'static str {
    match stage {
        ConversationStage::Inspecting => "Inspecting project",
        ConversationStage::SetupReview => "Preparing project work",
        ConversationStage::Coding => "Working",
        ConversationStage::Verifying => "Verifying",
        ConversationStage::Completed => "Completed",
        ConversationStage::Incomplete => "Incomplete",
        ConversationStage::Failed => "Needs attention",
        ConversationStage::Stopped => "Stopped",
    }
}
impl Snapshot {
    fn quiescent(&self) -> bool {
        if self.activity_pending {
            return false;
        }
        let preparing = self.conversation.auto_execute
            && self.conversation.stage == ConversationStage::SetupReview
            && self.conversation.setup_error.is_none();
        self.pending.is_some()
            || self.application_held
            || self.conversation.setup_error.is_some()
            || self.task.task.nodes.iter().any(|node| {
                node.status == "blocked" || node.execution_held || node.status == "paused"
            })
            || (self.task.task.summary.settled
                && self.conversation.turns.iter().any(|turn| {
                    turn.turn.task_id == self.conversation.active.task_id && turn.turn.settled
                })
                && !preparing
                && !self.application_unfinished)
    }
}

async fn refresh(
    agents: &AgentService,
    session: SessionId,
    displayed: &mut Displayed,
    output: &Output,
) -> Result<Snapshot, Error> {
    let teams = agents.teams()?;
    let harness = teams.harness()?;
    let conversation = harness.continuous_conversation(session, None, None).await?;
    let activity_pending = displayed
        .activity
        .refresh(teams.events()?, &conversation, output)
        .await?;
    if displayed.progress.is_empty() && conversation.next_before.is_some() {
        output.note("Showing the newest 32 turns; older turns remain saved in the database.")?;
    }
    for turn in &conversation.turns {
        if !turn.repeated_request && displayed.prompts.insert(turn.turn.task_id) {
            output.emit(
                "request",
                json!(turn),
                &format!("\nYou: {}", turn.turn.prompt),
            )?;
        }
        let setup_result = turn.phase == "discovery"
            && (turn.turn.response_truncated
                || turn.turn.response.as_deref().is_some_and(|text| {
                    serde_json::from_str::<Value>(text)
                        .is_ok_and(|value| value.get("project_setup").is_some())
                }));
        if turn.turn.settled
            && !setup_result
            && let Some(response) = &turn.turn.response
            && displayed.responses.insert(turn.turn.task_id)
        {
            output.emit("response", json!(turn), &format!("\nHarness: {response}"))?;
        }
    }
    let scope = &conversation.active;
    let task = teams
        .tasks()?
        .status(
            scope.team_id,
            scope.task_id,
            TaskInspectionOptions::default(),
        )
        .await?;
    let root = task
        .task
        .nodes
        .iter()
        .find(|node| node.task_id == scope.task_id);
    let progress = format!(
        "{}: {} · {} model calls{}",
        stage_label(conversation.stage),
        root.map_or("preparing", |node| node.status.as_str()),
        task.task.summary.visible_usage.admitted_model_calls,
        conversation
            .setup_error
            .as_ref()
            .map_or(String::new(), |error| format!(" · {error}")),
    );
    if displayed.progress != progress {
        output.emit(
            "status",
            json!({"stage":conversation.stage,"task":task,"setup_error":conversation.setup_error}),
            &progress,
        )?;
        for node in &task.task.nodes {
            if node.status == "blocked" || node.status == "failed" {
                output.note(&format!(
                    "{}: {}{}",
                    node.status,
                    node.reason.message,
                    node.run_id.map_or(String::new(), |run| format!(
                        " (inspect with run status {run})"
                    ))
                ))?;
            }
        }
        displayed.progress = progress;
    }
    let recovery = if root.is_some_and(|node| node.status == "failed") {
        let view = harness
            .turn_recovery(scope.session_id, scope.task_id)
            .await?;
        let key = format!(
            "{}:{}:{}:{:?}",
            scope.task_id, view.history_sequence, view.retry_allowed, view.blocked_reason
        );
        if displayed.recovery.as_ref() != Some(&key) {
            show_recovery(&view, output)?;
            displayed.recovery = Some(key);
        }
        Some(view)
    } else {
        displayed.recovery = None;
        None
    };
    // A queued follow-up can become active between polls. Finish displaying the
    // previous turn's delivery state before following the new active scope.
    if let Some(previous) = &displayed.steering_scope
        && previous.task_id != scope.task_id
    {
        let previous = harness
            .turn_steering(previous.session_id, previous.task_id)
            .await?;
        show_steering(&previous, displayed, output)?;
    }
    let steering = harness
        .turn_steering(scope.session_id, scope.task_id)
        .await?;
    show_steering(&steering, displayed, output)?;
    displayed.steering_scope = Some(scope.clone());
    let inbox = teams
        .tasks()?
        .inbox(
            scope.team_id,
            scope.task_id,
            TaskInspectionOptions::default(),
        )
        .await?;
    let mut pending = inbox
        .items
        .into_iter()
        .find(|item| item.actionable)
        .map(|item| Pending::Human(scope.clone(), item));
    if let Some(Pending::Human(_, item)) = &pending
        && displayed.waits.insert(item.wait.id)
    {
        let approval = matches!(
            item.kind,
            HumanWaitKind::ToolApproval | HumanWaitKind::ChildImportApproval
        );
        output.emit(
            if approval { "approval" } else { "question" },
            json!(item),
            &format!(
                "\n{}{}",
                item.wait.prompt,
                if approval {
                    "\nReply yes or no."
                } else {
                    "\nEnter your answer."
                }
            ),
        )?;
        if approval {
            output.emit(
                "approval_details",
                item.wait.payload.clone(),
                &serde_json::to_string_pretty(&item.wait.payload)?,
            )?;
        }
    }
    let mut application_unfinished = false;
    let mut application_held = false;
    if conversation.coding.is_some() {
        if task.task.summary.settled {
            show_patch(agents, scope, displayed, output, false).await?;
        }
        let applications = harness
            .applications(scope.session_id, scope.task_id)
            .await?;
        for application in applications.items {
            let view = harness
                .application(scope.session_id, scope.task_id, application.run_id)
                .await?;
            application_held |= matches!(view.status, RunStatus::Blocked | RunStatus::Paused);
            application_unfinished |=
                view.status.is_unfinished() || view.status == RunStatus::Blocked;
            let key = format!("{}:{:?}", view.run_id, view.status);
            if displayed.applications.insert(key) {
                output.emit(
                    "application",
                    json!(view),
                    &format!(
                        "Patch application {}: {:?}{}{}",
                        view.run_id,
                        view.status,
                        view.report.as_ref().map_or(String::new(), |report| format!(
                            " · {:?} ({} files)",
                            report.status,
                            report.applied_files.len()
                        )),
                        view.error.as_ref().map_or(String::new(), |error| format!(
                            " · {} (inspect with run status {})",
                            error.message, view.run_id
                        ))
                    ),
                )?;
            }
            if pending.is_none()
                && let Some(wait) = &view.wait
            {
                pending = Some(Pending::Application(scope.clone(), view.run_id, wait.id));
                if displayed.waits.insert(wait.id) {
                    output.emit(
                        "approval",
                        json!(view),
                        &format!("\n{}\nReply yes or no.", wait.prompt),
                    )?;
                    if let Some(preview) = &view.preview {
                        output.emit(
                            "approval_details",
                            json!(preview),
                            &serde_json::to_string_pretty(preview)?,
                        )?;
                    }
                }
            }
        }
    }
    Ok(Snapshot {
        conversation,
        task,
        pending,
        application_unfinished,
        application_held,
        steering,
        activity_pending,
        recovery,
    })
}

fn show_recovery(view: &TurnRecovery, output: &Output) -> Result<(), Error> {
    let mut text = String::new();
    if let Some(error) = &view.error {
        text.push_str(&format!(
            "Saved failure {}: {}\n",
            diagnostic(&error.code),
            diagnostic(&error.message)
        ));
    }
    text.push_str(&recovery_hint(view));
    output.emit("recovery", json!(view), &text)
}
fn recovery_hint(view: &TurnRecovery) -> String {
    if view.retry_allowed {
        return "Use /retry to retry the saved model request once.".into();
    }
    let mut reason = format!(
        "Retry unavailable: {}.",
        diagnostic(
            view.blocked_reason
                .as_deref()
                .unwrap_or("no eligible failed model request")
        )
    );
    if view.error.as_ref().is_some_and(|error| {
        error.code == "model_configuration"
            || error
                .request
                .as_ref()
                .is_some_and(|request| matches!(request.http_status, Some(401 | 403)))
    }) {
        reason.push_str(" Correct the model configuration or credentials, restart the CLI to reload environment settings, and send a new request.");
    } else if view
        .error
        .as_ref()
        .is_some_and(|error| error.code == "model_refusal")
    {
        reason.push_str(" Review the refusal and send a revised request.");
    }
    reason
}
fn diagnostic(text: &str) -> String {
    let mut bounded: String = text.chars().take(512).collect();
    if text.chars().nth(512).is_some() {
        bounded.push('…');
    }
    // Saved errors and reasons must remain one line of terminal text.
    bounded.escape_debug().to_string()
}

fn show_steering(
    view: &TurnSteering,
    displayed: &mut Displayed,
    output: &Output,
) -> Result<(), Error> {
    for entry in &view.items {
        let (status, label) = if entry.application.is_some() {
            ("delivered", "Steering included in a model request")
        } else if view.terminal {
            (
                "not_delivered",
                "Task ended before steering could be delivered",
            )
        } else {
            ("pending", "Steering pending the next fresh model request")
        };
        if displayed.steering.insert(entry.request.message_id, status) != Some(status) {
            output.emit(
                "steering",
                json!({"session_id":view.session_id,"task_id":view.task_id,"entry":entry,"status":status}),
                &format!("{label}: {}", entry.request.text),
            )?;
        }
    }
    Ok(())
}

async fn show_patch(
    agents: &AgentService,
    scope: &HarnessReceipt,
    displayed: &mut Displayed,
    output: &Output,
    force: bool,
) -> Result<(), Error> {
    let view = agents
        .teams()?
        .harness()?
        .changes(scope.session_id, scope.task_id)
        .await?;
    if let Some(changes) = &view.changes {
        if force || displayed.patches.insert(changes.activity_id) {
            let review = if crate::domain::harness::validate_application_changes(&view).is_ok() {
                "Use /apply to preview application to the original project."
            } else {
                "This candidate is not eligible for application; inspect its saved completion report."
            };
            output.emit(
                "patch",
                json!(view),
                &format!(
                    "\n{} changed files; verification: {:?}\n{}\n{review}",
                    changes.files.len(),
                    view.completion.as_ref().map(|report| report.status),
                    changes.patch()
                ),
            )?;
        }
    } else if force {
        output.note("No saved patch for this turn.")?;
    }
    Ok(())
}

async fn interact(
    mut options: ChatOptions,
    agents: &AgentService,
    database: &Path,
    output: &Output,
) -> Result<(), Error> {
    let mut lines = input::Input::new(!output.json, output.terminal.clone())?;
    if lines.interactive() {
        output.note("Enter adds a line · Ctrl+S sends (Alt+Enter also supported) · Ctrl+C exits with work saved.")?;
    }
    let mut supplied = options.prompt.take();
    let session = if let Some(session) = options.resume {
        // Verify the saved scope before consuming a supplied reply or admitting anything.
        agents
            .teams()?
            .harness()?
            .continuous_conversation(session, None, None)
            .await?
            .session_id
    } else {
        let workspace = options
            .project
            .map(|path| -> Result<String, Error> {
                let path = std::fs::canonicalize(path)?;
                if !path.is_dir() {
                    return Err(Error::Unavailable("project must be a directory".into()));
                }
                validate_database_project(database, &path)?;
                path.into_os_string()
                    .into_string()
                    .map_err(|_| Error::Unavailable("project path must be UTF-8".into()))
            })
            .transpose()?;
        let target = model_target(
            options.model.as_deref(),
            options.endpoint.as_deref(),
            options.action_format,
            options.model_timeout_ms,
        )?;
        output.note("Enter a request. Project work discovers its setup automatically.")?;
        let prompt = loop {
            let line = match supplied.take() {
                Some(line) => line,
                None => match lines.recv().await {
                    Some(line) => line?,
                    None => return Ok(()),
                },
            };
            if line.trim() == "/exit" {
                return Ok(());
            }
            if !line.trim().is_empty() {
                break line;
            }
        };
        let receipt: HarnessReceipt = serde_json::from_value(
            operator::execute(
                agents,
                operator::Request::HarnessStart {
                    request: HarnessRequest {
                        auto_execute: workspace.is_some(),
                        discover_project: workspace.is_some(),
                        message_id: MessageId::new(),
                        prompt,
                        target,
                        workspace,
                        coding: None,
                        budget: None,
                    },
                },
            )
            .await?,
        )?;
        receipt.session_id
    };
    let resume = format!(
        "act0r --db \"{}\" chat --resume {session}",
        database.display()
    );
    output.emit(
        "session",
        json!({"session_id":session,"database":database,"resume":resume}),
        &format!(
            "Session {session}\nResume: {resume}\n/exit saves and leaves; /help lists commands."
        ),
    )?;
    let mut displayed = Displayed {
        activity: activity::ActivityFeed::new(options.resume.is_some()),
        ..Default::default()
    };
    let mut eof = false;
    loop {
        let snapshot = refresh(agents, session, &mut displayed, output).await?;
        let line = if let Some(line) = supplied.take() {
            Some(Ok(line))
        } else {
            if eof && snapshot.quiescent() {
                break;
            }
            tokio::select! {
                line = lines.recv(), if !eof => line,
                _ = tokio::time::sleep(Duration::from_millis(250)) => continue,
            }
        };
        let Some(line) = line else {
            eof = true;
            continue;
        };
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if line.trim() == "/exit" {
            break;
        }
        let result = handle_line(&line, agents, &snapshot, &mut displayed, output).await;
        if let Err(error) = result {
            output.emit(
                "input_error",
                json!({"message":error.to_string()}),
                &error.to_string(),
            )?;
        }
    }
    output.emit(
        "saved",
        json!({"session_id":session,"resume":resume}),
        &format!("Session saved. Resume: {resume}"),
    )
}

async fn handle_line(
    line: &str,
    agents: &AgentService,
    snapshot: &Snapshot,
    displayed: &mut Displayed,
    output: &Output,
) -> Result<(), Error> {
    let scope = &snapshot.conversation.active;
    let teams = agents.teams()?;
    if let Some(text) = line.trim().strip_prefix("/steer")
        && (text.is_empty() || text.starts_with(char::is_whitespace))
    {
        let text = text.trim();
        if text.is_empty() {
            return Err(Error::Unavailable(
                "Usage: /steer MESSAGE (1..4096 UTF-8 bytes).".into(),
            ));
        }
        // Input may arrive after the worker launches a Run but before the next UI poll.
        let current = teams
            .harness()?
            .turn_steering(scope.session_id, scope.task_id)
            .await?;
        let run_id = current.run_id.ok_or_else(|| {
            Error::Unavailable("this turn has not started; wait for its planner to launch".into())
        })?;
        let receipt = teams
            .harness()?
            .send_steering(
                scope.session_id,
                scope.task_id,
                SteeringRequest {
                    message_id: MessageId::new(),
                    run_id,
                    text: text.into(),
                },
            )
            .await?;
        displayed
            .steering
            .insert(receipt.entry.request.message_id, "pending");
        let mut data = json!(receipt);
        data["status"] = json!("pending");
        return output.emit(
            "steering_saved",
            data,
            "Steering saved; pending the next fresh model request.",
        );
    }
    match line.trim() {
        "/help" => output.note("Enter requests or answers normally. Approvals take yes/no. /steer MESSAGE guides the current turn. Commands: /status, /retry, /patch, /apply, /pause, /resume, /stop, /exit."),
        "/status" => {
            let mut data = json!(snapshot.task);
            data["steering"] = json!(snapshot.steering);
            data["recovery"] = json!(snapshot.recovery);
            output.emit("status", data, &format!("{}; {} tasks pending", stage_label(snapshot.conversation.stage), snapshot.task.task.summary.pending))?;
            for entry in &snapshot.steering.items {
                let status = if entry.application.is_some() { "delivered" } else if snapshot.steering.terminal { "not delivered" } else { "pending" };
                output.note(&format!("Steering {status}: {}", entry.request.text))?;
            }
            if let Some(view) = &snapshot.recovery { show_recovery(view, output)?; }
            Ok(())
        }
        "/retry" => {
            let harness = teams.harness()?;
            // Re-read eligibility and bind the decision to this exact saved checkpoint.
            let view = harness.turn_recovery(scope.session_id, scope.task_id).await?;
            if !view.retry_allowed { return Err(Error::Unavailable(recovery_hint(&view))); }
            let request = match (view.run_id, view.activity_id, view.attempt_id) {
                (Some(run_id), Some(activity_id), Some(attempt_id)) => RetryTurnRequest {
                    request_id: MessageId::new(), run_id, activity_id, attempt_id,
                    expected_history_sequence: view.history_sequence,
                },
                _ => return Err(Error::Unavailable("recovery has no saved model checkpoint".into())),
            };
            let receipt = harness.retry_turn(scope.session_id, scope.task_id, request).await?;
            displayed.responses.remove(&scope.task_id);
            displayed.recovery = None;
            displayed.progress.clear();
            output.emit("recovery_saved", json!(receipt), "Retry saved; one model attempt will reuse the saved request and completed work. Provider retry deadlines still apply.")
        }
        "/patch" => show_patch(agents, scope, displayed, output, true).await,
        "/apply" => {
            if snapshot.application_unfinished {
                return Err(Error::Unavailable("an application is already pending; answer its saved approval first".into()));
            }
            let id = teams.harness()?.preview(scope.session_id, scope.task_id, RunId::new()).await?;
            output.emit("application_started", json!({"run_id":id}), "Preparing the saved patch for review. Application will ask for approval.")
        }
        "/pause" | "/resume" => {
            let controls = teams.controls()?;
            let current = controls.status(scope.team_id, scope.task_id).await?;
            let precondition = GoalPausePrecondition { expected_revision: current.pause.revision };
            let receipt = if line.trim() == "/pause" {
                controls.pause(scope.team_id, scope.task_id, precondition).await?
            } else {
                controls.resume(scope.team_id, scope.task_id, precondition).await?
            };
            output.emit("control", json!(receipt), if receipt.control.paused { "Work paused." } else { "Work resumed." })
        }
        "/stop" => {
            let receipt = teams.controls()?.cancel(scope.team_id, scope.task_id).await?;
            output.emit("control", json!(receipt), "Stop saved for this turn.")
        }
        command if command.starts_with('/') => output.note("Unknown command. Use /help, or start a request with ordinary text."),
        _ => {
            if let Some(pending) = &snapshot.pending {
                pending.respond(agents, line).await?;
                output.emit("decision_saved", json!({"wait_id":pending.id()}), "Reply saved.")
            } else {
                let receipt = teams.harness()?.append(snapshot.conversation.session_id, HarnessTurnRequest {
                    message_id: MessageId::new(), prompt: line.into(),
                }).await?;
                output.emit("turn_saved", json!(receipt), "Request saved; queued turns wait for earlier work.")
            }
        }
    }
}
