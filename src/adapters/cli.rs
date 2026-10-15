use crate::{
    Engine, Error,
    adapters::tokio_worker::{self, WorkerOptions},
    application::AgentService,
    domain::{
        ActivityStatus, AgentId, BenchmarkId, CheckpointId, DefinitionId, DomainError, ForkId,
        MessageId, ReconciliationRequest, RunId, RunStatus, SearchId, TaskId, WaitId,
        agent::{AgentConfig, ContextSelection},
        benchmark::BenchmarkConfig,
        search::SearchConfig,
        task::TaskConfig,
    },
};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
mod api;
mod chat;
#[path = "cli/task_inspection.rs"]
mod task_inspection;
mod team_events;
mod teams;

#[derive(Parser)]
#[command(version, about = "Durable local workflows for AI agents")]
pub struct Cli {
    /// Database file (chat: .act0r/chat.db; other commands: act0r.db).
    #[arg(long, global = true)]
    db: Option<PathBuf>,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: TopCommand,
}
#[derive(Subcommand)]
enum TopCommand {
    /// Start or resume a durable conversation; discover and work on an optional project.
    Chat(chat::ChatOptions),
    /// Inspect retained harness storage and remove reviewed generated workspace caches.
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
    /// Serve the local HTTP API, optionally alongside the durable worker.
    Api {
        #[command(subcommand)]
        command: api::ApiCommand,
    },
    Team {
        #[command(subcommand)]
        command: teams::TeamCommand,
    },
    Actor {
        #[command(subcommand)]
        command: ActorCommand,
    },
    Search {
        #[command(subcommand)]
        command: SearchCommand,
    },
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCommand,
    },
    Fork {
        #[command(subcommand)]
        command: ForkCommand,
    },
    Benchmark {
        #[command(subcommand)]
        command: BenchmarkCommand,
    },
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
    },
}
#[derive(Subcommand)]
enum ActorCommand {
    /// Discover installed native tools and their versioned contracts.
    Tool {
        #[command(subcommand)]
        command: ActorToolCommand,
    },
    /// Inspect failure history or explicitly recover a supervision pause.
    Supervision {
        #[command(subcommand)]
        command: ActorSupervisionCommand,
    },
    /// Schedule, inspect or cancel one-shot tasks for an actor.
    Reminder {
        #[command(subcommand)]
        command: ActorReminderCommand,
    },
    /// Inspect or clear the actor's bounded task history.
    Memory {
        #[command(subcommand)]
        command: ActorMemoryCommand,
    },
    /// Inspect registered integrations or create an agent using its adapter.
    Agent {
        #[command(subcommand)]
        command: ActorAgentCommand,
    },
    /// Inspect or cancel a submitted task using its original message ID.
    Task {
        #[command(subcommand)]
        command: ActorTaskCommand,
    },
    /// Create a long-lived agent with a pinned message behavior.
    Create {
        definition: String,
        #[arg(long, default_value_t = 1)]
        definition_version: u32,
        #[arg(long)]
        config_file: PathBuf,
        #[arg(long)]
        agent_id: Option<AgentId>,
        #[arg(long, value_parser=clap::value_parser!(u32).range(1..=64))]
        max_consecutive_failures: Option<u32>,
        #[arg(long, default_value_t=crate::domain::actors::DEFAULT_TASK_CAPACITY,value_parser=clap::value_parser!(u32).range(1..))]
        task_capacity: u32,
    },
    /// Create a coding worker with its own persistent workspace.
    CodingWorker {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        config_file: PathBuf,
        #[arg(long, default_value_t=100,value_parser=clap::value_parser!(u32).range(1..))]
        attempt_limit: u32,
        #[arg(long, default_value_t=crate::domain::actors::DEFAULT_TASK_CAPACITY,value_parser=clap::value_parser!(u32).range(1..))]
        task_capacity: u32,
    },
    Send {
        agent_id: AgentId,
        #[arg(long, default_value = "task")]
        kind: String,
        #[arg(long)]
        input_file: PathBuf,
        #[arg(long)]
        sender: Option<AgentId>,
        #[arg(long)]
        message_id: Option<MessageId>,
        #[arg(long)]
        correlation_id: Option<MessageId>,
    },
    Status {
        agent_id: AgentId,
    },
    Inbox {
        agent_id: AgentId,
    },
    Pause {
        agent_id: AgentId,
    },
    Resume {
        agent_id: AgentId,
    },
}
#[derive(Subcommand)]
enum ActorToolCommand {
    List,
}
#[derive(Subcommand)]
enum StorageCommand {
    Report,
    /// Requires the fingerprint from storage report; refuses new work or changed caches.
    Clean {
        #[arg(long)]
        session: crate::domain::SessionId,
        #[arg(long)]
        agent: AgentId,
        #[arg(long)]
        fingerprint: String,
    },
}
#[derive(Subcommand)]
enum ActorMemoryCommand {
    Status { agent_id: AgentId },
    Clear { agent_id: AgentId },
}
#[derive(Subcommand)]
enum ActorSupervisionCommand {
    Status { agent_id: AgentId },
    Recover { agent_id: AgentId },
}
#[derive(Subcommand)]
enum ActorReminderCommand {
    Schedule {
        agent_id: AgentId,
        #[arg(long)]
        input_file: PathBuf,
        /// Absolute UTC Unix timestamp in milliseconds.
        #[arg(
            long,
            allow_negative_numbers = true,
            required_unless_present = "after_ms",
            conflicts_with = "after_ms"
        )]
        at_unix_ms: Option<i64>,
        /// Resolve this delay once, when the reminder is first saved.
        #[arg(
            long,
            required_unless_present = "at_unix_ms",
            conflicts_with = "at_unix_ms"
        )]
        after_ms: Option<u64>,
        /// Reuse this ID when retrying an identical scheduling request.
        #[arg(long)]
        message_id: Option<MessageId>,
        #[arg(long)]
        sender: Option<AgentId>,
        #[arg(long)]
        correlation_id: Option<MessageId>,
    },
    Status {
        message_id: MessageId,
    },
    List {
        agent_id: AgentId,
    },
    Cancel {
        message_id: MessageId,
    },
}
#[derive(Subcommand)]
enum ActorTaskCommand {
    /// Prepare a durable preview against a target project; project files are not written.
    PreviewChanges {
        message_id: MessageId,
        #[arg(long)]
        target: PathBuf,
    },
    /// Inspect a saved patch application, its approval wait, conflicts, or final receipt.
    Application {
        run_id: RunId,
    },
    /// Approve the exact saved preview; the worker subsequently applies it.
    ApplyChanges {
        run_id: RunId,
    },
    /// Reject a saved preview without applying it.
    RejectChanges {
        run_id: RunId,
    },
    /// Show the immutable workspace patch saved for this physical actor task.
    Changes {
        message_id: MessageId,
        /// Export the saved unified patch to a new file; existing files are refused.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Hold execution throughout the original goal without changing individual Run controls.
    PauseGoal {
        message_id: MessageId,
    },
    /// Release the saved goal hold, retaining individual controls and original deadlines.
    ResumeGoal {
        message_id: MessageId,
    },
    /// Durably stop the entire original goal, including children and future follow-ups.
    CancelGoal {
        message_id: MessageId,
    },
    /// Inspect an entire goal through persisted forwarding, delegation, and follow-up ownership.
    Inspect {
        message_id: MessageId,
        #[arg(long,default_value_t=128,value_parser=clap::value_parser!(u32).range(1..=512))]
        max_nodes: u32,
        #[arg(long,default_value_t=16,value_parser=clap::value_parser!(u32).range(0..=128))]
        max_depth: u32,
    },
    Status {
        message_id: MessageId,
    },
    List {
        agent_id: AgentId,
    },
    Cancel {
        message_id: MessageId,
    },
}
#[derive(Subcommand)]
enum ActorAgentCommand {
    List,
    Create {
        adapter: String,
        /// Pin an installed version; defaults to the latest installed adapter.
        #[arg(long)]
        adapter_version: Option<u32>,
        #[arg(long)]
        config_file: PathBuf,
        #[arg(long)]
        source: Option<PathBuf>,
        #[arg(long, default_value_t=100,value_parser=clap::value_parser!(u32).range(1..))]
        attempt_limit: u32,
        #[arg(long, default_value_t=crate::domain::actors::DEFAULT_TASK_CAPACITY,value_parser=clap::value_parser!(u32).range(1..))]
        task_capacity: u32,
    },
}
#[derive(Subcommand)]
enum SearchCommand {
    /// Explore distinct next actions with bounded feedback rollouts.
    Start {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        config_file: PathBuf,
    },
    Status {
        search_id: SearchId,
    },
    Result {
        search_id: SearchId,
    },
}
#[derive(Subcommand)]
enum CheckpointCommand {
    /// Request a frozen checkpoint at a paused candidate's next model boundary.
    Create {
        run_id: RunId,
        #[arg(long)]
        checkpoint_id: Option<CheckpointId>,
    },
    Status {
        checkpoint_id: CheckpointId,
    },
}
#[derive(Subcommand)]
enum ForkCommand {
    /// Plan independent candidate continuations under the original task budget.
    Create {
        checkpoint_id: CheckpointId,
        #[arg(long, default_value_t=2, value_parser=clap::value_parser!(u32).range(1..=15))]
        candidates: u32,
        #[arg(long)]
        fork_id: Option<ForkId>,
    },
    Status {
        fork_id: ForkId,
    },
}
#[derive(Subcommand)]
enum RunCommand {
    /// Resolve an interrupted operation after inspecting its effects.
    Reconcile {
        run_id: RunId,
        /// JSON request with activity_id, attempt_id, decision, and evidence.
        #[arg(long)]
        decision_file: PathBuf,
    },
    Pause {
        run_id: RunId,
    },
    Resume {
        run_id: RunId,
    },
    Cancel {
        run_id: RunId,
    },
    Approve {
        run_id: RunId,
        wait_id: WaitId,
        #[arg(long)]
        deny: bool,
    },
    Signal {
        run_id: RunId,
        wait_id: WaitId,
        #[arg(long)]
        input_file: PathBuf,
    },
    Start {
        #[arg(default_value = "demo")]
        workflow: String,
        #[arg(long)]
        input_file: PathBuf,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..))]
        attempt_limit: u32,
        #[arg(long, default_value_t = 1)]
        workflow_version: u32,
    },
    Status {
        run_id: RunId,
    },
    History {
        run_id: RunId,
    },
}
#[derive(Subcommand)]
enum WorkerCommand {
    Run {
        /// Exit after all active runs have completed or stopped; honor pending retry deadlines.
        #[arg(long)]
        until_idle: bool,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
        poll_ms: u64,
        #[arg(long, default_value_t = 60_000, value_parser = clap::value_parser!(u64).range(1..))]
        /// Base activity timeout; HTTP model requests allow their pinned timeout plus a completion margin.
        activity_timeout_ms: u64,
        #[arg(long, default_value_t = 5_000)]
        shutdown_grace_ms: u64,
        /// Maximum activities in flight across Runs; each actor still executes one task at a time.
        #[arg(long, default_value = "1")]
        max_concurrent_activities: std::num::NonZeroUsize,
    },
}
#[derive(Subcommand)]
enum AgentCommand {
    /// Copy a project into a managed workspace and start a durable agent.
    Start {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        config_file: PathBuf,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..))]
        attempt_limit: u32,
    },
}

#[derive(Subcommand)]
enum TaskCommand {
    /// Snapshot a project and create isolated candidates under one shared budget.
    Start {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        config_file: PathBuf,
    },
    Status {
        task_id: TaskId,
    },
    /// Show the selected candidate's recorded fixed-test results and patch.
    Result {
        task_id: TaskId,
    },
}
#[derive(Subcommand)]
enum BenchmarkCommand {
    /// Freeze feedback/final inputs and plan repeated trials for the suite's strategies.
    Start {
        #[arg(long)]
        suite_file: PathBuf,
        /// Override the suite agent, for example with the LM Studio configuration.
        #[arg(long)]
        agent_config_file: Option<PathBuf>,
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=20))]
        repetitions: Option<u32>,
        /// Run one named case from the suite.
        #[arg(long)]
        case: Option<String>,
    },
    Status {
        benchmark_id: BenchmarkId,
    },
    /// Render the persisted comparison as Markdown, or JSON with --json.
    Report {
        benchmark_id: BenchmarkId,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

impl Cli {
    pub fn database(&self) -> &Path {
        self.db.as_deref().unwrap_or_else(|| {
            if matches!(self.command, TopCommand::Chat(_)) {
                Path::new(".act0r/chat.db")
            } else {
                Path::new("act0r.db")
            }
        })
    }
}

pub async fn execute(cli: Cli, engine: Engine, agents: AgentService) -> Result<(), Error> {
    let database = cli.database().to_path_buf();
    match cli.command {
        TopCommand::Chat(options) => {
            chat::execute(options, engine, &agents, &database, cli.json).await?
        }
        TopCommand::Storage { command } => {
            let value = match command {
                StorageCommand::Report => serde_json::to_value(agents.storage()?.report().await?)?,
                StorageCommand::Clean {
                    session,
                    agent,
                    fingerprint,
                } => serde_json::to_value(
                    agents
                        .storage()?
                        .clean(crate::domain::storage::CacheCleanupRequest {
                            session_id: session,
                            agent_id: agent,
                            fingerprint,
                        })
                        .await?,
                )?,
            };
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        TopCommand::Api { command } => api::execute(command, engine, agents, cli.json).await?,
        TopCommand::Team { command } => teams::execute(command, &agents, cli.json).await?,
        TopCommand::Actor { command } => {
            let actors = agents.actors()?;
            match command {
                ActorCommand::Tool {
                    command: ActorToolCommand::List,
                } => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&agents.tool_definitions()?)?
                    );
                }
                ActorCommand::Reminder { command } => {
                    let output = match command {
                        ActorReminderCommand::Schedule {
                            agent_id,
                            input_file,
                            at_unix_ms,
                            after_ms,
                            message_id,
                            sender,
                            correlation_id,
                        } => {
                            let deadline = match (at_unix_ms, after_ms) {
                                (Some(unix_ms), None) => {
                                    crate::domain::TimerDeadline::At { unix_ms }
                                }
                                (None, Some(milliseconds)) => {
                                    crate::domain::TimerDeadline::After { milliseconds }
                                }
                                _ => {
                                    return Err(crate::domain::DomainError::InvalidConfiguration(
                                        "choose one reminder deadline".into(),
                                    )
                                    .into());
                                }
                            };
                            serde_json::to_value(
                                actors
                                    .schedule_task(
                                        crate::domain::actors::AgentMessage {
                                            id: message_id.unwrap_or_default(),
                                            recipient: agent_id,
                                            sender,
                                            correlation_id,
                                            kind: "task".into(),
                                            payload: serde_json::from_slice(&std::fs::read(
                                                input_file,
                                            )?)?,
                                        },
                                        deadline,
                                    )
                                    .await?,
                            )?
                        }
                        ActorReminderCommand::Status { message_id } => {
                            serde_json::to_value(actors.reminder(message_id).await?)?
                        }
                        ActorReminderCommand::List { agent_id } => {
                            serde_json::to_value(actors.reminders(agent_id).await?)?
                        }
                        ActorReminderCommand::Cancel { message_id } => {
                            serde_json::to_value(actors.cancel_reminder(message_id).await?)?
                        }
                    };
                    println!("{}", serde_json::to_string_pretty(&output)?);
                }
                ActorCommand::Supervision { command } => {
                    let agent_id = match command {
                        ActorSupervisionCommand::Status { agent_id } => agent_id,
                        ActorSupervisionCommand::Recover { agent_id } => {
                            actors.recover_supervision(agent_id).await?;
                            agent_id
                        }
                    };
                    let actor = actors.status(agent_id).await?;
                    println!(
                        "{}",
                        serde_json::to_string_pretty(
                            &json!({"agent_id":agent_id,"status":actor.record().status,"supervision":actor.record().supervision})
                        )?
                    );
                }
                ActorCommand::Memory { command } => {
                    let agent_id = match command {
                        ActorMemoryCommand::Status { agent_id } => agent_id,
                        ActorMemoryCommand::Clear { agent_id } => {
                            actors.clear_memory(agent_id).await?;
                            agent_id
                        }
                    };
                    let memory = actors.memory(agent_id).await?;
                    let retained_bytes = memory.as_ref().map(|m| m.retained_bytes());
                    println!(
                        "{}",
                        serde_json::to_string_pretty(
                            &json!({"agent_id":agent_id,"memory":memory,"retained_bytes":retained_bytes})
                        )?
                    );
                }
                ActorCommand::Agent { command } => match command {
                    ActorAgentCommand::List => println!(
                        "{}",
                        serde_json::to_string_pretty(&agents.registrations()?)?
                    ),
                    ActorAgentCommand::Create {
                        adapter,
                        adapter_version,
                        config_file,
                        source,
                        attempt_limit,
                        task_capacity,
                    } => {
                        let config = serde_json::from_slice(&std::fs::read(config_file)?)?;
                        let adapter = match adapter_version {
                            Some(version) => DefinitionId::new(adapter, version),
                            None => agents.latest_adapter(&adapter)?,
                        };
                        let id = agents
                            .create_registered_actor(
                                adapter,
                                source,
                                config,
                                attempt_limit,
                                crate::domain::actors::AgentQueuePolicy { task_capacity },
                            )
                            .await?;
                        if cli.json {
                            println!("{}", json!({"agent_id":id}));
                        } else {
                            println!("{id}");
                        }
                    }
                },
                ActorCommand::Task { command } => {
                    if let ActorTaskCommand::Changes { message_id, output } = command {
                        let view = actors.task_changes(message_id).await?;
                        if let Some(path) = output {
                            let changes = view.changes.as_ref().ok_or_else(|| {
                                crate::Error::Unavailable(
                                    "this task has no committed change set to export".into(),
                                )
                            })?;
                            let parent = path
                                .parent()
                                .filter(|p| !p.as_os_str().is_empty())
                                .unwrap_or_else(|| std::path::Path::new("."));
                            use std::io::Write;
                            let mut file = tempfile::NamedTempFile::new_in(parent)?;
                            file.write_all(changes.patch().as_bytes())?;
                            file.as_file().sync_all()?;
                            file.persist_noclobber(&path)
                                .map_err(|e| crate::Error::Io(e.error))?;
                        }
                        if cli.json {
                            println!("{}", serde_json::to_string_pretty(&view)?);
                        } else if let Some(changes) = &view.changes {
                            println!(
                                "Task {}: {} changed files, {} changed bytes; tests: {:?}",
                                view.task_id,
                                changes.files.len(),
                                changes.changed_bytes,
                                changes.test_status
                            );
                            if let Some(report) = &view.completion {
                                println!(
                                    "Run: {:?}; completion: {:?}; reason: {:?}; captured workspace: {}",
                                    view.run_status,
                                    report.status,
                                    report.reason,
                                    report.workspace_digest
                                );
                                if let Some(error) = &view.error {
                                    println!("Stopped: {error}");
                                }
                            }
                            print!("{}", changes.patch());
                        } else {
                            println!("{}", serde_json::to_string_pretty(&view)?);
                        }
                        return Ok(());
                    }
                    if let ActorTaskCommand::Inspect {
                        message_id,
                        max_nodes,
                        max_depth,
                    } = command
                    {
                        let view = actors
                            .inspect_task(
                                message_id,
                                crate::application::TaskInspectionOptions {
                                    max_nodes,
                                    max_depth,
                                },
                            )
                            .await?;
                        if cli.json {
                            println!("{}", serde_json::to_string_pretty(&view)?)
                        } else {
                            print!("{}", task_inspection::render(&view))
                        }
                        return Ok(());
                    }
                    let output = match command {
                        ActorTaskCommand::Changes { .. } => unreachable!(),
                        ActorTaskCommand::Inspect { .. } => unreachable!(),
                        ActorTaskCommand::PreviewChanges { message_id, target } => {
                            let target = if target.is_absolute() {
                                target
                            } else {
                                std::env::current_dir()?.join(target)
                            };
                            let id = actors
                                .patch_applications()?
                                .preview(message_id, target)
                                .await?;
                            if cli.json {
                                println!("{}", json!({"run_id":id}));
                            } else {
                                println!("{id}");
                            }
                            return Ok(());
                        }
                        ActorTaskCommand::Application { run_id } => serde_json::to_value(
                            actors.patch_applications()?.status(run_id).await?,
                        )?,
                        ActorTaskCommand::ApplyChanges { run_id } => {
                            let applications = actors.patch_applications()?;
                            applications.approve(run_id, true).await?;
                            serde_json::to_value(applications.status(run_id).await?)?
                        }
                        ActorTaskCommand::RejectChanges { run_id } => {
                            let applications = actors.patch_applications()?;
                            applications.approve(run_id, false).await?;
                            serde_json::to_value(applications.status(run_id).await?)?
                        }
                        ActorTaskCommand::CancelGoal { message_id } => {
                            serde_json::to_value(actors.cancel_goal(message_id).await?)?
                        }
                        ActorTaskCommand::PauseGoal { message_id } => {
                            serde_json::to_value(actors.pause_goal(message_id).await?)?
                        }
                        ActorTaskCommand::ResumeGoal { message_id } => {
                            serde_json::to_value(actors.resume_goal(message_id).await?)?
                        }
                        ActorTaskCommand::Status { message_id } => {
                            serde_json::to_value(actors.task(message_id).await?)?
                        }
                        ActorTaskCommand::List { agent_id } => {
                            serde_json::to_value(actors.tasks(agent_id).await?)?
                        }
                        ActorTaskCommand::Cancel { message_id } => {
                            serde_json::to_value(actors.cancel_task(message_id).await?)?
                        }
                    };
                    println!("{}", serde_json::to_string_pretty(&output)?);
                }
                ActorCommand::Create {
                    definition,
                    definition_version,
                    config_file,
                    agent_id,
                    task_capacity,
                    max_consecutive_failures,
                } => {
                    let config = serde_json::from_slice(&std::fs::read(config_file)?)?;
                    let id = actors
                        .create_with_policies(
                            agent_id.unwrap_or_default(),
                            DefinitionId::new(definition, definition_version),
                            config,
                            crate::domain::actors::AgentQueuePolicy { task_capacity },
                            max_consecutive_failures.map(|max_consecutive_failures| {
                                crate::domain::actors::SupervisionPolicy {
                                    max_consecutive_failures,
                                }
                            }),
                        )
                        .await?;
                    if cli.json {
                        println!("{}", json!({"agent_id":id}));
                    } else {
                        println!("{id}");
                    }
                }
                ActorCommand::CodingWorker {
                    source,
                    config_file,
                    attempt_limit,
                    task_capacity,
                } => {
                    let config = serde_json::from_slice(&std::fs::read(config_file)?)?;
                    let id = agents
                        .create_registered_actor(
                            DefinitionId::new("coding", 1),
                            Some(source),
                            config,
                            attempt_limit,
                            crate::domain::actors::AgentQueuePolicy { task_capacity },
                        )
                        .await?;
                    if cli.json {
                        println!("{}", json!({"agent_id":id}));
                    } else {
                        println!("{id}");
                    }
                }
                ActorCommand::Send {
                    agent_id,
                    kind,
                    input_file,
                    sender,
                    message_id,
                    correlation_id,
                } => {
                    let payload = serde_json::from_slice(&std::fs::read(input_file)?)?;
                    let id = actors
                        .send(crate::domain::actors::AgentMessage {
                            id: message_id.unwrap_or_default(),
                            recipient: agent_id,
                            sender,
                            correlation_id,
                            kind,
                            payload,
                        })
                        .await?;
                    if cli.json {
                        println!("{}", json!({"message_id":id}));
                    } else {
                        println!("{id}");
                    }
                }
                ActorCommand::Status { agent_id } => {
                    let actor = actors.status(agent_id).await?;
                    let runs = actors.runs(agent_id).await?;
                    let queue = actors.queue(agent_id).await?;
                    println!(
                        "{}",
                        serde_json::to_string_pretty(
                            &json!({"agent":actor.record(),"runs":runs,"queue":queue})
                        )?
                    );
                }
                ActorCommand::Inbox { agent_id } => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&actors.mailbox(agent_id).await?)?
                    );
                }
                ActorCommand::Pause { agent_id } => {
                    actors.pause(agent_id).await?;
                }
                ActorCommand::Resume { agent_id } => {
                    actors.resume(agent_id).await?;
                }
            }
        }
        TopCommand::Search { command } => {
            match command {
                SearchCommand::Start {
                    source,
                    config_file,
                } => {
                    let config: SearchConfig =
                        serde_json::from_slice(&std::fs::read(&config_file)?)?;
                    let id = agents
                        .search()?
                        .start(
                            source,
                            config,
                            config_file.parent().unwrap_or(Path::new(".")),
                        )
                        .await?;
                    if cli.json {
                        println!("{}", json!({"search_id":id}));
                    } else {
                        println!("{id}");
                    }
                }
                SearchCommand::Status { search_id } => {
                    let search = agents.search()?.status(search_id).await?;
                    let task = agents.search()?.task(search_id).await?;
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(
                                &json!({"search":search.record(),"task":task.record()})
                            )?
                        );
                    } else {
                        let r = search.record();
                        println!(
                            "Search: {}\nTask: {}\nPhase: {:?}\nStrategy: {} v{}\nCheckpoint: {}\nModel calls: {}/{}\nAttempts: {}/{}\nVerified: {}",
                            r.id,
                            r.task_id,
                            r.phase,
                            r.config.strategy.name,
                            r.config.strategy.version,
                            r.checkpoint_id,
                            task.record().usage.admitted_model_calls,
                            task.record().config.budget.max_model_calls,
                            task.record().attempts,
                            task.record().config.budget.max_attempts,
                            r.verified
                        );
                        println!("Search version: {}", r.config.version);
                        for n in &r.nodes {
                            println!(
                                "Branch: {} action={} visits={} reward={} result={}",
                                n.run_id,
                                serde_json::to_string(&n.action)?,
                                n.visits,
                                n.reward,
                                serde_json::to_string(&n.result)?
                            );
                        }
                        if let Some(selected) = r.selected {
                            println!("Selected: {selected}");
                        }
                        if let Some(error) = &r.error {
                            println!("Error: {error}");
                        }
                    }
                }
                SearchCommand::Result { search_id } => {
                    let search = agents.search()?.status(search_id).await?;
                    if !search.record().verified {
                        return Err(Error::Unavailable("search has no verified result; inspect search status and candidate runs".into()));
                    }
                    let task = agents.search()?.task(search_id).await?;
                    let candidate = task
                        .record()
                        .candidates
                        .iter()
                        .find(|c| Some(c.run_id) == task.record().selected)
                        .ok_or_else(|| {
                            Error::Unavailable("selected task result is unavailable".into())
                        })?;
                    if cli.json {
                        println!("{}", serde_json::to_string_pretty(candidate)?);
                    } else {
                        println!(
                            "Selected: {}\nWorkspace: {}",
                            candidate.run_id, candidate.workspace
                        );
                        for file in &candidate
                            .evaluation
                            .as_ref()
                            .ok_or(DomainError::InvalidState)?
                            .diff
                        {
                            print!("{}", file.patch);
                        }
                    }
                }
            }
        }
        TopCommand::Checkpoint { command } => match command {
            CheckpointCommand::Create {
                run_id,
                checkpoint_id,
            } => {
                let checkpoint = agents
                    .branches()?
                    .create_checkpoint(checkpoint_id.unwrap_or_default(), run_id)
                    .await?;
                if cli.json {
                    println!(
                        "{}",
                        json!({"checkpoint_id":checkpoint.record().id,"status":checkpoint.record().status})
                    );
                } else {
                    println!("{}", checkpoint.record().id);
                }
            }
            CheckpointCommand::Status { checkpoint_id } => {
                let checkpoint = agents.branches()?.checkpoint(checkpoint_id).await?;
                let record = checkpoint.record();
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(record)?);
                } else {
                    println!(
                        "Checkpoint: {}\nTask: {}\nParent run: {}\nStatus: {:?}\nNext model turn: {}\nInherited attempts: {}\nInherited model calls: {}\nSnapshot: {}",
                        record.id,
                        record.task_id,
                        record.seed.origin.parent_run_id,
                        record.status,
                        record.seed.agent.turn,
                        record.seed.origin.attempts,
                        record.seed.origin.usage.admitted_model_calls,
                        record.snapshot.as_ref().map_or("pending", |s| &s.digest)
                    );
                    if let Some(error) = &record.error {
                        println!("Diagnostic: {error}");
                    }
                }
            }
        },
        TopCommand::Fork { command } => match command {
            ForkCommand::Create {
                checkpoint_id,
                candidates,
                fork_id,
            } => {
                let plan = agents
                    .branches()?
                    .create_fork(fork_id.unwrap_or_default(), checkpoint_id, candidates)
                    .await?;
                if cli.json {
                    println!(
                        "{}",
                        json!({"fork_id":plan.id,"status":plan.status,"run_ids":plan.run_ids})
                    );
                } else {
                    println!("{}", plan.id);
                }
            }
            ForkCommand::Status { fork_id } => {
                let plan = agents.branches()?.fork(fork_id).await?;
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(&plan)?);
                } else {
                    println!(
                        "Fork: {}\nTask: {}\nCheckpoint: {}\nStatus: {:?}",
                        plan.id, plan.task_id, plan.checkpoint_id, plan.status
                    );
                    for id in &plan.run_ids {
                        println!("Child run: {id}");
                    }
                    if let Some(error) = &plan.error {
                        println!("Diagnostic: {error}");
                    }
                }
            }
        },
        TopCommand::Benchmark { command } => match command {
            BenchmarkCommand::Start {
                suite_file,
                agent_config_file,
                repetitions,
                case,
            } => {
                let mut config: BenchmarkConfig =
                    serde_json::from_slice(&std::fs::read(&suite_file)?)?;
                if let Some(path) = agent_config_file {
                    config.agent = serde_json::from_slice(&std::fs::read(path)?)?;
                }
                if let Some(repetitions) = repetitions {
                    config.repetitions = repetitions;
                }
                if let Some(case) = case {
                    config.cases.retain(|c| c.name == case);
                    if config.cases.is_empty() {
                        return Err(Error::Unavailable(format!(
                            "benchmark case {case} was not found"
                        )));
                    }
                }
                let base = suite_file
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
                let id = agents.benchmarks()?.start(config, base.into()).await?;
                if cli.json {
                    println!("{}", json!({"benchmark_id":id}));
                } else {
                    println!("{id}");
                }
            }
            BenchmarkCommand::Status { benchmark_id } => {
                let job = agents.benchmarks()?.status(benchmark_id).await?;
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(job.record())?);
                } else {
                    println!(
                        "Benchmark: {}\nSuite: {}\nStatus: {:?}",
                        job.record().id,
                        job.record().config.name,
                        job.record().status
                    );
                    for trial in &job.record().trials {
                        println!(
                            "Trial {}: {} repetition={} strategy={} {:?} task={} search={}",
                            trial.ordinal + 1,
                            job.record().config.cases[trial.case_index as usize].name,
                            trial.repetition + 1,
                            trial.strategy.label(trial.candidates),
                            trial.status,
                            trial.task_id,
                            trial
                                .search_id
                                .map_or_else(|| "none".into(), |id| id.to_string())
                        );
                    }
                    for arm in &job.record().summary.arms {
                        println!(
                            "{}: {}/{} completed, {} successes, {} calls, {} charged tokens",
                            arm.strategy.label(arm.candidates),
                            arm.completed,
                            arm.trials,
                            arm.successes,
                            arm.model_calls,
                            arm.charged_tokens
                        );
                    }
                }
            }
            BenchmarkCommand::Report {
                benchmark_id,
                output,
            } => {
                let job = agents.benchmarks()?.status(benchmark_id).await?;
                let rendered = if cli.json {
                    serde_json::to_string_pretty(job.record())?
                } else {
                    super::benchmark_report::markdown(job.record())
                };
                if let Some(path) = output {
                    let parent = path
                        .parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or(Path::new("."));
                    std::fs::create_dir_all(parent)?;
                    super::workspace::atomic_write(&path, rendered.as_bytes())?;
                    if cli.json {
                        println!("{}", json!({"benchmark_id":benchmark_id,"output":path}));
                    } else {
                        println!("Saved report: {}", path.display());
                    }
                } else {
                    println!("{rendered}");
                }
            }
        },
        TopCommand::Task { command } => match command {
            TaskCommand::Start {
                source,
                config_file,
            } => {
                let config: TaskConfig = serde_json::from_slice(&std::fs::read(config_file)?)?;
                let id = agents.tasks()?.start(source, config).await?;
                if cli.json {
                    println!("{}", json!({"task_id":id}));
                } else {
                    println!("{id}");
                }
            }
            TaskCommand::Status { task_id } => {
                let task = agents.tasks()?.status(task_id).await?;
                let record = task.record();
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(record)?);
                } else {
                    println!(
                        "Task: {}\nStatus: {:?}\nSnapshot: {}\nAttempts: {}/{}\nModel calls: {}/{}\nTokens charged: {}/{}",
                        record.id,
                        record.status,
                        record.snapshot.digest,
                        record.attempts,
                        record.config.budget.max_attempts,
                        record.usage.admitted_model_calls,
                        record.config.budget.max_model_calls,
                        record.usage.charged_tokens(),
                        record.config.budget.max_tokens
                    );
                    for c in &record.candidates {
                        println!(
                            "Candidate {}: {} {} verified={}\n  Workspace: {}",
                            c.ordinal + 1,
                            c.run_id,
                            c.status.as_str(),
                            c.evaluation.as_ref().is_some_and(|e| e.passed),
                            c.workspace
                        );
                        if let Some(origin) = &c.origin {
                            println!(
                                "  Checkpoint: {} parent={} inherited_attempts={} inherited_model_calls={}",
                                origin.checkpoint_id,
                                origin.parent_run_id,
                                origin.attempts,
                                origin.usage.admitted_model_calls
                            );
                        }
                        if let Some(e) = &c.error {
                            println!("  Error: {}: {}", e.code, e.message);
                        }
                        if let Some(e) = c.evaluation.as_ref().and_then(|e| e.error.as_ref()) {
                            println!("  Evaluation: {e}");
                        }
                    }
                    for fork in &record.forks {
                        println!(
                            "Fork: {} {} checkpoint={} children={}",
                            fork.id,
                            fork.status.as_str(),
                            fork.checkpoint_id,
                            fork.run_ids.len()
                        );
                        if let Some(error) = &fork.error {
                            println!("  Error: {error}");
                        }
                    }
                    if let Some(selected) = record.selected {
                        println!("Selected: {selected}");
                    }
                }
            }
            TaskCommand::Result { task_id } => {
                let task = agents.tasks()?.status(task_id).await?;
                let record = task.record();
                let candidate = record.selected.and_then(|id| record.candidates.iter().find(|c| c.run_id == id))
                    .ok_or_else(|| Error::Unavailable(format!("task {:?} has no selected verified solution; inspect task status and candidate runs", record.status)))?;
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(candidate)?);
                } else {
                    println!(
                        "Selected: {}\nWorkspace: {}",
                        candidate.run_id, candidate.workspace
                    );
                    if let Some(evaluation) = &candidate.evaluation {
                        println!("Fixed tests: {}", evaluation.tests);
                        for file in &evaluation.diff {
                            print!("{}", file.patch);
                        }
                    }
                }
            }
        },
        TopCommand::Agent {
            command:
                AgentCommand::Start {
                    source,
                    config_file,
                    attempt_limit,
                },
        } => {
            let config: AgentConfig = serde_json::from_slice(&std::fs::read(config_file)?)?;
            let id = agents.start(source, config, attempt_limit).await?;
            if cli.json {
                println!("{}", json!({"run_id":id}));
            } else {
                println!("{id}");
            }
        }
        TopCommand::Run { command } => match command {
            RunCommand::Reconcile {
                run_id,
                decision_file,
            } => {
                let request: ReconciliationRequest =
                    serde_json::from_slice(&std::fs::read(decision_file)?)?;
                engine.reconcile(run_id, request).await?;
                print_control(cli.json, run_id, "reconciliation_recorded");
            }
            RunCommand::Pause { run_id } => {
                engine.pause(run_id).await?;
                print_control(cli.json, run_id, "pause_requested");
            }
            RunCommand::Resume { run_id } => {
                engine.resume(run_id).await?;
                print_control(cli.json, run_id, "resumed");
            }
            RunCommand::Cancel { run_id } => {
                engine.cancel(run_id).await?;
                print_control(cli.json, run_id, "cancel_requested");
            }
            RunCommand::Approve {
                run_id,
                wait_id,
                deny,
            } => {
                engine
                    .signal(run_id, wait_id, json!({"approved":!deny}))
                    .await?;
                print_control(cli.json, run_id, "approval_recorded");
            }
            RunCommand::Signal {
                run_id,
                wait_id,
                input_file,
            } => {
                engine
                    .signal(
                        run_id,
                        wait_id,
                        serde_json::from_slice(&std::fs::read(input_file)?)?,
                    )
                    .await?;
                print_control(cli.json, run_id, "input_recorded");
            }
            RunCommand::Start {
                workflow,
                input_file,
                attempt_limit,
                workflow_version,
            } => {
                let input = serde_json::from_slice(&std::fs::read(input_file)?)?;
                let id = engine
                    .start(
                        DefinitionId::new(workflow, workflow_version),
                        input,
                        attempt_limit,
                    )
                    .await?;
                if cli.json {
                    println!("{}", json!({"run_id": id}));
                } else {
                    println!("{id}");
                }
            }
            RunCommand::Status { run_id } => {
                let run = engine.status(run_id).await?;
                let shared_budgets = engine.shared_budgets(run_id).await?;
                if cli.json {
                    let mut output = serde_json::to_value(&run)?;
                    if !shared_budgets.is_empty() {
                        output["shared_budgets"] = serde_json::to_value(&shared_budgets)?;
                    }
                    println!("{}", serde_json::to_string_pretty(&output)?);
                } else {
                    let record = run.record();
                    println!(
                        "Run: {}\nWorkflow: {} v{}\nStatus: {}\nAttempts: {}/{}",
                        record.id,
                        record.workflow.name,
                        record.workflow.version,
                        record.status.as_str(),
                        record.allowance.admitted,
                        record.allowance.limit
                    );
                    if let Some(result) = &record.result {
                        println!("Result: {result}");
                    }
                    let context_path = match record.workflow.name.as_str() {
                        "agent" => Some("/context/selection"),
                        "coding.candidate" => Some("/agent/context/selection"),
                        "registered_agent.coding" => Some("/state/context/selection"),
                        "native_tool_agent" | "planner_agent" | "answer_agent" => {
                            Some("/context/selection")
                        }
                        "registered_agent.tool"
                        | "registered_agent.planner"
                        | "registered_agent.answer" => Some("/state/context/selection"),
                        _ => None,
                    };
                    if let Some(value) = context_path
                        .and_then(|path| record.state.pointer(path))
                        .filter(|value| !value.is_null())
                    {
                        let selection: ContextSelection = serde_json::from_value(value.clone())?;
                        println!(
                            "Context: {} / {} input bytes; {} of {} observations included, {} omitted (policy v{})",
                            selection.input_bytes,
                            selection.policy.max_input_bytes,
                            selection.included_observations,
                            selection.total_observations,
                            selection.omitted_observations,
                            selection.policy.version
                        );
                    }
                    if record.usage.budget.is_some() {
                        println!(
                            "Model usage: {} calls, {} charged tokens ({} input, {} output, {} held; {} uncertain calls)",
                            record.usage.admitted_model_calls,
                            record.usage.charged_tokens(),
                            record.usage.actual_input_tokens,
                            record.usage.actual_output_tokens,
                            record.usage.held_tokens,
                            record.usage.uncertain_calls
                        );
                    }
                    for scope in &shared_budgets {
                        let usage = &scope.record.usage;
                        let budget = usage.budget.as_ref().ok_or(DomainError::InvalidState)?;
                        println!(
                            "Shared {} budget {}: {}/{} model calls, {}/{} charged tokens ({} held; {} uncertain calls; {} Runs; {} calls and {} tokens remaining)",
                            scope.record.kind.as_str(),
                            scope.record.root_run_id,
                            usage.admitted_model_calls,
                            budget
                                .max_model_calls
                                .map_or_else(|| "unlimited".to_string(), |n| n.to_string()),
                            usage.charged_tokens(),
                            budget
                                .max_tokens
                                .map_or_else(|| "unlimited".to_string(), |n| n.to_string()),
                            usage.held_tokens,
                            usage.uncertain_calls,
                            scope.members.len(),
                            scope
                                .remaining_model_calls
                                .map_or_else(|| "unlimited".to_string(), |n| n.to_string()),
                            scope
                                .remaining_tokens
                                .map_or_else(|| "unlimited".to_string(), |n| n.to_string())
                        );
                        if let Some(exhaustion) = &scope.record.exhaustion {
                            println!(
                                "Shared exhaustion: {} (Run {}, UTC Unix ms {})",
                                exhaustion.reason, exhaustion.run_id, exhaustion.recorded_at
                            );
                        }
                    }
                    if let Some(wait) = &record.wait {
                        println!(
                            "Wait: {} {}\n{}\nOperation: {}",
                            wait.id, wait.key, wait.prompt, wait.payload
                        );
                    }
                    if let Some(delegation) = run.pending_delegation() {
                        println!(
                            "Delegated task: {}\nActor: {}\nGoal: {}",
                            delegation.id, delegation.recipient, delegation.goal
                        );
                        if let Some(deadline) = delegation.deadline_at {
                            println!("Delegation deadline (UTC Unix ms): {deadline}");
                        }
                        if let Some(at) = delegation.timed_out_at {
                            println!("Delegation timeout recorded (UTC Unix ms): {at}");
                        }
                    }
                    for group in &record.task_groups {
                        let terminal = group
                            .children
                            .iter()
                            .filter(|id| {
                                record
                                    .delegations
                                    .iter()
                                    .any(|d| &d.id == *id && d.outcome.is_some())
                            })
                            .count();
                        println!(
                            "Task group: {} {}/{} terminal; joined_at={:?}; deadline_at={:?}",
                            group.id,
                            terminal,
                            group.children.len(),
                            group.joined_at,
                            group.deadline_at
                        );
                        for id in &group.children {
                            println!("  Child task: {id}");
                        }
                    }
                    if let Some(error) = &record.error {
                        println!("Diagnostic: {}: {}", error.code, error.message);
                    }
                    for activity in run
                        .activities()
                        .iter()
                        .filter(|a| a.status == ActivityStatus::Queued && a.error.is_some())
                    {
                        println!(
                            "Retry: activity={} ready_at={} (UTC Unix ms)",
                            activity.id, activity.ready_at
                        );
                    }
                    for attempt in run.attempts() {
                        let Some(outcome) = &attempt.outcome else {
                            continue;
                        };
                        let diagnostics = match outcome {
                            crate::domain::Outcome::Failure(failure)
                            | crate::domain::Outcome::FailureWithUsage { failure, .. } => {
                                failure.request.clone()
                            }
                            _ => outcome
                                .result()
                                .and_then(|result| result.get("diagnostics"))
                                .and_then(|value| {
                                    serde_json::from_value::<crate::domain::RequestDiagnostics>(
                                        value.clone(),
                                    )
                                    .ok()
                                }),
                        };
                        if let Some(diagnostics) = diagnostics {
                            println!(
                                "Model request: attempt={} category={:?} elapsed_ms={} http_status={} request_id={}",
                                attempt.id,
                                diagnostics.category,
                                diagnostics.elapsed_ms,
                                diagnostics
                                    .http_status
                                    .map_or_else(|| "none".into(), |v| v.to_string()),
                                diagnostics.request_id.as_deref().unwrap_or("none")
                            );
                        }
                    }
                    for activity in run
                        .activities()
                        .iter()
                        .filter(|a| a.status == ActivityStatus::Indeterminate)
                    {
                        let attempt = run.attempts().iter().find(|t| {
                            t.activity_id == activity.id && t.ordinal == activity.attempts
                        });
                        println!(
                            "Uncertain activity: {}\nHandler: {} v{}\nInput: {}",
                            activity.id,
                            activity.spec.handler.name,
                            activity.spec.handler.version,
                            activity.spec.input
                        );
                        if let Some(attempt) = attempt {
                            println!(
                                "Interrupted attempt: {}\nOutcome: {}",
                                attempt.id,
                                serde_json::to_string(&attempt.outcome)?
                            );
                        }
                        if record.status == RunStatus::Blocked
                            && record
                                .error
                                .as_ref()
                                .is_some_and(|e| e.code == "uncertain_outcome")
                        {
                            println!(
                                "Resolve after inspection: act0r --db \"{}\" run reconcile {} --decision-file decision.json",
                                database.display(),
                                record.id
                            );
                        }
                    }
                    for timer in run.timers() {
                        println!(
                            "Timer: {} {} due_at={} (UTC Unix ms)",
                            timer.id,
                            timer.status.as_str(),
                            timer.due_at
                        );
                    }
                }
            }
            RunCommand::History { run_id } => {
                let history = engine.history(run_id).await?;
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(&history)?);
                } else {
                    for event in history {
                        println!(
                            "{:04} {} {} {}",
                            event.sequence, event.at, event.kind, event.payload
                        );
                    }
                }
            }
        },
        TopCommand::Worker {
            command:
                WorkerCommand::Run {
                    until_idle,
                    poll_ms,
                    activity_timeout_ms,
                    shutdown_grace_ms,
                    max_concurrent_activities,
                },
        } => {
            let worker = engine.worker().await?;
            tokio_worker::run(
                worker,
                WorkerOptions {
                    until_idle,
                    poll_interval: Duration::from_millis(poll_ms),
                    activity_timeout: Duration::from_millis(activity_timeout_ms),
                    shutdown_grace: Duration::from_millis(shutdown_grace_ms),
                    max_concurrent_activities: max_concurrent_activities.get(),
                },
            )
            .await?;
            if cli.json {
                println!("{}", json!({"worker": "stopped"}));
            }
        }
    }
    Ok(())
}
fn print_control(as_json: bool, id: RunId, action: &str) {
    if as_json {
        println!("{}", json!({"run_id":id,"action":action}));
    } else {
        println!("{id}: {action}");
    }
}
