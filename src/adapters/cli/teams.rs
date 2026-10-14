use crate::{
    application::{AgentService, Error},
    domain::{teams::*, *},
};
use clap::{Args, Subcommand};
use serde_json::json;
use std::{fs::File, io::Read, path::PathBuf};

#[derive(Subcommand)]
pub(super) enum TeamCommand {
    /// Validate and durably register one planner and its named native workers.
    Create {
        #[arg(long)]
        config_file: PathBuf,
    },
    /// Complete interrupted setup using its saved configuration and identities.
    Resume {
        team_id: TeamId,
    },
    /// Submit a goal to the team's planner; reuse message-id for an identical retry.
    Submit {
        team_id: TeamId,
        #[arg(long)]
        goal: String,
        #[arg(long)]
        message_id: Option<MessageId>,
    },
    Status {
        team_id: TeamId,
    },
    /// Show the newest 100 saved teams, marking truncation explicitly.
    List,
    /// Inspect a goal and resolve its durable questions or approvals.
    Task {
        #[command(subcommand)]
        command: TeamTaskCommand,
    },
}

#[derive(Args)]
pub(super) struct TeamTaskScope {
    team_id: TeamId,
    task_id: MessageId,
}
#[derive(Args)]
pub(super) struct TeamTaskRead {
    #[command(flatten)]
    scope: TeamTaskScope,
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u32).range(1..=512))]
    max_nodes: u32,
    #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u32).range(0..=128))]
    max_depth: u32,
}
impl TeamTaskRead {
    fn options(&self) -> crate::application::TaskInspectionOptions {
        crate::application::TaskInspectionOptions {
            max_nodes: self.max_nodes,
            max_depth: self.max_depth,
        }
    }
}
#[derive(Subcommand)]
pub(super) enum TeamTaskCommand {
    Status(TeamTaskRead),
    /// Read a bounded page of committed events across this goal's actors.
    Events {
        #[command(flatten)]
        scope: TeamTaskScope,
        #[command(flatten)]
        options: super::team_events::PageOptions,
        #[arg(long)]
        after: Option<crate::domain::timeline::EventCursor>,
    },
    /// Follow committed events until Ctrl+C; --json emits one compact page per line.
    Watch {
        #[command(flatten)]
        scope: TeamTaskScope,
        #[command(flatten)]
        options: super::team_events::PageOptions,
        #[arg(long, conflicts_with = "cursor_file")]
        after: Option<crate::domain::timeline::EventCursor>,
        /// Atomically save the cursor after flushing each output page; reuse to reconnect.
        #[arg(long)]
        cursor_file: Option<PathBuf>,
        #[arg(long, default_value_t = 250, value_parser = clap::value_parser!(u64).range(1..=60000))]
        poll_ms: u64,
    },
    /// Show exact saved requests, including payloads and stable wait IDs.
    Inbox(TeamTaskRead),
    Answer {
        #[command(flatten)]
        scope: TeamTaskScope,
        wait_id: WaitId,
        #[arg(
            long,
            required_unless_present = "answer_file",
            conflicts_with = "answer_file"
        )]
        answer: Option<String>,
        /// UTF-8 text file, at most 65536 bytes.
        #[arg(long)]
        answer_file: Option<PathBuf>,
    },
    /// Approve the exact saved operation; --deny records a refusal.
    Approve {
        #[command(flatten)]
        scope: TeamTaskScope,
        wait_id: WaitId,
        #[arg(long)]
        deny: bool,
    },
}
pub(super) async fn execute(
    command: TeamCommand,
    agents: &AgentService,
    json_output: bool,
) -> Result<(), Error> {
    let teams = agents.teams()?;
    match command {
        TeamCommand::Create { config_file } => {
            let config_file = std::fs::canonicalize(config_file)?;
            let mut bytes = Vec::new();
            File::open(&config_file)?
                .take(MAX_TEAM_SPEC_BYTES as u64 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > MAX_TEAM_SPEC_BYTES {
                return Err(
                    DomainError::InvalidConfiguration("team file exceeds 1 MiB".into()).into(),
                );
            }
            let mut spec: TeamSpec = serde_json::from_slice(&bytes)?;
            spec.validate()?;
            if let Some(source) = &mut spec.source {
                let selected = config_file
                    .parent()
                    .ok_or(DomainError::InvalidState)?
                    .join(&*source);
                *source = std::fs::canonicalize(selected)?
                    .to_string_lossy()
                    .into_owned();
            }
            let record = teams.create(spec).await?;
            if json_output {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&teams.status(record.id).await?)?
                );
            } else {
                println!("{}", record.id);
            }
        }
        TeamCommand::Resume { team_id } => {
            let record = teams.resume(team_id).await?;
            if json_output {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&teams.status(record.id).await?)?
                );
            } else {
                println!("{}", record.id);
            }
        }
        TeamCommand::Submit {
            team_id,
            goal,
            message_id,
        } => {
            let task_id = teams
                .submit(team_id, message_id.unwrap_or_default(), goal)
                .await?;
            if json_output {
                println!("{}", json!({"team_id":team_id,"task_id":task_id}));
            } else {
                println!("{task_id}");
            }
        }
        TeamCommand::Status { team_id } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&teams.status(team_id).await?)?
            );
        }
        TeamCommand::List => {
            println!("{}", serde_json::to_string_pretty(&teams.list().await?)?);
        }
        TeamCommand::Task { command } => {
            execute_task(command, teams, json_output).await?;
        }
    }
    Ok(())
}

async fn execute_task(
    command: TeamTaskCommand,
    teams: &crate::application::TeamService,
    json_output: bool,
) -> Result<(), Error> {
    match command {
        TeamTaskCommand::Events {
            scope,
            options,
            after,
        } => {
            let page = teams
                .events()?
                .read(scope.team_id, scope.task_id, options.query(after))
                .await?;
            super::team_events::print_page(&page, json_output)?;
        }
        TeamTaskCommand::Watch {
            scope,
            options,
            after,
            cursor_file,
            poll_ms,
        } => {
            super::team_events::watch(
                teams.events()?,
                scope.team_id,
                scope.task_id,
                options.query(after),
                cursor_file,
                poll_ms,
                json_output,
            )
            .await?;
        }
        TeamTaskCommand::Status(read) => {
            let view = teams
                .tasks()?
                .status(read.scope.team_id, read.scope.task_id, read.options())
                .await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&view)?);
            } else {
                println!("Team: {} ({})", view.team.name, view.team.id);
                for member in &view.members {
                    println!("Member: {} = {}", member.name, member.agent_id);
                }
                print!("{}", super::task_inspection::render(&view.task));
            }
        }
        TeamTaskCommand::Inbox(read) => {
            let view = teams
                .tasks()?
                .inbox(read.scope.team_id, read.scope.task_id, read.options())
                .await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&view)?);
            } else {
                println!(
                    "Team: {} ({})\nGoal: {}\nHuman decisions: {}",
                    view.team.name,
                    view.team.id,
                    view.root_task_id,
                    view.items.len()
                );
                if !view.truncations.is_empty() {
                    println!(
                        "PARTIAL INBOX: graph limits are {} nodes and depth {}; unseen waits may exist.",
                        view.limits.max_nodes, view.limits.max_depth
                    );
                }
                if view.goal_cancelled {
                    println!("Goal is cancelled; new decisions are unavailable.");
                }
                if view.goal_paused {
                    println!("Goal admission is paused; recorded replies do not resume it.");
                }
                for item in &view.items {
                    println!(
                        "\nWait: {}\nMember: {} ({})\nTask: {}\nRun: {}\nKind: {:?}\nActionable: {}{}\nPrompt: {}\nSaved request:\n{}",
                        item.wait.id,
                        item.member.as_deref().unwrap_or("unnamed"),
                        item.actor_id,
                        item.task_id,
                        item.run_id,
                        item.kind,
                        item.actionable,
                        item.unavailable_reason
                            .map_or_else(String::new, |r| format!(" ({r})")),
                        serde_json::to_string(&item.wait.prompt)?,
                        serde_json::to_string_pretty(&item.wait.payload)?
                    );
                }
            }
        }
        TeamTaskCommand::Answer {
            scope,
            wait_id,
            answer,
            answer_file,
        } => {
            let answer = match (answer, answer_file) {
                (Some(answer), None) => answer,
                (None, Some(path)) => {
                    let mut bytes = Vec::new();
                    File::open(path)?
                        .take(MAX_HUMAN_ANSWER_BYTES as u64 + 1)
                        .read_to_end(&mut bytes)?;
                    if bytes.len() > MAX_HUMAN_ANSWER_BYTES {
                        return Err(DomainError::InvalidConfiguration(
                            "answer file exceeds 65536 bytes".into(),
                        )
                        .into());
                    }
                    String::from_utf8(bytes).map_err(|_| {
                        DomainError::InvalidConfiguration("answer file must be UTF-8 text".into())
                    })?
                }
                _ => {
                    return Err(DomainError::InvalidConfiguration(
                        "select one answer or answer-file".into(),
                    )
                    .into());
                }
            };
            let receipt = teams
                .tasks()?
                .respond(
                    scope.team_id,
                    scope.task_id,
                    wait_id,
                    HumanResponse::Answer { answer },
                )
                .await?;
            print_receipt(&receipt, json_output)?;
        }
        TeamTaskCommand::Approve {
            scope,
            wait_id,
            deny,
        } => {
            let receipt = teams
                .tasks()?
                .respond(
                    scope.team_id,
                    scope.task_id,
                    wait_id,
                    HumanResponse::Approval { approved: !deny },
                )
                .await?;
            print_receipt(&receipt, json_output)?;
        }
    }
    Ok(())
}
fn print_receipt(
    receipt: &crate::application::TeamWaitReceipt,
    json_output: bool,
) -> Result<(), Error> {
    if json_output {
        println!("{}", serde_json::to_string_pretty(receipt)?);
    } else {
        println!(
            "{}: {} (task {})",
            if receipt.replayed {
                "Decision already recorded"
            } else {
                "Decision recorded"
            },
            receipt.wait_id,
            receipt.task_id
        );
    }
    Ok(())
}
