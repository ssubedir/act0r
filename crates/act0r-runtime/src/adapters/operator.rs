//! Typed operator requests behind the HTTP adapter. All decisions use application services.
use crate::{
    Error,
    application::{AgentService, TaskInspectionOptions},
    domain::{
        DomainError, HumanResponse, MessageId, SessionId, TeamId, WaitId,
        goal::GoalPausePrecondition, goal_list::GoalListQuery, teams::TeamSpec,
        timeline::EventQuery,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Teams {},
    HarnessConfig {},
    HarnessSessions {},
    HarnessStart {
        request: crate::domain::harness::HarnessRequest,
    },
    HarnessConversation {
        session: SessionId,
        before: Option<u64>,
    },
    HarnessTurn {
        session: SessionId,
        request: crate::domain::harness::HarnessTurnRequest,
    },
    CreateTeam {
        spec: TeamSpec,
    },
    Team {
        team: TeamId,
    },
    Goals {
        team: TeamId,
        query: GoalListQuery,
    },
    Submit {
        team: TeamId,
        message_id: MessageId,
        goal: String,
    },
    Task {
        team: TeamId,
        task: MessageId,
    },
    Inbox {
        team: TeamId,
        task: MessageId,
    },
    Controls {
        team: TeamId,
        task: MessageId,
    },
    Pause {
        team: TeamId,
        task: MessageId,
        expected_revision: u64,
    },
    Resume {
        team: TeamId,
        task: MessageId,
        expected_revision: u64,
    },
    Cancel {
        team: TeamId,
        task: MessageId,
    },
    Respond {
        team: TeamId,
        task: MessageId,
        wait: WaitId,
        response: HumanResponse,
    },
    Events {
        team: TeamId,
        task: MessageId,
        query: EventQuery,
    },
}

pub async fn execute(agents: &AgentService, request: Request) -> Result<Value, Error> {
    let teams = agents.teams()?;
    Ok(match request {
        Request::HarnessConfig {} => harness_config(),
        Request::HarnessSessions {} => json!(teams.harness()?.sessions().await?),
        Request::HarnessConversation { session, before } => {
            json!(teams.harness()?.conversation(session, before).await?)
        }
        Request::HarnessTurn { session, request } => {
            json!(teams.harness()?.append(session, request).await?)
        }
        Request::HarnessStart { request } => {
            let url = reqwest::Url::parse(&request.target.endpoint)
                .map_err(|_| DomainError::InvalidConfiguration("invalid model endpoint".into()))?;
            if !(url.scheme() == "https"
                || (url.scheme() == "http"
                    && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))))
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(DomainError::InvalidConfiguration("endpoint requires HTTPS or loopback HTTP, without credentials, query parameters or fragments".into()).into());
            }
            json!(teams.harness()?.start(request).await?)
        }
        Request::Teams {} => json!(teams.list().await?),
        Request::CreateTeam { spec } => {
            let team = teams.create(spec).await?;
            json!(teams.status(team.id).await?)
        }
        Request::Team { team } => json!(teams.status(team).await?),
        Request::Goals { team, query } => json!(teams.goals()?.list(team, query).await?),
        Request::Submit {
            team,
            message_id,
            goal,
        } => {
            let task = teams.submit(team, message_id, goal).await?;
            json!({"team_id":team,"task_id":task})
        }
        Request::Task { team, task } => json!(
            teams
                .tasks()?
                .status(team, task, TaskInspectionOptions::default())
                .await?
        ),
        Request::Inbox { team, task } => json!(
            teams
                .tasks()?
                .inbox(team, task, TaskInspectionOptions::default())
                .await?
        ),
        Request::Controls { team, task } => json!(teams.controls()?.status(team, task).await?),
        Request::Pause {
            team,
            task,
            expected_revision,
        } => json!(
            teams
                .controls()?
                .pause(team, task, GoalPausePrecondition { expected_revision })
                .await?
        ),
        Request::Resume {
            team,
            task,
            expected_revision,
        } => json!(
            teams
                .controls()?
                .resume(team, task, GoalPausePrecondition { expected_revision })
                .await?
        ),
        Request::Cancel { team, task } => json!(teams.controls()?.cancel(team, task).await?),
        Request::Respond {
            team,
            task,
            wait,
            response,
        } => json!(teams.tasks()?.respond(team, task, wait, response).await?),
        Request::Events { team, task, query } => {
            json!(teams.events()?.read(team, task, query).await?)
        }
    })
}

fn harness_config() -> Value {
    use crate::adapters::model::ModelRouter;
    json!({
        "target":harness_model_target(None, None),
        "api_key_available":ModelRouter::environment_key_available(),
        "workspace_access":"read_only",
          "budget_defaults":{"max_model_calls":null,"max_tokens":null},
        "coding_defaults": {
            "coder_count":1,
            "writable_paths":["src/"],
            "test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000}
        }
    })
}

/// Initial model defaults shared by the CLI and HTTP adapter; saved sessions retain their pins.
pub fn harness_model_target(model: Option<&str>, endpoint: Option<&str>) -> Value {
    let endpoint = endpoint.map(str::to_owned).unwrap_or_else(|| {
        std::env::var("ACT0R_MODEL_ENDPOINT")
            .unwrap_or_else(|_| "https://integrate.api.nvidia.com/v1/chat/completions".into())
    });
    let model = model.map(str::to_owned).unwrap_or_else(|| {
        std::env::var("ACT0R_MODEL").unwrap_or_else(|_| "moonshotai/kimi-k3".into())
    });
    harness_target(&model, &endpoint)
}

fn harness_target(model: &str, endpoint: &str) -> Value {
    let mut target = json!({
        "provider":"openai", "protocol":"chat_completions", "model":model, "endpoint":endpoint,
        "policy":{"request_timeout_ms":90000,"connect_timeout_ms":10000,"max_attempts":2,"retry_delay_ms":1000,"max_retry_delay_ms":60000}
    });
    if model == "moonshotai/kimi-k3"
        && reqwest::Url::parse(endpoint).is_ok_and(|url| {
            url.scheme() == "https"
                && url.host_str() == Some("integrate.api.nvidia.com")
                && url.port_or_known_default() == Some(443)
                && url.path().trim_end_matches('/') == "/v1/chat/completions"
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
        })
    {
        target["action_format"] = json!("function_calling");
    }
    target
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harness_default_functions_are_scoped_to_the_tested_nvidia_kimi_target() {
        let model = "moonshotai/kimi-k3";
        for endpoint in [
            "https://integrate.api.nvidia.com/v1/chat/completions",
            "https://integrate.api.nvidia.com:443/v1/chat/completions/",
        ] {
            let target = harness_target(model, endpoint);
            assert_eq!(target["action_format"], "function_calling");
            serde_json::from_value::<crate::domain::agent::ModelTarget>(target)
                .unwrap()
                .validate()
                .unwrap();
        }
        for (model, endpoint) in [
            (
                "other-model",
                "https://integrate.api.nvidia.com/v1/chat/completions",
            ),
            (model, "http://127.0.0.1:1234/v1/chat/completions"),
            (model, "https://custom.example/v1/chat/completions"),
            (
                model,
                "https://integrate.api.nvidia.com.example/v1/chat/completions",
            ),
            (
                model,
                "https://integrate.api.nvidia.com:8443/v1/chat/completions",
            ),
            (model, "https://integrate.api.nvidia.com/v1/responses"),
            (
                model,
                "https://user@integrate.api.nvidia.com/v1/chat/completions",
            ),
            (
                model,
                "https://integrate.api.nvidia.com/v1/chat/completions?extra=1",
            ),
            (model, "invalid"),
        ] {
            assert!(
                harness_target(model, endpoint)
                    .get("action_format")
                    .is_none()
            );
        }
    }
}

/// Operator refusal semantics for HTTP requests; ambiguous errors retain saved requests.
#[derive(Debug, Serialize)]
pub struct Problem {
    pub code: &'static str,
    pub message: String,
    pub status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}
impl Problem {
    pub fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            details: None,
        }
    }
    pub fn details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}
impl From<Error> for Problem {
    fn from(error: Error) -> Self {
        let (status,code)=match &error {
            Error::Domain(DomainError::SessionConflict(_)) => (409,"session_conflict"),
            Error::Domain(DomainError::ProjectProfileConflict(_)) => (409,"project_profile_conflict"),
            Error::ProjectProfileNotFound(_) => (404,"project_profile_not_found"),
            Error::StorageBusy(_) => (503,"storage_busy"),
            Error::StorageConflict(_) => (409,"storage_conflict"),
            Error::SessionNotFound(_)|Error::TeamNotFound(_)|Error::AgentTaskNotFound(_)|Error::AgentNotFound(_)|Error::NotFound(_)
                |Error::TaskNotFound(_)|Error::ActorReminderNotFound(_)|Error::BenchmarkNotFound(_)
                |Error::CheckpointNotFound(_)|Error::ForkNotFound(_)|Error::SearchNotFound(_) => (404,"not_found"),
            Error::QueueFull{agent_id,capacity,outstanding,requested} => return Self::new(429,"queue_full",error.to_string())
                .details(json!({"agent_id":agent_id,"capacity":capacity,"outstanding":outstanding,"requested":requested})),
            Error::Domain(DomainError::ConflictingMessage) => (409,"conflicting_message"),
            Error::Domain(DomainError::ConflictingOutcome) => (409,"conflicting_response"),
            Error::Domain(DomainError::GoalRevisionConflict{expected_revision,current_revision}) => return Self::new(409,"goal_revision_conflict",error.to_string())
                .details(json!({"expected_revision":expected_revision,"current_revision":current_revision})),
            Error::Domain(DomainError::InvalidState|DomainError::ReservedMessageId|DomainError::StaleAttempt|DomainError::ConflictingReconciliation)
                |Error::Ownership(_) => (409,"state_conflict"),
            Error::Domain(DomainError::UnknownWait) => (404,"unknown_wait"),
            Error::Domain(DomainError::UnknownAttempt|DomainError::UnknownTimer) => (404,"not_found"),
            Error::Domain(_)|Error::ToolRejected(_) => (400,"invalid_request"),
            Error::Unavailable(_) => (503,"unavailable"),
            Error::Storage(_)|Error::Io(_)|Error::Json(_) => {
                tracing::error!(%error,"operator service failed");
                return Self::new(500,"internal_error","Operator service failed; inspect the runtime log");
            }
        };
        Self::new(status, code, error.to_string())
    }
}
