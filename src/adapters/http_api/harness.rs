use super::{errors::*, *};
use crate::{
    adapters::operator::{self, Request},
    domain::{
        SessionId,
        harness::{HarnessRequest, HarnessTurnRequest},
    },
};
use axum::extract::rejection::JsonRejection;
use serde_json::Value;
#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct ConversationQuery {
    before: Option<u64>,
}
#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct ContinuousQuery {
    before: Option<crate::domain::conversation::ConversationCursor>,
    task: Option<crate::domain::MessageId>,
}
pub(super) async fn continuous(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
    ApiQuery(query): ApiQuery<ContinuousQuery>,
) -> ApiResult<Json<Value>> {
    Ok(Json(serde_json::json!(
        state
            .agents
            .teams()?
            .harness()?
            .continuous_conversation(session, query.before, query.task)
            .await?
    )))
}
pub(super) async fn conversations(
    State(state): State<Arc<ApiState>>,
    ApiQuery(query): ApiQuery<crate::domain::sessions::SessionListQuery>,
) -> ApiResult<Json<Value>> {
    Ok(Json(serde_json::json!(
        state
            .agents
            .teams()?
            .harness()?
            .conversation_sessions(query)
            .await?
    )))
}
pub(super) async fn conversation(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
    ApiQuery(query): ApiQuery<ConversationQuery>,
) -> ApiResult<Json<Value>> {
    Ok(Json(
        operator::execute(
            &state.agents,
            Request::HarnessConversation {
                session,
                before: query.before,
            },
        )
        .await?,
    ))
}
pub(super) async fn append(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
    input: Result<Json<HarnessTurnRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(
            operator::execute(
                &state.agents,
                Request::HarnessTurn {
                    session,
                    request: body(input)?,
                },
            )
            .await?,
        ),
    ))
}
pub(super) async fn config(State(state): State<Arc<ApiState>>) -> ApiResult<Json<Value>> {
    Ok(Json(
        operator::execute(&state.agents, Request::HarnessConfig {}).await?,
    ))
}
pub(super) async fn sessions(
    State(state): State<Arc<ApiState>>,
    ApiQuery(query): ApiQuery<crate::domain::sessions::SessionListQuery>,
) -> ApiResult<Json<Value>> {
    Ok(Json(serde_json::json!(
        state
            .agents
            .teams()?
            .harness()?
            .sessions_page(query)
            .await?
    )))
}
pub(super) async fn metadata(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
) -> ApiResult<Json<Value>> {
    Ok(Json(serde_json::json!(
        state
            .agents
            .teams()?
            .harness()?
            .session_metadata(session)
            .await?
    )))
}
pub(super) async fn update_metadata(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
    input: Result<Json<crate::domain::sessions::SessionUpdate>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(serde_json::json!(
        state
            .agents
            .teams()?
            .harness()?
            .update_session(session, body(input)?)
            .await?
    )))
}
pub(super) async fn start(
    State(state): State<Arc<ApiState>>,
    input: Result<Json<HarnessRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(
            operator::execute(
                &state.agents,
                Request::HarnessStart {
                    request: body(input)?,
                },
            )
            .await?,
        ),
    ))
}
pub(super) async fn project_setup(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task)): ApiPath<(SessionId, crate::domain::MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(serde_json::json!(
        state
            .agents
            .teams()?
            .harness()?
            .project_setup(session, task)
            .await?
    )))
}
pub(super) async fn accept_project_setup(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task)): ApiPath<(SessionId, crate::domain::MessageId)>,
    input: Result<Json<crate::domain::project_setup::ReviewProjectSetup>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!(
            state
                .agents
                .teams()?
                .harness()?
                .accept_project_setup(session, task, body(input)?)
                .await?
        )),
    ))
}
