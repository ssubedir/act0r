use super::{errors::*, *};
use crate::domain::{AgentId, MessageId, SessionId, session_memory::ClearMemoryRequest};
use axum::extract::rejection::JsonRejection;
use serde_json::{Value, json};

pub(super) async fn current(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state.agents.teams()?.harness()?.memory(session).await?
    )))
}
pub(super) async fn turn(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn)): ApiPath<(SessionId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .turn_memory(session, turn)
            .await?
    )))
}
pub(super) async fn clear(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, agent)): ApiPath<(SessionId, AgentId)>,
    input: Result<Json<ClearMemoryRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .clear_memory(session, agent, body(input)?)
            .await?
    )))
}
