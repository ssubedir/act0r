use super::{errors::*, *};
use crate::domain::{MessageId, TeamId, goal::GoalPausePrecondition};
use axum::extract::rejection::JsonRejection;
use serde_json::{Value, json};

pub(super) async fn status(
    State(state): State<Arc<ApiState>>,
    ApiPath((team, task)): ApiPath<(TeamId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state.agents.teams()?.controls()?.status(team, task).await?
    )))
}
pub(super) async fn pause(
    State(state): State<Arc<ApiState>>,
    ApiPath((team, task)): ApiPath<(TeamId, MessageId)>,
    input: Result<Json<GoalPausePrecondition>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .controls()?
            .pause(team, task, body(input)?)
            .await?
    )))
}
pub(super) async fn resume(
    State(state): State<Arc<ApiState>>,
    ApiPath((team, task)): ApiPath<(TeamId, MessageId)>,
    input: Result<Json<GoalPausePrecondition>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .controls()?
            .resume(team, task, body(input)?)
            .await?
    )))
}
pub(super) async fn cancel(
    State(state): State<Arc<ApiState>>,
    ApiPath((team, task)): ApiPath<(TeamId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state.agents.teams()?.controls()?.cancel(team, task).await?
    )))
}
