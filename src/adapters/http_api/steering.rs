use super::{errors::*, *};
use crate::domain::{MessageId, SessionId, steering::SteeringRequest};
use axum::extract::rejection::JsonRejection;
use serde_json::{Value, json};

pub(super) async fn current(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn)): ApiPath<(SessionId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .turn_steering(session, turn)
            .await?
    )))
}
pub(super) async fn send(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn)): ApiPath<(SessionId, MessageId)>,
    input: Result<Json<SteeringRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .send_steering(session, turn, body(input)?)
                .await?
        )),
    ))
}
pub(super) async fn worker_current(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn, task)): ApiPath<(SessionId, MessageId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .worker_steering(session, turn, task)
            .await?
    )))
}
pub(super) async fn worker_send(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn, task)): ApiPath<(SessionId, MessageId, MessageId)>,
    input: Result<Json<SteeringRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .send_worker_steering(session, turn, task, body(input)?)
                .await?
        )),
    ))
}
