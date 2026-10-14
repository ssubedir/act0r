use super::{errors::*, *};
use crate::domain::{MessageId, SessionId, turn_recovery::RetryTurnRequest};
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
            .turn_recovery(session, turn)
            .await?
    )))
}
pub(super) async fn retry(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn)): ApiPath<(SessionId, MessageId)>,
    input: Result<Json<RetryTurnRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .retry_turn(session, turn, body(input)?)
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
            .worker_recovery(session, turn, task)
            .await?
    )))
}
pub(super) async fn worker_decide(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn, task)): ApiPath<(SessionId, MessageId, MessageId)>,
    input: Result<Json<crate::domain::worker_recovery::WorkerRecoveryRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .decide_worker_recovery(session, turn, task, body(input)?)
                .await?
        )),
    ))
}
