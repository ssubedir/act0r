use super::{errors::*, *};
use crate::domain::{MessageId, SessionId, worker_cancellation::StopAssignmentRequest};
use axum::extract::rejection::JsonRejection;
use serde_json::{Value, json};

pub(super) async fn current(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn, task)): ApiPath<(SessionId, MessageId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .worker_cancellation(session, turn, task)
            .await?
    )))
}
pub(super) async fn stop(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn, task)): ApiPath<(SessionId, MessageId, MessageId)>,
    input: Result<Json<StopAssignmentRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .stop_assignment(session, turn, task, body(input)?)
                .await?
        )),
    ))
}
