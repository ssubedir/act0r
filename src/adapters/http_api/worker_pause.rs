use super::{errors::*, *};
use crate::domain::{MessageId, SessionId, worker_pause::AssignmentPauseRequest};
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
            .worker_pause(session, turn, task)
            .await?
    )))
}
pub(super) async fn control(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, turn, task)): ApiPath<(SessionId, MessageId, MessageId)>,
    input: Result<Json<AssignmentPauseRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .control_assignment_pause(session, turn, task, body(input)?)
                .await?
        )),
    ))
}
