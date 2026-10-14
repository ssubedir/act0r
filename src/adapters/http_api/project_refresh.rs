use super::{errors::*, *};
use crate::domain::{MessageId, SessionId, project_refresh::*};
use axum::extract::rejection::JsonRejection;
use serde_json::{Value, json};
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RefreshBody {
    request_id: MessageId,
    review_digest: String,
    prompt: String,
}
fn scope(session_id: SessionId, task_id: MessageId) -> RefreshScope {
    RefreshScope {
        session_id,
        task_id,
    }
}
pub(super) async fn status(
    State(state): State<Arc<ApiState>>,
    ApiPath((s, t)): ApiPath<(SessionId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .project_refresh(scope(s, t))
            .await?
    )))
}
pub(super) async fn inspect(
    State(state): State<Arc<ApiState>>,
    ApiPath((s, t)): ApiPath<(SessionId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .inspect_project_refresh(scope(s, t))
            .await?
    )))
}
pub(super) async fn start(
    State(state): State<Arc<ApiState>>,
    ApiPath((s, t)): ApiPath<(SessionId, MessageId)>,
    input: Result<Json<RefreshBody>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let input = body(input)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .start_project_refresh(
                    scope(s, t),
                    RefreshRequest {
                        request_id: input.request_id,
                        review_digest: input.review_digest,
                        prompt: input.prompt,
                        coding: None
                    }
                )
                .await?
        )),
    ))
}
pub(super) async fn update_setup(
    State(state): State<Arc<ApiState>>,
    ApiPath((s, t)): ApiPath<(SessionId, MessageId)>,
    input: Result<Json<SetupUpdateRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .update_project_setup(scope(s, t), body(input)?)
                .await?
        )),
    ))
}
pub(super) async fn cancel(
    State(state): State<Arc<ApiState>>,
    ApiPath((s, t)): ApiPath<(SessionId, MessageId)>,
    input: Result<Json<RefreshCancellation>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let request = body(input)?;
    state
        .agents
        .teams()?
        .harness()?
        .cancel_project_refresh(scope(s, t), request.request_id)
        .await?;
    Ok(Json(
        json!({"request_id":request.request_id,"cancelled":true}),
    ))
}
