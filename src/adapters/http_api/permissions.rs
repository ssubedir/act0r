use super::{errors::*, *};
use crate::domain::{MessageId, SessionId, permissions::PermissionRequest};
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
            .permissions(session, turn)
            .await?
    )))
}
pub(super) async fn change(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
    input: Result<Json<PermissionRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .change_permissions(session, body(input)?)
            .await?
    )))
}
