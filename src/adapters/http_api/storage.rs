use super::{errors::*, *};
use crate::domain::storage::CacheCleanupRequest;
use axum::extract::rejection::JsonRejection;
use serde_json::{Value, json};

pub(super) async fn report(State(state): State<Arc<ApiState>>) -> ApiResult<Json<Value>> {
    Ok(Json(json!(state.agents.storage()?.report().await?)))
}
pub(super) async fn clean(
    State(state): State<Arc<ApiState>>,
    input: Result<Json<CacheCleanupRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state.agents.storage()?.clean(body(input)?).await?
    )))
}
