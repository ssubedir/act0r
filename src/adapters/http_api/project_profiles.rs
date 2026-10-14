use super::{errors::*, *};
use crate::domain::{ProjectProfileId, project_profiles::ProfileRequest};
use axum::extract::rejection::JsonRejection;
use serde_json::{Value, json};

pub(super) async fn detect(
    State(state): State<Arc<ApiState>>,
    input: Result<Json<crate::domain::project_detection::DetectionRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .project_detection()?
            .detect(body(input)?)
            .await?
    )))
}

pub(super) async fn list(State(state): State<Arc<ApiState>>) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state.agents.teams()?.project_profiles()?.list().await?
    )))
}
pub(super) async fn get(
    State(state): State<Arc<ApiState>>,
    ApiPath(id): ApiPath<ProjectProfileId>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state.agents.teams()?.project_profiles()?.get(id).await?
    )))
}
pub(super) async fn change(
    State(state): State<Arc<ApiState>>,
    ApiPath(id): ApiPath<ProjectProfileId>,
    input: Result<Json<ProfileRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .project_profiles()?
            .change(id, body(input)?)
            .await?
    )))
}
