use super::{errors::*, *};
use crate::domain::{
    SessionId,
    session_summary::{SummaryPolicyRequest, SummaryRequest},
};
use axum::{extract::rejection::JsonRejection, http::StatusCode};
use serde_json::{Value, json};

pub(super) async fn current(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state.agents.teams()?.harness()?.summary(session).await?
    )))
}
pub(super) async fn policy(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
    input: Result<Json<SummaryPolicyRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .summary_policy(session, body(input)?)
            .await?
    )))
}
pub(super) async fn start(
    State(state): State<Arc<ApiState>>,
    ApiPath(session): ApiPath<SessionId>,
    input: Result<Json<SummaryRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .summarize(session, body(input)?)
                .await?
        )),
    ))
}
