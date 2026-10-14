//! Session-scoped change review and independent durable patch applications.
use super::{errors::*, *};
use crate::domain::{MessageId, RunId, SessionId, WaitId};
use axum::extract::rejection::JsonRejection;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreviewRequest {
    application_id: RunId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ApprovalRequest {
    wait_id: WaitId,
    approved: bool,
}
pub(super) async fn changes(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task)): ApiPath<(SessionId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .changes(session, task)
            .await?
    )))
}
pub(super) async fn delegated_work(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task)): ApiPath<(SessionId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .delegated_work(session, task)
            .await?
    )))
}
pub(super) async fn applications(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task)): ApiPath<(SessionId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .applications(session, task)
            .await?
    )))
}
pub(super) async fn preview(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task)): ApiPath<(SessionId, MessageId)>,
    input: Result<Json<PreviewRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let id = state
        .agents
        .teams()?
        .harness()?
        .preview(session, task, body(input)?.application_id)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(json!({"run_id":id}))))
}
pub(super) async fn application(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, id)): ApiPath<(SessionId, MessageId, RunId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .application(session, task, id)
            .await?
    )))
}
pub(super) async fn respond(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, id)): ApiPath<(SessionId, MessageId, RunId)>,
    input: Result<Json<ApprovalRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let input = body(input)?;
    state
        .agents
        .teams()?
        .harness()?
        .respond_application(session, task, id, input.wait_id, input.approved)
        .await?;
    Ok(Json(
        json!({"run_id":id,"wait_id":input.wait_id,"approved":input.approved}),
    ))
}
pub(super) async fn reconcile(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, id)): ApiPath<(SessionId, MessageId, RunId)>,
    input: Result<
        Json<crate::domain::patch_application::ApplicationRecoveryRequest>,
        JsonRejection,
    >,
) -> ApiResult<Json<Value>> {
    let input = body(input)?;
    let attempt = input.attempt_id;
    state
        .agents
        .teams()?
        .harness()?
        .reconcile_application(session, task, id, input)
        .await?;
    Ok(Json(json!({"run_id":id,"attempt_id":attempt})))
}

pub(super) async fn tool_recoveries(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task)): ApiPath<(SessionId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .tool_recoveries(session, task)
            .await?
    )))
}
pub(super) async fn reconcile_tool(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, id)): ApiPath<(SessionId, MessageId, RunId)>,
    input: Result<Json<crate::domain::tool_recovery::ToolRecoveryRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let input = body(input)?;
    let attempt = input.attempt_id;
    state
        .agents
        .teams()?
        .harness()?
        .reconcile_tool(session, task, id, input)
        .await?;
    Ok(Json(json!({"run_id":id,"attempt_id":attempt})))
}

pub(super) async fn child_import_recoveries(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task)): ApiPath<(SessionId, MessageId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .child_import_recoveries(session, task)
            .await?
    )))
}
pub(super) async fn reconcile_child_import(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, id)): ApiPath<(SessionId, MessageId, RunId)>,
    input: Result<Json<crate::domain::tool_recovery::ToolRecoveryRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let input = body(input)?;
    let attempt = input.attempt_id;
    state
        .agents
        .teams()?
        .harness()?
        .reconcile_child_import(session, task, id, input)
        .await?;
    Ok(Json(json!({"run_id":id,"attempt_id":attempt})))
}
