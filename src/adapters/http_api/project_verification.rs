use super::{errors::*, *};
use crate::domain::verification_repair::{RepairRequest, RepairScope};
use crate::domain::{MessageId, RunId, SessionId, WaitId, tool_recovery::ToolRecoveryRequest};
use axum::extract::rejection::JsonRejection;
use serde::Deserialize;
use serde_json::{Value, json};
fn repair_scope(
    session_id: SessionId,
    task_id: MessageId,
    application_id: RunId,
    verification_id: RunId,
) -> RepairScope {
    RepairScope {
        session_id,
        task_id,
        application_id,
        verification_id,
    }
}
pub(super) async fn repair(
    State(state): State<Arc<ApiState>>,
    ApiPath((s, t, a, v)): ApiPath<(SessionId, MessageId, RunId, RunId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .verification_repair(repair_scope(s, t, a, v))
            .await?
    )))
}
pub(super) async fn start_repair(
    State(state): State<Arc<ApiState>>,
    ApiPath((s, t, a, v)): ApiPath<(SessionId, MessageId, RunId, RunId)>,
    input: Result<Json<RepairRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(json!(
            state
                .agents
                .teams()?
                .harness()?
                .start_verification_repair(repair_scope(s, t, a, v), body(input)?)
                .await?
        )),
    ))
}
pub(super) async fn cancel_repair(
    State(state): State<Arc<ApiState>>,
    ApiPath((s, t, a, v)): ApiPath<(SessionId, MessageId, RunId, RunId)>,
    input: Result<Json<RepairRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let request = body(input)?;
    let id = request.request_id;
    state
        .agents
        .teams()?
        .harness()?
        .cancel_verification_repair(repair_scope(s, t, a, v), request)
        .await?;
    Ok(Json(json!({"request_id":id,"cancelled":true})))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Start {
    verification_id: RunId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Approval {
    wait_id: WaitId,
    approved: bool,
}
pub(super) async fn list(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, app)): ApiPath<(SessionId, MessageId, RunId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .project_verifications(session, task, app)
            .await?
    )))
}
pub(super) async fn start(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, app)): ApiPath<(SessionId, MessageId, RunId)>,
    input: Result<Json<Start>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let id = state
        .agents
        .teams()?
        .harness()?
        .verify_project(session, task, app, body(input)?.verification_id)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(json!({"run_id":id}))))
}
pub(super) async fn status(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, app, id)): ApiPath<(SessionId, MessageId, RunId, RunId)>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .harness()?
            .project_verification(session, task, app, id)
            .await?
    )))
}
pub(super) async fn respond(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, app, id)): ApiPath<(SessionId, MessageId, RunId, RunId)>,
    input: Result<Json<Approval>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let input = body(input)?;
    state
        .agents
        .teams()?
        .harness()?
        .respond_project_verification(session, task, app, id, input.wait_id, input.approved)
        .await?;
    Ok(Json(
        json!({"run_id":id,"wait_id":input.wait_id,"approved":input.approved}),
    ))
}
pub(super) async fn reconcile(
    State(state): State<Arc<ApiState>>,
    ApiPath((session, task, app, id)): ApiPath<(SessionId, MessageId, RunId, RunId)>,
    input: Result<Json<ToolRecoveryRequest>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let input = body(input)?;
    let attempt = input.attempt_id;
    state
        .agents
        .teams()?
        .harness()?
        .reconcile_project_verification(session, task, app, id, input)
        .await?;
    Ok(Json(json!({"run_id":id,"attempt_id":attempt})))
}
