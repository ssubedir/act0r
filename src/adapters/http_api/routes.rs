use super::{errors::*, *};
use crate::{
    application::TaskInspectionOptions,
    domain::{teams::*, *},
};
use axum::extract::rejection::JsonRejection;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct InspectionQuery {
    max_nodes: u32,
    max_depth: u32,
}
impl Default for InspectionQuery {
    fn default() -> Self {
        let d = TaskInspectionOptions::default();
        Self {
            max_nodes: d.max_nodes,
            max_depth: d.max_depth,
        }
    }
}
impl InspectionQuery {
    fn options(self) -> TaskInspectionOptions {
        TaskInspectionOptions {
            max_nodes: self.max_nodes,
            max_depth: self.max_depth,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Submit {
    message_id: MessageId,
    goal: String,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct GoalQuery {
    after: Option<goal_list::GoalListCursor>,
    limit: u32,
}
impl Default for GoalQuery {
    fn default() -> Self {
        Self {
            after: None,
            limit: goal_list::GoalListQuery::default().limit,
        }
    }
}
pub(super) async fn goals(
    State(state): State<Arc<ApiState>>,
    ApiPath(team): ApiPath<TeamId>,
    ApiQuery(query): ApiQuery<GoalQuery>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .goals()?
            .list(
                team,
                goal_list::GoalListQuery {
                    after: query.after,
                    limit: query.limit
                }
            )
            .await?
    )))
}

pub(super) async fn list(State(state): State<Arc<ApiState>>) -> ApiResult<Json<Value>> {
    Ok(Json(json!(state.agents.teams()?.list().await?)))
}
pub(super) async fn create(
    State(state): State<Arc<ApiState>>,
    input: Result<Json<TeamSpec>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let team = state.agents.teams()?.create(body(input)?).await?;
    Ok(Json(json!(state.agents.teams()?.status(team.id).await?)))
}
pub(super) async fn resume(
    State(state): State<Arc<ApiState>>,
    ApiPath(team): ApiPath<TeamId>,
) -> ApiResult<Json<Value>> {
    state.agents.teams()?.resume(team).await?;
    Ok(Json(json!(state.agents.teams()?.status(team).await?)))
}
pub(super) async fn team_status(
    State(state): State<Arc<ApiState>>,
    ApiPath(team): ApiPath<TeamId>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(state.agents.teams()?.status(team).await?)))
}
pub(super) async fn submit(
    State(state): State<Arc<ApiState>>,
    ApiPath(team): ApiPath<TeamId>,
    input: Result<Json<Submit>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let input = body(input)?;
    let task = state
        .agents
        .teams()?
        .submit(team, input.message_id, input.goal)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"team_id":team,"task_id":task})),
    ))
}
pub(super) async fn task_status(
    State(state): State<Arc<ApiState>>,
    ApiPath((team, task)): ApiPath<(TeamId, MessageId)>,
    ApiQuery(query): ApiQuery<InspectionQuery>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .tasks()?
            .status(team, task, query.options())
            .await?
    )))
}
pub(super) async fn inbox(
    State(state): State<Arc<ApiState>>,
    ApiPath((team, task)): ApiPath<(TeamId, MessageId)>,
    ApiQuery(query): ApiQuery<InspectionQuery>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .tasks()?
            .inbox(team, task, query.options())
            .await?
    )))
}
pub(super) async fn respond(
    State(state): State<Arc<ApiState>>,
    ApiPath((team, task, wait)): ApiPath<(TeamId, MessageId, WaitId)>,
    input: Result<Json<HumanResponse>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(
        state
            .agents
            .teams()?
            .tasks()?
            .respond(team, task, wait, body(input)?)
            .await?
    )))
}
