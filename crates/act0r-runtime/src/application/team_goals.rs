use super::{Error, ports::*};
use crate::domain::{actors::AgentTaskStatus, goal_list::*, teams::TeamSummary, *};
use serde::Serialize;
use std::sync::Arc;

pub const GOAL_PREVIEW_BYTES: usize = 1024;
#[derive(Clone, Debug, Serialize)]
pub struct TeamGoalSummary {
    pub task_id: MessageId,
    pub goal_preview: String,
    pub goal_truncated: bool,
    /// Physical root status, never an assertion that the entire goal has settled.
    pub root_status: AgentTaskStatus,
    pub created_at: i64,
    pub updated_at: i64,
    pub goal_paused: bool,
    pub goal_cancelled: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct TeamGoalPage {
    pub team: TeamSummary,
    pub query: GoalListQuery,
    pub snapshot_position: u64,
    pub items: Vec<TeamGoalSummary>,
    pub next_cursor: Option<GoalListCursor>,
    pub has_more: bool,
}
pub struct TeamGoalListService {
    port: Arc<dyn TeamGoalListPort>,
}
impl TeamGoalListService {
    pub fn new(port: Arc<dyn TeamGoalListPort>) -> Self {
        Self { port }
    }
    pub async fn list(&self, team: TeamId, query: GoalListQuery) -> Result<TeamGoalPage, Error> {
        query.validate()?;
        self.port.team_goals(team, query).await
    }
}
