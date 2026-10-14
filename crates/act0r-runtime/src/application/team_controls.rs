//! Team ownership scopes the existing goal admission hold and cancellation policy.
use super::{ActorService, Engine, Error};
use crate::domain::{MessageId, TeamId, goal::*};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct TeamGoalControls {
    pub team_id: TeamId,
    pub requested_task_id: MessageId,
    pub root_task_id: MessageId,
    pub pause: GoalPauseRecord,
    pub cancellation: Option<GoalCancellationView>,
}
#[derive(Debug, Serialize)]
pub struct TeamGoalPauseReceipt {
    pub team_id: TeamId,
    pub requested_task_id: MessageId,
    pub root_task_id: MessageId,
    pub control: GoalPauseRecord,
    pub replayed: bool,
}
pub struct TeamGoalControlService {
    engine: Engine,
    actors: Arc<ActorService>,
}
impl TeamGoalControlService {
    pub fn new(engine: Engine, actors: Arc<ActorService>) -> Self {
        Self { engine, actors }
    }
    pub async fn status(&self, team: TeamId, task: MessageId) -> Result<TeamGoalControls, Error> {
        self.actors
            .goal_controls()?
            .team_goal_controls(team, task)
            .await
    }
    pub async fn pause(
        &self,
        team: TeamId,
        task: MessageId,
        precondition: GoalPausePrecondition,
    ) -> Result<TeamGoalPauseReceipt, Error> {
        self.control(team, task, true, precondition).await
    }
    pub async fn resume(
        &self,
        team: TeamId,
        task: MessageId,
        precondition: GoalPausePrecondition,
    ) -> Result<TeamGoalPauseReceipt, Error> {
        self.control(team, task, false, precondition).await
    }
    async fn control(
        &self,
        team: TeamId,
        task: MessageId,
        paused: bool,
        precondition: GoalPausePrecondition,
    ) -> Result<TeamGoalPauseReceipt, Error> {
        self.actors
            .goal_controls()?
            .control_team_goal_pause(team, task, paused, precondition, self.engine.clock.now_ms())
            .await
    }
    pub async fn cancel(
        &self,
        team: TeamId,
        task: MessageId,
    ) -> Result<GoalCancellationView, Error> {
        let receipt = self
            .actors
            .goal_controls()?
            .request_team_goal_cancellation(team, task, self.engine.clock.now_ms())
            .await?;
        self.actors.settle_goal_cancellation(receipt).await
    }
}
