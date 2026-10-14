//! Read a goal timeline through a snapshot port; execution remains owned by Runs.
use super::{Error, ports::TeamTaskEventsPort};
use crate::domain::{teams::TeamSummary, timeline::*, *};
use serde::Serialize;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize)]
pub struct TeamTimelineEvent {
    pub position: u64,
    pub task_id: MessageId,
    pub actor_id: AgentId,
    pub member: Option<String>,
    pub event: HistoryEvent,
}
#[derive(Clone, Debug, Serialize)]
pub struct OversizedTimelineEvent {
    pub position: u64,
    pub event_id: EventId,
    /// Stored history JSON plus its compact timeline envelope; never load an unbounded body.
    pub required_bytes: u64,
}
#[derive(Debug, Serialize)]
pub struct TeamEventPage {
    pub team: TeamSummary,
    pub requested_task_id: MessageId,
    pub root_task_id: MessageId,
    pub query: EventQuery,
    pub events: Vec<TeamTimelineEvent>,
    pub event_bytes: u32,
    pub snapshot_position: u64,
    pub next_cursor: EventCursor,
    pub has_more: bool,
    pub oversized_event: Option<OversizedTimelineEvent>,
}
pub struct TeamEventService {
    port: Arc<dyn TeamTaskEventsPort>,
}
impl TeamEventService {
    pub fn new(port: Arc<dyn TeamTaskEventsPort>) -> Self {
        Self { port }
    }
    pub async fn read(
        &self,
        team: TeamId,
        task: MessageId,
        query: EventQuery,
    ) -> Result<TeamEventPage, Error> {
        query.validate()?;
        self.port.team_task_events(team, task, query).await
    }
}
