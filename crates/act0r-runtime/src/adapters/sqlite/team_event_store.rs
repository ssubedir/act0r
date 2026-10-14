use super::*;
use crate::{
    application::*,
    domain::{teams::TeamSummary, timeline::*},
};
use serde_json::{Value, json};

impl TeamTaskEventsPort for SqliteStore {
    fn team_task_events(
        &self,
        team: TeamId,
        task: MessageId,
        query: EventQuery,
    ) -> PortFuture<'_, TeamEventPage> {
        Box::pin(async move {
            query.validate()?;
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let (record, root) = Self::check_team_task(&mut tx, team, task).await?;
            let stream: String =
                sqlx::query_scalar("SELECT id FROM history_stream WHERE singleton=1")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let stream: EventStreamId = stream
                .parse()
                .map_err(|_| Error::Storage("invalid history stream identity".into()))?;
            let watermark: i64 =
                sqlx::query_scalar("SELECT COALESCE(MAX(position),0) FROM history_positions")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let after = if let Some(cursor) = query.after {
                cursor.validate_scope(stream, team, root)?;
                if cursor.position() > watermark as u64 {
                    return Err(DomainError::InvalidConfiguration(
                        "event cursor is ahead of the committed stream".into(),
                    )
                    .into());
                }
                cursor.position()
            } else {
                0
            };
            // Read bounded headers first. An oversized stored event never requires loading its body.
            let headers:Vec<(i64,String,String,String,i64)>=sqlx::query_as(
                "WITH RECURSIVE owned(id) AS (SELECT ? UNION SELECT o.id FROM agent_goal_ownership o JOIN owned p ON o.parent_id=p.id)
                SELECT p.position,p.event_id,a.request_id,a.agent_id,length(CAST(h.payload AS BLOB))
                FROM owned o JOIN agent_runs a ON a.request_id=o.id JOIN history_positions p ON p.run_id=a.run_id JOIN history h ON h.id=p.event_id
                WHERE p.position>? AND p.position<=? ORDER BY p.position LIMIT ?"
            ).bind(root.to_string()).bind(after as i64).bind(watermark).bind(i64::from(query.limit)+1)
                .fetch_all(&mut *tx).await.map_err(storage)?;
            let mut events = Vec::new();
            let mut event_bytes = 0u32;
            let mut oversized_event = None;
            let mut has_more = false;
            for (position, event_id, task_id, actor_id, history_bytes) in &headers {
                if events.len() >= query.limit as usize {
                    has_more = true;
                    break;
                }
                let task_id: MessageId = task_id
                    .parse()
                    .map_err(|_| Error::Storage("invalid timeline task identity".into()))?;
                let actor_id: AgentId = actor_id
                    .parse()
                    .map_err(|_| Error::Storage("invalid timeline actor identity".into()))?;
                let event_id: EventId = event_id
                    .parse()
                    .map_err(|_| Error::Storage("invalid timeline event identity".into()))?;
                let member = record
                    .members
                    .iter()
                    .find(|m| m.agent_id == actor_id)
                    .map(|m| m.name.clone());
                let envelope=serde_json::to_vec(&json!({"position":position,"task_id":task_id,"actor_id":actor_id,"member":member,"event":Value::Null}))?.len()-4;
                let bytes = u64::try_from(*history_bytes)
                    .map_err(|_| DomainError::InvalidState)?
                    .saturating_add(envelope as u64);
                if bytes > u64::from(query.max_bytes) {
                    oversized_event = Some(OversizedTimelineEvent {
                        position: *position as u64,
                        event_id,
                        required_bytes: bytes,
                    });
                    has_more = true;
                    break;
                }
                if bytes + u64::from(event_bytes) > u64::from(query.max_bytes) {
                    has_more = true;
                    break;
                }
                let payload: String = sqlx::query_scalar("SELECT payload FROM history WHERE id=?")
                    .bind(event_id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
                events.push(TeamTimelineEvent {
                    position: *position as u64,
                    task_id,
                    actor_id,
                    member,
                    event: serde_json::from_str(&payload)?,
                });
                event_bytes += bytes as u32;
            }
            let position = if has_more {
                events.last().map_or(after, |e| e.position)
            } else {
                watermark as u64
            };
            let next_cursor = EventCursor::new(stream, team, root, position)?;
            tx.commit().await.map_err(storage)?;
            Ok(TeamEventPage {
                team: TeamSummary::from(&record),
                requested_task_id: task,
                root_task_id: root,
                query,
                events,
                event_bytes,
                snapshot_position: watermark as u64,
                next_cursor,
                has_more,
                oversized_event,
            })
        })
    }
}
