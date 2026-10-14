use super::*;
use crate::{
    application::*,
    domain::{actors::AgentTaskStatus, goal_list::*, teams::*},
};

impl TeamGoalListPort for SqliteStore {
    fn team_goals(&self, team: TeamId, query: GoalListQuery) -> PortFuture<'_, TeamGoalPage> {
        Box::pin(async move {
            query.validate()?;
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let record = Self::load_team(&mut tx, team).await?;
            if record.status != TeamStatus::Ready {
                return Err(Error::Unavailable("team setup is not ready".into()));
            }
            Self::check_team_actors(&mut tx, &record).await?;
            let stream: String =
                sqlx::query_scalar("SELECT id FROM history_stream WHERE singleton=1")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let stream: EventStreamId = stream
                .parse()
                .map_err(|_| Error::Storage("invalid database stream identity".into()))?;
            let watermark: i64 =
                sqlx::query_scalar("SELECT COALESCE(MAX(sequence),0) FROM agent_mailbox")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let (snapshot, before) = if let Some(cursor) = query.after {
                cursor.validate_scope(stream, team)?;
                if cursor.snapshot() > watermark as u64 {
                    return Err(DomainError::InvalidConfiguration(
                        "goal cursor is ahead of the committed mailbox".into(),
                    )
                    .into());
                }
                (cursor.snapshot() as i64, cursor.before() as i64)
            } else {
                (watermark, 0)
            };
            // Select bounded scalar projections, never load result bodies or whole Run histories.
            // Ownership includes delegations and follow-ups, not just the physical parent_id.
            let rows = sqlx::query(
                "SELECT m.sequence,t.id,
                 substr(CAST(COALESCE(json_extract(m.payload,'$.message.payload.goal'),'') AS BLOB),1,1028) AS goal,
                 length(CAST(COALESCE(json_extract(m.payload,'$.message.payload.goal'),'') AS BLOB))>1024 AS goal_truncated,
                 json_extract(t.payload,'$.created_at') AS created_at,
                 json_extract(t.payload,'$.updated_at') AS updated_at,
                 CASE WHEN json_extract(t.payload,'$.status') IN ('succeeded','failed','rejected','budget_exhausted','cancelled') THEN json_extract(t.payload,'$.status')
                      WHEN json_extract(r.payload,'$.status') IN ('queued','running','waiting','paused','blocked') AND json_extract(r.payload,'$.controls.cancel_requested')=1 THEN 'cancelling'
                      WHEN json_extract(r.payload,'$.status')='queued' THEN 'running'
                      ELSE COALESCE(json_extract(r.payload,'$.status'),CASE WHEN a.status='ready' THEN 'queued' ELSE a.status END) END AS status,
                 COALESCE(json_extract(p.payload,'$.paused'),0) AS paused,
                 c.root_task_id IS NOT NULL AS cancelled
                 FROM agent_tasks t JOIN agent_mailbox m ON m.id=t.id
                 JOIN agent_goal_ownership o ON o.id=t.id AND o.parent_id IS NULL
                 JOIN agent_actors a ON a.id=t.agent_id LEFT JOIN runs r ON r.id=t.run_id
                 LEFT JOIN agent_goal_pauses p ON p.root_task_id=t.id
                 LEFT JOIN agent_goal_cancellations c ON c.root_task_id=t.id
                 WHERE t.agent_id=? AND m.sequence<=? AND (?=0 OR m.sequence<?)
                 ORDER BY m.sequence DESC LIMIT ?"
            ).bind(record.planner()?.agent_id.to_string()).bind(snapshot).bind(before).bind(before).bind(i64::from(query.limit)+1)
                .fetch_all(&mut *tx).await.map_err(storage)?;
            let has_more = rows.len() > query.limit as usize;
            let mut items = Vec::new();
            let mut last = 0;
            for row in rows.iter().take(query.limit as usize) {
                last = row.try_get::<i64, _>("sequence").map_err(storage)?;
                let id: String = row.try_get("id").map_err(storage)?;
                let status: String = row.try_get("status").map_err(storage)?;
                let root_status: AgentTaskStatus =
                    serde_json::from_value(serde_json::Value::String(status))?;
                let raw: Vec<u8> = row.try_get("goal").map_err(storage)?;
                // Up to four extra bytes cover a character crossing the preview boundary.
                // Only the discarded tail can be incomplete UTF-8; embedded NULs stay intact.
                let mut preview = String::from_utf8_lossy(&raw).into_owned();
                let truncated = row.try_get("goal_truncated").map_err(storage)?;
                let mut end = preview.len().min(GOAL_PREVIEW_BYTES);
                while !preview.is_char_boundary(end) {
                    end -= 1;
                }
                preview.truncate(end);
                items.push(TeamGoalSummary {
                    task_id: id
                        .parse()
                        .map_err(|_| Error::Storage("invalid goal identity".into()))?,
                    goal_preview: preview,
                    goal_truncated: truncated,
                    root_status,
                    created_at: row.try_get("created_at").map_err(storage)?,
                    updated_at: row.try_get("updated_at").map_err(storage)?,
                    goal_paused: row.try_get("paused").map_err(storage)?,
                    goal_cancelled: row.try_get("cancelled").map_err(storage)?,
                });
            }
            let next_cursor = if has_more {
                Some(GoalListCursor::new(
                    stream,
                    team,
                    snapshot as u64,
                    last as u64,
                )?)
            } else {
                None
            };
            tx.commit().await.map_err(storage)?;
            Ok(TeamGoalPage {
                team: TeamSummary::from(&record),
                query,
                snapshot_position: snapshot as u64,
                items,
                next_cursor,
                has_more,
            })
        })
    }
}
