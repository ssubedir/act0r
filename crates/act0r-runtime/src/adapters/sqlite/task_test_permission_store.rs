//! Task tests use the same accepted-handoff authority as task edits.
use super::*;
use crate::domain::permissions::*;

impl SqliteStore {
    pub(super) async fn task_test_candidate(
        conn: &mut SqliteConnection,
        session: SessionId,
        task_id: MessageId,
    ) -> Result<Option<TaskTestPermission>, Error> {
        let Some((turn, run, task, pinned)) =
            Self::accepted_task_permission(conn, session, task_id).await?
        else {
            return Ok(None);
        };
        let Ok((workspace_input, request)) = run.task_test_binding(&task, &pinned) else {
            return Ok(None);
        };
        Ok(Some(TaskTestPermission {
            grant_id: uuid::Uuid::nil(),
            session_id: session,
            turn_id: turn,
            task_id,
            run_id: run.record().id,
            actor_id: task.agent_id,
            workspace_input,
            request,
            granted_at: 0,
            revoked_at: None,
        }))
    }
    pub(super) async fn eligible_task_test_wait(
        conn: &mut SqliteConnection,
        session: SessionId,
        task: MessageId,
    ) -> Result<Option<(EligibleTaskTestWait, TaskTestPermission, Run)>, Error> {
        let Some(grant) = Self::task_test_candidate(conn, session, task).await? else {
            return Ok(None);
        };
        let run = Self::load(conn, grant.run_id).await?;
        let Ok(request) = run.pending_task_test_request() else {
            return Ok(None);
        };
        if grant.request != request {
            return Ok(None);
        }
        let wait_id = run
            .record()
            .wait
            .as_ref()
            .ok_or(DomainError::UnknownWait)?
            .id;
        let wait = EligibleTaskTestWait {
            task_id: task,
            run_id: grant.run_id,
            wait_id,
            actor_id: grant.actor_id,
            request,
        };
        Ok(Some((wait, grant, run)))
    }
    pub(super) async fn task_test_views(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
    ) -> Result<(Vec<TaskTestPermissionView>, Vec<EligibleTaskTestWait>, bool), Error> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_task_test_permissions WHERE session_id=? AND turn_id=? ORDER BY task_id LIMIT 129")
            .bind(session.to_string()).bind(turn.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let mut truncated = rows.len() > 128;
        let mut views = Vec::new();
        for payload in rows.into_iter().take(128) {
            let grant: TaskTestPermission = serde_json::from_str(&payload)?;
            let status = if grant.revoked_at.is_some() {
                "revoked"
            } else if Self::task_test_candidate(conn, session, grant.task_id)
                .await?
                .is_some_and(|candidate| same_task_test_scope(&candidate, &grant))
            {
                "active"
            } else {
                "expired"
            };
            views.push(TaskTestPermissionView { grant, status });
        }
        let tasks: Vec<String> = sqlx::query_scalar("WITH RECURSIVE owned(id) AS (SELECT ? UNION SELECT o.id FROM agent_goal_ownership o JOIN owned p ON o.parent_id=p.id)
            SELECT a.request_id FROM agent_runs a JOIN owned o ON o.id=a.request_id JOIN runs r ON r.id=a.run_id
            WHERE r.status='waiting' AND json_extract(r.payload,'$.wait.key')='native_tool_approval'
            ORDER BY r.created_at,r.id LIMIT 129")
            .bind(turn.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        truncated |= tasks.len() > 128;
        let mut waits = Vec::new();
        for task in tasks.into_iter().take(128) {
            let task: MessageId = task
                .parse()
                .map_err(|_| Error::Storage("invalid task identity".into()))?;
            // A saved active permission is already owned by the worker; do not offer a second grant.
            let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_task_test_permissions WHERE task_id=? AND revoked=0)")
                .bind(task.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            if !active
                && let Some((wait, _, _)) =
                    Self::eligible_task_test_wait(conn, session, task).await?
            {
                waits.push(wait);
            }
        }
        Ok((views, waits, truncated))
    }
    pub(super) async fn save_task_test_grant(
        conn: &mut SqliteConnection,
        grant: &TaskTestPermission,
    ) -> Result<(), Error> {
        sqlx::query("INSERT INTO harness_task_test_permissions(task_id,session_id,turn_id,grant_id,run_id,revoked,payload) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(task_id) DO UPDATE SET grant_id=excluded.grant_id,revoked=excluded.revoked,payload=excluded.payload")
            .bind(grant.task_id.to_string()).bind(grant.session_id.to_string()).bind(grant.turn_id.to_string())
            .bind(grant.grant_id.to_string()).bind(grant.run_id.to_string()).bind(grant.revoked_at.is_some())
            .bind(serde_json::to_string(grant)?).execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
    pub(super) async fn apply_task_test_permission(
        &self,
        decision: TaskTestPermissionDecision,
    ) -> Result<bool, Error> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let rows: Vec<String> = sqlx::query_scalar("SELECT p.payload FROM harness_task_test_permissions p JOIN runs r ON r.id=p.run_id
            JOIN harness_session_catalog c ON c.session_id=p.session_id
            WHERE p.revoked=0 AND c.archived=0 AND r.status='waiting' AND json_extract(r.payload,'$.wait.key')='native_tool_approval'
            AND json_extract(p.payload,'$.grant_id')=p.grant_id AND json_extract(p.payload,'$.task_id')=p.task_id
            AND json_extract(p.payload,'$.session_id')=p.session_id AND json_extract(p.payload,'$.turn_id')=p.turn_id
            AND json_extract(p.payload,'$.run_id')=p.run_id AND json_extract(p.payload,'$.revoked_at') IS NULL
            AND NOT EXISTS(SELECT 1 FROM agent_goal_paused_requests held WHERE held.id=p.task_id)
            ORDER BY r.created_at,r.id")
            .fetch_all(&mut *tx).await.map_err(storage)?;
        for payload in rows {
            let grant: TaskTestPermission = serde_json::from_str(&payload)?;
            let Some((_, candidate, mut run)) =
                Self::eligible_task_test_wait(&mut tx, grant.session_id, grant.task_id).await?
            else {
                continue;
            };
            if !same_task_test_scope(&candidate, &grant) {
                continue;
            }
            let before = run.clone();
            match decision(&mut run, &grant) {
                Ok(()) => {}
                Err(Error::Unavailable(_)) => continue,
                Err(error) => return Err(error),
            }
            self.save_change(&mut tx, &before, run, None).await?;
            tx.commit().await.map_err(storage)?;
            return Ok(true);
        }
        tx.commit().await.map_err(storage)?;
        Ok(false)
    }
}
fn same_task_test_scope(a: &TaskTestPermission, b: &TaskTestPermission) -> bool {
    a.session_id == b.session_id
        && a.turn_id == b.turn_id
        && a.task_id == b.task_id
        && a.run_id == b.run_id
        && a.actor_id == b.actor_id
        && a.workspace_input == b.workspace_input
        && a.request == b.request
}
