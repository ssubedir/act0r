//! Task authority comes from accepted handoffs, never from request paths or model payloads.
use super::*;
use crate::domain::{actors::RegisteredAgentConfig, permissions::*};

impl SqliteStore {
    pub(super) async fn accepted_task_permission(
        conn: &mut SqliteConnection,
        session: SessionId,
        task_id: MessageId,
    ) -> Result<
        Option<(
            MessageId,
            Run,
            actors::AgentTaskRecord,
            RegisteredAgentConfig,
        )>,
        Error,
    > {
        Self::accepted_task_binding(conn, session, task_id, false).await
    }
    pub(super) async fn accepted_task_binding(
        conn: &mut SqliteConnection,
        session: SessionId,
        task_id: MessageId,
        recovery: bool,
    ) -> Result<
        Option<(
            MessageId,
            Run,
            actors::AgentTaskRecord,
            RegisteredAgentConfig,
        )>,
        Error,
    > {
        let turn = Self::goal_root(conn, task_id).await?;
        let team = Self::permission_team(conn, session, turn).await?;
        let task = Self::load_agent_task(conn, task_id).await?;
        if task_id == turn
            || (!recovery && Self::load_session_metadata(conn, session).await?.archived)
            || Self::goal_receipt_for_task(conn, task_id).await?.is_some()
            || !team
                .members
                .iter()
                .any(|m| m.agent_id == task.record().agent_id)
        {
            return Ok(None);
        }
        let Some(run_id) = task.record().run_id else {
            return Ok(None);
        };
        let run = Self::load(conn, run_id).await?;
        let actor = Self::load_actor(conn, task.record().agent_id).await?;
        let pinned: RegisteredAgentConfig = serde_json::from_value(actor.record().config.clone())?;
        let binding = if recovery {
            run.task_recovery_binding(task.record(), &pinned)
        } else {
            run.task_permission_binding(task.record(), &pinned)
        };
        let Ok((workspace_input, _)) = binding else {
            return Ok(None);
        };
        let parent = Self::load(conn, workspace_input.parent_run_id).await?;
        let parent_owner: Option<String> =
            sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
                .bind(parent.record().id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        // Durable goal ownership is authoritative; mailbox envelopes cannot supply ancestry.
        let accepted_parent: Option<String> =
            sqlx::query_scalar("SELECT parent_id FROM agent_goal_ownership WHERE id=?")
                .bind(task_id.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
        if parent_owner.is_none()
            || parent_owner != accepted_parent
            || !(parent.record().status.is_unfinished()
                || parent.record().status == RunStatus::Blocked)
            || parent.record().controls.cancel_requested
            || !parent.record().delegations.iter().any(|d| {
                d.id == task_id
                    && d.recipient == task.record().agent_id
                    && d.workspace_input.as_ref() == Some(&workspace_input)
                    && d.outcome.is_none()
                    && d.timed_out_at.is_none()
            })
            || !run.activities().iter().any(|a| {
                a.spec.handler == workspace_handoff::handler("verify")
                    && a.spec.input == serde_json::json!(workspace_input)
                    && a.status == ActivityStatus::Succeeded
                    && a.result == Some(serde_json::json!({"verified":true}))
            })
        {
            return Ok(None);
        }
        Ok(Some((turn, run, task.record().clone(), pinned)))
    }
    pub(super) async fn task_edit_candidate(
        conn: &mut SqliteConnection,
        session: SessionId,
        task_id: MessageId,
    ) -> Result<Option<TaskEditPermission>, Error> {
        let Some((turn, run, task, pinned)) =
            Self::accepted_task_permission(conn, session, task_id).await?
        else {
            return Ok(None);
        };
        let Ok((workspace_input, tools)) = run.task_edit_binding(&task, &pinned) else {
            return Ok(None);
        };
        Ok(Some(TaskEditPermission {
            grant_id: uuid::Uuid::nil(),
            session_id: session,
            turn_id: turn,
            task_id,
            run_id: run.record().id,
            actor_id: task.agent_id,
            workspace_input,
            tools,
            granted_at: 0,
            revoked_at: None,
        }))
    }
    pub(super) async fn eligible_edit_wait(
        conn: &mut SqliteConnection,
        session: SessionId,
        task: MessageId,
    ) -> Result<Option<(EligibleEditWait, TaskEditPermission, Run)>, Error> {
        let Some(grant) = Self::task_edit_candidate(conn, session, task).await? else {
            return Ok(None);
        };
        let run = Self::load(conn, grant.run_id).await?;
        let Ok(request) = run.pending_task_edit_request() else {
            return Ok(None);
        };
        if !grant.tools.contains(&request.definition) {
            return Ok(None);
        }
        let wait_id = run
            .record()
            .wait
            .as_ref()
            .ok_or(DomainError::UnknownWait)?
            .id;
        let wait = EligibleEditWait {
            task_id: task,
            run_id: grant.run_id,
            wait_id,
            actor_id: grant.actor_id,
            workspace: grant.workspace_input.workspace.clone(),
            writable_paths: grant
                .workspace_input
                .request
                .writable_paths
                .clone()
                .ok_or(DomainError::InvalidState)?,
            tools: grant.tools.clone(),
        };
        Ok(Some((wait, grant, run)))
    }
    pub(super) async fn task_edit_views(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
    ) -> Result<(Vec<TaskEditPermissionView>, Vec<EligibleEditWait>, bool), Error> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_task_edit_permissions WHERE session_id=? AND turn_id=? ORDER BY task_id LIMIT 129")
            .bind(session.to_string()).bind(turn.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let mut truncated = rows.len() > 128;
        let mut views = Vec::new();
        for payload in rows.into_iter().take(128) {
            let grant: TaskEditPermission = serde_json::from_str(&payload)?;
            let status = if grant.revoked_at.is_some() {
                "revoked"
            } else if Self::task_edit_candidate(conn, session, grant.task_id)
                .await?
                .is_some_and(|candidate| same_task_edit_scope(&candidate, &grant))
            {
                "active"
            } else {
                "expired"
            };
            views.push(TaskEditPermissionView { grant, status });
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
            let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_task_edit_permissions WHERE task_id=? AND revoked=0)")
                .bind(task.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            if !active
                && let Some((wait, _, _)) = Self::eligible_edit_wait(conn, session, task).await?
            {
                waits.push(wait);
            }
        }
        Ok((views, waits, truncated))
    }
    pub(super) async fn save_task_edit_grant(
        conn: &mut SqliteConnection,
        grant: &TaskEditPermission,
    ) -> Result<(), Error> {
        sqlx::query("INSERT INTO harness_task_edit_permissions(task_id,session_id,turn_id,grant_id,run_id,revoked,payload) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(task_id) DO UPDATE SET grant_id=excluded.grant_id,revoked=excluded.revoked,payload=excluded.payload")
            .bind(grant.task_id.to_string()).bind(grant.session_id.to_string()).bind(grant.turn_id.to_string())
            .bind(grant.grant_id.to_string()).bind(grant.run_id.to_string()).bind(grant.revoked_at.is_some())
            .bind(serde_json::to_string(grant)?).execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
    pub(super) async fn apply_task_edit_permission(
        &self,
        decision: TaskEditPermissionDecision,
    ) -> Result<bool, Error> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let rows: Vec<String> = sqlx::query_scalar("SELECT p.payload FROM harness_task_edit_permissions p JOIN runs r ON r.id=p.run_id
            JOIN harness_session_catalog c ON c.session_id=p.session_id
            WHERE p.revoked=0 AND c.archived=0 AND r.status='waiting' AND json_extract(r.payload,'$.wait.key')='native_tool_approval'
            AND json_extract(p.payload,'$.grant_id')=p.grant_id AND json_extract(p.payload,'$.task_id')=p.task_id
            AND json_extract(p.payload,'$.session_id')=p.session_id AND json_extract(p.payload,'$.turn_id')=p.turn_id
            AND json_extract(p.payload,'$.run_id')=p.run_id AND json_extract(p.payload,'$.revoked_at') IS NULL
            AND NOT EXISTS(SELECT 1 FROM agent_goal_paused_requests held WHERE held.id=p.task_id)
            ORDER BY r.created_at,r.id")
            .fetch_all(&mut *tx).await.map_err(storage)?;
        for payload in rows {
            let grant: TaskEditPermission = serde_json::from_str(&payload)?;
            let Some((_, candidate, mut run)) =
                Self::eligible_edit_wait(&mut tx, grant.session_id, grant.task_id).await?
            else {
                continue;
            };
            if !same_task_edit_scope(&candidate, &grant) {
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
fn same_task_edit_scope(a: &TaskEditPermission, b: &TaskEditPermission) -> bool {
    a.session_id == b.session_id
        && a.turn_id == b.turn_id
        && a.task_id == b.task_id
        && a.run_id == b.run_id
        && a.actor_id == b.actor_id
        && a.workspace_input == b.workspace_input
        && a.tools == b.tools
}
