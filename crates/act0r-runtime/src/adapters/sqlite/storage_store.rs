use super::*;
use crate::domain::{storage::*, teams::TeamStatus};

struct CleanupGuard {
    tx: sqlx::Transaction<'static, sqlx::Sqlite>,
    workspace: StorageWorkspace,
}
impl CacheCleanupGuard for CleanupGuard {
    fn workspace(&self) -> &StorageWorkspace {
        &self.workspace
    }
    fn finish(self: Box<Self>) -> PortFuture<'static, ()> {
        Box::pin(async move { self.tx.commit().await.map_err(storage) })
    }
}
impl SqliteStore {
    pub(super) async fn session_storage_state(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<(crate::domain::teams::TeamRecord, Option<String>), Error> {
        let team: Option<String> =
            sqlx::query_scalar("SELECT team_id FROM harness_sessions WHERE id=?")
                .bind(session.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        let record = Self::load_team(
            conn,
            team.ok_or(Error::SessionNotFound(session))?
                .parse()
                .map_err(|_| DomainError::InvalidState)?,
        )
        .await?;
        let mut reason = if record.status != TeamStatus::Ready {
            Some("session setup is incomplete".to_string())
        } else {
            None
        };
        if reason.is_none()
            && let Err(error) = Self::check_team_actors(conn, &record).await
        {
            reason = Some(format!("actor configuration cannot be verified: {error}"));
        }
        let unsettled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_turns t JOIN harness_unsettled_turns u ON u.task_id=t.task_id WHERE t.session_id=?)")
            .bind(session.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        if unsettled {
            reason = Some(
                "session has active work, a durable wait, or pending completion/recovery".into(),
            );
        }
        if Self::summary_pending(conn, session).await? {
            reason = Some("session has a pending conversation summary".into());
        }
        let applications: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs r JOIN harness_turns t ON t.task_id=json_extract(r.payload,'$.input.task_id') WHERE t.session_id=? AND json_extract(r.payload,'$.workflow.name') IN ('workspace.apply','project.verify') AND r.status NOT IN ('succeeded','failed','budget_exhausted','cancelled'))")
            .bind(session.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        if applications {
            reason = Some(
                "session has a pending patch application, project verification or recovery".into(),
            );
        }
        for member in &record.members {
            let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_mailbox WHERE recipient=? AND status='queued') OR EXISTS(SELECT 1 FROM agent_reminders WHERE agent_id=? AND status='scheduled') OR EXISTS(SELECT 1 FROM agent_tasks WHERE agent_id=? AND json_extract(payload,'$.status') NOT IN ('succeeded','failed','rejected','budget_exhausted','cancelled')) OR EXISTS(SELECT 1 FROM agent_runs a JOIN runs r ON r.id=a.run_id WHERE a.agent_id=? AND (a.notified=0 OR r.status NOT IN ('succeeded','failed','budget_exhausted','cancelled'))) OR EXISTS(SELECT 1 FROM agent_actors WHERE id=? AND status<>'ready')")
                .bind(member.agent_id.to_string()).bind(member.agent_id.to_string()).bind(member.agent_id.to_string()).bind(member.agent_id.to_string()).bind(member.agent_id.to_string())
                .fetch_one(&mut *conn).await.map_err(storage)?;
            if busy && reason.is_none() {
                reason = Some(
                    "a session actor has queued work, a reminder, a hold or unsettled execution"
                        .into(),
                );
            }
        }
        for member in &record.members {
            if let Some(workspace) = &member.workspace {
                let referenced: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs r,json_tree(json_extract(r.payload,'$.input')) j WHERE r.status NOT IN ('succeeded','failed','budget_exhausted','cancelled') AND j.type='text' AND j.value=?)")
                    .bind(&workspace.path).fetch_one(&mut *conn).await.map_err(storage)?;
                if referenced {
                    reason = Some("workspace is referenced by an unfinished Run".into());
                }
            }
        }
        if reason.is_none() {
            let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_verification_repairs WHERE (root_session_id=? OR source_session_id=?) AND admitted=0 AND cancelled=0) OR EXISTS(SELECT 1 FROM harness_project_refreshes WHERE (root_session_id=? OR source_session_id=?) AND admitted=0 AND cancelled=0)")
                .bind(session.to_string()).bind(session.to_string()).bind(session.to_string()).bind(session.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            if pending {
                reason = Some("project workspace preparation is pending".into());
            }
        }
        Ok((record, reason))
    }
    async fn storage_owners(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<Vec<StorageWorkspace>, Error> {
        let (record, reason) = Self::session_storage_state(conn, session).await?;
        let mut owners = Vec::new();
        for member in record.members {
            if let Some(workspace) = member.workspace {
                owners.push(StorageWorkspace {
                    session_id: session,
                    agent_id: member.agent_id,
                    member: member.name,
                    workspace: workspace.path,
                    blocked_reason: reason.clone(),
                });
            }
        }
        Ok(owners)
    }
}
impl StorageStorePort for SqliteStore {
    fn storage_workspaces(&self) -> PortFuture<'_, (Vec<StorageWorkspace>, bool)> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let sessions: Vec<String> = sqlx::query_scalar("SELECT s.id FROM harness_sessions s JOIN agent_teams t ON t.id=s.team_id ORDER BY json_extract(t.payload,'$.created_at') DESC,s.id DESC LIMIT 65")
                .fetch_all(&mut *tx).await.map_err(storage)?;
            let mut truncated = sessions.len() > 64;
            let mut owners = Vec::new();
            for session in sessions.into_iter().take(64) {
                for owner in Self::storage_owners(
                    &mut tx,
                    session.parse().map_err(|_| DomainError::InvalidState)?,
                )
                .await?
                {
                    owners.push(owner);
                }
            }
            truncated |= owners.len() > 64;
            owners.truncate(64);
            tx.commit().await.map_err(storage)?;
            Ok((owners, truncated))
        })
    }
    fn lock_cache_cleanup(
        &self,
        session: SessionId,
        agent: AgentId,
    ) -> PortFuture<'_, Box<dyn CacheCleanupGuard>> {
        Box::pin(async move {
            // Keep admission exclusion alive throughout deletion, including HTTP cancellation.
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let workspace = Self::storage_owners(&mut tx, session)
                .await?
                .into_iter()
                .find(|w| w.agent_id == agent)
                .ok_or(Error::AgentNotFound(agent))?;
            if let Some(reason) = &workspace.blocked_reason {
                return Err(Error::StorageConflict(reason.clone()));
            }
            Ok(Box::new(CleanupGuard { tx, workspace }) as Box<dyn CacheCleanupGuard>)
        })
    }
}
