use super::*;
use crate::domain::sessions::*;

impl SqliteStore {
    async fn metadata_owner(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<SessionId, Error> {
        let root:String=sqlx::query_scalar("SELECT COALESCE((SELECT source_session_id FROM harness_project_continuations WHERE coding_session_id=?),?)")
            .bind(session.to_string()).bind(session.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        root.parse().map_err(|_| DomainError::InvalidState.into())
    }
    pub(super) async fn load_session_metadata(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<SessionMetadata, Error> {
        let row = sqlx::query("SELECT title,archived,revision,last_request FROM harness_session_catalog WHERE session_id=?")
            .bind(session.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?.ok_or(Error::SessionNotFound(session))?;
        let last: Option<String> = row.try_get("last_request").map_err(storage)?;
        Ok(SessionMetadata {
            title: row.try_get("title").map_err(storage)?,
            archived: row.try_get("archived").map_err(storage)?,
            revision: row.try_get::<i64, _>("revision").map_err(storage)? as u64,
            last_request: last.map(|s| serde_json::from_str(&s)).transpose()?,
        })
    }
    pub(super) async fn session_archive_reason(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<Option<String>, Error> {
        let ready: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_sessions WHERE id=?)")
                .bind(session.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
        if !ready {
            return Ok(Some("session setup is incomplete".into()));
        }
        let (_, reason) = Self::session_storage_state(conn, session).await?;
        if reason.is_none() {
            let children:Vec<String>=sqlx::query_scalar("SELECT coding_session_id FROM harness_project_continuations WHERE source_session_id=?")
                .bind(session.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
            for child in children {
                let (_, reason) = Self::session_storage_state(
                    conn,
                    child.parse().map_err(|_| DomainError::InvalidState)?,
                )
                .await?;
                if reason.is_some() {
                    return Ok(Some(
                        "linked coding work, patch application or project verification is unfinished".into(),
                    ));
                }
            }
            let pending:Vec<String>=sqlx::query_scalar("SELECT h.task_id FROM harness_turns h JOIN agent_tasks t ON t.id=h.task_id JOIN runs r ON r.id=t.run_id WHERE h.session_id=? AND r.status='succeeded' AND json_type(r.payload,'$.result.project_setup')='object' AND NOT EXISTS(SELECT 1 FROM harness_project_continuations c WHERE c.source_task_id=h.task_id)")
                .bind(session.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
            for task in pending {
                let id = project_setup::coding_start_id(
                    session,
                    task.parse().map_err(|_| DomainError::InvalidState)?,
                );
                let prepared:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_teams t WHERE t.name=? AND NOT EXISTS(SELECT 1 FROM harness_sessions s WHERE s.team_id=t.id))")
                    .bind(format!("{}{id}",harness::SESSION_PREFIX)).fetch_one(&mut *conn).await.map_err(storage)?;
                if prepared {
                    return Ok(Some("reviewed coding setup is incomplete; retry setup acceptance before archiving".into()));
                }
            }
        }
        Ok(reason)
    }
    pub(super) async fn read_session_metadata(
        &self,
        session: SessionId,
    ) -> Result<SessionMetadataView, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let session = Self::metadata_owner(&mut tx, session).await?;
        let metadata = Self::load_session_metadata(&mut tx, session).await?;
        let archive_blocked_reason = Self::session_archive_reason(&mut tx, session).await?;
        tx.commit().await.map_err(storage)?;
        Ok(SessionMetadataView {
            session_id: session,
            metadata,
            archive_blocked_reason,
        })
    }
    pub(super) async fn change_session_metadata(
        &self,
        session: SessionId,
        request: SessionUpdate,
    ) -> Result<SessionUpdateReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let session = Self::metadata_owner(&mut tx, session).await?;
        let mut metadata = Self::load_session_metadata(&mut tx, session).await?;
        let archive_blocked_reason = Self::session_archive_reason(&mut tx, session).await?;
        let replayed = metadata.apply(&request, archive_blocked_reason.as_deref())?;
        if !replayed {
            sqlx::query("UPDATE harness_session_catalog SET title=?,archived=?,revision=?,last_request=? WHERE session_id=?")
                .bind(&metadata.title).bind(metadata.archived).bind(metadata.revision as i64)
                .bind(serde_json::to_string(&request)?).bind(session.to_string())
                .execute(&mut *tx).await.map_err(storage)?;
            if matches!(request.change, SessionChange::Restore) {
                sqlx::query("UPDATE harness_session_catalog SET archived=0,revision=revision+1,last_request=NULL WHERE archived=1 AND session_id IN (SELECT coding_session_id FROM harness_project_continuations WHERE source_session_id=?)")
                    .bind(session.to_string()).execute(&mut *tx).await.map_err(storage)?;
            }
        }
        tx.commit().await.map_err(storage)?;
        Ok(SessionUpdateReceipt {
            view: SessionMetadataView {
                session_id: session,
                metadata,
                archive_blocked_reason,
            },
            replayed,
        })
    }
}
