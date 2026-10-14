use super::*;
use crate::domain::{harness::*, project_refresh::*};

impl SqliteStore {
    async fn refresh_source(
        conn: &mut SqliteConnection,
        scope: &RefreshScope,
    ) -> Result<RefreshContext, Error> {
        let context = Self::harness_context(conn, scope.session_id, scope.task_id).await?;
        if context.coding.is_none() {
            return Err(DomainError::InvalidConfiguration(
                "project refresh requires a coding session".into(),
            )
            .into());
        }
        let row=sqlx::query("SELECT s.team_id,h.prompt,COALESCE(json_extract(r.payload,'$.result'),json_extract(r.payload,'$.error'),'null') AS result FROM harness_sessions s JOIN harness_turns h ON h.session_id=s.id JOIN agent_tasks t ON t.id=h.task_id LEFT JOIN runs r ON r.id=t.run_id WHERE s.id=? AND h.task_id=?")
            .bind(scope.session_id.to_string()).bind(scope.task_id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?.ok_or(Error::AgentTaskNotFound(scope.task_id))?;
        let team: String = row.try_get("team_id").map_err(storage)?;
        let record =
            Self::load_team(conn, team.parse().map_err(|_| DomainError::InvalidState)?).await?;
        record.validate_assembly()?;
        let workspace = record
            .planner()?
            .workspace
            .as_ref()
            .ok_or(DomainError::InvalidState)?
            .path
            .clone();
        Ok(RefreshContext {
            root: Self::conversation_root(conn, scope.session_id).await?,
            scope: scope.clone(),
            original: record.spec,
            workspace,
            previous_prompt: row.try_get("prompt").map_err(storage)?,
            previous_result: row.try_get("result").map_err(storage)?,
        })
    }
    async fn saved_refresh(
        conn: &mut SqliteConnection,
        scope: &RefreshScope,
        request: MessageId,
    ) -> Result<Option<RefreshIntent>, Error> {
        let raw: Option<String> =
            sqlx::query_scalar("SELECT payload FROM harness_project_refreshes WHERE request_id=?")
                .bind(request.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        let intent = raw
            .map(|raw| serde_json::from_str::<RefreshIntent>(&raw))
            .transpose()?;
        if intent.as_ref().is_some_and(|i| &i.scope != scope) {
            return Err(DomainError::ConflictingMessage.into());
        }
        Ok(intent)
    }
    async fn save_refresh(
        conn: &mut SqliteConnection,
        intent: &RefreshIntent,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE harness_project_refreshes SET payload=?,admitted=?,cancelled=? WHERE request_id=?")
            .bind(serde_json::to_string(intent)?).bind(intent.receipt.is_some()).bind(intent.cancelled).bind(intent.request.request_id.to_string()).execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
    pub(super) async fn read_refresh_context(
        &self,
        scope: RefreshScope,
    ) -> Result<RefreshContext, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let context = Self::refresh_source(&mut tx, &scope).await?;
        Self::coding_stage_boundary(&mut tx, scope.session_id, scope.task_id, None).await?;
        tx.commit().await.map_err(storage)?;
        Ok(context)
    }
    pub(super) async fn read_refresh_intent(
        &self,
        scope: RefreshScope,
        request: MessageId,
    ) -> Result<Option<RefreshIntent>, Error> {
        validate_id(request)?;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        Self::refresh_source(&mut tx, &scope).await?;
        let intent = Self::saved_refresh(&mut tx, &scope, request).await?;
        tx.commit().await.map_err(storage)?;
        Ok(intent)
    }
    pub(super) async fn read_project_refresh(
        &self,
        scope: RefreshScope,
    ) -> Result<RefreshView, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let context = Self::refresh_source(&mut tx, &scope).await?;
        let raw:Option<String>=sqlx::query_scalar("SELECT payload FROM harness_project_refreshes WHERE source_session_id=? AND source_task_id=? ORDER BY rowid DESC LIMIT 1")
            .bind(scope.session_id.to_string()).bind(scope.task_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?;
        let intent = raw
            .map(|s| serde_json::from_str::<RefreshIntent>(&s))
            .transpose()?;
        let boundary = Self::coding_stage_boundary(
            &mut tx,
            scope.session_id,
            scope.task_id,
            intent
                .as_ref()
                .filter(|i| !i.cancelled && i.receipt.is_none())
                .map(|i| i.request.request_id),
        )
        .await;
        let reason = match boundary {
            Ok(_) => None,
            Err(Error::Domain(DomainError::SessionConflict(reason))) => Some(reason),
            Err(error) => return Err(error),
        };
        let view = RefreshView {
            coding: project_setup::coding_settings(&context.original)?,
            can_start: reason.is_none(),
            reason,
            preparing: intent
                .as_ref()
                .is_some_and(|i| !i.cancelled && i.receipt.is_none()),
            request: intent.as_ref().map(|i| i.request.clone()),
            receipt: intent.as_ref().and_then(|i| i.receipt.clone()),
            cancelled: intent.as_ref().is_some_and(|i| i.cancelled),
            error: intent.and_then(|i| i.error),
        };
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    pub(super) async fn prepare_project_refresh(
        &self,
        scope: RefreshScope,
        request: RefreshRequest,
        snapshot: RefreshSnapshot,
    ) -> Result<RefreshIntent, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let context = Self::refresh_source(&mut tx, &scope).await?;
        if let Some(intent) = Self::saved_refresh(&mut tx, &scope, request.request_id).await? {
            if intent.request != request {
                return Err(DomainError::ConflictingMessage.into());
            }
            if intent.cancelled {
                return Err(DomainError::SessionConflict(
                    "refresh preparation was cancelled".into(),
                )
                .into());
            }
            tx.commit().await.map_err(storage)?;
            return Ok(intent);
        }
        let reused:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_verification_repairs WHERE request_id=?) OR EXISTS(SELECT 1 FROM harness_turns WHERE task_id=?)")
            .bind(request.request_id.to_string()).bind(request.request_id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
        if reused {
            return Err(DomainError::ConflictingMessage.into());
        }
        Self::coding_stage_boundary(&mut tx, scope.session_id, scope.task_id, None).await?;
        let (source_sequence, phase) =
            Self::next_coding_stage(&mut tx, context.root, scope.session_id, scope.task_id).await?;
        let (spec, model_prompt) = refresh_spec(&context, &request, &snapshot)?;
        let intent = RefreshIntent {
            scope,
            root: context.root,
            request,
            source_sequence,
            phase,
            snapshot,
            spec,
            model_prompt,
            cancelled: false,
            receipt: None,
            error: None,
        };
        sqlx::query("INSERT INTO harness_project_refreshes(request_id,root_session_id,source_session_id,source_task_id,payload) VALUES(?,?,?,?,?)")
            .bind(intent.request.request_id.to_string()).bind(intent.root.to_string()).bind(intent.scope.session_id.to_string()).bind(intent.scope.task_id.to_string()).bind(serde_json::to_string(&intent)?).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(intent)
    }
    pub(super) async fn publish_project_refresh(
        &self,
        supplied: RefreshIntent,
        team: TeamId,
        now: i64,
    ) -> Result<HarnessReceipt, Error> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let mut intent = Self::saved_refresh(&mut tx, &supplied.scope, supplied.request.request_id)
            .await?
            .ok_or(DomainError::InvalidState)?;
        if let Some(receipt) = intent.receipt {
            return Ok(receipt);
        }
        if intent.cancelled {
            return Err(DomainError::SessionConflict(
                "refresh preparation was cancelled before admission".into(),
            )
            .into());
        }
        Self::coding_stage_boundary(
            &mut tx,
            intent.scope.session_id,
            intent.scope.task_id,
            Some(intent.request.request_id),
        )
        .await?;
        Self::refresh_source(&mut tx, &intent.scope).await?;
        Self::require_pristine_stage(&mut tx, team, &intent.spec, &intent.snapshot.source_digest)
            .await?;
        let session: SessionId = intent
            .request
            .request_id
            .to_string()
            .parse()
            .map_err(|_| DomainError::InvalidState)?;
        sqlx::query("INSERT INTO harness_sessions(id,team_id,can_continue) VALUES(?,?,1)")
            .bind(session.to_string())
            .bind(team.to_string())
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let receipt = Self::admit_harness_turn(
            &mut tx,
            session,
            team,
            HarnessTurnRequest {
                message_id: intent.request.request_id,
                prompt: intent.model_prompt.clone(),
            },
            true,
            now,
        )
        .await?;
        sqlx::query("INSERT INTO harness_project_continuations(source_session_id,source_task_id,coding_session_id,coding_task_id,source_sequence,phase,kind) VALUES(?,?,?,?,?,?,'refresh')")
            .bind(intent.root.to_string()).bind(intent.scope.task_id.to_string()).bind(session.to_string()).bind(intent.request.request_id.to_string()).bind(intent.source_sequence as i64).bind(i64::from(intent.phase)).execute(&mut *tx).await.map_err(storage)?;
        intent.receipt = Some(receipt.clone());
        intent.error = None;
        Self::save_refresh(&mut tx, &intent).await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    pub(super) async fn abandon_project_refresh(
        &self,
        scope: RefreshScope,
        request: MessageId,
    ) -> Result<(), Error> {
        validate_id(request)?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        Self::refresh_source(&mut tx, &scope).await?;
        let mut intent = Self::saved_refresh(&mut tx, &scope, request).await?.ok_or(
            DomainError::InvalidConfiguration("no refresh preparation is saved".into()),
        )?;
        if intent.receipt.is_some() {
            return Err(DomainError::SessionConflict(
                "refresh is already admitted; use its request Stop control".into(),
            )
            .into());
        }
        intent.cancelled = true;
        Self::save_refresh(&mut tx, &intent).await?;
        tx.commit().await.map_err(storage)?;
        Ok(())
    }
    pub(super) async fn record_project_refresh_error(
        &self,
        scope: RefreshScope,
        request: MessageId,
        error: String,
    ) -> Result<(), Error> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        if let Some(mut intent) = Self::saved_refresh(&mut tx, &scope, request).await?
            && !intent.cancelled
            && intent.receipt.is_none()
        {
            intent.error = Some(error.chars().take(2000).collect());
            Self::save_refresh(&mut tx, &intent).await?;
        }
        tx.commit().await.map_err(storage)?;
        Ok(())
    }
}
