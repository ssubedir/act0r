use super::*;
use crate::domain::{harness::*, project_verification::*, verification_repair::*};
use serde_json::json;

const PREPARING: &str = "project workspace preparation is pending";
impl SqliteStore {
    async fn repair_source(
        conn: &mut SqliteConnection,
        scope: &RepairScope,
    ) -> Result<(Run, teams::TeamSpec, String), Error> {
        let (context, applied) = Self::verification_application(
            conn,
            scope.session_id,
            scope.task_id,
            scope.application_id,
        )
        .await?;
        let verification = Self::load(conn, scope.verification_id).await?;
        if verification.record().workflow != workflow()
            || verification.record().input["task_id"] != json!(scope.task_id)
            || verification.record().input["application_id"] != json!(scope.application_id)
            || verification.record().input["target"] != applied.record().state["preview"]["target"]
            || verification.record().input["command"]
                != json!(
                    context
                        .coding
                        .as_ref()
                        .ok_or(DomainError::InvalidState)?
                        .test_command
                )
        {
            return Err(Error::NotFound(scope.verification_id));
        }
        if applied.record().status != RunStatus::Succeeded
            || applied
                .record()
                .result
                .as_ref()
                .is_none_or(|v| v["status"] != "applied")
            || verification.record().status != RunStatus::Succeeded
        {
            return Err(DomainError::InvalidConfiguration(
                "repair requires a settled failed project verification".into(),
            )
            .into());
        }
        let request = VerificationRequest {
            input: serde_json::from_value(verification.record().input.clone())?,
            snapshot: serde_json::from_value(verification.record().state["snapshot"].clone())?,
        };
        let report: VerificationReport = serde_json::from_value(
            verification
                .record()
                .result
                .clone()
                .ok_or(DomainError::InvalidState)?,
        )?;
        report.validate_for(&request)?;
        if report.status != VerificationStatus::Failed {
            return Err(DomainError::InvalidConfiguration(
                "only real failing tests against stable project inputs can start a repair".into(),
            )
            .into());
        }
        let row = sqlx::query("SELECT s.team_id,h.prompt FROM harness_sessions s JOIN harness_turns h ON h.session_id=s.id WHERE s.id=? AND h.task_id=?")
            .bind(scope.session_id.to_string()).bind(scope.task_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        let team: String = row.try_get("team_id").map_err(storage)?;
        let team =
            Self::load_team(conn, team.parse().map_err(|_| DomainError::InvalidState)?).await?;
        Ok((
            verification,
            team.spec,
            row.try_get("prompt").map_err(storage)?,
        ))
    }
    async fn repair_intent(
        conn: &mut SqliteConnection,
        scope: &RepairScope,
    ) -> Result<Option<RepairIntent>, Error> {
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT payload FROM harness_verification_repairs WHERE verification_id=?",
        )
        .bind(scope.verification_id.to_string())
        .fetch_optional(&mut *conn)
        .await
        .map_err(storage)?;
        let intent = raw
            .map(|s| serde_json::from_str::<RepairIntent>(&s))
            .transpose()?;
        if intent.as_ref().is_some_and(|v| &v.scope != scope) {
            return Err(Error::NotFound(scope.verification_id));
        }
        Ok(intent)
    }
    async fn repair_boundary(
        conn: &mut SqliteConnection,
        scope: &RepairScope,
        except: Option<MessageId>,
    ) -> Result<SessionId, Error> {
        Self::coding_stage_boundary(conn, scope.session_id, scope.task_id, except).await
    }
    pub(super) async fn coding_stage_boundary(
        conn: &mut SqliteConnection,
        session: SessionId,
        task: MessageId,
        except: Option<MessageId>,
    ) -> Result<SessionId, Error> {
        let root = Self::conversation_root(conn, session).await?;
        if Self::conversation_archived(conn, session).await? {
            return Err(DomainError::SessionConflict(
                "restore the conversation before starting a fresh workspace".into(),
            )
            .into());
        }
        let active = Self::active_conversation_turn(conn, root).await?;
        if active.session_id != session || active.task_id != task {
            return Err(DomainError::SessionConflict(
                "continue from the latest settled turn; a later request is already saved".into(),
            )
            .into());
        }
        let children: Vec<String> = sqlx::query_scalar(
            "SELECT coding_session_id FROM harness_project_continuations WHERE source_session_id=?",
        )
        .bind(root.to_string())
        .fetch_all(&mut *conn)
        .await
        .map_err(storage)?;
        for session in std::iter::once(root.to_string()).chain(children) {
            let (_, reason) = Self::session_storage_state(
                conn,
                session.parse().map_err(|_| DomainError::InvalidState)?,
            )
            .await?;
            if let Some(reason) = reason
                && reason != PREPARING
            {
                return Err(DomainError::SessionConflict(format!(
                    "settle existing conversation work before preparing a fresh workspace: {reason}"
                ))
                .into());
            }
        }
        let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_verification_repairs WHERE root_session_id=? AND admitted=0 AND cancelled=0 AND (? IS NULL OR request_id<>?)) OR EXISTS(SELECT 1 FROM harness_project_refreshes WHERE root_session_id=? AND admitted=0 AND cancelled=0 AND (? IS NULL OR request_id<>?))")
            .bind(root.to_string()).bind(except.map(|id|id.to_string())).bind(except.map(|id|id.to_string())).bind(root.to_string()).bind(except.map(|id|id.to_string())).bind(except.map(|id|id.to_string())).fetch_one(&mut *conn).await.map_err(storage)?;
        if pending {
            return Err(DomainError::SessionConflict(
                "continue or cancel the saved workspace preparation first".into(),
            )
            .into());
        }
        Ok(root)
    }
    pub(super) async fn next_coding_stage(
        conn: &mut SqliteConnection,
        root: SessionId,
        session: SessionId,
        task: MessageId,
    ) -> Result<(u64, u8), Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM harness_project_continuations WHERE source_session_id=?",
        )
        .bind(root.to_string())
        .fetch_one(&mut *conn)
        .await
        .map_err(storage)?;
        let stage:Option<(i64,i64)>=sqlx::query_as("SELECT source_sequence,phase FROM harness_project_continuations WHERE coding_session_id=?").bind(session.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        let (sequence, phase) = if let Some((sequence, phase)) = stage {
            (sequence, phase + 1)
        } else {
            (
                sqlx::query_scalar::<_, i64>("SELECT sequence FROM harness_turns WHERE task_id=?")
                    .bind(task.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?,
                1,
            )
        };
        if count >= 64 || phase > 64 {
            return Err(DomainError::SessionConflict(
                "conversation reached its 64 coding stage limit".into(),
            )
            .into());
        }
        Ok((sequence as u64, phase as u8))
    }
    pub(super) async fn require_pristine_stage(
        conn: &mut SqliteConnection,
        team: TeamId,
        spec: &teams::TeamSpec,
        digest: &str,
    ) -> Result<(), Error> {
        let record = Self::load_team(conn, team).await?;
        if &record.spec != spec
            || record.members.iter().any(|m| {
                m.needs_workspace()
                    && m.workspace
                        .as_ref()
                        .is_none_or(|w| w.snapshot_digest != digest)
            })
        {
            return Err(DomainError::SessionConflict(
                "prepared workspace differs from the reviewed source snapshot".into(),
            )
            .into());
        }
        for member in &record.members {
            let used:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_tasks WHERE agent_id=?) OR EXISTS(SELECT 1 FROM agent_mailbox WHERE recipient=?) OR EXISTS(SELECT 1 FROM agent_runs WHERE agent_id=?)")
                .bind(member.agent_id.to_string()).bind(member.agent_id.to_string()).bind(member.agent_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            if used {
                return Err(DomainError::SessionConflict(
                    "prepared actors already received work; cancel preparation and review again"
                        .into(),
                )
                .into());
            }
        }
        Ok(())
    }
    async fn save_repair_intent(
        conn: &mut SqliteConnection,
        intent: &RepairIntent,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE harness_verification_repairs SET payload=?,admitted=?,cancelled=? WHERE verification_id=? AND request_id=?")
            .bind(serde_json::to_string(intent)?).bind(intent.receipt.is_some()).bind(intent.cancelled)
            .bind(intent.scope.verification_id.to_string()).bind(intent.request_id.to_string()).execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
    pub(super) async fn read_verification_repair(
        &self,
        scope: RepairScope,
    ) -> Result<RepairView, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        Self::repair_source(&mut tx, &scope).await?;
        let intent = Self::repair_intent(&mut tx, &scope).await?;
        let (can_start, reason) = if intent.as_ref().is_some_and(|i| i.cancelled) {
            (false,Some("Preparation was cancelled. Create a new project verification before another repair.".into()))
        } else if intent.as_ref().is_some_and(|i| i.receipt.is_some()) {
            (false, None)
        } else {
            match Self::repair_boundary(&mut tx, &scope, intent.as_ref().map(|i| i.request_id))
                .await
            {
                Ok(_) => (true, None),
                Err(Error::Domain(DomainError::SessionConflict(reason))) => (false, Some(reason)),
                Err(e) => return Err(e),
            }
        };
        let view = RepairView {
            can_start,
            reason,
            request_id: intent.as_ref().map(|i| i.request_id),
            preparing: intent
                .as_ref()
                .is_some_and(|i| !i.cancelled && i.receipt.is_none()),
            cancelled: intent.as_ref().is_some_and(|i| i.cancelled),
            receipt: intent.as_ref().and_then(|i| i.receipt.clone()),
            error: intent.and_then(|i| i.error),
        };
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    pub(super) async fn prepare_verification_repair(
        &self,
        scope: RepairScope,
        request: RepairRequest,
    ) -> Result<RepairIntent, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let (run, original, goal) = Self::repair_source(&mut tx, &scope).await?;
        let reused:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_verification_repairs WHERE request_id=? AND verification_id<>?) OR EXISTS(SELECT 1 FROM harness_project_refreshes WHERE request_id=?)")
            .bind(request.request_id.to_string()).bind(scope.verification_id.to_string()).bind(request.request_id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
        if reused {
            return Err(DomainError::ConflictingMessage.into());
        }
        if let Some(intent) = Self::repair_intent(&mut tx, &scope).await? {
            if intent.request_id != request.request_id {
                return Err(DomainError::ConflictingMessage.into());
            }
            if intent.cancelled {
                return Err(DomainError::SessionConflict(
                    "this repair preparation was cancelled".into(),
                )
                .into());
            }
            if intent.receipt.is_none() {
                Self::repair_boundary(&mut tx, &scope, Some(request.request_id)).await?;
            }
            tx.commit().await.map_err(storage)?;
            return Ok(intent);
        }
        let root = Self::repair_boundary(&mut tx, &scope, None).await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM harness_project_continuations WHERE source_session_id=?",
        )
        .bind(root.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
        if count >= 64 {
            return Err(DomainError::InvalidConfiguration(
                "conversation already has 64 coding stages".into(),
            )
            .into());
        }
        let stage: Option<(i64,i64)> = sqlx::query_as("SELECT source_sequence,phase FROM harness_project_continuations WHERE coding_session_id=?")
            .bind(scope.session_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?;
        let (sequence, phase) = if let Some((sequence, phase)) = stage {
            (sequence, phase + 1)
        } else {
            let sequence: i64 =
                sqlx::query_scalar("SELECT sequence FROM harness_turns WHERE task_id=?")
                    .bind(scope.task_id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            (sequence, 1)
        };
        if phase > 64 {
            return Err(DomainError::InvalidConfiguration(
                "conversation reached its repair stage limit".into(),
            )
            .into());
        }
        let report: VerificationReport = serde_json::from_value(
            run.record()
                .result
                .clone()
                .ok_or(DomainError::InvalidState)?,
        )?;
        let (spec, prompt) = repair_spec(&request, &scope, &original, &goal, &report)?;
        let intent = RepairIntent {
            scope,
            request_id: request.request_id,
            root,
            source_sequence: sequence as u64,
            phase: phase as u8,
            snapshot_digest: run.record().state["snapshot"]["digest"]
                .as_str()
                .ok_or(DomainError::InvalidState)?
                .into(),
            spec,
            prompt,
            cancelled: false,
            receipt: None,
            error: None,
        };
        sqlx::query("INSERT INTO harness_verification_repairs(verification_id,request_id,root_session_id,source_session_id,payload) VALUES(?,?,?,?,?)")
            .bind(intent.scope.verification_id.to_string()).bind(intent.request_id.to_string()).bind(root.to_string()).bind(intent.scope.session_id.to_string()).bind(serde_json::to_string(&intent)?).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(intent)
    }
    pub(super) async fn publish_verification_repair(
        &self,
        supplied: RepairIntent,
        team: TeamId,
        now: i64,
    ) -> Result<HarnessReceipt, Error> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let mut intent = Self::repair_intent(&mut tx, &supplied.scope)
            .await?
            .ok_or(DomainError::InvalidState)?;
        if intent.request_id != supplied.request_id {
            return Err(DomainError::ConflictingMessage.into());
        }
        if let Some(receipt) = intent.receipt {
            return Ok(receipt);
        }
        if intent.cancelled {
            return Err(DomainError::SessionConflict(
                "repair preparation was cancelled before admission".into(),
            )
            .into());
        }
        Self::repair_boundary(&mut tx, &intent.scope, Some(intent.request_id)).await?;
        Self::repair_source(&mut tx, &intent.scope).await?;
        let record = Self::load_team(&mut tx, team).await?;
        if record.spec != intent.spec
            || record.members.iter().any(|m| {
                m.needs_workspace()
                    && m.workspace
                        .as_ref()
                        .is_none_or(|w| w.snapshot_digest != intent.snapshot_digest)
            })
        {
            return Err(DomainError::SessionConflict("prepared project copy differs from the failed verification; cancel preparation and verify the current project again".into()).into());
        }
        for member in &record.members {
            let used:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_tasks WHERE agent_id=?) OR EXISTS(SELECT 1 FROM agent_mailbox WHERE recipient=?) OR EXISTS(SELECT 1 FROM agent_runs WHERE agent_id=?)")
                .bind(member.agent_id.to_string()).bind(member.agent_id.to_string()).bind(member.agent_id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
            if used {
                return Err(DomainError::SessionConflict("prepared repair actors already received work; cancel preparation and verify the current project again".into()).into());
            }
        }
        let session: SessionId = intent
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
                message_id: intent.request_id,
                prompt: intent.prompt.clone(),
            },
            true,
            now,
        )
        .await?;
        sqlx::query("INSERT INTO harness_project_continuations(source_session_id,source_task_id,coding_session_id,coding_task_id,source_sequence,phase,kind) VALUES(?,?,?,?,?,?,'repair')")
            .bind(intent.root.to_string()).bind(intent.scope.task_id.to_string()).bind(session.to_string()).bind(intent.request_id.to_string()).bind(intent.source_sequence as i64).bind(i64::from(intent.phase)).execute(&mut *tx).await.map_err(storage)?;
        intent.receipt = Some(receipt.clone());
        intent.error = None;
        Self::save_repair_intent(&mut tx, &intent).await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    pub(super) async fn abandon_verification_repair(
        &self,
        scope: RepairScope,
        request: RepairRequest,
    ) -> Result<(), Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        Self::repair_source(&mut tx, &scope).await?;
        let mut intent = Self::repair_intent(&mut tx, &scope).await?.ok_or(
            DomainError::InvalidConfiguration("no repair preparation is saved".into()),
        )?;
        if intent.request_id != request.request_id {
            return Err(DomainError::ConflictingMessage.into());
        }
        if intent.receipt.is_some() {
            return Err(DomainError::SessionConflict(
                "repair is already admitted; use its request Stop control".into(),
            )
            .into());
        }
        intent.cancelled = true;
        Self::save_repair_intent(&mut tx, &intent).await?;
        tx.commit().await.map_err(storage)?;
        Ok(())
    }
    pub(super) async fn record_verification_repair_error(
        &self,
        scope: RepairScope,
        request: MessageId,
        error: String,
    ) -> Result<(), Error> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        if let Some(mut intent) = Self::repair_intent(&mut tx, &scope).await?
            && intent.request_id == request
            && !intent.cancelled
            && intent.receipt.is_none()
        {
            intent.error = Some(error.chars().take(2000).collect());
            Self::save_repair_intent(&mut tx, &intent).await?;
        }
        tx.commit().await.map_err(storage)?;
        Ok(())
    }
}
