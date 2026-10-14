use super::*;
use crate::domain::{permissions::*, teams::TeamRecord};

impl SqliteStore {
    async fn permission_state(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<PermissionState, Error> {
        let payload: Option<String> = sqlx::query_scalar(
            "SELECT payload FROM harness_session_permissions WHERE session_id=?",
        )
        .bind(session.to_string())
        .fetch_optional(&mut *conn)
        .await
        .map_err(storage)?;
        Ok(payload
            .map(|p| serde_json::from_str(&p))
            .transpose()?
            .unwrap_or_default())
    }
    pub(super) async fn permission_team(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
    ) -> Result<TeamRecord, Error> {
        Self::harness_context(conn, session, turn).await?;
        let team: String = sqlx::query_scalar("SELECT team_id FROM harness_sessions WHERE id=?")
            .bind(session.to_string())
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
        let team = team
            .parse()
            .map_err(|_| Error::Storage("invalid session team identity".into()))?;
        let (record, root) = Self::check_team_task(conn, team, turn).await?;
        if root != turn {
            return Err(DomainError::SessionConflict(
                "permission needs the session's root planner turn".into(),
            )
            .into());
        }
        Ok(record)
    }
    async fn eligible_test_wait(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
    ) -> Result<Option<(EligibleTestWait, AgentId, Run)>, Error> {
        let team = Self::permission_team(conn, session, turn).await?;
        let task = Self::load_agent_task(conn, turn).await?;
        if Self::load_session_metadata(conn, session).await?.archived
            || task.record().parent_id.is_some()
            || task.record().cancel_requested
            || task.record().status.is_terminal()
            || Self::goal_receipt_for_task(conn, turn).await?.is_some()
        {
            return Ok(None);
        }
        let Some(id) = task.record().run_id else {
            return Ok(None);
        };
        let run = Self::load(conn, id).await?;
        let Ok(request) = run.pending_test_request() else {
            return Ok(None);
        };
        let planner = team.planner()?;
        let config: planner::PlannerConfig =
            serde_json::from_value(planner.actor_config()?["input"].clone())?;
        // The saved actor pins, not the model action or HTTP body, select authority.
        if test_request(&config)? != request || task.record().agent_id != planner.agent_id {
            return Ok(None);
        }
        let wait_id = run
            .record()
            .wait
            .as_ref()
            .ok_or(DomainError::UnknownWait)?
            .id;
        Ok(Some((
            EligibleTestWait {
                task_id: turn,
                run_id: id,
                wait_id,
                request,
            },
            planner.agent_id,
            run,
        )))
    }
    pub(super) async fn read_permissions(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> Result<SessionPermissions, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        Self::harness_context(&mut tx, session, turn).await?;
        let state = Self::permission_state(&mut tx, session).await?;
        let archived = Self::load_session_metadata(&mut tx, session)
            .await?
            .archived;
        let eligible_wait = if state.test_command.is_none() {
            Self::eligible_test_wait(&mut tx, session, turn)
                .await?
                .map(|(wait, _, _)| wait)
        } else {
            None
        };
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_permission_receipts WHERE session_id=? ORDER BY revision DESC LIMIT 20")
            .bind(session.to_string()).fetch_all(&mut *tx).await.map_err(storage)?;
        let history = rows
            .iter()
            .map(|p| serde_json::from_str(p))
            .collect::<Result<_, _>>()?;
        let (task_edits, eligible_edit_waits, task_edits_truncated) =
            Self::task_edit_views(&mut tx, session, turn).await?;
        let (task_tests, eligible_task_test_waits, task_tests_truncated) =
            Self::task_test_views(&mut tx, session, turn).await?;
        tx.commit().await.map_err(storage)?;
        Ok(SessionPermissions {
            session_id: session,
            state,
            archived,
            history,
            eligible_wait,
            task_edits,
            eligible_edit_waits,
            task_edits_truncated,
            task_tests,
            eligible_task_test_waits,
            task_tests_truncated,
        })
    }
    pub(super) async fn save_permissions(
        &self,
        session: SessionId,
        request: PermissionRequest,
        now: i64,
        decision: TestPermissionDecision,
        edit_decision: TaskEditPermissionDecision,
        task_test_decision: TaskTestPermissionDecision,
    ) -> Result<PermissionReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<String> = sqlx::query_scalar(
            "SELECT payload FROM harness_permission_receipts WHERE request_id=?",
        )
        .bind(request.request_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?;
        if let Some(saved) = saved {
            let mut receipt: PermissionReceipt = serde_json::from_str(&saved)?;
            if receipt.session_id != session || receipt.request != request {
                return Err(DomainError::SessionConflict(
                    "permission request identity belongs to another decision".into(),
                )
                .into());
            }
            receipt.replayed = true;
            tx.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        Self::load_session_metadata(&mut tx, session).await?;
        let mut state = Self::permission_state(&mut tx, session).await?;
        state.check(&request)?;
        let mut task_edit = None;
        let mut task_test = None;
        let grant = match request.action {
            PermissionAction::AllowTestCommand {
                task_id,
                run_id,
                wait_id,
            } => {
                let (wait, actor_id, mut run) = Self::eligible_test_wait(&mut tx, session, task_id)
                    .await?
                    .filter(|(w, _, _)| w.run_id == run_id && w.wait_id == wait_id)
                    .ok_or_else(|| {
                        DomainError::SessionConflict(
                            "refresh the current planner test approval before allowing it".into(),
                        )
                    })?;
                let grant = TestPermission {
                    grant_id: request.request_id,
                    session_id: session,
                    actor_id,
                    request: wait.request,
                    granted_at: now,
                };
                let before = run.clone();
                decision(&mut run, &grant)?;
                self.save_change(&mut tx, &before, run, None).await?;
                Some(grant)
            }
            PermissionAction::RevokeTestCommand { .. } => None,
            PermissionAction::AllowTaskTests {
                task_id,
                run_id,
                wait_id,
            } => {
                let (wait, mut grant, mut run) =
                    Self::eligible_task_test_wait(&mut tx, session, task_id)
                        .await?
                        .filter(|(w, _, _)| w.run_id == run_id && w.wait_id == wait_id)
                        .ok_or_else(|| {
                            DomainError::SessionConflict(
                                "refresh the current worker test approval before allowing it"
                                    .into(),
                            )
                        })?;
                let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_task_test_permissions WHERE task_id=? AND revoked=0)")
                    .bind(wait.task_id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
                if active {
                    return Err(DomainError::SessionConflict(
                        "this task already has a test permission".into(),
                    )
                    .into());
                }
                grant.grant_id = request.request_id;
                grant.granted_at = now;
                let before = run.clone();
                task_test_decision(&mut run, &grant)?;
                self.save_change(&mut tx, &before, run, None).await?;
                Self::save_task_test_grant(&mut tx, &grant).await?;
                task_test = Some(grant);
                None
            }
            PermissionAction::RevokeTaskTests { grant_id } => {
                let payload: Option<String> = sqlx::query_scalar("SELECT payload FROM harness_task_test_permissions WHERE session_id=? AND grant_id=? AND revoked=0")
                    .bind(session.to_string()).bind(grant_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?;
                let mut grant: TaskTestPermission =
                    serde_json::from_str(&payload.ok_or_else(|| {
                        DomainError::SessionConflict(
                            "the reviewed task test permission is no longer active".into(),
                        )
                    })?)?;
                grant.revoked_at = Some(now);
                Self::save_task_test_grant(&mut tx, &grant).await?;
                task_test = Some(grant);
                None
            }
            PermissionAction::AllowTaskEdits {
                task_id,
                run_id,
                wait_id,
            } => {
                let (wait, mut grant, mut run) =
                    Self::eligible_edit_wait(&mut tx, session, task_id)
                        .await?
                        .filter(|(w, _, _)| w.run_id == run_id && w.wait_id == wait_id)
                        .ok_or_else(|| {
                            DomainError::SessionConflict(
                        "refresh the current scoped worker edit approval before allowing it".into(),
                    )
                        })?;
                let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_task_edit_permissions WHERE task_id=? AND revoked=0)")
                    .bind(wait.task_id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
                if active {
                    return Err(DomainError::SessionConflict(
                        "this task already has an edit permission".into(),
                    )
                    .into());
                }
                grant.grant_id = request.request_id;
                grant.granted_at = now;
                let before = run.clone();
                edit_decision(&mut run, &grant)?;
                self.save_change(&mut tx, &before, run, None).await?;
                Self::save_task_edit_grant(&mut tx, &grant).await?;
                task_edit = Some(grant);
                None
            }
            PermissionAction::RevokeTaskEdits { grant_id } => {
                let payload: Option<String> = sqlx::query_scalar("SELECT payload FROM harness_task_edit_permissions WHERE session_id=? AND grant_id=? AND revoked=0")
                    .bind(session.to_string()).bind(grant_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?;
                let mut grant: TaskEditPermission =
                    serde_json::from_str(&payload.ok_or_else(|| {
                        DomainError::SessionConflict(
                            "the reviewed task permission is no longer active".into(),
                        )
                    })?)?;
                grant.revoked_at = Some(now);
                Self::save_task_edit_grant(&mut tx, &grant).await?;
                task_edit = Some(grant);
                None
            }
        };
        state.apply(&request, grant)?;
        let receipt = PermissionReceipt {
            session_id: session,
            request,
            state,
            saved_at: now,
            replayed: false,
            task_edit,
            task_test,
        };
        sqlx::query("INSERT INTO harness_session_permissions(session_id,payload) VALUES(?,?) ON CONFLICT(session_id) DO UPDATE SET payload=excluded.payload")
            .bind(session.to_string()).bind(serde_json::to_string(&receipt.state)?).execute(&mut *tx).await.map_err(storage)?;
        sqlx::query("INSERT INTO harness_permission_receipts(request_id,session_id,revision,payload) VALUES(?,?,?,?)")
            .bind(receipt.request.request_id.to_string()).bind(session.to_string()).bind(receipt.state.revision as i64).bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    pub(super) async fn apply_test_permission(
        &self,
        decision: TestPermissionDecision,
    ) -> Result<bool, Error> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        // Only direct harness turns qualify; helper tasks and patch application Runs cannot inherit permission.
        let rows: Vec<(String, String, String)> = sqlx::query_as("SELECT h.session_id,h.task_id,p.payload FROM harness_turns h
            JOIN harness_session_permissions p ON p.session_id=h.session_id
            JOIN harness_session_catalog c ON c.session_id=h.session_id
            JOIN agent_runs a ON a.request_id=h.task_id JOIN runs r ON r.id=a.run_id
            WHERE c.archived=0 AND r.status='waiting' AND json_extract(p.payload,'$.test_command') IS NOT NULL
            AND json_extract(r.payload,'$.wait.key')='native_tool_approval'
            AND NOT EXISTS(SELECT 1 FROM agent_goal_paused_requests held WHERE held.id=h.task_id)
            ORDER BY r.created_at,r.id")
            .fetch_all(&mut *tx).await.map_err(storage)?;
        for (session, turn, payload) in rows {
            let session = session
                .parse()
                .map_err(|_| Error::Storage("invalid permission session identity".into()))?;
            let turn = turn
                .parse()
                .map_err(|_| Error::Storage("invalid permission task identity".into()))?;
            let state: PermissionState = serde_json::from_str(&payload)?;
            let Some(grant) = state.test_command else {
                continue;
            };
            let Some((wait, actor, mut run)) =
                Self::eligible_test_wait(&mut tx, session, turn).await?
            else {
                continue;
            };
            if grant.session_id != session
                || grant.actor_id != actor
                || grant.request != wait.request
            {
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
