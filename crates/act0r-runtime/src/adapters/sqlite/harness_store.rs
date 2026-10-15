use super::*;
use crate::application::DelegatedWorkView;
use crate::domain::{actors::AgentMessage, harness::*, teams::TeamStatus};
use serde_json::json;

pub(super) fn preview(raw: Vec<u8>, limit: usize) -> String {
    let mut text = String::from_utf8_lossy(&raw).into_owned();
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}
pub(super) fn coding(row: &sqlx::sqlite::SqliteRow) -> Result<Option<HarnessCoding>, Error> {
    let paths: Option<String> = row.try_get("writable_paths").map_err(storage)?;
    let command: Option<String> = row.try_get("test_command").map_err(storage)?;
    match (paths, command) {
        (None, None) => Ok(None),
        (Some(paths), Some(command)) => {
            let profile = HarnessCoding {
                coder_count: row
                    .try_get::<Option<i64>, _>("coder_count")
                    .map_err(storage)?
                    .unwrap_or(1)
                    .try_into()
                    .map_err(|_| DomainError::InvalidState)?,
                writable_paths: serde_json::from_str(&paths)?,
                test_command: serde_json::from_str(&command)?,
                project_commands: row
                    .try_get::<Option<String>, _>("project_commands")
                    .map_err(storage)?
                    .map(|raw| serde_json::from_str(&raw))
                    .transpose()?
                    .unwrap_or_default(),
            };
            profile.validate()?;
            Ok(Some(profile))
        }
        _ => Err(DomainError::InvalidState.into()),
    }
}
impl SqliteStore {
    pub(super) async fn harness_context(
        conn: &mut SqliteConnection,
        session: SessionId,
        task: MessageId,
    ) -> Result<HarnessTurnContext, Error> {
        let row = sqlx::query("SELECT json_extract(t.payload,'$.spec.source') AS workspace,
            json_extract(t.payload,'$.spec.planner.config.writable_paths') AS writable_paths,
            json_extract(t.payload,'$.spec.planner.config.test_command') AS test_command,
            json_extract(t.payload,'$.spec.planner.config.project_commands') AS project_commands,
            json_extract(t.payload,'$.spec.planner.config.delegation.max_parallel_tasks') AS coder_count,
            COALESCE(json_extract(p.payload,'$.paused'),0) AS paused,
            EXISTS(SELECT 1 FROM agent_goal_cancellations c WHERE c.root_task_id=h.task_id) AS cancelled
            FROM harness_turns h JOIN harness_sessions s ON s.id=h.session_id JOIN agent_teams t ON t.id=s.team_id
            LEFT JOIN agent_goal_pauses p ON p.root_task_id=h.task_id
            WHERE h.session_id=? AND h.task_id=?")
            .bind(session.to_string()).bind(task.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?
            .ok_or(Error::AgentTaskNotFound(task))?;
        Ok(HarnessTurnContext {
            workspace: row.try_get("workspace").map_err(storage)?,
            coding: coding(&row)?,
            paused: row.try_get("paused").map_err(storage)?,
            cancelled: row.try_get("cancelled").map_err(storage)?,
        })
    }
    pub(super) async fn admit_harness_turn(
        conn: &mut SqliteConnection,
        session: SessionId,
        team: TeamId,
        turn: HarnessTurnRequest,
        first: bool,
        now: i64,
    ) -> Result<HarnessReceipt, Error> {
        turn.validate()?;
        let record = Self::load_team(conn, team).await?;
        if record.status != TeamStatus::Ready {
            return Err(Error::Unavailable("session setup is not ready".into()));
        }
        Self::check_team_actors(conn, &record).await?;
        let saved: Option<(String, i64, String)> =
            sqlx::query_as("SELECT session_id,sequence,prompt FROM harness_turns WHERE task_id=?")
                .bind(turn.message_id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        let sequence = if let Some((owner, sequence, prompt)) = saved {
            if owner != session.to_string() || prompt != turn.prompt || first && sequence != 1 {
                return Err(DomainError::ConflictingMessage.into());
            }
            sequence
        } else {
            let sequence: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(sequence),0)+1 FROM harness_turns WHERE session_id=?",
            )
            .bind(session.to_string())
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
            if first && sequence != 1 {
                return Err(DomainError::ConflictingMessage.into());
            }
            sqlx::query("INSERT INTO harness_turns(task_id,session_id,sequence,prompt,created_at) VALUES(?,?,?,?,?)")
                .bind(turn.message_id.to_string()).bind(session.to_string()).bind(sequence).bind(&turn.prompt).bind(now)
                .execute(&mut *conn).await.map_err(storage)?;
            sequence
        };
        let message = AgentMessage {
            id: turn.message_id,
            recipient: record.planner()?.agent_id,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":turn.prompt}),
        };
        message.validate_public()?;
        Self::insert_message(conn, message.clone(), now).await?;
        Self::insert_actor_task(conn, &message, None, now).await?;
        Ok(HarnessReceipt {
            session_id: session,
            team_id: team,
            task_id: turn.message_id,
            sequence: sequence as u64,
        })
    }
}
impl SqliteStore {
    pub(super) async fn read_harness_sessions(
        &self,
        query: sessions::SessionListQuery,
        continuous: bool,
    ) -> Result<HarnessSessions, Error> {
        query.validate()?;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let stream: String = sqlx::query_scalar("SELECT id FROM history_stream WHERE singleton=1")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        let stream: EventStreamId = stream.parse().map_err(|_| DomainError::InvalidState)?;
        let stream = if continuous {
            conversation::list_stream(stream)
        } else {
            stream
        };
        let watermark: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(position),0) FROM harness_session_catalog")
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
        let (snapshot, before) = if let Some(cursor) = query.after {
            cursor.validate_scope(stream, query.archived)?;
            if cursor.snapshot > watermark as u64 {
                return Err(DomainError::InvalidConfiguration(
                    "session cursor is ahead of the catalog".into(),
                )
                .into());
            }
            (cursor.snapshot as i64, Some(cursor.before as i64))
        } else {
            (watermark, None)
        };
        // Include interrupted actor assembly; read bounded scalars rather than whole Runs.
        let sql = "SELECT h.id AS team_id,catalog.session_id,catalog.position,catalog.title,catalog.archived,catalog.revision,
                COALESCE(last.task_id,substr(h.name,12)) AS task_id,
                substr(CAST(json_extract(h.payload,'$.spec.planner.config.goal') AS BLOB),1,1028) AS prompt,
                length(CAST(json_extract(h.payload,'$.spec.planner.config.goal') AS BLOB))>1024 AS truncated,
                json_extract(h.payload,'$.spec.planner.config.target.model') AS model,
                json_extract(h.payload,'$.spec.source') AS workspace,
                json_extract(h.payload,'$.spec.planner.config.writable_paths') AS writable_paths,
                json_extract(h.payload,'$.spec.planner.config.test_command') AS test_command,
                json_extract(h.payload,'$.spec.planner.config.project_commands') AS project_commands,
                json_extract(h.payload,'$.spec.planner.config.delegation.max_parallel_tasks') AS coder_count,
                COALESCE(last.created_at,json_extract(h.payload,'$.created_at')) AS created_at,
                CASE WHEN t.id IS NULL THEN 'preparing' WHEN json_extract(t.payload,'$.status') IN ('succeeded','failed','rejected','budget_exhausted','cancelled') THEN json_extract(t.payload,'$.status')
                     ELSE COALESCE(r.status,'queued') END AS status,
                COALESCE(json_extract(p.payload,'$.paused'),0) AS paused,
                c.root_task_id IS NOT NULL AS cancelled,json_extract(h.payload,'$.error') AS error,
                (SELECT COUNT(*) FROM harness_turns ht WHERE ht.session_id=substr(h.name,12)) AS turn_count,
                h.name GLOB 'harness-v2-*' AS can_continue
                FROM harness_session_catalog catalog JOIN agent_teams h ON h.id=catalog.team_id LEFT JOIN harness_turns last ON last.task_id=(SELECT task_id FROM harness_turns WHERE session_id=catalog.session_id ORDER BY sequence DESC LIMIT 1)
                LEFT JOIN agent_tasks t ON t.id=COALESCE(last.task_id,substr(h.name,12)) AND t.agent_id=json_extract(h.payload,'$.members[0].agent_id')
                LEFT JOIN runs r ON r.id=t.run_id LEFT JOIN agent_goal_pauses p ON p.root_task_id=t.id
                LEFT JOIN agent_goal_cancellations c ON c.root_task_id=t.id
                WHERE catalog.archived=? AND catalog.position<=? AND (? IS NULL OR catalog.position<?)
                    AND json_type(h.payload,'$.spec.planner.config.goal')='text'
                    AND json_type(h.payload,'$.spec.planner.config.target.model')='text'
                ORDER BY catalog.position DESC LIMIT ?";

        let sql = if continuous {
            sql.replace("catalog.position,catalog.title", "catalog.position,substr(h.name,12) AS execution_session_id,catalog.title")
                    .replace("FROM harness_session_catalog catalog JOIN agent_teams h ON h.id=catalog.team_id", "FROM harness_session_catalog catalog JOIN agent_teams origin ON origin.id=catalog.team_id JOIN agent_teams h ON h.id=COALESCE((SELECT s.team_id FROM harness_project_continuations link JOIN harness_turns original ON original.task_id=link.source_task_id JOIN harness_sessions s ON s.id=link.coding_session_id WHERE link.source_session_id=catalog.session_id ORDER BY link.source_sequence DESC,link.phase DESC LIMIT 1),catalog.team_id)")
                    .replace("WHERE session_id=catalog.session_id ORDER BY", "WHERE session_id=substr(h.name,12) ORDER BY")
                    .replace("json_extract(h.payload,'$.spec.planner.config.goal') AS BLOB", "json_extract(origin.payload,'$.spec.planner.config.goal') AS BLOB")
                    .replace("WHERE catalog.archived=?", "WHERE NOT EXISTS(SELECT 1 FROM harness_project_continuations link WHERE link.coding_session_id=catalog.session_id) AND NOT (json_type(origin.payload,'$.spec.planner.config.discovery_context.evidence.session_id')='text' AND EXISTS(SELECT 1 FROM harness_sessions original WHERE original.id=json_extract(origin.payload,'$.spec.planner.config.discovery_context.evidence.session_id'))) AND catalog.archived=?")
                    .replace("CASE WHEN t.id IS NULL THEN 'preparing'", "CASE WHEN EXISTS(SELECT 1 FROM harness_auto_setup_errors e WHERE e.task_id=t.id) THEN 'failed' WHEN json_extract(h.payload,'$.spec.planner.config.discover_project')=1 AND json_type(r.payload,'$.result.project_setup')='object' THEN CASE WHEN json_extract(h.payload,'$.spec.planner.config.auto_execute')=1 THEN 'preparing' ELSE 'setup_review' END WHEN t.id IS NULL THEN 'preparing'")
        } else {
            sql.into()
        };
        // Both variants are built exclusively from audited static SQL above;
        // all request values remain bound parameters.
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(query.archived)
            .bind(snapshot)
            .bind(before)
            .bind(before)
            .bind(i64::from(query.limit) + 1)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
        let truncated = rows.len() > query.limit as usize;
        let mut items = Vec::new();
        let mut last_position = 0;
        for row in rows.into_iter().take(query.limit as usize) {
            last_position = row.try_get::<i64, _>("position").map_err(storage)? as u64;
            let task: String = row.try_get("task_id").map_err(storage)?;
            let session: String = row.try_get("session_id").map_err(storage)?;
            let (Ok(task_id), Ok(session_id)) = (task.parse(), session.parse()) else {
                continue;
            };
            let team: String = row.try_get("team_id").map_err(storage)?;
            items.push(HarnessSession {
                execution_session_id: if continuous {
                    let value: String = row.try_get("execution_session_id").map_err(storage)?;
                    Some(value.parse().map_err(|_| DomainError::InvalidState)?)
                } else {
                    None
                },
                title: row.try_get("title").map_err(storage)?,
                archived: row.try_get("archived").map_err(storage)?,
                metadata_revision: row.try_get::<i64, _>("revision").map_err(storage)? as u64,
                session_id,
                team_id: team.parse().map_err(|_| DomainError::InvalidState)?,
                task_id,
                prompt_preview: preview(row.try_get("prompt").map_err(storage)?, 1024),
                prompt_truncated: row.try_get("truncated").map_err(storage)?,
                model: row.try_get("model").map_err(storage)?,
                workspace: row.try_get("workspace").map_err(storage)?,
                coding: coding(&row)?,
                created_at: row.try_get("created_at").map_err(storage)?,
                root_status: row.try_get("status").map_err(storage)?,
                paused: row.try_get("paused").map_err(storage)?,
                cancelled: row.try_get("cancelled").map_err(storage)?,
                setup_error: row.try_get("error").map_err(storage)?,
                turn_count: row.try_get::<i64, _>("turn_count").map_err(storage)? as u64,
                can_continue: row.try_get("can_continue").map_err(storage)?,
            });
        }
        let next_after = if truncated {
            Some(sessions::SessionListCursor::new(
                stream,
                query.archived,
                snapshot as u64,
                last_position,
            )?)
        } else {
            None
        };
        tx.commit().await.map_err(storage)?;
        Ok(HarnessSessions {
            items,
            truncated,
            next_after,
        })
    }
}

impl SqliteStore {
    pub(super) async fn start_linked_harness_turn(
        &self,
        team: TeamId,
        turn: HarnessTurnRequest,
        source: Option<conversation::ProjectContinuation>,
        now: i64,
    ) -> Result<HarnessReceipt, Error> {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let record = Self::load_team(&mut tx, team).await?;
        let session: SessionId = turn
            .message_id
            .to_string()
            .parse()
            .map_err(|_| DomainError::InvalidState)?;
        if record.spec.name != format!("{SESSION_PREFIX}{}", turn.message_id) {
            return Err(DomainError::InvalidState.into());
        }
        if record.spec.planner.config["auto_execute"] == true
            && let Some(source) = &source
        {
            let existing: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM harness_project_continuations WHERE source_task_id=?)",
            )
            .bind(source.task_id.to_string())
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
            if !existing {
                let context =
                    Self::harness_context(&mut tx, source.session_id, source.task_id).await?;
                if context.paused
                    || context.cancelled
                    || Self::conversation_archived(&mut tx, source.session_id).await?
                {
                    return Err(DomainError::SessionConflict(
                        "project request is paused, stopped or archived".into(),
                    )
                    .into());
                }
            }
        }
        sqlx::query("INSERT INTO harness_sessions(id,team_id,can_continue) VALUES(?,?,?) ON CONFLICT(id) DO NOTHING")
                .bind(session.to_string()).bind(team.to_string()).bind(record.spec.name.starts_with(SESSION_PREFIX))
                .execute(&mut *tx).await.map_err(storage)?;
        let owner: String = sqlx::query_scalar("SELECT team_id FROM harness_sessions WHERE id=?")
            .bind(session.to_string())
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        if owner != team.to_string() {
            return Err(DomainError::ConflictingMessage.into());
        }
        let receipt = Self::admit_harness_turn(&mut tx, session, team, turn, true, now).await?;
        if let Some(source) = source {
            Self::save_project_continuation(&mut tx, &source, &receipt).await?;
        }
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
}

impl HarnessStorePort for SqliteStore {
    fn pending_auto_setup(&self) -> PortFuture<'_, Option<conversation::ProjectContinuation>> {
        Box::pin(async move {
            let row: Option<(String,String)> = sqlx::query_as("SELECT h.session_id,h.task_id FROM harness_turns h JOIN harness_sessions s ON s.id=h.session_id JOIN agent_teams t ON t.id=s.team_id JOIN agent_tasks a ON a.id=h.task_id JOIN runs r ON r.id=a.run_id JOIN harness_session_catalog catalog ON catalog.session_id=s.id WHERE json_extract(t.payload,'$.spec.planner.config.auto_execute')=1 AND json_extract(t.payload,'$.spec.planner.config.discover_project')=1 AND r.status='succeeded' AND json_type(r.payload,'$.result.project_setup')='object' AND catalog.archived=0 AND NOT EXISTS(SELECT 1 FROM harness_unsettled_turns u WHERE u.task_id=h.task_id) AND NOT EXISTS(SELECT 1 FROM harness_project_continuations c WHERE c.source_task_id=h.task_id) AND NOT EXISTS(SELECT 1 FROM harness_auto_setup_errors e WHERE e.task_id=h.task_id) AND NOT EXISTS(SELECT 1 FROM agent_goal_cancellations c WHERE c.root_task_id=h.task_id) AND NOT EXISTS(SELECT 1 FROM agent_goal_pauses p WHERE p.root_task_id=h.task_id AND json_extract(p.payload,'$.paused')=1) ORDER BY h.rowid LIMIT 1")
                .fetch_optional(&self.pool).await.map_err(storage)?;
            row.map(|(session, task)| {
                Ok(conversation::ProjectContinuation {
                    session_id: session.parse().map_err(|_| DomainError::InvalidState)?,
                    task_id: task.parse().map_err(|_| DomainError::InvalidState)?,
                })
            })
            .transpose()
        })
    }
    fn auto_setup_failed(
        &self,
        source: conversation::ProjectContinuation,
        error: String,
        now: i64,
    ) -> PortFuture<'_, ()> {
        Box::pin(async move {
            sqlx::query("INSERT INTO harness_auto_setup_errors(task_id,message,saved_at) SELECT task_id,?,? FROM harness_turns WHERE session_id=? AND task_id=? AND NOT EXISTS(SELECT 1 FROM harness_project_continuations WHERE source_task_id=?) ON CONFLICT(task_id) DO NOTHING")
                .bind(error).bind(now).bind(source.session_id.to_string()).bind(source.task_id.to_string()).bind(source.task_id.to_string()).execute(&self.pool).await.map_err(storage)?;
            Ok(())
        })
    }
    fn refresh_context(
        &self,
        scope: project_refresh::RefreshScope,
    ) -> PortFuture<'_, project_refresh::RefreshContext> {
        Box::pin(self.read_refresh_context(scope))
    }
    fn project_refresh(
        &self,
        scope: project_refresh::RefreshScope,
    ) -> PortFuture<'_, project_refresh::RefreshView> {
        Box::pin(self.read_project_refresh(scope))
    }
    fn refresh_intent(
        &self,
        scope: project_refresh::RefreshScope,
        request: MessageId,
    ) -> PortFuture<'_, Option<project_refresh::RefreshIntent>> {
        Box::pin(self.read_refresh_intent(scope, request))
    }
    fn begin_project_refresh(
        &self,
        scope: project_refresh::RefreshScope,
        request: project_refresh::RefreshRequest,
        snapshot: project_refresh::RefreshSnapshot,
    ) -> PortFuture<'_, project_refresh::RefreshIntent> {
        Box::pin(self.prepare_project_refresh(scope, request, snapshot))
    }
    fn admit_project_refresh(
        &self,
        intent: project_refresh::RefreshIntent,
        team: TeamId,
        now: i64,
    ) -> PortFuture<'_, HarnessReceipt> {
        Box::pin(self.publish_project_refresh(intent, team, now))
    }
    fn cancel_project_refresh(
        &self,
        scope: project_refresh::RefreshScope,
        request: MessageId,
    ) -> PortFuture<'_, ()> {
        Box::pin(self.abandon_project_refresh(scope, request))
    }
    fn project_refresh_error(
        &self,
        scope: project_refresh::RefreshScope,
        request: MessageId,
        error: String,
    ) -> PortFuture<'_, ()> {
        Box::pin(self.record_project_refresh_error(scope, request, error))
    }
    fn verification_repair(
        &self,
        scope: verification_repair::RepairScope,
    ) -> PortFuture<'_, verification_repair::RepairView> {
        Box::pin(self.read_verification_repair(scope))
    }
    fn begin_verification_repair(
        &self,
        scope: verification_repair::RepairScope,
        request: verification_repair::RepairRequest,
    ) -> PortFuture<'_, verification_repair::RepairIntent> {
        Box::pin(self.prepare_verification_repair(scope, request))
    }
    fn admit_verification_repair(
        &self,
        intent: verification_repair::RepairIntent,
        team: TeamId,
        now: i64,
    ) -> PortFuture<'_, HarnessReceipt> {
        Box::pin(self.publish_verification_repair(intent, team, now))
    }
    fn cancel_verification_repair(
        &self,
        scope: verification_repair::RepairScope,
        request: verification_repair::RepairRequest,
    ) -> PortFuture<'_, ()> {
        Box::pin(self.abandon_verification_repair(scope, request))
    }
    fn verification_repair_error(
        &self,
        scope: verification_repair::RepairScope,
        request: MessageId,
        error: String,
    ) -> PortFuture<'_, ()> {
        Box::pin(self.record_verification_repair_error(scope, request, error))
    }
    fn create_project_verification(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
        run: Run,
    ) -> PortFuture<'_, RunId> {
        Box::pin(self.admit_project_verification(session, task, application, run))
    }
    fn project_verifications(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
    ) -> PortFuture<'_, project_verification::VerificationPage> {
        Box::pin(self.read_project_verifications(session, task, application))
    }
    fn continuous_conversation(
        &self,
        session: SessionId,
        before: Option<conversation::ConversationCursor>,
        task: Option<MessageId>,
    ) -> PortFuture<'_, conversation::ContinuousConversation> {
        Box::pin(self.read_continuous_conversation(session, before, task))
    }
    fn conversation_sessions(
        &self,
        query: sessions::SessionListQuery,
    ) -> PortFuture<'_, HarnessSessions> {
        Box::pin(self.read_harness_sessions(query, true))
    }
    fn start_project_coding_turn(
        &self,
        team: TeamId,
        turn: HarnessTurnRequest,
        source: conversation::ProjectContinuation,
        now: i64,
    ) -> PortFuture<'_, HarnessReceipt> {
        Box::pin(self.start_linked_harness_turn(team, turn, Some(source), now))
    }
    fn worker_pause(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> PortFuture<'_, worker_pause::WorkerPause> {
        Box::pin(self.read_worker_pause(session, turn, task))
    }
    fn control_assignment_pause(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: worker_pause::AssignmentPauseRequest,
        now: i64,
    ) -> PortFuture<'_, worker_pause::AssignmentPauseReceipt> {
        Box::pin(self.admit_assignment_pause(session, turn, task, request, now))
    }
    fn worker_cancellation(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> PortFuture<'_, worker_cancellation::WorkerCancellation> {
        Box::pin(self.read_worker_cancellation(session, turn, task))
    }
    fn stop_assignment(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: worker_cancellation::StopAssignmentRequest,
        now: i64,
        decision: ActorCancellation,
    ) -> PortFuture<'_, worker_cancellation::StopAssignmentReceipt> {
        Box::pin(self.admit_assignment_stop(session, turn, task, request, now, decision))
    }
    fn session_permissions(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, permissions::SessionPermissions> {
        Box::pin(self.read_permissions(session, turn))
    }
    fn change_permissions(
        &self,
        session: SessionId,
        request: permissions::PermissionRequest,
        now: i64,
        decision: TestPermissionDecision,
        edit_decision: TaskEditPermissionDecision,
        task_test_decision: TaskTestPermissionDecision,
    ) -> PortFuture<'_, permissions::PermissionReceipt> {
        Box::pin(self.save_permissions(
            session,
            request,
            now,
            decision,
            edit_decision,
            task_test_decision,
        ))
    }
    fn turn_steering(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, steering::TurnSteering> {
        Box::pin(self.read_turn_steering(session, turn))
    }
    fn send_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        request: steering::SteeringRequest,
        now: i64,
    ) -> PortFuture<'_, steering::SteeringReceipt> {
        Box::pin(self.admit_steering(session, turn, request, now))
    }
    fn worker_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> PortFuture<'_, steering::TurnSteering> {
        Box::pin(self.read_worker_steering(session, turn, task))
    }
    fn send_worker_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: steering::SteeringRequest,
        now: i64,
    ) -> PortFuture<'_, steering::SteeringReceipt> {
        Box::pin(self.admit_worker_steering(session, turn, task, request, now))
    }
    fn turn_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, turn_recovery::TurnRecovery> {
        Box::pin(self.read_turn_recovery(session, turn))
    }
    fn retry_turn(
        &self,
        session: SessionId,
        turn: MessageId,
        request: turn_recovery::RetryTurnRequest,
        now: i64,
        check: RecoveryCheck,
    ) -> PortFuture<'_, turn_recovery::RetryTurnReceipt> {
        Box::pin(self.admit_turn_recovery(session, turn, request, now, check))
    }
    fn worker_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> PortFuture<'_, worker_recovery::WorkerRecovery> {
        Box::pin(self.read_worker_recovery(session, turn, task))
    }
    fn decide_worker_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: worker_recovery::WorkerRecoveryRequest,
        now: i64,
        check: RecoveryCheck,
    ) -> PortFuture<'_, worker_recovery::WorkerRecoveryReceipt> {
        Box::pin(self.admit_worker_recovery(session, turn, task, request, now, check))
    }
    fn change_summary_policy(
        &self,
        session: SessionId,
        request: session_summary::SummaryPolicyRequest,
    ) -> PortFuture<'_, session_summary::SummaryPolicyReceipt> {
        Box::pin(self.update_summary_policy(session, request))
    }
    fn session_summary(
        &self,
        session: SessionId,
    ) -> PortFuture<'_, session_summary::SessionSummary> {
        Box::pin(self.read_session_summary(session))
    }
    fn start_session_summary(
        &self,
        session: SessionId,
        request: session_summary::SummaryRequest,
        now: i64,
        factory: SummaryRunFactory,
    ) -> PortFuture<'_, session_summary::SummaryReceipt> {
        Box::pin(self.admit_session_summary(session, request, now, factory))
    }
    fn session_memory_actors(
        &self,
        session: SessionId,
    ) -> PortFuture<'_, Vec<(teams::TeamMember, actors::Agent)>> {
        Box::pin(self.read_session_memory_actors(session))
    }
    fn turn_memory(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, session_memory::TurnMemory> {
        Box::pin(self.read_turn_memory(session, turn))
    }
    fn clear_session_memory(
        &self,
        session: SessionId,
        agent: AgentId,
        request: session_memory::ClearMemoryRequest,
        now: i64,
        decision: ActorMemoryDecision,
    ) -> PortFuture<'_, session_memory::ClearMemoryReceipt> {
        Box::pin(self.change_session_memory(session, agent, request, now, decision))
    }
    fn session_metadata(
        &self,
        session: SessionId,
    ) -> PortFuture<'_, sessions::SessionMetadataView> {
        Box::pin(self.read_session_metadata(session))
    }
    fn update_session(
        &self,
        session: SessionId,
        request: sessions::SessionUpdate,
    ) -> PortFuture<'_, sessions::SessionUpdateReceipt> {
        Box::pin(self.change_session_metadata(session, request))
    }
    fn harness_tool_run(
        &self,
        session: SessionId,
        turn: MessageId,
        id: RunId,
    ) -> PortFuture<'_, (MessageId, Run)> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            Self::harness_context(&mut tx, session, turn).await?;
            let owner: Option<String> =
                sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
                    .bind(id.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            let owner: MessageId = owner
                .ok_or(Error::NotFound(id))?
                .parse()
                .map_err(|_| DomainError::InvalidState)?;
            if Self::goal_root(&mut tx, owner).await? != turn {
                return Err(Error::NotFound(id));
            }
            let run = Self::load(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok((owner, run))
        })
    }
    fn harness_tool_runs(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, (Vec<(MessageId, Run)>, bool)> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            Self::harness_context(&mut tx, session, turn).await?;
            let rows: Vec<(String,String)> = sqlx::query_as("WITH RECURSIVE owned(id) AS (SELECT ? UNION SELECT o.id FROM agent_goal_ownership o JOIN owned p ON o.parent_id=p.id)
                SELECT a.request_id,r.id FROM agent_runs a JOIN owned o ON o.id=a.request_id JOIN runs r ON r.id=a.run_id
                WHERE r.status='blocked' OR json_array_length(json_extract(r.payload,'$.reconciliation_receipts'))>0 ORDER BY json_extract(r.payload,'$.updated_at') DESC,r.id DESC LIMIT 129")
                .bind(turn.to_string()).fetch_all(&mut *tx).await.map_err(storage)?;
            let truncated = rows.len() > 128;
            let mut items = Vec::new();
            for (task, id) in rows.into_iter().take(128) {
                let task = task.parse().map_err(|_| DomainError::InvalidState)?;
                let id = id.parse().map_err(|_| DomainError::InvalidState)?;
                items.push((task, Self::load(&mut tx, id).await?));
            }
            tx.commit().await.map_err(storage)?;
            Ok((items, truncated))
        })
    }
    fn harness_delegated_work(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, DelegatedWorkView> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            Self::harness_context(&mut tx, session, turn).await?;
            let view = Self::read_delegated_work(&mut tx, session, turn).await?;
            tx.commit().await.map_err(storage)?;
            Ok(view)
        })
    }
    fn harness_turn_context(
        &self,
        session: SessionId,
        task: MessageId,
    ) -> PortFuture<'_, HarnessTurnContext> {
        Box::pin(async move {
            let mut conn = self.pool.acquire().await.map_err(storage)?;
            Self::harness_context(&mut conn, session, task).await
        })
    }
    fn create_harness_application(
        &self,
        session: SessionId,
        task: MessageId,
        run: Run,
    ) -> PortFuture<'_, RunId> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let context = Self::harness_context(&mut tx, session, task).await?;
            let id = run.record().id;
            if context.coding.is_none()
                || run.record().workflow != crate::domain::patch_application::workflow()
                || run.record().input["task_id"] != json!(task)
                || run.record().input["target"] != json!(context.workspace)
            {
                return Err(DomainError::InvalidConfiguration(
                    "application must use the coding session's task and original project".into(),
                )
                .into());
            }
            let existing: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE id=?)")
                .bind(id.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
            if existing {
                let saved = Self::load(&mut tx, id).await?;
                if saved.record().workflow != run.record().workflow
                    || saved.record().input != run.record().input
                {
                    return Err(DomainError::ConflictingMessage.into());
                }
            } else {
                if Self::conversation_archived(&mut tx, session).await? {
                    return Err(DomainError::SessionConflict(
                        "restore the archived session before creating a patch application".into(),
                    )
                    .into());
                }
                let saved = Self::saved_task_changes(&mut tx, task).await?;
                validate_application_changes(&saved)?;
                if run.record().input["changes"] != json!(saved.changes) {
                    return Err(DomainError::InvalidConfiguration(
                        "application requires a completed patch with fresh passing tests".into(),
                    )
                    .into());
                }
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE json_extract(payload,'$.workflow.name')='workspace.apply' AND json_extract(payload,'$.input.task_id')=?")
                    .bind(task.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
                if count >= 20 {
                    return Err(DomainError::InvalidConfiguration("this turn already has 20 application previews; use the CLI for further review".into()).into());
                }
                self.save(&mut tx, &run, true).await?;
            }
            tx.commit().await.map_err(storage)?;
            Ok(id)
        })
    }
    fn harness_applications(
        &self,
        session: SessionId,
        task: MessageId,
    ) -> PortFuture<'_, HarnessApplications> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let context = Self::harness_context(&mut tx, session, task).await?;
            let rows = sqlx::query("SELECT id,status,json_extract(payload,'$.result.status') AS outcome FROM runs WHERE json_extract(payload,'$.workflow.name')='workspace.apply' AND json_extract(payload,'$.input.task_id')=? AND json_extract(payload,'$.input.target')=? ORDER BY created_at DESC,id DESC LIMIT 21")
                .bind(task.to_string()).bind(context.workspace).fetch_all(&mut *tx).await.map_err(storage)?;
            let truncated = rows.len() > 20;
            let mut items = Vec::new();
            for row in rows.into_iter().take(20) {
                let id: String = row.try_get("id").map_err(storage)?;
                let status: String = row.try_get("status").map_err(storage)?;
                let outcome: Option<String> = row.try_get("outcome").map_err(storage)?;
                items.push(HarnessApplicationSummary {
                    run_id: id.parse().map_err(|_| DomainError::InvalidState)?,
                    status: serde_json::from_value(json!(status))?,
                    outcome: outcome
                        .map(|s| serde_json::from_value(json!(s)))
                        .transpose()?,
                });
            }
            tx.commit().await.map_err(storage)?;
            Ok(HarnessApplications { items, truncated })
        })
    }
    fn start_harness_turn(
        &self,
        team: TeamId,
        turn: HarnessTurnRequest,
        now: i64,
    ) -> PortFuture<'_, HarnessReceipt> {
        Box::pin(self.start_linked_harness_turn(team, turn, None, now))
    }

    fn append_harness_turn(
        &self,
        session: SessionId,
        turn: HarnessTurnRequest,
        now: i64,
    ) -> PortFuture<'_, HarnessReceipt> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let root = Self::conversation_root(&mut tx, session).await?;
            let existing = sqlx::query("SELECT h.session_id,s.team_id,h.task_id,h.sequence FROM harness_turns h JOIN harness_sessions s ON s.id=h.session_id LEFT JOIN harness_project_continuations c ON c.coding_session_id=h.session_id WHERE h.task_id=? AND (h.session_id=? OR c.source_session_id=?)")
                .bind(turn.message_id.to_string()).bind(root.to_string()).bind(root.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?;
            if let Some(row) = existing {
                let owner: String = row.try_get("session_id").map_err(storage)?;
                let team: String = row.try_get("team_id").map_err(storage)?;
                let receipt = Self::admit_harness_turn(
                    &mut tx,
                    owner.parse().map_err(|_| DomainError::InvalidState)?,
                    team.parse().map_err(|_| DomainError::InvalidState)?,
                    turn,
                    false,
                    now,
                )
                .await?;
                tx.commit().await.map_err(storage)?;
                return Ok(receipt);
            }
            let active = Self::active_conversation_turn(&mut tx, root).await?;
            let repair_pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_verification_repairs WHERE root_session_id=? AND admitted=0 AND cancelled=0) OR EXISTS(SELECT 1 FROM harness_project_refreshes WHERE root_session_id=? AND admitted=0 AND cancelled=0)")
                .bind(root.to_string()).bind(root.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
            if repair_pending {
                return Err(DomainError::SessionConflict(
                    "continue or cancel the saved workspace preparation before sending another turn"
                        .into(),
                )
                .into());
            }
            let session = active.session_id;
            let (team, can_continue): (String, bool) =
                sqlx::query_as("SELECT team_id,can_continue FROM harness_sessions WHERE id=?")
                    .bind(session.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?
                    .ok_or(Error::SessionNotFound(session))?;
            if !can_continue {
                return Err(DomainError::InvalidConfiguration("This saved session predates conversation memory. Start a new session to continue.".into()).into());
            }
            let team: TeamId = team.parse().map_err(|_| DomainError::InvalidState)?;
            let archived = Self::conversation_archived(&mut tx, session).await?;
            let automatic:bool=sqlx::query_scalar("SELECT COALESCE(json_extract(payload,'$.spec.planner.config.discover_project'),0) FROM agent_teams WHERE id=?").bind(team.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
            if automatic {
                let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_unsettled_turns WHERE task_id=?) OR EXISTS(SELECT 1 FROM agent_tasks t JOIN runs r ON r.id=t.run_id WHERE t.id=? AND json_type(r.payload,'$.result.project_setup')='object')")
                    .bind(active.task_id.to_string()).bind(active.task_id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
                if blocked {
                    return Err(DomainError::SessionConflict("Finish discovery and review its setup before sending a follow-up; answer questions or guide the active request to continue discovery".into()).into());
                }
            }
            let saved: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM harness_turns WHERE session_id=? AND task_id=?)",
            )
            .bind(session.to_string())
            .bind(turn.message_id.to_string())
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
            if archived && !saved {
                return Err(DomainError::SessionConflict(
                    "restore the archived session before sending a follow-up".into(),
                )
                .into());
            }
            if !saved && Self::summary_pending(&mut tx, session).await? {
                return Err(DomainError::SessionConflict(
                    "conversation summary is pending; wait for it to settle before sending a turn"
                        .into(),
                )
                .into());
            }
            let receipt =
                Self::admit_harness_turn(&mut tx, session, team, turn, false, now).await?;
            tx.commit().await.map_err(storage)?;
            Ok(receipt)
        })
    }
    fn harness_sessions(
        &self,
        query: sessions::SessionListQuery,
    ) -> PortFuture<'_, HarnessSessions> {
        Box::pin(self.read_harness_sessions(query, false))
    }

    fn harness_conversation(
        &self,
        session: SessionId,
        before: Option<u64>,
    ) -> PortFuture<'_, HarnessConversation> {
        Box::pin(async move {
            if before.is_some_and(|n| n == 0 || n > i64::MAX as u64) {
                return Err(DomainError::InvalidConfiguration(
                    "before must be a positive sequence within i64".into(),
                )
                .into());
            }
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let row = sqlx::query("SELECT s.team_id,s.can_continue,COALESCE(json_extract(t.payload,'$.spec.planner.config.discover_project'),0) AS discover_project,json_extract(t.payload,'$.spec.planner.config.target.model') AS model,json_extract(t.payload,'$.spec.source') AS workspace, json_extract(t.payload,'$.spec.planner.config.writable_paths') AS writable_paths, json_extract(t.payload,'$.spec.planner.config.test_command') AS test_command, json_extract(t.payload,'$.spec.planner.config.project_commands') AS project_commands, json_extract(t.payload,'$.spec.planner.config.delegation.max_parallel_tasks') AS coder_count FROM harness_sessions s JOIN agent_teams t ON t.id=s.team_id WHERE s.id=?")
                .bind(session.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?.ok_or(Error::SessionNotFound(session))?;
            let team: String = row.try_get("team_id").map_err(storage)?;
            // At most 768 KiB of text per page, before JSON escaping; full evidence stays in Runs.
            let rows = sqlx::query("SELECT h.task_id,h.sequence,h.created_at,
                substr(CAST(h.prompt AS BLOB),1,8196) AS prompt,length(CAST(h.prompt AS BLOB))>8192 AS prompt_truncated,
                substr(CAST(COALESCE(json_extract(r.payload,'$.result.summary'),json_extract(r.payload,'$.result.answer'),json_extract(r.payload,'$.result'),json_extract(r.payload,'$.error.message'),json_extract(t.payload,'$.reply.error.message')) AS BLOB),1,16388) AS response,
                length(CAST(COALESCE(json_extract(r.payload,'$.result.summary'),json_extract(r.payload,'$.result.answer'),json_extract(r.payload,'$.result'),json_extract(r.payload,'$.error.message'),json_extract(t.payload,'$.reply.error.message')) AS BLOB))>16384 AS response_truncated,
                CASE WHEN t.id IS NULL THEN 'preparing' WHEN json_extract(t.payload,'$.status') IN ('succeeded','failed','rejected','budget_exhausted','cancelled') THEN json_extract(t.payload,'$.status') ELSE COALESCE(r.status,'queued') END AS status,
                NOT EXISTS(SELECT 1 FROM harness_unsettled_turns u WHERE u.task_id=h.task_id) AS settled
                FROM harness_turns h LEFT JOIN agent_tasks t ON t.id=h.task_id LEFT JOIN runs r ON r.id=t.run_id
                WHERE h.session_id=? AND (? IS NULL OR h.sequence<?) ORDER BY h.sequence DESC LIMIT 33")
                .bind(session.to_string()).bind(before.map(|n|n as i64)).bind(before.map(|n|n as i64))
                .fetch_all(&mut *tx).await.map_err(storage)?;
            let more = rows.len() > 32;
            let mut turns = Vec::new();
            for row in rows.into_iter().take(32) {
                let task: String = row.try_get("task_id").map_err(storage)?;
                let response: Option<Vec<u8>> = row.try_get("response").map_err(storage)?;
                turns.push(HarnessTurn {
                    task_id: task.parse().map_err(|_| DomainError::InvalidState)?,
                    sequence: row.try_get::<i64, _>("sequence").map_err(storage)? as u64,
                    prompt: preview(row.try_get("prompt").map_err(storage)?, 8192),
                    prompt_truncated: row.try_get("prompt_truncated").map_err(storage)?,
                    response: response.map(|raw| preview(raw, 16384)),
                    response_truncated: row
                        .try_get::<Option<bool>, _>("response_truncated")
                        .map_err(storage)?
                        .unwrap_or(false),
                    status: row.try_get("status").map_err(storage)?,
                    created_at: row.try_get("created_at").map_err(storage)?,
                    settled: row.try_get("settled").map_err(storage)?,
                });
            }
            let next_before = if more {
                turns.last().map(|t| t.sequence)
            } else {
                None
            };
            turns.reverse();
            let metadata = Self::load_session_metadata(&mut tx, session).await?;
            let conversation = HarnessConversation {
                auto_execute: Self::load_team(
                    &mut tx,
                    team.parse().map_err(|_| DomainError::InvalidState)?,
                )
                .await?
                .spec
                .planner
                .config["auto_execute"]
                    == true,
                discover_project: row.try_get("discover_project").map_err(storage)?,
                title: metadata.title,
                archived: metadata.archived,
                session_id: session,
                team_id: team.parse().map_err(|_| DomainError::InvalidState)?,
                model: row.try_get("model").map_err(storage)?,
                workspace: row.try_get("workspace").map_err(storage)?,
                coding: coding(&row)?,
                can_continue: row.try_get("can_continue").map_err(storage)?,
                turns,
                next_before,
            };
            tx.commit().await.map_err(storage)?;
            Ok(conversation)
        })
    }
}
