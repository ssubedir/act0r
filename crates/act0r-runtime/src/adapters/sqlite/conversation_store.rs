use super::*;
use crate::domain::{conversation::*, harness::*, project_setup::coding_start_id};
use serde_json::Value;

impl SqliteStore {
    pub(super) async fn conversation_root(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<SessionId, Error> {
        let root: String = sqlx::query_scalar("SELECT COALESCE(c.source_session_id,s.id) FROM harness_sessions s LEFT JOIN harness_project_continuations c ON c.coding_session_id=s.id WHERE s.id=?")
            .bind(session.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?.ok_or(Error::SessionNotFound(session))?;
        root.parse().map_err(|_| DomainError::InvalidState.into())
    }
    pub(super) async fn conversation_archived(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<bool, Error> {
        let root = Self::conversation_root(conn, session).await?;
        Ok(Self::load_session_metadata(conn, root).await?.archived
            || Self::load_session_metadata(conn, session).await?.archived)
    }
    pub(super) async fn active_conversation_turn(
        conn: &mut SqliteConnection,
        root: SessionId,
    ) -> Result<HarnessReceipt, Error> {
        let row = sqlx::query("SELECT h.session_id,s.team_id,h.task_id,h.sequence FROM harness_turns h JOIN harness_sessions s ON s.id=h.session_id WHERE h.session_id=COALESCE((SELECT c.coding_session_id FROM harness_project_continuations c JOIN harness_turns original ON original.task_id=c.source_task_id WHERE c.source_session_id=? ORDER BY c.source_sequence DESC,c.phase DESC LIMIT 1),?) ORDER BY NOT EXISTS(SELECT 1 FROM harness_unsettled_turns u WHERE u.task_id=h.task_id),CASE WHEN EXISTS(SELECT 1 FROM harness_unsettled_turns u WHERE u.task_id=h.task_id) THEN h.sequence ELSE -h.sequence END LIMIT 1")
            .bind(root.to_string()).bind(root.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        Self::conversation_receipt(&row)
    }
    fn conversation_receipt(row: &sqlx::sqlite::SqliteRow) -> Result<HarnessReceipt, Error> {
        let id = |key| -> Result<String, Error> { row.try_get(key).map_err(storage) };
        Ok(HarnessReceipt {
            session_id: id("session_id")?
                .parse()
                .map_err(|_| DomainError::InvalidState)?,
            team_id: id("team_id")?
                .parse()
                .map_err(|_| DomainError::InvalidState)?,
            task_id: id("task_id")?
                .parse()
                .map_err(|_| DomainError::InvalidState)?,
            sequence: row.try_get::<i64, _>("sequence").map_err(storage)? as u64,
        })
    }
    pub(super) async fn save_project_continuation(
        conn: &mut SqliteConnection,
        source: &ProjectContinuation,
        receipt: &HarnessReceipt,
    ) -> Result<(), Error> {
        let expected = coding_start_id(source.session_id, source.task_id);
        if receipt.task_id != expected
            || receipt.session_id.to_string() != expected.to_string()
            || receipt.sequence != 1
        {
            return Err(DomainError::ConflictingMessage.into());
        }
        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_turns original JOIN harness_sessions origin ON origin.id=original.session_id JOIN agent_teams discovery ON discovery.id=origin.team_id JOIN agent_tasks task ON task.id=original.task_id JOIN runs run ON run.id=task.run_id JOIN harness_turns coding ON coding.task_id=? JOIN harness_sessions destination ON destination.id=coding.session_id JOIN agent_teams work ON work.id=destination.team_id WHERE original.session_id=? AND original.task_id=? AND json_extract(discovery.payload,'$.spec.planner.config.discover_project')=1 AND run.status='succeeded' AND json_type(run.payload,'$.result.project_setup')='object' AND coding.sequence=1 AND coding.prompt=original.prompt AND json_extract(work.payload,'$.spec.source')=json_extract(discovery.payload,'$.spec.source') AND json_extract(work.payload,'$.spec.planner.config.target')=json_extract(run.payload,'$.input.input.target') AND json_type(work.payload,'$.spec.planner.config.test_command')='object' AND COALESCE(json_extract(work.payload,'$.spec.planner.config.discover_project'),0)=0)")
            .bind(receipt.task_id.to_string()).bind(source.session_id.to_string()).bind(source.task_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        if !valid {
            return Err(DomainError::InvalidState.into());
        }
        sqlx::query("INSERT INTO harness_project_continuations(source_session_id,source_task_id,coding_session_id,coding_task_id,source_sequence) SELECT ?,?,?,?,sequence FROM harness_turns WHERE task_id=? ON CONFLICT(source_task_id) DO NOTHING")
            .bind(source.session_id.to_string()).bind(source.task_id.to_string()).bind(receipt.session_id.to_string()).bind(receipt.task_id.to_string()).bind(source.task_id.to_string()).execute(&mut *conn).await.map_err(storage)?;
        let saved:(String,String,String)=sqlx::query_as("SELECT source_session_id,coding_session_id,coding_task_id FROM harness_project_continuations WHERE source_task_id=?")
            .bind(source.task_id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        if saved
            != (
                source.session_id.to_string(),
                receipt.session_id.to_string(),
                receipt.task_id.to_string(),
            )
        {
            return Err(DomainError::ConflictingMessage.into());
        }
        Ok(())
    }

    pub(super) async fn read_continuous_conversation(
        &self,
        session: SessionId,
        before: Option<ConversationCursor>,
        task: Option<MessageId>,
    ) -> Result<ContinuousConversation, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let root = Self::conversation_root(&mut tx, session).await?;
        if let Some(cursor) = before {
            cursor.validate(root)?;
        }
        let active = Self::active_conversation_turn(&mut tx, root).await?;
        let selected = if let Some(task) = task {
            let row=sqlx::query("SELECT h.session_id,s.team_id,h.task_id,h.sequence FROM harness_turns h JOIN harness_sessions s ON s.id=h.session_id LEFT JOIN harness_project_continuations c ON c.coding_session_id=h.session_id WHERE h.task_id=? AND (h.session_id=? OR c.source_session_id=?)")
                .bind(task.to_string()).bind(root.to_string()).bind(root.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?.ok_or(Error::AgentTaskNotFound(task))?;
            Self::conversation_receipt(&row)?
        } else {
            HarnessReceipt {
                session_id: active.session_id,
                team_id: active.team_id,
                task_id: active.task_id,
                sequence: active.sequence,
            }
        };
        let rows=sqlx::query("WITH ordered AS (SELECT h.*,s.team_id,COALESCE(c.source_sequence,h.sequence) AS source_sequence,CASE WHEN h.session_id=? THEN 0 ELSE c.phase END AS phase,COALESCE(c.kind,'root') AS continuation_kind FROM harness_turns h JOIN harness_sessions s ON s.id=h.session_id LEFT JOIN harness_project_continuations c ON c.coding_session_id=h.session_id LEFT JOIN harness_turns original ON original.task_id=c.source_task_id WHERE h.session_id=? OR c.source_session_id=?) SELECT h.*,t.run_id,substr(CAST(h.prompt AS BLOB),1,8196) AS prompt_preview,length(CAST(h.prompt AS BLOB))>8192 AS prompt_truncated,substr(CAST(COALESCE(json_extract(r.payload,'$.result.summary'),json_extract(r.payload,'$.result.answer'),json_extract(r.payload,'$.result'),json_extract(r.payload,'$.error.message'),json_extract(t.payload,'$.reply.error.message')) AS BLOB),1,16388) AS response,length(CAST(COALESCE(json_extract(r.payload,'$.result.summary'),json_extract(r.payload,'$.result.answer'),json_extract(r.payload,'$.result'),json_extract(r.payload,'$.error.message'),json_extract(t.payload,'$.reply.error.message')) AS BLOB))>16384 AS response_truncated,CASE WHEN t.id IS NULL THEN 'preparing' WHEN json_extract(t.payload,'$.status') IN ('succeeded','failed','rejected','budget_exhausted','cancelled') THEN json_extract(t.payload,'$.status') ELSE COALESCE(r.status,'queued') END AS status,NOT EXISTS(SELECT 1 FROM harness_unsettled_turns u WHERE u.task_id=h.task_id) AS settled FROM ordered h LEFT JOIN agent_tasks t ON t.id=h.task_id LEFT JOIN runs r ON r.id=t.run_id WHERE (? IS NULL OR (h.source_sequence,h.phase,h.sequence)<(?,?,?)) ORDER BY h.source_sequence DESC,h.phase DESC,h.sequence DESC LIMIT 33")
            .bind(root.to_string()).bind(root.to_string()).bind(root.to_string()).bind(before.map(|c|c.source_sequence as i64)).bind(before.map(|c|c.source_sequence as i64)).bind(before.map(|c|i64::from(c.phase))).bind(before.map(|c|c.sequence as i64)).fetch_all(&mut *tx).await.map_err(storage)?;
        let more = rows.len() > 32;
        let root_team=sqlx::query("SELECT s.team_id,COALESCE(json_extract(t.payload,'$.spec.planner.config.discover_project'),0) AS discovery FROM harness_sessions s JOIN agent_teams t ON t.id=s.team_id WHERE s.id=?")
            .bind(root.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
        let automatic: bool = root_team.try_get("discovery").map_err(storage)?;
        let mut turns = vec![];
        for row in rows.into_iter().take(32) {
            let receipt = Self::conversation_receipt(&row)?;
            let phase = row.try_get::<i64, _>("phase").map_err(storage)? as u8;
            let run: Option<String> = row.try_get("run_id").map_err(storage)?;
            let (clarifications, omitted_clarifications) = if let Some(run) = run {
                Self::conversation_clarifications(&mut tx, &run).await?
            } else {
                (vec![], 0)
            };
            let response: Option<Vec<u8>> = row.try_get("response").map_err(storage)?;
            let kind: String = row.try_get("continuation_kind").map_err(storage)?;
            let refresh: Option<(String, bool)> = if kind == "refresh" {
                sqlx::query_as("SELECT json_extract(p.payload,'$.request.prompt'),COALESCE(json_type(p.payload,'$.request.coding'),'')='object' FROM harness_project_refreshes p JOIN harness_project_continuations c ON c.coding_task_id=p.request_id WHERE c.coding_session_id=? AND p.admitted=1")
                    .bind(receipt.session_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?
            } else {
                None
            };
            let setup_update = refresh.as_ref().is_some_and(|(_, updated)| *updated);
            let refresh_prompt = refresh
                .filter(|_| receipt.sequence == 1)
                .map(|(prompt, _)| prompt);
            turns.push(ConversationTurn {
                session_id: receipt.session_id,
                team_id: receipt.team_id,
                phase: if setup_update {
                    "setup_update"
                } else if kind == "repair" || kind == "refresh" {
                    &kind
                } else if phase == 0 && automatic {
                    "discovery"
                } else {
                    "coding"
                }
                .into(),
                repeated_request: phase == 1
                    && receipt.sequence == 1
                    && row
                        .try_get::<String, _>("continuation_kind")
                        .map_err(storage)?
                        == "setup",
                cursor: ConversationCursor {
                    session_id: root,
                    source_sequence: row.try_get::<i64, _>("source_sequence").map_err(storage)?
                        as u64,
                    phase,
                    sequence: receipt.sequence,
                },
                clarifications,
                omitted_clarifications,
                turn: HarnessTurn {
                    task_id: receipt.task_id,
                    sequence: receipt.sequence,
                    prompt: refresh_prompt
                        .clone()
                        .map(|prompt| super::harness_store::preview(prompt.into_bytes(), 8192))
                        .unwrap_or(super::harness_store::preview(
                            row.try_get("prompt_preview").map_err(storage)?,
                            8192,
                        )),
                    prompt_truncated: if let Some(prompt) = refresh_prompt {
                        prompt.len() > 8192
                    } else {
                        row.try_get("prompt_truncated").map_err(storage)?
                    },
                    response: response.map(|b| super::harness_store::preview(b, 16384)),
                    response_truncated: row
                        .try_get::<Option<bool>, _>("response_truncated")
                        .map_err(storage)?
                        .unwrap_or(false),
                    status: row.try_get("status").map_err(storage)?,
                    created_at: row.try_get("created_at").map_err(storage)?,
                    settled: row.try_get("settled").map_err(storage)?,
                },
            });
        }
        let next_before = if more {
            turns.last().map(|t| t.cursor)
        } else {
            None
        };
        turns.reverse();
        let metadata = Self::load_session_metadata(&mut tx, root).await?;
        let active_team = Self::load_team(&mut tx, active.team_id).await?;
        let current=sqlx::query("SELECT CASE WHEN r.status='failed' AND json_extract(r.payload,'$.error.code')='coding_incomplete' THEN 'incomplete' ELSE COALESCE(r.status,json_extract(t.payload,'$.status'),'queued') END AS status,json_extract(r.payload,'$.result') AS result,NOT EXISTS(SELECT 1 FROM harness_unsettled_turns u WHERE u.task_id=?) AS settled FROM agent_tasks t LEFT JOIN runs r ON r.id=t.run_id WHERE t.id=?")
            .bind(active.task_id.to_string()).bind(active.task_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?;
        let (status, result, settled) = if let Some(row) = current {
            let raw: Option<String> = row.try_get("result").map_err(storage)?;
            (
                row.try_get::<String, _>("status").map_err(storage)?,
                raw.map(|v| serde_json::from_str(&v))
                    .transpose()?
                    .unwrap_or(Value::Null),
                row.try_get::<bool, _>("settled").map_err(storage)?,
            )
        } else {
            ("queued".into(), Value::Null, false)
        };
        let verifying:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_turn_ownership o JOIN agent_tasks t ON t.id=o.id JOIN runs r ON r.id=t.run_id WHERE o.root_task_id=? AND (json_extract(r.payload,'$.wait.payload.tool.name')='run_tests' OR EXISTS(SELECT 1 FROM activities a WHERE a.run_id=r.id AND a.status IN ('queued','running') AND json_extract(a.payload,'$.spec.handler.name')='native.tool' AND json_extract(a.payload,'$.spec.input.definition.id.name')='run_tests')))")
            .bind(active.task_id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
        let discovery = active_team.spec.planner.config["discover_project"] == true;
        let archived =
            metadata.archived || Self::conversation_archived(&mut tx, active.session_id).await?;
        let auto_execute = active_team.spec.planner.config["auto_execute"] == true;
        let setup_error: Option<String> =
            sqlx::query_scalar("SELECT message FROM harness_auto_setup_errors WHERE task_id=?")
                .bind(active.task_id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        let stage = if setup_error.is_some() {
            ConversationStage::Failed
        } else {
            stage(discovery, &status, &result, settled, verifying)
        };
        let coding = if active_team
            .spec
            .planner
            .config
            .get("test_command")
            .is_some()
        {
            Some(HarnessCoding {
                writable_paths: serde_json::from_value(
                    active_team.spec.planner.config["writable_paths"].clone(),
                )?,
                test_command: serde_json::from_value(
                    active_team.spec.planner.config["test_command"].clone(),
                )?,
                project_commands: serde_json::from_value(
                    active_team
                        .spec
                        .planner
                        .config
                        .get("project_commands")
                        .cloned()
                        .unwrap_or(serde_json::json!({})),
                )?,
                coder_count: active_team.spec.planner.config["delegation"]["max_parallel_tasks"]
                    .as_u64()
                    .unwrap_or(1) as u8,
            })
        } else {
            None
        };
        let can_continue: bool =
            sqlx::query_scalar("SELECT can_continue FROM harness_sessions WHERE id=?")
                .bind(active.session_id.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
        let setup_failed = setup_error.is_some();
        let view = ContinuousConversation {
            auto_execute,
            setup_error,
            requested_session_id: session,
            session_id: root,
            title: metadata.title,
            archived,
            automatic,
            stage,
            can_continue: can_continue
                && !archived
                && !setup_failed
                && (!discovery || settled && stage != ConversationStage::SetupReview),
            model: active_team.spec.planner.config["target"]["model"]
                .as_str()
                .ok_or(DomainError::InvalidState)?
                .into(),
            workspace: active_team.spec.source,
            coding,
            active,
            selected,
            turns,
            next_before,
        };
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    async fn conversation_clarifications(
        conn: &mut SqliteConnection,
        run: &str,
    ) -> Result<(Vec<SavedClarification>, usize), Error> {
        let rows=sqlx::query("SELECT json_extract(receipt.value,'$.wait_id') AS wait_id,json_extract(receipt.value,'$.payload.answer') AS answer,(SELECT json_extract(e.payload,'$.payload.wait.prompt') FROM history e WHERE e.run_id=r.id AND json_extract(e.payload,'$.kind')='input_wait_created' AND json_extract(e.payload,'$.payload.wait.id')=json_extract(receipt.value,'$.wait_id') ORDER BY e.sequence LIMIT 1) AS question,COUNT(*) OVER () AS total FROM runs r,json_each(json_extract(r.payload,'$.input_receipts')) receipt WHERE r.id=? AND json_type(receipt.value,'$.payload.answer')='text' ORDER BY CAST(receipt.key AS INTEGER) DESC LIMIT 16")
            .bind(run).fetch_all(&mut *conn).await.map_err(storage)?;
        let total = rows
            .first()
            .map(|r| r.try_get::<i64, _>("total").map_err(storage))
            .transpose()?
            .unwrap_or(0) as usize;
        let mut items = vec![];
        for row in rows {
            let wait: String = row.try_get("wait_id").map_err(storage)?;
            let question: String = row.try_get("question").map_err(storage)?;
            let answer: String = row.try_get("answer").map_err(storage)?;
            items.push(SavedClarification {
                wait_id: wait.parse().map_err(|_| DomainError::InvalidState)?,
                question: discovery_context::Excerpt::new(&question),
                answer: discovery_context::Excerpt::new(&answer),
            });
        }
        items.reverse();
        Ok((items, total.saturating_sub(16)))
    }
}
