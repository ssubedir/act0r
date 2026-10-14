use super::*;
use crate::domain::session_summary::*;

impl SqliteStore {
    pub(super) async fn automatic_summary_view(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<AutomaticSummary, Error> {
        let policy: Option<(bool, i64)> = sqlx::query_as(
            "SELECT enabled,revision FROM harness_summary_policies WHERE session_id=?",
        )
        .bind(session.to_string())
        .fetch_optional(&mut *conn)
        .await
        .map_err(storage)?;
        let gate = sqlx::query("SELECT next_task_id,through_sequence,run_id,blocked_reason FROM harness_summary_gates WHERE session_id=?")
            .bind(session.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        let (enabled, revision) = policy.unwrap_or((false, 0));
        let mut view = AutomaticSummary {
            enabled,
            revision: revision as u64,
            turn_threshold: AUTO_TURN_THRESHOLD,
            byte_threshold: AUTO_BYTE_THRESHOLD,
            recent_turns: AUTO_RECENT_TURNS,
            held_turn: None,
            through_sequence: None,
            run_id: None,
            blocked_reason: None,
        };
        if let Some(gate) = gate {
            view.held_turn = Some(
                gate.try_get::<String, _>("next_task_id")
                    .map_err(storage)?
                    .parse()
                    .map_err(|_| DomainError::InvalidState)?,
            );
            view.through_sequence = Some(
                gate.try_get::<i64, _>("through_sequence")
                    .map_err(storage)? as u64,
            );
            view.run_id = gate
                .try_get::<Option<String>, _>("run_id")
                .map_err(storage)?
                .map(|id| id.parse().map_err(|_| DomainError::InvalidState))
                .transpose()?;
            view.blocked_reason = gate.try_get("blocked_reason").map_err(storage)?;
        }
        Ok(view)
    }

    pub(super) async fn update_summary_policy(
        &self,
        session: SessionId,
        request: SummaryPolicyRequest,
    ) -> Result<SummaryPolicyReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<(String,i64,String)> = sqlx::query_as("SELECT session_id,revision,request FROM harness_summary_policy_requests WHERE request_id=?")
            .bind(request.request_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?;
        if let Some((owner, revision, body)) = saved {
            if owner != session.to_string()
                || serde_json::from_str::<SummaryPolicyRequest>(&body)? != request
            {
                return Err(DomainError::SessionConflict(
                    "policy request ID already identifies another decision".into(),
                )
                .into());
            }
            tx.commit().await.map_err(storage)?;
            return Ok(SummaryPolicyReceipt {
                session_id: session,
                revision: revision as u64,
                request,
                replayed: true,
            });
        }
        let metadata = Self::load_session_metadata(&mut tx, session).await?;
        let can_continue: bool =
            sqlx::query_scalar("SELECT can_continue FROM harness_sessions WHERE id=?")
                .bind(session.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?;
        if metadata.archived || !can_continue {
            return Err(DomainError::SessionConflict(
                "automatic summarization requires an active conversation session".into(),
            )
            .into());
        }
        let view = Self::automatic_summary_view(&mut tx, session).await?;
        if request.expected_revision != view.revision {
            return Err(DomainError::SessionConflict(
                "summary policy changed; refresh before changing it".into(),
            )
            .into());
        }
        let revision = view.revision + 1;
        sqlx::query("INSERT INTO harness_summary_policies(session_id,revision,enabled) VALUES(?,?,?) ON CONFLICT(session_id) DO UPDATE SET revision=excluded.revision,enabled=excluded.enabled")
            .bind(session.to_string()).bind(revision as i64).bind(request.enabled).execute(&mut *tx).await.map_err(storage)?;
        sqlx::query("INSERT INTO harness_summary_policy_requests(request_id,session_id,revision,request) VALUES(?,?,?,?)")
            .bind(request.request_id.to_string()).bind(session.to_string()).bind(revision as i64).bind(serde_json::to_string(&request)?).execute(&mut *tx).await.map_err(storage)?;
        // An admitted Run still settles. Turning off the policy releases only failed/preflight holds.
        if !request.enabled && !Self::summary_pending(&mut tx, session).await? {
            sqlx::query("DELETE FROM harness_summary_gates WHERE session_id=?")
                .bind(session.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
        }
        tx.commit().await.map_err(storage)?;
        Ok(SummaryPolicyReceipt {
            session_id: session,
            revision,
            request,
            replayed: false,
        })
    }

    /// A held retry uses the frozen sources from its failed Run, despite later queued turns.
    pub(super) async fn summary_admission_preview(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<(SessionSummary, Option<SummaryInput>), Error> {
        let (mut view, mut input) = Self::summary_preview_through(conn, session, None).await?;
        if view.automatic.held_turn.is_some() && !view.pending {
            let frozen: Option<String> =
                sqlx::query_scalar("SELECT input FROM harness_summary_gates WHERE session_id=?")
                    .bind(session.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            if let Some(frozen) = frozen {
                let mut saved: SummaryInput = serde_json::from_str(&frozen)?;
                let through = saved
                    .sources
                    .last()
                    .ok_or(DomainError::InvalidState)?
                    .reference
                    .sequence;
                let (prefix, _) =
                    Self::summary_preview_through(conn, session, Some(through)).await?;
                if prefix.blocked_reason.is_none() && prefix.automatic.enabled {
                    saved.revision = view.revision + 1;
                    saved.validate()?;
                    view.next_sources = saved.sources.iter().map(|s| s.reference.clone()).collect();
                    view.next_through_sequence = Some(through);
                    view.blocked_reason = None;
                    input = Some(saved);
                }
            }
        }
        Ok((view, input))
    }

    /// Evaluated inside mailbox admission, after all earlier ownership trees settle.
    /// The summary Run, frozen boundary and untouched root message commit together.
    pub(super) async fn admit_automatic_summary(
        &self,
        conn: &mut SqliteConnection,
        task: MessageId,
        now: i64,
        factory: SummaryRunFactory,
    ) -> Result<bool, Error> {
        let candidate: Option<(String,i64,i64)> = sqlx::query_as("SELECT t.session_id,t.sequence,p.revision FROM harness_turns t JOIN harness_summary_policies p ON p.session_id=t.session_id WHERE t.task_id=? AND p.enabled=1")
            .bind(task.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        let Some((session, next, policy_revision)) = candidate else {
            return Ok(false);
        };
        let session: SessionId = session.parse().map_err(|_| DomainError::InvalidState)?;
        let latest: Option<String> = sqlx::query_scalar(
            "SELECT latest_run_id FROM harness_summary_heads WHERE session_id=?",
        )
        .bind(session.to_string())
        .fetch_optional(&mut *conn)
        .await
        .map_err(storage)?
        .flatten();
        let through = if let Some(latest) = latest {
            Self::summary_snapshot(conn, latest.parse().map_err(|_| DomainError::InvalidState)?)
                .await?
                .through_sequence
        } else {
            0
        };
        let bytes: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(length(CAST(h.prompt AS BLOB))+COALESCE(length(CAST(COALESCE(json_extract(r.payload,'$.result.summary'),json_extract(r.payload,'$.result.answer'),json_extract(r.payload,'$.result'),json_extract(r.payload,'$.error.message')) AS BLOB)),0)),0) FROM harness_turns h JOIN agent_tasks t ON t.id=h.task_id LEFT JOIN runs r ON r.id=t.run_id WHERE h.session_id=? AND h.sequence>? AND h.sequence<?")
            .bind(session.to_string()).bind(through as i64).bind(next).fetch_one(&mut *conn).await.map_err(storage)?;
        let Some(boundary) = automatic_boundary(through, next as u64, bytes as u64) else {
            return Ok(false);
        };
        let (view, input) = Self::summary_preview_through(conn, session, Some(boundary)).await?;
        let mut run_id = None;
        let actual_boundary = input
            .as_ref()
            .and_then(|input| input.sources.last())
            .map(|s| s.reference.sequence)
            .unwrap_or(boundary);
        let frozen = input.as_ref().map(serde_json::to_string).transpose()?;
        if let Some(input) = input {
            if view.blocked_reason.is_some() {
                return Err(DomainError::InvalidState.into());
            }
            let expected = serde_json::to_value(&input)?;
            let run = factory(input)?;
            if run.record().workflow != workflow()
                || run.record().input != expected
                || run.record().status != RunStatus::Queued
                || run.record().created_at != now
            {
                return Err(DomainError::InvalidState.into());
            }
            self.save(conn, &run, true).await?;
            sqlx::query("INSERT INTO harness_summary_heads(session_id,revision,pending_run_id,last_run_id) VALUES(?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET revision=excluded.revision,pending_run_id=excluded.pending_run_id,last_run_id=excluded.last_run_id")
                .bind(session.to_string()).bind(view.revision as i64+1).bind(run.record().id.to_string()).bind(run.record().id.to_string()).execute(&mut *conn).await.map_err(storage)?;
            run_id = Some(run.record().id.to_string());
            sqlx::query("INSERT INTO harness_automatic_summary_attempts(run_id,next_task_id,policy_revision,through_sequence) VALUES(?,?,?,?)")
                .bind(&run_id).bind(task.to_string()).bind(policy_revision).bind(actual_boundary as i64).execute(&mut *conn).await.map_err(storage)?;
        }
        sqlx::query("INSERT INTO harness_summary_gates(session_id,next_task_id,policy_revision,through_sequence,run_id,input,blocked_reason) VALUES(?,?,?,?,?,?,?)")
            .bind(session.to_string()).bind(task.to_string()).bind(policy_revision).bind(actual_boundary as i64).bind(run_id).bind(frozen)
            .bind(view.blocked_reason).execute(&mut *conn).await.map_err(storage)?;
        Ok(true)
    }
}
