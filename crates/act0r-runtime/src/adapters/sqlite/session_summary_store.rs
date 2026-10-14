use super::*;
use crate::domain::session_summary::*;
type SummaryHead = (i64, Option<String>, Option<String>, Option<String>);

impl SqliteStore {
    pub(super) async fn summary_pending(
        conn: &mut SqliteConnection,
        session: SessionId,
    ) -> Result<bool, Error> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_summary_heads WHERE session_id=? AND pending_run_id IS NOT NULL)")
            .bind(session.to_string()).fetch_one(conn).await.map_err(storage)
    }
    pub(super) async fn summary_snapshot(
        conn: &mut SqliteConnection,
        id: RunId,
    ) -> Result<SummarySnapshot, Error> {
        let run = Self::load(conn, id).await?;
        if run.record().workflow != workflow() || run.record().status != RunStatus::Succeeded {
            return Err(DomainError::InvalidState.into());
        }
        let input: SummaryInput = serde_json::from_value(run.record().input.clone())?;
        let text = run
            .record()
            .result
            .as_ref()
            .and_then(|r| r["summary"].as_str())
            .ok_or(DomainError::InvalidState)?;
        Ok(input.snapshot(id, text.into())?)
    }
    pub(super) async fn summary_preview_through(
        conn: &mut SqliteConnection,
        session: SessionId,
        boundary: Option<u64>,
    ) -> Result<(SessionSummary, Option<SummaryInput>), Error> {
        let (team, mut blocked_reason) = if let Some(boundary) = boundary {
            let team_id: String =
                sqlx::query_scalar("SELECT team_id FROM harness_sessions WHERE id=?")
                    .bind(session.to_string())
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(storage)?
                    .ok_or(Error::SessionNotFound(session))?;
            let team = Self::load_team(
                conn,
                team_id.parse().map_err(|_| DomainError::InvalidState)?,
            )
            .await?;
            Self::check_team_actors(conn, &team).await?;
            let unsettled: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM harness_turns t JOIN harness_unsettled_turns u ON u.task_id=t.task_id WHERE t.session_id=? AND t.sequence<=?)")
                .bind(session.to_string()).bind(boundary as i64).fetch_one(&mut *conn).await.map_err(storage)?;
            let reason = if unsettled || team.status != teams::TeamStatus::Ready {
                Some("summary sources have unsettled work".into())
            } else {
                None
            };
            (team, reason)
        } else {
            Self::session_storage_state(conn, session).await?
        };
        let config: actors::RegisteredAgentConfig =
            serde_json::from_value(team.planner()?.pinned_config.clone())?;
        let planner: planner::PlannerConfig = serde_json::from_value(config.input)?;
        let budget = planner.goal_budget.clone().unwrap_or(UsageBudget {
            max_model_calls: planner.max_model_calls,
            max_tokens: planner.max_tokens,
        });
        let metadata = Self::load_session_metadata(conn, session).await?;
        let can_continue: bool =
            sqlx::query_scalar("SELECT can_continue FROM harness_sessions WHERE id=?")
                .bind(session.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
        if metadata.archived {
            blocked_reason = Some("restore this session before summarizing".into());
        }
        if !can_continue {
            blocked_reason =
                Some("start a new session; this saved session predates conversation memory".into());
        }
        let head:Option<SummaryHead>=sqlx::query_as("SELECT revision,latest_run_id,pending_run_id,last_run_id FROM harness_summary_heads WHERE session_id=?")
            .bind(session.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        let (revision, latest, pending, last) = head.unwrap_or((0, None, None, None));
        let summary = match latest {
            Some(id) => Some(
                Self::summary_snapshot(conn, id.parse().map_err(|_| DomainError::InvalidState)?)
                    .await?,
            ),
            None => None,
        };
        if summary.as_ref().is_some_and(|s| s.session_id != session) {
            return Err(DomainError::InvalidState.into());
        }
        let last_attempt = match last {
            Some(id) => {
                let run =
                    Self::load(conn, id.parse().map_err(|_| DomainError::InvalidState)?).await?;
                Some(SummaryAttempt {
                    run_id: run.record().id,
                    status: run.record().status,
                    error: run.record().error.clone(),
                    usage: run.record().usage.clone(),
                })
            }
            None => None,
        };
        let through = summary.as_ref().map(|s| s.through_sequence).unwrap_or(0);
        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM harness_turns WHERE session_id=? AND sequence>? AND sequence<=?",
        )
        .bind(session.to_string())
        .bind(through as i64)
        .bind(boundary.unwrap_or(i64::MAX as u64) as i64)
        .fetch_one(&mut *conn)
        .await
        .map_err(storage)?;
        let mut sources = Vec::new();
        if blocked_reason.is_none() {
            // Each selected field is capped above the batch bound. Oversized turns are refused,
            // never silently truncated or skipped; later batches retain contiguous coverage.
            let rows=sqlx::query("SELECT h.task_id,h.sequence,substr(CAST(h.prompt AS BLOB),1,49153) AS prompt,
                json_extract(t.payload,'$.status') AS status,
                substr(CAST(COALESCE(json_extract(r.payload,'$.result.summary'),json_extract(r.payload,'$.result.answer'),json_extract(r.payload,'$.result'),json_extract(r.payload,'$.error.message'),json_extract(t.payload,'$.reply.error.message')) AS BLOB),1,49153) AS response
                FROM harness_turns h JOIN agent_tasks t ON t.id=h.task_id LEFT JOIN runs r ON r.id=t.run_id
                WHERE h.session_id=? AND h.sequence>? AND h.sequence<=? ORDER BY h.sequence LIMIT 32")
                .bind(session.to_string()).bind(through as i64).bind(boundary.unwrap_or(i64::MAX as u64) as i64).fetch_all(&mut *conn).await.map_err(storage)?;
            for row in rows {
                let prompt: Vec<u8> = row.try_get("prompt").map_err(storage)?;
                let response: Option<Vec<u8>> = row.try_get("response").map_err(storage)?;
                let oversized = prompt.len() > MAX_SOURCE_BYTES
                    || response
                        .as_ref()
                        .is_some_and(|r| r.len() > MAX_SOURCE_BYTES);
                if oversized {
                    if sources.is_empty() {
                        blocked_reason = Some(
                            "the next complete turn exceeds the 48 KiB summary source bound".into(),
                        );
                    }
                    break;
                }
                let source = SummarySource {
                    reference: SummarySourceRef {
                        task_id: row
                            .try_get::<String, _>("task_id")
                            .map_err(storage)?
                            .parse()
                            .map_err(|_| DomainError::InvalidState)?,
                        sequence: row.try_get::<i64, _>("sequence").map_err(storage)? as u64,
                        status: row.try_get("status").map_err(storage)?,
                    },
                    prompt: String::from_utf8(prompt).map_err(|_| DomainError::InvalidState)?,
                    response: response
                        .map(String::from_utf8)
                        .transpose()
                        .map_err(|_| DomainError::InvalidState)?,
                };
                sources.push(source);
                if serde_json::to_vec(&sources)?.len() > MAX_SOURCE_BYTES {
                    sources.pop();
                    if sources.is_empty() {
                        blocked_reason = Some(
                            "the next complete turn exceeds the 48 KiB summary source bound".into(),
                        );
                    }
                    break;
                }
                let candidate = SummaryInput {
                    session_id: session,
                    revision: revision as u64 + 1,
                    previous: summary.clone(),
                    sources: sources.clone(),
                    target: planner.target.clone(),
                    budget: budget.clone(),
                };
                if candidate.model_request().context_input_bytes() > MAX_INPUT_BYTES {
                    sources.pop();
                    if sources.is_empty() {
                        blocked_reason=Some("the next complete turn and previous summary exceed the 65536-byte model input bound".into());
                    }
                    break;
                }
            }
            if sources.is_empty() && blocked_reason.is_none() {
                blocked_reason = Some("no new settled turns to summarize".into());
            }
        }
        let next_sources = sources
            .iter()
            .map(|s| s.reference.clone())
            .collect::<Vec<_>>();
        let next_through_sequence = next_sources.last().map(|s| s.sequence);
        let input = if sources.is_empty() {
            None
        } else {
            let input = SummaryInput {
                session_id: session,
                revision: revision as u64 + 1,
                previous: summary.clone(),
                sources,
                target: planner.target.clone(),
                budget: budget.clone(),
            };
            input.validate()?;
            Some(input)
        };
        Ok((
            SessionSummary {
                session_id: session,
                revision: revision as u64,
                summary,
                pending: pending.is_some(),
                last_attempt,
                next_sources,
                next_through_sequence,
                remaining_turns: remaining as u64,
                blocked_reason,
                target: planner.target,
                budget,
                automatic: Self::automatic_summary_view(conn, session).await?,
            },
            input,
        ))
    }
    pub(super) async fn read_session_summary(
        &self,
        session: SessionId,
    ) -> Result<SessionSummary, Error> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let (view, _) = Self::summary_admission_preview(&mut tx, session).await?;
        tx.commit().await.map_err(storage)?;
        Ok(view)
    }
    pub(super) async fn admit_session_summary(
        &self,
        session: SessionId,
        request: SummaryRequest,
        now: i64,
        factory: SummaryRunFactory,
    ) -> Result<SummaryReceipt, Error> {
        request.validate()?;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<(String, String, String)> = sqlx::query_as(
            "SELECT session_id,run_id,request FROM harness_summary_requests WHERE request_id=?",
        )
        .bind(request.request_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?;
        if let Some((owner, run, body)) = saved {
            if owner != session.to_string()
                || serde_json::from_str::<SummaryRequest>(&body)? != request
            {
                return Err(DomainError::SessionConflict(
                    "summary request ID already identifies another decision".into(),
                )
                .into());
            }
            tx.commit().await.map_err(storage)?;
            return Ok(SummaryReceipt {
                session_id: session,
                run_id: run.parse().map_err(|_| DomainError::InvalidState)?,
                request,
                replayed: true,
            });
        }
        let (view, input) = Self::summary_admission_preview(&mut tx, session).await?;
        if request.expected_revision != view.revision {
            return Err(DomainError::SessionConflict(
                "summary changed; refresh before summarizing".into(),
            )
            .into());
        }
        if let Some(reason) = view.blocked_reason {
            return Err(DomainError::SessionConflict(reason).into());
        }
        if Some(request.through_sequence) != view.next_through_sequence {
            return Err(DomainError::SessionConflict(
                "summary sources changed; refresh before summarizing".into(),
            )
            .into());
        }
        let input = input.ok_or(DomainError::InvalidState)?;
        let expected = serde_json::to_value(&input)?;
        let run = factory(input.clone())?;
        if run.record().workflow != workflow()
            || run.record().input != expected
            || run.record().status != RunStatus::Queued
            || run.record().created_at != now
        {
            return Err(DomainError::InvalidState.into());
        }
        self.save(&mut tx, &run, true).await?;
        sqlx::query("INSERT INTO harness_summary_requests(request_id,session_id,run_id,request) VALUES(?,?,?,?)")
            .bind(request.request_id.to_string()).bind(session.to_string()).bind(run.record().id.to_string()).bind(serde_json::to_string(&request)?)
            .execute(&mut *tx).await.map_err(storage)?;
        sqlx::query("INSERT INTO harness_summary_heads(session_id,revision,pending_run_id,last_run_id) VALUES(?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET revision=excluded.revision,pending_run_id=excluded.pending_run_id,last_run_id=excluded.last_run_id")
            .bind(session.to_string()).bind(view.revision as i64+1).bind(run.record().id.to_string()).bind(run.record().id.to_string()).execute(&mut *tx).await.map_err(storage)?;
        sqlx::query("INSERT INTO harness_automatic_summary_attempts(run_id,next_task_id,policy_revision,through_sequence) SELECT ?,next_task_id,policy_revision,through_sequence FROM harness_summary_gates WHERE session_id=?")
            .bind(run.record().id.to_string()).bind(session.to_string()).execute(&mut *tx).await.map_err(storage)?;
        sqlx::query("UPDATE harness_summary_gates SET run_id=?,input=?,blocked_reason=NULL WHERE session_id=?")
            .bind(run.record().id.to_string()).bind(serde_json::to_string(&input)?).bind(session.to_string()).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(SummaryReceipt {
            session_id: session,
            run_id: run.record().id,
            request,
            replayed: false,
        })
    }
    /// Called inside the same transaction that records the model outcome and usage.
    pub(super) async fn publish_session_summary(
        conn: &mut SqliteConnection,
        run: &Run,
    ) -> Result<(), Error> {
        let record = run.record();
        if record.workflow != workflow()
            || !matches!(
                record.status,
                RunStatus::Succeeded
                    | RunStatus::Failed
                    | RunStatus::BudgetExhausted
                    | RunStatus::Cancelled
            )
        {
            return Ok(());
        }
        let input: SummaryInput = serde_json::from_value(record.input.clone())?;
        if record.status == RunStatus::Succeeded {
            let text = record
                .result
                .as_ref()
                .and_then(|v| v["summary"].as_str())
                .ok_or(DomainError::InvalidState)?;
            input.snapshot(record.id, text.into())?;
        }
        sqlx::query("UPDATE harness_summary_heads SET pending_run_id=NULL,latest_run_id=CASE WHEN ? THEN ? ELSE latest_run_id END WHERE session_id=? AND pending_run_id=? AND revision=?")
            .bind(record.status==RunStatus::Succeeded).bind(record.id.to_string()).bind(input.session_id.to_string()).bind(record.id.to_string()).bind(input.revision as i64)
            .execute(&mut *conn).await.map_err(storage)?;
        if record.status == RunStatus::Succeeded {
            sqlx::query("DELETE FROM harness_summary_gates WHERE session_id=? AND run_id=?")
                .bind(input.session_id.to_string())
                .bind(record.id.to_string())
                .execute(&mut *conn)
                .await
                .map_err(storage)?;
        } else {
            sqlx::query(
                "UPDATE harness_summary_gates SET blocked_reason=? WHERE session_id=? AND run_id=?",
            )
            .bind(
                record
                    .error
                    .as_ref()
                    .map(|e| e.message.clone())
                    .unwrap_or_else(|| format!("summary {:?}", record.status)),
            )
            .bind(input.session_id.to_string())
            .bind(record.id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
            sqlx::query("DELETE FROM harness_summary_gates WHERE session_id=? AND run_id=? AND NOT EXISTS(SELECT 1 FROM harness_summary_policies p WHERE p.session_id=harness_summary_gates.session_id AND p.enabled=1)")
                .bind(input.session_id.to_string()).bind(record.id.to_string()).execute(&mut *conn).await.map_err(storage)?;
        }
        Ok(())
    }
    pub(super) async fn task_session_summaries(
        conn: &mut SqliteConnection,
        actor: AgentId,
        origins: &mut ActorTaskOrigins,
    ) -> Result<(), Error> {
        let rows:Vec<(String,String,i64)>=sqlx::query_as("SELECT t.id,h.latest_run_id,turn.sequence FROM agent_tasks t JOIN harness_turn_ownership o ON o.id=t.id JOIN harness_turns turn ON turn.task_id=o.root_task_id JOIN harness_summary_heads h ON h.session_id=turn.session_id WHERE t.agent_id=? AND t.run_id IS NULL AND json_extract(t.payload,'$.status')='queued' AND h.latest_run_id IS NOT NULL")
            .bind(actor.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        for (task, run, sequence) in rows {
            let snapshot =
                Self::summary_snapshot(conn, run.parse().map_err(|_| DomainError::InvalidState)?)
                    .await?;
            if snapshot.through_sequence < sequence as u64 {
                // Read at most the actor's bounded retained entries, including delegated tasks
                // covered by any earlier summary, not just the latest batch's source refs.
                let covered: Vec<String> = sqlx::query_scalar("SELECT json_extract(e.value,'$.task_id') FROM agent_actors a,json_each(a.payload,'$.state.memory.entries') e JOIN harness_turn_ownership o ON o.id=json_extract(e.value,'$.task_id') JOIN harness_turns t ON t.task_id=o.root_task_id WHERE a.id=? AND t.session_id=? AND t.sequence<=?")
                    .bind(actor.to_string()).bind(snapshot.session_id.to_string()).bind(snapshot.through_sequence as i64).fetch_all(&mut *conn).await.map_err(storage)?;
                let task_id = task.parse().map_err(|_| DomainError::InvalidState)?;
                origins.summarized_memory.insert(
                    task_id,
                    covered
                        .into_iter()
                        .map(|id| {
                            id.parse()
                                .map_err(|_| Error::from(DomainError::InvalidState))
                        })
                        .collect::<Result<_, _>>()?,
                );
                origins.session_summaries.insert(task_id, snapshot);
            }
        }
        Ok(())
    }
}
