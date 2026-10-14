use super::*;
use crate::domain::{tools::*, worker_messages::PeerMessage, worker_questions::*};
use serde_json::json;

struct Scope {
    session: SessionId,
    turn: MessageId,
    task: MessageId,
    name: String,
    source: Run,
    parent: Run,
    parent_task: MessageId,
    group: Option<TaskGroupId>,
    team: teams::TeamRecord,
}
fn result(r: &QuestionReceipt) -> NativeToolResult {
    match &r.error {
        Some(error) => NativeToolResult::Rejected(error.clone()),
        None => NativeToolResult::Success(
            json!({"ok":true,"question_id":r.question_id,"deadline_at":r.deadline_at}),
        ),
    }
}
impl SqliteStore {
    async fn question_scope(
        conn: &mut SqliteConnection,
        call: &NativeToolCall,
        tool: &str,
    ) -> Result<Scope, Error> {
        let source = Self::load(conn, call.run_id).await?;
        let activity = source
            .activities()
            .iter()
            .find(|a| a.id == call.activity_id && a.status == ActivityStatus::Running)
            .ok_or(DomainError::InvalidState)?;
        let pinned: NativeToolRequest = serde_json::from_value(activity.spec.input.clone())?;
        if pinned.definition.id.name != tool
            || pinned.definition.id.version != 2
            || pinned.arguments != call.arguments
            || activity.active_attempt != Some(call.attempt_id)
            || activity.spec.handler != pinned.definition.handler()
            || !source.attempts().iter().any(|a| {
                a.id == call.attempt_id
                    && a.activity_id == call.activity_id
                    && a.status == AttemptStatus::Running
            })
        {
            return Err(DomainError::InvalidState.into());
        }
        let task: String = sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
            .bind(call.run_id.to_string())
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
        let task: MessageId = task.parse().map_err(|_| DomainError::InvalidState)?;
        let turn = Self::goal_root(conn, task).await?;
        let session: String =
            sqlx::query_scalar("SELECT session_id FROM harness_turns WHERE task_id=?")
                .bind(turn.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
        let session: SessionId = session.parse().map_err(|_| DomainError::InvalidState)?;
        let team: String = sqlx::query_scalar("SELECT team_id FROM harness_sessions WHERE id=?")
            .bind(session.to_string())
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
        let team =
            Self::load_team(conn, team.parse().map_err(|_| DomainError::InvalidState)?).await?;
        let physical = Self::load_agent_task(conn, task).await?;
        if task == turn
            && team.members.iter().any(|m| {
                m.role == teams::TeamRole::Planner && m.agent_id == physical.record().agent_id
            })
        {
            if tool != "reply_worker" || !source.accepts_planner_questions() {
                return Err(DomainError::InvalidState.into());
            }
            return Ok(Scope {
                session,
                turn,
                task,
                name: "parent".into(),
                parent: source.clone(),
                parent_task: task,
                source,
                group: None,
                team,
            });
        }
        let (assignment, delegation, _) =
            Self::harness_worker_assignment(conn, session, turn, task).await?;
        let name = team
            .members
            .iter()
            .find(|m| {
                m.agent_id == assignment.record().agent_id
                    && matches!(m.name.as_str(), "coder" | "coder2")
            })
            .ok_or(DomainError::InvalidState)?
            .name
            .clone();
        let parent: String =
            sqlx::query_scalar("SELECT parent_id FROM agent_goal_ownership WHERE id=?")
                .bind(task.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
        let parent =
            Self::load_agent_task(conn, parent.parse().map_err(|_| DomainError::InvalidState)?)
                .await?;
        let parent_task = parent.record().id;
        let parent = Self::load(
            conn,
            parent.record().run_id.ok_or(DomainError::InvalidState)?,
        )
        .await?;
        Ok(Scope {
            session,
            turn,
            task,
            name,
            source,
            parent,
            parent_task,
            group: delegation.group_id,
            team,
        })
    }
    async fn question_closed(
        conn: &mut SqliteConnection,
        task: MessageId,
        run: RunId,
    ) -> Result<bool, Error> {
        let task_state = Self::load_agent_task(conn, task).await?;
        let run = Self::load(conn, run).await?;
        Ok(task_state.record().cancel_requested
            || task_state.record().status.is_terminal()
            || run.record().controls.cancel_requested
            || (!run.record().status.is_unfinished()
                && run.record().status != RunStatus::Blocked
                && !run.needs_worker_recovery())
            || Self::goal_receipt_for_task(conn, task).await?.is_some())
    }
    async fn save_question(conn: &mut SqliteConnection, q: &WorkerQuestion) -> Result<(), Error> {
        sqlx::query("INSERT INTO harness_worker_questions(question_id,session_id,turn_id,requester_task_id,requester_run_id,recipient_task_id,recipient_run_id,deadline_at,resolved,payload) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(question_id) DO UPDATE SET resolved=excluded.resolved,payload=excluded.payload")
            .bind(q.question_id.to_string()).bind(q.session_id.to_string()).bind(q.turn_id.to_string()).bind(q.requester_task_id.to_string()).bind(q.requester_run_id.to_string()).bind(q.recipient_task_id.to_string()).bind(q.recipient_run_id.to_string()).bind(q.deadline_at).bind(q.outcome.is_some()).bind(serde_json::to_string(q)?).execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
    async fn settle_question(
        conn: &mut SqliteConnection,
        q: &mut WorkerQuestion,
        now: i64,
    ) -> Result<(), Error> {
        if q.outcome.is_none() {
            if Self::question_closed(conn, q.requester_task_id, q.requester_run_id).await? {
                q.settle(QuestionStatus::RequesterClosed, now);
            } else if now >= q.deadline_at {
                q.settle(QuestionStatus::TimedOut, now);
            } else if Self::question_closed(conn, q.recipient_task_id, q.recipient_run_id).await? {
                q.settle(QuestionStatus::RecipientClosed, now);
            }
        }
        Ok(())
    }
    /// Resolve saved deadlines and closed tasks even while an admission hold remains.
    pub(super) async fn refresh_worker_questions(
        conn: &mut SqliteConnection,
        now: i64,
    ) -> Result<bool, Error> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT q.payload FROM harness_worker_questions q JOIN runs s ON s.id=q.requester_run_id JOIN runs r ON r.id=q.recipient_run_id JOIN agent_tasks st ON st.id=q.requester_task_id JOIN agent_tasks rt ON rt.id=q.recipient_task_id WHERE q.resolved=0 AND (q.deadline_at<=? OR json_extract(st.payload,'$.cancel_requested')=1 OR json_extract(rt.payload,'$.cancel_requested')=1 OR json_extract(s.payload,'$.controls.cancel_requested')=1 OR json_extract(r.payload,'$.controls.cancel_requested')=1 OR (s.status IN ('succeeded','failed','cancelled','budget_exhausted') AND COALESCE(json_type(s.payload,'$.worker_recovery'),'')!='object') OR (r.status IN ('succeeded','failed','cancelled','budget_exhausted') AND COALESCE(json_type(r.payload,'$.worker_recovery'),'')!='object') OR EXISTS(SELECT 1 FROM agent_goal_cancelled_requests c WHERE c.id IN (q.requester_task_id,q.recipient_task_id))) ORDER BY q.deadline_at LIMIT 64")
            .bind(now).fetch_all(&mut *conn).await.map_err(storage)?;
        let mut changed = false;
        for row in rows {
            let mut q: WorkerQuestion = serde_json::from_str(&row)?;
            Self::settle_question(conn, &mut q, now).await?;
            if q.outcome.is_some() {
                Self::save_question(conn, &q).await?;
                changed = true;
            }
        }
        Ok(changed)
    }
    pub(super) async fn active_planner_question(
        conn: &mut SqliteConnection,
        run: &mut Run,
    ) -> Result<(), Error> {
        let Some(current) = &run.record().planner_question else {
            return Ok(());
        };
        let row: String = sqlx::query_scalar("SELECT payload FROM harness_worker_questions WHERE question_id=? AND recipient_run_id=?")
            .bind(current.question_id.to_string()).bind(run.record().id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
        let question: WorkerQuestion = serde_json::from_str(&row)?;
        let closed =
            Self::question_closed(conn, question.requester_task_id, question.requester_run_id)
                .await?;
        run.prepare_planner_question_status(&question, closed)?;
        Ok(())
    }
    pub(super) async fn question_for_timer(
        conn: &mut SqliteConnection,
        run: &mut Run,
        timer_id: TimerId,
    ) -> Result<(), Error> {
        if let Some(wait) = &run.record().worker_reply_wait {
            let row: String = sqlx::query_scalar(
                "SELECT payload FROM harness_worker_questions WHERE question_id=?",
            )
            .bind(wait.question_id.to_string())
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
            run.prepare_worker_reply_timer(&serde_json::from_str(&row)?)?;
        }
        let question_id = run
            .timers()
            .iter()
            .find(|t| t.id == timer_id && t.payload["key"] == PLANNER_QUESTION_KEY)
            .and_then(|t| t.payload["question"]["question_id"].as_str())
            .map(str::to_owned);
        if let Some(id) = question_id {
            let row: String = sqlx::query_scalar(
                "SELECT payload FROM harness_worker_questions WHERE question_id=?",
            )
            .bind(id)
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
            run.prepare_planner_question_timer(&serde_json::from_str(&row)?);
        }
        Ok(())
    }
    pub(super) async fn worker_question_history(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> Result<WorkerQuestions, Error> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_worker_questions WHERE session_id=? AND turn_id=? AND (requester_task_id=? OR recipient_task_id=?) ORDER BY rowid LIMIT 65").bind(session.to_string()).bind(turn.to_string()).bind(task.to_string()).bind(task.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let receipts: Vec<String> = sqlx::query_scalar("SELECT payload FROM harness_worker_question_receipts WHERE session_id=? AND turn_id=? AND task_id=? ORDER BY rowid LIMIT 65").bind(session.to_string()).bind(turn.to_string()).bind(task.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        Ok(WorkerQuestions {
            session_id: session,
            turn_id: turn,
            task_id: task,
            truncated: rows.len() > 64 || receipts.len() > 64,
            items: rows
                .iter()
                .take(64)
                .map(|r| serde_json::from_str(r))
                .collect::<Result<_, _>>()?,
            receipts: receipts
                .iter()
                .take(64)
                .map(|r| serde_json::from_str(r))
                .collect::<Result<_, _>>()?,
        })
    }
}
impl SqliteStore {
    pub(super) async fn execute_worker_question(
        &self,
        call: NativeToolCall,
        reply: bool,
    ) -> Result<NativeToolResult, Error> {
        let operation = if reply {
            serde_json::from_value(call.arguments.clone()).map(QuestionOperation::Reply)
        } else {
            serde_json::from_value(call.arguments.clone()).map(QuestionOperation::Ask)
        };
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let saved: Option<String> = sqlx::query_scalar(
            "SELECT payload FROM harness_worker_question_receipts WHERE activity_id=?",
        )
        .bind(call.activity_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?;
        if let Some(saved) = saved {
            let r: QuestionReceipt = serde_json::from_str(&saved)?;
            if r.run_id != call.run_id || operation.as_ref().ok() != Some(&r.operation) {
                return Err(DomainError::ConflictingMessage.into());
            }
            tx.commit().await.map_err(storage)?;
            return Ok(result(&r));
        }
        let operation = match operation {
            Ok(operation) => operation,
            Err(_) => {
                tx.commit().await.map_err(storage)?;
                return Ok(NativeToolResult::Rejected(ToolError::new(
                    "question_input",
                    "Use the exact question tool fields, a valid question UUID for replies, and an integer timeout for questions.",
                )));
            }
        };
        let scope = Self::question_scope(
            &mut tx,
            &call,
            if reply { "reply_worker" } else { "ask_worker" },
        )
        .await?;
        let now = crate::adapters::system_clock::SystemClock.now_ms();
        Self::refresh_worker_questions(&mut tx, now).await?;
        let mut receipt = QuestionReceipt {
            activity_id: call.activity_id,
            run_id: call.run_id,
            session_id: scope.session,
            turn_id: scope.turn,
            task_id: scope.task,
            operation: operation.clone(),
            question_id: None,
            deadline_at: None,
            error: None,
            saved_at: now,
        };
        if Self::question_closed(&mut tx, scope.task, call.run_id).await?
            || scope.source.record().controls.cancel_requested
            || Self::load_session_metadata(&mut tx, scope.session)
                .await?
                .archived
        {
            receipt.error = Some(ToolError::new(
                "question_scope",
                "This assignment is stopped or its session is unavailable.",
            ));
        } else {
            match operation {
                QuestionOperation::Ask(request) => {
                    if let Err(error) = request.validate() {
                        receipt.error = Some(error);
                    }
                    let target = if request.recipient == "parent"
                        && scope.name != "parent"
                        && scope.parent.accepts_planner_questions()
                    {
                        Some(scope.parent_task)
                    } else {
                        scope.group.and_then(|group| {
                            scope
                                .team
                                .members
                                .iter()
                                .find(|m| {
                                    m.name == request.recipient
                                        && matches!(m.name.as_str(), "coder" | "coder2")
                                        && m.name != scope.name
                                })
                                .and_then(|m| {
                                    scope.parent.record().delegations.iter().find(|d| {
                                        d.group_id == Some(group)
                                            && d.recipient == m.agent_id
                                            && d.outcome.is_none()
                                            && d.timed_out_at.is_none()
                                    })
                                })
                                .map(|d| d.id)
                        })
                    };
                    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM harness_worker_question_receipts WHERE session_id=? AND turn_id=? AND task_id=? AND json_extract(payload,'$.operation.kind')='ask'").bind(scope.session.to_string()).bind(scope.turn.to_string()).bind(scope.task.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
                    if receipt.error.is_none() && count >= 8 {
                        receipt.error = Some(ToolError::new(
                            "question_capacity",
                            "This assignment has already made 8 question decisions.",
                        ));
                    }
                    if receipt.error.is_none() {
                        if let Some(target) = target {
                            let task = Self::load_agent_task(&mut tx, target).await?;
                            if let Some(run_id) = task.record().run_id {
                                if Self::question_closed(&mut tx, target, run_id).await? {
                                    receipt.error = Some(ToolError::new(
                                        "question_recipient_closed",
                                        "This sibling has finished or is stopping.",
                                    ));
                                } else {
                                    let rows: Vec<(String, String)> = sqlx::query_as("SELECT requester_run_id,recipient_run_id FROM harness_worker_questions WHERE session_id=? AND turn_id=? AND resolved=0").bind(scope.session.to_string()).bind(scope.turn.to_string()).fetch_all(&mut *tx).await.map_err(storage)?;
                                    let edges: Vec<(RunId, RunId)> = rows
                                        .iter()
                                        .map(|(a, b)| {
                                            Ok((
                                                a.parse().map_err(|_| DomainError::InvalidState)?,
                                                b.parse().map_err(|_| DomainError::InvalidState)?,
                                            ))
                                        })
                                        .collect::<Result<_, DomainError>>()?;
                                    if would_cycle(&edges, call.run_id, run_id)
                                        || Self::dependency_cycle(
                                            &mut tx,
                                            scope.session,
                                            scope.turn,
                                            call.run_id,
                                            run_id,
                                        )
                                        .await?
                                    {
                                        receipt.error = Some(ToolError::new(
                                            "question_cycle",
                                            "This wait would be circular. Reply to the pending sibling question first.",
                                        ));
                                    } else {
                                        let deadline_at = TimerDeadline::After {
                                            milliseconds: request.timeout_ms as u64,
                                        }
                                        .resolve(now)?;
                                        let mut recipient = Self::load(&mut tx, run_id).await?;
                                        match recipient.queue_peer_message(
                                            PeerMessage {
                                                question_id: Some(call.activity_id),
                                                deadline_at: Some(deadline_at),
                                                message_id: call.activity_id,
                                                sender_task_id: scope.task,
                                                sender: scope.name.clone(),
                                                text: request.text.clone(),
                                                needs_coordination: false,
                                                sequence: 0,
                                                saved_at: now,
                                                application: None,
                                            },
                                            now,
                                        ) {
                                            Ok(_) => {
                                                let question = WorkerQuestion {
                                                    question_id: call.activity_id,
                                                    session_id: scope.session,
                                                    turn_id: scope.turn,
                                                    requester_task_id: scope.task,
                                                    requester_run_id: call.run_id,
                                                    requester: scope.name.clone(),
                                                    recipient_task_id: target,
                                                    recipient_run_id: run_id,
                                                    recipient: request.recipient,
                                                    text: request.text,
                                                    deadline_at,
                                                    saved_at: now,
                                                    outcome: None,
                                                };
                                                if question.recipient == "parent"
                                                    && let Err(error) = recipient
                                                        .queue_planner_question(&question, now)
                                                {
                                                    receipt.error = Some(ToolError::new(
                                                        "question_recipient_unavailable",
                                                        error.to_string(),
                                                    ));
                                                }
                                                if receipt.error.is_none() {
                                                    self.save(&mut tx, &recipient, false).await?;
                                                    Self::save_question(&mut tx, &question).await?;
                                                    receipt.question_id = Some(call.activity_id);
                                                    receipt.deadline_at = Some(deadline_at);
                                                }
                                            }
                                            Err(error) => {
                                                receipt.error = Some(ToolError::new(
                                                    "question_recipient_unavailable",
                                                    error.to_string(),
                                                ))
                                            }
                                        }
                                    }
                                }
                            } else {
                                receipt.error = Some(ToolError::new(
                                    "question_recipient_not_started",
                                    "This sibling has not started; continue useful work before making another decision.",
                                ));
                            }
                        } else {
                            receipt.error = Some(ToolError::new(
                                "question_recipient",
                                "Choose your enabled parent planner or another coder in this same parallel group, never self.",
                            ));
                        }
                    }
                }
                QuestionOperation::Reply(request) => {
                    if let Err(error) = request.validate() {
                        receipt.error = Some(error);
                    }
                    if scope.name == "parent"
                        && scope
                            .source
                            .record()
                            .planner_question
                            .as_ref()
                            .is_none_or(|q| q.question_id != request.question_id)
                    {
                        receipt.error = Some(ToolError::new(
                            "question_reply_scope",
                            "The planner can answer only its current saved child question.",
                        ));
                    }
                    let row: Option<String> = sqlx::query_scalar("SELECT payload FROM harness_worker_questions WHERE question_id=? AND session_id=? AND turn_id=? AND recipient_task_id=? AND recipient_run_id=?").bind(request.question_id.to_string()).bind(scope.session.to_string()).bind(scope.turn.to_string()).bind(scope.task.to_string()).bind(call.run_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)?;
                    if receipt.error.is_none() {
                        if let Some(row) = row {
                            let mut q: WorkerQuestion = serde_json::from_str(&row)?;
                            Self::settle_question(&mut tx, &mut q, now).await?;
                            if q.outcome.is_some() {
                                receipt.error = Some(ToolError::new(
                                    "question_closed",
                                    "This question already has an outcome or its original deadline has passed.",
                                ));
                            } else {
                                q.outcome = Some(QuestionOutcome {
                                    status: QuestionStatus::Answered,
                                    text: Some(request.text),
                                    reply_id: Some(call.activity_id),
                                    resolved_at: now,
                                });
                                receipt.question_id = Some(q.question_id);
                                receipt.deadline_at = Some(q.deadline_at);
                            }
                            Self::save_question(&mut tx, &q).await?;
                        } else {
                            receipt.error = Some(ToolError::new(
                                "question_reply_scope",
                                "Only the pinned recipient task can answer this question.",
                            ));
                        }
                    }
                }
            }
        }
        sqlx::query("INSERT INTO harness_worker_question_receipts(activity_id,session_id,turn_id,task_id,payload) VALUES(?,?,?,?,?)").bind(call.activity_id.to_string()).bind(scope.session.to_string()).bind(scope.turn.to_string()).bind(scope.task.to_string()).bind(serde_json::to_string(&receipt)?).execute(&mut *tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(result(&receipt))
    }
}
