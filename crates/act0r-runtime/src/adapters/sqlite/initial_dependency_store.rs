//! Initial hold projection and conversion, using the existing WorkCoordinator transaction.
use super::*;
use crate::domain::{actors::AgentTaskStatus, planner_pause::*};

enum InitialChange {
    Close(MessageId),
    Cancel(MessageId),
    Bind {
        parent: Box<Run>,
        hold: Box<PlannerHoldRecord>,
        dependency: Box<WorkerDependency>,
    },
}

impl SqliteStore {
    pub(super) async fn save_initial_dependencies(
        conn: &mut SqliteConnection,
        parent: &Run,
    ) -> Result<(), Error> {
        for waiting in &parent.record().delegations {
            let Some(initial) = &waiting.initial_dependency else {
                continue;
            };
            let config: planner::PlannerConfig = serde_json::from_value(
                parent
                    .record()
                    .input
                    .get("input")
                    .unwrap_or(&parent.record().input)
                    .clone(),
            )?;
            let prerequisite = parent
                .record()
                .delegations
                .iter()
                .find(|d| d.id == initial.prerequisite_task_id)
                .ok_or(DomainError::InvalidState)?;
            if !config.initial_dependencies
                || waiting.id == prerequisite.id
                || waiting.group_id.is_none()
                || waiting.group_id != prerequisite.group_id
                || waiting
                    .workspace_input
                    .as_ref()
                    .is_none_or(|p| p.activity_id != initial.activity_id)
                || prerequisite
                    .workspace_input
                    .as_ref()
                    .is_none_or(|p| p.activity_id != initial.activity_id)
                || !config.workers.iter().any(|w| {
                    w.agent_id == waiting.recipient && matches!(w.name.as_str(), "coder" | "coder2")
                })
                || !config.workers.iter().any(|w| {
                    w.agent_id == prerequisite.recipient
                        && matches!(w.name.as_str(), "coder" | "coder2")
                })
            {
                return Err(DomainError::InvalidState.into());
            }
            sqlx::query("INSERT INTO harness_initial_dependencies(task_id,parent_run_id,prerequisite_task_id,activity_id,status,payload) VALUES(?,?,?,?,'pending',?) ON CONFLICT(task_id) DO NOTHING")
                .bind(waiting.id.to_string()).bind(parent.record().id.to_string()).bind(prerequisite.id.to_string()).bind(initial.activity_id.to_string())
                .bind(serde_json::to_string(initial)?).execute(&mut *conn).await.map_err(storage)?;
        }
        Ok(())
    }

    pub(super) async fn initial_activity_held(
        conn: &mut SqliteConnection,
        activity: ActivityId,
    ) -> Result<bool, Error> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM harness_initial_blocked_activities WHERE id=?)",
        )
        .bind(activity.to_string())
        .fetch_one(&mut *conn)
        .await
        .map_err(storage)
    }

    pub(super) async fn initial_prerequisite(
        conn: &mut SqliteConnection,
        task: MessageId,
    ) -> Result<Option<MessageId>, Error> {
        let source: Option<String> = sqlx::query_scalar("SELECT prerequisite_task_id FROM harness_initial_dependencies WHERE task_id=? AND status='pending'")
            .bind(task.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        source
            .map(|s| s.parse().map_err(|_| DomainError::InvalidState.into()))
            .transpose()
    }

    pub(super) async fn initial_dependency_ready(
        conn: &mut SqliteConnection,
    ) -> Result<bool, Error> {
        Ok(Self::next_initial_change(conn, None).await?.is_some())
    }

    pub(super) async fn advance_initial_dependency(
        &self,
        conn: &mut SqliteConnection,
        now: i64,
    ) -> Result<Option<DelegationProgress>, Error> {
        let Some(change) = Self::next_initial_change(conn, Some(now)).await? else {
            return Ok(None);
        };
        let (task, status) = match change {
            InitialChange::Cancel(task) => return Ok(Some(DelegationProgress::CancelChild(task))),
            InitialChange::Close(task) => (task, "closed"),
            InitialChange::Bind {
                mut parent,
                hold,
                dependency,
            } => {
                sqlx::query("INSERT INTO harness_planner_holds(task_id,paused,payload) VALUES(?,1,?) ON CONFLICT(task_id) DO UPDATE SET paused=excluded.paused,payload=excluded.payload")
                    .bind(hold.task_id.to_string()).bind(serde_json::to_string(&hold)?).execute(&mut *conn).await.map_err(storage)?;
                Self::save_dependency(conn, &dependency).await?;
                parent.initial_dependency_bound(&dependency, now)?;
                if dependency.outcome.is_some() {
                    parent.queue_dependency_wake(dependency.activity_id, now)?;
                }
                self.save(conn, &parent, false).await?;
                (hold.task_id, "bound")
            }
        };
        sqlx::query(
            "UPDATE harness_initial_dependencies SET status=? WHERE task_id=? AND status='pending'",
        )
        .bind(status)
        .bind(task.to_string())
        .execute(&mut *conn)
        .await
        .map_err(storage)?;
        Ok(Some(DelegationProgress::Changed))
    }

    async fn next_initial_change(
        conn: &mut SqliteConnection,
        now: Option<i64>,
    ) -> Result<Option<InitialChange>, Error> {
        let rows: Vec<(String,String,String,String)> = sqlx::query_as("SELECT task_id,parent_run_id,prerequisite_task_id,activity_id FROM harness_initial_dependencies WHERE status='pending' ORDER BY rowid")
            .fetch_all(&mut *conn).await.map_err(storage)?;
        for (task, parent, source, activity) in rows {
            let task: MessageId = task.parse().map_err(|_| DomainError::InvalidState)?;
            let source: MessageId = source.parse().map_err(|_| DomainError::InvalidState)?;
            let activity: ActivityId = activity.parse().map_err(|_| DomainError::InvalidState)?;
            let parent =
                Self::load(conn, parent.parse().map_err(|_| DomainError::InvalidState)?).await?;
            let waiting = parent
                .record()
                .delegations
                .iter()
                .find(|d| d.id == task)
                .ok_or(DomainError::InvalidState)?;
            let prerequisite = parent
                .record()
                .delegations
                .iter()
                .find(|d| d.id == source)
                .ok_or(DomainError::InvalidState)?;
            if waiting.outcome.is_some() {
                return Ok(Some(InitialChange::Close(task)));
            }
            // Retain the admission guard until ordinary cancellation reaches the receiver.
            if waiting.timed_out_at.is_some()
                || parent.record().controls.cancel_requested
                || !parent.record().status.is_unfinished()
                || Self::goal_receipt_for_run(conn, parent.record().id)
                    .await?
                    .is_some()
            {
                continue;
            }
            let published: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_tasks WHERE id=?)")
                    .bind(task.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            if !published {
                continue;
            }
            let receiver = Self::agent_task_view(conn, task).await?;
            if receiver.status.is_terminal() || receiver.task.cancel_requested {
                return Ok(Some(InitialChange::Close(task)));
            }
            // A rejected prerequisite never allocated a Run, so it cannot supply an exact review.
            // Stop the still-unstarted receiver through normal task cancellation and the original join.
            if prerequisite
                .outcome
                .as_ref()
                .is_some_and(|o| o["run_id"].is_null())
            {
                return Ok(Some(InitialChange::Cancel(task)));
            }
            let Some(receiver_run) = receiver.run_id else {
                continue;
            };
            let receiver_state = Self::load(conn, receiver_run).await?;
            let state = receiver_state
                .record()
                .state
                .get("state")
                .unwrap_or(&receiver_state.record().state);
            if !change_review::ready_for_worker_changes(state) {
                continue;
            }
            let published: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_tasks WHERE id=?)")
                    .bind(source.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            if !published {
                continue;
            }
            let prerequisite_view = Self::agent_task_view(conn, source).await?;
            let Some(source_run) = prerequisite_view.run_id else {
                continue;
            };
            let config: planner::PlannerConfig = serde_json::from_value(
                parent
                    .record()
                    .input
                    .get("input")
                    .unwrap_or(&parent.record().input)
                    .clone(),
            )?;
            let worker = config
                .workers
                .iter()
                .find(|w| w.agent_id == waiting.recipient)
                .ok_or(DomainError::InvalidState)?
                .name
                .clone();
            let source_name = config
                .workers
                .iter()
                .find(|w| w.agent_id == prerequisite.recipient)
                .ok_or(DomainError::InvalidState)?
                .name
                .clone();
            let root: String =
                sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
                    .bind(parent.record().id.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            let session: String =
                sqlx::query_scalar("SELECT session_id FROM harness_turns WHERE task_id=?")
                    .bind(&root)
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            let turn_id = root.parse().map_err(|_| DomainError::InvalidState)?;
            let session_id = session.parse().map_err(|_| DomainError::InvalidState)?;
            let saved_at = now.unwrap_or(parent.record().updated_at);
            let mut hold = match Self::planner_hold_for_task(conn, task).await? {
                Some(h) if h.planner_run_id != parent.record().id || h.run_id != receiver_run => {
                    return Err(DomainError::InvalidState.into());
                }
                Some(h) if !h.paused => return Ok(Some(InitialChange::Close(task))),
                Some(h) => h,
                None => {
                    let mut h = PlannerHoldRecord {
                        session_id,
                        turn_id,
                        task_id: task,
                        run_id: receiver_run,
                        planner_run_id: parent.record().id,
                        activity_id: activity,
                        paused: false,
                        revision: 0,
                        reason: String::new(),
                        saved_at,
                    };
                    h.transition(
                        true,
                        activity,
                        format!("Initial assignment depends on {source_name}."),
                        saved_at,
                    )?;
                    h
                }
            };
            hold.saved_at = saved_at;
            let outcome: Option<AgentTaskStatus> = prerequisite
                .outcome
                .as_ref()
                .map(|o| serde_json::from_value(o["status"].clone()))
                .transpose()?;
            let dependency = WorkerDependency {
                activity_id: activity,
                session_id,
                turn_id,
                planner_run_id: parent.record().id,
                worker,
                task_id: task,
                run_id: receiver_run,
                prerequisite: source_name,
                prerequisite_task_id: source,
                prerequisite_run_id: source_run,
                hold_revision: hold.revision,
                saved_at,
                settled_at: outcome.map(|_| saved_at),
                outcome,
                review_timer_id: None,
                closed: false,
            };
            dependency.validate()?;
            return Ok(Some(InitialChange::Bind {
                parent: Box::new(parent),
                hold: Box::new(hold),
                dependency: Box::new(dependency),
            }));
        }
        Ok(None)
    }
}
