use super::*;
use crate::domain::{planner_guidance::GuidanceTarget, planner_pause::*, tools::ToolError};

impl SqliteStore {
    pub(super) async fn save_dependency(
        conn: &mut SqliteConnection,
        d: &WorkerDependency,
    ) -> Result<(), Error> {
        d.validate()?;
        sqlx::query("INSERT INTO harness_worker_dependencies(activity_id,planner_run_id,session_id,turn_id,task_id,prerequisite_task_id,closed,payload) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(activity_id) DO UPDATE SET closed=excluded.closed,payload=excluded.payload")
            .bind(d.activity_id.to_string()).bind(d.planner_run_id.to_string()).bind(d.session_id.to_string())
            .bind(d.turn_id.to_string()).bind(d.task_id.to_string()).bind(d.prerequisite_task_id.to_string())
            .bind(d.closed).bind(serde_json::to_string(d)?).execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
    pub(super) async fn close_worker_dependencies(
        conn: &mut SqliteConnection,
        task: MessageId,
    ) -> Result<(), Error> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM harness_worker_dependencies WHERE task_id=? AND closed=0",
        )
        .bind(task.to_string())
        .fetch_all(&mut *conn)
        .await
        .map_err(storage)?;
        for row in rows {
            let mut d: WorkerDependency = serde_json::from_str(&row)?;
            d.closed = true;
            Self::save_dependency(conn, &d).await?;
        }
        Ok(())
    }
    /// Include sibling question edges so either action order rejects a mixed wait cycle.
    pub(super) async fn dependency_cycle(
        conn: &mut SqliteConnection,
        session: SessionId,
        turn: MessageId,
        from: RunId,
        to: RunId,
    ) -> Result<bool, Error> {
        let mut edges: Vec<(String,String)> = sqlx::query_as("SELECT requester_run_id,recipient_run_id FROM harness_worker_questions WHERE session_id=? AND turn_id=? AND resolved=0")
            .bind(session.to_string()).bind(turn.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let rows: Vec<(String,String)> = sqlx::query_as("SELECT json_extract(d.payload,'$.run_id'),json_extract(d.payload,'$.prerequisite_run_id') FROM harness_worker_dependencies d JOIN harness_planner_holds h ON h.task_id=d.task_id WHERE d.session_id=? AND d.turn_id=? AND d.closed=0 AND json_extract(d.payload,'$.outcome') IS NULL AND h.paused=1 AND json_extract(h.payload,'$.revision')=json_extract(d.payload,'$.hold_revision')")
            .bind(session.to_string()).bind(turn.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        edges.extend(rows);
        // Pending initial edges already hold the receiver, even before baseline preparation.
        let initial: Vec<(String,String)> = sqlx::query_as("SELECT waiting.run_id,source.run_id FROM harness_initial_dependencies d JOIN agent_runs waiting ON waiting.request_id=d.task_id JOIN agent_runs source ON source.request_id=d.prerequisite_task_id JOIN agent_runs parent ON parent.run_id=d.parent_run_id JOIN harness_turns t ON t.task_id=parent.request_id WHERE d.status='pending' AND t.session_id=? AND t.task_id=?")
            .bind(session.to_string()).bind(turn.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        edges.extend(initial);
        let edges: Vec<(RunId, RunId)> = edges
            .into_iter()
            .map(|(a, b)| {
                Ok((
                    a.parse().map_err(|_| DomainError::InvalidState)?,
                    b.parse().map_err(|_| DomainError::InvalidState)?,
                ))
            })
            .collect::<Result<_, DomainError>>()?;
        Ok(worker_questions::would_cycle(&edges, from, to))
    }
    pub(super) async fn dependency_for_pause(
        conn: &mut SqliteConnection,
        source: &Run,
        receipt: &PlannerPauseReceipt,
        target: &GuidanceTarget,
        now: i64,
    ) -> Result<Result<Option<WorkerDependency>, ToolError>, Error> {
        let Some(name) = &receipt.request.dependency else {
            return Ok(Ok(None));
        };
        let Some(prerequisite) = source
            .record()
            .planner_guidance
            .as_ref()
            .and_then(|g| g.targets.iter().find(|t| &t.name == name))
        else {
            return Ok(Err(ToolError::new(
                "dependency_scope",
                "Choose the other coding assignment captured in this coordination step.",
            )));
        };
        if prerequisite.task_id == target.task_id {
            return Ok(Err(ToolError::new(
                "dependency_cycle",
                "An assignment cannot depend on itself.",
            )));
        }
        let waiter = source
            .record()
            .delegations
            .iter()
            .find(|d| d.id == target.task_id)
            .ok_or(DomainError::InvalidState)?;
        let required = source
            .record()
            .delegations
            .iter()
            .find(|d| d.id == prerequisite.task_id)
            .ok_or(DomainError::InvalidState)?;
        if waiter.group_id.is_none() || waiter.group_id != required.group_id {
            return Ok(Err(ToolError::new(
                "dependency_scope",
                "Dependencies require two captured assignments in the same parallel join.",
            )));
        }
        let (task, _, run) = Self::harness_worker_assignment(
            conn,
            receipt.session_id,
            receipt.turn_id,
            prerequisite.task_id,
        )
        .await?;
        if task.record().run_id != Some(prerequisite.run_id)
            || run
                .as_ref()
                .is_none_or(|r| r.record().id != prerequisite.run_id)
        {
            return Err(DomainError::InvalidState.into());
        }
        if Self::dependency_cycle(
            conn,
            receipt.session_id,
            receipt.turn_id,
            target.run_id,
            prerequisite.run_id,
        )
        .await?
        {
            return Ok(Err(ToolError::new(
                "dependency_cycle",
                "This dependency would form a circular assignment or sibling-question wait.",
            )));
        }
        let outcome = required
            .outcome
            .as_ref()
            .map(|o| serde_json::from_value(o["status"].clone()))
            .transpose()?;
        Ok(Ok(Some(WorkerDependency {
            activity_id: receipt.activity_id,
            session_id: receipt.session_id,
            turn_id: receipt.turn_id,
            planner_run_id: receipt.run_id,
            worker: target.name.clone(),
            task_id: target.task_id,
            run_id: target.run_id,
            prerequisite: prerequisite.name.clone(),
            prerequisite_task_id: prerequisite.task_id,
            prerequisite_run_id: prerequisite.run_id,
            hold_revision: 0,
            saved_at: now,
            settled_at: outcome.map(|_| now),
            outcome,
            review_timer_id: None,
            closed: false,
        })))
    }
    /// Called in the same writer transaction that saves the parent's final child receipt.
    pub(super) async fn settle_worker_dependencies(
        conn: &mut SqliteConnection,
        parent: &mut Run,
        child: MessageId,
        now: i64,
    ) -> Result<(), Error> {
        Self::close_worker_dependencies(conn, child).await?;
        let rows: Vec<String>=sqlx::query_scalar("SELECT payload FROM harness_worker_dependencies WHERE planner_run_id=? AND prerequisite_task_id=? AND closed=0 AND json_extract(payload,'$.outcome') IS NULL")
            .bind(parent.record().id.to_string()).bind(child.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        for row in rows {
            let mut d: WorkerDependency = serde_json::from_str(&row)?;
            let active = Self::planner_hold_for_task(conn, d.task_id)
                .await?
                .is_some_and(|h| {
                    h.paused
                        && h.revision == d.hold_revision
                        && h.planner_run_id == d.planner_run_id
                        && h.run_id == d.run_id
                });
            if !active
                || !parent.dependencies_allowed()
                || parent.pending_delegation_by_id(d.task_id).is_none()
            {
                d.closed = true;
            } else {
                let outcome = parent
                    .record()
                    .delegations
                    .iter()
                    .find(|p| p.id == child)
                    .and_then(|p| p.outcome.as_ref())
                    .ok_or(DomainError::InvalidState)?;
                d.settle(serde_json::from_value(outcome["status"].clone())?, now)?;
                parent.queue_dependency_wake(d.activity_id, now)?;
            }
            Self::save_dependency(conn, &d).await?;
        }
        Ok(())
    }
    pub(super) async fn ready_dependencies(
        conn: &mut SqliteConnection,
        run: &Run,
        id: TimerId,
        targets: &[GuidanceTarget],
    ) -> Result<Vec<WorkerDependency>, Error> {
        if !run.dependencies_allowed() {
            return Ok(vec![]);
        }
        let rows:Vec<String>=sqlx::query_scalar("SELECT payload FROM harness_worker_dependencies WHERE planner_run_id=? AND closed=0 AND json_extract(payload,'$.outcome') IS NOT NULL AND json_extract(payload,'$.review_timer_id') IS NULL ORDER BY rowid")
            .bind(run.record().id.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let mut ready = vec![];
        for row in rows {
            let mut d: WorkerDependency = serde_json::from_str(&row)?;
            if targets
                .iter()
                .any(|t| t.task_id == d.task_id && t.run_id == d.run_id)
                && Self::planner_hold_for_task(conn, d.task_id)
                    .await?
                    .is_some_and(|h| {
                        h.paused
                            && h.revision == d.hold_revision
                            && h.planner_run_id == d.planner_run_id
                    })
            {
                d.capture_review(id)?;
                ready.push(d);
            }
        }
        if ready.len() > 2 {
            return Err(DomainError::InvalidState.into());
        }
        Ok(ready)
    }
    pub(super) async fn mark_dependency_reviews(
        conn: &mut SqliteConnection,
        run: &Run,
        id: TimerId,
    ) -> Result<(), Error> {
        if let Some(t) = run
            .timers()
            .iter()
            .find(|t| t.id == id && t.status == TimerStatus::Fired)
            && let Some(items) = t.payload["guidance"]["dependencies"].as_array()
        {
            for item in items {
                let d: WorkerDependency = serde_json::from_value(item.clone())?;
                Self::save_dependency(conn, &d).await?;
            }
        }
        Ok(())
    }
}
