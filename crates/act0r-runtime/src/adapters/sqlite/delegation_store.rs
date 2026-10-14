use super::*;
use crate::domain::actors::*;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

type Pending = (String, String, Option<String>);
type Child = (MessageId, AgentTaskView, Option<i64>);
impl SqliteStore {
    async fn pending_native_delegations(
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Pending>, Error> {
        sqlx::query_as("SELECT d.id,d.parent_run_id,d.child_task_id FROM agent_delegations d JOIN runs r ON r.id=d.parent_run_id WHERE d.completed=0 AND (r.status<>'blocked' OR d.deadline_at IS NOT NULL) ORDER BY d.created_at,d.rowid")
            .fetch_all(&mut *conn).await.map_err(storage)
    }
    async fn native_child(
        conn: &mut SqliteConnection,
        id: Option<String>,
    ) -> Result<Option<Child>, Error> {
        let Some(id) = id else {
            return Ok(None);
        };
        let id: MessageId = parse(&id)?;
        let view = Self::agent_task_view(conn, id).await?;
        let completed_at = if view.status.is_terminal() {
            Some(match view.run_id {
                Some(run) => Self::load(conn, run).await?.record().updated_at,
                None => view.task.updated_at,
            })
        } else {
            None
        };
        Ok(Some((id, view, completed_at)))
    }
    pub(super) async fn native_delegation_ready(
        conn: &mut SqliteConnection,
    ) -> Result<bool, Error> {
        if Self::initial_dependency_ready(conn).await? {
            return Ok(true);
        }
        for (id, parent, child) in Self::pending_native_delegations(conn).await? {
            let parent = Self::load(conn, parse(&parent)?).await?;
            let intent = parent
                .pending_delegation_by_id(parse(&id)?)
                .ok_or(DomainError::InvalidState)?;
            let child = Self::native_child(conn, child).await?;
            if !delegation_cancelling(&parent, intent.id)
                && Self::goal_run_paused(conn, parent.record().id).await?
            {
                continue;
            }
            // Future deadlines keep the polling worker alive, just like durable timers.
            if parent.delegation_clock_enabled()
                && intent.deadline_pending(child.as_ref().and_then(|(_, _, at)| *at))
            {
                return Ok(true);
            }
            let blocked = parent.record().status == RunStatus::Blocked;
            let cancelling = delegation_cancelling(&parent, intent.id);
            if let Some((_, child, _)) = child {
                if (child.status.is_terminal() && !blocked)
                    || (cancelling
                        && !child.task.cancel_requested
                        && (child.status != AgentTaskStatus::Blocked || child.run_id.is_none())
                        && !Self::goal_task_reconciliation_pending(conn, &child).await?)
                {
                    return Ok(true);
                }
            } else if !blocked && (cancelling || !parent.record().controls.pause_requested) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(super) fn advance_native_delegation(
        &self,
        now: i64,
        decision: DelegationDecision,
    ) -> PortFuture<'_, DelegationProgress> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            if let Some(progress) = self.advance_initial_dependency(&mut tx, now).await? {
                tx.commit().await.map_err(storage)?;
                return Ok(progress);
            }
            for (id, parent, child) in Self::pending_native_delegations(&mut tx).await? {
                let id: MessageId = parse(&id)?;
                let mut parent = Self::load(&mut tx, parse(&parent)?).await?;
                let before = parent.clone();
                if !delegation_cancelling(&parent, id)
                    && Self::goal_run_paused(&mut tx, parent.record().id).await?
                {
                    continue;
                }
                let child = Self::native_child(&mut tx, child).await?;
                let completed_at = child.as_ref().and_then(|(_, _, at)| *at);
                if parent.delegation_clock_enabled()
                    && parent
                        .pending_delegation_by_id(id)
                        .ok_or(DomainError::InvalidState)?
                        .deadline_due(now, completed_at)
                {
                    decision(
                        &mut parent,
                        id,
                        DelegationEvent::DeadlineElapsed {
                            child_completed_at: completed_at,
                        },
                    )?;
                    self.save_change(&mut tx, &before, parent, None).await?;
                    tx.commit().await.map_err(storage)?;
                    return Ok(DelegationProgress::Changed);
                }
                let blocked = parent.record().status == RunStatus::Blocked;
                let cancelling = delegation_cancelling(&parent, id);
                if let Some((child, view, _)) = child {
                    if view.status.is_terminal() {
                        if blocked {
                            continue;
                        }
                        let outcome = json!({"task_id":child,"run_id":view.run_id,"status":view.status,"result":view.reply.as_ref().and_then(|r|r.get("result")),"error":view.reply.as_ref().and_then(|r|r.get("error")),"usage":view.usage});
                        decision(&mut parent, id, DelegationEvent::ChildCompleted(outcome))?;
                        Self::settle_worker_dependencies(&mut tx, &mut parent, id, now).await?;
                    } else if cancelling
                        && !view.task.cancel_requested
                        && (view.status != AgentTaskStatus::Blocked || view.run_id.is_none())
                        && !Self::goal_task_reconciliation_pending(&mut tx, &view).await?
                    {
                        tx.commit().await.map_err(storage)?;
                        return Ok(DelegationProgress::CancelChild(child));
                    } else {
                        continue;
                    }
                } else {
                    if let Some(receipt) =
                        Self::goal_receipt_for_run(&mut tx, parent.record().id).await?
                    {
                        if goal::GoalCancellationReceipt::protects_reconciliation(&parent) {
                            continue;
                        }
                        receipt.cancel_run(&mut parent, now)?;
                    }
                    if blocked || (!cancelling && parent.record().controls.pause_requested) {
                        continue;
                    }
                    if cancelling || parent.record().controls.cancel_requested {
                        decision(
                            &mut parent,
                            id,
                            DelegationEvent::ChildCompleted(rejected(
                                id,
                                "cancelled",
                                "delegation cancelled before child admission",
                            )),
                        )?;
                    } else {
                        decision(&mut parent, id, DelegationEvent::BeforePublication)?;
                        if parent.record().status != RunStatus::Blocked
                            && let Some(outcome) =
                                Self::publish_native_delegation(&mut tx, &parent, id, now).await?
                        {
                            decision(&mut parent, id, DelegationEvent::ChildCompleted(outcome))?;
                        }
                    }
                }
                self.save_change(&mut tx, &before, parent, None).await?;
                tx.commit().await.map_err(storage)?;
                return Ok(DelegationProgress::Changed);
            }
            tx.commit().await.map_err(storage)?;
            Ok(DelegationProgress::Idle)
        })
    }
    async fn publish_native_delegation(
        conn: &mut SqliteConnection,
        run: &Run,
        id: MessageId,
        now: i64,
    ) -> Result<Option<Value>, Error> {
        let delegation = run
            .pending_delegation_by_id(id)
            .ok_or(DomainError::InvalidState)?;
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_runs WHERE run_id=?")
                .bind(run.record().id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        let Some(payload) = payload else {
            return Ok(Some(rejected(
                id,
                "actor_run_required",
                "delegation requires an actor-owned Run",
            )));
        };
        let owner: AgentRun = serde_json::from_str(&payload)?;
        let recipient = match Self::load_actor(conn, delegation.recipient).await {
            Ok(actor) => actor,
            Err(Error::AgentNotFound(_)) => {
                return Ok(Some(rejected(
                    id,
                    "recipient_unavailable",
                    "delegated actor does not exist",
                )));
            }
            Err(error) => return Err(error),
        };
        if recipient.record().definition != RegisteredAgentDefinition.id() {
            return Ok(Some(rejected(
                id,
                "unsupported_worker",
                "delegation requires a registered native agent actor",
            )));
        }
        if let Some(input) = &delegation.workspace_input {
            // The trusted input and physical task must originate in this publication transaction.
            // Mailbox deduplication must never attach it to an earlier public task.
            let occupied: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_tasks WHERE id=?)")
                    .bind(id.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            if occupied {
                return Ok(Some(rejected(
                    id,
                    "workspace_handoff_rejected",
                    "prepared task identity already belongs to an accepted request",
                )));
            }
            let config: RegisteredAgentConfig =
                serde_json::from_value(recipient.record().config.clone())?;
            let parent: planner::PlannerConfig =
                serde_json::from_value(run.record().input["input"].clone())?;
            if input.parent_run_id != run.record().id
                || input.task_id != id
                || input.check_worker(delegation.recipient, &config).is_err()
                || parent
                    .validate_scope(input.request.writable_paths.as_deref())
                    .is_err()
            {
                return Ok(Some(rejected(
                    id,
                    "workspace_handoff_rejected",
                    "prepared input differs from the worker's pinned configuration",
                )));
            }
        }
        let mut root = run.record().id;
        let mut depth = 1u32;
        let mut current = owner.clone();
        let mut seen = HashSet::from([current.agent_id]);
        loop {
            let prior: Option<String> = sqlx::query_scalar(
                "SELECT parent_run_id FROM agent_delegations WHERE child_task_id=?",
            )
            .bind(current.request_id.to_string())
            .fetch_optional(&mut *conn)
            .await
            .map_err(storage)?;
            let Some(prior) = prior else {
                break;
            };
            let prior_run = Self::load(conn, parse(&prior)?).await?;
            depth = depth.checked_add(1).ok_or(DomainError::InvalidState)?;
            if prior_run
                .record()
                .delegation_budget
                .as_ref()
                .is_none_or(|b| depth > b.max_depth)
            {
                return Ok(Some(rejected(
                    id,
                    "delegation_depth",
                    "inherited delegation depth exhausted",
                )));
            }
            root = prior_run.record().id;
            let payload: String =
                sqlx::query_scalar("SELECT payload FROM agent_runs WHERE run_id=?")
                    .bind(root.to_string())
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(storage)?;
            current = serde_json::from_str(&payload)?;
            if !seen.insert(current.agent_id) {
                return Ok(Some(rejected(
                    id,
                    "delegation_cycle",
                    "delegation ancestry contains a cycle",
                )));
            }
        }
        let root_run = Self::load(conn, root).await?;
        let budget = root_run
            .record()
            .delegation_budget
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        sqlx::query("UPDATE agent_delegations SET root_run_id=?,depth=? WHERE id=?")
            .bind(root.to_string())
            .bind(i64::from(depth))
            .bind(id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_delegations WHERE root_run_id=?")
                .bind(root.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
        if count > i64::from(budget.max_tasks) {
            return Ok(Some(rejected(
                id,
                "delegation_budget",
                "shared delegation task allowance exhausted",
            )));
        }
        if seen.contains(&delegation.recipient)
            || Self::native_wait_cycle(conn, owner.agent_id, delegation.recipient).await?
        {
            return Ok(Some(rejected(
                id,
                "delegation_cycle",
                "delegation would wait on its own actor ancestry or form a wait cycle",
            )));
        }
        let message = AgentMessage {
            id,
            recipient: delegation.recipient,
            sender: None,
            correlation_id: Some(id),
            kind: "task".into(),
            payload: json!({"goal":delegation.goal}),
        };
        match Self::insert_message(conn, message.clone(), now).await {
            Ok(_) => {}
            Err(error @ Error::QueueFull { .. }) => {
                return Ok(Some(rejected(id, "queue_full", &error.to_string())));
            }
            Err(error) => return Err(error),
        }
        Self::insert_actor_task(conn, &message, None, now).await?;
        if let Some(input) = &delegation.workspace_input {
            let mut task = Self::load_agent_task(conn, id).await?;
            task.record.workspace_input = Some(input.clone());
            Self::save_agent_task(conn, &task).await?;
        }
        sqlx::query("UPDATE agent_delegations SET child_task_id=? WHERE id=?")
            .bind(id.to_string())
            .bind(id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Ok(None)
    }
    async fn native_wait_cycle(
        conn: &mut SqliteConnection,
        owner: AgentId,
        target: AgentId,
    ) -> Result<bool, Error> {
        let rows: Vec<(String,String)> = sqlx::query_as("SELECT a.agent_id,d.parent_run_id FROM agent_delegations d JOIN agent_runs a ON a.run_id=d.parent_run_id JOIN runs r ON r.id=a.run_id WHERE d.completed=0 AND r.status IN ('waiting','paused')")
            .fetch_all(&mut *conn).await.map_err(storage)?;
        let mut edges: HashMap<AgentId, HashSet<AgentId>> = HashMap::new();
        for (agent, run) in rows {
            let run = Self::load(conn, parse(&run)?).await?;
            for delegation in run
                .record()
                .delegations
                .iter()
                .filter(|d| d.outcome.is_none())
            {
                edges
                    .entry(parse::<AgentId>(&agent)?)
                    .or_default()
                    .insert(delegation.recipient);
            }
        }
        let mut pending = vec![target];
        let mut seen = HashSet::new();
        while let Some(next) = pending.pop() {
            if next == owner {
                return Ok(true);
            }
            if seen.insert(next)
                && let Some(targets) = edges.get(&next)
            {
                pending.extend(targets);
            }
        }
        Ok(false)
    }
}
fn delegation_cancelling(parent: &Run, id: MessageId) -> bool {
    parent.record().controls.cancel_requested
        || (parent.record().status != RunStatus::Blocked && !parent.record().status.is_unfinished())
        || parent
            .pending_delegation_by_id(id)
            .is_some_and(|d| d.timed_out_at.is_some())
}
fn parse<T: std::str::FromStr>(value: &str) -> Result<T, Error> {
    value
        .parse()
        .map_err(|_| Error::Storage("invalid delegation identity".into()))
}
fn rejected(id: MessageId, code: &str, message: &str) -> Value {
    json!({"task_id":id,"run_id":null,"status":if code == "cancelled" {"cancelled"} else {"rejected"},"result":null,"error":{"code":code,"message":message},"usage":null})
}
