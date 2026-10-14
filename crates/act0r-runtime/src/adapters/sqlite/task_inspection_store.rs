use super::*;
use crate::{
    application::*,
    domain::{actors::*, shared_budget::*},
};
use std::collections::{HashSet, VecDeque};

#[derive(Clone)]
struct Candidate {
    id: MessageId,
    actor_id: Option<AgentId>,
    intent: Option<TaskInspectionIntent>,
}
fn id<T: std::str::FromStr>(s: &str) -> Result<T, Error> {
    s.parse()
        .map_err(|_| Error::Storage("invalid task inspection identity".into()))
}
impl SqliteStore {
    async fn inspection_task(
        conn: &mut SqliteConnection,
        task: MessageId,
    ) -> Result<Option<AgentTaskRecord>, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_tasks WHERE id=?")
                .bind(task.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        payload.map(|p| Ok(serde_json::from_str(&p)?)).transpose()
    }
    async fn inspection_reminder(
        conn: &mut SqliteConnection,
        task: MessageId,
    ) -> Result<Option<ActorReminder>, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_reminders WHERE id=?")
                .bind(task.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        payload.map(|p| Ok(serde_json::from_str(&p)?)).transpose()
    }
    async fn inspection_intent(
        conn: &mut SqliteConnection,
        task: MessageId,
    ) -> Result<Option<TaskInspectionIntent>, Error> {
        let payload:Option<String>=sqlx::query_scalar("SELECT r.payload FROM agent_delegations d JOIN runs r ON r.id=d.parent_run_id WHERE d.id=?").bind(task.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        if let Some(payload) = payload {
            let run: RunRecord = serde_json::from_str(&payload)?;
            return Ok(Some(TaskInspectionIntent::Delegation(
                run.delegations
                    .into_iter()
                    .find(|d| d.id == task)
                    .ok_or(DomainError::InvalidState)?,
            )));
        }
        let payload: Option<String> = sqlx::query_scalar(
            "SELECT payload FROM runs WHERE json_extract(payload,'$.follow_up.reminder_id')=?",
        )
        .bind(task.to_string())
        .fetch_optional(&mut *conn)
        .await
        .map_err(storage)?;
        payload
            .map(|p| {
                let run: RunRecord = serde_json::from_str(&p)?;
                Ok(TaskInspectionIntent::FollowUp(
                    run.follow_up.ok_or(DomainError::InvalidState)?,
                ))
            })
            .transpose()
    }
    async fn inspection_parent(
        conn: &mut SqliteConnection,
        task: MessageId,
    ) -> Result<Option<MessageId>, Error> {
        let saved = Self::inspection_task(conn, task).await?;
        if let Some(t) = &saved {
            if let Some(parent) = t.parent_id {
                return Ok(Some(parent));
            }
            if let Some(origin) = &t.follow_up_origin {
                return Ok(Some(origin.task_id));
            }
        }
        let reminder = Self::inspection_reminder(conn, task).await?;
        if let Some(r) = &reminder
            && let Some(origin) = &r.origin
        {
            return Ok(Some(origin.task_id));
        }
        let parent:Option<String>=sqlx::query_scalar("SELECT a.request_id FROM agent_delegations d JOIN agent_runs a ON a.run_id=d.parent_run_id WHERE d.id=?").bind(task.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        if let Some(parent) = parent {
            return Ok(Some(id(&parent)?));
        }
        if let Some(TaskInspectionIntent::FollowUp(intent)) =
            Self::inspection_intent(conn, task).await?
        {
            return Ok(Some(intent.origin.task_id));
        }
        if saved.is_none() && reminder.is_none() {
            return Err(Error::AgentTaskNotFound(task));
        }
        Ok(None)
    }
    async fn inspection_budgets(
        conn: &mut SqliteConnection,
        run: RunId,
    ) -> Result<Vec<TaskInspectionBudget>, Error> {
        // Exact persisted scope totals may include members beyond a truncated visible graph.
        let rows:Vec<(String,i64)>=sqlx::query_as("SELECT s.payload,(SELECT COUNT(*) FROM agent_budget_members all_m WHERE all_m.root_run_id=s.root_run_id AND all_m.kind=s.kind) FROM agent_budget_scopes s JOIN agent_budget_members m ON m.root_run_id=s.root_run_id AND m.kind=s.kind WHERE m.run_id=? ORDER BY s.root_run_id,s.kind")
            .bind(run.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        rows.into_iter()
            .map(|(payload, count)| {
                let record: SharedBudgetRecord = serde_json::from_str(&payload)?;
                let budget = record
                    .usage
                    .budget
                    .as_ref()
                    .ok_or(DomainError::InvalidState)?;
                Ok(TaskInspectionBudget {
                    remaining_model_calls: budget.remaining_model_calls(&record.usage),
                    remaining_tokens: budget.remaining_tokens(&record.usage),
                    record,
                    member_count: u64::try_from(count).map_err(|_| DomainError::InvalidState)?,
                })
            })
            .collect()
    }
    async fn inspection_facts(
        conn: &mut SqliteConnection,
        candidate: Candidate,
        depth: u32,
    ) -> Result<TaskInspectionFacts, Error> {
        let task = Self::inspection_task(conn, candidate.id).await?;
        let reminder = Self::inspection_reminder(conn, candidate.id).await?;
        let intent = if candidate.intent.is_some() {
            candidate.intent
        } else {
            Self::inspection_intent(conn, candidate.id).await?
        };
        let actor_id = task
            .as_ref()
            .map(|t| t.agent_id)
            .or_else(|| reminder.as_ref().map(|r| r.message.recipient))
            .or(candidate.actor_id)
            .ok_or(Error::AgentTaskNotFound(candidate.id))?;
        let actor = match Self::load_actor(conn, actor_id).await {
            Ok(a) => Some(a),
            Err(Error::AgentNotFound(_)) => None,
            Err(e) => return Err(e),
        };
        let mailbox:Option<(String,Option<String>)>=sqlx::query_as("SELECT status,CASE WHEN json_type(payload,'$.message.payload.goal')='text' THEN json_extract(payload,'$.message.payload.goal') END FROM agent_mailbox WHERE id=?").bind(candidate.id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        let mailbox_status = mailbox
            .as_ref()
            .map(|(s, _)| match s.as_str() {
                "queued" => Ok(MessageStatus::Queued),
                "processed" => Ok(MessageStatus::Processed),
                "rejected" => Ok(MessageStatus::Rejected),
                _ => Err(DomainError::InvalidState),
            })
            .transpose()?;
        let mut goal = mailbox.and_then(|(_, goal)| goal).or_else(|| {
            reminder
                .as_ref()
                .and_then(|r| r.message.payload["goal"].as_str())
                .map(str::to_owned)
        });
        if goal.is_none() {
            goal = match &intent {
                Some(TaskInspectionIntent::Delegation(d)) => Some(d.goal.clone()),
                Some(TaskInspectionIntent::FollowUp(f)) => Some(f.goal.clone()),
                None => None,
            };
        }
        let mut run = None;
        let mut budgets = vec![];
        if let Some(run_id) = task.as_ref().and_then(|t| t.run_id) {
            let payload: String = sqlx::query_scalar("SELECT payload FROM runs WHERE id=?")
                .bind(run_id.to_string())
                .fetch_one(&mut *conn)
                .await
                .map_err(storage)?;
            let record: RunRecord = serde_json::from_str(&payload)?;
            goal = record.input["input"]["goal"]
                .as_str()
                .or_else(|| record.input["goal"].as_str())
                .map(str::to_owned)
                .or(goal);
            let activity:Option<String>=sqlx::query_scalar("SELECT json_object('id',id,'handler',json_extract(payload,'$.spec.handler'),'status',status,'ready_at',ready_at,'error',json_extract(payload,'$.error')) FROM activities WHERE run_id=? AND status IN ('running','queued') ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END,ready_at,scheduling_revision,command_ordinal LIMIT 1").bind(run_id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
            let timer:Option<(String,i64)>=sqlx::query_as("SELECT id,due_at FROM timers WHERE run_id=? AND status='scheduled' ORDER BY due_at,id LIMIT 1").bind(run_id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
            budgets = Self::inspection_budgets(conn, run_id).await?;
            run = Some(TaskInspectionRun {
                record,
                activity: activity
                    .map(|p| Ok::<_, Error>(serde_json::from_str(&p)?))
                    .transpose()?,
                timer: timer
                    .map(|(t, d)| Ok::<_, Error>((id(&t)?, d)))
                    .transpose()?,
            });
        }
        if goal.is_none() {
            goal = actor
                .as_ref()
                .and_then(|a| a.record().config["input"]["goal"].as_str())
                .map(str::to_owned);
        }
        let forwarded: Option<String> =
            sqlx::query_scalar("SELECT id FROM agent_tasks WHERE parent_id=?")
                .bind(candidate.id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        Ok(TaskInspectionFacts {
            execution_held: Self::task_execution_held(conn, candidate.id).await?,
            id: candidate.id,
            actor_id,
            actor: actor.map(|a| TaskInspectionActor {
                status: a.record().status,
                error: a.record().error.clone(),
                supervision_pause: a
                    .record()
                    .supervision
                    .as_ref()
                    .and_then(|s| s.pause.clone()),
            }),
            depth,
            goal,
            task,
            run,
            reminder,
            intent,
            mailbox_status,
            forwarded_task: forwarded.map(|s| id(&s)).transpose()?,
            budgets,
        })
    }
    async fn inspection_children(
        conn: &mut SqliteConnection,
        facts: &TaskInspectionFacts,
    ) -> Result<Vec<(Candidate, TaskRelation, Option<TaskGroupId>)>, Error> {
        let mut children = vec![];
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT id,agent_id FROM agent_tasks WHERE parent_id=? ORDER BY id")
                .bind(facts.id.to_string())
                .fetch_all(&mut *conn)
                .await
                .map_err(storage)?;
        for (task, actor) in rows {
            children.push((
                Candidate {
                    id: id(&task)?,
                    actor_id: Some(id(&actor)?),
                    intent: None,
                },
                TaskRelation::Forwarded,
                None,
            ))
        }
        if let Some(run) = &facts.run {
            for d in &run.record.delegations {
                children.push((
                    Candidate {
                        id: d.id,
                        actor_id: Some(d.recipient),
                        intent: Some(TaskInspectionIntent::Delegation(d.clone())),
                    },
                    TaskRelation::Delegated,
                    d.group_id,
                ))
            }
            if let Some(f) = &run.record.follow_up {
                children.push((
                    Candidate {
                        id: f.reminder_id,
                        actor_id: Some(facts.actor_id),
                        intent: Some(TaskInspectionIntent::FollowUp(f.clone())),
                    },
                    TaskRelation::FollowUp,
                    None,
                ))
            }
        }
        Ok(children)
    }
}
impl SqliteStore {
    pub(super) async fn inspection_snapshot(
        conn: &mut SqliteConnection,
        requested: MessageId,
        options: TaskInspectionOptions,
    ) -> Result<TaskInspectionSnapshot, Error> {
        options.validate()?;
        // Resolve real ownership, never correlation IDs or untrusted task payloads.
        let mut root = requested;
        let mut ancestors = HashSet::new();
        loop {
            if !ancestors.insert(root) || ancestors.len() > 512 {
                return Err(Error::Storage(
                    "task inspection ancestry is cyclic or exceeds 512 links".into(),
                ));
            }
            match Self::inspection_parent(&mut *conn, root).await? {
                Some(parent) => root = parent,
                None => break,
            }
        }
        Self::inspection_subtree(conn, root, options).await
    }
    /// Start at an already authorized turn, without expanding its ancestors.
    pub(super) async fn inspection_subtree(
        conn: &mut SqliteConnection,
        root: MessageId,
        options: TaskInspectionOptions,
    ) -> Result<TaskInspectionSnapshot, Error> {
        options.validate()?;
        let mut snapshot = TaskInspectionSnapshot {
            root_task_id: root,
            goal_cancellation: Self::goal_receipt_for_task(&mut *conn, root).await?,
            goal_pause: Self::goal_pause_for_root(&mut *conn, root).await?,
            nodes: vec![],
            edges: vec![],
            truncations: vec![],
        };
        let mut queue = VecDeque::from([(
            Candidate {
                id: root,
                actor_id: None,
                intent: None,
            },
            0,
        )]);
        let mut seen = HashSet::from([root]);
        while let Some((candidate, depth)) = queue.pop_front() {
            let facts = Self::inspection_facts(&mut *conn, candidate, depth).await?;
            let children = Self::inspection_children(&mut *conn, &facts).await?;
            let mut omitted = 0;
            for (child, relation, group_id) in children {
                if depth >= options.max_depth || seen.len() >= options.max_nodes as usize {
                    omitted += 1;
                    continue;
                }
                if !seen.insert(child.id) {
                    return Err(Error::Storage(
                        "task inspection encountered repeated task ownership".into(),
                    ));
                }
                snapshot.edges.push(TaskInspectionEdge {
                    parent_task_id: facts.id,
                    child_task_id: child.id,
                    relation,
                    group_id,
                });
                queue.push_back((child, depth + 1));
            }
            if omitted > 0 {
                snapshot.truncations.push(TaskInspectionTruncation {
                    parent_task_id: facts.id,
                    reason: if depth >= options.max_depth {
                        "max_depth"
                    } else {
                        "max_nodes"
                    },
                    omitted_children: omitted,
                })
            }
            snapshot.nodes.push(facts);
        }
        Ok(snapshot)
    }
}
impl TaskInspectionReadPort for SqliteStore {
    fn task_inspection_snapshot(
        &self,
        requested: MessageId,
        options: TaskInspectionOptions,
    ) -> PortFuture<'_, TaskInspectionSnapshot> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let snapshot = Self::inspection_snapshot(&mut tx, requested, options).await?;
            tx.commit().await.map_err(storage)?;
            Ok(snapshot)
        })
    }
}
