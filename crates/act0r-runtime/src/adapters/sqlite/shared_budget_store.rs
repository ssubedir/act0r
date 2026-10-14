use super::*;
use crate::domain::shared_budget::*;

impl SqliteStore {
    pub(super) async fn initialize_shared_budget(
        conn: &mut SqliteConnection,
        run: &RunRecord,
    ) -> Result<(), Error> {
        let delegation = run
            .delegation_budget
            .as_ref()
            .and_then(|d| d.shared_budget.clone());
        // A continuation inherits its goal scopes from the saved source task.
        let goal = run
            .goal_budget
            .clone()
            .filter(|_| run.follow_up_context.as_ref().is_none_or(|c| c.depth == 0));
        for (kind, budget) in [
            (SharedBudgetKind::Delegation, delegation),
            (SharedBudgetKind::Goal, goal),
        ] {
            let Some(budget) = budget else { continue };
            let mut scope = SharedBudget::with_kind(run.id, kind, budget, run.created_at)?;
            scope.refresh(std::slice::from_ref(run), run.id, run.created_at)?;
            sqlx::query("INSERT INTO agent_budget_scopes(root_run_id,kind,payload) VALUES(?,?,?)")
                .bind(run.id.to_string())
                .bind(kind.as_str())
                .bind(serde_json::to_string(scope.record())?)
                .execute(&mut *conn)
                .await
                .map_err(storage)?;
            sqlx::query("INSERT INTO agent_budget_members(root_run_id,kind,run_id) VALUES(?,?,?)")
                .bind(run.id.to_string())
                .bind(kind.as_str())
                .bind(run.id.to_string())
                .execute(&mut *conn)
                .await
                .map_err(storage)?;
        }
        if run.follow_up_context.as_ref().is_some_and(|c| c.depth > 0) {
            Self::inherit_follow_up_budgets(conn, run).await?;
        }
        Ok(())
    }
    /// Ownership was committed before publication. Membership commits with Run creation.
    pub(super) async fn inherit_shared_budgets(
        conn: &mut SqliteConnection,
        link: &actors::AgentRun,
    ) -> Result<(), Error> {
        sqlx::query("INSERT INTO agent_budget_members(root_run_id,kind,run_id) SELECT m.root_run_id,m.kind,? FROM agent_delegations d JOIN agent_budget_members m ON m.run_id=d.parent_run_id WHERE d.child_task_id=? ON CONFLICT DO NOTHING")
            .bind(link.run_id.to_string()).bind(link.request_id.to_string())
            .execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
    async fn inherit_follow_up_budgets(
        conn: &mut SqliteConnection,
        run: &RunRecord,
    ) -> Result<(), Error> {
        let context = run
            .follow_up_context
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        let task = Self::load_agent_task(conn, context.task_id).await?;
        let origin = task
            .record
            .follow_up_origin
            .as_ref()
            .ok_or(DomainError::InvalidState)?;
        let source = Self::load(conn, origin.run_id).await?;
        let expected = origin.context(context.actor_id, context.task_id)?;
        // Membership authority is the persisted intent, never a model/user payload.
        if task.record.agent_id != context.actor_id
            || source
                .record()
                .follow_up_context
                .as_ref()
                .is_none_or(|c| c.actor_id != context.actor_id || c.task_id != origin.task_id)
            || source.record().follow_up.as_ref().is_none_or(|f| {
                f.reminder_id != context.task_id
                    || &f.origin != origin
                    || f.status != follow_up::FollowUpStatus::Registered
            })
            || context != &expected
            || run.goal_budget != source.record().goal_budget
        {
            return Err(DomainError::InvalidState.into());
        }
        sqlx::query("INSERT INTO agent_budget_members(root_run_id,kind,run_id) SELECT root_run_id,kind,? FROM agent_budget_members WHERE run_id=? AND kind='goal' ON CONFLICT DO NOTHING")
                .bind(run.id.to_string()).bind(origin.run_id.to_string())
                .execute(&mut *conn).await.map_err(storage)?;
        Ok(())
    }
    pub(super) async fn load_shared_scopes(
        conn: &mut SqliteConnection,
        id: RunId,
    ) -> Result<Vec<SharedBudget>, Error> {
        let payloads: Vec<String> = sqlx::query_scalar("SELECT s.payload FROM agent_budget_scopes s JOIN agent_budget_members m ON m.root_run_id=s.root_run_id AND m.kind=s.kind WHERE m.run_id=? ORDER BY json_extract(s.payload,'$.created_at'),s.root_run_id,s.kind")
            .bind(id.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        payloads
            .iter()
            .map(|p| {
                Ok(SharedBudget {
                    record: serde_json::from_str(p)?,
                })
            })
            .collect()
    }
    pub(super) async fn refresh_shared_scope(
        conn: &mut SqliteConnection,
        mut scope: SharedBudget,
        changed: RunId,
        now: i64,
    ) -> Result<(), Error> {
        let payloads: Vec<String> = sqlx::query_scalar("SELECT r.payload FROM agent_budget_members m JOIN runs r ON r.id=m.run_id WHERE m.root_run_id=? AND m.kind=? ORDER BY r.id")
            .bind(scope.record().root_run_id.to_string()).bind(scope.record().kind.as_str()).fetch_all(&mut *conn).await.map_err(storage)?;
        let records: Vec<RunRecord> = payloads
            .iter()
            .map(|p| serde_json::from_str(p))
            .collect::<Result<_, _>>()?;
        scope.refresh(&records, changed, now)?;
        sqlx::query("UPDATE agent_budget_scopes SET payload=? WHERE root_run_id=? AND kind=?")
            .bind(serde_json::to_string(scope.record())?)
            .bind(scope.record().root_run_id.to_string())
            .bind(scope.record().kind.as_str())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Ok(())
    }
    pub(super) async fn shared_budget_views(
        conn: &mut SqliteConnection,
        id: RunId,
    ) -> Result<Vec<SharedBudgetView>, Error> {
        let mut views = vec![];
        for scope in Self::load_shared_scopes(conn, id).await? {
            let rows: Vec<(String,Option<String>,Option<String>)> = sqlx::query_as("SELECT r.payload,a.agent_id,a.request_id FROM agent_budget_members m JOIN runs r ON r.id=m.run_id LEFT JOIN agent_runs a ON a.run_id=r.id WHERE m.root_run_id=? AND m.kind=? ORDER BY r.created_at,r.id")
                .bind(scope.record().root_run_id.to_string()).bind(scope.record().kind.as_str()).fetch_all(&mut *conn).await.map_err(storage)?;
            let mut members = vec![];
            for (payload, agent, task) in rows {
                let run: RunRecord = serde_json::from_str(&payload)?;
                members.push(SharedBudgetMember {
                    run_id: run.id,
                    status: run.status,
                    usage: run.usage,
                    agent_id: agent
                        .map(|id| {
                            id.parse().map_err(|_| {
                                Error::Storage("invalid shared budget agent identity".into())
                            })
                        })
                        .transpose()?,
                    task_id: task
                        .map(|id| {
                            id.parse().map_err(|_| {
                                Error::Storage("invalid shared budget task identity".into())
                            })
                        })
                        .transpose()?,
                });
            }
            let budget = scope
                .record
                .usage
                .budget
                .as_ref()
                .ok_or(DomainError::InvalidState)?;
            views.push(SharedBudgetView {
                remaining_model_calls: budget.remaining_model_calls(&scope.record.usage),
                remaining_tokens: budget.remaining_tokens(&scope.record.usage),
                record: scope.record,
                members,
            });
        }
        Ok(views)
    }
}
