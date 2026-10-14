use super::*;
use crate::domain::{actors::*, workspace_handoff::*};

impl WorkspaceHandoffReadPort for SqliteStore {
    fn handoff_worker(
        &self,
        parent: RunId,
        request: PrepareRequest,
    ) -> PortFuture<'_, RegisteredAgentConfig> {
        Box::pin(async move {
            self.load_handoff_workers(parent, vec![request], false)
                .await?
                .into_iter()
                .next()
                .ok_or(DomainError::InvalidState.into())
        })
    }
    fn handoff_group_workers(
        &self,
        parent: RunId,
        request: PrepareGroupRequest,
    ) -> PortFuture<'_, Vec<RegisteredAgentConfig>> {
        Box::pin(async move {
            request.validate()?;
            self.load_handoff_workers(
                parent,
                request
                    .tasks
                    .iter()
                    .map(|t| request.member_request(t))
                    .collect(),
                true,
            )
            .await
        })
    }
    fn verify_workspace_input(
        &self,
        child: RunId,
        input: PreparedWorkspace,
    ) -> PortFuture<'_, bool> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let payload: Option<String> =
                sqlx::query_scalar("SELECT payload FROM agent_runs WHERE run_id=?")
                    .bind(child.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            let link: AgentRun = serde_json::from_str(&payload.ok_or_else(|| {
                Error::Unavailable("workspace input requires an actor-owned child Run".into())
            })?)?;
            let task = Self::load_agent_task(&mut tx, link.request_id).await?;
            let actor = Self::load_actor(&mut tx, link.agent_id).await?;
            let config: RegisteredAgentConfig =
                serde_json::from_value(actor.record().config.clone())?;
            input.check_worker(link.agent_id, &config)?;
            if link.request_id != input.task_id
                || task.record().run_id != Some(child)
                || task.record().workspace_input.as_ref() != Some(&input)
            {
                return Err(Error::Unavailable(
                    "workspace input differs from the accepted task record".into(),
                ));
            }
            let run = Self::load(&mut tx, child).await?;
            let invocation: integration::AgentInvocation =
                serde_json::from_value(run.record().input.clone())?;
            let effective: answer::AnswerConfig = serde_json::from_value(invocation.input)?;
            let pinned: answer::AnswerConfig = serde_json::from_value(config.input)?;
            if effective.workspace.as_ref() != Some(&input.workspace)
                || effective.workspace_input.as_ref() != Some(&input)
                || &effective.writable_paths
                    != input
                        .request
                        .writable_paths
                        .as_ref()
                        .unwrap_or(&pinned.writable_paths)
            {
                return Err(Error::Unavailable(
                    "child Run permissions differ from its accepted workspace input".into(),
                ));
            }
            let committed = run.activities().iter().any(|a| {
                a.spec.handler == handler("verify")
                    && a.spec.input == serde_json::json!(input)
                    && a.status == ActivityStatus::Succeeded
                    && a.result.as_ref() == Some(&serde_json::json!({"verified":true}))
            });
            tx.commit().await.map_err(storage)?;
            Ok(committed)
        })
    }
}

impl SqliteStore {
    async fn load_handoff_workers(
        &self,
        parent: RunId,
        requests: Vec<PrepareRequest>,
        group: bool,
    ) -> Result<Vec<RegisteredAgentConfig>, Error> {
        for request in &requests {
            request.validate()?;
        }
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let run = Self::load(&mut tx, parent).await?;
        let invocation: integration::AgentInvocation =
            serde_json::from_value(run.record().input.clone())?;
        let config: planner::PlannerConfig = serde_json::from_value(invocation.input)?;
        let owner: Option<String> =
            sqlx::query_scalar("SELECT payload FROM agent_runs WHERE run_id=?")
                .bind(parent.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        if owner.is_none()
            || invocation.registration.definition != planner::PlannerWorkflow::definition_id()
            || !config.workspace_handoff
            || (group
                && requests.len() > config.delegation.max_parallel_tasks.unwrap_or(1) as usize)
            || requests.iter().any(|request| {
                config.workspace.as_ref() != Some(&request.workspace)
                    || !config
                        .workers
                        .iter()
                        .any(|w| w.agent_id == request.recipient)
            })
        {
            return Err(Error::Unavailable(
                "workspace handoff differs from the parent's pinned workspace or workers".into(),
            ));
        }
        let mut workers = Vec::with_capacity(requests.len());
        for request in requests {
            config
                .validate_scope(request.writable_paths.as_deref())
                .map_err(|e| Error::Unavailable(e.to_string()))?;
            let actor = Self::load_actor(&mut tx, request.recipient)
                .await
                .map_err(|error| match error {
                    Error::AgentNotFound(_) => {
                        Error::Unavailable("workspace handoff worker does not exist".into())
                    }
                    error => error,
                })?;
            if actor.record().definition != RegisteredAgentDefinition.id() {
                return Err(Error::Unavailable(
                    "workspace handoff requires a registered native worker".into(),
                ));
            }
            let worker: RegisteredAgentConfig =
                serde_json::from_value(actor.record().config.clone())?;
            check_worker(&worker).map_err(|e| Error::Unavailable(e.to_string()))?;
            if let Some(paths) = &request.writable_paths {
                let input: answer::AnswerConfig = serde_json::from_value(worker.input.clone())?;
                tools::validate_write_scope(paths, &input.writable_paths)
                    .map_err(|e| Error::Unavailable(e.to_string()))?;
            }
            workers.push(worker);
        }
        tx.commit().await.map_err(storage)?;
        Ok(workers)
    }
}
