use super::*;
use crate::domain::{search::*, task::*};

impl SqliteStore {
    pub(super) async fn load_task_runs(
        conn: &mut SqliteConnection,
        task: &CodingTask,
    ) -> Result<Vec<Run>, Error> {
        let mut runs = vec![];
        for id in task
            .record()
            .candidates
            .iter()
            .map(|c| c.run_id)
            .chain(task.record().auxiliary_runs.iter().copied())
        {
            runs.push(Self::load(conn, id).await?);
        }
        Ok(runs)
    }
    pub(super) async fn load_search(
        conn: &mut SqliteConnection,
        id: SearchId,
    ) -> Result<Search, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM search_jobs WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        Ok(Search {
            record: serde_json::from_str(&payload.ok_or(Error::SearchNotFound(id))?)?,
        })
    }
    async fn save_search(conn: &mut SqliteConnection, search: &Search) -> Result<(), Error> {
        sqlx::query("UPDATE search_jobs SET phase=?,payload=? WHERE id=?")
            .bind(
                serde_json::to_value(search.record().phase)?
                    .as_str()
                    .unwrap(),
            )
            .bind(serde_json::to_string(search.record())?)
            .bind(search.record().id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        let task = Self::load_task(conn, search.record().task_id).await?;
        Self::sync_benchmark(conn, task.record()).await
    }
    pub(super) async fn insert_search(
        conn: &mut SqliteConnection,
        search: &Search,
        task: &CodingTask,
    ) -> Result<(), Error> {
        if search.record().task_id != task.record().id
            || task.record().candidates.len() != 1
            || search.record().root != task.record().candidates[0].run_id
            || task.record().search_id != Some(search.record().id)
            || search.record().phase != SearchPhase::Warmup
        {
            return Err(DomainError::InvalidState.into());
        }
        sqlx::query(
            "INSERT INTO search_jobs(id,task_id,phase,created_at,payload) VALUES(?,?,?,?,?)",
        )
        .bind(search.record().id.to_string())
        .bind(search.record().task_id.to_string())
        .bind("warmup")
        .bind(search.record().created_at)
        .bind(serde_json::to_string(search.record())?)
        .execute(conn)
        .await
        .map_err(storage)?;
        Ok(())
    }
}
impl SearchStore for SqliteStore {
    fn create_search(&self, search: Search, mut task: CodingTask, root: Run) -> PortFuture<'_, ()> {
        Box::pin(async move {
            if search.record().task_id != task.record().id
                || search.record().root != root.record().id
                || task.record().search_id != Some(search.record().id)
                || task.record().candidates.len() != 1
            {
                return Err(DomainError::InvalidState.into());
            }
            task.refresh(std::slice::from_ref(&root), search.record().created_at)?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            self.create_task_on(&mut tx, &task, &[root]).await?;
            Self::insert_search(&mut tx, &search, &task).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn search(&self, id: SearchId) -> PortFuture<'_, Search> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let search = Self::load_search(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(search)
        })
    }
    fn active_searches(&self) -> PortFuture<'_, Vec<SearchId>> {
        Box::pin(async move {
            let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM search_jobs WHERE phase NOT IN ('completed','failed') ORDER BY created_at,id").fetch_all(&self.pool).await.map_err(storage)?;
            ids.iter()
                .map(|id| {
                    id.parse()
                        .map_err(|e: uuid::Error| Error::Storage(e.to_string()))
                })
                .collect()
        })
    }
    fn checkpoint_requested(&self, id: SearchId, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut search = Self::load_search(&mut tx, id).await?;
            let root = Self::load(&mut tx, search.record.root).await?;
            if search.checkpoint_requested(&root, now)? {
                Self::save_search(&mut tx, &search).await?;
            }
            tx.commit().await.map_err(storage)
        })
    }
    fn begin_proposals(&self, id: SearchId, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut search = Self::load_search(&mut tx, id).await?;
            if search.record.phase != SearchPhase::Checkpoint {
                tx.commit().await.map_err(storage)?;
                return Ok(());
            }
            let checkpoint = Self::load_checkpoint(&mut tx, search.record.checkpoint_id).await?;
            if checkpoint.record().status != branch::PreparationStatus::Ready {
                return Err(DomainError::InvalidState.into());
            }
            let mut task = Self::load_task(&mut tx, search.record.task_id).await?;
            let input = ProposalInput {
                search_id: id,
                task_id: task.record().id,
                checkpoint_id: checkpoint.record().id,
                request: ProposalRequest::from_checkpoint(&checkpoint, &search.record.config)?,
                budget: UsageBudget {
                    max_model_calls: task.record().config.agent.max_model_calls,
                    max_tokens: task.record().config.agent.max_tokens,
                },
            };
            let run = Run::start_with_id(
                search.record.proposal_run_id,
                &ProposalWorkflow,
                serde_json::to_value(input)?,
                task.record().config.budget.max_attempts,
                now,
            )?;
            self.save(&mut tx, &run, true).await?;
            sqlx::query("INSERT INTO task_auxiliary_runs(run_id,task_id) VALUES(?,?)")
                .bind(run.record().id.to_string())
                .bind(task.record().id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            task.record.auxiliary_runs.push(run.record().id);
            let runs = Self::load_task_runs(&mut tx, &task).await?;
            task.refresh(&runs, now)?;
            Self::save_task(&mut tx, &task).await?;
            search.proposals_started(now)?;
            Self::save_search(&mut tx, &search).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn begin_rollouts(&self, id: SearchId, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut search = Self::load_search(&mut tx, id).await?;
            if search.record.phase != SearchPhase::Proposing {
                tx.commit().await.map_err(storage)?;
                return Ok(());
            }
            let proposal = Self::load(&mut tx, search.record.proposal_run_id).await?;
            let actions = search.proposals(&proposal)?;
            let checkpoint = Self::load_checkpoint(&mut tx, search.record.checkpoint_id).await?;
            let mut task = Self::load_task(&mut tx, search.record.task_id).await?;
            let mut plan = task.plan_fork(
                search.record.fork_id,
                &checkpoint,
                actions.len() as u32,
                now,
            )?;
            let marker = SearchFork {
                search_id: id,
                proposal_run_id: proposal.record().id,
                actions: actions.clone(),
                version: search.record.config.version,
            };
            plan.search = Some(marker.clone());
            task.record
                .forks
                .iter_mut()
                .find(|f| f.id == plan.id)
                .unwrap()
                .search = Some(marker);
            sqlx::query("INSERT INTO candidate_forks(id,task_id,checkpoint_id,status,created_at) VALUES(?,?,?,?,?)").bind(plan.id.to_string()).bind(task.record().id.to_string()).bind(plan.checkpoint_id.to_string()).bind("preparing").bind(now).execute(&mut *tx).await.map_err(storage)?;
            search.rollouts_planned(&plan, actions, now)?;
            Self::save_task(&mut tx, &task).await?;
            Self::save_search(&mut tx, &search).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn begin_feedback(&self, id: SearchId, run: RunId, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let search = Self::load_search(&mut tx, id).await?;
            let mut candidate = Self::load(&mut tx, run).await?;
            if search.needs_feedback(&candidate)? {
                candidate.begin_search_feedback(now)?;
                self.save(&mut tx, &candidate, false).await?;
                let mut task = Self::load_task(&mut tx, search.record.task_id).await?;
                task.refresh(&Self::load_task_runs(&mut tx, &task).await?, now)?;
                Self::save_task(&mut tx, &task).await?;
            }
            tx.commit().await.map_err(storage)
        })
    }
    fn score_rollout(&self, id: SearchId, run: RunId, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut search = Self::load_search(&mut tx, id).await?;
            if search.record.phase != SearchPhase::RollingOut {
                tx.commit().await.map_err(storage)?;
                return Ok(());
            }
            let candidate = Self::load(&mut tx, run).await?;
            let checkpoint = Self::load_checkpoint(&mut tx, search.record.checkpoint_id).await?;
            if search.score(&candidate, &checkpoint.record().seed.origin, now)? {
                Self::save_search(&mut tx, &search).await?;
            }
            tx.commit().await.map_err(storage)
        })
    }
    fn select_search(&self, id: SearchId, selected: Option<RunId>, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut search = Self::load_search(&mut tx, id).await?;
            if !search.choose(selected, now)? {
                tx.commit().await.map_err(storage)?;
                return Ok(());
            }
            let mut task = Self::load_task(&mut tx, search.record.task_id).await?;
            let mut runs = Self::load_task_runs(&mut tx, &task).await?;
            for run in &mut runs {
                if Some(run.record().id) == selected {
                    run.select_search_for_verification(now)?;
                    self.save(&mut tx, run, false).await?;
                } else if run.record().status.is_unfinished() {
                    run.cancel(now)?;
                    self.save(&mut tx, run, false).await?;
                }
            }
            task.refresh(&runs, now)?;
            Self::save_task(&mut tx, &task).await?;
            Self::save_search(&mut tx, &search).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn finish_search(&self, id: SearchId, error: Option<String>, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut search = Self::load_search(&mut tx, id).await?;
            if matches!(
                search.record.phase,
                SearchPhase::Completed | SearchPhase::Failed
            ) {
                tx.commit().await.map_err(storage)?;
                return Ok(());
            }
            let mut task = Self::load_task(&mut tx, search.record.task_id).await?;
            let mut runs = Self::load_task_runs(&mut tx, &task).await?;
            if let Some(error) = error {
                for run in &mut runs {
                    if run.record().status.is_unfinished() {
                        run.cancel(now)?;
                        self.save(&mut tx, run, false).await?;
                    }
                }
                task.refresh(&runs, now)?;
                Self::save_task(&mut tx, &task).await?;
                search.fail(error, now);
            } else {
                let selected = runs
                    .iter()
                    .find(|r| Some(r.record().id) == search.record.selected)
                    .ok_or(DomainError::InvalidState)?;
                search.verification_observed(selected, now)?;
            }
            Self::save_search(&mut tx, &search).await?;
            tx.commit().await.map_err(storage)
        })
    }
}
