use crate::{
    application::{Error, ports::*},
    domain::*,
};
use sqlx::{
    Row, SqliteConnection, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

mod schema;

pub struct SqliteStore {
    pool: SqlitePool,
    path: PathBuf,
    #[cfg(test)]
    probe: Option<TestProbe>,
}
#[path = "sqlite/actor_reminder_store.rs"]
mod actor_reminder_store;
#[path = "sqlite/actor_store.rs"]
mod actor_store;
#[path = "sqlite/agent_task_store.rs"]
mod agent_task_store;
mod assignment_inspection_store;
mod automatic_summary_store;
mod conversation_store;
mod delegated_work_store;
#[path = "sqlite/delegation_store.rs"]
mod delegation_store;
#[path = "sqlite/follow_up_store.rs"]
mod follow_up_store;
#[path = "sqlite/goal_control_store.rs"]
mod goal_control_store;
mod harness_store;
mod initial_dependency_store;
mod permission_store;
mod planner_coordination_store;
mod planner_guidance_store;
mod planner_pause_store;
mod planner_stop_store;
mod project_profile_store;
mod project_refresh_store;
mod project_verification_store;
#[path = "sqlite/result_recall_store.rs"]
mod result_recall_store;
#[path = "sqlite/search_store.rs"]
mod search_store;
mod session_memory_store;
mod session_store;
mod session_summary_store;
#[path = "sqlite/shared_budget_store.rs"]
mod shared_budget_store;
mod steering_store;
mod storage_store;
mod task_edit_permission_store;
#[path = "sqlite/task_inspection_store.rs"]
mod task_inspection_store;
mod task_test_permission_store;
mod team_event_store;
mod team_goal_store;
mod team_store;
mod team_task_store;
mod turn_recovery_store;
mod verification_repair_store;
mod worker_cancellation_store;
mod worker_changes_store;
mod worker_dependency_store;
mod worker_message_store;
mod worker_pause_store;
mod worker_question_store;
mod worker_recovery_store;
mod workspace_changes_store;
mod workspace_handoff_store;
#[cfg(test)]
type TestProbe = std::sync::Arc<dyn Fn(&str, &Run) + Send + Sync>;
fn storage(error: sqlx::Error) -> Error {
    if error
        .as_database_error()
        .and_then(|e| e.code())
        .and_then(|code| code.parse::<u32>().ok())
        .is_some_and(|code| matches!(code & 0xff, 5 | 6))
    {
        return Error::StorageBusy(error.to_string());
    }
    Error::Storage(error.to_string())
}

impl SqliteStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let options = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(storage)?;
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.map_err(storage)?;
        schema::initialize(&mut tx).await?;
        tx.commit().await.map_err(storage)?;
        Ok(Self {
            pool,
            path: std::fs::canonicalize(path)?,
            #[cfg(test)]
            probe: None,
        })
    }
    pub fn database_path(&self) -> &Path {
        &self.path
    }
    pub async fn close(&self) {
        self.pool.close().await;
    }

    async fn load(conn: &mut SqliteConnection, id: RunId) -> Result<Run, Error> {
        let payload: Option<String> = sqlx::query_scalar("SELECT payload FROM runs WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&mut *conn)
            .await
            .map_err(storage)?;
        let record = serde_json::from_str(&payload.ok_or(Error::NotFound(id))?)?;
        let activities: Vec<String> = sqlx::query_scalar("SELECT payload FROM activities WHERE run_id=? ORDER BY scheduling_revision, command_ordinal").bind(id.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let attempts: Vec<String> = sqlx::query_scalar("SELECT t.payload FROM attempts t JOIN activities a ON a.id=t.activity_id WHERE a.run_id=? ORDER BY a.scheduling_revision, a.command_ordinal, t.ordinal").bind(id.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let timers: Vec<String> = sqlx::query_scalar("SELECT payload FROM timers WHERE run_id=? ORDER BY scheduling_revision,command_ordinal").bind(id.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        Ok(Run {
            record,
            activities: activities
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?,
            attempts: attempts
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?,
            timers: timers
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?,
            pending_events: vec![],
        })
    }

    async fn save(
        &self,
        conn: &mut SqliteConnection,
        run: &Run,
        creating: bool,
    ) -> Result<(), Error> {
        let record = run.record();
        if creating {
            sqlx::query("INSERT INTO runs(id,status,created_at,payload) VALUES(?,?,?,?)")
                .bind(record.id.to_string())
                .bind(record.status.as_str())
                .bind(record.created_at)
                .bind(serde_json::to_string(record)?)
                .execute(&mut *conn)
                .await
                .map_err(storage)?;
        } else {
            sqlx::query("UPDATE runs SET status=?,payload=? WHERE id=?")
                .bind(record.status.as_str())
                .bind(serde_json::to_string(record)?)
                .bind(record.id.to_string())
                .execute(&mut *conn)
                .await
                .map_err(storage)?;
        }
        #[cfg(test)]
        if let Some(probe) = &self.probe {
            probe("during_completion", run);
        }
        for a in run.activities() {
            sqlx::query("INSERT INTO activities(id,run_id,scheduling_revision,command_ordinal,status,ready_at,payload) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,ready_at=excluded.ready_at,payload=excluded.payload")
                .bind(a.id.to_string()).bind(a.run_id.to_string()).bind(a.scheduling_revision as i64).bind(i64::from(a.command_ordinal)).bind(a.status.as_str()).bind(a.ready_at).bind(serde_json::to_string(a)?).execute(&mut *conn).await.map_err(storage)?;
        }
        for a in run.attempts() {
            sqlx::query("INSERT INTO attempts(id,activity_id,ordinal,status,payload) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,payload=excluded.payload")
                .bind(a.id.to_string()).bind(a.activity_id.to_string()).bind(i64::from(a.ordinal)).bind(a.status.as_str()).bind(serde_json::to_string(a)?).execute(&mut *conn).await.map_err(storage)?;
        }
        for timer in run.timers() {
            sqlx::query("INSERT INTO timers(id,run_id,scheduling_revision,command_ordinal,status,due_at,payload) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,payload=excluded.payload")
                .bind(timer.id.to_string()).bind(timer.run_id.to_string()).bind(timer.scheduling_revision as i64).bind(i64::from(timer.command_ordinal)).bind(timer.status.as_str()).bind(timer.due_at).bind(serde_json::to_string(timer)?).execute(&mut *conn).await.map_err(storage)?;
        }
        for group in &record.task_groups {
            sqlx::query("INSERT INTO agent_task_groups(id,parent_run_id,payload) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload")
                .bind(group.id.to_string()).bind(record.id.to_string()).bind(serde_json::to_string(group)?)
                .execute(&mut *conn).await.map_err(storage)?;
        }
        for delegation in &record.delegations {
            sqlx::query("INSERT INTO agent_delegations(id,parent_run_id,completed,created_at,deadline_at,timed_out_at,group_id) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET completed=excluded.completed,deadline_at=excluded.deadline_at,timed_out_at=excluded.timed_out_at")
                .bind(delegation.id.to_string()).bind(record.id.to_string()).bind(delegation.outcome.is_some()).bind(delegation.created_at)
                .bind(delegation.deadline_at).bind(delegation.timed_out_at)
                .bind(delegation.group_id.map(|id|id.to_string()))
                .execute(&mut *conn).await.map_err(storage)?;
        }
        Self::save_initial_dependencies(conn, run).await?;
        for event in &run.pending_events {
            sqlx::query("INSERT INTO history(id,run_id,sequence,payload) VALUES(?,?,?,?)")
                .bind(event.id.to_string())
                .bind(event.run_id.to_string())
                .bind(event.sequence as i64)
                .bind(serde_json::to_string(event)?)
                .execute(&mut *conn)
                .await
                .map_err(storage)?;
        }
        if creating {
            Self::initialize_shared_budget(conn, record).await?;
        }
        Self::publish_session_summary(conn, run).await?;
        Ok(())
    }

    async fn load_task(conn: &mut SqliteConnection, id: TaskId) -> Result<task::CodingTask, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM coding_tasks WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        Ok(task::CodingTask {
            record: serde_json::from_str(&payload.ok_or(Error::TaskNotFound(id))?)?,
        })
    }
    async fn create_task_on(
        &self,
        conn: &mut SqliteConnection,
        task: &task::CodingTask,
        runs: &[Run],
    ) -> Result<(), Error> {
        sqlx::query("INSERT INTO coding_tasks(id,created_at,payload) VALUES(?,?,?)")
            .bind(task.record().id.to_string())
            .bind(task.record().created_at)
            .bind(serde_json::to_string(task.record())?)
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        for run in runs {
            self.save(conn, run, true).await?;
        }
        for candidate in &task.record().candidates {
            sqlx::query("INSERT INTO task_candidates(task_id,run_id,ordinal) VALUES(?,?,?)")
                .bind(task.record().id.to_string())
                .bind(candidate.run_id.to_string())
                .bind(i64::from(candidate.ordinal))
                .execute(&mut *conn)
                .await
                .map_err(storage)?;
        }
        Ok(())
    }
    async fn load_benchmark(
        conn: &mut SqliteConnection,
        id: BenchmarkId,
    ) -> Result<benchmark::Benchmark, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM benchmark_jobs WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        Ok(benchmark::Benchmark {
            record: serde_json::from_str(&payload.ok_or(Error::BenchmarkNotFound(id))?)?,
        })
    }
    async fn load_checkpoint(
        conn: &mut SqliteConnection,
        id: CheckpointId,
    ) -> Result<branch::Checkpoint, Error> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM candidate_checkpoints WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        Ok(branch::Checkpoint {
            record: serde_json::from_str(&payload.ok_or(Error::CheckpointNotFound(id))?)?,
        })
    }
    async fn save_checkpoint(
        conn: &mut SqliteConnection,
        checkpoint: &branch::Checkpoint,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE candidate_checkpoints SET status=?,payload=? WHERE id=?")
            .bind(checkpoint.record().status.as_str())
            .bind(serde_json::to_string(checkpoint.record())?)
            .bind(checkpoint.record().id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Ok(())
    }
    async fn load_fork(
        conn: &mut SqliteConnection,
        id: ForkId,
    ) -> Result<(task::CodingTask, branch::ForkRecord), Error> {
        let task_id: Option<String> =
            sqlx::query_scalar("SELECT task_id FROM candidate_forks WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *conn)
                .await
                .map_err(storage)?;
        let task_id: TaskId = task_id
            .ok_or(Error::ForkNotFound(id))?
            .parse()
            .map_err(|e: uuid::Error| Error::Storage(e.to_string()))?;
        let task = Self::load_task(conn, task_id).await?;
        let plan = task
            .record()
            .forks
            .iter()
            .find(|f| f.id == id)
            .cloned()
            .ok_or(Error::ForkNotFound(id))?;
        Ok((task, plan))
    }
    async fn save_task(conn: &mut SqliteConnection, task: &task::CodingTask) -> Result<(), Error> {
        sqlx::query("UPDATE coding_tasks SET payload=? WHERE id=?")
            .bind(serde_json::to_string(task.record())?)
            .bind(task.record().id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Self::sync_benchmark(conn, task.record()).await
    }
    async fn save_benchmark(
        conn: &mut SqliteConnection,
        job: &benchmark::Benchmark,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE benchmark_jobs SET status=?,payload=? WHERE id=?")
            .bind(job.record().status.as_str())
            .bind(serde_json::to_string(job.record())?)
            .bind(job.record().id.to_string())
            .execute(&mut *conn)
            .await
            .map_err(storage)?;
        Ok(())
    }
    async fn sync_benchmark(
        conn: &mut SqliteConnection,
        task: &task::TaskRecord,
    ) -> Result<(), Error> {
        let payload: Option<String> = sqlx::query_scalar("SELECT b.payload FROM benchmark_jobs b JOIN benchmark_trials t ON t.benchmark_id=b.id WHERE t.task_id=?")
            .bind(task.id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        if let Some(payload) = payload {
            let mut job = benchmark::Benchmark {
                record: serde_json::from_str(&payload)?,
            };
            let search = if let Some(id) = task.search_id {
                Some(Self::load_search(conn, id).await?)
            } else {
                None
            };
            if job.observe(task, search.as_ref().map(|s| s.record()))? {
                Self::save_benchmark(conn, &job).await?;
            }
        }
        Ok(())
    }
    async fn linked_task(
        conn: &mut SqliteConnection,
        id: RunId,
    ) -> Result<Option<task::CodingTask>, Error> {
        let payload: Option<String> = sqlx::query_scalar("SELECT t.payload FROM coding_tasks t WHERE EXISTS(SELECT 1 FROM task_candidates c WHERE c.task_id=t.id AND c.run_id=?) OR EXISTS(SELECT 1 FROM task_auxiliary_runs a WHERE a.task_id=t.id AND a.run_id=?)")
            .bind(id.to_string()).bind(id.to_string()).fetch_optional(&mut *conn).await.map_err(storage)?;
        payload
            .map(|payload| {
                Ok(task::CodingTask {
                    record: serde_json::from_str(&payload)?,
                })
            })
            .transpose()
    }
    async fn save_change(
        &self,
        conn: &mut SqliteConnection,
        before: &Run,
        mut run: Run,
        mut result: Option<ActivityExecution>,
    ) -> Result<(Run, Option<ActivityExecution>), Error> {
        if let Some(execution) = &result
            && Self::initial_activity_held(conn, execution.activity_id).await?
        {
            run = before.clone();
            result = None;
        }
        if let Some(receipt) = Self::goal_receipt_for_run(conn, run.record().id).await? {
            let now = run.record().updated_at.max(receipt.requested_at);
            if result.is_some()
                || run.record().allowance.admitted > before.record().allowance.admitted
            {
                run = before.clone();
                result = None;
            }
            receipt.cancel_run(&mut run, now)?;
        }
        if (result.is_some()
            || run.record().allowance.admitted > before.record().allowance.admitted)
            && Self::goal_run_paused(conn, run.record().id).await?
        {
            run = before.clone();
            result = None;
        }
        let task = Self::linked_task(conn, run.record().id).await?;
        if let Some(task) = &task
            && !task.permits_admission(before, &run)
        {
            let now = run.record().updated_at;
            run = before.clone();
            run.exhaust_task_budget(now);
            result = None;
        }
        let mut scopes = Self::load_shared_scopes(conn, run.record().id).await?;
        let now = run.record().updated_at;
        let denied: Vec<_> = scopes
            .iter()
            .filter(|s| !s.permits_admission(before.record(), run.record()))
            .map(|s| (s.record().root_run_id, s.record().kind))
            .collect();
        if let Some((root, kind)) = denied
            .iter()
            .find(|(_, kind)| *kind == shared_budget::SharedBudgetKind::Goal)
            .or_else(|| denied.first())
        {
            for scope in &mut scopes {
                if denied.contains(&(scope.record().root_run_id, scope.record().kind)) {
                    scope.exhaust(run.record().id, now, "admission_denied");
                }
            }
            run = before.clone();
            run.exhaust_shared_budget(*root, *kind, now);
            result = None;
        }
        self.save_worker_transfer_changes(conn, before, &run, result.as_ref())
            .await?;
        self.save(conn, &run, false).await?;
        for scope in scopes {
            Self::refresh_shared_scope(conn, scope, run.record().id, now).await?;
        }
        if let Some(mut task) = task {
            let runs = Self::load_task_runs(conn, &task).await?;
            task.refresh(&runs, run.record().updated_at)?;
            sqlx::query("UPDATE coding_tasks SET payload=? WHERE id=?")
                .bind(serde_json::to_string(task.record())?)
                .bind(task.record().id.to_string())
                .execute(&mut *conn)
                .await
                .map_err(storage)?;
            Self::sync_benchmark(conn, task.record()).await?;
        }
        Ok((run, result))
    }
}

impl TaskStore for SqliteStore {
    fn create_task(&self, mut task: task::CodingTask, runs: Vec<Run>) -> PortFuture<'_, ()> {
        Box::pin(async move {
            task.refresh(&runs, task.record().created_at)?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            self.create_task_on(&mut tx, &task, &runs).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn task(&self, id: TaskId) -> PortFuture<'_, task::CodingTask> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let task = Self::load_task(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(task)
        })
    }
}

impl BranchStore for SqliteStore {
    fn begin_checkpoint(
        &self,
        id: CheckpointId,
        run_id: RunId,
        now: i64,
    ) -> PortFuture<'_, branch::Checkpoint> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let existing: Option<String> =
                sqlx::query_scalar("SELECT payload FROM candidate_checkpoints WHERE id=?")
                    .bind(id.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            let checkpoint = if let Some(payload) = existing {
                let checkpoint = branch::Checkpoint {
                    record: serde_json::from_str(&payload)?,
                };
                if checkpoint.record().seed.origin.parent_run_id != run_id {
                    return Err(DomainError::ConflictingOutcome.into());
                }
                checkpoint
            } else {
                let run = Self::load(&mut tx, run_id).await?;
                let task = Self::linked_task(&mut tx, run_id).await?.ok_or_else(|| {
                    Error::Unavailable("checkpoints require a coding task candidate".into())
                })?;
                let checkpoint = branch::Checkpoint::new(id, &task, &run, now)?;
                sqlx::query("INSERT INTO candidate_checkpoints(id,task_id,parent_run_id,status,created_at,payload) VALUES(?,?,?,?,?,?)")
                    .bind(id.to_string()).bind(task.record().id.to_string()).bind(run_id.to_string())
                    .bind(checkpoint.record().status.as_str()).bind(now).bind(serde_json::to_string(checkpoint.record())?)
                    .execute(&mut *tx).await.map_err(storage)?;
                checkpoint
            };
            tx.commit().await.map_err(storage)?;
            Ok(checkpoint)
        })
    }
    fn checkpoint(&self, id: CheckpointId) -> PortFuture<'_, branch::Checkpoint> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let checkpoint = Self::load_checkpoint(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(checkpoint)
        })
    }
    fn pending_checkpoints(&self) -> PortFuture<'_, Vec<CheckpointId>> {
        Box::pin(async move {
            let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM candidate_checkpoints WHERE status='preparing' ORDER BY created_at,id")
                .fetch_all(&self.pool).await.map_err(storage)?;
            ids.into_iter()
                .map(|id| {
                    id.parse()
                        .map_err(|e: uuid::Error| Error::Storage(e.to_string()))
                })
                .collect()
        })
    }
    fn publish_checkpoint(
        &self,
        id: CheckpointId,
        snapshot: task::WorkspaceSnapshot,
        now: i64,
    ) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut checkpoint = Self::load_checkpoint(&mut tx, id).await?;
            let parent = Self::load(&mut tx, checkpoint.record().seed.origin.parent_run_id).await?;
            checkpoint.publish(snapshot, &parent, now)?;
            Self::save_checkpoint(&mut tx, &checkpoint).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn fail_checkpoint(&self, id: CheckpointId, error: String, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut checkpoint = Self::load_checkpoint(&mut tx, id).await?;
            checkpoint.fail(error, now)?;
            Self::save_checkpoint(&mut tx, &checkpoint).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn begin_fork(
        &self,
        id: ForkId,
        checkpoint_id: CheckpointId,
        count: u32,
        now: i64,
    ) -> PortFuture<'_, branch::ForkRecord> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let existing: Option<String> =
                sqlx::query_scalar("SELECT task_id FROM candidate_forks WHERE id=?")
                    .bind(id.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            let plan = if existing.is_some() {
                let (_, plan) = Self::load_fork(&mut tx, id).await?;
                if plan.checkpoint_id != checkpoint_id || plan.run_ids.len() != count as usize {
                    return Err(DomainError::ConflictingOutcome.into());
                }
                plan
            } else {
                let checkpoint = Self::load_checkpoint(&mut tx, checkpoint_id).await?;
                let mut task = Self::load_task(&mut tx, checkpoint.record().task_id).await?;
                if task.record().search_id.is_some() {
                    return Err(DomainError::InvalidConfiguration(
                        "search owns candidate expansion; manual forks are not allowed".into(),
                    )
                    .into());
                }
                let benchmark: Option<String> =
                    sqlx::query_scalar("SELECT benchmark_id FROM benchmark_trials WHERE task_id=?")
                        .bind(task.record().id.to_string())
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(storage)?;
                if benchmark.is_some() {
                    return Err(DomainError::InvalidConfiguration(
                        "fixed benchmark arms cannot add fork candidates".into(),
                    )
                    .into());
                }
                let plan = task.plan_fork(id, &checkpoint, count, now)?;
                sqlx::query("INSERT INTO candidate_forks(id,task_id,checkpoint_id,status,created_at) VALUES(?,?,?,?,?)")
                    .bind(id.to_string()).bind(task.record().id.to_string()).bind(checkpoint_id.to_string())
                    .bind(plan.status.as_str()).bind(now).execute(&mut *tx).await.map_err(storage)?;
                Self::save_task(&mut tx, &task).await?;
                plan
            };
            tx.commit().await.map_err(storage)?;
            Ok(plan)
        })
    }
    fn fork_record(&self, id: ForkId) -> PortFuture<'_, branch::ForkRecord> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let (_, plan) = Self::load_fork(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(plan)
        })
    }
    fn pending_forks(&self) -> PortFuture<'_, Vec<ForkId>> {
        Box::pin(async move {
            let ids: Vec<String> = sqlx::query_scalar(
                "SELECT id FROM candidate_forks WHERE status='preparing' ORDER BY created_at,id",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
            ids.into_iter()
                .map(|id| {
                    id.parse()
                        .map_err(|e: uuid::Error| Error::Storage(e.to_string()))
                })
                .collect()
        })
    }
    fn publish_fork(&self, id: ForkId, runs: Vec<Run>, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let (mut task, plan) = Self::load_fork(&mut tx, id).await?;
            if plan.status == branch::PreparationStatus::Ready {
                if runs.iter().map(|r| r.record().id).collect::<Vec<_>>() != plan.run_ids {
                    return Err(DomainError::ConflictingOutcome.into());
                }
                tx.commit().await.map_err(storage)?;
                return Ok(());
            }
            let checkpoint = Self::load_checkpoint(&mut tx, plan.checkpoint_id).await?;
            task.activate_fork(id, &checkpoint, &runs, now)?;
            for (index, run) in runs.iter().enumerate() {
                self.save(&mut tx, run, true).await?;
                sqlx::query("INSERT INTO task_candidates(task_id,run_id,ordinal) VALUES(?,?,?)")
                    .bind(task.record().id.to_string())
                    .bind(run.record().id.to_string())
                    .bind(i64::from(plan.first_ordinal) + index as i64)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
            }
            let all = Self::load_task_runs(&mut tx, &task).await?;
            task.refresh(&all, now)?;
            Self::save_task(&mut tx, &task).await?;
            sqlx::query("UPDATE candidate_forks SET status='ready' WHERE id=?")
                .bind(id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            tx.commit().await.map_err(storage)
        })
    }
    fn fail_fork(&self, id: ForkId, error: String, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let (mut task, _) = Self::load_fork(&mut tx, id).await?;
            task.fail_fork(id, error, now)?;
            let runs = Self::load_task_runs(&mut tx, &task).await?;
            task.refresh(&runs, now)?;
            Self::save_task(&mut tx, &task).await?;
            sqlx::query("UPDATE candidate_forks SET status='failed' WHERE id=?")
                .bind(id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            tx.commit().await.map_err(storage)
        })
    }
}

impl BenchmarkStore for SqliteStore {
    fn create_benchmark(&self, job: benchmark::Benchmark) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            sqlx::query("INSERT INTO benchmark_jobs(id,status,created_at,payload) VALUES(?,?,?,?)")
                .bind(job.record().id.to_string())
                .bind(job.record().status.as_str())
                .bind(job.record().created_at)
                .bind(serde_json::to_string(job.record())?)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            tx.commit().await.map_err(storage)
        })
    }
    fn benchmark(&self, id: BenchmarkId) -> PortFuture<'_, benchmark::Benchmark> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let job = Self::load_benchmark(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(job)
        })
    }
    fn active_benchmarks(&self) -> PortFuture<'_, Vec<BenchmarkId>> {
        Box::pin(async move {
            let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM benchmark_jobs WHERE status IN ('running','waiting') ORDER BY created_at,id")
                .fetch_all(&self.pool).await.map_err(storage)?;
            ids.into_iter()
                .map(|id| {
                    id.parse()
                        .map_err(|e: uuid::Error| Error::Storage(e.to_string()))
                })
                .collect()
        })
    }
    fn begin_trial(&self, id: BenchmarkId, task_id: TaskId, now: i64) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut job = Self::load_benchmark(&mut tx, id).await?;
            job.begin(task_id, now)?;
            Self::save_benchmark(&mut tx, &job).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn activate_trial(
        &self,
        id: BenchmarkId,
        mut task: task::CodingTask,
        runs: Vec<Run>,
        search: Option<search::Search>,
    ) -> PortFuture<'_, ()> {
        Box::pin(async move {
            task.refresh(&runs, task.record().created_at)?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut job = Self::load_benchmark(&mut tx, id).await?;
            job.activate(task.record(), search.as_ref().map(|s| s.record()))?;
            self.create_task_on(&mut tx, &task, &runs).await?;
            if let Some(search) = &search {
                Self::insert_search(&mut tx, search, &task).await?;
            }
            sqlx::query("INSERT INTO benchmark_trials(benchmark_id,task_id,ordinal) VALUES(?,?,?)")
                .bind(id.to_string())
                .bind(task.record().id.to_string())
                .bind(i64::from(job.trial(task.record().id)?.ordinal))
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            Self::save_benchmark(&mut tx, &job).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn fail_preparation(
        &self,
        id: BenchmarkId,
        task_id: TaskId,
        message: String,
        now: i64,
    ) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut job = Self::load_benchmark(&mut tx, id).await?;
            job.fail_preparation(task_id, message, now)?;
            Self::save_benchmark(&mut tx, &job).await?;
            tx.commit().await.map_err(storage)
        })
    }
}

impl ExecutionStore for SqliteStore {
    fn authorize_task_edit_wait(
        &self,
        decision: TaskEditPermissionDecision,
    ) -> PortFuture<'_, bool> {
        Box::pin(self.apply_task_edit_permission(decision))
    }
    fn authorize_test_wait(&self, decision: TestPermissionDecision) -> PortFuture<'_, bool> {
        Box::pin(self.apply_test_permission(decision))
    }
    fn authorize_task_test_wait(
        &self,
        decision: TaskTestPermissionDecision,
    ) -> PortFuture<'_, bool> {
        Box::pin(self.apply_task_test_permission(decision))
    }
    fn shared_budgets(&self, id: RunId) -> PortFuture<'_, Vec<shared_budget::SharedBudgetView>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            Self::load(&mut tx, id).await?;
            let views = Self::shared_budget_views(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(views)
        })
    }
    fn create(&self, run: Run) -> PortFuture<'_, ()> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            self.save(&mut tx, &run, true).await?;
            tx.commit().await.map_err(storage)
        })
    }
    fn mutate(&self, id: RunId, decision: Decision) -> PortFuture<'_, Option<ActivityExecution>> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let mut run = Self::load(&mut tx, id).await?;
            let before = run.clone();
            let now = run.record().updated_at;
            Self::guard_goal_run(&mut tx, &mut run, now).await?;
            Self::active_planner_question(&mut tx, &mut run).await?;
            let result = decision(&mut run)?;
            let (_run, result) = self.save_change(&mut tx, &before, run, result).await?;
            tx.commit().await.map_err(storage)?;
            #[cfg(test)]
            if let Some(probe) = &self.probe {
                probe("after_commit", &_run);
            }
            Ok(result)
        })
    }
    fn mutate_due(
        &self,
        now: i64,
        decision: Decision,
    ) -> PortFuture<'_, Option<ActivityExecution>> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let row = sqlx::query("SELECT a.run_id FROM activities a JOIN runs r ON r.id=a.run_id WHERE a.status='queued' AND a.ready_at<=? AND r.status IN ('queued','running') AND NOT EXISTS(SELECT 1 FROM harness_initial_blocked_activities b WHERE b.id=a.id) AND NOT EXISTS(SELECT 1 FROM agent_runs l JOIN agent_goal_paused_requests h ON h.id=l.request_id WHERE l.run_id=r.id) AND NOT EXISTS (SELECT 1 FROM activities running WHERE running.run_id=a.run_id AND running.status='running') ORDER BY a.ready_at,r.created_at,a.scheduling_revision,a.command_ordinal LIMIT 1").bind(now).fetch_optional(&mut *tx).await.map_err(storage)?;
            let result = if let Some(row) = row {
                let id: String = row.get(0);
                let id = id
                    .parse()
                    .map_err(|e: uuid::Error| Error::Storage(e.to_string()))?;
                let mut run = Self::load(&mut tx, id).await?;
                let before = run.clone();
                let result = if Self::guard_goal_run(&mut tx, &mut run, now).await? {
                    None
                } else {
                    Self::active_planner_question(&mut tx, &mut run).await?;
                    decision(&mut run)?
                };
                let (_, result) = self.save_change(&mut tx, &before, run, result).await?;
                result
            } else {
                None
            };
            tx.commit().await.map_err(storage)?;
            Ok(result)
        })
    }
    fn get(&self, id: RunId) -> PortFuture<'_, Run> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            let run = Self::load(&mut tx, id).await?;
            tx.commit().await.map_err(storage)?;
            Ok(run)
        })
    }
    fn mutate_due_timer(&self, now: i64, decision: TimerDecision) -> PortFuture<'_, bool> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let resolved = Self::refresh_worker_questions(&mut tx, now).await?;
            let row = sqlx::query("SELECT t.run_id,t.id FROM timers t JOIN runs r ON r.id=t.run_id LEFT JOIN harness_worker_questions q ON q.question_id=json_extract(t.payload,'$.payload.question_id') AND q.requester_run_id=r.id WHERE t.status='scheduled' AND (t.due_at<=? OR q.resolved=1) AND (r.status IN ('queued','running') OR r.status='waiting' AND (json_extract(r.payload,'$.worker_reply_wait.timer_id')=t.id OR json_extract(t.payload,'$.payload.key') IN ('planner_worker_question','planner_guidance'))) AND (COALESCE(json_extract(t.payload,'$.payload.key'),'') NOT IN ('planner_worker_question','planner_guidance') OR r.status='waiting' AND COALESCE(json_type(r.payload,'$.planner_question'),'')!='object' AND COALESCE(json_type(r.payload,'$.planner_guidance'),'')!='object' AND NOT EXISTS(SELECT 1 FROM activities a WHERE a.run_id=r.id AND a.status IN ('queued','running'))) AND NOT EXISTS(SELECT 1 FROM agent_runs l JOIN agent_goal_paused_requests h ON h.id=l.request_id WHERE l.run_id=r.id) AND NOT EXISTS (SELECT 1 FROM activities running WHERE running.run_id=t.run_id AND running.status='running') ORDER BY t.due_at,r.created_at,t.scheduling_revision,t.command_ordinal LIMIT 1")
                .bind(now).fetch_optional(&mut *tx).await.map_err(storage)?;
            let result = if let Some(row) = row {
                let run_id: String = row.get(0);
                let timer_id: String = row.get(1);
                let run_id = run_id
                    .parse()
                    .map_err(|e: uuid::Error| Error::Storage(e.to_string()))?;
                let timer_id = timer_id
                    .parse()
                    .map_err(|e: uuid::Error| Error::Storage(e.to_string()))?;
                let mut run = Self::load(&mut tx, run_id).await?;
                let before = run.clone();
                Self::question_for_timer(&mut tx, &mut run, timer_id).await?;
                Self::guidance_for_timer(&mut tx, &mut run, timer_id).await?;
                let result = if Self::guard_goal_run(&mut tx, &mut run, now).await? {
                    run.record().history_sequence != before.record().history_sequence
                } else {
                    decision(&mut run, timer_id)?
                };
                Self::mark_dependency_reviews(&mut tx, &run, timer_id).await?;
                let (_run, _) = self.save_change(&mut tx, &before, run, None).await?;
                tx.commit().await.map_err(storage)?;
                #[cfg(test)]
                if let Some(probe) = &self.probe {
                    probe("after_commit", &_run);
                }
                return Ok(result);
            } else {
                resolved
            };
            tx.commit().await.map_err(storage)?;
            Ok(result)
        })
    }
    fn history(&self, id: RunId) -> PortFuture<'_, Vec<HistoryEvent>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(storage)?;
            if sqlx::query_scalar::<_, String>("SELECT id FROM runs WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?
                .is_none()
            {
                return Err(Error::NotFound(id));
            }
            let rows: Vec<String> =
                sqlx::query_scalar("SELECT payload FROM history WHERE run_id=? ORDER BY sequence")
                    .bind(id.to_string())
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(storage)?;
            let events = rows
                .iter()
                .map(|s| serde_json::from_str(s))
                .collect::<Result<_, _>>()?;
            tx.commit().await.map_err(storage)?;
            Ok(events)
        })
    }
    fn active_runs(&self) -> PortFuture<'_, Vec<RunId>> {
        Box::pin(async move {
            let ids: Vec<String> = sqlx::query_scalar(
                "SELECT id FROM runs WHERE status IN ('queued','running','waiting','paused') ORDER BY created_at,id",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
            ids.iter()
                .map(|id| {
                    id.parse()
                        .map_err(|e: uuid::Error| Error::Storage(e.to_string()))
                })
                .collect()
        })
    }
    fn runnable_runs(&self) -> PortFuture<'_, Vec<RunId>> {
        Box::pin(async move {
            let ids: Vec<String> = sqlx::query_scalar("SELECT r.id FROM runs r WHERE r.status IN ('queued','running') AND (EXISTS(SELECT 1 FROM activities a WHERE a.run_id=r.id AND a.status IN ('queued','running') AND NOT EXISTS(SELECT 1 FROM harness_initial_blocked_activities b WHERE b.id=a.id)) OR NOT EXISTS(SELECT 1 FROM agent_runs l JOIN harness_initial_dependencies d ON d.task_id=l.request_id WHERE l.run_id=r.id AND d.status='pending')) AND (NOT EXISTS(SELECT 1 FROM agent_runs a JOIN agent_goal_paused_requests h ON h.id=a.request_id WHERE a.run_id=r.id) OR EXISTS(SELECT 1 FROM activities a WHERE a.run_id=r.id AND a.status='running')) ORDER BY r.created_at,r.id")
                .fetch_all(&self.pool).await.map_err(storage)?;
            ids.iter()
                .map(|id| {
                    id.parse()
                        .map_err(|e: uuid::Error| Error::Storage(e.to_string()))
                })
                .collect()
        })
    }
}

#[cfg(test)]
#[path = "sqlite/recovery_tests.rs"]
mod recovery_tests;
