use act0r::{
    adapters::{candidates::LocalCandidates, sqlite::SqliteStore},
    application::{BranchService, ports::*},
    bootstrap::local_app,
    domain::{branch::*, task::*, *},
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

fn config() -> TaskConfig {
    let mut config: TaskConfig =
        serde_json::from_str(include_str!("../examples/configs/task-scripted.json")).unwrap();
    config.candidates = 1;
    config
}
fn source() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/coding-task")
}
async fn drain(worker: &mut act0r::Worker) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline);
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
async fn ready_checkpoint(
    engine: &act0r::Engine,
    agents: &act0r::application::AgentService,
    worker: &mut act0r::Worker,
    parent: RunId,
) -> CheckpointId {
    engine.pause(parent).await.unwrap();
    let id = CheckpointId::new();
    agents
        .branches()
        .unwrap()
        .create_checkpoint(id, parent)
        .await
        .unwrap();
    assert!(worker.advance_work().await.unwrap());
    assert_eq!(
        agents
            .branches()
            .unwrap()
            .checkpoint(id)
            .await
            .unwrap()
            .record()
            .status,
        PreparationStatus::Ready
    );
    id
}
async fn pool(path: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(path)
            .foreign_keys(true),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn publication_rejects_a_child_whose_saved_continuation_differs_from_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("forged.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let task_id = agents
        .tasks()
        .unwrap()
        .start(source(), config())
        .await
        .unwrap();
    let task = agents.tasks().unwrap().status(task_id).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    let id = ready_checkpoint(
        &engine,
        &agents,
        &mut worker,
        task.record().candidates[0].run_id,
    )
    .await;
    let checkpoint = agents.branches().unwrap().checkpoint(id).await.unwrap();
    let plan = agents
        .branches()
        .unwrap()
        .create_fork(ForkId::new(), id, 1)
        .await
        .unwrap();
    let snapshot = checkpoint.record().snapshot.clone().unwrap();
    let root = directory.path().join(".act0r");
    let workspaces = LocalCandidates::new(root.join("snapshots"), root.join("workspaces"));
    let workspace = workspaces
        .fork_checkpoint(snapshot, plan.run_ids[0])
        .await
        .unwrap();
    let mut input = checkpoint.record().input.clone();
    input.config.workspace = workspace;
    let mut seed = checkpoint.record().seed.clone();
    seed.agent.config = input.config.clone();
    seed.next_model.input["instructions"] = serde_json::json!("a different saved request");
    input.branch = Some(seed);
    let forged = Run::start_with_id(
        plan.run_ids[0],
        &BranchCandidateWorkflow,
        serde_json::to_value(input).unwrap(),
        task.record().config.budget.max_attempts,
        0,
    )
    .unwrap();
    let store = SqliteStore::open(&db).await.unwrap();
    assert!(store.publish_fork(plan.id, vec![forged], 0).await.is_err());
    assert_eq!(
        agents
            .branches()
            .unwrap()
            .fork(plan.id)
            .await
            .unwrap()
            .status,
        PreparationStatus::Preparing
    );
    assert_eq!(
        agents
            .tasks()
            .unwrap()
            .status(task_id)
            .await
            .unwrap()
            .record()
            .candidates
            .len(),
        1
    );
    // The same prepared workspace can still publish with its exact pinned seed.
    assert!(worker.advance_work().await.unwrap());
    assert_eq!(
        agents
            .branches()
            .unwrap()
            .fork(plan.id)
            .await
            .unwrap()
            .status,
        PreparationStatus::Ready
    );
}

#[tokio::test]
async fn forks_reuse_progress_context_and_request_with_prefix_charged_once() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("forks.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let task_id = agents
        .tasks()
        .unwrap()
        .start(source(), config())
        .await
        .unwrap();
    let task = agents.tasks().unwrap().status(task_id).await.unwrap();
    let parent = task.record().candidates[0].run_id;
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..6 {
        assert!(worker.step().await.unwrap());
    }
    let prefix = engine.status(parent).await.unwrap();
    assert_eq!(prefix.record().allowance.admitted, 6);
    assert_eq!(prefix.record().state["agent"]["turn"], 3);
    let next_request = prefix
        .activities()
        .iter()
        .find(|a| a.status == ActivityStatus::Queued)
        .unwrap()
        .spec
        .input
        .clone();
    let cp_id = ready_checkpoint(&engine, &agents, &mut worker, parent).await;
    let cp = agents.branches().unwrap().checkpoint(cp_id).await.unwrap();
    let repeated = agents
        .branches()
        .unwrap()
        .create_checkpoint(cp_id, parent)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(cp.record()).unwrap(),
        serde_json::to_value(repeated.record()).unwrap()
    );
    assert!(
        agents
            .branches()
            .unwrap()
            .create_checkpoint(cp_id, RunId::new())
            .await
            .is_err()
    );
    let fork_id = ForkId::new();
    let plan = agents
        .branches()
        .unwrap()
        .create_fork(fork_id, cp_id, 2)
        .await
        .unwrap();
    assert_eq!(
        plan.run_ids,
        agents
            .branches()
            .unwrap()
            .create_fork(fork_id, cp_id, 2)
            .await
            .unwrap()
            .run_ids
    );
    assert!(
        agents
            .branches()
            .unwrap()
            .create_fork(fork_id, cp_id, 1)
            .await
            .is_err()
    );
    assert!(worker.advance_work().await.unwrap());
    let task = agents.tasks().unwrap().status(task_id).await.unwrap();
    assert_eq!(task.record().attempts, 6);
    assert_eq!(task.record().usage.admitted_model_calls, 3);
    assert_eq!(task.record().candidates.len(), 3);
    for child in &plan.run_ids {
        let run = engine.status(*child).await.unwrap();
        assert_eq!(run.record().workflow, BranchCandidateWorkflow.id());
        assert_eq!(run.record().allowance.admitted, 6);
        assert_eq!(run.record().usage.admitted_model_calls, 3);
        assert!(run.attempts().is_empty());
        assert_eq!(run.activities().len(), 1);
        assert_eq!(run.activities()[0].spec.input, next_request);
        assert_eq!(
            run.record().state["agent"]["observations"],
            prefix.record().state["agent"]["observations"]
        );
        let workspace = run.record().input["config"]["workspace"].as_str().unwrap();
        assert!(
            fs::read_to_string(Path::new(workspace).join("src/lib.rs"))
                .unwrap()
                .contains("left + right")
        );
        assert!(
            engine
                .history(*child)
                .await
                .unwrap()
                .iter()
                .any(|e| e.kind == "run_forked")
        );
    }
    let first = engine.status(plan.run_ids[0]).await.unwrap();
    let first_workspace = Path::new(
        first.record().input["config"]["workspace"]
            .as_str()
            .unwrap(),
    );
    fs::write(
        first_workspace.join("src/lib.rs"),
        "pub fn add(left: i32, right: i32) -> i32 { left + right }\n// Branch-local note.\n",
    )
    .unwrap();
    let second = engine.status(plan.run_ids[1]).await.unwrap();
    assert!(
        !fs::read_to_string(
            Path::new(
                second.record().input["config"]["workspace"]
                    .as_str()
                    .unwrap()
            )
            .join("src/lib.rs")
        )
        .unwrap()
        .contains("Branch-local")
    );
    engine.cancel(parent).await.unwrap();
    drain(&mut worker).await;
    let task = agents.tasks().unwrap().status(task_id).await.unwrap();
    assert_eq!(task.record().status, TaskStatus::Succeeded);
    assert_eq!(task.record().selected, Some(plan.run_ids[1]));
    assert_eq!(task.record().attempts, 14);
    assert_eq!(task.record().usage.admitted_model_calls, 7);
    assert_eq!(task.record().usage.charged_tokens(), 140);
    for child in &plan.run_ids {
        let run = engine.status(*child).await.unwrap();
        assert_eq!(run.record().allowance.admitted, 10);
        assert_eq!(run.attempts().len(), 4);
        assert_eq!(run.record().usage.admitted_model_calls, 5);
        assert_eq!(run.record().result.as_ref().unwrap()["tests_passed"], true);
    }
    let saved = serde_json::to_value(task.record()).unwrap();
    drop(worker);
    drop(engine);
    drop(agents);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.step().await.unwrap());
    assert_eq!(
        serde_json::to_value(
            agents
                .tasks()
                .unwrap()
                .status(task_id)
                .await
                .unwrap()
                .record()
        )
        .unwrap(),
        saved
    );
    assert_eq!(
        agents
            .branches()
            .unwrap()
            .create_fork(fork_id, cp_id, 2)
            .await
            .unwrap()
            .run_ids,
        plan.run_ids
    );
}

#[tokio::test]
async fn nested_forks_keep_uncertain_prefix_usage_and_cannot_reset_shared_limits() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("budget.db")).await.unwrap();
    let mut cfg = config();
    cfg.budget.max_model_calls = 5;
    let task_id = agents.tasks().unwrap().start(source(), cfg).await.unwrap();
    let parent = agents
        .tasks()
        .unwrap()
        .status(task_id)
        .await
        .unwrap()
        .record()
        .candidates[0]
        .run_id;
    let mut worker = engine.worker().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    // Successful action with missing provider usage leaves a permanently held reservation.
    worker
        .complete(
            execution,
            Outcome::Success(outcome.result().unwrap().clone()),
        )
        .await
        .unwrap();
    for _ in 0..5 {
        worker.step().await.unwrap();
    }
    let cp = ready_checkpoint(&engine, &agents, &mut worker, parent).await;
    let held = engine
        .status(parent)
        .await
        .unwrap()
        .record()
        .usage
        .held_tokens;
    assert!(held > 0);
    let first = agents
        .branches()
        .unwrap()
        .create_fork(ForkId::new(), cp, 1)
        .await
        .unwrap();
    worker.advance_work().await.unwrap();
    let child = first.run_ids[0];
    let nested = ready_checkpoint(&engine, &agents, &mut worker, child).await;
    let second = agents
        .branches()
        .unwrap()
        .create_fork(ForkId::new(), nested, 1)
        .await
        .unwrap();
    worker.advance_work().await.unwrap();
    let task = agents.tasks().unwrap().status(task_id).await.unwrap();
    assert_eq!(task.record().usage.held_tokens, held);
    assert_eq!(task.record().usage.uncertain_calls, 1);
    assert_eq!(task.record().usage.admitted_model_calls, 3);
    engine.cancel(parent).await.unwrap();
    engine.resume(child).await.unwrap();
    drain(&mut worker).await;
    let task = agents.tasks().unwrap().status(task_id).await.unwrap();
    assert_eq!(task.record().usage.admitted_model_calls, 5);
    assert_eq!(task.record().usage.held_tokens, held);
    assert_eq!(task.record().usage.uncertain_calls, 1);
    assert!(
        task.record()
            .candidates
            .iter()
            .any(|c| c.status == RunStatus::BudgetExhausted)
    );
    let grandchild = engine.status(second.run_ids[0]).await.unwrap();
    assert!(grandchild.record().usage.admitted_model_calls >= 3);
    assert_eq!(grandchild.record().usage.held_tokens, held);
}

#[tokio::test]
async fn checkpoint_guards_reject_active_tool_retry_wait_and_changed_parent_boundaries() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("guards.db")).await.unwrap();
    let task = agents
        .tasks()
        .unwrap()
        .start(source(), config())
        .await
        .unwrap();
    let parent = agents
        .tasks()
        .unwrap()
        .status(task)
        .await
        .unwrap()
        .record()
        .candidates[0]
        .run_id;
    let mut worker = engine.worker().await.unwrap();
    assert!(
        agents
            .branches()
            .unwrap()
            .create_checkpoint(CheckpointId::new(), parent)
            .await
            .is_err()
    );
    worker.step().await.unwrap();
    engine.pause(parent).await.unwrap();
    assert!(
        agents
            .branches()
            .unwrap()
            .create_checkpoint(CheckpointId::new(), parent)
            .await
            .is_err()
    );
    engine.resume(parent).await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    engine.pause(parent).await.unwrap();
    assert!(
        agents
            .branches()
            .unwrap()
            .create_checkpoint(CheckpointId::new(), parent)
            .await
            .is_err()
    );
    let result = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, result).await.unwrap();
    let id = CheckpointId::new();
    agents
        .branches()
        .unwrap()
        .create_checkpoint(id, parent)
        .await
        .unwrap();
    let revision = engine.status(parent).await.unwrap().record().revision;
    engine.resume(parent).await.unwrap();
    engine.pause(parent).await.unwrap();
    assert_eq!(
        engine.status(parent).await.unwrap().record().revision,
        revision
    );
    worker.advance_work().await.unwrap();
    let cp = agents.branches().unwrap().checkpoint(id).await.unwrap();
    assert_eq!(cp.record().status, PreparationStatus::Failed);
    assert!(
        cp.record()
            .error
            .as_ref()
            .unwrap()
            .contains("parent changed")
    );
    assert!(
        agents
            .branches()
            .unwrap()
            .create_fork(ForkId::new(), id, 1)
            .await
            .is_err()
    );
    engine.resume(parent).await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    worker
        .complete(execution, Outcome::Uncertain("lost response".into()))
        .await
        .unwrap();
    engine.pause(parent).await.unwrap();
    assert!(
        agents
            .branches()
            .unwrap()
            .create_checkpoint(CheckpointId::new(), parent)
            .await
            .is_err()
    );
    engine.cancel(parent).await.unwrap();
    let mut cfg = config();
    cfg.agent.require_approval = true;
    let task = agents.tasks().unwrap().start(source(), cfg).await.unwrap();
    let waiting = agents
        .tasks()
        .unwrap()
        .status(task)
        .await
        .unwrap()
        .record()
        .candidates[0]
        .run_id;
    engine.resume(waiting).await.unwrap();
    drain(&mut worker).await;
    engine.pause(waiting).await.unwrap();
    assert!(
        agents
            .branches()
            .unwrap()
            .create_checkpoint(CheckpointId::new(), waiting)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn protected_and_tampered_snapshots_fail_without_publishing_branches_or_charges() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("tamper.db")).await.unwrap();
    let task_id = agents
        .tasks()
        .unwrap()
        .start(source(), config())
        .await
        .unwrap();
    let parent = agents
        .tasks()
        .unwrap()
        .status(task_id)
        .await
        .unwrap()
        .record()
        .candidates[0]
        .run_id;
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..6 {
        worker.step().await.unwrap();
    }
    engine.pause(parent).await.unwrap();
    let parent_run = engine.status(parent).await.unwrap();
    let tests = Path::new(
        parent_run.record().input["config"]["workspace"]
            .as_str()
            .unwrap(),
    )
    .join("tests/add.rs");
    let original = fs::read(&tests).unwrap();
    fs::write(&tests, "#[test] fn fake() {}\n").unwrap();
    let bad = CheckpointId::new();
    agents
        .branches()
        .unwrap()
        .create_checkpoint(bad, parent)
        .await
        .unwrap();
    worker.advance_work().await.unwrap();
    assert_eq!(
        agents
            .branches()
            .unwrap()
            .checkpoint(bad)
            .await
            .unwrap()
            .record()
            .status,
        PreparationStatus::Failed
    );
    fs::write(tests, original).unwrap();
    let cp = ready_checkpoint(&engine, &agents, &mut worker, parent).await;
    let record = agents.branches().unwrap().checkpoint(cp).await.unwrap();
    let fork = agents
        .branches()
        .unwrap()
        .create_fork(ForkId::new(), cp, 2)
        .await
        .unwrap();
    fs::write(
        Path::new(&record.record().snapshot.as_ref().unwrap().workspace).join("src/lib.rs"),
        "tampered",
    )
    .unwrap();
    worker.advance_work().await.unwrap();
    assert_eq!(
        agents
            .branches()
            .unwrap()
            .fork(fork.id)
            .await
            .unwrap()
            .status,
        PreparationStatus::Failed
    );
    let task = agents.tasks().unwrap().status(task_id).await.unwrap();
    assert_eq!(task.record().candidates.len(), 1);
    assert_eq!(task.record().attempts, 6);
    assert_eq!(task.record().usage.admitted_model_calls, 3);
    for child in fork.run_ids {
        assert!(engine.status(child).await.is_err());
    }
}

#[tokio::test]
async fn checkpoint_and_fork_publication_roll_back_and_retry_with_the_same_identities() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("rollback.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let task_id = agents
        .tasks()
        .unwrap()
        .start(source(), config())
        .await
        .unwrap();
    let parent = agents
        .tasks()
        .unwrap()
        .status(task_id)
        .await
        .unwrap()
        .record()
        .candidates[0]
        .run_id;
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..6 {
        worker.step().await.unwrap();
    }
    engine.pause(parent).await.unwrap();
    let cp = CheckpointId::new();
    agents
        .branches()
        .unwrap()
        .create_checkpoint(cp, parent)
        .await
        .unwrap();
    let pool = pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_cp BEFORE UPDATE ON candidate_checkpoints BEGIN SELECT RAISE(ABORT,'rollback checkpoint'); END;").execute(&pool).await.unwrap();
    assert!(worker.advance_work().await.is_err());
    let pending = agents.branches().unwrap().checkpoint(cp).await.unwrap();
    assert_eq!(pending.record().status, PreparationStatus::Preparing);
    sqlx::raw_sql("DROP TRIGGER reject_cp;")
        .execute(&pool)
        .await
        .unwrap();
    worker.advance_work().await.unwrap();
    assert_eq!(
        agents
            .branches()
            .unwrap()
            .checkpoint(cp)
            .await
            .unwrap()
            .record()
            .artifact_id,
        pending.record().artifact_id
    );
    let plan = agents
        .branches()
        .unwrap()
        .create_fork(ForkId::new(), cp, 2)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_fork BEFORE UPDATE ON candidate_forks BEGIN SELECT RAISE(ABORT,'rollback fork'); END;").execute(&pool).await.unwrap();
    assert!(worker.advance_work().await.is_err());
    assert_eq!(
        agents
            .branches()
            .unwrap()
            .fork(plan.id)
            .await
            .unwrap()
            .status,
        PreparationStatus::Preparing
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_candidates")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        agents
            .tasks()
            .unwrap()
            .status(task_id)
            .await
            .unwrap()
            .record()
            .attempts,
        6
    );
    sqlx::raw_sql("DROP TRIGGER reject_fork;")
        .execute(&pool)
        .await
        .unwrap();
    worker.advance_work().await.unwrap();
    let published = agents.branches().unwrap().fork(plan.id).await.unwrap();
    assert_eq!(published.run_ids, plan.run_ids);
    let store = SqliteStore::open(&db).await.unwrap();
    let mut runs = vec![];
    for child in plan.run_ids {
        runs.push(engine.status(child).await.unwrap());
    }
    store
        .publish_fork(plan.id, runs, published.updated_at)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    pool.close().await;
}

#[tokio::test]
async fn concurrent_plans_reserve_slots_and_fixed_benchmark_arms_reject_forks() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("slots.db")).await.unwrap();
    let task = agents
        .tasks()
        .unwrap()
        .start(source(), config())
        .await
        .unwrap();
    let parent = agents
        .tasks()
        .unwrap()
        .status(task)
        .await
        .unwrap()
        .record()
        .candidates[0]
        .run_id;
    let mut worker = engine.worker().await.unwrap();
    let cp = ready_checkpoint(&engine, &agents, &mut worker, parent).await;
    let service = agents.branches().unwrap();
    let (first, second) = tokio::join!(
        service.create_fork(ForkId::new(), cp, 8),
        service.create_fork(ForkId::new(), cp, 8)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let task = agents.tasks().unwrap().status(task).await.unwrap();
    assert_eq!(task.record().forks.len(), 1);
    assert!(service.create_fork(ForkId::new(), cp, 8).await.is_err());
    // A separate database keeps the unfinished capacity test out of the benchmark scheduler.
    drop(worker);
    fs::create_dir(directory.path().join("arms")).unwrap();
    let (engine, agents) = local_app(directory.path().join("arms/benchmark.db"))
        .await
        .unwrap();
    let mut suite: act0r::domain::benchmark::BenchmarkConfig =
        serde_json::from_str(include_str!("../examples/configs/benchmark-scripted.json")).unwrap();
    suite.cases.truncate(1);
    let benchmark = agents
        .benchmarks()
        .unwrap()
        .start(
            suite,
            Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/configs"),
        )
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    worker.advance_work().await.unwrap();
    let job = agents
        .benchmarks()
        .unwrap()
        .status(benchmark)
        .await
        .unwrap();
    let task = agents
        .tasks()
        .unwrap()
        .status(job.record().trials[0].task_id)
        .await
        .unwrap();
    let cp = ready_checkpoint(
        &engine,
        &agents,
        &mut worker,
        task.record().candidates[0].run_id,
    )
    .await;
    let error = agents
        .branches()
        .unwrap()
        .create_fork(ForkId::new(), cp, 1)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("fixed benchmark"));
}

struct CrashWorkspaces {
    inner: LocalCandidates,
    marker: PathBuf,
    phase: String,
}
impl CandidateWorkspaces for CrashWorkspaces {
    fn snapshot(&self, source: PathBuf, id: TaskId) -> PortFuture<'_, WorkspaceSnapshot> {
        self.inner.snapshot(source, id)
    }
    fn fork(&self, snapshot: WorkspaceSnapshot, id: RunId) -> PortFuture<'_, String> {
        self.inner.fork(snapshot, id)
    }
    fn evaluate(
        &self,
        request: EvaluationRequest,
        id: ActivityId,
    ) -> PortFuture<'_, CandidateEvaluation> {
        self.inner.evaluate(request, id)
    }
    fn capture_checkpoint(
        &self,
        input: CandidateInput,
        id: TaskId,
    ) -> PortFuture<'_, WorkspaceSnapshot> {
        Box::pin(async move {
            let result = self.inner.capture_checkpoint(input, id).await?;
            if self.phase == "checkpoint_before_publish" {
                fs::write(&self.marker, b"ready").unwrap();
                loop {
                    std::thread::park();
                }
            }
            Ok(result)
        })
    }
    fn fork_checkpoint(&self, snapshot: WorkspaceSnapshot, id: RunId) -> PortFuture<'_, String> {
        Box::pin(async move {
            let result = self.inner.fork_checkpoint(snapshot, id).await?;
            if self.phase == "fork_before_publish" {
                fs::write(&self.marker, b"ready").unwrap();
                loop {
                    std::thread::park();
                }
            }
            Ok(result)
        })
    }
}
#[tokio::test]
#[ignore = "subprocess checkpoint/fork crash fixture"]
async fn branch_crash_child() {
    let db = PathBuf::from(std::env::var("ACT0R_BRANCH_CRASH_DB").unwrap());
    let marker = PathBuf::from(std::env::var("ACT0R_BRANCH_CRASH_MARKER").unwrap());
    let phase = std::env::var("ACT0R_BRANCH_CRASH_PHASE").unwrap();
    let (engine, _) = local_app(&db).await.unwrap();
    let store = Arc::new(SqliteStore::open(&db).await.unwrap());
    let root = db.parent().unwrap().join(".act0r");
    let workspaces = Arc::new(CrashWorkspaces {
        inner: LocalCandidates::new(root.join("snapshots"), root.join("workspaces")),
        marker: marker.clone(),
        phase: phase.clone(),
    });
    let service = Arc::new(BranchService::new(
        engine.clone(),
        store.clone(),
        store,
        workspaces,
    ));
    let engine = engine.with_coordinator(service);
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.advance_work().await.unwrap());
    if phase == "fork_after_commit" {
        fs::write(marker, b"ready").unwrap();
    }
    loop {
        std::thread::park();
    }
}
#[tokio::test]
async fn killed_preparations_reuse_checkpoints_child_ids_and_committed_fork_graphs() {
    for phase in [
        "checkpoint_before_publish",
        "fork_before_publish",
        "fork_after_commit",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("crash.db");
        let marker = directory.path().join("marker");
        let (engine, agents) = local_app(&db).await.unwrap();
        let task_id = agents
            .tasks()
            .unwrap()
            .start(source(), config())
            .await
            .unwrap();
        let parent = agents
            .tasks()
            .unwrap()
            .status(task_id)
            .await
            .unwrap()
            .record()
            .candidates[0]
            .run_id;
        let mut worker = engine.worker().await.unwrap();
        for _ in 0..6 {
            worker.step().await.unwrap();
        }
        engine.pause(parent).await.unwrap();
        let cp = CheckpointId::new();
        agents
            .branches()
            .unwrap()
            .create_checkpoint(cp, parent)
            .await
            .unwrap();
        let artifact = agents
            .branches()
            .unwrap()
            .checkpoint(cp)
            .await
            .unwrap()
            .record()
            .artifact_id;
        let mut plan = None;
        if phase != "checkpoint_before_publish" {
            worker.advance_work().await.unwrap();
            plan = Some(
                agents
                    .branches()
                    .unwrap()
                    .create_fork(ForkId::new(), cp, 2)
                    .await
                    .unwrap(),
            );
        }
        drop(worker);
        drop(engine);
        drop(agents);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "branch_crash_child", "--ignored", "--nocapture"])
            .env("ACT0R_BRANCH_CRASH_DB", &db)
            .env("ACT0R_BRANCH_CRASH_MARKER", &marker)
            .env("ACT0R_BRANCH_CRASH_PHASE", phase)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !marker.exists() {
            assert!(child.try_wait().unwrap().is_none());
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        if phase != "fork_after_commit" {
            worker.advance_work().await.unwrap();
        }
        let ready = agents.branches().unwrap().checkpoint(cp).await.unwrap();
        assert_eq!(ready.record().status, PreparationStatus::Ready);
        assert_eq!(ready.record().artifact_id, artifact);
        if plan.is_none() {
            plan = Some(
                agents
                    .branches()
                    .unwrap()
                    .create_fork(ForkId::new(), cp, 2)
                    .await
                    .unwrap(),
            );
            worker.advance_work().await.unwrap();
        }
        let plan = plan.unwrap();
        assert_eq!(
            agents
                .branches()
                .unwrap()
                .fork(plan.id)
                .await
                .unwrap()
                .run_ids,
            plan.run_ids
        );
        engine.cancel(parent).await.unwrap();
        drain(&mut worker).await;
        let task = agents.tasks().unwrap().status(task_id).await.unwrap();
        assert_eq!(task.record().candidates.len(), 3);
        assert_eq!(task.record().status, TaskStatus::Succeeded);
        assert_eq!(task.record().attempts, 14);
        assert_eq!(task.record().usage.admitted_model_calls, 7);
        for child in &plan.run_ids {
            let run = engine.status(*child).await.unwrap();
            assert_eq!(run.record().status, RunStatus::Succeeded);
            assert_eq!(run.attempts().len(), 4);
        }
    }
}
