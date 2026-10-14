use act0r::{
    bootstrap::local_app,
    domain::{benchmark::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn config() -> BenchmarkConfig {
    serde_json::from_str(include_str!("../examples/configs/benchmark-scripted.json")).unwrap()
}
fn search_config() -> BenchmarkConfig {
    serde_json::from_str(include_str!(
        "../examples/configs/benchmark-search-scripted.json"
    ))
    .unwrap()
}
fn base() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/configs")
}
async fn drain(worker: &mut act0r::Worker) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline, "benchmark did not reach idle");
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

#[tokio::test]
async fn five_cases_compare_equal_caps_with_hidden_tests_and_saved_statistics() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("pilot.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let id = agents
        .benchmarks()
        .unwrap()
        .start(config(), base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.advance_work().await.unwrap());
    assert!(worker.advance_work().await.unwrap());
    let initial = agents.benchmarks().unwrap().status(id).await.unwrap();
    let first = agents
        .tasks()
        .unwrap()
        .status(initial.record().trials[0].task_id)
        .await
        .unwrap();
    let workspace = Path::new(&first.record().candidates[0].workspace);
    assert!(workspace.join("tests/feedback.rs").exists());
    assert!(!workspace.join("tests/held_out.rs").exists());
    let request = engine
        .status(first.record().candidates[0].run_id)
        .await
        .unwrap();
    let model_input = request.activities()[0].spec.input.to_string();
    assert!(!model_input.contains("held_out.rs"));
    assert!(!model_input.contains("final_tests_source"));
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().status, BenchmarkStatus::Completed);
    assert_eq!(job.record().trials.len(), 10);
    for arm in &job.record().summary.arms {
        assert_eq!(arm.completed, 5);
        assert_eq!(arm.successes, 5);
        assert_eq!(arm.success_rate, Some(1.0));
        assert_eq!(arm.model_calls, 15 * u64::from(arm.candidates));
        assert_eq!(arm.attempts, 30 * u64::from(arm.candidates));
        assert_eq!(arm.charged_tokens, 300 * u64::from(arm.candidates));
        assert_eq!(arm.held_tokens, 0);
    }
    for trial in &job.record().trials {
        assert_eq!(trial.status, TrialStatus::Completed);
        let task = agents.tasks().unwrap().status(trial.task_id).await.unwrap();
        assert_eq!(
            task.record().config.agent.max_model_calls,
            Some(job.record().config.budget.max_model_calls)
        );
        assert_eq!(
            task.record().config.agent.max_tokens,
            Some(job.record().config.budget.max_tokens)
        );
        let metrics = trial.metrics.as_ref().unwrap();
        assert!(metrics.success);
        assert_eq!(metrics.attempts, task.record().attempts);
        let evaluation = metrics
            .selected
            .as_ref()
            .unwrap()
            .evaluation
            .as_ref()
            .unwrap();
        assert_eq!(
            evaluation.final_tests_digest.as_ref(),
            Some(
                &job.record().inputs[trial.case_index as usize]
                    .final_tests
                    .digest
            )
        );
        assert!(
            evaluation.tests["stdout"]
                .as_str()
                .unwrap()
                .contains("held_out")
        );
        assert!(!evaluation.diff.is_empty());
        for candidate in &task.record().candidates {
            assert!(
                !Path::new(&candidate.workspace)
                    .join("tests/held_out.rs")
                    .exists()
            );
        }
    }
    let saved = serde_json::to_value(job.record()).unwrap();
    drop(worker);
    drop(engine);
    drop(agents);
    let (engine, agents) = local_app(&db).await.unwrap();
    assert_eq!(
        serde_json::to_value(
            agents
                .benchmarks()
                .unwrap()
                .status(id)
                .await
                .unwrap()
                .record()
        )
        .unwrap(),
        saved
    );
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.has_active_runs().await.unwrap());
    assert!(!worker.step().await.unwrap());
}

#[tokio::test]
async fn passing_feedback_does_not_count_as_success_when_hidden_cases_fail() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("held-out.db"))
        .await
        .unwrap();
    let mut cfg = config();
    cfg.cases.retain(|c| c.name == "palindrome");
    cfg.cases[0].script = Some(vec![crate_action_tests(), crate_action_finish()]);
    let id = agents
        .benchmarks()
        .unwrap()
        .start(cfg, base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().status, BenchmarkStatus::Completed);
    for arm in &job.record().summary.arms {
        assert_eq!(arm.completed, 1);
        assert_eq!(arm.successes, 0);
        assert_eq!(arm.success_rate, Some(0.0));
    }
    for trial in &job.record().trials {
        assert!(!trial.metrics.as_ref().unwrap().success);
        assert!(trial.metrics.as_ref().unwrap().selected.is_none());
        let task = agents.tasks().unwrap().status(trial.task_id).await.unwrap();
        for candidate in &task.record().candidates {
            let evaluation = candidate.evaluation.as_ref().unwrap();
            assert!(!evaluation.passed);
            assert_eq!(evaluation.tests["ok"], false);
            let run = engine.status(candidate.run_id).await.unwrap();
            assert!(run.activities().iter().any(|a| {
                a.spec.handler.name == "agent.tool"
                    && a.result
                        .as_ref()
                        .is_some_and(|r| r["ok"] == true && r["exit_code"] == 0)
            }));
        }
    }
}
fn crate_action_tests() -> act0r::domain::agent::AgentAction {
    act0r::domain::agent::AgentAction::RunTests
}
fn crate_action_finish() -> act0r::domain::agent::AgentAction {
    act0r::domain::agent::AgentAction::Finish {
        summary: "Feedback passed.".into(),
    }
}

#[tokio::test]
async fn trial_activation_and_final_report_roll_back_with_the_task_graph() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("atomic.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = config();
    cfg.cases.truncate(1);
    let id = agents
        .benchmarks()
        .unwrap()
        .start(cfg, base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    worker.advance_work().await.unwrap();
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    let trial = job.record().trials[0].clone();
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
    let reject = "CREATE TRIGGER reject_benchmark BEFORE UPDATE ON benchmark_jobs BEGIN SELECT RAISE(ABORT,'reject benchmark update'); END";
    sqlx::query(reject).execute(&pool).await.unwrap();
    assert!(worker.advance_work().await.is_err());
    assert_eq!(
        agents
            .benchmarks()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .trials[0]
            .status,
        TrialStatus::Preparing
    );
    assert!(matches!(
        agents.tasks().unwrap().status(trial.task_id).await,
        Err(act0r::Error::TaskNotFound(_))
    ));
    for query in [
        "SELECT COUNT(*) FROM runs",
        "SELECT COUNT(*) FROM benchmark_trials",
        "SELECT COUNT(*) FROM task_candidates",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(query)
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }
    sqlx::query("DROP TRIGGER reject_benchmark")
        .execute(&pool)
        .await
        .unwrap();
    assert!(worker.advance_work().await.unwrap());
    let task = agents.tasks().unwrap().status(trial.task_id).await.unwrap();
    assert_eq!(task.record().candidates[0].run_id, trial.run_ids[0]);
    loop {
        let execution = worker.admit().await.unwrap().unwrap();
        let outcome = worker.execute(execution.clone()).await.unwrap();
        if execution.handler.name == "coding.evaluate" {
            sqlx::query(reject).execute(&pool).await.unwrap();
            assert!(
                worker
                    .complete(execution.clone(), outcome.clone())
                    .await
                    .is_err()
            );
            assert_eq!(
                engine
                    .status(execution.run_id)
                    .await
                    .unwrap()
                    .activities()
                    .last()
                    .unwrap()
                    .status,
                ActivityStatus::Running
            );
            assert_eq!(
                agents
                    .tasks()
                    .unwrap()
                    .status(trial.task_id)
                    .await
                    .unwrap()
                    .record()
                    .status,
                act0r::domain::task::TaskStatus::Running
            );
            let job = agents.benchmarks().unwrap().status(id).await.unwrap();
            assert!(job.record().trials[0].metrics.is_none());
            assert_eq!(job.record().summary.arms[0].completed, 0);
            sqlx::query("DROP TRIGGER reject_benchmark")
                .execute(&pool)
                .await
                .unwrap();
            worker
                .complete(execution.clone(), outcome.clone())
                .await
                .unwrap();
            let saved = serde_json::to_value(
                agents
                    .benchmarks()
                    .unwrap()
                    .status(id)
                    .await
                    .unwrap()
                    .record(),
            )
            .unwrap();
            worker.complete(execution, outcome).await.unwrap();
            assert_eq!(
                serde_json::to_value(
                    agents
                        .benchmarks()
                        .unwrap()
                        .status(id)
                        .await
                        .unwrap()
                        .record()
                )
                .unwrap(),
                saved
            );
            assert_eq!(saved["summary"]["arms"][0]["completed"], 1);
            break;
        }
        worker.complete(execution, outcome).await.unwrap();
    }
    pool.close().await;
}

#[tokio::test]
async fn waiting_trials_stop_until_operator_input_then_resume_the_remaining_plan() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("waiting.db"))
        .await
        .unwrap();
    let mut cfg = config();
    cfg.cases.truncate(1);
    cfg.agent.require_approval = true;
    let id = agents
        .benchmarks()
        .unwrap()
        .start(cfg, base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().status, BenchmarkStatus::Waiting);
    assert_eq!(job.record().trials[0].status, TrialStatus::Waiting);
    assert_eq!(job.record().trials[1].status, TrialStatus::Planned);
    assert!(!worker.step().await.unwrap());
    let first = agents
        .tasks()
        .unwrap()
        .status(job.record().trials[0].task_id)
        .await
        .unwrap();
    engine
        .cancel(first.record().candidates[0].run_id)
        .await
        .unwrap();
    assert_eq!(
        agents
            .benchmarks()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .trials[0]
            .status,
        TrialStatus::Completed
    );
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().trials[1].status, TrialStatus::Waiting);
    assert_eq!(job.record().summary.arms[0].successes, 0);
}

#[tokio::test]
async fn a_killed_preparing_trial_reuses_frozen_inputs_and_all_reserved_identities() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("crash.db");
    let marker = directory.path().join("marker.json");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = config();
    cfg.cases.truncate(1);
    cfg.repetitions = 2;
    let project = directory.path().join("source");
    for file in [
        "Cargo.toml",
        "Cargo.lock",
        "src/lib.rs",
        "tests/feedback.rs",
    ] {
        let target = project.join(file);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(base().join(&cfg.cases[0].source).join(file), target).unwrap();
    }
    cfg.cases[0].source = project.to_string_lossy().into_owned();
    let id = agents
        .benchmarks()
        .unwrap()
        .start(cfg, base())
        .await
        .unwrap();
    fs::write(
        project.join("src/lib.rs"),
        "pub fn add(_: i32, _: i32) -> i32 { 0 }\n",
    )
    .unwrap();
    let planned = serde_json::to_value(
        agents
            .benchmarks()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .trials
            .clone(),
    )
    .unwrap();
    drop(engine);
    drop(agents);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "benchmark_crash_child",
            "--ignored",
            "--nocapture",
        ])
        .env("ACT0R_BENCHMARK_CRASH_DB", &db)
        .env("ACT0R_BENCHMARK_CRASH_MARKER", &marker)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if fs::read(&marker).is_ok_and(|bytes| serde_json::from_slice::<Value>(&bytes).is_ok()) {
            break;
        }
        assert!(child.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let (engine, agents) = local_app(&db).await.unwrap();
    let resumed = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(resumed.record().trials[0].status, TrialStatus::Preparing);
    assert_eq!(
        resumed
            .record()
            .trials
            .iter()
            .map(|t| t.candidates)
            .collect::<Vec<_>>(),
        vec![1, 3, 3, 1]
    );
    let original_started = resumed.record().trials[0].started_at;
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().status, BenchmarkStatus::Completed);
    assert_eq!(job.record().trials[0].started_at, original_started);
    for (index, trial) in job.record().trials.iter().enumerate() {
        assert_eq!(json!(trial.task_id), planned[index]["task_id"]);
        assert_eq!(json!(trial.run_ids), planned[index]["run_ids"]);
        assert_eq!(
            trial.metrics.as_ref().unwrap().attempts,
            6 * trial.candidates
        );
        let task = agents.tasks().unwrap().status(trial.task_id).await.unwrap();
        assert_eq!(
            task.record().snapshot.digest,
            job.record().inputs[0].baseline.digest
        );
    }
    assert_eq!(job.record().summary.arms[0].completed, 2);
    assert_eq!(job.record().summary.arms[1].completed, 2);
}

#[tokio::test]
async fn invalid_or_overlapping_final_inputs_are_rejected_and_corrupt_baselines_fail_trials() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("invalid.db"))
        .await
        .unwrap();
    let mut cfg = config();
    cfg.cases.truncate(1);
    cfg.cases[0].final_tests_source = cfg.cases[0].source.clone();
    assert!(
        agents
            .benchmarks()
            .unwrap()
            .start(cfg, base())
            .await
            .is_err()
    );
    let mut cfg = config();
    cfg.cases.truncate(1);
    let id = agents
        .benchmarks()
        .unwrap()
        .start(cfg, base())
        .await
        .unwrap();
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    fs::write(
        Path::new(&job.record().inputs[0].baseline.workspace).join("src/lib.rs"),
        "corrupted",
    )
    .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().status, BenchmarkStatus::Completed);
    for trial in &job.record().trials {
        let metrics = trial.metrics.as_ref().unwrap();
        assert_eq!(metrics.attempts, 0);
        assert!(!metrics.success);
        assert!(metrics.error.as_ref().unwrap().contains("digest"));
    }
    let mut cfg = config();
    cfg.repetitions = 0;
    assert!(cfg.validate().is_err());
    cfg.repetitions = 20;
    assert!(cfg.validate().is_ok());
    cfg.cases.push(cfg.cases[0].clone());
    assert!(cfg.validate().is_err());
}

#[tokio::test]
#[ignore = "subprocess benchmark preparation fixture"]
async fn benchmark_crash_child() {
    let db = PathBuf::from(std::env::var("ACT0R_BENCHMARK_CRASH_DB").unwrap());
    let marker = PathBuf::from(std::env::var("ACT0R_BENCHMARK_CRASH_MARKER").unwrap());
    let (engine, _) = local_app(db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.advance_work().await.unwrap());
    fs::write(marker, b"{\"preparing\":true}").unwrap();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::test]
async fn search_arm_uses_equal_caps_and_freezes_proposals_scores_and_verified_metrics_once() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("three-arms.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let id = agents
        .benchmarks()
        .unwrap()
        .start(search_config(), base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().version, 2);
    assert_eq!(job.record().status, BenchmarkStatus::Completed);
    assert_eq!(job.record().trials.len(), 15);
    assert_eq!(job.record().summary.arms.len(), 3);
    for arm in &job.record().summary.arms {
        assert_eq!(arm.trials, 5);
        assert_eq!(arm.completed, 5);
        assert_eq!(arm.successes, 5);
    }
    let search_arm = &job.record().summary.arms[2];
    assert_eq!(search_arm.strategy, BenchmarkStrategy::BoundedSearch);
    assert_eq!(search_arm.model_calls, 30);
    assert_eq!(search_arm.attempts, 90);
    assert_eq!(search_arm.charged_tokens, 600);
    for trial in &job.record().trials {
        let task = agents.tasks().unwrap().status(trial.task_id).await.unwrap();
        assert_eq!(
            serde_json::to_value(&task.record().config.budget).unwrap(),
            serde_json::to_value(&job.record().config.budget).unwrap()
        );
        let metrics = trial.metrics.as_ref().unwrap();
        assert_eq!(
            serde_json::to_value(&metrics.usage).unwrap(),
            serde_json::to_value(&task.record().usage).unwrap()
        );
        if trial.strategy == BenchmarkStrategy::BoundedSearch {
            let search = metrics.search.as_ref().unwrap();
            assert_eq!(Some(search.id), trial.search_id);
            assert!(search.verified);
            assert_eq!(search.phase, act0r::domain::search::SearchPhase::Completed);
            assert_eq!(search.nodes.len(), 3);
            assert!(search.nodes.iter().all(|n| n.visits == 1));
            assert_eq!(metrics.usage.admitted_model_calls, 6);
            assert_eq!(task.record().auxiliary_runs, vec![search.proposal_run_id]);
            assert_eq!(
                engine
                    .status(search.proposal_run_id)
                    .await
                    .unwrap()
                    .record()
                    .usage
                    .admitted_model_calls,
                1
            );
            let mut verifications = 0;
            for candidate in &task.record().candidates {
                assert!(
                    !Path::new(&candidate.workspace)
                        .join("tests/held_out.rs")
                        .exists()
                );
                let run = engine.status(candidate.run_id).await.unwrap();
                verifications += run
                    .activities()
                    .iter()
                    .filter(|a| a.spec.handler.name == "coding.evaluate")
                    .count();
            }
            assert_eq!(verifications, 1);
        } else {
            assert!(metrics.search.is_none());
        }
    }
    let markdown = act0r::adapters::benchmark_report::markdown(job.record());
    assert!(markdown.contains("bounded search"));
    assert!(markdown.contains("Proposal calls"));
    assert!(markdown.contains("New charged tokens"));
    let saved = serde_json::to_value(job.record()).unwrap();
    drop(worker);
    drop(engine);
    drop(agents);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.step().await.unwrap());
    assert_eq!(
        serde_json::to_value(
            agents
                .benchmarks()
                .unwrap()
                .status(id)
                .await
                .unwrap()
                .record()
        )
        .unwrap(),
        saved
    );
}

#[tokio::test]
async fn search_trial_activation_and_final_metrics_are_atomic_with_the_search_graph() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("search-atomic.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = search_config();
    cfg.cases.truncate(1);
    let id = agents
        .benchmarks()
        .unwrap()
        .start(cfg, base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    loop {
        let job = agents.benchmarks().unwrap().status(id).await.unwrap();
        if job.record().trials[2].status == TrialStatus::Preparing {
            break;
        }
        assert!(worker.step().await.unwrap());
    }
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&db)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER reject_search_trial BEFORE UPDATE ON benchmark_jobs BEGIN SELECT RAISE(FAIL, 'reject search trial'); END").execute(&pool).await.unwrap();
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    let trial = job.record().trials[2].clone();
    assert!(worker.advance_work().await.is_err());
    assert!(matches!(
        agents.tasks().unwrap().status(trial.task_id).await,
        Err(act0r::Error::TaskNotFound(_))
    ));
    assert!(matches!(
        agents
            .search()
            .unwrap()
            .status(trial.search_id.unwrap())
            .await,
        Err(act0r::Error::SearchNotFound(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM benchmark_trials")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    sqlx::query("DROP TRIGGER reject_search_trial")
        .execute(&pool)
        .await
        .unwrap();
    assert!(worker.advance_work().await.unwrap());
    loop {
        let search = agents
            .search()
            .unwrap()
            .status(trial.search_id.unwrap())
            .await
            .unwrap();
        if search.record().phase == act0r::domain::search::SearchPhase::Verifying {
            let run = engine
                .status(search.record().selected.unwrap())
                .await
                .unwrap();
            if !run.record().status.is_unfinished() {
                break;
            }
        }
        assert!(worker.step().await.unwrap());
    }
    // Final evaluation is saved, but the controller has not frozen its receipt yet.
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert!(job.record().trials[2].metrics.is_none());
    assert_eq!(job.record().summary.arms[2].completed, 0);
    sqlx::query("CREATE TRIGGER reject_search_trial BEFORE UPDATE ON benchmark_jobs BEGIN SELECT RAISE(FAIL, 'reject search trial'); END").execute(&pool).await.unwrap();
    assert!(worker.advance_work().await.is_err());
    assert_eq!(
        agents
            .search()
            .unwrap()
            .status(trial.search_id.unwrap())
            .await
            .unwrap()
            .record()
            .phase,
        act0r::domain::search::SearchPhase::Verifying
    );
    sqlx::query("DROP TRIGGER reject_search_trial")
        .execute(&pool)
        .await
        .unwrap();
    assert!(worker.advance_work().await.unwrap());
    let saved = serde_json::to_value(
        agents
            .benchmarks()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record(),
    )
    .unwrap();
    assert_eq!(saved["summary"]["arms"][2]["completed"], 1);
    assert!(!worker.advance_work().await.unwrap());
    assert_eq!(
        serde_json::to_value(
            agents
                .benchmarks()
                .unwrap()
                .status(id)
                .await
                .unwrap()
                .record()
        )
        .unwrap(),
        saved
    );
    pool.close().await;
}

#[tokio::test]
async fn search_feedback_false_positives_and_proposal_budget_exhaustion_count_as_failed_trials() {
    for (cap, attempt_cap) in [(2, 60), (24, 60), (24, 1)] {
        let directory = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(directory.path().join("failed-search.db"))
            .await
            .unwrap();
        let mut cfg = search_config();
        cfg.cases.retain(|c| c.name == "palindrome");
        cfg.budget.max_model_calls = cap;
        cfg.budget.max_attempts = attempt_cap;
        if cap == 24 {
            // Every proposal preserves the bug that weak feedback misses.
            let actions = &mut cfg.cases[0].scripted_proposals;
            for (index, action) in actions.iter_mut().enumerate() {
                if let act0r::domain::agent::AgentAction::ReplaceText { new, old, .. } = action {
                    *new = format!("{old} /* proposal {index} */");
                }
            }
        }
        let id = agents
            .benchmarks()
            .unwrap()
            .start(cfg, base())
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let job = agents.benchmarks().unwrap().status(id).await.unwrap();
        let metrics = job.record().trials[2].metrics.as_ref().unwrap();
        assert!(!metrics.success);
        assert_eq!(job.record().summary.arms[2].successes, 0);
        let search = metrics.search.as_ref().unwrap();
        assert!(!search.verified);
        if attempt_cap == 1 {
            assert_eq!(metrics.attempts, 1);
            assert_eq!(search.phase, act0r::domain::search::SearchPhase::Failed);
            assert!(
                metrics
                    .failures
                    .iter()
                    .any(|f| f.failure.code.contains("budget"))
            );
            assert!(act0r::adapters::benchmark_report::markdown(job.record()).contains("budget"));
        } else if cap == 2 {
            assert_eq!(metrics.usage.admitted_model_calls, 2);
            assert_eq!(search.phase, act0r::domain::search::SearchPhase::Failed);
            assert!(search.nodes.is_empty());
        } else {
            assert!(search.nodes.iter().all(|n| n.reward == 1));
            assert_eq!(search.phase, act0r::domain::search::SearchPhase::Completed);
        }
    }
}

#[tokio::test]
async fn v2_rotates_three_arms_and_a_killed_search_trial_reuses_the_reserved_plan() {
    for proposing in [false, true] {
        killed_search_trial(proposing).await;
    }
}
async fn killed_search_trial(proposing: bool) {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("search-crash.db");
    let marker = directory.path().join("search-marker.json");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = search_config();
    cfg.cases.truncate(1);
    cfg.repetitions = if proposing { 1 } else { 3 };
    let id = agents
        .benchmarks()
        .unwrap()
        .start(cfg, base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    loop {
        let job = agents.benchmarks().unwrap().status(id).await.unwrap();
        if job.record().trials[2].status == TrialStatus::Preparing {
            break;
        }
        assert!(worker.step().await.unwrap());
    }
    let plan = agents.benchmarks().unwrap().status(id).await.unwrap();
    let trials = plan.record().trials.clone();
    if !proposing {
        assert_eq!(
            trials
                .iter()
                .map(|t| (t.strategy, t.candidates))
                .collect::<Vec<_>>(),
            vec![
                (BenchmarkStrategy::Independent, 1),
                (BenchmarkStrategy::Independent, 3),
                (BenchmarkStrategy::BoundedSearch, 1),
                (BenchmarkStrategy::Independent, 3),
                (BenchmarkStrategy::BoundedSearch, 1),
                (BenchmarkStrategy::Independent, 1),
                (BenchmarkStrategy::BoundedSearch, 1),
                (BenchmarkStrategy::Independent, 1),
                (BenchmarkStrategy::Independent, 3),
            ]
        );
    }
    drop(worker);
    drop(engine);
    drop(agents);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "benchmark_search_crash_child",
            "--ignored",
            "--nocapture",
        ])
        .env("ACT0R_BENCHMARK_CRASH_DB", &db)
        .env("ACT0R_BENCHMARK_CRASH_MARKER", &marker)
        .env("ACT0R_BENCHMARK_CRASH_JOB", id.to_string())
        .env("ACT0R_BENCHMARK_CRASH_PROPOSING", proposing.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if fs::read(&marker).is_ok_and(|bytes| serde_json::from_slice::<Value>(&bytes).is_ok()) {
            break;
        }
        assert!(child.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let receipt: Value = serde_json::from_slice(&fs::read(&marker).unwrap()).unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().status, BenchmarkStatus::Completed);
    for (trial, planned) in job.record().trials.iter().zip(trials) {
        assert_eq!(trial.task_id, planned.task_id);
        assert_eq!(trial.run_ids, planned.run_ids);
        assert_eq!(trial.search_id, planned.search_id);
    }
    let search = job.record().trials[2]
        .metrics
        .as_ref()
        .unwrap()
        .search
        .as_ref()
        .unwrap();
    assert_eq!(json!(search.checkpoint_id), receipt["checkpoint_id"]);
    assert_eq!(json!(search.proposal_run_id), receipt["proposal_run_id"]);
    assert_eq!(json!(search.fork_id), receipt["fork_id"]);
    assert_eq!(
        job.record().summary.arms[2].completed,
        if proposing { 1 } else { 3 }
    );
    let usage = &job.record().trials[2].metrics.as_ref().unwrap().usage;
    assert_eq!(usage.admitted_model_calls, if proposing { 7 } else { 6 });
    assert_eq!(usage.held_tokens > 0, proposing);
}

#[tokio::test]
#[ignore = "subprocess benchmark search activation fixture"]
async fn benchmark_search_crash_child() {
    let db = PathBuf::from(std::env::var("ACT0R_BENCHMARK_CRASH_DB").unwrap());
    let marker = PathBuf::from(std::env::var("ACT0R_BENCHMARK_CRASH_MARKER").unwrap());
    let (engine, agents) = local_app(db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.advance_work().await.unwrap());
    let id = std::env::var("ACT0R_BENCHMARK_CRASH_JOB")
        .unwrap()
        .parse()
        .unwrap();
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    let search_id = job.record().trials[2].search_id.unwrap();
    if std::env::var("ACT0R_BENCHMARK_CRASH_PROPOSING").unwrap() == "true" {
        loop {
            let search = agents.search().unwrap().status(search_id).await.unwrap();
            if search.record().phase == act0r::domain::search::SearchPhase::Proposing {
                break;
            }
            assert!(worker.step().await.unwrap());
        }
        let execution = worker.admit().await.unwrap().unwrap();
        assert_eq!(execution.handler.name, "search.propose");
    }
    let search = agents.search().unwrap().status(search_id).await.unwrap();
    fs::write(marker, serde_json::to_vec(search.record()).unwrap()).unwrap();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[test]
fn benchmark_versions_reject_mixed_search_contracts_and_enforce_the_total_trial_limit() {
    let mut cfg = search_config();
    cfg.version = 1;
    assert!(cfg.validate().is_err());
    let mut cfg = config();
    cfg.version = 2;
    assert!(cfg.validate().is_err());
    let mut cfg = search_config();
    cfg.search.as_mut().unwrap().width = 5;
    assert!(cfg.validate().is_err());
    let mut cfg = search_config();
    cfg.repetitions = 14;
    assert!(cfg.validate().is_err());
    cfg.repetitions = 13;
    assert!(cfg.validate().is_ok());
    let v1: Value = serde_json::to_value(config()).unwrap();
    assert!(v1.get("search").is_none());
}
