use act0r::{
    Engine, Worker,
    adapters::sqlite::SqliteStore,
    application::{AgentService, ports::*},
    bootstrap::local_app,
    domain::{benchmark::*, branch::PreparationStatus, search::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn base() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/configs")
}
fn source() -> PathBuf {
    base().join("../benchmark-cases/addition/source")
}
fn config() -> SearchConfig {
    // Feedback must still run after the last permitted model call.
    serde_json::from_str(include_str!("../examples/configs/search-feedback.json")).unwrap()
}
async fn drain(worker: &mut Worker) {
    let deadline = Instant::now() + Duration::from_secs(600);
    while worker.has_active_runs().await.unwrap() {
        assert!(Instant::now() < deadline, "feedback search did not finish");
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
async fn first_boundary(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    id: SearchId,
) -> RunId {
    for _ in 0..200 {
        let search = agents.search().unwrap().status(id).await.unwrap();
        if let Some(node) = search.record().nodes.first()
            && agents
                .branches()
                .unwrap()
                .fork(search.record().fork_id)
                .await
                .unwrap()
                .status
                == PreparationStatus::Ready
        {
            let run = engine.status(node.run_id).await.unwrap();
            if run.record().status == RunStatus::Paused && run.record().state["steps"] == 1 {
                assert!(run.record().state.get("feedback").is_none());
                return node.run_id;
            }
        }
        assert!(worker.step().await.unwrap());
    }
    panic!("no rollout boundary");
}
async fn admit_feedback(worker: &mut Worker) -> ActivityExecution {
    for _ in 0..200 {
        while worker.advance_work().await.unwrap() {}
        let execution = worker.admit().await.unwrap().unwrap();
        if execution.handler == DefinitionId::new("search.feedback", 1) {
            return execution;
        }
        let outcome = worker.execute(execution.clone()).await.unwrap();
        worker.complete(execution, outcome).await.unwrap();
    }
    panic!("no feedback activity");
}
async fn pool(db: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(db)
            .foreign_keys(true),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn stale_edits_get_saved_feedback_without_another_model_call_and_v1_keeps_its_bound() {
    for version in [1, 2] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("feedback.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut config = config();
        config.version = version;
        let id = agents
            .search()
            .unwrap()
            .start(source(), config, &base())
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let search = agents.search().unwrap().status(id).await.unwrap();
        let r = search.record();
        assert_eq!(r.phase, SearchPhase::Completed);
        assert_eq!(r.verified, version == 2);
        assert_eq!(
            r.nodes.iter().map(|n| n.reward).collect::<Vec<_>>(),
            if version == 2 {
                vec![0, 1, 0]
            } else {
                vec![0, 0, 0]
            }
        );
        let task = agents.search().unwrap().task(id).await.unwrap();
        assert_eq!(task.record().usage.admitted_model_calls, 3);
        assert_eq!(task.record().usage.charged_tokens(), 60);
        assert_eq!(task.record().attempts, if version == 2 { 15 } else { 11 });
        let mut final_checks = 0;
        for node in &r.nodes {
            assert_eq!(node.visits, 1);
            let score = node.result.as_ref().unwrap();
            assert_eq!(score.steps, 1);
            assert_eq!(score.model_calls, 0);
            let run = engine.status(node.run_id).await.unwrap();
            assert_eq!(
                run.record().workflow,
                DefinitionId::new("coding.search_candidate", version)
            );
            let checks: Vec<_> = run
                .activities()
                .iter()
                .filter(|a| a.spec.handler.name == "search.feedback")
                .collect();
            final_checks += run
                .activities()
                .iter()
                .filter(|a| a.spec.handler.name == "coding.evaluate")
                .count();
            assert_eq!(checks.len(), usize::from(version == 2));
            if version == 2 {
                let receipt = score.automatic_feedback.as_ref().unwrap();
                assert_eq!(receipt.phase, FeedbackPhase::Completed);
                assert_eq!(receipt.activity_id, Some(checks[0].id));
                assert_eq!(receipt.result.as_ref().unwrap()["ok"], node.reward == 1);
                assert!(checks[0].spec.usage.is_none());
                assert_eq!(checks[0].attempts, 1);
                assert!(
                    !receipt
                        .result
                        .as_ref()
                        .unwrap()
                        .to_string()
                        .contains("held_out")
                );
                assert!(
                    !Path::new(run.record().input["config"]["workspace"].as_str().unwrap())
                        .join("tests/held_out.rs")
                        .exists()
                );
            } else {
                assert!(score.automatic_feedback.is_none());
                assert!(run.record().state.get("feedback_checked").is_none());
                assert_eq!(run.record().input["search"]["version"], 1);
            }
        }
        assert_eq!(final_checks, usize::from(version == 2));
        let saved = json!({"search":r, "task":task.record()});
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert!(!worker.step().await.unwrap());
        assert_eq!(
            saved,
            json!({"search":agents.search().unwrap().status(id).await.unwrap().record(), "task":agents.search().unwrap().task(id).await.unwrap().record()})
        );
    }
}

#[tokio::test]
async fn fresh_agent_feedback_is_reused_and_evaluation_cannot_exceed_the_attempt_cap() {
    for (steps, cap, calls, expected_checks, verified) in
        [(2, 60, 6, 0, true), (1, 14, 3, 3, false)]
    {
        let directory = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(directory.path().join("budget.db")).await.unwrap();
        let mut config = config();
        config.rollout_steps = steps;
        config.task.budget.max_attempts = cap;
        config.task.budget.max_model_calls = calls;
        let id = agents
            .search()
            .unwrap()
            .start(source(), config, &base())
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let search = agents.search().unwrap().status(id).await.unwrap();
        assert_eq!(search.record().verified, verified);
        assert_eq!(
            search
                .record()
                .nodes
                .iter()
                .map(|n| n.reward)
                .collect::<Vec<_>>(),
            vec![0, 1, 0]
        );
        let task = agents.search().unwrap().task(id).await.unwrap();
        assert_eq!(task.record().attempts, if steps == 2 { 18 } else { cap });
        assert_eq!(task.record().usage.admitted_model_calls, calls);
        let mut count = 0;
        for node in &search.record().nodes {
            count += engine
                .status(node.run_id)
                .await
                .unwrap()
                .activities()
                .iter()
                .filter(|a| a.spec.handler.name == "search.feedback")
                .count();
        }
        assert_eq!(count, expected_checks);
        if !verified {
            let winner = engine
                .status(search.record().selected.unwrap())
                .await
                .unwrap();
            assert_eq!(winner.record().status, RunStatus::BudgetExhausted);
            assert!(
                winner
                    .activities()
                    .iter()
                    .filter(|a| a.spec.handler.name == "coding.evaluate")
                    .all(|a| a.attempts == 0)
            );
        }
    }
}

#[tokio::test]
async fn automatic_feedback_false_positives_do_not_expose_held_out_failures_to_search() {
    let directory = tempfile::tempdir().unwrap();
    let copy = directory.path().join("source");
    fs::create_dir_all(copy.join("src")).unwrap();
    fs::create_dir_all(copy.join("tests")).unwrap();
    for name in ["Cargo.toml", "Cargo.lock", "src/lib.rs"] {
        fs::copy(source().join(name), copy.join(name)).unwrap();
    }
    fs::write(
        copy.join("tests/feedback.rs"),
        "#[test] fn feedback() { assert_eq!(bench_addition::add(0,0),0); }\n",
    )
    .unwrap();
    let (engine, agents) = local_app(directory.path().join("weak.db")).await.unwrap();
    let id = agents
        .search()
        .unwrap()
        .start(copy, config(), &base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let search = agents.search().unwrap().status(id).await.unwrap();
    assert!(
        search
            .record()
            .nodes
            .iter()
            .all(|n| n.reward == 1 && n.visits == 1)
    );
    assert_eq!(
        search.record().selected,
        Some(search.record().nodes[0].run_id)
    );
    assert!(!search.record().verified);
    let mut final_checks = 0;
    for node in &search.record().nodes {
        let run = engine.status(node.run_id).await.unwrap();
        final_checks += run
            .activities()
            .iter()
            .filter(|a| a.spec.handler.name == "coding.evaluate")
            .count();
        assert!(
            !node
                .result
                .as_ref()
                .unwrap()
                .automatic_feedback
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .to_string()
                .contains("held_out")
        );
    }
    assert_eq!(final_checks, 1);
}

#[tokio::test]
async fn feedback_approvals_survive_reopen_and_denial_saves_a_zero_reward() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("approval.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut config = config();
    config.task.agent.require_approval = true;
    let id = agents
        .search()
        .unwrap()
        .start(source(), config, &base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    loop {
        drain(&mut worker).await;
        let task = agents.search().unwrap().task(id).await.unwrap();
        let mut approved = false;
        for candidate in &task.record().candidates {
            let run = engine.status(candidate.run_id).await.unwrap();
            if let Some(wait) = &run.record().wait
                && wait.key == "tool_approval"
            {
                engine
                    .signal(candidate.run_id, wait.id, json!({"approved":true}))
                    .await
                    .unwrap();
                approved = true;
            }
        }
        if !approved {
            break;
        }
    }
    let search = agents.search().unwrap().status(id).await.unwrap();
    let mut waits = vec![];
    for node in &search.record().nodes {
        let run = engine.status(node.run_id).await.unwrap();
        let wait = run.record().wait.as_ref().unwrap();
        assert_eq!(wait.key, "search_feedback");
        assert!(
            !run.activities()
                .iter()
                .any(|a| a.spec.handler.name == "search.feedback")
        );
        waits.push((node.run_id, wait.id));
    }
    assert_eq!(waits.len(), 3);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.has_active_runs().await.unwrap());
    for (index, (run, wait)) in waits.into_iter().enumerate() {
        assert_eq!(
            engine
                .status(run)
                .await
                .unwrap()
                .record()
                .wait
                .as_ref()
                .unwrap()
                .id,
            wait
        );
        // Approve one failing branch; deny the passing branch and the other branch.
        engine
            .signal(run, wait, json!({"approved":index == 0}))
            .await
            .unwrap();
    }
    drain(&mut worker).await;
    let search = agents.search().unwrap().status(id).await.unwrap();
    assert!(!search.record().verified);
    for (index, node) in search.record().nodes.iter().enumerate() {
        assert_eq!(node.reward, 0);
        let feedback = node
            .result
            .as_ref()
            .unwrap()
            .automatic_feedback
            .as_ref()
            .unwrap();
        assert_eq!(
            feedback.phase,
            if index == 0 {
                FeedbackPhase::Completed
            } else {
                FeedbackPhase::Denied
            }
        );
    }
    assert_eq!(
        agents
            .search()
            .unwrap()
            .task(id)
            .await
            .unwrap()
            .record()
            .attempts,
        12
    );
}

#[tokio::test]
async fn feedback_intent_completion_and_score_are_atomic_and_idempotent() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("rollback.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let id = agents
        .search()
        .unwrap()
        .start(source(), config(), &base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    let first = first_boundary(&engine, &agents, &mut worker, id).await;
    let connection = pool(&db).await;
    let store = SqliteStore::open(&db).await.unwrap();
    let before = json!({"run":engine.status(first).await.unwrap().record(),"task":agents.search().unwrap().task(id).await.unwrap().record()});
    sqlx::raw_sql("CREATE TRIGGER reject_feedback BEFORE UPDATE ON runs WHEN json_extract(NEW.payload,'$.state.feedback.phase')='scheduled' BEGIN SELECT RAISE(ABORT,'rollback feedback'); END;").execute(&connection).await.unwrap();
    assert!(store.begin_feedback(id, first, 0).await.is_err());
    assert_eq!(
        before,
        json!({"run":engine.status(first).await.unwrap().record(),"task":agents.search().unwrap().task(id).await.unwrap().record()})
    );
    sqlx::raw_sql("DROP TRIGGER reject_feedback;")
        .execute(&connection)
        .await
        .unwrap();
    store.begin_feedback(id, first, 0).await.unwrap();
    let saved = serde_json::to_value(engine.status(first).await.unwrap().record()).unwrap();
    store.begin_feedback(id, first, 1).await.unwrap();
    assert_eq!(
        saved,
        serde_json::to_value(engine.status(first).await.unwrap().record()).unwrap()
    );
    assert!(store.score_rollout(id, first, 0).await.is_err());
    let execution = admit_feedback(&mut worker).await;
    assert_eq!(execution.run_id, first);
    let outcome = worker.execute(execution.clone()).await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_result BEFORE UPDATE ON runs WHEN json_extract(NEW.payload,'$.state.feedback.phase')='completed' BEGIN SELECT RAISE(ABORT,'rollback result'); END;").execute(&connection).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    let run = engine.status(first).await.unwrap();
    assert_eq!(run.record().state["feedback"]["phase"], "scheduled");
    assert!(
        run.activities()
            .iter()
            .any(|a| a.id == execution.activity_id && a.status == ActivityStatus::Running)
    );
    sqlx::raw_sql("DROP TRIGGER reject_result;")
        .execute(&connection)
        .await
        .unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    worker.complete(execution, outcome).await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_score BEFORE UPDATE ON search_jobs BEGIN SELECT RAISE(ABORT,'rollback score'); END;").execute(&connection).await.unwrap();
    assert!(store.score_rollout(id, first, 0).await.is_err());
    assert_eq!(
        agents
            .search()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .nodes[0]
            .visits,
        0
    );
    sqlx::raw_sql("DROP TRIGGER reject_score;")
        .execute(&connection)
        .await
        .unwrap();
    store.score_rollout(id, first, 0).await.unwrap();
    store.score_rollout(id, first, 1).await.unwrap();
    drain(&mut worker).await;
    let search = agents.search().unwrap().status(id).await.unwrap();
    assert!(search.record().verified);
    assert!(search.record().nodes.iter().all(|n| n.visits == 1));
    assert_eq!(
        agents
            .search()
            .unwrap()
            .task(id)
            .await
            .unwrap()
            .record()
            .attempts,
        15
    );
}

#[tokio::test]
async fn pause_and_cancel_during_feedback_preserve_the_planner_boundary() {
    for cancel in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(directory.path().join("control.db"))
            .await
            .unwrap();
        let id = agents
            .search()
            .unwrap()
            .start(source(), config(), &base())
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        let execution = admit_feedback(&mut worker).await;
        let run_id = execution.run_id;
        if cancel {
            engine.cancel(run_id).await.unwrap();
        } else {
            engine.pause(run_id).await.unwrap();
        }
        let outcome = worker.execute(execution.clone()).await.unwrap();
        worker.complete(execution, outcome).await.unwrap();
        if !cancel {
            assert_eq!(
                engine.status(run_id).await.unwrap().record().status,
                RunStatus::Paused
            );
            assert!(
                engine
                    .status(run_id)
                    .await
                    .unwrap()
                    .record()
                    .controls
                    .pause_requested
            );
            // Other branches can finish, but this branch cannot be scored while user-paused.
            drain(&mut worker).await;
            assert!(
                agents
                    .search()
                    .unwrap()
                    .status(id)
                    .await
                    .unwrap()
                    .record()
                    .nodes
                    .iter()
                    .find(|n| n.run_id == run_id)
                    .unwrap()
                    .result
                    .is_none()
            );
            engine.resume(run_id).await.unwrap();
            assert_eq!(
                engine.status(run_id).await.unwrap().record().status,
                RunStatus::Paused
            );
        }
        drain(&mut worker).await;
        let search = agents.search().unwrap().status(id).await.unwrap();
        assert!(search.record().verified);
        assert!(search.record().nodes.iter().all(|n| n.visits == 1));
        if cancel {
            assert_eq!(
                search
                    .record()
                    .nodes
                    .iter()
                    .find(|n| n.run_id == run_id)
                    .unwrap()
                    .reward,
                0
            );
        }
    }
}

#[tokio::test]
async fn multifile_benchmarks_opt_in_to_feedback_at_shorter_rollout_boundaries() {
    let mut config: BenchmarkConfig = serde_json::from_str(include_str!(
        "../examples/configs/benchmark-multifile-scripted.json"
    ))
    .unwrap();
    assert_eq!(config.search.as_ref().unwrap().version, 1);
    assert_eq!(
        serde_json::to_value(&config).unwrap()["search"]["version"],
        1
    );
    config.repetitions = 1;
    config.search.as_mut().unwrap().version = 2;
    config.search.as_mut().unwrap().rollout_steps = 2;
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("multifile.db"))
        .await
        .unwrap();
    let id = agents
        .benchmarks()
        .unwrap()
        .start(config, base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().status, BenchmarkStatus::Completed);
    assert_eq!(job.record().trials.len(), 9);
    let report = act0r::adapters::benchmark_report::markdown(job.record());
    assert!(report.contains("Search v2 checks stale feedback"));
    assert_eq!(report.matches("automatic feedback: Completed").count(), 9);
    for trial in &job.record().trials {
        let metrics = trial.metrics.as_ref().unwrap();
        assert!(metrics.success);
        if let Some(search) = &metrics.search {
            assert_eq!(search.config.version, 2);
            assert_eq!(metrics.usage.admitted_model_calls, 7);
            assert_eq!(metrics.attempts, 23);
            assert_eq!(
                search.nodes.iter().map(|n| n.reward).collect::<Vec<_>>(),
                vec![0, 1, 0]
            );
            assert!(search.nodes.iter().all(|n| {
                n.result
                    .as_ref()
                    .unwrap()
                    .automatic_feedback
                    .as_ref()
                    .unwrap()
                    .phase
                    == FeedbackPhase::Completed
            }));
            assert_eq!(
                metrics
                    .selected
                    .as_ref()
                    .unwrap()
                    .evaluation
                    .as_ref()
                    .unwrap()
                    .diff
                    .len(),
                2
            );
        }
    }
}

#[tokio::test]
#[ignore = "subprocess durable feedback crash fixture"]
async fn feedback_crash_child() {
    let db = std::env::var("FEEDBACK_CRASH_DB").unwrap();
    let id: SearchId = std::env::var("FEEDBACK_CRASH_ID").unwrap().parse().unwrap();
    let marker = std::env::var("FEEDBACK_CRASH_MARKER").unwrap();
    let boundary = std::env::var("FEEDBACK_CRASH_BOUNDARY").unwrap();
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    let first = first_boundary(&engine, &agents, &mut worker, id).await;
    let store = SqliteStore::open(&db).await.unwrap();
    store.begin_feedback(id, first, 0).await.unwrap();
    let activity_id = engine
        .status(first)
        .await
        .unwrap()
        .activities()
        .iter()
        .find(|a| a.spec.handler.name == "search.feedback")
        .unwrap()
        .id;
    let mut receipt = json!({"run_id":first,"activity_id":activity_id});
    if boundary != "intent" {
        let execution = admit_feedback(&mut worker).await;
        assert_eq!(execution.run_id, first);
        let outcome = worker.execute(execution.clone()).await.unwrap();
        receipt["execution"] = serde_json::to_value(&execution).unwrap();
        receipt["result"] = outcome.result().unwrap().clone();
        if matches!(boundary.as_str(), "completed" | "scored") {
            worker.complete(execution, outcome).await.unwrap();
        }
        if boundary == "scored" {
            store.score_rollout(id, first, 0).await.unwrap();
        }
    }
    fs::write(marker, serde_json::to_vec(&receipt).unwrap()).unwrap();
    loop {
        std::thread::park();
    }
}

#[tokio::test]
async fn killed_feedback_workers_reuse_receipts_and_unsafe_tests_require_reconciliation() {
    for boundary in [
        "intent",
        "executed",
        "completed",
        "scored",
        "unsafe_executed",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("crash.db");
        let marker = directory.path().join("marker.json");
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut config = config();
        config.task.agent.repeat_safe_tests = boundary != "unsafe_executed";
        let id = agents
            .search()
            .unwrap()
            .start(source(), config, &base())
            .await
            .unwrap();
        drop(agents);
        drop(engine);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "feedback_crash_child",
                "--nocapture",
            ])
            .env("FEEDBACK_CRASH_DB", &db)
            .env("FEEDBACK_CRASH_ID", id.to_string())
            .env("FEEDBACK_CRASH_MARKER", &marker)
            .env("FEEDBACK_CRASH_BOUNDARY", boundary)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        while !marker.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited at {boundary}"
            );
            if Instant::now() >= deadline {
                child.kill().unwrap();
                panic!("child timed out at {boundary}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let receipt: Value = serde_json::from_slice(&fs::read(&marker).unwrap()).unwrap();
        let run_id: RunId = serde_json::from_value(receipt["run_id"].clone()).unwrap();
        let activity_id: ActivityId =
            serde_json::from_value(receipt["activity_id"].clone()).unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        if boundary == "unsafe_executed" {
            let run = engine.status(run_id).await.unwrap();
            assert_eq!(run.record().status, RunStatus::Blocked);
            assert!(run.activities().iter().any(|a| a.id == activity_id
                && a.status == ActivityStatus::Indeterminate
                && a.attempts == 1));
            drain(&mut worker).await;
            assert!(
                agents
                    .search()
                    .unwrap()
                    .status(id)
                    .await
                    .unwrap()
                    .record()
                    .nodes[0]
                    .result
                    .is_none()
            );
            let execution: ActivityExecution =
                serde_json::from_value(receipt["execution"].clone()).unwrap();
            let decision = ReconciliationRequest {
                activity_id,
                attempt_id: execution.attempt_id,
                decision: ReconciliationDecision::Accept {
                    result: receipt["result"].clone(),
                },
                evidence: "Verified the saved subprocess test output after worker loss".into(),
            };
            engine.reconcile(run_id, decision.clone()).await.unwrap();
            engine.reconcile(run_id, decision).await.unwrap();
        }
        drain(&mut worker).await;
        let search = agents.search().unwrap().status(id).await.unwrap();
        assert!(search.record().verified, "boundary {boundary}");
        assert!(search.record().nodes.iter().all(|n| n.visits == 1));
        let feedback = search.record().nodes[0]
            .result
            .as_ref()
            .unwrap()
            .automatic_feedback
            .as_ref()
            .unwrap();
        assert_eq!(feedback.activity_id, Some(activity_id));
        assert_eq!(feedback.phase, FeedbackPhase::Completed);
        let run = engine.status(run_id).await.unwrap();
        let checks: Vec<_> = run
            .activities()
            .iter()
            .filter(|a| a.spec.handler.name == "search.feedback")
            .collect();
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].attempts,
            if boundary == "executed" { 2 } else { 1 }
        );
        let task = agents.search().unwrap().task(id).await.unwrap();
        assert_eq!(
            task.record().attempts,
            if boundary == "executed" { 16 } else { 15 }
        );
        assert_eq!(task.record().usage.admitted_model_calls, 3);
        assert_eq!(task.record().usage.held_tokens, 0);
    }
}
