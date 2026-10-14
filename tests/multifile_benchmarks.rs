use act0r::{
    Worker,
    bootstrap::local_app,
    domain::{benchmark::*, search::SearchPhase, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn config() -> BenchmarkConfig {
    serde_json::from_str(include_str!(
        "../examples/configs/benchmark-multifile-scripted.json"
    ))
    .unwrap()
}
fn base() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/configs")
}
async fn drain(worker: &mut Worker) {
    let deadline = Instant::now() + Duration::from_secs(600);
    while worker.has_active_runs().await.unwrap() {
        assert!(
            Instant::now() < deadline,
            "multi-file benchmark did not finish"
        );
        if !worker.step().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

#[tokio::test]
async fn repeated_multifile_trials_expand_failed_feedback_and_verify_two_file_repairs() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("multifile.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let id = agents
        .benchmarks()
        .unwrap()
        .start(config(), base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    assert_eq!(job.record().status, BenchmarkStatus::Completed);
    assert_eq!(job.record().trials.len(), 27);
    for case in &job.record().summary.cases {
        for arm in &case.arms {
            assert_eq!(arm.completed, 3);
            assert_eq!(arm.successes, 3);
            assert_eq!(arm.success_rate, Some(1.0));
            let calls = if arm.strategy == BenchmarkStrategy::BoundedSearch {
                10
            } else {
                7 * arm.candidates
            };
            assert_eq!(arm.model_calls, u64::from(calls * 3));
            assert_eq!(arm.charged_tokens, arm.model_calls * 20);
            assert_eq!(arm.held_tokens, 0);
        }
    }
    for trial in &job.record().trials {
        let metrics = trial.metrics.as_ref().unwrap();
        assert!(metrics.success);
        let evaluation = metrics
            .selected
            .as_ref()
            .unwrap()
            .evaluation
            .as_ref()
            .unwrap();
        assert_eq!(
            evaluation.diff.len(),
            2,
            "each task needs two module repairs"
        );
        assert!(
            evaluation
                .diff
                .iter()
                .all(|file| file.path.starts_with("src/"))
        );
        assert!(
            evaluation.tests["stdout"]
                .as_str()
                .unwrap()
                .contains("held_out")
        );
        let task = agents.tasks().unwrap().status(trial.task_id).await.unwrap();
        assert_eq!(task.record().config.budget.max_model_calls, 24);
        assert_eq!(task.record().config.budget.max_attempts, 80);
        assert_eq!(
            metrics.usage.admitted_model_calls,
            task.record().usage.admitted_model_calls
        );
        for candidate in &task.record().candidates {
            assert!(
                !Path::new(&candidate.workspace)
                    .join("tests/held_out.rs")
                    .exists()
            );
            let run = engine.status(candidate.run_id).await.unwrap();
            for activity in run
                .activities()
                .iter()
                .filter(|a| a.spec.handler.name == "agent.model")
            {
                assert!(!activity.spec.input.to_string().contains("held_out"));
            }
        }
        if let Some(search) = &metrics.search {
            assert_eq!(search.nodes.len(), 3);
            assert_eq!(
                search.nodes.iter().map(|n| n.reward).collect::<Vec<_>>(),
                vec![0, 1, 0]
            );
            assert!(search.nodes.iter().all(|n| n.visits == 1));
            assert_eq!(search.selected, Some(search.nodes[1].run_id));
            let checkpoint = agents
                .branches()
                .unwrap()
                .checkpoint(search.checkpoint_id)
                .await
                .unwrap();
            assert!(!checkpoint.record().seed.agent.tests_passed);
            assert_eq!(
                checkpoint.record().seed.origin.usage.admitted_model_calls,
                3
            );
            assert_eq!(
                checkpoint
                    .record()
                    .seed
                    .agent
                    .observations
                    .last()
                    .unwrap()
                    .result["ok"],
                false
            );
            assert_eq!(
                checkpoint.record().snapshot.as_ref().unwrap().digest,
                task.record().snapshot.digest
            );
            let proposal_run = engine.status(search.proposal_run_id).await.unwrap();
            assert!(
                !proposal_run.activities()[0]
                    .spec
                    .input
                    .to_string()
                    .contains("held_out")
            );
            assert_eq!(metrics.usage.admitted_model_calls, 10);
            assert_eq!(metrics.attempts, 26);
        } else {
            assert_eq!(metrics.attempts, 14 * trial.candidates);
        }
    }
    let markdown = act0r::adapters::benchmark_report::markdown(job.record());
    assert!(
        markdown
            .contains("| Case | Strategy | Completed | Successes | Success rate | Model calls |")
    );
    assert!(
        markdown
            .contains("| retry-schedule | bounded search | 3/3 | 3 | 100.0% | 30 | 600 | 0 | 78 |")
    );
    let saved = serde_json::to_value(job.record()).unwrap();
    drop(worker);
    drop(engine);
    drop(agents);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.step().await.unwrap());
    assert_eq!(
        saved,
        serde_json::to_value(
            agents
                .benchmarks()
                .unwrap()
                .status(id)
                .await
                .unwrap()
                .record()
        )
        .unwrap()
    );
}

#[tokio::test]
async fn shared_caps_stop_partial_multifile_repairs_without_a_verified_result() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("capped.db")).await.unwrap();
    let mut cfg = config();
    cfg.cases.truncate(1);
    cfg.repetitions = 1;
    cfg.budget.max_model_calls = 4;
    let id = agents
        .benchmarks()
        .unwrap()
        .start(cfg, base())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    for trial in &job.record().trials {
        let metrics = trial.metrics.as_ref().unwrap();
        assert!(!metrics.success);
        assert!(metrics.selected.is_none());
        assert_eq!(metrics.usage.admitted_model_calls, 4);
        assert_eq!(metrics.usage.held_tokens, 0);
        assert!(
            metrics
                .failures
                .iter()
                .any(|f| f.failure.code.contains("budget"))
        );
        if let Some(search) = &metrics.search {
            assert!(!search.verified);
            assert!(search.nodes.iter().all(|n| n.reward == 0 && n.visits == 1));
        }
    }
}

#[tokio::test]
#[ignore = "subprocess multi-file edit and score recovery fixture"]
async fn multifile_search_crash_child() {
    let db = std::env::var("ACT0R_MULTIFILE_CRASH_DB").unwrap();
    let marker = std::env::var("ACT0R_MULTIFILE_CRASH_MARKER").unwrap();
    let boundary = std::env::var("ACT0R_MULTIFILE_CRASH_BOUNDARY").unwrap();
    let id: BenchmarkId = std::env::var("ACT0R_MULTIFILE_CRASH_JOB")
        .unwrap()
        .parse()
        .unwrap();
    let (engine, agents) = local_app(db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    let job = agents.benchmarks().unwrap().status(id).await.unwrap();
    let search_id = job.record().trials[2].search_id.unwrap();
    loop {
        if let Ok(search) = agents.search().unwrap().status(search_id).await
            && search.record().phase == SearchPhase::RollingOut
        {
            break;
        }
        assert!(worker.step().await.unwrap());
    }
    let interrupted = if boundary == "edited_before_completion" {
        loop {
            if worker.advance_work().await.unwrap() || worker.fire_due_timer().await.unwrap() {
                continue;
            }
            let Some(execution) = worker.admit().await.unwrap() else {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            let outcome = worker.execute(execution.clone()).await.unwrap();
            if execution.handler.name == "agent.tool"
                && execution.input["action"]["kind"] == "replace_text"
            {
                assert_eq!(outcome.result().unwrap()["ok"], true);
                break Some(json!({"activity_id":execution.activity_id,"run_id":execution.run_id}));
            }
            worker.complete(execution, outcome).await.unwrap();
        }
    } else {
        loop {
            let search = agents.search().unwrap().status(search_id).await.unwrap();
            if search.record().nodes[0].result.is_some() {
                break;
            }
            assert!(worker.step().await.unwrap());
        }
        None
    };
    let search = agents.search().unwrap().status(search_id).await.unwrap();
    fs::write(
        marker,
        serde_json::to_vec(&json!({"search":search.record(),"interrupted":interrupted})).unwrap(),
    )
    .unwrap();
    loop {
        std::thread::park();
    }
}

#[tokio::test]
async fn killed_multifile_workers_reuse_partial_edits_and_frozen_scores() {
    for boundary in ["edited_before_completion", "score_committed"] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("crash.db");
        let marker = directory.path().join("marker.json");
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut cfg = config();
        cfg.cases.retain(|c| c.name == "path-routing");
        cfg.repetitions = 1;
        let id = agents
            .benchmarks()
            .unwrap()
            .start(cfg, base())
            .await
            .unwrap();
        drop(engine);
        drop(agents);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "multifile_search_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("ACT0R_MULTIFILE_CRASH_DB", &db)
            .env("ACT0R_MULTIFILE_CRASH_MARKER", &marker)
            .env("ACT0R_MULTIFILE_CRASH_JOB", id.to_string())
            .env("ACT0R_MULTIFILE_CRASH_BOUNDARY", boundary)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(90);
        let receipt: Value = loop {
            if let Ok(bytes) = fs::read(&marker)
                && let Ok(receipt) = serde_json::from_slice(&bytes)
            {
                break receipt;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before {boundary}"
            );
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("child timed out before {boundary}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        child.kill().unwrap();
        child.wait().unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let job = agents.benchmarks().unwrap().status(id).await.unwrap();
        let metrics = job.record().trials[2].metrics.as_ref().unwrap();
        assert!(metrics.success);
        assert_eq!(metrics.usage.admitted_model_calls, 10);
        assert_eq!(metrics.usage.held_tokens, 0);
        assert_eq!(
            metrics.attempts,
            if boundary == "edited_before_completion" {
                27
            } else {
                26
            }
        );
        let search = metrics.search.as_ref().unwrap();
        assert!(search.nodes.iter().all(|n| n.visits == 1));
        assert_eq!(
            json!(search.checkpoint_id),
            receipt["search"]["checkpoint_id"]
        );
        assert_eq!(json!(search.fork_id), receipt["search"]["fork_id"]);
        if boundary == "score_committed" {
            assert_eq!(json!(search.nodes[0]), receipt["search"]["nodes"][0]);
        } else {
            let run_id = serde_json::from_value(receipt["interrupted"]["run_id"].clone()).unwrap();
            let run = engine.status(run_id).await.unwrap();
            let activity = run
                .activities()
                .iter()
                .find(|a| json!(a.id) == receipt["interrupted"]["activity_id"])
                .unwrap();
            assert_eq!(activity.attempts, 2);
            assert_eq!(activity.result.as_ref().unwrap()["already_applied"], true);
        }
    }
}
