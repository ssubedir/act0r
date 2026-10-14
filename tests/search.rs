use act0r::{
    adapters::sqlite::SqliteStore,
    application::ports::*,
    bootstrap::local_app,
    domain::{branch::PreparationStatus, search::*, task::TaskStatus, *},
};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn config() -> SearchConfig {
    serde_json::from_str(include_str!("../examples/configs/search-scripted.json")).unwrap()
}
fn examples() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples")
}
fn source() -> PathBuf {
    examples().join("benchmark-cases/addition/source")
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
async fn pool(db: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(db)
            .foreign_keys(true),
    )
    .await
    .unwrap()
}
async fn phase(
    engine: &act0r::Engine,
    agents: &act0r::application::AgentService,
    worker: &mut act0r::Worker,
    id: SearchId,
    phase: SearchPhase,
) {
    for _ in 0..200 {
        if agents
            .search()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .phase
            == phase
        {
            return;
        }
        assert!(
            worker.step().await.unwrap(),
            "idle before desired phase; root={:?}",
            engine
                .status(
                    agents
                        .search()
                        .unwrap()
                        .status(id)
                        .await
                        .unwrap()
                        .record()
                        .root
                )
                .await
                .unwrap()
                .record()
                .status
        );
    }
    panic!("phase was never reached");
}

#[tokio::test]
async fn search_proposes_distinct_actions_scores_once_and_verifies_only_the_selected_patch() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("search.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let id = agents
        .search()
        .unwrap()
        .start(source(), config(), &examples().join("configs"))
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let search = agents.search().unwrap().status(id).await.unwrap();
    let r = search.record();
    assert_eq!(r.phase, SearchPhase::Completed);
    assert!(r.verified);
    assert_eq!(r.nodes.len(), 3);
    assert_eq!(r.selected, Some(r.nodes[1].run_id));
    assert_eq!(
        r.nodes.iter().map(|n| n.reward).collect::<Vec<_>>(),
        vec![0, 1, 0]
    );
    let task = agents.search().unwrap().task(id).await.unwrap();
    assert_eq!(task.record().status, TaskStatus::Succeeded);
    assert_eq!(task.record().attempts, 18);
    assert_eq!(task.record().usage.admitted_model_calls, 6);
    assert_eq!(task.record().usage.charged_tokens(), 120);
    assert_eq!(task.record().auxiliary_runs, vec![r.proposal_run_id]);
    let proposal = engine.status(r.proposal_run_id).await.unwrap();
    assert_eq!(proposal.attempts().len(), 1);
    assert_eq!(proposal.record().usage.admitted_model_calls, 1);
    assert_eq!(
        proposal.record().result.as_ref().unwrap()["actions"],
        serde_json::to_value(&config().scripted_proposals).unwrap()
    );
    let cp = agents
        .branches()
        .unwrap()
        .checkpoint(r.checkpoint_id)
        .await
        .unwrap();
    assert_eq!(cp.record().status, PreparationStatus::Ready);
    assert_eq!(cp.record().seed.origin.attempts, 4);
    assert_eq!(cp.record().seed.origin.usage.admitted_model_calls, 2);
    let mut verification_count = 0;
    for node in &r.nodes {
        assert_eq!(node.visits, 1);
        assert_eq!(node.result.as_ref().unwrap().steps, 2);
        assert_eq!(node.result.as_ref().unwrap().model_calls, 1);
        let run = engine.status(node.run_id).await.unwrap();
        assert_eq!(run.record().usage.admitted_model_calls, 3);
        assert_eq!(
            run.activities()[0].spec.input["action"],
            serde_json::to_value(&node.action).unwrap()
        );
        assert!(run.activities()[0].spec.usage.is_none());
        verification_count += run
            .activities()
            .iter()
            .filter(|a| a.spec.handler == DefinitionId::new("coding.evaluate", 1))
            .count();
        let workspace = Path::new(run.record().input["config"]["workspace"].as_str().unwrap());
        for file in &task.record().final_tests.as_ref().unwrap().snapshot.files {
            assert!(!workspace.join(&file.path).exists());
        }
    }
    assert_eq!(verification_count, 1);
    let winner = task
        .record()
        .candidates
        .iter()
        .find(|c| Some(c.run_id) == r.selected)
        .unwrap();
    assert!(
        winner.evaluation.as_ref().unwrap().diff[0]
            .patch
            .contains("+    left + right")
    );
    assert!(
        agents
            .branches()
            .unwrap()
            .create_fork(ForkId::new(), r.checkpoint_id, 2)
            .await
            .is_err()
    );
    let saved = json!({"search":r,"task":task.record()});
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.step().await.unwrap());
    assert_eq!(
        saved,
        json!({"search":agents.search().unwrap().status(id).await.unwrap().record(),"task":agents.search().unwrap().task(id).await.unwrap().record()})
    );
}

#[tokio::test]
async fn feedback_false_positives_do_not_leak_final_tests_into_selection() {
    let directory = tempfile::tempdir().unwrap();
    let source_copy = directory.path().join("source");
    fs::create_dir_all(source_copy.join("src")).unwrap();
    fs::create_dir_all(source_copy.join("tests")).unwrap();
    for name in ["Cargo.toml", "Cargo.lock", "src/lib.rs"] {
        fs::copy(source().join(name), source_copy.join(name)).unwrap();
    }
    fs::write(
        source_copy.join("tests/feedback.rs"),
        "#[test] fn feedback() { assert_eq!(bench_addition::add(0,0),0); }\n",
    )
    .unwrap();
    let (engine, agents) = local_app(directory.path().join("false-positive.db"))
        .await
        .unwrap();
    let id = agents
        .search()
        .unwrap()
        .start(source_copy, config(), &examples().join("configs"))
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    drain(&mut worker).await;
    let search = agents.search().unwrap().status(id).await.unwrap();
    let r = search.record();
    assert!(r.nodes.iter().all(|n| n.reward == 1));
    assert_eq!(r.selected, Some(r.nodes[0].run_id));
    assert_eq!(r.phase, SearchPhase::Completed);
    assert!(!r.verified);
    assert_eq!(
        agents
            .search()
            .unwrap()
            .task(id)
            .await
            .unwrap()
            .record()
            .status,
        TaskStatus::NoSolution
    );
    let selected = engine.status(r.selected.unwrap()).await.unwrap();
    assert_eq!(
        selected.record().result.as_ref().unwrap()["evaluation"]["passed"],
        false
    );
    // A passing alternative is still retired: held-out failure is not a new planning signal.
    assert_eq!(
        engine
            .status(r.nodes[1].run_id)
            .await
            .unwrap()
            .record()
            .status,
        RunStatus::Cancelled
    );
}

#[tokio::test]
async fn proposal_generation_and_rollouts_share_admission_limits_and_uncertain_reservations() {
    for max_calls in [2, 4] {
        let directory = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(directory.path().join("limits.db")).await.unwrap();
        let mut config = config();
        config.task.budget.max_model_calls = max_calls;
        let id = agents
            .search()
            .unwrap()
            .start(source(), config, &examples().join("configs"))
            .await
            .unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let search = agents.search().unwrap().status(id).await.unwrap();
        let task = agents.search().unwrap().task(id).await.unwrap();
        assert_eq!(task.record().usage.admitted_model_calls, max_calls);
        assert!(!search.record().verified);
        if max_calls == 2 {
            assert_eq!(search.record().phase, SearchPhase::Failed);
        } else {
            assert_eq!(search.record().phase, SearchPhase::Completed);
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("uncertain.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let id = agents
        .search()
        .unwrap()
        .start(source(), config(), &examples().join("configs"))
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    phase(&engine, &agents, &mut worker, id, SearchPhase::Proposing).await;
    let execution = worker.admit().await.unwrap().unwrap();
    assert_eq!(execution.handler, DefinitionId::new("search.propose", 1));
    let held = engine
        .status(execution.run_id)
        .await
        .unwrap()
        .record()
        .usage
        .held_tokens;
    worker
        .complete(
            execution,
            Outcome::Success(json!({"actions":config().scripted_proposals})),
        )
        .await
        .unwrap();
    drain(&mut worker).await;
    let task = agents.search().unwrap().task(id).await.unwrap();
    assert_eq!(task.record().usage.uncertain_calls, 1);
    assert_eq!(task.record().usage.held_tokens, held);
    assert_eq!(task.record().usage.admitted_model_calls, 6);
}

#[tokio::test]
async fn user_pause_and_approvals_stop_the_planner_until_explicit_input() {
    let directory = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(directory.path().join("waits.db")).await.unwrap();
    let mut config = config();
    config.task.agent.require_approval = true;
    let id = agents
        .search()
        .unwrap()
        .start(source(), config, &examples().join("configs"))
        .await
        .unwrap();
    let root = agents
        .search()
        .unwrap()
        .status(id)
        .await
        .unwrap()
        .record()
        .root;
    engine.pause(root).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(!worker.has_active_runs().await.unwrap());
    assert!(!worker.step().await.unwrap());
    engine.resume(root).await.unwrap();
    drain(&mut worker).await;
    let search = agents.search().unwrap().status(id).await.unwrap();
    assert_eq!(search.record().phase, SearchPhase::RollingOut);
    for _ in 0..6 {
        let task = agents.search().unwrap().task(id).await.unwrap();
        let mut answered = false;
        for c in &task.record().candidates {
            let run = engine.status(c.run_id).await.unwrap();
            if let Some(wait) = &run.record().wait {
                engine
                    .signal(c.run_id, wait.id, json!({"approved":true}))
                    .await
                    .unwrap();
                answered = true;
            }
        }
        if !answered {
            break;
        }
        drain(&mut worker).await;
    }
    assert!(
        agents
            .search()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .verified
    );
}

#[tokio::test]
async fn proposal_intent_scores_and_selection_roll_back_without_duplicate_contributions() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("rollback.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let id = agents
        .search()
        .unwrap()
        .start(source(), config(), &examples().join("configs"))
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    phase(&engine, &agents, &mut worker, id, SearchPhase::Checkpoint).await;
    worker.advance_work().await.unwrap(); // capture checkpoint
    let connection = pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_search BEFORE UPDATE ON search_jobs BEGIN SELECT RAISE(ABORT,'rollback search'); END;").execute(&connection).await.unwrap();
    assert!(worker.advance_work().await.is_err());
    assert_eq!(
        agents
            .search()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .phase,
        SearchPhase::Checkpoint
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_auxiliary_runs")
            .fetch_one(&connection)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs")
            .fetch_one(&connection)
            .await
            .unwrap(),
        1
    );
    sqlx::raw_sql("DROP TRIGGER reject_search;")
        .execute(&connection)
        .await
        .unwrap();
    phase(&engine, &agents, &mut worker, id, SearchPhase::RollingOut).await;
    worker.advance_work().await.unwrap(); // publish the reserved child identities
    let first = agents
        .search()
        .unwrap()
        .status(id)
        .await
        .unwrap()
        .record()
        .nodes[0]
        .run_id;
    while engine.status(first).await.unwrap().record().state["steps"]
        .as_u64()
        .unwrap()
        < 2
    {
        worker.step().await.unwrap();
    }
    sqlx::raw_sql("CREATE TRIGGER reject_score BEFORE UPDATE ON search_jobs BEGIN SELECT RAISE(ABORT,'rollback score'); END;").execute(&connection).await.unwrap();
    assert!(worker.advance_work().await.is_err());
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
    while agents
        .search()
        .unwrap()
        .status(id)
        .await
        .unwrap()
        .record()
        .nodes
        .iter()
        .any(|n| n.result.is_none())
    {
        worker.step().await.unwrap();
    }
    let before =
        serde_json::to_value(agents.search().unwrap().status(id).await.unwrap().record()).unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_selection BEFORE UPDATE ON search_jobs BEGIN SELECT RAISE(ABORT,'rollback selection'); END;").execute(&connection).await.unwrap();
    assert!(worker.advance_work().await.is_err());
    assert_eq!(
        before,
        serde_json::to_value(agents.search().unwrap().status(id).await.unwrap().record()).unwrap()
    );
    for node in &agents
        .search()
        .unwrap()
        .status(id)
        .await
        .unwrap()
        .record()
        .nodes
    {
        let run = engine.status(node.run_id).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Paused);
        assert!(
            !run.activities()
                .iter()
                .any(|a| a.spec.handler == DefinitionId::new("coding.evaluate", 1))
        );
    }
    sqlx::raw_sql("DROP TRIGGER reject_selection;")
        .execute(&connection)
        .await
        .unwrap();
    drain(&mut worker).await;
    let search = agents.search().unwrap().status(id).await.unwrap();
    let store = SqliteStore::open(&db).await.unwrap();
    for n in &search.record().nodes {
        store.score_rollout(id, n.run_id, 0).await.unwrap();
    }
    store
        .select_search(id, search.record().selected, 0)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(search.record()).unwrap(),
        serde_json::to_value(agents.search().unwrap().status(id).await.unwrap().record()).unwrap()
    );
    connection.close().await;
}

#[test]
fn invalid_search_bounds_and_duplicate_proposals_are_rejected() {
    let mut c = config();
    c.scripted_proposals[1] = c.scripted_proposals[0].clone();
    assert!(c.validate().is_err());
    for width in [0, 1, 5] {
        let mut c = config();
        c.width = width;
        assert!(c.validate().is_err());
    }
    let mut c = config();
    c.rollout_steps = 0;
    assert!(c.validate().is_err());
}

#[tokio::test]
#[ignore = "subprocess durable search crash fixture"]
async fn search_crash_child() {
    let db = std::env::var("SEARCH_CRASH_DB").unwrap();
    let id: SearchId = std::env::var("SEARCH_CRASH_ID").unwrap().parse().unwrap();
    let marker = std::env::var("SEARCH_CRASH_MARKER").unwrap();
    let boundary = std::env::var("SEARCH_CRASH_BOUNDARY").unwrap();
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    if boundary.starts_with("proposal_") {
        phase(&engine, &agents, &mut worker, id, SearchPhase::Proposing).await;
        let execution = worker.admit().await.unwrap().unwrap();
        assert_eq!(execution.handler, DefinitionId::new("search.propose", 1));
        if boundary == "proposal_after_execution" {
            let _ = worker.execute(execution).await.unwrap();
        }
    } else if boundary == "expansion_plan" {
        phase(&engine, &agents, &mut worker, id, SearchPhase::RollingOut).await;
    } else if boundary == "score_commit" {
        phase(&engine, &agents, &mut worker, id, SearchPhase::RollingOut).await;
        while agents
            .search()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .nodes
            .iter()
            .all(|n| n.result.is_none())
        {
            worker.step().await.unwrap();
        }
    } else {
        phase(&engine, &agents, &mut worker, id, SearchPhase::Verifying).await;
    }
    fs::write(marker, b"committed boundary").unwrap();
    loop {
        std::thread::park();
    }
}

#[tokio::test]
async fn killed_search_workers_reuse_plans_actions_scores_and_final_selection() {
    for boundary in [
        "proposal_after_admission",
        "proposal_after_execution",
        "expansion_plan",
        "score_commit",
        "selection_commit",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("crash.db");
        let marker = directory.path().join("marker");
        let (engine, agents) = local_app(&db).await.unwrap();
        let id = agents
            .search()
            .unwrap()
            .start(source(), config(), &examples().join("configs"))
            .await
            .unwrap();
        let initial = agents
            .search()
            .unwrap()
            .status(id)
            .await
            .unwrap()
            .record()
            .clone();
        drop(engine);
        drop(agents);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "search_crash_child", "--nocapture"])
            .env("SEARCH_CRASH_DB", &db)
            .env("SEARCH_CRASH_ID", id.to_string())
            .env("SEARCH_CRASH_MARKER", &marker)
            .env("SEARCH_CRASH_BOUNDARY", boundary)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(45);
        while !marker.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before {boundary}"
            );
            if Instant::now() >= deadline {
                child.kill().unwrap();
                panic!("child timed out at {boundary}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        drain(&mut worker).await;
        let search = agents.search().unwrap().status(id).await.unwrap();
        let r = search.record();
        assert!(r.verified, "boundary {boundary}");
        assert_eq!(r.checkpoint_id, initial.checkpoint_id);
        assert_eq!(r.proposal_run_id, initial.proposal_run_id);
        assert_eq!(r.fork_id, initial.fork_id);
        assert_eq!(r.nodes.len(), 3);
        assert!(r.nodes.iter().all(|n| n.visits == 1));
        let task = agents.search().unwrap().task(id).await.unwrap();
        assert_eq!(task.record().candidates.len(), 4);
        assert_eq!(task.record().auxiliary_runs.len(), 1);
        let interrupted = boundary.starts_with("proposal_");
        assert_eq!(
            task.record().usage.admitted_model_calls,
            if interrupted { 7 } else { 6 }
        );
        assert_eq!(task.record().attempts, if interrupted { 19 } else { 18 });
        assert_eq!(task.record().usage.uncertain_calls, u32::from(interrupted));
        assert_eq!(task.record().usage.held_tokens > 0, interrupted);
    }
}
