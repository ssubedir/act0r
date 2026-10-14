//! Durable planner-owned assignment holds and their independent manual controls.
use super::*;
use act0r::{application::ports::PlannerPausePort, domain::tools::NativeToolResult};
use std::path::PathBuf;

pub(super) async fn receipts(db: &Path) -> Vec<Value> {
    let pool = summary::pool(db).await;
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT payload FROM harness_planner_pauses ORDER BY rowid")
            .fetch_all(&pool)
            .await
            .unwrap();
    pool.close().await;
    rows.into_iter()
        .map(|s| serde_json::from_str(&s).unwrap())
        .collect()
}
pub(super) async fn manual(
    s: &Server,
    r: &Value,
    child: &Value,
    revision: u64,
    paused: bool,
) -> Value {
    post(
        &client(),
        &format!(
            "{}/workers/{}/pause",
            base(s, r),
            child["task_id"].as_str().unwrap()
        ),
        &json!({"request_id":MessageId::new(),"expected_revision":revision,"paused":paused}),
        StatusCode::ACCEPTED,
    )
    .await
}
async fn goal_pause(s: &Server, r: &Value, revision: u64, paused: bool) {
    post(
        &client(),
        &format!(
            "{}/v1/teams/{}/tasks/{}/{}",
            s.url,
            r["team_id"].as_str().unwrap(),
            r["task_id"].as_str().unwrap(),
            if paused { "pause" } else { "resume" }
        ),
        &json!({"expected_revision":revision}),
        StatusCode::OK,
    )
    .await;
}

#[tokio::test]
async fn planner_pause_receipts_and_holds_are_atomic_and_resume_preserves_manual_ownership() {
    let model = Model::start("pause").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("pause.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    let original = engine.status(run(&r)).await.unwrap();
    let before = engine.status(run(&a)).await.unwrap();
    manual(&s, &r, &a, 0, true).await;
    steer(
        &s,
        &r,
        "Pause dependent implementation; let the sibling continue.",
    )
    .await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await;
    let pause = admit(&mut w).await;
    assert_eq!(pause.input["definition"]["id"]["name"], "pause_worker");
    let store = SqliteStore::open(&db).await.unwrap();
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER pause_rollback BEFORE INSERT ON harness_planner_pauses BEGIN SELECT RAISE(ABORT,'pause rollback'); END;").execute(&pool).await.unwrap();
    assert!(
        store
            .control_worker_pause(call(&pause), true)
            .await
            .is_err()
    );
    assert!(receipts(&db).await.is_empty());
    assert!(child(&s, &r, 0).await["pause"]["planner_hold"].is_null());
    assert_eq!(child(&s, &r, 0).await["pause"]["control"]["revision"], 1);
    sqlx::raw_sql("DROP TRIGGER pause_rollback")
        .execute(&pool)
        .await
        .unwrap();
    let NativeToolResult::Success(saved) = store
        .control_worker_pause(call(&pause), true)
        .await
        .unwrap()
    else {
        panic!("pause refused")
    };
    assert_eq!(saved["control"]["task_id"], a["task_id"]);
    assert_eq!(saved["control"]["run_id"], a["run_id"]);
    assert_eq!(saved["control"]["revision"], 1);
    assert_eq!(saved["manual_paused"], true);
    let displayed = work(&s, &r).await["coordination"]["batches"][0]["pauses"][0].clone();
    assert_eq!(displayed["activity_status"], "running");
    assert_eq!(displayed["planner_paused"], true);
    assert!(
        store
            .control_worker_pause(call(&pause), false)
            .await
            .is_err()
    );
    for field in ["worker", "reason"] {
        let mut changed = call(&pause);
        changed.arguments[field] = json!("Changed");
        assert!(store.control_worker_pause(changed, true).await.is_err());
    }
    let mut changed = call(&pause);
    changed.run_id = run(&b);
    assert!(store.control_worker_pause(changed, true).await.is_err());
    // Releasing a manual hold does not release the planner's independent hold.
    manual(&s, &r, &a, 1, false).await;
    assert_eq!(child(&s, &r, 0).await["pause"]["execution_held"], true);
    assert_eq!(child(&s, &r, 0).await["pause"]["can_resume"], false);
    store.close().await;
    pool.close().await;
    drop(w);
    s.close().await;
    drop(engine);

    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let mut w = engine.worker().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let retry = admit(&mut w).await;
    assert_eq!(retry.activity_id, pause.activity_id);
    assert_ne!(retry.attempt_id, pause.attempt_id);
    let out = w.execute(retry.clone()).await.unwrap();
    assert_eq!(out.result().unwrap(), &saved);
    w.complete(retry, out).await.unwrap();
    drain(&mut w).await;
    drop(w);
    assert_eq!(receipts(&db).await.len(), 1);
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["revision"],
        1
    );
    answer(&s, &r, &a).await;
    answer(&s, &r, &b).await;
    settle(&engine).await;
    assert_eq!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        before.record().usage.admitted_model_calls
    );
    assert_eq!(child(&s, &r, 1).await["status"], "waiting");
    manual(&s, &r, &a, 2, true).await;
    // The planner resume releases its own hold; it cannot undo the new manual decision.
    steer(&s, &r, "Resume dependent implementation now.").await;
    settle(&engine).await;
    let shown = child(&s, &r, 0).await;
    assert_eq!(shown["pause"]["planner_hold"]["paused"], false);
    assert_eq!(shown["pause"]["planner_hold"]["revision"], 2);
    assert_eq!(shown["pause"]["control"]["paused"], true);
    assert_eq!(shown["pause"]["execution_held"], true);
    assert_eq!(shown["pause"]["control"]["revision"], 3);
    let decisions = receipts(&db).await;
    assert_eq!(decisions.len(), 2);
    assert_eq!(decisions[1]["result"]["execution_held"], true);
    let view = work(&s, &r).await;
    assert_eq!(
        view["coordination"]["batches"][0]["guidance"]["targets"][0]["pause"]["planner_paused"],
        true
    );
    assert_eq!(
        view["coordination"]["batches"][0]["pauses"][0]["planner_paused"],
        false
    );
    let store = SqliteStore::open(&db).await.unwrap();
    let NativeToolResult::Success(replayed) = store
        .control_worker_pause(call(&pause), true)
        .await
        .unwrap()
    else {
        panic!("replay refused")
    };
    assert_eq!(replayed, saved);
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        false
    );
    store.close().await;
    manual(&s, &r, &a, 3, false).await;
    settle(&engine).await;
    let a = child(&s, &r, 0).await;
    answer(&s, &r, &a).await;
    let b = child(&s, &r, 1).await;
    answer(&s, &r, &b).await;
    settle(&engine).await;
    assert_eq!(work(&s, &r).await["groups"][0]["joined"], true);
    assert_eq!(
        engine.status(run(&r)).await.unwrap().record().task_groups[0].id,
        original.record().task_groups[0].id
    );
    assert_eq!(
        engine
            .history(run(&r))
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    s.close().await;
}

#[tokio::test]
async fn planner_pause_resume_attempts_are_bounded_scoped_and_include_saved_refusals() {
    for mode in [
        "pause_refuse",
        "pause_limit",
        "pause_twice",
        "pause_cycle",
        "pause_single",
        "pause_outside",
        "pause_resume_outside",
        "pause_escaped",
    ] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("bounds.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        if !mode.ends_with("outside") {
            steer(&s, &r, "Pause work until its dependency is known.").await;
            settle(&engine).await;
        }
        let saved = receipts(&db).await;
        let expected = match mode {
            "pause_refuse" => 3,
            "pause_limit" | "pause_cycle" => 4,
            "pause_twice" => 2,
            "pause_outside" | "pause_resume_outside" => 0,
            _ => 1,
        };
        assert_eq!(saved.len(), expected, "{mode}");
        if mode == "pause_refuse" || mode == "pause_limit" {
            assert!(saved.iter().all(|s| s["result"]["ok"] == false));
            assert_eq!(child(&s, &r, 0).await["execution_held"], false);
        }
        if mode == "pause_twice" {
            assert_eq!(saved[0]["result"]["ok"], true);
            assert_eq!(saved[1]["result"]["error"]["code"], "worker_pause_state");
        }
        if mode == "pause_cycle" {
            assert!(saved.iter().all(|s| s["result"]["ok"] == true));
            for i in 0..2 {
                assert_eq!(
                    child(&s, &r, i).await["pause"]["planner_hold"]["paused"],
                    false
                );
                assert_eq!(
                    child(&s, &r, i).await["pause"]["planner_hold"]["revision"],
                    2
                );
            }
        }
        if mode == "pause_escaped" {
            assert_eq!(saved[0]["request"]["reason"], "\0".repeat(1300));
            assert!(serde_json::to_vec(&saved[0]["result"]).unwrap().len() < 1024);
            assert_eq!(
                child(&s, &r, 0).await["pause"]["planner_hold"]["reason"],
                "\0".repeat(1300)
            );
        }
        if mode == "pause_limit" || mode.ends_with("outside") {
            assert_eq!(
                engine.status(run(&r)).await.unwrap().record().status,
                RunStatus::Failed
            );
            if mode == "pause_limit" {
                let requests = model.requests.lock().unwrap();
                let last = requests
                    .iter()
                    .rev()
                    .find(|p| p["coordinating_guidance"].is_object())
                    .unwrap();
                assert_eq!(last["worker_pausing"]["remaining"], 0);
                assert!(
                    last["tools"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|t| t["name"] != "pause_worker" && t["name"] != "resume_worker")
                );
            }
        }
        s.close().await;
    }
}

#[tokio::test]
async fn a_sibling_finding_can_resume_a_planner_held_assignment_in_its_original_join() {
    let model = Model::start("finding_pause").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("finding.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    steer(
        &s,
        &r,
        "Pause the dependent worker and await the sibling's report.",
    )
    .await;
    settle(&engine).await;
    assert_eq!(child(&s, &r, 0).await["execution_held"], true);
    answer(&s, &r, &a).await;
    answer(&s, &r, &b).await;
    settle(&engine).await;
    let saved = receipts(&db).await;
    assert_eq!(saved.len(), 2);
    assert_eq!(saved[0]["paused"], true);
    assert_eq!(saved[1]["paused"], false);
    assert_eq!(child(&s, &r, 0).await["execution_held"], false);
    assert_eq!(child(&s, &r, 0).await["task_id"], a["task_id"]);
    assert_eq!(work(&s, &r).await["groups"][0]["joined"], false);
    assert!(
        work(&s, &r).await["coordination"]["batches"][0]["guidance"]["findings"]
            .as_array()
            .is_some_and(|f| !f.is_empty())
    );
    s.close().await;
}

#[tokio::test]
async fn planner_pause_obeys_model_budgets_goal_controls_and_target_completion_races() {
    for stage in 0..4 {
        let model = Model::start("pause").await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("races.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(
            &s,
            &engine,
            &model,
            root.path(),
            if stage == 0 {
                Some(json!({"max_model_calls":5,"max_tokens":null}))
            } else {
                None
            },
        )
        .await;
        let a = child(&s, &r, 0).await;
        steer(&s, &r, "Pause dependent work.").await;
        if stage == 0 {
            settle(&engine).await;
            assert!(receipts(&db).await.is_empty());
            assert_eq!(model.coordination_calls(), 0);
            assert_eq!(
                engine.status(run(&r)).await.unwrap().record().status,
                RunStatus::BudgetExhausted
            );
        } else {
            let mut w = engine.worker().await.unwrap();
            step(&mut w).await;
            let pause = admit(&mut w).await;
            if stage == 1 {
                engine.pause(run(&r)).await.unwrap();
            } else if stage == 2 {
                engine.cancel(run(&a)).await.unwrap();
            } else {
                engine.cancel(run(&r)).await.unwrap();
            }
            let out = w.execute(pause.clone()).await.unwrap();
            w.complete(pause, out).await.unwrap();
            drain(&mut w).await;
            drop(w);
            assert_eq!(receipts(&db).await[0]["result"]["ok"], stage == 1);
            if stage == 1 {
                assert_eq!(child(&s, &r, 0).await["execution_held"], true);
                assert!(work(&s, &r).await["coordination"]["batches"][0]["decision"].is_null());
                engine.resume(run(&r)).await.unwrap();
                settle(&engine).await;
                assert!(work(&s, &r).await["coordination"]["batches"][0]["decision"].is_object());
            }
        }
        s.close().await;
    }
}

#[tokio::test]
async fn a_planner_pause_keeps_absolute_delegation_deadlines_and_stop_precedence() {
    let model = Model::start("pause_single").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("deadline.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let pool = summary::pool(&db).await;
    for deadline in [i64::MAX, 0] {
        sqlx::query(
            "UPDATE runs SET payload=json_set(payload,'$.delegations[0].deadline_at',?) WHERE id=?",
        )
        .bind(deadline)
        .bind(run(&r).to_string())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE agent_delegations SET deadline_at=? WHERE id=?")
            .bind(deadline)
            .bind(a["task_id"].as_str().unwrap())
            .execute(&pool)
            .await
            .unwrap();
        if deadline == i64::MAX {
            steer(&s, &r, "Pause until the dependency is ready.").await;
            settle(&engine).await;
            assert_eq!(child(&s, &r, 0).await["execution_held"], true);
            assert_eq!(
                engine.status(run(&r)).await.unwrap().record().delegations[0].deadline_at,
                Some(deadline)
            );
        }
    }
    settle(&engine).await;
    assert_eq!(child(&s, &r, 0).await["status"], "cancelled");
    assert_eq!(child(&s, &r, 0).await["execution_held"], false);
    assert_eq!(work(&s, &r).await["groups"][0]["joined"], true);
    pool.close().await;
    s.close().await;
}

#[tokio::test]
async fn planner_resume_cannot_release_goal_or_run_holds_and_admitted_models_may_settle() {
    let model = Model::start("pause").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("independent.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    answer(&s, &r, &a).await;
    let mut w = engine.worker().await.unwrap();
    let model_call = admit(&mut w).await;
    assert_eq!(model_call.run_id, run(&a));
    assert_eq!(model_call.handler.name, "agent.model");
    steer(&s, &r, "Pause dependent work.").await;
    step(&mut w).await;
    let pause = step(&mut w).await;
    assert_eq!(pause.input["definition"]["id"]["name"], "pause_worker");
    assert_eq!(child(&s, &r, 0).await["pause"]["admitted_operations"], 1);
    let frozen = model_call.input.clone();
    let out = w.execute(model_call.clone()).await.unwrap();
    w.complete(model_call.clone(), out).await.unwrap();
    drain(&mut w).await;
    assert_eq!(model_call.input, frozen);
    assert_eq!(child(&s, &r, 0).await["pause"]["admitted_operations"], 0);
    assert!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .activities()
            .iter()
            .any(|x| x.id == model_call.activity_id
                && x.status == act0r::domain::ActivityStatus::Succeeded)
    );
    steer(&s, &r, "Resume dependent work.").await;
    step(&mut w).await;
    let resume = admit(&mut w).await;
    assert_eq!(resume.input["definition"]["id"]["name"], "resume_worker");
    // The already admitted resume may settle, but it cannot release either new hold.
    goal_pause(&s, &r, 0, true).await;
    engine.pause(run(&a)).await.unwrap();
    let before = engine.status(run(&a)).await.unwrap();
    let out = w.execute(resume.clone()).await.unwrap();
    assert_eq!(out.result().unwrap()["control"]["paused"], false);
    assert_eq!(out.result().unwrap()["execution_held"], true);
    assert_eq!(out.result().unwrap()["inherited_held"], true);
    w.complete(resume, out).await.unwrap();
    drain(&mut w).await;
    drop(w);
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        false
    );
    assert_eq!(child(&s, &r, 0).await["pause"]["inherited_held"], true);
    goal_pause(&s, &r, 1, false).await;
    settle(&engine).await;
    assert_eq!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        before.record().usage.admitted_model_calls
    );
    assert!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .controls
            .pause_requested
    );
    engine.resume(run(&a)).await.unwrap();
    settle(&engine).await;
    assert_eq!(child(&s, &r, 0).await["status"], "waiting");
    s.close().await;
}

#[tokio::test]
async fn pause_and_resume_preserve_unsafe_edit_reconciliation_across_restart() {
    use act0r::domain::{Outcome, ReconciliationDecision, ReconciliationRequest};
    let model = Model::start("pause_unsafe").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("unsafe.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    answer(&s, &r, &a).await;
    settle(&engine).await;
    let goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        s.url,
        r["team_id"].as_str().unwrap(),
        r["task_id"].as_str().unwrap()
    );
    let inbox = get(&client(), &format!("{goal}/inbox")).await;
    let approval = inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["task_id"] == a["task_id"] && i["actionable"] == true)
        .unwrap();
    post(
        &client(),
        &format!(
            "{goal}/waits/{}/responses",
            approval["wait"]["id"].as_str().unwrap()
        ),
        &json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    let mut w = engine.worker().await.unwrap();
    let edit = admit(&mut w).await;
    assert_eq!(edit.input["definition"]["id"]["name"], "replace_text");
    let result = w.execute(edit.clone()).await.unwrap();
    assert!(matches!(
        result,
        Outcome::Success(_) | Outcome::SuccessWithUsage { .. }
    ));
    let before = engine.status(run(&a)).await.unwrap();
    let copy = PathBuf::from(
        before.record().input["input"]["workspace"]
            .as_str()
            .unwrap(),
    );
    assert_eq!(
        fs::read_to_string(copy.join("src/lib.rs")).unwrap(),
        "Edited source: id and display_name\n"
    );
    steer(
        &s,
        &r,
        "Pause this assignment while the sibling checks the API.",
    )
    .await;
    step(&mut w).await;
    let pause = step(&mut w).await;
    assert_eq!(pause.input["definition"]["id"]["name"], "pause_worker");
    w.complete(
        edit.clone(),
        Outcome::Uncertain("Edit saved but acknowledgement lost".into()),
    )
    .await
    .unwrap();
    drain(&mut w).await;
    drop(w);
    let held = engine.status(run(&a)).await.unwrap();
    assert_eq!(held.record().status, RunStatus::Blocked);
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        true
    );
    assert_eq!(child(&s, &r, 1).await["pause"]["execution_held"], false);
    let projection = work(&s, &r).await["coordination"]["batches"][0]["pauses"][0].clone();
    s.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    assert_eq!(
        work(&s, &r).await["coordination"]["batches"][0]["pauses"][0],
        projection
    );
    steer(
        &s,
        &r,
        "Resume this assignment after the dependency was checked.",
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        false
    );
    let blocked = engine.status(run(&a)).await.unwrap();
    assert_eq!(blocked.record().status, RunStatus::Blocked);
    assert_eq!(blocked.attempts().len(), held.attempts().len());
    assert_eq!(json!(blocked.record().usage), json!(held.record().usage));
    // Explicit reconciliation is still required even after resume.
    engine
        .reconcile(
            edit.run_id,
            ReconciliationRequest {
                activity_id: edit.activity_id,
                attempt_id: edit.attempt_id,
                evidence: "Inspected the saved file and tool journal; the write has stopped."
                    .into(),
                decision: ReconciliationDecision::Accept {
                    result: result.result().unwrap().clone(),
                },
            },
        )
        .await
        .unwrap();
    settle(&engine).await;
    assert_eq!(
        fs::read_to_string(copy.join("src/lib.rs")).unwrap(),
        "Edited source: id and display_name\n"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
        "Saved source: id and display_name\n"
    );
    assert_eq!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .attempts()
            .iter()
            .filter(|x| x.activity_id == edit.activity_id)
            .count(),
        1
    );
    assert_eq!(work(&s, &r).await["groups"][0]["joined"], false);
    s.close().await;
}

#[tokio::test]
async fn old_pause_and_resume_replays_cannot_affect_later_assignments_on_the_same_actor() {
    let model = Model::start("pause_reassign").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("later.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    let mut calls = Vec::new();
    for (instruction, paused) in [
        ("Pause dependent work.", true),
        ("Resume dependent work.", false),
    ] {
        steer(&s, &r, instruction).await;
        let mut w = engine.worker().await.unwrap();
        step(&mut w).await;
        let action = admit(&mut w).await;
        assert_eq!(
            action.input["definition"]["id"]["name"],
            if paused {
                "pause_worker"
            } else {
                "resume_worker"
            }
        );
        calls.push(action.clone());
        let out = w.execute(action.clone()).await.unwrap();
        w.complete(action, out).await.unwrap();
        drain(&mut w).await;
    }
    for _ in 0..2 {
        for c in [&a, &b] {
            let current = work(&s, &r).await["groups"][0]["children"]
                .as_array()
                .unwrap()
                .iter()
                .find(|x| x["task_id"] == c["task_id"])
                .unwrap()
                .clone();
            answer(&s, &r, &current).await;
        }
        settle(&engine).await;
    }
    let view = work(&s, &r).await;
    let next = view["groups"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|g| g["children"].as_array().unwrap())
        .find(|c| c["actor_id"] == a["actor_id"] && c["task_id"] != a["task_id"])
        .unwrap()
        .clone();
    assert_eq!(next["status"], "waiting");
    assert_eq!(next["execution_held"], false);
    assert!(next["pause"]["planner_hold"].is_null());
    let before = json!(engine.status(run(&next)).await.unwrap().record());
    let saved = receipts(&db).await;
    let store = SqliteStore::open(&db).await.unwrap();
    for (index, action) in calls.iter().enumerate() {
        let NativeToolResult::Success(out) = store
            .control_worker_pause(call(action), index == 0)
            .await
            .unwrap()
        else {
            panic!("replay refused")
        };
        assert_eq!(out, saved[index]["result"]);
    }
    assert_eq!(
        json!(engine.status(run(&next)).await.unwrap().record()),
        before
    );
    assert_eq!(receipts(&db).await.len(), 2);
    store.close().await;
    s.close().await;
}
