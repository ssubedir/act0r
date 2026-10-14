//! Real planner selection, transactional holds, interrupted receipts and original joins.
use super::*;
use act0r::{application::ports::PlannerStopPort, domain::tools::NativeToolResult};

async fn receipts(db: &Path) -> Vec<Value> {
    let pool = summary::pool(db).await;
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT payload FROM harness_planner_stops ORDER BY rowid")
            .fetch_all(&pool)
            .await
            .unwrap();
    pool.close().await;
    rows.iter()
        .map(|r| serde_json::from_str(r).unwrap())
        .collect()
}
#[tokio::test]
async fn stop_worker_rolls_back_and_replays_after_restart_while_sibling_and_join_continue() {
    let model = Model::start("stop_reassign").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("stop.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    let original = engine.status(run(&r)).await.unwrap();
    let prior = engine.status(run(&a)).await.unwrap();
    let sibling = engine.status(run(&b)).await.unwrap();
    steer(
        &s,
        &r,
        "Stop duplicate work; let the other assignment continue.",
    )
    .await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await;
    let stop = admit(&mut w).await;
    assert_eq!(stop.input["definition"]["id"]["name"], "stop_worker");
    let store = SqliteStore::open(&db).await.unwrap();
    let pool = summary::pool(&db).await;
    // Failure at the final receipt must undo the hold and ordinary cancellation receipt too.
    sqlx::raw_sql("CREATE TRIGGER stop_rollback BEFORE INSERT ON harness_planner_stops BEGIN SELECT RAISE(ABORT,'stop rollback'); END;").execute(&pool).await.unwrap();
    assert!(store.stop_worker(call(&stop)).await.is_err());
    assert!(receipts(&db).await.is_empty());
    assert_eq!(child(&s, &r, 0).await["cancellation"]["requested"], false);
    assert!(
        child(&s, &r, 0).await["cancellation"]["receipts"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    sqlx::raw_sql("DROP TRIGGER stop_rollback")
        .execute(&pool)
        .await
        .unwrap();
    let NativeToolResult::Success(saved) = store.stop_worker(call(&stop)).await.unwrap() else {
        panic!("stop refused")
    };
    assert_eq!(saved["stop_requested"], true);
    assert_eq!(saved["cancellation"]["task_id"], a["task_id"]);
    assert_eq!(saved["cancellation"]["run_id"], a["run_id"]);
    assert_eq!(child(&s, &r, 0).await["cancellation"]["requested"], true);
    let view = work(&s, &r).await;
    let shown = &view["coordination"]["batches"][0]["stops"][0];
    assert_eq!(shown["receipt"]["result"], saved);
    assert_eq!(shown["activity_status"], "running");
    assert_eq!(shown["settled"], false);
    assert_eq!(
        json!(engine.status(run(&b)).await.unwrap().record()),
        json!(sibling.record())
    );
    let mut changed = call(&stop);
    changed.arguments["reason"] = json!("Changed reason");
    assert!(store.stop_worker(changed).await.is_err());
    let mut changed = call(&stop);
    changed.arguments["worker"] = json!("coder2");
    assert!(store.stop_worker(changed).await.is_err());
    let mut changed = call(&stop);
    changed.run_id = run(&b);
    assert!(store.stop_worker(changed).await.is_err());
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
    assert_eq!(retry.activity_id, stop.activity_id);
    assert_ne!(retry.attempt_id, stop.attempt_id);
    let out = w.execute(retry.clone()).await.unwrap();
    assert_eq!(out.result().unwrap(), &saved);
    w.complete(retry, out).await.unwrap();
    drain(&mut w).await;
    drop(w);
    let stopped = engine.status(run(&a)).await.unwrap();
    assert_eq!(stopped.record().status, RunStatus::Cancelled);
    assert_eq!(json!(stopped.record().usage), json!(prior.record().usage));
    assert_eq!(child(&s, &r, 1).await["status"], "waiting");
    let view = work(&s, &r).await;
    assert_eq!(view["groups"][0]["finished"], 1);
    assert_eq!(view["groups"][0]["joined"], false);
    assert_eq!(
        view["coordination"]["batches"][0]["stops"][0]["settled"],
        true
    );
    assert_eq!(
        view["coordination"]["batches"][0]["stops"][0]["current_status"],
        "cancelled"
    );
    assert_eq!(
        view["coordination"]["batches"][0]["decision"]["request"]["coder"],
        Value::Null
    );
    assert_eq!(receipts(&db).await.len(), 1);
    answer(&s, &r, &b).await;
    settle(&engine).await;
    let b = child(&s, &r, 1).await;
    answer(&s, &r, &b).await;
    settle(&engine).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(
        parent.record().task_groups[0].id,
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
    assert!(
        parent
            .record()
            .delegations
            .iter()
            .take(original.record().delegations.len())
            .all(|d| d.outcome.is_some())
    );
    let next = work(&s, &r).await["groups"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|g| g["children"].as_array().unwrap())
        .find(|c| c["actor_id"] == a["actor_id"] && c["task_id"] != a["task_id"])
        .unwrap()
        .clone();
    assert_eq!(next["status"], "waiting");
    assert_eq!(next["cancellation"]["requested"], false);
    let next_saved = json!(engine.status(run(&next)).await.unwrap().record());
    let store = SqliteStore::open(&db).await.unwrap();
    let NativeToolResult::Success(replay) = store.stop_worker(call(&stop)).await.unwrap() else {
        panic!("replay refused")
    };
    assert_eq!(replay, saved);
    assert_eq!(
        json!(engine.status(run(&next)).await.unwrap().record()),
        next_saved
    );
    assert_eq!(child(&s, &r, 0).await["status"], "cancelled");
    assert_eq!(receipts(&db).await.len(), 1);
    store.close().await;
    s.close().await;
}

#[tokio::test]
async fn stop_worker_refusals_are_saved_and_bounded_and_single_assignment_can_stop() {
    for mode in [
        "stop_refuse",
        "stop_oversize",
        "stop_limit",
        "stop_single",
        "stop_outside",
    ] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("stop.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        if mode != "stop_outside" {
            steer(&s, &r, "Consider stopping the redundant assignment.").await;
            settle(&engine).await;
        }
        let saved = receipts(&db).await;
        assert_eq!(
            saved.len(),
            if mode == "stop_outside" {
                0
            } else if mode == "stop_limit" || mode == "stop_refuse" {
                2
            } else {
                1
            }
        );
        if mode == "stop_single" {
            assert_eq!(saved[0]["result"]["ok"], true);
            assert_eq!(child(&s, &r, 0).await["status"], "cancelled");
            assert_eq!(
                engine
                    .history(run(&r))
                    .await
                    .unwrap()
                    .iter()
                    .filter(|e| e.kind == "delegation_completed")
                    .count(),
                1
            );
        } else {
            assert!(saved.iter().all(|r| r["result"]["ok"] == false));
            if mode != "stop_outside" && mode != "stop_limit" {
                assert_eq!(child(&s, &r, 0).await["cancellation"]["requested"], false);
                assert_eq!(child(&s, &r, 1).await["status"], "waiting");
            }
            if mode == "stop_limit" || mode == "stop_outside" {
                assert!(engine.status(run(&r)).await.unwrap().record().status == RunStatus::Failed);
            }
        }
        if mode == "stop_limit" {
            let seen = model.requests.lock().unwrap();
            let last = seen
                .iter()
                .rev()
                .find(|p| p["coordinating_guidance"].is_object())
                .unwrap();
            assert_eq!(last["worker_stopping"]["remaining"], 0);
            assert!(
                last["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|t| t["name"] != "stop_worker")
            );
        }
        s.close().await;
    }
}

#[tokio::test]
async fn stop_worker_preserves_an_admitted_unsafe_edit_and_reconciliation_across_restart() {
    use act0r::domain::{Outcome, ReconciliationDecision, ReconciliationRequest};
    let model = Model::start("stop_unsafe").await;
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
    assert_eq!(edit.run_id, run(&a));
    let out = w.execute(edit.clone()).await.unwrap();
    assert!(matches!(
        out,
        Outcome::Success(_) | Outcome::SuccessWithUsage { .. }
    ));
    let before = engine.status(run(&a)).await.unwrap();
    let copy = before.record().input["input"]["workspace"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        fs::read_to_string(Path::new(&copy).join("src/lib.rs")).unwrap(),
        "Edited source: id and display_name\n"
    );
    steer(
        &s,
        &r,
        "Stop this duplicate implementation; keep the other worker.",
    )
    .await;
    step(&mut w).await;
    let stop = step(&mut w).await;
    assert_eq!(stop.input["definition"]["id"]["name"], "stop_worker");
    assert_eq!(child(&s, &r, 0).await["cancellation"]["requested"], true);
    w.complete(
        edit.clone(),
        Outcome::Uncertain("Write completed but acknowledgement was lost".into()),
    )
    .await
    .unwrap();
    drain(&mut w).await;
    drop(w);
    let stop_view = work(&s, &r).await["coordination"]["batches"][0]["stops"][0].clone();
    assert_eq!(stop_view["current_status"], "blocked");
    assert_eq!(stop_view["settled"], false);
    assert_eq!(child(&s, &r, 1).await["status"], "waiting");
    assert_eq!(work(&s, &r).await["groups"][0]["joined"], false);
    s.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    assert_eq!(
        work(&s, &r).await["coordination"]["batches"][0]["stops"][0],
        stop_view
    );
    engine
        .reconcile(
            edit.run_id,
            ReconciliationRequest {
                activity_id: edit.activity_id,
                attempt_id: edit.attempt_id,
                evidence: "Inspected the saved edit and journal; operation has stopped".into(),
                decision: ReconciliationDecision::Retry {},
            },
        )
        .await
        .unwrap();
    settle(&engine).await;
    let after = engine.status(run(&a)).await.unwrap();
    assert_eq!(after.record().status, RunStatus::Cancelled);
    assert_eq!(after.attempts().len(), before.attempts().len());
    assert_eq!(json!(after.record().usage), json!(before.record().usage));
    assert_eq!(
        fs::read_to_string(Path::new(&copy).join("src/lib.rs")).unwrap(),
        "Edited source: id and display_name\n"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
        "Saved source: id and display_name\n"
    );
    assert_eq!(child(&s, &r, 1).await["cancellation"]["requested"], false);
    s.close().await;
}

#[tokio::test]
async fn stop_worker_obeys_pauses_budgets_and_closed_assignment_or_goal_races() {
    for stage in 0..4 {
        let model = Model::start("stop").await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("race.db");
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
        let b = child(&s, &r, 1).await;
        steer(&s, &r, "Stop duplicate work").await;
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
            let stop = admit(&mut w).await;
            assert_eq!(stop.input["definition"]["id"]["name"], "stop_worker");
            if stage == 1 {
                // Already admitted stop may settle while the root is paused; fresh routing holds.
                engine.pause(run(&r)).await.unwrap();
                engine.pause(run(&a)).await.unwrap();
            } else if stage == 2 {
                engine.cancel(run(&a)).await.unwrap(); // A captured assignment closes after selection.
            } else {
                engine.cancel(run(&r)).await.unwrap();
            }
            let out = w.execute(stop.clone()).await.unwrap();
            w.complete(stop, out).await.unwrap();
            drain(&mut w).await;
            drop(w);
            let saved = receipts(&db).await;
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0]["result"]["ok"], stage == 1);
            if stage == 1 {
                assert_eq!(child(&s, &r, 0).await["status"], "cancelled");
                assert!(work(&s, &r).await["coordination"]["batches"][0]["decision"].is_null());
                engine.resume(run(&r)).await.unwrap();
                settle(&engine).await;
                assert!(work(&s, &r).await["coordination"]["batches"][0]["decision"].is_object());
            }
            if stage < 3 {
                assert_eq!(child(&s, &r, 1).await["cancellation"]["requested"], false);
                assert_eq!(child(&s, &r, 1).await["task_id"], b["task_id"]);
            }
        }
        s.close().await;
    }
}

#[tokio::test]
async fn a_worker_finding_can_stop_the_other_assignment_and_repeated_stops_refuse() {
    for mode in ["finding_stop", "stop_twice"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("finding.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        let a = child(&s, &r, 0).await;
        if mode == "finding_stop" {
            engine.pause(run(&r)).await.unwrap();
            answer(&s, &r, &a).await;
            settle(&engine).await;
            assert_eq!(model.coordination_calls(), 0);
            engine.resume(run(&r)).await.unwrap();
        } else {
            steer(&s, &r, "Stop the duplicate assignment.").await;
        }
        settle(&engine).await;
        let saved = receipts(&db).await;
        assert_eq!(saved.len(), if mode == "finding_stop" { 1 } else { 2 });
        assert_eq!(saved[0]["result"]["ok"], true);
        if mode == "finding_stop" {
            assert_eq!(saved[0]["request"]["worker"], "coder2");
            assert_eq!(child(&s, &r, 1).await["status"], "cancelled");
            assert_eq!(child(&s, &r, 0).await["cancellation"]["requested"], false);
            let view = work(&s, &r).await;
            assert_eq!(view["coordination"]["findings"][0]["status"], "routed");
            assert_eq!(
                view["coordination"]["batches"][0]["stops"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
        } else {
            assert_eq!(saved[1]["result"]["ok"], false);
            assert_eq!(
                child(&s, &r, 0).await["cancellation"]["receipts"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(child(&s, &r, 1).await["cancellation"]["requested"], false);
        }
        assert_eq!(work(&s, &r).await["groups"][0]["joined"], false);
        s.close().await;
    }
}

#[tokio::test]
async fn stop_worker_retains_escaped_reasons_with_a_compact_acknowledgement() {
    let model = Model::start("stop_escaped").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("escaped.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    steer(&s, &r, "Stop duplicate work.").await;
    settle(&engine).await;
    let saved = receipts(&db).await;
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0]["result"]["ok"], true);
    assert_eq!(saved[0]["request"]["reason"], "\0".repeat(1300));
    assert!(serde_json::to_vec(&saved[0]["result"]).unwrap().len() < 1024);
    assert_eq!(
        work(&s, &r).await["coordination"]["batches"][0]["stops"][0]["receipt"]["request"]["reason"],
        saved[0]["request"]["reason"]
    );
    assert_eq!(child(&s, &r, 0).await["status"], "cancelled");
    assert_eq!(child(&s, &r, 1).await["status"], "waiting");
    assert!(work(&s, &r).await["coordination"]["batches"][0]["decision"].is_object());
    s.close().await;
}
