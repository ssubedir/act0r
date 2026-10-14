//! Exact assignment dependencies use the original durable join and planner Run.
use super::*;
use act0r::{application::ports::PlannerPausePort, domain::tools::NativeToolResult};

async fn dependencies(db: &Path) -> Vec<Value> {
    let pool = summary::pool(db).await;
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT payload FROM harness_worker_dependencies ORDER BY rowid")
            .fetch_all(&pool)
            .await
            .unwrap();
    pool.close().await;
    rows.into_iter()
        .map(|s| serde_json::from_str(&s).unwrap())
        .collect()
}
async fn finish(s: &Server, engine: &act0r::Engine, r: &Value, c: &Value) {
    for _ in 0..2 {
        answer(s, r, c).await;
        settle(engine).await;
    }
}
async fn join_count(engine: &act0r::Engine, r: &Value) -> usize {
    engine
        .history(run(r))
        .await
        .unwrap()
        .iter()
        .filter(|e| e.kind == "task_group_joined")
        .count()
}

#[tokio::test]
async fn prerequisite_finishing_between_capture_and_registration_has_no_lost_wake() {
    let model = Model::start("dependency").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("race.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    steer(&s, &r, "Wait for the captured API worker.").await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await;
    let wait = admit(&mut w).await;
    assert_eq!(wait.input["definition"]["id"]["name"], "wait_for_worker");
    for _ in 0..2 {
        answer(&s, &r, &b).await;
        drain(&mut w).await;
    }
    assert_eq!(child(&s, &r, 1).await["status"], "failed");
    assert!(
        engine.status(run(&r)).await.unwrap().record().delegations[1]
            .outcome
            .is_some()
    );
    let out = w.execute(wait.clone()).await.unwrap();
    w.complete(wait, out).await.unwrap();
    drain(&mut w).await;
    let d = dependencies(&db).await.remove(0);
    assert_eq!(d["outcome"], "failed");
    assert_eq!(d["closed"], true);
    assert!(d["review_timer_id"].is_string());
    assert_eq!(child(&s, &r, 0).await["task_id"], a["task_id"]);
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        false
    );
    assert_eq!(join_count(&engine, &r).await, 0);
    s.close().await;
}

#[tokio::test]
async fn released_dependencies_and_old_receipts_cannot_hold_later_assignments() {
    for early_release in [true, false] {
        let model = Model::start(if early_release {
            "dependency"
        } else {
            "dependency_reassign"
        })
        .await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("stale.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        let a = child(&s, &r, 0).await;
        let b = child(&s, &r, 1).await;
        steer(&s, &r, "Wait for the sibling.").await;
        settle(&engine).await;
        let parent = engine.status(run(&r)).await.unwrap();
        let activity = parent
            .activities()
            .iter()
            .find(|a| a.spec.input["definition"]["id"]["name"] == "wait_for_worker")
            .unwrap();
        let request: NativeToolRequest =
            serde_json::from_value(activity.spec.input.clone()).unwrap();
        let old = NativeToolCall {
            run_id: run(&r),
            activity_id: activity.id,
            attempt_id: parent
                .attempts()
                .iter()
                .find(|x| x.activity_id == activity.id)
                .unwrap()
                .id,
            arguments: request.arguments,
            workspace: None,
            writable_paths: vec![],
            test_command: None,
            project_command: None,
        };
        if early_release {
            steer(
                &s,
                &r,
                "Resume the saved worker; no longer wait for the API.",
            )
            .await;
            settle(&engine).await;
            assert_eq!(dependencies(&db).await[0]["closed"], true);
        }
        let calls = model.coordination_calls();
        finish(&s, &engine, &r, &b).await;
        if early_release {
            assert_eq!(model.coordination_calls(), calls);
        }
        finish(&s, &engine, &r, &a).await;
        assert_eq!(join_count(&engine, &r).await, 1);
        let before = work(&s, &r).await;
        let records = dependencies(&db).await;
        let store = SqliteStore::open(&db).await.unwrap();
        assert!(matches!(
            store.control_worker_pause(old, true).await.unwrap(),
            NativeToolResult::Success(_)
        ));
        store.close().await;
        assert_eq!(work(&s, &r).await, before);
        assert_eq!(dependencies(&db).await, records);
        if !early_release {
            let parent = engine.status(run(&r)).await.unwrap();
            assert_eq!(parent.record().delegations.len(), 3);
            let later = parent.record().delegations[2].id;
            assert_ne!(later.to_string(), a["task_id"]);
            let pool = summary::pool(&db).await;
            let held: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM agent_goal_paused_requests WHERE id=?)",
            )
            .bind(later.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(!held);
            pool.close().await;
        }
        s.close().await;
    }
}

#[tokio::test]
async fn prerequisite_deadlines_and_goal_stop_preserve_the_original_join() {
    for stop in [false, true] {
        let model = Model::start("dependency").await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("deadline.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        let b = child(&s, &r, 1).await;
        let pool = summary::pool(&db).await;
        sqlx::query(
            "UPDATE runs SET payload=json_set(payload,'$.delegations[1].deadline_at',?) WHERE id=?",
        )
        .bind(i64::MAX)
        .bind(run(&r).to_string())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE agent_delegations SET deadline_at=? WHERE id=?")
            .bind(i64::MAX)
            .bind(b["task_id"].as_str().unwrap())
            .execute(&pool)
            .await
            .unwrap();
        steer(&s, &r, "Wait for the prerequisite.").await;
        settle(&engine).await;
        assert_eq!(
            engine.status(run(&r)).await.unwrap().record().delegations[1].deadline_at,
            Some(i64::MAX)
        );
        if stop {
            engine.cancel(run(&r)).await.unwrap();
        } else {
            sqlx::query("UPDATE runs SET payload=json_set(payload,'$.delegations[1].deadline_at',0) WHERE id=?").bind(run(&r).to_string()).execute(&pool).await.unwrap();
            sqlx::query("UPDATE agent_delegations SET deadline_at=0 WHERE id=?")
                .bind(b["task_id"].as_str().unwrap())
                .execute(&pool)
                .await
                .unwrap();
        }
        settle(&engine).await;
        if stop {
            assert_eq!(
                engine.status(run(&r)).await.unwrap().record().status,
                RunStatus::Cancelled
            );
            assert_eq!(dependencies(&db).await[0]["closed"], true);
        } else {
            let d = dependencies(&db).await.remove(0);
            assert!(d["outcome"].is_string());
            assert!(d["review_timer_id"].is_string());
            assert!(work(&s,&r).await["coordination"]["batches"][0]["inspections"][0]["receipt"]["result"]["assignment"]["terminal_outcome"]["error"]["text"].as_str().unwrap().contains("delegation_timeout"));
            assert_eq!(d["closed"], true);
            assert_eq!(
                engine.status(run(&r)).await.unwrap().record().delegations[1].deadline_at,
                Some(0)
            );
            assert_eq!(join_count(&engine, &r).await, 0);
        }
        pool.close().await;
        s.close().await;
    }
}

#[tokio::test]
async fn dependency_registration_is_atomic_and_exact_retry_survives_restart() {
    let model = Model::start("dependency").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("dependency.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    steer(&s, &r, "Hold the UI worker until its sibling finishes.").await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await;
    let wait = admit(&mut w).await;
    assert_eq!(wait.input["definition"]["id"]["name"], "wait_for_worker");
    let store = SqliteStore::open(&db).await.unwrap();
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER fail_dependency BEFORE INSERT ON harness_worker_dependencies BEGIN SELECT RAISE(ABORT,'dependency rollback'); END;").execute(&pool).await.unwrap();
    assert!(store.control_worker_pause(call(&wait), true).await.is_err());
    assert!(dependencies(&db).await.is_empty());
    assert!(planner_pauses::receipts(&db).await.is_empty());
    assert!(child(&s, &r, 0).await["pause"]["planner_hold"].is_null());
    sqlx::raw_sql("DROP TRIGGER fail_dependency")
        .execute(&pool)
        .await
        .unwrap();
    let NativeToolResult::Success(saved) =
        store.control_worker_pause(call(&wait), true).await.unwrap()
    else {
        panic!("wait refused")
    };
    assert_eq!(saved["dependency"]["prerequisite_task_id"], b["task_id"]);
    assert_eq!(saved["control"]["task_id"], a["task_id"]);
    assert_eq!(dependencies(&db).await.len(), 1);
    let NativeToolResult::Success(replay) =
        store.control_worker_pause(call(&wait), true).await.unwrap()
    else {
        panic!("replay refused")
    };
    assert_eq!(saved, replay);
    let mut changed = call(&wait);
    changed.arguments["dependency"] = json!("coder");
    assert!(store.control_worker_pause(changed, true).await.is_err());
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
    assert_eq!(retry.activity_id, wait.activity_id);
    let out = w.execute(retry.clone()).await.unwrap();
    w.complete(retry, out).await.unwrap();
    drain(&mut w).await;
    assert_eq!(dependencies(&db).await.len(), 1);
    assert_eq!(planner_pauses::receipts(&db).await.len(), 1);
    assert_eq!(child(&s, &r, 0).await["execution_held"], true);
    assert_eq!(
        work(&s, &r).await["coordination"]["dependencies"][0]["outcome"],
        Value::Null
    );
    s.close().await;
}

#[tokio::test]
async fn prerequisite_completion_wakes_once_after_restart_and_keeps_manual_holds() {
    let model = Model::start("dependency").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("ready.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    steer(&s, &r, "Wait for the API assignment.").await;
    settle(&engine).await;
    assert_eq!(dependencies(&db).await.len(), 1);
    planner_pauses::manual(&s, &r, &a, 0, true).await;
    engine.pause(run(&r)).await.unwrap();
    let calls = model.coordination_calls();
    finish(&s, &engine, &r, &b).await;
    assert_eq!(child(&s, &r, 1).await["status"], "failed");
    let d = dependencies(&db).await.remove(0);
    assert_eq!(d["outcome"], "failed");
    assert!(d["review_timer_id"].is_null());
    assert_eq!(model.coordination_calls(), calls);
    assert_eq!(join_count(&engine, &r).await, 0);
    s.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    assert_eq!(dependencies(&db).await[0], d);
    engine.resume(run(&r)).await.unwrap();
    settle(&engine).await;
    let view = work(&s, &r).await;
    let batch = &view["coordination"]["batches"][0];
    assert_eq!(
        batch["guidance"]["dependencies"][0]["prerequisite_task_id"],
        b["task_id"]
    );
    assert_eq!(batch["inspections"][0]["receipt"]["result"]["ok"], true);
    assert_eq!(
        batch["inspections"][0]["receipt"]["result"]["assignment"]["task_id"],
        b["task_id"]
    );
    assert_eq!(
        batch["inspections"][0]["receipt"]["result"]["assignment"]["status"],
        "failed"
    );
    assert!(batch["inspections"][0]["receipt"]["result"]["assignment"]["terminal_outcome"]["error"]["text"].as_str().unwrap().contains("coding_incomplete"));
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        false
    );
    assert_eq!(child(&s, &r, 0).await["pause"]["control"]["paused"], true);
    assert_eq!(child(&s, &r, 0).await["execution_held"], true);
    assert_eq!(dependencies(&db).await[0]["closed"], true);
    let current = model.coordination_calls();
    settle(&engine).await;
    assert_eq!(model.coordination_calls(), current);
    planner_pauses::manual(&s, &r, &a, 1, false).await;
    finish(&s, &engine, &r, &a).await;
    assert_eq!(join_count(&engine, &r).await, 1);
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(parent.record().delegations[0].id.to_string(), a["task_id"]);
    assert_eq!(parent.record().delegations[1].id.to_string(), b["task_id"]);
    s.close().await;
}

#[tokio::test]
async fn prerequisite_success_and_cancel_are_reviewed_without_implicit_release() {
    for mode in ["dependency_success", "dependency_stop"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("outcome.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        let a = child(&s, &r, 0).await;
        let b = child(&s, &r, 1).await;
        steer(&s, &r, "Wait for the other assignment.").await;
        settle(&engine).await;
        engine.pause(run(&r)).await.unwrap();
        let expected = if mode == "dependency_success" {
            answer(&s, &r, &b).await;
            settle(&engine).await;
            let saved = engine.status(run(&b)).await.unwrap();
            engine
                .signal(
                    run(&b),
                    saved.record().wait.as_ref().unwrap().id,
                    json!({"approved":true}),
                )
                .await
                .unwrap();
            settle(&engine).await;
            "succeeded"
        } else {
            engine.cancel(run(&b)).await.unwrap();
            settle(&engine).await;
            "cancelled"
        };
        assert_eq!(dependencies(&db).await[0]["outcome"], expected);
        assert_eq!(
            child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
            true
        );
        engine.resume(run(&r)).await.unwrap();
        settle(&engine).await;
        assert_eq!(
            work(&s, &r).await["coordination"]["batches"][0]["guidance"]["dependencies"][0]["outcome"],
            expected
        );
        let reviewed = work(&s, &r).await;
        assert_eq!(
            reviewed["coordination"]["batches"][0]["inspections"][0]["receipt"]["result"]["assignment"]
                ["terminal_outcome"]["status"],
            expected
        );
        if mode == "dependency_success" {
            assert_eq!(
                child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
                false
            );
            assert_eq!(join_count(&engine, &r).await, 0);
            finish(&s, &engine, &r, &a).await;
        } else {
            assert_eq!(child(&s, &r, 0).await["status"], "cancelled");
        }
        assert_eq!(join_count(&engine, &r).await, 1);
        s.close().await;
    }
}

#[tokio::test]
async fn dependency_actions_refuse_self_cycles_scope_and_exhausted_allowance() {
    for (mode, count, code) in [
        ("dependency_self", 1, "dependency_cycle"),
        ("dependency_scope", 1, "dependency_scope"),
        ("dependency_cycle", 2, "dependency_cycle"),
        ("dependency_limit", 4, "dependency_scope"),
    ] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("refusal.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        steer(&s, &r, "Save the dependency.").await;
        settle(&engine).await;
        let saved = planner_pauses::receipts(&db).await;
        assert_eq!(saved.len(), count);
        assert_eq!(saved.last().unwrap()["result"]["error"]["code"], code);
        assert_eq!(
            dependencies(&db).await.len(),
            usize::from(mode == "dependency_cycle")
        );
        assert_eq!(work(&s, &r).await["groups"][0]["joined"], false);
        if mode == "dependency_limit" {
            let prompts = model.requests.lock().unwrap();
            let p = prompts
                .iter()
                .rev()
                .find(|p| p["coordinating_guidance"].is_object())
                .unwrap();
            assert_eq!(p["worker_pausing"]["remaining"], 0);
            assert!(
                p["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|t| t["name"] != "wait_for_worker")
            );
        }
        s.close().await;
    }
    let model = Model::start("dependency_outside").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("outside.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    assert_eq!(
        engine.status(run(&r)).await.unwrap().record().status,
        RunStatus::Failed
    );
    assert!(dependencies(&db).await.is_empty());
    s.close().await;
}

#[tokio::test]
async fn sibling_questions_and_assignment_dependencies_reject_mixed_cycles_in_both_orders() {
    for question_first in [true, false] {
        let model = Model::start("dependency_question").await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("cycle.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        let b = child(&s, &r, 1).await;
        if question_first {
            answer(&s, &r, &b).await;
            settle(&engine).await;
        }
        steer(&s, &r, "Wait for the API worker.").await;
        settle(&engine).await;
        if question_first {
            assert_eq!(
                planner_pauses::receipts(&db).await[0]["result"]["error"]["code"],
                "dependency_cycle"
            );
            assert!(dependencies(&db).await.is_empty());
        } else {
            answer(&s, &r, &b).await;
            settle(&engine).await;
            let page = child(&s, &r, 1).await;
            assert_eq!(
                page["questions"]["receipts"][0]["error"]["code"],
                "question_cycle"
            );
            assert_eq!(dependencies(&db).await.len(), 1);
        }
        s.close().await;
    }
}
