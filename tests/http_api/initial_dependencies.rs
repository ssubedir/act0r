//! Initial ordering, preflight-only admission and the existing approved patch path.
use super::*;

pub(super) fn model_action(p: &Value, mode: &str) -> Value {
    let turn = p["turn"].as_u64().unwrap();
    if p["coordinating_guidance"].is_object() {
        if mode == "initial_failure" || mode == "initial_stop" || mode == "initial_timeout" {
            if p["worker_stopping"]["used"] == 0 {
                return json!({"kind":"call_tool","tool":"stop_worker","arguments":{"worker":"coder","reason":"The prerequisite cannot supply its implementation."}});
            }
            return json!({"kind":"call_tool","tool":"guide_workers","arguments":{"coder":null,"coder2":null}});
        }
        return worker_changes::model_action(p, "transfer_full");
    }
    if p["goal"] == "Worker 0" {
        return match turn {
            0 => json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}}),
            1 => {
                json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/ui.rs","old":"Waiting UI","new":"Integrated UI"}})
            }
            2 => json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
            _ => json!({"kind":"finish","summary":"UI and API verified together"}),
        };
    }
    if p["goal"] == "Worker 1" && mode == "initial_question" && turn == 0 {
        return json!({"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":"coder","text":"I need the dependent UI decision first","timeout_ms":30000}});
    }
    if p["goal"] == "Worker 1" && mode == "initial_failure" && turn >= 2 {
        return json!({"kind":"finish_incomplete","summary":"The API prerequisite cannot complete"});
    }
    let mut action = worker_changes::model_action(p, "transfer_full");
    if turn == 0 && action["kind"] == "delegate_tasks" {
        action["tasks"][0]["depends_on"] = json!(match mode {
            "initial_self" => "coder",
            "initial_missing" => "helper",
            _ => "coder2",
        });
        if mode == "initial_cycle" {
            action["tasks"][1]["depends_on"] = json!("coder");
        }
        if mode == "initial_scope" {
            action["tasks"][0]["writable_paths"] = json!(["src/ui.rs"]);
        }
    }
    action
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

fn receiver_calls(model: &Model) -> usize {
    model
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p["goal"] == "Worker 0")
        .count()
}

async fn approve(engine: &act0r::Engine, r: &Value, yes: bool) {
    let saved = engine.status(run(r)).await.unwrap();
    let wait = saved.record().wait.as_ref().unwrap();
    engine
        .signal(run(r), wait.id, json!({"approved":yes}))
        .await
        .unwrap();
    settle(engine).await;
}

async fn continue_source(s: &Server, engine: &act0r::Engine, r: &Value, b: &Value) {
    answer(s, r, b).await;
    settle(engine).await;
    approve(engine, b, true).await;
    approve(engine, b, true).await;
}

#[tokio::test]
async fn initial_dependency_survives_publication_and_approval_restart_then_verifies_combined_patch()
{
    let model = Model::start("initial_full").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("initial.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let request = input(&model, root.path(), None);
    fs::write(root.path().join("project/src/ui.rs"), "Waiting UI\n").unwrap();
    let mut r = post(
        &client(),
        &format!("{}/v1/harness/sessions", s.url),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    let mut w = engine.worker().await.unwrap();
    for _ in 0..40 {
        if w.advance_work().await.unwrap() || w.fire_due_timer().await.unwrap() {
            continue;
        }
        if let Some(e) = w.admit().await.unwrap() {
            let preparation =
                e.handler == act0r::domain::workspace_handoff::handler("prepare_group");
            let out = w.execute(e.clone()).await.unwrap();
            w.complete(e, out).await.unwrap();
            if preparation {
                break;
            }
        }
    }
    r["run_id"] = get(&client(), &format!("{}/steering", base(&s, &r))).await["run_id"].clone();
    let group = engine.status(run(&r)).await.unwrap().record().task_groups[0].clone();
    let pool = summary::pool(&db).await;
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM harness_initial_dependencies WHERE status='pending'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, 1);
    pool.close().await;
    s.close().await;
    drop(w);
    drop(engine);

    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    assert_eq!(receiver_calls(&model), 0);
    assert_eq!(a["pause"]["planner_hold"]["paused"], true);
    assert_eq!(a["pause"]["execution_held"], true);
    assert_eq!(a["task_id"], json!(group.children[0]));
    let receiver = engine.status(run(&a)).await.unwrap();
    assert_eq!(receiver.record().usage.admitted_model_calls, 0);
    assert!(
        receiver
            .activities()
            .iter()
            .any(|a| a.spec.handler.name == "workspace.baseline"
                && a.status == act0r::domain::ActivityStatus::Succeeded)
    );
    continue_source(&s, &engine, &r, &b).await;
    assert_eq!(receiver_calls(&model), 0);
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(
        parent.record().wait.as_ref().unwrap().payload["worker_transfer"],
        true
    );
    s.close().await;
    drop(engine);

    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(receiver_calls(&model), 0);
    approve(&engine, &r, true).await;
    assert_eq!(receiver_calls(&model), 2); // read transferred code, then request the UI edit
    approve(&engine, &a, true).await;
    approve(&engine, &a, true).await;
    assert_eq!(child(&s, &r, 0).await["status"], "succeeded");
    assert_eq!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .wait
            .as_ref()
            .unwrap()
            .key,
        "child_changes_approval"
    );
    approve(&engine, &r, true).await;
    approve(&engine, &r, true).await;
    assert_eq!(
        engine.status(run(&r)).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    assert_eq!(join_count(&engine, &r).await, 1);
    assert_eq!(
        engine
            .history(run(&r))
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "initial_dependency_bound")
            .count(),
        1
    );
    assert_eq!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .task_groups
            .len(),
        1
    );
    use act0r::application::ports::WorkspaceChangesReadPort;
    let store = SqliteStore::open(&db).await.unwrap();
    let changes = json!(
        store
            .task_changes(a["task_id"].as_str().unwrap().parse().unwrap())
            .await
            .unwrap()
    );
    store.close().await;
    assert_eq!(changes["changes"]["files"].as_array().unwrap().len(), 2);
    assert_eq!(changes["changes"]["test_status"], "matching");
    assert!(
        fs::read_to_string(root.path().join("project/src/lib.rs"))
            .unwrap()
            .contains("Saved source")
    );
    s.close().await;
}

#[tokio::test]
async fn declined_or_out_of_scope_initial_handoff_never_starts_the_receiver() {
    for mode in ["initial_full", "initial_scope"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("declined.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        fs::create_dir_all(root.path().join("project/src")).unwrap();
        fs::write(root.path().join("project/src/ui.rs"), "Waiting UI\n").unwrap();
        let r = start(&s, &engine, &model, root.path(), None).await;
        let a = child(&s, &r, 0).await;
        let b = child(&s, &r, 1).await;
        continue_source(&s, &engine, &r, &b).await;
        if mode == "initial_full" {
            approve(&engine, &r, false).await;
        }
        assert_eq!(receiver_calls(&model), 0);
        assert_eq!(
            child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
            true
        );
        assert_eq!(join_count(&engine, &r).await, 0);
        let receiver = engine.status(run(&a)).await.unwrap();
        let state = &receiver.record().state["state"];
        let path = state["config"]["workspace"].as_str().unwrap();
        assert!(
            fs::read_to_string(Path::new(path).join("src/lib.rs"))
                .unwrap()
                .contains("Saved source")
        );
        s.close().await;
    }
}

#[tokio::test]
async fn failed_or_stopped_prerequisites_wake_the_planner_to_stop_only_the_waiting_assignment() {
    for mode in ["initial_failure", "initial_stop"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("failure.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        let b = child(&s, &r, 1).await;
        if mode == "initial_failure" {
            answer(&s, &r, &b).await;
        } else {
            engine.cancel(run(&b)).await.unwrap();
        }
        settle(&engine).await;
        assert_eq!(receiver_calls(&model), 0);
        assert_eq!(child(&s, &r, 0).await["status"], "cancelled");
        assert_eq!(join_count(&engine, &r).await, 1);
        assert!(model.coordination_calls() > 0);
        s.close().await;
    }
}

#[tokio::test]
async fn self_missing_and_circular_initial_declarations_publish_no_assignments() {
    for mode in ["initial_self", "initial_missing", "initial_cycle"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("invalid.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        let parent = engine.status(run(&r)).await.unwrap();
        assert!(parent.record().delegations.is_empty());
        assert_eq!(
            parent.record().error.as_ref().unwrap().code,
            "invalid_workflow"
        );
        assert_eq!(receiver_calls(&model), 0);
        s.close().await;
    }
}

#[tokio::test]
async fn prerequisite_finishing_before_receiver_baseline_cannot_race_its_first_model_admission() {
    let model = Model::start("initial_question").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("preflight.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let request = input(&model, root.path(), None);
    fs::write(root.path().join("project/src/ui.rs"), "Waiting UI\n").unwrap();
    let mut r = post(
        &client(),
        &format!("{}/v1/harness/sessions", s.url),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    let mut w = engine.worker().await.unwrap();
    let baseline = loop {
        let e = admit(&mut w).await;
        r["run_id"] = get(&client(), &format!("{}/steering", base(&s, &r))).await["run_id"].clone();
        let parent = engine.status(run(&r)).await.unwrap();
        let receiver = parent.record().delegations.first().map(|d| d.id);
        if e.handler.name == "workspace.baseline" && receiver.is_some() {
            let pool = summary::pool(&db).await;
            let task: Option<String> =
                sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
                    .bind(e.run_id.to_string())
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
            pool.close().await;
            if task == receiver.map(|t| t.to_string()) {
                break e;
            }
        }
        let out = w.execute(e.clone()).await.unwrap();
        w.complete(e, out).await.unwrap();
    };
    drain(&mut w).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    assert_eq!(a["pause"]["initial_dependency"], b["task_id"]);
    assert_eq!(a["pause"]["execution_held"], true);
    assert_eq!(
        b["questions"]["receipts"][0]["error"]["code"],
        "question_cycle"
    );
    answer(&s, &r, &b).await;
    drain(&mut w).await;
    for _ in 0..2 {
        let source = engine.status(run(&b)).await.unwrap();
        engine
            .signal(
                run(&b),
                source.record().wait.as_ref().unwrap().id,
                json!({"approved":true}),
            )
            .await
            .unwrap();
        drain(&mut w).await;
    }
    assert_eq!(child(&s, &r, 1).await["status"], "succeeded");
    assert_eq!(receiver_calls(&model), 0);
    assert_eq!(model.coordination_calls(), 0);
    let out = w.execute(baseline.clone()).await.unwrap();
    w.complete(baseline, out).await.unwrap();
    assert!(
        w.admit().await.unwrap().is_none(),
        "first model stays held before dependency binding"
    );
    drain(&mut w).await;
    assert_eq!(receiver_calls(&model), 0);
    assert_eq!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .wait
            .as_ref()
            .unwrap()
            .payload["worker_transfer"],
        true
    );
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["revision"],
        1
    );
    s.close().await;
}

#[tokio::test]
async fn initial_handoff_keeps_manual_holds_and_saved_prerequisite_deadlines() {
    for mode in ["initial_full", "initial_timeout"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("controls.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        fs::create_dir_all(root.path().join("project/src")).unwrap();
        fs::write(root.path().join("project/src/ui.rs"), "Waiting UI\n").unwrap();
        let r = start(&s, &engine, &model, root.path(), None).await;
        let a = child(&s, &r, 0).await;
        let b = child(&s, &r, 1).await;
        if mode == "initial_full" {
            planner_pauses::manual(&s, &r, &a, 0, true).await;
            continue_source(&s, &engine, &r, &b).await;
            approve(&engine, &r, true).await;
            let held = child(&s, &r, 0).await;
            assert_eq!(held["pause"]["planner_hold"]["paused"], false);
            assert_eq!(held["pause"]["control"]["paused"], true);
            assert_eq!(receiver_calls(&model), 0);
            planner_pauses::manual(&s, &r, &a, 1, false).await;
            settle(&engine).await;
            assert!(receiver_calls(&model) > 0);
        } else {
            let pool = summary::pool(&db).await;
            sqlx::query("UPDATE runs SET payload=json_set(payload,'$.delegations[1].deadline_at',0) WHERE id=?").bind(run(&r).to_string()).execute(&pool).await.unwrap();
            sqlx::query("UPDATE agent_delegations SET deadline_at=0 WHERE id=?")
                .bind(b["task_id"].as_str().unwrap())
                .execute(&pool)
                .await
                .unwrap();
            pool.close().await;
            settle(&engine).await;
            assert_eq!(receiver_calls(&model), 0);
            assert_eq!(child(&s, &r, 0).await["status"], "cancelled");
            assert_eq!(join_count(&engine, &r).await, 1);
            let parent = engine.status(run(&r)).await.unwrap();
            assert_eq!(
                parent.record().delegations[1].outcome.as_ref().unwrap()["error"]["code"],
                "delegation_timeout"
            );
        }
        s.close().await;
    }
}

#[tokio::test]
async fn unpublished_goal_stop_and_prerequisites_stopped_before_run_allocation_never_release_initial_work()
 {
    for whole_goal in [true, false] {
        let model = Model::start("initial_full").await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("queued.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let agents = Arc::new(agents);
        let s = Server::start(agents.clone(), 2).await;
        let mut r = post(
            &client(),
            &format!("{}/v1/harness/sessions", s.url),
            &input(&model, root.path(), None),
            StatusCode::ACCEPTED,
        )
        .await;
        let mut w = engine.worker().await.unwrap();
        loop {
            let e = admit(&mut w).await;
            let prepared = e.handler == act0r::domain::workspace_handoff::handler("prepare_group");
            let out = w.execute(e.clone()).await.unwrap();
            w.complete(e, out).await.unwrap();
            if prepared {
                break;
            }
        }
        r["run_id"] = get(&client(), &format!("{}/steering", base(&s, &r))).await["run_id"].clone();
        let parent = engine.status(run(&r)).await.unwrap();
        let source = parent.record().delegations[1].id;
        if whole_goal {
            agents
                .actors()
                .unwrap()
                .cancel_goal(r["task_id"].as_str().unwrap().parse().unwrap())
                .await
                .unwrap();
        } else {
            let pool = summary::pool(&db).await;
            loop {
                let exists: bool =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_tasks WHERE id=?)")
                        .bind(source.to_string())
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                if exists {
                    break;
                }
                assert!(w.advance_work().await.unwrap());
            }
            pool.close().await;
            assert!(
                agents
                    .actors()
                    .unwrap()
                    .task(source)
                    .await
                    .unwrap()
                    .run_id
                    .is_none()
            );
            agents.actors().unwrap().cancel_task(source).await.unwrap();
        }
        drain(&mut w).await;
        assert_eq!(receiver_calls(&model), 0);
        assert_eq!(model.coordination_calls(), 0);
        let pool = summary::pool(&db).await;
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM harness_initial_dependencies WHERE status='pending'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(pending, 0);
        pool.close().await;
        if whole_goal {
            assert_eq!(
                engine.status(run(&r)).await.unwrap().record().status,
                RunStatus::Cancelled
            );
        } else {
            assert_eq!(child(&s, &r, 0).await["status"], "cancelled");
            assert_eq!(join_count(&engine, &r).await, 1);
        }
        s.close().await;
    }
}
