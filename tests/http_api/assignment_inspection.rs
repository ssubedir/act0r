//! Exact assignment evidence through real planner/model/native-tool checkpoints.
use super::*;
use act0r::domain::tools::NativeToolResult;

pub(super) async fn receipts(db: &Path) -> Vec<Value> {
    let pool = summary::pool(db).await;
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT payload FROM harness_assignment_inspections ORDER BY rowid")
            .fetch_all(&pool)
            .await
            .unwrap();
    pool.close().await;
    rows.iter()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect()
}
#[tokio::test]
async fn assignment_inspection_receipt_rolls_back_then_replays_frozen_evidence_after_restart() {
    let model = Model::start("inspect_guidance").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspect.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    let original = engine.status(run(&r)).await.unwrap();
    steer(&s, &r, "Inspect the API work before routing guidance").await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await;
    let read = admit(&mut w).await;
    assert_eq!(read.input["definition"]["id"]["name"], "inspect_assignment");
    let store = SqliteStore::open(&db).await.unwrap();
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER inspection_rollback BEFORE INSERT ON harness_assignment_inspections BEGIN SELECT RAISE(ABORT,'snapshot rollback'); END;").execute(&pool).await.unwrap();
    assert!(store.inspect_assignment(call(&read)).await.is_err());
    assert!(receipts(&db).await.is_empty());
    sqlx::raw_sql("DROP TRIGGER inspection_rollback")
        .execute(&pool)
        .await
        .unwrap();
    let NativeToolResult::Success(saved) = store.inspect_assignment(call(&read)).await.unwrap()
    else {
        panic!("read refused")
    };
    let coordination = work(&s, &r).await["coordination"].clone();
    assert_eq!(
        coordination["batches"][0]["inspections"][0]["receipt"]["result"],
        saved
    );
    assert_eq!(
        coordination["batches"][0]["inspections"][0]["activity_status"],
        "running"
    );
    assert!(coordination["batches"][0]["decision"].is_null());
    assert_eq!(saved["assignment"]["task_id"], a["task_id"]);
    assert_eq!(saved["assignment"]["run_id"], a["run_id"]);
    assert_eq!(saved["assignment"]["status"], "waiting");
    assert_eq!(saved["assignment"]["pending_wait"]["kind"], "question");
    assert!(
        saved["assignment"]["recent_results"]
            .to_string()
            .contains("Saved source")
    );
    assert!(
        saved["assignment"]["received_messages"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let mut changed = call(&read);
    changed.arguments["worker"] = json!("coder2");
    assert!(store.inspect_assignment(changed).await.is_err());
    let mut changed = call(&read);
    changed.run_id = run(&b);
    assert!(store.inspect_assignment(changed).await.is_err());
    engine.pause(run(&a)).await.unwrap();
    answer(&s, &r, &a).await;
    assert!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .wait
            .is_none()
    );
    store.close().await;
    pool.close().await;
    drop(w);
    s.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut w = engine.worker().await.unwrap();
    let retry = admit(&mut w).await;
    assert_eq!(retry.activity_id, read.activity_id);
    assert_ne!(retry.attempt_id, read.attempt_id);
    let out = w.execute(retry.clone()).await.unwrap();
    assert_eq!(out.result().unwrap(), &saved);
    w.complete(retry, out).await.unwrap();
    let next = admit(&mut w).await;
    let p: Value = serde_json::from_str(next.input["prompt"].as_str().unwrap()).unwrap();
    assert_eq!(p["assignment_inspection"]["used"], 1);
    assert!(
        p["observations"]
            .to_string()
            .contains(&a["task_id"].as_str().unwrap().to_string())
    );
    let out = w.execute(next.clone()).await.unwrap();
    w.complete(next, out).await.unwrap();
    drain(&mut w).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(parent.record().status, RunStatus::Waiting);
    assert_eq!(
        parent.record().task_groups[0].id,
        original.record().task_groups[0].id
    );
    assert_eq!(
        parent
            .record()
            .delegations
            .iter()
            .map(|d| d.id)
            .collect::<Vec<_>>(),
        original
            .record()
            .delegations
            .iter()
            .map(|d| d.id)
            .collect::<Vec<_>>()
    );
    assert!(
        parent.record().state["state"]
            .get("assignment_inspections")
            .is_none()
    );
    let saved_receipts = receipts(&db).await;
    assert_eq!(saved_receipts.len(), 3);
    assert_eq!(saved_receipts[0]["result"], saved);
    assert_eq!(
        saved_receipts[1]["result"]["assignment"]["task_id"],
        b["task_id"]
    );
    s.close().await;
}

#[tokio::test]
async fn assignment_inspection_question_observes_waits_and_returns_to_the_original_join() {
    let model = Model::start("inspect_question").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspect.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    let original = engine.status(run(&r)).await.unwrap();
    answer(&s, &r, &a).await;
    settle(&engine).await;
    let reads = receipts(&db).await;
    assert_eq!(reads.len(), 2);
    assert_eq!(reads[0]["result"]["assignment"]["task_id"], a["task_id"]);
    assert!(reads[0]["result"]["assignment"]["worker_reply_wait"].is_object());
    assert_eq!(reads[1]["result"]["assignment"]["task_id"], b["task_id"]);
    assert!(reads[1]["result"]["assignment"]["pending_wait"].is_object());
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(parent.record().status, RunStatus::Waiting);
    assert!(parent.record().planner_question.is_none());
    assert_eq!(
        parent.record().task_groups[0].id,
        original.record().task_groups[0].id
    );
    let history = child(&s, &r, 0).await;
    assert_eq!(
        history["questions"]["items"][0]["outcome"]["status"],
        "answered"
    );
    assert!(
        engine
            .status(run(&b))
            .await
            .unwrap()
            .record()
            .wait
            .is_some()
    );
    s.close().await;
}

#[tokio::test]
async fn assignment_inspection_refusals_are_saved_and_single_join_cannot_inspect_another_actor() {
    for mode in ["inspect_refuse", "inspect_single"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("inspect.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        let a = child(&s, &r, 0).await;
        steer(&s, &r, "Use only this assignment's evidence").await;
        settle(&engine).await;
        let reads = receipts(&db).await;
        let index = if mode == "inspect_refuse" { 0 } else { 1 };
        assert_eq!(reads[index]["result"]["ok"], false);
        assert!(
            reads
                .iter()
                .any(|r| r["result"]["assignment"]["task_id"] == a["task_id"])
        );
        assert_eq!(
            engine.status(run(&r)).await.unwrap().record().status,
            RunStatus::Waiting
        );
        assert!(
            engine
                .status(run(&a))
                .await
                .unwrap()
                .record()
                .wait
                .is_some()
        );
        assert_eq!(
            engine
                .status(run(&r))
                .await
                .unwrap()
                .record()
                .delegations
                .len(),
            if mode == "inspect_single" { 1 } else { 2 }
        );
        s.close().await;
    }
}

#[tokio::test]
async fn assignment_inspection_limit_and_coordination_scope_are_enforced_before_dispatch() {
    for mode in ["inspect_limit", "inspect_outside"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("inspect.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        if mode == "inspect_limit" {
            steer(&s, &r, "Do not poll").await;
            settle(&engine).await;
        }
        let parent = engine.status(run(&r)).await.unwrap();
        assert_eq!(parent.record().status, RunStatus::Failed);
        assert_eq!(
            parent.record().error.as_ref().unwrap().code,
            "model_invalid_action"
        );
        assert_eq!(
            receipts(&db).await.len(),
            if mode == "inspect_limit" { 3 } else { 0 }
        );
        let first = model.requests.lock().unwrap()[0].clone();
        assert!(
            !first["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|t| t["name"] == "inspect_assignment")
        );
        s.close().await;
    }
}

#[tokio::test]
async fn assignment_inspection_of_terminal_children_retains_outcomes_and_joins_once() {
    let model = Model::start("inspect_guidance").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspect.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    steer(&s, &r, "Inspect before guiding").await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await;
    let read = admit(&mut w).await;
    for _ in 0..2 {
        answer(&s, &r, &a).await;
        answer(&s, &r, &b).await;
        drain(&mut w).await;
    }
    assert!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .delegations
            .iter()
            .all(|d| d.outcome.is_some())
    );
    assert!(
        engine.status(run(&r)).await.unwrap().record().task_groups[0]
            .joined_at
            .is_none()
    );
    let out = w.execute(read.clone()).await.unwrap();
    assert_eq!(out.result().unwrap()["assignment"]["status"], "failed");
    assert_eq!(out.result().unwrap()["assignment"]["task_id"], a["task_id"]);
    w.complete(read, out).await.unwrap();
    drain(&mut w).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(
        parent.record().error.as_ref().unwrap().code,
        "coding_incomplete"
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
    assert_eq!(receipts(&db).await.len(), 3);
    s.close().await;
}

#[tokio::test]
async fn assignment_inspection_obeys_shared_budget_and_stop_after_admission() {
    for stop in [false, true] {
        let model = Model::start("inspect_guidance").await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("inspect.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(
            &s,
            &engine,
            &model,
            root.path(),
            if stop {
                None
            } else {
                Some(json!({"max_model_calls":6,"max_tokens":1_000_000}))
            },
        )
        .await;
        steer(&s, &r, "Inspect this worker").await;
        if stop {
            let mut w = engine.worker().await.unwrap();
            step(&mut w).await;
            let read = admit(&mut w).await;
            engine.cancel(run(&r)).await.unwrap();
            let out = w.execute(read.clone()).await.unwrap();
            w.complete(read, out).await.unwrap();
            drain(&mut w).await;
            assert_eq!(
                receipts(&db).await[0]["result"]["error"]["code"],
                "assignment_stopped"
            );
        } else {
            settle(&engine).await;
            assert_eq!(
                engine.status(run(&r)).await.unwrap().record().status,
                RunStatus::BudgetExhausted
            );
            assert_eq!(receipts(&db).await.len(), 1);
        }
        assert!(
            engine
                .history(run(&r))
                .await
                .unwrap()
                .iter()
                .all(|e| e.kind != "planner_guidance_finished")
        );
        s.close().await;
    }
}

#[tokio::test]
async fn assignment_inspection_excerpts_are_bounded_utf8_and_report_omission() {
    let model = Model::start("inspect_large").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspect.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let body = input(&model, root.path(), None);
    fs::write(
        root.path().join("project/src/lib.rs"),
        "雪雪\n".repeat(2000),
    )
    .unwrap();
    let mut r = post(
        &client(),
        &format!("{}/v1/harness/sessions", s.url),
        &body,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    r["run_id"] = get(&client(), &format!("{}/steering", base(&s, &r))).await["run_id"].clone();
    steer(&s, &r, "Inspect saved source evidence").await;
    settle(&engine).await;
    let reads = receipts(&db).await;
    assert_eq!(reads.len(), 3);
    for read in reads {
        assert!(serde_json::to_vec(&read["result"]).unwrap().len() <= 16_384);
        let preview = &read["result"]["assignment"]["recent_results"][0]["result"];
        assert_eq!(preview["truncated"], true);
        assert!(preview["text"].as_str().unwrap().len() <= 768);
        assert!(preview["bytes"].as_u64().unwrap() > 768);
    }
    s.close().await;
}

#[tokio::test]
async fn assignment_inspection_admitted_read_settles_a_closed_question_without_a_reply() {
    let model = Model::start("inspect_question").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspect.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    answer(&s, &r, &a).await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await; // worker model chooses its question
    step(&mut w).await; // question acceptance and durable parent wake
    step(&mut w).await; // planner selects the evidence read
    let read = admit(&mut w).await;
    assert_eq!(read.input["definition"]["id"]["name"], "inspect_assignment");
    engine.cancel(run(&a)).await.unwrap();
    drain(&mut w).await;
    let out = w.execute(read.clone()).await.unwrap();
    assert_eq!(out.result().unwrap()["assignment"]["status"], "cancelled");
    w.complete(read, out).await.unwrap();
    drain(&mut w).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert!(parent.record().planner_question.is_none());
    assert_eq!(parent.record().status, RunStatus::Waiting);
    assert_eq!(receipts(&db).await.len(), 1);
    assert!(
        !parent
            .activities()
            .iter()
            .any(|a| a.spec.input["definition"]["id"]["name"] == "reply_worker")
    );
    s.close().await;
}

#[tokio::test]
async fn assignment_inspection_paused_coordination_restarts_with_its_saved_count_and_evidence() {
    let model = Model::start("inspect_guidance").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspect.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    steer(&s, &r, "Inspect and pause safely").await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await;
    let read = admit(&mut w).await;
    engine.pause(run(&r)).await.unwrap();
    let out = w.execute(read.clone()).await.unwrap();
    w.complete(read, out).await.unwrap();
    assert_eq!(
        engine.status(run(&r)).await.unwrap().record().status,
        RunStatus::Paused
    );
    assert_eq!(receipts(&db).await.len(), 1);
    drop(w);
    s.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(receipts(&db).await.len(), 1);
    engine.resume(run(&r)).await.unwrap();
    let mut w = engine.worker().await.unwrap();
    let decision = admit(&mut w).await;
    let p: Value = serde_json::from_str(decision.input["prompt"].as_str().unwrap()).unwrap();
    assert_eq!(p["assignment_inspection"]["used"], 1);
    assert!(p["observations"].to_string().contains("recent_results"));
    let out = w.execute(decision.clone()).await.unwrap();
    w.complete(decision, out).await.unwrap();
    drain(&mut w).await;
    assert_eq!(receipts(&db).await.len(), 3);
    assert_eq!(
        engine.status(run(&r)).await.unwrap().record().status,
        RunStatus::Waiting
    );
    s.close().await;
}

#[tokio::test]
async fn assignment_inspection_shows_approval_and_fresh_child_tests_without_granting_parent_evidence()
 {
    let model = Model::start("inspect_tests").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspect.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    steer(&s, &r, "Inspect the test approval wait").await;
    settle(&engine).await;
    let reads = receipts(&db).await;
    assert_eq!(
        reads[0]["result"]["assignment"]["pending_wait"]["kind"],
        "tool_approval"
    );
    assert_eq!(
        reads[0]["result"]["assignment"]["pending_wait"]["tool"],
        "run_tests"
    );
    assert!(reads[0]["result"]["assignment"]["test_status"].is_null());
    let wait = engine
        .status(run(&a))
        .await
        .unwrap()
        .record()
        .wait
        .clone()
        .unwrap();
    engine
        .signal(run(&a), wait.id, json!({"approved":true}))
        .await
        .unwrap();
    settle(&engine).await;
    steer(&s, &r, "Inspect committed test evidence").await;
    settle(&engine).await;
    let reads = receipts(&db).await;
    assert_eq!(
        reads[3]["result"]["assignment"]["test_status"]["passed"],
        true
    );
    assert_eq!(
        reads[3]["result"]["assignment"]["test_status"]["stale"],
        false
    );
    assert!(
        reads[3]["result"]["assignment"]["received_messages"]
            .to_string()
            .contains("Keep the existing API")
    );
    assert!(
        engine.status(run(&r)).await.unwrap().record().state["state"]["context"]["test_status"]
            .is_null()
    );
    s.close().await;
}

#[tokio::test]
async fn assignment_inspection_reads_recent_outgoing_findings_with_their_exact_sender_task() {
    let model = Model::start("inspect_messages").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspect.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    steer(&s, &r, "Consider the saved findings").await;
    settle(&engine).await;
    let reads = receipts(&db).await;
    let snapshot = &reads[0]["result"]["assignment"];
    assert_eq!(snapshot["task_id"], a["task_id"]);
    assert_eq!(snapshot["sent_messages"][0]["recipient"], "parent");
    assert_eq!(
        snapshot["sent_messages"][0]["text"]["text"],
        "Preserve display_name; saved worker finding."
    );
    assert_eq!(snapshot["sent_messages"][0]["text"]["truncated"], false);
    assert_eq!(snapshot["sent_messages_truncated"], false);
    s.close().await;
}
