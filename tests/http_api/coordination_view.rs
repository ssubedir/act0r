//! The console restores facts without scheduling work or consulting a model.
use super::*;

async fn view(s: &Server, r: &Value) -> Value {
    work(s, r).await["coordination"].clone()
}
async fn restart(db: &Path) -> (act0r::Engine, Server) {
    let (engine, agents) = local_app(db).await.unwrap();
    (engine, Server::start(Arc::new(agents), 2).await)
}
async fn counts(db: &Path) -> Value {
    let pool = summary::pool(db).await;
    let count: String=sqlx::query_scalar("SELECT json_object('history',(SELECT COUNT(*) FROM history),'activities',(SELECT COUNT(*) FROM activities),'attempts',(SELECT COUNT(*) FROM attempts),'timers',(SELECT COUNT(*) FROM timers),'inspections',(SELECT COUNT(*) FROM harness_assignment_inspections),'stops',(SELECT COUNT(*) FROM harness_planner_stops),'pauses',(SELECT COUNT(*) FROM harness_planner_pauses),'holds',(SELECT COUNT(*) FROM harness_planner_holds),'dependencies',(SELECT COUNT(*) FROM harness_worker_dependencies),'guidance',(SELECT COUNT(*) FROM harness_planner_guidance))")
        .fetch_one(&pool).await.unwrap();
    pool.close().await;
    serde_json::from_str(&count).unwrap()
}

#[tokio::test]
async fn coordination_view_restores_coalesced_findings_evidence_exact_deliveries_and_first_inclusion()
 {
    let model = Model::start("finding_inspect").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("coordination.db");
    let (engine, s) = restart(&db).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    engine.pause(run(&r)).await.unwrap();
    for c in [&a, &b] {
        answer(&s, &r, c).await;
        settle(&engine).await;
    }
    steer(&s, &r, "Keep the public API").await;
    let before = counts(&db).await;
    let queued = view(&s, &r).await;
    assert_eq!(queued["findings"].as_array().unwrap().len(), 2);
    assert!(queued["execution_held"].as_bool().unwrap());
    assert!(queued["batches"].as_array().unwrap().is_empty());
    assert!(
        queued["findings"]
            .as_array()
            .unwrap()
            .iter()
            .all(|f| f["status"] == "queued" && f["batch_id"].is_null())
    );
    assert_eq!(view(&s, &r).await, queued);
    assert_eq!(counts(&db).await, before);
    assert_eq!(model.coordination_calls(), 0);
    engine.resume(run(&r)).await.unwrap();
    settle(&engine).await;
    let saved = view(&s, &r).await;
    assert_eq!(saved["batches"].as_array().unwrap().len(), 1);
    let batch = &saved["batches"][0];
    assert_eq!(batch["status"], "finished");
    assert_eq!(batch["guidance"]["findings"].as_array().unwrap().len(), 2);
    assert_eq!(
        batch["guidance"]["instructions"][0]["text"],
        "Keep the public API"
    );
    assert_eq!(batch["inspections"].as_array().unwrap().len(), 1);
    let snapshot = &batch["inspections"][0]["receipt"]["result"]["assignment"];
    assert_eq!(snapshot["task_id"], b["task_id"]);
    assert_eq!(snapshot["run_id"], b["run_id"]);
    assert!(
        snapshot["recent_results"]
            .to_string()
            .contains("Saved source")
    );
    assert!(
        saved["findings"]
            .as_array()
            .unwrap()
            .iter()
            .all(|f| f["batch_id"] == batch["timer_id"] && f["status"] == "routed")
    );
    assert_eq!(batch["decision"]["deliveries"].as_array().unwrap().len(), 2);
    for (index, c) in [&a, &b].iter().enumerate() {
        let delivery = &batch["decision"]["deliveries"][index];
        assert_eq!(delivery["receipt"]["recipient_task_id"], c["task_id"]);
        assert_eq!(delivery["receipt"]["recipient_run_id"], c["run_id"]);
        assert!(delivery["application"].is_null());
        assert_eq!(delivery["recipient_terminal"], false);
    }
    let frozen = snapshot.clone();
    let calls = model.coordination_calls();
    let before = counts(&db).await;
    for _ in 0..3 {
        assert_eq!(view(&s, &r).await, saved);
    }
    assert_eq!(model.coordination_calls(), calls);
    assert_eq!(counts(&db).await, before);
    s.close().await;
    drop(engine);
    let (engine, s) = restart(&db).await;
    assert_eq!(view(&s, &r).await, saved);
    answer(&s, &r, &a).await;
    settle(&engine).await;
    let included = view(&s, &r).await;
    assert!(
        included["batches"][0]["decision"]["deliveries"][0]["application"]["activity_id"]
            .is_string()
    );
    assert!(included["batches"][0]["decision"]["deliveries"][1]["application"].is_null());
    assert_eq!(
        included["batches"][0]["inspections"][0]["receipt"]["result"]["assignment"],
        frozen
    );
    assert_eq!(model.coordination_calls(), calls);
    // A follow-up turn in the same session must not borrow this Run's coordination history.
    let response = client()
        .post(format!(
            "{}/v1/harness/sessions/{}/turns",
            s.url,
            r["session_id"].as_str().unwrap()
        ))
        .json(&json!({"message_id":act0r::domain::MessageId::new(),"prompt":"Another request"}))
        .send()
        .await
        .unwrap();
    // The first goal still owns the workers, so a queued follow-up has no planner Run yet.
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let next: Value = response.json().await.unwrap();
    let endpoint = format!(
        "{}/v1/harness/sessions/{}/turns/{}/delegated-work",
        s.url,
        r["session_id"].as_str().unwrap(),
        next["task_id"].as_str().unwrap()
    );
    assert!(get(&client(), &endpoint).await["coordination"].is_null());
    s.close().await;
}

#[tokio::test]
async fn coordination_view_shows_captured_receipts_before_tool_completion_and_restores_no_action() {
    let model = Model::start("finding_noop").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("coordination.db");
    let (engine, s) = restart(&db).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    engine.pause(run(&r)).await.unwrap();
    answer(&s, &r, &a).await;
    settle(&engine).await;
    engine.resume(run(&r)).await.unwrap();
    let mut w = engine.worker().await.unwrap();
    let routing = loop {
        let e = admit(&mut w).await;
        if e.input["definition"]["id"]["name"] == "guide_workers" {
            break e;
        }
        let out = w.execute(e.clone()).await.unwrap();
        w.complete(e, out).await.unwrap();
    };
    let reviewing = view(&s, &r).await;
    assert_eq!(reviewing["batches"][0]["status"], "reviewing");
    assert!(reviewing["batches"][0]["decision"].is_null());
    let store = SqliteStore::open(&db).await.unwrap();
    store.guide_workers(call(&routing)).await.unwrap();
    let captured = view(&s, &r).await;
    assert_eq!(captured["batches"][0]["status"], "reviewing");
    assert_eq!(
        captured["batches"][0]["decision"]["activity_status"],
        "running"
    );
    assert_eq!(captured["findings"][0]["status"], "no_action");
    assert!(
        captured["batches"][0]["decision"]["deliveries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    store.close().await;
    drop(w);
    s.close().await;
    drop(engine);
    let (engine, s) = restart(&db).await;
    let mut w = engine.worker().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    drain(&mut w).await;
    drop(w);
    let finished = view(&s, &r).await;
    assert_eq!(finished["batches"][0]["status"], "finished");
    assert_eq!(finished["findings"][0]["status"], "no_action");
    assert_eq!(model.coordination_calls(), 1);
    s.close().await;
}

#[tokio::test]
async fn coordination_view_keeps_worker_question_snapshots_out_of_guidance_reviews() {
    let model = Model::start("inspect_question").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("coordination.db");
    let (engine, s) = restart(&db).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    answer(&s, &r, &a).await;
    settle(&engine).await;
    assert_eq!(assignment_inspection::receipts(&db).await.len(), 2);
    assert!(view(&s, &r).await["batches"].as_array().unwrap().is_empty());
    steer(&s, &r, "Review the workers before routing").await;
    settle(&engine).await;
    let captured = view(&s, &r).await;
    assert_eq!(captured["batches"].as_array().unwrap().len(), 1);
    let reads = captured["batches"][0]["inspections"].as_array().unwrap();
    assert_eq!(reads.len(), 3);
    let all = assignment_inspection::receipts(&db).await;
    assert_eq!(all.len(), 5);
    for (visible, saved) in reads.iter().zip(&all[2..]) {
        assert_eq!(visible["receipt"], *saved);
    }
    s.close().await;
}
