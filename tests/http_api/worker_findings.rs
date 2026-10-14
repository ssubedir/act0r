//! Worker reports wake the same planner without replacing its original child join.
use super::*;
use act0r::domain::tools::NativeToolResult;

async fn send(s: &Server, r: &Value, e: &act0r::Engine, child_index: usize) {
    let c = child(s, r, child_index).await;
    answer(s, r, &c).await;
    settle(e).await;
}
async fn restart(db: &Path) -> (act0r::Engine, Server) {
    let (engine, agents) = local_app(db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    (engine, s)
}

#[tokio::test]
async fn worker_findings_coalesce_with_guidance_over_pause_restart_and_restore_original_join_once()
{
    let model = Model::start("finding_inspect").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("findings.db");
    let (engine, s) = restart(&db).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let original = engine.status(run(&r)).await.unwrap();
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    engine.pause(run(&r)).await.unwrap();
    send(&s, &r, &engine, 0).await;
    send(&s, &r, &engine, 1).await;
    steer(&s, &r, "Keep the public API").await;
    assert_eq!(model.coordination_calls(), 0);
    let paused = engine.status(run(&r)).await.unwrap();
    assert_eq!(paused.record().peer_messages.len(), 2);
    assert_eq!(paused.record().findings_handled_through, 0);
    assert_eq!(
        paused
            .timers()
            .iter()
            .filter(|t| t.payload["finding_sequence"].is_number())
            .count(),
        2
    );
    s.close().await;
    drop(engine);
    let (engine, s) = restart(&db).await;
    engine.resume(run(&r)).await.unwrap();
    settle(&engine).await;
    let saved = engine.status(run(&r)).await.unwrap();
    assert_eq!(saved.record().status, RunStatus::Waiting);
    assert!(saved.record().planner_guidance.is_none());
    assert_eq!(saved.record().findings_handled_through, 2);
    assert_eq!(saved.record().guidance_handled_through, 1);
    assert_eq!(
        saved.record().task_groups[0].id,
        original.record().task_groups[0].id
    );
    assert_eq!(
        saved
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
    assert!(saved.record().task_groups[0].joined_at.is_none());
    assert_eq!(model.coordination_calls(), 2); // One read decision and one routing decision.
    {
        let seen = model.requests.lock().unwrap();
        let p = seen
            .iter()
            .find(|p| p["coordinating_guidance"].is_object())
            .unwrap();
        assert_eq!(
            p["coordinating_guidance"]["findings"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            p["coordinating_guidance"]["findings"][0]["sender_task_id"],
            a["task_id"]
        );
        assert_eq!(
            p["coordinating_guidance"]["findings"][1]["sender_task_id"],
            b["task_id"]
        );
        assert_eq!(
            p["coordinating_guidance"]["instructions"][0]["text"],
            "Keep the public API"
        );
        let last = seen
            .iter()
            .rfind(|p| p["coordinating_guidance"].is_object())
            .unwrap();
        assert!(last.to_string().contains("recent_results"));
    }
    for c in [&a, &b] {
        let worker = engine.status(run(c)).await.unwrap();
        assert_eq!(worker.record().peer_messages.len(), 1);
        assert_eq!(worker.record().peer_messages[0].sender, "parent");
        assert!(!worker.record().peer_messages[0].needs_coordination);
    }
    answer(&s, &r, &a).await;
    answer(&s, &r, &b).await;
    settle(&engine).await;
    assert!(work(&s, &r).await["groups"][0]["joined"].as_bool().unwrap());
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
    assert_eq!(model.coordination_calls(), 2);
    s.close().await;
}

#[tokio::test]
async fn worker_findings_receipt_and_wake_roll_back_and_replay_after_interrupted_sender_completion()
{
    let model = Model::start("finding_noop").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("findings.db");
    let (engine, s) = restart(&db).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    engine.pause(run(&r)).await.unwrap();
    answer(&s, &r, &a).await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await; // Worker selects the report.
    let e = admit(&mut w).await;
    assert_eq!(e.input["definition"]["id"]["version"], 2);
    assert_eq!(e.input["arguments"]["needs_coordination"], true);
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER finding_rollback BEFORE INSERT ON harness_worker_messages BEGIN SELECT RAISE(ABORT,'finding rollback'); END;").execute(&pool).await.unwrap();
    let store = SqliteStore::open(&db).await.unwrap();
    assert!(store.send_worker_message(call(&e)).await.is_err());
    let parent = engine.status(run(&r)).await.unwrap();
    assert!(parent.record().peer_messages.is_empty());
    assert!(
        parent
            .timers()
            .iter()
            .all(|t| t.payload["finding_sequence"].is_null())
    );
    sqlx::raw_sql("DROP TRIGGER finding_rollback")
        .execute(&pool)
        .await
        .unwrap();
    let NativeToolResult::Success(saved) = store.send_worker_message(call(&e)).await.unwrap()
    else {
        panic!("report refused")
    };
    assert_eq!(saved["receipt"]["recipient_task_id"], r["task_id"]);
    let mut changed = call(&e);
    changed.arguments["needs_coordination"] = json!(false);
    assert!(store.send_worker_message(changed).await.is_err());
    let mut changed = call(&e);
    changed.run_id = run(&r);
    assert!(store.send_worker_message(changed).await.is_err());
    pool.close().await;
    store.close().await;
    drop(w);
    s.close().await;
    drop(engine);
    let (engine, s) = restart(&db).await;
    let store = SqliteStore::open(&db).await.unwrap();
    let NativeToolResult::Success(replayed) = store.send_worker_message(call(&e)).await.unwrap()
    else {
        panic!("replay refused")
    };
    assert_eq!(saved, replayed);
    engine.resume(run(&r)).await.unwrap();
    settle(&engine).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(parent.record().peer_messages.len(), 1);
    assert_eq!(
        parent
            .timers()
            .iter()
            .filter(|t| t.payload["finding_sequence"].is_number())
            .count(),
        1
    );
    assert_eq!(parent.record().findings_handled_through, 1);
    assert_eq!(model.coordination_calls(), 1);
    assert!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .peer_messages
            .is_empty()
    );
    store.close().await;
    s.close().await;
}

#[tokio::test]
async fn worker_findings_ordinary_and_nullable_flags_do_not_wake_and_sibling_flags_are_saved_refusals()
 {
    for mode in ["finding_ordinary", "finding_null", "finding_sibling"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("findings.db");
        let (engine, s) = restart(&db).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        send(&s, &r, &engine, 0).await;
        assert_eq!(model.coordination_calls(), 0);
        let item = child(&s, &r, 0).await["messages"]["items"][0].clone();
        if mode == "finding_sibling" {
            assert_eq!(item["receipt"]["error"]["code"], "message_coordination");
            assert!(
                engine
                    .status(run(&r))
                    .await
                    .unwrap()
                    .record()
                    .peer_messages
                    .is_empty()
            );
            assert!(
                child(&s, &r, 1).await["messages"]["items"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
        } else {
            assert!(item["receipt"]["error"].is_null());
            assert_eq!(
                engine
                    .status(run(&r))
                    .await
                    .unwrap()
                    .record()
                    .peer_messages
                    .len(),
                1
            );
        }
        s.close().await;
    }
}

#[tokio::test]
async fn worker_findings_later_report_queues_next_step_without_repeating_handled_guidance() {
    let model = Model::start("finding_repeat").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("findings.db");
    let (engine, s) = restart(&db).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    engine.pause(run(&r)).await.unwrap();
    send(&s, &r, &engine, 0).await;
    steer(&s, &r, "Preserve the API once").await;
    steer(&s, &r, "Preserve the API twice").await;
    engine.resume(run(&r)).await.unwrap();
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await; // First batch is frozen in the admitted routing decision.
    let routing = admit(&mut w).await;
    assert_eq!(routing.input["definition"]["id"]["name"], "guide_workers");
    let a = child(&s, &r, 0).await;
    answer(&s, &r, &a).await; // Another finding while routing is admitted.
    drain(&mut w).await;
    assert_eq!(model.coordination_calls(), 1);
    let out = w.execute(routing.clone()).await.unwrap();
    w.complete(routing, out).await.unwrap();
    drain(&mut w).await;
    assert_eq!(model.coordination_calls(), 2);
    let coordination = work(&s, &r).await["coordination"].clone();
    assert_eq!(coordination["batches"].as_array().unwrap().len(), 2);
    assert_ne!(
        coordination["batches"][0]["timer_id"],
        coordination["batches"][1]["timer_id"]
    );
    assert_eq!(
        coordination["batches"][0]["guidance"]["findings"][0]["text"],
        "A later finding needs coordination"
    );
    assert!(
        coordination["batches"][0]["guidance"]["instructions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    {
        let seen = model.requests.lock().unwrap();
        let batches = seen
            .iter()
            .filter(|p| p["coordinating_guidance"].is_object())
            .collect::<Vec<_>>();
        assert_eq!(
            batches[0]["coordinating_guidance"]["findings"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            batches[1]["coordinating_guidance"]["findings"][0]["text"],
            "A later finding needs coordination"
        );
        assert!(
            batches[1]["coordinating_guidance"]["instructions"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .findings_handled_through,
        2
    );
    s.close().await;
}

#[tokio::test]
async fn worker_findings_respect_shared_budget_and_whole_goal_stop_before_and_after_admission() {
    for stage in 0..3 {
        let model = Model::start("finding_noop").await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("findings.db");
        let (engine, s) = restart(&db).await;
        let r = start(
            &s,
            &engine,
            &model,
            root.path(),
            if stage == 0 {
                Some(json!({"max_model_calls":6,"max_tokens":null}))
            } else {
                None
            },
        )
        .await;
        let a = child(&s, &r, 0).await;
        engine.pause(run(&r)).await.unwrap();
        answer(&s, &r, &a).await;
        let mut w = engine.worker().await.unwrap();
        step(&mut w).await; // Sixth call selects the finding.
        step(&mut w).await; // Commit the report; leave its next model unadmitted.
        if stage == 0 {
            engine.pause(run(&a)).await.unwrap(); // Isolate the planner's admission from the sender's queued continuation.
            engine.resume(run(&r)).await.unwrap();
            drop(w);
            settle(&engine).await;
            let parent = engine.status(run(&r)).await.unwrap();
            assert_eq!(parent.record().status, RunStatus::BudgetExhausted);
            assert!(parent.record().peer_messages[0].application.is_none());
            assert_eq!(model.coordination_calls(), 0);
            let coordination = work(&s, &r).await["coordination"].clone();
            assert_eq!(coordination["batches"][0]["status"], "budget_exhausted");
            assert_eq!(coordination["findings"][0]["status"], "budget_exhausted");
        } else {
            engine.pause(run(&a)).await.unwrap();
            let admitted = if stage == 2 {
                engine.resume(run(&r)).await.unwrap();
                step(&mut w).await;
                Some(admit(&mut w).await)
            } else {
                None
            };
            post(
                &client(),
                &format!(
                    "{}/v1/teams/{}/tasks/{}/cancel",
                    s.url,
                    r["team_id"].as_str().unwrap(),
                    r["task_id"].as_str().unwrap()
                ),
                &json!({}),
                StatusCode::OK,
            )
            .await;
            drain(&mut w).await;
            if let Some(e) = admitted {
                let out = w.execute(e.clone()).await.unwrap();
                w.complete(e, out).await.unwrap();
                drain(&mut w).await;
            }
            assert_eq!(
                engine.status(run(&r)).await.unwrap().record().status,
                RunStatus::Cancelled
            );
            let coordination = work(&s, &r).await["coordination"].clone();
            if stage == 1 {
                assert!(coordination["batches"].as_array().unwrap().is_empty());
                assert_eq!(coordination["findings"][0]["status"], "stopped");
            } else {
                assert_eq!(coordination["batches"][0]["status"], "stopped");
                assert_eq!(coordination["findings"][0]["status"], "no_action");
            }
            assert_eq!(model.coordination_calls(), usize::from(stage == 2));
        }
        assert!(
            engine
                .status(run(&a))
                .await
                .unwrap()
                .record()
                .peer_messages
                .is_empty()
        );
        let b = child(&s, &r, 1).await;
        assert!(
            engine
                .status(run(&b))
                .await
                .unwrap()
                .record()
                .peer_messages
                .is_empty()
        );
        s.close().await;
    }
}

#[tokio::test]
async fn worker_findings_closed_sender_can_inform_sibling_but_closed_join_cannot_reach_later_assignment()
 {
    for later in [false, true] {
        let model = Model::start(if later {
            "finding_reassign"
        } else {
            "finding_both"
        })
        .await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("findings.db");
        let (engine, s) = restart(&db).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        engine.pause(run(&r)).await.unwrap();
        send(&s, &r, &engine, 0).await;
        let a = child(&s, &r, 0).await;
        answer(&s, &r, &a).await;
        settle(&engine).await; // Finish the reporting worker.
        assert_eq!(child(&s, &r, 0).await["status"], "failed");
        if later {
            let b = child(&s, &r, 1).await;
            answer(&s, &r, &b).await;
            settle(&engine).await; // It sends one more report.
            answer(&s, &r, &b).await;
            settle(&engine).await; // Finish the original join.
        }
        engine.resume(run(&r)).await.unwrap();
        settle(&engine).await;
        if later {
            assert_eq!(model.coordination_calls(), 0);
            let coordination = work(&s, &r).await["coordination"].clone();
            assert!(coordination["batches"].as_array().unwrap().is_empty());
            assert!(
                coordination["findings"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|f| f["status"] == "skipped" && f["batch_id"].is_null())
            );
            let parent = engine.status(run(&r)).await.unwrap();
            assert_eq!(parent.record().delegations.len(), 3);
            assert_eq!(parent.record().findings_handled_through, 2);
            let replacement = parent.record().delegations.last().unwrap();
            assert_ne!(
                replacement.id,
                a["task_id"].as_str().unwrap().parse().unwrap()
            );
            let item = work(&s, &r).await;
            let c = item["groups"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|g| g["children"].as_array().unwrap())
                .find(|c| c["task_id"] == json!(replacement.id))
                .unwrap();
            assert!(
                engine
                    .status(run(c))
                    .await
                    .unwrap()
                    .record()
                    .peer_messages
                    .is_empty()
            );
        } else {
            assert_eq!(model.coordination_calls(), 1);
            {
                let seen = model.requests.lock().unwrap();
                let g = &seen
                    .iter()
                    .find(|p| p["coordinating_guidance"].is_object())
                    .unwrap()["coordinating_guidance"];
                assert_eq!(g["findings"][0]["sender_task_id"], a["task_id"]);
                assert_eq!(g["targets"].as_array().unwrap().len(), 1);
                assert_eq!(g["targets"][0]["name"], "coder2");
            }
            let b = child(&s, &r, 1).await;
            assert_eq!(
                engine
                    .status(run(&b))
                    .await
                    .unwrap()
                    .record()
                    .peer_messages
                    .len(),
                1
            );
        }
        s.close().await;
    }
}

#[tokio::test]
async fn worker_findings_and_planner_questions_run_serially_in_the_same_run() {
    let model = Model::start("finding_question").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("findings.db");
    let (engine, s) = restart(&db).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    engine.pause(run(&r)).await.unwrap();
    send(&s, &r, &engine, 0).await;
    send(&s, &r, &engine, 1).await;
    let b = child(&s, &r, 1).await;
    assert!(b["questions"]["items"][0]["outcome"].is_null());
    engine.resume(run(&r)).await.unwrap();
    settle(&engine).await;
    assert_eq!(
        child(&s, &r, 1).await["questions"]["items"][0]["outcome"]["status"],
        "answered"
    );
    assert_eq!(model.coordination_calls(), 1);
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(parent.record().findings_handled_through, 1);
    assert_eq!(parent.record().status, RunStatus::Waiting);
    assert!(parent.record().planner_question.is_none());
    assert!(parent.record().planner_guidance.is_none());
    assert!(parent.record().task_groups[0].joined_at.is_none());
    let history = engine.history(run(&r)).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|e| e.kind == "planner_question_finished")
            .count(),
        1
    );
    assert_eq!(
        history
            .iter()
            .filter(|e| e.kind == "planner_guidance_finished")
            .count(),
        1
    );
    s.close().await;
}
