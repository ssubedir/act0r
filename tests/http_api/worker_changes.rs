//! Real native edits, immutable sibling artifacts, approvals and exact held task recovery.
use super::*;
use act0r::{
    application::ports::WorkerChangesReadPort,
    domain::{child_changes::Inspection, worker_changes::InspectWorkerChanges},
};
use std::path::PathBuf;

pub(super) fn model_action(p: &Value, mode: &str) -> Value {
    let turn = p["turn"].as_u64().unwrap();
    if p["coordinating_guidance"].is_object() {
        let ready = p["coordinating_guidance"]["dependencies"]
            .as_array()
            .is_some_and(|v| !v.is_empty());
        let pauses = p["worker_pausing"]["used"].as_u64().unwrap_or(0);
        let inspections = p["worker_changes"]["inspections_used"]
            .as_u64()
            .unwrap_or(0);
        let imports = p["worker_changes"]["imports_used"].as_u64().unwrap_or(0);
        let observed = p["observations"].as_array().unwrap();
        if !ready && pauses == 0 {
            return json!({"kind":"call_tool","tool":"wait_for_worker","arguments":{"worker":"coder","dependency":"coder2","reason":"Wait for the saved API implementation."}});
        }
        if ready && mode == "transfer_scope_action" {
            return json!({"kind":"inspect_worker_changes","worker":"coder2","offset":0});
        }
        if ready && inspections == 0 {
            return json!({"kind":"inspect_worker_changes","worker":"coder","offset":0});
        }
        let inspected = observed
            .iter()
            .rev()
            .find(|o| o["action"]["kind"] == "inspect_worker_changes");
        if ready
            && imports == 0
            && inspected
                .is_some_and(|o| o["result"]["ok"] == true && o["result"]["conflicted"] == false)
        {
            return json!({"kind":"import_worker_changes","worker":"coder"});
        }
        let applied = observed
            .iter()
            .rev()
            .find(|o| o["action"]["kind"] == "import_worker_changes")
            .is_some_and(|o| o["result"]["status"] == "applied");
        if ready && applied && pauses == 0 && mode != "transfer_hold" {
            return json!({"kind":"call_tool","tool":"resume_worker","arguments":{"worker":"coder","reason":"Read the transferred API and run fresh tests."}});
        }
        return json!({"kind":"call_tool","tool":"guide_workers","arguments":{"coder":if applied {json!("Read the transferred API. Implement the UI and run fresh tests; your patch includes the API dependency.")} else {Value::Null},"coder2":null}});
    }
    if p["goal"].as_str().unwrap().starts_with("Worker") {
        if p["goal"] == "Worker 1" {
            return match turn {
                0 => {
                    json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}})
                }
                1 => json!({"kind":"ask_human","question":"Continue API implementation?"}),
                2 => {
                    json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"Saved source","new":"Transferred API"}})
                }
                3 => json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
                _ => json!({"kind":"finish","summary":"API implementation verified"}),
            };
        }
        return match turn {
            0 => json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
            1 => json!({"kind":"ask_human","question":"Continue waiting UI assignment?"}),
            2 => json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}}),
            3 if mode == "transfer_old_tests" => {
                json!({"kind":"finish","summary":"Earlier tests should be rejected"})
            }
            3 => {
                json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/ui.rs","old":"Waiting UI","new":"Integrated UI"}})
            }
            4 => json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
            _ => json!({"kind":"finish","summary":"UI and API verified together"}),
        };
    }
    if turn == 0 {
        return if mode == "transfer_outside" {
            json!({"kind":"inspect_worker_changes","worker":"coder","offset":0})
        } else {
            json!({"kind":"delegate_tasks","tasks":[{"worker":"coder","goal":"Worker 0","writable_paths":if mode=="transfer_write_scope" {json!(["src/ui.rs"])} else {json!(["src/"])}},{"worker":"coder2","goal":"Worker 1","writable_paths":["src/"]}]})
        };
    }
    if mode == "transfer_full" {
        let observations = p["observations"].as_array().unwrap();
        if !observations
            .iter()
            .any(|o| o["action"]["kind"] == "inspect_child_changes")
        {
            return json!({"kind":"inspect_child_changes","worker":"coder","offset":0});
        }
        if !observations
            .iter()
            .any(|o| o["action"]["kind"] == "import_child_changes")
        {
            return json!({"kind":"import_child_changes","worker":"coder"});
        }
        if !observations
            .iter()
            .any(|o| o["action"]["tool"] == "run_tests" || o["action"]["kind"] == "run_tests")
        {
            return json!({"kind":"call_tool","tool":"run_tests","arguments":{}});
        }
        return json!({"kind":"finish","summary":"Combined worker patch verified"});
    }
    json!({"kind":"finish_incomplete","summary":"Original join finished"})
}
async fn approve(engine: &act0r::Engine, task: &Value, approved: bool) {
    let saved = engine.status(run(task)).await.unwrap();
    let wait = saved.record().wait.as_ref().expect("approval wait");
    assert!(
        wait.payload
            .get("tool")
            .and_then(|t| t.get("name"))
            .is_some()
            || wait.key == "child_changes_approval"
    );
    engine
        .signal(run(task), wait.id, json!({"approved":approved}))
        .await
        .unwrap();
    settle(engine).await;
}
async fn ready(
    s: &Server,
    engine: &act0r::Engine,
    model: &Model,
    root: &Path,
) -> (Value, Value, Value) {
    fs::create_dir_all(root.join("project/src")).unwrap();
    fs::write(root.join("project/src/ui.rs"), "Waiting UI\n").unwrap();
    let r = start(s, engine, model, root, None).await;
    let a = child(s, &r, 0).await;
    let b = child(s, &r, 1).await;
    approve(engine, &a, true).await;
    steer(
        s,
        &r,
        "Hold the UI until the API completes, then review and transfer its patch.",
    )
    .await;
    settle(engine).await;
    assert_eq!(
        child(s, &r, 0).await["pause"]["planner_hold"]["paused"],
        true
    );
    (r, a, b)
}
async fn source_done(s: &Server, engine: &act0r::Engine, r: &Value, b: &Value) {
    answer(s, r, b).await;
    settle(engine).await;
    approve(engine, b, true).await;
    approve(engine, b, true).await;
    assert_eq!(child(s, r, 1).await["status"], "succeeded");
}
fn workspace(run: &act0r::domain::Run) -> String {
    run.record().state["state"]["config"]["workspace"]
        .as_str()
        .unwrap()
        .into()
}
async fn transfer_inspection(engine: &act0r::Engine, r: &Value) -> Inspection {
    let p = engine.status(run(r)).await.unwrap();
    let wait = p.record().wait.as_ref().unwrap_or_else(|| {
        panic!(
            "transfer approval missing: status {:?}, error {:?}, transfers {:?}",
            p.record().status,
            p.record().error,
            p.activities()
                .iter()
                .filter(|a| act0r::domain::worker_changes::is_handler(&a.spec.handler))
                .map(|a| (&a.spec.handler, &a.result))
                .collect::<Vec<_>>()
        )
    });
    serde_json::from_value(wait.payload["inspection"].clone()).unwrap()
}

#[tokio::test]
async fn approved_transfer_restarts_replays_and_verifies_consolidated_patch() {
    let model = Model::start("transfer_full").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("transfer.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let (r, a, b) = ready(&s, &engine, &model, root.path()).await;
    source_done(&s, &engine, &r, &b).await;
    let inspection = transfer_inspection(&engine, &r).await;
    let target = PathBuf::from(&inspection.request.target.workspace);
    assert!(
        fs::read_to_string(target.join("src/lib.rs"))
            .unwrap()
            .starts_with("Saved source")
    );
    assert_eq!(
        inspection
            .request
            .dependency
            .as_ref()
            .unwrap()
            .task_id
            .to_string(),
        a["task_id"]
    );
    assert_eq!(inspection.task_id.to_string(), b["task_id"]);
    assert_eq!(
        work(&s, &r).await["coordination"]["transfers"][0]["status"],
        "awaiting_approval"
    );
    let wait = engine
        .status(run(&r))
        .await
        .unwrap()
        .record()
        .wait
        .clone()
        .unwrap();
    let source_workspace = workspace(&engine.status(run(&b)).await.unwrap());
    fs::write(
        Path::new(&source_workspace).join("src/lib.rs"),
        "Later source workspace content\n",
    )
    .unwrap();
    s.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    assert_eq!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .wait
            .as_ref()
            .unwrap()
            .id,
        wait.id
    );
    planner_pauses::manual(&s, &r, &a, 0, true).await;
    approve(&engine, &r, true).await;
    assert!(
        fs::read_to_string(target.join("src/lib.rs"))
            .unwrap()
            .starts_with("Transferred API")
    );
    let receiver = engine.status(run(&a)).await.unwrap();
    assert_eq!(
        receiver.record().state["state"]["change_review_state"]["tests"],
        Value::Null
    );
    assert_eq!(
        receiver.record().state["state"]["context"]["test_status"]["stale"],
        true
    );
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        false
    );
    assert_eq!(child(&s, &r, 0).await["pause"]["control"]["paused"], true);
    planner_pauses::manual(&s, &r, &a, 1, false).await;
    let history = engine.history(run(&a)).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|e| e.kind == "worker_changes_started")
            .count(),
        1
    );
    assert_eq!(
        history
            .iter()
            .filter(|e| e.kind == "worker_changes_received")
            .count(),
        1
    );
    answer(&s, &r, &a).await;
    settle(&engine).await;
    approve(&engine, &a, true).await;
    approve(&engine, &a, true).await;
    use act0r::application::ports::WorkspaceChangesReadPort;
    let store = SqliteStore::open(&db).await.unwrap();
    let child_changes = json!(
        store
            .task_changes(a["task_id"].as_str().unwrap().parse().unwrap())
            .await
            .unwrap()
    );
    assert_eq!(
        child_changes["changes"]["files"].as_array().unwrap().len(),
        2
    );
    assert_eq!(child_changes["completion"]["status"], "verified");
    // The parent imports only the downstream combined patch, then runs its own tests.
    approve(&engine, &r, true).await;
    approve(&engine, &r, true).await;
    assert_eq!(
        engine.status(run(&r)).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(
        fs::read_to_string(Path::new(&workspace(&parent)).join("src/ui.rs")).unwrap(),
        "Integrated UI\n"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("project/src/ui.rs")).unwrap(),
        "Waiting UI\n"
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
    // A filesystem receipt replay cannot touch a later workspace or repeat writes.
    let call = parent
        .activities()
        .iter()
        .find(|a| a.spec.handler == act0r::domain::worker_changes::handler("import"))
        .unwrap();
    let (apps, tools) = local_artifacts(&db);
    let adapter = act0r::adapters::child_changes::LocalChildChanges::new(
        Arc::new(SqliteStore::open(&db).await.unwrap()),
        apps,
        tools,
    );
    let execution = ActivityExecution {
        run_id: run(&r),
        activity_id: call.id,
        attempt_id: act0r::domain::AttemptId::new(),
        handler: call.spec.handler.clone(),
        input: call.spec.input.clone(),
        ordinal: 1,
    };
    fs::write(target.join("src/lib.rs"), "Later receiver content\n").unwrap();
    use act0r::application::ports::ChildChangesPort;
    assert!(matches!(
        adapter.execute(execution).await.unwrap(),
        act0r::domain::Outcome::Success(_)
    ));
    assert_eq!(
        fs::read_to_string(target.join("src/lib.rs")).unwrap(),
        "Later receiver content\n"
    );
    s.close().await;
}

#[tokio::test]
async fn interrupted_transfer_requires_explicit_recovery_and_replays_finished_journal() {
    use act0r::application::ports::ChildChangesPort;
    let model = Model::start("transfer_hold").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("interrupted.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let (r, a, b) = ready(&s, &engine, &model, root.path()).await;
    source_done(&s, &engine, &r, &b).await;
    let i = transfer_inspection(&engine, &r).await;
    let wait = engine
        .status(run(&r))
        .await
        .unwrap()
        .record()
        .wait
        .clone()
        .unwrap();
    engine
        .signal(run(&r), wait.id, json!({"approved":true}))
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    assert_eq!(
        execution.handler,
        act0r::domain::worker_changes::handler("import")
    );
    assert_eq!(
        engine.status(run(&a)).await.unwrap().record().state["state"]["change_review_state"]["tests"],
        Value::Null
    );
    let (apps, tools) = local_artifacts(&db);
    let store = Arc::new(SqliteStore::open(&db).await.unwrap());
    let adapter =
        act0r::adapters::child_changes::LocalChildChanges::new(store.clone(), apps, tools)
            .with_worker_changes(store);
    assert!(matches!(
        adapter.execute(execution.clone()).await.unwrap(),
        act0r::domain::Outcome::Success(_)
    ));
    // Simulate process loss after journal completion but before SQLite acknowledgement.
    drop(worker);
    s.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let worker = engine.worker().await.unwrap();
    drop(worker);
    assert_eq!(
        engine.status(run(&r)).await.unwrap().record().status,
        RunStatus::Blocked
    );
    let recovery = get(
        &client(),
        &format!("{}/child-import-recovery", base(&s, &r)),
    )
    .await;
    assert_eq!(
        recovery["items"][0]["recovery"]["inspection"]["report"]["status"],
        "applied"
    );
    assert_eq!(recovery["items"][0]["recovery"]["retry_allowed"], true);
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        true
    );
    let attempt = recovery["items"][0]["recovery"]["attempt_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let decision = act0r::domain::ReconciliationRequest {
        activity_id: execution.activity_id,
        attempt_id: attempt,
        decision: act0r::domain::ReconciliationDecision::Retry {},
        evidence: "The saved journal confirms the original transfer completed.".into(),
    };
    engine.reconcile(run(&r), decision.clone()).await.unwrap();
    settle(&engine).await;
    let replay = act0r::domain::tool_recovery::ToolRecoveryRequest {
        activity_id: decision.activity_id,
        attempt_id: decision.attempt_id,
        decision: act0r::domain::tool_recovery::ToolRecoveryDecision::Retry {},
        evidence: decision.evidence.clone(),
        operation_stopped: true,
    };
    let body = json!(replay);
    let mut fabricated = body.clone();
    fabricated["decision"] = json!({"action":"accept","result":{"status":"applied"}});
    post(
        &client(),
        &format!(
            "{}/runs/{}/child-import-reconciliation",
            base(&s, &r),
            run(&r)
        ),
        &fabricated,
        StatusCode::BAD_REQUEST,
    )
    .await;
    post(
        &client(),
        &format!(
            "{}/runs/{}/child-import-reconciliation",
            base(&s, &r),
            run(&r)
        ),
        &body,
        StatusCode::OK,
    )
    .await;
    let recovered = get(
        &client(),
        &format!("{}/child-import-recovery", base(&s, &r)),
    )
    .await;
    assert_eq!(
        recovered["items"][0]["reconciliation_receipts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        fs::read_to_string(Path::new(&i.request.target.workspace).join("src/lib.rs")).unwrap(),
        "Transferred API: id and display_name\n"
    );
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        true
    );
    assert_eq!(
        engine
            .history(run(&a))
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "worker_changes_started")
            .count(),
        1
    );
    assert_eq!(
        engine
            .history(run(&a))
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "worker_changes_received")
            .count(),
        1
    );
    s.close().await;
}
fn local_artifacts(
    db: &Path,
) -> (
    Arc<act0r::adapters::patch_applications::LocalPatchApplications>,
    Arc<act0r::adapters::workspace::LocalTools>,
) {
    let local = db.parent().unwrap().join(".act0r");
    (
        Arc::new(
            act0r::adapters::patch_applications::LocalPatchApplications::new(
                local.join("applications"),
            ),
        ),
        Arc::new(act0r::adapters::workspace::LocalTools::new(
            local.join("workspaces"),
        )),
    )
}

#[tokio::test]
async fn earlier_receiver_tests_cannot_verify_a_transferred_patch() {
    let model = Model::start("transfer_old_tests").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("old-tests.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let (r, a, b) = ready(&s, &engine, &model, root.path()).await;
    source_done(&s, &engine, &r, &b).await;
    approve(&engine, &r, true).await;
    answer(&s, &r, &a).await;
    settle(&engine).await;
    let receiver = engine.status(run(&a)).await.unwrap();
    assert_eq!(receiver.record().status, RunStatus::Waiting);
    assert_eq!(
        receiver.record().wait.as_ref().unwrap().payload["tool"]["name"],
        "run_tests"
    );
    let context = &receiver.record().state["state"]["context"];
    assert!(
        context["observations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["value"]["completion"]["reason"] == "tests_not_run")
    );
    approve(&engine, &a, true).await;
    assert_eq!(
        engine.status(run(&a)).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    s.close().await;
}

#[tokio::test]
async fn receiver_task_permissions_refuse_prerequisite_files_without_widening_scope() {
    let model = Model::start("transfer_write_scope").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("scopes.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let (r, a, b) = ready(&s, &engine, &model, root.path()).await;
    source_done(&s, &engine, &r, &b).await;
    let parent = engine.status(run(&r)).await.unwrap();
    let result = parent
        .activities()
        .iter()
        .find(|a| a.spec.handler == act0r::domain::worker_changes::handler("inspect"))
        .unwrap()
        .result
        .as_ref()
        .unwrap();
    assert_eq!(result["ok"], true);
    assert!(
        result["inspection"]["preview"]["files"][0]["conflict"]
            .as_str()
            .unwrap()
            .contains("writable paths")
    );
    assert!(
        !parent
            .activities()
            .iter()
            .any(|a| a.spec.handler == act0r::domain::worker_changes::handler("import"))
    );
    let receiver = engine.status(run(&a)).await.unwrap();
    assert_eq!(
        receiver.record().state["state"]["config"]["writable_paths"],
        json!(["src/ui.rs"])
    );
    assert_eq!(
        child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
        true
    );
    assert!(
        fs::read_to_string(Path::new(&workspace(&receiver)).join("src/lib.rs"))
            .unwrap()
            .starts_with("Saved source")
    );
    s.close().await;
}

#[tokio::test]
async fn handoff_actions_cannot_escape_the_captured_dependency_review() {
    for mode in ["transfer_outside", "transfer_scope_action"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("action-scope.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = if mode == "transfer_outside" {
            start(&s, &engine, &model, root.path(), None).await
        } else {
            let (r, _a, b) = ready(&s, &engine, &model, root.path()).await;
            source_done(&s, &engine, &r, &b).await;
            r
        };
        let parent = engine.status(run(&r)).await.unwrap();
        assert_eq!(parent.record().status, RunStatus::Failed);
        assert!(
            !parent
                .activities()
                .iter()
                .any(|a| act0r::domain::worker_changes::is_handler(&a.spec.handler))
        );
        s.close().await;
    }
}

#[tokio::test]
async fn denial_and_conflict_keep_original_dependency_hold_and_worker_files() {
    for conflict in [false, true] {
        let model = Model::start("transfer_hold").await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("refused.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let (r, _a, b) = ready(&s, &engine, &model, root.path()).await;
        source_done(&s, &engine, &r, &b).await;
        let i = transfer_inspection(&engine, &r).await;
        if conflict {
            fs::write(
                Path::new(&i.request.target.workspace).join("src/lib.rs"),
                "Receiver changed after review\n",
            )
            .unwrap();
        }
        approve(&engine, &r, conflict).await;
        assert_eq!(
            child(&s, &r, 0).await["pause"]["planner_hold"]["paused"],
            true
        );
        let expected = if conflict {
            "Receiver changed after review\n"
        } else {
            "Saved source: id and display_name\n"
        };
        assert_eq!(
            fs::read_to_string(Path::new(&i.request.target.workspace).join("src/lib.rs")).unwrap(),
            expected
        );
        let p = engine.status(run(&r)).await.unwrap();
        if conflict {
            assert_eq!(
                p.activities()
                    .iter()
                    .find(|a| a.spec.handler == act0r::domain::worker_changes::handler("import"))
                    .unwrap()
                    .result
                    .as_ref()
                    .unwrap()["status"],
                "conflicted"
            );
        } else {
            assert!(
                !p.activities()
                    .iter()
                    .any(|a| a.spec.handler == act0r::domain::worker_changes::handler("import"))
            );
        }
        s.close().await;
    }
}

#[tokio::test]
async fn transfer_rechecks_scope_hold_cancellation_and_saved_artifact() {
    let model = Model::start("transfer_hold").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("guards.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let (r, a, b) = ready(&s, &engine, &model, root.path()).await;
    source_done(&s, &engine, &r, &b).await;
    let i = transfer_inspection(&engine, &r).await;
    let store = SqliteStore::open(&db).await.unwrap();
    store
        .validate_worker_import(run(&r), i.clone())
        .await
        .unwrap();
    let mut forged = i.clone();
    forged.request.target.writable_paths = vec!["src/ui.rs".into()];
    assert!(store.validate_worker_import(run(&r), forged).await.is_err());
    let mut forged = i.clone();
    forged.artifact_id = act0r::domain::ActivityId::new();
    assert!(store.validate_worker_import(run(&r), forged).await.is_err());
    let mut request = InspectWorkerChanges {
        dependency: i.request.dependency.clone().unwrap(),
        offset: 0,
    };
    request.dependency.hold_revision += 1;
    assert!(store.worker_changes(run(&r), request).await.is_err());
    // Cancelling only the receiver leaves the exact approval inspectable, but no fresh write.
    engine.cancel(run(&a)).await.unwrap();
    settle(&engine).await;
    assert!(
        store
            .validate_worker_import(run(&r), i.clone())
            .await
            .is_err()
    );
    approve(&engine, &r, true).await;
    assert_eq!(
        fs::read_to_string(Path::new(&i.request.target.workspace).join("src/lib.rs")).unwrap(),
        "Saved source: id and display_name\n"
    );
    let p = engine.status(run(&r)).await.unwrap();
    assert_eq!(
        p.activities()
            .iter()
            .find(|a| a.spec.handler == act0r::domain::worker_changes::handler("import"))
            .unwrap()
            .result
            .as_ref()
            .unwrap()["ok"],
        false
    );
    s.close().await;
}
