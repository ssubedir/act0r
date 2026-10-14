//! Public planner -> child artifact -> approval -> integration -> verification contract.
use act0r::{
    Engine, Worker,
    adapters::sqlite::SqliteStore,
    application::{AgentService, ports::*},
    bootstrap::local_app,
    domain::{actors::*, change_review::TestEvidenceStatus, child_changes::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

fn config() -> Value {
    json!({"goal":"Repair and deliver", "target":{"provider":"scripted","model":"handoff","endpoint":""},
        "max_model_calls":8,"max_tokens":1000000,"max_output_tokens":512,"change_review":true})
}
fn test_command() -> Value {
    json!({"argv":[std::env::current_exe().unwrap(),"--ignored","--exact","integrated_test_process_fixture","--nocapture"],"timeout_ms":10000})
}
async fn setup(root: &Path, mode: &str) -> (Engine, AgentService, AgentId, MessageId, PathBuf) {
    let source = root.join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(
        source.join("src/lib.rs"),
        if mode == "pages" {
            format!("old café\n{}", "unchanged café\n".repeat(1200))
        } else {
            "old café\n".into()
        },
    )
    .unwrap();
    fs::write(source.join("README.md"), "Original\n").unwrap();
    let (engine, agents) = local_app(root.join("app.db")).await.unwrap();
    let mut child = config();
    child["tools"] = json!([{"name":"replace_text","version":1},{"name":"create_file","version":1},{"name":"run_tests","version":1}]);
    child["writable_paths"] = json!(["src/"]);
    child["test_command"] = test_command();
    child["script"] = json!([
        {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"old café","new":"new café"}},
        {"kind":"call_tool","tool":"create_file","arguments":{"path":"src/notes.txt","content":"Worker evidence\n"}},
        {"kind":"call_tool","tool":"run_tests","arguments":{}},
        {"kind":"finish","summary":"Repaired and verified."}]);
    if mode == "missing" {
        child["change_review"] = json!(false);
    }
    if mode == "pages" {
        child["script"][1]["arguments"]["content"] =
            json!(format!("Worker evidence\n{}", "notes café\n".repeat(2000)));
    }
    if mode == "failed" {
        child["script"][0]["tool"] = json!("forbidden");
        child["script"] = json!([]);
        child["max_tokens"] = json!(1);
    }
    let worker = agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            Some(source.clone()),
            child,
            40,
            Default::default(),
        )
        .await
        .unwrap();
    let mut planner = config();
    planner["workers"] = json!([{"name":"repairer","agent_id":worker,"description":"Repair code and return a reviewed patch."}]);
    planner["delegation"] = json!({"max_tasks":2,"max_depth":2});
    planner["child_changes"] = json!(true);
    planner["writable_paths"] = json!(if mode == "permissions" {
        vec!["README.md"]
    } else {
        vec!["src/"]
    });
    planner["tools"] = json!([{"name":"run_tests","version":1}]);
    planner["test_command"] = test_command();
    planner["script"] = json!([
        {"kind":"delegate_task","worker":"repairer","goal":"Repair and verify"},
        {"kind":"inspect_child_changes","worker":"repairer","offset":0},
        {"kind":"import_child_changes","worker":"repairer"},
        {"kind":"call_tool","tool":"run_tests","arguments":{}},
        {"kind":"finish","summary":"Integrated and verified."}]);
    if ["permissions", "missing", "failed"].contains(&mode) {
        planner["script"][3] =
            json!({"kind":"finish","summary":"Cannot integrate; reported feedback."});
    }
    if mode == "stale" {
        planner["script"] = json!([
            {"kind":"call_tool","tool":"run_tests","arguments":{}},
            {"kind":"delegate_task","worker":"repairer","goal":"Repair and verify"},
            {"kind":"inspect_child_changes","worker":"repairer","offset":0},
            {"kind":"import_child_changes","worker":"repairer"},
            {"kind":"finish","summary":"Imported; parent tests are stale."}]);
    }
    let parent = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source.clone()),
            planner,
            if mode == "attempts" { 5 } else { 50 },
            Default::default(),
        )
        .await
        .unwrap();
    let task = agents.actors().unwrap().send(AgentMessage {id:MessageId::new(),recipient:parent,sender:None,correlation_id:None,kind:"task".into(),payload:json!({"goal":"Integrate repair","child_changes":false,"workspace":"forged","writable_paths":["/"]})}).await.unwrap();
    (engine, agents, worker, task, source)
}
async fn approve_child(engine: &Engine, agents: &AgentService, actor: AgentId) {
    for task in agents.actors().unwrap().tasks(actor).await.unwrap() {
        if let (Some(run), Some(wait)) = (task.run_id, task.wait) {
            assert_eq!(wait.key, "native_tool_approval");
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await
                .unwrap();
        }
    }
}
async fn settle(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    child: AgentId,
    task: MessageId,
    stop_import: bool,
    approved: bool,
) {
    for _ in 0..300 {
        worker.step().await.unwrap();
        approve_child(engine, agents, child).await;
        let view = agents.actors().unwrap().task(task).await.unwrap();
        if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
            if wait.key == APPROVAL_KEY && stop_import {
                return;
            }
            assert!(wait.key == APPROVAL_KEY || wait.key == "native_tool_approval");
            engine
                .signal(
                    run,
                    wait.id,
                    json!({"approved":if wait.key==APPROVAL_KEY {approved} else {true}}),
                )
                .await
                .unwrap();
        }
        if view.status.is_terminal() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("planner did not settle");
}
async fn parent_run(engine: &Engine, agents: &AgentService, task: MessageId) -> Run {
    engine
        .status(
            agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .run_id
                .unwrap(),
        )
        .await
        .unwrap()
}
fn workspace(run: &Run) -> PathBuf {
    PathBuf::from(run.record().input["input"]["workspace"].as_str().unwrap())
}
#[tokio::test]
async fn approval_survives_restart_and_import_uses_immutable_child_artifact_then_parent_tests() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, child, task, source) = setup(dir.path(), "success").await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, child, task, true, true).await;
    let saved = parent_run(&engine, &agents, task).await;
    let wait = saved.record().wait.clone().unwrap();
    assert_eq!(wait.key, APPROVAL_KEY);
    let model = saved
        .activities()
        .iter()
        .find(|a| a.spec.handler == DefinitionId::new("agent.model", 1))
        .unwrap();
    let schema = &model.spec.input["schema"]["properties"]["action"]["anyOf"];
    assert!(
        schema
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["properties"]["kind"]["enum"] == json!(["inspect_child_changes"]))
    );
    assert!(
        schema
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["properties"]["kind"]["enum"] == json!(["import_child_changes"]))
    );
    let inspection: Inspection =
        serde_json::from_value(wait.payload["inspection"].clone()).unwrap();
    assert_eq!(inspection.preview.test_status, TestEvidenceStatus::Matching);
    assert_eq!(
        inspection.preview.tests.as_ref().unwrap().result["ok"],
        true
    );
    assert!(inspection.patch.contains("+new café"));
    let target = workspace(&saved);
    assert_eq!(
        fs::read_to_string(target.join("src/lib.rs")).unwrap(),
        "old café\n"
    );
    let child_workspace = workspace(&engine.status(inspection.child_run_id).await.unwrap());
    // No live child workspace is read when importing. Subsequent worker work cannot change this artifact.
    fs::rename(&child_workspace, child_workspace.with_extension("removed")).unwrap();
    fs::write(target.join("README.md"), "Parent's unrelated edit\n").unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
    let same = parent_run(&engine, &agents, task).await;
    assert_eq!(same.record().wait.as_ref().unwrap().id, wait.id);
    assert!(
        engine
            .signal(
                same.record().id,
                wait.id,
                json!({"approved":true,"target":"forged"})
            )
            .await
            .is_err()
    );
    engine
        .signal(same.record().id, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, child, task, false, true).await;
    let finished = parent_run(&engine, &agents, task).await;
    assert_eq!(finished.record().status, RunStatus::Succeeded);
    assert_eq!(finished.record().usage.admitted_model_calls, 5);
    let changes = agents
        .actors()
        .unwrap()
        .task_changes(task)
        .await
        .unwrap()
        .changes
        .unwrap();
    assert_eq!(changes.test_status, TestEvidenceStatus::Matching);
    assert_eq!(changes.tests.as_ref().unwrap().result["ok"], true);
    assert_eq!(changes.files.len(), 3);
    assert!(changes.patch().contains("+new café"));
    assert_eq!(
        fs::read_to_string(source.join("src/lib.rs")).unwrap(),
        "old café\n"
    );
    assert!(!source.join("src/notes.txt").exists());
    let import = finished
        .activities()
        .iter()
        .find(|a| a.spec.handler == handler("import"))
        .unwrap();
    assert_eq!(import.spec.recovery, RecoveryPolicy::RequiresReconciliation);
    assert_eq!(
        import.result.as_ref().unwrap()["applied_files"],
        json!(["src/lib.rs", "src/notes.txt"])
    );
}
#[tokio::test]
async fn denial_missing_failed_and_unwritable_children_produce_feedback_without_imports() {
    for mode in ["denied", "missing", "failed", "permissions"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents, child, task, _) = setup(dir.path(), mode).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&engine, &agents, &mut worker, child, task, false, false).await;
        let run = parent_run(&engine, &agents, task).await;
        assert_eq!(
            run.record().status,
            RunStatus::Succeeded,
            "{mode}: {:?}",
            run.record().error
        );
        assert!(
            !run.activities()
                .iter()
                .any(|a| a.spec.handler == handler("import")),
            "{mode}"
        );
        assert_eq!(
            fs::read_to_string(workspace(&run).join("src/lib.rs")).unwrap(),
            "old café\n"
        );
        let changes = agents
            .actors()
            .unwrap()
            .task_changes(task)
            .await
            .unwrap()
            .changes
            .unwrap();
        assert!(changes.files.is_empty());
        let inspect = run
            .activities()
            .iter()
            .find(|a| a.spec.handler == handler("inspect"))
            .unwrap();
        assert_eq!(
            inspect.result.as_ref().unwrap()["ok"],
            mode != "missing" && mode != "failed"
        );
        if mode == "permissions" {
            assert!(
                inspect.result.as_ref().unwrap()["inspection"]["preview"]["files"][0]["conflict"]
                    .as_str()
                    .unwrap()
                    .contains("outside the parent's writable paths")
            );
        }
    }
}
#[tokio::test]
async fn target_drift_after_approval_is_conflict_without_partial_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, child, task, _) = setup(dir.path(), "success").await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, child, task, true, true).await;
    let run = parent_run(&engine, &agents, task).await;
    let target = workspace(&run);
    fs::write(target.join("src/lib.rs"), "Parent's intervening edit\n").unwrap();
    let wait = run.record().wait.as_ref().unwrap();
    engine
        .signal(run.record().id, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    settle(&engine, &agents, &mut worker, child, task, false, true).await;
    let run = parent_run(&engine, &agents, task).await;
    let import = run
        .activities()
        .iter()
        .find(|a| a.spec.handler == handler("import"))
        .unwrap();
    assert_eq!(import.result.as_ref().unwrap()["status"], "conflicted");
    assert!(!target.join("src/notes.txt").exists());
    assert_eq!(
        fs::read_to_string(target.join("src/lib.rs")).unwrap(),
        "Parent's intervening edit\n"
    );
}
#[tokio::test]
async fn import_marks_previous_parent_test_evidence_stale() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, child, task, _) = setup(dir.path(), "stale").await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, child, task, false, true).await;
    let run = parent_run(&engine, &agents, task).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    let context = &run.record().state["state"]["context"];
    assert_eq!(context["test_status"]["stale"], true);
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .task_changes(task)
            .await
            .unwrap()
            .changes
            .unwrap()
            .test_status,
        TestEvidenceStatus::Stale
    );
}
#[tokio::test]
async fn source_reader_checks_actual_parent_pins_and_direct_delegation_scope() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, child, task, _) = setup(dir.path(), "success").await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, child, task, true, true).await;
    let run = parent_run(&engine, &agents, task).await;
    let inspection: Inspection =
        serde_json::from_value(run.record().wait.as_ref().unwrap().payload["inspection"].clone())
            .unwrap();
    let store = SqliteStore::open(dir.path().join("app.db")).await.unwrap();
    assert_eq!(
        store
            .child_changes(run.record().id, inspection.request.clone())
            .await
            .unwrap()
            .task_id,
        inspection.task_id
    );
    for field in ["worker", "recipient", "workspace", "paths"] {
        let mut request = inspection.request.clone();
        match field {
            "worker" => request.worker = "outsider".into(),
            "recipient" => request.recipient = AgentId::new(),
            "workspace" => request.target.workspace = "C:/unrelated".into(),
            _ => request.target.writable_paths = vec!["README.md".into()],
        };
        assert!(
            store.child_changes(run.record().id, request).await.is_err(),
            "{field}"
        );
    }
}
#[test]
#[ignore = "real subprocess run by the consumer tests"]
fn integrated_test_process_fixture() {
    assert!(
        fs::read_to_string("src/lib.rs")
            .unwrap()
            .starts_with("new café\n")
    );
    assert!(
        fs::read_to_string("src/notes.txt")
            .unwrap()
            .starts_with("Worker evidence\n")
    );
}

#[tokio::test]
async fn goal_hold_survives_approval_and_restart_then_resume_imports_the_same_patch() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, child, task, _) = setup(dir.path(), "success").await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, child, task, true, true).await;
    let run = parent_run(&engine, &agents, task).await;
    let target = workspace(&run);
    let wait = run.record().wait.as_ref().unwrap();
    agents.actors().unwrap().pause_goal(task).await.unwrap();
    engine
        .signal(run.record().id, wait.id, json!({"approved":true}))
        .await
        .unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..4 {
        worker.step().await.unwrap();
    }
    assert_eq!(
        fs::read_to_string(target.join("src/lib.rs")).unwrap(),
        "old café\n"
    );
    let held = parent_run(&engine, &agents, task).await;
    let import = held
        .activities()
        .iter()
        .find(|a| a.spec.handler == handler("import"))
        .unwrap();
    assert_eq!(import.attempts, 0);
    agents.actors().unwrap().resume_goal(task).await.unwrap();
    settle(&engine, &agents, &mut worker, child, task, false, true).await;
    assert_eq!(
        parent_run(&engine, &agents, task).await.record().status,
        RunStatus::Succeeded
    );
    assert_eq!(
        fs::read_to_string(target.join("src/lib.rs")).unwrap(),
        "new café\n"
    );
}
#[tokio::test]
async fn cancellation_and_attempt_limit_prevent_unadmitted_imports() {
    for mode in ["cancel", "attempts"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents, child, task, _) = setup(dir.path(), mode).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&engine, &agents, &mut worker, child, task, true, true).await;
        let run = parent_run(&engine, &agents, task).await;
        let target = workspace(&run);
        let wait = run.record().wait.as_ref().unwrap();
        engine
            .signal(run.record().id, wait.id, json!({"approved":true}))
            .await
            .unwrap();
        if mode == "cancel" {
            agents.actors().unwrap().cancel_goal(task).await.unwrap();
        }
        settle(&engine, &agents, &mut worker, child, task, false, true).await;
        let run = parent_run(&engine, &agents, task).await;
        assert_eq!(
            run.record().status,
            if mode == "cancel" {
                RunStatus::Cancelled
            } else {
                RunStatus::BudgetExhausted
            }
        );
        assert_eq!(
            fs::read_to_string(target.join("src/lib.rs")).unwrap(),
            "old café\n"
        );
        assert!(!target.join("src/notes.txt").exists());
        assert!(
            run.activities()
                .iter()
                .filter(|a| a.spec.handler == handler("import"))
                .all(|a| a.attempts == 0)
        );
    }
}
#[tokio::test]
async fn patch_pages_are_bounded_exact_utf8_and_bad_offsets_are_saved_feedback() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, child, task, _) = setup(dir.path(), "pages").await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, child, task, true, true).await;
    let run = parent_run(&engine, &agents, task).await;
    let first: Inspection =
        serde_json::from_value(run.record().wait.as_ref().unwrap().payload["inspection"].clone())
            .unwrap();
    assert!(first.patch.len() <= PAGE_BYTES);
    assert!(first.next_offset.is_some());
    let store = std::sync::Arc::new(SqliteStore::open(dir.path().join("app.db")).await.unwrap());
    let ws_root = workspace(&run).parent().unwrap().to_path_buf();
    let adapter = act0r::adapters::child_changes::LocalChildChanges::new(
        store.clone(),
        std::sync::Arc::new(
            act0r::adapters::patch_applications::LocalPatchApplications::new(
                ws_root.parent().unwrap().join("applications"),
            ),
        ),
        std::sync::Arc::new(act0r::adapters::workspace::LocalTools::new(ws_root)),
    );
    let saved = store
        .child_changes(run.record().id, first.request.clone())
        .await
        .unwrap()
        .changes
        .unwrap();
    let mut combined = first.patch.clone();
    let mut offset = first.next_offset;
    while let Some(next) = offset {
        let mut request = first.request.clone();
        request.offset = next;
        let mut call = run
            .activities()
            .iter()
            .find(|a| a.spec.handler == handler("inspect"))
            .unwrap()
            .clone();
        call.id = ActivityId::new();
        call.spec.input = json!(request);
        let execution = ActivityExecution {
            run_id: run.record().id,
            activity_id: call.id,
            attempt_id: AttemptId::new(),
            handler: handler("inspect"),
            input: call.spec.input,
            ..inspection_execution_template(&run)
        };
        let Outcome::Success(value) = adapter.execute(execution.clone()).await.unwrap() else {
            panic!("inspection failed")
        };
        let page: Inspection = serde_json::from_value(value["inspection"].clone()).unwrap();
        assert!(page.patch.len() <= PAGE_BYTES);
        combined.push_str(&page.patch);
        offset = page.next_offset;
        // Receipts remain exact when files drift after inspection.
        fs::write(workspace(&run).join("README.md"), "later\n").unwrap();
        let Outcome::Success(replayed) = adapter.execute(execution).await.unwrap() else {
            panic!("replay failed")
        };
        assert_eq!(replayed, value);
    }
    assert_eq!(combined, saved.patch());
    let mut request = first.request.clone();
    request.offset = first.patch_bytes + 1;
    let mut execution = inspection_execution_template(&run);
    execution.activity_id = ActivityId::new();
    execution.input = json!(request);
    let Outcome::Success(value) = adapter.execute(execution).await.unwrap() else {
        panic!("expected feedback")
    };
    assert_eq!(value["ok"], false);
}
fn inspection_execution_template(run: &Run) -> ActivityExecution {
    let activity = run
        .activities()
        .iter()
        .find(|a| a.spec.handler == handler("inspect"))
        .unwrap();
    ActivityExecution {
        run_id: run.record().id,
        activity_id: activity.id,
        attempt_id: AttemptId::new(),
        handler: activity.spec.handler.clone(),
        input: activity.spec.input.clone(),
        ordinal: 1,
    }
}

#[tokio::test]
async fn killed_import_host_blocks_parent_and_explicit_retry_replays_the_finished_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, child, task, _) = setup(dir.path(), "success").await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, child, task, true, true).await;
    let run = parent_run(&engine, &agents, task).await;
    let parent_id = run.record().id;
    let target = workspace(&run);
    engine
        .signal(
            parent_id,
            run.record().wait.as_ref().unwrap().id,
            json!({"approved":true}),
        )
        .await
        .unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let marker = dir.path().join("import-executed.json");
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--ignored",
            "--exact",
            "child_import_host_fixture",
            "--nocapture",
        ])
        .env("ACT0R_CHILD_DB", dir.path().join("app.db"))
        .env("ACT0R_CHILD_MARKER", &marker)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut host = command.spawn().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !marker.exists() && std::time::Instant::now() < deadline {
        assert!(
            host.try_wait().unwrap().is_none(),
            "import host exited before the boundary"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if !marker.exists() {
        host.kill().unwrap();
        host.wait().unwrap();
        panic!("import host did not reach the boundary");
    }
    host.kill().unwrap();
    host.wait().unwrap();
    let interrupted: ActivityExecution =
        serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
    assert_eq!(
        fs::read_to_string(target.join("src/lib.rs")).unwrap(),
        "new café\n"
    );
    let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    let blocked = engine.status(parent_id).await.unwrap();
    assert_eq!(blocked.record().status, RunStatus::Blocked);
    assert_eq!(
        blocked
            .activities()
            .iter()
            .find(|a| a.id == interrupted.activity_id)
            .unwrap()
            .status,
        ActivityStatus::Indeterminate
    );
    assert!(worker.admit().await.unwrap().is_none());
    engine
        .reconcile(
            parent_id,
            ReconciliationRequest {
                activity_id: interrupted.activity_id,
                attempt_id: interrupted.attempt_id,
                decision: ReconciliationDecision::Retry {},
                evidence: "Reviewed saved child import journal; replay the finished receipt."
                    .into(),
            },
        )
        .await
        .unwrap();
    settle(&engine, &agents, &mut worker, child, task, false, true).await;
    let finished = parent_run(&engine, &agents, task).await;
    assert_eq!(finished.record().status, RunStatus::Succeeded);
    assert_eq!(finished.record().usage.admitted_model_calls, 5);
    let import = finished
        .activities()
        .iter()
        .find(|a| a.id == interrupted.activity_id)
        .unwrap();
    assert_eq!(import.attempts, 2);
    assert_eq!(import.result.as_ref().unwrap()["status"], "applied");
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .task_changes(task)
            .await
            .unwrap()
            .changes
            .unwrap()
            .test_status,
        TestEvidenceStatus::Matching
    );
}
#[tokio::test]
#[ignore = "real killed host run by the consumer test"]
async fn child_import_host_fixture() {
    let db = std::env::var_os("ACT0R_CHILD_DB").unwrap();
    let marker = std::env::var_os("ACT0R_CHILD_MARKER").unwrap();
    let (engine, _agents) = local_app(PathBuf::from(db)).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..100 {
        worker.advance_work().await.unwrap();
        if let Some(call) = worker.admit().await.unwrap() {
            assert_eq!(call.handler, handler("import"));
            let outcome = worker.execute(call.clone()).await.unwrap();
            assert!(matches!(outcome, Outcome::Success(_)), "{outcome:?}");
            // Intentionally omit SQLite completion; parent kills this process after effects.
            fs::write(Path::new(&marker), serde_json::to_vec(&call).unwrap()).unwrap();
            std::future::pending::<()>().await;
        }
    }
    panic!("import was not admitted");
}

#[tokio::test]
async fn disabled_planners_keep_the_existing_schema_and_cannot_request_child_actions() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
    let mut worker_config = config();
    worker_config["change_review"] = json!(false);
    worker_config["script"] = json!([{"kind":"finish","summary":"Registered repairer."}]);
    let worker_id = agents
        .create_registered_actor(
            DefinitionId::new("answer", 1),
            None,
            worker_config,
            8,
            Default::default(),
        )
        .await
        .unwrap();
    let mut cfg = config();
    cfg["change_review"] = json!(false);
    cfg["workers"] = json!([{"name":"repairer","agent_id":worker_id,"description":"Repair"}]);
    cfg["delegation"] = json!({"max_tasks":1,"max_depth":1});
    cfg["script"] = json!([{"kind":"finish","summary":"Untracked planner."}]);
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            None,
            cfg.clone(),
            8,
            Default::default(),
        )
        .await
        .unwrap();
    let task = agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Untracked"}),
        })
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, actor, task, false, true).await;
    let run = parent_run(&engine, &agents, task).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    let schema = run.activities()[0].spec.input["schema"].to_string();
    assert!(!schema.contains("child_changes"));
    assert!(run.record().state["state"].get("inspection").is_none());
    cfg["script"] = json!([{"kind":"inspect_child_changes","worker":"repairer","offset":0}]);
    assert!(
        agents
            .create_registered_actor(
                DefinitionId::new("planner", 3),
                None,
                cfg,
                8,
                Default::default()
            )
            .await
            .is_err()
    );
}
