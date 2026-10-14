//! Public native actor review contract, including actual subprocess and killed-host recovery.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, change_review::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn source(root: &Path, mode: &str) -> PathBuf {
    let path = root.join("source");
    fs::create_dir_all(path.join("src")).unwrap();
    fs::write(
        path.join("src/lib.rs"),
        "pub fn answer() -> i64 { 19 - 23 }\n",
    )
    .unwrap();
    fs::write(path.join("README.md"), "Original project\n").unwrap();
    fs::write(path.join("test-mode"), mode).unwrap();
    path
}
fn config() -> Value {
    json!({"goal":"Repair and save a patch","target":{"provider":"scripted","model":"review","endpoint":""},
        "max_model_calls":5,"max_tokens":1000000,"max_output_tokens":512,"change_review":true,
        "tools":[{"name":"search_text","version":1},{"name":"replace_text","version":1},{"name":"create_file","version":1},{"name":"run_tests","version":1}],
        "writable_paths":["src/"],
        "test_command":{"argv":[std::env::current_exe().unwrap(),"--ignored","--exact","review_test_process_fixture","--nocapture"],"timeout_ms":10000},
        "script":[{"kind":"call_tool","tool":"search_text","arguments":{"query":"fn answer","path":"src"}},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"19 - 23","new":"19 + 23"}},
            {"kind":"call_tool","tool":"create_file","arguments":{"path":"src/notes.txt","content":"Created file\n"}},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},
            {"kind":"finish","summary":"Repaired and tested."}]})
}
async fn create(
    agents: &AgentService,
    source: PathBuf,
    policy: &str,
    mut cfg: Value,
    attempts: u32,
) -> AgentId {
    if policy == "planner" {
        let reviewer=agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({"goal":"Review","target":{"provider":"scripted","model":"reviewer","endpoint":""},"max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,"script":[{"kind":"finish","summary":"Reviewed."}]}),4,Default::default()).await.unwrap();
        cfg["workers"] = json!([{"name":"reviewer","agent_id":reviewer,"description":"Reviews"}]);
        cfg["delegation"] = json!({"max_tasks":1,"max_depth":1});
    }
    agents
        .create_registered_actor(
            DefinitionId::new(policy, if policy == "planner" { 3 } else { 1 }),
            Some(source),
            cfg,
            attempts,
            Default::default(),
        )
        .await
        .unwrap()
}
async fn send(agents: &AgentService, actor: AgentId) -> MessageId {
    agents.actors().unwrap().send(AgentMessage {id:MessageId::new(),recipient:actor,sender:None,correlation_id:None,kind:"task".into(),payload:json!({"goal":"Repair safely","change_review":false,"workspace":"forged","baseline":{"digest":"fake"}})}).await.unwrap()
}
async fn settle(engine: &Engine, agents: &AgentService, worker: &mut Worker, task: MessageId) {
    for _ in 0..500 {
        worker.step().await.unwrap();
        let view = agents.actors().unwrap().task(task).await.unwrap();
        if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
            assert_eq!(wait.key, "native_tool_approval");
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await
                .unwrap();
        }
        if agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status
            .is_terminal()
            || !worker.has_active_runs().await.unwrap()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("task did not settle");
}
async fn admit_until(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
    target: &str,
) -> ActivityExecution {
    for _ in 0..200 {
        worker.advance_work().await.unwrap();
        if let Some(call) = worker.admit().await.unwrap() {
            if call.handler == handler(target) || call.input["definition"]["id"]["name"] == target {
                return call;
            }
            let result = worker.execute(call.clone()).await.unwrap();
            worker.complete(call, result).await.unwrap();
        }
        let view = agents.actors().unwrap().task(task).await.unwrap();
        if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await
                .unwrap();
        }
    }
    panic!("{target} was not admitted");
}
fn workspace(engine_run: &Run) -> PathBuf {
    PathBuf::from(
        engine_run.record().input["input"]["workspace"]
            .as_str()
            .unwrap(),
    )
}
fn count_tests(workspace: &Path) -> usize {
    fs::read_to_string(workspace.join(".act0r-tools/review-tests"))
        .unwrap_or_default()
        .lines()
        .count()
}

#[test]
#[ignore = "fixed subprocess invoked by native review tests"]
fn review_test_process_fixture() {
    use std::io::Write;
    let workspace = std::env::current_dir().unwrap();
    let mut ledger = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(workspace.join(".act0r-tools/review-tests"))
        .unwrap();
    writeln!(ledger, "test execution").unwrap();
    let mode = fs::read_to_string(workspace.join("test-mode")).unwrap();
    if mode == "mutate" {
        fs::write(workspace.join("README.md"), "Changed by the test command\n").unwrap();
    }
    if mode == "fail" {
        panic!("expected failing test evidence");
    }
    assert!(
        fs::read_to_string(workspace.join("src/lib.rs"))
            .unwrap()
            .contains("19 + 23")
    );
    assert_eq!(
        fs::read_to_string(workspace.join("src/notes.txt")).unwrap(),
        "Created file\n"
    );
}

#[tokio::test]
async fn both_native_policies_save_task_scoped_patches_and_matching_test_evidence_across_restart() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("review.db");
        let src = source(dir.path(), "pass");
        let (engine, agents) = local_app(&db).await.unwrap();
        let actor = create(&agents, src.clone(), policy, config(), 30).await;
        let first = send(&agents, actor).await;
        let second = send(&agents, actor).await;
        let queued = agents.actors().unwrap().task_changes(second).await.unwrap();
        assert!(queued.enabled);
        assert!(queued.changes.is_none());
        let mut worker = engine.worker().await.unwrap();
        let baseline = admit_until(&engine, &agents, &mut worker, first, "baseline").await;
        let result = worker.execute(baseline.clone()).await.unwrap();
        worker
            .complete(baseline.clone(), result.clone())
            .await
            .unwrap();
        let saved = engine.status(baseline.run_id).await.unwrap();
        let next = saved.activities().last().unwrap().spec.input.clone();
        let ws = workspace(&saved);
        assert_eq!(saved.record().usage.admitted_model_calls, 0);
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert_eq!(
            engine
                .status(baseline.run_id)
                .await
                .unwrap()
                .activities()
                .last()
                .unwrap()
                .spec
                .input,
            next
        );
        worker.complete(baseline, result).await.unwrap();
        let final_call = admit_until(&engine, &agents, &mut worker, first, "changes").await;
        let final_result = worker.execute(final_call.clone()).await.unwrap();
        assert_eq!(count_tests(&ws), 1);
        assert!(
            agents
                .actors()
                .unwrap()
                .task_changes(first)
                .await
                .unwrap()
                .changes
                .is_none()
        );
        worker
            .complete(final_call.clone(), final_result.clone())
            .await
            .unwrap();
        let snapshot = json!(agents.actors().unwrap().task_changes(first).await.unwrap());
        let history = json!(engine.history(final_call.run_id).await.unwrap());
        worker
            .complete(final_call.clone(), final_result)
            .await
            .unwrap();
        assert_eq!(
            json!(engine.history(final_call.run_id).await.unwrap()),
            history
        );
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert_eq!(
            json!(agents.actors().unwrap().task_changes(first).await.unwrap()),
            snapshot
        );
        settle(&engine, &agents, &mut worker, second).await;
        let first = agents
            .actors()
            .unwrap()
            .task_changes(first)
            .await
            .unwrap()
            .changes
            .unwrap();
        let second = agents
            .actors()
            .unwrap()
            .task_changes(second)
            .await
            .unwrap()
            .changes
            .unwrap();
        assert_eq!(
            first
                .files
                .iter()
                .map(|f| f.path.as_str())
                .collect::<Vec<_>>(),
            vec!["src/lib.rs", "src/notes.txt"]
        );
        assert!(
            first
                .patch()
                .contains("+pub fn answer() -> i64 { 19 + 23 }")
        );
        assert!(first.patch().contains("--- /dev/null"));
        assert_eq!(first.test_status, TestEvidenceStatus::Matching);
        assert_eq!(first.tests.as_ref().unwrap().result["ok"], true);
        assert_eq!(first.tests.as_ref().unwrap().result["exit_code"], 0);
        assert_eq!(second.baseline_digest, first.workspace_digest);
        assert!(second.files.is_empty());
        assert_eq!(second.test_status, TestEvidenceStatus::Matching);
        assert_eq!(count_tests(&ws), 2);
        assert!(
            fs::read_to_string(src.join("src/lib.rs"))
                .unwrap()
                .contains("19 - 23")
        );
        fs::write(ws.join("src/lib.rs"), "A later manual edit\n").unwrap();
        let saved = agents
            .actors()
            .unwrap()
            .task_changes(snapshot["task_id"].as_str().unwrap().parse().unwrap())
            .await
            .unwrap()
            .changes
            .unwrap();
        assert_eq!(json!(saved), json!(first));
        assert_eq!(
            engine
                .status(first.run_id)
                .await
                .unwrap()
                .record()
                .usage
                .admitted_model_calls,
            5
        );
    }
}

#[tokio::test]
async fn evidence_distinguishes_failed_tests_post_test_edits_test_mutations_and_no_test() {
    for mode in ["fail", "mutate", "later", "none"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("review.db")).await.unwrap();
        let src = source(
            dir.path(),
            if mode == "later" || mode == "none" {
                "pass"
            } else {
                mode
            },
        );
        let mut cfg = config();
        if mode == "later" {
            cfg["max_model_calls"] = json!(6);
            cfg["script"].as_array_mut().unwrap().insert(4,json!({"kind":"call_tool","tool":"create_file","arguments":{"path":"src/later.txt","content":"After testing\n"}}));
        } else if mode == "none" {
            cfg["script"].as_array_mut().unwrap().remove(3);
        }
        let actor = create(&agents, src, "tool", cfg, 30).await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&engine, &agents, &mut worker, task).await;
        let changes = agents
            .actors()
            .unwrap()
            .task_changes(task)
            .await
            .unwrap()
            .changes
            .unwrap();
        assert_eq!(
            changes.test_status,
            match mode {
                "fail" => TestEvidenceStatus::Matching,
                "none" => TestEvidenceStatus::NotRun,
                _ => TestEvidenceStatus::Stale,
            }
        );
        if mode == "fail" {
            assert_eq!(changes.tests.as_ref().unwrap().result["ok"], false);
        }
        if mode == "mutate" {
            let tests = changes.tests.as_ref().unwrap();
            assert_ne!(tests.before_digest, tests.after_digest);
        }
    }
}

#[tokio::test]
async fn goal_holds_cancellation_and_attempt_caps_keep_baselines_and_prevent_new_capture() {
    for mode in ["hold", "cancel", "cap"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("review.db")).await.unwrap();
        let actor = create(
            &agents,
            source(dir.path(), "pass"),
            "tool",
            config(),
            if mode == "cap" { 1 } else { 30 },
        )
        .await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let baseline = admit_until(&engine, &agents, &mut worker, task, "baseline").await;
        worker
            .complete(
                baseline.clone(),
                worker.execute(baseline.clone()).await.unwrap(),
            )
            .await
            .unwrap();
        if mode == "hold" {
            agents.actors().unwrap().pause_goal(task).await.unwrap();
            assert!(worker.admit().await.unwrap().is_none());
            assert_eq!(
                engine
                    .status(baseline.run_id)
                    .await
                    .unwrap()
                    .record()
                    .usage
                    .admitted_model_calls,
                0
            );
            agents.actors().unwrap().resume_goal(task).await.unwrap();
        } else if mode == "cancel" {
            agents.actors().unwrap().cancel_goal(task).await.unwrap();
        }
        settle(&engine, &agents, &mut worker, task).await;
        let view = agents.actors().unwrap().task_changes(task).await.unwrap();
        assert_eq!(view.changes.is_some(), mode == "hold");
        if mode != "hold" {
            assert!(view.error.is_some());
            assert_eq!(
                count_tests(&workspace(&engine.status(baseline.run_id).await.unwrap())),
                0
            );
        }
    }
}

#[tokio::test]
async fn unsupported_changed_content_fails_without_a_partial_patch_or_source_mutation() {
    for kind in ["binary", "oversized"] {
        let dir = tempfile::tempdir().unwrap();
        let src = source(dir.path(), "pass");
        let (engine, agents) = local_app(dir.path().join("review.db")).await.unwrap();
        let actor = create(&agents, src.clone(), "tool", config(), 30).await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let call = admit_until(&engine, &agents, &mut worker, task, "changes").await;
        let ws = workspace(&engine.status(call.run_id).await.unwrap());
        fs::write(
            ws.join("src/bad.dat"),
            if kind == "binary" {
                vec![0, 0xff]
            } else {
                vec![b'x'; 1_048_577]
            },
        )
        .unwrap();
        let result = worker.execute(call.clone()).await.unwrap();
        assert!(matches!(result, Outcome::Failure(_)));
        worker.complete(call, result).await.unwrap();
        let view = agents.actors().unwrap().task_changes(task).await.unwrap();
        assert!(view.changes.is_none());
        assert!(view.error.is_some());
        assert_eq!(view.run_status, Some(RunStatus::Failed));
        assert!(!src.join("src/bad.dat").exists());
    }
}

#[test]
fn change_set_validation_rejects_inconsistent_hashes_paths_status_and_patch_bounds() {
    let mut value = WorkspaceChangeSet {
        version: 1,
        run_id: RunId::new(),
        activity_id: ActivityId::new(),
        baseline_digest: "a".repeat(64),
        workspace_digest: "b".repeat(64),
        files: vec![task::FileDiff {
            path: "src/x.rs".into(),
            before_sha256: Some("a".repeat(64)),
            after_sha256: Some("b".repeat(64)),
            patch: "a patch".into(),
            changed_bytes: 1,
        }],
        changed_bytes: 1,
        test_status: TestEvidenceStatus::NotRun,
        tests: None,
    };
    value.validate().unwrap();
    value.test_status = TestEvidenceStatus::Matching;
    assert!(value.validate().is_err());
    value.test_status = TestEvidenceStatus::NotRun;
    value.files[0].path = "../secret".into();
    assert!(value.validate().is_err());
    value.files[0].path = "src/x.rs".into();
    value.changed_bytes = 2;
    assert!(value.validate().is_err());
    value.changed_bytes = 1;
    value.files[0].patch = "x".repeat(MAX_PATCH_BYTES + 1);
    assert!(value.validate().is_err());
}

fn cli(db: &Path, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_act0r"));
    cmd.arg("--db").arg(db).arg("--json").args(args);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000);
    }
    cmd.output().unwrap()
}
#[tokio::test]
async fn cli_reads_only_committed_task_artifacts_and_exports_the_exact_patch_without_overwriting() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("review.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actor = create(&agents, source(dir.path(), "pass"), "tool", config(), 30).await;
    let task = send(&agents, actor).await;
    let id = task.to_string();
    let out = dir.path().join("review.patch");
    let path = out.to_str().unwrap();
    assert!(
        !cli(&db, &["actor", "task", "changes", &id, "--output", path])
            .status
            .success()
    );
    assert!(!out.exists());
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, task).await;
    let expected = agents
        .actors()
        .unwrap()
        .task_changes(task)
        .await
        .unwrap()
        .changes
        .unwrap()
        .patch();
    let ws = workspace(
        &engine
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
            .unwrap(),
    );
    assert!(ws.starts_with(dir.path().canonicalize().unwrap()));
    fs::rename(&ws, ws.with_extension("retired")).unwrap();
    let result = cli(&db, &["actor", "task", "changes", &id, "--output", path]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let view: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(view["changes"]["test_status"], "matching");
    assert_eq!(fs::read_to_string(&out).unwrap(), expected);
    assert!(
        !cli(&db, &["actor", "task", "changes", &id, "--output", path])
            .status
            .success()
    );
    assert_eq!(fs::read_to_string(&out).unwrap(), expected);
    assert!(
        !cli(
            &db,
            &["actor", "task", "changes", &MessageId::new().to_string()]
        )
        .status
        .success()
    );
}

#[tokio::test]
#[ignore = "spawned and killed by native workspace review recovery test"]
async fn review_crash_fixture() {
    let root = PathBuf::from(std::env::var("ACT0R_REVIEW_CRASH_ROOT").unwrap());
    let stage = std::env::var("ACT0R_REVIEW_CRASH_STAGE").unwrap();
    let (engine, agents) = local_app(root.join("review.db")).await.unwrap();
    let actor = create(&agents, source(&root, "pass"), "tool", config(), 30).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let call = admit_until(
        &engine,
        &agents,
        &mut worker,
        task,
        match stage.as_str() {
            "baseline" => "baseline",
            "fingerprint" => "fingerprint",
            "tested" => "run_tests",
            _ => "changes",
        },
    )
    .await;
    let result = worker.execute(call.clone()).await.unwrap();
    if stage == "saved" {
        worker.complete(call.clone(), result.clone()).await.unwrap();
    }
    let saved = engine.status(call.run_id).await.unwrap();
    fs::write(root.join("ready.json"),serde_json::to_vec(&json!({"task":task,"call":call,"result":result,"workspace":workspace(&saved),"next":saved.activities().last().unwrap().spec.input,"usage":saved.record().usage})).unwrap()).unwrap();
    let _keep = (engine, agents, worker);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
#[tokio::test]
async fn killed_hosts_reuse_published_baselines_fingerprints_and_patches_and_reconcile_interrupted_tests()
 {
    for stage in ["baseline", "fingerprint", "tested", "changes", "saved"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "review_crash_fixture"])
            .env("ACT0R_REVIEW_CRASH_ROOT", root)
            .env("ACT0R_REVIEW_CRASH_STAGE", stage)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let start = Instant::now();
        while !root.join("ready.json").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "fixture exited at {stage}"
            );
            if start.elapsed() > Duration::from_secs(30) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("fixture timed out at {stage}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let ready: Value =
            serde_json::from_slice(&fs::read(root.join("ready.json")).unwrap()).unwrap();
        let task: MessageId = serde_json::from_value(ready["task"].clone()).unwrap();
        let call: ActivityExecution = serde_json::from_value(ready["call"].clone()).unwrap();
        let result: Outcome = serde_json::from_value(ready["result"].clone()).unwrap();
        let ws = PathBuf::from(ready["workspace"].as_str().unwrap());
        if stage == "changes" {
            fs::write(
                ws.join("src/notes.txt"),
                "Later manual edit after capture\n",
            )
            .unwrap();
        }
        let (engine, agents) = local_app(root.join("review.db")).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        let recovered = engine.status(call.run_id).await.unwrap();
        assert_eq!(
            recovered.activities().last().unwrap().spec.input,
            ready["next"]
        );
        assert_eq!(json!(recovered.record().usage), ready["usage"]);
        if stage == "tested" {
            assert_eq!(recovered.record().status, RunStatus::Blocked);
            assert_eq!(count_tests(&ws), 1);
            engine.reconcile(call.run_id,ReconciliationRequest {activity_id:call.activity_id,attempt_id:call.attempt_id,decision:ReconciliationDecision::Accept {result:result.result().unwrap().clone()},evidence:"Fixed subprocess has stopped; recovered its saved output and verified workspace.".into()}).await.unwrap();
        }
        settle(&engine, &agents, &mut worker, task).await;
        let view = agents.actors().unwrap().task_changes(task).await.unwrap();
        let changes = view.changes.unwrap();
        assert_eq!(view.run_status, Some(RunStatus::Succeeded));
        assert_eq!(changes.test_status, TestEvidenceStatus::Matching);
        assert_eq!(count_tests(&ws), 1);
        if stage == "changes" || stage == "saved" {
            assert_eq!(json!(changes), result.result().unwrap().clone());
        }
        assert_eq!(
            engine
                .status(call.run_id)
                .await
                .unwrap()
                .record()
                .usage
                .admitted_model_calls,
            5
        );
        assert_eq!(
            engine
                .status(call.run_id)
                .await
                .unwrap()
                .attempts()
                .iter()
                .filter(|a| a.activity_id == call.activity_id)
                .count(),
            if stage == "saved" || stage == "tested" {
                1
            } else {
                2
            }
        );
    }
}

#[tokio::test]
async fn final_receipts_bind_requests_and_preserve_empty_files_deletions_unicode_and_unterminated_lines()
 {
    let dir = tempfile::tempdir().unwrap();
    let src = source(dir.path(), "pass");
    fs::write(src.join("src/empty-old.txt"), "").unwrap();
    fs::write(src.join("src/deleted.txt"), "Deleted content\n").unwrap();
    let (engine, agents) = local_app(dir.path().join("review.db")).await.unwrap();
    let actor = create(&agents, src, "tool", config(), 30).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let call = admit_until(&engine, &agents, &mut worker, task, "changes").await;
    let ws = workspace(&engine.status(call.run_id).await.unwrap());
    fs::remove_file(ws.join("src/empty-old.txt")).unwrap();
    fs::remove_file(ws.join("src/deleted.txt")).unwrap();
    fs::write(ws.join("src/empty-new.txt"), "").unwrap();
    fs::write(ws.join("src/notes.txt"), "Café 🦀").unwrap();
    let result = worker.execute(call.clone()).await.unwrap();
    let changes: WorkspaceChangeSet =
        serde_json::from_value(result.result().unwrap().clone()).unwrap();
    assert_eq!(changes.test_status, TestEvidenceStatus::Stale);
    assert!(changes.patch().contains("\\ No newline at end of file"));
    assert!(
        changes
            .files
            .iter()
            .find(|f| f.path == "src/empty-new.txt")
            .unwrap()
            .patch
            .contains("new file mode 100644")
    );
    assert!(
        changes
            .files
            .iter()
            .find(|f| f.path == "src/empty-old.txt")
            .unwrap()
            .patch
            .contains("deleted file mode 100644")
    );
    let mut altered = call.clone();
    altered.input["tests"] = Value::Null;
    assert!(matches!(
        worker.execute(altered).await.unwrap(),
        Outcome::Failure(_)
    ));
    fs::write(ws.join("src/notes.txt"), "Changed after publication").unwrap();
    assert_eq!(worker.execute(call.clone()).await.unwrap(), result);
    worker.complete(call, result).await.unwrap();
    assert_eq!(
        json!(
            agents
                .actors()
                .unwrap()
                .task_changes(task)
                .await
                .unwrap()
                .changes
                .unwrap()
        ),
        json!(changes)
    );
}

#[tokio::test]
async fn workspace_ownership_retry_keeps_the_baseline_intent_and_review_is_opt_in() {
    let dir = tempfile::tempdir().unwrap();
    let src = source(dir.path(), "pass");
    let db = dir.path().join("review.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let actor = create(&agents, src.clone(), "tool", config(), 30).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let call = admit_until(&engine, &agents, &mut worker, task, "baseline").await;
    let ws = workspace(&engine.status(call.run_id).await.unwrap());
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(ws.join(".act0r-tools/ownership.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    let result = worker.execute(call.clone()).await.unwrap();
    assert!(matches!(&result,Outcome::Failure(f) if f.retryable));
    worker.complete(call.clone(), result).await.unwrap();
    drop(lock);
    let pending = engine.status(call.run_id).await.unwrap();
    assert_eq!(pending.activities()[0].spec.input, call.input);
    assert!(pending.activities()[0].ready_at > pending.record().updated_at);
    settle(&engine, &agents, &mut worker, task).await;
    assert_eq!(
        engine
            .status(call.run_id)
            .await
            .unwrap()
            .attempts()
            .iter()
            .filter(|a| a.activity_id == call.activity_id)
            .count(),
        2
    );
    let mut cfg = config();
    cfg.as_object_mut().unwrap().remove("change_review");
    let minimal = create(&agents, src, "tool", cfg, 30).await;
    let task = send(&agents, minimal).await;
    settle(&engine, &agents, &mut worker, task).await;
    let view = agents.actors().unwrap().task_changes(task).await.unwrap();
    assert!(!view.enabled);
    assert!(view.changes.is_none());
    assert!(
        !engine
            .status(view.run_id.unwrap())
            .await
            .unwrap()
            .activities()
            .iter()
            .any(|a| is_handler(&a.spec.handler))
    );
    let mut no_source = config();
    no_source["tools"] = json!([]);
    no_source["writable_paths"] = json!([]);
    no_source["test_command"] = Value::Null;
    no_source["script"] = json!([{"kind":"finish","summary":"Done"}]);
    assert!(
        agents
            .create_registered_actor(
                DefinitionId::new("tool", 1),
                None,
                no_source,
                30,
                Default::default()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn planner_follow_ups_finalize_their_own_artifacts_under_one_goal_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("review.db")).await.unwrap();
    let mut cfg = config();
    cfg["follow_up"] = json!({"max_follow_ups":1,"min_delay_ms":1,"max_delay_ms":1000});
    cfg["goal_budget"] = json!({"max_model_calls":10,"max_tokens":2000000});
    cfg["script"][4] = json!({"kind":"schedule_follow_up","summary":"Repaired and tested","goal":"Review the existing repair","after_ms":1});
    let actor = create(&agents, source(dir.path(), "pass"), "planner", cfg, 30).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, task).await;
    let parent = agents.actors().unwrap().task(task).await.unwrap();
    let child = parent.follow_up.unwrap().reminder_id;
    for _ in 0..500 {
        worker.step().await.unwrap();
        if let Ok(view) = agents.actors().unwrap().task(child).await {
            if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
                engine
                    .signal(run, wait.id, json!({"approved":true}))
                    .await
                    .unwrap();
            }
            if view.status.is_terminal() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let a = agents
        .actors()
        .unwrap()
        .task_changes(task)
        .await
        .unwrap()
        .changes
        .unwrap();
    let b = agents
        .actors()
        .unwrap()
        .task_changes(child)
        .await
        .unwrap()
        .changes
        .unwrap();
    assert_eq!(b.baseline_digest, a.workspace_digest);
    assert!(b.files.is_empty());
    assert_eq!(b.test_status, TestEvidenceStatus::Matching);
    assert_eq!(
        agents
            .actors()
            .unwrap()
            .inspect_task(task, Default::default())
            .await
            .unwrap()
            .summary
            .visible_usage
            .admitted_model_calls,
        10
    );
}
