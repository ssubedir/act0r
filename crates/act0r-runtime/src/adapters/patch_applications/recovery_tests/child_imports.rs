//! Kill a real harness worker between child-import files, then reconcile through HTTP.
use super::*;
use crate::{
    adapters::{child_changes::LocalChildChanges, sqlite::SqliteStore},
    domain::{agent::ModelRequest, harness::*, tools::WorkspaceTestCommand},
};

struct ImportExecutor {
    inner: Arc<dyn ActivityExecutor>,
    imports: LocalChildChanges,
}
impl ActivityExecutor for ImportExecutor {
    fn supports(&self, id: &DefinitionId) -> bool {
        self.inner.supports(id)
    }
    fn execute(&self, call: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if call.handler == DefinitionId::new("agent.model", 1) {
                let request: ModelRequest = serde_json::from_value(call.input)?;
                let planner = request.schema.to_string().contains("import_child_changes");
                let actions = if planner {
                    vec![
                        json!({"kind":"delegate_task","worker":"coder","goal":"Repair both files and test"}),
                        json!({"kind":"inspect_child_changes","worker":"coder","offset":0}),
                        json!({"kind":"import_child_changes","worker":"coder"}),
                        json!({"kind":"finish","summary":"Premature finish must require combined tests"}),
                        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
                        json!({"kind":"finish","summary":"Imported and verified both files"}),
                    ]
                } else {
                    vec![
                        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/a.txt","old":"old a","new":"new a"}}),
                        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/b.txt","old":"old b","new":"new b"}}),
                        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
                        json!({"kind":"finish","summary":"Child patch tested"}),
                    ]
                };
                let action = actions
                    .get(request.turn as usize)
                    .expect("unexpected fixture model call");
                return Ok(Outcome::Success(json!({"action":action,"request_id":null})));
            }
            if call.handler == child_changes::handler("import") {
                self.imports.execute(call).await
            } else {
                self.inner.execute(call).await
            }
        })
    }
}
async fn import_app(root: &Path, boundary: Option<&str>) -> (Engine, AgentService) {
    let (mut engine, agents) = local_app(root.join("recovery.db")).await.unwrap();
    let mut applications = LocalPatchApplications::new(root.join(".act0r/applications"));
    if let Some(boundary) = boundary {
        let boundary = boundary.to_owned();
        let marker = root.join("import-boundary");
        applications.probe = Some(Arc::new(move |name| {
            if name == boundary {
                fs::write(&marker, name).unwrap();
                loop {
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }));
    }
    engine.executor = Arc::new(ImportExecutor {
        inner: engine.executor.clone(),
        imports: LocalChildChanges::new(
            Arc::new(SqliteStore::open(root.join("recovery.db")).await.unwrap()),
            Arc::new(applications),
            Arc::new(LocalTools::new(root.join(".act0r/workspaces"))),
        ),
    });
    (engine, agents)
}
fn command() -> WorkspaceTestCommand {
    WorkspaceTestCommand {
        argv: vec![
            std::env::current_exe().unwrap().to_string_lossy().into(),
            "--ignored".into(),
            "--exact".into(),
            "adapters::patch_applications::recovery_tests::child_imports::combined_test".into(),
            "--nocapture".into(),
        ],
        timeout_ms: 10000,
    }
}
#[test]
#[ignore = "runs only as the fixed command inside a managed fixture workspace"]
fn combined_test() {
    let root = std::env::current_dir().unwrap();
    assert!(root.join(PRIVATE).join("managed").is_file());
    assert_eq!(fs::read_to_string(root.join("src/a.txt")).unwrap(), "new a");
    assert_eq!(fs::read_to_string(root.join("src/b.txt")).unwrap(), "new b");
    fs::write(
        root.join(PRIVATE).join("combined-tested"),
        "both files verified",
    )
    .unwrap();
}
async fn settle_import(engine: &Engine) {
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..120 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("import fixture did not settle");
}
async fn fixture_import(root: &Path) -> (HarnessReceipt, RunId, String) {
    let source = root.join("project");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/a.txt"), "old a").unwrap();
    fs::write(source.join("src/b.txt"), "old b").unwrap();
    let (engine, agents) = import_app(root, None).await;
    let receipt=agents.teams().unwrap().harness().unwrap().start(serde_json::from_value(json!({
        "message_id":MessageId::new(),"prompt":"Repair both files","workspace":source.to_string_lossy(),
        "target":{"provider":"openai","model":"fixture","endpoint":"http://127.0.0.1:1234/v1/chat/completions"},
        "coding":{"writable_paths":["src/"],"test_command":command()}})).unwrap()).await.unwrap();
    for _ in 0..10 {
        settle_import(&engine).await;
        let inbox = agents
            .teams()
            .unwrap()
            .tasks()
            .unwrap()
            .inbox(receipt.team_id, receipt.task_id, Default::default())
            .await
            .unwrap();
        let item = inbox
            .items
            .iter()
            .find(|item| item.actionable)
            .expect("approval wait missing");
        agents
            .teams()
            .unwrap()
            .tasks()
            .unwrap()
            .respond(
                receipt.team_id,
                receipt.task_id,
                item.wait.id,
                HumanResponse::Approval { approved: true },
            )
            .await
            .unwrap();
        if item.wait.key == child_changes::APPROVAL_KEY {
            let workspace = agents
                .teams()
                .unwrap()
                .get(receipt.team_id)
                .await
                .unwrap()
                .members[0]
                .workspace
                .as_ref()
                .unwrap()
                .path
                .clone();
            assert_eq!(
                fs::read_to_string(Path::new(&workspace).join("src/a.txt")).unwrap(),
                "old a"
            );
            return (receipt, item.run_id, workspace);
        }
    }
    panic!("import approval was never reached");
}
#[tokio::test]
#[ignore = "worker stopped at a durable per-file import boundary"]
async fn import_host() {
    let root = std::env::current_dir().unwrap();
    let boundary = fs::read_to_string(root.join("import-stop-at")).unwrap();
    let (engine, _) = import_app(&root, Some(&boundary)).await;
    settle_import(&engine).await;
    panic!("import did not reach selected boundary");
}
async fn kill_import(root: &Path, boundary: &str) {
    fs::write(root.join("import-stop-at"), boundary).unwrap();
    let marker = root.join("import-boundary");
    let _ = fs::remove_file(&marker);
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--ignored",
            "--exact",
            "adapters::patch_applications::recovery_tests::child_imports::import_host",
            "--nocapture",
        ])
        .current_dir(root)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = command.spawn().unwrap();
    let start = Instant::now();
    while !marker.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("import host exited early: {status}");
        }
        if start.elapsed() > Duration::from_secs(30) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("import boundary timed out");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
}
fn import_base(server: &Server, receipt: &HarnessReceipt) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        server.url, receipt.session_id, receipt.task_id
    )
}
fn decision(recovery: &Value, action: &str) -> Value {
    json!({"activity_id":recovery["activity_id"],"attempt_id":recovery["attempt_id"],"operation_stopped":true,"evidence":"Previous worker exited; inspected exact saved import and managed files.",
        "decision":if action=="retry" {json!({"action":"retry"})} else {json!({"action":"fail","message":"Retain partial import for review."})}})
}
#[tokio::test]
async fn partial_child_import_recovers_scoped_receipts_and_requires_combined_tests() {
    for boundary in ["file_effect", "file_saved", "completed"] {
        let dir = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(dir.path()).unwrap();
        let (receipt, run, workspace) = fixture_import(&root).await;
        kill_import(&root, boundary).await;
        let (engine, agents) = import_app(&root, None).await;
        settle_import(&engine).await;
        let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
        let coder = team
            .members
            .iter()
            .find(|member| member.name == "coder")
            .unwrap();
        // Recovery never consults the child's now-mutable workspace.
        fs::write(
            Path::new(&coder.workspace.as_ref().unwrap().path).join("src/b.txt"),
            "later child edit",
        )
        .unwrap();
        let server = Server::start(agents).await;
        let client = Client::new();
        let base = import_base(&server, &receipt);
        let page = get(&client, &format!("{base}/child-import-recovery")).await;
        let recovery = &page["items"][0]["recovery"];
        assert_eq!(recovery["retry_allowed"], true, "{page}");
        let delegated = get(&client, &format!("{base}/delegated-work")).await;
        assert_eq!(delegated["groups"][0]["joined"], true);
        let child = &delegated["groups"][0]["children"][0];
        assert_eq!(child["status"], "succeeded");
        assert_eq!(child["review"]["status"], "interrupted");
        assert_eq!(
            child["review"]["artifact_id"],
            recovery["request"]["artifact_id"]
        );
        assert_eq!(child["changes"]["file_count"], 2);
        assert_eq!(recovery["request"]["request"]["worker"], "coder");
        assert_eq!(
            recovery["inspection"]["files"][0]["progress"],
            if boundary == "file_effect" {
                "intended"
            } else {
                "applied"
            }
        );
        assert_eq!(
            recovery["inspection"]["files"][1]["progress"],
            if boundary == "completed" {
                "applied"
            } else {
                "pending"
            }
        );
        assert!(
            !Path::new(&workspace)
                .join(PRIVATE)
                .join("combined-tested")
                .exists()
        );
        assert!(
            get(&client, &format!("{base}/tool-recovery")).await["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let url = format!("{base}/runs/{run}/child-import-reconciliation");
        let body = decision(recovery, "retry");
        let mut invalid = body.clone();
        invalid["operation_stopped"] = json!(false);
        post(&client, &url, &invalid, StatusCode::BAD_REQUEST).await;
        let mut invalid = body.clone();
        invalid["decision"] = json!({"action":"accept","result":{"ok":true}});
        let rejected = post(&client, &url, &invalid, StatusCode::BAD_REQUEST).await;
        assert_eq!(rejected["error"]["code"], "invalid_request");
        let mut invalid = body.clone();
        invalid["target"] = json!(workspace);
        post(&client, &url, &invalid, StatusCode::UNPROCESSABLE_ENTITY).await;
        post(
            &client,
            &format!("{base}/runs/{}/child-import-reconciliation", RunId::new()),
            &body,
            StatusCode::NOT_FOUND,
        )
        .await;
        let foreign = format!(
            "{}/v1/harness/sessions/{}/turns/{}/runs/{run}/child-import-reconciliation",
            server.url,
            SessionId::new(),
            receipt.task_id
        );
        post(&client, &foreign, &body, StatusCode::NOT_FOUND).await;
        post(
            &client,
            &format!("{base}/runs/{run}/reconciliation"),
            &body,
            StatusCode::BAD_REQUEST,
        )
        .await;
        let (a, b) = tokio::join!(
            post(&client, &url, &body, StatusCode::OK),
            post(&client, &url, &body, StatusCode::OK)
        );
        assert_eq!(a, b);
        // Restart after the recovery receipt, before any resumed effect.
        server.close().await;
        drop(engine);
        let (engine, agents) = import_app(&root, None).await;
        settle_import(&engine).await;
        let inbox = agents
            .teams()
            .unwrap()
            .tasks()
            .unwrap()
            .inbox(receipt.team_id, receipt.task_id, Default::default())
            .await
            .unwrap();
        let item = inbox
            .items
            .iter()
            .find(|item| item.actionable)
            .expect("fresh parent tests require approval");
        assert_eq!(item.wait.key, "native_tool_approval");
        assert!(
            !Path::new(&workspace)
                .join(PRIVATE)
                .join("combined-tested")
                .exists()
        );
        agents
            .teams()
            .unwrap()
            .tasks()
            .unwrap()
            .respond(
                receipt.team_id,
                receipt.task_id,
                item.wait.id,
                HumanResponse::Approval { approved: true },
            )
            .await
            .unwrap();
        settle_import(&engine).await;
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Succeeded
        );
        let changes = agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .changes(receipt.session_id, receipt.task_id)
            .await
            .unwrap();
        assert_eq!(
            changes.changes.unwrap().test_status,
            change_review::TestEvidenceStatus::Matching
        );
        assert!(
            Path::new(&workspace)
                .join(PRIVATE)
                .join("combined-tested")
                .exists()
        );
        assert_eq!(
            fs::read_to_string(root.join("project/src/a.txt")).unwrap(),
            "old a"
        );
        let server = Server::start(agents).await;
        let base = import_base(&server, &receipt);
        let url = format!("{base}/runs/{run}/child-import-reconciliation");
        post(&client, &url, &body, StatusCode::OK).await;
        let mut changed = body.clone();
        changed["evidence"] = json!("changed receipt");
        post(&client, &url, &changed, StatusCode::CONFLICT).await;
        let page = get(&client, &format!("{base}/child-import-recovery")).await;
        assert!(page["items"][0]["recovery"].is_null());
        let delegated = get(&client, &format!("{base}/delegated-work")).await;
        assert_eq!(
            delegated["groups"][0]["children"][0]["review"]["status"],
            "imported"
        );
        assert_eq!(
            page["items"][0]["reconciliation_receipts"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        server.close().await;
    }
}

#[tokio::test]
async fn import_recovery_detects_conflicts_and_corruption_and_failure_retains_partial_files() {
    for damage in [
        "applied_file",
        "pending_file",
        "journal",
        "receipt",
        "permissions",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(dir.path()).unwrap();
        let (receipt, run, workspace) = fixture_import(&root).await;
        kill_import(&root, "file_saved").await;
        let (engine, agents) = import_app(&root, None).await;
        settle_import(&engine).await;
        let saved = engine.status(run).await.unwrap();
        let activity = saved
            .activities()
            .iter()
            .find(|a| a.spec.handler == child_changes::handler("import"))
            .unwrap();
        let inspection: child_changes::Inspection =
            serde_json::from_value(activity.spec.input.clone()).unwrap();
        let storage = root
            .join(".act0r/applications")
            .join(inspection.preview.preparation_id.to_string());
        match damage {
            "applied_file" => {
                fs::write(Path::new(&workspace).join("src/a.txt"), "external edit a").unwrap()
            }
            "pending_file" => {
                fs::write(Path::new(&workspace).join("src/b.txt"), "external edit b").unwrap()
            }
            "journal" => fs::write(storage.join("write.json"), "{}").unwrap(),
            "receipt" => fs::remove_file(storage.join("inspection.json")).unwrap(),
            _ => {
                let path = storage.join("prepared.json");
                let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                value["managed"]["writable_paths"] = json!(["README.md"]);
                fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
            }
        }
        let server = Server::start(agents).await;
        let client = Client::new();
        let base = import_base(&server, &receipt);
        let page = get(&client, &format!("{base}/child-import-recovery")).await;
        let recovery = &page["items"][0]["recovery"];
        assert_eq!(recovery["retry_allowed"], false, "{damage}: {page}");
        if damage.ends_with("file") {
            assert!(
                recovery["inspection"]["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|file| file["file"]["conflict"].is_string())
            );
        } else {
            assert!(recovery["inspection_error"].is_string(), "{page}");
        }
        if damage == "receipt" {
            assert!(!storage.join("inspection.json").exists());
        }
        if damage == "journal" {
            assert_eq!(
                fs::read_to_string(storage.join("write.json")).unwrap(),
                "{}"
            );
        }
        // GET is read-only, including when the inspection receipt or journal is unreadable.
        let before = fs::read(Path::new(&workspace).join("src/a.txt")).unwrap();
        let remaining = fs::read(Path::new(&workspace).join("src/b.txt")).unwrap();
        let body = decision(recovery, "fail");
        let url = format!("{base}/runs/{run}/child-import-reconciliation");
        post(&client, &url, &body, StatusCode::OK).await;
        server.close().await;
        drop(engine);
        let (engine, agents) = import_app(&root, None).await;
        settle_import(&engine).await;
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Failed
        );
        assert_eq!(
            fs::read(Path::new(&workspace).join("src/a.txt")).unwrap(),
            before
        );
        assert_eq!(
            fs::read(Path::new(&workspace).join("src/b.txt")).unwrap(),
            remaining
        );
        assert_eq!(
            fs::read_to_string(root.join("project/src/a.txt")).unwrap(),
            "old a"
        );
        let server = Server::start(agents).await;
        let base = import_base(&server, &receipt);
        let url = format!("{base}/runs/{run}/child-import-reconciliation");
        post(&client, &url, &body, StatusCode::OK).await;
        assert!(
            get(&client, &format!("{base}/child-import-recovery")).await["items"][0]["recovery"]
                .is_null()
        );
        server.close().await;
    }
}

#[tokio::test]
async fn import_recovery_preserves_goal_holds_turn_scope_and_newer_interruption() {
    let dir = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(dir.path()).unwrap();
    let (receipt, run, workspace) = fixture_import(&root).await;
    kill_import(&root, "file_saved").await;
    let (engine, agents) = import_app(&root, None).await;
    settle_import(&engine).await;
    let follow = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .append(
            receipt.session_id,
            HarnessTurnRequest {
                message_id: MessageId::new(),
                prompt: "Queued work".into(),
            },
        )
        .await
        .unwrap();
    agents
        .actors()
        .unwrap()
        .pause_goal(receipt.task_id)
        .await
        .unwrap();
    let server = Server::start(agents).await;
    let client = Client::new();
    let base = import_base(&server, &receipt);
    let page = get(&client, &format!("{base}/child-import-recovery")).await;
    let recovery = &page["items"][0]["recovery"];
    assert_eq!(recovery["paused"], true);
    let body = decision(recovery, "retry");
    let url = format!("{base}/runs/{run}/child-import-reconciliation");
    let wrong_turn = format!(
        "{}/runs/{run}/child-import-reconciliation",
        import_base(&server, &follow)
    );
    post(&client, &wrong_turn, &body, StatusCode::NOT_FOUND).await;
    post(&client, &url, &body, StatusCode::OK).await;
    settle_import(&engine).await;
    assert_eq!(
        fs::read_to_string(Path::new(&workspace).join("src/b.txt")).unwrap(),
        "old b"
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = import_app(&root, None).await;
    settle_import(&engine).await;
    assert_eq!(
        fs::read_to_string(Path::new(&workspace).join("src/b.txt")).unwrap(),
        "old b"
    );
    assert!(
        agents
            .actors()
            .unwrap()
            .task(follow.task_id)
            .await
            .unwrap()
            .run_id
            .is_none()
    );
    agents
        .actors()
        .unwrap()
        .resume_goal(receipt.task_id)
        .await
        .unwrap();
    drop(agents);
    drop(engine);
    // The second attempt dies after the full receipt; replay of the old decision must not unblock it.
    kill_import(&root, "completed").await;
    let (engine, agents) = import_app(&root, None).await;
    settle_import(&engine).await;
    let server = Server::start(agents).await;
    let base = import_base(&server, &receipt);
    let url = format!("{base}/runs/{run}/child-import-reconciliation");
    let page = get(&client, &format!("{base}/child-import-recovery")).await;
    let newer = &page["items"][0]["recovery"];
    assert_ne!(newer["attempt_id"], body["attempt_id"]);
    post(&client, &url, &body, StatusCode::OK).await;
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Blocked
    );
    assert_eq!(
        get(&client, &format!("{base}/child-import-recovery")).await["items"][0]["recovery"]["attempt_id"],
        newer["attempt_id"]
    );
    // A completed journal may acknowledge its result even after unrelated later workspace changes.
    fs::write(Path::new(&workspace).join("src/a.txt"), "later parent edit").unwrap();
    assert_eq!(
        get(&client, &format!("{base}/child-import-recovery")).await["items"][0]["recovery"]["retry_allowed"],
        true
    );
    post(&client, &url, &decision(newer, "fail"), StatusCode::OK).await;
    server.close().await;
}

#[tokio::test]
async fn cancellation_prevents_child_import_recovery_from_releasing_new_writes() {
    let dir = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(dir.path()).unwrap();
    let (receipt, run, workspace) = fixture_import(&root).await;
    kill_import(&root, "file_saved").await;
    let (engine, agents) = import_app(&root, None).await;
    settle_import(&engine).await;
    let recovery = serde_json::to_value(
        agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .child_import_recoveries(receipt.session_id, receipt.task_id)
            .await
            .unwrap(),
    )
    .unwrap()["items"][0]["recovery"]
        .clone();
    agents
        .actors()
        .unwrap()
        .cancel_goal(receipt.task_id)
        .await
        .unwrap();
    let server = Server::start(agents).await;
    let client = Client::new();
    let base = import_base(&server, &receipt);
    let cancelled = get(&client, &format!("{base}/child-import-recovery")).await;
    assert_eq!(cancelled["items"][0]["recovery"]["cancelled"], true);
    assert_eq!(cancelled["items"][0]["recovery"]["retry_allowed"], false);
    post(
        &client,
        &format!("{base}/runs/{run}/child-import-reconciliation"),
        &decision(&recovery, "retry"),
        StatusCode::OK,
    )
    .await;
    let page = get(&client, &format!("{base}/child-import-recovery")).await;
    assert!(
        page["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["recovery"].is_null())
    );
    settle_import(&engine).await;
    assert_eq!(
        fs::read_to_string(Path::new(&workspace).join("src/a.txt")).unwrap(),
        "new a"
    );
    assert_eq!(
        fs::read_to_string(Path::new(&workspace).join("src/b.txt")).unwrap(),
        "old b"
    );
    server.close().await;
}
