//! Real killed worker and HTTP recovery against a persisted coding-session scope.
use super::*;
use crate::{Engine, adapters::http_api, application::AgentService, bootstrap::local_app};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::{
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::oneshot, task::JoinHandle};
mod child_imports;

struct Executor(Arc<LocalPatchApplications>);
impl ActivityExecutor for Executor {
    fn supports(&self, id: &DefinitionId) -> bool {
        is_handler(id)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        self.0.execute(execution)
    }
}
async fn app(root: &Path, boundary: Option<&str>) -> (Engine, AgentService) {
    let (mut engine, agents) = local_app(root.join("recovery.db")).await.unwrap();
    let mut adapter = LocalPatchApplications::new(root.join(".act0r/applications"));
    if let Some(boundary) = boundary {
        let boundary = boundary.to_owned();
        let marker = root.join("boundary-reached");
        adapter.probe = Some(Arc::new(move |name| {
            if name == boundary {
                fs::write(&marker, name).unwrap();
                loop {
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }));
    }
    // This fixture executes only the patch workflow; queued model tasks never run.
    engine.executor = Arc::new(Executor(Arc::new(adapter)));
    engine.coordinator = None;
    (engine, agents)
}
struct Server {
    url: String,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), Error>>,
}
impl Server {
    async fn start(agents: AgentService) -> Self {
        let listener = http_api::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(http_api::serve(
            listener,
            Arc::new(agents),
            http_api::ApiOptions::default(),
            async {
                let _ = stopped.await;
                Ok(())
            },
        ));
        Self { url, stop, task }
    }
    async fn close(self) {
        let _ = self.stop.send(());
        self.task.await.unwrap().unwrap();
    }
}
async fn post(client: &Client, url: &str, body: &Value, expected: StatusCode) -> Value {
    let response = client.post(url).json(body).send().await.unwrap();
    let status = response.status();
    let value: Value = response.json().await.unwrap();
    assert_eq!(status, expected, "{value}");
    value
}
async fn get(client: &Client, url: &str) -> Value {
    let response = client.get(url).send().await.unwrap();
    let status = response.status();
    let value = response.json().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{url}: {value}");
    value
}
fn fixture(target: &Path, task: MessageId) -> ApplicationInput {
    let files = ["a", "b"]
        .into_iter()
        .map(|name| {
            let path = format!("src/{name}.rs");
            let before = format!("old {name}");
            let after = format!("new {name}");
            let (patch, changed_bytes) = patch(&path, true, true, &before, &after);
            task::FileDiff {
                path,
                before_sha256: Some(hash(before.as_bytes())),
                after_sha256: Some(hash(after.as_bytes())),
                patch,
                changed_bytes,
            }
        })
        .collect::<Vec<_>>();
    ApplicationInput {
        task_id: task,
        target: target.to_string_lossy().into(),
        changes: change_review::WorkspaceChangeSet {
            version: 1,
            run_id: RunId::new(),
            activity_id: ActivityId::new(),
            baseline_digest: hash(b"before"),
            workspace_digest: hash(b"after"),
            changed_bytes: files.iter().map(|file| file.changed_bytes).sum(),
            files,
            test_status: change_review::TestEvidenceStatus::NotRun,
            tests: None,
        },
    }
}
fn base(server: &Server, session: SessionId, task: MessageId, run: RunId) -> String {
    format!(
        "{}/v1/harness/sessions/{session}/turns/{task}/applications/{run}",
        server.url
    )
}
async fn application(engine: &Engine, target: &Path, task: MessageId) -> RunId {
    let id = engine
        .start(workflow(), json!(fixture(target, task)), 20)
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.step().await.unwrap());
    let run = engine.status(id).await.unwrap();
    engine
        .signal(
            id,
            run.record().wait.as_ref().unwrap().id,
            json!({"approved":true}),
        )
        .await
        .unwrap();
    id
}
async fn session(agents: &AgentService, target: &Path) -> harness::HarnessReceipt {
    agents.teams().unwrap().harness().unwrap().start(serde_json::from_value(json!({
        "message_id":MessageId::new(),"prompt":"Repair this fixture","workspace":target.to_string_lossy(),
        "target":{"provider":"openai","model":"unused","endpoint":"http://127.0.0.1:1234/v1/chat/completions"},
        "coding":{"writable_paths":["src/"],"test_command":{"argv":["cargo","test"],"timeout_ms":1000}}
    })).unwrap()).await.unwrap()
}
#[tokio::test]
#[ignore = "child worker killed after the first synced file progress"]
async fn interrupted_application_worker() {
    let root = std::env::current_dir().unwrap();
    let boundary = fs::read_to_string(root.join("boundary")).unwrap();
    let (engine, _) = app(&root, Some(&boundary)).await;
    let mut worker = engine.worker().await.unwrap();
    worker.step().await.unwrap();
    panic!("worker did not reach the selected boundary");
}
async fn kill_at(root: &Path, boundary: &str) {
    fs::write(root.join("boundary"), boundary).unwrap();
    let _ = fs::remove_file(root.join("boundary-reached"));
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--ignored",
            "--exact",
            "adapters::patch_applications::recovery_tests::interrupted_application_worker",
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
    while !root.join("boundary-reached").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("application fixture exited early: {status}");
        }
        if start.elapsed() > Duration::from_secs(30) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("application boundary timed out");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
}
#[tokio::test]
async fn http_recovers_partial_writes_with_exact_scoped_durable_decisions() {
    let dir = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(dir.path()).unwrap();
    let target = root.join("project");
    fs::create_dir_all(target.join("src")).unwrap();
    fs::write(target.join("src/a.rs"), "old a").unwrap();
    fs::write(target.join("src/b.rs"), "old b").unwrap();
    let (engine, agents) = app(&root, None).await;
    let session = agents.teams().unwrap().harness().unwrap().start(serde_json::from_value(json!({
        "message_id":MessageId::new(),"prompt":"Repair this fixture","workspace":target.to_string_lossy(),
        "target":{"provider":"openai","model":"unused","endpoint":"http://127.0.0.1:1234/v1/chat/completions"},
        "coding":{"writable_paths":["src/"],"test_command":{"argv":["cargo","test"],"timeout_ms":1000}}
    })).unwrap()).await.unwrap();
    let run = application(&engine, &target, session.task_id).await;
    let server = Server::start(agents).await;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let url = base(&server, session.session_id, session.task_id, run);
    assert!(get(&client, &url).await["recovery"].is_null());
    post(&client,&format!("{url}/reconciliation"),&json!({"activity_id":ActivityId::new(),"attempt_id":AttemptId::new(),"decision":{"action":"retry"},"evidence":"Unrelated identity"}),StatusCode::BAD_REQUEST).await;
    server.close().await;
    drop(engine);
    kill_at(&root, "file_saved").await;
    assert_eq!(
        fs::read_to_string(target.join("src/a.rs")).unwrap(),
        "new a"
    );
    assert_eq!(
        fs::read_to_string(target.join("src/b.rs")).unwrap(),
        "old b"
    );
    let a_metadata = fs::metadata(target.join("src/a.rs"))
        .unwrap()
        .modified()
        .unwrap();
    let (engine, agents) = app(&root, None).await;
    let mut worker = engine.worker().await.unwrap(); // Persist the interrupted attempt before showing recovery.
    assert!(!worker.step().await.unwrap());
    let server = Server::start(agents).await;
    let url = base(&server, session.session_id, session.task_id, run);
    let view = get(&client, &url).await;
    assert_eq!(view["status"], "blocked");
    assert_eq!(view["recovery"]["retry_allowed"], true);
    assert_eq!(
        view["recovery"]["inspection"]["files"][0]["progress"],
        "applied"
    );
    assert_eq!(
        view["recovery"]["inspection"]["files"][1]["progress"],
        "pending"
    );
    let request = json!({"activity_id":view["recovery"]["activity_id"],"attempt_id":view["recovery"]["attempt_id"],"decision":{"action":"retry"},"evidence":"Killed the old worker and checked the journal and files."});
    // A live target conflict is visible and is not overwritten, even on an explicit retry.
    fs::write(target.join("src/b.rs"), "user conflict").unwrap();
    let conflict = get(&client, &url).await;
    assert_eq!(conflict["recovery"]["retry_allowed"], false);
    assert!(!conflict["recovery"]["inspection"]["files"][1]["file"]["conflict"].is_null());
    for wrong in [
        base(&server, SessionId::new(), session.task_id, run),
        base(&server, session.session_id, MessageId::new(), run),
    ] {
        post(
            &client,
            &format!("{wrong}/reconciliation"),
            &request,
            StatusCode::NOT_FOUND,
        )
        .await;
    }
    for invalid in [
        json!({"action":"accept","result":{"status":"applied"}}),
        json!({"action":"retry","extra":1}),
    ] {
        let mut invalid_request = request.clone();
        invalid_request["decision"] = invalid;
        post(
            &client,
            &format!("{url}/reconciliation"),
            &invalid_request,
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await;
    }
    let decision_url = format!("{url}/reconciliation");
    let (receipt, replayed) = tokio::join!(
        post(&client, &decision_url, &request, StatusCode::OK),
        post(&client, &decision_url, &request, StatusCode::OK)
    );
    assert_eq!(receipt, replayed);
    worker.step().await.unwrap();
    let second = get(&client, &url).await;
    assert_eq!(second["status"], "blocked");
    assert_ne!(second["recovery"]["attempt_id"], request["attempt_id"]);
    assert_eq!(
        fs::read_to_string(target.join("src/b.rs")).unwrap(),
        "user conflict"
    );
    post(&client, &decision_url, &request, StatusCode::OK).await;
    assert_eq!(
        get(&client, &url).await["recovery"]["attempt_id"],
        second["recovery"]["attempt_id"]
    );
    let mut changed = request.clone();
    changed["evidence"] = json!("Changed notes");
    post(&client, &decision_url, &changed, StatusCode::CONFLICT).await;
    fs::write(target.join("src/b.rs"), "old b").unwrap();
    let next = json!({"activity_id":second["recovery"]["activity_id"],"attempt_id":second["recovery"]["attempt_id"],"decision":{"action":"retry"},"evidence":"Restored the remaining file to its before hash; kept the applied file."});
    post(&client, &decision_url, &next, StatusCode::OK).await;
    drop(worker);
    server.close().await;
    drop(engine);
    // The saved decision survives API and worker restart before execution.
    let (engine, agents) = app(&root, None).await;
    let server = Server::start(agents).await;
    let url = base(&server, session.session_id, session.task_id, run);
    post(
        &client,
        &format!("{url}/reconciliation"),
        &next,
        StatusCode::OK,
    )
    .await;
    let mut worker = engine.worker().await.unwrap();
    assert!(worker.step().await.unwrap());
    let final_view = get(&client, &url).await;
    assert_eq!(final_view["report"]["status"], "applied");
    assert!(final_view["recovery"].is_null());
    assert_eq!(
        final_view["reconciliation_receipts"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        fs::read_to_string(target.join("src/b.rs")).unwrap(),
        "new b"
    );
    assert_eq!(
        fs::metadata(target.join("src/a.rs"))
            .unwrap()
            .modified()
            .unwrap(),
        a_metadata,
        "applied file was rewritten"
    );
    post(
        &client,
        &format!("{url}/reconciliation"),
        &request,
        StatusCode::OK,
    )
    .await;
    post(
        &client,
        &format!("{url}/reconciliation"),
        &next,
        StatusCode::OK,
    )
    .await;
    server.close().await;
}

#[tokio::test]
async fn http_handles_missing_corrupt_and_completed_journals_and_failure_after_partial_writes() {
    for mode in ["not_started", "completed", "corrupt", "partial_failure"] {
        let dir = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(dir.path()).unwrap();
        let target = root.join("project");
        fs::create_dir_all(target.join("src")).unwrap();
        fs::write(target.join("src/a.rs"), "old a").unwrap();
        fs::write(target.join("src/b.rs"), "old b").unwrap();
        let (engine, agents) = app(&root, None).await;
        let session = session(&agents, &target).await;
        let run = application(&engine, &target, session.task_id).await;
        if mode == "partial_failure" {
            kill_at(&root, "file_saved").await;
        } else {
            let mut worker = engine.worker().await.unwrap();
            let call = worker.admit().await.unwrap().unwrap();
            if mode != "not_started" {
                worker.execute(call).await.unwrap();
            }
            // Deliberately omit SQLite completion, as a host killed after filesystem execution would.
        }
        drop(engine);
        drop(agents);
        let (engine, agents) = app(&root, None).await;
        let mut worker = engine.worker().await.unwrap();
        assert!(!worker.step().await.unwrap());
        let server = Server::start(agents).await;
        let client = Client::builder().no_proxy().build().unwrap();
        let url = base(&server, session.session_id, session.task_id, run);
        let view = get(&client, &url).await;
        let preparation = view["preview"]["preparation_id"].as_str().unwrap();
        let journal = root
            .join(".act0r/applications")
            .join(preparation)
            .join("write.json");
        if mode == "not_started" {
            assert_eq!(view["recovery"]["inspection"]["journal_exists"], false);
            assert!(!journal.exists(), "inspection created a journal");
        }
        if mode == "completed" {
            fs::write(target.join("src/a.rs"), "later user edit").unwrap();
            let observed = get(&client, &url).await;
            assert_eq!(observed["recovery"]["retry_allowed"], true);
            assert_eq!(
                observed["recovery"]["inspection"]["report"]["status"],
                "applied"
            );
            assert!(!observed["recovery"]["inspection"]["files"][0]["file"]["conflict"].is_null());
        }
        if mode == "corrupt" {
            let mut saved: Value = serde_json::from_slice(&fs::read(&journal).unwrap()).unwrap();
            saved["activity_id"] = json!(ActivityId::new());
            fs::write(&journal, serde_json::to_vec(&saved).unwrap()).unwrap();
            let observed = get(&client, &url).await;
            assert_eq!(observed["recovery"]["retry_allowed"], false);
            assert!(!observed["recovery"]["inspection_error"].is_null());
        }
        let failed = matches!(mode, "partial_failure" | "corrupt");
        let request = json!({"activity_id":view["recovery"]["activity_id"],"attempt_id":view["recovery"]["attempt_id"],"decision":if failed {json!({"action":"fail","message":"Keep existing effects for manual inspection."})} else {json!({"action":"retry"})},"evidence":"The old worker stopped and the target and journal were inspected."});
        let mut invalid = request.clone();
        invalid["evidence"] = json!(" ");
        post(
            &client,
            &format!("{url}/reconciliation"),
            &invalid,
            StatusCode::BAD_REQUEST,
        )
        .await;
        invalid["evidence"] = json!("x".repeat(16385));
        post(
            &client,
            &format!("{url}/reconciliation"),
            &invalid,
            StatusCode::BAD_REQUEST,
        )
        .await;
        if failed {
            invalid = request.clone();
            invalid["decision"]["message"] = json!(" ");
            post(
                &client,
                &format!("{url}/reconciliation"),
                &invalid,
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
        post(
            &client,
            &format!("{url}/reconciliation"),
            &request,
            StatusCode::OK,
        )
        .await;
        if !failed {
            assert!(worker.step().await.unwrap());
        }
        let final_view = get(&client, &url).await;
        assert_eq!(
            final_view["status"],
            if failed { "failed" } else { "succeeded" }
        );
        if failed {
            assert_eq!(final_view["error"]["code"], "reconciliation_failed");
        }
        if mode == "partial_failure" {
            assert_eq!(
                fs::read_to_string(target.join("src/a.rs")).unwrap(),
                "new a"
            );
            assert_eq!(
                fs::read_to_string(target.join("src/b.rs")).unwrap(),
                "old b"
            );
        }
        if mode == "completed" {
            assert_eq!(
                fs::read_to_string(target.join("src/a.rs")).unwrap(),
                "later user edit"
            );
        }
        drop(worker);
        server.close().await;
        drop(engine);
        let (_, agents) = app(&root, None).await;
        let server = Server::start(agents).await;
        let url = base(&server, session.session_id, session.task_id, run);
        post(
            &client,
            &format!("{url}/reconciliation"),
            &request,
            StatusCode::OK,
        )
        .await;
        assert_eq!(
            get(&client, &url).await["reconciliation_receipts"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let mut changed = request;
        changed["decision"] = json!({"action":"fail","message":"Changed outcome"});
        post(
            &client,
            &format!("{url}/reconciliation"),
            &changed,
            StatusCode::CONFLICT,
        )
        .await;
        server.close().await;
    }
}
