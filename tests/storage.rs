use act0r::{
    Engine, Error,
    adapters::{sqlite::SqliteStore, storage::LocalStorage},
    application::{AgentService, StorageService, ports::*},
    bootstrap::local_app,
    domain::{agent::*, harness::*, storage::*, *},
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

fn cache(workspace: &Path) {
    let target = workspace.join("target");
    fs::create_dir_all(target.join("debug")).unwrap();
    fs::write(
        target.join("CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55\n",
    )
    .unwrap();
    fs::write(target.join(".rustc_info.json"), "{}").unwrap();
    fs::write(target.join("debug/output.bin"), [42; 2048]).unwrap();
}
async fn setup(root: &Path) -> (Engine, AgentService, HarnessReceipt, PathBuf) {
    let source = root.join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("README.md"), "Original source\n").unwrap();
    let (engine, agents) = local_app(root.join("storage.db")).await.unwrap();
    let receipt = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .start(HarnessRequest {
            auto_execute: false,
            discover_project: false,
            budget: None,
            message_id: MessageId::new(),
            prompt: "Inspect this project".into(),
            workspace: Some(fs::canonicalize(source).unwrap().to_string_lossy().into()),
            coding: None,
            target: ModelTarget {
                provider: "openai".into(),
                model: "fixture".into(),
                endpoint: "http://127.0.0.1:1/v1/chat/completions".into(),
                protocol: ModelProtocol::ChatCompletions,
                action_format: Default::default(),
                policy: Default::default(),
            },
        })
        .await
        .unwrap();
    let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
    let workspace = PathBuf::from(&team.members[0].workspace.as_ref().unwrap().path);
    cache(&workspace);
    (engine, agents, receipt, workspace)
}
async fn settle(engine: &Engine, agents: &AgentService, receipt: &HarnessReceipt) {
    agents
        .actors()
        .unwrap()
        .cancel_goal(receipt.task_id)
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..50 {
        if !worker.step().await.unwrap() {
            break;
        }
    }
}
fn request(item: &StorageItem) -> CacheCleanupRequest {
    CacheCleanupRequest {
        session_id: item.owner.session_id,
        agent_id: item.owner.agent_id,
        fingerprint: item.usage.fingerprint.clone().unwrap(),
    }
}

#[tokio::test]
async fn preview_refuses_active_work_then_cleans_only_reviewed_cache_and_replays_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, receipt, workspace) = setup(root.path()).await;
    fs::write(
        workspace.join(".act0r-tools/evidence.json"),
        "retained evidence",
    )
    .unwrap();
    let report = agents.storage().unwrap().report().await.unwrap();
    let item = report
        .items
        .iter()
        .find(|i| i.owner.member == "planner")
        .unwrap();
    assert!(item.owner.blocked_reason.is_some());
    assert_eq!(report.reclaimable_bytes, 0);
    assert!(matches!(
        agents.storage().unwrap().clean(request(item)).await,
        Err(Error::StorageConflict(_))
    ));
    assert!(workspace.join("target/debug/output.bin").exists());
    settle(&engine, &agents, &receipt).await;
    let report = agents.storage().unwrap().report().await.unwrap();
    let item = report
        .items
        .iter()
        .find(|i| i.owner.member == "planner")
        .unwrap();
    assert_eq!(item.owner.blocked_reason, None, "{report:?}");
    let original = request(item);
    fs::write(workspace.join("target/debug/output.bin"), [1; 4096]).unwrap();
    assert!(matches!(
        agents.storage().unwrap().clean(original).await,
        Err(Error::StorageConflict(_))
    ));
    let report = agents.storage().unwrap().report().await.unwrap();
    let item = report
        .items
        .iter()
        .find(|i| i.owner.member == "planner")
        .unwrap();
    let reviewed = request(item);
    let cleaned = agents
        .storage()
        .unwrap()
        .clean(reviewed.clone())
        .await
        .unwrap();
    assert_eq!(cleaned.removed_bytes, item.usage.cache_bytes);
    assert_eq!(cleaned.removed_files, 3);
    assert!(!cleaned.already_absent);
    assert!(!workspace.join("target").exists());
    assert_eq!(
        fs::read_to_string(workspace.join("README.md")).unwrap(),
        "Original source\n"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("source/README.md")).unwrap(),
        "Original source\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join(".act0r-tools/evidence.json")).unwrap(),
        "retained evidence"
    );
    let conversation = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .conversation(receipt.session_id, None)
        .await
        .unwrap();
    drop(agents);
    drop(engine);
    let (_, agents) = local_app(root.path().join("storage.db")).await.unwrap();
    assert!(
        agents
            .storage()
            .unwrap()
            .clean(reviewed)
            .await
            .unwrap()
            .already_absent
    );
    assert_eq!(
        agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .conversation(receipt.session_id, None)
            .await
            .unwrap()
            .turns
            .len(),
        conversation.turns.len()
    );
}

#[tokio::test]
async fn fresh_guard_protects_new_turns_pending_applications_and_uncertain_external_runs() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, receipt, workspace) = setup(root.path()).await;
    settle(&engine, &agents, &receipt).await;
    let store = Arc::new(
        SqliteStore::open(root.path().join("storage.db"))
            .await
            .unwrap(),
    );
    let report = agents.storage().unwrap().report().await.unwrap();
    let item = report
        .items
        .iter()
        .find(|i| i.owner.member == "planner")
        .unwrap();
    let reviewed = request(item);
    for (status, input, workflow) in [
        (
            "queued",
            serde_json::json!({"task_id":receipt.task_id}),
            "workspace.apply",
        ),
        (
            "blocked",
            serde_json::json!({"workspace":workspace.to_string_lossy()}),
            "native.tool",
        ),
        (
            "waiting",
            serde_json::json!({"nested":{"workspace":workspace.to_string_lossy()}}),
            "fixture",
        ),
    ] {
        let pool = sqlx::SqlitePool::connect(&format!(
            "sqlite:{}",
            root.path().join("storage.db").display()
        ))
        .await
        .unwrap();
        let run = RunId::new();
        sqlx::query("INSERT INTO runs(id,status,created_at,payload) VALUES(?,?,0,?)")
            .bind(run.to_string())
            .bind(status)
            .bind(serde_json::json!({"workflow":{"name":workflow},"input":input}).to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            matches!(
                agents.storage().unwrap().clean(reviewed.clone()).await,
                Err(Error::StorageConflict(_))
            ),
            "{status}"
        );
        sqlx::query("DELETE FROM runs WHERE id=?")
            .bind(run.to_string())
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
    }
    let guard = store
        .lock_cache_cleanup(receipt.session_id, item.owner.agent_id)
        .await
        .unwrap();
    let next = agents.clone();
    let session = receipt.session_id;
    let mut append = tokio::spawn(async move {
        next.teams()
            .unwrap()
            .harness()
            .unwrap()
            .append(
                session,
                HarnessTurnRequest {
                    message_id: MessageId::new(),
                    prompt: "Next request".into(),
                },
            )
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(120), &mut append)
            .await
            .is_err()
    );
    guard.finish().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), append)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        agents.storage().unwrap().clean(reviewed).await,
        Err(Error::StorageConflict(_))
    ));
    assert!(workspace.join("target").exists());
}

struct PausingFiles {
    local: LocalStorage,
    entered: std::sync::mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}
impl StorageFilesPort for PausingFiles {
    fn database_bytes(&self) -> Result<u64, Error> {
        self.local.database_bytes()
    }
    fn inspect(&self, w: &str) -> Result<WorkspaceUsage, Error> {
        self.local.inspect(w)
    }
    fn clean(&self, w: &str, f: &str) -> Result<(u64, u64, bool), Error> {
        self.entered.send(()).unwrap();
        let (mutex, condition) = &*self.release;
        let mut released = mutex.lock().unwrap();
        while !*released {
            released = condition.wait(released).unwrap();
        }
        self.local.clean(w, f)
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_http_wait_cannot_release_admission_during_owned_deletion() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, receipt, workspace) = setup(root.path()).await;
    settle(&engine, &agents, &receipt).await;
    let report = agents.storage().unwrap().report().await.unwrap();
    let reviewed = request(
        report
            .items
            .iter()
            .find(|i| i.owner.member == "planner")
            .unwrap(),
    );
    let (entered, received) = std::sync::mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let store = Arc::new(
        SqliteStore::open(root.path().join("storage.db"))
            .await
            .unwrap(),
    );
    let service = StorageService::new(
        store,
        Arc::new(PausingFiles {
            local: LocalStorage::new(
                workspace.parent().unwrap().into(),
                root.path().join("storage.db"),
            ),
            entered,
            release: release.clone(),
        }),
    );
    let cleanup = tokio::spawn(async move { service.clean(reviewed).await });
    tokio::task::spawn_blocking(move || received.recv_timeout(Duration::from_secs(3)))
        .await
        .unwrap()
        .unwrap();
    cleanup.abort();
    let _ = cleanup.await;
    let session = receipt.session_id;
    let mut append = tokio::spawn(async move {
        agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .append(
                session,
                HarnessTurnRequest {
                    message_id: MessageId::new(),
                    prompt: "Next".into(),
                },
            )
            .await
    });
    let blocked = tokio::time::timeout(Duration::from_millis(120), &mut append)
        .await
        .is_err();
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    assert!(blocked);
    tokio::time::timeout(Duration::from_secs(3), append)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!workspace.join("target").exists());
}

#[tokio::test]
async fn filesystem_refuses_unmarked_targets_evidence_and_links_without_touching_source() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, receipt, workspace) = setup(root.path()).await;
    settle(&engine, &agents, &receipt).await;
    let files = LocalStorage::new(
        workspace.parent().unwrap().into(),
        root.path().join("storage.db"),
    );
    let path = workspace.to_str().unwrap();
    let fingerprint = files.inspect(path).unwrap().fingerprint.unwrap();
    assert!(
        files
            .clean(root.path().join("source").to_str().unwrap(), &fingerprint)
            .is_err()
    );
    fs::write(workspace.join("target/preserved.db"), "database").unwrap();
    assert!(files.clean(path, &fingerprint).is_err());
    fs::remove_file(workspace.join("target/preserved.db")).unwrap();
    let external = root.path().join("external");
    fs::create_dir(&external).unwrap();
    fs::write(external.join("keep"), "keep").unwrap();
    let linked = workspace.join("target/link");
    #[cfg(windows)]
    assert!(
        std::process::Command::new("cmd")
            .arg("/c")
            .arg("mklink")
            .arg("/J")
            .arg(&linked)
            .arg(&external)
            .output()
            .unwrap()
            .status
            .success()
    );
    #[cfg(unix)]
    std::os::unix::fs::symlink(&external, &linked).unwrap();
    assert!(files.clean(path, &fingerprint).is_err());
    assert_eq!(fs::read_to_string(external.join("keep")).unwrap(), "keep");
    #[cfg(windows)]
    fs::remove_dir(linked).unwrap();
    #[cfg(unix)]
    fs::remove_file(linked).unwrap();
    fs::remove_file(workspace.join("target/CACHEDIR.TAG")).unwrap();
    let report = agents.storage().unwrap().report().await.unwrap();
    assert_eq!(report.inspection_errors, 1);
    assert!(
        !report
            .items
            .iter()
            .find(|i| i.owner.member == "planner")
            .unwrap()
            .inspected
    );
    assert!(files.clean(path, &fingerprint).is_err());
    assert!(workspace.join("target/debug/output.bin").exists());
}

#[tokio::test]
async fn http_report_and_cleanup_reject_arbitrary_paths_and_recheck_preview() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, receipt, workspace) = setup(root.path()).await;
    settle(&engine, &agents, &receipt).await;
    let listener = act0r::adapters::http_api::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(act0r::adapters::http_api::serve(
        listener,
        Arc::new(agents),
        Default::default(),
        async {
            let _ = stopped.await;
            Ok(())
        },
    ));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let report: StorageReport = client
        .get(format!("{url}/v1/harness/storage"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let reviewed = request(
        report
            .items
            .iter()
            .find(|i| i.owner.member == "planner")
            .unwrap(),
    );
    let mut extra = serde_json::to_value(&reviewed).unwrap();
    extra["workspace"] = serde_json::json!(root.path().join("source"));
    assert_eq!(
        client
            .post(format!("{url}/v1/harness/storage/cleanup"))
            .json(&extra)
            .send()
            .await
            .unwrap()
            .status(),
        422
    );
    fs::write(
        workspace.join("target/debug/output.bin"),
        "changed since review",
    )
    .unwrap();
    let refused = client
        .post(format!("{url}/v1/harness/storage/cleanup"))
        .json(&reviewed)
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 409);
    assert_eq!(
        refused.json::<serde_json::Value>().await.unwrap()["error"]["code"],
        "storage_conflict"
    );
    let report: StorageReport = client
        .get(format!("{url}/v1/harness/storage"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let reviewed = request(
        report
            .items
            .iter()
            .find(|i| i.owner.member == "planner")
            .unwrap(),
    );
    let response = client
        .post(format!("{url}/v1/harness/storage/cleanup"))
        .json(&reviewed)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let result: CacheCleanupResult = response.json().await.unwrap();
    assert!(result.removed_bytes > 0);
    let replay: CacheCleanupResult = client
        .post(format!("{url}/v1/harness/storage/cleanup"))
        .json(&reviewed)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(replay.already_absent);
    assert!(root.path().join("source/README.md").exists());
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn hosted_worker_survives_a_cleanup_lock_longer_than_sqlite_busy_timeout() {
    let root = tempfile::tempdir().unwrap();
    let (engine, agents, receipt, _) = setup(root.path()).await;
    settle(&engine, &agents, &receipt).await;
    let run = engine
        .start(
            DefinitionId::new("demo", 1),
            serde_json::json!({"message":"unrelated work"}),
            5,
        )
        .await
        .unwrap();
    let worker = engine.worker().await.unwrap();
    let store = SqliteStore::open(root.path().join("storage.db"))
        .await
        .unwrap();
    let report = agents.storage().unwrap().report().await.unwrap();
    let item = report
        .items
        .iter()
        .find(|i| i.owner.member == "planner")
        .unwrap();
    let guard = store
        .lock_cache_cleanup(receipt.session_id, item.owner.agent_id)
        .await
        .unwrap();
    let host = tokio::spawn(act0r::adapters::tokio_worker::run_with_shutdown(
        worker,
        act0r::adapters::tokio_worker::WorkerOptions {
            until_idle: true,
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
        std::future::pending(),
    ));
    tokio::time::sleep(Duration::from_millis(5400)).await;
    let running = !host.is_finished();
    guard.finish().await.unwrap();
    assert!(
        running,
        "worker stopped during temporary storage contention"
    );
    tokio::time::timeout(Duration::from_secs(12), host)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let record = engine.status(run).await.unwrap();
    assert_eq!(record.record().status, RunStatus::Succeeded);
    assert_eq!(record.record().allowance.admitted, 3);
}
