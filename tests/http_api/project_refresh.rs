//! Current source edits enter a reviewed fresh stage without a failing verification.
use super::verification_repair::{apply, complete, fixture_for_refresh, turn_url, verify};
use super::*;
use act0r::{
    adapters::{
        patch_applications::LocalPatchApplications, project_verification::LocalProjectVerification,
        sqlite::SqliteStore,
    },
    application::ports::{HarnessStorePort, ProjectVerificationInspectionPort},
    domain::project_refresh::*,
};
use std::sync::atomic::Ordering;

fn route(server: &Server, receipt: &Value) -> String {
    format!("{}/project-refresh", turn_url(server, receipt))
}
fn scope(receipt: &Value) -> RefreshScope {
    serde_json::from_value(json!({"session_id":receipt["session_id"],"task_id":receipt["task_id"]}))
        .unwrap()
}
fn request(review: &Value) -> Value {
    json!({"request_id":MessageId::new(),"review_digest":review["review_digest"],"prompt":"Adapt add to my current settings; preserve my files and verify"})
}
fn inspector(root: &Path) -> LocalProjectVerification {
    LocalProjectVerification::new(
        root.join(".act0r/project-verifications"),
        Arc::new(LocalPatchApplications::new(
            root.join(".act0r/applications"),
        )),
    )
}

#[tokio::test]
async fn project_refresh_keeps_auto_conversation_and_source_edits_through_restart_and_verified_application()
 {
    let root = tempfile::tempdir().unwrap();
    let f = fixture_for_refresh(root.path(), true).await;
    assert!(f.verification.is_null());
    fs::write(root.path().join("project/user-note.txt"), "keep my note\n").unwrap();
    fs::remove_file(root.path().join("project/README.md")).unwrap();
    let old = f
        .agents
        .teams()
        .unwrap()
        .get(serde_json::from_value(f.receipt["team_id"].clone()).unwrap())
        .await
        .unwrap();
    let old_actor = json!(
        f.agents
            .actors()
            .unwrap()
            .status(old.planner().unwrap().agent_id)
            .await
            .unwrap()
    );
    let base = route(&f.server, &f.receipt);
    let review = get(&f.client, &format!("{base}/inspection")).await;
    assert_eq!(
        review["changes"],
        json!([{"path":"README.md","change":"deleted"},{"path":"settings.json","change":"modified"},{"path":"user-note.txt","change":"added"}])
    );
    assert_eq!(f.count.load(Ordering::SeqCst), 5);
    let body = request(&review);
    for field in ["source", "target", "command", "coding"] {
        let mut bad = body.clone();
        bad[field] = json!("override");
        post(&f.client, &base, &bad, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
    // Save preparation and copies without admitting a task, then reopen the API.
    let store = SqliteStore::open(&f.db).await.unwrap();
    let context = store.refresh_context(scope(&f.receipt)).await.unwrap();
    let (snapshot, _) = inspector(root.path())
        .inspect_refresh(context.original.source.unwrap(), context.workspace)
        .await
        .unwrap();
    let intent = store
        .begin_project_refresh(
            scope(&f.receipt),
            serde_json::from_value(body.clone()).unwrap(),
            snapshot,
        )
        .await
        .unwrap();
    let prepared = f
        .agents
        .teams()
        .unwrap()
        .create(intent.spec.clone())
        .await
        .unwrap();
    let copy = prepared.planner().unwrap().workspace.as_ref().unwrap();
    assert_eq!(
        fs::read_to_string(Path::new(&copy.path).join("settings.json")).unwrap(),
        "{\"offset\":5}\n"
    );
    assert_eq!(
        fs::read_to_string(Path::new(&copy.path).join("user-note.txt")).unwrap(),
        "keep my note\n"
    );
    assert!(!Path::new(&copy.path).join("README.md").exists());
    for key in [
        "target",
        "writable_paths",
        "test_command",
        "project_commands",
        "delegation",
        "goal_budget",
        "max_model_calls",
        "max_tokens",
    ] {
        assert_eq!(
            prepared.spec.planner.config[key],
            old.spec.planner.config[key]
        );
    }
    assert_ne!(
        copy.path,
        old.planner().unwrap().workspace.as_ref().unwrap().path
    );
    assert_eq!(get(&f.client, &base).await["preparing"], true);
    store.close().await;
    f.server.close().await;
    drop(f.agents);
    drop(f.engine);
    let (engine, agents) = local_app(&f.db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let base = route(&server, &f.receipt);
    let (a, b) = tokio::join!(
        post(&f.client, &base, &body, StatusCode::ACCEPTED),
        post(&f.client, &base, &body, StatusCode::ACCEPTED)
    );
    assert_eq!(a, b);
    assert_eq!(a["team_id"], json!(prepared.id));
    assert_eq!(a["conversation_id"], f.receipt["conversation_id"]);
    assert_eq!(f.count.load(Ordering::SeqCst), 5);
    assert_eq!(
        json!(
            agents
                .actors()
                .unwrap()
                .status(old.planner().unwrap().agent_id)
                .await
                .unwrap()
        ),
        old_actor
    );
    complete(&engine, &f.client, &server, &a).await;
    let app = apply(&engine, &f.client, &server, &a).await;
    let verified = verify(&engine, &f.client, &server, &a, &app).await;
    assert_eq!(verified["report"]["status"], "verified");
    assert!(
        verified["report"]["output"]["stdout"]
            .as_str()
            .unwrap()
            .contains("5 contracts passed")
    );
    let visible = format!(
        "{}/v1/harness/sessions/{}/conversation",
        server.url,
        a["conversation_id"].as_str().unwrap()
    );
    let history = get(&f.client, &visible).await;
    assert_eq!(
        history["turns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["phase"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["discovery", "coding", "refresh"]
    );
    assert_eq!(history["turns"][2]["prompt"], body["prompt"]);
    let cursor = history["turns"][2]["cursor"].as_str().unwrap();
    let older = get(&f.client, &format!("{visible}?before={cursor}")).await;
    assert_eq!(older["turns"].as_array().unwrap().len(), 2);
    let follow = post(
        &f.client,
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            a["conversation_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"Verify without more changes"}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(follow["team_id"], a["team_id"]);
    complete(&engine, &f.client, &server, &follow).await;
    assert_eq!(f.count.load(Ordering::SeqCst), 12);
    assert_eq!(
        fs::read_to_string(root.path().join("project/settings.json")).unwrap(),
        "{\"offset\":5}\n"
    );
    assert!(f.requests.lock().unwrap().iter().any(|body| {
        serde_json::to_string(body)
            .unwrap()
            .contains("Prior saved context")
    }));
    post(
        &f.client,
        &format!(
            "{}/v1/harness/sessions/{}/metadata",
            server.url,
            a["conversation_id"].as_str().unwrap()
        ),
        &json!({"kind":"archive","expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    fs::write(root.path().join("project/user-note.txt"), "later edit\n").unwrap();
    assert_eq!(post(&f.client, &base, &body, StatusCode::ACCEPTED).await, a);
    assert_eq!(f.count.load(Ordering::SeqCst), 12);
    let mut conflict = body.clone();
    conflict["prompt"] = json!("different");
    post(&f.client, &base, &conflict, StatusCode::CONFLICT).await;
    server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn project_refresh_refuses_stale_review_source_and_managed_edits_and_never_starts_model_work()
{
    let root = tempfile::tempdir().unwrap();
    let f = fixture_for_refresh(root.path(), false).await;
    let base = route(&f.server, &f.receipt);
    let review = get(&f.client, &format!("{base}/inspection")).await;
    let body = request(&review);
    fs::write(root.path().join("project/later.txt"), "external edit").unwrap();
    post(&f.client, &base, &body, StatusCode::CONFLICT).await;
    assert_eq!(get(&f.client, &base).await["preparing"], false);
    fs::remove_file(root.path().join("project/later.txt")).unwrap();
    let team = f
        .agents
        .teams()
        .unwrap()
        .get(serde_json::from_value(f.receipt["team_id"].clone()).unwrap())
        .await
        .unwrap();
    fs::write(
        Path::new(&team.planner().unwrap().workspace.as_ref().unwrap().path)
            .join("private-note.txt"),
        "unapplied change",
    )
    .unwrap();
    post(&f.client, &base, &body, StatusCode::CONFLICT).await;
    assert_eq!(f.count.load(Ordering::SeqCst), 3);
    let wrong = format!(
        "{}/v1/harness/sessions/{}/turns/{}/project-refresh",
        f.server.url,
        MessageId::new(),
        f.receipt["task_id"].as_str().unwrap()
    );
    post(&f.client, &wrong, &body, StatusCode::NOT_FOUND).await;
    let mut nil = body;
    nil["request_id"] = json!("00000000-0000-0000-0000-000000000000");
    post(&f.client, &base, &nil, StatusCode::BAD_REQUEST).await;
    f.server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn project_refresh_preparation_blocks_new_work_and_cancel_survives_restart_and_rejects_revival()
 {
    let root = tempfile::tempdir().unwrap();
    let f = fixture_for_refresh(root.path(), false).await;
    let base = route(&f.server, &f.receipt);
    let body = request(&get(&f.client, &format!("{base}/inspection")).await);
    let store = SqliteStore::open(&f.db).await.unwrap();
    let context = store.refresh_context(scope(&f.receipt)).await.unwrap();
    let (snapshot, _) = inspector(root.path())
        .inspect_refresh(context.original.source.unwrap(), context.workspace)
        .await
        .unwrap();
    store
        .begin_project_refresh(
            scope(&f.receipt),
            serde_json::from_value(body.clone()).unwrap(),
            snapshot,
        )
        .await
        .unwrap();
    let turns = format!(
        "{}/v1/harness/sessions/{}/turns",
        f.server.url,
        f.receipt["session_id"].as_str().unwrap()
    );
    post(
        &f.client,
        &turns,
        &json!({"message_id":MessageId::new(),"prompt":"later"}),
        StatusCode::CONFLICT,
    )
    .await;
    post(
        &f.client,
        &format!(
            "{}/v1/harness/sessions/{}/metadata",
            f.server.url,
            f.receipt["session_id"].as_str().unwrap()
        ),
        &json!({"kind":"archive","expected_revision":0}),
        StatusCode::CONFLICT,
    )
    .await;
    fs::write(
        root.path().join("project/source-drift.txt"),
        "changed after preparation",
    )
    .unwrap();
    post(&f.client, &base, &body, StatusCode::CONFLICT).await;
    assert!(get(&f.client, &base).await["error"].is_string());
    let cancellation = json!({"request_id":body["request_id"]});
    post(
        &f.client,
        &format!("{base}/cancellation"),
        &json!({"request_id":MessageId::new()}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    post(
        &f.client,
        &format!("{base}/cancellation"),
        &cancellation,
        StatusCode::OK,
    )
    .await;
    store.close().await;
    f.server.close().await;
    drop(f.agents);
    drop(f.engine);
    let (_, agents) = local_app(&f.db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let base = route(&server, &f.receipt);
    post(
        &f.client,
        &format!("{base}/cancellation"),
        &cancellation,
        StatusCode::OK,
    )
    .await;
    post(&f.client, &base, &body, StatusCode::CONFLICT).await;
    assert_eq!(get(&f.client, &base).await["can_start"], true);
    assert_eq!(get(&f.client, &base).await["cancelled"], true);
    assert_eq!(f.count.load(Ordering::SeqCst), 3);
    server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn project_refresh_rejects_modified_prepared_copies_and_previously_used_actors() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture_for_refresh(root.path(), false).await;
    let base = route(&f.server, &f.receipt);
    let body = request(&get(&f.client, &format!("{base}/inspection")).await);
    let store = SqliteStore::open(&f.db).await.unwrap();
    let context = store.refresh_context(scope(&f.receipt)).await.unwrap();
    let (snapshot, _) = inspector(root.path())
        .inspect_refresh(context.original.source.unwrap(), context.workspace)
        .await
        .unwrap();
    let intent = store
        .begin_project_refresh(
            scope(&f.receipt),
            serde_json::from_value(body.clone()).unwrap(),
            snapshot,
        )
        .await
        .unwrap();
    let prepared = f.agents.teams().unwrap().create(intent.spec).await.unwrap();
    let settings = Path::new(&prepared.planner().unwrap().workspace.as_ref().unwrap().path)
        .join("settings.json");
    fs::write(&settings, "{\"offset\":99}\n").unwrap();
    post(&f.client, &base, &body, StatusCode::CONFLICT).await;
    assert!(
        get(&f.client, &base).await["error"]
            .as_str()
            .unwrap()
            .contains("copy changed")
    );
    fs::write(&settings, "{\"offset\":5}\n").unwrap();
    post(
        &f.client,
        &format!("{}/v1/teams/{}/tasks", f.server.url, prepared.id),
        &json!({"message_id":MessageId::new(),"goal":"separate work"}),
        StatusCode::ACCEPTED,
    )
    .await;
    post(&f.client, &base, &body, StatusCode::CONFLICT).await;
    assert!(
        get(&f.client, &base).await["error"]
            .as_str()
            .unwrap()
            .contains("already received work")
    );
    assert_eq!(f.count.load(Ordering::SeqCst), 3);
    post(
        &f.client,
        &format!("{base}/cancellation"),
        &json!({"request_id":body["request_id"]}),
        StatusCode::OK,
    )
    .await;
    store.close().await;
    f.server.close().await;
    f.provider.abort();
}
