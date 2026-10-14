//! Reviewed coding settings apply only to fresh actors in the same conversation.
use super::verification_repair::{
    Fixture, apply, complete, fixture_for_refresh, fixture_with_next_actions, turn_url, verify,
};
use super::*;
use act0r::{
    adapters::{
        patch_applications::LocalPatchApplications, project_verification::LocalProjectVerification,
        sqlite::SqliteStore,
    },
    application::ports::{HarnessStorePort, ProjectVerificationInspectionPort},
    domain::{project_refresh::*, project_setup},
};
use std::sync::atomic::Ordering;

fn route(server: &Server, receipt: &Value) -> String {
    format!("{}/project-setup-review", turn_url(server, receipt))
}
fn scope(receipt: &Value) -> RefreshScope {
    serde_json::from_value(json!({"session_id":receipt["session_id"],"task_id":receipt["task_id"]}))
        .unwrap()
}
fn inspector(root: &Path) -> LocalProjectVerification {
    LocalProjectVerification::new(
        root.join(".act0r/project-verifications"),
        Arc::new(LocalPatchApplications::new(
            root.join(".act0r/applications"),
        )),
    )
}
fn coding() -> Value {
    json!({"writable_paths":["src/math.mjs","lib/"],"test_command":{"argv":["node","new-contracts.mjs"],"timeout_ms":12000},"project_commands":{"check":{"argv":["node","check-files.mjs"],"timeout_ms":10000}}})
}
fn request(review: &Value) -> Value {
    json!({"request_id":MessageId::new(),"review_digest":review["review_digest"],"prompt":"Use the reviewed new folder and verifier; preserve settings and prove the sum API","coding":coding()})
}
async fn fixture(root: &Path) -> Fixture {
    let f=fixture_with_next_actions(root,vec![
        json!({"kind":"call_tool","tool":"create_file","arguments":{"path":"lib/value.mjs","content":"export const total = (a, b) => a + b;\n"}}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/math.mjs","old":"a + b + offset","new":"a + b"}}),
        json!({"kind":"call_tool","tool":"run_command","arguments":{"name":"check"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"New folder and named check verified by the reviewed new contracts"}),
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"lib/value.mjs"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Follow-up passed the new contracts"}),
    ]).await;
    fs::create_dir_all(root.join("project/lib")).unwrap();
    fs::write(
        root.join("project/lib/README.md"),
        "Preserve this newly added project folder.\n",
    )
    .unwrap();
    fs::write(root.join("project/new-contracts.mjs"),"import assert from 'node:assert/strict';\nimport { add } from './src/math.mjs';\nimport { total } from './lib/value.mjs';\nfor (const [a,b] of [[2,3],[-2,3],[0,5],[1.5,2.5],[-4,-6]]) { assert.equal(add(a,b),a+b); assert.equal(total(a,b),a+b); }\nconsole.log('10 updated contracts passed');\n").unwrap();
    fs::write(root.join("project/check-files.mjs"),"import assert from 'node:assert/strict';\nimport { total } from './lib/value.mjs';\nassert.equal(total(2,3),5); console.log('reviewed named check passed');\n").unwrap();
    f
}

#[tokio::test]
async fn project_setup_update_restarts_exact_preparation_and_uses_new_scopes_commands_and_verifier()
{
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path()).await;
    let base = route(&f.server, &f.receipt);
    let view = get(&f.client, &base).await;
    assert_eq!(view["coding"]["writable_paths"], json!(["src/math.mjs"]));
    let before = f
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
            .status(before.planner().unwrap().agent_id)
            .await
            .unwrap()
    );
    let inspection = get(&f.client, &format!("{base}/inspection")).await;
    assert_eq!(inspection["coding"], view["coding"]);
    let body = request(&inspection);
    // The existing refresh endpoint still cannot expand the saved setup.
    post(
        &f.client,
        &format!("{}/project-refresh", turn_url(&f.server, &f.receipt)),
        &body,
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    for field in ["source", "target", "budget", "workers"] {
        let mut invalid = body.clone();
        invalid[field] = json!("override");
        post(&f.client, &base, &invalid, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
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
    assert_eq!(
        json!(project_setup::coding_settings(&prepared.spec).unwrap()),
        coding()
    );
    assert_eq!(prepared.spec.workers.len(), before.spec.workers.len());
    for key in [
        "target",
        "delegation",
        "goal_budget",
        "max_model_calls",
        "max_tokens",
        "completion_policy",
        "model_action_contract",
    ] {
        assert_eq!(
            before.spec.planner.config[key], prepared.spec.planner.config[key],
            "changed {key}"
        );
    }
    for (old, new) in before.spec.workers.iter().zip(&prepared.spec.workers) {
        if old.config.get("writable_paths").is_some() {
            assert_eq!(new.config["writable_paths"], coding()["writable_paths"]);
            assert_eq!(new.config["test_command"], coding()["test_command"]);
            assert_eq!(new.config["project_commands"], coding()["project_commands"]);
        } else {
            assert_eq!(new.config.get("writable_paths"), None);
            assert_eq!(new.config.get("test_command"), None);
            assert_eq!(new.config["tools"], old.config["tools"]);
        }
    }
    assert_eq!(f.count.load(Ordering::SeqCst), 5);
    assert_eq!(get(&f.client, &base).await["request"]["coding"], coding());
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
    assert_eq!(
        json!(
            agents
                .actors()
                .unwrap()
                .status(before.planner().unwrap().agent_id)
                .await
                .unwrap()
        ),
        old_actor
    );
    let permissions = get(&f.client, &format!("{}/permissions", turn_url(&server, &a))).await;
    assert!(permissions["state"]["test_command"].is_null());
    complete(&engine, &f.client, &server, &a).await;
    let changes = get(&f.client, &format!("{}/changes", turn_url(&server, &a))).await;
    assert_eq!(changes["run_status"], "succeeded");
    assert_eq!(f.count.load(Ordering::SeqCst), 10);
    let app = apply(&engine, &f.client, &server, &a).await;
    let checked = verify(&engine, &f.client, &server, &a, &app).await;
    assert_eq!(checked["report"]["command"], coding()["test_command"]);
    assert_eq!(checked["report"]["status"], "verified");
    assert!(
        checked["report"]["output"]["stdout"]
            .as_str()
            .unwrap()
            .contains("10 updated contracts passed")
    );
    assert_eq!(
        fs::read_to_string(root.path().join("project/settings.json")).unwrap(),
        "{\"offset\":5}\n"
    );
    let visible = format!(
        "{}/v1/harness/sessions/{}/conversation",
        server.url,
        a["conversation_id"].as_str().unwrap()
    );
    let history = get(&f.client, &visible).await;
    assert_eq!(history["turns"][2]["phase"], "setup_update");
    assert_eq!(history["turns"][2]["prompt"], body["prompt"]);
    let follow=post(&f.client,&format!("{}/v1/harness/sessions/{}/turns",server.url,a["conversation_id"].as_str().unwrap()),&json!({"message_id":MessageId::new(),"prompt":"Verify this setup without further changes"}),StatusCode::ACCEPTED).await;
    assert_eq!(follow["team_id"], a["team_id"]);
    complete(&engine, &f.client, &server, &follow).await;
    assert_eq!(f.count.load(Ordering::SeqCst), 13);
    let mut changed = body.clone();
    changed["coding"]["test_command"]["timeout_ms"] = json!(14000);
    post(&f.client, &base, &changed, StatusCode::CONFLICT).await;
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
    fs::write(root.path().join("project/README.md"), "later edit").unwrap();
    assert_eq!(post(&f.client, &base, &body, StatusCode::ACCEPTED).await, a);
    assert_eq!(f.count.load(Ordering::SeqCst), 13);
    server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn project_setup_update_rejects_invalid_changes_and_cancelled_preparation_cannot_be_revived()
{
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path()).await;
    let base = route(&f.server, &f.receipt);
    let review = get(&f.client, &format!("{base}/inspection")).await;
    let body = request(&review);
    let mut invalid = body.clone();
    invalid["coding"]["coder_count"] = json!(2);
    post(&f.client, &base, &invalid, StatusCode::BAD_REQUEST).await;
    invalid = body.clone();
    invalid["coding"]["writable_paths"] = json!(["../escape"]);
    post(&f.client, &base, &invalid, StatusCode::BAD_REQUEST).await;
    invalid = body.clone();
    invalid["coding"] = review["coding"].clone();
    post(&f.client, &base, &invalid, StatusCode::BAD_REQUEST).await;
    assert_eq!(get(&f.client, &base).await["preparing"], false);
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
    post(
        &f.client,
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            f.server.url,
            f.receipt["conversation_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"queued later"}),
        StatusCode::CONFLICT,
    )
    .await;
    fs::write(root.path().join("project/new-note.txt"), "source changed").unwrap();
    post(&f.client, &base, &body, StatusCode::CONFLICT).await;
    let cancel = json!({"request_id":body["request_id"]});
    post(
        &f.client,
        &format!("{base}/cancellation"),
        &cancel,
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
        &cancel,
        StatusCode::OK,
    )
    .await;
    post(&f.client, &base, &body, StatusCode::CONFLICT).await;
    assert_eq!(get(&f.client, &base).await["can_start"], true);
    assert_eq!(get(&f.client, &base).await["cancelled"], true);
    assert_eq!(f.count.load(Ordering::SeqCst), 5);
    server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn reopening_preserves_a_pending_refresh_without_coding_overrides() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture_for_refresh(root.path(), false).await;
    let base = format!("{}/project-refresh", turn_url(&f.server, &f.receipt));
    let review = get(&f.client, &format!("{base}/inspection")).await;
    let body = json!({"request_id":MessageId::new(),"review_digest":review["review_digest"],"prompt":"Continue with the same settings"});
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
    assert!(intent.request.coding.is_none());
    let team = f
        .agents
        .teams()
        .unwrap()
        .create(intent.spec.clone())
        .await
        .unwrap();
    store.close().await;
    f.server.close().await;
    drop(f.agents);
    drop(f.engine);
    let (_, agents) = local_app(&f.db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let saved = get(
        &f.client,
        &format!("{}/project-refresh", turn_url(&server, &f.receipt)),
    )
    .await;
    assert_eq!(saved["request"], body);
    let admitted = post(
        &f.client,
        &format!("{}/project-refresh", turn_url(&server, &f.receipt)),
        &body,
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(admitted["team_id"], json!(team.id));
    assert_eq!(f.count.load(Ordering::SeqCst), 3);
    server.close().await;
    f.provider.abort();
}
