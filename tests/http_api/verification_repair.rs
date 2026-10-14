//! Real Node failures drive fresh, durable coding stages through public HTTP.
use super::*;
use act0r::{
    adapters::sqlite::SqliteStore,
    application::ports::HarnessStorePort,
    domain::{
        RunId,
        verification_repair::{RepairRequest, RepairScope},
    },
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

const MODULE: &str = "import { readFileSync } from 'node:fs';\nconst { offset } = JSON.parse(readFileSync(new URL('../settings.json', import.meta.url), 'utf8'));\nexport const add = (a, b) => a - b + offset;\n";
pub(super) struct Fixture {
    pub(super) engine: Engine,
    pub(super) agents: Arc<AgentService>,
    pub(super) server: Server,
    pub(super) client: Client,
    pub(super) receipt: Value,
    pub(super) verification: Value,
    pub(super) app: Value,
    pub(super) count: Arc<AtomicUsize>,
    pub(super) requests: Arc<Mutex<Vec<Value>>>,
    pub(super) provider: JoinHandle<()>,
    pub(super) db: std::path::PathBuf,
}
pub(super) fn turn_url(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
pub(super) fn goal_url(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
pub(super) async fn complete(engine: &Engine, client: &Client, server: &Server, receipt: &Value) {
    let goal = goal_url(server, receipt);
    let turn = turn_url(server, receipt);
    for _ in 0..12 {
        settle(engine).await;
        let changes = get(client, &format!("{turn}/changes")).await;
        if changes["run_status"] == "succeeded" {
            assert_eq!(changes["completion"]["status"], "verified");
            return;
        }
        let inbox = get(client, &format!("{goal}/inbox")).await;
        let approval = wait(&inbox, "tool_approval");
        answer(
            client,
            &goal,
            &approval,
            json!({"kind":"approval","approved":true}),
            StatusCode::OK,
        )
        .await;
    }
    panic!("repair fixture did not complete");
}
pub(super) async fn apply(
    engine: &Engine,
    client: &Client,
    server: &Server,
    receipt: &Value,
) -> Value {
    let route = format!("{}/applications", turn_url(server, receipt));
    let id = RunId::new();
    post(
        client,
        &route,
        &json!({"application_id":id}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(engine).await;
    let app = get(client, &format!("{route}/{id}")).await;
    post(
        client,
        &format!("{route}/{id}/responses"),
        &json!({"wait_id":app["wait"]["id"],"approved":true}),
        StatusCode::OK,
    )
    .await;
    settle(engine).await;
    let app = get(client, &format!("{route}/{id}")).await;
    assert_eq!(app["report"]["status"], "applied");
    app
}
pub(super) async fn verify(
    engine: &Engine,
    client: &Client,
    server: &Server,
    receipt: &Value,
    app: &Value,
) -> Value {
    let route = format!(
        "{}/applications/{}/verifications",
        turn_url(server, receipt),
        app["run_id"].as_str().unwrap()
    );
    let id = RunId::new();
    post(
        client,
        &route,
        &json!({"verification_id":id}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(engine).await;
    let view = get(client, &format!("{route}/{id}")).await;
    post(
        client,
        &format!("{route}/{id}/responses"),
        &json!({"wait_id":view["wait"]["id"],"approved":true}),
        StatusCode::OK,
    )
    .await;
    settle(engine).await;
    get(client, &format!("{route}/{id}")).await
}
pub(super) async fn fixture(root: &Path) -> Fixture {
    fixture_mode(root, false).await
}
pub(super) async fn fixture_mode(root: &Path, automatic: bool) -> Fixture {
    fixture_options(root, automatic, true, None).await
}
pub(super) async fn fixture_for_refresh(root: &Path, automatic: bool) -> Fixture {
    fixture_options(root, automatic, false, None).await
}
pub(super) async fn fixture_with_next_actions(root: &Path, actions: Vec<Value>) -> Fixture {
    fixture_options(root, true, false, Some(actions)).await
}
async fn fixture_options(
    root: &Path,
    automatic: bool,
    failed_verification: bool,
    next_actions: Option<Vec<Value>>,
) -> Fixture {
    let project = root.join("project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(project.join("src/math.mjs"), MODULE).unwrap();
    fs::write(project.join("settings.json"), "{\"offset\":0}\n").unwrap();
    fs::write(project.join("README.md"),"Keep the add(a,b) numeric API. Verify with node contracts.mjs. Preserve settings and tests.\n").unwrap();
    fs::write(project.join("contracts.mjs"),"import assert from 'node:assert/strict';\nimport {add} from './src/math.mjs';\nfor (const [a,b] of [[2,3],[-2,3],[0,5],[1.5,2.5],[-4,-6]]) assert.equal(add(a,b),a+b);\nconsole.log('5 contracts passed');\n").unwrap();
    let requests = Arc::new(Mutex::new(vec![]));
    let saved = requests.clone();
    let mut actions = vec![
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/math.mjs","old":"a - b","new":"a + b"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Five contracts passed in the isolated project"}),
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"settings.json"}}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/math.mjs","old":"a + b + offset","new":"a + b"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Repaired the original-project failure; five contracts passed"}),
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/math.mjs"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Follow-up verified the repaired copy"}),
    ];
    if let Some(next) = next_actions {
        actions.truncate(3);
        actions.extend(next);
    }
    if automatic {
        actions.splice(0..0,[json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),json!({"kind":"propose_project_setup","proposal":{"summary":"README documents Node verification and one writable module","writable_paths":["src/math.mjs"],"test_command":{"argv":["node","contracts.mjs"],"timeout_ms":10000},"commands":[]}})]);
    }
    let (endpoint, count, provider) =
        super::coding::provider_with_check(actions, move |_, body| {
            saved.lock().unwrap().push(body.clone())
        })
        .await;
    let db = root.join("repair.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let mut start = json!({"message_id":MessageId::new(),"prompt":"Repair add while preserving settings, documentation and tests","workspace":project,"target":{"provider":"openai","model":"repair-fixture","endpoint":endpoint,"protocol":"chat_completions"},"coding":{"writable_paths":["src/math.mjs"],"test_command":{"argv":["node","contracts.mjs"],"timeout_ms":10000}}});
    let coding = start["coding"].clone();
    if automatic {
        start["discover_project"] = json!(true);
        start.as_object_mut().unwrap().remove("coding");
    }
    let mut receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &start,
        StatusCode::ACCEPTED,
    )
    .await;
    if automatic {
        settle(&engine).await;
        let visible = receipt["session_id"].clone();
        receipt = post(
            &client,
            &format!("{}/project-setup", turn_url(&server, &receipt)),
            &json!({"coding":coding}),
            StatusCode::ACCEPTED,
        )
        .await;
        receipt["conversation_id"] = visible;
    }
    complete(&engine, &client, &server, &receipt).await;
    let app = apply(&engine, &client, &server, &receipt).await;
    // A user's unrelated configuration edit is absent from the first managed copy.
    fs::write(project.join("settings.json"), "{\"offset\":5}\n").unwrap();
    let verification = if failed_verification {
        let verification = verify(&engine, &client, &server, &receipt, &app).await;
        assert_eq!(verification["report"]["status"], "failed");
        verification
    } else {
        Value::Null
    };
    assert_eq!(count.load(Ordering::SeqCst), if automatic { 5 } else { 3 });
    Fixture {
        engine,
        agents,
        server,
        client,
        receipt,
        verification,
        app,
        count,
        requests,
        provider,
        db,
    }
}
fn scope(f: &Fixture) -> RepairScope {
    serde_json::from_value(json!({"session_id":f.receipt["session_id"],"task_id":f.receipt["task_id"],"application_id":f.app["run_id"],"verification_id":f.verification["run_id"]})).unwrap()
}
fn repair_url(server: &Server, receipt: &Value, app: &Value, verification: &Value) -> String {
    format!(
        "{}/applications/{}/verifications/{}/repair",
        turn_url(server, receipt),
        app["run_id"].as_str().unwrap(),
        verification["run_id"].as_str().unwrap()
    )
}

#[tokio::test]
async fn failed_source_verification_repairs_a_fresh_copy_in_one_conversation_across_restart() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path()).await;
    let old_team = f
        .agents
        .teams()
        .unwrap()
        .get(serde_json::from_value(f.receipt["team_id"].clone()).unwrap())
        .await
        .unwrap();
    let old_actor = f
        .agents
        .actors()
        .unwrap()
        .status(old_team.planner().unwrap().agent_id)
        .await
        .unwrap();
    let request = RepairRequest {
        request_id: MessageId::new(),
    };
    let route = repair_url(&f.server, &f.receipt, &f.app, &f.verification);
    assert_eq!(get(&f.client, &route).await["can_start"], true);
    post(
        &f.client,
        &route,
        &json!({"request_id":request.request_id,"command":["other"]}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    let store = SqliteStore::open(&f.db).await.unwrap();
    let intent = store
        .begin_verification_repair(scope(&f), request.clone())
        .await
        .unwrap();
    let team = f
        .agents
        .teams()
        .unwrap()
        .create(intent.spec.clone())
        .await
        .unwrap();
    let copy = team.planner().unwrap().workspace.as_ref().unwrap();
    assert_eq!(team.spec.source, old_team.spec.source);
    assert_eq!(team.spec.workers.len(), old_team.spec.workers.len());
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
            team.spec.planner.config[key], old_team.spec.planner.config[key],
            "reviewed {key} changed"
        );
    }
    assert_eq!(
        copy.snapshot_digest,
        f.verification["snapshot"]["digest"].as_str().unwrap()
    );
    assert_eq!(
        fs::read_to_string(Path::new(&copy.path).join("settings.json")).unwrap(),
        "{\"offset\":5}\n"
    );
    assert_ne!(
        copy.path,
        old_team.planner().unwrap().workspace.as_ref().unwrap().path
    );
    assert_eq!(f.count.load(Ordering::SeqCst), 3);
    store.close().await;
    f.server.close().await;
    drop(f.agents);
    drop(f.engine);
    let (engine, agents) = local_app(&f.db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let route = repair_url(&server, &f.receipt, &f.app, &f.verification);
    let (repair, replay) = tokio::join!(
        post(&f.client, &route, &request, StatusCode::ACCEPTED),
        post(&f.client, &route, &request, StatusCode::ACCEPTED)
    );
    assert_eq!(repair, replay);
    assert_eq!(repair["team_id"], json!(team.id));
    assert_eq!(repair["conversation_id"], f.receipt["session_id"]);
    post(
        &f.client,
        &route,
        &json!({"request_id":MessageId::new()}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(
        json!(
            agents
                .actors()
                .unwrap()
                .status(old_team.planner().unwrap().agent_id)
                .await
                .unwrap()
        ),
        json!(old_actor)
    );
    let grouped = get(
        &f.client,
        &format!(
            "{}/v1/harness/sessions/{}/conversation",
            server.url,
            f.receipt["session_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(grouped["active"]["task_id"], repair["task_id"]);
    assert_eq!(grouped["turns"][1]["phase"], "repair");
    assert_eq!(grouped["turns"][1]["repeated_request"], false);
    assert_eq!(
        get(
            &f.client,
            &format!("{}/v1/harness/conversations", server.url)
        )
        .await["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    complete(&engine, &f.client, &server, &repair).await;
    let model = f.requests.lock().unwrap()[3].to_string();
    assert!(model.contains("Fix verification failure"));
    assert!(model.contains("AssertionError"));
    assert!(model.contains(f.verification["run_id"].as_str().unwrap()));
    assert_eq!(f.count.load(Ordering::SeqCst), 7);
    assert_eq!(
        fs::read_to_string(root.path().join("project/settings.json")).unwrap(),
        "{\"offset\":5}\n"
    );
    let app = apply(&engine, &f.client, &server, &repair).await;
    let verified = verify(&engine, &f.client, &server, &repair, &app).await;
    assert_eq!(verified["report"]["status"], "verified");
    assert!(
        verified["report"]["output"]["stdout"]
            .as_str()
            .unwrap()
            .contains("5 contracts passed")
    );
    assert_eq!(
        verified["report"]["before"]["digest"],
        verified["report"]["after"]["digest"]
    );
    assert_eq!(
        get(&f.client, &route).await["receipt"]["task_id"],
        repair["task_id"]
    );
    let follow = post(
        &f.client,
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            f.receipt["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"Verify the repair without new edits"}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(follow["session_id"], repair["session_id"]);
    complete(&engine, &f.client, &server, &follow).await;
    assert_eq!(f.count.load(Ordering::SeqCst), 10);
    // Exact admission replay is historical evidence, even after later source drift.
    post(
        &f.client,
        &format!(
            "{}/v1/harness/sessions/{}/metadata",
            server.url,
            f.receipt["session_id"].as_str().unwrap()
        ),
        &json!({"kind":"archive","expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    fs::write(root.path().join("project/README.md"), "later user edit\n").unwrap();
    assert_eq!(
        post(&f.client, &route, &request, StatusCode::ACCEPTED).await,
        repair
    );
    assert_eq!(f.count.load(Ordering::SeqCst), 10);
    server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn repair_refuses_drift_and_pending_preparation_is_cancellable_without_model_calls() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path()).await;
    let route = repair_url(&f.server, &f.receipt, &f.app, &f.verification);
    let request = RepairRequest {
        request_id: MessageId::new(),
    };
    fs::write(
        root.path().join("project/settings.json"),
        "{\"offset\":6}\n",
    )
    .unwrap();
    post(&f.client, &route, &request, StatusCode::CONFLICT).await;
    let pending = get(&f.client, &route).await;
    assert_eq!(pending["preparing"], true);
    assert_eq!(pending["request_id"], json!(request.request_id));
    assert!(pending["error"].as_str().unwrap().contains("changed"));
    post(
        &f.client,
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            f.server.url,
            f.receipt["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"another request"}),
        StatusCode::CONFLICT,
    )
    .await;
    let metadata = format!(
        "{}/v1/harness/sessions/{}/metadata",
        f.server.url,
        f.receipt["session_id"].as_str().unwrap()
    );
    post(
        &f.client,
        &metadata,
        &json!({"kind":"archive","expected_revision":0}),
        StatusCode::CONFLICT,
    )
    .await;
    let cancellation = format!("{route}/cancellation");
    post(
        &f.client,
        &cancellation,
        &json!({"request_id":MessageId::new()}),
        StatusCode::CONFLICT,
    )
    .await;
    post(&f.client, &cancellation, &request, StatusCode::OK).await;
    post(&f.client, &cancellation, &request, StatusCode::OK).await;
    post(&f.client, &route, &request, StatusCode::CONFLICT).await;
    assert_eq!(get(&f.client, &route).await["cancelled"], true);
    let current = verify(&f.engine, &f.client, &f.server, &f.receipt, &f.app).await;
    assert_eq!(current["report"]["status"], "failed");
    let current_route = repair_url(&f.server, &f.receipt, &f.app, &current);
    post(&f.client, &current_route, &request, StatusCode::CONFLICT).await;
    assert!(get(&f.client, &current_route).await["request_id"].is_null());
    post(
        &f.client,
        &metadata,
        &json!({"kind":"archive","expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(f.count.load(Ordering::SeqCst), 3);
    assert_eq!(
        fs::read_to_string(root.path().join("project/settings.json")).unwrap(),
        "{\"offset\":6}\n"
    );
    f.server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn repair_snapshot_admission_and_saved_cancellation_prevent_stale_actor_execution() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path()).await;
    let store = SqliteStore::open(&f.db).await.unwrap();
    let request = RepairRequest {
        request_id: MessageId::new(),
    };
    let intent = store
        .begin_verification_repair(scope(&f), request.clone())
        .await
        .unwrap();
    // Preparation captures a racing edit; restoring the source cannot make that copy safe.
    fs::write(
        root.path().join("project/settings.json"),
        "{\"offset\":9}\n",
    )
    .unwrap();
    let team = f
        .agents
        .teams()
        .unwrap()
        .create(intent.spec.clone())
        .await
        .unwrap();
    fs::write(
        root.path().join("project/settings.json"),
        "{\"offset\":5}\n",
    )
    .unwrap();
    let route = repair_url(&f.server, &f.receipt, &f.app, &f.verification);
    post(&f.client, &route, &request, StatusCode::CONFLICT).await;
    assert_eq!(f.count.load(Ordering::SeqCst), 3);
    post(
        &f.client,
        &format!("{route}/cancellation"),
        &request,
        StatusCode::OK,
    )
    .await;
    assert!(
        store
            .admit_verification_repair(intent, team.id, 0)
            .await
            .is_err()
    );
    let mut wrong = scope(&f);
    wrong.task_id = MessageId::new();
    assert!(store.verification_repair(wrong).await.is_err());
    store.close().await;
    f.server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn successive_repairs_keep_stage_order_and_route_followups_to_the_latest_copy() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path()).await;
    let route = repair_url(&f.server, &f.receipt, &f.app, &f.verification);
    let first = post(
        &f.client,
        &route,
        &RepairRequest {
            request_id: MessageId::new(),
        },
        StatusCode::ACCEPTED,
    )
    .await;
    complete(&f.engine, &f.client, &f.server, &first).await;
    let application = apply(&f.engine, &f.client, &f.server, &first).await;
    fs::write(root.path().join("project/contracts.mjs"),"import assert from 'node:assert/strict'; import {add} from './src/math.mjs'; assert.equal(add(2,3),6);\n").unwrap();
    let failure = verify(&f.engine, &f.client, &f.server, &first, &application).await;
    assert_eq!(failure["report"]["status"], "failed");
    let second_route = repair_url(&f.server, &first, &application, &failure);
    let second = post(
        &f.client,
        &second_route,
        &RepairRequest {
            request_id: MessageId::new(),
        },
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(second["conversation_id"], f.receipt["session_id"]);
    let conversation = format!(
        "{}/v1/harness/sessions/{}/conversation",
        f.server.url,
        f.receipt["session_id"].as_str().unwrap()
    );
    let grouped = get(&f.client, &conversation).await;
    assert_eq!(grouped["active"]["task_id"], second["task_id"]);
    let cursors: Vec<_> = grouped["turns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|turn| turn["cursor"].as_str().unwrap())
        .collect();
    let session = f.receipt["session_id"].as_str().unwrap();
    assert_eq!(
        cursors,
        vec![
            format!("a1:{session}:1:0:1"),
            format!("a1:{session}:1:1:1"),
            format!("a1:{session}:1:2:1")
        ]
    );
    let earlier = get(
        &f.client,
        &format!("{conversation}?before=a1:{session}:1:1:1"),
    )
    .await;
    assert_eq!(earlier["turns"].as_array().unwrap().len(), 1);
    assert_eq!(earlier["turns"][0]["task_id"], f.receipt["task_id"]);
    let follow=post(&f.client,&format!("{}/v1/harness/sessions/{session}/turns",f.server.url),&json!({"message_id":MessageId::new(),"prompt":"Keep this follow-up with the newest repair"}),StatusCode::ACCEPTED).await;
    assert_eq!(follow["session_id"], second["session_id"]);
    assert_eq!(f.count.load(Ordering::SeqCst), 7);
    f.server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn auto_discovery_coding_and_failed_verification_repair_keep_the_original_conversation() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture_mode(root.path(), true).await;
    let route = repair_url(&f.server, &f.receipt, &f.app, &f.verification);
    let repaired = post(
        &f.client,
        &route,
        &RepairRequest {
            request_id: MessageId::new(),
        },
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(repaired["conversation_id"], f.receipt["conversation_id"]);
    complete(&f.engine, &f.client, &f.server, &repaired).await;
    let grouped = get(
        &f.client,
        &format!(
            "{}/v1/harness/sessions/{}/conversation",
            f.server.url,
            f.receipt["conversation_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(grouped["automatic"], true);
    assert_eq!(grouped["active"]["task_id"], repaired["task_id"]);
    let phases: Vec<_> = grouped["turns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|turn| turn["phase"].as_str().unwrap())
        .collect();
    assert_eq!(phases, vec!["discovery", "coding", "repair"]);
    assert_eq!(grouped["turns"][1]["repeated_request"], true);
    assert_eq!(grouped["turns"][2]["repeated_request"], false);
    assert_eq!(
        get(
            &f.client,
            &format!("{}/v1/harness/conversations", f.server.url)
        )
        .await["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(f.count.load(Ordering::SeqCst), 9);
    f.server.close().await;
    f.provider.abort();
}

#[tokio::test]
async fn prepared_copy_edits_and_advanced_actor_work_cannot_borrow_a_repair_receipt() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path()).await;
    let store = SqliteStore::open(&f.db).await.unwrap();
    let request = RepairRequest {
        request_id: MessageId::new(),
    };
    let intent = store
        .begin_verification_repair(scope(&f), request.clone())
        .await
        .unwrap();
    let team = f.agents.teams().unwrap().create(intent.spec).await.unwrap();
    let settings =
        Path::new(&team.planner().unwrap().workspace.as_ref().unwrap().path).join("settings.json");
    fs::write(&settings, "{\"offset\":99}\n").unwrap();
    let route = repair_url(&f.server, &f.receipt, &f.app, &f.verification);
    post(&f.client, &route, &request, StatusCode::CONFLICT).await;
    assert!(
        get(&f.client, &route).await["error"]
            .as_str()
            .unwrap()
            .contains("copy changed")
    );
    fs::write(settings, "{\"offset\":5}\n").unwrap();
    post(
        &f.client,
        &format!("{}/v1/teams/{}/tasks", f.server.url, team.id),
        &json!({"message_id":MessageId::new(),"goal":"Separate advanced work"}),
        StatusCode::ACCEPTED,
    )
    .await;
    post(&f.client, &route, &request, StatusCode::CONFLICT).await;
    assert!(
        get(&f.client, &route).await["error"]
            .as_str()
            .unwrap()
            .contains("already received work")
    );
    assert!(
        store
            .harness_conversation(request.request_id.to_string().parse().unwrap(), None)
            .await
            .is_err()
    );
    assert_eq!(f.count.load(Ordering::SeqCst), 3);
    assert_eq!(
        fs::read_to_string(root.path().join("project/settings.json")).unwrap(),
        "{\"offset\":5}\n"
    );
    post(
        &f.client,
        &format!("{route}/cancellation"),
        &request,
        StatusCode::OK,
    )
    .await;
    store.close().await;
    f.server.close().await;
    f.provider.abort();
}
