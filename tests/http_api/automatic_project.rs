//! Auto discovers, edits and verifies without a browser driving setup or approvals.
use super::project_setup::{provider, start_body};
use super::*;
use act0r::domain::RunStatus;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn automatic_project_batched_reads_survive_restart_and_flow_into_coding_context() {
    for native in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = project(root.path());
        let mut script = actions(false);
        script[0] = json!({"kind":"call_tool","tool":"read_files","arguments":{"paths":["README.md","math.mjs","missing.md"]}});
        script[2] = json!({"kind":"call_tool","tool":"read_files","arguments":{"paths":["math.mjs","contracts.mjs"]}});
        let (endpoint, count, provider) = provider(script).await;
        let db = root.path().join("batch-auto.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let agents = Arc::new(agents);
        let server = Server::start(agents.clone(), 2).await;
        let client = client();
        let body = start(&endpoint, &source, native);
        let original = post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &body,
            StatusCode::ACCEPTED,
        )
        .await;
        let mut worker = engine.worker().await.unwrap();
        let saved = loop {
            assert!(worker.step().await.unwrap());
            let task = agents
                .actors()
                .unwrap()
                .task(serde_json::from_value(original["task_id"].clone()).unwrap())
                .await
                .unwrap();
            if let Some(id) = task.run_id {
                let run = engine.status(id).await.unwrap();
                if run.activities().iter().any(|a| {
                    a.spec.input["definition"]["id"]["name"] == "read_files"
                        && a.status == act0r::domain::ActivityStatus::Succeeded
                }) {
                    break run;
                }
            }
        };
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let batch = saved
            .activities()
            .iter()
            .find(|a| a.spec.input["definition"]["id"]["name"] == "read_files")
            .unwrap();
        assert_eq!(
            batch.result.as_ref().unwrap()["files"][2]["error"]["code"],
            "file_not_found"
        );
        drop(worker);
        server.close().await;
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        assert_eq!(
            json!(engine.status(saved.record().id).await.unwrap()),
            json!(saved)
        );
        let agents = Arc::new(agents);
        let server = Server::start(agents, 2).await;
        settle(&engine).await;
        let history = get(&client, &conversation(&server, &original)).await;
        assert_eq!(history["stage"], "completed");
        assert_eq!(count.load(Ordering::SeqCst), 7);
        let current = history["active"].clone();
        let changes = get(
            &client,
            &format!(
                "{}/changes",
                super::verification_repair::turn_url(&server, &current)
            ),
        )
        .await;
        assert_eq!(changes["completion"]["status"], "verified");
        let pool =
            sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
                .await
                .unwrap();
        let findings:Vec<String>=sqlx::query_scalar("SELECT json_extract(payload,'$.spec.planner.config.discovery_context.evidence.findings') FROM agent_teams WHERE json_type(payload,'$.spec.planner.config.discovery_context')='object'").fetch_all(&pool).await.unwrap();
        assert_eq!(findings.len(), 1);
        let findings: Value = serde_json::from_str(&findings[0]).unwrap();
        assert_eq!(findings[0]["tool"], "read_files");
        assert!(
            findings[0]["result"]["text"]
                .as_str()
                .unwrap()
                .contains("README.md")
        );
        pool.close().await;
        assert_eq!(
            post(
                &client,
                &format!("{}/v1/harness/sessions", server.url),
                &body,
                StatusCode::ACCEPTED
            )
            .await,
            original
        );
        settle(&engine).await;
        assert_eq!(count.load(Ordering::SeqCst), 7);
        server.close().await;
        provider.abort();
    }
}

fn project(directory: &Path) -> std::path::PathBuf {
    let source = directory.join("project");
    fs::create_dir_all(&source).unwrap();
    fs::write(
        source.join("math.mjs"),
        "export const add = (a, b) => a - b;\n",
    )
    .unwrap();
    fs::write(source.join("README.md"), "Repair add in math.mjs. Run node contracts.mjs to verify, and node --check math.mjs for syntax.\n").unwrap();
    fs::write(source.join("contracts.mjs"), "import assert from 'node:assert/strict';\nimport {add} from './math.mjs';\nfor (const [a,b] of [[2,3],[-2,3],[0,5],[1.5,2.5],[-4,-6]]) assert.equal(add(a,b), a+b);\nconsole.log('5 automatic contracts passed');\n").unwrap();
    source
}
fn proposal() -> Value {
    json!({"summary":"README documents the Node contracts and syntax check", "writable_paths":["math.mjs"],"test_command":{"argv":["node","contracts.mjs"],"timeout_ms":10000},"commands":[{"name":"check","argv":["node","--check","math.mjs"],"timeout_ms":10000}]})
}
fn actions(question: bool) -> Vec<Value> {
    let mut actions =
        vec![json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}})];
    if question {
        actions.push(json!({"kind":"ask_human","question":"Should the public name remain add?"}));
    }
    actions.extend([
        json!({"kind":"propose_project_setup","proposal":proposal()}),
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"math.mjs"}}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"math.mjs","old":"a - b","new":"a + b"}}),
        json!({"kind":"call_tool","tool":"run_command","arguments":{"name":"check"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Fixed add and passed five contracts and the syntax check"}),
    ]);
    actions
}
fn start(endpoint: &str, source: &Path, native: bool) -> Value {
    let mut body = start_body(endpoint, source, native);
    body["auto_execute"] = json!(true);
    body
}
fn conversation(server: &Server, original: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/conversation",
        server.url,
        original["session_id"].as_str().unwrap()
    )
}
fn setup(server: &Server, original: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}/project-setup",
        server.url,
        original["session_id"].as_str().unwrap(),
        original["task_id"].as_str().unwrap()
    )
}
async fn stop_at_proposal(engine: &Engine, agents: &AgentService, original: &Value) {
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..100 {
        assert!(worker.step().await.unwrap());
        let task = agents
            .actors()
            .unwrap()
            .task(serde_json::from_value(original["task_id"].clone()).unwrap())
            .await
            .unwrap();
        if let Some(run) = task.run_id {
            let saved = engine.status(run).await.unwrap();
            if saved.record().status == RunStatus::Succeeded
                && saved
                    .record()
                    .result
                    .as_ref()
                    .is_some_and(|r| r.get("project_setup").is_some())
            {
                return;
            }
        }
    }
    panic!("proposal did not complete");
}

#[tokio::test]
async fn automatic_project_continues_after_restart_without_setup_or_tool_approvals() {
    for native in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source = project(root.path());
        let mut script = actions(true);
        script.extend([
            json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
            json!({"kind":"finish","summary":"Follow-up verified the same workspace"}),
        ]);
        let (endpoint, count, provider) = provider(script).await;
        let db = root.path().join("auto.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let agents = Arc::new(agents);
        let server = Server::start(agents.clone(), 2).await;
        let client = client();
        let body = start(&endpoint, &source, native);
        let original = post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &body,
            StatusCode::ACCEPTED,
        )
        .await;
        settle(&engine).await;
        let goal = super::verification_repair::goal_url(&server, &original);
        let question = wait(&get(&client, &format!("{goal}/inbox")).await, "question");
        answer(
            &client,
            &goal,
            &question,
            json!({"kind":"answer","answer":"Yes, preserve add."}),
            StatusCode::OK,
        )
        .await;
        stop_at_proposal(&engine, &agents, &original).await;
        assert_eq!(count.load(Ordering::SeqCst), 3);
        // Interrupt after actors/copies are prepared, before the continuation commits.
        let pool =
            sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
                .await
                .unwrap();
        sqlx::raw_sql("CREATE TRIGGER interrupt_auto_link BEFORE INSERT ON harness_project_continuations BEGIN SELECT RAISE(ABORT,'simulated admission failure'); END;").execute(&pool).await.unwrap();
        let review = get(&client, &setup(&server, &original)).await;
        let coding = serde_json::from_value::<act0r::domain::project_setup::ProjectSetupProposal>(
            review["proposal"].clone(),
        )
        .unwrap()
        .coding()
        .unwrap();
        post(
            &client,
            &setup(&server, &original),
            &json!({"coding":coding}),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let prepared:String=sqlx::query_scalar("SELECT payload FROM agent_teams WHERE json_extract(payload,'$.spec.planner.config.auto_execute')=1 AND COALESCE(json_extract(payload,'$.spec.planner.config.discover_project'),0)=0").fetch_one(&pool).await.unwrap();
        sqlx::query("DROP TRIGGER interrupt_auto_link")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        server.close().await;
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let agents = Arc::new(agents);
        let server = Server::start(agents.clone(), 2).await;
        settle(&engine).await;
        let history = get(&client, &conversation(&server, &original)).await;
        assert_eq!(history["stage"], "completed", "{history}");
        assert_eq!(history["auto_execute"], true);
        assert!(history["setup_error"].is_null());
        assert_eq!(history["turns"].as_array().unwrap().len(), 2);
        assert_eq!(
            history["turns"][0]["clarifications"][0]["answer"]["text"],
            "Yes, preserve add."
        );
        let current = history["active"].clone();
        assert_eq!(
            current["team_id"],
            serde_json::from_str::<Value>(&prepared).unwrap()["id"]
        );
        let changes = get(
            &client,
            &format!(
                "{}/changes",
                super::verification_repair::turn_url(&server, &current)
            ),
        )
        .await;
        assert_eq!(changes["completion"]["status"], "verified");
        assert_eq!(count.load(Ordering::SeqCst), 8);
        assert!(
            fs::read_to_string(source.join("math.mjs"))
                .unwrap()
                .contains("a - b")
        );
        let pool =
            sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
                .await
                .unwrap();
        let approval_count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM history WHERE json_extract(payload,'$.kind')='input_wait_created' AND json_extract(payload,'$.payload.wait.key') IN ('native_tool_approval','child_changes_approval')").fetch_one(&pool).await.unwrap();
        assert_eq!(approval_count, 0);
        pool.close().await;
        // Applying to the source still requires the existing explicit result review.
        super::verification_repair::apply(&engine, &client, &server, &current).await;
        assert!(
            fs::read_to_string(source.join("math.mjs"))
                .unwrap()
                .contains("a + b")
        );
        let follow = post(
            &client,
            &format!(
                "{}/v1/harness/sessions/{}/turns",
                server.url,
                original["session_id"].as_str().unwrap()
            ),
            &json!({"message_id":MessageId::new(),"prompt":"Check the result once more"}),
            StatusCode::ACCEPTED,
        )
        .await;
        assert_eq!(follow["team_id"], current["team_id"]);
        settle(&engine).await;
        assert_eq!(count.load(Ordering::SeqCst), 10);
        assert_eq!(
            post(
                &client,
                &format!("{}/v1/harness/sessions", server.url),
                &body,
                StatusCode::ACCEPTED
            )
            .await,
            original
        );
        settle(&engine).await;
        assert_eq!(count.load(Ordering::SeqCst), 10);
        server.close().await;
        provider.abort();
    }
}

#[tokio::test]
async fn automatic_project_honors_a_saved_pause_and_stop_before_coding_admission() {
    let root = tempfile::tempdir().unwrap();
    let source = project(root.path());
    let (endpoint, count, provider) = provider(actions(false)).await;
    let db = root.path().join("held.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let original = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &start(&endpoint, &source, true),
        StatusCode::ACCEPTED,
    )
    .await;
    stop_at_proposal(&engine, &agents, &original).await;
    let task = serde_json::from_value(original["task_id"].clone()).unwrap();
    agents.actors().unwrap().pause_goal(task).await.unwrap();
    settle(&engine).await;
    assert_eq!(
        get(&client, &conversation(&server, &original)).await["active"]["task_id"],
        original["task_id"]
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    server.close().await;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    settle(&engine).await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    agents.actors().unwrap().cancel_goal(task).await.unwrap();
    assert!(agents.actors().unwrap().resume_goal(task).await.is_err());
    settle(&engine).await;
    assert_eq!(
        get(&client, &conversation(&server, &original)).await["active"]["task_id"],
        original["task_id"]
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    server.close().await;
    provider.abort();
}

#[tokio::test]
async fn automatic_setup_failure_is_saved_without_stopping_other_requests_or_retrying_forever() {
    let root = tempfile::tempdir().unwrap();
    let source = project(root.path());
    let mut script = actions(false);
    script.truncate(2);
    script.push(json!({"kind":"finish","summary":"Another request completed"}));
    let (endpoint, count, provider) = provider(script).await;
    let db = root.path().join("failure.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let original = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &start(&endpoint, &source, true),
        StatusCode::ACCEPTED,
    )
    .await;
    stop_at_proposal(&engine, &agents, &original).await;
    fs::rename(&source, root.path().join("retained-project")).unwrap();
    settle(&engine).await;
    let failed = get(&client, &conversation(&server, &original)).await;
    assert_eq!(failed["stage"], "failed");
    assert_eq!(failed["can_continue"], false);
    assert!(!failed["setup_error"].as_str().unwrap().is_empty());
    assert_eq!(count.load(Ordering::SeqCst), 2);
    server.close().await;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(
        get(&client, &conversation(&server, &original)).await["setup_error"],
        failed["setup_error"]
    );
    let mut another = start_body(&endpoint, &root.path().join("retained-project"), true);
    another.as_object_mut().unwrap().remove("workspace");
    another["discover_project"] = json!(false);
    post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &another,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(count.load(Ordering::SeqCst), 3);
    server.close().await;
    provider.abort();
}

#[tokio::test]
async fn automatic_project_worker_edits_tests_and_imports_before_fresh_parent_verification() {
    let root = tempfile::tempdir().unwrap();
    let source = project(root.path());
    let script = vec![
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        json!({"kind":"propose_project_setup","proposal":proposal()}),
        json!({"kind":"delegate_task","worker":"coder","goal":"Repair add in math.mjs and verify the five contracts","writable_paths":["math.mjs"]}),
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"math.mjs"}}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"math.mjs","old":"a - b","new":"a + b"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Worker repaired addition and passed five contracts"}),
        json!({"kind":"inspect_child_changes","worker":"coder","offset":0}),
        json!({"kind":"import_child_changes","worker":"coder"}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Imported the worker patch and verified the integrated project"}),
    ];
    let (endpoint, count, provider) = provider(script).await;
    let (engine, agents) = local_app(root.path().join("worker.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let original = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &start(&endpoint, &source, true),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let history = get(&client, &conversation(&server, &original)).await;
    assert_eq!(history["stage"], "completed", "{history}");
    assert_eq!(count.load(Ordering::SeqCst), 11);
    let changes = get(
        &client,
        &format!(
            "{}/changes",
            super::verification_repair::turn_url(&server, &history["active"])
        ),
    )
    .await;
    assert_eq!(changes["completion"]["status"], "verified");
    assert!(
        fs::read_to_string(source.join("math.mjs"))
            .unwrap()
            .contains("a - b")
    );
    server.close().await;
    provider.abort();
}
