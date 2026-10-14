use super::*;
use act0r::domain::RunId;
use uuid::Uuid;

#[tokio::test]
async fn paused_restart_tampering_automatic_rollback_and_revocation_race_preserve_authority() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("held.db");
    let (endpoint, model) = model(true).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &endpoint, root.path()).await;
    settle(&engine).await;
    let request = allow(&view(&server, &receipt).await);
    let id: RunId = request["action"]["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    post(
        &client(),
        &format!("{}/pause", goal(&server, &receipt)),
        &json!({"expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    let saved = post(
        &client(),
        &policy(&server, &receipt),
        &request,
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert!(
        !engine
            .status(id)
            .await
            .unwrap()
            .activities()
            .iter()
            .any(|a| a.spec.input["definition"]["id"]["name"] == "run_tests"
                && a.status == act0r::domain::ActivityStatus::Succeeded)
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(
        view(&server, &receipt).await["task_tests"][0]["status"],
        "active"
    );
    post(
        &client(),
        &format!("{}/resume", goal(&server, &receipt)),
        &json!({"expected_revision":1}),
        StatusCode::OK,
    )
    .await;
    let mut worker = engine.worker().await.unwrap();
    // Persist the next native test wait before the worker can automatically approve it.
    for _ in 0..40 {
        if engine
            .status(id)
            .await
            .unwrap()
            .record()
            .wait
            .as_ref()
            .is_some_and(|w| {
                json!(w.id) != request["action"]["wait_id"]
                    && w.payload["tool"]["name"] == "run_tests"
            })
        {
            break;
        }
        assert!(worker.step().await.unwrap());
    }
    let before = serde_json::to_value(engine.status(id).await.unwrap()).unwrap();
    assert_eq!(
        before["record"]["wait"]["payload"]["tool"]["name"],
        "run_tests"
    );
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_auto_permission BEFORE INSERT ON history WHEN json_extract(NEW.payload,'$.kind')='task_test_authorized' BEGIN SELECT RAISE(ABORT,'automatic permission rollback'); END;").execute(&pool).await.unwrap();
    assert!(worker.advance_work().await.is_err());
    assert_eq!(
        before,
        serde_json::to_value(engine.status(id).await.unwrap()).unwrap()
    );
    sqlx::query("DROP TRIGGER reject_auto_permission")
        .execute(&pool)
        .await
        .unwrap();
    let original = saved["task_test"].clone();
    let mut changes = Vec::new();
    for field in [
        "actor_id",
        "task_id",
        "run_id",
        "grant_id",
        "session_id",
        "turn_id",
    ] {
        let mut grant = original.clone();
        grant[field] = json!(Uuid::new_v4());
        changes.push(grant);
    }
    for (pointer, value) in [
        ("/request/test_command/argv", json!(["other"])),
        ("/request/test_command/timeout_ms", json!(1)),
        ("/request/workspace", json!("other")),
        ("/request/arguments", json!({"extra":true})),
        ("/request/definition/description", json!("changed contract")),
        ("/workspace_input/workspace", json!("other")),
        ("/workspace_input/snapshot_digest", json!("0".repeat(64))),
        ("/workspace_input/request/writable_paths", json!(["other/"])),
    ] {
        let mut grant = original.clone();
        *grant.pointer_mut(pointer).unwrap() = value;
        changes.push(grant);
    }
    for grant in changes {
        sqlx::query("UPDATE harness_task_test_permissions SET payload=? WHERE task_id=?")
            .bind(grant.to_string())
            .bind(original["task_id"].as_str().unwrap())
            .execute(&pool)
            .await
            .unwrap();
        worker.advance_work().await.unwrap();
        assert_eq!(
            before,
            serde_json::to_value(engine.status(id).await.unwrap()).unwrap()
        );
    }
    sqlx::query("UPDATE harness_task_test_permissions SET payload=? WHERE task_id=?")
        .bind(original.to_string())
        .bind(original["task_id"].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let stop = revoke(&view(&server, &receipt).await);
    let http = client();
    let url = policy(&server, &receipt);
    let (authorization, revocation) = tokio::join!(
        worker.advance_work(),
        post(&http, &url, &stop, StatusCode::OK)
    );
    authorization.unwrap();
    assert!(revocation["task_test"]["revoked_at"].is_number());
    let authorized = engine.status(id).await.unwrap().record().wait.is_none();
    if authorized {
        for _ in 0..20 {
            if engine
                .status(id)
                .await
                .unwrap()
                .activities()
                .iter()
                .filter(|a| {
                    a.spec.input["definition"]["id"]["name"] == "run_tests"
                        && a.status == act0r::domain::ActivityStatus::Succeeded
                })
                .count()
                == 2
            {
                break;
            }
            assert!(worker.step().await.unwrap());
        }
    }
    let run = engine.status(id).await.unwrap();
    assert_eq!(
        run.activities()
            .iter()
            .filter(|a| a.spec.input["definition"]["id"]["name"] == "run_tests"
                && a.status == act0r::domain::ActivityStatus::Succeeded)
            .count(),
        if authorized { 2 } else { 1 }
    );
    assert_eq!(
        view(&server, &receipt).await["task_tests"][0]["status"],
        "revoked"
    );
    pool.close().await;
    drop(worker);
    server.close().await;
    model.abort();
}

#[tokio::test]
async fn cancellation_expires_saved_grants_and_refuses_fresh_task_test_authority() {
    let root = tempfile::tempdir().unwrap();
    let (endpoint, model) = model(true).await;
    let (engine, agents) = local_app(root.path().join("cancel.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &endpoint, root.path()).await;
    settle(&engine).await;
    let request = allow(&view(&server, &receipt).await);
    post(
        &client(),
        &policy(&server, &receipt),
        &request,
        StatusCode::OK,
    )
    .await;
    post(
        &client(),
        &format!("{}/cancel", goal(&server, &receipt)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        view(&server, &receipt).await["task_tests"][0]["status"],
        "expired"
    );
    let mut fresh = request.clone();
    fresh["request_id"] = json!(Uuid::new_v4());
    fresh["expected_revision"] = json!(1);
    post(
        &client(),
        &policy(&server, &receipt),
        &fresh,
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(
        post(
            &client(),
            &policy(&server, &receipt),
            &request,
            StatusCode::OK
        )
        .await["replayed"],
        true
    );
    post(
        &client(),
        &policy(&server, &receipt),
        &revoke(&view(&server, &receipt).await),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    server.close().await;
    model.abort();
}

fn tool(name: &str) -> Value {
    json!({"kind":"call_tool","tool":name,"arguments":{}})
}
async fn model(scoped: bool) -> (String, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let task = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| async move {
            let prompt: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            let turn = prompt["turn"].as_u64().unwrap();
            let goal = prompt["goal"].as_str().unwrap();
            let action = if goal.contains("[first]") {
                match turn {
                    0 | 1 | 5 | 6 => tool("run_tests"),
                    2 => json!({"kind":"call_tool","tool":"create_file","arguments":{"path":"src/note.txt","content":"Requires separate edit approval"}}),
                    3 => json!({"kind":"call_tool","tool":"run_command","arguments":{"name":"version"}}),
                    4 => json!({"kind":"ask_human","question":"Continue after revoking test permission?"}),
                    7 => json!({"kind":"finish","summary":"Verified worker tests"}),
                    _ => panic!("unexpected first worker turn {turn}"),
                }
            } else if goal.contains("[second]") {
                match turn {
                    0 => tool("run_tests"),
                    1 => json!({"kind":"finish","summary":"Verified later task"}),
                    _ => panic!("unexpected second worker turn {turn}"),
                }
            } else {
                match turn {
                    0 | 1 => {
                        let mut action = json!({"kind":"delegate_task","worker":"coder","goal":if turn==0 {"Test code [first]"} else {"Test code [second]"}});
                        if scoped {action["writable_paths"] = json!(["src/"]);}
                        action
                    }
                    2 => tool("run_tests"),
                    3 => json!({"kind":"finish_incomplete","summary":"Child work retained for review"}),
                    _ => panic!("unexpected planner turn {turn}"),
                }
            };
            axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))
        }))).await.unwrap();
    });
    (endpoint, task)
}
async fn start(server: &Server, endpoint: &str, root: &Path) -> Value {
    let project = root.join("project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(project.join("src/lib.rs"), "Original source\n").unwrap();
    post(&client(), &format!("{}/v1/harness/sessions", server.url), &json!({
        "message_id":MessageId::new(),"prompt":"Task test permission fixture","workspace":fs::canonicalize(project).unwrap(),
        "target":{"provider":"openai","model":"task-test-fixture","endpoint":endpoint,"protocol":"chat_completions"},
        "coding":{"writable_paths":["src/"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000},"project_commands":{"version":{"argv":["rustc","--version"],"timeout_ms":30000}}}
    }), StatusCode::ACCEPTED).await
}
fn goal(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn policy(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/permissions",
        server.url,
        receipt["session_id"].as_str().unwrap()
    )
}
async fn view(server: &Server, receipt: &Value) -> Value {
    get(
        &client(),
        &format!(
            "{}/v1/harness/sessions/{}/turns/{}/permissions",
            server.url,
            receipt["session_id"].as_str().unwrap(),
            receipt["task_id"].as_str().unwrap()
        ),
    )
    .await
}
fn allow(view: &Value) -> Value {
    let wait = &view["eligible_task_test_waits"][0];
    assert!(wait.is_object(), "missing worker test approval: {view}");
    json!({"request_id":Uuid::new_v4(),"expected_revision":view["state"]["revision"],"action":{"kind":"allow_task_tests","task_id":wait["task_id"],"run_id":wait["run_id"],"wait_id":wait["wait_id"]}})
}
fn revoke(view: &Value) -> Value {
    json!({"request_id":Uuid::new_v4(),"expected_revision":view["state"]["revision"],"action":{"kind":"revoke_task_tests","grant_id":view["task_tests"][0]["grant"]["grant_id"]}})
}
async fn respond(server: &Server, receipt: &Value, kind: &str, response: Value) -> Value {
    let url = goal(server, receipt);
    let item = wait(&get(&client(), &format!("{url}/inbox")).await, kind);
    answer(&client(), &url, &item, response, StatusCode::OK).await;
    item
}

#[tokio::test]
async fn task_tests_survive_restart_revoke_regrant_and_expire_without_inheritance() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("tests.db");
    let (endpoint, model) = model(true).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &endpoint, root.path()).await;
    settle(&engine).await;
    let request = allow(&view(&server, &receipt).await);
    let id: RunId = request["action"]["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let url = policy(&server, &receipt);
    let http = client();
    let (a, b) = tokio::join!(
        post(&http, &url, &request, StatusCode::OK),
        post(&http, &url, &request, StatusCode::OK)
    );
    assert_ne!(a["replayed"], b["replayed"]);
    assert_eq!(a["task_test"], b["task_test"]);
    assert_eq!(a["state"]["revision"], 1);
    assert!(a["state"]["test_command"].is_null());
    assert_eq!(
        engine
            .status(id)
            .await
            .unwrap()
            .record()
            .input_receipts
            .len(),
        1
    );
    // Restart after authorization is committed but before executing the command.
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    assert_eq!(
        view(&server, &receipt).await["task_tests"][0]["status"],
        "active"
    );
    settle(&engine).await;
    let edit = respond(
        &server,
        &receipt,
        "tool_approval",
        json!({"kind":"approval","approved":false}),
    )
    .await;
    assert_eq!(edit["wait"]["payload"]["tool"]["name"], "create_file");
    settle(&engine).await;
    let command = respond(
        &server,
        &receipt,
        "tool_approval",
        json!({"kind":"approval","approved":false}),
    )
    .await;
    assert_eq!(command["wait"]["payload"]["tool"]["name"], "run_command");
    settle(&engine).await;
    let history = engine.history(id).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|e| e.kind == "task_test_authorized")
            .count(),
        2
    );
    assert_eq!(
        engine
            .status(id)
            .await
            .unwrap()
            .activities()
            .iter()
            .filter(|a| a.spec.input["definition"]["id"]["name"] == "run_tests"
                && a.status == act0r::domain::ActivityStatus::Succeeded)
            .count(),
        2
    );
    let stopped = revoke(&view(&server, &receipt).await);
    post(
        &client(),
        &policy(&server, &receipt),
        &stopped,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        post(
            &client(),
            &policy(&server, &receipt),
            &request,
            StatusCode::OK
        )
        .await["replayed"],
        true
    );
    assert_eq!(
        view(&server, &receipt).await["task_tests"][0]["status"],
        "revoked"
    );
    respond(
        &server,
        &receipt,
        "question",
        json!({"kind":"answer","answer":"Continue"}),
    )
    .await;
    settle(&engine).await;
    let next = allow(&view(&server, &receipt).await);
    assert_ne!(next["action"]["wait_id"], request["action"]["wait_id"]);
    assert_eq!(next["action"]["task_id"], request["action"]["task_id"]);
    post(&client(), &policy(&server, &receipt), &next, StatusCode::OK).await;
    post(
        &client(),
        &policy(&server, &receipt),
        &stopped,
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let current = view(&server, &receipt).await;
    assert_eq!(current["task_tests"][0]["status"], "expired");
    assert_eq!(
        current["task_tests"][0]["grant"]["grant_id"],
        next["request_id"]
    );
    let later = allow(&current);
    assert_ne!(later["action"]["task_id"], request["action"]["task_id"]);
    assert_eq!(
        current["eligible_task_test_waits"][0]["actor_id"],
        a["task_test"]["actor_id"]
    );
    respond(
        &server,
        &receipt,
        "tool_approval",
        json!({"kind":"approval","approved":true}),
    )
    .await;
    settle(&engine).await;
    let root_wait = view(&server, &receipt).await;
    assert!(
        root_wait["eligible_task_test_waits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(root_wait["eligible_wait"]["task_id"], receipt["task_id"]);
    let forged = json!({"request_id":Uuid::new_v4(),"expected_revision":root_wait["state"]["revision"],"action":{"kind":"allow_task_tests","task_id":receipt["task_id"],"run_id":root_wait["eligible_wait"]["run_id"],"wait_id":root_wait["eligible_wait"]["wait_id"]}});
    post(
        &client(),
        &policy(&server, &receipt),
        &forged,
        StatusCode::CONFLICT,
    )
    .await;
    respond(
        &server,
        &receipt,
        "tool_approval",
        json!({"kind":"approval","approved":true}),
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
        "Original source\n"
    );
    assert!(!root.path().join("project/src/note.txt").exists());
    server.close().await;
    model.abort();
}

#[tokio::test]
async fn task_test_grants_reject_overrides_stale_identities_and_rollback_together() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("rollback.db");
    // Test authority does not require an explicit edit scope; inherited paths remain pinned.
    let (endpoint, model) = model(false).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &endpoint, root.path()).await;
    settle(&engine).await;
    let request = allow(&view(&server, &receipt).await);
    let id: RunId = request["action"]["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    for field in ["task_id", "run_id", "wait_id"] {
        let mut changed = request.clone();
        changed["action"][field] = json!(Uuid::new_v4());
        post(
            &client(),
            &policy(&server, &receipt),
            &changed,
            if field == "task_id" {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::CONFLICT
            },
        )
        .await;
    }
    for (field, value) in [
        ("command", json!(["anything"])),
        ("workspace", json!("other")),
        ("timeout_ms", json!(1)),
        ("writable_paths", json!(["other/"])),
    ] {
        let mut changed = request.clone();
        changed["action"][field] = value;
        post(
            &client(),
            &policy(&server, &receipt),
            &changed,
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await;
    }
    let before = serde_json::to_value(engine.status(id).await.unwrap()).unwrap();
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_permission BEFORE INSERT ON harness_permission_receipts BEGIN SELECT RAISE(ABORT,'permission rollback'); END;").execute(&pool).await.unwrap();
    post(
        &client(),
        &policy(&server, &receipt),
        &request,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(
        before,
        serde_json::to_value(engine.status(id).await.unwrap()).unwrap()
    );
    assert_eq!(view(&server, &receipt).await["state"]["revision"], 0);
    assert!(
        view(&server, &receipt).await["task_tests"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    sqlx::query("DROP TRIGGER reject_permission")
        .execute(&pool)
        .await
        .unwrap();
    post(
        &client(),
        &policy(&server, &receipt),
        &request,
        StatusCode::OK,
    )
    .await;
    let mut changed = request.clone();
    changed["expected_revision"] = json!(1);
    post(
        &client(),
        &policy(&server, &receipt),
        &changed,
        StatusCode::CONFLICT,
    )
    .await;
    let mut stale = revoke(&view(&server, &receipt).await);
    stale["expected_revision"] = json!(0);
    post(
        &client(),
        &policy(&server, &receipt),
        &stale,
        StatusCode::CONFLICT,
    )
    .await;
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    pool.close().await;
    server.close().await;
    model.abort();
}
