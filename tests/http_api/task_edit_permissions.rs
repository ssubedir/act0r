use super::*;
use act0r::domain::RunId;
use uuid::Uuid;

fn tool(name: &str, arguments: Value) -> Value {
    json!({"kind":"call_tool","tool":name,"arguments":arguments})
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
                    0 => tool("replace_text",json!({"path":"README.md","old":"Before","new":"After"})),
                    1 => tool("create_file",json!({"path":"src/note.txt","content":"Created under task permission"})),
                    2 => tool("replace_text",json!({"path":"OTHER.md","old":"Before","new":"Denied"})),
                    3 => json!({"kind":"ask_human","question":"Continue this task after revoking edit permission?"}),
                    4 => tool("replace_text",json!({"path":"README.md","old":"After","new":"Final"})),
                    5 => tool("run_tests",json!({})),
                    6 => json!({"kind":"finish","summary":"Verified task edits"}),
                    _ => panic!("unexpected first worker turn {turn}"),
                }
            } else if goal.contains("[second]") {
                match turn {
                    0 => tool("replace_text",json!({"path":"README.md","old":"Before","new":"Second task"})),
                    1 => json!({"kind":"finish_incomplete","summary":"Second task left for review"}),
                    _ => panic!("unexpected second worker turn {turn}"),
                }
            } else {
                match turn {
                    0 | 1 => {
                        let mut action = json!({"kind":"delegate_task","worker":"coder","goal":if turn==0 {"Edit files [first]"} else {"Edit files [second]"}});
                        if scoped { action["writable_paths"] = json!(["README.md","src/"]); }
                        action
                    }
                    2 => json!({"kind":"finish_incomplete","summary":"Child patches retained for review"}),
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
    fs::write(project.join("README.md"), "Before\n").unwrap();
    fs::write(project.join("OTHER.md"), "Before\n").unwrap();
    post(&client(),&format!("{}/v1/harness/sessions",server.url),&json!({
        "message_id":MessageId::new(),"prompt":"Task edit permission fixture","workspace":fs::canonicalize(project).unwrap(),
        "target":{"provider":"openai","model":"task-permission-fixture","endpoint":endpoint,"protocol":"chat_completions"},
        "coding":{"writable_paths":["README.md","src/"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000}}
    }),StatusCode::ACCEPTED).await
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
    let wait = &view["eligible_edit_waits"][0];
    assert!(wait.is_object(), "no eligible scoped edit: {view}");
    json!({"request_id":Uuid::new_v4(),"expected_revision":view["state"]["revision"],"action":{"kind":"allow_task_edits","task_id":wait["task_id"],"run_id":wait["run_id"],"wait_id":wait["wait_id"]}})
}
fn revoke(view: &Value) -> Value {
    json!({"request_id":Uuid::new_v4(),"expected_revision":view["state"]["revision"],"action":{"kind":"revoke_task_edits","grant_id":view["task_edits"][0]["grant"]["grant_id"]}})
}
async fn respond(server: &Server, receipt: &Value, kind: &str, response: Value) -> Value {
    let url = goal(server, receipt);
    let item = wait(&get(&client(), &format!("{url}/inbox")).await, kind);
    answer(&client(), &url, &item, response, StatusCode::OK).await;
    item
}

#[tokio::test]
async fn task_edits_survive_restart_revoke_expire_and_never_cover_tests_or_later_tasks() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("permission.db");
    let (endpoint, model) = model(true).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &endpoint, root.path()).await;
    settle(&engine).await;
    let initial = view(&server, &receipt).await;
    let request = allow(&initial);
    let run_id: RunId = request["action"]["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let wait_id = request["action"]["wait_id"].clone();
    let url = policy(&server, &receipt);
    let http = client();
    let (a, b) = tokio::join!(
        post(&http, &url, &request, StatusCode::OK),
        post(&http, &url, &request, StatusCode::OK)
    );
    assert_ne!(a["replayed"], b["replayed"]);
    assert_eq!(a["state"]["revision"], 1);
    assert_eq!(a["task_edit"], b["task_edit"]);
    let saved = engine.status(run_id).await.unwrap();
    assert_eq!(
        saved
            .record()
            .input_receipts
            .iter()
            .filter(|r| json!(r.wait_id) == wait_id)
            .count(),
        1
    );
    assert_eq!(
        saved
            .activities()
            .iter()
            .filter(|a| a.spec.input["definition"]["id"]["name"] == "replace_text")
            .count(),
        1
    );
    // The first approval and continuation are saved; restart before executing any edit.
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    assert_eq!(
        view(&server, &receipt).await["task_edits"][0]["status"],
        "active"
    );
    settle(&engine).await;
    let current = view(&server, &receipt).await;
    assert!(
        current["eligible_edit_waits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let outside = respond(
        &server,
        &receipt,
        "tool_approval",
        json!({"kind":"approval","approved":false}),
    )
    .await;
    assert_eq!(outside["wait"]["payload"]["arguments"]["path"], "OTHER.md");
    let workspace = a["task_edit"]["workspace_input"]["workspace"]
        .as_str()
        .unwrap();
    assert_eq!(
        fs::read_to_string(Path::new(workspace).join("README.md")).unwrap(),
        "After\n"
    );
    assert!(Path::new(workspace).join("src/note.txt").is_file());
    assert_eq!(
        fs::read_to_string(Path::new(workspace).join("OTHER.md")).unwrap(),
        "Before\n"
    );
    let history = engine.history(run_id).await.unwrap();
    let auth: Vec<_> = history
        .iter()
        .filter(|e| e.kind == "task_edit_authorized")
        .collect();
    assert_eq!(auth.len(), 2);
    assert!(
        auth.iter()
            .all(|e| e.payload["grant_id"] == request["request_id"])
    );
    settle(&engine).await;
    let stopped = revoke(&view(&server, &receipt).await);
    post(
        &client(),
        &policy(&server, &receipt),
        &stopped,
        StatusCode::OK,
    )
    .await;
    let replay = post(
        &client(),
        &policy(&server, &receipt),
        &request,
        StatusCode::OK,
    )
    .await;
    assert_eq!(replay["replayed"], true);
    assert_eq!(
        view(&server, &receipt).await["task_edits"][0]["status"],
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
    let new_view = view(&server, &receipt).await;
    let next = allow(&new_view);
    assert_ne!(next["action"]["wait_id"], request["action"]["wait_id"]);
    assert_eq!(next["action"]["task_id"], request["action"]["task_id"]);
    post(&client(), &policy(&server, &receipt), &next, StatusCode::OK).await;
    // Retry of old revoke cannot revoke the replacement grant.
    post(
        &client(),
        &policy(&server, &receipt),
        &stopped,
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let tests = respond(
        &server,
        &receipt,
        "tool_approval",
        json!({"kind":"approval","approved":true}),
    )
    .await;
    assert_eq!(tests["wait"]["payload"]["tool"]["name"], "run_tests");
    settle(&engine).await;
    let current = view(&server, &receipt).await;
    assert_eq!(current["task_edits"][0]["status"], "expired");
    assert_eq!(
        current["task_edits"][0]["grant"]["grant_id"],
        next["request_id"]
    );
    let second = allow(&current);
    assert_ne!(second["action"]["task_id"], next["action"]["task_id"]);
    // The same actor's later task still requires a fresh approval.
    assert_ne!(second["action"]["run_id"], next["action"]["run_id"]);
    assert_eq!(
        fs::read_to_string(root.path().join("project/README.md")).unwrap(),
        "Before\n"
    );
    respond(
        &server,
        &receipt,
        "tool_approval",
        json!({"kind":"approval","approved":false}),
    )
    .await;
    settle(&engine).await;
    assert_eq!(view(&server, &receipt).await["state"]["revision"], 3);
    server.close().await;
    model.abort();
}

#[tokio::test]
async fn edit_permission_validates_identity_scope_revision_and_rolls_back_approval_on_failure() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("atomic.db");
    let (endpoint, model) = model(true).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &endpoint, root.path()).await;
    settle(&engine).await;
    let current = view(&server, &receipt).await;
    let request = allow(&current);
    let id: RunId = request["action"]["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    for field in ["task_id", "run_id", "wait_id"] {
        let mut forged = request.clone();
        forged["action"][field] = json!(Uuid::new_v4());
        let status = if field == "task_id" {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::CONFLICT
        };
        post(&client(), &policy(&server, &receipt), &forged, status).await;
    }
    let mut extra = request.clone();
    extra["action"]["writable_paths"] = json!(["OTHER.md"]);
    post(
        &client(),
        &policy(&server, &receipt),
        &extra,
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
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
        view(&server, &receipt).await["task_edits"]
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

#[tokio::test]
async fn inherited_scope_and_cancelled_tasks_cannot_receive_edit_grants() {
    let root = tempfile::tempdir().unwrap();
    let (endpoint, model) = model(false).await;
    let (engine, agents) = local_app(root.path().join("inherited.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &endpoint, root.path()).await;
    settle(&engine).await;
    assert!(
        view(&server, &receipt).await["eligible_edit_waits"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let item = wait(
        &get(&client(), &format!("{}/inbox", goal(&server, &receipt))).await,
        "tool_approval",
    );
    let task = get(&client(), &goal(&server, &receipt)).await;
    let child = &task["task"]["nodes"][1];
    let request = json!({"request_id":Uuid::new_v4(),"expected_revision":0,"action":{"kind":"allow_task_edits","task_id":child["task_id"],"run_id":item["run_id"],"wait_id":item["wait"]["id"]}});
    post(
        &client(),
        &policy(&server, &receipt),
        &request,
        StatusCode::CONFLICT,
    )
    .await;
    server.close().await;
    model.abort();
    let root = tempfile::tempdir().unwrap();
    let (endpoint, model) = self::model(true).await;
    let (engine, agents) = local_app(root.path().join("cancelled.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &endpoint, root.path()).await;
    settle(&engine).await;
    let request = allow(&view(&server, &receipt).await);
    post(
        &client(),
        &format!("{}/cancel", goal(&server, &receipt)),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    post(
        &client(),
        &policy(&server, &receipt),
        &request,
        StatusCode::CONFLICT,
    )
    .await;
    server.close().await;
    model.abort();
}

#[tokio::test]
async fn held_permissions_and_changed_saved_bindings_cannot_approve_edits_and_revocation_serializes()
 {
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
    let workspace = Path::new(
        saved["task_edit"]["workspace_input"]["workspace"]
            .as_str()
            .unwrap(),
    );
    settle(&engine).await;
    assert_eq!(
        fs::read_to_string(workspace.join("README.md")).unwrap(),
        "Before\n"
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(
        view(&server, &receipt).await["task_edits"][0]["status"],
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
    // Stop immediately after the next edit wait is persisted, before automatic authorization.
    for _ in 0..40 {
        if engine
            .status(id)
            .await
            .unwrap()
            .record()
            .wait
            .as_ref()
            .is_some_and(|w| w.payload["tool"]["name"] == "create_file")
        {
            break;
        }
        assert!(worker.step().await.unwrap());
    }
    let before = serde_json::to_value(engine.status(id).await.unwrap()).unwrap();
    assert_eq!(
        before["record"]["wait"]["payload"]["tool"]["name"],
        "create_file"
    );
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER reject_auto_permission BEFORE INSERT ON history WHEN json_extract(NEW.payload,'$.kind')='task_edit_authorized' BEGIN SELECT RAISE(ABORT,'automatic permission rollback'); END;").execute(&pool).await.unwrap();
    assert!(worker.advance_work().await.is_err());
    assert_eq!(
        before,
        serde_json::to_value(engine.status(id).await.unwrap()).unwrap()
    );
    sqlx::query("DROP TRIGGER reject_auto_permission")
        .execute(&pool)
        .await
        .unwrap();
    let original = saved["task_edit"].clone();
    let mut changed = Vec::new();
    for field in ["actor_id", "run_id", "grant_id"] {
        let mut grant = original.clone();
        grant[field] = json!(Uuid::new_v4());
        changed.push(grant);
    }
    let mut grant = original.clone();
    grant["workspace_input"]["request"]["writable_paths"] = json!(["OTHER.md"]);
    changed.push(grant);
    let mut grant = original.clone();
    grant["workspace_input"]["workspace"] = json!("different workspace");
    changed.push(grant);
    let mut grant = original.clone();
    grant["workspace_input"]["snapshot_digest"] = json!("0".repeat(64));
    changed.push(grant);
    let mut grant = original.clone();
    grant["tools"][0]["description"] = json!("changed contract");
    changed.push(grant);
    let mut grant = original.clone();
    grant["tools"][1]["id"]["version"] = json!(2);
    changed.push(grant);
    for grant in changed {
        sqlx::query("UPDATE harness_task_edit_permissions SET payload=? WHERE task_id=?")
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
        assert!(!workspace.join("src/note.txt").exists());
    }
    sqlx::query("UPDATE harness_task_edit_permissions SET payload=? WHERE task_id=?")
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
    assert!(revocation["task_edit"]["revoked_at"].is_number());
    let run = engine.status(id).await.unwrap();
    let authorized = run.record().wait.is_none();
    if authorized {
        assert!(worker.step().await.unwrap());
    }
    assert_eq!(workspace.join("src/note.txt").exists(), authorized);
    assert!(!worker.advance_work().await.unwrap());
    assert_eq!(
        view(&server, &receipt).await["task_edits"][0]["status"],
        "revoked"
    );
    pool.close().await;
    drop(worker);
    server.close().await;
    model.abort();
}
