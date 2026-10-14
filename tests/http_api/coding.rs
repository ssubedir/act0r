//! Browser coding uses actual Rust tests, immutable patches, and saved approvals.
use super::*;
use act0r::domain::{RunId, SessionId};
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) async fn provider(actions: Vec<Value>) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    provider_with_check(actions, |index, body| {
        // The helper reads a snapshot of the edited planner workspace.
        if index == 6 {
            assert!(body["messages"].to_string().contains("a + b"));
        }
    })
    .await
}
pub(super) async fn provider_with_check(
    actions: Vec<Value>,
    check: impl Fn(usize, &Value) + Send + Sync + 'static,
) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    provider_with_usage(
        actions,
        check,
        Some(json!({"prompt_tokens":10,"completion_tokens":10})),
    )
    .await
}
async fn provider_with_usage(
    actions: Vec<Value>,
    check: impl Fn(usize, &Value) + Send + Sync + 'static,
    usage: Option<Value>,
) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let check = Arc::new(check);
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let task = tokio::spawn(async move {
        axum::serve(listener,axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
            let index=counted.fetch_add(1,Ordering::SeqCst);
            let action=actions.get(index).unwrap_or_else(|| panic!("unexpected model request {index}")).clone();
            check(index, &body);
            let usage=usage.clone();
            async move {
                axum::Json(json!({"id":"coding-fixture","choices":[{"message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()},"finish_reason":"stop"}],"usage":usage}))
            }
        }))).await.unwrap();
    });
    (endpoint, calls, task)
}
fn project(root: &Path) -> String {
    let source = root.join("project");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"harness_coding_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"harness_coding_fixture\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    fs::write(source.join("src/lib.rs"),"pub fn add(a: i32, b: i32) -> i32 { a - b }\n#[test]\nfn addition() { assert_eq!(add(2,3),5); }\n").unwrap();
    fs::write(source.join("README.md"), "Original project").unwrap();
    fs::canonicalize(source)
        .unwrap()
        .to_string_lossy()
        .into_owned()
}
fn input(endpoint: &str, source: &str) -> Value {
    json!({"message_id":MessageId::new(),"prompt":"Fix addition and verify it","workspace":source,
        "target":{"provider":"openai","model":"coding-fixture","endpoint":endpoint,"protocol":"chat_completions"},
        "coding":{"writable_paths":["src/"],"test_command":{"argv":["cargo","test","--locked","--offline","--quiet"],"timeout_ms":30000}}})
}
fn tool(name: &str, arguments: Value) -> Value {
    json!({"kind":"call_tool","tool":name,"arguments":arguments})
}
fn urls(server: &Server, receipt: &Value) -> (String, String) {
    (
        format!(
            "{}/v1/teams/{}/tasks/{}",
            server.url,
            receipt["team_id"].as_str().unwrap(),
            receipt["task_id"].as_str().unwrap()
        ),
        format!(
            "{}/v1/harness/sessions/{}/turns/{}",
            server.url,
            receipt["session_id"].as_str().unwrap(),
            receipt["task_id"].as_str().unwrap()
        ),
    )
}
async fn approve_tool(client: &Client, base: &str) {
    let item = wait(
        &get(client, &format!("{base}/inbox")).await,
        "tool_approval",
    );
    answer(
        client,
        base,
        &item,
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
}
async fn preview(client: &Client, engine: &Engine, base: &str) -> (Value, Value) {
    let request = json!({"application_id":RunId::new()});
    let url = format!("{base}/applications");
    let (receipt, replayed) = tokio::join!(
        post(client, &url, &request, StatusCode::ACCEPTED),
        post(client, &url, &request, StatusCode::ACCEPTED)
    );
    assert_eq!(receipt, replayed);
    settle(engine).await;
    let view = get(
        client,
        &format!(
            "{base}/applications/{}",
            receipt["run_id"].as_str().unwrap()
        ),
    )
    .await;
    (request, view)
}
async fn decide(client: &Client, base: &str, view: &Value, approved: bool) -> (String, Value) {
    let url = format!(
        "{base}/applications/{}/responses",
        view["run_id"].as_str().unwrap()
    );
    let body = json!({"wait_id":view["wait"]["id"],"approved":approved});
    post(client, &url, &body, StatusCode::OK).await;
    (url, body)
}

#[tokio::test]
async fn harness_coder_recovers_same_task_imports_current_code_and_requires_parent_tests() {
    let root = tempfile::tempdir().unwrap();
    let source = project(root.path());
    let source_file = Path::new(&source).join("src/lib.rs");
    let original = fs::read_to_string(&source_file).unwrap();
    let database = root.path().join("coder.db");
    let actions = vec![
        tool(
            "replace_text",
            json!({"path":"src/lib.rs","old":"a - b","new":"a - b /* planner input */"}),
        ),
        json!({"kind":"delegate_task","worker":"coder","goal":"Repair addition in the current code and run the fixed tests"}),
        tool("read_file", json!({"path":"src/lib.rs"})),
        tool(
            "replace_text",
            json!({"path":"src/lib.rs","old":"a - b","new":"a + b"}),
        ),
        tool("run_tests", json!({})),
        json!({"kind":"finish","summary":"Verified child repair"}),
        json!({"kind":"inspect_child_changes","worker":"coder","offset":0}),
        json!({"kind":"import_child_changes","worker":"coder"}),
        // The child's passing tests must never satisfy the parent's completion gate.
        json!({"kind":"finish","summary":"Attempting to finish without combined tests"}),
        tool("run_tests", json!({})),
        json!({"kind":"finish","summary":"Imported addition repair and verified the combined code"}),
    ];
    let (endpoint, calls, model) = provider_with_check(actions, |index, body| {
        let context = body["messages"].to_string();
        if index == 0 {
            assert!(context.contains("coder"));
            assert!(context.contains("inspect_child_changes"));
        }
        if index == 3 {
            assert!(context.contains("planner input"));
        }
        if index == 9 {
            assert!(context.contains("incomplete"));
        }
    })
    .await;
    let (engine, agents) = local_app(&database).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let initial = input(&endpoint, &source);
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &initial,
        StatusCode::ACCEPTED,
    )
    .await;
    let (goal, _) = urls(&server, &receipt);
    settle(&engine).await;
    approve_tool(&client, &goal).await; // Parent's current-code edit.
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    let edit = wait(
        &get(&client, &format!("{goal}/inbox")).await,
        "tool_approval",
    );
    assert_eq!(edit["member"], "coder");
    let child: MessageId = serde_json::from_value(edit["task_id"].clone()).unwrap();
    let child_run: RunId = serde_json::from_value(edit["run_id"].clone()).unwrap();
    let saved = engine.status(child_run).await.unwrap();
    let workspace_input = saved.record().input["input"]["workspace_input"].clone();
    let copy = saved.record().input["input"]["workspace"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(workspace_input.is_object());
    assert!(
        fs::read_to_string(Path::new(&copy).join("src/lib.rs"))
            .unwrap()
            .contains("planner input")
    );
    assert_eq!(fs::read_to_string(&source_file).unwrap(), original);
    // A durable hold can save the answer without admitting the child's edit.
    post(
        &client,
        &format!("{goal}/pause"),
        &json!({"expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    answer(
        &client,
        &goal,
        &edit,
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    server.close().await;
    drop(agents);
    drop(engine);

    let (engine, agents) = local_app(&database).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let (goal, _) = urls(&server, &receipt);
    assert_eq!(
        post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &initial,
            StatusCode::ACCEPTED
        )
        .await,
        receipt
    );
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    let restored = engine.status(child_run).await.unwrap();
    assert_eq!(
        restored.record().input["input"]["workspace_input"],
        workspace_input
    );
    assert_eq!(restored.record().input["input"]["workspace"], copy);
    assert!(
        fs::read_to_string(Path::new(&copy).join("src/lib.rs"))
            .unwrap()
            .contains("a - b")
    );
    assert_eq!(
        answer(
            &client,
            &goal,
            &edit,
            json!({"kind":"approval","approved":true}),
            StatusCode::OK
        )
        .await["replayed"],
        true
    );
    post(
        &client,
        &format!("{goal}/resume"),
        &json!({"expected_revision":1}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    approve_tool(&client, &goal).await; // Child's fixed tests.
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 8);
    let import = wait(
        &get(&client, &format!("{goal}/inbox")).await,
        "child_import_approval",
    );
    let inspection = &import["wait"]["payload"]["inspection"];
    assert_eq!(inspection["task_id"], json!(child));
    assert_eq!(inspection["preview"]["test_status"], "matching");
    assert_eq!(inspection["preview"]["tests"]["result"]["ok"], true);
    assert!(
        inspection["patch"]
            .as_str()
            .unwrap()
            .contains("+pub fn add")
    );
    let child_changes = agents.actors().unwrap().task_changes(child).await.unwrap();
    assert_eq!(
        child_changes.completion.unwrap().status,
        act0r::domain::completion::CompletionStatus::Verified
    );
    let parent_copy = inspection["request"]["target"]["workspace"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        fs::read_to_string(Path::new(&parent_copy).join("src/lib.rs"))
            .unwrap()
            .contains("a - b")
    );
    // Restart at the exact immutable import wait, without another delegation/model call.
    server.close().await;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let (goal, turn) = urls(&server, &receipt);
    assert_eq!(
        wait(
            &get(&client, &format!("{goal}/inbox")).await,
            "child_import_approval"
        )["wait"],
        import["wait"]
    );
    answer(
        &client,
        &goal,
        &import,
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 10);
    assert!(
        get(&client, &turn[..turn.rfind("/turns/").unwrap()]).await["turns"][0]["response"]
            .is_null()
    );
    assert!(
        fs::read_to_string(Path::new(&parent_copy).join("src/lib.rs"))
            .unwrap()
            .contains("a + b /* planner input */")
    );
    approve_tool(&client, &goal).await; // Fresh combined tests, after import.
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 11);
    let final_changes = get(&client, &format!("{turn}/changes")).await;
    assert_eq!(final_changes["run_status"], "succeeded");
    assert_eq!(final_changes["completion"]["status"], "verified");
    assert_eq!(final_changes["changes"]["test_status"], "matching");
    assert!(
        final_changes["changes"]["files"][0]["patch"]
            .as_str()
            .unwrap()
            .contains("a + b /* planner input */")
    );
    let task = get(&client, &goal).await;
    assert_eq!(
        task["task"]["summary"]["visible_usage"]["admitted_model_calls"],
        11
    );
    let children = task["task"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|n| n["task_id"] == json!(child))
        .collect::<Vec<_>>();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0]["task_id"], json!(child));
    assert_eq!(fs::read_to_string(&source_file).unwrap(), original);
    server.close().await;
    model.abort();
}

#[tokio::test]
async fn harness_coder_cancellation_and_shared_limits_survive_restart_without_edits() {
    let mut token_allowance = 0;
    for mode in ["cancel", "call_limit", "token_limit"] {
        let root = tempfile::tempdir().unwrap();
        let source = project(root.path());
        let original = fs::read_to_string(Path::new(&source).join("src/lib.rs")).unwrap();
        let database = root.path().join("stopped-coder.db");
        let (endpoint, calls, model) = provider_with_usage(
            vec![
                json!({"kind":"delegate_task","worker":"coder","goal":"Repair addition"}),
                tool(
                    "replace_text",
                    json!({"path":"src/lib.rs","old":"a - b","new":"a + b"}),
                ),
            ],
            |_, _| {},
            if mode == "token_limit" {
                None
            } else {
                Some(json!({"prompt_tokens":10,"completion_tokens":10}))
            },
        )
        .await;
        let (engine, agents) = local_app(&database).await.unwrap();
        let agents = Arc::new(agents);
        let server = Server::start(agents.clone(), 2).await;
        let client = client();
        let mut initial = input(&endpoint, &source);
        if mode != "cancel" {
            // The first planner request is allowed; the child shares its allowance.
            initial["budget"] = if mode == "call_limit" {
                json!({"max_model_calls":1,"max_tokens":null})
            } else {
                // Missing provider usage retains the parent's conservative hold.
                // The remaining shared tokens cannot cover the child's request.
                json!({"max_model_calls":null,"max_tokens":token_allowance})
            };
        }
        let receipt = post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &initial,
            StatusCode::ACCEPTED,
        )
        .await;
        let (goal, _) = urls(&server, &receipt);
        settle(&engine).await;
        let task = get(&client, &goal).await;
        if mode == "cancel" {
            let parent_run = engine
                .status(serde_json::from_value(task["task"]["nodes"][0]["run_id"].clone()).unwrap())
                .await
                .unwrap();
            let reservation = parent_run
                .activities()
                .iter()
                .find(|a| a.spec.handler == act0r::domain::DefinitionId::new("agent.model", 1))
                .unwrap()
                .spec
                .usage
                .as_ref()
                .unwrap();
            // Measure the fixture's actual planner request; allow small path/config
            // differences while leaving too little for any child model request.
            token_allowance =
                reservation.input_tokens + u64::from(reservation.output_tokens) + 1024;
        }
        let coder = task["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["name"] == "coder")
            .unwrap();
        let child = task["task"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["actor_id"] == coder["agent_id"])
            .unwrap_or_else(|| panic!("{mode}: missing child: {task}"));
        let child_id: MessageId = serde_json::from_value(child["task_id"].clone()).unwrap();
        let run = engine
            .status(serde_json::from_value(child["run_id"].clone()).unwrap())
            .await
            .unwrap();
        let copy = run.record().input["input"]["workspace"]
            .as_str()
            .unwrap()
            .to_owned();
        let edit = if mode == "cancel" {
            let edit = wait(
                &get(&client, &format!("{goal}/inbox")).await,
                "tool_approval",
            );
            post(
                &client,
                &format!("{goal}/cancel"),
                &json!({}),
                StatusCode::OK,
            )
            .await;
            Some(edit)
        } else {
            assert_eq!(child["status"], "budget_exhausted");
            None
        };
        let expected_calls = if mode == "cancel" { 2 } else { 1 };
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
        server.close().await;
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&database).await.unwrap();
        let agents = Arc::new(agents);
        let server = Server::start(agents.clone(), 2).await;
        let (goal, _) = urls(&server, &receipt);
        settle(&engine).await;
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
        let task = get(&client, &goal).await;
        assert_eq!(task["task"]["summary"]["settled"], true);
        if let Some(edit) = edit {
            assert_eq!(
                get(&client, &format!("{goal}/controls")).await["cancellation"]["settled"],
                true
            );
            answer(
                &client,
                &goal,
                &edit,
                json!({"kind":"approval","approved":true}),
                StatusCode::NOT_FOUND,
            )
            .await;
        }
        assert!(
            agents
                .actors()
                .unwrap()
                .task(child_id)
                .await
                .unwrap()
                .status
                .is_terminal()
        );
        assert_eq!(
            fs::read_to_string(Path::new(&copy).join("src/lib.rs")).unwrap(),
            original
        );
        assert_eq!(
            fs::read_to_string(Path::new(&source).join("src/lib.rs")).unwrap(),
            original
        );
        server.close().await;
        model.abort();
    }
}

#[tokio::test]
async fn coding_turns_keep_edits_and_recover_reviewed_applications_across_restart() {
    let root = tempfile::tempdir().unwrap();
    let source = project(root.path());
    let source_file = Path::new(&source).join("src/lib.rs");
    let original = fs::read_to_string(&source_file).unwrap();
    let database = root.path().join("coding.db");
    let actions = vec![
        tool("read_file", json!({"path":"src/lib.rs"})),
        tool(
            "replace_text",
            json!({"path":"src/lib.rs","old":"a - b","new":"a + b"}),
        ),
        tool("run_tests", json!({})),
        json!({"kind":"finish","summary":"Addition repaired and tested"}),
        json!({"kind":"delegate_task","worker":"helper","goal":"Read src/lib.rs and confirm the current addition expression"}),
        tool("read_file", json!({"path":"src/lib.rs"})),
        json!({"kind":"finish","summary":"Current workspace uses a + b"}),
        tool("read_file", json!({"path":"src/lib.rs"})),
        tool(
            "replace_text",
            json!({"path":"src/lib.rs","old":"pub fn add","new":"// Addition\npub fn add"}),
        ),
        tool("run_tests", json!({})),
        json!({"kind":"finish","summary":"Comment added and tested"}),
    ];
    let (endpoint, calls, model) = provider(actions).await;
    let (engine, agents) = local_app(&database).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let initial = input(&endpoint, &source);
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &initial,
        StatusCode::ACCEPTED,
    )
    .await;
    let (goal, _) = urls(&server, &receipt);
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(fs::read_to_string(&source_file).unwrap(), original);
    let edit_wait = wait(
        &get(&client, &format!("{goal}/inbox")).await,
        "tool_approval",
    );
    let session = receipt["session_id"].as_str().unwrap();
    let follow =
        json!({"message_id":MessageId::new(),"prompt":"Add a comment to that repaired function"});
    let second = post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/turns", server.url),
        &follow,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    // Both ordered turns and the edit approval remain durable.
    server.close().await;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    assert_eq!(
        receipt,
        post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &initial,
            StatusCode::ACCEPTED
        )
        .await
    );
    let (goal, base) = urls(&server, &receipt);
    let (_, second_base) = urls(&server, &second);
    assert_eq!(
        wait(
            &get(&client, &format!("{goal}/inbox")).await,
            "tool_approval"
        )["wait"]["id"],
        edit_wait["wait"]["id"]
    );
    approve_tool(&client, &goal).await;
    settle(&engine).await;
    approve_tool(&client, &goal).await;
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 9); // The queued turn sees the repaired copy.
    let changes = get(&client, &format!("{base}/changes")).await;
    assert_eq!(changes["completion"]["status"], "verified");
    assert_eq!(changes["changes"]["test_status"], "matching");
    assert!(
        changes["changes"]["files"][0]["patch"]
            .as_str()
            .unwrap()
            .contains("a + b")
    );
    assert_eq!(fs::read_to_string(&source_file).unwrap(), original);
    let (request, application) = preview(&client, &engine, &base).await;
    assert_eq!(application["status"], "waiting");
    let app_id = application["run_id"].as_str().unwrap();
    // IDs cannot be rebound to another turn or inspected outside their session.
    post(
        &client,
        &format!("{second_base}/applications"),
        &request,
        StatusCode::BAD_REQUEST,
    )
    .await;
    value(
        client
            .get(format!(
                "{}/v1/harness/sessions/{}/turns/{}/changes",
                server.url,
                SessionId::new(),
                receipt["task_id"].as_str().unwrap()
            ))
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    value(
        client
            .get(format!("{second_base}/applications/{app_id}"))
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;
    let waiting_id = application["wait"]["id"].clone();
    server.close().await;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let (goal, base) = urls(&server, &receipt);
    let (second_goal, second_base) = urls(&server, &second);
    let application = get(&client, &format!("{base}/applications/{app_id}")).await;
    assert_eq!(application["wait"]["id"], waiting_id);
    assert_eq!(
        post(
            &client,
            &format!("{base}/applications"),
            &request,
            StatusCode::ACCEPTED
        )
        .await["run_id"],
        app_id
    );
    let (approval_url, approval) = decide(&client, &base, &application, true).await;
    settle(&engine).await;
    post(&client, &approval_url, &approval, StatusCode::OK).await;
    let mut conflict = approval.clone();
    conflict["approved"] = json!(false);
    post(&client, &approval_url, &conflict, StatusCode::CONFLICT).await;
    assert_eq!(
        get(&client, &format!("{base}/applications/{app_id}")).await["report"]["status"],
        "applied"
    );
    let repaired = original.replace("a - b", "a + b");
    assert_eq!(fs::read_to_string(&source_file).unwrap(), repaired);
    approve_tool(&client, &second_goal).await;
    settle(&engine).await;
    approve_tool(&client, &second_goal).await;
    settle(&engine).await;
    assert_eq!(calls.load(Ordering::SeqCst), 11);
    assert_eq!(
        get(&client, &format!("{second_base}/changes")).await["completion"]["status"],
        "verified"
    );
    post(
        &client,
        &format!("{second_base}/applications"),
        &request,
        StatusCode::CONFLICT,
    )
    .await;
    // A changed target blocks preview before any project-file write.
    fs::write(&source_file, "External edit").unwrap();
    let (_, conflicted) = preview(&client, &engine, &second_base).await;
    assert_eq!(conflicted["report"]["status"], "conflicted");
    assert!(conflicted["wait"].is_null());
    assert_eq!(fs::read_to_string(&source_file).unwrap(), "External edit");
    fs::write(&source_file, &repaired).unwrap();
    let (_, rejected) = preview(&client, &engine, &second_base).await;
    let (url, decision) = decide(&client, &second_base, &rejected, false).await;
    settle(&engine).await;
    post(&client, &url, &decision, StatusCode::OK).await;
    assert_eq!(fs::read_to_string(&source_file).unwrap(), repaired);
    // Drift after approval is checked again by the write activity.
    let (_, approved) = preview(&client, &engine, &second_base).await;
    decide(&client, &second_base, &approved, true).await;
    fs::write(&source_file, "Later external edit").unwrap();
    settle(&engine).await;
    assert_eq!(
        get(
            &client,
            &format!(
                "{second_base}/applications/{}",
                approved["run_id"].as_str().unwrap()
            )
        )
        .await["report"]["status"],
        "conflicted"
    );
    assert_eq!(
        fs::read_to_string(&source_file).unwrap(),
        "Later external edit"
    );
    fs::write(&source_file, &repaired).unwrap();
    let (_, approved) = preview(&client, &engine, &second_base).await;
    decide(&client, &second_base, &approved, true).await;
    settle(&engine).await;
    assert!(
        fs::read_to_string(&source_file)
            .unwrap()
            .starts_with("// Addition\n")
    );
    assert_eq!(
        get(&client, &format!("{base}/applications")).await["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        get(&client, &format!("{second_base}/applications")).await["items"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        get(&client, &goal).await["task"]["summary"]["settled"],
        true
    );
    assert_eq!(
        fs::read_to_string(Path::new(&source).join("README.md")).unwrap(),
        "Original project"
    );
    server.close().await;
    model.abort();
}

#[tokio::test]
async fn failed_tests_save_evidence_and_refuse_application() {
    let root = tempfile::tempdir().unwrap();
    let source = project(root.path());
    let (endpoint, _, model) = provider(vec![
        tool("run_tests", json!({})),
        json!({"kind":"finish","summary":"Incorrect success"}),
        json!({"kind":"finish_incomplete","summary":"The tests still fail"}),
    ])
    .await;
    let (engine, agents) = local_app(root.path().join("failed.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &input(&endpoint, &source),
        StatusCode::ACCEPTED,
    )
    .await;
    let (goal, base) = urls(&server, &receipt);
    settle(&engine).await;
    approve_tool(&client, &goal).await;
    settle(&engine).await;
    let changes = get(&client, &format!("{base}/changes")).await;
    assert_eq!(changes["run_status"], "failed");
    assert_eq!(changes["changes"]["tests"]["result"]["ok"], false);
    assert_eq!(changes["completion"]["status"], "incomplete");
    post(
        &client,
        &format!("{base}/applications"),
        &json!({"application_id":RunId::new()}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(
        get(&client, &format!("{base}/applications")).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    server.close().await;
    model.abort();
}

#[tokio::test]
async fn coding_configuration_and_application_bodies_are_strict() {
    let root = tempfile::tempdir().unwrap();
    let source = project(root.path());
    let (_, agents) = local_app(root.path().join("validation.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let base = input("http://127.0.0.1:1234/v1/chat/completions", &source);
    for coding in [
        json!({"writable_paths":[],"test_command":{"argv":["cargo","test"]}}),
        json!({"writable_paths":["../outside"],"test_command":{"argv":["cargo","test"]}}),
        json!({"writable_paths":["src/"],"test_command":{"argv":[],"timeout_ms":1}}),
    ] {
        let mut invalid = base.clone();
        invalid["coding"] = coding;
        post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &invalid,
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let mut invalid = base.clone();
    invalid["workspace"] = Value::Null;
    post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &invalid,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(
        get(&client, &format!("{}/v1/harness/sessions", server.url)).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &base,
        StatusCode::ACCEPTED,
    )
    .await;
    let (_, turn) = urls(&server, &receipt);
    let mut changed = base.clone();
    changed["coding"]["writable_paths"] = json!(["README.md"]);
    post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &changed,
        StatusCode::BAD_REQUEST,
    )
    .await;
    post(
        &client,
        &format!("{turn}/applications"),
        &json!({"application_id":RunId::new(),"target":source}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    post(
        &client,
        &format!("{turn}/applications/{}/responses", RunId::new()),
        &json!({"wait_id":WaitId::new(),"approved":true,"extra":1}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    let mut readonly = base;
    readonly["message_id"] = json!(MessageId::new());
    readonly.as_object_mut().unwrap().remove("coding");
    let readonly = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &readonly,
        StatusCode::ACCEPTED,
    )
    .await;
    let (_, turn) = urls(&server, &readonly);
    value(
        client.get(format!("{turn}/changes")).send().await.unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    post(
        &client,
        &format!("{turn}/applications"),
        &json!({"application_id":RunId::new()}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    server.close().await;
}
