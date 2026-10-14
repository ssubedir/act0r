use super::*;
use act0r::domain::{RunId, TeamId, project_instructions::ProjectInstructions};
use std::sync::Mutex;

struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl Model {
    async fn start() -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let router=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
                let prompt:Value=serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                seen.lock().unwrap().push(prompt.clone());
                let action=if prompt["goal"]=="Edit project conventions" {
                    match prompt["turn"].as_u64().unwrap() {
                        0=>json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"AGENTS.md","old":"Use the original conventions.","new":"Use the updated conventions."}}),
                        1=>json!({"kind":"delegate_task","worker":"helper","goal":"Read your saved conventions"}),
                        _=>json!({"kind":"finish_incomplete","summary":"Guidance changed and inspected; no tests requested in this fixture"}),
                    }
                } else if prompt["goal"]=="Inspect conventions" {
                    match prompt["turn"].as_u64().unwrap() {
                        0=>json!({"kind":"delegate_task","worker":"helper","goal":"Read your saved conventions"}),
                        1=>json!({"kind":"ask_human","question":"Continue with these saved conventions?"}),
                        _=>json!({"kind":"finish","summary":"Used the saved root instructions"}),
                    }
                } else {json!({"kind":"finish","summary":"Finished using this task's snapshot"})};
                async move {axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
            }));
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }
}

#[tokio::test]
async fn edited_guidance_keeps_the_planner_snapshot_but_helpers_capture_the_prepared_handoff() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("AGENTS.md"), "Use the original conventions.").unwrap();
    let (engine, agents) = local_app(root.path().join("handoff.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let mut input = summary::input(&model.endpoint);
    input["workspace"] = json!(fs::canonicalize(&source).unwrap().to_string_lossy());
    input["prompt"] = json!("Edit project conventions");
    input["coding"] = json!({"writable_paths":["AGENTS.md"],"test_command":{"argv":["host-pinned-tests"],"timeout_ms":60000}});
    let receipt = post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let inbox = get(&client(), &format!("{}/inbox", goal(&server, &receipt))).await;
    let wait = inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["actionable"] == true)
        .unwrap();
    assert_eq!(wait["wait"]["key"], "native_tool_approval");
    post(
        &client(),
        &format!(
            "{}/waits/{}/responses",
            goal(&server, &receipt),
            wait["wait"]["id"].as_str().unwrap()
        ),
        &json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    let view = get(&client(), &goal(&server, &receipt)).await;
    assert_eq!(snapshot(&view)["text"], "Use the original conventions.");
    let helper = view["task"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["depth"] == 1)
        .unwrap();
    assert_eq!(
        helper["project_instructions"]["text"],
        "Use the updated conventions."
    );
    let requests = model.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests[2]["project_instructions"]["text"],
        "Use the updated conventions."
    );
    assert_eq!(
        requests[3]["project_instructions"]["text"],
        "Use the original conventions."
    );
    assert_eq!(requests[3]["writable_paths"], json!(["AGENTS.md"]));
    assert_eq!(requests[3]["test_command"], input["coding"]["test_command"]);
    assert_eq!(
        fs::read_to_string(source.join("AGENTS.md")).unwrap(),
        "Use the original conventions."
    );
    server.close().await;
}
impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn goal(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
fn snapshot(view: &Value) -> Value {
    view["task"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["task_id"] == view["task"]["requested_task_id"])
        .unwrap()["project_instructions"]
        .clone()
}
async fn start(server: &Server, model: &Model, source: &Path, prompt: &str) -> Value {
    let mut input = summary::input(&model.endpoint);
    input["prompt"] = json!(prompt);
    input["workspace"] = json!(fs::canonicalize(source).unwrap().to_string_lossy());
    post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await
}

#[tokio::test]
async fn instructions_pin_at_launch_survive_restart_and_refresh_only_for_new_turns() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("instructions.db");
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    let text =
        "# Project conventions\nUse DDD and ports/adapters 🙂. <script>literal text</script>";
    fs::write(source.join("AGENTS.md"), text).unwrap();
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let receipt = start(&server, &model, &source, "Inspect conventions").await;
    assert!(snapshot(&get(&client(), &goal(&server, &receipt)).await).is_null());
    settle(&engine).await;
    let view = get(&client(), &goal(&server, &receipt)).await;
    let saved = snapshot(&view);
    assert_eq!(
        saved,
        json!(ProjectInstructions::loaded(text.into()).unwrap())
    );
    let nodes = view["task"]["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
    assert!(
        nodes
            .iter()
            .all(|node| node["project_instructions"] == saved)
    );
    assert_eq!(model.requests.lock().unwrap().len(), 3);
    let team = agents
        .teams()
        .unwrap()
        .get(serde_json::from_value::<TeamId>(receipt["team_id"].clone()).unwrap())
        .await
        .unwrap();
    let copy = Path::new(&team.members[0].workspace.as_ref().unwrap().path);
    let changed = "Use the updated project conventions for future turns.";
    fs::write(copy.join("AGENTS.md"), changed).unwrap();
    fs::write(
        source.join("AGENTS.md"),
        "The original folder is not the managed copy",
    )
    .unwrap();
    let session = format!(
        "{}/v1/harness/sessions/{}",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    let later = post(
        &client(),
        &format!("{session}/turns"),
        &json!({"message_id":MessageId::new(),"prompt":"Follow up"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert!(snapshot(&get(&client(), &goal(&server, &later)).await).is_null());
    assert_eq!(
        snapshot(&get(&client(), &goal(&server, &receipt)).await),
        saved
    );
    server.close().await;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    assert_eq!(
        snapshot(&get(&client(), &goal(&server, &receipt)).await),
        saved
    );
    assert_eq!(model.requests.lock().unwrap().len(), 3);
    let inbox = get(&client(), &format!("{}/inbox", goal(&server, &receipt))).await;
    let wait = inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["actionable"] == true)
        .unwrap();
    post(
        &client(),
        &format!(
            "{}/waits/{}/responses",
            goal(&server, &receipt),
            wait["wait"]["id"].as_str().unwrap()
        ),
        &json!({"kind":"answer","answer":"Continue"}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        snapshot(&get(&client(), &goal(&server, &receipt)).await),
        saved
    );
    let updated = snapshot(&get(&client(), &goal(&server, &later)).await);
    assert_eq!(
        updated,
        json!(ProjectInstructions::loaded(changed.into()).unwrap())
    );
    let prompts = model.requests.lock().unwrap().clone();
    assert!(
        prompts[..4]
            .iter()
            .all(|prompt| prompt["project_instructions"] == saved)
    );
    assert_eq!(prompts[4]["project_instructions"], updated);
    // Duplicate HTTP admission must not recapture or replace the already launched turn.
    fs::remove_file(copy.join("AGENTS.md")).unwrap();
    let replay = post(
        &client(),
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            receipt["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":later["task_id"],"prompt":"Follow up"}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(replay, later);
    assert_eq!(
        snapshot(&get(&client(), &goal(&server, &later)).await),
        updated
    );
    let no_file = post(
        &client(),
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            receipt["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"No instructions now"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        snapshot(&get(&client(), &goal(&server, &no_file)).await),
        json!({"status":"missing"})
    );
    // The saved native launch input is also the inspection source, including wrappers.
    let turn_view = get(&client(), &goal(&server, &receipt)).await;
    let run_id: RunId =
        serde_json::from_value(turn_view["task"]["nodes"][0]["run_id"].clone()).unwrap();
    assert_eq!(
        act0r::domain::project_instructions::from_run(
            engine.status(run_id).await.unwrap().record()
        )
        .unwrap(),
        Some(serde_json::from_value(saved).unwrap())
    );
    server.close().await;
}

#[tokio::test]
async fn refused_guidance_is_saved_without_partial_model_text() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("AGENTS.md"), "x".repeat(16385)).unwrap();
    let (engine, agents) = local_app(root.path().join("refused.db")).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let receipt = start(&server, &model, &source, "Oversize instructions").await;
    settle(&engine).await;
    assert_eq!(
        snapshot(&get(&client(), &goal(&server, &receipt)).await),
        json!({"status":"refused","reason":"too_large"})
    );
    assert_eq!(
        model.requests.lock().unwrap()[0]["project_instructions"],
        json!({"status":"refused","reason":"too_large"})
    );
    server.close().await;
}
