use super::*;
use act0r::domain::{HistoryEvent, RunId};
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) async fn provider(actions: Vec<Value>) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let (endpoint, count, handle, _) = recording_provider(actions).await;
    (endpoint, count, handle)
}
async fn recording_provider(
    actions: Vec<Value>,
) -> (
    String,
    Arc<AtomicUsize>,
    JoinHandle<()>,
    Arc<std::sync::Mutex<Vec<Value>>>,
) {
    let count = Arc::new(AtomicUsize::new(0));
    let counted = count.clone();
    let requests = Arc::new(std::sync::Mutex::new(vec![]));
    let recorded = requests.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let handle = tokio::spawn(async move {
        axum::serve(listener,axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
            let index=counted.fetch_add(1,Ordering::SeqCst);
            recorded.lock().unwrap().push(body.clone());
            let action=actions.get(index).expect("unexpected model call").clone();
            if index==0 {
                let raw=body.to_string();
                assert!(raw.contains("propose_project_setup"));
                assert!(!raw.contains("\"name\":\"run_tests\""));
                assert!(!raw.contains("\"name\":\"replace_text\""));
                assert!(!raw.contains("\"name\":\"delegate_task\""));
                assert!(raw.contains("host_os"));
            }
            let message=if body["tools"].is_array() {
                let (name,args)=if action["kind"]=="call_tool" {(action["tool"].as_str().unwrap().to_owned(),action["arguments"].clone())} else {
                    let mut args=action.as_object().unwrap().clone();let name=args.remove("kind").unwrap().as_str().unwrap().to_owned();(name,json!(args))
                };
                json!({"tool_calls":[{"id":format!("setup-{index}"),"type":"function","function":{"name":name,"arguments":args.to_string()}}]})
            } else {json!({"content":json!({"action":native_wire_action(action,&body)}).to_string()})};
            async move {axum::Json(json!({"id":"project-setup-fixture","choices":[{"message":message,"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
        }))).await.unwrap();
    });
    (endpoint, count, handle, requests)
}
pub(super) fn proposal() -> Value {
    json!({"summary":"README documents the custom verifier; only logic.txt needs editing", "writable_paths":["logic.txt"],"test_command":{"argv":["custom-check","--contracts"],"timeout_ms":60000},"commands":[{"name":"lint","argv":["custom-lint"],"timeout_ms":30000}]})
}
pub(super) fn start_body(endpoint: &str, source: &Path, native: bool) -> Value {
    let mut value = json!({"message_id":MessageId::new(),"prompt":"Repair the custom project and verify the result","workspace":source,"discover_project":true,"budget":{"max_model_calls":12,"max_tokens":1000000},"target":{"provider":"openai","model":"setup-fixture","endpoint":endpoint,"protocol":"chat_completions"}});
    if native {
        value["target"]["action_format"] = json!("function_calling");
    }
    value
}
#[test]
#[ignore = "launched as a real approved verification process by the parent test"]
fn custom_verifier_child() {
    assert_eq!(fs::read_to_string("logic.txt").unwrap(), "fixed\n");
}
#[tokio::test]
async fn general_project_setup_executes_reviewed_custom_verification_and_returns_a_patch() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("custom");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("logic.txt"), "broken\n").unwrap();
    fs::write(
        source.join("README.md"),
        "Repair logic.txt; verify using the supplied custom contract checker.\n",
    )
    .unwrap();
    let mut proposed = proposal();
    proposed["test_command"]["argv"] = json!([
        std::env::current_exe().unwrap(),
        "--ignored",
        "--exact",
        "project_setup::custom_verifier_child"
    ]);
    let actions = vec![
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        json!({"kind":"propose_project_setup","proposal":proposed}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"logic.txt","old":"broken\n","new":"fixed\n"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Repaired logic.txt and passed the custom contract verifier"}),
    ];
    let (endpoint, count, handle) = provider(actions).await;
    let (engine, agents) = local_app(root.path().join("execute.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &start_body(&endpoint, &source, true),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let url = format!(
        "{}/v1/harness/sessions/{}/turns/{}/project-setup",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    let view = get(&client, &url).await;
    let saved: act0r::domain::project_setup::ProjectSetupProposal =
        serde_json::from_value(view["proposal"].clone()).unwrap();
    let coding = saved.coding().unwrap();
    let accepted = post(
        &client,
        &url,
        &json!({"coding":coding}),
        StatusCode::ACCEPTED,
    )
    .await;
    let goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        accepted["team_id"].as_str().unwrap(),
        accepted["task_id"].as_str().unwrap()
    );
    for _ in 0..2 {
        settle(&engine).await;
        let approval = wait(
            &get(&client, &format!("{goal}/inbox")).await,
            "tool_approval",
        );
        answer(
            &client,
            &goal,
            &approval,
            json!({"kind":"approval","approved":true}),
            StatusCode::OK,
        )
        .await;
    }
    settle(&engine).await;
    let changes = get(
        &client,
        &format!(
            "{}/v1/harness/sessions/{}/turns/{}/changes",
            server.url,
            accepted["session_id"].as_str().unwrap(),
            accepted["task_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(changes["completion"]["status"], "verified");
    assert_eq!(changes["run_status"], "succeeded");
    assert_eq!(changes["changes"]["files"][0]["path"], "logic.txt");
    assert_eq!(
        fs::read_to_string(source.join("logic.txt")).unwrap(),
        "broken\n"
    );
    assert_eq!(count.load(Ordering::SeqCst), 5);
    server.close().await;
    handle.abort();
}
#[tokio::test]
async fn general_project_setup_recovers_questions_and_reviews_once_with_original_pins() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("custom");
    fs::create_dir(&source).unwrap();
    fs::write(
        source.join("README.md"),
        "Edit logic.txt. Verify with the custom checker; ask for its executable path.\n",
    )
    .unwrap();
    fs::write(source.join("logic.txt"), "broken\n").unwrap();
    let actions = vec![
        json!({"kind":"propose_project_setup","proposal":proposal()}),
        json!({"kind":"call_tool","tool":"list_files","arguments":{}}),
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        json!({"kind":"ask_human","question":"Where is the custom verifier installed?"}),
        json!({"kind":"propose_project_setup","proposal":proposal()}),
    ];
    let (endpoint, count, provider) = provider(actions).await;
    let db = root.path().join("setup.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let client = client();
    let request = start_body(&endpoint, &source, true);
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    let team_base = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    let setup_path = format!(
        "/v1/harness/sessions/{}/turns/{}/project-setup",
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    let denied = json!({"coding":{"writable_paths":["logic.txt"],"test_command":{"argv":["check"],"timeout_ms":60000}}});
    post(
        &client,
        &format!("{}{setup_path}", server.url),
        &denied,
        StatusCode::BAD_REQUEST,
    )
    .await;
    settle(&engine).await;
    assert_eq!(count.load(Ordering::SeqCst), 4);
    let question = wait(
        &get(&client, &format!("{team_base}/inbox")).await,
        "question",
    );
    assert!(
        !get(&client, &format!("{}{setup_path}", server.url)).await["can_accept"]
            .as_bool()
            .unwrap()
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let team_base = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    assert_eq!(
        wait(
            &get(&client, &format!("{team_base}/inbox")).await,
            "question"
        )["wait"]["id"],
        question["wait"]["id"]
    );
    answer(&client,&team_base,&question,json!({"kind":"answer","answer":"Use the fixed custom checker; no dependencies need downloading"}),StatusCode::OK).await;
    settle(&engine).await;
    assert_eq!(count.load(Ordering::SeqCst), 5);
    let url = format!("{}{setup_path}", server.url);
    let view = get(&client, &url).await;
    assert_eq!(view["proposal"], proposal());
    assert_eq!(
        fs::read_to_string(source.join("logic.txt")).unwrap(),
        "broken\n"
    );
    let mut coding = json!({"writable_paths":["logic.txt"],"test_command":{"argv":["reviewed-check","--exact"],"timeout_ms":45000},"coder_count":2,"project_commands":{"lint":{"argv":["custom-lint"],"timeout_ms":30000}}});
    let accepted = json!({"coding":coding});
    let (a, b) = tokio::join!(
        post(&client, &url, &accepted, StatusCode::ACCEPTED),
        post(&client, &url, &accepted, StatusCode::ACCEPTED)
    );
    assert_eq!(a, b);
    let saved = get(
        &client,
        &format!(
            "{}/v1/harness/sessions/{}",
            server.url,
            a["session_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(saved["coding"], coding);
    assert_eq!(saved["turns"][0]["prompt"], request["prompt"]);
    assert_eq!(saved["discover_project"], false);
    let team: TeamId = a["team_id"].as_str().unwrap().parse().unwrap();
    let team = agents.teams().unwrap().get(team).await.unwrap();
    assert_eq!(team.spec.planner.config["target"], request["target"]);
    assert_eq!(
        team.spec.planner.config["goal_budget"]["max_model_calls"],
        7
    );
    assert_eq!(
        team.spec.planner.config["goal_budget"]["max_tokens"],
        999900
    );
    coding["test_command"]["argv"] = json!(["changed-check"]);
    post(
        &client,
        &url,
        &json!({"coding":coding}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(
        get(&client, &format!("{}/v1/harness/sessions", server.url)).await["items"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(count.load(Ordering::SeqCst), 5);
    server.close().await;
    drop(engine);
    drop(agents);
    let (_, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    assert_eq!(
        get(&client, &format!("{}{setup_path}", server.url)).await["proposal"],
        proposal()
    );
    assert_eq!(
        post(
            &client,
            &format!("{}{setup_path}", server.url),
            &accepted,
            StatusCode::ACCEPTED
        )
        .await,
        a
    );
    assert_eq!(count.load(Ordering::SeqCst), 5);
    server.close().await;
    provider.abort();
}
#[tokio::test]
async fn general_project_setup_accepts_non_rust_layouts_and_refuses_invalid_authority() {
    for (manifest, content) in [
        ("package.json", "{\"scripts\":{\"test\":\"node --test\"}}"),
        ("pyproject.toml", "[project]\nname='python_fixture'"),
        ("Makefile", "test:\n\tcustom-check\n"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("project");
        fs::create_dir(&source).unwrap();
        fs::write(source.join(manifest), content).unwrap();
        let (endpoint, count, handle) = provider(vec![
            json!({"kind":"call_tool","tool":"read_file","arguments":{"path":manifest}}),
            json!({"kind":"propose_project_setup","proposal":proposal()}),
        ])
        .await;
        let (engine, agents) = local_app(root.path().join("layout.db")).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let client = client();
        let mut request = start_body(&endpoint, &source, false);
        let start = format!("{}/v1/harness/sessions", server.url);
        request["coding"] = json!({"writable_paths":["logic.txt"],"test_command":{"argv":["check"],"timeout_ms":60000}});
        post(&client, &start, &request, StatusCode::BAD_REQUEST).await;
        request.as_object_mut().unwrap().remove("coding");
        let receipt = post(&client, &start, &request, StatusCode::ACCEPTED).await;
        settle(&engine).await;
        let url = format!(
            "{}/v1/harness/sessions/{}/turns/{}/project-setup",
            server.url,
            receipt["session_id"].as_str().unwrap(),
            receipt["task_id"].as_str().unwrap()
        );
        assert_eq!(get(&client, &url).await["proposal"], proposal());
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(fs::read_to_string(source.join(manifest)).unwrap(), content);
        post(&client,&url,&json!({"coding":{"writable_paths":["../escape"],"test_command":{"argv":["check"],"timeout_ms":60000}}}),StatusCode::BAD_REQUEST).await;
        post(
            &client,
            &url,
            &json!({"coding":{},"target":request["target"]}),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await;
        let wrong = url.replace(
            receipt["session_id"].as_str().unwrap(),
            &act0r::domain::SessionId::new().to_string(),
        );
        assert_eq!(
            client.get(wrong).send().await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        server.close().await;
        handle.abort();
    }
}

#[tokio::test]
async fn discovery_handoff_recovers_pruned_answers_and_pins_worker_context_over_restart() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "Use a custom verifier.\n").unwrap();
    fs::write(source.join("logic.txt"), "broken\n").unwrap();
    let reply = "Use checker-Ω --contract; keep the public interface unchanged.";
    let mut actions = vec![
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        json!({"kind":"ask_human","question":"Which verifier and compatibility constraints apply?"}),
    ];
    // Exceed the normal 16-observation window, evicting the human answer.
    for _ in 0..17 {
        actions
            .push(json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"logic.txt"}}));
    }
    actions.extend([
        json!({"kind":"propose_project_setup","proposal":proposal()}),
        json!({"kind":"delegate_task","worker":"coder","goal":"Inspect logic.txt using the saved discovery answers"}),
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"logic.txt"}}),
        json!({"kind":"finish_incomplete","summary":"Fixture stops before editing"}),
        json!({"kind":"finish_incomplete","summary":"Fixture completed context checks"}),
    ]);
    let (endpoint, count, handle, requests) = recording_provider(actions).await;
    let db = root.path().join("context.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let client = client();
    let mut request = start_body(&endpoint, &source, true);
    request["budget"] = json!({"max_model_calls":null,"max_tokens":null});
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let goal = format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    let question = wait(&get(&client, &format!("{goal}/inbox")).await, "question");
    answer(
        &client,
        &goal,
        &question,
        json!({"kind":"answer","answer":reply}),
        StatusCode::OK,
    )
    .await;
    settle(&engine).await;
    assert_eq!(count.load(Ordering::SeqCst), 20);
    let setup_path = format!(
        "/v1/harness/sessions/{}/turns/{}/project-setup",
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    let view = get(&client, &format!("{}{setup_path}", server.url)).await;
    let snapshot = view["discovery_context"].clone();
    assert_eq!(
        snapshot["evidence"]["clarifications"][0]["answer"]["text"],
        reply
    );
    assert_eq!(
        snapshot["evidence"]["clarifications"][0]["answer"]["truncated"],
        false
    );
    assert_eq!(
        snapshot["evidence"]["clarifications"][0]["wait_id"],
        question["wait"]["id"]
    );
    assert_eq!(snapshot["evidence"]["run_id"], view["run_id"]);
    assert_eq!(snapshot["evidence"]["session_id"], receipt["session_id"]);
    assert_eq!(
        snapshot["evidence"]["setup_rationale"]["text"],
        proposal()["summary"]
    );
    assert!(snapshot["evidence"]["omitted_findings"].as_u64().unwrap() > 0);
    let source_run = engine
        .status(view["run_id"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap();
    assert!(
        !source_run.record().state["state"]["context"]["observations"]
            .to_string()
            .contains(reply)
    );
    assert_eq!(
        source_run.record().input_receipts[0].payload["answer"],
        reply
    );
    let mut history = engine.history(source_run.record().id).await.unwrap();
    let capture = |events: &[HistoryEvent]| {
        act0r::domain::discovery_context::DiscoveryContext::capture(
            receipt["session_id"].as_str().unwrap().parse().unwrap(),
            receipt["task_id"].as_str().unwrap().parse().unwrap(),
            &source_run,
            events,
            snapshot["evidence"]["workspace_digest"]
                .as_str()
                .unwrap()
                .into(),
        )
    };
    assert_eq!(json!(capture(&history).unwrap()), snapshot);
    history[0].run_id = RunId::new();
    assert!(capture(&history).is_err());
    history[0].run_id = source_run.record().id;
    let input = history
        .iter_mut()
        .find(|event| event.kind == "input_received")
        .unwrap();
    input.payload["payload"]["answer"] = json!("changed answer");
    assert!(capture(&history).is_err());
    // Restart before accepting. Preview must be deterministic from durable evidence.
    server.close().await;
    drop(engine);
    drop(agents);
    let (engine, agents) = local_app(&db).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), 2).await;
    let url = format!("{}{setup_path}", server.url);
    assert_eq!(get(&client, &url).await["discovery_context"], snapshot);
    let reviewed = json!({"coding":{"writable_paths":["logic.txt"],"test_command":{"argv":["reviewed-check"],"timeout_ms":45000},"coder_count":2}});
    let accepted = post(&client, &url, &reviewed, StatusCode::ACCEPTED).await;
    let team_id = accepted["team_id"].as_str().unwrap().parse().unwrap();
    let team = agents.teams().unwrap().get(team_id).await.unwrap();
    assert_eq!(team.spec.planner.config["discovery_context"], snapshot);
    for worker in &team.spec.workers {
        assert_eq!(worker.config["discovery_context"], snapshot);
    }
    // Proposed verifier remains evidence; only reviewed settings grant commands.
    assert_eq!(
        team.spec.planner.config["test_command"],
        reviewed["coding"]["test_command"]
    );
    assert_eq!(team.spec.planner.config["goal_budget"], request["budget"]);
    assert_eq!(count.load(Ordering::SeqCst), 20);
    server.close().await;
    drop(engine);
    drop(agents);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let url = format!("{}{setup_path}", server.url);
    assert_eq!(
        post(&client, &url, &reviewed, StatusCode::ACCEPTED).await,
        accepted
    );
    assert_eq!(get(&client, &url).await["discovery_context"], snapshot);
    settle(&engine).await;
    assert_eq!(count.load(Ordering::SeqCst), 24);
    // Every coordinator/worker request uses the same exact snapshot, including
    // the worker's second turn after its own native context has changed.
    {
        let recorded = requests.lock().unwrap();
        let last_discovery: Value =
            serde_json::from_str(recorded[19]["messages"][1]["content"].as_str().unwrap()).unwrap();
        assert!(
            last_discovery["context"]["omitted_observations"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(!last_discovery.to_string().contains(reply));
        for body in &recorded[20..] {
            let prompt: Value =
                serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            assert_eq!(prompt["discovery_context"], snapshot);
            assert!(
                body["messages"][0]["content"]
                    .as_str()
                    .unwrap()
                    .contains("grants no tools or permissions")
            );
        }
    }
    assert_eq!(
        fs::read_to_string(source.join("logic.txt")).unwrap(),
        "broken\n"
    );
    server.close().await;
    handle.abort();
}
