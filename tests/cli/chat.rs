//! Real CLI processes use a loopback provider; no hosted credentials or model calls.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[path = "chat/reconciliation.rs"]
mod reconciliation;
#[path = "chat/terminal.rs"]
mod terminal;

struct Running {
    child: Child,
    events: tokio::sync::mpsc::Receiver<Value>,
}
impl Running {
    fn send(&mut self, text: &str) {
        writeln!(self.child.stdin.as_mut().unwrap(), "{text}").unwrap();
    }
    fn start(db: &Path, args: &[&str], keep_input: bool) -> Self {
        Self::start_at(Some(db), args, keep_input, None)
    }
    fn start_at(
        db: Option<&Path>,
        args: &[&str],
        keep_input: bool,
        directory: Option<&Path>,
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_act0r"));
        if let Some(db) = db {
            command.arg("--db").arg(db);
        }
        if let Some(directory) = directory {
            command.current_dir(directory);
        }
        command
            .arg("--json")
            .arg("chat")
            .args(args)
            .stdin(if keep_input {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        for (name, _) in std::env::vars_os() {
            if name
                .to_string_lossy()
                .to_ascii_uppercase()
                .ends_with("_API_KEY")
            {
                command.env_remove(name);
            }
        }
        command
            .env_remove("ACT0R_MCP_CONFIG")
            .env_remove("ACT0R_MCP_FILESYSTEM_CONFIG");
        let mut child = command.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, events) = tokio::sync::mpsc::channel(64);
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.unwrap();
                let value: Value = serde_json::from_str(&line)
                    .unwrap_or_else(|_| panic!("invalid JSON event: {line}"));
                if sender.blocking_send(value).is_err() {
                    break;
                }
            }
        });
        Self { child, events }
    }
    async fn event(&mut self, event: &str) -> Value {
        self.event_where(event, |_| true).await
    }
    async fn event_where(&mut self, event: &str, matches: impl Fn(&Value) -> bool) -> Value {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let value = self
                    .events
                    .recv()
                    .await
                    .expect("CLI exited before the expected event");
                assert!(
                    value["event"] != "input_error" || event == "input_error",
                    "unexpected CLI input error: {value}"
                );
                if value["event"] == event && matches(&value["data"]) {
                    return value["data"].clone();
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("CLI did not emit {event}"))
    }
    async fn finish(mut self) -> Vec<Value> {
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            let mut events = vec![];
            while let Some(event) = self.events.recv().await {
                events.push(event);
            }
            events
        })
        .await
        .expect("CLI did not settle after stdin closed");
        assert!(self.child.wait().unwrap().success());
        result
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
struct Provider {
    endpoint: String,
    count: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Provider {
    async fn new(actions: Vec<Value>) -> Self {
        Self::with_first_gate(actions, None).await
    }
    async fn with_first_gate(actions: Vec<Value>, gate: Option<Arc<tokio::sync::Notify>>) -> Self {
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let requests = Arc::new(Mutex::new(vec![]));
        let recorded = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let index = calls.fetch_add(1, Ordering::SeqCst);
            let gate = gate.clone().filter(|_| index == 0);
            recorded.lock().unwrap().push(body.clone());
            let mut action = actions.get(index).unwrap_or_else(|| panic!("unexpected model call {index}")).as_object().unwrap().clone();
            let kind = action.remove("kind").unwrap().as_str().unwrap().to_owned();
            let unauthorized = kind == "fixture_http_error";
            let (name, arguments) = if kind == "call_tool" {
                (action["tool"].as_str().unwrap().to_owned(), action["arguments"].clone())
            } else { (kind, json!(action)) };
            assert!(body["tools"].is_array(), "CLI did not pin function calling");
            async move {
                if let Some(gate) = gate { gate.notified().await; }
                let status = if unauthorized { axum::http::StatusCode::UNAUTHORIZED } else { axum::http::StatusCode::OK };
                (status, axum::Json(json!({"id":format!("chat-fixture-{index}"),"choices":[{"message":{"tool_calls":[{"id":format!("call-{index}"),"type":"function","function":{"name":name,"arguments":arguments.to_string()}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":10}})))
            }
        }));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            endpoint,
            count,
            requests,
            task,
        }
    }
    fn calls(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}
impl Drop for Provider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_steering_survives_pause_and_process_death_then_reaches_the_same_turn_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("steering.db");
    let provider = Provider::new(vec![
        json!({"kind":"ask_human","question":"May I continue?"}),
        json!({"kind":"ask_human","question":"Ready to finish?"}),
        json!({"kind":"finish","summary":"Incorporated the instructions."}),
        json!({"kind":"finish","summary":"Independent follow-up."}),
    ])
    .await;
    let mut first = Running::start(
        &db,
        &[
            "--model",
            "steering-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Ask before finishing",
        ],
        true,
    );
    let session = first.event("session").await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let question = first.event("question").await;
    first.send("/pause");
    first.event("control").await;
    let instructions = ["Focus on the API first 🦀", "Also explain restart recovery"];
    let mut ids = vec![];
    for text in instructions {
        first.send(&format!("/steer\t{text}"));
        let saved = first.event("steering_saved").await;
        assert_eq!(saved["status"], "pending");
        assert_eq!(saved["entry"]["request"]["text"], text);
        assert_eq!(saved["entry"]["request"]["run_id"], question["run_id"]);
        assert_eq!(saved["task_id"], question["task_id"]);
        ids.push(saved["entry"]["request"]["message_id"].clone());
    }
    first.send("/steer");
    assert!(
        first.event("input_error").await["message"]
            .as_str()
            .unwrap()
            .contains("Usage:")
    );
    first.send(&format!("/steer {}", "x".repeat(4097)));
    assert!(
        first.event("input_error").await["message"]
            .as_str()
            .unwrap()
            .contains("4096")
    );
    first.send("/status");
    let status = first
        .event_where("status", |data| data.get("steering").is_some())
        .await;
    assert_eq!(status["steering"]["items"].as_array().unwrap().len(), 2);
    assert!(status["steering"]["items"][0]["application"].is_null());
    assert_eq!(provider.calls(), 1);
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    drop(first);
    let (_, agents) = act0r::bootstrap::local_app(&db).await.unwrap();
    let history = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .conversation(session.parse().unwrap(), None)
        .await
        .unwrap();
    assert_eq!(
        history.turns.len(),
        1,
        "steering must not create a queued follow-up or answer the question"
    );
    drop(agents);

    let mut resumed = Running::start(&db, &["--resume", &session], true);
    for id in &ids {
        let restored = resumed.event("steering").await;
        assert_eq!(restored["status"], "pending");
        assert_eq!(&restored["entry"]["request"]["message_id"], id);
    }
    assert_eq!(
        resumed.event("question").await["wait"]["id"],
        question["wait"]["id"]
    );
    resumed.send("/resume");
    resumed.event("control").await;
    resumed.send("Continue");
    resumed.event("decision_saved").await;
    for id in &ids {
        let delivered = resumed.event("steering").await;
        assert_eq!(delivered["status"], "delivered");
        assert_eq!(&delivered["entry"]["request"]["message_id"], id);
        assert!(delivered["entry"]["application"]["activity_id"].is_string());
    }
    let next_question = resumed.event("question").await;
    assert_ne!(next_question["wait"]["id"], question["wait"]["id"]);
    let prompt: Value = serde_json::from_str(
        provider.requests.lock().unwrap()[1]["messages"][1]["content"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        prompt["steering"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["text"].as_str().unwrap())
            .collect::<Vec<_>>(),
        instructions
    );
    resumed.send("Finish now");
    resumed.event("response").await;
    resumed.send("/steer Too late");
    assert!(
        resumed.event("input_error").await["message"]
            .as_str()
            .unwrap()
            .contains("finished")
    );
    assert_eq!(provider.calls(), 3);
    resumed.send("/exit");
    resumed.finish().await;
    Running::start(&db, &["--resume", &session, "A separate follow-up"], false)
        .finish()
        .await;
    let prompt: Value = serde_json::from_str(
        provider.requests.lock().unwrap()[3]["messages"][1]["content"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert!(prompt.get("steering").is_none());
    assert_eq!(provider.calls(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_steering_does_not_change_an_inflight_request_or_leak_into_a_queued_turn() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("inflight-steering.db");
    let gate = Arc::new(tokio::sync::Notify::new());
    let provider = Provider::with_first_gate(
        vec![
            json!({"kind":"finish","summary":"Finished the already admitted request."}),
            json!({"kind":"finish","summary":"Separate queued request."}),
        ],
        Some(gate.clone()),
    )
    .await;
    let mut chat = Running::start(
        &db,
        &[
            "--model",
            "inflight-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Finish this request",
        ],
        true,
    );
    let session = chat.event("session").await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::time::timeout(Duration::from_secs(10), async {
        while provider.calls() != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let original = provider.requests.lock().unwrap()[0].clone();
    chat.send("/steer Arrived during the final model request");
    let saved = chat.event("steering_saved").await;
    assert_eq!(saved["status"], "pending");
    let message = saved["entry"]["request"]["message_id"].clone();
    chat.send("A queued follow-up");
    chat.event("turn_saved").await;
    assert_eq!(provider.calls(), 1);
    assert_eq!(provider.requests.lock().unwrap()[0], original);
    gate.notify_one();
    let ended = chat
        .event_where("steering", |data| data["status"] == "not_delivered")
        .await;
    assert_eq!(ended["entry"]["request"]["message_id"], message);
    assert!(ended["entry"]["application"].is_null());
    chat.send("/exit");
    chat.finish().await;
    Running::start(&db, &["--resume", &session], false)
        .finish()
        .await;
    assert_eq!(provider.calls(), 2);
    let prompt: Value = serde_json::from_str(
        provider.requests.lock().unwrap()[1]["messages"][1]["content"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert!(prompt.get("steering").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_exit_releases_worker_ownership_even_when_stdin_remains_open() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("exit.db");
    let provider = Provider::new(vec![json!({"kind":"finish","summary":"Saved answer."})]).await;
    let mut chat = Running::start(
        &db,
        &[
            "--model",
            "exit-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Explain a mailbox",
        ],
        true,
    );
    let session = chat.event("session").await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    chat.event("response").await;
    chat.child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"/exit\n")
        .unwrap();
    let events = chat.finish().await;
    assert!(has(&events, "saved"));
    let resumed = Running::start(&db, &["--resume", &session], false)
        .finish()
        .await;
    assert!(has(&resumed, "response"));
    assert_eq!(provider.calls(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_current_folder_default_excludes_runtime_state_and_refuses_an_exposed_database() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("README.md"),
        "Inspect this project without editing it.\n",
    )
    .unwrap();
    let provider = Provider::new(vec![
        json!({"kind":"call_tool","tool":"list_files","arguments":{}}),
        json!({"kind":"finish","summary":"Inspected the project without edits."}),
    ])
    .await;
    let events = Running::start_at(
        None,
        &[
            "--project",
            ".",
            "--model",
            "folder-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Inspect the project",
        ],
        false,
        Some(dir.path()),
    )
    .finish()
    .await;
    assert!(has(&events, "response"));
    assert!(dir.path().join(".act0r/chat.db").is_file());
    assert!(!dir.path().join("act0r.db").exists());
    assert!(
        !provider.requests.lock().unwrap()[1]
            .to_string()
            .contains("chat.db")
    );
    let session = &events.iter().find(|e| e["event"] == "session").unwrap()["data"];
    assert!(Path::new(session["database"].as_str().unwrap()).is_absolute());
    let exposed = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .current_dir(dir.path())
        .args([
            "--db",
            "exposed.db",
            "chat",
            "--project",
            ".",
            "Do not send this",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!exposed.status.success());
    assert!(String::from_utf8_lossy(&exposed.stderr).contains("keep --db outside the project"));
    assert_eq!(provider.calls(), 2);
}
fn has(events: &[Value], event: &str) -> bool {
    events.iter().any(|e| e["event"] == event)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_retry_preserves_edits_tests_and_exact_request_across_failure_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("project");
    std::fs::create_dir(&project).unwrap();
    std::fs::write(
        project.join("README.md"),
        "Repair logic.txt with the supplied verifier.\n",
    )
    .unwrap();
    std::fs::write(project.join("logic.txt"), "broken\n").unwrap();
    let provider = Provider::new(vec![
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        json!({"kind":"propose_project_setup","proposal":{"summary":"Repair and verify logic.txt", "writable_paths":["logic.txt"],"test_command":{"argv":[std::env::current_exe().unwrap(),"--ignored","--exact","chat::contract_verifier"],"timeout_ms":10000},"commands":[]}}),
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"logic.txt"}}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"logic.txt","old":"broken\n","new":"fixed\n"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"fixture_invalid_action"}),
        json!({"kind":"fixture_invalid_action"}),
        json!({"kind":"fixture_invalid_action"}),
        json!({"kind":"finish","summary":"Recovered the same verified repair."}),
    ]).await;
    let db = dir.path().join("retry.db");
    let mut first = Running::start(
        &db,
        &[
            "--project",
            project.to_str().unwrap(),
            "--model",
            "retry-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Repair the project",
        ],
        true,
    );
    let session = first.event("session").await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let failed = first
        .event_where("recovery", |data| data["retry_allowed"] == true)
        .await;
    assert_eq!(failed["error"]["code"], "model_invalid_action");
    assert_eq!(provider.calls(), 7);
    first.send("/status");
    let status = first
        .event_where("status", |data| data["recovery"]["retry_allowed"] == true)
        .await;
    assert_eq!(status["recovery"]["run_id"], failed["run_id"]);
    first.send("/retry");
    let retry = first.event("recovery_saved").await;
    assert_eq!(retry["request"]["run_id"], failed["run_id"]);
    assert_eq!(retry["request"]["activity_id"], failed["activity_id"]);
    let failed_again = first
        .event_where("response", |data| data["status"] == "failed")
        .await;
    assert_eq!(failed_again["task_id"], failed["task_id"]);
    let next = first
        .event_where("recovery", |data| {
            data["retry_allowed"] == true
                && data["receipts"]
                    .as_array()
                    .is_some_and(|items| items.len() == 1)
        })
        .await;
    assert_ne!(next["activity_id"], failed["activity_id"]);
    assert_eq!(provider.calls(), 8, "explicit recovery grants one attempt");
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    drop(first);
    let (engine, agents) = act0r::bootstrap::local_app(&db).await.unwrap();
    let run_id = failed["run_id"].as_str().unwrap().parse().unwrap();
    let before = engine.status(run_id).await.unwrap();
    let tools = |run: &act0r::domain::Run| {
        run.activities()
            .iter()
            .filter(|activity| activity.spec.handler.name == "native.tool")
            .map(|activity| serde_json::to_value(activity).unwrap())
            .collect::<Vec<_>>()
    };
    let saved_tools = tools(&before);
    assert_eq!(saved_tools.len(), 3);
    assert!(saved_tools.iter().all(|activity| activity["attempts"] == 1));
    assert_eq!(
        std::fs::read_to_string(project.join("logic.txt")).unwrap(),
        "broken\n"
    );
    drop(agents);
    drop(engine);
    let recovered = Running::start(&db, &["--resume", &session, "/retry"], false)
        .finish()
        .await;
    assert!(has(&recovered, "recovery_saved"));
    assert!(recovered.iter().any(|event| event["event"] == "response"
        && event["data"]["status"] == "succeeded"
        && event["data"]["response"] == "Recovered the same verified repair."));
    assert_eq!(provider.calls(), 9);
    assert!(
        recovered.iter().any(|event| event["event"] == "patch"
            && event["data"]["completion"]["status"] == "verified")
    );
    let (engine, agents) = act0r::bootstrap::local_app(&db).await.unwrap();
    let after = engine.status(run_id).await.unwrap();
    assert_eq!(after.record().status, act0r::domain::RunStatus::Succeeded);
    assert_eq!(
        after.record().usage.admitted_model_calls,
        before.record().usage.admitted_model_calls + 1
    );
    assert_eq!(
        json!(&after.attempts()[..before.attempts().len()]),
        json!(before.attempts()),
        "previous attempts remain unchanged"
    );
    assert_eq!(
        tools(&after),
        saved_tools,
        "reads, edits and tests must retain exact saved results and attempt counts"
    );
    let recovered_activity = &recovered
        .iter()
        .find(|event| event["event"] == "recovery_saved")
        .unwrap()["data"]["resumed_activity_id"];
    let model = after
        .activities()
        .iter()
        .find(|activity| json!(activity.id) == *recovered_activity)
        .unwrap();
    assert_eq!(
        model.spec.input,
        before.activities().last().unwrap().spec.input
    );
    assert_eq!(model.attempts, 1);
    assert!(model.preserve_input);
    let conversation = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .continuous_conversation(session.parse().unwrap(), None, None)
        .await
        .unwrap();
    assert_eq!(
        conversation.turns.len(),
        2,
        "recovery must reuse the discovery and coding turns"
    );
    drop(agents);
    drop(engine);
    {
        let requests = provider.requests.lock().unwrap();
        for index in [6, 7, 8] {
            assert_eq!(
                requests[5], requests[index],
                "all retries must use the frozen request"
            );
        }
    }
    let done = Running::start(&db, &["--resume", &session, "/retry"], false)
        .finish()
        .await;
    assert!(has(&done, "input_error"));
    assert!(!has(&done, "recovery_saved"));
    assert_eq!(provider.calls(), 9);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_retry_refuses_running_and_waiting_requests_without_answering_the_question() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("retry-wait.db");
    let gate = Arc::new(tokio::sync::Notify::new());
    let provider = Provider::with_first_gate(
        vec![
            json!({"kind":"ask_human","question":"May I continue?"}),
            json!({"kind":"finish","summary":"Received the actual answer."}),
        ],
        Some(gate.clone()),
    )
    .await;
    let mut chat = Running::start(
        &db,
        &[
            "--model",
            "retry-wait-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Ask before finishing",
        ],
        true,
    );
    chat.event_where("activity", |data| data["status"] == "started")
        .await;
    chat.send("/retry");
    assert!(
        chat.event("input_error").await["message"]
            .as_str()
            .unwrap()
            .contains("Retry unavailable")
    );
    assert_eq!(provider.calls(), 1);
    gate.notify_one();
    let question = chat.event("question").await;
    chat.send("/retry");
    chat.event("input_error").await;
    chat.send("Continue");
    assert_eq!(
        chat.event("decision_saved").await["wait_id"],
        question["wait"]["id"]
    );
    chat.event("response").await;
    chat.send("/exit");
    chat.finish().await;
    assert_eq!(provider.calls(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_retry_explains_configuration_and_authentication_failures_without_another_call() {
    for configuration in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("retry-config.db");
        let provider = Provider::new(vec![json!({"kind":"fixture_http_error"})]).await;
        let endpoint = if configuration {
            "https://missing-credentials.invalid/v1/chat/completions"
        } else {
            &provider.endpoint
        };
        let mut chat = Running::start(
            &db,
            &[
                "--model",
                "configuration-fixture",
                "--endpoint",
                endpoint,
                "--action-format",
                "function-calling",
                "Explain restart recovery",
            ],
            true,
        );
        let failure = chat.event("recovery").await;
        assert_eq!(failure["retry_allowed"], false);
        assert_eq!(
            failure["error"]["code"],
            if configuration {
                "model_configuration"
            } else {
                "model_http_status"
            }
        );
        chat.send("/retry");
        let message = chat.event("input_error").await;
        assert!(
            message["message"]
                .as_str()
                .unwrap()
                .contains("configuration or credentials")
        );
        chat.send("/exit");
        chat.finish().await;
        assert_eq!(provider.calls(), usize::from(!configuration));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_activity_follows_a_turn_queued_while_the_current_model_request_is_inflight() {
    for restart in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("queued-activity.db");
        let gate = Arc::new(tokio::sync::Notify::new());
        let mut actions = vec![
            if restart {
                json!({"kind":"ask_human","question":"Continue?"})
            } else {
                json!({"kind":"finish","summary":"First request completed."})
            },
            json!({"kind":"finish","summary":"Queued request completed."}),
        ];
        if restart {
            actions
                .push(json!({"kind":"finish","summary":"Queued request completed after restart."}));
        }
        let provider = Provider::with_first_gate(actions, Some(gate.clone())).await;
        let mut chat = Running::start(
            &db,
            &[
                "--model",
                "queued-fixture",
                "--endpoint",
                &provider.endpoint,
                "--action-format",
                "function-calling",
                "First request",
            ],
            true,
        );
        let session = chat.event("session").await["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        chat.event_where("activity", |data| data["status"] == "started")
            .await;
        chat.send("A queued request");
        let queued = chat.event("turn_saved").await;
        // Force a refresh with the future turn visible before releasing the current call.
        chat.send("/status");
        chat.event_where("status", |data| data.get("steering").is_some())
            .await;
        assert_eq!(provider.calls(), 1);
        gate.notify_one();
        if restart {
            chat.event("question").await;
            chat.child.kill().unwrap();
            chat.child.wait().unwrap();
            drop(chat);
            let events = Running::start(&db, &["--resume", &session, "Continue"], false)
                .finish()
                .await;
            assert!(events.iter().any(|event| event["event"] == "activity"
                && event["data"]["task_id"] == queued["task_id"]
                && event["data"]["replayed"] == false));
            assert!(events.iter().any(|event| event["event"] == "response"
                && event["data"]["task_id"] == queued["task_id"]));
            assert_eq!(provider.calls(), 3);
            continue;
        }
        let started = chat
            .event_where("activity", |data| {
                data["task_id"] == queued["task_id"] && data["status"] == "started"
            })
            .await;
        assert_eq!(started["replayed"], false);
        chat.event_where("response", |data| data["task_id"] == queued["task_id"])
            .await;
        chat.send("/exit");
        chat.finish().await;
        assert_eq!(provider.calls(), 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_activity_recap_is_bounded_and_preserves_event_identity_without_repeating_tools() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("project");
    std::fs::create_dir(&project).unwrap();
    std::fs::write(project.join("README.md"), "private-file-content-sentinel\n").unwrap();
    let mut actions =
        vec![json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"missing.md"}})];
    actions.extend(
        (0..20).map(
            |_| json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        ),
    );
    actions.push(json!({"kind":"ask_human","question":"Ready to finish?"}));
    actions.push(json!({"kind":"finish","summary":"Inspected without edits."}));
    let provider = Provider::new(actions).await;
    let db = dir.path().join("recap.db");
    let first = Running::start(
        &db,
        &[
            "--project",
            project.to_str().unwrap(),
            "--model",
            "recap-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Inspect files",
        ],
        false,
    )
    .finish()
    .await;
    let session = first
        .iter()
        .find(|event| event["event"] == "session")
        .unwrap()["data"]["session_id"]
        .as_str()
        .unwrap();
    let activities = first
        .iter()
        .filter(|event| event["event"] == "activity")
        .map(|event| &event["data"])
        .collect::<Vec<_>>();
    assert!(activities.len() > 24);
    assert!(
        activities
            .iter()
            .any(|data| data["tool"] == "read_file" && data["status"] == "failed")
    );
    assert!(activities.iter().all(|data| data["replayed"] == false
        && !data.to_string().contains("private-file-content-sentinel")));
    let ids = activities
        .iter()
        .map(|data| data["event_id"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), activities.len());
    assert!(
        activities
            .windows(2)
            .all(|pair| pair[0]["position"].as_u64() < pair[1]["position"].as_u64())
    );
    assert_eq!(provider.calls(), 22);
    let resumed = Running::start(&db, &["--resume", session], false)
        .finish()
        .await;
    let recap = resumed
        .iter()
        .filter(|event| event["event"] == "activity")
        .map(|event| &event["data"])
        .collect::<Vec<_>>();
    assert_eq!(recap.len(), 24);
    assert!(recap.iter().all(|data| data["replayed"] == true));
    assert_eq!(
        recap
            .iter()
            .map(|data| &data["event_id"])
            .collect::<Vec<_>>(),
        activities[activities.len() - 24..]
            .iter()
            .map(|data| &data["event_id"])
            .collect::<Vec<_>>()
    );
    assert_eq!(provider.calls(), 22);
    let completed = Running::start(&db, &["--resume", session, "Finish now"], false)
        .finish()
        .await;
    assert!(has(&completed, "response"));
    assert!(
        completed
            .iter()
            .any(|event| event["event"] == "activity" && event["data"]["replayed"] == false)
    );
    assert_eq!(provider.calls(), 23);
}

#[test]
#[ignore = "launched as the generated project's actual verifier by the chat integration test"]
fn contract_verifier() {
    assert_eq!(std::fs::read_to_string("logic.txt").unwrap(), "fixed\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_discovers_codes_reviews_and_resumes_after_a_killed_cli_without_replaying_saved_work()
{
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("project");
    std::fs::create_dir(&project).unwrap();
    std::fs::write(project.join("README.md"), "Repair logic.txt; preserve the public format. Ask which format to preserve before coding. Use the supplied contract verifier.\n").unwrap();
    std::fs::write(project.join("logic.txt"), "broken\n").unwrap();
    let provider = Provider::new(vec![
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
        json!({"kind":"ask_human","question":"Which public format should be preserved?"}),
        json!({"kind":"propose_project_setup","proposal":{"summary":"Repair the one documented file and run its contract verifier", "writable_paths":["logic.txt"],"test_command":{"argv":[std::env::current_exe().unwrap(),"--ignored","--exact","chat::contract_verifier"],"timeout_ms":10000},"commands":[]}}),
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"logic.txt"}}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"logic.txt","old":"broken\n","new":"fixed\n"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Repaired the public format and passed the real verifier."}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"finish","summary":"Follow-up verified the same workspace without edits."}),
    ]).await;
    let db = dir.path().join("chat.db");
    let mut first = Running::start(
        &db,
        &[
            "--project",
            project.to_str().unwrap(),
            "--model",
            "chat-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Repair the documented project",
        ],
        true,
    );
    let session = first.event("session").await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let read = first
        .event_where("activity", |data| {
            data["tool"] == "read_file" && data["status"] == "completed"
        })
        .await;
    assert_eq!(read["replayed"], false);
    assert!(read["message"].as_str().unwrap().contains("README.md"));
    let question = first.event("question").await;
    let wait = question["wait"]["id"].clone();
    assert_eq!(provider.calls(), 2);
    // Terminate the actual terminal process at a committed read/question boundary.
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    drop(first);
    let paused = Running::start(&db, &["--resume", &session, "/pause"], false)
        .finish()
        .await;
    assert!(has(&paused, "control"));
    let replayed = paused
        .iter()
        .find(|event| event["event"] == "activity" && event["data"]["event_id"] == read["event_id"])
        .unwrap();
    assert_eq!(replayed["data"]["replayed"], true);
    assert_eq!(replayed["data"]["activity_id"], read["activity_id"]);
    assert_eq!(
        paused.iter().find(|e| e["event"] == "question").unwrap()["data"]["wait"]["id"],
        wait
    );
    let unpaused = Running::start(&db, &["--resume", &session, "/resume"], false)
        .finish()
        .await;
    assert!(has(&unpaused, "control"));
    assert_eq!(provider.calls(), 2);
    let completed = Running::start(
        &db,
        &[
            "--resume",
            &session,
            "Keep the newline-terminated public format.",
        ],
        false,
    )
    .finish()
    .await;
    assert!(has(&completed, "decision_saved"));
    let patch = &completed.iter().find(|e| e["event"] == "patch").unwrap()["data"];
    assert_eq!(patch["completion"]["status"], "verified");
    assert!(
        patch["changes"]["files"][0]["patch"]
            .as_str()
            .unwrap()
            .contains("+fixed")
    );
    assert_eq!(provider.calls(), 7);
    for tool in ["replace_text", "run_tests"] {
        let updates = completed
            .iter()
            .filter(|event| event["event"] == "activity" && event["data"]["tool"] == tool)
            .map(|event| &event["data"])
            .collect::<Vec<_>>();
        assert_eq!(
            updates.len(),
            2,
            "expected one start and one completion for {tool}"
        );
        assert_eq!(updates[0]["status"], "started");
        assert_eq!(updates[1]["status"], "completed");
        assert_eq!(updates[0]["activity_id"], updates[1]["activity_id"]);
    }
    assert!(completed.iter().any(|event| event["event"] == "activity" && event["data"]["message"] == "Tests passed"));
    let identities = completed
        .iter()
        .filter(|event| event["event"] == "activity")
        .map(|event| event["data"]["event_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        identities
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        identities.len()
    );
    assert_eq!(
        std::fs::read_to_string(project.join("logic.txt")).unwrap(),
        "broken\n"
    );
    assert!(
        provider.requests.lock().unwrap()[2]
            .to_string()
            .contains("newline-terminated public format")
    );
    let (engine, agents) = act0r::bootstrap::local_app(&db).await.unwrap();
    let view = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .continuous_conversation(session.parse().unwrap(), None, None)
        .await
        .unwrap();
    let team = view.active.team_id;
    let pinned = agents.teams().unwrap().get(team).await.unwrap();
    assert!(pinned.spec.planner.config["max_model_calls"].is_null());
    assert!(pinned.spec.planner.config["max_tokens"].is_null());
    let ids = pinned
        .members
        .iter()
        .map(|member| member.agent_id)
        .collect::<Vec<_>>();
    drop(agents);
    drop(engine);
    let preview = Running::start(&db, &["--resume", &session, "/apply"], false)
        .finish()
        .await;
    let approval = &preview.iter().find(|e| e["event"] == "approval").unwrap()["data"];
    let application = approval["run_id"].clone();
    let approval_wait = approval["wait"]["id"].clone();
    assert_eq!(
        std::fs::read_to_string(project.join("logic.txt")).unwrap(),
        "broken\n"
    );
    let invalid = Running::start(&db, &["--resume", &session, "maybe"], false)
        .finish()
        .await;
    assert!(has(&invalid, "input_error"));
    assert_eq!(
        invalid.iter().find(|e| e["event"] == "approval").unwrap()["data"]["wait"]["id"],
        approval_wait
    );
    let applied = Running::start(&db, &["--resume", &session, "yes"], false)
        .finish()
        .await;
    assert!(applied.iter().any(|e| e["event"] == "application"
        && e["data"]["run_id"] == application
        && e["data"]["report"]["status"] == "applied"));
    assert_eq!(
        std::fs::read_to_string(project.join("logic.txt")).unwrap(),
        "fixed\n"
    );
    assert_eq!(provider.calls(), 7);
    let followup = Running::start(
        &db,
        &[
            "--resume",
            &session,
            "Recheck the same project without editing it.",
        ],
        false,
    )
    .finish()
    .await;
    assert!(has(&followup, "turn_saved"));
    assert_eq!(provider.calls(), 9);
    let repeated = Running::start(&db, &["--resume", &session], false)
        .finish()
        .await;
    assert!(has(&repeated, "response"));
    assert_eq!(provider.calls(), 9);
    let (_, agents) = act0r::bootstrap::local_app(&db).await.unwrap();
    let view = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .continuous_conversation(session.parse().unwrap(), None, None)
        .await
        .unwrap();
    assert_eq!(view.active.team_id, team);
    assert_eq!(
        agents
            .teams()
            .unwrap()
            .get(team)
            .await
            .unwrap()
            .members
            .iter()
            .map(|member| member.agent_id)
            .collect::<Vec<_>>(),
        ids
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_without_a_project_supports_followups_and_lists_sessions_without_worker_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("plain.db");
    let provider = Provider::new(vec![
        json!({"kind":"finish","summary":"A durable actor retains its identity and mailbox."}),
        json!({"kind":"finish","summary":"Saved activity results are reused after restart."}),
    ])
    .await;
    let first = Running::start(
        &db,
        &[
            "--model",
            "plain-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Explain durable actors",
        ],
        false,
    )
    .finish()
    .await;
    let session = first.iter().find(|e| e["event"] == "session").unwrap()["data"]["session_id"]
        .as_str()
        .unwrap();
    assert!(has(&first, "response"));
    assert!(!has(&first, "patch"));
    let followup = Running::start(&db, &["--resume", session, "What about restart?"], false)
        .finish()
        .await;
    assert!(has(&followup, "turn_saved"));
    assert_eq!(provider.calls(), 2);
    let (engine, _agents) = act0r::bootstrap::local_app(&db).await.unwrap();
    let _worker = engine.worker().await.unwrap();
    let listed = Running::start(&db, &["--list"], false).finish().await;
    assert_eq!(listed[0]["event"], "sessions");
    assert_eq!(listed[0]["data"]["items"][0]["session_id"], session);
    let conflict = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&db)
        .args(["chat", "--resume", session, "--model", "replacement"])
        .output()
        .unwrap();
    assert_eq!(conflict.status.code(), Some(2));
    assert_eq!(provider.calls(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_custom_deadline_is_enforced_and_retry_reuses_the_saved_request() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("deadline.db");
    let gate = Arc::new(tokio::sync::Notify::new());
    let provider = Provider::with_first_gate(
        vec![
            json!({"kind":"finish","summary":"Delayed response"}),
            json!({"kind":"finish","summary":"Recovered after the request deadline"}),
        ],
        Some(gate.clone()),
    )
    .await;
    let events = Running::start(
        &database,
        &[
            "--model",
            "deadline-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "--model-timeout-ms",
            "250",
            "Answer briefly",
        ],
        false,
    )
    .finish()
    .await;
    gate.notify_waiters();
    assert_eq!(provider.calls(), 2);
    assert!(
        events
            .iter()
            .any(|e| e["event"] == "response" && e["data"]["status"] == "succeeded")
    );
    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests[0], requests[1]);
    }
    let response = events.iter().find(|e| e["event"] == "response").unwrap();
    let task: act0r::domain::MessageId = response["data"]["task_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let (engine, agents) = act0r::bootstrap::local_app(&database).await.unwrap();
    let task = agents.actors().unwrap().task(task).await.unwrap();
    let run = engine.status(task.run_id.unwrap()).await.unwrap();
    assert_eq!(
        run.activities()[0].spec.input["target"]["policy"]["request_timeout_ms"],
        250
    );
    assert_eq!(
        run.activities()[0].spec.input["target"]["policy"]["connect_timeout_ms"],
        250
    );
    assert_eq!(run.activities()[0].attempts, 2);
    let Some(act0r::domain::Outcome::Failure(error)) = &run.attempts()[0].outcome else {
        panic!("first request did not retain its timeout");
    };
    assert_eq!(error.code, "model_timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_long_deadline_survives_restart_and_cannot_be_overridden_on_resume() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("pinned-deadline.db");
    let provider = Provider::new(vec![
        json!({"kind":"ask_human","question":"Continue?"}),
        json!({"kind":"finish","summary":"Kept the saved deadline"}),
    ])
    .await;
    let mut first = Running::start(
        &database,
        &[
            "--model",
            "deadline-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "--model-timeout-ms",
            "300000",
            "Ask before finishing",
        ],
        true,
    );
    let session = first.event("session").await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let question = first.event("question").await;
    let run_id = question["run_id"].as_str().unwrap().parse().unwrap();
    drop(first);
    let (engine, agents) = act0r::bootstrap::local_app(&database).await.unwrap();
    let before = engine.status(run_id).await.unwrap();
    assert_eq!(
        before.activities()[0].spec.input["target"]["policy"]["request_timeout_ms"],
        300000
    );
    assert_eq!(
        before.activities()[0].spec.input["target"]["policy"]["connect_timeout_ms"],
        10000
    );
    drop(agents);
    drop(engine);
    let refused = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&database)
        .args(["chat", "--resume", &session, "--model-timeout-ms", "90000"])
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(2));
    let mut second = Running::start(&database, &["--resume", &session], true);
    assert_eq!(
        second.event("question").await["wait"]["id"],
        question["wait"]["id"]
    );
    assert_eq!(provider.calls(), 1);
    second.send("yes");
    second
        .event_where("response", |data| data["status"] == "succeeded")
        .await;
    second.send("/exit");
    second.finish().await;
    let (engine, _agents) = act0r::bootstrap::local_app(&database).await.unwrap();
    let after = engine.status(run_id).await.unwrap();
    assert_eq!(json!(before.activities()[0]), json!(after.activities()[0]));
    assert!(
        after
            .activities()
            .iter()
            .all(|a| a.spec.input["target"]["policy"]["request_timeout_ms"] == 300000)
    );
    assert_eq!(provider.calls(), 2);
}
