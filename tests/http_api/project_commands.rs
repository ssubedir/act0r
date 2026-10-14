use super::*;
use act0r::domain::RunId;
use std::sync::Mutex;

struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl Model {
    async fn start(actions: Vec<Value>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let task = tokio::spawn(async move {
            let router=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
                let prompt:Value=serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                let mut requests=seen.lock().unwrap();let index=requests.len();requests.push(prompt);
                let action=actions[index].clone();
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
impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn tool(name: &str, args: Value) -> Value {
    json!({"kind":"call_tool","tool":name,"arguments":args})
}
fn command(fixture: &str, timeout: u32) -> Value {
    json!({"argv":[std::env::current_exe().unwrap().to_string_lossy(),"--ignored","--exact",format!("project_commands::{fixture}"),"--nocapture"],"timeout_ms":timeout})
}
fn input(model: &Model, source: &Path, timeout: u32) -> Value {
    json!({"message_id":MessageId::new(),"prompt":"Format this project and verify the result", "workspace":fs::canonicalize(source).unwrap().to_string_lossy(),"target":{"provider":"openai","model":"command-fixture","endpoint":model.endpoint,"protocol":"chat_completions"},"coding":{"writable_paths":["README.md"],"test_command":command("tests_fixture",10000),"project_commands":{"format":command("format_fixture",timeout)}}})
}
fn goal(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
async fn wait(server: &Server, receipt: &Value) -> Value {
    let inbox = get(&client(), &format!("{}/inbox", goal(server, receipt))).await;
    inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["actionable"] == true)
        .unwrap()
        .clone()
}
async fn approve(server: &Server, receipt: &Value, wait: &Value, approved: bool) {
    let url = format!(
        "{}/waits/{}/responses",
        goal(server, receipt),
        wait["wait"]["id"].as_str().unwrap()
    );
    let body = json!({"kind":"approval","approved":approved});
    post(&client(), &url, &body, StatusCode::OK).await;
    post(&client(), &url, &body, StatusCode::OK).await;
}
fn root_node<'a>(view: &'a Value, receipt: &Value) -> &'a Value {
    view["task"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["task_id"] == receipt["task_id"])
        .unwrap()
}

#[tokio::test]
async fn named_commands_keep_exact_approval_and_nonzero_output_across_restart_and_require_retesting()
 {
    let model = Model::start(vec![
        tool("run_tests", json!({})),
        tool("run_command", json!({"name":"format"})),
        json!({"kind":"finish","summary":"Formatted"}),
        tool("run_tests", json!({})),
        json!({"kind":"finish","summary":"Formatted and retested"}),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "before").unwrap();
    let database = root.path().join("commands.db");
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let input = input(&model, &source, 10000);
    let receipt = post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await;
    // Exact start retry retains the catalog and original assembly.
    assert_eq!(
        post(
            &client(),
            &format!("{}/v1/harness/sessions", server.url),
            &input,
            StatusCode::ACCEPTED
        )
        .await,
        receipt
    );
    settle(&engine).await;
    let tests = wait(&server, &receipt).await;
    approve(&server, &receipt, &tests, true).await;
    settle(&engine).await;
    let pending = wait(&server, &receipt).await;
    assert_eq!(pending["wait"]["payload"]["tool"]["name"], "run_command");
    assert_eq!(
        pending["wait"]["payload"]["project_command"],
        json!({"name":"format","command":input["coding"]["project_commands"]["format"]})
    );
    assert!(pending["wait"]["payload"].get("test_command").is_none());
    let copy = pending["wait"]["payload"]["workspace"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(!Path::new(&copy).join("command-runs").exists());
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(wait(&server, &receipt).await["wait"], pending["wait"]);
    assert_eq!(model.requests.lock().unwrap().len(), 2);
    let conversation = get(
        &client(),
        &format!(
            "{}/v1/harness/sessions/{}",
            server.url,
            receipt["session_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(
        conversation["coding"]["project_commands"],
        input["coding"]["project_commands"]
    );
    let permissions = get(
        &client(),
        &format!(
            "{}/v1/harness/sessions/{}/turns/{}/permissions",
            server.url,
            receipt["session_id"].as_str().unwrap(),
            receipt["task_id"].as_str().unwrap()
        ),
    )
    .await;
    assert!(
        permissions["eligible_wait"].is_null(),
        "named command must not receive test-only session permission"
    );
    approve(&server, &receipt, &pending, true).await;
    settle(&engine).await;
    assert_eq!(
        fs::read_to_string(Path::new(&copy).join("command-runs")).unwrap(),
        "once\n"
    );
    assert_eq!(
        fs::read_to_string(source.join("README.md")).unwrap(),
        "before"
    );
    let retest = wait(&server, &receipt).await;
    assert_eq!(retest["wait"]["payload"]["tool"]["name"], "run_tests");
    let view = get(&client(), &goal(&server, &receipt)).await;
    let run_id: RunId =
        serde_json::from_value(root_node(&view, &receipt)["run_id"].clone()).unwrap();
    let run = engine.status(run_id).await.unwrap();
    let activity = run
        .activities()
        .iter()
        .find(|a| a.spec.handler == act0r::domain::DefinitionId::new("native.tool", 6))
        .unwrap();
    assert_eq!(activity.result.as_ref().unwrap()["ok"], false);
    assert_eq!(activity.result.as_ref().unwrap()["exit_code"], 7);
    assert_eq!(activity.result.as_ref().unwrap()["command_name"], "format");
    assert_eq!(
        run.attempts()
            .iter()
            .filter(|attempt| attempt.activity_id == activity.id)
            .count(),
        1
    );
    assert!(
        model.requests.lock().unwrap()[3]
            .to_string()
            .contains("completion")
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(model.requests.lock().unwrap().len(), 4);
    assert_eq!(
        fs::read_to_string(Path::new(&copy).join("command-runs")).unwrap(),
        "once\n"
    );
    approve(&server, &receipt, &retest, true).await;
    settle(&engine).await;
    let view = get(&client(), &goal(&server, &receipt)).await;
    assert_eq!(root_node(&view, &receipt)["status"], "succeeded");
    assert_eq!(model.requests.lock().unwrap().len(), 5);
    assert_eq!(
        model.requests.lock().unwrap()[0]["project_commands"],
        input["coding"]["project_commands"]
    );
    server.close().await;
}

#[test]
#[ignore = "invoked by the named-command integration fixture"]
fn tests_fixture() {
    println!("tests passed");
}
#[test]
#[ignore = "invoked by the named-command integration fixture"]
fn format_fixture() {
    fs::write("README.md", "formatted").unwrap();
    fs::write("command-runs", "once\n").unwrap();
    println!("formatted, lint warning");
    std::process::exit(7);
}

#[tokio::test]
async fn timed_out_command_is_unknown_after_restart_and_only_exact_confirmed_retry_dispatches() {
    let model = Model::start(vec![
        tool("run_command", json!({"name":"format"})),
        json!({"kind":"finish_incomplete","summary":"Command recovered; no tests requested"}),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "before").unwrap();
    let database = root.path().join("interrupted.db");
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let mut input = input(&model, &source, 1000);
    input["coding"]["project_commands"]["format"] = command("interrupted_fixture", 1000);
    let receipt = post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let pending = wait(&server, &receipt).await;
    let copy = pending["wait"]["payload"]["workspace"]
        .as_str()
        .unwrap()
        .to_owned();
    approve(&server, &receipt, &pending, true).await;
    settle(&engine).await;
    let recovery_url = format!(
        "{}/v1/harness/sessions/{}/turns/{}/tool-recovery",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    let view = get(&client(), &recovery_url).await;
    let item = &view["items"][0];
    let recovery = item["recovery"].clone();
    assert_eq!(recovery["inspection"]["state"], "command_outcome_unknown");
    assert_eq!(
        recovery["request"]["project_command"]["command"],
        input["coding"]["project_commands"]["format"]
    );
    assert_eq!(
        fs::read_to_string(Path::new(&copy).join("interrupted-runs")).unwrap(),
        "once\n"
    );
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    let url = format!(
        "{}/v1/harness/sessions/{}/turns/{}/runs/{}/reconciliation",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap(),
        item["run_id"].as_str().unwrap()
    );
    let mut decision = json!({"activity_id":recovery["activity_id"],"attempt_id":recovery["attempt_id"],"decision":{"action":"retry"},"evidence":"Inspected partial effects; local process tree stopped","operation_stopped":false});
    post(&client(), &url, &decision, StatusCode::BAD_REQUEST).await;
    decision["operation_stopped"] = json!(true);
    fs::write(Path::new(&copy).join("allow-finish"), "").unwrap();
    post(&client(), &url, &decision, StatusCode::OK).await;
    post(&client(), &url, &decision, StatusCode::OK).await;
    settle(&engine).await;
    assert_eq!(
        fs::read_to_string(Path::new(&copy).join("interrupted-runs")).unwrap(),
        "once\nonce\n"
    );
    let view = get(&client(), &goal(&server, &receipt)).await;
    assert_eq!(root_node(&view, &receipt)["status"], "failed");
    let run_id: RunId =
        serde_json::from_value(root_node(&view, &receipt)["run_id"].clone()).unwrap();
    let run = engine.status(run_id).await.unwrap();
    let activity = run
        .activities()
        .iter()
        .find(|activity| {
            activity.spec.handler == act0r::domain::DefinitionId::new("native.tool", 6)
        })
        .unwrap();
    assert_eq!(activity.attempts, 2);
    assert_eq!(activity.result.as_ref().unwrap()["ok"], true);
    assert_eq!(
        activity.spec.input["project_command"],
        recovery["request"]["project_command"]
    );
    assert_eq!(model.requests.lock().unwrap().len(), 2);
    server.close().await;
}

#[test]
#[ignore = "invoked by interrupted-command integration"]
fn interrupted_fixture() {
    use std::io::Write;
    let mut ledger = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("interrupted-runs")
        .unwrap();
    writeln!(ledger, "once").unwrap();
    drop(ledger);
    if !Path::new("allow-finish").exists() {
        std::thread::sleep(Duration::from_secs(30));
    }
}

#[tokio::test]
async fn test_session_permission_does_not_authorize_commands_and_denial_preserves_test_evidence() {
    let model = Model::start(vec![
        tool("run_tests", json!({})),
        tool("run_command", json!({"name":"format"})),
        json!({"kind":"finish_incomplete","summary":"User denied formatting"}),
    ])
    .await;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "before").unwrap();
    let (engine, agents) = local_app(root.path().join("permission.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let input = input(&model, &source, 10000);
    let receipt = post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let policy_url = format!(
        "{}/v1/harness/sessions/{}/permissions",
        server.url,
        receipt["session_id"].as_str().unwrap()
    );
    let view_url = format!(
        "{}/v1/harness/sessions/{}/turns/{}/permissions",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    let view = get(&client(), &view_url).await;
    let eligible = &view["eligible_wait"];
    post(&client(),&policy_url,&json!({"request_id":uuid::Uuid::new_v4(),"expected_revision":view["state"]["revision"],"action":{"kind":"allow_test_command","task_id":eligible["task_id"],"run_id":eligible["run_id"],"wait_id":eligible["wait_id"]}}),StatusCode::OK).await;
    settle(&engine).await;
    let pending = wait(&server, &receipt).await;
    assert_eq!(pending["wait"]["payload"]["tool"]["name"], "run_command");
    let copy = pending["wait"]["payload"]["workspace"]
        .as_str()
        .unwrap()
        .to_owned();
    let view = get(&client(), &view_url).await;
    assert!(view["state"]["test_command"].is_object());
    assert!(view["eligible_wait"].is_null());
    assert!(!Path::new(&copy).join("command-runs").exists());
    approve(&server, &receipt, &pending, false).await;
    settle(&engine).await;
    let view = get(&client(), &goal(&server, &receipt)).await;
    let run_id: RunId =
        serde_json::from_value(root_node(&view, &receipt)["run_id"].clone()).unwrap();
    let run = engine.status(run_id).await.unwrap();
    assert!(!run.activities().iter().any(
        |activity| activity.spec.handler == act0r::domain::DefinitionId::new("native.tool", 6)
    ));
    let capture = run
        .activities()
        .iter()
        .find(|activity| {
            activity.spec.handler == act0r::domain::DefinitionId::new("workspace.changes", 1)
        })
        .unwrap();
    assert!(
        capture.spec.input["tests"].is_object(),
        "denial must retain saved verification evidence"
    );
    assert!(!Path::new(&copy).join("command-runs").exists());
    server.close().await;
}
