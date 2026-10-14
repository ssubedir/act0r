//! Real loopback HTTP/SSE requests reuse durable teams, waits and goal cursors.
#[path = "http_api/automatic_project.rs"]
mod automatic_project;
#[path = "http_api/automatic_summary.rs"]
mod automatic_summary;
#[path = "http_api/budgets.rs"]
mod budgets;
#[path = "http_api/coding.rs"]
mod coding;
#[path = "http_api/continuous_conversation.rs"]
mod continuous_conversation;
#[path = "http_api/goal_controls.rs"]
mod goal_controls;
#[path = "http_api/goal_list.rs"]
mod goal_list;
#[path = "http_api/harness.rs"]
mod harness;
#[path = "http_api/host.rs"]
mod host;
#[path = "http_api/memory.rs"]
mod memory;
#[path = "http_api/parallel_coding.rs"]
mod parallel_coding;
#[path = "http_api/permissions.rs"]
mod permissions;
#[path = "http_api/planner_guidance.rs"]
mod planner_guidance;
#[path = "http_api/project_commands.rs"]
mod project_commands;
#[path = "http_api/project_detection.rs"]
mod project_detection;
#[path = "http_api/project_instructions.rs"]
mod project_instructions;
#[path = "http_api/project_profiles.rs"]
mod project_profiles;
#[path = "http_api/project_refresh.rs"]
mod project_refresh;
#[path = "http_api/project_setup.rs"]
mod project_setup;
#[path = "http_api/project_setup_update.rs"]
mod project_setup_update;
#[path = "http_api/project_verification.rs"]
mod project_verification;
#[path = "http_api/sessions.rs"]
mod sessions;
#[path = "http_api/steering.rs"]
mod steering;
#[path = "http_api/summary.rs"]
mod summary;
#[path = "http_api/task_edit_permissions.rs"]
mod task_edit_permissions;
#[path = "http_api/task_plans.rs"]
mod task_plans;
#[path = "http_api/task_test_permissions.rs"]
mod task_test_permissions;
#[path = "http_api/turn_recovery.rs"]
mod turn_recovery;
#[path = "http_api/verification_repair.rs"]
mod verification_repair;
#[path = "http_api/worker_cancellation.rs"]
mod worker_cancellation;
#[path = "http_api/worker_clarification.rs"]
mod worker_clarification;
#[path = "http_api/worker_messages.rs"]
mod worker_messages;
#[path = "http_api/worker_pause.rs"]
mod worker_pause;
#[path = "http_api/worker_questions.rs"]
mod worker_questions;
#[path = "http_api/worker_recovery.rs"]
mod worker_recovery;
#[path = "http_api/worker_steering.rs"]
mod worker_steering;
use act0r::{
    Engine, Error,
    adapters::http_api::{self, ApiOptions, MAX_JSON_BODY_BYTES},
    application::AgentService,
    bootstrap::local_app,
    domain::{MessageId, TeamId, WaitId, teams::TeamSpec},
};
use reqwest::{Client, Response, StatusCode};
use serde_json::{Value, json};
use std::{fs, path::Path, sync::Arc, time::Duration};
use tokio::{sync::oneshot, task::JoinHandle};

/// Emit the actual pinned wire format while fixture scripts keep canonical decisions.
fn native_wire_action(action: Value, body: &Value) -> Value {
    if action["kind"] != "call_tool" {
        return action;
    }
    let schema = &body["response_format"]["json_schema"]["schema"];
    let saved = &schema["properties"]["action"];
    let variants = saved["anyOf"]
        .as_array()
        .map_or_else(|| vec![saved], |v| v.iter().collect());
    let Some(variant) = variants
        .into_iter()
        .find(|variant| variant["properties"]["kind"]["enum"][0] == action["tool"])
    else {
        return action;
    };
    let mut fields = action["arguments"].as_object().unwrap().clone();
    for (name, property) in variant["properties"].as_object().unwrap() {
        if property["anyOf"]
            .as_array()
            .is_some_and(|variants| variants.iter().any(|variant| variant["type"] == "null"))
            || property["type"]
                .as_array()
                .is_some_and(|types| types.iter().any(|value| value == "null"))
        {
            fields.entry(name.clone()).or_insert(Value::Null);
        }
    }
    fields.insert("kind".into(), action["tool"].clone());
    json!(fields)
}

fn spec(root: &Path) -> TeamSpec {
    let source = root.join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("README.md"), "Draft\n").unwrap();
    let mut spec: TeamSpec =
        serde_json::from_str(include_str!("../examples/configs/team-inbox.json")).unwrap();
    spec.source = Some(
        fs::canonicalize(source)
            .unwrap()
            .to_string_lossy()
            .into_owned(),
    );
    spec
}
fn client() -> Client {
    Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap()
}
async fn value(response: Response, status: StatusCode) -> Value {
    let actual = response.status();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(actual, status, "{}", String::from_utf8_lossy(&bytes));
    serde_json::from_slice(&bytes).unwrap()
}
async fn get(client: &Client, url: &str) -> Value {
    value(client.get(url).send().await.unwrap(), StatusCode::OK).await
}
async fn post(
    client: &Client,
    url: &str,
    input: &impl serde::Serialize,
    status: StatusCode,
) -> Value {
    value(
        client
            .post(url)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(input).unwrap())
            .send()
            .await
            .unwrap(),
        status,
    )
    .await
}
struct Server {
    url: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), Error>>>,
}
impl Server {
    async fn host(engine: &Engine, agents: Arc<AgentService>) -> Self {
        let listener = http_api::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(http_api::serve_with_worker(
            listener,
            agents,
            engine.worker().await.unwrap(),
            http_api::HarnessHostOptions {
                worker: act0r::adapters::tokio_worker::WorkerOptions {
                    max_concurrent_activities: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
            async {
                let _ = stopped.await;
                Ok(())
            },
        ));
        Self {
            url,
            stop: Some(stop),
            task: Some(task),
        }
    }
    async fn start(agents: Arc<AgentService>, max_streams: usize) -> Self {
        Self::options(
            agents,
            ApiOptions {
                event_poll_interval: Duration::from_millis(10),
                max_event_streams: max_streams,
                ..Default::default()
            },
        )
        .await
    }
    async fn options(agents: Arc<AgentService>, options: ApiOptions) -> Self {
        let listener = http_api::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(http_api::serve(listener, agents, options, async {
            let _ = stopped.await;
            Ok(())
        }));
        Self {
            url,
            stop: Some(stop),
            task: Some(task),
        }
    }
    async fn close(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
async fn setup(
    root: &Path,
    max_streams: usize,
) -> (Engine, Arc<AgentService>, Server, Client, Value, MessageId) {
    let (engine, agents) = local_app(root.join("api.db")).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::start(agents.clone(), max_streams).await;
    let client = client();
    let team = post(
        &client,
        &format!("{}/v1/teams", server.url),
        &spec(root),
        StatusCode::OK,
    )
    .await;
    let task = MessageId::new();
    submit(
        &client,
        &server.url,
        team["id"].as_str().unwrap(),
        task,
        "Reviewed edit",
    )
    .await;
    (engine, agents, server, client, team, task)
}
async fn submit(client: &Client, url: &str, team: &str, task: MessageId, goal: &str) -> Value {
    post(
        client,
        &format!("{url}/v1/teams/{team}/tasks"),
        &json!({"message_id":task,"goal":goal}),
        StatusCode::ACCEPTED,
    )
    .await
}
async fn settle(engine: &Engine) {
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..300 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("fixture did not settle");
}
fn base(url: &str, team: &Value, task: MessageId) -> String {
    format!(
        "{url}/v1/teams/{}/tasks/{task}",
        team["id"].as_str().unwrap()
    )
}
fn wait(inbox: &Value, kind: &str) -> Value {
    inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["kind"] == kind)
        .unwrap()
        .clone()
}
async fn answer(
    client: &Client,
    base: &str,
    wait: &Value,
    response: Value,
    status: StatusCode,
) -> Value {
    post(
        client,
        &format!(
            "{base}/waits/{}/responses",
            wait["wait"]["id"].as_str().unwrap()
        ),
        &response,
        status,
    )
    .await
}
#[derive(Debug)]
struct Frame {
    event: String,
    id: Option<String>,
    data: Value,
}
struct Sse {
    response: Response,
    buffer: Vec<u8>,
}
impl Sse {
    async fn open(client: &Client, url: &str, cursor: Option<&str>) -> Self {
        let mut request = client.get(url);
        if let Some(cursor) = cursor {
            request = request.header("last-event-id", cursor);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        Self {
            response,
            buffer: Vec::new(),
        }
    }
    async fn next(&mut self) -> Option<Frame> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(end) = self.buffer.windows(2).position(|w| w == b"\n\n") {
                    let bytes: Vec<_> = self.buffer.drain(..end + 2).collect();
                    let text = String::from_utf8(bytes).unwrap();
                    let mut event = None;
                    let mut id = None;
                    let mut data = None;
                    for line in text.lines() {
                        if let Some(v) = line.strip_prefix("event:") {
                            event = Some(v.trim_start().to_owned());
                        }
                        if let Some(v) = line.strip_prefix("id:") {
                            id = Some(v.trim_start().to_owned());
                        }
                        if let Some(v) = line.strip_prefix("data:") {
                            data = Some(serde_json::from_str(v.trim_start()).unwrap());
                        }
                    }
                    if let Some(data) = data {
                        return Some(Frame {
                            event: event.unwrap_or_else(|| "message".into()),
                            id,
                            data,
                        });
                    }
                    continue;
                }
                match self.response.chunk().await.unwrap() {
                    Some(chunk) => self.buffer.extend_from_slice(&chunk),
                    None => return None,
                }
                assert!(self.buffer.len() <= 20 * 1024 * 1024);
            }
        })
        .await
        .expect("SSE must flush or close")
    }
}

#[tokio::test]
async fn rest_creation_submission_and_retries_leave_execution_with_the_worker() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _, server, client, team, task) = setup(dir.path(), 32).await;
    let team_id = team["id"].as_str().unwrap();
    let root = base(&server.url, &team, task);
    assert_eq!(
        get(&client, &format!("{}/v1/health", server.url)).await,
        json!({"status":"ok","api_version":1})
    );
    assert_eq!(
        get(&client, &format!("{}/v1/teams", server.url)).await["teams"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let retry = post(
        &client,
        &format!("{}/v1/teams", server.url),
        &spec(dir.path()),
        StatusCode::OK,
    )
    .await;
    assert_eq!(retry["id"], team["id"]);
    assert_eq!(
        retry["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["agent_id"].clone())
            .collect::<Vec<_>>(),
        team["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["agent_id"].clone())
            .collect::<Vec<_>>()
    );
    let retry = submit(&client, &server.url, team_id, task, "Reviewed edit").await;
    assert_eq!(retry["task_id"], task.to_string());
    let conflict = post(
        &client,
        &format!("{}/v1/teams/{team_id}/tasks", server.url),
        &json!({"message_id":task,"goal":"Different"}),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(conflict["error"]["code"], "conflicting_message");
    let view = get(&client, &root).await;
    assert_eq!(view["task"]["nodes"][0]["status"], "queued");
    assert!(
        get(&client, &format!("{root}/events")).await["events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    post(
        &client,
        &format!("{}/v1/teams/{team_id}/resume", server.url),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    let mut worker = engine.worker().await.unwrap();
    // Reads and responses remain available while another process owns execution.
    get(&client, &format!("{}/v1/teams/{team_id}", server.url)).await;
    while worker.step().await.unwrap() {}
    let inbox = get(&client, &format!("{root}/inbox")).await;
    assert_eq!(inbox["items"].as_array().unwrap().len(), 2);
    let ask = wait(&inbox, "question");
    let approval = wait(&inbox, "tool_approval");
    let (a, b) = tokio::join!(
        answer(
            &client,
            &root,
            &ask,
            json!({"kind":"answer","answer":"durability"}),
            StatusCode::OK
        ),
        answer(
            &client,
            &root,
            &ask,
            json!({"kind":"answer","answer":"durability"}),
            StatusCode::OK
        )
    );
    assert_ne!(a["replayed"], b["replayed"]);
    answer(
        &client,
        &root,
        &approval,
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    while worker.step().await.unwrap() {}
    let done = get(&client, &root).await;
    assert_eq!(done["task"]["summary"]["succeeded"], 3);
    assert_eq!(
        done["task"]["summary"]["visible_usage"]["admitted_model_calls"],
        6
    );
    assert!(
        get(&client, &format!("{root}/inbox")).await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    server.close().await;
}

#[tokio::test]
async fn task_admission_requires_a_client_identity_and_preserves_queue_capacity_on_retry() {
    let dir = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(dir.path().join("api.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 32).await;
    let client = client();
    let mut spec = spec(dir.path());
    spec.planner.queue_policy.task_capacity = 1;
    let team = post(
        &client,
        &format!("{}/v1/teams", server.url),
        &spec,
        StatusCode::OK,
    )
    .await;
    let team_id = team["id"].as_str().unwrap();
    let task = MessageId::new();
    let accepted = submit(&client, &server.url, team_id, task, "Reviewed edit").await;
    assert_eq!(
        submit(&client, &server.url, team_id, task, "Reviewed edit").await,
        accepted
    );
    let target = format!("{}/v1/teams/{team_id}/tasks", server.url);
    let error = post(
        &client,
        &target,
        &json!({"message_id":MessageId::new(),"goal":"Over capacity"}),
        StatusCode::TOO_MANY_REQUESTS,
    )
    .await;
    assert_eq!(error["error"]["code"], "queue_full");
    assert_eq!(error["error"]["details"]["capacity"], 1);
    assert_eq!(error["error"]["details"]["outstanding"], 1);
    assert_eq!(error["error"]["details"]["requested"], 1);
    post(
        &client,
        &target,
        &json!({"goal":"Missing durable identity"}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    assert_eq!(
        get(&client, &base(&server.url, &team, task)).await["task"]["nodes"][0]["status"],
        "queued"
    );
    server.close().await;
}

#[tokio::test]
async fn typed_http_errors_bounds_and_wrong_goal_decisions_cannot_mutate_a_wait() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _, server, client, team, task) = setup(dir.path(), 32).await;
    settle(&engine).await;
    let root = base(&server.url, &team, task);
    let before = get(&client, &format!("{root}/inbox")).await;
    let ask = wait(&before, "question");
    let paths = [
        ("/v2/teams".to_owned(), StatusCode::NOT_FOUND, "not_found"),
        (
            "/v1/teams/bad".to_owned(),
            StatusCode::BAD_REQUEST,
            "invalid_path",
        ),
        (
            format!("/v1/teams/{}", TeamId::new()),
            StatusCode::NOT_FOUND,
            "not_found",
        ),
    ];
    for (path, status, code) in paths {
        let error = value(
            client
                .get(format!("{}{path}", server.url))
                .send()
                .await
                .unwrap(),
            status,
        )
        .await;
        assert_eq!(error["error"]["code"], code);
    }
    for query in [
        "max_nodes=0",
        "max_depth=129",
        "max_nodes=-1",
        "typo=1",
        "max_nodes=1&max_nodes=2",
    ] {
        value(
            client
                .get(format!("{root}/inbox?{query}"))
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let response_url = format!(
        "{root}/waits/{}/responses",
        ask["wait"]["id"].as_str().unwrap()
    );
    for (input, status) in [
        (
            json!({"kind":"answer","answer":""}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"kind":"approval","approved":true}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"kind":"answer","answer":"okay","extra":true}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        post(&client, &response_url, &input, status).await;
    }
    let wrong_media = value(
        client.post(&response_url).body("{}").send().await.unwrap(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
    )
    .await;
    assert_eq!(wrong_media["error"]["code"], "json_required");
    let oversized = value(
        client
            .post(&response_url)
            .header("content-type", "application/json")
            .body("x".repeat(MAX_JSON_BODY_BYTES + 1))
            .send()
            .await
            .unwrap(),
        StatusCode::PAYLOAD_TOO_LARGE,
    )
    .await;
    assert_eq!(oversized["error"]["code"], "body_too_large");
    value(
        client
            .post(&response_url)
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let other = MessageId::new();
    submit(
        &client,
        &server.url,
        team["id"].as_str().unwrap(),
        other,
        "Other goal",
    )
    .await;
    let cursor = get(&client, &format!("{root}/events")).await["next_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let other_root = base(&server.url, &team, other);
    for response in [
        client
            .get(format!("{other_root}/events?after={cursor}"))
            .send()
            .await
            .unwrap(),
        client
            .get(format!("{other_root}/events/stream"))
            .header("last-event-id", &cursor)
            .send()
            .await
            .unwrap(),
    ] {
        let error = value(response, StatusCode::BAD_REQUEST).await;
        assert_eq!(error["error"]["code"], "invalid_request");
    }
    answer(
        &client,
        &other_root,
        &ask,
        json!({"kind":"answer","answer":"wrong scope"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(before, get(&client, &format!("{root}/inbox")).await);
    post(
        &client,
        &format!("{root}/waits/{}/responses", WaitId::new()),
        &json!({"kind":"answer","answer":"stale"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    value(
        client.delete(&root).send().await.unwrap(),
        StatusCode::METHOD_NOT_ALLOWED,
    )
    .await;
    server.close().await;
}

#[tokio::test]
async fn loopback_host_and_browser_origin_checks_precede_team_creation() {
    let dir = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(dir.path().join("api.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 32).await;
    let client = client();
    let spec = spec(dir.path());
    let target = format!("{}/v1/teams", server.url);
    for (header, value, status, code) in [
        ("host", "evil.test", StatusCode::BAD_REQUEST, "invalid_host"),
        (
            "origin",
            "https://evil.test",
            StatusCode::FORBIDDEN,
            "origin_refused",
        ),
        ("origin", "null", StatusCode::FORBIDDEN, "origin_refused"),
        (
            "sec-fetch-site",
            "same-site",
            StatusCode::FORBIDDEN,
            "origin_refused",
        ),
    ] {
        let response = client
            .post(&target)
            .header(header, value)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&spec).unwrap())
            .send()
            .await
            .unwrap();
        assert!(
            response
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );
        assert_eq!(response.headers()["cache-control"], "no-store");
        let error = crate::value(response, status).await;
        assert_eq!(error["error"]["code"], code);
    }
    assert!(
        get(&client, &target).await["teams"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let response = client
        .post(&target)
        .header("origin", &server.url)
        .header("sec-fetch-site", "same-origin")
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&spec).unwrap())
        .send()
        .await
        .unwrap();
    value(response, StatusCode::OK).await;
    assert!(http_api::bind("0.0.0.0:0".parse().unwrap()).await.is_err());
    server.close().await;
}

#[tokio::test]
async fn oversized_sse_event_retains_its_cursor_emits_an_error_and_closes() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _, server, client, team, task) = setup(dir.path(), 32).await;
    settle(&engine).await;
    let root = base(&server.url, &team, task);
    let mut stream = Sse::open(&client, &format!("{root}/events/stream?max_bytes=1"), None).await;
    let first = stream.next().await.unwrap();
    assert_eq!(first.event, "page");
    assert_eq!(first.id.as_deref(), first.data["next_cursor"].as_str());
    assert!(first.data["events"].as_array().unwrap().is_empty());
    assert!(first.id.as_ref().unwrap().ends_with(":0"));
    let error = stream.next().await.unwrap();
    assert_eq!(error.event, "api_error");
    assert!(error.id.is_none());
    assert_eq!(error.data["error"]["code"], "event_too_large");
    assert_eq!(
        error.data["error"]["details"]["next_cursor"],
        first.data["next_cursor"]
    );
    assert!(stream.next().await.is_none());
    let mut retry = Sse::open(
        &client,
        &format!("{root}/events/stream"),
        first.id.as_deref(),
    )
    .await;
    assert!(
        !retry.next().await.unwrap().data["events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    drop(retry);
    server.close().await;
}

#[tokio::test]
async fn sse_capacity_disconnect_shutdown_and_cursor_validation_are_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _, server, client, team, task) = setup(dir.path(), 1).await;
    settle(&engine).await;
    let root = base(&server.url, &team, task);
    for query in [
        "after=invalid",
        "limit=0",
        "max_bytes=0",
        "limit=513",
        "unknown=1",
    ] {
        value(
            client
                .get(format!("{root}/events/stream?{query}"))
                .send()
                .await
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    value(
        client
            .get(format!("{root}/events/stream"))
            .header("last-event-id", "invalid")
            .send()
            .await
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let mut stream = Sse::open(&client, &format!("{root}/events/stream?limit=2"), None).await;
    let first = stream.next().await.unwrap();
    assert_eq!(first.data["events"].as_array().unwrap().len(), 2);
    let full = get(&client, &format!("{root}/events?limit=512")).await;
    let mut received = first.data["events"].as_array().unwrap().clone();
    let mut cursor = first.id;
    while received.len() < full["events"].as_array().unwrap().len() {
        let frame = stream.next().await.unwrap();
        received.extend(frame.data["events"].as_array().unwrap().iter().cloned());
        cursor = frame.id;
    }
    assert_eq!(json!(received), full["events"]);
    let error = value(
        client
            .get(format!("{root}/events/stream"))
            .send()
            .await
            .unwrap(),
        StatusCode::TOO_MANY_REQUESTS,
    )
    .await;
    assert_eq!(error["error"]["code"], "stream_limit");
    drop(stream);
    // A disconnected response releases its capacity instead of leaving a detached producer.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let response = loop {
        let response = client
            .get(format!("{root}/events/stream"))
            .header("last-event-id", cursor.as_deref().unwrap())
            .send()
            .await
            .unwrap();
        if response.status() == StatusCode::OK {
            break response;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let mut idle = Sse {
        response,
        buffer: Vec::new(),
    };
    assert!(
        idle.next().await.unwrap().data["events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    server.close().await;
    assert!(idle.next().await.is_none());
}

#[tokio::test]
async fn wait_write_failure_is_generic_over_http_and_retry_keeps_one_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _, server, client, team, task) = setup(dir.path(), 32).await;
    settle(&engine).await;
    let root = base(&server.url, &team, task);
    let before = get(&client, &format!("{root}/inbox")).await;
    let ask = wait(&before, "question");
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(dir.path().join("api.db")),
    )
    .await
    .unwrap();
    sqlx::raw_sql("CREATE TRIGGER reject_http_input BEFORE INSERT ON history_positions BEGIN SELECT RAISE(ABORT,'private failure details'); END;").execute(&pool).await.unwrap();
    let error = answer(
        &client,
        &root,
        &ask,
        json!({"kind":"answer","answer":"durability"}),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(error["error"]["code"], "internal_error");
    assert!(!error.to_string().contains("private failure details"));
    assert_eq!(before, get(&client, &format!("{root}/inbox")).await);
    sqlx::query("DROP TRIGGER reject_http_input")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        answer(
            &client,
            &root,
            &ask,
            json!({"kind":"answer","answer":"durability"}),
            StatusCode::OK
        )
        .await["replayed"],
        false
    );
    assert_eq!(
        answer(
            &client,
            &root,
            &ask,
            json!({"kind":"answer","answer":"durability"}),
            StatusCode::OK
        )
        .await["replayed"],
        true
    );
    assert_eq!(
        answer(
            &client,
            &root,
            &ask,
            json!({"kind":"answer","answer":"different"}),
            StatusCode::CONFLICT
        )
        .await["error"]["code"],
        "conflicting_response"
    );
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn unfinished_json_body_cannot_hold_graceful_shutdown_indefinitely() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(dir.path().join("api.db")).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::options(
        agents.clone(),
        ApiOptions {
            request_timeout: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .await;
    let address = server.url.strip_prefix("http://").unwrap().to_owned();
    let mut socket = tokio::net::TcpStream::connect(&address).await.unwrap();
    socket.write_all(format!("POST /v1/teams HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n{{").as_bytes()).await.unwrap();
    // Let the handler accept the partial body before requesting graceful shutdown.
    tokio::time::sleep(Duration::from_millis(25)).await;
    server.close().await;
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(2), socket.read_to_string(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 408"), "{response}");
    assert!(response.contains("request_timeout"));
    assert!(
        agents
            .teams()
            .unwrap()
            .list()
            .await
            .unwrap()
            .teams
            .is_empty()
    );
}

struct Process {
    child: std::process::Child,
    url: String,
}
impl Process {
    async fn start(db: &Path) -> Self {
        Self::start_with(db, &[]).await
    }
    async fn start_with(db: &Path, extra: &[&str]) -> Self {
        use std::io::BufRead;
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"))
            .arg("--db")
            .arg(db)
            .arg("--json")
            .args(["api", "serve", "--bind", "127.0.0.1:0"])
            .args(extra)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let result = std::io::BufReader::new(stdout)
                .read_line(&mut line)
                .map(|_| line);
            let _ = tx.send(result);
        });
        let line = rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
        let ready: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(ready["api"], "listening");
        let process = Self {
            child,
            url: ready["url"].as_str().unwrap().into(),
        };
        let client = client();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if client
                .get(format!("{}/v1/health", process.url))
                .send()
                .await
                .is_ok_and(|r| r.status() == StatusCode::OK)
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        process
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn worker_process(db: &Path) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(db)
        .args([
            "worker",
            "run",
            "--until-idle",
            "--max-concurrent-activities",
            "2",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_api_restart_keeps_waits_receipts_and_last_event_id_across_processes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("api.db");
    let server = Process::start(&db).await;
    let client = client();
    let team = post(
        &client,
        &format!("{}/v1/teams", server.url),
        &spec(dir.path()),
        StatusCode::OK,
    )
    .await;
    let task = MessageId::new();
    submit(
        &client,
        &server.url,
        team["id"].as_str().unwrap(),
        task,
        "Reviewed edit",
    )
    .await;
    worker_process(&db);
    let root = base(&server.url, &team, task);
    let waiting = get(&client, &format!("{root}/inbox")).await;
    assert_eq!(waiting["items"].as_array().unwrap().len(), 2);
    let full = get(&client, &format!("{root}/events?limit=512")).await;
    let cursor = full["next_cursor"].as_str().unwrap().to_owned();
    let mut stream = Sse::open(
        &client,
        &format!("{root}/events/stream?after={cursor}"),
        None,
    )
    .await;
    assert!(
        stream.next().await.unwrap().data["events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let ask = wait(&waiting, "question");
    let approval = wait(&waiting, "tool_approval");
    answer(
        &client,
        &root,
        &ask,
        json!({"kind":"answer","answer":"durability"}),
        StatusCode::OK,
    )
    .await;
    let question_events = stream.next().await.unwrap();
    assert!(
        question_events.data["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["event"]["kind"] == "input_received")
    );
    let checkpoint = question_events.id.unwrap();
    drop(stream);
    drop(server);
    let server = Process::start(&db).await;
    let root = base(&server.url, &team, task);
    let restored = get(&client, &format!("{root}/inbox")).await;
    assert_eq!(restored["items"].as_array().unwrap().len(), 1);
    assert_eq!(wait(&restored, "tool_approval"), approval);
    assert_eq!(
        answer(
            &client,
            &root,
            &ask,
            json!({"kind":"answer","answer":"durability"}),
            StatusCode::OK
        )
        .await["replayed"],
        true
    );
    answer(
        &client,
        &root,
        &approval,
        json!({"kind":"approval","approved":true}),
        StatusCode::OK,
    )
    .await;
    worker_process(&db);
    // Browser reconnections keep the original URL's after parameter; the header must take precedence.
    let mut stream = Sse::open(
        &client,
        &format!("{root}/events/stream?after={cursor}"),
        Some(&checkpoint),
    )
    .await;
    let recovered = stream.next().await.unwrap();
    let combined = full["events"]
        .as_array()
        .unwrap()
        .iter()
        .cloned()
        .chain(
            question_events.data["events"]
                .as_array()
                .unwrap()
                .iter()
                .cloned(),
        )
        .chain(recovered.data["events"].as_array().unwrap().iter().cloned())
        .collect::<Vec<_>>();
    assert_eq!(
        json!(combined),
        get(&client, &format!("{root}/events?limit=512")).await["events"]
    );
    let done = get(&client, &root).await;
    assert_eq!(done["task"]["summary"]["succeeded"], 3);
    assert_eq!(
        done["task"]["summary"]["visible_usage"]["admitted_model_calls"],
        6
    );
    let editor = team["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == "editor")
        .unwrap();
    assert_eq!(
        fs::read_to_string(
            Path::new(editor["workspace"]["path"].as_str().unwrap()).join("README.md")
        )
        .unwrap(),
        "Reviewed\n"
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("source/README.md")).unwrap(),
        "Draft\n"
    );
    drop(stream);
    drop(server);
}
