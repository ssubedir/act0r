//! Real loopback HTTP transport; no Node or hosted services required.
use super::*;
use act0r::adapters::mcp::McpHttpConfig;
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

const READ: &str = "mcp_service_lookup";
const WRITE: &str = "mcp_service_record_action";
const NORMAL: usize = 0;
const SSE: usize = 1;
const LOST: usize = 2;
const EXPIRED: usize = 3;
const HANG: usize = 4;
const HUGE: usize = 5;
const CHANGED: usize = 6;
const HUGE_SSE: usize = 7;
const BAD_SESSION: usize = 8;

#[derive(Default)]
struct Service {
    mode: AtomicUsize,
    reads: AtomicUsize,
    writes: AtomicUsize,
    sessions: AtomicUsize,
    seen: Mutex<Vec<Value>>,
    expected_token: Mutex<Option<String>>,
    redirect_to: Mutex<Option<String>>,
    stateful: bool,
}
struct Server {
    url: String,
    state: Arc<Service>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(token: Option<&str>, stateful: bool) -> Self {
        let state = Arc::new(Service {
            expected_token: Mutex::new(token.map(str::to_owned)),
            stateful,
            ..Default::default()
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let router = axum::Router::new()
            .route("/mcp", any(handle))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, state, task }
    }
    fn config(&self) -> McpConfig {
        serde_json::from_value(
            json!({"servers":[{"name":"service","http":{"url":self.url,"timeout_ms":3000},
            "tools":[{"name":"lookup","read_only":true,"requires_workspace":false},
                {"name":"record_action","read_only":false,"requires_workspace":false}]}]}),
        )
        .unwrap()
    }
}
fn tools(changed: bool) -> Value {
    json!([
        {"name":"lookup","description":"Read a service record","inputSchema":{"type":"object","properties":{"query":{"type":"string","minLength":if changed {2} else {1}}},"required":["query"],"additionalProperties":false},
            "outputSchema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}},
        {"name":"record_action","description":"Update a service record","inputSchema":{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false},
            "outputSchema":{"type":"object","properties":{"receipt":{"type":"string"}},"required":["receipt"],"additionalProperties":false}}
    ])
}
async fn handle(
    State(s): State<Arc<Service>>,
    method: Method,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let expected_token = s.expected_token.lock().unwrap().clone();
    let authorized = expected_token.as_ref().map_or_else(
        || !headers.contains_key("authorization"),
        |token| {
            headers.get("authorization").and_then(|v| v.to_str().ok())
                == Some(&format!("Bearer {token}"))
        },
    );
    if !authorized {
        return (StatusCode::UNAUTHORIZED, expected_token.unwrap_or_default()).into_response();
    }
    if let Some(url) = s.redirect_to.lock().unwrap().as_ref() {
        return (StatusCode::TEMPORARY_REDIRECT, [("location", url.clone())]).into_response();
    }
    if method == Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if method == Method::DELETE {
        return StatusCode::NO_CONTENT.into_response();
    }
    let request: Value = serde_json::from_slice(&body).unwrap();
    s.seen.lock().unwrap().push(json!({"request":request,"version":headers.get("mcp-protocol-version").and_then(|v|v.to_str().ok()),"session":headers.get("mcp-session-id").and_then(|v|v.to_str().ok()),"authorized":authorized}));
    let name = request["method"].as_str().unwrap();
    if request.get("id").is_none() {
        return StatusCode::ACCEPTED.into_response();
    }
    let mode = s.mode.load(Ordering::SeqCst);
    let mut session = None;
    let result=match name {
        "initialize"=>{
            let ordinal=s.sessions.fetch_add(1,Ordering::SeqCst);
            if s.stateful {session=Some(if mode==BAD_SESSION {String::new()} else {format!("session-{ordinal}")});}
            json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"http-records","version":"1"}})
        },
        "tools/list"=>json!({"tools":tools(mode==CHANGED)}),
        "tools/call"=>{
            if request["params"]["name"]=="record_action" {s.writes.fetch_add(1,Ordering::SeqCst);} else {s.reads.fetch_add(1,Ordering::SeqCst);}
            if mode==HANG {std::future::pending::<()>().await;}
            if mode==EXPIRED {return StatusCode::NOT_FOUND.into_response();}
            if mode==LOST {
                let stream=futures_util::stream::once(async {Err::<String,_>(std::io::Error::new(std::io::ErrorKind::ConnectionReset,"lost after effect"))});
                return Response::builder().header("content-type","application/json").body(Body::from_stream(stream)).unwrap();
            }
            if mode==HUGE {
                return Response::builder().header("content-type","application/json").body(Body::from("x".repeat(1_048_577))).unwrap();
            }
            if mode==HUGE_SSE {
                return Response::builder().header("content-type","text/event-stream").body(Body::from(format!("data: {}", "x".repeat(1_048_577)))).unwrap();
            }
            let value=if request["params"]["name"]=="record_action" {json!({"receipt":"saved-effect"})} else {json!({"answer":"saved HTTP answer"})};
            json!({"content":[{"type":"text","text":value.to_string()}],"structuredContent":value})
        },
        _=>return axum::Json(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32601,"message":"Unknown method"}})).into_response(),
    };
    let response = json!({"jsonrpc":"2.0","id":request["id"],"result":result});
    let mut reply = if mode == SSE {
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(format!(
                ": heartbeat\n\nevent: message\ndata: {response}\n\n"
            )))
            .unwrap()
    } else {
        axum::Json(response).into_response()
    };
    if let Some(session) = session {
        reply
            .headers_mut()
            .insert("mcp-session-id", session.parse().unwrap());
    }
    reply
}
fn config_actor(action: &str) -> Value {
    let arguments = if action == WRITE {
        json!({"text":"approved change"})
    } else {
        json!({"query":"durability"})
    };
    json!({"goal":"Use the configured HTTP service","target":{"provider":"scripted","model":"local","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,"tools":[],
        "script":[{"kind":"call_tool","tool":action,"arguments":arguments},{"kind":"finish","summary":"Used the saved HTTP result"}]})
}
async fn actor(agents: &AgentService, name: &str) -> MessageId {
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            None,
            config_actor(name),
            20,
            Default::default(),
        )
        .await
        .unwrap();
    send(agents, actor).await
}
async fn admit(worker: &mut Worker) -> ActivityExecution {
    for _ in 0..100 {
        worker.advance_work().await.unwrap();
        if let Some(call) = worker.admit().await.unwrap() {
            if call.handler.name == "native.tool" {
                return call;
            }
            let outcome = worker.execute(call.clone()).await.unwrap();
            worker.complete(call, outcome).await.unwrap();
        }
    }
    panic!("HTTP activity not admitted");
}
fn execution_http(definition: &ToolDefinition) -> ActivityExecution {
    let args = if definition.external.as_ref().unwrap().read_only {
        json!({"query":"durability"})
    } else {
        json!({"text":"approved change"})
    };
    let Command::Schedule(spec) = definition.schedule_call(&args).unwrap() else {
        panic!()
    };
    ActivityExecution {
        run_id: RunId::new(),
        activity_id: ActivityId::new(),
        attempt_id: AttemptId::new(),
        ordinal: 1,
        handler: spec.handler,
        input: spec.input,
    }
}

#[test]
fn http_configuration_refuses_ambiguous_connections_credentials_urls_and_local_paths() {
    let valid = json!({"servers":[{"name":"remote","http":{"url":"https://service.invalid/mcp","bearer_token_env":"MCP_SERVICE_TOKEN"},
        "tools":[{"name":"lookup","read_only":true,"requires_workspace":false}]}]});
    let cfg: McpConfig = serde_json::from_value(valid.clone()).unwrap();
    cfg.validate().unwrap();
    assert!(
        serde_json::to_value(&cfg).unwrap()["servers"][0]
            .get("command")
            .is_none()
    );
    for url in [
        "http://remote.invalid/mcp",
        "https://user:secret@host/mcp",
        "https://host/mcp?token=secret",
        "https://host/mcp#secret",
        "file:///private",
    ] {
        let mut cfg = cfg.clone();
        cfg.servers[0].http.as_mut().unwrap().url = url.into();
        assert!(cfg.validate().is_err());
    }
    for url in [
        "http://localhost:1/mcp",
        "http://127.0.0.1:1/mcp",
        "http://[::1]:1/mcp",
    ] {
        let mut cfg = cfg.clone();
        cfg.servers[0].http.as_mut().unwrap().url = url.into();
        cfg.validate().unwrap();
    }
    for change in [
        "missing",
        "both",
        "workspace",
        "paths",
        "env",
        "token",
        "timeout",
    ] {
        let mut value = valid.clone();
        match change {
            "missing" => {
                value["servers"][0].as_object_mut().unwrap().remove("http");
            }
            "both" => {
                value["servers"][0]["command"] =
                    json!({"program":std::env::current_exe().unwrap(),"args":[]})
            }
            "workspace" => value["servers"][0]["tools"][0]["requires_workspace"] = json!(true),
            "paths" => value["servers"][0]["tools"][0]["path_parameters"] = json!(["path"]),
            "env" => value["servers"][0]["env"] = json!({"KEY":"not-for-http"}),
            "token" => {
                value["servers"][0]["http"]["bearer_token_env"] = json!("invalid token name")
            }
            _ => value["servers"][0]["http"]["timeout_ms"] = json!(60001),
        }
        let cfg: McpConfig = serde_json::from_value(value).unwrap();
        assert!(cfg.validate().is_err(), "{change}");
    }
    let mut missing: McpHttpConfig = serde_json::from_value(
        json!({"url":"http://127.0.0.1:1/mcp","bearer_token_env":"ACT0R_HTTP_TEST_MISSING_28796"}),
    )
    .unwrap();
    assert!(missing.check_credentials().is_err());
    missing.bearer_token_env = None;
    missing.check_credentials().unwrap();
}

#[tokio::test]
async fn http_json_sse_and_stateful_sessions_use_the_existing_tool_contract() {
    for (mode, stateful) in [(NORMAL, false), (NORMAL, true), (SSE, false), (SSE, true)] {
        let server = Server::new(None, stateful).await;
        server.state.mode.store(mode, Ordering::SeqCst);
        let root = tempfile::tempdir().unwrap();
        let managed = root.path().join("scratch");
        let tools = discover_tools(
            server.config(),
            Arc::new(LocalTools::new(managed.clone())),
            &managed,
        )
        .await
        .unwrap();
        assert_eq!(server.state.reads.load(Ordering::SeqCst), 0);
        assert_eq!(server.state.writes.load(Ordering::SeqCst), 0);
        let definition = tools
            .iter()
            .find(|tool| tool.definition().id.name == READ)
            .unwrap()
            .definition();
        assert!(!definition.requires_workspace);
        assert!(!definition.requires_approval);
        let registry = ToolRegistry::new(tools).unwrap();
        let result = registry.execute(execution_http(&definition)).await.unwrap();
        assert_eq!(
            result.result().unwrap()["structuredContent"]["answer"],
            "saved HTTP answer"
        );
        let records = server.state.seen.lock().unwrap();
        for call in records
            .iter()
            .filter(|e| e["request"]["method"] == "tools/call")
        {
            assert_eq!(call["version"], "2025-11-25");
            assert_eq!(call["session"].is_string(), stateful);
        }
    }
    let server = Server::new(None, true).await;
    server.state.mode.store(BAD_SESSION, Ordering::SeqCst);
    let root = tempfile::tempdir().unwrap();
    assert!(
        discover_tools(
            server.config(),
            Arc::new(LocalTools::new(root.path().into())),
            root.path()
        )
        .await
        .is_err()
    );
    assert_eq!(server.state.reads.load(Ordering::SeqCst), 0);
    assert_eq!(server.state.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn http_saved_results_and_exact_model_continuation_survive_restart() {
    let server = Server::new(None, true).await;
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("http.db");
    let cfg = server.config();
    let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
        .await
        .unwrap();
    actor(&agents, READ).await;
    let mut worker = engine.worker().await.unwrap();
    let call = admit(&mut worker).await;
    let outcome = worker.execute(call.clone()).await.unwrap();
    worker.complete(call.clone(), outcome).await.unwrap();
    let before = engine.status(call.run_id).await.unwrap();
    let next = before.activities().last().unwrap().spec.input.clone();
    let saved = before
        .activities()
        .iter()
        .find(|a| a.id == call.activity_id)
        .unwrap()
        .clone();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, _agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
    assert_eq!(
        engine
            .status(call.run_id)
            .await
            .unwrap()
            .activities()
            .last()
            .unwrap()
            .spec
            .input,
        next
    );
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    let after = engine.status(call.run_id).await.unwrap();
    assert_eq!(after.record().status, RunStatus::Succeeded);
    assert_eq!(
        json!(
            after
                .activities()
                .iter()
                .find(|a| a.id == saved.id)
                .unwrap()
        ),
        json!(saved)
    );
    assert_eq!(server.state.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_action_approval_survives_restart_and_denial_never_dispatches() {
    for approved in [false, true] {
        let server = Server::new(None, true).await;
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("approval.db");
        let cfg = server.config();
        let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
            .await
            .unwrap();
        let task = actor(&agents, WRITE).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        let view = agents.actors().unwrap().task(task).await.unwrap();
        let run = view.run_id.unwrap();
        let wait = view.wait.unwrap();
        assert_eq!(wait.payload["arguments"], json!({"text":"approved change"}));
        assert_eq!(server.state.writes.load(Ordering::SeqCst), 0);
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, _agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert_eq!(
            json!(engine.status(run).await.unwrap().record().wait),
            json!(wait)
        );
        engine
            .signal(run, wait.id, json!({"approved":approved}))
            .await
            .unwrap();
        engine
            .signal(run, wait.id, json!({"approved":approved}))
            .await
            .unwrap();
        settle(&mut worker).await;
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Succeeded
        );
        assert_eq!(
            server.state.writes.load(Ordering::SeqCst),
            usize::from(approved)
        );
    }
}

#[tokio::test]
async fn lost_or_expired_http_action_responses_block_without_hidden_transport_retries() {
    for mode in [LOST, EXPIRED, HANG, HUGE, HUGE_SSE] {
        let server = Server::new(None, true).await;
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("unknown.db");
        let mut cfg = server.config();
        if mode == HANG {
            cfg.servers[0].http.as_mut().unwrap().timeout_ms = 300;
        }
        let (engine, agents) = local_app_with_mcp_tools(&database, cfg.clone())
            .await
            .unwrap();
        let task = actor(&agents, WRITE).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        let view = agents.actors().unwrap().task(task).await.unwrap();
        let run = view.run_id.unwrap();
        let wait = view.wait.unwrap();
        engine
            .signal(run, wait.id, json!({"approved":true}))
            .await
            .unwrap();
        let call = admit(&mut worker).await;
        server.state.mode.store(mode, Ordering::SeqCst);
        let outcome = worker.execute(call.clone()).await.unwrap();
        assert!(
            matches!(outcome, Outcome::Uncertain(_)),
            "mode {mode}: {outcome:?}"
        );
        worker.complete(call.clone(), outcome).await.unwrap();
        assert_eq!(server.state.writes.load(Ordering::SeqCst), 1);
        drop(worker);
        drop(agents);
        drop(engine);
        server.state.mode.store(NORMAL, Ordering::SeqCst);
        let (engine, _agents) = local_app_with_mcp_tools(&database, cfg).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        settle(&mut worker).await;
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Blocked
        );
        assert!(engine.resume(run).await.is_err());
        assert_eq!(server.state.writes.load(Ordering::SeqCst), 1);
        let decision=ReconciliationRequest {activity_id:call.activity_id,attempt_id:call.attempt_id,evidence:"Inspected the service and confirmed the approved effect exists; the operation has stopped.".into(),decision:ReconciliationDecision::Accept {result:json!({"ok":true,"content":[],"structuredContent":{"receipt":"saved-effect"}})}};
        engine.reconcile(run, decision.clone()).await.unwrap();
        engine.reconcile(run, decision).await.unwrap();
        settle(&mut worker).await;
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Succeeded
        );
        assert_eq!(server.state.writes.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn changed_http_contract_refuses_dispatch_and_read_transport_errors_stay_repeat_safe() {
    let server = Server::new(None, false).await;
    let root = tempfile::tempdir().unwrap();
    let managed = root.path().join("scratch");
    let tools = discover_tools(
        server.config(),
        Arc::new(LocalTools::new(managed.clone())),
        &managed,
    )
    .await
    .unwrap();
    let definition = tools
        .iter()
        .find(|t| t.definition().id.name == READ)
        .unwrap()
        .definition();
    let registry = ToolRegistry::new(tools).unwrap();
    server.state.mode.store(CHANGED, Ordering::SeqCst);
    assert!(matches!(
        registry.execute(execution_http(&definition)).await.unwrap(),
        Outcome::Failure(_)
    ));
    assert_eq!(server.state.reads.load(Ordering::SeqCst), 0);
    for mode in [LOST, HUGE, HUGE_SSE] {
        server.state.mode.store(mode, Ordering::SeqCst);
        assert!(matches!(
            registry.execute(execution_http(&definition)).await.unwrap(),
            Outcome::Failure(_)
        ));
    }
    assert_eq!(server.state.reads.load(Ordering::SeqCst), 3);
}

async fn cli_http(database: &Path, args: &[&str], token: Option<&str>) -> std::process::Output {
    let database = database.to_owned();
    let args: Vec<_> = args.iter().map(|arg| arg.to_string()).collect();
    let token = token.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"));
        command
            .arg("--db")
            .arg(database)
            .arg("--json")
            .args(args)
            .env_remove("ACT0R_MCP_CONFIG")
            .env_remove("ACT0R_MCP_FILESYSTEM_CONFIG")
            .env("OPENAI_API_KEY", "model-key-must-not-be-used-for-mcp")
            .env_remove("ACT0R_HTTP_TEST_TOKEN");
        if let Some(token) = token {
            command.env("ACT0R_HTTP_TEST_TOKEN", token);
        }
        command.output().unwrap()
    })
    .await
    .unwrap()
}
fn cli_value(output: std::process::Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn named_bearer_credentials_authenticate_real_cli_calls_without_being_saved_or_logged() {
    let token = "http-secret-sentinel-98347";
    let server = Server::new(Some(token), true).await;
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("auth.db");
    let mut cfg = server.config();
    cfg.servers[0].http.as_mut().unwrap().bearer_token_env = Some("ACT0R_HTTP_TEST_TOKEN".into());
    fs::write(
        root.path().join("mcp.json"),
        serde_json::to_vec(&cfg).unwrap(),
    )
    .unwrap();
    for credential in [None, Some(""), Some("bad token"), Some("wrong-token")] {
        let output = cli_http(&database, &["actor", "tool", "list"], credential).await;
        assert!(!output.status.success());
        for bytes in [&output.stdout, &output.stderr] {
            let text = String::from_utf8_lossy(bytes);
            assert!(!text.contains(token));
            assert!(!text.contains("wrong-token"));
            assert!(!text.contains("model-key-must-not-be-used-for-mcp"));
        }
    }
    assert_eq!(server.state.sessions.load(Ordering::SeqCst), 0);
    let catalog = cli_value(cli_http(&database, &["actor", "tool", "list"], Some(token)).await);
    assert!(
        catalog
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["id"]["name"] == READ)
    );
    let config = root.path().join("actor.json");
    let input = root.path().join("input.json");
    fs::write(&config, serde_json::to_vec(&config_actor(READ)).unwrap()).unwrap();
    fs::write(&input, br#"{"goal":"Look up durability"}"#).unwrap();
    let created = cli_value(
        cli_http(
            &database,
            &[
                "actor",
                "agent",
                "create",
                "tool",
                "--config-file",
                config.to_str().unwrap(),
            ],
            Some(token),
        )
        .await,
    );
    let task = cli_value(
        cli_http(
            &database,
            &[
                "actor",
                "send",
                created["agent_id"].as_str().unwrap(),
                "--input-file",
                input.to_str().unwrap(),
            ],
            Some(token),
        )
        .await,
    );
    // Rotating the value after the actor pinned its catalog must not change its binding.
    let rotated = "rotated-http-secret-sentinel-98347";
    *server.state.expected_token.lock().unwrap() = Some(rotated.into());
    cli_value(cli_http(&database, &["worker", "run", "--until-idle"], Some(rotated)).await);
    let status = cli_value(
        cli_http(
            &database,
            &[
                "actor",
                "task",
                "status",
                task["message_id"].as_str().unwrap(),
            ],
            Some(rotated),
        )
        .await,
    );
    assert_eq!(status["status"], "succeeded");
    assert_eq!(server.state.reads.load(Ordering::SeqCst), 1);
    for entry in fs::read_dir(root.path()).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            let bytes = fs::read(path).unwrap();
            for secret in [token, rotated] {
                assert!(
                    !bytes
                        .windows(secret.len())
                        .any(|part| part == secret.as_bytes())
                );
            }
        }
    }
    assert!(!catalog.to_string().contains(token));
    assert!(!status.to_string().contains(token));
    assert!(
        server
            .state
            .seen
            .lock()
            .unwrap()
            .iter()
            .all(|e| e["authorized"] == true)
    );
}

#[tokio::test]
async fn redirects_are_not_followed_and_endpoint_changes_block_saved_tool_bindings() {
    let server = Server::new(None, false).await;
    let other = Server::new(None, false).await;
    let root = tempfile::tempdir().unwrap();
    let scratch = root.path().join("scratch");
    *server.state.redirect_to.lock().unwrap() = Some(other.url.clone());
    assert!(
        discover_tools(
            server.config(),
            Arc::new(LocalTools::new(scratch.clone())),
            &scratch
        )
        .await
        .is_err()
    );
    assert!(other.state.seen.lock().unwrap().is_empty());
    *server.state.redirect_to.lock().unwrap() = None;
    let database = root.path().join("binding.db");
    let (engine, agents) = local_app_with_mcp_tools(&database, server.config())
        .await
        .unwrap();
    actor(&agents, READ).await;
    let mut worker = engine.worker().await.unwrap();
    let call = admit(&mut worker).await;
    let run = call.run_id;
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app_with_mcp_tools(&database, other.config())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Blocked
    );
    assert_eq!(other.state.reads.load(Ordering::SeqCst), 0);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, _agents) = local_app_with_mcp_tools(&database, server.config())
        .await
        .unwrap();
    engine.resume(run).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        engine.status(run).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    assert_eq!(server.state.reads.load(Ordering::SeqCst), 1);
}
