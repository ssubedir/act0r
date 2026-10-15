//! Real CLI, model functions and HTTP MCP peer; effects happen before a lost response.
use super::*;
use act0r::domain::{ReconciliationDecision, tool_recovery::*};

const ACTION: &str = "mcp_records_change_record";
struct Service {
    calls: AtomicUsize,
    effects: AtomicUsize,
    losses: usize,
    apply_on_loss: bool,
    arguments: Mutex<Vec<Value>>,
}
struct Mcp {
    state: Arc<Service>,
    task: tokio::task::JoinHandle<()>,
    config: Value,
}
impl Drop for Mcp {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Mcp {
    async fn new(root: &Path, losses: usize, apply_on_loss: bool) -> Self {
        let state = Arc::new(Service {
            calls: AtomicUsize::new(0),
            effects: AtomicUsize::new(0),
            losses,
            apply_on_loss,
            arguments: Mutex::new(vec![]),
        });
        let service = state.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let app=axum::Router::new().route("/mcp",axum::routing::post(move |axum::Json(request):axum::Json<Value>| {
            use axum::response::IntoResponse;
            let service=service.clone();
            async move {
                if request.get("id").is_none() {return axum::http::StatusCode::NO_CONTENT.into_response();}
                let result=match request["method"].as_str().unwrap() {
                    "initialize"=>json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"recovery-fixture","version":"1"}}),
                    "tools/list"=>json!({"tools":[{"name":"change_record","description":"Change a fixture record","inputSchema":{"type":"object","properties":{"text":{"type":"string"},"note":{"type":"string"}},"required":["text"],"additionalProperties":false},"outputSchema":{"type":"object","properties":{"receipt":{"type":"string"}},"required":["receipt"],"additionalProperties":false}}]}),
                    "tools/call"=>{
                        let lost=service.calls.fetch_add(1,Ordering::SeqCst)<service.losses;
                        service.arguments.lock().unwrap().push(request["params"]["arguments"].clone());
                        if !lost || service.apply_on_loss {service.effects.fetch_add(1,Ordering::SeqCst);}
                        if lost {
                            let stream=futures_util::stream::once(async {Err::<String,_>(std::io::Error::new(std::io::ErrorKind::ConnectionReset,"response lost after inspection boundary"))});
                            return axum::http::Response::builder().header("content-type","application/json").body(axum::body::Body::from_stream(stream)).unwrap();
                        }
                        json!({"content":[{"type":"text","text":"record updated"}],"structuredContent":{"receipt":"r-1"}})
                    }
                    _=>return axum::Json(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32601,"message":"unknown method"}})).into_response(),
                };
                axum::Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result})).into_response()
            }
        }));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = json!({"servers":[{"name":"records","http":{"url":url,"timeout_ms":3000},"tools":[{"name":"change_record","read_only":false,"requires_workspace":false}]}]});
        std::fs::write(root.join("mcp.json"), serde_json::to_vec(&config).unwrap()).unwrap();
        Self {
            state,
            task,
            config,
        }
    }
}
async fn blocked(db: &Path, provider: &Provider) -> (Running, String, Value) {
    let mut cli = Running::start(
        db,
        &[
            "--model",
            "reconciliation-fixture",
            "--endpoint",
            &provider.endpoint,
            "--action-format",
            "function-calling",
            "Update the fixture record",
        ],
        true,
    );
    let session = cli.event("session").await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    cli.event("approval").await;
    cli.send("yes");
    cli.event("decision_saved").await;
    let recovery = cli.event("tool_recovery").await;
    (cli, session, recovery)
}
fn actions() -> Vec<Value> {
    vec![
        json!({"kind":"call_tool","tool":ACTION,"arguments":{"text":"approved change","note":null}}),
        json!({"kind":"finish","summary":"Used the reconciled record"}),
    ]
}
fn accept(attempt: &str, result: &Value) -> String {
    format!(
        "/reconcile {attempt} accept --stopped {result} -- Inspected receipt r-1; the service operation stopped."
    )
}
fn result() -> Value {
    json!({"ok":true,"content":[{"type":"text","text":"receipt -- verified"}],"structuredContent":{"receipt":"r-1"}})
}

#[tokio::test]
async fn verified_mcp_results_resume_in_chat_after_process_death_and_hold_the_existing_pause() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("chat.db");
    let service = Mcp::new(root.path(), 1, true).await;
    let provider = Provider::new(actions()).await;
    let (mut first, session, recovery) = blocked(&db, &provider).await;
    assert_eq!(recovery["arguments"], json!({"text":"approved change"}));
    let attempt = recovery["recovery"]["attempt_id"].as_str().unwrap();
    first.send("/pause");
    first.event("control").await;
    drop(first); // Abrupt CLI death after the effect and saved uncertain outcome.
    assert_eq!(service.state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls(), 1);
    let mut resumed = Running::start(&db, &["--resume", &session], true);
    let restored = resumed.event("tool_recovery").await;
    assert_eq!(restored["recovery"]["attempt_id"], attempt);
    assert_eq!(restored["recovery"]["paused"], true);
    for command in [
        format!("/reconcile {attempt} retry no stopped confirmation"),
        format!("/reconcile {attempt} retry --stopped"),
        accept(
            attempt,
            &json!({"ok":true,"content":[],"structuredContent":{"receipt":4}}),
        ),
        accept(
            attempt,
            &json!({"ok":true,"content":[{"type":"image"}],"structuredContent":{"receipt":"r-1"}}),
        ),
    ] {
        resumed.send(&command);
        resumed.event("input_error").await;
    }
    // A valid attempt from this conversation cannot be reconciled through another session.
    resumed.send("/exit");
    resumed.finish().await;
    let (engine, agents) = act0r::bootstrap::local_app_with_mcp_tools(
        &db,
        serde_json::from_value(service.config.clone()).unwrap(),
    )
    .await
    .unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    let other = harness
        .start(act0r::domain::harness::HarnessRequest {
            message_id: act0r::domain::MessageId::new(),
            prompt: "Unrelated read-only session".into(),
            target: serde_json::from_value(
                json!({"provider":"openai","model":"reconciliation-fixture","endpoint":provider.endpoint,"protocol":"chat_completions","action_format":"function_calling"}),
            )
            .unwrap(),
            workspace: None,
            coding: None,
            budget: None,
            auto_execute: false,
            discover_project: false,
        })
        .await
        .unwrap();
    let request = ToolRecoveryRequest {
        activity_id: serde_json::from_value(recovery["recovery"]["activity_id"].clone()).unwrap(),
        attempt_id: attempt.parse().unwrap(),
        decision: ToolRecoveryDecision::Accept { result: result() },
        evidence: "Inspected receipt r-1; the service operation stopped.".into(),
        operation_stopped: true,
    };
    let run_id = serde_json::from_value(recovery["run_id"].clone()).unwrap();
    for invalid in [
        json!({"ok":true,"content":[{"type":"text","text":"x".repeat(65_536)}],"structuredContent":{"receipt":"r-1"}}),
        json!({"ok":true,"isError":true,"content":[],"structuredContent":{"receipt":"r-1"}}),
    ] {
        let mut invalid_request = request.clone();
        invalid_request.decision = ToolRecoveryDecision::Accept { result: invalid };
        assert!(
            harness
                .reconcile_tool(
                    session.parse().unwrap(),
                    session.parse().unwrap(),
                    run_id,
                    invalid_request
                )
                .await
                .is_err()
        );
    }
    assert!(
        harness
            .reconcile_tool(other.session_id, other.task_id, run_id, request)
            .await
            .is_err()
    );
    agents
        .teams()
        .unwrap()
        .controls()
        .unwrap()
        .cancel(other.team_id, other.task_id)
        .await
        .unwrap();
    drop(agents);
    drop(engine);
    let mut resumed = Running::start(&db, &["--resume", &session], true);
    resumed.event("tool_recovery").await;
    let command = accept(attempt, &result());
    resumed.send(&command);
    resumed.event("reconciliation_saved").await;
    resumed.send(&command);
    resumed.event("reconciliation_saved").await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(provider.calls(), 1);
    assert_eq!(service.state.calls.load(Ordering::SeqCst), 1);
    drop(resumed);
    let mut resumed = Running::start(&db, &["--resume", &session], true);
    resumed.event("reconciliation").await;
    resumed.send("/resume");
    resumed.event("control").await;
    assert_eq!(resumed.event("response").await["status"], "succeeded");
    assert_eq!(provider.calls(), 2);
    assert_eq!(service.state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(service.state.effects.load(Ordering::SeqCst), 1);
    resumed.send(&command);
    resumed.event("reconciliation_saved").await;
    resumed.send("/exit");
    resumed.finish().await;
    let replay = Running::start(&db, &["--resume", &session], false)
        .finish()
        .await;
    assert!(
        replay
            .iter()
            .any(|event| event["event"] == "reconciliation")
    );
    assert_eq!(provider.calls(), 2);
}

#[tokio::test]
async fn explicit_mcp_retry_keeps_the_saved_arguments_and_duplicate_old_decisions_cannot_retry_a_new_attempt()
 {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("retry.db");
    let service = Mcp::new(root.path(), 2, false).await;
    let provider = Provider::new(actions()).await;
    let (mut cli, _session, recovery) = blocked(&db, &provider).await;
    let first = recovery["recovery"]["attempt_id"].as_str().unwrap();
    let command = format!(
        "/reconcile {first} retry --stopped Inspected the service: no effect; previous operation stopped."
    );
    cli.send(&command);
    cli.event("reconciliation_saved").await;
    let second = cli
        .event_where("tool_recovery", |view| {
            view["recovery"]["attempt_id"] != first
        })
        .await;
    assert_eq!(service.state.calls.load(Ordering::SeqCst), 2);
    cli.send(&command);
    cli.event("reconciliation_saved").await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(service.state.calls.load(Ordering::SeqCst), 2);
    let next = second["recovery"]["attempt_id"].as_str().unwrap();
    cli.send(&format!(
        "/reconcile {next} retry --stopped Inspected again: no effect; operation stopped."
    ));
    cli.event("reconciliation_saved").await;
    assert_eq!(cli.event("response").await["status"], "succeeded");
    assert_eq!(service.state.calls.load(Ordering::SeqCst), 3);
    assert_eq!(service.state.effects.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls(), 2);
    assert!(
        service
            .state
            .arguments
            .lock()
            .unwrap()
            .iter()
            .all(|args| args == &json!({"text":"approved change"}))
    );
    cli.send("/exit");
    cli.finish().await;
}

#[tokio::test]
async fn explicitly_failing_an_uncertain_mcp_action_is_durable_and_never_redispatches() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("failed.db");
    let service = Mcp::new(root.path(), 1, true).await;
    let provider = Provider::new(actions()).await;
    let (mut cli, session, recovery) = blocked(&db, &provider).await;
    let attempt = recovery["recovery"]["attempt_id"].as_str().unwrap();
    let command = format!(
        "/reconcile {attempt} fail --stopped Service operation stopped; its partial effect needs manual attention."
    );
    cli.send(&command);
    cli.event("reconciliation_saved").await;
    assert_eq!(cli.event("response").await["status"], "failed");
    cli.send(&command);
    cli.event("reconciliation_saved").await;
    cli.send("/exit");
    cli.finish().await;
    let replay = Running::start(&db, &["--resume", &session], false)
        .finish()
        .await;
    assert!(
        replay
            .iter()
            .any(|e| e["event"] == "response" && e["data"]["status"] == "failed")
    );
    assert_eq!(provider.calls(), 1);
    assert_eq!(service.state.calls.load(Ordering::SeqCst), 1);
    // The saved request remains an operator assertion, not a fabricated tool attempt.
    let (engine, agents) = act0r::bootstrap::local_app_with_mcp_tools(
        &db,
        serde_json::from_value(service.config.clone()).unwrap(),
    )
    .await
    .unwrap();
    let run = engine
        .status(serde_json::from_value(recovery["run_id"].clone()).unwrap())
        .await
        .unwrap();
    assert_eq!(run.record().reconciliation_receipts.len(), 1);
    assert!(matches!(
        run.record().reconciliation_receipts[0].request.decision,
        ReconciliationDecision::Fail { .. }
    ));
    drop(agents);
}
