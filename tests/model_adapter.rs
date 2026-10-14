mod common;
use act0r::{
    adapters::model::ModelRouter,
    application::ports::{ModelClient, ProposalClient},
    domain::{ActivityId, agent::*},
};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn request(endpoint: String) -> ModelRequest {
    ModelRequest {
        native_tool_names: vec![],
        native_tool_flattened: false,
        target: ModelTarget {
            provider: "openai".into(),
            model: "test-model".into(),
            endpoint,
            protocol: ModelProtocol::Responses,
            action_format: Default::default(),
            policy: ModelPolicy::default(),
        },
        prompt: "Fix the fixture".into(),
        instructions: "Persisted fixture instructions".into(),
        schema: action_schema(),
        max_output_tokens: 256,
        turn: 0,
        script: vec![],
        scripted_delay_ms: 0,
        context: None,
    }
}

#[tokio::test]
async fn hosted_compatible_endpoints_require_the_standard_api_key() {
    let router = ModelRouter::new(None).unwrap();
    for (endpoint, protocol) in [
        (
            "https://integrate.api.nvidia.com/v1/chat/completions",
            ModelProtocol::ChatCompletions,
        ),
        (
            "https://api.openai.com/v1/responses",
            ModelProtocol::Responses,
        ),
        (
            "https://provider.example/v1/chat/completions",
            ModelProtocol::ChatCompletions,
        ),
    ] {
        let mut request = request(endpoint.into());
        request.target.protocol = protocol;
        let reply = router.generate(request, ActivityId::new()).await.unwrap();
        assert_eq!(reply.decision.unwrap_err(), "OPENAI_API_KEY is missing");
        assert_eq!(
            reply.diagnostics.unwrap().category,
            act0r::domain::RequestCategory::Configuration
        );
        assert!(!reply.retryable);
        assert!(reply.usage.is_none());
    }
}

#[tokio::test]
async fn child_handoff_actions_use_the_pinned_planner_schema_in_both_provider_protocols() {
    use act0r::domain::{planner::PlannerWorkflow, task::WorkspaceSnapshot, *};
    for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
        for action in [
            AgentAction::InspectChildChanges {
                worker: "repairer".into(),
                offset: 0,
            },
            AgentAction::ImportChildChanges {
                worker: "repairer".into(),
            },
        ] {
            let text = json!({"action":action}).to_string();
            let body = match protocol {
                ModelProtocol::Responses => {
                    json!({"status":"completed","id":"handoff","usage":{"input_tokens":40,"output_tokens":12},"output":[{"type":"message","content":[{"type":"output_text","text":text}]}]})
                }
                ModelProtocol::ChatCompletions => {
                    json!({"id":"handoff","usage":{"prompt_tokens":40,"completion_tokens":12},"choices":[{"finish_reason":"stop","message":{"content":text}}]})
                }
            };
            let (endpoint, server) = server_with_auth(200, body, None);
            let workflow = PlannerWorkflow::default();
            let started=workflow.transition(&Value::Null,&WorkflowEvent::Started(json!({
                "goal":"Integrate child repair","target":{"provider":"openai","model":"fixture","endpoint":endpoint,"protocol":protocol},
                "max_model_calls":5,"max_tokens":100000,"max_output_tokens":256,
                "workers":[{"name":"repairer","agent_id":AgentId::new(),"description":"Repair"}],"delegation":{"max_tasks":1,"max_depth":1},
                "child_changes":true,"change_review":true,"workspace":"runtime-pinned-workspace","writable_paths":["src/"]
            }))).unwrap();
            let ready = workflow
                .transition(
                    &started.state,
                    &WorkflowEvent::ActivityCompleted {
                        activity_id: ActivityId::new(),
                        handler: change_review::handler("baseline"),
                        result: json!(WorkspaceSnapshot {
                            version: 1,
                            task_id: TaskId::new(),
                            workspace: "saved-baseline".into(),
                            digest: "0".repeat(64),
                            files: vec![]
                        }),
                    },
                )
                .unwrap();
            let Command::Schedule(spec) = &ready.commands[0] else {
                panic!("model request missing")
            };
            let request: ModelRequest = serde_json::from_value(spec.input.clone()).unwrap();
            let schema = request.schema.clone();
            let reply = ModelRouter::new(None)
                .unwrap()
                .generate(request, ActivityId::new())
                .await
                .unwrap();
            assert_eq!(reply.decision.unwrap().action, action);
            assert_eq!(reply.usage.unwrap().input_tokens, 40);
            let sent = server.join().unwrap();
            let wire = match protocol {
                ModelProtocol::Responses => &sent["text"]["format"]["schema"],
                ModelProtocol::ChatCompletions => &sent["response_format"]["json_schema"]["schema"],
            };
            assert_eq!(wire, &schema);
            assert!(wire.to_string().contains("inspect_child_changes"));
            assert!(wire.to_string().contains("import_child_changes"));
            assert!(!wire.to_string().contains("runtime-pinned-workspace"));
        }
    }
}

#[tokio::test]
async fn agent_and_proposal_http_retries_keep_receipts_reservations_and_model_caps() {
    use act0r::{
        Engine,
        adapters::{
            agent_executor::AgentExecutor, file_lock::FileOwnership, search::SearchExecutor,
            sqlite::SqliteStore, workspace::LocalTools,
        },
        application::ports::ExecutionStore,
        domain::{search::*, *},
    };
    use common::{ManualClock, TestExecutor};
    use std::sync::Arc;
    for proposal in [false, true] {
        for known_usage in [false, true] {
            let body = if known_usage {
                json!({"usage":{"input_tokens":8,"output_tokens":3}})
            } else {
                json!({"error":"limited"})
            };
            let (endpoint, server) = server_options(
                429,
                body.to_string(),
                None,
                "Retry-After: 5\r\nX-Request-ID: req_persisted\r\n",
                0,
                false,
            );
            let directory = tempfile::tempdir().unwrap();
            let db = directory.path().join("retry.db");
            let clock = Arc::new(ManualClock::new(1_000));
            let store = Arc::new(SqliteStore::open(&db).await.unwrap());
            let models = Arc::new(ModelRouter::new(None).unwrap());
            let agent = Arc::new(AgentExecutor::new(
                models.clone(),
                Arc::new(LocalTools::new(directory.path().into())),
                Arc::new(TestExecutor::default()),
            ));
            let executor = Arc::new(SearchExecutor::new(models, agent));
            let definitions: Vec<Arc<dyn WorkflowDefinition>> =
                vec![Arc::new(AgentWorkflow), Arc::new(ProposalWorkflow)];
            let engine = Engine::new(
                store.clone(),
                executor.clone(),
                clock.clone(),
                Arc::new(FileOwnership::new(&db)),
                definitions,
            )
            .unwrap();
            let (definition, input) = if proposal {
                let input = ProposalInput {
                    search_id: SearchId::new(),
                    task_id: TaskId::new(),
                    checkpoint_id: CheckpointId::new(),
                    request: ProposalRequest {
                        model: request(endpoint),
                        width: 3,
                        scripted_proposals: vec![],
                    },
                    budget: UsageBudget {
                        max_model_calls: Some(1),
                        max_tokens: Some(100_000),
                    },
                };
                (
                    DefinitionId::new("search.proposals", 1),
                    serde_json::to_value(input).unwrap(),
                )
            } else {
                let mut config: AgentConfig =
                    serde_json::from_str(include_str!("../examples/configs/agent-scripted.json"))
                        .unwrap();
                config.workspace = directory.path().to_string_lossy().into();
                config.target = request(endpoint).target;
                config.max_model_calls = Some(1);
                config.max_tokens = Some(100_000);
                (
                    DefinitionId::new("agent", 2),
                    serde_json::to_value(config).unwrap(),
                )
            };
            let id = engine.start(definition, input, 10).await.unwrap();
            let mut worker = engine.worker().await.unwrap();
            assert!(worker.step().await.unwrap());
            server.join().unwrap();
            let saved = engine.status(id).await.unwrap();
            let receipt = saved.activities()[0].error.as_ref().unwrap();
            assert_eq!(receipt.retry_after, Some(RetryAfter::DelayMs(5_000)));
            assert_eq!(
                receipt.request.as_ref().unwrap().request_id.as_deref(),
                Some("req_persisted")
            );
            assert_eq!(saved.activities()[0].ready_at, 6_000);
            assert_eq!(saved.record().usage.admitted_model_calls, 1);
            assert_eq!(
                saved.record().usage.uncertain_calls,
                u32::from(!known_usage)
            );
            assert_eq!(saved.record().usage.held_tokens > 0, !known_usage);
            if known_usage {
                assert_eq!(saved.record().usage.charged_tokens(), 11);
            }
            drop(worker);
            drop(engine);
            let reopened = Arc::new(SqliteStore::open(&db).await.unwrap());
            assert_eq!(
                serde_json::to_value(&saved).unwrap(),
                serde_json::to_value(reopened.get(id).await.unwrap()).unwrap()
            );
            let engine = Engine::new(
                reopened,
                executor,
                clock.clone(),
                Arc::new(FileOwnership::new(&db)),
                vec![Arc::new(AgentWorkflow), Arc::new(ProposalWorkflow)],
            )
            .unwrap();
            let mut worker = engine.worker().await.unwrap();
            clock.set(5_999);
            assert!(worker.admit().await.unwrap().is_none());
            clock.set(6_000);
            assert!(worker.admit().await.unwrap().is_none());
            let exhausted = engine.status(id).await.unwrap();
            assert_eq!(exhausted.record().status, RunStatus::BudgetExhausted);
            assert_eq!(exhausted.record().usage.admitted_model_calls, 1);
            assert_eq!(
                exhausted.record().usage.charged_tokens(),
                saved.record().usage.charged_tokens()
            );
            assert_eq!(exhausted.attempts().len(), 1);
        }
    }
}

#[tokio::test]
async fn proposal_adapter_supports_both_protocols_and_preserves_reported_usage() {
    use act0r::domain::search::ProposalRequest;
    for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
        let text =
            json!({"actions":[{"kind":"list_files"},{"kind":"read_file","path":"src/lib.rs"}]})
                .to_string();
        let body = match protocol {
            ModelProtocol::Responses => {
                json!({"status":"completed","usage":{"input_tokens":31,"output_tokens":19},"output":[{"content":[{"type":"output_text","text":text}]}]})
            }
            ModelProtocol::ChatCompletions => {
                json!({"usage":{"prompt_tokens":31,"completion_tokens":19},"choices":[{"finish_reason":"stop","message":{"content":text}}]})
            }
        };
        let (endpoint, server) = server_with_auth(200, body, None);
        let mut model = request(endpoint);
        model.target.protocol = protocol;
        model.schema = json!({"type":"object","properties":{"actions":{"type":"array","items":{"anyOf":action_schema()["properties"]["action"]["anyOf"]}}},"required":["actions"],"additionalProperties":false});
        let request = ProposalRequest {
            model,
            width: 3,
            scripted_proposals: vec![],
        };
        let schema = request.model.schema.clone();
        let reply = ModelRouter::new(None)
            .unwrap()
            .propose(request, ActivityId::new())
            .await
            .unwrap();
        assert_eq!(reply.actions.unwrap().len(), 2);
        assert_eq!(reply.usage.unwrap().input_tokens, 31);
        let sent = server.join().unwrap();
        let format = match protocol {
            ModelProtocol::Responses => &sent["text"]["format"]["schema"],
            ModelProtocol::ChatCompletions => &sent["response_format"]["json_schema"]["schema"],
        };
        assert_eq!(*format, schema);
    }
}
fn server(status: u16, body: Value) -> (String, std::thread::JoinHandle<Value>) {
    server_with_auth(status, body, Some("fixture-key"))
}
fn server_with_auth(
    status: u16,
    body: Value,
    expected_auth: Option<&str>,
) -> (String, std::thread::JoinHandle<Value>) {
    server_options(status, body.to_string(), expected_auth, "", 0, false)
}
fn server_options(
    status: u16,
    body: String,
    expected_auth: Option<&str>,
    extra_headers: &str,
    delay_ms: u64,
    headers_first: bool,
) -> (String, std::thread::JoinHandle<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let expected_auth = expected_auth.map(String::from);
    let extra_headers = extra_headers.to_owned();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let (request, _) = read_http_request(&mut stream, expected_auth.as_deref());
        let headers = format!(
            "HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra_headers}Connection: close\r\n\r\n",
            body.len()
        );
        if headers_first {
            let _ = stream.write_all(headers.as_bytes());
        }
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        if !headers_first {
            let _ = stream.write_all(headers.as_bytes());
        }
        let _ = stream.write_all(body.as_bytes());
        request
    });
    (format!("http://{address}/v1/responses"), handle)
}
fn read_http_request(stream: &mut TcpStream, expected_auth: Option<&str>) -> (Value, String) {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let mut bytes = vec![];
    let mut buffer = [0; 4096];
    let (offset, length, client_id) = loop {
        let n = stream.read(&mut buffer).unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(position) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..position]).to_lowercase();
            if let Some(key) = expected_auth {
                assert!(headers.contains(&format!("authorization: bearer {key}")));
            } else {
                assert!(!headers.contains("authorization:"));
            }
            let client_id = headers
                .lines()
                .find_map(|line| line.strip_prefix("x-client-request-id:").map(str::trim))
                .unwrap()
                .to_string();
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .map(|s| s.trim().parse().unwrap())
                })
                .unwrap();
            break (position + 4, length, client_id);
        }
    };
    while bytes.len() < offset + length {
        let n = stream.read(&mut buffer).unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    (
        serde_json::from_slice(&bytes[offset..offset + length]).unwrap(),
        client_id,
    )
}

type RecordedRequests = Vec<(Value, String)>;
fn sequence_server(bodies: Vec<Value>) -> (String, std::thread::JoinHandle<RecordedRequests>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let handle = std::thread::spawn(move || {
        let mut requests = vec![];
        for body in bodies {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "request never arrived"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(error) => panic!("{error}"),
                }
            };
            requests.push(read_http_request(&mut stream, None));
            let body = body.to_string();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
        requests
    });
    (format!("http://{address}/v1/responses"), handle)
}
fn response() -> Value {
    json!({"id":"resp_fixture","status":"completed","usage":{"input_tokens":40,"output_tokens":12},"output":[{"type":"message","content":[{"type":"output_text","text":"{\"action\":{\"kind\":\"list_files\"}}"}]}]})
}

#[tokio::test]
async fn responses_adapter_sends_strict_schema_and_reconciles_usage() {
    let (endpoint, server) = server(200, response());
    let request = request(endpoint);
    let input_bytes = request.context_input_bytes();
    let reply = ModelRouter::new(Some("fixture-key".into()))
        .unwrap()
        .generate(request, ActivityId::new())
        .await
        .unwrap();
    assert_eq!(reply.decision.unwrap().action, AgentAction::ListFiles);
    let usage = reply.usage.unwrap();
    assert_eq!(usage.input_tokens, 40);
    assert_eq!(usage.output_tokens, 12);
    let sent = server.join().unwrap();
    assert_eq!(sent["model"], "test-model");
    assert_eq!(sent["instructions"], "Persisted fixture instructions");
    assert_eq!(sent["max_output_tokens"], 256);
    assert_eq!(sent["store"], false);
    assert_eq!(sent["text"]["format"]["strict"], true);
    assert_eq!(sent["text"]["format"]["schema"], action_schema());
    assert_eq!(
        input_bytes,
        (sent["instructions"].as_str().unwrap().len()
            + sent["input"].as_str().unwrap().len()
            + sent["text"]["format"]["schema"].to_string().len()) as u64
    );
}
#[tokio::test]
async fn refusals_incomplete_and_malformed_actions_keep_known_usage() {
    let mut refusal = response();
    refusal["output"][0]["content"][0] = json!({"type":"refusal","refusal":"No"});
    let mut incomplete = response();
    incomplete["status"] = json!("incomplete");
    let mut malformed = response();
    malformed["output"][0]["content"][0]["text"] = json!("{\"action\":{\"kind\":\"run_shell\"}}");
    for value in [refusal, incomplete, malformed] {
        let (endpoint, server) = server(200, value);
        let reply = ModelRouter::new(Some("fixture-key".into()))
            .unwrap()
            .generate(request(endpoint), ActivityId::new())
            .await
            .unwrap();
        assert!(reply.decision.is_err());
        assert_eq!(reply.usage.unwrap().input_tokens, 40);
        assert_eq!(
            reply.retryable,
            reply.diagnostics.as_ref().unwrap().category
                == act0r::domain::RequestCategory::InvalidAction
        );
        server.join().unwrap();
    }
}
#[tokio::test]
async fn missing_usage_and_retryable_http_failure_are_conservative() {
    let mut missing = response();
    missing["usage"] = Value::Null;
    for (status, value) in [
        (200, missing),
        (429, json!({"error":"fixture"})),
        (401, json!({"error":"fixture"})),
    ] {
        let (endpoint, server) = server(status, value);
        let reply = ModelRouter::new(Some("fixture-key".into()))
            .unwrap()
            .generate(request(endpoint), ActivityId::new())
            .await
            .unwrap();
        assert!(reply.usage.is_none());
        assert_eq!(reply.retryable, status == 429);
        assert_eq!(reply.decision.is_ok(), status == 200);
        server.join().unwrap();
    }
    let reply = ModelRouter::new(None)
        .unwrap()
        .generate(
            request("https://api.openai.com/v1/responses".into()),
            ActivityId::new(),
        )
        .await
        .unwrap();
    assert!(reply.decision.is_err());
    assert!(!reply.retryable);
}

fn chat_response() -> Value {
    json!({"id":"chatcmpl_fixture","usage":{"prompt_tokens":40,"completion_tokens":12},"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"{\"action\":{\"kind\":\"list_files\"}}"}}]})
}

#[tokio::test]
async fn flat_native_tools_require_the_saved_catalog_and_exact_fields() {
    for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
        for (text, valid) in [
            (
                r#"{"action":{"kind":"custom_inspect","path":"saved"}}"#,
                true,
            ),
            (r#"{"action":{"kind":"unknown","path":"saved"}}"#, false),
            (r#"{"action":{"kind":"custom_inspect"}}"#, false),
            (
                r#"{"action":{"kind":"custom_inspect","path":"saved","extra":true}}"#,
                false,
            ),
            (
                r#"{"action":{"kind":"custom_inspect","arguments":{"path":"saved"}}}"#,
                false,
            ),
            (
                r#"{"action":{"kind":"call_tool","tool":"custom_inspect","arguments":{"path":"saved"}}}"#,
                false,
            ),
            (
                r#"{"action":{"kind":"custom_inspect","path":"saved"},"extra":true}"#,
                false,
            ),
            (
                r#"{"action":{"kind":"custom_inspect","kind":"unknown","path":"saved"}}"#,
                false,
            ),
        ] {
            let body = match protocol {
                ModelProtocol::Responses => {
                    json!({"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":text}]}],"usage":{"input_tokens":10,"output_tokens":5}})
                }
                ModelProtocol::ChatCompletions => {
                    json!({"choices":[{"finish_reason":"stop","message":{"content":text}}],"usage":{"prompt_tokens":10,"completion_tokens":5}})
                }
            };
            let (endpoint, server) = server_with_auth(200, body, None);
            let mut req = request(endpoint);
            req.target.protocol = protocol;
            req.native_tool_names = vec!["custom_inspect".into()];
            req.native_tool_flattened = true;
            req.schema = json!({"properties":{"action":{"anyOf":[{"properties":{"kind":{"enum":["custom_inspect"]},"path":{"type":"string"}},"required":["kind","path"],"additionalProperties":false}]}}});
            let reply = ModelRouter::new(None)
                .unwrap()
                .generate(req, ActivityId::new())
                .await
                .unwrap();
            if valid {
                assert_eq!(
                    reply.decision.unwrap().action,
                    AgentAction::CallTool {
                        tool: "custom_inspect".into(),
                        arguments: json!({"path":"saved"})
                    }
                );
            } else {
                assert!(reply.decision.is_err(), "accepted {text}");
                assert_eq!(
                    reply.diagnostics.unwrap().category,
                    act0r::domain::RequestCategory::InvalidAction
                );
            }
            assert_eq!(reply.usage.unwrap().input_tokens, 10);
            server.join().unwrap();
        }
    }
}

fn chat_request(endpoint: String) -> ModelRequest {
    let mut request = request(endpoint.replace("/v1/responses", "/v1/chat/completions"));
    request.target.protocol = ModelProtocol::ChatCompletions;
    request
}

#[tokio::test]
async fn flat_planner_accepts_only_current_direct_actions_and_single_tool_coordination() {
    for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
        let delegate = json!({"type":"object","properties":{"kind":{"enum":["delegate_task"]},"worker":{"type":"string"},"goal":{"type":"string"}},"required":["kind","worker","goal"],"additionalProperties":false});
        let mut scoped = delegate.clone();
        scoped["properties"]["writable_paths"] = json!({"type":"array","items":{"type":"string"}});
        scoped["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("writable_paths"));
        let inspect = json!({"type":"object","properties":{"kind":{"enum":["inspect_worker_changes"]},"worker":{"type":"string"},"offset":{"type":"integer"}},"required":["kind","worker","offset"],"additionalProperties":false});
        let guide = json!({"type":"object","properties":{"kind":{"enum":["guide_workers"]},"coder":{"type":["string","null"]},"coder2":{"type":["string","null"]}},"required":["kind","coder","coder2"],"additionalProperties":false});
        for (action, single, valid, canonical) in [
            (
                json!({"kind":"delegate_task","worker":"coder","goal":"Repair"}),
                false,
                true,
                None,
            ),
            (
                json!({"kind":"delegate_task","worker":"coder","goal":"Repair","writable_paths":["src/lib.rs"]}),
                false,
                true,
                None,
            ),
            (
                json!({"kind":"inspect_worker_changes","worker":"coder","offset":0}),
                false,
                true,
                None,
            ),
            (
                json!({"kind":"guide_workers","coder":null,"coder2":null}),
                true,
                true,
                Some(
                    json!({"kind":"call_tool","tool":"guide_workers","arguments":{"coder":null,"coder2":null}}),
                ),
            ),
            (
                json!({"kind":"guide_workers","coder":null}),
                true,
                false,
                None,
            ),
            (
                json!({"kind":"guide_workers","coder":null,"coder2":null,"extra":true}),
                true,
                false,
                None,
            ),
            (
                json!({"kind":"call_tool","tool":"guide_workers","arguments":{"coder":null,"coder2":null}}),
                true,
                false,
                None,
            ),
            (
                json!({"kind":"finish","summary":"Escape coordination"}),
                true,
                false,
                None,
            ),
            (
                json!({"kind":"delegate_task","worker":"coder","goal":"Escape coordination"}),
                true,
                false,
                None,
            ),
            (
                json!({"kind":"import_worker_changes","worker":"coder"}),
                false,
                false,
                None,
            ),
            (json!({"kind":"run_tests"}), false, false, None),
            (
                json!({"kind":"delegate_task","worker":"coder","goal":"Repair","extra":true}),
                false,
                false,
                None,
            ),
        ] {
            let text = json!({"action":action}).to_string();
            let body = match protocol {
                ModelProtocol::Responses => {
                    json!({"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":text}]}],"usage":{"input_tokens":10,"output_tokens":5}})
                }
                ModelProtocol::ChatCompletions => {
                    json!({"choices":[{"finish_reason":"stop","message":{"content":text}}],"usage":{"prompt_tokens":10,"completion_tokens":5}})
                }
            };
            let (endpoint, server) = server_with_auth(200, body, None);
            let mut req = request(endpoint);
            req.target.protocol = protocol;
            req.native_tool_flattened = true;
            req.native_tool_names = if single {
                vec!["guide_workers".into()]
            } else {
                vec![]
            };
            req.schema = json!({"type":"object","properties":{"action":if single {guide.clone()}else{json!({"anyOf":[delegate,scoped,inspect]})}},"required":["action"],"additionalProperties":false});
            let reply = ModelRouter::new(None)
                .unwrap()
                .generate(req, ActivityId::new())
                .await
                .unwrap();
            assert_eq!(reply.usage.unwrap().input_tokens, 10);
            if valid {
                assert_eq!(
                    json!(reply.decision.unwrap().action),
                    canonical.unwrap_or(action)
                );
            } else {
                assert!(reply.decision.is_err(), "accepted {text}");
                assert_eq!(
                    reply.diagnostics.unwrap().category,
                    act0r::domain::RequestCategory::InvalidAction
                );
            }
            server.join().unwrap();
        }
    }
}

#[tokio::test]
async fn planner_and_proposal_format_retries_survive_restart_and_honor_caps() {
    use act0r::{
        Engine,
        adapters::{
            agent_executor::AgentExecutor, file_lock::FileOwnership, search::SearchExecutor,
            sqlite::SqliteStore, workspace::LocalTools,
        },
        application::ports::ExecutionStore,
        domain::{planner::PlannerWorkflow, search::*, *},
    };
    use common::{ManualClock, TestExecutor};
    use std::sync::Arc;
    for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
        for proposal in [false, true] {
            for (case, known_usage) in [
                ("recovered", true),
                ("recovered", false),
                ("attempts", true),
                ("model_calls", true),
                ("tokens", true),
            ] {
                let input_tokens = if case == "tokens" { 48_000 } else { 40 };
                let body = |text: &str, id: &str| {
                    let mut body = match protocol {
                        ModelProtocol::Responses => json!({"id":id,"status":"completed",
                            "usage":{"input_tokens":input_tokens,"output_tokens":12},
                            "output":[{"content":[{"type":"output_text","text":text}]}]}),
                        ModelProtocol::ChatCompletions => json!({"id":id,
                            "usage":{"prompt_tokens":input_tokens,"completion_tokens":12},
                            "choices":[{"finish_reason":"stop","message":{"content":text}}]}),
                    };
                    if !known_usage {
                        body["usage"] = Value::Null;
                    }
                    body
                };
                let mut responses = vec![body(
                    "Here is an explanation containing fixture-key",
                    "req_invalid",
                )];
                if !matches!(case, "model_calls" | "tokens") {
                    let text = if case == "attempts" {
                        "{\"action\":{\"kind\":\"unsupported\"}}"
                    } else if proposal {
                        "{\"actions\":[{\"kind\":\"list_files\"},{\"kind\":\"read_file\",\"path\":\"src/lib.rs\"}]}"
                    } else {
                        "{\"action\":{\"kind\":\"finish\",\"summary\":\"Recovered\"}}"
                    };
                    responses.push(body(text, "req_final"));
                }
                let (endpoint, server) = sequence_server(responses);
                let mut request = request(endpoint);
                request.target.protocol = protocol;
                let directory = tempfile::tempdir().unwrap();
                let db = directory.path().join("format-retry.db");
                let clock = Arc::new(ManualClock::new(1_000));
                let models = Arc::new(ModelRouter::new(None).unwrap());
                let executor = Arc::new(SearchExecutor::new(
                    models.clone(),
                    Arc::new(AgentExecutor::new(
                        models,
                        Arc::new(LocalTools::new(directory.path().into())),
                        Arc::new(TestExecutor::default()),
                    )),
                ));
                let budget = UsageBudget {
                    max_model_calls: Some(if case == "model_calls" { 1 } else { 2 }),
                    max_tokens: Some(if case == "tokens" { 50_000 } else { 100_000 }),
                };
                let definitions = || -> Vec<Arc<dyn WorkflowDefinition>> {
                    vec![
                        Arc::new(PlannerWorkflow::default()),
                        Arc::new(ProposalWorkflow),
                    ]
                };
                let engine = Engine::new(
                    Arc::new(SqliteStore::open(&db).await.unwrap()),
                    executor.clone(),
                    clock.clone(),
                    Arc::new(FileOwnership::new(&db)),
                    definitions(),
                )
                .unwrap();
                let (definition, input) = if proposal {
                    request.schema = json!({"type":"object","properties":{"actions":{"type":"array","items":{"anyOf":action_schema()["properties"]["action"]["anyOf"]}}},"required":["actions"],"additionalProperties":false});
                    (
                        ProposalWorkflow.id(),
                        serde_json::to_value(ProposalInput {
                            search_id: SearchId::new(),
                            task_id: TaskId::new(),
                            checkpoint_id: CheckpointId::new(),
                            request: ProposalRequest {
                                model: request,
                                width: 2,
                                scripted_proposals: vec![],
                            },
                            budget,
                        })
                        .unwrap(),
                    )
                } else {
                    (
                        PlannerWorkflow::default().id(),
                        json!({
                            "goal":"Explain how durable actors recover after a restart.","target":request.target,
                            "max_model_calls":budget.max_model_calls,"max_tokens":budget.max_tokens,"max_output_tokens":256,
                            "workers":[{"name":"helper","agent_id":AgentId::new(),"description":"Analyze questions"}],
                            "delegation":{"max_tasks":1,"max_depth":1},
                        }),
                    )
                };
                let id = engine.start(definition, input, 10).await.unwrap();
                let mut worker = engine.worker().await.unwrap();
                assert!(worker.step().await.unwrap());
                let saved = engine.status(id).await.unwrap();
                let failure = saved.activities()[0].error.as_ref().unwrap();
                assert_eq!(failure.code, "model_invalid_action");
                assert!(failure.retryable);
                assert_eq!(
                    failure.request.as_ref().unwrap().request_id.as_deref(),
                    Some("req_invalid")
                );
                assert_eq!(saved.activities()[0].status, ActivityStatus::Queued);
                assert_eq!(saved.activities()[0].ready_at, 1_500);
                assert_eq!(saved.attempts().len(), 1);
                assert!(
                    !serde_json::to_string(&saved)
                        .unwrap()
                        .contains("fixture-key")
                );
                drop(worker);
                drop(engine);
                let store = Arc::new(SqliteStore::open(&db).await.unwrap());
                assert_eq!(
                    serde_json::to_value(&saved).unwrap(),
                    serde_json::to_value(store.get(id).await.unwrap()).unwrap()
                );
                let engine = Engine::new(
                    store,
                    executor,
                    clock.clone(),
                    Arc::new(FileOwnership::new(&db)),
                    definitions(),
                )
                .unwrap();
                let mut worker = engine.worker().await.unwrap();
                clock.set(1_499);
                assert!(worker.admit().await.unwrap().is_none());
                clock.set(1_500);
                let limited = matches!(case, "model_calls" | "tokens");
                assert_eq!(worker.step().await.unwrap(), !limited);
                let finished = engine.status(id).await.unwrap();
                assert_eq!(
                    finished.record().status,
                    match case {
                        "recovered" => RunStatus::Succeeded,
                        "attempts" => RunStatus::Failed,
                        _ => RunStatus::BudgetExhausted,
                    },
                    "protocol={protocol:?}, proposal={proposal}, case={case}, known_usage={known_usage}, error={:?}",
                    finished.record().error
                );
                let count = if limited { 1 } else { 2 };
                assert_eq!(finished.attempts().len(), count);
                assert_eq!(finished.record().usage.admitted_model_calls, count as u32);
                assert_eq!(
                    finished.record().usage.actual_input_tokens,
                    if known_usage {
                        input_tokens * count as u64
                    } else {
                        0
                    }
                );
                assert_eq!(
                    finished.record().usage.uncertain_calls,
                    if known_usage { 0 } else { count as u32 }
                );
                assert_eq!(finished.record().usage.held_tokens > 0, !known_usage);
                if case == "attempts" {
                    let failure = finished.record().error.as_ref().unwrap();
                    assert_eq!(failure.code, "model_invalid_action");
                    assert_eq!(
                        failure.request.as_ref().unwrap().request_id.as_deref(),
                        Some("req_final")
                    );
                }
                let requests = server.join().unwrap();
                assert_eq!(requests.len(), count);
                if count == 2 {
                    assert_eq!(
                        requests[0], requests[1],
                        "retry must preserve the pinned request and logical client ID"
                    );
                }
                assert!(worker.admit().await.unwrap().is_none());
            }
        }
    }
}

#[tokio::test]
async fn chat_adapter_sends_schema_messages_output_limit_and_records_usage_without_local_auth() {
    let (endpoint, server) = server_with_auth(200, chat_response(), None);
    let request = chat_request(endpoint);
    let input_bytes = request.context_input_bytes();
    let instructions = request.chat_instructions();
    let reply = ModelRouter::new(None)
        .unwrap()
        .generate(request, ActivityId::new())
        .await
        .unwrap();
    let decision = reply.decision.unwrap();
    assert_eq!(decision.action, AgentAction::ListFiles);
    assert_eq!(decision.request_id.as_deref(), Some("chatcmpl_fixture"));
    let usage = reply.usage.unwrap();
    assert_eq!(usage.input_tokens, 40);
    assert_eq!(usage.output_tokens, 12);
    let sent = server.join().unwrap();
    assert_eq!(sent["messages"][0]["role"], "system");
    assert_eq!(sent["messages"][0]["content"], instructions);
    assert!(instructions.contains("including its outer envelope"));
    assert!(instructions.contains("Do not add Markdown fences"));
    assert_eq!(sent["messages"][1]["content"], "Fix the fixture");
    assert_eq!(sent["max_tokens"], 256);
    assert_eq!(sent["n"], 1);
    assert_eq!(sent["stream"], false);
    assert_eq!(sent["response_format"]["json_schema"]["strict"], true);
    assert_eq!(
        sent["response_format"]["json_schema"]["schema"],
        action_schema()
    );
    assert!(sent.get("max_output_tokens").is_none());
    assert!(sent.get("text").is_none());
    assert_eq!(
        input_bytes,
        (sent["messages"][0]["content"].as_str().unwrap().len()
            + sent["messages"][1]["content"].as_str().unwrap().len()
            + sent["response_format"]["json_schema"]["schema"]
                .to_string()
                .len()) as u64
    );
}

#[tokio::test]
async fn complete_json_fences_preserve_strict_actions_and_usage_in_both_protocols() {
    for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
        for language in ["json", "", "JSON\r"] {
            let text = format!(
                "  ```{language}\n{{\"action\":{{\"kind\":\"finish\",\"summary\":\"Hello!\"}}}}\n```\n"
            );
            let mut body = match protocol {
                ModelProtocol::Responses => response(),
                ModelProtocol::ChatCompletions => chat_response(),
            };
            match protocol {
                ModelProtocol::Responses => body["output"][0]["content"][0]["text"] = json!(text),
                ModelProtocol::ChatCompletions => {
                    body["choices"][0]["message"]["content"] = json!(text)
                }
            }
            let (endpoint, server) = server_with_auth(200, body, None);
            let mut request = request(endpoint);
            request.target.protocol = protocol;
            let reply = ModelRouter::new(None)
                .unwrap()
                .generate(request, ActivityId::new())
                .await
                .unwrap();
            assert_eq!(
                reply.decision.unwrap().action,
                AgentAction::Finish {
                    summary: "Hello!".into()
                }
            );
            assert_eq!(reply.usage.unwrap().output_tokens, 12);
            assert!(!reply.retryable);
            server.join().unwrap();
        }
    }
}

#[tokio::test]
async fn structured_output_errors_remain_strict_specific_and_secret_safe() {
    for (text, expected) in [
        (
            "Here is the action: {\"action\":{\"kind\":\"finish\",\"summary\":\"fixture-key\"}}",
            "invalid JSON",
        ),
        (
            "```json\n{\"action\":{\"kind\":\"finish\",\"summary\":\"fixture-key\"}}\n```\nExtra prose",
            "invalid JSON",
        ),
        ("```json\n{\"action\":", "invalid JSON"),
        (
            "```json\n{\"action\":{\"kind\":\"finish\",\"summary\":\"fixture-key\"}}\n```\n```json\n{}\n```",
            "invalid JSON",
        ),
        (
            "{\"action\":{\"kind\":\"finish\",\"summary\":\"fixture-key\"}} {}",
            "invalid JSON",
        ),
        (
            "{\"kind\":\"finish\",\"summary\":\"fixture-key\"}",
            "required envelope",
        ),
        (
            "{\"action\":{\"kind\":\"fixture-key\"}}",
            "required envelope",
        ),
        (
            "```json\n{\"action\":{\"kind\":\"finish\",\"summary\":\"Hello\",\"fixture-key\":true}}\n```",
            "required envelope",
        ),
        ("{\"action\":{\"kind\":\"finish\"}}", "required envelope"),
    ] {
        let mut body = chat_response();
        body["choices"][0]["message"]["content"] = json!(text);
        let (endpoint, server) = server_with_auth(200, body, None);
        let reply = ModelRouter::new(None)
            .unwrap()
            .generate(chat_request(endpoint), ActivityId::new())
            .await
            .unwrap();
        let error = reply.decision.unwrap_err();
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("fixture-key"), "{error}");
        assert_eq!(
            reply.diagnostics.unwrap().category,
            act0r::domain::RequestCategory::InvalidAction
        );
        assert_eq!(reply.usage.unwrap().input_tokens, 40);
        assert!(reply.retryable);
        server.join().unwrap();
    }
}

#[tokio::test]
async fn reasoning_only_chat_replies_are_incomplete_and_never_execute_reasoning_as_an_action() {
    for content in [Value::Null, json!(""), json!("  \n")] {
        let mut body = chat_response();
        body["choices"][0]["message"]["content"] = content;
        body["choices"][0]["message"]["reasoning_content"] =
            json!("{\"action\":{\"kind\":\"finish\",\"summary\":\"fixture-key\"}}");
        let (endpoint, server) = server_with_auth(200, body, None);
        let reply = ModelRouter::new(None)
            .unwrap()
            .generate(chat_request(endpoint), ActivityId::new())
            .await
            .unwrap();
        let error = reply.decision.unwrap_err();
        assert!(error.contains("empty or reasoning-only content"));
        assert!(!error.contains("fixture-key"));
        assert_eq!(
            reply.diagnostics.unwrap().category,
            act0r::domain::RequestCategory::Incomplete
        );
        assert_eq!(reply.usage.unwrap().output_tokens, 12);
        assert!(!reply.retryable);
        server.join().unwrap();
    }
}

#[tokio::test]
async fn chat_adapter_rejects_incomplete_refused_malformed_and_provider_tool_responses() {
    let mut length = chat_response();
    length["choices"][0]["finish_reason"] = json!("length");
    let mut refusal = chat_response();
    refusal["choices"][0]["message"]["refusal"] = json!("No");
    let mut malformed = chat_response();
    malformed["choices"][0]["message"]["content"] = json!("{\"action\":{\"kind\":\"run_shell\"}}");
    let mut tools = chat_response();
    tools["choices"][0]["message"]["tool_calls"] = json!([{"id":"call_fixture"}]);
    let mut multiple = chat_response();
    let second_choice = multiple["choices"][0].clone();
    multiple["choices"]
        .as_array_mut()
        .unwrap()
        .push(second_choice);
    for (value, retryable) in [
        (length, false),
        (refusal, false),
        (malformed, true),
        (tools, false),
        (multiple, false),
    ] {
        let (endpoint, server) = server(200, value);
        let reply = ModelRouter::new(Some("fixture-key".into()))
            .unwrap()
            .generate(chat_request(endpoint), ActivityId::new())
            .await
            .unwrap();
        assert!(reply.decision.is_err());
        assert_eq!(reply.usage.unwrap().output_tokens, 12);
        assert_eq!(reply.retryable, retryable);
        server.join().unwrap();
    }
}

#[tokio::test]
async fn protocol_defaults_preserve_old_requests_and_missing_chat_usage_is_retained() {
    let target: ModelTarget = serde_json::from_value(json!({"provider":"openai","model":"minimal","endpoint":"https://api.openai.com/v1/responses"})).unwrap();
    assert!(matches!(target.protocol, ModelProtocol::Responses));
    let mut value = chat_response();
    value["usage"] = Value::Null;
    let (endpoint, server) = server(200, value);
    let reply = ModelRouter::new(Some("fixture-key".into()))
        .unwrap()
        .generate(chat_request(endpoint), ActivityId::new())
        .await
        .unwrap();
    assert!(reply.decision.is_ok());
    assert!(reply.usage.is_none());
    server.join().unwrap();
}

#[tokio::test]
async fn retry_after_status_request_ids_and_known_error_usage_support_both_protocols() {
    use act0r::domain::{RequestCategory, RetryAfter};
    for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
        for (status, header, expected) in [
            (429, "5", Some(RetryAfter::DelayMs(5000))),
            (
                429,
                "Thu, 01 Jan 1970 00:00:05 GMT",
                Some(RetryAfter::At(5000)),
            ),
            (429, "broken", None),
            (429, "-1", None),
            (503, "5", Some(RetryAfter::DelayMs(5000))),
            (400, "5", None),
        ] {
            let usage = match protocol {
                ModelProtocol::Responses => json!({"input_tokens":8,"output_tokens":3}),
                ModelProtocol::ChatCompletions => json!({"prompt_tokens":8,"completion_tokens":3}),
            };
            let (endpoint, server) = server_options(
                status,
                json!({"error":"private provider detail", "usage":usage}).to_string(),
                None,
                &format!("Retry-After: {header}\r\nX-Request-Id: req_limited\r\n"),
                0,
                false,
            );
            let mut request = request(endpoint);
            request.target.protocol = protocol;
            let activity = ActivityId::new();
            let reply = ModelRouter::new(None)
                .unwrap()
                .generate(request, activity)
                .await
                .unwrap();
            assert!(reply.decision.is_err());
            assert_eq!(reply.retryable, status != 400);
            assert_eq!(reply.retry_after, expected);
            let diagnostics = reply.diagnostics.unwrap();
            assert_eq!(diagnostics.category, RequestCategory::HttpStatus);
            assert_eq!(diagnostics.client_request_id, activity);
            assert_eq!(diagnostics.http_status, Some(status));
            assert_eq!(diagnostics.request_id.as_deref(), Some("req_limited"));
            assert_eq!(reply.usage.unwrap().input_tokens, 8);
            server.join().unwrap();
        }
    }
}

#[tokio::test]
async fn transport_timeouts_connection_failures_and_response_errors_are_distinct_and_safe() {
    use act0r::domain::RequestCategory;
    for headers_first in [false, true] {
        let (endpoint, server) = server_options(
            200,
            response().to_string(),
            None,
            "X-Request-Id: req_slow\r\n",
            300,
            headers_first,
        );
        let mut request = request(endpoint);
        request.target.policy.request_timeout_ms = 50;
        request.target.policy.connect_timeout_ms = 50;
        let reply = ModelRouter::new(None)
            .unwrap()
            .generate(request, ActivityId::new())
            .await
            .unwrap();
        let diagnostics = reply.diagnostics.unwrap();
        assert_eq!(diagnostics.category, RequestCategory::Timeout);
        assert_eq!(diagnostics.http_status, headers_first.then_some(200));
        assert!(diagnostics.elapsed_ms >= 40);
        assert!(reply.retryable);
        assert!(reply.usage.is_none());
        server.join().unwrap();
    }
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let reply = ModelRouter::new(None)
        .unwrap()
        .generate(
            request(format!("http://{address}/v1/responses")),
            ActivityId::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        reply.diagnostics.unwrap().category,
        RequestCategory::Connection
    );
    assert!(reply.retryable);
    for (body, category) in [("private-non-json-body".into(), RequestCategory::InvalidJson),
        (json!({"id":"fixture-key", "status":"completed","output":[{"content":[{"type":"output_text","text":"invalid action containing fixture-key"}]}]}).to_string(), RequestCategory::InvalidAction)] {
        let (endpoint, server) = server_options(200, body, Some("fixture-key"), "X-Request-Id: fixture-key\r\n", 0, false);
        let reply = ModelRouter::new(Some("fixture-key".into())).unwrap().generate(request(endpoint), ActivityId::new()).await.unwrap();
        let diagnostics = reply.diagnostics.unwrap();
        assert_eq!(diagnostics.category, category);
        assert!(diagnostics.request_id.is_none());
        assert!(!reply.decision.err().unwrap().contains("fixture-key"));
        assert_eq!(reply.retryable, category == RequestCategory::InvalidAction); server.join().unwrap();
    }
}

#[tokio::test]
async fn http_model_deadline_outlives_a_shorter_worker_timeout_and_saves_its_receipt() {
    use act0r::{
        Engine,
        adapters::{
            agent_executor::AgentExecutor,
            file_lock::FileOwnership,
            sqlite::SqliteStore,
            tokio_worker::{self, WorkerOptions},
            workspace::LocalTools,
        },
        domain::*,
    };
    use std::{sync::Arc, time::Duration};
    let mut body = response();
    body["output"][0]["content"][0]["text"] =
        json!("{\"action\":{\"kind\":\"ask_human\",\"question\":\"Continue?\"}}");
    let (endpoint, server) = server_options(
        200,
        body.to_string(),
        None,
        "X-Request-Id: req_aligned\r\n",
        150,
        false,
    );
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("deadline.db");
    let engine = Engine::new(
        Arc::new(SqliteStore::open(&db).await.unwrap()),
        Arc::new(AgentExecutor::new(
            Arc::new(ModelRouter::new(None).unwrap()),
            Arc::new(LocalTools::new(directory.path().into())),
            Arc::new(common::TestExecutor::default()),
        )),
        Arc::new(common::ManualClock::new(1000)),
        Arc::new(FileOwnership::new(&db)),
        vec![Arc::new(AgentWorkflow)],
    )
    .unwrap();
    let mut cfg: AgentConfig =
        serde_json::from_str(include_str!("../examples/configs/agent-scripted.json")).unwrap();
    cfg.workspace = directory.path().to_string_lossy().into_owned();
    cfg.target = request(endpoint).target;
    cfg.target.policy.request_timeout_ms = 500;
    cfg.target.policy.connect_timeout_ms = 500;
    cfg.target.policy.max_attempts = 1;
    let id = engine
        .start(AgentWorkflow.id(), serde_json::to_value(cfg).unwrap(), 4)
        .await
        .unwrap();
    tokio_worker::run(
        engine.worker().await.unwrap(),
        WorkerOptions {
            until_idle: true,
            poll_interval: Duration::from_millis(1),
            activity_timeout: Duration::from_millis(10),
            shutdown_grace: Duration::ZERO,
            max_concurrent_activities: 1,
        },
    )
    .await
    .unwrap();
    let run = engine.status(id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Waiting);
    assert_eq!(run.attempts().len(), 1);
    let receipt = run.attempts()[0]
        .outcome
        .as_ref()
        .unwrap()
        .result()
        .unwrap();
    assert_eq!(receipt["diagnostics"]["category"], "success");
    assert_eq!(receipt["diagnostics"]["http_status"], 200);
    assert_eq!(receipt["diagnostics"]["request_id"], "req_aligned");
    assert_eq!(run.record().usage.actual_input_tokens, 40);
    server.join().unwrap();
}

#[test]
fn model_policy_defaults_and_invalid_limits_are_checked() {
    let old = json!({"provider":"openai","model":"minimal","endpoint":"https://provider.example/v1/responses","protocol":"responses"});
    let target: ModelTarget = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(serde_json::to_value(target).unwrap(), old);
    for policy in [
        ModelPolicy {
            request_timeout_ms: 0,
            ..Default::default()
        },
        ModelPolicy {
            request_timeout_ms: 600001,
            ..Default::default()
        },
        ModelPolicy {
            connect_timeout_ms: 45001,
            ..Default::default()
        },
        ModelPolicy {
            max_attempts: 9,
            ..Default::default()
        },
        ModelPolicy {
            retry_delay_ms: 2001,
            ..Default::default()
        },
    ] {
        assert!(policy.validate().is_err());
    }
    let old = json!({"code":"transient","message":"later","retryable":true});
    let failure: act0r::domain::Failure = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(serde_json::to_value(failure).unwrap(), old);
}
