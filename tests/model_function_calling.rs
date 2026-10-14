use act0r::{
    adapters::model::ModelRouter,
    adapters::{workspace::LocalTools, workspace_reads::ReadFilesTool},
    application::ports::ModelClient,
    application::ports::NativeTool,
    domain::{ActivityId, RequestCategory, agent::*},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn native_functions_preserve_bounded_string_array_parameters_and_arguments() {
    let definition = ReadFilesTool::new(Arc::new(LocalTools::new("unused".into()))).definition();
    let mut request = native_request();
    let mut action = definition.input_schema();
    action["properties"]["kind"] = json!({"type":"string","enum":["read_files"]});
    action["required"] = json!(["kind", "paths"]);
    request.schema["properties"]["action"]["anyOf"][0] = action;
    request.native_tool_names = vec!["read_files".into()];
    let (reply, sent) = exchange(
        request,
        response("read_files", r#"{"paths":["README.md","src/lib.rs"]}"#),
    )
    .await;
    let paths = &sent["tools"][0]["function"]["parameters"]["properties"]["paths"];
    assert_eq!(paths["type"], "array");
    assert_eq!(paths["items"]["type"], "string");
    assert_eq!(paths["maxItems"], 8);
    assert_eq!(
        reply.decision.unwrap().action,
        AgentAction::CallTool {
            tool: "read_files".into(),
            arguments: json!({"paths":["README.md","src/lib.rs"]})
        }
    );
}

fn request() -> ModelRequest {
    serde_json::from_value(json!({
        "target":{"provider":"openai","model":"fixture","endpoint":"http://127.0.0.1:1/v1/chat/completions","protocol":"chat_completions","action_format":"function_calling"},
        "prompt":"Use saved evidence","instructions":"Act on the goal. Return only JSON with one action matching the supplied schema.",
        "schema":action_schema(),"max_output_tokens":512,"turn":0,"script":[],"scripted_delay_ms":0
    })).unwrap()
}
fn native_request() -> ModelRequest {
    let mut request = request();
    request.native_tool_flattened = true;
    request.native_tool_names = vec!["echo".into()];
    request.schema["properties"]["action"] = json!({"anyOf":[
        {"type":"object","properties":{"kind":{"type":"string","enum":["echo"]},"text":{"type":"string"}},"required":["kind","text"],"additionalProperties":false},
        {"type":"object","properties":{"kind":{"type":"string","enum":["finish"]},"summary":{"type":"string"}},"required":["kind","summary"],"additionalProperties":false}
    ]});
    request
}
fn response(name: &str, arguments: &str) -> Value {
    json!({"id":"function-receipt","usage":{"prompt_tokens":21,"completion_tokens":7},"choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-fixture","type":"function","function":{"name":name,"arguments":arguments}}]}}]})
}
async fn exchange(mut request: ModelRequest, response: Value) -> (ModelReply, Value) {
    let seen = Arc::new(Mutex::new(None));
    let copy = seen.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    request.target.endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                *copy.lock().unwrap() = Some(body);
                let response = response.clone();
                async move { axum::Json(response) }
            }),
        );
        axum::serve(listener, app).await.unwrap();
    });
    let reply = ModelRouter::new(None)
        .unwrap()
        .generate(request, ActivityId::new())
        .await
        .unwrap();
    let sent = seen.lock().unwrap().clone().unwrap();
    server.abort();
    (reply, sent)
}

#[tokio::test]
async fn functions_project_the_saved_schema_and_normalize_canonical_decisions() {
    let request = native_request();
    let exported = ModelRouter::request_body(&request);
    let (reply, sent) = exchange(
        request.clone(),
        response("echo", r#"{"text":"saved evidence"}"#),
    )
    .await;
    assert_eq!(sent, exported);
    assert!(sent.get("response_format").is_none());
    assert_eq!(sent["tool_choice"], "required");
    assert_eq!(sent["parallel_tool_calls"], false);
    assert_eq!(sent["tools"][0]["function"]["name"], "echo");
    assert_eq!(
        sent["tools"][0]["function"]["parameters"]["required"],
        json!(["text"])
    );
    assert!(
        sent["tools"][0]["function"]["parameters"]["properties"]
            .get("kind")
            .is_none()
    );
    assert!(
        !sent["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("Return only JSON")
    );
    assert_eq!(
        reply.decision.unwrap().action,
        AgentAction::CallTool {
            tool: "echo".into(),
            arguments: json!({"text":"saved evidence"})
        }
    );
    assert_eq!(reply.usage.unwrap().input_tokens, 21);
    let material = sent["messages"][0]["content"].as_str().unwrap().len()
        + sent["messages"][1]["content"].as_str().unwrap().len()
        + sent["tools"].to_string().len();
    assert!(request.context_input_bytes() >= material as u64);
    assert!(request.reservation().input_tokens >= material as u64 + 2048);
    let (reply, _) = exchange(
        request,
        response("finish", r#"{"summary":"Evidence used"}"#),
    )
    .await;
    assert_eq!(
        reply.decision.unwrap().action,
        AgentAction::Finish {
            summary: "Evidence used".into()
        }
    );
}

#[tokio::test]
async fn malformed_unknown_parallel_and_text_fallback_replies_never_select_an_action() {
    let mut parallel = response("echo", r#"{"text":"saved"}"#);
    let calls = parallel["choices"][0]["message"]["tool_calls"]
        .as_array_mut()
        .unwrap();
    calls.push(calls[0].clone());
    let mut wrong_type = response("echo", "{}");
    wrong_type["choices"][0]["message"]["tool_calls"][0]["type"] = json!("unsupported");
    let mut no_id = response("echo", "{}");
    no_id["choices"][0]["message"]["tool_calls"][0]
        .as_object_mut()
        .unwrap()
        .remove("id");
    for body in [
        response("unknown", "{}"),
        response("echo", "{"),
        response("echo", "[]"),
        response("echo", r#"{"text":"first","text":"second"}"#),
        response("echo", r#"{"text":"saved","kind":"finish"}"#),
        response("echo", "{}"),
        response("echo", r#"{"text":"saved","extra":true}"#),
        response(
            "call_tool",
            r#"{"tool":"echo","arguments":{"text":"saved"}}"#,
        ),
        response("echo", r#"{"text":"saved"} trailing"#),
        parallel,
        wrong_type,
        no_id,
        json!({"usage":{"prompt_tokens":21,"completion_tokens":7},"choices":[{"finish_reason":"stop","message":{"content":"{\"action\":{\"kind\":\"echo\",\"text\":\"saved\"}}"}}]}),
    ] {
        let (reply, _) = exchange(native_request(), body).await;
        assert!(reply.decision.is_err());
        assert_eq!(
            reply.diagnostics.unwrap().category,
            RequestCategory::InvalidAction
        );
        assert!(reply.retryable);
        assert_eq!(reply.usage.unwrap().output_tokens, 7);
    }
    let mut length = response("echo", r#"{"text":"saved"}"#);
    length["choices"][0]["finish_reason"] = json!("length");
    let (reply, _) = exchange(native_request(), length).await;
    assert!(reply.decision.is_err());
    assert_eq!(
        reply.diagnostics.unwrap().category,
        RequestCategory::Incomplete
    );
    assert_eq!(reply.usage.unwrap().input_tokens, 21);
    let mut minimal = native_request();
    minimal.target.action_format = ModelActionFormat::JsonSchema;
    let (reply, sent) = exchange(minimal, response("echo", r#"{"text":"saved"}"#)).await;
    assert!(reply.decision.is_err());
    assert!(sent.get("tools").is_none());
}

#[tokio::test]
async fn scoped_delegation_and_single_coordination_keep_the_current_allowed_actions() {
    let mut request = request();
    let plain = json!({"type":"object","properties":{"kind":{"type":"string","enum":["delegate_task"]},"worker":{"type":"string","enum":["repairer"]},"goal":{"type":"string"}},"required":["kind","worker","goal"],"additionalProperties":false});
    let mut scoped = plain.clone();
    scoped["properties"]["writable_paths"] = json!({"type":"array","items":{"type":"string"}});
    scoped["required"]
        .as_array_mut()
        .unwrap()
        .push(json!("writable_paths"));
    request.schema["properties"]["action"] = json!({"anyOf":[plain, scoped]});
    let functions = request.function_actions().unwrap();
    assert_eq!(functions.len(), 1);
    assert_eq!(
        functions[0].parameters["required"],
        json!(["worker", "goal"])
    );
    for arguments in [
        r#"{"worker":"repairer","goal":"Repair"}"#,
        r#"{"worker":"repairer","goal":"Repair","writable_paths":["src/lib.rs"]}"#,
    ] {
        let (reply, _) = exchange(request.clone(), response("delegate_task", arguments)).await;
        assert!(matches!(
            reply.decision.unwrap().action,
            AgentAction::DelegateTask { .. }
        ));
    }
    request.native_tool_flattened = true;
    request.native_tool_names = vec!["reply_worker".into()];
    request.schema["properties"]["action"] = json!({"type":"object","properties":{"kind":{"type":"string","enum":["reply_worker"]},"question_id":{"type":"string"},"text":{"type":"string"}},"required":["kind","question_id","text"],"additionalProperties":false});
    let (reply, sent) = exchange(
        request.clone(),
        response(
            "reply_worker",
            r#"{"question_id":"saved-question","text":"Proceed within your scope"}"#,
        ),
    )
    .await;
    assert_eq!(sent["tools"].as_array().unwrap().len(), 1);
    assert!(
        matches!(reply.decision.unwrap().action, AgentAction::CallTool { tool, .. } if tool == "reply_worker")
    );
    let (reply, _) = exchange(
        request,
        response("finish", r#"{"summary":"Unavailable now"}"#),
    )
    .await;
    assert!(reply.decision.is_err());
}

#[test]
fn mode_is_explicit_pinned_and_rejects_unsupported_targets() {
    let mut request = request();
    request.target.validate().unwrap();
    let serialized = json!(request);
    let restored: ModelRequest = serde_json::from_value(serialized.clone()).unwrap();
    assert_eq!(json!(restored), serialized);
    assert_eq!(
        ModelRouter::request_body(&restored),
        ModelRouter::request_body(&request)
    );
    request.target.protocol = ModelProtocol::Responses;
    assert!(request.target.validate().is_err());
    request.target.protocol = ModelProtocol::ChatCompletions;
    request.target.provider = "scripted".into();
    assert!(request.target.validate().is_err());
    request.target.action_format = ModelActionFormat::JsonSchema;
    assert!(json!(request.target).get("action_format").is_none());
    let old = json!({"provider":"openai","model":"minimal","endpoint":"http://127.0.0.1:1","protocol":"chat_completions"});
    let old_target: ModelTarget = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(json!(old_target), old);
}

#[test]
fn native_actor_configuration_requires_flat_contract_before_preparing_work() {
    use act0r::{
        adapters::{
            agents::{PlannerAgentAdapter, ToolAgentAdapter},
            native_tools::EchoTool,
        },
        application::{ToolRegistry, ports::AgentAdapter},
        domain::AgentId,
    };
    let registry = Arc::new(ToolRegistry::new(vec![Arc::new(EchoTool)]).unwrap());
    let tool = ToolAgentAdapter::new(registry.clone());
    let planner = PlannerAgentAdapter::new(registry);
    let config = json!({"goal":"Echo evidence", "target":request().target,
        "max_model_calls":null,"max_tokens":null,"max_output_tokens":512,"tools":[{"name":"echo","version":1}]});
    for planner_mode in [false, true] {
        let adapter: &dyn AgentAdapter = if planner_mode { &planner } else { &tool };
        let mut input = config.clone();
        if planner_mode {
            input["workers"] = json!([{"name":"helper","agent_id":AgentId::new(),"description":"Read-only helper"}]);
            input["delegation"] = json!({"max_tasks":1,"max_depth":1});
        }
        let pinned = adapter.pin_config(input.clone(), None).unwrap();
        assert_eq!(pinned["model_contract_version"], 4);
        assert_eq!(pinned["target"]["action_format"], "function_calling");
        adapter.check_config(&pinned).unwrap();
        input["model_contract_version"] = json!(1);
        assert!(adapter.pin_config(input, None).is_err());
    }
}
