use act0r::{
    adapters::{
        native_tools::{EchoTool, SumTool},
        sqlite::SqliteStore,
    },
    application::ports::{ExecutionStore, NativeTool},
    domain::{agent::*, answer::ToolAgentWorkflow, *},
};
use serde_json::json;

fn fixture(version: Option<u32>) -> (ToolAgentWorkflow, Run) {
    let mut echo = EchoTool.definition();
    echo.requires_approval = true;
    let sum = SumTool.definition();
    let workflow = ToolAgentWorkflow::new(vec![echo.clone(), sum.clone()]);
    let mut config = json!({"goal":"Echo the assigned text", "target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":null,"max_tokens":null,"max_output_tokens":512,"context":{},"clarification_policy":{"version":1,"max_consecutive_questions":3},"tools":[echo.id,sum.id],"tool_definitions":[echo,sum]});
    if let Some(version) = version {
        config["model_contract_version"] = json!(version);
    }
    let run = Run::start(&workflow, config, 30, 0).unwrap();
    (workflow, run)
}
fn request(run: &Run) -> ModelRequest {
    serde_json::from_value(
        run.activities()
            .iter()
            .rev()
            .find(|a| a.spec.handler.name == "agent.model")
            .unwrap()
            .spec
            .input
            .clone(),
    )
    .unwrap()
}

#[tokio::test]
async fn pinned_contract_survives_approval_restart_without_changing_dispatch_or_scope() {
    let version = 4;

    let (workflow, mut run) = fixture(Some(version));
    let execution = run.admit(0).unwrap().unwrap();
    run.complete(
        &execution,
        Outcome::Success(json!(ModelDecision {
            action: AgentAction::CallTool {
                tool: "echo".into(),
                arguments: json!({"text":"saved"})
            },
            request_id: None,
            diagnostics: None
        })),
        &workflow,
        0,
    )
    .unwrap();
    let wait = run.record().wait.clone().unwrap();
    assert_eq!(wait.key, "native_tool_approval");
    assert_eq!(wait.payload["tool"]["name"], "echo");
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("approval.db");
    let store = SqliteStore::open(&path).await.unwrap();
    store.create(run.clone()).await.unwrap();
    store.close().await;
    let store = SqliteStore::open(&path).await.unwrap();
    let mut restored = store.get(run.record().id).await.unwrap();
    assert_eq!(restored.record().wait.as_ref().unwrap().id, wait.id);
    restored
        .signal(wait.id, json!({"approved":true}), &workflow, 0)
        .unwrap();
    let tool = restored.admit(0).unwrap().unwrap();
    assert_eq!(tool.handler.name, "native.tool");
    assert_eq!(tool.input["arguments"], json!({"text":"saved"}));
    restored
        .complete(
            &tool,
            Outcome::Success(json!({"text":"saved"})),
            &workflow,
            0,
        )
        .unwrap();
    let next = request(&restored);
    assert_eq!(next.schema, request(&run).schema);

    assert_eq!(
        next.schema["properties"]["action"]["anyOf"][0]["properties"]["kind"]["enum"][0],
        "echo"
    );
    assert!(next.native_tool_flattened);
    assert_eq!(next.native_tool_names, vec!["echo", "sum"]);

    assert_eq!(restored.record().input["model_contract_version"], version);
}

#[test]
fn unsupported_contract_versions_fail_before_any_model_admission() {
    let (workflow, run) = fixture(None);
    for version in [0, 1, 2, 3, 5, u32::MAX] {
        let mut input = run.record().input.clone();
        input["model_contract_version"] = json!(version);
        assert!(Run::start(&workflow, input, 30, 0).is_err());
    }
}

#[test]
fn flat_contract_rejects_control_names_and_kind_parameters() {
    let (_, run) = fixture(None);
    for name in ["finish", "ask_human", "finish_incomplete"] {
        let mut tool = EchoTool.definition();
        tool.id.name = name.into();
        let workflow = ToolAgentWorkflow::new(vec![tool.clone()]);
        let mut input = run.record().input.clone();
        input["tools"] = json!([tool.id]);
        input["tool_definitions"] = json!([tool]);
        input["model_contract_version"] = json!(4);
        assert!(Run::start(&workflow, input.clone(), 30, 0).is_err());
        input
            .as_object_mut()
            .unwrap()
            .remove("model_contract_version");
        assert!(Run::start(&workflow, input, 30, 0).is_err());
    }
    let mut tool = EchoTool.definition();
    tool.parameters
        .insert("kind".into(), tool.parameters["text"].clone());
    let workflow = ToolAgentWorkflow::new(vec![tool.clone()]);
    let mut input = run.record().input.clone();
    input["tools"] = json!([tool.id]);
    input["tool_definitions"] = json!([tool]);
    input["model_contract_version"] = json!(4);
    assert!(Run::start(&workflow, input.clone(), 30, 0).is_err());
    input
        .as_object_mut()
        .unwrap()
        .remove("model_contract_version");
    assert!(Run::start(&workflow, input, 30, 0).is_err());
}

#[test]
fn adapter_uses_only_the_flat_native_contract() {
    use act0r::{
        adapters::agents::ToolAgentAdapter,
        application::{ToolRegistry, ports::AgentAdapter},
    };
    use std::sync::Arc;
    let tools = ToolRegistry::new(vec![Arc::new(EchoTool), Arc::new(SumTool)]).unwrap();
    let adapter = ToolAgentAdapter::new(Arc::new(tools));
    let (_, run) = fixture(None);
    let mut original = run.record().input.clone();
    original["tool_definitions"][0]["requires_approval"] = json!(false);
    let before = original.clone();
    adapter.check_config(&original).unwrap();
    assert_eq!(original.get("model_contract_version"), None);
    let mut fresh = original.clone();
    fresh.as_object_mut().unwrap().remove("tool_definitions");
    let pinned = adapter.pin_config(fresh.clone(), None).unwrap();
    assert_eq!(pinned["model_contract_version"], 4);
    fresh["model_contract_version"] = json!(1);
    assert!(adapter.pin_config(fresh, None).is_err());
    adapter.check_config(&original).unwrap();
    assert_eq!(original, before);
}
