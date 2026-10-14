use act0r::{
    adapters::{agents::PlannerAgentAdapter, native_tools::EchoTool, sqlite::SqliteStore},
    application::{
        ToolRegistry,
        ports::{AgentAdapter, ExecutionStore, NativeTool},
    },
    domain::{agent::*, planner::PlannerWorkflow, *},
};
use serde_json::{Value, json};
use std::sync::Arc;

fn fixture(version: Option<u32>) -> (PlannerWorkflow, Run) {
    let mut tool = EchoTool.definition();
    tool.requires_approval = true;
    let workflow = PlannerWorkflow::new(vec![tool.clone()]);
    let mut config = json!({"goal":"Coordinate and echo evidence","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":null,"max_tokens":null,"max_output_tokens":512,"context":{},
        "workers":[{"name":"helper","agent_id":AgentId::new(),"description":"Read-only helper"}],
        "delegation":{"max_tasks":2,"max_depth":2},"tools":[tool.id],"tool_definitions":[tool]});
    if let Some(v) = version {
        config["model_contract_version"] = json!(v);
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

#[test]
fn planners_use_only_flat_tools() {
    let adapter = PlannerAgentAdapter::new(Arc::new(
        ToolRegistry::new(vec![Arc::new(EchoTool)]).unwrap(),
    ));
    let (_, old) = fixture(None);
    let mut saved = old.record().input.clone();
    saved["tool_definitions"][0]["requires_approval"] = json!(false);
    let before = saved.clone();
    adapter.check_config(&saved).unwrap();
    assert_eq!(saved, before);
    let mut fresh = saved.clone();
    fresh.as_object_mut().unwrap().remove("tool_definitions");
    assert_eq!(
        adapter.pin_config(fresh.clone(), None).unwrap()["model_contract_version"],
        4
    );
    fresh["model_contract_version"] = json!(1);
    assert!(adapter.pin_config(fresh, None).is_err());
    assert!(request(&old).native_tool_flattened);
}

#[tokio::test]
async fn reopening_keeps_original_planner_request_and_pin_verbatim() {
    let (_, old) = fixture(None);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("planner.db");
    let store = SqliteStore::open(&db).await.unwrap();
    store.create(old.clone()).await.unwrap();
    store.close().await;
    let store = SqliteStore::open(&db).await.unwrap();
    let restored = store.get(old.record().id).await.unwrap();
    assert_eq!(json!(restored.record()), json!(old.record()));
    assert_eq!(json!(restored.activities()), json!(old.activities()));
    assert_eq!(json!(request(&restored)), json!(request(&old)));
}

#[tokio::test]
async fn flat_echo_survives_approval_restart_with_canonical_saved_results() {
    let (workflow, mut run) = fixture(Some(4));
    let first = request(&run);
    assert_eq!(first.native_tool_names, vec!["echo"]);
    assert!(first.native_tool_flattened);
    assert_eq!(
        first.schema["properties"]["action"]["anyOf"][0]["properties"]["kind"]["enum"],
        json!(["echo"])
    );
    let prompt: Value = serde_json::from_str(&first.prompt).unwrap();
    assert_eq!(prompt["tools"][0]["action_kind"], "echo");
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
    assert_eq!(wait.payload["arguments"], json!({"text":"saved"}));
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("approval.db");
    let store = SqliteStore::open(&db).await.unwrap();
    store.create(run.clone()).await.unwrap();
    store.close().await;
    let store = SqliteStore::open(&db).await.unwrap();
    let mut restored = store.get(run.record().id).await.unwrap();
    assert_eq!(json!(restored.record()), json!(run.record()));
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
    assert_eq!(next.schema, first.schema);
    let prompt: Value = serde_json::from_str(&next.prompt).unwrap();
    assert_eq!(
        prompt["observations"][0]["action"],
        json!({"kind":"echo","text":"saved"})
    );
    assert_eq!(
        restored.activities()[0].result.as_ref().unwrap()["action"]["kind"],
        "call_tool"
    );
    assert_eq!(
        restored
            .record()
            .usage
            .budget
            .as_ref()
            .unwrap()
            .max_model_calls,
        None
    );
    assert_eq!(
        restored.record().usage.budget.as_ref().unwrap().max_tokens,
        None
    );
}

#[test]
fn unsupported_versions_and_ambiguous_flat_tool_names_fail_before_admission() {
    let (workflow, old) = fixture(None);
    for version in [0, 1, 2, 3, 5, u32::MAX] {
        let mut input = old.record().input.clone();
        input["model_contract_version"] = json!(version);
        assert!(Run::start(&workflow, input, 30, 0).is_err());
    }
    for name in [
        "finish",
        "ask_human",
        "finish_incomplete",
        "delegate_task",
        "delegate_tasks",
        "update_plan",
        "schedule_follow_up",
        "inspect_child_changes",
        "import_child_changes",
        "inspect_worker_changes",
        "import_worker_changes",
        "call_tool",
    ] {
        let mut tool = EchoTool.definition();
        tool.id.name = name.into();
        let workflow = PlannerWorkflow::new(vec![tool.clone()]);
        let mut input = old.record().input.clone();
        input["tools"] = json!([tool.id]);
        input["tool_definitions"] = json!([tool]);
        input["model_contract_version"] = json!(4);
        assert!(
            Run::start(&workflow, input.clone(), 30, 0).is_err(),
            "accepted {name}"
        );
        input
            .as_object_mut()
            .unwrap()
            .remove("model_contract_version");
        assert!(Run::start(&workflow, input, 30, 0).is_err());
    }
    let mut tool = EchoTool.definition();
    tool.parameters
        .insert("kind".into(), tool.parameters["text"].clone());
    let workflow = PlannerWorkflow::new(vec![tool.clone()]);
    let mut input = old.record().input.clone();
    input["tools"] = json!([tool.id]);
    input["tool_definitions"] = json!([tool]);
    input["model_contract_version"] = json!(4);
    assert!(Run::start(&workflow, input, 30, 0).is_err());
}
