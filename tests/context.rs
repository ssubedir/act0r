use act0r::{
    bootstrap::local_app,
    domain::{agent::*, *},
};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant},
};

fn config(protocol: ModelProtocol) -> AgentConfig {
    let mut config: AgentConfig =
        serde_json::from_str(include_str!("../examples/configs/agent-scripted.json")).unwrap();
    config.workspace = "managed".into();
    config.require_approval = false;
    config.max_model_calls = Some(100);
    config.max_tokens = Some(1_000_000);
    config.target.protocol = protocol;
    config
}
fn model_request(run: &Run) -> ModelRequest {
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
fn decision(run: &mut Run, action: AgentAction, now: i64) {
    let execution = run.admit(now).unwrap().unwrap();
    assert_eq!(execution.handler.name, "agent.model");
    run.complete(
        &execution,
        Outcome::SuccessWithUsage {
            result: serde_json::to_value(ModelDecision {
                diagnostics: None,
                action,
                request_id: None,
            })
            .unwrap(),
            usage: TokenUsage {
                input_tokens: 10,
                output_tokens: 10,
            },
        },
        &AgentWorkflow,
        now,
    )
    .unwrap();
}

#[test]
fn recent_windows_bound_complete_unicode_observations_and_preserve_full_outcomes() {
    for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
        for observation_limit in [2, 100] {
            let mut cfg = config(protocol);
            cfg.goal = "Inspect \"quoted\" paths and Unicode 🙂\nwithout changing the goal.".into();
            let seed =
                Run::start(&AgentWorkflow, serde_json::to_value(&cfg).unwrap(), 100, 0).unwrap();
            cfg.context.max_input_bytes = model_request(&seed).context_input_bytes() as u32 + 900;
            cfg.context.max_observations = observation_limit;
            let mut run =
                Run::start(&AgentWorkflow, serde_json::to_value(&cfg).unwrap(), 100, 0).unwrap();
            for index in 1..=12 {
                decision(&mut run, AgentAction::ListFiles, index);
                let execution = run.admit(index).unwrap().unwrap();
                let result = json!({"index": index, "text": format!("\"\\\n{}", "🙂".repeat(80))});
                run.complete(
                    &execution,
                    Outcome::Success(result.clone()),
                    &AgentWorkflow,
                    index,
                )
                .unwrap();
                assert_eq!(run.record().status, RunStatus::Running);
                let request = model_request(&run);
                let selection = request.context.as_ref().unwrap();
                let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
                let observations = prompt["observations"].as_array().unwrap();
                assert!(request.context_input_bytes() <= u64::from(cfg.context.max_input_bytes));
                assert_eq!(selection.input_bytes, request.context_input_bytes());
                assert_eq!(selection.total_observations, index as u64);
                assert_eq!(observations.len(), selection.included_observations as usize);
                assert!(observations.len() <= 2);
                assert_eq!(observations.last().unwrap()["result"], result);
                assert_eq!(prompt["goal"], cfg.goal);
                assert_eq!(prompt["writable_paths"], json!(cfg.writable_paths));
                assert_eq!(prompt["test_command"], json!(cfg.test_command));
                assert_eq!(prompt["tests_passed"], false);
                assert_eq!(
                    selection.first_included,
                    Some(index as u64 - observations.len() as u64 + 1)
                );
                assert_eq!(
                    selection.omitted_observations + observations.len() as u64,
                    index as u64
                );
                let state: AgentState = serde_json::from_value(run.record().state.clone()).unwrap();
                assert_eq!(state.observations.len(), observations.len());
                assert_eq!(state.context.selection.as_ref(), Some(selection));
                assert_eq!(
                    run.activities()
                        .iter()
                        .find(|a| a.id == execution.activity_id)
                        .unwrap()
                        .result
                        .as_ref(),
                    Some(&result)
                );
            }
            assert!(model_request(&run).context.unwrap().omitted_observations > 0);
            assert_eq!(
                run.activities()
                    .iter()
                    .filter(|a| a.spec.handler.name == "agent.tool" && a.result.is_some())
                    .count(),
                12
            );
        }
    }
}

#[test]
fn exact_input_limits_include_essential_material_and_overflow_preserves_the_tool_result() {
    for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
        let mut cfg = config(protocol);
        let seed = Run::start(&AgentWorkflow, serde_json::to_value(&cfg).unwrap(), 10, 0).unwrap();
        let bytes = model_request(&seed).context_input_bytes() as u32;
        cfg.context.max_input_bytes = bytes;
        let mut run =
            Run::start(&AgentWorkflow, serde_json::to_value(&cfg).unwrap(), 10, 0).unwrap();
        assert_eq!(model_request(&run).context_input_bytes(), u64::from(bytes));
        cfg.context.max_input_bytes -= 1;
        assert!(matches!(
            Run::start(&AgentWorkflow, serde_json::to_value(&cfg).unwrap(), 10, 0),
            Err(DomainError::ContextLimit { .. })
        ));
        decision(
            &mut run,
            AgentAction::ReadFile {
                path: "src/lib.rs".into(),
            },
            1,
        );
        let tool = run.admit(1).unwrap().unwrap();
        let result = json!({"ok":true,"content":"large result".repeat(1000)});
        run.complete(&tool, Outcome::Success(result.clone()), &AgentWorkflow, 1)
            .unwrap();
        assert_eq!(run.record().status, RunStatus::Failed);
        assert_eq!(run.record().error.as_ref().unwrap().code, "context_limit");
        assert!(
            run.record()
                .error
                .as_ref()
                .unwrap()
                .message
                .contains(&bytes.to_string())
        );
        assert_eq!(
            run.activities().last().unwrap().status,
            ActivityStatus::Succeeded
        );
        assert_eq!(
            run.activities().last().unwrap().result.as_ref(),
            Some(&result)
        );
        assert_eq!(run.activities().len(), 2);
        assert_eq!(run.record().usage.admitted_model_calls, 1);
        assert!(run.admit(2).unwrap().is_none());
    }
}

#[test]
fn default_policies_are_pinned_and_unknown_or_empty_limits_are_rejected() {
    let mut input = serde_json::to_value(config(ModelProtocol::Responses)).unwrap();
    input.as_object_mut().unwrap().remove("context");
    let config: AgentConfig = serde_json::from_value(input.clone()).unwrap();
    assert_eq!(config.context, ContextPolicy::default());
    let run = Run::start(&AgentWorkflow, serde_json::to_value(config).unwrap(), 10, 0).unwrap();
    let policy = model_request(&run).context.unwrap().policy;
    assert_eq!(run.record().workflow.version, 2);
    assert_eq!(policy, ContextPolicy::default());
    assert_eq!(
        run.record().state["config"]["context"],
        serde_json::to_value(policy).unwrap()
    );
    for context in [
        json!({"version":2}),
        json!({"max_input_bytes":0}),
        json!({"max_observations":0}),
    ] {
        input["context"] = context;
        assert!(
            serde_json::from_value::<AgentConfig>(input.clone())
                .unwrap()
                .validate()
                .is_err()
        );
    }
    input["context"] = json!({"max_input_bytes": 12000, "typo": true});
    assert!(serde_json::from_value::<AgentConfig>(input).is_err());
}

#[test]
fn human_inputs_denials_and_test_state_survive_observation_eviction() {
    let mut cfg = config(ModelProtocol::Responses);
    cfg.context.max_observations = 1;
    cfg.require_approval = true;
    let mut run = Run::start(&AgentWorkflow, serde_json::to_value(cfg).unwrap(), 30, 0).unwrap();
    decision(
        &mut run,
        AgentAction::AskHuman {
            question: "Which behavior should add have?".into(),
        },
        1,
    );
    let wait = run.record().wait.as_ref().unwrap().id;
    run.signal(
        wait,
        json!({"answer":"Add both integers."}),
        &AgentWorkflow,
        2,
    )
    .unwrap();
    decision(
        &mut run,
        AgentAction::ReplaceText {
            path: "src/lib.rs".into(),
            old: "old".into(),
            new: "new".into(),
        },
        3,
    );
    let wait = run.record().wait.as_ref().unwrap().id;
    run.signal(wait, json!({"approved":false}), &AgentWorkflow, 4)
        .unwrap();
    let snapshot = serde_json::to_value(&run).unwrap();
    run.signal(wait, json!({"approved":false}), &AgentWorkflow, 5)
        .unwrap();
    assert_eq!(snapshot, serde_json::to_value(&run).unwrap());
    let prompt: Value = serde_json::from_str(&model_request(&run).prompt).unwrap();
    assert_eq!(prompt["observations"].as_array().unwrap().len(), 1);
    assert_eq!(prompt["observations"][0]["result"]["approved"], false);
    assert_eq!(run.record().input_receipts.len(), 2);
    assert_eq!(run.record().state["context"]["total_observations"], 2);

    decision(&mut run, AgentAction::RunTests, 6);
    let wait = run.record().wait.as_ref().unwrap().id;
    run.signal(wait, json!({"approved":true}), &AgentWorkflow, 7)
        .unwrap();
    let test = run.admit(7).unwrap().unwrap();
    run.complete(
        &test,
        Outcome::Success(json!({"ok":true,"exit_code":0})),
        &AgentWorkflow,
        7,
    )
    .unwrap();
    for now in 8..11 {
        decision(&mut run, AgentAction::ListFiles, now);
        let tool = run.admit(now).unwrap().unwrap();
        run.complete(
            &tool,
            Outcome::Success(json!({"files":[]})),
            &AgentWorkflow,
            now,
        )
        .unwrap();
    }
    let prompt: Value = serde_json::from_str(&model_request(&run).prompt).unwrap();
    assert_eq!(prompt["tests_passed"], true);
    assert_eq!(prompt["observations"][0]["action"]["kind"], "list_files");
    assert!(model_request(&run).context.unwrap().omitted_observations > 0);
    decision(
        &mut run,
        AgentAction::Finish {
            summary: "Verified tests despite evicted test output.".into(),
        },
        11,
    );
    assert_eq!(run.record().status, RunStatus::Succeeded);
}

#[tokio::test]
async fn long_coding_run_keeps_small_context_and_recovers_the_identical_saved_request() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("context.db");
    let (mut engine, agents) = local_app(&db).await.unwrap();
    let mut cfg = config(ModelProtocol::ChatCompletions);
    cfg.context.max_input_bytes = 12000;
    cfg.context.max_observations = 3;
    cfg.script = vec![AgentAction::ListFiles; 24];
    cfg.script.extend([
        AgentAction::ReadFile {
            path: "src/lib.rs".into(),
        },
        AgentAction::ReplaceText {
            path: "src/lib.rs".into(),
            old: "left - right".into(),
            new: "left + right".into(),
        },
        AgentAction::RunTests,
        AgentAction::Finish {
            summary: "Fixed addition and verified the tests.".into(),
        },
    ]);
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/coding-task");
    let id = agents.start(source, cfg.clone(), 100).await.unwrap();
    drop(agents);
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..32 {
        assert!(worker.step().await.unwrap());
    }
    let execution = worker.admit().await.unwrap().unwrap();
    let saved = execution.input.clone();
    let request: ModelRequest = serde_json::from_value(saved.clone()).unwrap();
    assert!(request.context.as_ref().unwrap().omitted_observations > 0);
    drop(worker);
    drop(engine);
    engine = local_app(&db).await.unwrap().0;
    let mut worker = engine.worker().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let retry = loop {
        if let Some(execution) = worker.admit().await.unwrap() {
            break execution;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(retry.activity_id, execution.activity_id);
    assert_ne!(retry.attempt_id, execution.attempt_id);
    assert_eq!(retry.input, saved);
    let outcome = worker.execute(retry.clone()).await.unwrap();
    worker.complete(retry, outcome).await.unwrap();
    for _ in 0..100 {
        if !worker.step().await.unwrap() {
            break;
        }
    }
    let run = engine.status(id).await.unwrap();
    assert_eq!(
        run.record().status,
        RunStatus::Succeeded,
        "{:?}",
        run.record().error
    );
    assert_eq!(run.record().result.as_ref().unwrap()["tests_passed"], true);
    assert_eq!(
        run.record().usage.admitted_model_calls,
        cfg.script.len() as u32 + 1
    );
    assert_eq!(run.record().usage.uncertain_calls, 1);
    let state: AgentState = serde_json::from_value(run.record().state.clone()).unwrap();
    assert!(state.observations.len() <= 3);
    assert_eq!(state.context.total_observations, 27);
    assert!(run.record().usage.held_tokens > 0);
    let history = engine.history(id).await.unwrap();
    let requests: Vec<ModelRequest> = history
        .iter()
        .filter(|e| e.kind == "activity_scheduled" && e.payload["handler"]["name"] == "agent.model")
        .map(|e| serde_json::from_value(e.payload["input"].clone()).unwrap())
        .collect();
    assert_eq!(requests.len(), cfg.script.len());
    for request in requests {
        let selection = request.context.as_ref().unwrap();
        assert!(selection.input_bytes <= u64::from(cfg.context.max_input_bytes));
        assert_eq!(selection.input_bytes, request.context_input_bytes());
        assert!(selection.included_observations <= 3);
    }
    assert_eq!(
        run.activities()
            .iter()
            .filter(|a| a.spec.handler.name == "agent.tool" && a.result.is_some())
            .count(),
        27
    );
    assert!(
        history
            .iter()
            .any(|e| e.kind == "attempt_finished"
                && e.payload["outcome"]["value"]["files"].is_array())
    );
}
