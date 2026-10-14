use act0r::{
    adapters::{
        native_tools::{EchoTool, ReadFileTool, ReplaceTextTool, RunTestsTool},
        workspace::LocalTools,
    },
    application::ports::NativeTool,
    domain::{
        agent::*, answer::ToolAgentWorkflow, native_context::NativeContextState,
        planner::PlannerWorkflow, tools::ToolDefinition, *,
    },
};
use serde_json::{Value, json};
use std::sync::Arc;

fn definitions() -> Vec<ToolDefinition> {
    let runner = Arc::new(LocalTools::new("managed-root".into()));
    vec![
        EchoTool.definition(),
        ReadFileTool::new(runner.clone()).definition(),
        ReplaceTextTool::new(runner.clone()).definition(),
        RunTestsTool::new(runner).definition(),
    ]
}
fn workflow(policy: &str) -> Box<dyn WorkflowDefinition> {
    if policy == "planner" {
        Box::new(PlannerWorkflow::new(definitions()))
    } else {
        Box::new(ToolAgentWorkflow::new(definitions()))
    }
}
fn config(policy: &str, protocol: ModelProtocol, bounded: bool) -> Value {
    let definitions = definitions();
    let mut config = json!({"goal":"Inspect \"quoted\" Unicode 🙂\nproject evidence",
        "target":{"provider":"scripted","model":"fixture","endpoint":"","protocol":protocol},
        "max_model_calls":100,"max_tokens":2000000,"max_output_tokens":512,"max_input_bytes":65536,
        "tools":definitions.iter().map(|d|&d.id).collect::<Vec<_>>(),"tool_definitions":definitions,
        "workspace":"managed-workspace","writable_paths":["src/"],"test_command":{"argv":["fixed-tests"],"timeout_ms":60000}});
    if bounded {
        config["context"] = json!({"max_observations":2});
    }
    if policy == "planner" {
        config["workers"] = json!([{"name":"a","agent_id":AgentId::new(),"description":"First worker"},
            {"name":"b","agent_id":AgentId::new(),"description":"Second worker"}]);
        config["delegation"] = json!({"max_tasks":4,"max_depth":1,"max_parallel_tasks":2});
    }
    config
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
fn decision(run: &mut Run, workflow: &dyn WorkflowDefinition, action: AgentAction) {
    let execution = run.admit(0).unwrap().unwrap();
    assert_eq!(execution.handler.name, "agent.model");
    run.complete(
        &execution,
        Outcome::SuccessWithUsage {
            result: json!(ModelDecision {
                action,
                request_id: None,
                diagnostics: None
            }),
            usage: TokenUsage {
                input_tokens: 10,
                output_tokens: 10,
            },
        },
        workflow,
        0,
    )
    .unwrap();
}
fn tool(
    run: &mut Run,
    workflow: &dyn WorkflowDefinition,
    action: AgentAction,
    result: Value,
) -> ActivityId {
    decision(run, workflow, action);
    if let Some(wait) = run.record().wait.as_ref() {
        run.signal(wait.id, json!({"approved":true}), workflow, 0)
            .unwrap();
    }
    let execution = run.admit(0).unwrap().unwrap();
    assert!(act0r::domain::tools::is_native_tool_handler(
        &execution.handler
    ));
    run.complete(&execution, Outcome::Success(result), workflow, 0)
        .unwrap();
    execution.activity_id
}
fn call(name: &str, arguments: Value) -> AgentAction {
    AgentAction::CallTool {
        tool: name.into(),
        arguments,
    }
}

#[test]
fn native_windows_bound_complete_unicode_observations_and_retain_full_history() {
    for policy in ["tool", "planner"] {
        for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
            for max_observations in [2, 100] {
                let workflow = workflow(policy);
                let mut cfg = config(policy, protocol, true);
                let seed = Run::start(workflow.as_ref(), cfg.clone(), 100, 0).unwrap();
                let limit = request(&seed).context_input_bytes() as u32 + 1050;
                cfg["max_input_bytes"] = json!(limit);
                cfg["context"]["max_input_bytes"] = json!(limit + 500);
                cfg["context"]["max_observations"] = json!(max_observations);
                let mut run = Run::start(workflow.as_ref(), cfg.clone(), 100, 0).unwrap();
                for index in 1..=24 {
                    let result = json!({"text":format!("\"\\\n{}", "🙂".repeat(80)),"index":index});
                    let id = tool(
                        &mut run,
                        workflow.as_ref(),
                        call("echo", json!({"text":"inspect"})),
                        result.clone(),
                    );
                    assert_eq!(
                        run.record().status,
                        RunStatus::Running,
                        "{:?}",
                        run.record().error
                    );
                    let request = request(&run);
                    let selection = request.context.as_ref().unwrap();
                    let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
                    let observations = prompt
                        .get("tool_observations")
                        .unwrap_or(&prompt["observations"])
                        .as_array()
                        .unwrap();
                    assert_eq!(selection.policy.max_input_bytes, limit);
                    assert_eq!(selection.input_bytes, request.context_input_bytes());
                    assert!(selection.input_bytes <= u64::from(limit));
                    assert_eq!(selection.total_observations, index);
                    assert_eq!(
                        selection.first_included,
                        Some(index - observations.len() as u64 + 1)
                    );
                    assert_eq!(
                        selection.omitted_observations + observations.len() as u64,
                        index
                    );
                    assert!(observations.len() <= 2);
                    assert_eq!(observations.last().unwrap()["result"], result);
                    assert_eq!(prompt["goal"], cfg["goal"]);
                    assert_eq!(prompt["writable_paths"], cfg["writable_paths"]);
                    assert_eq!(prompt["test_command"], cfg["test_command"]);
                    if policy == "planner" {
                        assert_eq!(prompt["workers"], cfg["workers"]);
                    }
                    let context: NativeContextState =
                        serde_json::from_value(run.record().state["context"].clone()).unwrap();
                    assert_eq!(context.observations.len(), observations.len());
                    assert_eq!(context.selection.as_ref(), Some(selection));
                    assert_eq!(
                        run.activities()
                            .iter()
                            .find(|a| a.id == id)
                            .unwrap()
                            .result
                            .as_ref(),
                        Some(&result)
                    );
                    assert!(
                        run.record().state["tool_observations"]
                            .as_array()
                            .is_none_or(|a| a.is_empty())
                    );
                }
                let request = request(&run);
                assert!(request.context.unwrap().omitted_observations >= 22);
                assert_eq!(
                    run.activities()
                        .iter()
                        .filter(|a| a.spec.handler.name == "native.tool" && a.result.is_some())
                        .count(),
                    24
                );
            }
        }
    }
}

#[test]
fn native_exact_byte_limits_and_mandatory_overflow_preserve_completed_work() {
    for policy in ["tool", "planner"] {
        for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
            let workflow = workflow(policy);
            let mut cfg = config(policy, protocol, true);
            let seed = Run::start(workflow.as_ref(), cfg.clone(), 10, 0).unwrap();
            let bytes = request(&seed).context_input_bytes() as u32;
            cfg["context"]["max_input_bytes"] = json!(bytes);
            let mut run = Run::start(workflow.as_ref(), cfg.clone(), 10, 0).unwrap();
            assert_eq!(request(&run).context_input_bytes(), u64::from(bytes));
            cfg["context"]["max_input_bytes"] = json!(bytes - 1);
            assert!(matches!(
                Run::start(workflow.as_ref(), cfg, 10, 0),
                Err(DomainError::ContextLimit { .. })
            ));
            let result = json!({"text":"newest complete evidence".repeat(500)});
            let id = tool(
                &mut run,
                workflow.as_ref(),
                call("read_file", json!({"path":"src/lib.rs"})),
                result.clone(),
            );
            assert_eq!(run.record().status, RunStatus::Failed);
            assert_eq!(run.record().error.as_ref().unwrap().code, "context_limit");
            assert_eq!(
                run.activities()
                    .iter()
                    .find(|a| a.id == id)
                    .unwrap()
                    .result
                    .as_ref(),
                Some(&result)
            );
            assert_eq!(run.record().usage.actual_input_tokens, 10);
        }
    }
}

#[test]
fn native_test_status_survives_eviction_and_successful_writes_invalidate_it() {
    for policy in ["tool", "planner"] {
        let workflow = workflow(policy);
        let mut cfg = config(policy, ModelProtocol::Responses, true);
        cfg["context"]["max_observations"] = json!(1);
        let mut run = Run::start(workflow.as_ref(), cfg, 100, 0).unwrap();
        let tests = tool(
            &mut run,
            workflow.as_ref(),
            call("run_tests", json!({})),
            json!({"ok":true,"exit_code":0,"stdout":"full saved test output"}),
        );
        for _ in 0..3 {
            tool(
                &mut run,
                workflow.as_ref(),
                call("echo", json!({"text":"next"})),
                json!({"text":"next"}),
            );
        }
        let prompt: Value = serde_json::from_str(&request(&run).prompt).unwrap();
        assert_eq!(
            prompt["test_status"],
            json!({"activity_id":tests,"passed":true,"exit_code":0,"stale":false})
        );
        assert!(!request(&run).prompt.contains("full saved test output"));
        tool(
            &mut run,
            workflow.as_ref(),
            call(
                "replace_text",
                json!({"path":"src/lib.rs","old":"a","new":"b"}),
            ),
            json!({"ok":false,"error":{"code":"no_match","message":"refused"}}),
        );
        assert_eq!(run.record().state["context"]["test_status"]["stale"], false);
        decision(&mut run, workflow.as_ref(), call("run_tests", json!({})));
        let wait = run.record().wait.as_ref().unwrap().id;
        run.signal(wait, json!({"approved":false}), workflow.as_ref(), 0)
            .unwrap();
        assert_eq!(
            run.record().state["context"]["test_status"]["activity_id"],
            json!(tests)
        );
        tool(
            &mut run,
            workflow.as_ref(),
            call(
                "replace_text",
                json!({"path":"src/lib.rs","old":"a","new":"b"}),
            ),
            json!({"ok":true,"already_applied":true}),
        );
        assert_eq!(run.record().state["context"]["test_status"]["stale"], true);
        let failed = tool(
            &mut run,
            workflow.as_ref(),
            call("run_tests", json!({})),
            json!({"ok":false,"exit_code":101}),
        );
        assert_eq!(
            run.record().state["context"]["test_status"],
            json!({"activity_id":failed,"passed":false,"exit_code":101,"stale":false})
        );
        let unknown = tool(
            &mut run,
            workflow.as_ref(),
            call("run_tests", json!({})),
            json!({"custom":"unknown status"}),
        );
        assert_eq!(
            run.record().state["context"]["test_status"],
            json!({"activity_id":unknown,"passed":null,"exit_code":null,"stale":false})
        );
    }
}

#[test]
fn native_clarifications_denials_and_child_groups_share_one_chronological_window() {
    for policy in ["tool", "planner"] {
        let workflow = workflow(policy);
        let mut cfg = config(policy, ModelProtocol::Responses, true);
        cfg["context"]["max_observations"] = json!(1);
        let mut run = Run::start(workflow.as_ref(), cfg, 20, 0).unwrap();
        tool(
            &mut run,
            workflow.as_ref(),
            call("echo", json!({"text":"first"})),
            json!({"text":"first"}),
        );
        decision(
            &mut run,
            workflow.as_ref(),
            AgentAction::AskHuman {
                question: "What next?".into(),
            },
        );
        let wait = run.record().wait.as_ref().unwrap().id;
        let before = json!(run);
        assert!(
            run.signal(
                wait,
                json!({"answer":"x".repeat(100000)}),
                workflow.as_ref(),
                0
            )
            .is_err()
        );
        assert_eq!(json!(run), before);
        run.signal(wait, json!({"answer":"clarified"}), workflow.as_ref(), 0)
            .unwrap();
        let prompt: Value = serde_json::from_str(&request(&run).prompt).unwrap();
        if policy == "tool" {
            assert_eq!(prompt["tool_observations"], json!([]));
            assert_eq!(
                prompt["clarifications"][0]["response"]["answer"],
                "clarified"
            );
        } else {
            assert_eq!(prompt["observations"][0]["result"]["answer"], "clarified");
        }
        decision(
            &mut run,
            workflow.as_ref(),
            call(
                "replace_text",
                json!({"path":"src/lib.rs","old":"a","new":"b"}),
            ),
        );
        let wait = run.record().wait.as_ref().unwrap().id;
        run.signal(wait, json!({"approved":false}), workflow.as_ref(), 0)
            .unwrap();
        assert_eq!(
            request(&run).context.as_ref().unwrap().total_observations,
            3
        );
        if policy == "planner" {
            // Pure policy transitions verify the grouped reply is one indivisible observation.
            let state = run.record().state.clone();
            let decision = workflow
                .transition(
                    &state,
                    &WorkflowEvent::ActivityCompleted {
                        activity_id: ActivityId::new(),
                        handler: DefinitionId::new("agent.model", 1),
                        result: json!(ModelDecision {
                            action: AgentAction::DelegateTasks {
                                tasks: vec![
                                    PlannerTask {
                                        depends_on: None,
                                        writable_paths: None,
                                        worker: "a".into(),
                                        goal: "one".into()
                                    },
                                    PlannerTask {
                                        depends_on: None,
                                        writable_paths: None,
                                        worker: "b".into(),
                                        goal: "two".into()
                                    }
                                ]
                            },
                            request_id: None,
                            diagnostics: None
                        }),
                    },
                )
                .unwrap();
            let joined = workflow
                .transition(
                    &decision.state,
                    &WorkflowEvent::TaskGroupCompleted {
                        group_id: TaskGroupId::new(),
                        outcomes: vec![json!({"status":"succeeded"}), json!({"status":"failed"})],
                    },
                )
                .unwrap();
            assert_eq!(joined.state["context"]["total_observations"], 4);
            assert_eq!(
                joined.state["context"]["observations"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                joined.state["context"]["observations"][0]["value"]["results"]
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );
        }
    }
}

#[test]
fn disabled_native_context_keeps_complete_observations() {
    for policy in ["tool", "planner"] {
        let workflow = workflow(policy);
        let cfg = config(policy, ModelProtocol::ChatCompletions, false);
        let mut run = Run::start(workflow.as_ref(), cfg.clone(), 100, 0).unwrap();
        for _ in 0..20 {
            tool(
                &mut run,
                workflow.as_ref(),
                call("echo", json!({"text":"minimal"})),
                json!({"text":"minimal"}),
            );
        }
        let request = request(&run);
        assert!(request.context.is_none());
        let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
        assert_eq!(
            prompt
                .get("tool_observations")
                .unwrap_or(&prompt["observations"])
                .as_array()
                .unwrap()
                .len(),
            20
        );
        assert!(prompt.get("context").is_none());
        assert!(prompt.get("test_status").is_none());
        assert!(!request.instructions.contains("recent window"));
        assert!(run.record().state.get("context").is_none());
        assert!(run.record().state["config"].get("context").is_none());
        let execution = run.admit(0).unwrap().unwrap();
        let input = execution.input.clone();
        run.recover(1);
        let mut resumed = run.clone();
        let retry = resumed.admit(501).unwrap().unwrap();
        assert_eq!(retry.activity_id, execution.activity_id);
        assert_eq!(retry.input, input);
        assert_eq!(
            resumed.record().workflow.version,
            if policy == "planner" { 3 } else { 1 }
        );
    }
}

#[tokio::test]
async fn disabled_native_context_is_restored_without_changing_the_policy() {
    use act0r::{
        Engine,
        adapters::{
            agent_executor::AgentExecutor, demo::DemoExecutor, file_lock::FileOwnership,
            model::ModelRouter, sqlite::SqliteStore,
        },
        application::{ToolRegistry, ports::Clock},
    };
    use std::{
        path::Path,
        sync::atomic::{AtomicI64, Ordering},
    };
    struct TestClock(AtomicI64);
    impl Clock for TestClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    async fn open(db: &Path, policy: &str, clock: Arc<TestClock>) -> Engine {
        Engine::new(
            Arc::new(SqliteStore::open(db).await.unwrap()),
            Arc::new(
                AgentExecutor::new(
                    Arc::new(ModelRouter::new(None).unwrap()),
                    Arc::new(LocalTools::new(db.parent().unwrap().join("workspaces"))),
                    Arc::new(DemoExecutor),
                )
                .with_native_tools(Arc::new(
                    ToolRegistry::new(vec![Arc::new(EchoTool)]).unwrap(),
                )),
            ),
            clock,
            Arc::new(FileOwnership::new(db)),
            vec![Arc::from(workflow(policy))],
        )
        .unwrap()
    }
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("minimal.db");
        let clock = Arc::new(TestClock(AtomicI64::new(0)));
        let engine = open(&db, policy, clock.clone()).await;
        let mut cfg = config(policy, ModelProtocol::ChatCompletions, false);
        let mut script: Vec<_> = (0..20)
            .map(|_| call("echo", json!({"text":"minimal evidence"})))
            .collect();
        script.push(AgentAction::Finish {
            summary: "minimal complete".into(),
        });
        cfg["script"] = json!(script);
        let run = engine.start(workflow(policy).id(), cfg, 100).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        for _ in 0..40 {
            assert!(worker.step().await.unwrap());
        }
        let before = engine.status(run).await.unwrap();
        assert!(before.record().state.get("context").is_none());
        assert!(request(&before).context.is_none());
        let interrupted = worker.admit().await.unwrap().unwrap();
        let mut usage = json!(engine.status(run).await.unwrap().record().usage);
        usage["uncertain_calls"] = json!(1);
        drop(worker);
        drop(engine);
        let engine = open(&db, policy, clock.clone()).await;
        let mut worker = engine.worker().await.unwrap();
        let restored = engine.status(run).await.unwrap();
        assert_eq!(restored.record().state, before.record().state);
        assert_eq!(request(&restored).prompt, request(&before).prompt);
        assert_eq!(json!(restored.record().usage), usage);
        clock.0.store(500, Ordering::SeqCst);
        let retry = worker.admit().await.unwrap().unwrap();
        assert_eq!(retry.input, interrupted.input);
        assert_eq!(retry.activity_id, interrupted.activity_id);
        worker
            .complete(retry.clone(), worker.execute(retry).await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Succeeded
        );
    }
}
