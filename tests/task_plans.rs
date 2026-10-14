use act0r::domain::{agent::*, planner::PlannerWorkflow, *};
use serde_json::{Value, json};

fn input(context: bool) -> Value {
    let mut input = json!({"goal":"Inspect then implement","target":{"provider":"scripted","model":"fixture","endpoint":"local"},"max_model_calls":null,"max_tokens":null,"max_output_tokens":512,"workers":[{"name":"helper","agent_id":AgentId::new(),"description":"Inspect"}],"delegation":{"max_tasks":4,"max_depth":2}});
    input["task_planning"] = json!(true);
    if context {
        input["context"] = json!({"version":1,"max_input_bytes":65536,"max_observations":1});
    }
    input
}
fn decide(state: &Value, action: Value) -> Transition {
    PlannerWorkflow::default()
        .transition(
            state,
            &WorkflowEvent::ActivityCompleted {
                activity_id: ActivityId::new(),
                handler: DefinitionId::new("agent.model", 1),
                result: json!({"action":action,"request_id":null}),
            },
        )
        .unwrap()
}
fn update(revision: u32, steps: Value) -> Value {
    json!({"kind":"update_plan","expected_revision":revision,"explanation":"Working through the request","steps":steps})
}
fn steps() -> Value {
    json!([
        {"id":"inspect","title":"Inspect the project","status":"in_progress"},
        {"id":"build","title":"Implement the change","status":"pending"}
    ])
}
fn request(t: &Transition) -> ModelRequest {
    t.commands
        .iter()
        .find_map(|c| {
            if let Command::Schedule(s) = c {
                Some(serde_json::from_value(s.input.clone()).unwrap())
            } else {
                None
            }
        })
        .unwrap()
}

#[test]
fn plan_is_fixed_context_when_observations_are_trimmed_and_omitted_when_disabled() {
    let workflow = PlannerWorkflow::default();
    let started = workflow
        .transition(&Value::Null, &WorkflowEvent::Started(input(true)))
        .unwrap();
    workflow.validate_state(&started.state).unwrap();
    assert!(started.state.get("task_plan").is_none());
    let plan = decide(&started.state, update(0, steps()));
    assert!(matches!(plan.commands[0], Command::RecordTaskPlan(_)));
    assert_eq!(plan.state["task_plan"]["revision"], 1);
    let mut state = plan.state;
    for _ in 0..3 {
        state = decide(&state, json!({"kind":"ask_human","question":"Continue?"})).state;
        let next = workflow
            .transition(
                &state,
                &WorkflowEvent::InputReceived {
                    wait_id: WaitId::new(),
                    key: "planner_answer".into(),
                    payload: json!({"answer":"Continue"}),
                },
            )
            .unwrap();
        let prompt: Value = serde_json::from_str(&request(&next).prompt).unwrap();
        assert_eq!(prompt["task_plan"], next.state["task_plan"]);
        assert_eq!(prompt["observations"].as_array().unwrap().len(), 1);
        state = next.state;
    }
    assert_eq!(state["context"]["total_observations"], 3);
    let mut old = started.state;
    old.as_object_mut().unwrap().remove("task_plan");
    old["config"]
        .as_object_mut()
        .unwrap()
        .remove("task_planning");
    workflow.validate_state(&old).unwrap();
    let unplanned_request = workflow
        .transition(&Value::Null, &WorkflowEvent::Started(old["config"].clone()))
        .unwrap();
    assert!(
        !request(&unplanned_request)
            .schema
            .to_string()
            .contains("update_plan")
    );
    assert!(
        !request(&unplanned_request)
            .instructions
            .contains("update_plan")
    );
    let unplanned_prompt: Value =
        serde_json::from_str(&request(&unplanned_request).prompt).unwrap();
    assert!(unplanned_prompt.get("task_plan").is_none());
    let finished = decide(&old, json!({"kind":"finish","summary":"A simple answer"}));
    assert!(matches!(&finished.commands[0], Command::Complete(_)));
}

#[test]
fn invalid_revisions_and_completed_step_changes_leave_the_checkpoint_unchanged() {
    let started = PlannerWorkflow::default()
        .transition(&Value::Null, &WorkflowEvent::Started(input(false)))
        .unwrap();
    let mut items = steps();
    items[0]["status"] = json!("completed");
    items[1]["status"] = json!("blocked");
    let first = decide(&started.state, update(0, items.clone()));
    let saved = first.state["task_plan"].clone();
    let mut variants = vec![update(0, items.clone())];
    for alteration in ["missing", "renamed", "reopened"] {
        let mut changed = items.clone();
        match alteration {
            "missing" => {
                changed.as_array_mut().unwrap().remove(0);
            }
            "renamed" => changed[0]["title"] = json!("Forget completed work"),
            _ => changed[0]["status"] = json!("pending"),
        }
        variants.push(update(1, changed));
    }
    for action in variants {
        let refused = decide(&first.state, action);
        assert_eq!(refused.state["task_plan"], saved);
        assert_eq!(refused.commands.len(), 1);
        let prompt: Value = serde_json::from_str(&request(&refused).prompt).unwrap();
        assert_eq!(
            prompt["observations"][0]["result"]["error"]["code"],
            "invalid_plan_update"
        );
        assert_eq!(prompt["task_plan"], saved);
    }
    items[1] =
        json!({"id":"review","title":"Review instead of implementing","status":"in_progress"});
    let revised = decide(&first.state, update(1, items));
    assert_eq!(revised.state["task_plan"]["revision"], 2);
    assert_eq!(revised.state["task_plan"]["steps"][0], saved["steps"][0]);
}

#[test]
fn malformed_and_oversized_plans_are_feedback_and_cannot_publish_a_plan_event() {
    let started = PlannerWorkflow::default()
        .transition(&Value::Null, &WorkflowEvent::Started(input(false)))
        .unwrap();
    let mut duplicates = steps();
    duplicates[1]["id"] = json!("inspect");
    let mut two_active = steps();
    two_active[1]["status"] = json!("in_progress");
    let mut bad_id = steps();
    bad_id[0]["id"] = json!("../path");
    let mut blank = steps();
    blank[0]["title"] = json!(" ");
    let mut unicode = steps();
    unicode[0]["title"] = json!("🦀".repeat(65));
    let mut multiline = steps();
    multiline[0]["title"] = json!("one\ntwo");
    let too_many = Value::Array(
        (0..17)
            .map(|i| json!({"id":format!("step-{i}"),"title":"A step","status":"pending"}))
            .collect(),
    );
    for invalid in [
        json!([]),
        duplicates,
        two_active,
        bad_id,
        blank,
        unicode,
        multiline,
        too_many,
    ] {
        let refused = decide(&started.state, update(0, invalid));
        assert!(refused.state.get("task_plan").is_none());
        assert_eq!(refused.commands.len(), 1);
    }
    let mut action = update(0, steps());
    action["extra"] = json!(true);
    assert!(serde_json::from_value::<AgentAction>(action).is_err());
}

#[test]
fn inspection_projects_only_direct_or_pinned_registered_native_planners() {
    let workflow = PlannerWorkflow::default();
    let run = Run::start(&workflow, input(false), 20, 0).unwrap();
    let plan = decide(&run.record().state, update(0, steps()));
    let mut record = run.record().clone();
    record.state = plan.state.clone();
    assert_eq!(
        json!(task_plan::from_run(&record).unwrap()),
        plan.state["task_plan"]
    );
    let registration = json!({"contract_version":1,"adapter":{"name":"planner","version":3},"definition":workflow.id(),"capabilities":{"checkpoints":"model_and_tool","cooperative_cancellation":true,"human_input_waits":true,"usage":"reported_or_reserved","branching":false,"delegation":true}});
    record.workflow = DefinitionId::new("registered_agent.planner", 3);
    record.input = json!({"registration":registration,"input":input(false)});
    record.state = json!({"registration":registration,"state":plan.state});
    assert!(task_plan::from_run(&record).unwrap().is_some());
    record.input["registration"]["adapter"]["version"] = json!(2);
    assert!(task_plan::from_run(&record).is_err());
    record.workflow = DefinitionId::new("custom_workflow", 1);
    record.state =
        json!({"registration":"unrelated application data","task_plan":{"invalid":true}});
    assert!(task_plan::from_run(&record).unwrap().is_none());
}

#[test]
fn finishing_a_checklist_does_not_supply_test_evidence_or_require_a_plan_for_simple_work() {
    use act0r::{
        adapters::{native_tools::RunTestsTool, workspace::LocalTools},
        application::ports::NativeTool,
    };
    let tool =
        RunTestsTool::new(std::sync::Arc::new(LocalTools::new("C:/isolated".into()))).definition();
    let mut config = input(false);
    config["workspace"] = json!("C:/isolated/project");
    config["tools"] = json!([tool.id]);
    config["tool_definitions"] = json!([tool]);
    config["test_command"] = json!({"argv":["cargo","test"],"timeout_ms":1000});
    config["change_review"] = json!(true);
    config["completion_policy"] = json!("require_passing_tests");
    let workflow = PlannerWorkflow::new(vec![tool]);
    let started = workflow
        .transition(&Value::Null, &WorkflowEvent::Started(config))
        .unwrap();
    let ready=workflow.transition(&started.state,&WorkflowEvent::ActivityCompleted{activity_id:ActivityId::new(),handler:change_review::handler("baseline"),result:json!({"version":1,"task_id":TaskId::new(),"workspace":"C:/isolated/project","digest":"a".repeat(64),"files":[]})}).unwrap();
    let mut all_done = steps();
    for step in all_done.as_array_mut().unwrap() {
        step["status"] = json!("completed");
    }
    let event = |action: Value| WorkflowEvent::ActivityCompleted {
        activity_id: ActivityId::new(),
        handler: DefinitionId::new("agent.model", 1),
        result: json!({"action":action,"request_id":null}),
    };
    let saved = workflow
        .transition(&ready.state, &event(update(0, all_done)))
        .unwrap();
    let finishing = workflow
        .transition(
            &saved.state,
            &event(json!({"kind":"finish","summary":"All checklist steps are completed"})),
        )
        .unwrap();
    assert!(
        matches!(&finishing.commands[0],Command::Schedule(s) if s.handler==change_review::handler("changes"))
    );
    let changes = change_review::WorkspaceChangeSet {
        version: 1,
        run_id: RunId::new(),
        activity_id: ActivityId::new(),
        baseline_digest: "a".repeat(64),
        workspace_digest: "a".repeat(64),
        files: vec![],
        changed_bytes: 0,
        test_status: change_review::TestEvidenceStatus::NotRun,
        tests: None,
    };
    let refused = workflow
        .transition(
            &finishing.state,
            &WorkflowEvent::ActivityCompleted {
                activity_id: changes.activity_id,
                handler: change_review::handler("changes"),
                result: json!(changes),
            },
        )
        .unwrap();
    let report = change_review::completion_report(&refused.state)
        .unwrap()
        .unwrap();
    assert_eq!(report.status, completion::CompletionStatus::Refused);
    assert_eq!(
        report.reason,
        Some(completion::CompletionReason::TestsNotRun)
    );
    assert_eq!(refused.state["task_plan"], saved.state["task_plan"]);
    assert_eq!(refused.commands.len(), 1);
    let prompt: Value = serde_json::from_str(&request(&refused).prompt).unwrap();
    assert_eq!(
        prompt["observations"][0]["completion"]["reason"],
        "tests_not_run"
    );
}
