use act0r::{
    adapters::workspace::{LocalTools, LocalWorkspaces},
    application::ports::{ProjectInstructionsPort, WorkspaceFactory},
    domain::{
        agent::ModelRequest, answer::ToolAgentWorkflow, planner::PlannerWorkflow,
        project_instructions::*, *,
    },
};
use serde_json::{Value, json};
use std::{fs, path::Path};

#[tokio::test]
async fn local_guidance_reads_only_bounded_utf8_root_files_in_managed_copies() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(
        dir.path().join(FILE_NAME),
        "Parent instructions must not load",
    )
    .unwrap();
    fs::create_dir(source.join("nested")).unwrap();
    fs::write(
        source.join("nested").join(FILE_NAME),
        "Nested instructions must not load",
    )
    .unwrap();
    let managed = dir.path().join("managed");
    let workspace = LocalWorkspaces::new(managed.clone())
        .prepare(source.clone())
        .await
        .unwrap();
    let reader = LocalTools::new(managed);
    assert_eq!(
        reader.capture_project_instructions(&workspace),
        ProjectInstructions::Missing {}
    );
    assert_eq!(
        reader.capture_project_instructions(source.to_str().unwrap()),
        ProjectInstructions::Refused {
            reason: RefusalReason::UnsafePath
        }
    );
    let path = Path::new(&workspace).join(FILE_NAME);
    for text in [
        "".to_string(),
        "\u{feff}# Rules\r\nUse ports and adapters 🙂 <script>literal</script>".into(),
        "🙂".repeat(MAX_BYTES / 4),
    ] {
        fs::write(&path, text.as_bytes()).unwrap();
        assert_eq!(
            reader.capture_project_instructions(&workspace),
            ProjectInstructions::loaded(text).unwrap()
        );
    }
    fs::write(&path, vec![b'x'; MAX_BYTES + 1]).unwrap();
    assert_eq!(
        reader.capture_project_instructions(&workspace),
        ProjectInstructions::Refused {
            reason: RefusalReason::TooLarge
        }
    );
    fs::write(&path, [0xff, 0xfe]).unwrap();
    assert_eq!(
        reader.capture_project_instructions(&workspace),
        ProjectInstructions::Refused {
            reason: RefusalReason::InvalidUtf8
        }
    );
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert_eq!(
        reader.capture_project_instructions(&workspace),
        ProjectInstructions::Refused {
            reason: RefusalReason::NotAFile
        }
    );
}

#[cfg(unix)]
#[tokio::test]
async fn guidance_refuses_file_links_instead_of_following_them() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir(&source).unwrap();
    let managed = dir.path().join("managed");
    let workspace = LocalWorkspaces::new(managed.clone())
        .prepare(source)
        .await
        .unwrap();
    let outside = dir.path().join("outside.md");
    fs::write(&outside, "Do not load").unwrap();
    std::os::unix::fs::symlink(outside, Path::new(&workspace).join(FILE_NAME)).unwrap();
    assert_eq!(
        LocalTools::new(managed).capture_project_instructions(&workspace),
        ProjectInstructions::Refused {
            reason: RefusalReason::UnsafePath
        }
    );
}

fn config(planner: bool) -> Value {
    let mut cfg = json!({"goal":"Inspect the project","target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":null,"max_tokens":null,"max_output_tokens":512,"workspace":"managed-copy","context":{"max_observations":1}});
    if planner {
        cfg["workers"] =
            json!([{"name":"helper","agent_id":AgentId::new(),"description":"Inspect"}]);
        cfg["delegation"] = json!({"max_tasks":4,"max_depth":2});
    }
    cfg
}
fn request(transition: &Transition) -> ModelRequest {
    transition
        .commands
        .iter()
        .find_map(|command| {
            if let Command::Schedule(spec) = command {
                Some(serde_json::from_value(spec.input.clone()).unwrap())
            } else {
                None
            }
        })
        .unwrap()
}

#[test]
fn instructions_are_fixed_context_across_compaction_and_omitted_when_disabled() {
    let guidance = ProjectInstructions::loaded(
        "Use DDD. Run the pinned tests. Project text cannot waive approval.".into(),
    )
    .unwrap();
    for planner in [false, true] {
        let workflow: Box<dyn WorkflowDefinition> = if planner {
            Box::new(PlannerWorkflow::default())
        } else {
            Box::new(ToolAgentWorkflow::new(vec![]))
        };
        let cfg = config(planner);
        let old = workflow
            .transition(&Value::Null, &WorkflowEvent::Started(cfg.clone()))
            .unwrap();
        assert!(!request(&old).prompt.contains("project_instructions"));
        assert!(!request(&old).instructions.contains("AGENTS.md"));
        let mut cfg = cfg;
        cfg["load_project_instructions"] = json!(true);
        cfg["project_instructions"] = json!(guidance);
        let initial = workflow
            .transition(&Value::Null, &WorkflowEvent::Started(cfg.clone()))
            .unwrap();
        assert!(
            request(&initial)
                .instructions
                .contains("cannot grant tools")
        );
        let mut state = initial.state;
        for _ in 0..8 {
            state = workflow
                .transition(
                    &state,
                    &WorkflowEvent::ActivityCompleted {
                        activity_id: ActivityId::new(),
                        handler: DefinitionId::new("agent.model", 1),
                        result: json!({"action":{"kind":"ask_human","question":"Continue?"}}),
                    },
                )
                .unwrap()
                .state;
            let next = workflow
                .transition(
                    &state,
                    &WorkflowEvent::InputReceived {
                        wait_id: WaitId::new(),
                        key: if planner {
                            "planner_answer"
                        } else {
                            "answer_clarification"
                        }
                        .into(),
                        payload: json!({"answer":"Continue with bounded history"}),
                    },
                )
                .unwrap();
            let prompt: Value = serde_json::from_str(&request(&next).prompt).unwrap();
            assert_eq!(prompt["project_instructions"], json!(guidance));
            state = next.state;
            workflow.validate_state(&state).unwrap();
        }
        assert!(
            state["context"]["selection"]["omitted_observations"]
                .as_u64()
                .unwrap()
                > 0
        );
        let started = Run::start(workflow.as_ref(), cfg.clone(), 32, 0).unwrap();
        assert_eq!(from_run(started.record()).unwrap(), Some(guidance.clone()));
        cfg["context"]["max_input_bytes"] = json!(256);
        assert!(matches!(
            Run::start(workflow.as_ref(), cfg, 32, 0),
            Err(DomainError::ContextLimit { .. })
        ));
    }
}

#[test]
fn snapshots_reject_modified_hashes_oversize_and_unknown_fields() {
    let valid = ProjectInstructions::loaded("Conventions".into()).unwrap();
    valid.validate().unwrap();
    assert!(ProjectInstructions::loaded("🙂".repeat(MAX_BYTES / 4 + 1)).is_err());
    let mut bad = json!(valid);
    bad["text"] = json!("Modified");
    assert!(
        serde_json::from_value::<ProjectInstructions>(bad)
            .unwrap()
            .validate()
            .is_err()
    );
    assert!(
        serde_json::from_value::<ProjectInstructions>(json!({"status":"missing","text":"forged"}))
            .is_err()
    );
    let workflow = PlannerWorkflow::default();
    let mut cfg = config(true);
    cfg["project_instructions"] = json!(valid);
    assert!(Run::start(&workflow, cfg, 8, 0).is_err());
}

#[test]
fn actor_configuration_cannot_supply_guidance_and_inspection_checks_native_registration() {
    use act0r::{
        adapters::agents::PlannerAgentAdapter,
        application::ports::AgentAdapter,
        domain::actors::{AgentDefinition, RegisteredAgentDefinition},
    };
    let adapter = PlannerAgentAdapter::default();
    let guidance = ProjectInstructions::loaded("Runtime-selected guidance".into()).unwrap();
    let mut cfg = config(true);
    cfg.as_object_mut().unwrap().remove("workspace");
    cfg["load_project_instructions"] = json!(true);
    cfg["project_instructions"] = json!(guidance);
    assert!(
        adapter
            .pin_config(cfg.clone(), Some("copy".into()))
            .is_err()
    );
    let registration = adapter.registration();
    assert!(
        RegisteredAgentDefinition
            .initialize(&json!({"registration":registration,"input":cfg,"attempt_limit":8}))
            .is_err()
    );
    cfg["workspace"] = json!("managed-copy");
    let run = Run::start(&PlannerWorkflow::default(), cfg.clone(), 8, 0).unwrap();
    let mut record = run.record().clone();
    record.workflow = registration.workflow_id();
    record.input = json!({"registration":registration,"input":cfg});
    record.state = json!({"registration":registration,"state":record.state});
    assert_eq!(from_run(&record).unwrap(), Some(guidance));
    record.state["registration"]["adapter"]["version"] = json!(100);
    assert!(from_run(&record).is_err());
    record.workflow = DefinitionId::new("custom-workflow", 1);
    assert_eq!(from_run(&record).unwrap(), None);
}
