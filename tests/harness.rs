use act0r::application::Error;
use act0r::{
    adapters::operator::{self, Request},
    bootstrap::local_app,
    domain::{agent::*, harness::*, *},
};
use serde_json::json;
fn request(prompt: &str) -> HarnessRequest {
    HarnessRequest {
        auto_execute: false,
        discover_project: false,
        budget: None,
        message_id: MessageId::new(),
        prompt: prompt.into(),
        workspace: None,
        coding: None,
        target: ModelTarget {
            provider: "openai".into(),
            model: "test-model".into(),
            endpoint: "http://127.0.0.1:1234/v1/chat/completions".into(),
            protocol: ModelProtocol::ChatCompletions,
            action_format: Default::default(),
            policy: Default::default(),
        },
    }
}

#[tokio::test]
async fn coding_pins_planner_read_only_helper_and_isolated_verified_coder() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let (_, agents) = local_app(root.path().join("coding-pins.db")).await.unwrap();
    let mut input = request("Repair code");
    input.workspace = Some(
        std::fs::canonicalize(source)
            .unwrap()
            .to_string_lossy()
            .into_owned(),
    );
    input.coding = Some(HarnessCoding {
        coder_count: 1,
        writable_paths: vec!["src/".into()],
        test_command: crate_command(),
        project_commands: [("check".into(), crate_command())].into(),
    });
    let receipt = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .start(input.clone())
        .await
        .unwrap();
    let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
    assert_eq!(team.members.len(), 3);
    assert_eq!(team.members[2].name, "coder");
    let planner = &team.members[0].pinned_config["input"];
    assert_eq!(planner["child_changes"], true);
    assert_eq!(planner["completion_policy"], "require_passing_tests");
    assert_eq!(planner["writable_paths"], json!(["src/"]));
    assert_eq!(planner["workspace_handoff"], true);
    assert_eq!(planner["delegation_write_scopes"], true);
    assert_eq!(planner["test_command"], json!(crate_command()));
    assert!(
        planner["tool_definitions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["requires_writable_paths"] == true || t["requires_test_command"] == true)
            .all(|t| t["requires_approval"] == true)
    );
    let helper = &team.members[1].pinned_config["input"];
    assert_eq!(helper["accept_workspace_handoff"], true);
    assert!(helper.get("writable_paths").is_none());
    assert!(
        helper["tool_definitions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["requires_writable_paths"] != true && t["requires_test_command"] != true)
    );
    let coder = &team.members[2].pinned_config["input"];
    assert_eq!(coder["accept_workspace_handoff"], true);
    assert_eq!(coder["completion_policy"], "require_passing_tests");
    assert_eq!(coder["writable_paths"], planner["writable_paths"]);
    assert_eq!(coder["test_command"], planner["test_command"]);
    assert_eq!(coder["project_commands"], planner["project_commands"]);
    let coding_tools = coder["tool_definitions"].as_array().unwrap();
    let shared: Vec<_> = coding_tools
        .iter()
        .filter(|t| {
            !["send_worker_message", "ask_worker", "reply_worker"]
                .contains(&t["id"]["name"].as_str().unwrap())
        })
        .cloned()
        .collect();
    let planner_shared: Vec<_> = planner["tool_definitions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| {
            ![
                "reply_worker",
                "guide_workers",
                "inspect_assignment",
                "stop_worker",
                "pause_worker",
                "resume_worker",
                "wait_for_worker",
            ]
            .contains(&t["id"]["name"].as_str().unwrap())
        })
        .cloned()
        .collect();
    assert_eq!(shared, planner_shared);
    assert_eq!(planner["guidance_routing"], true);
    assert_eq!(planner["worker_findings"], true);
    assert!(coder.get("worker_findings").is_none());
    assert_eq!(planner["assignment_inspection_limit"], 3);
    assert_eq!(planner["worker_stop_limit"], 2);
    assert_eq!(planner["worker_pause_limit"], 4);
    assert_eq!(planner["worker_dependencies"], true);
    assert!(coder.get("worker_dependencies").is_none());
    assert!(coder.get("worker_pause_limit").is_none());
    assert!(coder.get("worker_stop_limit").is_none());
    assert!(coder.get("assignment_inspection_limit").is_none());
    assert!(coder.get("guidance_routing").is_none());
    assert!(
        coding_tools
            .iter()
            .all(|t| t["id"]["name"] != "guide_workers"
                && t["id"]["name"] != "inspect_assignment"
                && t["id"]["name"] != "stop_worker"
                && t["id"]["name"] != "pause_worker"
                && t["id"]["name"] != "resume_worker"
                && t["id"]["name"] != "wait_for_worker")
    );
    let guidance = planner["tool_definitions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["id"]["name"] == "guide_workers")
        .unwrap();
    assert_eq!(guidance["id"]["version"], 1);
    assert_eq!(guidance["recovery"], "SafeToRepeat");
    assert_eq!(guidance["requires_approval"], false);
    assert!(
        planner["tool_definitions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["id"]["name"] == "reply_worker")
    );
    let messaging = coding_tools
        .iter()
        .find(|t| t["id"]["name"] == "send_worker_message")
        .unwrap();
    assert_eq!(messaging["recovery"], "SafeToRepeat");
    assert_eq!(messaging["id"]["version"], 2);
    assert_eq!(
        messaging["parameters"]["needs_coordination"]["required"],
        false
    );
    assert_eq!(messaging["requires_approval"], false);
    for name in ["ask_worker", "reply_worker"] {
        let tool = coding_tools
            .iter()
            .find(|t| t["id"]["name"] == name)
            .unwrap();
        assert_eq!(tool["id"]["version"], 2);
        assert_eq!(tool["recovery"], "SafeToRepeat");
        assert_eq!(tool["requires_approval"], false);
        assert_ne!(tool["requires_workspace"], true);
    }
    assert_eq!(coder["load_project_instructions"], true);
    assert!(coder.get("goal_budget").is_none());
    assert!(coder.get("delegation_write_scopes").is_none());
    assert_ne!(
        team.members[2].workspace.as_ref().unwrap().path,
        team.members[0].workspace.as_ref().unwrap().path
    );
    assert_ne!(
        team.members[2].workspace.as_ref().unwrap().path,
        team.members[1].workspace.as_ref().unwrap().path
    );
    assert_eq!(
        planner["workers"][1]["agent_id"],
        json!(team.members[2].agent_id)
    );
    for actor in &team.members {
        let config = &actor.pinned_config["input"];
        assert!(config["max_model_calls"].is_null());
        assert!(config["max_tokens"].is_null());
    }
    let saved = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .conversation(receipt.session_id, None)
        .await
        .unwrap();
    assert_eq!(saved.coding, input.coding);
}

fn crate_command() -> tools::WorkspaceTestCommand {
    tools::WorkspaceTestCommand {
        argv: vec!["cargo".into(), "test".into(), "--offline".into()],
        timeout_ms: 60000,
    }
}

#[tokio::test]
async fn two_coders_pin_distinct_actors_permissions_and_group_bounds_across_restart() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let database = root.path().join("parallel.db");
    let mut input = request("Repair independent files");
    input.workspace = Some(source.to_string_lossy().into_owned());
    input.coding = Some(HarnessCoding {
        coder_count: 2,
        writable_paths: vec!["src/".into()],
        test_command: crate_command(),
        project_commands: Default::default(),
    });
    let (_, agents) = local_app(&database).await.unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    let receipt = harness.start(input.clone()).await.unwrap();
    let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
    assert_eq!(team.members.len(), 4);
    assert_eq!(team.members[2].name, "coder");
    assert_eq!(team.members[3].name, "coder2");
    assert_ne!(team.members[2].agent_id, team.members[3].agent_id);
    assert_ne!(team.members[2].workspace, team.members[3].workspace);
    let planner = &team.members[0].pinned_config["input"];
    assert_eq!(planner["delegation"]["max_parallel_tasks"], 2);
    assert_eq!(planner["question_inspection_limit"], 3);
    assert_eq!(planner["delegation"]["max_tasks"], 4);
    for coder in &team.members[2..] {
        let config = &coder.pinned_config["input"];
        assert_eq!(config["writable_paths"], planner["writable_paths"]);
        assert_eq!(config["test_command"], planner["test_command"]);
        assert_eq!(config["accept_workspace_handoff"], true);
        assert_eq!(config["completion_policy"], "require_passing_tests");
        assert!(config["max_model_calls"].is_null());
        assert!(config["max_tokens"].is_null());
    }
    drop(agents);
    let (_, agents) = local_app(&database).await.unwrap();
    let teams = agents.teams().unwrap();
    let harness = teams.harness().unwrap();
    assert_eq!(
        json!(harness.start(input.clone()).await.unwrap()),
        json!(receipt)
    );
    assert_eq!(
        json!(teams.get(receipt.team_id).await.unwrap().members),
        json!(team.members)
    );
    assert_eq!(
        harness
            .conversation(receipt.session_id, None)
            .await
            .unwrap()
            .coding,
        input.coding
    );
    assert_eq!(
        harness.sessions().await.unwrap().items[0].coding,
        input.coding
    );
    let follow = HarnessTurnRequest {
        message_id: MessageId::new(),
        prompt: "Continue with these workers".into(),
    };
    let next = harness
        .append(receipt.session_id, follow.clone())
        .await
        .unwrap();
    assert_eq!(next.team_id, receipt.team_id);
    assert_eq!(
        json!(harness.append(receipt.session_id, follow).await.unwrap()),
        json!(next)
    );
    assert_eq!(
        harness
            .conversation(receipt.session_id, None)
            .await
            .unwrap()
            .coding,
        input.coding
    );
    let mut changed = input.clone();
    changed.coding.as_mut().unwrap().coder_count = 1;
    assert!(harness.start(changed.clone()).await.is_err());
    changed.message_id = MessageId::new();
    let single = harness.start(changed.clone()).await.unwrap();
    let saved_single = teams.get(single.team_id).await.unwrap();
    assert_eq!(saved_single.members.len(), 3);
    assert!(
        saved_single.spec.planner.config["delegation"]
            .get("max_parallel_tasks")
            .is_none()
    );
    changed.coding.as_mut().unwrap().coder_count = 2;
    assert!(harness.start(changed).await.is_err());
}

#[test]
fn coding_worker_count_defaults_to_one_and_only_accepts_one_or_two() {
    let old = json!({"writable_paths":["src/"],"test_command":crate_command()});
    let coding: HarnessCoding = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(coding.coder_count, 1);
    assert_eq!(json!(coding), old);
    for count in [0, 3, 255] {
        let mut bad = coding.clone();
        bad.coder_count = count;
        assert!(bad.validate().is_err());
    }
    for count in [json!(null), json!("2"), json!(1.5), json!(256)] {
        let mut bad = old.clone();
        bad["coder_count"] = count;
        assert!(serde_json::from_value::<HarnessCoding>(bad).is_err());
    }
}

#[tokio::test]
async fn prompt_creates_native_actors_without_team_setup_and_exact_retries_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("harness.db");
    let (_, agents) = local_app(&database).await.unwrap();
    let input = request("Explain durable actors 🦀");
    let receipt = operator::execute(
        &agents,
        Request::HarnessStart {
            request: input.clone(),
        },
    )
    .await
    .unwrap();
    let team: TeamId = serde_json::from_value(receipt["team_id"].clone()).unwrap();
    let view = agents.teams().unwrap().status(team).await.unwrap();
    assert_eq!(view.members.len(), 2);
    assert_eq!(
        operator::execute(
            &agents,
            Request::HarnessStart {
                request: input.clone()
            }
        )
        .await
        .unwrap(),
        receipt
    );
    drop(agents);
    let (_, agents) = local_app(&database).await.unwrap();
    assert_eq!(
        operator::execute(
            &agents,
            Request::HarnessStart {
                request: input.clone()
            }
        )
        .await
        .unwrap(),
        receipt
    );
    let sessions = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .sessions()
        .await
        .unwrap();
    assert_eq!(sessions.items.len(), 1);
    assert_eq!(sessions.items[0].prompt_preview, input.prompt);
    assert_eq!(sessions.items[0].task_id, input.message_id);
    let mut changed = input.clone();
    changed.prompt = "Changed request".into();
    assert!(
        operator::execute(&agents, Request::HarnessStart { request: changed })
            .await
            .is_err()
    );
    assert_eq!(agents.teams().unwrap().list().await.unwrap().teams.len(), 1);
}
#[tokio::test]
async fn retry_recovers_an_assembly_intent_before_the_root_message_was_submitted() {
    let root = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(root.path().join("intent.db")).await.unwrap();
    let input = request("Recover submission");
    let teams = agents.teams().unwrap();
    let prepared = teams.create(input.team_spec().unwrap()).await.unwrap();
    let page = teams.harness().unwrap().sessions().await.unwrap();
    assert_eq!(page.items[0].root_status, "preparing");
    let resumed = teams.harness().unwrap().start(input).await.unwrap();
    assert_eq!(resumed.team_id, prepared.id);
    assert_eq!(
        teams
            .goals()
            .unwrap()
            .list(prepared.id, Default::default())
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}
#[tokio::test]
async fn optional_workspace_pins_only_inspection_tools_and_preserves_source() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("README.md"), "Original").unwrap();
    let (_, agents) = local_app(root.path().join("workspace.db")).await.unwrap();
    let mut input = request("Inspect this project");
    input.workspace = Some(
        std::fs::canonicalize(&source)
            .unwrap()
            .to_string_lossy()
            .into_owned(),
    );
    let receipt = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .start(input)
        .await
        .unwrap();
    let record = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
    for member in record.members {
        assert!(member.workspace.is_some());
        let tools = member.pinned_config["input"]["tool_definitions"]
            .as_array()
            .unwrap();
        assert_eq!(tools.len(), 4);
        assert!(
            tools
                .iter()
                .all(|tool| tool["requires_writable_paths"] != true)
        );
    }
    assert_eq!(
        std::fs::read_to_string(source.join("README.md")).unwrap(),
        "Original"
    );
}

#[tokio::test]
async fn harness_sessions_use_unlimited_or_explicit_budgets() {
    let root = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(root.path().join("budgets.db")).await.unwrap();
    let harness = agents.teams().unwrap().harness().unwrap();
    for budget in [
        None,
        Some(UsageBudget::default()),
        Some(UsageBudget {
            max_model_calls: Some(3),
            max_tokens: None,
        }),
        Some(UsageBudget {
            max_model_calls: None,
            max_tokens: Some(200000),
        }),
    ] {
        let mut next = request("New budgets");
        next.budget = budget.clone();
        let receipt = harness.start(next).await.unwrap();
        let saved = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
        let budget = budget.unwrap_or_default();
        assert_eq!(saved.spec.planner.config["goal_budget"], json!(budget));
        for actor in &saved.members {
            let pinned = &actor.pinned_config["input"];
            assert_eq!(pinned["max_model_calls"], json!(budget.max_model_calls));
            assert_eq!(pinned["max_tokens"], json!(budget.max_tokens));
        }
    }
}
#[tokio::test]
async fn malformed_requests_do_not_create_sessions_and_unicode_previews_are_bounded() {
    let root = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(root.path().join("invalid.db")).await.unwrap();
    for input in [request(" "), request(&"🦀".repeat(17000))] {
        assert!(
            operator::execute(&agents, Request::HarnessStart { request: input })
                .await
                .is_err()
        );
    }
    let mut bad = request("Invalid endpoint");
    bad.target.endpoint = "https://user:password@example.com/v1/chat/completions".into();
    assert!(
        operator::execute(&agents, Request::HarnessStart { request: bad })
            .await
            .is_err()
    );
    assert!(
        agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .sessions()
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let input = request(&"🦀".repeat(400));
    agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .start(input)
        .await
        .unwrap();
    let page = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .sessions()
        .await
        .unwrap();
    assert!(page.items[0].prompt_truncated);
    assert_eq!(page.items[0].prompt_preview, "🦀".repeat(256));
    assert!(serde_json::from_value::<Request>(json!({"operation":"harness_start","request":{"message_id":MessageId::new(),"prompt":"x","target":{},"team_spec":{}}})).is_err());
}

#[tokio::test]
async fn turns_have_atomic_order_idempotent_admission_bounded_history_and_capacity() {
    let root = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(root.path().join("turns.db")).await.unwrap();
    let teams = agents.teams().unwrap();
    let harness = teams.harness().unwrap();
    let first = harness.start(request(&"🦀".repeat(8000))).await.unwrap();
    let a = HarnessTurnRequest {
        message_id: MessageId::new(),
        prompt: "Turn A".into(),
    };
    let b = HarnessTurnRequest {
        message_id: MessageId::new(),
        prompt: "Turn B".into(),
    };
    let (a_receipt, b_receipt) = tokio::join!(
        harness.append(first.session_id, a.clone()),
        harness.append(first.session_id, b)
    );
    let a_receipt = a_receipt.unwrap();
    let b_receipt = b_receipt.unwrap();
    let mut order = [a_receipt.sequence, b_receipt.sequence];
    order.sort();
    assert_eq!(order, [2, 3]);
    assert_eq!(
        harness
            .append(first.session_id, a.clone())
            .await
            .unwrap()
            .sequence,
        a_receipt.sequence
    );
    let mut changed = a.clone();
    changed.prompt = "Changed".into();
    assert!(matches!(
        harness.append(first.session_id, changed).await,
        Err(Error::Domain(DomainError::ConflictingMessage))
    ));
    let other = harness
        .start(request("Separate conversation"))
        .await
        .unwrap();
    assert!(matches!(
        harness.append(other.session_id, a).await,
        Err(Error::Domain(DomainError::ConflictingMessage))
    ));
    for n in 4..=65 {
        let receipt = harness
            .append(
                first.session_id,
                HarnessTurnRequest {
                    message_id: MessageId::new(),
                    prompt: format!("Turn {n}"),
                },
            )
            .await
            .unwrap();
        assert_eq!(receipt.sequence, n);
    }
    assert!(matches!(
        harness
            .append(
                first.session_id,
                HarnessTurnRequest {
                    message_id: MessageId::new(),
                    prompt: "No capacity".into()
                }
            )
            .await,
        Err(Error::QueueFull { .. })
    ));
    let latest = harness.conversation(first.session_id, None).await.unwrap();
    assert_eq!(latest.turns.len(), 32);
    assert_eq!(latest.turns.last().unwrap().sequence, 65);
    let middle = harness
        .conversation(first.session_id, latest.next_before)
        .await
        .unwrap();
    assert_eq!(middle.turns.len(), 32);
    let earliest = harness
        .conversation(first.session_id, middle.next_before)
        .await
        .unwrap();
    assert_eq!(earliest.turns.len(), 1);
    assert!(earliest.next_before.is_none());
    assert_eq!(earliest.turns[0].prompt, "🦀".repeat(2048));
    assert!(earliest.turns[0].prompt_truncated);
    assert_eq!(
        harness
            .sessions()
            .await
            .unwrap()
            .items
            .iter()
            .find(|s| s.session_id == first.session_id)
            .unwrap()
            .turn_count,
        65
    );
    assert!(matches!(
        harness.conversation(SessionId::new(), None).await,
        Err(Error::SessionNotFound(_))
    ));
    assert!(
        harness
            .conversation(first.session_id, Some(0))
            .await
            .is_err()
    );
}
