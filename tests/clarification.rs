use act0r::{
    adapters::native_tools::EchoTool,
    application::ports::NativeTool,
    domain::{agent::*, answer::ToolAgentWorkflow, clarification::*, *},
};
use serde_json::{Value, json};

fn fixture(policy: bool, approved_tool: bool) -> (ToolAgentWorkflow, Run) {
    let mut echo = EchoTool.definition();
    echo.requires_approval = approved_tool;
    let workflow = ToolAgentWorkflow::new(vec![echo.clone()]);
    let mut config = json!({"goal":"Inspect the assigned evidence", "target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":null,"max_tokens":null,"max_output_tokens":512,"context":{"max_observations":1},"tools":[echo.id],"tool_definitions":[echo]});
    if policy {
        config["clarification_policy"] = json!(ClarificationPolicy::default());
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
fn decision(workflow: &ToolAgentWorkflow, run: &mut Run, action: AgentAction) {
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
                input_tokens: 5,
                output_tokens: 5,
            },
        },
        workflow,
        0,
    )
    .unwrap();
}
fn ask(workflow: &ToolAgentWorkflow, run: &mut Run, n: usize) -> WaitId {
    decision(
        workflow,
        run,
        AgentAction::AskHuman {
            question: format!("May I inspect the file? {n} 🙂"),
        },
    );
    run.record().wait.as_ref().unwrap().id
}
fn answer(workflow: &ToolAgentWorkflow, run: &mut Run, wait: WaitId, text: &str) -> bool {
    run.respond_to_human(
        wait,
        HumanResponse::Answer {
            answer: text.into(),
        },
        Some(workflow),
        0,
    )
    .unwrap()
}

#[test]
fn bounded_clarifications_enter_guidance_without_another_call_and_resume_exactly_once() {
    let (workflow, mut run) = fixture(true, false);
    for n in 1..=2 {
        let wait = ask(&workflow, &mut run, n);
        assert_eq!(
            run.record().wait.as_ref().unwrap().key,
            "answer_clarification"
        );
        assert!(!answer(
            &workflow,
            &mut run,
            wait,
            "Use read_file; the scope is already pinned"
        ));
        let prompt: Value = serde_json::from_str(&request(&run).prompt).unwrap();
        assert_eq!(
            prompt["clarifications"][0]["response"]["answer"],
            "Use read_file; the scope is already pinned"
        );
        assert_eq!(prompt["clarification_status"]["consecutive_questions"], n);
    }
    // The context evicts earlier answers, but the separate durable counter retains them.
    let wait = ask(&workflow, &mut run, 3);
    assert_eq!(run.record().wait.as_ref().unwrap().key, WAIT_KEY);
    assert_eq!(
        run.record().wait.as_ref().unwrap().payload["clarifications"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(run.record().usage.admitted_model_calls, 3);
    assert!(run.admit(0).unwrap().is_none());
    let saved = serde_json::to_value(&run).unwrap();
    assert!(
        run.respond_to_human(
            wait,
            HumanResponse::Approval { approved: true },
            Some(&workflow),
            0
        )
        .is_err()
    );
    assert!(
        run.respond_to_human(
            wait,
            HumanResponse::Answer { answer: " ".into() },
            Some(&workflow),
            0
        )
        .is_err()
    );
    assert!(
        run.respond_to_human(
            wait,
            HumanResponse::Answer {
                answer: "x".repeat(65_537)
            },
            Some(&workflow),
            0
        )
        .is_err()
    );
    assert_eq!(serde_json::to_value(&run).unwrap(), saved);
    assert!(!answer(
        &workflow,
        &mut run,
        wait,
        "Read the file and proceed within your assigned scope"
    ));
    let scheduled = serde_json::to_value(&run).unwrap();
    assert!(answer(
        &workflow,
        &mut run,
        wait,
        "Read the file and proceed within your assigned scope"
    ));
    assert_eq!(serde_json::to_value(&run).unwrap(), scheduled);
    assert!(
        run.respond_to_human(
            wait,
            HumanResponse::Answer {
                answer: "Changed guidance".into()
            },
            Some(&workflow),
            0
        )
        .is_err()
    );
    let next = request(&run);
    let prompt: Value = serde_json::from_str(&next.prompt).unwrap();
    assert_eq!(prompt["clarifications"][0]["guidance"], true);
    assert_eq!(prompt["clarification_status"]["consecutive_questions"], 0);
    assert!(
        next.instructions
            .starts_with("Execute the assigned task with the listed tools.")
    );
    assert!(next.instructions.contains("the runtime handles approvals"));
    assert_eq!(
        run.record().usage.budget.as_ref().unwrap().max_model_calls,
        None
    );
    assert_eq!(run.record().usage.budget.as_ref().unwrap().max_tokens, None);
}

#[test]
fn requesting_a_tool_keeps_approval_and_only_saved_tool_feedback_resets_questions() {
    let (workflow, mut run) = fixture(true, true);
    for n in 1..=2 {
        let wait = ask(&workflow, &mut run, n);
        answer(&workflow, &mut run, wait, "Continue within scope");
    }
    decision(
        &workflow,
        &mut run,
        AgentAction::CallTool {
            tool: "echo".into(),
            arguments: json!({"text":"evidence"}),
        },
    );
    let wait = run.record().wait.as_ref().unwrap().clone();
    assert_eq!(wait.key, "native_tool_approval");
    assert_eq!(
        run.record().state["clarification"]["consecutive_questions"],
        2
    );
    assert!(
        run.respond_to_human(
            wait.id,
            HumanResponse::Answer {
                answer: "Go ahead".into()
            },
            Some(&workflow),
            0
        )
        .is_err()
    );
    run.respond_to_human(
        wait.id,
        HumanResponse::Approval { approved: true },
        Some(&workflow),
        0,
    )
    .unwrap();
    assert_eq!(
        run.record().state["clarification"]["consecutive_questions"],
        2
    );
    let execution = run.admit(0).unwrap().unwrap();
    assert_eq!(execution.handler.name, "native.tool");
    run.complete(
        &execution,
        Outcome::Success(json!({"ok":false,"error":{"message":"Expected refusal"}})),
        &workflow,
        0,
    )
    .unwrap();
    assert_eq!(
        run.record().state["clarification"]["consecutive_questions"],
        0
    );
    let wait = ask(&workflow, &mut run, 4);
    assert_eq!(
        run.record().wait.as_ref().unwrap().key,
        "answer_clarification"
    );
    answer(&workflow, &mut run, wait, "Try another permitted tool");
    decision(
        &workflow,
        &mut run,
        AgentAction::CallTool {
            tool: "echo".into(),
            arguments: json!({"text":"next"}),
        },
    );
    let wait = run.record().wait.as_ref().unwrap().id;
    run.respond_to_human(
        wait,
        HumanResponse::Approval { approved: false },
        Some(&workflow),
        0,
    )
    .unwrap();
    assert_eq!(
        run.record().state["clarification"]["consecutive_questions"],
        0
    );
}

#[test]
fn optional_clarification_policy_and_guard_state_are_validated() {
    let (workflow, mut run) = fixture(false, false);
    for n in 0..5 {
        let wait = ask(&workflow, &mut run, n);
        assert_eq!(
            run.record().wait.as_ref().unwrap().key,
            "answer_clarification"
        );
        answer(&workflow, &mut run, wait, "Saved answer");
    }
    let minimal = request(&run);
    assert!(
        minimal
            .instructions
            .starts_with("Execute the assigned task with the listed tools.")
    );
    assert!(!minimal.prompt.contains("clarification_status"));
    assert!(run.record().state.get("clarification").is_none());
    let (workflow, run) = fixture(true, false);
    for state in [
        json!(null),
        json!({"consecutive_questions":4,"needs_guidance":false}),
        json!({"consecutive_questions":3,"needs_guidance":false}),
        json!({"consecutive_questions":3,"needs_guidance":true}),
    ] {
        let mut saved = run.record().state.clone();
        saved["clarification"] = state;
        assert!(workflow.validate_state(&saved).is_err());
    }
    for policy in [
        json!({"version":2,"max_consecutive_questions":3}),
        json!({"version":1,"max_consecutive_questions":0}),
        json!({"version":1,"max_consecutive_questions":9}),
    ] {
        let mut saved = run.record().state["config"].clone();
        saved["clarification_policy"] = policy;
        assert!(Run::start(&workflow, saved, 30, 0).is_err());
    }
}

#[tokio::test]
async fn reopening_preserves_pinned_questions_exact_requests_usage_and_receipts() {
    use act0r::{adapters::sqlite::SqliteStore, application::ports::ExecutionStore};
    let (workflow, mut run) = fixture(false, false);
    for n in 1..=2 {
        let wait = ask(&workflow, &mut run, n);
        answer(&workflow, &mut run, wait, "Previously saved answer");
    }
    let wait = ask(&workflow, &mut run, 3);
    let original = serde_json::to_value(&run).unwrap();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("minimal.db");
    let store = SqliteStore::open(&path).await.unwrap();
    store.create(run.clone()).await.unwrap();
    store.close().await;
    let store = SqliteStore::open(&path).await.unwrap();
    let mut restored = store.get(run.record().id).await.unwrap();
    // Pending events are consumed on persistence, so compare authoritative records and activities.
    assert_eq!(
        serde_json::to_value(restored.record()).unwrap(),
        original["record"]
    );
    assert_eq!(
        serde_json::to_value(restored.activities()).unwrap(),
        original["activities"]
    );
    assert_eq!(
        serde_json::to_value(restored.attempts()).unwrap(),
        original["attempts"]
    );
    workflow.validate_state(&restored.record().state).unwrap();
    answer(
        &workflow,
        &mut restored,
        wait,
        "Continue this minimal question",
    );
    assert_eq!(restored.record().usage.admitted_model_calls, 3);
    assert!(
        request(&restored)
            .instructions
            .starts_with("Execute the assigned task with the listed tools.")
    );
    assert!(restored.record().state.get("clarification").is_none());
    let pool =
        sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&path))
            .await
            .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA user_version")
            .fetch_one(&pool)
            .await
            .unwrap(),
        62
    );
    pool.close().await;
    store.close().await;
}
