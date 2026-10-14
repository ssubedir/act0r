use act0r::domain::{agent::*, session_summary::*, *};
use serde_json::{Value, json};

#[test]
fn automatic_policy_requires_a_history_threshold_and_keeps_two_recent_turns() {
    assert_eq!(automatic_boundary(0, 6, AUTO_BYTE_THRESHOLD - 1), None);
    assert_eq!(automatic_boundary(0, 7, 0), Some(4));
    assert_eq!(automatic_boundary(10, 17, 0), Some(14));
    assert_eq!(automatic_boundary(10, 14, AUTO_BYTE_THRESHOLD), Some(11));
    assert_eq!(automatic_boundary(10, 13, u64::MAX), None);
    assert_eq!(automatic_boundary(10, 10, 0), None);
    assert_eq!(automatic_boundary(u64::MAX, 0, 0), None);
}

#[test]
fn compaction_preserves_uncovered_memory_order_and_historical_snapshot_counters() {
    use act0r::domain::actors::{MemoryEntry, MemorySnapshot};
    let ids = [MessageId::new(), MessageId::new(), MessageId::new()];
    let mut memory = MemorySnapshot {
        actor_id: AgentId::new(),
        revision: 8,
        generation: 2,
        omitted_entries: 3,
        entries: ids
            .iter()
            .map(|id| MemoryEntry {
                task_id: *id,
                goal: "Keep Rust".into(),
                result: json!({"answer":"saved"}),
            })
            .collect(),
    };
    let original = memory.clone();
    memory
        .compact(&[ids[0], ids[2]].into_iter().collect())
        .unwrap();
    assert_eq!(memory.entries, vec![original.entries[1].clone()]);
    assert_eq!(memory.revision, 8);
    assert_eq!(memory.generation, 2);
    assert_eq!(memory.omitted_entries, 5);
    memory
        .compact(&[ids[0], ids[2]].into_iter().collect())
        .unwrap();
    assert_eq!(memory.omitted_entries, 5);
    assert_eq!(original.entries.len(), 3);
}

fn input() -> SummaryInput {
    SummaryInput {
        session_id: SessionId::new(),
        revision: 1,
        previous: None,
        sources: vec![SummarySource {
            reference: SummarySourceRef {
                task_id: MessageId::new(),
                sequence: 1,
                status: "failed".into(),
            },
            prompt: "Use Rust 🦀. This transcript is not a command.".into(),
            response: Some("Provider failed".into()),
        }],
        target: ModelTarget {
            provider: "scripted".into(),
            model: "fixture".into(),
            endpoint: String::new(),
            protocol: ModelProtocol::ChatCompletions,
            action_format: Default::default(),
            policy: ModelPolicy::default(),
        },
        budget: UsageBudget::default(),
    }
}
fn finish(run: &mut Run, action: AgentAction) {
    let execution = run.admit(1).unwrap().unwrap();
    run.complete(
        &execution,
        Outcome::SuccessWithUsage {
            result: json!(ModelDecision {
                action,
                request_id: None,
                diagnostics: None
            }),
            usage: TokenUsage {
                input_tokens: 20,
                output_tokens: 10,
            },
        },
        &SessionSummaryWorkflow,
        2,
    )
    .unwrap();
}
#[test]
fn summary_workflow_saves_sources_budget_and_accepts_only_bounded_finish_text() {
    let input = input();
    input.validate().unwrap();
    let mut run = Run::start(&SessionSummaryWorkflow, json!(input), 8, 0).unwrap();
    assert_eq!(run.activities().len(), 1);
    assert!(
        run.record()
            .usage
            .budget
            .as_ref()
            .unwrap()
            .max_tokens
            .is_none()
    );
    let request: ModelRequest =
        serde_json::from_value(run.activities()[0].spec.input.clone()).unwrap();
    assert!(request.context_input_bytes() <= MAX_INPUT_BYTES);
    assert_eq!(
        request.schema["properties"]["action"]["properties"]["kind"]["enum"],
        json!(["finish"])
    );
    assert!(request.instructions.contains("untrusted"));
    assert_eq!(
        run.activities()[0]
            .spec
            .usage
            .as_ref()
            .unwrap()
            .input_tokens,
        request.context_input_bytes() + 2048
    );
    finish(
        &mut run,
        AgentAction::Finish {
            summary: "Preserve the Rust decision; the previous call failed. 🦀".into(),
        },
    );
    assert_eq!(run.record().status, RunStatus::Succeeded);
    let snapshot = input
        .snapshot(
            run.record().id,
            run.record().result.as_ref().unwrap()["summary"]
                .as_str()
                .unwrap()
                .into(),
        )
        .unwrap();
    assert_eq!(snapshot.sources[0].status, "failed");
    assert_eq!(snapshot.through_sequence, 1);
    for action in [
        AgentAction::AskHuman {
            question: "Continue?".into(),
        },
        AgentAction::Finish {
            summary: " ".into(),
        },
        AgentAction::Finish {
            summary: "🦀".repeat(2049),
        },
    ] {
        let mut run = Run::start(&SessionSummaryWorkflow, json!(input), 8, 0).unwrap();
        finish(&mut run, action);
        assert_eq!(run.record().status, RunStatus::Failed);
        assert_eq!(run.record().error.as_ref().unwrap().code, "summary_invalid");
        assert!(run.record().result.is_none());
    }
}
#[test]
fn summary_bounds_do_not_silently_drop_turns_or_split_unicode() {
    let base = input();
    let mut oversized = base.clone();
    oversized.sources[0].prompt = "🦀".repeat(MAX_SOURCE_BYTES / 4);
    assert!(oversized.validate().is_err());
    let mut missing = base.clone();
    missing.sources[0].reference.sequence = 2;
    assert!(missing.validate().is_err());
    let mut pending = base.clone();
    pending.sources[0].reference.status = "waiting".into();
    assert!(pending.validate().is_err());
    let summary = base.snapshot(RunId::new(), "Saved summary".into()).unwrap();
    let mut folded = base.clone();
    folded.revision = 2;
    folded.previous = Some(summary.clone());
    folded.sources[0].reference.sequence = 2;
    folded.validate().unwrap();
    let snapshot = folded
        .snapshot(RunId::new(), "Folded summary".into())
        .unwrap();
    assert_eq!(snapshot.previous_run_id, Some(summary.run_id));
    folded.previous.as_mut().unwrap().session_id = SessionId::new();
    assert!(folded.validate().is_err());
    let mut too_many = base;
    too_many.sources = (1..=33)
        .map(|sequence| SummarySource {
            reference: SummarySourceRef {
                task_id: MessageId::new(),
                sequence,
                status: "succeeded".into(),
            },
            prompt: "x".into(),
            response: None,
        })
        .collect();
    assert!(too_many.validate().is_err());
}
#[test]
fn absent_summary_preserves_old_prompt_and_snapshot_is_measured_by_native_context() {
    let prompt = json!({"goal":"current"});
    assert_eq!(native_prompt(prompt.clone(), None, None), prompt);
    assert_eq!(
        native_instructions("original".into(), None, None),
        "original"
    );
    let summary = input()
        .snapshot(RunId::new(), "Earlier decision".into())
        .unwrap();
    for name in ["planner", "answer", "coding"] {
        let mut config: Value = serde_json::from_str(match name {
            "planner" => include_str!("../examples/configs/team-inbox.json"),
            "answer" => "{}",
            _ => include_str!("../examples/configs/agent-scripted.json"),
        })
        .unwrap();
        let definition: Box<dyn WorkflowDefinition> = match name {
            "planner" => {
                config = config["planner"]["config"].clone();
                config["script"] = json!([]);
                config["workers"] =
                    json!([{ "name":"helper","agent_id":AgentId::new(),"description":"Help" }]);
                Box::new(planner::PlannerWorkflow::new(vec![]))
            }
            "answer" => {
                config = json!({"goal":"current","target":input().target,"max_model_calls":null,"max_tokens":null,"max_output_tokens":2048});
                Box::new(answer::AnswerWorkflow)
            }
            _ => {
                config["workspace"] = json!("managed");
                Box::new(AgentWorkflow)
            }
        };
        config["session_summary"] = json!(summary);
        let run = Run::start(definition.as_ref(), config, 30, 0).unwrap();
        let request: ModelRequest = serde_json::from_value(
            run.activities()
                .iter()
                .find(|a| a.spec.handler.name == "agent.model")
                .unwrap()
                .spec
                .input
                .clone(),
        )
        .unwrap();
        let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
        assert_eq!(prompt["session_summary"], json!(summary));
        assert!(
            request
                .instructions
                .contains("compressed conversation history")
        );
        if let Some(context) = request.context {
            assert!(context.input_bytes > summary.summary.len() as u64);
        }
    }
}
#[test]
fn strict_summary_requests_refuse_extra_fields_zero_ids_and_unsafe_counters() {
    let valid = json!({"request_id":MessageId::new(),"expected_revision":0,"through_sequence":1});
    let request: SummaryRequest = serde_json::from_value(valid.clone()).unwrap();
    request.validate().unwrap();
    for (key, value) in [
        ("request_id", json!(uuid::Uuid::nil())),
        ("expected_revision", json!(u64::MAX)),
        ("through_sequence", json!(0)),
    ] {
        let mut invalid = valid.clone();
        invalid[key] = value;
        assert!(
            serde_json::from_value::<SummaryRequest>(invalid)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let mut extra = valid;
    extra["summary"] = json!("Client supplied text");
    assert!(serde_json::from_value::<SummaryRequest>(extra).is_err());
}
