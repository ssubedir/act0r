use act0r::domain::{project_setup::*, *};
use serde_json::json;

#[test]
fn proposals_validate_general_commands_and_never_allow_unbounded_scope() {
    let proposal: ProjectSetupProposal=serde_json::from_value(json!({"summary":"Project documentation names a custom verifier", "writable_paths":["app.py"],"test_command":{"argv":["python","-m","pytest"],"timeout_ms":60000},"commands":[{"name":"lint","argv":["custom-linter","app.py"],"timeout_ms":10000}]})).unwrap();
    let coding = proposal.coding().unwrap();
    assert_eq!(coding.test_command.argv, ["python", "-m", "pytest"]);
    let mut invalid = proposal.clone();
    invalid.commands.push(invalid.commands[0].clone());
    assert!(invalid.coding().is_err());
    invalid = proposal.clone();
    invalid.writable_paths = vec!["../escape".into()];
    assert!(invalid.coding().is_err());
    invalid = proposal.clone();
    invalid.test_command.argv.clear();
    assert!(invalid.coding().is_err());
    invalid = proposal;
    invalid.summary = " ".into();
    assert!(invalid.coding().is_err());
}
#[test]
fn discovery_charges_actual_and_held_usage_before_the_single_coding_start() {
    let budget = UsageBudget {
        max_model_calls: Some(10),
        max_tokens: Some(1000),
    };
    let usage = UsageLedger {
        admitted_model_calls: 3,
        actual_input_tokens: 200,
        actual_output_tokens: 100,
        held_tokens: 400,
        ..Default::default()
    };
    assert_eq!(
        remaining_budget(&budget, &usage).unwrap(),
        UsageBudget {
            max_model_calls: Some(7),
            max_tokens: Some(300)
        }
    );
    assert_eq!(
        remaining_budget(&UsageBudget::default(), &usage).unwrap(),
        UsageBudget::default()
    );
    assert!(
        remaining_budget(
            &UsageBudget {
                max_model_calls: Some(3),
                max_tokens: None
            },
            &usage
        )
        .is_err()
    );
    assert!(
        remaining_budget(
            &UsageBudget {
                max_model_calls: None,
                max_tokens: Some(700)
            },
            &usage
        )
        .is_err()
    );
    let session = SessionId::new();
    let task = MessageId::new();
    assert_eq!(
        coding_start_id(session, task),
        coding_start_id(session, task)
    );
    assert_ne!(coding_start_id(session, task), task);
    assert_ne!(
        coding_start_id(session, task),
        coding_start_id(session, MessageId::new())
    );
}
