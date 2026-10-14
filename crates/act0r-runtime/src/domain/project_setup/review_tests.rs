use super::*;
use crate::domain::{harness::HarnessRequest, teams::TeamSpec};
fn original() -> TeamSpec {
    let request:HarnessRequest=serde_json::from_value(json!({"message_id":MessageId::new(),"prompt":"Work in the project","workspace":std::env::current_dir().unwrap(),"target":{"provider":"openai","model":"fixture","endpoint":"http://127.0.0.1:12345/v1","protocol":"chat_completions"},"budget":{"max_model_calls":5,"max_tokens":100000},"coding":{"writable_paths":["src/"],"test_command":{"argv":["node","old-tests.mjs"],"timeout_ms":10000},"coder_count":2}})).unwrap();
    request.team_spec().unwrap()
}
#[test]
fn reviewed_settings_retain_models_budgets_two_coders_and_read_only_helper() {
    let before = original();
    let mut coding = coding_settings(&before).unwrap();
    coding.writable_paths.push("lib/".into());
    coding.test_command.argv = vec!["node".into(), "new-tests.mjs".into()];
    coding.project_commands.insert(
        "check".into(),
        WorkspaceTestCommand {
            argv: vec!["node".into(), "--version".into()],
            timeout_ms: 1000,
        },
    );
    let after = with_coding_settings(&before, &coding).unwrap();
    assert_eq!(coding_settings(&after).unwrap(), coding);
    for field in [
        "target",
        "max_model_calls",
        "max_tokens",
        "goal_budget",
        "delegation",
        "completion_policy",
        "task_planning",
    ] {
        assert_eq!(after.planner.config[field], before.planner.config[field]);
    }
    assert_eq!(after.source, before.source);
    assert_eq!(after.workers.len(), 3);
    for (old, new) in before.workers.iter().zip(after.workers.iter()) {
        assert_eq!(old.name, new.name);
        assert_eq!(old.adapter, new.adapter);
        assert_eq!(old.description, new.description);
        assert_eq!(old.attempt_limit, new.attempt_limit);
        assert_eq!(old.queue_policy, new.queue_policy);
        if old.name == "helper" {
            assert_eq!(old.config, new.config);
        } else {
            assert_eq!(new.config["writable_paths"], json!(coding.writable_paths));
            assert_eq!(new.config["test_command"], json!(coding.test_command));
            assert_eq!(new.config["target"], old.config["target"]);
            assert!(
                new.config["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|id| id["name"] == "run_command")
            );
        }
    }
}
#[test]
fn removing_named_commands_removes_dispatch_only_from_editable_actors() {
    let mut before = original();
    before.workers[0].config["writable_paths"] = Value::Null;
    let mut coding = coding_settings(&before).unwrap();
    coding.project_commands.insert(
        "check".into(),
        WorkspaceTestCommand {
            argv: vec!["node".into(), "--version".into()],
            timeout_ms: 1000,
        },
    );
    let with_command = with_coding_settings(&before, &coding).unwrap();
    coding.project_commands.clear();
    let without = with_coding_settings(&with_command, &coding).unwrap();
    assert_eq!(without.workers[0].config, before.workers[0].config);
    for config in std::iter::once(&without.planner.config)
        .chain(without.workers.iter().skip(1).map(|w| &w.config))
    {
        assert!(config.get("project_commands").is_none());
        assert!(
            !config["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|id| id["name"] == "run_command")
        );
    }
}
#[test]
fn setup_review_refuses_worker_count_changes_noops_and_unsafe_paths() {
    let before = original();
    let mut coding = coding_settings(&before).unwrap();
    assert!(with_coding_settings(&before, &coding).is_err());
    coding.coder_count = 1;
    assert!(with_coding_settings(&before, &coding).is_err());
    coding.coder_count = 2;
    coding.writable_paths = vec!["../elsewhere".into()];
    assert!(with_coding_settings(&before, &coding).is_err());
}
