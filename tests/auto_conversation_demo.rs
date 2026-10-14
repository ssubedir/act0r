//! The shipped Auto example uses public routes, real Node checks and durable decisions.
#[path = "../examples/support/auto_conversation.rs"]
mod demo;
use serde_json::{Value, json};

fn options(root: &std::path::Path) -> demo::Options {
    demo::Options {
        output: root.to_owned(),
        target_config: None,
        prepare_only: false,
        resume: None,
        stop_after: None,
    }
}
fn read(path: impl AsRef<std::path::Path>) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[tokio::test]
async fn auto_demo_verifies_applies_follows_up_and_reopens_without_new_calls() {
    let root = tempfile::tempdir().unwrap();
    let report = demo::run(options(root.path())).await.unwrap();
    assert_eq!(report["status"], "verified");
    assert_eq!(report["model_calls"], 10);
    assert_eq!(report["applied_tests_ok"], true);
    assert_eq!(
        report["project_verification"]["report"]["status"],
        "verified"
    );
    assert_eq!(
        report["project_verification"]["report"]["before"]["digest"],
        report["project_verification"]["report"]["after"]["digest"]
    );
    assert_eq!(report["conversation"]["turns"].as_array().unwrap().len(), 3);
    let directory = std::path::Path::new(report["directory"].as_str().unwrap());
    for stage in [
        "question",
        "test",
        "application",
        "project-verification",
        "completed",
    ] {
        assert_eq!(
            read(directory.join(format!("restart-{stage}.json")))["verified"],
            true
        );
    }
    let before = read(directory.join("evidence.json"));
    let application = read(directory.join("application.json"));
    let verification = read(directory.join("project-verification.json"));
    let repeated = demo::run(demo::Options {
        resume: Some(directory.to_owned()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(repeated["model_calls"], report["model_calls"]);
    assert_eq!(repeated["session"], report["session"]);
    assert_eq!(read(directory.join("evidence.json")), before);
    assert_eq!(read(directory.join("application.json")), application);
    assert_eq!(
        read(directory.join("project-verification.json")),
        verification
    );
    assert!(
        std::fs::read_to_string(directory.join("applied-tests.log"))
            .unwrap()
            .contains("5 contracts passed")
    );
}

#[tokio::test]
async fn auto_demo_resumes_each_saved_checkpoint_and_refuses_changed_protected_files() {
    let root = tempfile::tempdir().unwrap();
    let first = demo::run(demo::Options {
        stop_after: Some(demo::StopAfter::Question),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(first["status"], "checkpoint");
    assert_eq!(first["model_calls"], 2);
    let directory = std::path::Path::new(first["directory"].as_str().unwrap());
    for (stage, label) in [
        (demo::StopAfter::SetupReview, "setup_review"),
        (demo::StopAfter::TestApproval, "test"),
        (demo::StopAfter::ApplicationApproval, "application_approval"),
        (
            demo::StopAfter::ProjectVerificationApproval,
            "project_verification_approval",
        ),
    ] {
        let result = demo::run(demo::Options {
            resume: Some(directory.to_owned()),
            stop_after: Some(stage),
            ..options(root.path())
        })
        .await
        .unwrap();
        assert_eq!(result["status"], "checkpoint");
        assert_eq!(result["phase"], label);
    }
    let result = demo::run(demo::Options {
        resume: Some(directory.to_owned()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(result["status"], "verified");
    assert_eq!(result["model_calls"], 10);
    std::fs::write(directory.join("project/contracts.mjs"), "changed checker").unwrap();
    let error = demo::run(demo::Options {
        resume: Some(directory.to_owned()),
        ..options(root.path())
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("fixture changed unexpectedly"));
}

#[tokio::test]
async fn auto_demo_prepares_exact_native_request_with_no_provider_calls_then_resumes_original_actors()
 {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let root = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let counted = count.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        axum::serve(listener,axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>|{
            counted.fetch_add(1,Ordering::SeqCst);
            assert!(body.get("response_format").is_none());assert_eq!(body["tool_choice"],"required");
            let prompt:Value=serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            let mut action=demo::fixture_action(&prompt);
            if action["kind"] == "propose_project_setup" {
                action["proposal"]["commands"] = json!([{"name":"verify-contracts","argv":["node","contracts.mjs"],"timeout_ms":60000}]);
            }
            let name=action.as_object_mut().unwrap().remove("kind").unwrap();
            async move{axum::Json(json!({"choices":[{"message":{"tool_calls":[{"id":"test-auto-call","type":"function","function":{"name":name,"arguments":action.to_string()}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
        }))).await.unwrap();
    });
    let config = root.path().join("target.json");
    std::fs::write(&config,json!({"target":{"provider":"openai","model":"auto-fixture","endpoint":endpoint,"protocol":"chat_completions"}}).to_string()).unwrap();
    let prepared = demo::run(demo::Options {
        target_config: Some(config),
        prepare_only: true,
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(prepared["status"], "prepared");
    assert_eq!(prepared["model_calls"], 0);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let directory = std::path::Path::new(prepared["directory"].as_str().unwrap());
    let request = read(directory.join("first-request.json"));
    assert_eq!(request["tool_choice"], "required");
    assert!(
        request["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["function"]["name"] == "propose_project_setup")
    );
    let root_receipt = read(directory.join("session.json"));
    let resumed = demo::run(demo::Options {
        resume: Some(directory.to_owned()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(resumed["status"], "verified");
    assert_eq!(resumed["session"], root_receipt);
    assert_eq!(count.load(Ordering::SeqCst), 10);
    assert_eq!(
        read(directory.join("setup-decision.json"))["coding"].get("project_commands"),
        None
    );
    server.abort();
}
