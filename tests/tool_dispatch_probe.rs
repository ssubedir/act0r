//! Exercise the shipped probe against HTTP decisions and actual runtime tools.
#[path = "../examples/support/tool_dispatch_probe.rs"]
mod probe;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

fn options(path: &Path) -> probe::Options {
    probe::Options {
        output: path.into(),
        target_config: None,
        prepare_only: false,
        resume: None,
    }
}
fn saved(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[tokio::test]
async fn explicit_contract_executes_real_native_tools_with_its_original_pins() {
    let root = tempfile::tempdir().unwrap();
    let report = probe::run(probe::Options {
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "verified");
    let directory = Path::new(report["directory"].as_str().unwrap());
    let manifest = saved(&directory.join("manifest.json"));
    assert_eq!(
        manifest["arms"][0]["pinned_config"]["input"]["model_contract_version"],
        4
    );
    assert_eq!(report["arms"][0]["model_calls"], 4);
    assert_eq!(
        report["arms"][0]["tool_sequence"],
        json!(["read_file", "replace_text", "run_tests"])
    );
    assert_eq!(
        report["arms"][0]["requests"][0]["saved_request"]["schema"]["properties"]["action"]["anyOf"]
            [0]["properties"]["kind"]["enum"][0],
        "read_file"
    );
    let first = &report["arms"][0]["requests"][0];
    assert_eq!(
        first["saved_request"]["native_tool_names"],
        json!(["read_file", "replace_text", "run_tests"])
    );
    assert_eq!(first["result"]["action"]["kind"], "call_tool");
    let next: Value = serde_json::from_str(
        report["arms"][0]["requests"][1]["saved_request"]["prompt"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(next["tool_observations"][0]["action"]["kind"], "read_file");
    assert!(next["tool_observations"][0]["action"].get("tool").is_none());

    assert_eq!(first["saved_request"]["native_tool_flattened"], true);
    assert_eq!(next["tool_observations"][0]["action"]["path"], "src/lib.rs");
    assert!(
        next["tool_observations"][0]["action"]
            .get("arguments")
            .is_none()
    );
}

async fn server(
    root: &Path,
    mode: &'static str,
) -> (
    std::path::PathBuf,
    Arc<Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let seen = requests.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let target = root.join("target.json");
    std::fs::write(&target, json!({"provider":"openai","model":"probe-fixture","protocol":"chat_completions","endpoint":endpoint,"policy":{"request_timeout_ms":30000,"connect_timeout_ms":1000,"max_attempts":2,"retry_delay_ms":1,"max_retry_delay_ms":1}}).to_string()).unwrap();
    let task = tokio::spawn(async move {
        let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            seen.lock().unwrap().push(body.clone());
            async move {
                let p: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                let native = p.get("tool_observations").is_some();
                let content = if native && mode == "invalid" { "no structured object".to_owned() }
                else {
                    let action = if native && mode == "premature" && p["turn"] == 0 {json!({"kind":"finish","summary":"Starting by reading src/lib.rs."})}
                    else if native && matches!(mode, "question" | "premature") {json!({"kind":"ask_human","question":"May I read the file?"})}
                    else {probe::fixture_action(&p)};
                    json!({"action":action}).to_string()
                };
                axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":content}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))
            }
        }));
        axum::serve(listener, app).await.unwrap();
    });
    (target, requests, task)
}

#[tokio::test]
async fn both_contracts_dispatch_real_tools_and_export_the_actual_http_bodies() {
    let root = tempfile::tempdir().unwrap();
    let (target, requests, server) = server(root.path(), "success").await;
    let report = probe::run(probe::Options {
        target_config: Some(target),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "verified");
    let directory = Path::new(report["directory"].as_str().unwrap());
    let wire = requests.lock().unwrap().clone();
    assert_eq!(wire.len(), 8);
    for (i, name) in ["native", "coding"].iter().enumerate() {
        let arm = &report["arms"][i];
        assert_eq!(arm["model_calls"], 4);
        assert_eq!(
            arm["tool_sequence"],
            json!(["read_file", "replace_text", "run_tests"])
        );
        assert_eq!(arm["tests_passed"], true);
        assert_eq!(arm["protected_unchanged"], true);
        assert_eq!(
            arm["usage"]["budget"],
            json!({"max_model_calls":null,"max_tokens":null})
        );
        for (j, request) in arm["requests"].as_array().unwrap().iter().enumerate() {
            assert_eq!(request["wire_body"], wire[i * 4 + j]);
            assert!(request["wire_body"].get("Authorization").is_none());
        }
        // The next real HTTP call contains the saved read feedback, before editing.
        let next: Value = serde_json::from_str(
            arm["requests"][1]["saved_request"]["prompt"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert!(next.to_string().contains("left - right"));
        assert_eq!(
            arm["requests"][0]["result"]["action"]["kind"],
            if *name == "native" {
                "call_tool"
            } else {
                "read_file"
            }
        );
        let workspace = Path::new(arm["workspace"].as_str().unwrap());
        assert!(
            std::fs::read_to_string(workspace.join("src/lib.rs"))
                .unwrap()
                .contains("left + right")
        );
        assert!(
            std::fs::read_to_string(directory.join(name).join("source/src/lib.rs"))
                .unwrap()
                .contains("left - right")
        );
        let evidence = saved(&directory.join(name).join("evidence.json"));
        assert_eq!(
            evidence["run"]["input_receipts"].as_array().unwrap().len(),
            2
        );
        assert!(
            arm["tool_results"][2]["result"]["stdout"]
                .as_str()
                .unwrap()
                .contains("3 passed")
        );
    }
    // Reopening completed tasks retains their exact identities and makes no calls.
    let resumed = probe::run(probe::Options {
        resume: Some(directory.into()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(resumed["status"], "verified");
    assert_eq!(requests.lock().unwrap().len(), 8);
    assert_eq!(
        resumed["arms"][0]["task"]["task"]["id"],
        report["arms"][0]["task"]["task"]["id"]
    );
    server.abort();
}

#[tokio::test]
async fn first_question_stops_native_arm_without_injecting_guidance_or_blocking_comparison() {
    let root = tempfile::tempdir().unwrap();
    let (target, requests, server) = server(root.path(), "question").await;
    let report = probe::run(probe::Options {
        target_config: Some(target),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "not_verified");
    assert_eq!(report["arms"][0]["stop"], "needs_input");
    assert_eq!(report["arms"][0]["model_calls"], 1);
    assert_eq!(report["arms"][0]["tool_results"], json!([]));
    assert_eq!(
        report["arms"][0]["task"]["wait"]["key"],
        "answer_clarification"
    );
    assert_eq!(report["arms"][1]["status"], "verified");
    assert_eq!(requests.lock().unwrap().len(), 5);
    server.abort();
}

#[tokio::test]
async fn malformed_decisions_save_retry_diagnostics_and_never_execute_tools() {
    let root = tempfile::tempdir().unwrap();
    let (target, requests, server) = server(root.path(), "invalid").await;
    let report = probe::run(probe::Options {
        target_config: Some(target),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "not_verified");
    assert_eq!(report["arms"][0]["model_calls"], 2);
    assert_eq!(report["arms"][0]["tool_results"], json!([]));
    assert_eq!(
        report["arms"][0]["requests"][0]["error"]["code"],
        "model_invalid_action"
    );
    assert_eq!(report["arms"][0]["requests"][0]["attempts"], 2);
    let wire = requests.lock().unwrap();
    assert_eq!(wire[0], wire[1]);
    assert_eq!(report["arms"][1]["status"], "verified");
    server.abort();
}

#[tokio::test]
async fn claimed_read_in_a_finish_summary_never_counts_as_executed_work() {
    let root = tempfile::tempdir().unwrap();
    let (target, requests, server) = server(root.path(), "premature").await;
    let report = probe::run(probe::Options {
        target_config: Some(target),
        ..options(root.path())
    })
    .await
    .unwrap();
    let arm = &report["arms"][0];
    assert_eq!(arm["status"], "not_verified");
    assert_eq!(arm["stop"], "needs_input");
    assert_eq!(arm["model_calls"], 2);
    assert_eq!(arm["tool_results"], json!([]));
    assert_eq!(arm["tests_passed"], false);
    let next: Value = serde_json::from_str(
        arm["requests"][1]["saved_request"]["prompt"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        next["tool_observations"][0]["completion"]["status"],
        "refused"
    );
    assert_eq!(
        next["tool_observations"][0]["completion"]["reason"],
        "tests_not_run"
    );
    assert_eq!(report["arms"][1]["status"], "verified");
    assert_eq!(requests.lock().unwrap().len(), 6);
    server.abort();
}

#[tokio::test]
async fn preparation_exports_payloads_without_calls_and_resume_rejects_changed_pins() {
    let root = tempfile::tempdir().unwrap();
    let (target, requests, server) = server(root.path(), "success").await;
    let report = probe::run(probe::Options {
        target_config: Some(target),
        prepare_only: true,
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "prepared");
    assert!(requests.lock().unwrap().is_empty());
    let directory = Path::new(report["directory"].as_str().unwrap());
    for name in ["native", "coding"] {
        let body = saved(&directory.join(name).join("first-request.json"));
        assert_eq!(body["model"], "probe-fixture");
        let evidence = saved(&directory.join(name).join("evidence.json"));
        assert_eq!(evidence["requests"][0]["attempts"], 0);
        assert_eq!(evidence["run"]["usage"]["admitted_model_calls"], 0);
    }
    let manifest_path = directory.join("manifest.json");
    let manifest = saved(&manifest_path);
    let mut wrong = manifest.clone();
    wrong["arms"][0]["pinned_config"]["input"]["target"]["model"] = json!("replacement");
    std::fs::write(&manifest_path, wrong.to_string()).unwrap();
    let error = probe::run(probe::Options {
        resume: Some(directory.into()),
        ..options(root.path())
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("pins differ"));
    assert!(requests.lock().unwrap().is_empty());
    std::fs::write(&manifest_path, manifest.to_string()).unwrap();
    let completed = probe::run(probe::Options {
        resume: Some(directory.into()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(completed["status"], "verified");
    assert_eq!(requests.lock().unwrap().len(), 8);
    assert_eq!(
        completed["arms"][0]["task"]["run_id"],
        report["arms"][0]["task"]["run_id"]
    );
    server.abort();
}
