//! HTTP model decisions exercise the installed planner, imports, tests and durable waits.
#[path = "../examples/support/planner_dispatch_probe.rs"]
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
        live_worker: false,
        prepare_only: false,
        action_format: None,
        resume: None,
        stop_after: None,
    }
}
fn read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[tokio::test]
async fn flat_planner_recovers_refused_finish_using_real_parent_tests() {
    for format in ["json_schema", "function_calling"] {
        let root = tempfile::tempdir().unwrap();
        let report = probe::run(probe::Options {
            action_format: Some(format.into()),
            ..options(root.path())
        })
        .await
        .unwrap();
        assert_eq!(report["status"], "verified", "stop: {}", report["stop"]);
        assert_eq!(report["model_calls"], 6);
        assert_eq!(report["refusal_recovered"], true);
        assert_eq!(report["restarts"].as_array().unwrap().len(), 3);
        assert!(
            report["restarts"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["verified_after_restart"] == true)
        );
        assert_eq!(report["parent_tests_passed"], true);
        assert_eq!(report["source_unchanged"], true);
        assert_eq!(report["protected_unchanged"], true);
        assert_eq!(
            report["evidence"]["children"][0]["run"]["usage"]["admitted_model_calls"],
            4
        );
        let requests = report["evidence"]["requests"].as_array().unwrap();
        assert_eq!(
            requests[4]["result"]["action"],
            json!({"kind":"call_tool","tool":"run_tests","arguments":{}})
        );
        let prompt: Value =
            serde_json::from_str(requests[5]["saved_request"]["prompt"].as_str().unwrap()).unwrap();
        assert!(
            prompt["observations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|o| o["action"] == json!({"kind":"run_tests"}))
        );
        let directory = Path::new(report["directory"].as_str().unwrap());
        let refused = read(&directory.join("checkpoint-refusal.json"));
        assert_eq!(refused["changes"]["completion"]["reason"], "tests_not_run");
        assert!(
            refused["activities"]
                .as_array()
                .unwrap()
                .iter()
                .all(|a| a["spec"]["input"]["definition"]["id"]["name"] != "run_tests")
        );
        assert_eq!(
            report["evidence"]["run"]["usage"]["budget"]["max_model_calls"],
            Value::Null
        );
        assert_eq!(
            report["evidence"]["run"]["usage"]["budget"]["max_tokens"],
            Value::Null
        );
    }
}

async fn server(
    root: &Path,
    mode: &'static str,
) -> (
    std::path::PathBuf,
    Arc<Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    let seen = Arc::new(Mutex::new(vec![]));
    let copy = seen.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = root.join("target.json");
    std::fs::write(&target,json!({"provider":"openai","model":"planner-probe-fixture","protocol":"chat_completions","endpoint":format!("http://{}/v1/chat/completions",listener.local_addr().unwrap())}).to_string()).unwrap();
    let task = tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let first = {
                    let mut requests = copy.lock().unwrap();
                    requests.push(body.clone());
                    requests.len() == 1
                };
                async move {
                    let p: Value =
                        serde_json::from_str(body["messages"][1]["content"].as_str().unwrap())
                            .unwrap();
                    let worker = p.get("tool_observations").is_some();
                    let action = if worker {
                        if mode == "worker_question" {json!({"kind":"ask_human","question":"Essential worker information?"})}
                        else if mode == "worker_loop" {json!({"kind":"read_file","path":"src/lib.rs"})}
                        else { match p["turn"].as_u64().unwrap() {
                            0 => json!({"kind":"read_file","path":"src/lib.rs"}),
                            1 => {
                                let text = p["tool_observations"][0]["result"]["text"].as_str().unwrap();
                                assert!(text.contains("left - right"));
                                json!({"kind":"replace_text","path":"src/lib.rs","old":"left - right","new":"left + right"})
                            },
                            2 => json!({"kind":"run_tests"}),
                            3 => {
                                assert_eq!(p["tool_observations"][2]["result"]["ok"],true);
                                json!({"kind":"finish","summary":"Repaired addition and ran the real contracts."})
                            },
                            _ => panic!("unexpected worker turn"),
                        }}
                    } else if mode == "parent_baseline" {
                        let seen = |kind:&str| p["observations"].as_array().unwrap().iter().any(|o|o["action"]["kind"]==kind);
                        if p["turn"] == 0 {probe::fixture_action(&p,false)}
                        else if p["turn"] == 1 {json!({"kind":"run_tests"})}
                        else if !seen("inspect_child_changes") {json!({"kind":"inspect_child_changes","worker":"repairer","offset":0})}
                        else if !seen("import_child_changes") {json!({"kind":"import_child_changes","worker":"repairer"})}
                        else {
                            let observations=p["observations"].as_array().unwrap();
                            let import=observations.iter().rposition(|o|o["action"]["kind"]=="import_child_changes").unwrap();
                            let test=observations.iter().rposition(|o|o["action"]["kind"]=="run_tests").unwrap();
                            if test<import {json!({"kind":"run_tests"})}else {json!({"kind":"finish","summary":"Imported repair and verified fresh parent tests."})}
                        }
                    } else if mode == "question" {
                        json!({"kind":"ask_human","question":"Essential missing information?"})
                    } else if mode == "repeat" && p["turn"] == 4 {
                        probe::fixture_action(&json!({"turn":0,"observations":[]}), true)
                    } else {
                        probe::fixture_action(&p, true)
                    };
                    let mut reply = probe::fixture_reply(&body, action);
                    if mode == "retry" && first {
                        reply["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"] =
                            json!("{");
                    }
                    axum::Json(reply)
                }
            }),
        );
        axum::serve(listener, app).await.unwrap();
    });
    (target, seen, task)
}

#[tokio::test]
async fn early_baseline_test_does_not_replace_fresh_post_import_evidence_or_approval_restart() {
    let root = tempfile::tempdir().unwrap();
    let (target, _, server) = server(root.path(), "parent_baseline").await;
    let report = probe::run(probe::Options {
        target_config: Some(target),
        live_worker: true,
        action_format: Some("function_calling".into()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "verified");
    assert_eq!(report["model_calls"], 6);
    assert_eq!(report["worker_model_calls"], 4);
    assert_eq!(
        report["restarts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["kind"] == "test_approval")
            .count(),
        2
    );
    assert!(
        report["restarts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["verified_after_restart"] == true)
    );
    let mut evidence = report["evidence"].clone();
    assert!(probe::parent_test_passed_after_import(&evidence));
    let activities = evidence["activities"].as_array_mut().unwrap();
    let import = activities
        .iter()
        .find(|a| a["spec"]["handler"]["name"] == "planner.child_changes.import")
        .unwrap()["scheduling_revision"]
        .clone();
    let latest = activities
        .iter_mut()
        .rev()
        .find(|a| a["spec"]["input"]["definition"]["id"]["name"] == "run_tests")
        .unwrap();
    latest["scheduling_revision"] = import;
    assert!(
        !probe::parent_test_passed_after_import(&evidence),
        "pre-import passing tests must never verify imported changes"
    );
    server.abort();
}

#[tokio::test]
async fn model_worker_prepares_restarts_and_finishes_with_separate_requests_and_usage() {
    let root = tempfile::tempdir().unwrap();
    let (target, seen, server) = server(root.path(), "success").await;
    let prepared = probe::run(probe::Options {
        target_config: Some(target),
        live_worker: true,
        action_format: Some("function_calling".into()),
        prepare_only: true,
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(prepared["worker_mode"], "model");
    assert!(seen.lock().unwrap().is_empty());
    let directory = Path::new(prepared["directory"].as_str().unwrap());
    let manifest = read(&directory.join("manifest.json"));
    assert_eq!(
        manifest["worker_config"]["input"]["target"],
        manifest["pinned_config"]["input"]["target"]
    );
    assert_eq!(manifest["worker_config"]["input"]["script"], json!([]));
    let checkpoint = probe::run(probe::Options {
        resume: Some(directory.into()),
        stop_after: Some(probe::StopAfter::WorkerEditApproval),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(checkpoint["status"], "checkpoint");
    assert_eq!(checkpoint["worker_model_calls"], 2);
    assert_eq!(
        checkpoint["evidence"]["children"][0]["task"]["wait"]["payload"]["arguments"]["path"],
        "src/lib.rs"
    );
    let verified = probe::run(probe::Options {
        resume: Some(directory.into()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(verified["status"], "verified", "{}", verified["stop"]);
    assert_eq!(verified["model_calls"], 6);
    assert_eq!(verified["worker_model_calls"], 4);
    assert_eq!(verified["worker_usage"]["actual_input_tokens"], 40);
    assert_eq!(verified["planner_usage"]["actual_input_tokens"], 60);
    assert_eq!(
        verified["worker_usage"]["budget"]["max_model_calls"],
        Value::Null
    );
    assert_eq!(
        verified["worker_usage"]["budget"]["max_tokens"],
        Value::Null
    );
    assert_eq!(verified["worker_protected_unchanged"], true);
    assert_eq!(verified["restarts"].as_array().unwrap().len(), 4);
    assert!(
        verified["restarts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["verified_after_restart"] == true)
    );
    let worker_requests = verified["evidence"]["children"][0]["requests"]
        .as_array()
        .unwrap();
    assert_eq!(worker_requests.len(), 4);
    let captured = seen.lock().unwrap().clone();
    assert_eq!(captured.len(), 10);
    for request in worker_requests
        .iter()
        .chain(verified["evidence"]["requests"].as_array().unwrap())
    {
        assert!(captured.contains(&request["wire_body"]));
        assert_eq!(
            request["saved_request"]["target"]["action_format"],
            "function_calling"
        );
    }
    let reopened = probe::run(probe::Options {
        resume: Some(directory.into()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(reopened["status"], "verified");
    assert_eq!(seen.lock().unwrap().len(), 10);
    assert_eq!(manifest, read(&directory.join("manifest.json")));
    assert!(
        probe::run(probe::Options {
            resume: Some(directory.into()),
            live_worker: true,
            ..options(root.path())
        })
        .await
        .is_err()
    );
    server.abort();
}

#[tokio::test]
async fn model_worker_questions_and_call_guard_preserve_unfinished_work_without_fallback() {
    for mode in ["worker_question", "worker_loop"] {
        let root = tempfile::tempdir().unwrap();
        let (target, seen, server) = server(root.path(), mode).await;
        let report = probe::run(probe::Options {
            target_config: Some(target),
            live_worker: true,
            action_format: Some("function_calling".into()),
            ..options(root.path())
        })
        .await
        .unwrap();
        assert_eq!(report["status"], "not_verified");
        assert_eq!(report["model_calls"], 1);
        assert_eq!(
            report["stop"],
            if mode == "worker_question" {
                "needs_input"
            } else {
                "worker_call_guard"
            }
        );
        assert_eq!(
            report["worker_model_calls"],
            if mode == "worker_question" { 1 } else { 8 }
        );
        assert_eq!(report["parent_tests_passed"], false);
        let calls = seen.lock().unwrap().len();
        let directory = Path::new(report["directory"].as_str().unwrap());
        let reopened = probe::run(probe::Options {
            resume: Some(directory.into()),
            ..options(root.path())
        })
        .await
        .unwrap();
        assert_eq!(reopened["stop"], report["stop"]);
        assert_eq!(seen.lock().unwrap().len(), calls);
        assert_eq!(reopened["evidence"], report["evidence"]);
        assert_eq!(report["source_unchanged"], true);
        server.abort();
    }
}

#[tokio::test]
async fn function_format_retry_keeps_usage_and_exact_request_then_verifies_after_restarts() {
    let root = tempfile::tempdir().unwrap();
    let (target, seen, server) = server(root.path(), "retry").await;
    let report = probe::run(probe::Options {
        target_config: Some(target),
        action_format: Some("function_calling".into()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "verified");
    assert_eq!(report["model_calls"], 7);
    {
        let captured = seen.lock().unwrap();
        assert_eq!(captured.len(), 7);
        assert_eq!(captured[0], captured[1]);
    }
    let requests = report["evidence"]["requests"].as_array().unwrap();
    assert_eq!(requests.len(), 6);
    assert_eq!(requests[0]["attempts"], 2);
    assert_eq!(requests[0]["result"]["action"]["kind"], "delegate_task");
    assert_eq!(
        report["evidence"]["run"]["usage"]["actual_input_tokens"],
        70
    );
    assert_eq!(report["refusal_recovered"], true);
    assert!(
        report["restarts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["verified_after_restart"] == true)
    );
    assert_eq!(report["source_unchanged"], true);
    server.abort();
}

#[tokio::test]
async fn prepare_and_resume_keep_exact_requests_import_waits_and_model_pins() {
    for (checkpoint, format) in [
        (probe::StopAfter::ImportApproval, "json_schema"),
        (probe::StopAfter::Refusal, "json_schema"),
        (probe::StopAfter::ImportApproval, "function_calling"),
        (probe::StopAfter::Refusal, "function_calling"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (target, seen, server) = server(root.path(), "success").await;
        let prepared = probe::run(probe::Options {
            target_config: Some(target),
            prepare_only: true,
            action_format: Some(format.into()),
            ..options(root.path())
        })
        .await
        .unwrap();
        assert_eq!(prepared["status"], "prepared");
        assert!(seen.lock().unwrap().is_empty());
        let directory = Path::new(prepared["directory"].as_str().unwrap());
        let manifest = read(&directory.join("manifest.json"));
        let first = read(&directory.join("first-request.json"));
        if format == "function_calling" {
            assert_eq!(first["tool_choice"], "required");
            assert_eq!(first["parallel_tool_calls"], false);
            assert!(first.get("response_format").is_none());
            assert_eq!(
                manifest["pinned_config"]["input"]["target"]["action_format"],
                "function_calling"
            );
        }
        let saved = probe::run(probe::Options {
            resume: Some(directory.into()),
            stop_after: Some(checkpoint),
            ..options(root.path())
        })
        .await
        .unwrap();
        assert_eq!(saved["status"], "checkpoint");
        let calls = seen.lock().unwrap().len();
        assert_eq!(
            calls,
            if checkpoint == probe::StopAfter::ImportApproval {
                3
            } else {
                4
            }
        );
        assert_eq!(first, seen.lock().unwrap()[0]);
        assert_eq!(saved["parent_tests_passed"], false);
        let resumed = probe::run(probe::Options {
            resume: Some(directory.into()),
            ..options(root.path())
        })
        .await
        .unwrap();
        assert_eq!(resumed["status"], "verified", "stop: {}", resumed["stop"]);
        assert_eq!(seen.lock().unwrap().len(), 6);
        assert_eq!(manifest, read(&directory.join("manifest.json")));
        for (actual, exported) in seen
            .lock()
            .unwrap()
            .iter()
            .zip(resumed["evidence"]["requests"].as_array().unwrap())
        {
            assert_eq!(actual, &exported["wire_body"]);
        }
        server.abort();
    }
}

#[tokio::test]
async fn duplicate_delegation_stops_before_any_second_worker_and_stays_stopped_on_resume() {
    let root = tempfile::tempdir().unwrap();
    let (target, seen, server) = server(root.path(), "repeat").await;
    let report = probe::run(probe::Options {
        target_config: Some(target),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "not_verified");
    assert_eq!(report["stop"], "repeated_delegation");
    assert_eq!(seen.lock().unwrap().len(), 5);
    assert_eq!(report["evidence"]["children"].as_array().unwrap().len(), 1);
    assert_eq!(report["parent_tests_passed"], false);
    let resumed = probe::run(probe::Options {
        resume: Some(report["directory"].as_str().unwrap().into()),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(resumed["stop"], "repeated_delegation");
    assert_eq!(seen.lock().unwrap().len(), 5);
    assert_eq!(resumed["evidence"], report["evidence"]);
    server.abort();
}

#[tokio::test]
async fn essential_question_stops_without_approval_or_tool_fallback() {
    let root = tempfile::tempdir().unwrap();
    let (target, seen, server) = server(root.path(), "question").await;
    let report = probe::run(probe::Options {
        target_config: Some(target),
        ..options(root.path())
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "not_verified");
    assert_eq!(report["stop"], "needs_input");
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(report["parent_tests_passed"], false);
    assert!(
        report["evidence"]["children"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    server.abort();
}
