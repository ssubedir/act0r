//! Exercise the shipped example through its public harness request/approval routes.
#[path = "../examples/support/dependent_coding.rs"]
mod demo;

fn assert_schema_combinators(schema: &serde_json::Value) {
    if schema.is_boolean() {
        return;
    }
    let object = schema
        .as_object()
        .expect("schema must be an object or boolean");
    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(value) = object.get(keyword) {
            let variants = value
                .as_array()
                .expect("schema combinator must be an array");
            assert!(!variants.is_empty(), "schema combinator must not be empty");
            for variant in variants {
                assert_schema_combinators(variant);
            }
        }
    }
    for keyword in ["properties", "$defs", "definitions", "patternProperties"] {
        if let Some(value) = object.get(keyword) {
            for child in value.as_object().unwrap().values() {
                assert_schema_combinators(child);
            }
        }
    }
    for keyword in ["items", "additionalProperties", "not", "if", "then", "else"] {
        if let Some(child) = object.get(keyword) {
            assert_schema_combinators(child);
        }
    }
}

async fn assert_saved_model_schemas(directory: &std::path::Path) {
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(directory.join("demo.db"))
            .read_only(true),
    )
    .await
    .unwrap();
    let activities: Vec<String> = sqlx::query_scalar(
        "SELECT payload FROM activities WHERE json_extract(payload, '$.spec.handler.name') = 'agent.model'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    pool.close().await;
    let mut coordination_requests = 0;
    for activity in activities {
        let activity: serde_json::Value = serde_json::from_str(&activity).unwrap();
        let request = &activity["spec"]["input"];
        assert_schema_combinators(&request["schema"]);
        let prompt: serde_json::Value =
            serde_json::from_str(request["prompt"].as_str().unwrap()).unwrap();
        coordination_requests += usize::from(prompt["coordinating_guidance"].is_object());
    }
    assert!(
        coordination_requests > 0,
        "must check dependency coordination schemas"
    );
}

#[tokio::test]
async fn dependent_coding_demo_restarts_transfers_tests_and_applies_once() {
    let root = tempfile::tempdir().unwrap();
    let report = demo::run(demo::Options {
        output: root.path().to_owned(),
        target_config: None,
        resume: None,
        wait_id: None,
        guidance_file: None,
        stop_after: None,
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "verified");
    assert_eq!(report["restarts"], 2);
    assert_eq!(report["receiver_calls_before_release"], 0);
    assert_eq!(report["group_joins"], 1);
    assert_eq!(report["dependency_bindings"], 1);
    assert_eq!(report["source_calls"], 4);
    assert_eq!(report["receiver_calls"], 4);
    let directory = std::path::Path::new(report["directory"].as_str().unwrap());
    assert_saved_model_schemas(directory).await;
    let evidence: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("test-evidence.json")).unwrap())
            .unwrap();
    for member in ["api", "consumer", "parent"] {
        let tests = &evidence[member]["changes"]["tests"];
        assert_eq!(tests["result"]["ok"], true);
        assert_eq!(
            tests["command"]["argv"],
            serde_json::json!([
                "cargo",
                "test",
                "--locked",
                "--offline",
                "--test",
                "contracts"
            ])
        );
        assert!(
            tests["result"]["stdout"]
                .as_str()
                .unwrap()
                .contains("8 passed")
        );
    }
}

fn resumed(directory: &std::path::Path) -> demo::Options {
    demo::Options {
        output: directory.join("must-not-create"),
        target_config: None,
        resume: Some(directory.to_owned()),
        wait_id: None,
        guidance_file: None,
        stop_after: None,
    }
}

#[tokio::test]
async fn consumer_dependency_check_accepts_imports_and_rejects_duplicated_formatting() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("src");
    std::fs::create_dir(&source).unwrap();
    let duplicated = include_str!("../examples/dependent-coding-project/src/consumer.rs");
    let variants = [
        (
            "pub fn render_greeting(user: &crate::api::User<'_>) -> String { format!(\"Hello, {}!\", crate::api::display_name(user)) }",
            true,
        ),
        (
            "use crate::api::{display_name, User};\npub fn render_greeting(user: &User<'_>) -> String { format!(\"Hello, {}!\", display_name(user)) }",
            true,
        ),
        (duplicated, false),
    ];
    for (index, (consumer, expected)) in variants.into_iter().enumerate() {
        std::fs::write(source.join("consumer.rs"), consumer).unwrap();
        assert_eq!(
            demo::consumer_uses_api(root.path(), &root.path().join(format!("check-{index}.log")))
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            std::fs::read_to_string(source.join("consumer.rs")).unwrap(),
            consumer
        );
    }
}

#[tokio::test]
async fn native_functions_cover_both_workers_dependency_transfer_and_fresh_parent_tests() {
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};
    let root = tempfile::tempdir().unwrap();
    let seen = Arc::new(Mutex::new(vec![]));
    let copy = seen.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let app=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
            copy.lock().unwrap().push(body.clone());
            assert!(body.get("response_format").is_none());
            assert_eq!(body["tool_choice"],"required");
            assert_eq!(body["parallel_tool_calls"],false);
            let prompt:Value=serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            let mut action=demo::scripted_action(&prompt).unwrap();
            if action["kind"] == "replace_text" && action["path"] == "src/consumer.rs" {
                action["old"] = json!(include_str!("../examples/dependent-coding-project/src/consumer.rs"));
                action["new"] = json!("use crate::api::{display_name, User};\npub fn render_greeting(user: &User<'_>) -> String { format!(\"Hello, {}!\", display_name(user)) }\n");
            }
            let name=action.as_object_mut().unwrap().remove("kind").unwrap();
            async move {axum::Json(json!({"choices":[{"finish_reason":"tool_calls","message":{"content":null,"tool_calls":[{"id":"native-dependent-call","type":"function","function":{"name":name,"arguments":action.to_string()}}]}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
        }));
        axum::serve(listener, app).await.unwrap();
    });
    let target = root.path().join("target.json");
    std::fs::write(&target,json!({"provider":"openai","model":"native-dependent-fixture","endpoint":endpoint,"protocol":"chat_completions","action_format":"function_calling"}).to_string()).unwrap();
    let report = demo::run(demo::Options {
        output: root.path().into(),
        target_config: Some(target),
        resume: None,
        wait_id: None,
        guidance_file: None,
        stop_after: None,
    })
    .await
    .unwrap();
    assert_eq!(report["status"], "verified");
    assert_eq!(report["source_calls"], 4);
    assert_eq!(report["receiver_calls"], 4);
    assert_eq!(report["planner_calls"], 9);
    assert_eq!(report["fresh_api_consumer_parent_tests"], true);
    assert_eq!(report["consumer_uses_api"], true);
    assert_eq!(report["receiver_calls_before_release"], 0);
    let directory = std::path::Path::new(report["directory"].as_str().unwrap());
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 17);
    let bodies: Vec<Value> = std::fs::read_dir(directory)
        .unwrap()
        .map(|p| p.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("model-request-")
        })
        .map(|p| serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap())
        .collect();
    assert_eq!(bodies.len(), 17);
    assert!(requests.iter().all(|body| bodies.contains(body)));
    assert_saved_model_schemas(directory).await;
    let reopened = demo::run(resumed(directory)).await.unwrap();
    assert_eq!(reopened["status"], "verified");
    assert_eq!(seen.lock().unwrap().len(), 17);
    server.abort();
}

fn saved(directory: &std::path::Path, name: &str) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(directory.join(name)).unwrap()).unwrap()
}

#[tokio::test]
async fn saved_guidance_and_both_approvals_resume_same_workers_and_apply_once() {
    use serde_json::{Value, json};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    let root = tempfile::tempdir().unwrap();
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let seen = requests.clone();
    let questions = Arc::new(AtomicBool::new(true));
    let questioning = questions.clone();
    let question_count = Arc::new(AtomicU64::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let mut p: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            seen.lock().unwrap().push(p.clone());
            let api = p["goal"].as_str().unwrap().starts_with("Repair src/api.rs");
            let action = if api && questioning.load(Ordering::SeqCst) {
                question_count.fetch_add(1, Ordering::SeqCst);
                json!({"kind":"ask_human","question":format!("May I start? {}", p["turn"])})
            } else {
                if api {
                    assert!(p.to_string().contains("Read src/api.rs and implement the assigned repair"));
                    assert_eq!(p["clarifications"].as_array().unwrap().iter().filter(|o| o["guidance"] == true).count(), 2);
                    p["turn"] = json!(p["turn"].as_u64().unwrap() - question_count.load(Ordering::SeqCst));
                }
                demo::scripted_action(&p).unwrap()
            };
            async move { axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":action}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}})) }
        }))).await.unwrap();
    });
    let target = root.path().join("target.json");
    std::fs::write(&target, json!({"provider":"openai","model":"resume-fixture","protocol":"chat_completions","endpoint":endpoint}).to_string()).unwrap();
    let error = demo::run(demo::Options {
        target_config: Some(target),
        ..resumed(root.path())
    })
    .await
    .unwrap_err();
    // That option combination is refused before creating a database.
    assert!(error.to_string().contains("cannot replace"));
    let target = root.path().join("target.json");
    let error = demo::run(demo::Options {
        output: root.path().to_owned(),
        resume: None,
        target_config: Some(target),
        ..resumed(root.path())
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("repeated questions"));
    assert_eq!(requests.lock().unwrap().len(), 4);
    let directory = std::fs::read_dir(root.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.is_dir())
        .unwrap();
    let initial = saved(&directory, "failed-checkpoint.json");
    let identity = saved(&directory, "session.json");
    let children = initial["delegated_work"]["groups"][0]["children"].clone();
    let wait: act0r::domain::WaitId = initial["inbox"]["items"][0]["wait"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let guidance = directory.join("guidance.txt");
    let text = "Read src/api.rs and implement the assigned repair. Use call_tool; the runtime handles approvals.";
    std::fs::write(&guidance, text).unwrap();
    let error = demo::run(demo::Options {
        wait_id: Some(act0r::domain::WaitId::new()),
        guidance_file: Some(guidance.clone()),
        ..resumed(&directory)
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("not pending"));
    for invalid in [" ".to_owned(), "x".repeat(65537)] {
        std::fs::write(&guidance, invalid).unwrap();
        assert!(
            demo::run(demo::Options {
                wait_id: Some(wait),
                guidance_file: Some(guidance.clone()),
                ..resumed(&directory)
            })
            .await
            .unwrap_err()
            .to_string()
            .contains("nonblank")
        );
    }
    assert_eq!(requests.lock().unwrap().len(), 4);
    assert!(!directory.join(format!("guidance-{wait}.json")).exists());
    std::fs::write(&guidance, text).unwrap();
    let error = demo::run(demo::Options {
        wait_id: Some(wait),
        guidance_file: Some(guidance.clone()),
        ..resumed(&directory)
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("repeated questions"));
    assert_eq!(requests.lock().unwrap().len(), 7);
    let latest = saved(&directory, "resume-report.json");
    assert_eq!(latest["status"], "needs_guidance");
    assert_eq!(latest["guidance"]["response"]["replayed"], false);
    let next_wait: act0r::domain::WaitId = latest["inbox"]["items"][0]["wait"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_ne!(next_wait, wait);
    // Replaying old guidance acknowledges only its receipt; it cannot answer the new wait.
    assert!(
        demo::run(demo::Options {
            wait_id: Some(wait),
            guidance_file: Some(guidance.clone()),
            ..resumed(&directory)
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("repeated questions")
    );
    assert_eq!(requests.lock().unwrap().len(), 7);
    assert_eq!(
        saved(&directory, "resume-report.json")["inbox"]["items"][0]["wait"]["id"],
        serde_json::json!(next_wait)
    );
    let wait = next_wait;
    questions.store(false, Ordering::SeqCst);
    let checkpoint = demo::run(demo::Options {
        wait_id: Some(wait),
        guidance_file: Some(guidance.clone()),
        stop_after: Some(demo::StopAfter::TransferApproval),
        ..resumed(&directory)
    })
    .await
    .unwrap();
    assert_eq!(checkpoint["phase"], "transfer_approval");
    let approval = saved(&directory, "checkpoint-approval.json");
    for index in 0..2 {
        assert_eq!(
            approval["delegated_work"]["groups"][0]["children"][index]["task_id"],
            children[index]["task_id"]
        );
        assert_eq!(
            approval["delegated_work"]["groups"][0]["children"][index]["run_id"],
            children[index]["run_id"]
        );
    }
    assert_eq!(approval["receiver"]["usage"]["admitted_model_calls"], 0);
    assert_eq!(
        saved(&directory, "resume-report.json")["guidance"]["response"]["replayed"],
        false
    );
    let calls = requests.lock().unwrap().len();
    std::fs::write(&guidance, "Changed guidance").unwrap();
    assert!(
        demo::run(demo::Options {
            wait_id: Some(wait),
            guidance_file: Some(guidance.clone()),
            ..resumed(&directory)
        })
        .await
        .unwrap_err()
        .to_string()
        .contains("conflicts")
    );
    std::fs::write(&guidance, text).unwrap();
    demo::run(demo::Options {
        wait_id: Some(wait),
        guidance_file: Some(guidance),
        stop_after: Some(demo::StopAfter::TransferApproval),
        ..resumed(&directory)
    })
    .await
    .unwrap();
    assert_eq!(requests.lock().unwrap().len(), calls);
    assert_eq!(
        saved(&directory, "resume-report.json")["guidance"]["response"]["replayed"],
        true
    );
    let checkpoint = demo::run(demo::Options {
        stop_after: Some(demo::StopAfter::ApplicationApproval),
        ..resumed(&directory)
    })
    .await
    .unwrap();
    assert_eq!(checkpoint["phase"], "application_approval");
    assert!(
        std::fs::read_to_string(directory.join("project/src/api.rs"))
            .unwrap()
            .contains("user.first.trim().to_owned()")
    );
    let application = saved(&directory, "application-request.json");
    let calls = requests.lock().unwrap().len();
    let report = demo::run(resumed(&directory)).await.unwrap();
    assert_eq!(report["status"], "verified");
    assert_eq!(report["source_calls"], 10);
    assert_eq!(report["receiver_calls"], 4);
    assert_eq!(report["group_joins"], 1);
    assert_eq!(report["dependency_bindings"], 1);
    assert_eq!(report["session"], identity);
    assert_eq!(
        report["approved_application"],
        application["application_id"]
    );
    assert!(!directory.join("must-not-create").exists());
    let replayed = demo::run(resumed(&directory)).await.unwrap();
    assert_eq!(
        replayed["approved_application"],
        report["approved_application"]
    );
    assert_eq!(requests.lock().unwrap().len(), calls);
    let (_, agents) = act0r::bootstrap::local_app(directory.join("demo.db"))
        .await
        .unwrap();
    let apps = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .applications(
            identity["session_id"].as_str().unwrap().parse().unwrap(),
            identity["task_id"].as_str().unwrap().parse().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(apps.items.len(), 1);
    let evidence = saved(&directory, "test-evidence.json");
    for member in ["api", "consumer", "parent"] {
        assert_eq!(evidence[member]["changes"]["tests"]["result"]["ok"], true);
        assert!(
            evidence[member]["changes"]["tests"]["result"]["stdout"]
                .as_str()
                .unwrap()
                .contains("8 passed")
        );
    }
    server.abort();
}

#[tokio::test]
async fn incompatible_live_decisions_stop_before_unbounded_questions_or_repeated_delegation() {
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};

    for incompatibility in [
        "missing_dependency",
        "repeated_questions",
        "repeated_delegation",
    ] {
        let missing_dependency = incompatibility == "missing_dependency";
        let repeated_delegation = incompatibility == "repeated_delegation";
        let root = tempfile::tempdir().unwrap();
        let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let seen = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let p: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                seen.lock().unwrap().push(p.clone());
                let goal = p["goal"].as_str().unwrap();
                let action = if repeated_delegation {
                    let imported = p["observations"].as_array().is_some_and(|observations| observations.iter().any(|o| o["action"]["kind"] == "import_child_changes"));
                    let refused = p["observations"].as_array().is_some_and(|observations| observations.iter().any(|o| o["completion"]["status"] == "refused"));
                    if goal.starts_with("Use exactly one") && imported {
                        if refused { serde_json::from_str(goal.lines().last().unwrap()).unwrap() }
                        else { json!({"kind":"finish","summary":"The imported patch passed parent tests"}) }
                    } else { demo::scripted_action(&p).unwrap() }
                } else if goal.starts_with("Use exactly one") {
                    let mut action: Value = serde_json::from_str(goal.lines().last().unwrap()).unwrap();
                    if missing_dependency { action["tasks"][0].as_object_mut().unwrap().remove("depends_on"); }
                    action
                } else {
                    json!({"kind":"ask_human","question":format!("May I proceed? Request {}", p["turn"])})
                };
                async move { axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":action}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}})) }
            }));
            axum::serve(listener, app).await.unwrap();
        });
        let target = root.path().join("target.json");
        std::fs::write(&target, serde_json::to_vec(&json!({"provider":"openai","model":"incompatible-demo-model","protocol":"chat_completions","endpoint":endpoint})).unwrap()).unwrap();
        let error = demo::run(demo::Options {
            output: root.path().to_owned(),
            target_config: Some(target),
            resume: None,
            wait_id: None,
            guidance_file: None,
            stop_after: None,
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains(if repeated_delegation {
            "repeated delegation"
        } else if missing_dependency {
            "omitted the structured depends_on"
        } else {
            "repeated questions"
        }));
        let calls = requests.lock().unwrap().clone();
        assert_eq!(
            calls.len(),
            if repeated_delegation {
                17
            } else if missing_dependency {
                1
            } else {
                4
            }
        );
        assert!(
            repeated_delegation
                || calls.iter().all(|p| {
                    !p["goal"]
                        .as_str()
                        .unwrap()
                        .starts_with("After the approved")
                })
        );
        let directory = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.is_dir())
            .unwrap();
        let evidence: Value = serde_json::from_slice(
            &std::fs::read(directory.join("failed-checkpoint.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            evidence["reason"],
            if repeated_delegation {
                "repeated_delegation"
            } else if missing_dependency {
                "missing_initial_dependency"
            } else {
                "repeated_questions"
            }
        );
        assert!(
            std::fs::read_to_string(directory.join("project/src/api.rs"))
                .unwrap()
                .contains("user.first.trim().to_owned()")
        );
        assert!(!directory.join("combined.patch").exists());
        if repeated_delegation {
            assert_eq!(evidence["delegation_proposals"], 2);
            assert_eq!(
                evidence["delegated_work"]["groups"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(evidence["delegated_work"]["groups"][0]["joined"], true);
            assert_eq!(
                calls
                    .iter()
                    .filter(|p| p["goal"].as_str().unwrap().starts_with("Repair src/api.rs"))
                    .count(),
                4
            );
            assert_eq!(
                calls
                    .iter()
                    .filter(|p| p["goal"]
                        .as_str()
                        .unwrap()
                        .starts_with("After the approved"))
                    .count(),
                4
            );
            let refused = calls.last().unwrap()["observations"]
                .as_array()
                .unwrap()
                .iter()
                .find(|o| o["completion"]["status"] == "refused")
                .unwrap();
            assert_eq!(refused["completion"]["reason"], "tests_not_run");
            assert!(
                demo::run(resumed(&directory))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("repeated delegation")
            );
            assert_eq!(*requests.lock().unwrap(), calls);
            assert!(!directory.join("application-request.json").exists());
        }
        server.abort();
    }
}

#[tokio::test]
async fn resume_preserves_provider_failure_and_rejects_tampered_request_pins() {
    use serde_json::{Value, json};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let root = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let model = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            counted.fetch_add(1, Ordering::SeqCst);
            let p: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            let planner = p["goal"].as_str().unwrap().starts_with("Use exactly one");
            let action = if planner { demo::scripted_action(&p).unwrap().to_string() } else { "invalid fixture action".to_owned() };
            let content = if planner { json!({"action":serde_json::from_str::<Value>(&action).unwrap()}).to_string() } else { action };
            async move { axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":content}}],"usage":{"prompt_tokens":10,"completion_tokens":10}})) }
        }))).await.unwrap();
    });
    let target = root.path().join("target.json");
    std::fs::write(&target, json!({"provider":"openai","model":"failed-provider-fixture","protocol":"chat_completions","endpoint":endpoint}).to_string()).unwrap();
    assert!(
        demo::run(demo::Options {
            output: root.path().to_owned(),
            resume: None,
            target_config: Some(target),
            ..resumed(root.path())
        })
        .await
        .is_err()
    );
    let directory = std::fs::read_dir(root.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.is_dir())
        .unwrap();
    let before_calls = calls.load(Ordering::SeqCst);
    // The adapter's pinned invalid-action repair makes a second worker request.
    assert_eq!(before_calls, 3);
    for _ in 0..2 {
        assert!(demo::run(resumed(&directory)).await.is_err());
        let report = saved(&directory, "resume-report.json");
        assert_eq!(report["status"], "model_failure");
        assert!(
            report["runs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["error"]["code"] == "model_invalid_action")
        );
        assert_eq!(calls.load(Ordering::SeqCst), before_calls);
    }
    let original = saved(&directory, "request.json");
    let mut tampered = original.clone();
    tampered["target"]["model"] = json!("replacement-model");
    std::fs::write(directory.join("request.json"), tampered.to_string()).unwrap();
    assert!(
        demo::run(resumed(&directory))
            .await
            .unwrap_err()
            .to_string()
            .contains("original team model")
    );
    std::fs::write(directory.join("request.json"), original.to_string()).unwrap();
    std::fs::write(
        directory.join("project/tests/contracts.rs"),
        "changed tests",
    )
    .unwrap();
    assert!(
        demo::run(resumed(&directory))
            .await
            .unwrap_err()
            .to_string()
            .contains("protected fixture")
    );
    assert_eq!(calls.load(Ordering::SeqCst), before_calls);
    model.abort();
}
