//! Real compatible HTTP calls overlap, join after restart, and import verified patches.
use super::*;
use act0r::domain::{change_review::TestEvidenceStatus, completion::CompletionStatus};
use std::sync::atomic::{AtomicUsize, Ordering};

const ORIGINAL: &str = "pub fn value() -> i32 { 0 }\n";
fn repaired(value: usize) -> String {
    format!(
        "pub fn value() -> i32 {{ {value} }}\n#[test]\nfn repaired_{value}() {{ assert_eq!(value(), {value}); }}\n"
    )
}
fn tool(name: &str, arguments: Value) -> Value {
    json!({"kind":"call_tool","tool":name,"arguments":arguments})
}
fn assert_scope_schema(schema: &Value) {
    let variants = schema["properties"]["action"]["anyOf"].as_array().unwrap();
    let singles: Vec<_> = variants
        .iter()
        .filter(|v| v["properties"]["kind"]["enum"][0] == "delegate_task")
        .collect();
    assert_eq!(singles.len(), 2);
    let group = variants
        .iter()
        .find(|v| v["properties"]["kind"]["enum"][0] == "delegate_tasks")
        .unwrap();
    let members = group["properties"]["tasks"]["items"]["anyOf"]
        .as_array()
        .unwrap();
    assert_eq!(members.len(), 4);
    assert_eq!(
        members
            .iter()
            .filter(|m| m["properties"].get("depends_on").is_some())
            .count(),
        2
    );
    for alternative in singles.into_iter().chain(members.iter()) {
        assert_eq!(alternative["additionalProperties"], false);
        let required = alternative["required"].as_array().unwrap();
        assert!(
            alternative["properties"]
                .as_object()
                .unwrap()
                .keys()
                .all(|key| required.contains(&json!(key)))
        );
    }
    assert!(
        members
            .iter()
            .any(|v| v["properties"].get("writable_paths").is_none())
    );
    assert!(members.iter().any(|v| {
        v["required"]
            .as_array()
            .unwrap()
            .contains(&json!("writable_paths"))
    }));
}
async fn model(conflict: bool, scoped: bool) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let task = tokio::spawn(async move {
        axum::serve(listener,axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
            counted.fetch_add(1,Ordering::SeqCst);
            let barrier=barrier.clone();
            async move {
                let prompt:Value=serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                let turn=prompt["turn"].as_u64().unwrap();
                let goal=prompt["goal"].as_str().unwrap();
                let second=goal.contains("[second]");
                let child=second || goal.contains("[first]");
                if child && turn==0 {
                    // A single-slot host cannot satisfy this barrier: prove both calls overlap.
                    tokio::time::timeout(Duration::from_secs(10),barrier.wait()).await.unwrap();
                }
                let action=if child {
                    let step=if second {turn.checked_sub(1)} else {Some(turn)};
                    let path=if second && !conflict {"src/b.rs"} else {"src/a.rs"};
                    if scoped { assert_eq!(prompt["writable_paths"], json!([path])); }
                    let outside=if path=="src/a.rs" {"src/b.rs"} else {"src/a.rs"};
                    if scoped && step==Some(0) {
                        tool("read_file",json!({"path":outside}))
                    } else if scoped && step==Some(1) {
                        tool("replace_text",json!({"path":outside,"old":ORIGINAL,"new":"should be denied"}))
                    } else if scoped && step==Some(2) {
                        tool("create_file",json!({"path":"src/unassigned.rs","content":"should be denied"}))
                    } else { match step.map(|n| if scoped {n-3} else {n}) {
                        None=>json!({"kind":"ask_human","question":"Continue second coding task?"}),
                        Some(0)=>tool("replace_text",json!({"path":path,"old":ORIGINAL,"new":repaired(if second {2} else {1})})),
                        Some(1)=>tool("run_tests",json!({})),
                        Some(2)=>json!({"kind":"finish","summary":"Verified isolated child patch"}),
                        _=>panic!("unexpected child turn {turn}: {prompt}"),
                    } }
                } else {
                    match turn {
                        0=>{
                            assert!(body["response_format"]["json_schema"]["schema"].to_string().contains("delegate_tasks"));
                            let mut action=json!({"kind":"delegate_tasks","tasks":[
                                {"worker":"coder","goal":"Repair src/a.rs independently [first]"},
                                {"worker":"coder2","goal":if conflict {"Repair src/a.rs independently [second]"} else {"Repair src/b.rs independently [second]"}}
                            ]});
                            if scoped {
                                assert_scope_schema(&body["response_format"]["json_schema"]["schema"]);
                                action["tasks"][0]["writable_paths"]=json!(["src/a.rs"]);
                                action["tasks"][1]["writable_paths"]=json!([if conflict {"src/a.rs"} else {"src/b.rs"}]);
                            }
                            action
                        },
                        1=>json!({"kind":"inspect_child_changes","worker":"coder","offset":0}),
                        2=>json!({"kind":"import_child_changes","worker":"coder"}),
                        3=>json!({"kind":"inspect_child_changes","worker":"coder2","offset":0}),
                        4=>json!({"kind":"import_child_changes","worker":"coder2"}),
                        5=>{
                            if conflict {assert!(prompt.to_string().to_lowercase().contains("conflict"));}
                            json!({"kind":"finish","summary":"Premature completion must require fresh combined tests"})
                        },
                        6=>{
                            assert!(prompt.to_string().contains("incomplete"));
                            tool("run_tests",json!({}))
                        },
                        7=>json!({"kind":"finish","summary":"Available patches imported and combined code verified"}),
                        _=>panic!("unexpected planner turn {turn}: {prompt}"),
                    }
                };
                axum::Json(json!({"id":"parallel-coding-fixture","choices":[{"message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))
            }
        }))).await.unwrap();
    });
    (endpoint, calls, task)
}

async fn until(client: &Client, goal: &str, partial: bool, panel: &str, decline: bool) -> Value {
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let inbox = get(client, &format!("{goal}/inbox")).await;
            for item in inbox["items"].as_array().unwrap() {
                if item["kind"] == "question" {
                    continue;
                }
                assert!(
                    item["kind"] == "tool_approval" || item["kind"] == "child_import_approval",
                    "{item}"
                );
                if item["kind"] == "child_import_approval" {
                    let page = get(client, panel).await;
                    let child = page["groups"][0]["children"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|c| {
                            c["task_id"] == item["task_id"]
                                || c["task_id"] == item["wait"]["payload"]["inspection"]["task_id"]
                        })
                        .unwrap();
                    assert_eq!(child["review"]["status"], "awaiting_approval");
                    assert_eq!(
                        child["review"]["artifact_id"],
                        child["changes"]["artifact_id"]
                    );
                    assert_eq!(
                        child["review"]["inspection_id"],
                        item["wait"]["payload"]["inspection"]["preview"]["preparation_id"]
                    );
                }
                let approved = !(decline
                    && item["kind"] == "child_import_approval"
                    && item["wait"]["payload"]["inspection"]["request"]["worker"] == "coder2");
                answer(
                    client,
                    goal,
                    item,
                    json!({"kind":"approval","approved":approved}),
                    StatusCode::OK,
                )
                .await;
            }
            let view = get(client, goal).await;
            if partial {
                let nodes = view["task"]["nodes"].as_array().unwrap();
                if nodes.iter().any(|n| n["status"] == "succeeded")
                    && inbox["items"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|i| i["kind"] == "question")
                {
                    return inbox;
                }
            } else if view["task"]["summary"]["settled"] == true {
                return view;
            }
            assert!(
                !view["task"]["nodes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|n| n["status"] == "blocked" || n["status"] == "failed"),
                "{view}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}

async fn exercise(conflict: bool, decline: bool, scoped: bool) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"parallel_coding_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"parallel_coding_fixture\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    fs::write(source.join("src/lib.rs"), "mod a;\nmod b;\n").unwrap();
    for file in ["a", "b"] {
        fs::write(source.join(format!("src/{file}.rs")), ORIGINAL).unwrap();
    }
    let database = root.path().join("parallel.db");
    let (endpoint, calls, model) = model(conflict, scoped).await;
    let initial = json!({"message_id":MessageId::new(),"prompt":"Repair independent files","workspace":source,
        "target":{"provider":"openai","model":"parallel-fixture","endpoint":endpoint,"protocol":"chat_completions"},
        "coding":{"coder_count":2,"writable_paths":["src/"],"test_command":{"argv":["cargo","test","--locked","--offline","--quiet"],"timeout_ms":30000}}});
    let (engine, agents) = local_app(&database).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::host(&engine, agents.clone()).await;
    let client = client();
    let receipt = post(
        &client,
        &format!("{}/v1/harness/sessions", server.url),
        &initial,
        StatusCode::ACCEPTED,
    )
    .await;
    let team_id: TeamId = serde_json::from_value(receipt["team_id"].clone()).unwrap();
    let task_id: MessageId = serde_json::from_value(receipt["task_id"].clone()).unwrap();
    let goal = format!("{}/v1/teams/{team_id}/tasks/{task_id}", server.url);
    let session = receipt["session_id"].as_str().unwrap();
    let panel = format!(
        "{}/v1/harness/sessions/{session}/turns/{task_id}/delegated-work",
        server.url
    );
    let before = until(&client, &goal, true, &panel, decline).await;
    let partial = get(&client, &panel).await;
    assert_eq!(partial["groups"][0]["finished"], 1);
    assert_eq!(partial["groups"][0]["total"], 2);
    assert_eq!(partial["groups"][0]["joined"], false);
    assert_eq!(partial["truncated"], false);
    let children = partial["groups"][0]["children"].as_array().unwrap();
    assert_eq!(children[0]["goal"], "Repair src/a.rs independently [first]");
    assert_eq!(children[0]["review"]["status"], "not_reviewed");
    assert_eq!(children[0]["changes"]["tests"]["ok"], true);
    assert!(children[1]["changes"].is_null());
    for (index, child) in children.iter().enumerate() {
        assert_eq!(
            child["write_scope"]["source"],
            if scoped { "task" } else { "worker" }
        );
        assert_eq!(
            child["write_scope"]["paths"],
            if scoped {
                json!([if index == 0 || conflict {
                    "src/a.rs"
                } else {
                    "src/b.rs"
                }])
            } else {
                json!(["src/"])
            }
        );
        assert_eq!(child["write_scope"]["paths_truncated"], false);
    }
    let wrong = format!(
        "{}/v1/harness/sessions/{}/turns/{task_id}/delegated-work",
        server.url,
        act0r::domain::SessionId::new()
    );
    assert_eq!(
        client.get(wrong).send().await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    let wrong = format!(
        "{}/v1/harness/sessions/{session}/turns/{}/delegated-work",
        server.url,
        children[0]["task_id"].as_str().unwrap()
    );
    assert_eq!(
        client.get(wrong).send().await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    let question = wait(&before, "question");
    let parent_id = agents
        .actors()
        .unwrap()
        .task(task_id)
        .await
        .unwrap()
        .run_id
        .unwrap();
    // The child commits its terminal state before its mailbox receipt reaches the parent.
    // Poll the receipt we assert, rather than treating the child status as that acknowledgement.
    let parent = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let saved = engine.status(parent_id).await.unwrap();
            if saved.record().delegations[0].outcome.is_some() {
                return saved;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(parent.record().task_groups.len(), 1);
    assert_eq!(parent.record().delegations.len(), 2);
    assert!(parent.record().task_groups[0].joined_at.is_none());
    assert!(parent.record().delegations[0].outcome.is_some());
    assert!(parent.record().delegations[1].outcome.is_none());
    let identities = json!(parent.record().task_groups[0]);
    let mut copies = vec![];
    let mut snapshots = vec![];
    for child in &parent.record().delegations {
        let id = agents
            .actors()
            .unwrap()
            .task(child.id)
            .await
            .unwrap()
            .run_id
            .unwrap();
        let run = engine.status(id).await.unwrap();
        if scoped {
            let prepared = child.workspace_input.as_ref().unwrap();
            assert_eq!(
                run.record().input["input"]["writable_paths"],
                json!(prepared.request.writable_paths.as_ref().unwrap())
            );
            let pinned = agents
                .actors()
                .unwrap()
                .status(child.recipient)
                .await
                .unwrap();
            assert_eq!(
                pinned.record().config["input"]["writable_paths"],
                json!(["src/"])
            );
        }
        copies.push(run.record().input["input"]["workspace"].clone());
        snapshots.push(run.record().input["input"]["workspace_input"].clone());
    }
    assert_ne!(copies[0], copies[1]);
    assert_eq!(snapshots[0]["snapshot_id"], snapshots[1]["snapshot_id"]);
    assert_eq!(
        snapshots[0]["snapshot_digest"],
        snapshots[1]["snapshot_digest"]
    );
    let calls_before = calls.load(Ordering::SeqCst);
    post(
        &client,
        &format!("{goal}/pause"),
        &json!({"expected_revision":0}),
        StatusCode::OK,
    )
    .await;
    answer(
        &client,
        &goal,
        &question,
        json!({"kind":"answer","answer":"Continue"}),
        StatusCode::OK,
    )
    .await;
    server.close().await;
    drop(agents);
    drop(engine);

    let (engine, agents) = local_app(&database).await.unwrap();
    let agents = Arc::new(agents);
    let server = Server::host(&engine, agents.clone()).await;
    let goal = format!("{}/v1/teams/{team_id}/tasks/{task_id}", server.url);
    let panel = format!(
        "{}/v1/harness/sessions/{session}/turns/{task_id}/delegated-work",
        server.url
    );
    let reopened = get(&client, &panel).await;
    assert_eq!(
        reopened["groups"][0]["group_id"],
        partial["groups"][0]["group_id"]
    );
    assert_eq!(
        reopened["groups"][0]["children"][0],
        partial["groups"][0]["children"][0]
    );
    assert_eq!(reopened["groups"][0]["children"][1]["execution_held"], true);
    assert_eq!(
        post(
            &client,
            &format!("{}/v1/harness/sessions", server.url),
            &initial,
            StatusCode::ACCEPTED
        )
        .await,
        receipt
    );
    assert_eq!(
        json!(engine.status(parent_id).await.unwrap().record().task_groups[0]),
        identities
    );
    assert_eq!(calls.load(Ordering::SeqCst), calls_before);
    assert_eq!(
        answer(
            &client,
            &goal,
            &question,
            json!({"kind":"answer","answer":"Continue"}),
            StatusCode::OK
        )
        .await["replayed"],
        true
    );
    assert_eq!(
        fs::read_to_string(Path::new(copies[1].as_str().unwrap()).join("src/a.rs")).unwrap(),
        ORIGINAL
    );
    post(
        &client,
        &format!("{goal}/resume"),
        &json!({"expected_revision":1}),
        StatusCode::OK,
    )
    .await;
    let done = until(&client, &goal, false, &panel, decline).await;
    let completed = get(&client, &panel).await;
    assert_eq!(completed["groups"][0]["finished"], 2);
    assert_eq!(completed["groups"][0]["joined"], true);
    let children = completed["groups"][0]["children"].as_array().unwrap();
    assert_eq!(children[0]["review"]["status"], "imported");
    assert_eq!(
        children[1]["review"]["status"],
        if conflict {
            "conflicted"
        } else if decline {
            "declined"
        } else {
            "imported"
        }
    );
    for child in children {
        assert_eq!(child["status"], "succeeded");
        assert_eq!(child["result"], "Verified isolated child patch");
        assert_eq!(child["changes"]["completion"], "verified");
        assert_eq!(child["changes"]["file_count"], 1);
        assert_eq!(child["changes"]["test_status"], "matching");
        assert_eq!(child["changes"]["files_truncated"], false);
    }
    assert_eq!(done["task"]["summary"]["succeeded"], 3);
    assert_eq!(calls.load(Ordering::SeqCst), if scoped { 21 } else { 15 });
    if scoped {
        for copy in &copies {
            let copy = Path::new(copy.as_str().unwrap());
            assert!(!copy.join("src/unassigned.rs").exists());
            let contents = fs::read_to_string(copy.join("src/a.rs")).unwrap()
                + &fs::read_to_string(copy.join("src/b.rs")).unwrap();
            assert!(!contents.contains("should be denied"));
        }
    }
    let parent = engine.status(parent_id).await.unwrap();
    assert_eq!(parent.record().task_groups.len(), 1);
    assert_eq!(parent.record().delegations.len(), 2);
    assert_eq!(
        parent.record().task_groups[0].children,
        serde_json::from_value::<Vec<MessageId>>(identities["children"].clone()).unwrap()
    );
    assert!(parent.record().task_groups[0].joined_at.is_some());
    let history = engine.history(parent_id).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    let changes = agents
        .actors()
        .unwrap()
        .task_changes(task_id)
        .await
        .unwrap();
    assert_eq!(
        changes.completion.unwrap().status,
        CompletionStatus::Verified
    );
    let changes = changes.changes.unwrap();
    assert_eq!(changes.test_status, TestEvidenceStatus::Matching);
    assert_eq!(changes.files.len(), if conflict || decline { 1 } else { 2 });
    let tests = changes.tests.unwrap();
    assert_eq!(tests.result["ok"], true);
    assert!(tests.result.to_string().contains(if conflict || decline {
        "running 1 test"
    } else {
        "running 2 tests"
    }));
    for child in &parent.record().delegations {
        let artifact = agents
            .actors()
            .unwrap()
            .task_changes(child.id)
            .await
            .unwrap();
        assert_eq!(
            artifact.completion.unwrap().status,
            CompletionStatus::Verified
        );
        assert_ne!(
            artifact.changes.unwrap().tests.unwrap().activity_id,
            tests.activity_id
        );
    }
    let copy = parent.record().input["input"]["workspace"]
        .as_str()
        .unwrap();
    assert_eq!(
        fs::read_to_string(Path::new(copy).join("src/a.rs")).unwrap(),
        repaired(1)
    );
    assert_eq!(
        fs::read_to_string(Path::new(copy).join("src/b.rs")).unwrap(),
        if conflict || decline {
            ORIGINAL.into()
        } else {
            repaired(2)
        }
    );
    for file in ["a", "b"] {
        assert_eq!(
            fs::read_to_string(source.join(format!("src/{file}.rs"))).unwrap(),
            ORIGINAL
        );
    }
    let session = receipt["session_id"].as_str().unwrap();
    assert_eq!(
        get(
            &client,
            &format!("{}/v1/harness/sessions/{session}", server.url)
        )
        .await["coding"]["coder_count"],
        2
    );
    let members = agents.teams().unwrap().get(team_id).await.unwrap().members;
    assert_eq!(members.len(), 4);
    for (index, child) in parent.record().delegations.iter().enumerate() {
        assert_eq!(members[index + 2].agent_id, child.recipient);
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .tasks(child.recipient)
                .await
                .unwrap()
                .len(),
            1
        );
    }
    server.close().await;
    drop(agents);
    drop(engine);
    let (_, agents) = local_app(&database).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let panel = format!(
        "{}/v1/harness/sessions/{session}/turns/{task_id}/delegated-work",
        server.url
    );
    assert_eq!(get(&client, &panel).await, completed);
    let next = post(
        &client,
        &format!("{}/v1/harness/sessions/{session}/turns", server.url),
        &json!({"message_id":MessageId::new(),"prompt":"Separate later request"}),
        StatusCode::ACCEPTED,
    )
    .await;
    let next_panel = format!(
        "{}/v1/harness/sessions/{session}/turns/{}/delegated-work",
        server.url,
        next["task_id"].as_str().unwrap()
    );
    assert_eq!(get(&client, &next_panel).await["groups"], json!([]));
    assert_eq!(get(&client, &panel).await, completed);
    server.close().await;
    model.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_coders_overlap_recover_partial_join_and_verify_combined_imports() {
    exercise(false, false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_coder_patches_refuse_second_import_and_preserve_first() {
    exercise(true, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declined_child_import_survives_completion_as_a_saved_review() {
    exercise(false, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_coders_read_siblings_refuse_outside_edits_and_recover_their_saved_paths() {
    exercise(false, false, true).await;
}
