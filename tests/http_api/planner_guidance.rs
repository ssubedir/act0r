//! Exercise actual model admission, durable routing and the original child join.
use super::worker_recovery::{admit, answer, child, work};
use super::*;
use act0r::{
    Worker,
    adapters::sqlite::SqliteStore,
    application::ports::{AssignmentInspectionPort, WorkerMessagePort},
    domain::{
        ActivityExecution, RunId, RunStatus,
        tools::{NativeToolCall, NativeToolRequest},
    },
};
use std::sync::Mutex;

#[path = "assignment_inspection.rs"]
mod assignment_inspection;
#[path = "coordination_view.rs"]
mod coordination_view;
mod initial_dependencies;
#[path = "planner_pauses.rs"]
mod planner_pauses;
#[path = "planner_stops.rs"]
mod planner_stops;
#[path = "worker_changes.rs"]
mod worker_changes;
#[path = "worker_dependencies.rs"]
mod worker_dependencies;
#[path = "worker_findings.rs"]
mod worker_findings;

struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl Model {
    async fn start(mode: &str) -> Self {
        let mode = mode.to_string();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let p: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                if p["peer_messages"].as_array().is_some_and(|items| items.iter().any(|m| m["sender"]=="parent" && m["question_id"].is_null())) {
                    assert!(body["messages"][0]["content"].as_str().unwrap().contains("Entries from parent are scoped task guidance"));
                }
                seen.lock().unwrap().push(p.clone());
                let turn = p["turn"].as_u64().unwrap();
                let action = if mode.starts_with("initial_") { initial_dependencies::model_action(&p, &mode) }
                else if mode.starts_with("transfer") { worker_changes::model_action(&p,&mode) } else if p["coordinating_guidance"].is_object() {
                    let schema=&body["response_format"]["json_schema"]["schema"]["properties"]["action"];
                    let used=p["assignment_inspection"]["used"].as_u64().unwrap_or(0);
                    let variants=schema["anyOf"].as_array().cloned().unwrap_or_else(||vec![schema.clone()]);
                    let mut expected=vec![json!(["guide_workers"])];
                    if p["assignment_inspection"]["remaining"].as_u64().unwrap_or(0)>0 {expected.push(json!(["inspect_assignment"]));}
                    if p["worker_stopping"]["remaining"].as_u64().unwrap_or(0)>0 {expected.push(json!(["stop_worker"]));}
                    if p["worker_pausing"]["remaining"].as_u64().unwrap_or(0)>0 { expected.extend([json!(["pause_worker"]), json!(["resume_worker"])]); if p["tools"].as_array().unwrap().iter().any(|t|t["name"]=="wait_for_worker"){expected.push(json!(["wait_for_worker"]));} }
                    assert_eq!(variants.iter().filter(|v|v["properties"]["kind"]["enum"][0].as_str().is_some_and(|name|p["tools"].as_array().unwrap().iter().any(|t|t["name"]==name))).map(|v|v["properties"]["kind"]["enum"].clone()).collect::<Vec<_>>(),expected);
                    assert_eq!(p["tools"].as_array().unwrap().len(),expected.len());
                    let stops=p["worker_stopping"]["used"].as_u64().unwrap_or(0);
                    let pauses=p["worker_pausing"]["used"].as_u64().unwrap_or(0);
                    if mode.starts_with("dependency") {
                        let ready=p["coordinating_guidance"]["dependencies"].as_array().is_some_and(|d|!d.is_empty());
                        if ready && used==0 {
                            json!({"kind":"call_tool","tool":"inspect_assignment","arguments":{"worker":"coder2"}})
                        } else if pauses==0 && (mode!="dependency_stop" || stops==0) && (ready || p["coordinating_guidance"]["instructions"].as_array().unwrap().iter().any(|i|i["text"].as_str().unwrap().starts_with("Resume"))) {
                            json!({"kind":"call_tool","tool":if mode=="dependency_stop" {"stop_worker"} else {"resume_worker"},"arguments":{"worker":"coder","reason":"Review the saved prerequisite outcome before continuing."}})
                        } else if !ready && (pauses==0 || mode=="dependency_cycle" && pauses<2 || mode=="dependency_limit" && pauses<4) {
                            json!({"kind":"call_tool","tool":"wait_for_worker","arguments":{"worker":if mode=="dependency_cycle" && pauses==1 {"coder2"} else {"coder"},"dependency":if mode=="dependency_cycle" && pauses==1 || mode=="dependency_self" {"coder"} else if mode=="dependency_scope" || mode=="dependency_limit" {"helper"} else {"coder2"},"reason":"The UI assignment depends on the API assignment."}})
                        } else {
                            json!({"kind":"call_tool","tool":"guide_workers","arguments":{"coder":null,"coder2":null}})
                        }
                    } else if (mode.starts_with("pause") || mode=="finding_pause") && (pauses==0 || mode=="pause_limit" || mode=="pause_cycle" && pauses<4 || mode=="pause_refuse" && pauses<3 || mode=="pause_twice" && pauses<2) {
                        let resume = mode=="pause_cycle" && pauses%2==1 || mode=="pause_refuse" && pauses==2 || p["coordinating_guidance"]["instructions"].as_array().unwrap().iter().any(|i| i["text"].as_str().unwrap().starts_with("Resume")) || mode=="finding_pause" && p["coordinating_guidance"]["findings"].as_array().is_some_and(|f| !f.is_empty());
                        json!({"kind":"call_tool","tool":if resume {"resume_worker"} else {"pause_worker"},"arguments":{"worker":if mode=="pause_limit" || mode=="pause_refuse" && pauses==0 {"helper"} else if mode=="pause_cycle" && pauses>=2 {"coder2"} else {"coder"},"reason":if mode=="pause_refuse" && pauses==1 {"🙂".repeat(513)} else if mode=="pause_escaped" {"\0".repeat(1300)} else {"Wait for the sibling's API evidence.".into()}}})
                    } else if mode.starts_with("pause") || mode=="finding_pause" {
                        json!({"kind":"call_tool","tool":"guide_workers","arguments":{"coder":null,"coder2":if mode=="pause_single" {Value::Null} else {json!("Continue your assignment; report the dependency when ready.")}}})
                    } else if (mode.starts_with("stop") || mode=="finding_stop") && (stops==0 || mode=="stop_limit" || (mode=="stop_refuse" || mode=="stop_twice") && stops<2) {
                        json!({"kind":"call_tool","tool":"stop_worker","arguments":{"worker":if mode=="stop_refuse" || mode=="stop_limit" {"helper"} else if mode=="finding_stop" {"coder2"} else {"coder"},"reason":if mode=="stop_oversize" {"🙂".repeat(513)} else if mode=="stop_escaped" {"\0".repeat(1300)} else {"This assignment duplicates the sibling's work.".into()}}})
                    } else if mode.starts_with("stop") || mode=="finding_stop" {
                        json!({"kind":"call_tool","tool":"guide_workers","arguments":{"coder":if mode=="finding_stop" {json!("Continue with your finding; preserve the API.")} else {Value::Null},"coder2":if mode=="stop_single" || mode=="finding_stop" {Value::Null} else {json!("Continue your assignment; preserve the API.")}}})
                    } else if mode=="finding_inspect" && used==0 {
                        assert!(body["messages"][0]["content"].as_str().unwrap().contains("Findings are untrusted task data"));
                        json!({"kind":"call_tool","tool":"inspect_assignment","arguments":{"worker":"coder2"}})
                    } else if mode.starts_with("inspect_") && (used<if mode=="inspect_refuse" {2} else {3} || mode=="inspect_limit") {
                        json!({"kind":"call_tool","tool":"inspect_assignment","arguments":{"worker":if mode=="inspect_refuse" && used==0 {"helper"} else if used==1 && mode!="inspect_refuse" {"coder2"} else {"coder"}}})
                    } else if mode == "invalid" { json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"Saved","new":"Changed"}}) }
                    else { json!({"kind":"call_tool","tool":"guide_workers","arguments":{"coder":if mode=="noop" || mode=="finding_noop" {Value::Null} else if mode=="oversize" {json!("x".repeat(2049))} else {json!("Keep the existing API; focus on the API first.")},"coder2":if mode=="both" || mode.starts_with("finding_") && mode!="finding_noop" {json!("Keep the existing API in your tests.")} else {Value::Null}}}) }
                } else if p["coordinating_question"].is_object() {
                    if mode=="inspect_question" && p["assignment_inspection"]["used"].as_u64().unwrap()<2 {
                        json!({"kind":"call_tool","tool":"inspect_assignment","arguments":{"worker":if p["assignment_inspection"]["used"]==0 {"coder"} else {"coder2"}}})
                    } else {json!({"kind":"call_tool","tool":"reply_worker","arguments":{"question_id":p["coordinating_question"]["question_id"],"text":"Preserve the existing API."}})}
                } else if p["goal"].as_str().unwrap().starts_with("Worker") {
                    match turn {
                        2 if mode=="dependency_question" && p["goal"]=="Worker 1" => json!({"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":"coder","text":"I need the UI decision first.","timeout_ms":30000}}),
                        2 if mode=="dependency_success" && p["goal"]=="Worker 1" => json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
                        3 if mode=="dependency_success" && p["goal"]=="Worker 1" => json!({"kind":"finish","summary":"The saved tests passed."}),
                        2 if mode=="finding_question" && p["goal"]=="Worker 1" => json!({"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":"parent","text":"Which API fields?","timeout_ms":30000}}),
                        2 if mode=="finding_pause" && p["goal"]=="Worker 0" => json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}}),
                        2 if mode.starts_with("finding_") => json!({"kind":"call_tool","tool":"send_worker_message","arguments":{"recipient":if mode=="finding_sibling" {"coder2"} else {"parent"},"text":format!("{} found display_name compatibility issue",p["goal"].as_str().unwrap()),"needs_coordination":if mode=="finding_null" {Value::Null} else {json!(mode!="finding_ordinary")}}}),
                        4 if mode=="finding_repeat" => json!({"kind":"call_tool","tool":"send_worker_message","arguments":{"recipient":"parent","text":"A later finding needs coordination","needs_coordination":true}}),
                        5 if mode=="finding_repeat" => json!({"kind":"ask_human","question":"Finish this worker?"}),
                        0 if mode=="inspect_tests" => json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
                        0 if mode=="inspect_messages" => json!({"kind":"call_tool","tool":"send_worker_message","arguments":{"recipient":"parent","text":"Preserve display_name; saved worker finding."}}),
                        2 if (mode=="stop_unsafe" || mode=="pause_unsafe") && p["goal"]=="Worker 0" => json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"Saved source","new":"Edited source"}}),
                        0|2 if mode!="question" && mode!="inspect_question" => json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}}),
                        1 if (mode=="question" || mode=="inspect_question") && p["goal"]=="Worker 0" => json!({"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":"parent","text":"Which API should I preserve?","timeout_ms":30000}}),
                        0..=3 => json!({"kind":"ask_human","question":"Continue this worker?"}),
                        _ => json!({"kind":"finish_incomplete","summary":"Worker finished"}),
                    }
                } else if turn==0 {
                    if mode=="dependency_outside" {json!({"kind":"call_tool","tool":"wait_for_worker","arguments":{"worker":"coder","dependency":"coder2","reason":"Outside the saved join"}})}
                    else if mode=="pause_outside" || mode=="pause_resume_outside" { json!({"kind":"call_tool","tool":if mode=="pause_outside" {"pause_worker"} else {"resume_worker"},"arguments":{"worker":"coder","reason":"Cannot control outside coordination"}}) }
                    else if mode=="inspect_outside" || mode=="stop_outside" { json!({"kind":"call_tool","tool":if mode=="stop_outside" {"stop_worker"} else {"inspect_assignment"},"arguments":if mode=="stop_outside" {json!({"worker":"coder","reason":"Cannot stop outside coordination"})} else {json!({"worker":"coder"})}}) }
                    else if mode=="single" || mode=="inspect_single" || mode=="finding_single" || mode=="stop_single" || mode=="pause_single" { json!({"kind":"delegate_task","worker":"coder","goal":"Worker 0","writable_paths":["src/"]}) }
                    else {json!({"kind":"delegate_tasks","tasks":[{"worker":"coder","goal":"Worker 0","writable_paths":["src/"]},{"worker":"coder2","goal":"Worker 1","writable_paths":["src/"]}]})}
                } else if mode=="finding_reassign" && turn==1 || mode=="stop_reassign" && turn==3 || mode=="pause_reassign" && turn==5 || mode=="dependency_reassign" && turn==6 { json!({"kind":"delegate_task","worker":"coder","goal":"Worker replacement","writable_paths":["src/"]}) }
                else { json!({"kind":"finish_incomplete","summary":"Parent received original worker results"}) };
                async move { axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}})) }
            }));
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }
    fn coordination_calls(&self) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p["coordinating_guidance"].is_object())
            .count()
    }
}
impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn input(model: &Model, root: &Path, budget: Option<Value>) -> Value {
    let project = root.join("project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(
        project.join("src/lib.rs"),
        "Saved source: id and display_name\n",
    )
    .unwrap();
    let mut input = json!({"message_id":MessageId::new(),"prompt":"Coordinate coding workers","workspace":fs::canonicalize(project).unwrap(),"target":{"provider":"openai","model":"guidance-fixture","endpoint":model.endpoint,"protocol":"chat_completions","policy":{"max_attempts":1}},"coding":{"writable_paths":["src/"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000},"coder_count":2}});
    if let Some(budget) = budget {
        input["budget"] = budget;
    }
    input
}
async fn start(
    s: &Server,
    engine: &act0r::Engine,
    model: &Model,
    root: &Path,
    budget: Option<Value>,
) -> Value {
    let mut receipt = post(
        &client(),
        &format!("{}/v1/harness/sessions", s.url),
        &input(model, root, budget),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(engine).await;
    receipt["run_id"] =
        get(&client(), &format!("{}/steering", base(s, &receipt))).await["run_id"].clone();
    receipt
}
fn base(s: &Server, r: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        s.url,
        r["session_id"].as_str().unwrap(),
        r["task_id"].as_str().unwrap()
    )
}
fn run(c: &Value) -> RunId {
    c["run_id"].as_str().unwrap().parse().unwrap()
}
async fn steer(s: &Server, r: &Value, text: &str) -> Value {
    post(
        &client(),
        &format!("{}/steering", base(s, r)),
        &json!({"message_id":MessageId::new(),"run_id":r["run_id"],"text":text}),
        StatusCode::ACCEPTED,
    )
    .await
}
fn call(e: &ActivityExecution) -> NativeToolCall {
    let r: NativeToolRequest = serde_json::from_value(e.input.clone()).unwrap();
    NativeToolCall {
        run_id: e.run_id,
        activity_id: e.activity_id,
        attempt_id: e.attempt_id,
        arguments: r.arguments,
        workspace: r.workspace,
        writable_paths: r.writable_paths,
        test_command: r.test_command,
        project_command: r.project_command,
    }
}
async fn step(w: &mut Worker) -> ActivityExecution {
    let e = admit(w).await;
    let out = w.execute(e.clone()).await.unwrap();
    w.complete(e.clone(), out).await.unwrap();
    e
}
async fn drain(w: &mut Worker) {
    for _ in 0..100 {
        if w.advance_work().await.unwrap() || w.fire_due_timer().await.unwrap() {
            continue;
        }
        if let Some(e) = w.admit().await.unwrap() {
            let out = w.execute(e.clone()).await.unwrap();
            w.complete(e, out).await.unwrap();
            continue;
        }
        return;
    }
    panic!("fixture did not become idle");
}

#[tokio::test]
async fn paused_restart_coalesces_guidance_routes_exact_assignment_and_joins_once() {
    let model = Model::start("one").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("guidance.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    settle(&engine).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    let original = engine.status(run(&r)).await.unwrap();
    engine.pause(run(&r)).await.unwrap();
    let receipt = steer(&s, &r, "Keep the existing API").await;
    let exact = post(
        &client(),
        &format!("{}/steering", base(&s, &r)),
        &receipt["entry"]["request"],
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(exact["replayed"], true);
    steer(&s, &r, "Focus on the API first").await;
    settle(&engine).await;
    assert_eq!(model.coordination_calls(), 0);
    assert!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .steering
            .iter()
            .all(|i| i.application.is_none())
    );
    s.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    engine.resume(run(&r)).await.unwrap();
    settle(&engine).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(parent.record().status, RunStatus::Waiting);
    assert!(parent.record().planner_guidance.is_none());
    assert_eq!(
        parent.record().delegations.len(),
        original.record().delegations.len()
    );
    assert_eq!(
        parent
            .record()
            .delegations
            .iter()
            .map(|d| d.id)
            .collect::<Vec<_>>(),
        original
            .record()
            .delegations
            .iter()
            .map(|d| d.id)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        parent.record().task_groups[0].id,
        original.record().task_groups[0].id
    );
    assert!(parent.record().task_groups[0].joined_at.is_none());
    assert!(
        parent
            .record()
            .steering
            .iter()
            .all(|i| i.application.is_some())
    );
    assert_eq!(model.coordination_calls(), 1);
    assert_eq!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .peer_messages
            .len(),
        1
    );
    assert!(
        engine
            .status(run(&b))
            .await
            .unwrap()
            .record()
            .peer_messages
            .is_empty()
    );
    let item = &child(&s, &r, 0).await["messages"]["items"][0];
    assert_eq!(item["receipt"]["sender_task_id"], r["task_id"]);
    assert_eq!(item["receipt"]["recipient_task_id"], a["task_id"]);
    assert_eq!(item["application"], Value::Null);
    answer(&s, &r, &a).await;
    settle(&engine).await;
    assert!(child(&s, &r, 0).await["messages"]["items"][0]["application"].is_object());
    assert!(model.requests.lock().unwrap().iter().any(|p| {
        p["goal"] == "Worker 0"
            && p["turn"] == 2
            && p["peer_messages"][0]["text"]
                .as_str()
                .unwrap()
                .contains("existing API")
    }));
    answer(&s, &r, &a).await;
    answer(&s, &r, &b).await;
    settle(&engine).await;
    answer(&s, &r, &b).await;
    settle(&engine).await;
    assert_eq!(work(&s, &r).await["groups"][0]["joined"], true);
    let history = engine.history(run(&r)).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    assert_eq!(model.coordination_calls(), 1);
    s.close().await;
}

#[tokio::test]
async fn receipt_and_two_inboxes_commit_atomically_and_replay_after_interrupted_completion() {
    let model = Model::start("both").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("guidance.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    settle(&engine).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    steer(&s, &r, "Preserve the API and its tests").await;
    let mut w = engine.worker().await.unwrap();
    let decision = step(&mut w).await;
    assert_eq!(decision.handler.name, "agent.model");
    let e = admit(&mut w).await;
    assert_eq!(e.input["definition"]["id"]["name"], "guide_workers");
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER fail_guidance BEFORE INSERT ON harness_planner_guidance BEGIN SELECT RAISE(ABORT,'guidance rollback'); END;").execute(&pool).await.unwrap();
    let store = SqliteStore::open(&db).await.unwrap();
    assert!(store.guide_workers(call(&e)).await.is_err());
    assert!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .peer_messages
            .is_empty()
    );
    assert!(
        engine
            .status(run(&b))
            .await
            .unwrap()
            .record()
            .peer_messages
            .is_empty()
    );
    sqlx::raw_sql("DROP TRIGGER fail_guidance")
        .execute(&pool)
        .await
        .unwrap();
    let act0r::domain::tools::NativeToolResult::Success(accepted) =
        store.guide_workers(call(&e)).await.unwrap()
    else {
        panic!("guidance refused");
    };
    let act0r::domain::tools::NativeToolResult::Success(repeated) =
        store.guide_workers(call(&e)).await.unwrap()
    else {
        panic!("guidance replay refused");
    };
    assert_eq!(accepted, repeated);
    let mut changed = call(&e);
    changed.arguments["coder"] = json!("Changed scope");
    assert!(store.guide_workers(changed).await.is_err());
    store.close().await;
    pool.close().await;
    drop(w);
    s.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut w = engine.worker().await.unwrap();
    let replay = admit(&mut w).await;
    assert_eq!(replay.activity_id, e.activity_id);
    assert_eq!(replay.input, e.input);
    assert_ne!(replay.attempt_id, e.attempt_id);
    let out = w.execute(replay.clone()).await.unwrap();
    w.complete(replay, out).await.unwrap();
    drop(w);
    settle(&engine).await;
    for c in [&a, &b] {
        assert_eq!(
            engine
                .status(run(c))
                .await
                .unwrap()
                .record()
                .peer_messages
                .len(),
            1
        );
        assert_eq!(
            child(&s, &r, if c == &a { 0 } else { 1 }).await["messages"]["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    assert_eq!(model.coordination_calls(), 1);
    assert!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .planner_guidance
            .is_none()
    );
    s.close().await;
}

#[tokio::test]
async fn admitted_worker_input_stays_frozen_and_guidance_enters_its_next_fresh_call() {
    let model = Model::start("one").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("guidance.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    settle(&engine).await;
    let a = child(&s, &r, 0).await;
    answer(&s, &r, &a).await;
    let mut w = engine.worker().await.unwrap();
    let frozen = admit(&mut w).await;
    assert_eq!(frozen.run_id, run(&a));
    let old = frozen.input.clone();
    steer(&s, &r, "Keep the existing API").await;
    assert_eq!(step(&mut w).await.run_id, run(&r));
    assert_eq!(
        step(&mut w).await.input["definition"]["id"]["name"],
        "guide_workers"
    );
    assert_eq!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .activities()
            .iter()
            .find(|e| e.id == frozen.activity_id)
            .unwrap()
            .spec
            .input,
        old
    );
    assert!(
        engine.status(run(&a)).await.unwrap().record().peer_messages[0]
            .application
            .is_none()
    );
    let out = w.execute(frozen.clone()).await.unwrap();
    w.complete(frozen, out).await.unwrap();
    drain(&mut w).await;
    drop(w);
    assert!(
        engine.status(run(&a)).await.unwrap().record().peer_messages[0]
            .application
            .is_some()
    );
    s.close().await;
}

#[tokio::test]
async fn terminal_children_during_guidance_keep_outcomes_and_join_once_after_routing() {
    let model = Model::start("both").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("guidance.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    settle(&engine).await;
    let a = child(&s, &r, 0).await;
    let b = child(&s, &r, 1).await;
    steer(&s, &r, "Preserve the API").await;
    let mut w = engine.worker().await.unwrap();
    let model_call = admit(&mut w).await;
    let out = w.execute(model_call.clone()).await.unwrap();
    answer(&s, &r, &a).await;
    answer(&s, &r, &b).await;
    drain(&mut w).await;
    answer(&s, &r, &a).await;
    answer(&s, &r, &b).await;
    drain(&mut w).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert!(
        parent
            .record()
            .delegations
            .iter()
            .all(|d| d.outcome.is_some())
    );
    assert!(parent.record().task_groups[0].joined_at.is_none());
    w.complete(model_call, out).await.unwrap();
    let routed = step(&mut w).await;
    assert_eq!(routed.input["definition"]["id"]["name"], "guide_workers");
    drain(&mut w).await;
    drop(w);
    assert_eq!(work(&s, &r).await["groups"][0]["joined"], true);
    for n in 0..2 {
        assert_eq!(
            child(&s, &r, n).await["messages"]["items"][0]["receipt"]["error"]["code"],
            "guidance_recipient_closed"
        );
    }
    let coordination = work(&s, &r).await["coordination"].clone();
    assert_eq!(coordination["batches"][0]["status"], "finished");
    assert!(
        coordination["batches"][0]["decision"]["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|d| d["receipt"]["error"]["code"] == "guidance_recipient_closed")
    );
    assert_eq!(
        engine
            .history(run(&r))
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    s.close().await;
}

#[tokio::test]
async fn guidance_applied_in_question_reply_still_routes_when_that_step_returns_to_join() {
    let model = Model::start("question").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("guidance.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    settle(&engine).await;
    let a = child(&s, &r, 0).await;
    answer(&s, &r, &a).await;
    let mut w = engine.worker().await.unwrap();
    step(&mut w).await;
    step(&mut w).await;
    while w.advance_work().await.unwrap() {}
    assert!(w.fire_due_timer().await.unwrap());
    assert!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .planner_question
            .is_some()
    );
    steer(&s, &r, "Keep the existing API").await;
    let reply_model = step(&mut w).await;
    assert_eq!(reply_model.run_id, run(&r));
    assert!(
        engine.status(run(&r)).await.unwrap().record().steering[0]
            .application
            .is_some()
    );
    drain(&mut w).await;
    drop(w);
    assert_eq!(model.coordination_calls(), 1);
    assert_eq!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .peer_messages
            .len(),
        1
    );
    let parent = engine.status(run(&r)).await.unwrap();
    assert!(parent.record().planner_guidance.is_none());
    assert!(parent.record().planner_question.is_none());
    assert!(parent.record().task_groups[0].joined_at.is_none());
    s.close().await;
}

#[tokio::test]
async fn shared_budget_refusal_keeps_guidance_pending_without_delivering_to_workers() {
    let model = Model::start("one").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("guidance.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(
        &s,
        &engine,
        &model,
        root.path(),
        Some(json!({"max_model_calls":5,"max_tokens":null})),
    )
    .await;
    settle(&engine).await;
    let a = child(&s, &r, 0).await;
    steer(&s, &r, "Preserve API").await;
    settle(&engine).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(parent.record().status, RunStatus::BudgetExhausted);
    assert!(parent.record().steering[0].application.is_none());
    assert!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .peer_messages
            .is_empty()
    );
    assert_eq!(model.coordination_calls(), 0);
    s.close().await;
}

#[tokio::test]
async fn restricted_guidance_rejects_edits_and_noop_returns_to_original_join() {
    for mode in ["invalid", "noop", "oversize"] {
        let model = Model::start(mode).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("guidance.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        settle(&engine).await;
        let a = child(&s, &r, 0).await;
        steer(&s, &r, "Preserve API").await;
        settle(&engine).await;
        let parent = engine.status(run(&r)).await.unwrap();
        assert_eq!(
            parent.record().status,
            if mode == "invalid" {
                RunStatus::Failed
            } else {
                RunStatus::Waiting
            }
        );
        let coordination = work(&s, &r).await["coordination"].clone();
        let batch = &coordination["batches"][0];
        if mode == "invalid" {
            assert_eq!(batch["status"], "failed");
            assert!(batch["decision"].is_null());
        } else if mode == "oversize" {
            assert_eq!(batch["status"], "finished");
            assert_eq!(batch["decision"]["text_truncated"], true);
            assert_eq!(
                batch["decision"]["request"]["coder"]
                    .as_str()
                    .unwrap()
                    .len(),
                2048
            );
            assert!(batch["decision"]["deliveries"][0]["receipt"]["error"].is_object());
        }
        assert!(
            engine
                .status(run(&a))
                .await
                .unwrap()
                .record()
                .peer_messages
                .is_empty()
        );
        assert!(
            parent
                .activities()
                .iter()
                .all(|a| a.spec.input["definition"]["id"]["name"] != "replace_text")
        );
        assert_eq!(
            fs::read_to_string(root.path().join("project/src/lib.rs")).unwrap(),
            "Saved source: id and display_name\n"
        );
        s.close().await;
    }
}

#[tokio::test]
async fn whole_goal_stop_prevents_pending_and_admitted_guidance_delivery() {
    for stage in 0..3 {
        let model = Model::start("both").await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("guidance.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let s = Server::start(Arc::new(agents), 2).await;
        let r = start(&s, &engine, &model, root.path(), None).await;
        let a = child(&s, &r, 0).await;
        let b = child(&s, &r, 1).await;
        steer(&s, &r, "Preserve API").await;
        let mut w = engine.worker().await.unwrap();
        let admitted = if stage == 1 {
            Some(admit(&mut w).await)
        } else if stage == 2 {
            step(&mut w).await;
            Some(admit(&mut w).await)
        } else {
            None
        };
        post(
            &client(),
            &format!(
                "{}/v1/teams/{}/tasks/{}/cancel",
                s.url,
                r["team_id"].as_str().unwrap(),
                r["task_id"].as_str().unwrap()
            ),
            &json!({}),
            StatusCode::OK,
        )
        .await;
        drain(&mut w).await;
        if let Some(e) = admitted {
            let out = w.execute(e.clone()).await.unwrap();
            w.complete(e.clone(), out).await.unwrap();
            assert!(
                engine
                    .status(run(&r))
                    .await
                    .unwrap()
                    .activities()
                    .iter()
                    .find(|a| a.id == e.activity_id)
                    .unwrap()
                    .result
                    .is_some()
            );
        }
        drain(&mut w).await;
        drop(w);
        settle(&engine).await;
        assert_eq!(
            engine.status(run(&r)).await.unwrap().record().status,
            RunStatus::Cancelled
        );
        for c in [&a, &b] {
            assert!(
                engine
                    .status(run(c))
                    .await
                    .unwrap()
                    .record()
                    .peer_messages
                    .is_empty()
            );
        }
        assert_eq!(model.coordination_calls(), if stage == 0 { 0 } else { 1 });
        s.close().await;
    }
}

#[tokio::test]
async fn worker_question_arriving_during_guidance_is_saved_and_answered_after_routing() {
    let model = Model::start("question").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("guidance.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let r = start(&s, &engine, &model, root.path(), None).await;
    let a = child(&s, &r, 0).await;
    steer(&s, &r, "Preserve API").await;
    let mut w = engine.worker().await.unwrap();
    let guidance_model = admit(&mut w).await;
    let result = w.execute(guidance_model.clone()).await.unwrap();
    answer(&s, &r, &a).await;
    drain(&mut w).await;
    let question = child(&s, &r, 0).await["questions"]["items"][0].clone();
    assert!(question.is_object());
    assert!(question["outcome"].is_null());
    assert!(
        engine
            .status(run(&r))
            .await
            .unwrap()
            .record()
            .planner_guidance
            .is_some()
    );
    w.complete(guidance_model, result).await.unwrap();
    drain(&mut w).await;
    drop(w);
    assert_eq!(
        child(&s, &r, 0).await["questions"]["items"][0]["outcome"]["status"],
        "answered"
    );
    assert_eq!(model.coordination_calls(), 1);
    assert!(
        engine.status(run(&r)).await.unwrap().record().task_groups[0]
            .joined_at
            .is_none()
    );
    s.close().await;
}

#[tokio::test]
async fn single_coder_guidance_preserves_single_task_join_and_exact_target() {
    let model = Model::start("single").await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("guidance.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let s = Server::start(Arc::new(agents), 2).await;
    let mut request = input(&model, root.path(), None);
    request["coding"]["coder_count"] = json!(1);
    let mut r = post(
        &client(),
        &format!("{}/v1/harness/sessions", s.url),
        &request,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    r["run_id"] = get(&client(), &format!("{}/steering", base(&s, &r))).await["run_id"].clone();
    let a = child(&s, &r, 0).await;
    steer(&s, &r, "Preserve API").await;
    settle(&engine).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert_eq!(parent.record().status, RunStatus::Waiting);
    assert!(parent.record().task_groups.is_empty());
    assert_eq!(
        parent.record().delegations[0].id,
        a["task_id"].as_str().unwrap().parse().unwrap()
    );
    assert_eq!(
        engine
            .status(run(&a))
            .await
            .unwrap()
            .record()
            .peer_messages
            .len(),
        1
    );
    answer(&s, &r, &a).await;
    settle(&engine).await;
    answer(&s, &r, &a).await;
    settle(&engine).await;
    let parent = engine.status(run(&r)).await.unwrap();
    assert!(parent.record().delegations[0].outcome.is_some());
    // The fixture deliberately reports incomplete work; routing must preserve that outcome.
    assert_eq!(parent.record().status, RunStatus::Failed);
    assert_eq!(
        parent.record().error.as_ref().unwrap().code,
        "coding_incomplete"
    );
    assert_eq!(
        engine
            .history(run(&r))
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "delegation_completed")
            .count(),
        1
    );
    assert_eq!(model.coordination_calls(), 1);
    s.close().await;
}
