//! Real model/tool boundaries exercise questions, durable timers and correlated replies.
use super::worker_recovery::{admit, answer, child, work};
use super::*;
use act0r::{
    adapters::sqlite::SqliteStore,
    application::ports::WorkerMessagePort,
    domain::{
        ActivityExecution, RunId, RunStatus,
        tools::{NativeToolCall, NativeToolRequest},
    },
};
use std::sync::Mutex;

struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl Model {
    async fn start(mode: &str, timeout: u32) -> Self {
        let mode = mode.to_string();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let app=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
                let p:Value=serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap(); seen.lock().unwrap().push(p.clone());
                let turn=p["turn"].as_u64().unwrap(); let goal=p["goal"].as_str().unwrap();
                let action=if p["coordinating_question"].is_object() {
                    if mode=="parent_invalid" {json!({"kind":"delegate_task","worker":"coder","goal":"Forbidden nested question"})}
                    else if mode=="parent_inspect_write" {json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"Saved source","new":"Changed source"}})}
                    else if mode.starts_with("parent_inspect") && (p["question_inspection"]["used"].as_u64().unwrap() < if mode=="parent_inspect_feedback" {2} else {3} || mode=="parent_inspect_loop") {
                        let used=p["question_inspection"]["used"].as_u64().unwrap();
                        let (tool,args)=if mode=="parent_inspect_feedback" {("read_file",json!({"path":if used==0 {"missing.rs"} else {"src/lib.rs"}}))}
                        else {match used {0=>("list_files",json!({})),1=>("search_text",json!({"query":"display_name","path":"src/","max_matches":20,"max_files":200})),_=>("read_file",json!({"path":"src/lib.rs"}))}};
                        json!({"kind":"call_tool","tool":tool,"arguments":args})
                    }
                    else if mode.starts_with("parent_inspect") {assert!(p.to_string().contains("display_name"));json!({"kind":"call_tool","tool":"reply_worker","arguments":{"question_id":p["coordinating_question"]["question_id"],"text":"src/lib.rs defines id and display_name; preserve that API contract."}})}
                    else {json!({"kind":"call_tool","tool":"reply_worker","arguments":{"question_id":p["coordinating_question"]["question_id"],"text":"Return id and display_name. Keep all existing permissions."}})}
                } else {match (goal,turn) {
                    ("Worker 0"|"Worker 1",0)=>json!({"kind":"ask_human","question":"Begin this worker?"}),
                    ("Worker 0",1..=9) if mode=="capacity"=>json!({"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":"helper","text":"Refused bounded question","timeout_ms":timeout}}),
                    ("Worker 0",1)=>json!({"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":if mode.starts_with("parent_") {"parent"} else if mode=="parent" {"helper"} else {"coder2"},"text":"Which field names should the API return?","timeout_ms":timeout}}),
                    ("Worker 1",1) if mode=="parent_both"=>json!({"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":"parent","text":"Which error status should I return?","timeout_ms":timeout}}),
                    ("Worker 1",1) if mode=="cycle"=>json!({"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":"coder","text":"Wait for my API contract first?","timeout_ms":timeout}}),
                    ("Worker 1",1|2) if mode!="hold" && mode!="cycle" && !mode.starts_with("parent_")=>json!({"kind":"call_tool","tool":"reply_worker","arguments":{"question_id":if mode=="wrong" {json!(MessageId::new())} else if mode=="malformed" {json!("invalid UUID")} else {p["peer_messages"][0]["question_id"].clone()},"text":"Return id and display_name. This note is not test evidence."}}),
                    ("Worker 0"|"Worker 1",_)=>json!({"kind":"ask_human","question":"Inspect saved coordination outcome?"}),
                    (_,0) if mode=="parent_single"=>json!({"kind":"delegate_task","worker":"coder","goal":"Worker 0","writable_paths":["src/"]}),
                    (_,0)=>json!({"kind":"delegate_tasks","tasks":[{"worker":"coder","goal":"Worker 0","writable_paths":["src/"]},{"worker":"coder2","goal":"Worker 1","writable_paths":["src/"]}]}),
                    _=>json!({"kind":"finish_incomplete","summary":"Fixture retains saved outcomes"}),
                }};
                async move {axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
            }));
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }
}
impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn start(server: &Server, model: &Model, root: &Path) -> Value {
    post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &start_input(model, root),
        StatusCode::ACCEPTED,
    )
    .await
}
fn start_input(model: &Model, root: &Path) -> Value {
    let project = root.join("project");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(
        project.join("src/lib.rs"),
        "Saved source: API fields id and display_name\n",
    )
    .unwrap();
    json!({"message_id":MessageId::new(),"prompt":"Coordinate two workers","workspace":fs::canonicalize(project).unwrap(),"target":{"provider":"openai","model":"worker-question-fixture","endpoint":model.endpoint,"protocol":"chat_completions","policy":{"max_attempts":1}},"coding":{"writable_paths":["src/"],"test_command":{"argv":["rustc","--version"],"timeout_ms":30000},"coder_count":2}})
}
fn run(c: &Value) -> RunId {
    c["run_id"].as_str().unwrap().parse().unwrap()
}
fn base(s: &Server, r: &Value) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        s.url,
        r["session_id"].as_str().unwrap(),
        r["task_id"].as_str().unwrap()
    )
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
async fn pause(s: &Server, r: &Value, c: &Value, revision: u64, paused: bool) {
    post(
        &client(),
        &format!(
            "{}/workers/{}/pause",
            base(s, r),
            c["task_id"].as_str().unwrap()
        ),
        &json!({"request_id":MessageId::new(),"expected_revision":revision,"paused":paused}),
        StatusCode::ACCEPTED,
    )
    .await;
}

#[tokio::test]
async fn reply_resumes_same_run_once_and_duplicate_reply_is_saved_feedback() {
    let model = Model::start("reply", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("questions.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let requester = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &requester).await;
    settle(&engine).await;
    let waiting = engine.status(run(&requester)).await.unwrap();
    assert_eq!(waiting.record().status, RunStatus::Waiting);
    assert!(waiting.record().wait.is_none());
    assert!(waiting.record().worker_reply_wait.is_some());
    assert_eq!(waiting.record().usage.admitted_model_calls, 2);
    let mut idle_worker = engine.worker().await.unwrap();
    assert!(idle_worker.has_active_runs().await.unwrap());
    assert!(idle_worker.admit().await.unwrap().is_none());
    drop(idle_worker);
    engine.pause(run(&requester)).await.unwrap();
    pause(&server, &receipt, &requester, 0, true).await;
    let task_count = summary::pool(&db).await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_tasks")
        .fetch_one(&task_count)
        .await
        .unwrap();
    task_count.close().await;
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    answer(&server, &receipt, &recipient).await;
    settle(&engine).await;
    let q = child(&server, &receipt, 0).await["questions"]["items"][0].clone();
    assert_eq!(q["outcome"]["status"], "answered");
    assert_eq!(q["requester_task_id"], requester["task_id"]);
    assert_eq!(q["recipient_task_id"], recipient["task_id"]);
    assert_eq!(
        engine
            .status(run(&requester))
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        2
    );
    pause(&server, &receipt, &requester, 1, false).await;
    settle(&engine).await;
    assert_eq!(
        engine
            .status(run(&requester))
            .await
            .unwrap()
            .record()
            .status,
        RunStatus::Paused
    );
    engine.resume(run(&requester)).await.unwrap();
    settle(&engine).await;
    let after = engine.status(run(&requester)).await.unwrap();
    assert!(after.record().worker_reply_wait.is_none());
    assert!(after.record().wait.is_some());
    assert_eq!(after.timers().len(), 1);
    assert_eq!(after.timers()[0].status, act0r::domain::TimerStatus::Fired);
    assert_eq!(after.record().usage.admitted_model_calls, 3);
    let source = child(&server, &receipt, 1).await;
    assert!(
        source["questions"]["receipts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["error"]["code"] == "question_closed")
    );
    let seen = model.requests.lock().unwrap().clone();
    let prompt = seen
        .iter()
        .find(|p| p["goal"] == "Worker 0" && p["turn"] == 2)
        .unwrap();
    assert!(prompt.to_string().contains("Return id and display_name"));
    let pool = summary::pool(&db).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_tasks")
            .fetch_one(&pool)
            .await
            .unwrap(),
        count
    );
    pool.close().await;
    server.close().await;
}

#[tokio::test]
async fn reply_before_sending_completion_and_restart_cannot_lose_or_redeliver_question() {
    let model = Model::start("reply", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("questions.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let requester = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &requester).await;
    let mut worker = engine.worker().await.unwrap();
    let model_call = admit(&mut worker).await;
    let outcome = worker.execute(model_call.clone()).await.unwrap();
    worker.complete(model_call, outcome).await.unwrap();
    let sending = admit(&mut worker).await;
    assert_eq!(sending.input["definition"]["id"]["name"], "ask_worker");
    let store = SqliteStore::open(&db).await.unwrap();
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER refuse_question_receipt BEFORE INSERT ON harness_worker_question_receipts BEGIN SELECT RAISE(ABORT,'fixture rollback'); END;").execute(&pool).await.unwrap();
    assert!(store.worker_question(call(&sending), false).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM harness_worker_questions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(
        engine
            .status(run(&recipient))
            .await
            .unwrap()
            .record()
            .peer_messages
            .is_empty()
    );
    sqlx::raw_sql("DROP TRIGGER refuse_question_receipt;")
        .execute(&pool)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        store.worker_question(call(&sending), false),
        store.worker_question(call(&sending), false)
    );
    let (
        act0r::domain::tools::NativeToolResult::Success(a),
        act0r::domain::tools::NativeToolResult::Success(b),
    ) = (a.unwrap(), b.unwrap())
    else {
        panic!("question was refused")
    };
    assert_eq!(a, b);
    let mut changed = call(&sending);
    changed.arguments["text"] = json!("Changed question");
    assert!(store.worker_question(changed, false).await.is_err());
    let mut malformed = call(&sending);
    malformed.arguments["timeout_ms"] = json!("changed type");
    assert!(store.worker_question(malformed, false).await.is_err());
    answer(&server, &receipt, &recipient).await;
    let recipient_model = admit(&mut worker).await;
    assert_eq!(recipient_model.run_id, run(&recipient));
    let outcome = worker.execute(recipient_model.clone()).await.unwrap();
    worker.complete(recipient_model, outcome).await.unwrap();
    let replying = admit(&mut worker).await;
    assert_eq!(replying.input["definition"]["id"]["name"], "reply_worker");
    sqlx::raw_sql("CREATE TRIGGER refuse_reply_receipt BEFORE INSERT ON harness_worker_question_receipts BEGIN SELECT RAISE(ABORT,'fixture rollback'); END;").execute(&pool).await.unwrap();
    assert!(store.worker_question(call(&replying), true).await.is_err());
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"],
        Value::Null
    );
    sqlx::raw_sql("DROP TRIGGER refuse_reply_receipt;")
        .execute(&pool)
        .await
        .unwrap();
    worker.execute(replying).await.unwrap();
    let q = child(&server, &receipt, 0).await["questions"]["items"][0].clone();
    assert_eq!(q["outcome"]["status"], "answered");
    let deadline = q["deadline_at"].clone();
    // Stop with the question published and answered, but the sender activity still running.
    pool.close().await;
    drop(store);
    drop(worker);
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let recovery_worker = engine.worker().await.unwrap();
    assert!(recovery_worker.has_active_runs().await.unwrap());
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(recovery_worker);
    settle(&engine).await;
    let after = engine.status(run(&requester)).await.unwrap();
    assert!(after.record().worker_reply_wait.is_none());
    assert_eq!(after.timers().len(), 1);
    assert_eq!(json!(after.timers()[0].due_at), deadline);
    let recipient_run = engine.status(run(&recipient)).await.unwrap();
    assert_eq!(recipient_run.record().peer_messages.len(), 1);
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        engine
            .history(run(&requester))
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "timer_fired")
            .count(),
        1
    );
    server.close().await;
}

#[tokio::test]
async fn saved_deadline_expires_during_pause_and_downtime_and_late_reply_is_refused() {
    let model = Model::start("reply", 1000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("questions.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let requester = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &requester).await;
    settle(&engine).await;
    let deadline =
        child(&server, &receipt, 0).await["questions"]["items"][0]["deadline_at"].clone();
    pause(&server, &receipt, &requester, 0, true).await;
    server.close().await;
    drop(engine);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    let q = child(&server, &receipt, 0).await["questions"]["items"][0].clone();
    assert_eq!(q["outcome"]["status"], "timed_out");
    assert_eq!(q["deadline_at"], deadline);
    assert_eq!(
        engine
            .status(run(&requester))
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        2
    );
    answer(&server, &receipt, &recipient).await;
    settle(&engine).await;
    assert!(
        child(&server, &receipt, 1).await["questions"]["receipts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["error"]["code"] == "question_closed")
    );
    pause(&server, &receipt, &requester, 1, false).await;
    settle(&engine).await;
    let after = engine.status(run(&requester)).await.unwrap();
    assert_eq!(json!(after.timers()[0].due_at), deadline);
    assert!(after.record().worker_reply_wait.is_none());
    assert_eq!(after.record().usage.admitted_model_calls, 3);
    server.close().await;
}

#[tokio::test]
async fn circular_wait_is_refused_and_original_question_remains_pending() {
    let model = Model::start("cycle", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("questions.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let requester = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &requester).await;
    settle(&engine).await;
    answer(&server, &receipt, &recipient).await;
    settle(&engine).await;
    let history = child(&server, &receipt, 1).await["questions"].clone();
    assert_eq!(history["items"].as_array().unwrap().len(), 1);
    assert_eq!(history["items"][0]["outcome"], Value::Null);
    assert_eq!(history["receipts"][0]["error"]["code"], "question_cycle");
    assert!(
        engine
            .status(run(&requester))
            .await
            .unwrap()
            .record()
            .worker_reply_wait
            .is_some()
    );
    assert!(
        engine
            .status(run(&recipient))
            .await
            .unwrap()
            .record()
            .worker_reply_wait
            .is_none()
    );
    server.close().await;
}

#[tokio::test]
async fn stopped_recipient_resolves_wait_without_stopping_sibling_or_joining_parent() {
    let model = Model::start("hold", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("questions.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let requester = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &requester).await;
    settle(&engine).await;
    post(
        &client(),
        &format!(
            "{}/workers/{}/cancellation",
            base(&server, &receipt),
            recipient["task_id"].as_str().unwrap()
        ),
        &json!({"request_id":MessageId::new()}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"],
        "recipient_closed"
    );
    let r = engine.status(run(&requester)).await.unwrap();
    assert!(r.record().worker_reply_wait.is_none());
    assert!(!r.record().controls.cancel_requested);
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    server.close().await;
}

#[tokio::test]
async fn parent_questions_bad_timeouts_and_unowned_reply_ids_are_saved_refusals() {
    for (mode, timeout, error) in [
        ("parent", 30000, "question_recipient"),
        ("reply", 999, "question_timeout"),
        ("wrong", 30000, "question_reply_scope"),
        ("malformed", 30000, "question_input"),
    ] {
        let model = Model::start(mode, timeout).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("questions.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let receipt = start(&server, &model, root.path()).await;
        settle(&engine).await;
        let requester = child(&server, &receipt, 0).await;
        let recipient = child(&server, &receipt, 1).await;
        answer(&server, &receipt, &requester).await;
        settle(&engine).await;
        let selected = if mode == "wrong" || mode == "malformed" {
            answer(&server, &receipt, &recipient).await;
            settle(&engine).await;
            1
        } else {
            0
        };
        let history = child(&server, &receipt, selected).await["questions"].clone();
        if mode == "malformed" {
            let saved = engine.status(run(&recipient)).await.unwrap();
            assert!(saved.activities().iter().any(|a| {
                a.result
                    .as_ref()
                    .is_some_and(|r| r["error"]["code"] == error)
            }));
            assert!(saved.record().wait.is_some());
        } else {
            assert_eq!(history["receipts"][0]["error"]["code"], error);
        }
        assert!(
            engine
                .status(run(if selected == 0 {
                    &requester
                } else {
                    &recipient
                }))
                .await
                .unwrap()
                .record()
                .worker_reply_wait
                .is_none()
        );
        server.close().await;
    }
}

#[tokio::test]
async fn stopping_requester_closes_question_without_a_new_model_call() {
    let model = Model::start("hold", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("questions.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let requester = child(&server, &receipt, 0).await;
    let recipient = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &requester).await;
    settle(&engine).await;
    pause(&server, &receipt, &requester, 0, true).await;
    post(
        &client(),
        &format!(
            "{}/workers/{}/cancellation",
            base(&server, &receipt),
            requester["task_id"].as_str().unwrap()
        ),
        &json!({"request_id":MessageId::new()}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"],
        "requester_closed"
    );
    let r = engine.status(run(&requester)).await.unwrap();
    assert_eq!(r.record().status, RunStatus::Cancelled);
    assert!(r.record().worker_reply_wait.is_none());
    assert_eq!(r.record().usage.admitted_model_calls, 2);
    assert!(
        !engine
            .status(run(&recipient))
            .await
            .unwrap()
            .record()
            .controls
            .cancel_requested
    );
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    server.close().await;
}

#[tokio::test]
async fn question_decisions_remain_bounded_after_refusals() {
    let model = Model::start("capacity", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("questions.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let requester = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &requester).await;
    settle(&engine).await;
    let history = child(&server, &receipt, 0).await["questions"].clone();
    assert!(history["items"].as_array().unwrap().is_empty());
    assert_eq!(history["receipts"].as_array().unwrap().len(), 9);
    assert!(
        history["receipts"].as_array().unwrap()[..8]
            .iter()
            .all(|r| r["error"]["code"] == "question_recipient")
    );
    assert_eq!(history["receipts"][8]["error"]["code"], "question_capacity");
    let r = engine.status(run(&requester)).await.unwrap();
    assert_eq!(r.record().usage.admitted_model_calls, 11);
    assert!(r.record().worker_reply_wait.is_none());
    assert!(r.record().wait.is_some());
    server.close().await;
}

async fn parent_run(db: &Path, receipt: &Value) -> RunId {
    let pool = summary::pool(db).await;
    let id: String = sqlx::query_scalar("SELECT run_id FROM agent_runs WHERE request_id=?")
        .bind(receipt["task_id"].as_str().unwrap())
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    id.parse().unwrap()
}
async fn stop_assignment(server: &Server, receipt: &Value, child: &Value) {
    post(
        &client(),
        &format!(
            "{}/workers/{}/cancellation",
            base(server, receipt),
            child["task_id"].as_str().unwrap()
        ),
        &json!({"request_id":MessageId::new()}),
        StatusCode::ACCEPTED,
    )
    .await;
}
async fn execute_complete(worker: &mut act0r::Worker, execution: ActivityExecution) {
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker.complete(execution, outcome).await.unwrap();
}

#[tokio::test]
async fn parent_answers_single_and_group_children_without_consuming_the_original_join() {
    for mode in ["parent_single", "parent_reply", "parent_both"] {
        let model = Model::start(mode, 30000).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("parent.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let receipt = start(&server, &model, root.path()).await;
        settle(&engine).await;
        let first = child(&server, &receipt, 0).await;
        answer(&server, &receipt, &first).await;
        settle(&engine).await;
        if mode == "parent_both" {
            let second = child(&server, &receipt, 1).await;
            answer(&server, &receipt, &second).await;
            settle(&engine).await;
        }
        let view = work(&server, &receipt).await;
        assert_eq!(view["groups"][0]["joined"], false);
        let parent = engine
            .status(parent_run(&db, &receipt).await)
            .await
            .unwrap();
        assert_eq!(parent.record().status, RunStatus::Waiting);
        assert!(parent.record().planner_question.is_none());
        assert_eq!(
            parent.record().state["state"]["pending"]["kind"],
            if mode == "parent_single" {
                "delegate_task"
            } else {
                "delegate_tasks"
            }
        );
        let count = if mode == "parent_both" { 2 } else { 1 };
        assert_eq!(parent.record().usage.admitted_model_calls, 1 + count);
        for n in 0..count as usize {
            let assignment = child(&server, &receipt, n).await;
            let question = &assignment["questions"]["items"][0];
            assert_eq!(question["recipient"], "parent");
            assert_eq!(question["recipient_task_id"], receipt["task_id"]);
            assert_eq!(question["recipient_run_id"], json!(parent.record().id));
            assert_eq!(question["outcome"]["status"], "answered");
            let requester = engine.status(run(&assignment)).await.unwrap();
            assert!(requester.record().worker_reply_wait.is_none());
            assert_eq!(requester.record().usage.admitted_model_calls, 3);
            assert!(
                requester
                    .record()
                    .state
                    .to_string()
                    .contains("Return id and display_name")
            );
        }
        let task_count = parent.record().delegations.len();
        for assignment in view["groups"][0]["children"].as_array().unwrap() {
            stop_assignment(&server, &receipt, assignment).await;
        }
        settle(&engine).await;
        let parent = engine.status(parent.record().id).await.unwrap();
        assert_eq!(parent.record().delegations.len(), task_count);
        assert!(
            parent
                .record()
                .delegations
                .iter()
                .all(|d| d.outcome.is_some())
        );
        assert_eq!(parent.record().usage.admitted_model_calls, 2 + count);
        let joins = engine
            .history(parent.record().id)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.kind == "task_group_joined")
            .count();
        assert_eq!(joins, if mode == "parent_single" { 0 } else { 1 });
        server.close().await;
    }
}

#[tokio::test]
async fn parent_reply_and_suspended_join_recover_after_uncommitted_tool_completion() {
    let model = Model::start("parent_reply", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parent.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &first).await;
    let mut worker = engine.worker().await.unwrap();
    let decision = admit(&mut worker).await;
    execute_complete(&mut worker, decision).await;
    let ask = admit(&mut worker).await;
    execute_complete(&mut worker, ask).await;
    let parent_model = admit(&mut worker).await;
    let parent = parent_run(&db, &receipt).await;
    assert_eq!(parent_model.run_id, parent);
    let input = parent_model.input.clone();
    let schema = &input["schema"]["properties"]["action"];
    assert_eq!(schema["anyOf"].as_array().unwrap().len(), 5);
    assert_eq!(
        schema["anyOf"][0]["properties"]["kind"]["enum"],
        json!(["reply_worker"])
    );
    execute_complete(&mut worker, parent_model).await;
    let reply = admit(&mut worker).await;
    assert_eq!(reply.input["definition"]["id"]["name"], "reply_worker");
    worker.execute(reply).await.unwrap();
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"],
        "answered"
    );
    let deadline =
        child(&server, &receipt, 0).await["questions"]["items"][0]["deadline_at"].clone();
    engine.pause(parent).await.unwrap();
    drop(worker);
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let worker = engine.worker().await.unwrap();
    drop(worker);
    tokio::time::sleep(Duration::from_millis(150)).await;
    settle(&engine).await;
    assert_eq!(
        engine.status(parent).await.unwrap().record().status,
        RunStatus::Paused
    );
    engine.resume(parent).await.unwrap();
    settle(&engine).await;
    let state = engine.status(parent).await.unwrap();
    assert_eq!(state.record().status, RunStatus::Waiting);
    assert!(state.record().planner_question.is_none());
    assert_eq!(state.record().usage.admitted_model_calls, 2);
    assert_eq!(
        state.record().state["state"]["pending"]["kind"],
        "delegate_tasks"
    );
    let question = child(&server, &receipt, 0).await["questions"]["items"][0].clone();
    assert_eq!(question["deadline_at"], deadline);
    let pool = summary::pool(&db).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM harness_worker_question_receipts WHERE task_id=?"
        )
        .bind(receipt["task_id"].as_str().unwrap())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    pool.close().await;
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    server.close().await;
}

#[tokio::test]
async fn expired_parent_question_is_skipped_after_paused_downtime_without_model_spend() {
    let model = Model::start("parent_reply", 1000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parent.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let parent = parent_run(&db, &receipt).await;
    engine.pause(parent).await.unwrap();
    let first = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &first).await;
    settle(&engine).await;
    let deadline =
        child(&server, &receipt, 0).await["questions"]["items"][0]["deadline_at"].clone();
    server.close().await;
    drop(engine);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    let q = child(&server, &receipt, 0).await["questions"]["items"][0].clone();
    assert_eq!(q["deadline_at"], deadline);
    assert_eq!(q["outcome"]["status"], "timed_out");
    engine.resume(parent).await.unwrap();
    settle(&engine).await;
    let state = engine.status(parent).await.unwrap();
    assert_eq!(state.record().usage.admitted_model_calls, 1);
    assert_eq!(state.record().status, RunStatus::Waiting);
    assert!(state.record().planner_question.is_none());
    assert_eq!(
        engine
            .status(run(&first))
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        3
    );
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    server.close().await;
}

#[tokio::test]
async fn all_child_results_remain_saved_until_inflight_parent_reply_finishes() {
    let model = Model::start("parent_reply", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parent.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    let second = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &first).await;
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..2 {
        let e = admit(&mut worker).await;
        execute_complete(&mut worker, e).await;
    }
    let parent_model = admit(&mut worker).await;
    let parent = parent_model.run_id;
    let outcome = worker.execute(parent_model.clone()).await.unwrap();
    stop_assignment(&server, &receipt, &first).await;
    stop_assignment(&server, &receipt, &second).await;
    for _ in 0..100 {
        if !worker.advance_work().await.unwrap() {
            break;
        }
    }
    let saved = engine.status(parent).await.unwrap();
    assert!(
        saved
            .record()
            .delegations
            .iter()
            .all(|d| d.outcome.is_some())
    );
    assert!(saved.record().task_groups[0].joined_at.is_none());
    assert!(saved.record().planner_question.is_some());
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    worker.complete(parent_model, outcome).await.unwrap();
    drop(worker);
    settle(&engine).await;
    let finished = engine.status(parent).await.unwrap();
    assert!(finished.record().planner_question.is_none());
    assert!(finished.record().task_groups[0].joined_at.is_some());
    assert_eq!(finished.record().usage.admitted_model_calls, 3);
    assert_eq!(
        engine
            .history(parent)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"],
        "requester_closed"
    );
    server.close().await;
}

#[tokio::test]
async fn parent_question_cannot_create_a_nested_wait_or_another_assignment() {
    let model = Model::start("parent_invalid", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parent.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &first).await;
    settle(&engine).await;
    let parent = engine
        .status(parent_run(&db, &receipt).await)
        .await
        .unwrap();
    assert_eq!(parent.record().delegations.len(), 2);
    assert_eq!(parent.record().status, RunStatus::Failed);
    assert_eq!(
        parent.record().error.as_ref().unwrap().code,
        "model_invalid_action"
    );
    assert!(
        parent
            .record()
            .error
            .as_ref()
            .unwrap()
            .message
            .contains("saved schema")
    );
    assert!(matches!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"].as_str(),
        Some("recipient_closed" | "requester_closed")
    ));
    server.close().await;
}

#[tokio::test]
async fn parent_question_wakeup_rolls_back_and_survives_restart_before_ask_completion() {
    let model = Model::start("parent_reply", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parent.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    let parent = parent_run(&db, &receipt).await;
    answer(&server, &receipt, &first).await;
    let mut worker = engine.worker().await.unwrap();
    let model_call = admit(&mut worker).await;
    execute_complete(&mut worker, model_call).await;
    let ask = admit(&mut worker).await;
    let store = SqliteStore::open(&db).await.unwrap();
    let pool = summary::pool(&db).await;
    sqlx::raw_sql("CREATE TRIGGER refuse_parent_receipt BEFORE INSERT ON harness_worker_question_receipts BEGIN SELECT RAISE(ABORT,'fixture rollback'); END;").execute(&pool).await.unwrap();
    assert!(store.worker_question(call(&ask), false).await.is_err());
    assert!(engine.status(parent).await.unwrap().timers().is_empty());
    assert!(
        engine
            .status(parent)
            .await
            .unwrap()
            .record()
            .peer_messages
            .is_empty()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM harness_worker_questions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    sqlx::raw_sql("DROP TRIGGER refuse_parent_receipt;")
        .execute(&pool)
        .await
        .unwrap();
    worker.execute(ask.clone()).await.unwrap();
    let parent_model = admit(&mut worker).await;
    assert_eq!(parent_model.run_id, parent);
    execute_complete(&mut worker, parent_model).await;
    let reply = admit(&mut worker).await;
    execute_complete(&mut worker, reply).await;
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"],
        "answered"
    );
    pool.close().await;
    drop(store);
    drop(worker);
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let worker = engine.worker().await.unwrap();
    drop(worker);
    tokio::time::sleep(Duration::from_millis(150)).await;
    settle(&engine).await;
    let history = child(&server, &receipt, 0).await["questions"].clone();
    assert_eq!(history["items"].as_array().unwrap().len(), 1);
    assert_eq!(history["receipts"].as_array().unwrap().len(), 1);
    let caller = engine.status(run(&first)).await.unwrap();
    assert_eq!(caller.record().usage.admitted_model_calls, 3);
    assert_eq!(caller.timers().len(), 1);
    assert!(caller.record().worker_reply_wait.is_none());
    let planner = engine.status(parent).await.unwrap();
    assert_eq!(planner.timers().len(), 1);
    assert_eq!(planner.record().usage.admitted_model_calls, 2);
    assert_eq!(planner.record().status, RunStatus::Waiting);
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    server.close().await;
}

#[tokio::test]
async fn two_pending_parent_questions_run_one_reply_step_at_a_time() {
    let model = Model::start("parent_both", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parent.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    let second = child(&server, &receipt, 1).await;
    answer(&server, &receipt, &first).await;
    answer(&server, &receipt, &second).await;
    let mut worker = engine.worker().await.unwrap();
    let model_a = admit(&mut worker).await;
    let model_b = admit(&mut worker).await;
    assert_ne!(model_a.run_id, model_b.run_id);
    execute_complete(&mut worker, model_a).await;
    execute_complete(&mut worker, model_b).await;
    let ask_a = admit(&mut worker).await;
    let ask_b = admit(&mut worker).await;
    assert_eq!(ask_a.input["definition"]["id"]["name"], "ask_worker");
    assert_eq!(ask_b.input["definition"]["id"]["name"], "ask_worker");
    let (a, b) = tokio::join!(worker.execute(ask_a.clone()), worker.execute(ask_b.clone()));
    let parent = parent_run(&db, &receipt).await;
    assert_eq!(engine.status(parent).await.unwrap().timers().len(), 2);
    assert!(worker.fire_due_timer().await.unwrap());
    assert!(
        engine
            .status(parent)
            .await
            .unwrap()
            .record()
            .planner_question
            .is_some()
    );
    assert!(!worker.fire_due_timer().await.unwrap());
    worker.complete(ask_a, a.unwrap()).await.unwrap();
    worker.complete(ask_b, b.unwrap()).await.unwrap();
    assert!(!worker.fire_due_timer().await.unwrap());
    drop(worker);
    settle(&engine).await;
    let planner = engine.status(parent).await.unwrap();
    assert!(planner.record().planner_question.is_none());
    assert_eq!(planner.record().status, RunStatus::Waiting);
    assert_eq!(planner.record().usage.admitted_model_calls, 3);
    for n in 0..2 {
        assert_eq!(
            child(&server, &receipt, n).await["questions"]["items"][0]["outcome"]["status"],
            "answered"
        );
    }
    assert_eq!(
        engine
            .history(parent)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "planner_question_finished")
            .count(),
        2
    );
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    server.close().await;
}

#[tokio::test]
async fn whole_goal_stop_waits_for_admitted_parent_model_after_children_settle() {
    let model = Model::start("parent_reply", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("parent.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &first).await;
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..2 {
        let e = admit(&mut worker).await;
        execute_complete(&mut worker, e).await;
    }
    let parent_model = admit(&mut worker).await;
    let parent = parent_model.run_id;
    let outcome = worker.execute(parent_model.clone()).await.unwrap();
    post(
        &client(),
        &format!(
            "{}/v1/teams/{}/tasks/{}/cancel",
            server.url,
            receipt["team_id"].as_str().unwrap(),
            receipt["task_id"].as_str().unwrap()
        ),
        &json!({}),
        StatusCode::OK,
    )
    .await;
    for _ in 0..100 {
        if !worker.advance_work().await.unwrap() {
            break;
        }
    }
    let stopping = engine.status(parent).await.unwrap();
    assert!(stopping.record().controls.cancel_requested);
    assert!(
        stopping
            .record()
            .delegations
            .iter()
            .all(|d| d.outcome.is_some())
    );
    assert_eq!(stopping.record().status, RunStatus::Running);
    worker.complete(parent_model, outcome).await.unwrap();
    drop(worker);
    settle(&engine).await;
    let stopped = engine.status(parent).await.unwrap();
    assert_eq!(stopped.record().status, RunStatus::Cancelled);
    assert!(stopped.record().planner_question.is_none());
    assert_eq!(stopped.record().usage.admitted_model_calls, 2);
    assert_eq!(stopped.record().usage.actual_input_tokens, 20);
    assert!(
        !stopped
            .activities()
            .iter()
            .any(|a| a.spec.input["definition"]["id"]["name"] == "reply_worker")
    );
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"],
        "requester_closed"
    );
    server.close().await;
}

async fn begin_parent_inspection(worker: &mut act0r::Worker, parent: RunId) -> ActivityExecution {
    // Requester decision, ask tool, then the parent's first coordination decision.
    for _ in 0..3 {
        let e = admit(worker).await;
        execute_complete(worker, e).await;
    }
    let read = admit(worker).await;
    assert_eq!(read.run_id, parent);
    assert_eq!(read.input["definition"]["id"]["name"], "list_files");
    assert!(call(&read).writable_paths.is_empty());
    assert!(read.input["test_command"].is_null());
    read
}

#[tokio::test]
async fn planner_inspection_saved_evidence_and_original_join_survive_restart() {
    let model = Model::start("parent_inspect", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspection.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let parent = parent_run(&db, &receipt).await;
    let first = child(&server, &receipt, 0).await;
    let first_run = engine.status(run(&first)).await.unwrap();
    let child_workspace = first_run.record().state["state"]["config"]["workspace"]
        .as_str()
        .unwrap();
    fs::write(
        Path::new(child_workspace).join("src/lib.rs"),
        "Child-only draft\n",
    )
    .unwrap();
    answer(&server, &receipt, &first).await;
    let mut worker = engine.worker().await.unwrap();
    let list = begin_parent_inspection(&mut worker, parent).await;
    let parent_before = engine.status(parent).await.unwrap();
    assert_eq!(
        list.input["workspace"],
        parent_before.record().state["state"]["config"]["workspace"]
    );
    assert_ne!(list.input["workspace"], json!(child_workspace));
    let list_id = list.activity_id;
    execute_complete(&mut worker, list).await;
    engine.pause(parent).await.unwrap();
    let checkpoint = engine.status(parent).await.unwrap();
    assert_eq!(
        checkpoint.record().state["state"]["question_inspections"],
        1
    );
    let join = checkpoint.record().state["state"]["suspended_delegation"].clone();
    let deadline =
        child(&server, &receipt, 0).await["questions"]["items"][0]["deadline_at"].clone();
    drop(worker);
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    assert_eq!(
        engine.status(parent).await.unwrap().record().status,
        RunStatus::Paused
    );
    engine.resume(parent).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    // Search, then a read of the parent's code, preserving all three saved observations.
    for expected in ["search_text", "read_file"] {
        let decision = admit(&mut worker).await;
        execute_complete(&mut worker, decision).await;
        let read = admit(&mut worker).await;
        assert_eq!(read.input["definition"]["id"]["name"], expected);
        assert_eq!(
            read.input["workspace"],
            checkpoint.record().state["state"]["config"]["workspace"]
        );
        execute_complete(&mut worker, read).await;
    }
    let decision = admit(&mut worker).await;
    let schema = &decision.input["schema"]["properties"]["action"];
    assert_eq!(
        schema["anyOf"][0]["properties"]["kind"]["enum"],
        json!(["reply_worker"])
    );
    assert_eq!(schema["anyOf"].as_array().unwrap().len(), 2);
    let p: Value = serde_json::from_str(decision.input["prompt"].as_str().unwrap()).unwrap();
    assert_eq!(p["question_inspection"]["used"], 3);
    assert_eq!(p["question_inspection"]["remaining"], 0);
    assert_eq!(p["tools"].as_array().unwrap().len(), 2);
    assert!(
        p.to_string()
            .contains("Saved source: API fields id and display_name")
    );
    assert!(!p.to_string().contains("Child-only draft"));
    execute_complete(&mut worker, decision).await;
    let reply = admit(&mut worker).await;
    execute_complete(&mut worker, reply).await;
    drop(worker);
    settle(&engine).await;
    let saved = engine.status(parent).await.unwrap();
    assert_eq!(saved.record().status, RunStatus::Waiting);
    assert!(saved.record().planner_question.is_none());
    assert!(
        saved.record().state["state"]
            .get("question_inspections")
            .is_none()
    );
    assert_eq!(saved.record().state["state"]["pending"], join);
    assert_eq!(saved.record().usage.admitted_model_calls, 5);
    assert_eq!(
        saved
            .activities()
            .iter()
            .filter(|a| a.id == list_id)
            .count(),
        1
    );
    assert_eq!(
        saved
            .activities()
            .iter()
            .find(|a| a.id == list_id)
            .unwrap()
            .attempts,
        1
    );
    let q = child(&server, &receipt, 0).await["questions"]["items"][0].clone();
    assert_eq!(q["deadline_at"], deadline);
    assert_eq!(q["outcome"]["status"], "answered");
    assert!(
        q["outcome"]["text"]
            .as_str()
            .unwrap()
            .contains("src/lib.rs")
    );
    let view = work(&server, &receipt).await;
    assert_eq!(view["groups"][0]["joined"], false);
    for c in view["groups"][0]["children"].as_array().unwrap() {
        stop_assignment(&server, &receipt, c).await;
    }
    settle(&engine).await;
    assert_eq!(
        engine
            .history(parent)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    assert!(
        fs::read_to_string(root.path().join("project/src/lib.rs"))
            .unwrap()
            .starts_with("Saved source")
    );
    server.close().await;
}

#[tokio::test]
async fn planner_inspection_uncommitted_read_recovers_same_activity_and_counter() {
    let model = Model::start("parent_inspect", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspection.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let parent = parent_run(&db, &receipt).await;
    let first = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &first).await;
    let mut worker = engine.worker().await.unwrap();
    let read = begin_parent_inspection(&mut worker, parent).await;
    worker.execute(read.clone()).await.unwrap();
    engine.pause(parent).await.unwrap();
    drop(worker);
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let worker = engine.worker().await.unwrap();
    drop(worker);
    engine.resume(parent).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut worker = engine.worker().await.unwrap();
    let replay = admit(&mut worker).await;
    assert_eq!(replay.activity_id, read.activity_id);
    assert_eq!(replay.input, read.input);
    assert_eq!(
        engine.status(parent).await.unwrap().record().state["state"]["question_inspections"],
        1
    );
    execute_complete(&mut worker, replay).await;
    drop(worker);
    settle(&engine).await;
    let saved = engine.status(parent).await.unwrap();
    assert_eq!(saved.record().status, RunStatus::Waiting);
    assert_eq!(saved.record().usage.admitted_model_calls, 5);
    assert_eq!(
        saved
            .activities()
            .iter()
            .find(|a| a.id == read.activity_id)
            .unwrap()
            .attempts,
        2
    );
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"],
        "answered"
    );
    server.close().await;
}

#[tokio::test]
async fn planner_inspection_deadline_survives_paused_restart_without_more_model_spend() {
    let model = Model::start("parent_inspect", 1000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspection.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let parent = parent_run(&db, &receipt).await;
    let first = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &first).await;
    let mut worker = engine.worker().await.unwrap();
    let read = begin_parent_inspection(&mut worker, parent).await;
    execute_complete(&mut worker, read.clone()).await;
    engine.pause(parent).await.unwrap();
    let deadline =
        child(&server, &receipt, 0).await["questions"]["items"][0]["deadline_at"].clone();
    drop(worker);
    server.close().await;
    drop(engine);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    settle(&engine).await;
    engine.resume(parent).await.unwrap();
    settle(&engine).await;
    let saved = engine.status(parent).await.unwrap();
    assert_eq!(saved.record().status, RunStatus::Waiting);
    assert!(saved.record().planner_question.is_none());
    assert_eq!(saved.record().usage.admitted_model_calls, 2);
    assert!(
        saved
            .activities()
            .iter()
            .find(|a| a.id == read.activity_id)
            .unwrap()
            .result
            .is_some()
    );
    assert_eq!(
        saved.record().state["state"]["pending"]["kind"],
        "delegate_tasks"
    );
    let q = child(&server, &receipt, 0).await["questions"]["items"][0].clone();
    assert_eq!(q["deadline_at"], deadline);
    assert_eq!(q["outcome"]["status"], "timed_out");
    assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
    server.close().await;
}

#[tokio::test]
async fn planner_inspection_admitted_read_settles_after_deadline_or_requester_stop() {
    for stop in [false, true] {
        let model = Model::start("parent_inspect", if stop { 30000 } else { 1000 }).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("inspection.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let receipt = start(&server, &model, root.path()).await;
        settle(&engine).await;
        let parent = parent_run(&db, &receipt).await;
        let first = child(&server, &receipt, 0).await;
        answer(&server, &receipt, &first).await;
        let mut worker = engine.worker().await.unwrap();
        let read = begin_parent_inspection(&mut worker, parent).await;
        let result = worker.execute(read.clone()).await.unwrap();
        if stop {
            stop_assignment(&server, &receipt, &first).await;
        } else {
            tokio::time::sleep(Duration::from_millis(1100)).await;
        }
        worker.complete(read.clone(), result).await.unwrap();
        drop(worker);
        settle(&engine).await;
        let saved = engine.status(parent).await.unwrap();
        assert_eq!(saved.record().status, RunStatus::Waiting);
        assert!(saved.record().planner_question.is_none());
        assert_eq!(saved.record().usage.admitted_model_calls, 2);
        assert!(
            saved
                .activities()
                .iter()
                .find(|a| a.id == read.activity_id)
                .unwrap()
                .result
                .is_some()
        );
        assert!(saved.record().state.to_string().contains("src/lib.rs"));
        assert_eq!(
            child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"],
            if stop {
                "requester_closed"
            } else {
                "timed_out"
            }
        );
        assert_eq!(work(&server, &receipt).await["groups"][0]["joined"], false);
        server.close().await;
    }
}

#[tokio::test]
async fn planner_inspection_refused_reads_can_be_corrected_and_share_the_limit() {
    let model = Model::start("parent_inspect_feedback", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspection.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(&server, &model, root.path()).await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &first).await;
    settle(&engine).await;
    let saved = engine
        .status(parent_run(&db, &receipt).await)
        .await
        .unwrap();
    assert_eq!(saved.record().status, RunStatus::Waiting);
    assert_eq!(saved.record().usage.admitted_model_calls, 4);
    let reads: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.input["definition"]["id"]["name"] == "read_file")
        .collect();
    assert_eq!(reads.len(), 2);
    assert_eq!(reads[0].result.as_ref().unwrap()["ok"], false);
    assert_eq!(reads[1].result.as_ref().unwrap()["ok"], true);
    let seen = model.requests.lock().unwrap().clone();
    let corrected = seen
        .iter()
        .find(|p| p["question_inspection"]["used"] == 1)
        .unwrap();
    assert!(corrected.to_string().contains("missing.rs"));
    assert_eq!(
        child(&server, &receipt, 0).await["questions"]["items"][0]["outcome"]["status"],
        "answered"
    );
    server.close().await;
}

#[tokio::test]
async fn planner_inspection_cannot_exceed_limit_or_dispatch_an_edit() {
    for mode in ["parent_inspect_loop", "parent_inspect_write"] {
        let model = Model::start(mode, 30000).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("inspection.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let receipt = start(&server, &model, root.path()).await;
        settle(&engine).await;
        let first = child(&server, &receipt, 0).await;
        answer(&server, &receipt, &first).await;
        settle(&engine).await;
        let saved = engine
            .status(parent_run(&db, &receipt).await)
            .await
            .unwrap();
        assert_eq!(saved.record().status, RunStatus::Failed);
        assert_eq!(
            saved.record().error.as_ref().unwrap().code,
            "model_invalid_action"
        );
        let reads = saved
            .activities()
            .iter()
            .filter(|a| {
                ["read_file", "search_text", "list_files"].contains(
                    &a.spec.input["definition"]["id"]["name"]
                        .as_str()
                        .unwrap_or(""),
                )
            })
            .count();
        assert_eq!(reads, if mode == "parent_inspect_loop" { 3 } else { 0 });
        assert!(saved.record().wait.is_none());
        assert!(
            saved
                .activities()
                .iter()
                .all(|a| a.spec.input["definition"]["id"]["name"] != "replace_text")
        );
        let workspace = saved.record().state["state"]["config"]["workspace"]
            .as_str()
            .unwrap();
        assert!(
            fs::read_to_string(Path::new(workspace).join("src/lib.rs"))
                .unwrap()
                .starts_with("Saved source")
        );
        server.close().await;
    }
}

#[tokio::test]
async fn planner_inspection_child_join_and_whole_goal_stop_wait_for_admitted_read() {
    for stop_goal in [false, true] {
        let model = Model::start("parent_inspect", 30000).await;
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("inspection.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let server = Server::start(Arc::new(agents), 2).await;
        let receipt = start(&server, &model, root.path()).await;
        settle(&engine).await;
        let parent = parent_run(&db, &receipt).await;
        let first = child(&server, &receipt, 0).await;
        answer(&server, &receipt, &first).await;
        let mut worker = engine.worker().await.unwrap();
        let read = begin_parent_inspection(&mut worker, parent).await;
        let outcome = worker.execute(read.clone()).await.unwrap();
        if stop_goal {
            post(
                &client(),
                &format!(
                    "{}/v1/teams/{}/tasks/{}/cancel",
                    server.url,
                    receipt["team_id"].as_str().unwrap(),
                    receipt["task_id"].as_str().unwrap()
                ),
                &json!({}),
                StatusCode::OK,
            )
            .await;
        } else {
            let view = work(&server, &receipt).await;
            for c in view["groups"][0]["children"].as_array().unwrap() {
                stop_assignment(&server, &receipt, c).await;
            }
        }
        for _ in 0..100 {
            if !worker.advance_work().await.unwrap() {
                break;
            }
        }
        let saved = engine.status(parent).await.unwrap();
        assert_eq!(saved.record().status, RunStatus::Running);
        // Whole-goal cancellation may record settled child outcomes early; it
        // cannot dispatch their continuation until the admitted read settles.
        if stop_goal {
            assert!(saved.record().controls.cancel_requested);
        } else {
            assert!(saved.record().task_groups[0].joined_at.is_none());
        }
        assert!(
            saved
                .record()
                .delegations
                .iter()
                .all(|d| d.outcome.is_some())
        );
        worker.complete(read.clone(), outcome).await.unwrap();
        drop(worker);
        settle(&engine).await;
        let saved = engine.status(parent).await.unwrap();
        assert!(saved.record().planner_question.is_none());
        assert!(
            saved
                .activities()
                .iter()
                .find(|a| a.id == read.activity_id)
                .unwrap()
                .result
                .is_some()
        );
        assert!(
            saved
                .activities()
                .iter()
                .all(|a| a.spec.input["definition"]["id"]["name"] != "reply_worker")
        );
        if stop_goal {
            assert_eq!(saved.record().status, RunStatus::Cancelled);
            assert_eq!(saved.record().usage.admitted_model_calls, 2);
        } else {
            assert_eq!(saved.record().usage.admitted_model_calls, 3);
            assert!(saved.record().task_groups[0].joined_at.is_some());
            assert_eq!(
                engine
                    .history(parent)
                    .await
                    .unwrap()
                    .iter()
                    .filter(|e| e.kind == "task_group_joined")
                    .count(),
                1
            );
        }
        server.close().await;
    }
}

#[tokio::test]
async fn planner_inspection_continuations_obey_the_original_shared_model_budget() {
    let model = Model::start("parent_inspect", 30000).await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("inspection.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let mut input = start_input(&model, root.path());
    // Root delegation, both initial child decisions, requester ask, parent read decision.
    input["budget"] = json!({"max_model_calls":5,"max_tokens":null});
    let receipt = post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    let first = child(&server, &receipt, 0).await;
    answer(&server, &receipt, &first).await;
    settle(&engine).await;
    let parent = parent_run(&db, &receipt).await;
    let saved = engine.status(parent).await.unwrap();
    assert_eq!(saved.record().status, RunStatus::BudgetExhausted);
    assert_eq!(saved.record().usage.admitted_model_calls, 2);
    let reads: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.input["definition"]["id"]["name"] == "list_files")
        .collect();
    assert_eq!(reads.len(), 1);
    assert!(reads[0].result.is_some());
    assert!(
        saved
            .activities()
            .iter()
            .all(|a| a.spec.input["definition"]["id"]["name"] != "reply_worker")
    );
    assert_eq!(model.requests.lock().unwrap().len(), 5);
    server.close().await;
}
