use super::*;
use act0r::{
    Worker,
    domain::{RunId, RunStatus},
};
use std::sync::Mutex;

struct Model {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
fn update(revision: u32, inspect: &str, remaining_id: &str, remaining_status: &str) -> Value {
    json!({"kind":"update_plan","expected_revision":revision,"explanation":if remaining_id=="review" {"User steering changed the remaining work to a review"} else {"Inspect the source before implementing"},"steps":[
        {"id":"inspect","title":"Inspect the project","status":inspect},
        {"id":remaining_id,"title":if remaining_id=="review" {"Review the design"} else {"Implement the change"},"status":remaining_status}
    ]})
}
impl Model {
    async fn start() -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let task = tokio::spawn(async move {
            let router=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move |axum::Json(body):axum::Json<Value>| {
                let prompt:Value=serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                seen.lock().unwrap().push(prompt.clone());
                let action=if prompt["goal"]=="Simple follow-up" {json!({"kind":"finish","summary":"Simple answer without a checklist"})}
                    else { match prompt["turn"].as_u64().unwrap() {
                        0=>update(0,"in_progress","build","pending"),
                        1=>json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}),
                        2=>update(1,"completed","build","in_progress"),
                        3=>json!({"kind":"ask_human","question":"Implementation or review?"}),
                        4=>if prompt["goal"]=="Refuse an update" {update(1,"pending","build","pending")} else {update(2,"completed","review","in_progress")},
                        5=>if prompt["goal"]=="Refuse an update" {json!({"kind":"finish","summary":"Incomplete checklist is retained"})} else {update(3,"completed","review","completed")},
                        _=>json!({"kind":"finish","summary":"Inspected and reviewed the design"}),
                    }};
                async move {axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":native_wire_action(action, &body)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))}
            }));
            axum::serve(listener, router).await.unwrap();
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
fn goal(server: &Server, receipt: &Value) -> String {
    format!(
        "{}/v1/teams/{}/tasks/{}",
        server.url,
        receipt["team_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    )
}
async fn start(server: &Server, model: &Model, source: &Path, prompt: &str) -> Value {
    fs::create_dir_all(source).unwrap();
    fs::write(source.join("README.md"), "Project design\n").unwrap();
    let mut input = summary::input(&model.endpoint);
    input["prompt"] = json!(prompt);
    input["workspace"] = json!(fs::canonicalize(source).unwrap().to_string_lossy());
    post(
        &client(),
        &format!("{}/v1/harness/sessions", server.url),
        &input,
        StatusCode::ACCEPTED,
    )
    .await
}
fn root_plan(view: &Value) -> Value {
    view["task"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["task_id"] == view["task"]["requested_task_id"])
        .unwrap()["plan"]
        .clone()
}
async fn admit(worker: &mut Worker) -> act0r::domain::ActivityExecution {
    for _ in 0..100 {
        if worker.advance_work().await.unwrap() {
            continue;
        }
        if let Some(execution) = worker.admit().await.unwrap() {
            return execution;
        }
    }
    panic!("activity was not admitted");
}
async fn answer(server: &Server, receipt: &Value) {
    let url = goal(server, receipt);
    let inbox = get(&client(), &format!("{url}/inbox")).await;
    let wait = inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["actionable"] == true)
        .unwrap();
    post(
        &client(),
        &format!(
            "{url}/waits/{}/responses",
            wait["wait"]["id"].as_str().unwrap()
        ),
        &json!({"kind":"answer","answer":"Review the design"}),
        StatusCode::OK,
    )
    .await;
}

#[tokio::test]
async fn checkpoint_and_plan_events_commit_once_and_restore_after_restart_and_steering() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("plans.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(
        &server,
        &model,
        &root.path().join("source"),
        "Plan and revise",
    )
    .await;
    assert!(root_plan(&get(&client(), &goal(&server, &receipt)).await).is_null());
    let mut worker = engine.worker().await.unwrap();
    let execution = admit(&mut worker).await;
    let outcome = worker.execute(execution.clone()).await.unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    let before = engine.status(execution.run_id).await.unwrap();
    worker.complete(execution.clone(), outcome).await.unwrap();
    let after = engine.status(execution.run_id).await.unwrap();
    assert_eq!(before.record().state, after.record().state);
    assert_eq!(
        before.record().history_sequence,
        after.record().history_sequence
    );
    assert_eq!(
        root_plan(&get(&client(), &goal(&server, &receipt)).await)["revision"],
        1
    );
    drop(worker);
    settle(&engine).await;
    let saved = root_plan(&get(&client(), &goal(&server, &receipt)).await);
    assert_eq!(saved["revision"], 2);
    assert_eq!(saved["steps"][0]["status"], "completed");
    let steering_url = format!(
        "{}/v1/harness/sessions/{}/turns/{}/steering",
        server.url,
        receipt["session_id"].as_str().unwrap(),
        receipt["task_id"].as_str().unwrap()
    );
    post(&client(),&steering_url,&json!({"message_id":MessageId::new(),"run_id":execution.run_id,"text":"Replace implementation with a review"}),StatusCode::ACCEPTED).await;
    server.close().await;
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    assert_eq!(
        root_plan(&get(&client(), &goal(&server, &receipt)).await),
        saved
    );
    assert_eq!(model.requests.lock().unwrap().len(), 4);
    answer(&server, &receipt).await;
    settle(&engine).await;
    let final_plan = root_plan(&get(&client(), &goal(&server, &receipt)).await);
    assert_eq!(final_plan["revision"], 4);
    assert_eq!(final_plan["steps"][0], saved["steps"][0]);
    assert_eq!(final_plan["steps"][1]["id"], "review");
    assert_eq!(final_plan["steps"][1]["status"], "completed");
    let requests = model.requests.lock().unwrap().clone();
    assert_eq!(requests[4]["task_plan"], saved);
    assert_eq!(
        requests[4]["steering"][0]["text"],
        "Replace implementation with a review"
    );
    assert_eq!(requests[6]["task_plan"], final_plan);
    let id: RunId = execution.run_id;
    assert_eq!(
        engine.status(id).await.unwrap().record().status,
        RunStatus::Succeeded
    );
    let events = get(
        &client(),
        &format!("{}/events?limit=200", goal(&server, &receipt)),
    )
    .await;
    let plans: Vec<_> = events["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["event"]["kind"] == "task_plan_updated")
        .collect();
    assert_eq!(plans.len(), 4);
    assert_eq!(
        plans.last().unwrap()["event"]["payload"]["plan"],
        final_plan
    );
    let later = post(
        &client(),
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            server.url,
            receipt["session_id"].as_str().unwrap()
        ),
        &json!({"message_id":MessageId::new(),"prompt":"Simple follow-up"}),
        StatusCode::ACCEPTED,
    )
    .await;
    settle(&engine).await;
    assert!(root_plan(&get(&client(), &goal(&server, &later)).await).is_null());
    assert_eq!(
        root_plan(&get(&client(), &goal(&server, &receipt)).await),
        final_plan
    );
    assert!(model.requests.lock().unwrap().last().unwrap()["task_plan"].is_null());
    server.close().await;
}

#[tokio::test]
async fn refused_update_keeps_the_saved_plan_and_returns_feedback_in_the_next_request() {
    let model = Model::start().await;
    let root = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(root.path().join("refused.db")).await.unwrap();
    let server = Server::start(Arc::new(agents), 2).await;
    let receipt = start(
        &server,
        &model,
        &root.path().join("source"),
        "Refuse an update",
    )
    .await;
    settle(&engine).await;
    let saved = root_plan(&get(&client(), &goal(&server, &receipt)).await);
    answer(&server, &receipt).await;
    settle(&engine).await;
    assert_eq!(
        root_plan(&get(&client(), &goal(&server, &receipt)).await),
        saved
    );
    let requests = model.requests.lock().unwrap().clone();
    let feedback = requests.last().unwrap()["observations"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(feedback["result"]["error"]["code"], "invalid_plan_update");
    let events = get(
        &client(),
        &format!("{}/events?limit=200", goal(&server, &receipt)),
    )
    .await;
    assert_eq!(
        events["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["event"]["kind"] == "task_plan_updated")
            .count(),
        2
    );
    server.close().await;
}
