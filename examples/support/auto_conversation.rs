//! Example orchestration only. The installed harness owns execution and decisions.
use act0r::{
    Engine, Worker,
    adapters::{
        http_api::{self, ApiOptions},
        model::ModelRouter,
    },
    bootstrap::local_app,
    domain::{
        MessageId,
        agent::{ModelRequest, ModelTarget},
        project_setup::ProjectSetupProposal,
    },
};
use clap::{Parser, ValueEnum};
use reqwest::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::oneshot, task::JoinHandle};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Parser)]
#[command(about = "Auto discovery, reviewed JavaScript repair, restart, application and follow-up")]
pub struct Options {
    #[arg(long, default_value = ".build/auto-conversation")]
    pub output: PathBuf,
    /// Use this configuration's model for every native actor. Otherwise use a local fixture.
    #[arg(long, conflicts_with = "resume")]
    pub target_config: Option<PathBuf>,
    /// Save the first exact model request without admitting a provider call.
    #[arg(long, requires = "target_config", conflicts_with = "resume")]
    pub prepare_only: bool,
    #[arg(long, conflicts_with_all = ["target_config", "prepare_only"])]
    pub resume: Option<PathBuf>,
    /// Leave a committed decision pending for a separate resume invocation.
    #[arg(long, value_enum)]
    pub stop_after: Option<StopAfter>,
}
#[derive(Clone, Copy, PartialEq, ValueEnum)]
pub enum StopAfter {
    Question,
    SetupReview,
    TestApproval,
    ApplicationApproval,
    ProjectVerificationApproval,
}

pub const GOAL: &str = "Repair add in this JavaScript project so its documented numeric contracts pass. Inspect the project to discover the setup. Before proposing setup, ask me which public API compatibility requirement to preserve. Keep documentation, package metadata and tests unchanged. Only src/math.mjs needs editing. Run the project's real verification and return a verified patch.";
pub const ANSWER: &str = "Keep the exported add(left, right) API unchanged. Support numeric inputs including negatives and zero. Node is available as node; verify with node contracts.mjs. Edit only src/math.mjs; preserve all tests and project documentation.";
pub const FOLLOWUP: &str = "Recheck the repaired add implementation in this same editable copy. Read src/math.mjs and run the configured tests again, then report the verified result. Make no additional edits; preserve the previous repair and API compatibility answer.";
const OLD: &str = "export function add(left, right) { return left - right; }\n";
const NEW: &str = "export function add(left, right) { return left + right; }\n";
const FIXTURE: &[(&str, &str)] = &[
    (
        "README.md",
        "# Numeric API\n\nExport add(left, right) from src/math.mjs. It must add numbers, including negatives, zero and fractions. Ask the user for the public API compatibility requirement before proposing changes. Verification: node contracts.mjs (Node built-in assertions; no install). Edit only src/math.mjs. Keep contracts.mjs, README.md and package.json unchanged.\n",
    ),
    (
        "package.json",
        "{\"name\":\"auto-conversation-fixture\",\"private\":true,\"type\":\"module\",\"scripts\":{\"test\":\"node contracts.mjs\"}}\n",
    ),
    ("src/math.mjs", OLD),
    (
        "contracts.mjs",
        "import assert from 'node:assert/strict';\nimport { add } from './src/math.mjs';\nfor (const [left, right, expected] of [[2,3,5],[-4,-7,-11],[0,3,3],[5,-2,3],[0.5,0.25,0.75]]) { assert.equal(add(left,right), expected); }\nconsole.log('5 contracts passed');\n",
    ),
];
fn read(path: impl AsRef<Path>) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn save(path: impl AsRef<Path>, value: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
// Identities and exact decisions are published before HTTP, never regenerated on resume.
fn create(path: impl AsRef<Path>, value: &Value) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    Ok(())
}
async fn get(client: &Client, url: &str) -> Result<Value> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}
async fn post(client: &Client, url: &str, body: &Value) -> Result<Value> {
    let response = client.post(url).json(body).send().await?;
    let status = response.status();
    let saved: Value = response.json().await?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {saved}").into());
    }
    Ok(saved)
}
struct App {
    engine: Engine,
    url: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::result::Result<(), act0r::application::Error>>>,
}
impl App {
    async fn open(directory: &Path) -> Result<Self> {
        let (engine, agents) = local_app(directory.join("demo.db")).await?;
        let listener = http_api::bind("127.0.0.1:0".parse()?).await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(http_api::serve(
            listener,
            Arc::new(agents),
            ApiOptions::default(),
            async {
                let _ = stopped.await;
                Ok(())
            },
        ));
        Ok(Self {
            engine,
            url,
            stop: Some(stop),
            task: Some(task),
        })
    }
    async fn close(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
    fn turn(&self, r: &Value) -> String {
        format!(
            "{}/v1/harness/sessions/{}/turns/{}",
            self.url,
            r["session_id"].as_str().unwrap(),
            r["task_id"].as_str().unwrap()
        )
    }
    fn goal(&self, r: &Value) -> String {
        format!(
            "{}/v1/teams/{}/tasks/{}",
            self.url,
            r["team_id"].as_str().unwrap(),
            r["task_id"].as_str().unwrap()
        )
    }
    fn conversation(&self, r: &Value) -> String {
        format!(
            "{}/v1/harness/sessions/{}/conversation",
            self.url,
            r["session_id"].as_str().unwrap()
        )
    }
}
impl Drop for App {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Local fixture takes the same function-calling path; no runtime policies are replaced.
pub fn fixture_action(prompt: &Value) -> Value {
    let read = |path: &str| {
        let tools = prompt["tools"].as_array();
        let native =
            tools.is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "read_file"));
        if !native
            && let Some(tool) = tools.and_then(|tools| {
                tools
                    .iter()
                    .find(|tool| tool["name"] == "mcp_files_read_text_file")
            })
        {
            json!({"kind":tool["name"],"path":path})
        } else {
            json!({"kind":"read_file","path":path})
        }
    };
    let turn = prompt["turn"].as_u64().unwrap();
    let discovery = prompt["tools"].as_array().is_some_and(|tools| {
        tools
            .iter()
            .any(|t| t["action_kind"] == "propose_project_setup")
    }) || prompt["project_discovery"].is_object();
    if discovery {
        return match turn {
            0 => read("README.md"),
            1 => {
                json!({"kind":"ask_human","question":"Which public API compatibility requirement should be preserved?"})
            }
            _ => {
                json!({"kind":"propose_project_setup","proposal":{"summary":"README documents Node verification and a single writable module", "writable_paths":["src/math.mjs"], "test_command":{"argv":["node","contracts.mjs"],"timeout_ms":60000},"commands":[]}})
            }
        };
    }
    let followup = prompt["goal"]
        .as_str()
        .is_some_and(|g| g.starts_with("Recheck"));
    match (followup, turn) {
        (_, 0) => read("src/math.mjs"),
        (false, 1) => json!({"kind":"replace_text","path":"src/math.mjs","old":OLD,"new":NEW}),
        (false, 2) | (true, 1) => json!({"kind":"run_tests"}),
        _ => {
            json!({"kind":"finish","summary":"Preserved the numeric API; five real Node contracts passed."})
        }
    }
}
struct FixtureModel(JoinHandle<()>);
impl FixtureModel {
    async fn close(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}
impl Drop for FixtureModel {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn fixture_model(endpoint: Option<&str>) -> Result<(ModelTarget, FixtureModel)> {
    let address = match endpoint {
        Some(e) => e
            .strip_prefix("http://")
            .and_then(|e| e.strip_suffix("/v1/chat/completions"))
            .ok_or("invalid local fixture endpoint")?,
        None => "127.0.0.1:0",
    };
    let address: std::net::SocketAddr = address.parse()?;
    if !address.ip().is_loopback() {
        return Err("local fixture requires a loopback address".into());
    }
    // A saved fixture endpoint must be reusable across separate invocations on Windows.
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    let listener = socket.listen(16)?;
    let target = serde_json::from_value(
        json!({"provider":"openai","model":"auto-fixture","endpoint":format!("http://{}/v1/chat/completions",listener.local_addr()?),"protocol":"chat_completions","action_format":"function_calling"}),
    )?;
    let task = tokio::spawn(async move {
        let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(|axum::Json(body): axum::Json<Value>| async move {
            let prompt: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            let mut action = fixture_action(&prompt);
            let name = action.as_object_mut().unwrap().remove("kind").unwrap();
            // Discovered schemas can add nullable optional fields to the strict
            // function projection. The local provider must honor that wire contract.
            if let Some(parameters) = body["tools"].as_array().and_then(|tools| tools.iter().find(|tool| tool["function"]["name"] == name))
                .and_then(|tool| tool["function"]["parameters"]["properties"].as_object()) {
                for (field, schema) in parameters {
                    if action.get(field).is_none() && schema["anyOf"].as_array().is_some_and(|variants| variants.iter().any(|variant| variant["type"] == "null")) {
                        action[field] = Value::Null;
                    }
                }
            }
            axum::Json(json!({"choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","content":null,"tool_calls":[{"id":"auto-fixture-call","type":"function","function":{"name":name,"arguments":action.to_string()}}]}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))
        }));
        axum::serve(listener, app).await.unwrap();
    });
    Ok((target, FixtureModel(task)))
}
fn check_protected(directory: &Path, original: bool) -> Result<()> {
    for (path, contents) in FIXTURE {
        if (original || *path != "src/math.mjs")
            && fs::read_to_string(directory.join("project").join(path))? != *contents
        {
            return Err(format!("fixture changed unexpectedly: {path}").into());
        }
    }
    Ok(())
}
async fn pool(directory: &Path) -> Result<sqlx::SqlitePool> {
    Ok(sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new()
            .filename(directory.join("demo.db"))
            .read_only(true),
    )
    .await?)
}
async fn evidence(directory: &Path) -> Result<Value> {
    let db = pool(directory).await?;
    let runs: Vec<String> = sqlx::query_scalar("SELECT payload FROM runs ORDER BY id")
        .fetch_all(&db)
        .await?;
    let actors: Vec<String> = sqlx::query_scalar("SELECT payload FROM agent_actors ORDER BY id")
        .fetch_all(&db)
        .await?;
    let activities: Vec<String> = sqlx::query_scalar("SELECT payload FROM activities ORDER BY id")
        .fetch_all(&db)
        .await?;
    let history: Vec<String> =
        sqlx::query_scalar("SELECT payload FROM history ORDER BY run_id,sequence")
            .fetch_all(&db)
            .await?;
    db.close().await;
    let parse = |values: Vec<String>| -> Result<Vec<Value>> {
        values
            .into_iter()
            .map(|v| Ok(serde_json::from_str(&v)?))
            .collect()
    };
    Ok(
        json!({"runs":parse(runs)?,"actors":parse(actors)?,"activities":parse(activities)?,"history":parse(history)?}),
    )
}
fn calls(saved: &Value) -> u64 {
    saved["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["usage"]["admitted_model_calls"].as_u64().unwrap_or(0))
        .sum()
}
fn export_requests(directory: &Path, saved: &Value) -> Result<usize> {
    let mut count = 0;
    for activity in saved["activities"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["spec"]["handler"]["name"] == "agent.model")
    {
        let request: ModelRequest = serde_json::from_value(activity["spec"]["input"].clone())?;
        save(
            directory.join(format!("request-{}.json", activity["id"].as_str().unwrap())),
            &ModelRouter::request_body(&request),
        )?;
        count += 1;
    }
    Ok(count)
}
async fn restart(
    app: &mut App,
    directory: &Path,
    client: &Client,
    root: &Value,
    label: &str,
) -> Result<()> {
    let before = evidence(directory).await?;
    let view = get(client, &app.conversation(root)).await?;
    if let Some(stop) = app.stop.take() {
        let _ = stop.send(());
    }
    if let Some(task) = app.task.take() {
        task.await??;
    }
    *app = App::open(directory).await?;
    let after = evidence(directory).await?;
    if before != after || view != get(client, &app.conversation(root)).await? {
        return Err(
            "saved actors, Runs, requests, usage or conversation changed on restart".into(),
        );
    }
    save(
        directory.join(format!("restart-{label}.json")),
        &json!({"verified":true,"model_calls":calls(&before),"digest":format!("{:x}",Sha256::digest(serde_json::to_vec(&before)?)),"conversation":view}),
    )?;
    eprintln!(
        "Restart verified at {label} ({} model calls retained).",
        calls(&before)
    );
    Ok(())
}
enum Checkpoint {
    Prepared,
    Complete,
    Pending(Value),
}
async fn drive(
    app: &App,
    directory: &Path,
    client: &Client,
    worker: &mut Worker,
    root: &Value,
    receipt: &Value,
    prepare: bool,
) -> Result<Checkpoint> {
    let deadline = Instant::now() + Duration::from_secs(1800);
    for _ in 0..1000 {
        let saved = evidence(directory).await?;
        export_requests(directory, &saved)?;
        let task: MessageId = receipt["task_id"].as_str().ok_or("missing task")?.parse()?;
        let view = get(client, &app.conversation(root)).await?;
        save(directory.join("conversation.json"), &view)?;
        let inbox = get(client, &format!("{}/inbox", app.goal(receipt))).await?;
        if let Some(item) = inbox["items"].as_array().ok_or("missing inbox")?.first() {
            return Ok(Checkpoint::Pending(item.clone()));
        }
        if view["turns"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["task_id"] == json!(task) && t["settled"] == true)
        {
            let turn = view["turns"]
                .as_array()
                .unwrap()
                .iter()
                .find(|t| t["task_id"] == json!(task))
                .unwrap();
            if turn["status"] != "succeeded" {
                return Err(format!("turn stopped without verification: {turn}").into());
            }
            return Ok(Checkpoint::Complete);
        }
        if Instant::now() >= deadline {
            return Err("trial time guard reached; saved work retained".into());
        }
        if worker.advance_work().await? || worker.fire_due_timer().await? {
            continue;
        }
        // Stop before admission. Runtime budgets stay unlimited; this bounds only the demo.
        let queued_model = saved["activities"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["spec"]["handler"]["name"] == "agent.model" && a["status"] == "queued");
        if let Some(queued_model) = queued_model {
            if prepare {
                if calls(&saved) != 0 {
                    return Err("preparation admitted model work".into());
                }
                let request: ModelRequest =
                    serde_json::from_value(queued_model["spec"]["input"].clone())?;
                save(
                    directory.join("first-request.json"),
                    &ModelRouter::request_body(&request),
                )?;
                return Ok(Checkpoint::Prepared);
            }
            if calls(&saved) >= 40 {
                return Err("trial stopped before a provider call after 40 admitted calls; runtime budgets and actors retained".into());
            }
        }
        if let Some(execution) = worker.admit().await? {
            eprintln!("{} · Run {}", execution.handler.name, execution.run_id);
            let outcome = match worker.execute(execution.clone()).await {
                Ok(outcome) => outcome,
                Err(error) => act0r::domain::Outcome::Uncertain(format!("executor error: {error}")),
            };
            worker.complete(execution, outcome).await?;
        } else {
            let retry =
                saved["activities"].as_array().unwrap().iter().any(|a| {
                    a["status"] == "queued" && a["ready_at"].as_i64().is_some_and(|t| t > 0)
                });
            if !retry {
                return Err(
                    "trial is idle and needs explicit recovery; inspect evidence.json".into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    Err("trial activity guard reached; saved work retained".into())
}
async fn answer(
    app: &App,
    directory: &Path,
    client: &Client,
    receipt: &Value,
    item: &Value,
) -> Result<()> {
    if item["actionable"] != true {
        return Err("saved decision is not actionable".into());
    }
    let body = match item["kind"].as_str() {
        Some("question") => json!({"kind":"answer","answer":ANSWER}),
        Some("tool_approval" | "child_import_approval") => {
            json!({"kind":"approval","approved":true})
        }
        _ => return Err("unsupported wait needs explicit user input; trial retained".into()),
    };
    let wait = item["wait"]["id"].as_str().ok_or("missing wait")?;
    let path = directory.join(format!("decision-{wait}.json"));
    if item["kind"] == "question" && !path.exists() {
        let answered = fs::read_dir(directory)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("decision-"))
            .map(|entry| read(entry.path()))
            .collect::<Result<Vec<_>>>()?
            .iter()
            .filter(|v| v["item"]["kind"] == "question" && v["item"]["task_id"] == item["task_id"])
            .count();
        if answered >= 2 {
            return Err("repeated questions need explicit guidance; saved work retained".into());
        }
    }
    let journal = json!({"receipt":receipt,"item":item,"body":body});
    if path.exists() {
        if read(&path)? != journal {
            return Err("changed saved decision".into());
        }
    } else {
        create(&path, &journal)?;
    }
    let url = format!("{}/waits/{wait}/responses", app.goal(receipt));
    post(client, &url, &body).await?;
    if post(client, &url, &body).await?["replayed"] != true {
        return Err("duplicate answer did not replay".into());
    }
    Ok(())
}
async fn verified(app: &App, client: &Client, receipt: &Value) -> Result<Value> {
    let view = get(client, &format!("{}/changes", app.turn(receipt))).await?;
    if view["completion"]["status"] != "verified"
        || view["changes"]["test_status"] != "matching"
        || view["changes"]["tests"]["result"]["ok"] != true
        || !view["changes"]["tests"]["result"]["stdout"]
            .as_str()
            .is_some_and(|s| s.contains("5 contracts passed"))
    {
        return Err("turn lacks actual fresh passing Node verification".into());
    }
    Ok(view)
}
fn report(directory: &Path, status: &str, phase: &str) -> Value {
    json!({"status":status,"phase":phase,"directory":directory,"database":directory.join("demo.db")})
}
async fn node_tests(directory: &Path, log: &str) -> Result<bool> {
    let mut command = tokio::process::Command::new("node");
    command
        .arg("contracts.mjs")
        .current_dir(directory.join("project"))
        .kill_on_drop(true);
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .ends_with("_API_KEY")
        {
            command.env_remove(name);
        }
    }
    #[cfg(windows)]
    command.creation_flags(0x08000000);
    let output = tokio::time::timeout(Duration::from_secs(60), command.output()).await??;
    fs::write(
        directory.join(log),
        [&output.stdout[..], &output.stderr[..]].concat(),
    )?;
    Ok(output.status.success())
}

pub async fn run(options: Options) -> Result<Value> {
    let directory = if let Some(path) = &options.resume {
        fs::canonicalize(path)?
    } else {
        let path = options.output.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(&path)?;
        fs::canonicalize(path)?
    };
    let mut model = None;
    if options.resume.is_none() {
        for (path, contents) in FIXTURE {
            let file = directory.join("project").join(path);
            fs::create_dir_all(file.parent().unwrap())?;
            fs::write(file, contents)?;
        }
        if node_tests(&directory, "baseline-tests.log").await? {
            return Err("the original addition fixture must fail its real verifier".into());
        }
        let target: ModelTarget = if let Some(config) = &options.target_config {
            let mut target = read(config)?["target"].clone();
            target["action_format"] = json!("function_calling");
            serde_json::from_value(target)?
        } else {
            let (target, fixture) = fixture_model(None).await?;
            model = Some(fixture);
            target
        };
        create(
            directory.join("manifest.json"),
            &json!({"version":1,"mode":if model.is_some(){"local"}else{"live"},"source":directory.join("project"),"target":target}),
        )?;
        create(
            directory.join("session-request.json"),
            &json!({"message_id":MessageId::new(),"prompt":GOAL,"workspace":directory.join("project"),"discover_project":true,"target":target}),
        )?;
    }
    let manifest = read(directory.join("manifest.json"))?;
    let request = read(directory.join("session-request.json"))?;
    if manifest["version"] != 1
        || manifest["source"] != json!(directory.join("project"))
        || request["workspace"] != manifest["source"]
        || request["target"] != manifest["target"]
        || request["prompt"] != GOAL
        || request["discover_project"] != true
        || request.get("coding").is_some()
        || request.get("budget").is_some()
    {
        return Err("resume refused changed fixture/model/request pins".into());
    }
    check_protected(&directory, false)?;
    if manifest["mode"] == "local" && model.is_none() {
        let (_, fixture) = fixture_model(manifest["target"]["endpoint"].as_str()).await?;
        model = Some(fixture);
    }
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(40))
        .build()?;
    let mut app = App::open(&directory).await?;
    let root = post(
        &client,
        &format!("{}/v1/harness/sessions", app.url),
        &request,
    )
    .await?;
    if directory.join("session.json").exists() {
        if read(directory.join("session.json"))? != root {
            return Err("changed root receipt".into());
        }
    } else {
        create(directory.join("session.json"), &root)?;
    }
    eprintln!("Auto trial: {}", directory.display());
    let result = execute(&options, &directory, &client, &mut app, &root).await;
    let saved = evidence(&directory).await?;
    export_requests(&directory, &saved)?;
    save(directory.join("evidence.json"), &saved)?;
    let mut outcome = match &result {
        Ok(v) => v.clone(),
        Err(e) => {
            let mut v = report(&directory, "needs_inspection", "stopped");
            v["error"] = json!(e.to_string());
            v
        }
    };
    outcome["mode"] = manifest["mode"].clone();
    outcome["model_calls"] = json!(calls(&saved));
    save(directory.join("report.json"), &outcome)?;
    app.close().await?;
    if let Some(model) = model {
        model.close().await;
    }
    result.map(|_| outcome)
}

async fn execute(
    options: &Options,
    directory: &Path,
    client: &Client,
    app: &mut App,
    root: &Value,
) -> Result<Value> {
    let mut receipt = root.clone();
    loop {
        let phase = if receipt["session_id"] == root["session_id"] {
            "discovery"
        } else {
            "coding"
        };
        let mut worker = app.engine.worker().await?;
        let point = drive(
            app,
            directory,
            client,
            &mut worker,
            root,
            &receipt,
            options.prepare_only,
        )
        .await?;
        drop(worker);
        match point {
            Checkpoint::Prepared => return Ok(report(directory, "prepared", "first_request")),
            Checkpoint::Pending(item) => {
                let question = phase == "discovery" && item["kind"] == "question";
                let test = item["kind"] == "tool_approval"
                    && item["wait"]["payload"].to_string().contains("run_tests");
                let label = if question {
                    Some("question")
                } else if test {
                    Some("test")
                } else {
                    None
                };
                if let Some(label) = label
                    && !directory.join(format!("restart-{label}.json")).exists()
                {
                    restart(app, directory, client, root, label).await?;
                    if (question && options.stop_after == Some(StopAfter::Question))
                        || (test && options.stop_after == Some(StopAfter::TestApproval))
                    {
                        return Ok(report(directory, "checkpoint", label));
                    }
                }
                answer(app, directory, client, &receipt, &item).await?;
            }
            Checkpoint::Complete if phase == "discovery" => {
                let path = format!("{}/project-setup", app.turn(root));
                let view = get(client, &path).await?;
                save(directory.join("setup-review.json"), &view)?;
                if view["proposal"].is_null() {
                    return Err("discovery completed without the required setup proposal".into());
                }
                if options.stop_after == Some(StopAfter::SetupReview)
                    && !directory.join("coding.json").exists()
                {
                    return Ok(report(directory, "checkpoint", "setup_review"));
                }
                let proposal: ProjectSetupProposal =
                    serde_json::from_value(view["proposal"].clone())?;
                let mut coding = proposal.coding()?;
                if coding.writable_paths != ["src/math.mjs"]
                    || coding.test_command.argv != ["node", "contracts.mjs"]
                {
                    return Err("proposed setup differs from fixture's allowed scope/Node command; inspect saved review".into());
                }
                // Review narrows optional suggestions to the fixture's single known
                // verifier. Named commands are unnecessary for this repair.
                coding.project_commands.clear();
                coding.test_command.timeout_ms = 60000;
                let body = json!({"coding":coding});
                let journal = directory.join("setup-decision.json");
                if journal.exists() {
                    if read(&journal)? != body {
                        return Err("changed reviewed setup".into());
                    }
                } else {
                    create(&journal, &body)?;
                }
                receipt = post(client, &path, &body).await?;
                if post(client, &path, &body).await? != receipt {
                    return Err("setup acceptance duplicated coding".into());
                }
                if directory.join("coding.json").exists() {
                    if read(directory.join("coding.json"))? != receipt {
                        return Err("changed coding receipt".into());
                    }
                } else {
                    create(directory.join("coding.json"), &receipt)?;
                }
                let combined = get(client, &app.conversation(root)).await?;
                if combined["session_id"] != root["session_id"] {
                    return Err("Auto lost canonical identity".into());
                }
            }
            Checkpoint::Complete => break,
        }
    }
    let coding = receipt;
    let changes = verified(app, client, &coding).await?;
    let files = changes["changes"]["files"]
        .as_array()
        .ok_or("missing patch")?;
    if files.len() != 1 || files[0]["path"] != "src/math.mjs" {
        return Err("repair patch differs from the one allowed module".into());
    }
    save(directory.join("coding-verification.json"), &changes)?;
    if let Some(checkpoint) = apply(options, directory, client, app, root, &coding).await? {
        return Ok(checkpoint);
    }
    let project_verification =
        verify_applied(options, directory, client, app, root, &coding).await?;
    if project_verification["status"] == "checkpoint" {
        return Ok(project_verification);
    }
    check_protected(directory, false)?;
    if !node_tests(directory, "applied-tests.log").await? {
        return Err("independent verification of applied project failed".into());
    }
    let applied_bytes = fs::read(directory.join("project/src/math.mjs"))?;
    let followup_path = directory.join("followup-request.json");
    if !followup_path.exists() {
        create(
            &followup_path,
            &json!({"message_id":MessageId::new(),"prompt":FOLLOWUP}),
        )?;
    }
    let followup = post(
        client,
        &format!(
            "{}/v1/harness/sessions/{}/turns",
            app.url,
            root["session_id"].as_str().unwrap()
        ),
        &read(&followup_path)?,
    )
    .await?;
    if followup["session_id"] != coding["session_id"] || followup["team_id"] != coding["team_id"] {
        return Err("follow-up did not reuse coding actors".into());
    }
    loop {
        let mut worker = app.engine.worker().await?;
        let point = drive(app, directory, client, &mut worker, root, &followup, false).await?;
        drop(worker);
        match point {
            Checkpoint::Complete => break,
            Checkpoint::Pending(item) => answer(app, directory, client, &followup, &item).await?,
            _ => return Err("unexpected follow-up preparation".into()),
        }
    }
    let followup_changes = verified(app, client, &followup).await?;
    check_protected(directory, false)?;
    if fs::read(directory.join("project/src/math.mjs"))? != applied_bytes {
        return Err("follow-up unexpectedly changed the applied source".into());
    }
    if followup_changes["changes"]["files"]
        .as_array()
        .is_none_or(|f| !f.is_empty())
        || followup_changes["changes"]["tests"]["activity_id"]
            == changes["changes"]["tests"]["activity_id"]
    {
        return Err("follow-up needs new tests and no extra edits".into());
    }
    save(
        directory.join("followup-verification.json"),
        &followup_changes,
    )?;
    // Final reopening must perform no work or reapply a patch.
    restart(app, directory, client, root, "completed").await?;
    let view = get(client, &app.conversation(root)).await?;
    let list = get(client, &format!("{}/v1/harness/conversations", app.url)).await?;
    let applications = get(client, &format!("{}/applications", app.turn(&coding))).await?;
    if applications["items"]
        .as_array()
        .is_none_or(|items| items.len() != 1)
        || applications["items"][0]["run_id"]
            != read(directory.join("application-request.json"))?["application_id"]
    {
        return Err("trial must retain exactly one original application".into());
    }
    let saved = evidence(directory).await?;
    // Check the actual requests sent to the coding planner and follow-up, rather
    // than accepting the transcript alone as proof of a model-context handoff.
    let carried = [&changes["run_id"], &followup_changes["run_id"]]
        .iter()
        .all(|run| {
            saved["activities"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|a| a["spec"]["handler"]["name"] == "agent.model" && &a["run_id"] == *run)
                .filter_map(|a| {
                    serde_json::from_str::<Value>(a["spec"]["input"]["prompt"].as_str()?).ok()
                })
                .any(|p| {
                    p["discovery_context"]["evidence"]["clarifications"]
                        .as_array()
                        .is_some_and(|q| q.iter().any(|q| q["answer"]["text"] == ANSWER))
                })
        });
    if !carried {
        return Err("coding/follow-up model requests lost the saved compatibility answer".into());
    }
    if view["stage"] != "completed"
        || view["turns"].as_array().is_none_or(|t| t.len() != 3)
        || list["items"].as_array().is_none_or(|i| i.len() != 1)
        || list["items"][0]["session_id"] != root["session_id"]
        || view["turns"][0]["clarifications"]
            .as_array()
            .is_none_or(|q| !q.iter().any(|q| q["answer"]["text"] == ANSWER))
    {
        return Err("final grouped history/answer/stage was not preserved".into());
    }
    let mut outcome = report(directory, "verified", "completed");
    outcome["session"] = root.clone();
    outcome["coding"] = coding;
    outcome["followup"] = followup;
    outcome["conversation"] = view;
    outcome["applied_tests_ok"] = json!(true);
    outcome["application"] = read(directory.join("application.json"))?;
    outcome["project_verification"] = project_verification;
    outcome["discovery_answer_carried"] = json!(true);
    outcome["contracts_per_verification"] = json!(5);
    let mut restarts = Vec::new();
    for label in [
        "question",
        "test",
        "application",
        "project-verification",
        "completed",
    ] {
        let saved = read(directory.join(format!("restart-{label}.json")))?;
        if saved["verified"] != true {
            return Err("required restart evidence is missing".into());
        }
        restarts.push(json!({"phase":label,"verified":true,"model_calls":saved["model_calls"]}));
    }
    outcome["restarts"] = json!(restarts);
    Ok(outcome)
}

async fn verify_applied(
    options: &Options,
    directory: &Path,
    client: &Client,
    app: &mut App,
    root: &Value,
    coding: &Value,
) -> Result<Value> {
    let application = read(directory.join("application-request.json"))?["application_id"]
        .as_str()
        .ok_or("missing application")?
        .to_owned();
    let journal = directory.join("project-verification-request.json");
    if !journal.exists() {
        create(
            &journal,
            &json!({"verification_id":act0r::domain::RunId::new()}),
        )?;
    }
    let body = read(&journal)?;
    let id = body["verification_id"]
        .as_str()
        .ok_or("missing verification")?;
    post(
        client,
        &format!(
            "{}/applications/{application}/verifications",
            app.turn(coding)
        ),
        &body,
    )
    .await?;
    for _ in 0..40 {
        let url = format!(
            "{}/applications/{application}/verifications/{id}",
            app.turn(coding)
        );
        let view = get(client, &url).await?;
        match view["status"].as_str() {
            Some("succeeded") if view["report"]["status"] == "verified" => {
                save(directory.join("project-verification.json"), &view)?;
                return Ok(view);
            }
            Some("waiting") => {
                if !directory.join("restart-project-verification.json").exists() {
                    restart(app, directory, client, root, "project-verification").await?;
                    if options.stop_after == Some(StopAfter::ProjectVerificationApproval) {
                        return Ok(report(
                            directory,
                            "checkpoint",
                            "project_verification_approval",
                        ));
                    }
                }
                let decision = json!({"wait_id":view["wait"]["id"],"approved":true});
                let path = directory.join("project-verification-decision.json");
                if path.exists() {
                    if read(&path)? != decision {
                        return Err("changed verification decision".into());
                    }
                } else {
                    create(&path, &decision)?;
                }
                let url = format!(
                    "{}/applications/{application}/verifications/{id}/responses",
                    app.turn(coding)
                );
                post(client, &url, &decision).await?;
                post(client, &url, &decision).await?;
            }
            Some("failed" | "blocked" | "cancelled" | "paused" | "succeeded") => {
                return Err(
                    "applied project verification did not pass; inspect its saved result".into(),
                );
            }
            _ => {}
        }
        let mut worker = app.engine.worker().await?;
        if !worker.step().await? {
            return Err("project verification is idle before completion".into());
        }
    }
    Err("project verification exceeded its activity guard".into())
}

async fn apply(
    options: &Options,
    directory: &Path,
    client: &Client,
    app: &mut App,
    root: &Value,
    coding: &Value,
) -> Result<Option<Value>> {
    let journal = directory.join("application-request.json");
    if !journal.exists() {
        check_protected(directory, true)?;
        create(
            &journal,
            &json!({"application_id":act0r::domain::RunId::new()}),
        )?;
    }
    let request = read(&journal)?;
    let base = format!("{}/applications", app.turn(coding));
    post(client, &base, &request).await?;
    for _ in 0..50 {
        let url = format!(
            "{}/applications/{}",
            app.turn(coding),
            request["application_id"]
                .as_str()
                .ok_or("missing application identity")?
        );
        let view = get(client, &url).await?;
        match view["status"].as_str() {
            Some("succeeded") if view["report"]["status"] == "applied" => {
                save(directory.join("application.json"), &view)?;
                return Ok(None);
            }
            Some("waiting") => {
                check_protected(directory, true)?;
                if !directory.join("restart-application.json").exists() {
                    restart(app, directory, client, root, "application").await?;
                    if options.stop_after == Some(StopAfter::ApplicationApproval) {
                        return Ok(Some(report(
                            directory,
                            "checkpoint",
                            "application_approval",
                        )));
                    }
                }
                let body = json!({"wait_id":view["wait"]["id"],"approved":true});
                let decision = directory.join("application-decision.json");
                if decision.exists() {
                    if read(&decision)? != body {
                        return Err("changed application decision".into());
                    }
                } else {
                    create(&decision, &body)?;
                }
                // The listener changes on restart; regenerate the URL from the pinned physical receipt.
                let url = format!(
                    "{}/applications/{}/responses",
                    app.turn(coding),
                    request["application_id"].as_str().unwrap()
                );
                post(client, &url, &body).await?;
                post(client, &url, &body).await?;
            }
            Some("failed" | "blocked" | "cancelled" | "paused") => {
                return Err("application needs explicit recovery; not retried".into());
            }
            _ => {}
        }
        let mut worker = app.engine.worker().await?;
        if !worker.step().await? {
            return Err("application idle before completion".into());
        }
    }
    Err("application activity guard reached".into())
}
