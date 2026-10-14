//! Example-only orchestration. All workflow decisions go through the installed harness.
use act0r::{
    Engine, Worker,
    adapters::http_api::{self, ApiOptions},
    application::AgentService,
    bootstrap::local_app,
    domain::{
        MessageId, RunId, RunStatus, WaitId,
        agent::{ModelRequest, ModelTarget},
    },
};
use clap::{Parser, ValueEnum};
use reqwest::Client;
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::oneshot, task::JoinHandle};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[command(about = "Two dependent coding workers, real Rust tests and two durable restarts")]
pub struct Options {
    /// Parent for a fresh, isolated demo directory. Previous runs are retained.
    #[arg(long, default_value = ".build/dependent-coding-demo")]
    pub output: PathBuf,
    /// Opt into a real model using the target in an existing agent JSON configuration.
    #[arg(long)]
    pub target_config: Option<PathBuf>,
    /// Continue an existing demo folder with its original database and model pins.
    #[arg(long, conflicts_with = "target_config")]
    pub resume: Option<PathBuf>,
    /// Exact saved guidance wait to answer. Ordinary questions are not guidance.
    #[arg(long, requires_all = ["resume", "guidance_file"])]
    pub wait_id: Option<WaitId>,
    /// UTF-8 guidance for only --wait-id; never reused for a later wait.
    #[arg(long, requires_all = ["resume", "wait_id"])]
    pub guidance_file: Option<PathBuf>,
    /// Exit at a committed approval so another invocation can demonstrate recovery.
    #[arg(long, value_enum)]
    pub stop_after: Option<StopAfter>,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub enum StopAfter {
    TransferApproval,
    ApplicationApproval,
}

const API_GOAL: &str = "Repair src/api.rs display_name. Trim both names, join nonempty parts with one space, use Anonymous when both are empty. Start by requesting read_file with path src/api.rs, then request replace_text using that saved file, run_tests and finish. Use the supplied action schema. The runtime obtains the edit and test approvals. Edit only src/api.rs.";
const CONSUMER_GOAL: &str = "After the approved API transfer, read src/api.rs and replace duplicated name formatting in src/consumer.rs with crate::api::display_name(user). Preserve greeting output and run the fixed tests. Your final patch includes the transferred API and the consumer change.";
const PROMPT: &str = "Use exactly one delegate_tasks group with tasks in this order: coder, then coder2. Assign coder the consumer refactor in src/consumer.rs, with writable_paths [src/api.rs, src/consumer.rs] and depends_on coder2. Assign coder2 the API repair in src/api.rs, with writable_paths [src/api.rs]. The API display_name must trim both names, join nonempty parts with one space, and use Anonymous for an empty name. The consumer must replace duplicated formatting with crate::api::display_name(user), preserving its output. Do not edit Cargo.toml, Cargo.lock or tests. When the API finishes, inspect_worker_changes for receiving worker coder, import_worker_changes with approval, explicitly resume_worker coder and guide_workers. After both finish, inspect and import only coder's consolidated patch once, then run fresh parent tests and finish. Do not import coder2's patch separately.";
const API_OLD: &str = "    user.first.trim().to_owned()";
const API_NEW: &str = "    let parts: Vec<_> = [user.first.trim(), user.last.trim()]\n        .into_iter()\n        .filter(|part| !part.is_empty())\n        .collect();\n    if parts.is_empty() {\n        \"Anonymous\".to_owned()\n    } else {\n        parts.join(\" \")\n    }";
const CONSUMER_OLD: &str = "    let parts: Vec<_> = [user.first.trim(), user.last.trim()]\n        .into_iter()\n        .filter(|part| !part.is_empty())\n        .collect();\n    let name = if parts.is_empty() {\n        \"Anonymous\".to_owned()\n    } else {\n        parts.join(\" \")\n    };";
const CONSUMER_NEW: &str = "    let name = crate::api::display_name(user);";
const FIXTURE: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        include_str!("../dependent-coding-project/Cargo.toml"),
    ),
    (
        "Cargo.lock",
        include_str!("../dependent-coding-project/Cargo.lock"),
    ),
    (
        "src/lib.rs",
        include_str!("../dependent-coding-project/src/lib.rs"),
    ),
    (
        "src/api.rs",
        include_str!("../dependent-coding-project/src/api.rs"),
    ),
    (
        "src/consumer.rs",
        include_str!("../dependent-coding-project/src/consumer.rs"),
    ),
    (
        "tests/contracts.rs",
        include_str!("../dependent-coding-project/tests/contracts.rs"),
    ),
];

fn tool(name: &str, arguments: Value) -> Value {
    json!({"kind":"call_tool","tool":name,"arguments":arguments})
}

fn initial_action() -> Value {
    json!({"kind":"delegate_tasks","tasks":[
        {"worker":"coder","goal":CONSUMER_GOAL,"writable_paths":["src/api.rs","src/consumer.rs"],"depends_on":"coder2"},
        {"worker":"coder2","goal":API_GOAL,"writable_paths":["src/api.rs"]}
    ]})
}

pub(crate) fn scripted_action(p: &Value) -> Result<Value> {
    let action = canonical_scripted_action(p)?;
    if action["kind"] == "call_tool"
        && p["tools"].as_array().is_some_and(|tools| {
            tools
                .iter()
                .any(|tool| tool["action_kind"] == action["tool"])
        })
    {
        let mut fields = action["arguments"]
            .as_object()
            .ok_or("missing native arguments")?
            .clone();
        fields.insert("kind".into(), action["tool"].clone());
        return Ok(json!(fields));
    }
    Ok(action)
}

fn canonical_scripted_action(p: &Value) -> Result<Value> {
    let turn = p["turn"].as_u64().ok_or("missing model turn")?;
    if p["coordinating_guidance"].is_object() {
        if p["coordinating_guidance"]["dependencies"]
            .as_array()
            .is_none_or(|d| d.len() != 1)
        {
            return Err("expected one exact dependency review".into());
        }
        if p["worker_changes"]["inspections_used"] == 0 {
            return Ok(json!({"kind":"inspect_worker_changes","worker":"coder","offset":0}));
        }
        if p["worker_changes"]["imports_used"] == 0 {
            return Ok(json!({"kind":"import_worker_changes","worker":"coder"}));
        }
        if p["worker_pausing"]["used"] == 0 {
            return Ok(tool(
                "resume_worker",
                json!({"worker":"coder","reason":"The approved API patch is available. Read it and verify the consumer."}),
            ));
        }
        return Ok(tool(
            "guide_workers",
            json!({"coder":"Read the transferred API, refactor the consumer, then run fresh full tests.","coder2":null}),
        ));
    }
    if p["goal"] == API_GOAL {
        return Ok(match turn {
            0 => tool("read_file", json!({"path":"src/api.rs"})),
            1 => tool(
                "replace_text",
                json!({"path":"src/api.rs","old":API_OLD,"new":API_NEW}),
            ),
            2 => tool("run_tests", json!({})),
            3 => {
                json!({"kind":"finish","summary":"API repaired; all eight contract tests passed."})
            }
            _ => return Err("unexpected API continuation".into()),
        });
    }
    if p["goal"] == CONSUMER_GOAL {
        return Ok(match turn {
            0 => tool("read_file", json!({"path":"src/api.rs"})),
            1 => tool(
                "replace_text",
                json!({"path":"src/consumer.rs","old":CONSUMER_OLD,"new":CONSUMER_NEW}),
            ),
            2 => tool("run_tests", json!({})),
            3 => {
                json!({"kind":"finish","summary":"Consumer uses the transferred API; all eight contract tests passed."})
            }
            _ => return Err("unexpected consumer continuation".into()),
        });
    }
    if turn == 0 {
        return Ok(initial_action());
    }
    let seen = |kind: &str| {
        p["observations"]
            .as_array()
            .is_some_and(|items| items.iter().any(|o| o["action"]["kind"] == kind))
    };
    if !seen("inspect_child_changes") {
        Ok(json!({"kind":"inspect_child_changes","worker":"coder","offset":0}))
    } else if !seen("import_child_changes") {
        Ok(json!({"kind":"import_child_changes","worker":"coder"}))
    } else if !p["observations"].as_array().is_some_and(|items| {
        items
            .iter()
            .any(|o| o["action"]["tool"] == "run_tests" || o["action"]["kind"] == "run_tests")
    }) {
        Ok(tool("run_tests", json!({})))
    } else {
        Ok(
            json!({"kind":"finish","summary":"One consolidated API/consumer patch verified by fresh parent tests."}),
        )
    }
}

struct ScriptedModel {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl ScriptedModel {
    async fn start() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/v1/chat/completions", listener.local_addr()?);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let task = tokio::spawn(async move {
            let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let result = (|| -> Result<Value> {
                    let prompt: Value = serde_json::from_str(body["messages"][1]["content"].as_str().ok_or("missing prompt")?)?;
                    let action = scripted_action(&prompt)?;
                    seen.lock().map_err(|_| "model request lock poisoned")?.push(prompt);
                    Ok(json!({"id":"dependent-coding-script","choices":[{"finish_reason":"stop","message":{"content":json!({"action":action}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))
                })();
                let response = match result {
                    Ok(body) => (axum::http::StatusCode::OK, axum::Json(body)),
                    Err(error) => (axum::http::StatusCode::BAD_REQUEST, axum::Json(json!({"error":error.to_string()}))),
                };
                async move { response }
            }));
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            endpoint,
            requests,
            task,
        })
    }
}
impl Drop for ScriptedModel {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Api {
    url: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::result::Result<(), act0r::application::Error>>>,
}
impl Api {
    async fn start(agents: Arc<AgentService>) -> Result<Self> {
        let listener = http_api::bind("127.0.0.1:0".parse()?).await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(http_api::serve(
            listener,
            agents,
            ApiOptions::default(),
            async {
                let _ = stopped.await;
                Ok(())
            },
        ));
        Ok(Self {
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
}
impl Drop for Api {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
struct App {
    engine: Engine,
    agents: Arc<AgentService>,
    api: Api,
    directory: PathBuf,
}
impl App {
    async fn open(db: &Path) -> Result<Self> {
        let (engine, agents) = local_app(db).await?;
        let agents = Arc::new(agents);
        let api = Api::start(agents.clone()).await?;
        Ok(Self {
            engine,
            agents,
            api,
            directory: db.parent().ok_or("missing database folder")?.to_owned(),
        })
    }
    async fn close(self) -> Result<()> {
        self.api.close().await
    }
    fn turn(&self, r: &Value) -> String {
        format!(
            "{}/v1/harness/sessions/{}/turns/{}",
            self.api.url,
            r["session_id"].as_str().unwrap(),
            r["task_id"].as_str().unwrap()
        )
    }
    fn goal(&self, r: &Value) -> String {
        format!(
            "{}/v1/teams/{}/tasks/{}",
            self.api.url,
            r["team_id"].as_str().unwrap(),
            r["task_id"].as_str().unwrap()
        )
    }
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
fn id(value: &Value) -> Result<RunId> {
    Ok(value["run_id"]
        .as_str()
        .ok_or("missing Run identity")?
        .parse()?)
}
async fn reply(client: &Client, app: &App, r: &Value, item: &Value) -> Result<()> {
    if item["actionable"] != true {
        return Err("demo encountered an unavailable human decision".into());
    }
    let body = match item["kind"].as_str() {
        Some("question") => {
            json!({"kind":"answer","answer":"Proceed with the assigned repair in the isolated demo copy and run the fixed tests."})
        }
        Some("tool_approval" | "child_import_approval") => {
            json!({"kind":"approval","approved":true})
        }
        _ => return Err("demo encountered an unsupported wait".into()),
    };
    // Explicit invocation authorizes only this generated fixture's pinned edits,
    // Cargo tests and reviewed transfers. No session-wide grants are created.
    let url = format!(
        "{}/waits/{}/responses",
        app.goal(r),
        item["wait"]["id"].as_str().ok_or("missing wait")?
    );
    let first = post(client, &url, &body).await?;
    let duplicate = post(client, &url, &body).await?;
    assert_eq!(first["replayed"], false);
    assert_eq!(duplicate["replayed"], true);
    eprintln!(
        "Saved {} for {} (duplicate acknowledged once).",
        item["kind"].as_str().unwrap_or("decision"),
        item["member"].as_str().unwrap_or("assignment")
    );
    Ok(())
}

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    DependencyWait,
    TransferApproval,
    Complete,
}
async fn drive(
    client: &Client,
    app: &App,
    worker: &mut Worker,
    r: &Value,
    phase: Phase,
) -> Result<Value> {
    let until = Instant::now() + Duration::from_secs(1800);
    let mut steps = 0;
    let mut questions = std::collections::HashMap::<String, usize>::new();
    loop {
        if Instant::now() >= until || steps >= 1000 {
            break;
        }
        let task = app
            .agents
            .actors()?
            .task(r["task_id"].as_str().ok_or("missing task")?.parse()?)
            .await?;
        let work = get(client, &format!("{}/delegated-work", app.turn(r))).await?;
        let children = work["groups"][0]["children"].as_array();
        let inbox = get(client, &format!("{}/inbox", app.goal(r))).await?;
        let items = inbox["items"].as_array().ok_or("missing inbox items")?;
        // Example-only provider-call guard; the saved runtime budgets stay unlimited.
        for run in std::iter::once(task.run_id)
            .chain(children.into_iter().flatten().map(|c| id(c).ok()))
            .flatten()
        {
            let saved = app.engine.status(run).await?;
            if saved.record().usage.admitted_model_calls >= 24
                && saved.activities().iter().any(|a| {
                    a.spec.handler.name == "agent.model"
                        && a.status == act0r::domain::ActivityStatus::Queued
                })
            {
                save(
                    &app.directory.join("failed-checkpoint.json"),
                    &json!({"reason":"model_call_guard","run_id":run,"model_calls":saved.record().usage.admitted_model_calls,"delegated_work":work,"inbox":inbox,"task":task}),
                )?;
                return Err("demo stopped before another model call after 24 calls in one role; runtime budgets and saved work are retained".into());
            }
        }
        if let Some(parent_run) = task.run_id {
            let parent = app.engine.status(parent_run).await?;
            let delegation_proposals: Vec<_> = parent
                .activities()
                .iter()
                .filter(|a| {
                    a.spec.handler.name == "agent.model"
                        && a.result.as_ref().is_some_and(|result| {
                            matches!(
                                result["action"]["kind"].as_str(),
                                Some("delegate_task" | "delegate_tasks")
                            )
                        })
                })
                .collect();
            if parent.record().task_groups.len() > 1
                || parent.record().delegations.len() > 2
                || (!parent.record().delegations.is_empty() && delegation_proposals.len() > 1)
            {
                save(
                    &app.directory.join("failed-checkpoint.json"),
                    &json!({"reason":"repeated_delegation","delegated_work":work,"inbox":inbox,"task":task,
                    "delegation_proposals":delegation_proposals.len(),
                    "latest_delegation":delegation_proposals.last().and_then(|a|a.result.as_ref())}),
                )?;
                return Err("demo stopped after repeated delegation; inspect the saved planner decision before admitting more work".into());
            }
            if phase == Phase::DependencyWait && children.is_some_and(|c| c.len() == 2) {
                let declarations = &parent.record().delegations;
                if declarations.len() != 2
                    || declarations[0]
                        .initial_dependency
                        .as_ref()
                        .is_none_or(|d| d.prerequisite_task_id != declarations[1].id)
                {
                    save(
                        &app.directory.join("failed-checkpoint.json"),
                        &json!({"reason":"missing_initial_dependency","delegated_work":work,"inbox":inbox,"task":task}),
                    )?;
                    return Err("the planner omitted the structured depends_on declaration; no coder model work was admitted by the demo".into());
                }
            }
        }
        if phase == Phase::DependencyWait
            && children.is_some_and(|c| {
                c.len() == 2
                    && c[0]["pause"]["planner_hold"]["paused"] == true
                    && c[0]["run_id"].is_string()
                    && items.iter().any(|item| item["task_id"] == c[1]["task_id"])
            })
        {
            return Ok(work);
        }
        if phase == Phase::TransferApproval
            && items
                .iter()
                .any(|item| item["wait"]["payload"]["worker_transfer"] == true)
        {
            return Ok(work);
        }
        if task.status.is_terminal() {
            if phase != Phase::Complete
                || task.status != act0r::domain::actors::AgentTaskStatus::Succeeded
            {
                save(
                    &app.directory.join("failed-checkpoint.json"),
                    &json!({"delegated_work":work,"inbox":inbox,"task":task}),
                )?;
                return Err(format!(
                    "demo stopped before the expected checkpoint: {:?}",
                    task.status
                )
                .into());
            }
            return Ok(work);
        }
        if task.status == act0r::domain::actors::AgentTaskStatus::Blocked {
            return Err(
                "demo needs explicit recovery; inspect the saved session before retrying".into(),
            );
        }
        if phase != Phase::DependencyWait
            && let Some(item) = items.first()
        {
            if item["kind"] == "guidance" {
                save(
                    &app.directory.join("failed-checkpoint.json"),
                    &json!({"reason":"repeated_questions","delegated_work":work,"inbox":inbox,"task":task}),
                )?;
                return Err("demo stopped after repeated questions; the saved assignment needs explicit guidance in the harness".into());
            }
            if item["kind"] == "question" {
                let count = questions
                    .entry(
                        item["task_id"]
                            .as_str()
                            .ok_or("missing question task")?
                            .to_owned(),
                    )
                    .or_default();
                *count += 1;
                if *count > 2 {
                    save(
                        &app.directory.join("failed-checkpoint.json"),
                        &json!({"reason":"repeated_questions","delegated_work":work,"inbox":inbox,"task":task}),
                    )?;
                    return Err("demo stopped after repeated questions from one assignment; inspect the saved answers before retrying".into());
                }
            }
            reply(client, app, r, item).await?;
            continue;
        }
        if worker.advance_work().await? || worker.fire_due_timer().await? {
            steps += 1;
            continue;
        }
        if let Some(execution) = worker.admit().await? {
            if execution.handler.name.contains("model") {
                if execution.handler.name == "agent.model" {
                    let request: ModelRequest = serde_json::from_value(execution.input.clone())?;
                    save(
                        &app.directory
                            .join(format!("model-request-{}.json", execution.activity_id)),
                        &act0r::adapters::model::ModelRouter::request_body(&request),
                    )?;
                }
                eprintln!(
                    "Model activity {} · Run {}",
                    execution.handler.name, execution.run_id
                );
            }
            let result = worker.execute(execution.clone()).await?;
            worker.complete(execution, result).await?;
            steps += 1;
        } else {
            // Provider backoff is a saved activity deadline. Wait for the worker's
            // normal admission, without issuing a second provider request here.
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis();
            let mut retry_pending = false;
            for run in std::iter::once(task.run_id)
                .chain(children.into_iter().flatten().map(|c| id(c).ok()))
                .flatten()
            {
                let saved = app.engine.status(run).await?;
                retry_pending |= saved.activities().iter().any(|a| {
                    a.status == act0r::domain::ActivityStatus::Queued
                        && a.ready_at > 0
                        && a.ready_at as u128 > now
                });
            }
            if retry_pending {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            save(
                &app.directory.join("failed-checkpoint.json"),
                &json!({"delegated_work":work,"inbox":inbox,"task":task}),
            )?;
            return Err(format!(
                "demo became idle before its checkpoint; inspect {}",
                app.directory.join("failed-checkpoint.json").display()
            )
            .into());
        }
    }
    Err("demo checkpoint exceeded its time/activity bound".into())
}

fn save(path: &Path, value: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
async fn cargo_tests(project: &Path, log: &Path) -> Result<bool> {
    let mut command = tokio::process::Command::new("cargo");
    command
        .args(["test", "--locked", "--offline", "--test", "contracts"])
        .current_dir(project);
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
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        command.kill_on_drop(true).output(),
    )
    .await??;
    fs::write(log, [&output.stdout[..], &output.stderr[..]].concat())?;
    Ok(output.status.success())
}
pub async fn consumer_uses_api(project: &Path, log: &Path) -> Result<bool> {
    // Exercise the saved consumer against a changed API in a disposable project.
    // This accepts equivalent Rust imports while rejecting duplicated formatting.
    let probe = tempfile::Builder::new()
        .prefix("consumer-dependency-")
        .tempdir_in(log.parent().ok_or("missing dependency log parent")?)?;
    for (path, contents) in FIXTURE
        .iter()
        .filter(|(path, _)| matches!(*path, "Cargo.toml" | "Cargo.lock" | "src/lib.rs"))
    {
        let file = probe.path().join(path);
        fs::create_dir_all(file.parent().ok_or("missing dependency fixture parent")?)?;
        fs::write(file, contents)?;
    }
    fs::copy(
        project.join("src/consumer.rs"),
        probe.path().join("src/consumer.rs"),
    )?;
    fs::write(
        probe.path().join("src/api.rs"),
        include_str!("../dependent-coding-project/src/api.rs").replace(
            API_OLD,
            "    format!(\"dependency:{}|{}\", user.first, user.last)",
        ),
    )?;
    fs::create_dir(probe.path().join("tests"))?;
    fs::write(
        probe.path().join("tests/contracts.rs"),
        r#"use dependent_coding_fixture::{api::User, consumer::render_greeting};
#[test]
fn consumer_follows_api_output() {
    for (first, last) in [("Ada", "Lovelace"), ("  Grace  ", ""), ("李", "雷")] {
        let user = User { first, last };
        assert_eq!(render_greeting(&user), format!("Hello, dependency:{first}|{last}!"));
    }
}
"#,
    )?;
    cargo_tests(probe.path(), log).await
}
fn check_original(source: &Path) -> Result<()> {
    for (path, contents) in FIXTURE {
        assert_eq!(fs::read_to_string(source.join(path))?, *contents);
    }
    Ok(())
}
async fn assert_held(app: &App, work: &Value) -> Result<Value> {
    let receiver = &work["groups"][0]["children"][0];
    assert_eq!(receiver["pause"]["execution_held"], true);
    let run = app.engine.status(id(receiver)?).await?;
    assert_eq!(run.record().usage.admitted_model_calls, 0);
    assert!(
        run.activities()
            .iter()
            .any(|a| a.spec.handler.name == "workspace.baseline"
                && a.status == act0r::domain::ActivityStatus::Succeeded)
    );
    let state = if run.record().state["config"].is_object() {
        &run.record().state
    } else {
        &run.record().state["state"]
    };
    let baseline = &state["change_review_state"]["baseline"];
    assert!(
        baseline["digest"].is_string(),
        "the held receiver must retain a real baseline"
    );
    Ok(
        json!({"task_id":receiver["task_id"],"run_id":receiver["run_id"],"usage":run.record().usage,"baseline":baseline}),
    )
}
async fn verified(app: &App, task: &Value) -> Result<Value> {
    let changes = app
        .agents
        .actors()?
        .task_changes(task["task_id"].as_str().ok_or("missing task")?.parse()?)
        .await?;
    let saved = json!(changes);
    if saved["completion"]["status"] != "verified"
        || saved["changes"]["test_status"] != "matching"
        || saved["changes"]["tests"]["result"]["ok"] != true
        || !saved["changes"]["tests"]["result"]["stdout"]
            .as_str()
            .is_some_and(|text| text.contains("8 passed"))
    {
        return Err("assignment lacks a verified patch with eight fresh passing contracts".into());
    }
    Ok(saved)
}

pub async fn run(options: Options) -> Result<Value> {
    if options.resume.is_some() {
        return resume::run(options).await;
    }
    if options.wait_id.is_some() || options.guidance_file.is_some() {
        return Err("guidance requires --resume, --wait-id and --guidance-file".into());
    }
    fresh(options).await
}

#[path = "dependent_coding_resume.rs"]
mod resume;

async fn fresh(options: Options) -> Result<Value> {
    fs::create_dir_all(&options.output)?;
    let folder = options.output.join(uuid::Uuid::new_v4().to_string());
    fs::create_dir(&folder)?;
    let folder = fs::canonicalize(folder)?;
    let source = folder.join("project");
    for (path, contents) in FIXTURE {
        let file = source.join(path);
        fs::create_dir_all(file.parent().ok_or("missing fixture parent")?)?;
        fs::write(file, contents)?;
    }
    let db = folder.join("demo.db");
    eprintln!("Demo files: {}", folder.display());
    assert!(
        !cargo_tests(&source, &folder.join("baseline-tests.log")).await?,
        "the original API must fail the contract tests"
    );
    let baseline_output = fs::read_to_string(folder.join("baseline-tests.log"))?;
    assert!(
        baseline_output.contains("4 passed; 4 failed"),
        "baseline must fail the four API contracts, not compilation or setup"
    );

    let model = if options.target_config.is_none() {
        Some(ScriptedModel::start().await?)
    } else {
        None
    };
    let target = if let Some(path) = &options.target_config {
        let config: Value = serde_json::from_slice(&fs::read(path)?)?;
        let target = config.get("target").unwrap_or(&config).clone();
        let _: ModelTarget = serde_json::from_value(target.clone())?;
        target
    } else {
        json!({"provider":"openai","model":"dependent-coding-script","protocol":"chat_completions","endpoint":model.as_ref().unwrap().endpoint})
    };
    let prompt = format!(
        "{PROMPT}\nThe first action must match this complete action object. Encode depends_on in the task object; mentioning the dependency only in its goal does not establish a durable hold.\n{}",
        initial_action()
    );
    let request = json!({"message_id":MessageId::new(),"prompt":prompt,"workspace":source,"target":target,
        "coding":{"coder_count":2,"writable_paths":["src/api.rs","src/consumer.rs"],"test_command":{"argv":["cargo","test","--locked","--offline","--test","contracts"],"timeout_ms":120000}}});
    save(&folder.join("request.json"), &request)?;
    let mut nvidia_request = request.clone();
    nvidia_request["message_id"] = json!(MessageId::new());
    let nvidia_source = folder.join("project-nvidia");
    for (path, contents) in FIXTURE {
        let file = nvidia_source.join(path);
        fs::create_dir_all(file.parent().ok_or("missing fixture parent")?)?;
        fs::write(file, contents)?;
    }
    nvidia_request["workspace"] = json!(nvidia_source);
    nvidia_request["target"] = serde_json::from_str::<Value>(include_str!(
        "../configs/benchmark-nvidia-agent.json"
    ))?["target"]
        .clone();
    save(&folder.join("request-nvidia.json"), &nvidia_request)?;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    let app = App::open(&db).await?;
    let r = post(
        &client,
        &format!("{}/v1/harness/sessions", app.api.url),
        &request,
    )
    .await?;
    save(&folder.join("session.json"), &r)?;
    let mut worker = app.engine.worker().await?;
    let waiting = drive(&client, &app, &mut worker, &r, Phase::DependencyWait).await?;
    let held = assert_held(&app, &waiting).await?;
    let inbox = get(&client, &format!("{}/inbox", app.goal(&r))).await?;
    let source_wait = inbox["items"][0].clone();
    save(
        &folder.join("checkpoint-wait.json"),
        &json!({"delegated_work":waiting,"receiver":held,"source_wait":source_wait}),
    )?;
    let root_run = app
        .agents
        .actors()?
        .task(r["task_id"].as_str().unwrap().parse()?)
        .await?
        .run_id
        .ok_or("missing planner Run")?;
    let parent_calls = app
        .engine
        .status(root_run)
        .await?
        .record()
        .usage
        .admitted_model_calls;
    check_original(&source)?;
    eprintln!("Consumer durably held before its first model call. Restarting API and worker.");
    drop(worker);
    app.close().await?;

    let app = App::open(&db).await?;
    let mut worker = app.engine.worker().await?;
    let restored = get(&client, &format!("{}/delegated-work", app.turn(&r))).await?;
    assert_eq!(assert_held(&app, &restored).await?, held);
    assert_eq!(
        app.engine
            .status(root_run)
            .await?
            .record()
            .usage
            .admitted_model_calls,
        parent_calls
    );
    let inbox = get(&client, &format!("{}/inbox", app.goal(&r))).await?;
    assert_eq!(inbox["items"][0]["wait"]["id"], source_wait["wait"]["id"]);
    let approving = drive(&client, &app, &mut worker, &r, Phase::TransferApproval).await?;
    if options.stop_after == Some(StopAfter::TransferApproval) {
        let report = resume::checkpoint(&app, &r, "transfer_approval");
        save(&folder.join("checkpoint-report.json"), &report)?;
        drop(worker);
        app.close().await?;
        return Ok(report);
    }
    let held = assert_held(&app, &approving).await?;
    let prerequisite = verified(&app, &approving["groups"][0]["children"][1]).await?;
    assert_eq!(
        prerequisite["changes"]["files"]
            .as_array()
            .ok_or("missing API patch")?
            .len(),
        1
    );
    let inbox = get(&client, &format!("{}/inbox", app.goal(&r))).await?;
    let approval = inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["wait"]["payload"]["worker_transfer"] == true)
        .ok_or("missing transfer approval")?
        .clone();
    save(
        &folder.join("checkpoint-approval.json"),
        &json!({"delegated_work":approving,"receiver":held,"approval":approval,"prerequisite":prerequisite}),
    )?;
    let source_calls = app
        .engine
        .status(id(&approving["groups"][0]["children"][1])?)
        .await?
        .record()
        .usage
        .admitted_model_calls;
    eprintln!(
        "API passed all eight tests. Exact reviewed transfer awaits approval. Restarting again."
    );
    drop(worker);
    app.close().await?;

    let app = App::open(&db).await?;
    let mut worker = app.engine.worker().await?;
    let restored = get(&client, &format!("{}/delegated-work", app.turn(&r))).await?;
    assert_eq!(assert_held(&app, &restored).await?, held);
    let inbox = get(&client, &format!("{}/inbox", app.goal(&r))).await?;
    assert_eq!(inbox["items"][0]["wait"], approval["wait"]);
    assert_eq!(
        verified(&app, &restored["groups"][0]["children"][1]).await?,
        prerequisite
    );
    reply(&client, &app, &r, &inbox["items"][0]).await?;
    let completed = drive(&client, &app, &mut worker, &r, Phase::Complete).await?;
    assert_eq!(
        app.engine
            .status(id(&completed["groups"][0]["children"][1])?)
            .await?
            .record()
            .usage
            .admitted_model_calls,
        source_calls
    );
    let report = finish(
        &client,
        &app,
        &mut worker,
        &r,
        &completed,
        model.as_ref(),
        options.stop_after,
        2,
    )
    .await?;
    drop(worker);
    app.close().await?;
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn finish(
    client: &Client,
    app: &App,
    worker: &mut Worker,
    r: &Value,
    completed: &Value,
    model: Option<&ScriptedModel>,
    stop_after: Option<StopAfter>,
    restarts: u32,
) -> Result<Value> {
    let folder = &app.directory;
    let db = folder.join("demo.db");
    let source = folder.join("project");
    let root_run = app
        .agents
        .actors()?
        .task(r["task_id"].as_str().ok_or("missing task")?.parse()?)
        .await?
        .run_id
        .ok_or("missing planner Run")?;
    if completed["groups"].as_array().is_none_or(|g| g.len() != 1)
        || completed["groups"][0]["joined"] != true
    {
        return Err("completion requires one original joined group".into());
    }
    let prerequisite = verified(app, &completed["groups"][0]["children"][1]).await?;
    let source_usage = app
        .engine
        .status(id(&completed["groups"][0]["children"][1])?)
        .await?
        .record()
        .usage
        .clone();
    let source_calls = source_usage.admitted_model_calls;
    let receiver = verified(app, &completed["groups"][0]["children"][0]).await?;
    let parent = verified(app, r).await?;
    let parent_saved = app.engine.status(root_run).await?;
    if parent_saved.record().status != RunStatus::Succeeded
        || parent_saved.record().delegations.len() != 2
    {
        return Err("planner did not complete the original two assignments".into());
    }
    for changes in [&receiver, &parent] {
        let paths: Vec<_> = changes["changes"]["files"]
            .as_array()
            .ok_or("missing patch files")?
            .iter()
            .map(|f| f["path"].as_str().ok_or("missing patch path"))
            .collect::<std::result::Result<_, _>>()?;
        if paths != ["src/api.rs", "src/consumer.rs"] {
            return Err("final patch must contain exactly the two assigned files".into());
        }
    }
    if receiver["changes"]["baseline_digest"] != parent["changes"]["baseline_digest"]
        || prerequisite["changes"]["tests"]["activity_id"]
            == receiver["changes"]["tests"]["activity_id"]
        || receiver["changes"]["tests"]["activity_id"] == parent["changes"]["tests"]["activity_id"]
    {
        return Err(
            "combined repair requires the original baseline and independent fresh tests".into(),
        );
    }
    let history = app.engine.history(root_run).await?;
    let count = |kind: &str| history.iter().filter(|e| e.kind == kind).count();
    if count("task_group_joined") != 1 || count("initial_dependency_bound") != 1 {
        return Err("completion must preserve one dependency and one group join".into());
    }
    for handler in [
        act0r::domain::worker_changes::handler("import"),
        act0r::domain::child_changes::handler("import"),
    ] {
        let imports: Vec<_> = parent_saved
            .activities()
            .iter()
            .filter(|a| a.spec.handler == handler)
            .collect();
        if imports.len() != 1 || imports[0].status != act0r::domain::ActivityStatus::Succeeded {
            return Err("each transfer and consolidated import must succeed exactly once".into());
        }
    }
    let patch = app
        .agents
        .actors()?
        .task_changes(r["task_id"].as_str().unwrap().parse()?)
        .await?
        .changes
        .ok_or("missing parent patch")?
        .patch();
    fs::write(folder.join("combined.patch"), patch)?;
    save(&folder.join("completed-work.json"), completed)?;
    save(
        &folder.join("test-evidence.json"),
        &json!({"api":prerequisite,"consumer":receiver,"parent":parent}),
    )?;
    let applied = resume::apply(client, app, worker, r, stop_after).await?;
    if applied["status"] == "checkpoint" {
        return Ok(applied);
    }
    if !cargo_tests(&source, &folder.join("applied-tests.log")).await? {
        return Err("the separately applied project failed its contract tests".into());
    }
    if !consumer_uses_api(&source, &folder.join("consumer-dependency-tests.log")).await? {
        return Err("the applied consumer does not use the API's display_name output".into());
    }
    for (path, contents) in FIXTURE
        .iter()
        .filter(|(p, _)| !matches!(*p, "src/api.rs" | "src/consumer.rs"))
    {
        if fs::read_to_string(source.join(path))? != *contents {
            return Err("the applied patch changed protected fixture files".into());
        }
    }
    if let Some(model) = model {
        let requests = model
            .requests
            .lock()
            .map_err(|_| "model request lock poisoned")?;
        assert_eq!(requests.iter().filter(|p| p["goal"] == API_GOAL).count(), 4);
        assert_eq!(
            requests
                .iter()
                .filter(|p| p["goal"] == CONSUMER_GOAL)
                .count(),
            4
        );
        let consumer_read = requests
            .iter()
            .find(|p| p["goal"] == CONSUMER_GOAL && p["turn"] == 1)
            .ok_or("missing post-transfer consumer read")?;
        let text = consumer_read["tool_observations"][0]["result"]["text"]
            .as_str()
            .ok_or("missing saved API read")?;
        assert_eq!(
            text,
            FIXTURE
                .iter()
                .find(|(path, _)| *path == "src/api.rs")
                .unwrap()
                .1
                .replace(API_OLD, API_NEW)
        );
        save(&folder.join("model-requests.json"), &json!(*requests))?;
    }
    let receiver_usage = app
        .engine
        .status(id(&completed["groups"][0]["children"][0])?)
        .await?
        .record()
        .usage
        .clone();
    let report = json!({"status":"verified","mode":if model.is_some() {"scripted"} else {"live"},"directory":folder,"database":db,"session":r,
        "restarts":restarts,"receiver_calls_before_release":0,"group_joins":count("task_group_joined"),"dependency_bindings":count("initial_dependency_bound"),
        "source_calls":source_calls,"receiver_calls":receiver_usage.admitted_model_calls,
        "source_usage":source_usage,"receiver_usage":receiver_usage,"planner_usage":parent_saved.record().usage,
        "planner_calls":parent_saved.record().usage.admitted_model_calls,"test_cases_per_workspace":8,"fresh_api_consumer_parent_tests":true,
        "baseline_tests_failed":true,"project_unchanged_before_application":true,"approved_application":applied["run_id"],"applied_tests_ok":true,"consumer_uses_api":true,
        "changed_files":["src/api.rs","src/consumer.rs"],"patch":folder.join("combined.patch"),"evidence":folder.join("test-evidence.json")});
    save(&folder.join("report.json"), &report)?;
    eprintln!(
        "Combined patch applied to the demo project with separate approval; all eight tests pass."
    );
    Ok(report)
}
