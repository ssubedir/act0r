//! Example-only guards and fixture decisions; the installed runtime owns every effect.
use act0r::{
    Engine,
    adapters::model::ModelRouter,
    application::AgentService,
    bootstrap::local_app,
    domain::{
        actors::*,
        agent::{ModelActionFormat, ModelRequest, ModelTarget},
        completion::CompletionStatus,
        *,
    },
};
use clap::{Parser, ValueEnum};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub enum StopAfter {
    WorkerEditApproval,
    ImportApproval,
    Refusal,
}
#[derive(Parser)]
#[command(about = "Probe planner imports, actual parent tests, and durable completion recovery")]
pub struct Options {
    #[arg(long, default_value = ".build/planner-dispatch-probe")]
    pub output: PathBuf,
    /// Planner target, also used by the repair worker when --live-worker is selected.
    #[arg(long, conflicts_with = "resume")]
    pub target_config: Option<PathBuf>,
    /// Use the configured model for the repair worker instead of scripted decisions.
    #[arg(long, requires = "target_config", conflicts_with = "resume")]
    pub live_worker: bool,
    #[arg(long, requires = "target_config", conflicts_with = "resume")]
    pub prepare_only: bool,
    /// Pinned model transport; JSON schema remains the default.
    #[arg(long, conflicts_with = "resume", value_parser = ["json_schema", "function_calling"])]
    pub action_format: Option<String>,
    #[arg(long, conflicts_with_all = ["target_config", "live_worker", "prepare_only", "action_format"])]
    pub resume: Option<PathBuf>,
    #[arg(long, value_enum)]
    pub stop_after: Option<StopAfter>,
}

const GOAL: &str = "Delegate exactly one task to repairer: fix add in src/lib.rs and run the fixed tests. After it succeeds, inspect_child_changes for repairer with offset 0 and import_child_changes for repairer. Then request your own run_tests in the parent workspace and finish only after those fresh tests pass. Child tests do not verify your workspace. If completion is refused, use the saved reason to request parent tests in this same task; do not delegate again. The runtime obtains import and test approvals. Do not edit protected fixture files.";
const COMMAND: &[&str] = &[
    "cargo",
    "test",
    "--locked",
    "--offline",
    "--test",
    "contracts",
];
const FIXTURE: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        "[package]\nname = \"planner_dispatch_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n[profile.dev]\ndebug = 0\nincremental = false\n[profile.test]\ndebug = 0\nincremental = false\n",
    ),
    (
        "Cargo.lock",
        "version = 4\n\n[[package]]\nname = \"planner_dispatch_fixture\"\nversion = \"0.1.0\"\n",
    ),
    (
        "src/lib.rs",
        "pub fn add(left: i64, right: i64) -> i64 { left - right }\n",
    ),
    (
        "tests/contracts.rs",
        "use planner_dispatch_fixture::add;\n#[test] fn positive() { assert_eq!(add(2, 3), 5); }\n#[test] fn negative() { assert_eq!(add(-4, -7), -11); }\n#[test] fn zero() { assert_eq!(add(0, 3), 3); }\n",
    ),
];
fn read(path: impl AsRef<Path>) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn write(path: impl AsRef<Path>, value: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

/// Deliberately claims completion once before parent tests, to exercise real refusal.
pub fn fixture_action(prompt: &Value, refuse_once: bool) -> Value {
    let observations = prompt["observations"].as_array().unwrap();
    let seen = |kind: &str| observations.iter().any(|o| o["action"]["kind"] == kind);
    if prompt["turn"] == 0 {
        return json!({"kind":"delegate_task","worker":"repairer","goal":"Fix add in src/lib.rs and run the fixed tests."});
    }
    if !seen("inspect_child_changes") {
        return json!({"kind":"inspect_child_changes","worker":"repairer","offset":0});
    }
    if !seen("import_child_changes") {
        return json!({"kind":"import_child_changes","worker":"repairer"});
    }
    let refused = observations
        .iter()
        .any(|o| o["completion"]["status"] == "refused");
    if refuse_once && !refused {
        return json!({"kind":"finish","summary":"The child tests passed, so the parent is verified."});
    }
    if !observations
        .iter()
        .any(|o| o["action"]["tool"] == "run_tests" || o["action"]["kind"] == "run_tests")
    {
        return json!({"kind":"run_tests"});
    }
    json!({"kind":"finish","summary":"Imported the repair and verified all three fresh parent contracts."})
}
struct FixtureServer(JoinHandle<()>);
impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub fn fixture_reply(body: &Value, mut action: Value) -> Value {
    let choice = if body["tools"].is_array() {
        let name = action.as_object_mut().unwrap().remove("kind").unwrap();
        json!({"finish_reason":"tool_calls","message":{"role":"assistant","content":null,"tool_calls":[{"id":"fixture-function-call","type":"function","function":{"name":name,"arguments":action.to_string()}}]}})
    } else {
        json!({"finish_reason":"stop","message":{"content":json!({"action":action}).to_string()}})
    };
    json!({"choices":[choice],"usage":{"prompt_tokens":10,"completion_tokens":10}})
}
async fn fixture_server() -> Result<(ModelTarget, FixtureServer)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let target = serde_json::from_value(
        json!({"provider":"openai","model":"planner-fixture","protocol":"chat_completions","endpoint":format!("http://{}/v1/chat/completions",listener.local_addr()?)}),
    )?;
    let task = tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|axum::Json(body): axum::Json<Value>| async move {
                let prompt: Value =
                    serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                axum::Json(fixture_reply(&body, fixture_action(&prompt, true)))
            }),
        );
        axum::serve(listener, app).await.unwrap();
    });
    Ok((target, FixtureServer(task)))
}

async fn prepare(directory: &Path, target: &ModelTarget, live_worker: bool) -> Result<Value> {
    let source = directory.join("source");
    for (path, contents) in FIXTURE {
        let file = source.join(path);
        fs::create_dir_all(file.parent().ok_or("missing parent")?)?;
        fs::write(file, contents)?;
    }
    let source = fs::canonicalize(source)?;
    let (engine, agents) = local_app(directory.join("probe.db")).await?;
    let mut worker_config = json!({
        "goal":"Repair addition","target":{"provider":"scripted","model":"repair-fixture","endpoint":""},
        "max_model_calls":null,"max_tokens":null,"max_output_tokens":512,"accept_workspace_handoff":true,
        "tools":[{"name":"read_file","version":2},{"name":"replace_text","version":1},{"name":"run_tests","version":1}],
        "writable_paths":["src/lib.rs"],"test_command":{"argv":COMMAND,"timeout_ms":120000},
        "change_review":true,"completion_policy":"require_passing_tests",
        "script":[{"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"left - right","new":"left + right"}},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},{"kind":"finish","summary":"Addition repaired; three contracts passed."}]
    });
    if live_worker {
        worker_config["target"] = json!(target);
        worker_config["script"] = json!([]);
        worker_config["max_output_tokens"] = json!(2048);
    }
    let child = agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            Some(source.clone()),
            worker_config,
            100,
            Default::default(),
        )
        .await?;
    let mut config = json!({"goal":GOAL,"target":target,"max_model_calls":null,"max_tokens":null,"max_output_tokens":2048,
        "context":{"version":1,"max_input_bytes":65536,"max_observations":16},
        "workers":[{"name":"repairer","agent_id":child,"description":"Deterministic addition repair with real tests and a verified patch."}],
        "delegation":{"max_tasks":1,"max_depth":2},"workspace_handoff":true,"child_changes":true,
        "change_review":true,"completion_policy":"require_passing_tests","writable_paths":["src/lib.rs"],
        "tools":[{"name":"run_tests","version":1}],"test_command":{"argv":COMMAND,"timeout_ms":120000}});

    if live_worker {
        config["workers"][0]["description"] =
            json!("Repair addition with the pinned tools and return a verified patch.");
    }
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source.clone()),
            config,
            100,
            Default::default(),
        )
        .await?;
    let task = MessageId::new();
    agents
        .actors()?
        .send(AgentMessage {
            id: task,
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":GOAL}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    for _ in 0..20 {
        worker.advance_work().await?;
        if let Some(run) = agents.actors()?.task(task).await?.run_id {
            let state = engine.status(run).await?;
            if let Some(activity) = state
                .activities()
                .iter()
                .find(|a| a.spec.handler.name == "agent.model")
            {
                if state.record().usage.admitted_model_calls != 0 {
                    return Err("first request was admitted during preparation".into());
                }
                let request: ModelRequest = serde_json::from_value(activity.spec.input.clone())?;
                write(
                    directory.join("first-request.json"),
                    &ModelRouter::request_body(&request),
                )?;
                return Ok(
                    json!({"version":1,"worker_mode":if live_worker {"model"}else{"scripted"},"actor_id":actor,"worker_id":child,"task_id":task,"run_id":run,"source":source,
                    "pinned_config":agents.actors()?.status(actor).await?.record().config,
                    "worker_config":agents.actors()?.status(child).await?.record().config}),
                );
            }
            let execution = worker.admit().await?.ok_or("missing baseline")?;
            if execution.handler.name != "workspace.baseline" {
                return Err("unexpected preparation activity".into());
            }
            let outcome = worker.execute(execution.clone()).await?;
            worker.complete(execution, outcome).await?;
        }
    }
    Err("could not prepare planner request".into())
}

async fn snapshot(
    engine: &Engine,
    agents: &AgentService,
    directory: &Path,
    task: MessageId,
    run: RunId,
) -> Result<Value> {
    let state = engine.status(run).await?;
    let mut requests = vec![];
    for a in state
        .activities()
        .iter()
        .filter(|a| a.spec.handler.name == "agent.model")
    {
        let request: ModelRequest = serde_json::from_value(a.spec.input.clone())?;
        let body = ModelRouter::request_body(&request);
        write(directory.join(format!("request-{}.json", a.id)), &body)?;
        requests.push(json!({"activity_id":a.id,"status":a.status,"saved_request":request,"wire_body":body,"result":a.result,"error":a.error,"attempts":a.attempts}));
    }
    let mut children = vec![];
    for child in &state.record().delegations {
        let view = agents.actors()?.task(child.id).await?;
        let changes = agents.actors()?.task_changes(child.id).await?;
        let child_run = if let Some(id) = view.run_id {
            Some(engine.status(id).await?)
        } else {
            None
        };
        let mut evidence = json!({"task":view,"changes":changes,"run":child_run.as_ref().map(|r|r.record()),"activities":child_run.as_ref().map(|r|r.activities())});
        if let Some(saved) = &child_run {
            let mut requests = vec![];
            for activity in saved
                .activities()
                .iter()
                .filter(|a| a.spec.handler.name == "agent.model")
            {
                let request: ModelRequest = serde_json::from_value(activity.spec.input.clone())?;
                if request.target.provider == "openai" {
                    let body = ModelRouter::request_body(&request);
                    write(
                        directory.join(format!("worker-request-{}.json", activity.id)),
                        &body,
                    )?;
                    requests.push(json!({"activity_id":activity.id,"status":activity.status,"saved_request":request,"wire_body":body,"result":activity.result,"error":activity.error,"attempts":activity.attempts}));
                }
            }
            evidence["requests"] = json!(requests);
        }
        children.push(evidence);
    }
    let evidence = json!({"task":agents.actors()?.task(task).await?,"run":state.record(),"activities":state.activities(),"requests":requests,
        "changes":agents.actors()?.task_changes(task).await?,"children":children});
    write(directory.join("evidence.json"), &evidence)?;
    Ok(evidence)
}

fn call_guard(run: &Run) -> bool {
    run.record().usage.admitted_model_calls >= 8
        && run
            .activities()
            .iter()
            .any(|a| a.spec.handler.name == "agent.model" && a.status == ActivityStatus::Queued)
}
fn checkpointed(checkpoints: &[Value], kind: &str, wait: WaitId) -> bool {
    checkpoints
        .iter()
        .any(|c| c["kind"] == kind && c["wait"]["id"] == wait.to_string())
}

pub fn parent_test_passed_after_import(evidence: &Value) -> bool {
    let Some(activities) = evidence["activities"].as_array() else {
        return false;
    };
    let imports: Vec<_> = activities
        .iter()
        .filter(|a| a["spec"]["handler"]["name"] == "planner.child_changes.import")
        .collect();
    let Some(test) = activities.iter().rev().find(|a| {
        a["spec"]["handler"]["name"] == "native.tool"
            && a["spec"]["input"]["definition"]["id"] == json!({"name":"run_tests","version":1})
    }) else {
        return false;
    };
    imports.len() == 1
        && imports[0]["status"] == "succeeded"
        && imports[0]["result"]["status"] == "applied"
        && test["scheduling_revision"]
            .as_u64()
            .zip(imports[0]["scheduling_revision"].as_u64())
            .is_some_and(|(test, import)| test > import)
        && test["status"] == "succeeded"
        && test["result"]["ok"] == true
        && test["result"]["exit_code"] == 0
        && test["result"]["stdout"]
            .as_str()
            .is_some_and(|s| s.contains("3 passed"))
}

async fn drive(
    engine: &Engine,
    agents: &AgentService,
    task: MessageId,
    run: RunId,
    child: AgentId,
    checkpoints: &[Value],
) -> Result<&'static str> {
    let mut worker = engine.worker().await?;
    let deadline = Instant::now() + Duration::from_secs(900);
    loop {
        let view = agents.actors()?.task(task).await?;
        let state = engine.status(run).await?;
        if view.status.is_terminal() {
            return Ok("terminal");
        }
        if matches!(
            view.status,
            AgentTaskStatus::Blocked | AgentTaskStatus::NeedsRecovery | AgentTaskStatus::Paused
        ) {
            return Ok("needs_operator");
        }
        // A small trial guard only: unlimited saved budgets and general planner policy remain intact.
        let proposals = state
            .activities()
            .iter()
            .filter(|a| {
                a.result.as_ref().is_some_and(|r| {
                    matches!(
                        r["action"]["kind"].as_str(),
                        Some("delegate_task" | "delegate_tasks")
                    )
                })
            })
            .count();
        if state.record().delegations.len() > 1 || proposals > 1 {
            return Ok("repeated_delegation");
        }
        let changes = agents.actors()?.task_changes(task).await?;
        if changes
            .completion
            .as_ref()
            .is_some_and(|c| c.status == CompletionStatus::Refused)
            && !checkpoints.iter().any(|c| c["kind"] == "refusal")
        {
            return Ok("refusal");
        }
        if let Some(wait) = view.wait {
            if wait.key == child_changes::APPROVAL_KEY {
                if !checkpointed(checkpoints, "import_approval", wait.id) {
                    return Ok("import_approval");
                }
                let inspection: child_changes::Inspection =
                    serde_json::from_value(wait.payload["inspection"].clone())?;
                if inspection.request.worker != "repairer"
                    || inspection.preview.has_conflicts()
                    || inspection
                        .preview
                        .files
                        .iter()
                        .any(|f| f.path != "src/lib.rs")
                {
                    return Ok("unsupported_approval");
                }
            } else if wait.key == "native_tool_approval"
                && wait.payload["tool"] == json!({"name":"run_tests","version":1})
            {
                if !checkpointed(checkpoints, "test_approval", wait.id) {
                    return Ok("test_approval");
                }
                if wait.payload["test_command"] != json!({"argv":COMMAND,"timeout_ms":120000}) {
                    return Ok("unsupported_approval");
                }
            } else {
                return Ok("needs_input");
            }
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await?;
        }
        for view in agents.actors()?.tasks(child).await? {
            if matches!(
                view.status,
                AgentTaskStatus::Blocked | AgentTaskStatus::NeedsRecovery | AgentTaskStatus::Paused
            ) {
                return Ok("needs_operator");
            }
            if let (Some(id), Some(wait)) = (view.run_id, view.wait) {
                if wait.key != "native_tool_approval" {
                    return Ok("needs_input");
                }
                if wait.payload["tool"] == json!({"name":"replace_text","version":1}) {
                    if wait.payload["arguments"]["path"] != "src/lib.rs"
                        || wait.payload["writable_paths"] != json!(["src/lib.rs"])
                    {
                        return Ok("unsupported_approval");
                    }
                    if agents.actors()?.status(child).await?.record().config["input"]["target"]["provider"]
                        == "openai"
                        && !checkpoints
                            .iter()
                            .any(|c| c["kind"] == "worker_edit_approval")
                    {
                        return Ok("worker_edit_approval");
                    }
                } else if wait.payload["tool"] == json!({"name":"run_tests","version":1}) {
                    if wait.payload["test_command"] != json!({"argv":COMMAND,"timeout_ms":120000}) {
                        return Ok("unsupported_approval");
                    }
                } else {
                    return Ok("unsupported_approval");
                }
                // Approve only the generated fixture's pinned edit/test operations.
                engine.signal(id, wait.id, json!({"approved":true})).await?;
            }
            if let Some(id) = view.run_id
                && call_guard(&engine.status(id).await?)
            {
                return Ok("worker_call_guard");
            }
        }
        if call_guard(&state) {
            return Ok("call_guard");
        }
        if Instant::now() >= deadline {
            return Ok("time_guard");
        }
        let mut progressed = worker.advance_work().await?;
        progressed |= worker.fire_due_timer().await?;
        if let Some(execution) = worker.admit().await? {
            eprintln!(
                "Executing {} · Run {}",
                execution.handler.name, execution.run_id
            );
            let outcome = worker.execute(execution.clone()).await?;
            worker.complete(execution, outcome).await?;
            progressed = true;
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

pub async fn run(options: Options) -> Result<Value> {
    if options.resume.is_some()
        && (options.target_config.is_some()
            || options.prepare_only
            || options.action_format.is_some()
            || options.live_worker)
    {
        return Err("resume cannot replace saved pins".into());
    }
    if options.prepare_only && options.target_config.is_none() {
        return Err("prepare-only requires target-config".into());
    }
    if options.live_worker && options.target_config.is_none() {
        return Err("live-worker requires target-config".into());
    }
    let mut server = None;
    let (directory, manifest) = if let Some(path) = options.resume {
        let directory = fs::canonicalize(path)?;
        let manifest = read(directory.join("manifest.json"))?;
        if manifest["version"] != 1 || manifest["mode"] != "configured" {
            return Err("resume requires an original configured probe".into());
        }
        (directory, manifest)
    } else {
        let mut target: ModelTarget = if let Some(path) = &options.target_config {
            let value = read(path)?;
            serde_json::from_value(value.get("target").unwrap_or(&value).clone())?
        } else {
            let (target, fixture) = fixture_server().await?;
            server = Some(fixture);
            target
        };
        if let Some(format) = &options.action_format {
            target.action_format = match format.as_str() {
                "json_schema" => ModelActionFormat::JsonSchema,
                "function_calling" => ModelActionFormat::FunctionCalling,
                _ => return Err("unknown action format".into()),
            };
        }
        target.validate()?;
        if target.provider != "openai" {
            return Err("planner probe requires an HTTP target".into());
        }
        let directory = options.output.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(&directory)?;
        let directory = fs::canonicalize(directory)?;
        eprintln!("Planner probe directory: {}", directory.display());
        let mut manifest = prepare(&directory, &target, options.live_worker).await?;
        manifest["mode"] = json!(if options.target_config.is_some() {
            "configured"
        } else {
            "fixture"
        });
        write(directory.join("manifest.json"), &manifest)?;
        (directory, manifest)
    };
    let task: MessageId = manifest["task_id"]
        .as_str()
        .ok_or("missing task")?
        .parse()?;
    let run: RunId = manifest["run_id"].as_str().ok_or("missing Run")?.parse()?;
    let actor: AgentId = manifest["actor_id"]
        .as_str()
        .ok_or("missing actor")?
        .parse()?;
    let child: AgentId = manifest["worker_id"]
        .as_str()
        .ok_or("missing worker")?
        .parse()?;
    let source = fs::canonicalize(directory.join("source"))?;
    if Path::new(manifest["source"].as_str().ok_or("missing source")?) != source {
        return Err("source identity changed".into());
    }
    let checkpoint_path = directory.join("checkpoints.json");
    let mut checkpoints: Vec<Value> = if checkpoint_path.exists() {
        serde_json::from_value(read(&checkpoint_path)?)?
    } else {
        vec![]
    };
    let mut restart_snapshot = if directory.join("restart-pending.json").exists() {
        Some(read(directory.join("restart-pending.json"))?)
    } else {
        None
    };
    let (stop, evidence) = loop {
        let (engine, agents) = local_app(directory.join("probe.db")).await?;
        if agents.actors()?.status(actor).await?.record().config != manifest["pinned_config"]
            || agents.actors()?.status(child).await?.record().config != manifest["worker_config"]
            || agents.actors()?.task(task).await?.run_id != Some(run)
            || engine.status(run).await?.record().input["input"]["goal"] != GOAL
        {
            return Err("task identity or saved pins changed".into());
        }
        let workspace = PathBuf::from(
            manifest["pinned_config"]["input"]["workspace"]
                .as_str()
                .ok_or("missing workspace")?,
        );
        for (path, contents) in FIXTURE {
            if fs::read_to_string(source.join(path))? != *contents
                || (*path != "src/lib.rs" && fs::read_to_string(workspace.join(path))? != *contents)
            {
                return Err("protected fixture changed".into());
            }
        }
        if let Some(expected) = restart_snapshot.take() {
            let actual = snapshot(&engine, &agents, &directory, task, run).await?;
            if actual != expected {
                return Err("committed checkpoint changed during restart".into());
            }
            let last = checkpoints.last_mut().ok_or("missing restart checkpoint")?;
            last["verified_after_restart"] = json!(true);
            write(&checkpoint_path, &json!(checkpoints))?;
            fs::remove_file(directory.join("restart-pending.json"))?;
        }
        let stop = if options.prepare_only {
            "prepared"
        } else {
            drive(&engine, &agents, task, run, child, &checkpoints).await?
        };
        let evidence = snapshot(&engine, &agents, &directory, task, run).await?;
        if matches!(
            stop,
            "worker_edit_approval" | "import_approval" | "test_approval" | "refusal"
        ) {
            checkpoints.push(json!({"kind":stop,"verified_after_restart":false,"wait":evidence["run"]["wait"],"worker_waits":evidence["children"].as_array().unwrap().iter().map(|c|c["task"]["wait"].clone()).collect::<Vec<_>>(),"completion":evidence["changes"]["completion"],"model_calls":evidence["run"]["usage"]["admitted_model_calls"]}));
            write(&checkpoint_path, &json!(checkpoints))?;
            write(directory.join(format!("checkpoint-{stop}.json")), &evidence)?;
            write(directory.join("restart-pending.json"), &evidence)?;
            let halt = matches!(
                (options.stop_after, stop),
                (Some(StopAfter::WorkerEditApproval), "worker_edit_approval")
                    | (Some(StopAfter::ImportApproval), "import_approval")
                    | (Some(StopAfter::Refusal), "refusal")
            );
            if halt {
                break (stop, evidence);
            }
            restart_snapshot = Some(evidence);
            drop(agents);
            drop(engine);
            continue;
        }
        break (stop, evidence);
    };
    let parent_tests: Vec<_> = evidence["activities"]
        .as_array()
        .ok_or("missing activities")?
        .iter()
        .filter(|a| {
            a["spec"]["handler"]["name"] == "native.tool"
                && a["spec"]["input"]["definition"]["id"] == json!({"name":"run_tests","version":1})
        })
        .collect();
    let tests_passed = parent_test_passed_after_import(&evidence);
    let child_verified = evidence["children"].as_array().is_some_and(|c| {
        c.len() == 1
            && c[0]["task"]["status"] == "succeeded"
            && c[0]["changes"]["completion"]["status"] == "verified"
    });
    let imports: Vec<_> = evidence["activities"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["spec"]["handler"]["name"] == "planner.child_changes.import")
        .collect();
    let import_verified = imports.len() == 1
        && imports[0]["status"] == "succeeded"
        && imports[0]["result"]["status"] == "applied";
    let completion_verified = evidence["changes"]["completion"]["status"] == "verified"
        && evidence["changes"]["completion"]["test_activity_id"]
            == parent_tests.last().map_or(Value::Null, |a| a["id"].clone());
    let source_unchanged = FIXTURE.iter().all(|(path, contents)| {
        fs::read_to_string(source.join(path)).ok().as_deref() == Some(*contents)
    });
    let workspace = Path::new(
        manifest["pinned_config"]["input"]["workspace"]
            .as_str()
            .ok_or("missing workspace")?,
    );
    let protected_unchanged = FIXTURE
        .iter()
        .filter(|(path, _)| *path != "src/lib.rs")
        .all(|(path, contents)| {
            fs::read_to_string(workspace.join(path)).ok().as_deref() == Some(*contents)
        });
    let worker_protected_unchanged = evidence["children"]
        .as_array()
        .unwrap()
        .iter()
        .all(|child| {
            if child["run"].is_null() {
                return true;
            }
            child["run"]["input"]["input"]["workspace"]
                .as_str()
                .is_some_and(|workspace| {
                    FIXTURE
                        .iter()
                        .filter(|(path, _)| *path != "src/lib.rs")
                        .all(|(path, contents)| {
                            fs::read_to_string(Path::new(workspace).join(path))
                                .ok()
                                .as_deref()
                                == Some(*contents)
                        })
                })
        });
    let verified = evidence["task"]["status"] == "succeeded"
        && child_verified
        && import_verified
        && tests_passed
        && completion_verified
        && source_unchanged
        && protected_unchanged
        && worker_protected_unchanged;
    let checkpoint = matches!(stop, "worker_edit_approval" | "import_approval" | "refusal");
    let worker_mode = if manifest["worker_config"]["input"]["target"]["provider"] == "openai" {
        "model"
    } else {
        "scripted"
    };
    let worker_usage = evidence["children"]
        .as_array()
        .unwrap()
        .first()
        .map_or(Value::Null, |c| c["run"]["usage"].clone());
    let report = json!({"status":if options.prepare_only {"prepared"}else if checkpoint {"checkpoint"}else if verified {"verified"}else {"not_verified"},"directory":directory,"mode":manifest["mode"],"stop":stop,
        "worker_mode":worker_mode,"planner_usage":evidence["run"]["usage"],"worker_usage":worker_usage,
        "worker_model_calls":worker_usage["admitted_model_calls"],
        "model_calls":evidence["run"]["usage"]["admitted_model_calls"],"parent_tests_passed":tests_passed,
        "parent_test_activities":parent_tests.len(),"parent_test_activity_id":parent_tests.last().map(|a|a["id"].clone()),
        "child_verified":child_verified,"import_verified":import_verified,"completion_verified":completion_verified,
        "refusal_recovered":verified && checkpoints.iter().any(|c|c["kind"]=="refusal" && c["verified_after_restart"]==true),
        "restarts":checkpoints,"source_unchanged":source_unchanged,"protected_unchanged":protected_unchanged,"worker_protected_unchanged":worker_protected_unchanged,"evidence":evidence});
    write(directory.join("report.json"), &report)?;
    drop(server);
    Ok(report)
}
