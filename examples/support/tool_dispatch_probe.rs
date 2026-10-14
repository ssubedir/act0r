//! Example-only driver. Saved Runs, approvals and activities use the installed runtime.
use act0r::{
    Engine,
    adapters::model::ModelRouter,
    application::AgentService,
    bootstrap::local_app,
    domain::{
        actors::*,
        agent::{ModelRequest, ModelTarget},
        *,
    },
};
use clap::Parser;
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[command(
    about = "Compare native tools with standalone coding actions on a generated Rust fixture"
)]
pub struct Options {
    #[arg(long, default_value = ".build/tool-dispatch-probe")]
    pub output: PathBuf,
    /// Load only the target from an agent config, or a ModelTarget JSON object.
    #[arg(long, conflicts_with = "resume")]
    pub target_config: Option<PathBuf>,
    /// Save both first requests without making model calls; requires a configured target.
    #[arg(long, requires = "target_config", conflicts_with = "resume")]
    pub prepare_only: bool,
    /// Continue exactly these prepared tasks and model pins, never create replacements.
    #[arg(long, conflicts_with_all = ["target_config", "prepare_only"])]
    pub resume: Option<PathBuf>,
}

const GOAL: &str = "Read src/lib.rs, fix add so it adds its i64 arguments, run the fixed contracts, then finish. Only src/lib.rs may change. Use your supplied action schema for real tool requests. The runtime obtains the existing edit and test approvals.";
const COMMAND: &[&str] = &[
    "cargo",
    "test",
    "--locked",
    "--offline",
    "--test",
    "contracts",
];
const LIB: &str = "pub fn add(left: i64, right: i64) -> i64 { left - right }\n";
const FIXTURE: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        "[package]\nname = \"tool_dispatch_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n[profile.dev]\ndebug = 0\nincremental = false\n[profile.test]\ndebug = 0\nincremental = false\n",
    ),
    (
        "Cargo.lock",
        "version = 4\n\n[[package]]\nname = \"tool_dispatch_fixture\"\nversion = \"0.1.0\"\n",
    ),
    ("src/lib.rs", LIB),
    (
        "tests/contracts.rs",
        "use tool_dispatch_fixture::add;\n#[test] fn positive() { assert_eq!(add(2, 3), 5); }\n#[test] fn negative() { assert_eq!(add(-4, -7), -11); }\n#[test] fn zero() { assert_eq!(add(0, 3), 3); }\n",
    ),
];

fn write(path: impl AsRef<Path>, value: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
fn read(path: impl AsRef<Path>) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

/// The fixture supplies decisions only. The installed actors execute every tool.
pub fn fixture_action(p: &Value) -> Value {
    let (name, args) = match p["turn"].as_u64().unwrap_or(u64::MAX) {
        0 => ("read_file", json!({"path":"src/lib.rs"})),
        1 => (
            "replace_text",
            json!({"path":"src/lib.rs","old":"left - right","new":"left + right"}),
        ),
        2 => ("run_tests", json!({})),
        _ => {
            return json!({"kind":"finish","summary":"Fixed addition; all three contracts passed."});
        }
    };
    let mut action = args;
    action["kind"] = json!(name);
    action
}

struct FixtureServer(JoinHandle<()>);
impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn fixture_server() -> Result<(ModelTarget, FixtureServer)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let target = serde_json::from_value(
        json!({"provider":"openai","model":"dispatch-fixture","protocol":"chat_completions","endpoint":format!("http://{}/v1/chat/completions", listener.local_addr()?)}),
    )?;
    let task = tokio::spawn(async move {
        let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(|axum::Json(body): axum::Json<Value>| async move {
            let prompt: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            axum::Json(json!({"choices":[{"finish_reason":"stop","message":{"content":json!({"action":fixture_action(&prompt)}).to_string()}}],"usage":{"prompt_tokens":10,"completion_tokens":10}}))
        }));
        axum::serve(listener, app).await.unwrap();
    });
    Ok((target, FixtureServer(task)))
}

fn config(name: &str, target: &ModelTarget) -> Value {
    let mut config = json!({"goal":GOAL,"target":target,
        "max_model_calls":null,"max_tokens":null,"max_output_tokens":2048,
        "writable_paths":["src/lib.rs"],"context":{"version":1,"max_input_bytes":65536,"max_observations":16}});
    if name == "native" {
        config["tools"] = json!([{"name":"read_file","version":2},{"name":"replace_text","version":1},{"name":"run_tests","version":1}]);
        config["test_command"] = json!({"argv":COMMAND,"timeout_ms":120000});
        config["change_review"] = json!(true);
        config["completion_policy"] = json!("require_passing_tests");
    } else {
        config["workspace"] = json!("");
        config["test_command"] = json!(COMMAND);
        config["require_approval"] = json!(true);
        config["repeat_safe_tests"] = json!(true);
    }
    config
}

async fn prepare(directory: &Path, name: &str, target: &ModelTarget) -> Result<Value> {
    let folder = directory.join(name);
    let source = folder.join("source");
    for (path, contents) in FIXTURE {
        let file = source.join(path);
        fs::create_dir_all(file.parent().ok_or("missing parent")?)?;
        fs::write(file, contents)?;
    }
    let source = fs::canonicalize(source)?;
    let (engine, agents) = local_app(folder.join("probe.db")).await?;
    let input = config(name, target);

    let actor = agents
        .create_registered_actor(
            DefinitionId::new(if name == "native" { "tool" } else { "coding" }, 1),
            Some(source.clone()),
            input,
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
    // Save native baseline if needed, but never admit the first model activity.
    let run = loop {
        worker.advance_work().await?;
        if let Some(run) = agents.actors()?.task(task).await?.run_id {
            let state = engine.status(run).await?;
            if let Some(model) = state
                .activities()
                .iter()
                .find(|a| a.spec.handler.name == "agent.model")
            {
                if model.status != ActivityStatus::Queued || model.attempts != 0 {
                    return Err("prepare unexpectedly executed a model".into());
                }
                let request: ModelRequest = serde_json::from_value(model.spec.input.clone())?;
                write(
                    folder.join("first-request.json"),
                    &ModelRouter::request_body(&request),
                )?;
                break run;
            }
        }
        let execution = worker
            .admit()
            .await?
            .ok_or("fixture baseline did not schedule")?;
        if execution.handler.name == "agent.model" {
            return Err("prepare may not admit models".into());
        }
        let outcome = worker.execute(execution.clone()).await?;
        worker.complete(execution, outcome).await?;
    };
    let pinned = agents
        .actors()?
        .status(actor)
        .await?
        .record()
        .config
        .clone();
    Ok(
        json!({"name":name,"actor_id":actor,"task_id":task,"run_id":run,"source":source,"pinned_config":pinned}),
    )
}

async fn validate(
    engine: &Engine,
    agents: &AgentService,
    folder: &Path,
    arm: &Value,
) -> Result<(MessageId, RunId, PathBuf)> {
    let task: MessageId = arm["task_id"].as_str().ok_or("missing task")?.parse()?;
    let run: RunId = arm["run_id"].as_str().ok_or("missing Run")?.parse()?;
    let actor: AgentId = arm["actor_id"].as_str().ok_or("missing actor")?.parse()?;
    let saved = agents.actors()?.task(task).await?;
    let pinned = agents
        .actors()?
        .status(actor)
        .await?
        .record()
        .config
        .clone();
    if saved.task.agent_id != actor
        || saved.run_id != Some(run)
        || pinned != arm["pinned_config"]
        || engine.status(run).await?.record().input["input"]["goal"] != GOAL
    {
        return Err("probe identity or model/configuration pins differ from saved tasks".into());
    }
    let source = fs::canonicalize(folder.join("source"))?;
    if source != Path::new(arm["source"].as_str().ok_or("missing source")?) {
        return Err("probe source identity changed".into());
    }
    for (path, contents) in FIXTURE {
        if fs::read_to_string(source.join(path))? != *contents {
            return Err("probe source fixture changed".into());
        }
    }
    let workspace = PathBuf::from(
        pinned["input"]["workspace"]
            .as_str()
            .ok_or("missing managed workspace")?,
    );
    for (path, contents) in FIXTURE.iter().filter(|(p, _)| *p != "src/lib.rs") {
        if fs::read_to_string(workspace.join(path))? != *contents {
            return Err("protected managed fixture changed".into());
        }
    }
    Ok((task, run, workspace))
}

async fn snapshot(engine: &Engine, directory: &Path, run: RunId) -> Result<Value> {
    let state = engine.status(run).await?;
    let mut requests = vec![];
    for a in state
        .activities()
        .iter()
        .filter(|a| a.spec.handler.name == "agent.model")
    {
        let request: ModelRequest = serde_json::from_value(a.spec.input.clone())?;
        let wire = ModelRouter::request_body(&request);
        write(directory.join(format!("request-{}.json", a.id)), &wire)?;
        requests.push(json!({"activity_id":a.id,"status":a.status,"saved_request":request,"wire_body":wire,"result":a.result,"error":a.error,"attempts":a.attempts}));
    }
    let evidence = json!({"run":state.record(),"requests":requests,"activities":state.activities(),"attempts":state.attempts()});
    write(directory.join("evidence.json"), &evidence)?;
    Ok(evidence)
}

async fn drive(
    engine: &Engine,
    agents: &AgentService,
    folder: &Path,
    task: MessageId,
    run: RunId,
) -> Result<&'static str> {
    let deadline = Instant::now() + Duration::from_secs(900);
    let mut worker = engine.worker().await?;
    loop {
        let view = agents.actors()?.task(task).await?;
        let state = engine.status(run).await?;
        snapshot(engine, folder, run).await?;
        if view.status.is_terminal() {
            return Ok("terminal");
        }
        if view.status == AgentTaskStatus::Blocked {
            return Ok("blocked");
        }
        if view.status == AgentTaskStatus::Paused {
            return Ok("paused");
        }
        if view.status == AgentTaskStatus::NeedsRecovery {
            return Ok("needs_recovery");
        }
        if let Some(wait) = view.wait {
            if !matches!(wait.key.as_str(), "native_tool_approval" | "tool_approval") {
                return Ok("needs_input");
            }
            let tool = if wait.key == "native_tool_approval" {
                &wait.payload["tool"]["name"]
            } else {
                &wait.payload["action"]["kind"]
            };
            if tool != "replace_text" && tool != "run_tests" {
                return Ok("unsupported_approval");
            }
            // Only this generated fixture's two pinned mutating operations are approved.
            // Runtime scope validation and the exact saved wait retain authority.
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await?;
        }
        // A driver guard, not a token/call budget or a changed actor policy.
        if state.record().usage.admitted_model_calls >= 8 {
            return Ok("call_guard");
        }
        if Instant::now() >= deadline {
            return Ok("time_guard");
        }
        let mut progressed = worker.advance_work().await?;
        progressed |= worker.fire_due_timer().await?;
        if let Some(execution) = worker.admit().await? {
            eprintln!(
                "{}: executing {}",
                folder.file_name().unwrap().to_string_lossy(),
                execution.handler.name
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

async fn finish_arm(directory: &Path, arm: &Value, prepared: bool) -> Result<Value> {
    let name = arm["name"].as_str().ok_or("missing arm name")?;
    let folder = directory.join(name);
    let (engine, agents) = local_app(folder.join("probe.db")).await?;
    let (task, run, workspace) = validate(&engine, &agents, &folder, arm).await?;
    let stop = if prepared {
        "prepared"
    } else {
        drive(&engine, &agents, &folder, task, run).await?
    };
    let evidence = snapshot(&engine, &folder, run).await?;
    let state = engine.status(run).await?;
    let view = agents.actors()?.task(task).await?;
    let tools: Vec<_> = state
        .activities()
        .iter()
        .filter(|a| matches!(a.spec.handler.name.as_str(), "native.tool" | "agent.tool"))
        .collect();
    let names: Vec<&str> = tools
        .iter()
        .map(|a| {
            if name == "native" {
                a.spec.input["definition"]["id"]["name"]
                    .as_str()
                    .unwrap_or("")
            } else {
                a.spec.input["action"]["kind"].as_str().unwrap_or("")
            }
        })
        .collect();
    let tests = tools.iter().rev().find(|a| {
        if name == "native" {
            a.spec.input["definition"]["id"]["name"] == "run_tests"
        } else {
            a.spec.input["action"]["kind"] == "run_tests"
        }
    });
    let protected_unchanged = FIXTURE
        .iter()
        .filter(|(p, _)| *p != "src/lib.rs")
        .all(|(p, c)| fs::read_to_string(workspace.join(p)).ok().as_deref() == Some(*c));
    let tests_passed = tests.is_some_and(|a| {
        a.status == ActivityStatus::Succeeded
            && a.result.as_ref().is_some_and(|r| {
                r["ok"] == true && r["stdout"].as_str().is_some_and(|s| s.contains("3 passed"))
            })
    });
    let dispatch_verified = names == ["read_file", "replace_text", "run_tests"]
        && tools.iter().all(|a| {
            a.status == ActivityStatus::Succeeded
                && a.result.as_ref().is_some_and(|r| r["ok"] == true)
        });
    let verified = view.status == AgentTaskStatus::Succeeded
        && dispatch_verified
        && tests_passed
        && protected_unchanged;
    let report = json!({"name":name,"status":if prepared {"prepared"} else if verified {"verified"} else {"not_verified"},"stop":stop,
        "task":view,"workspace":workspace,"model_calls":state.record().usage.admitted_model_calls,
        "tool_sequence":names,"dispatch_verified":dispatch_verified,"tests_passed":tests_passed,"protected_unchanged":protected_unchanged,
        "requests":evidence["requests"],"tool_results":tools,"usage":state.record().usage});
    write(folder.join("report.json"), &report)?;
    Ok(report)
}

pub async fn run(options: Options) -> Result<Value> {
    if options.resume.is_some() && (options.target_config.is_some() || options.prepare_only) {
        return Err("resume cannot replace model pins or prepare again".into());
    }
    if options.prepare_only && options.target_config.is_none() {
        return Err("prepare-only requires target-config".into());
    }
    let mut server = None;
    let (directory, manifest) = if let Some(path) = options.resume {
        let directory = fs::canonicalize(path)?;
        let manifest = read(directory.join("manifest.json"))?;
        if manifest["version"] != 1
            || manifest["mode"] != "configured"
            || manifest["arms"].as_array().is_none_or(|a| {
                a.len() != 2 || a[0]["name"] != "native" || a[1]["name"] != "coding"
            })
        {
            return Err("resume requires an original configured probe manifest".into());
        }
        (directory, manifest)
    } else {
        let target: ModelTarget = if let Some(path) = &options.target_config {
            let value = read(path)?;
            serde_json::from_value(value.get("target").unwrap_or(&value).clone())?
        } else {
            let (target, fixture) = fixture_server().await?;
            server = Some(fixture);
            target
        };
        target.policy.validate()?;
        if target.provider != "openai" {
            return Err("probe requires an HTTP model target".into());
        }
        let directory = options.output.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(&directory)?;
        let directory = fs::canonicalize(directory)?;
        eprintln!("Probe directory: {}", directory.display());
        let native = prepare(&directory, "native", &target).await?;
        let coding = prepare(&directory, "coding", &target).await?;
        let manifest = json!({"version":1,"mode":if options.target_config.is_some(){"configured"}else{"fixture"},"arms":[native,coding]});
        write(directory.join("manifest.json"), &manifest)?;
        (directory, manifest)
    };
    // Separate databases prevent one arm's waiting task from executing in the other.
    let mut arms = vec![];
    for arm in manifest["arms"].as_array().ok_or("missing arms")? {
        arms.push(finish_arm(&directory, arm, options.prepare_only).await?);
    }
    let verified = arms.iter().all(|a| a["status"] == "verified");
    let native_contract =
        manifest["arms"][0]["pinned_config"]["input"]["model_contract_version"].as_u64();
    let native_description = "flat tool names and fields; normalized to registered native tools";
    let report = json!({"status":if options.prepare_only {"prepared"} else if verified {"verified"} else {"not_verified"},"directory":directory,"mode":manifest["mode"],"arms":arms,
        "comparison":{"native":native_description,"native_contract_version":native_contract,"coding":"flat read_file/replace_text/run_tests action kinds; existing coding policy","shared":"same fixture, goal, model target, HTTP adapter, outer action envelope and unlimited budgets"}});
    write(directory.join("report.json"), &report)?;
    drop(server);
    Ok(report)
}
