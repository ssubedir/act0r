//! Find a Rust function, reuse saved search across restart, repair, test, and review.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
};
type DemoResult<T> = Result<T, Box<dyn std::error::Error>>;
const ORIGINAL: &str = "pub fn answer() -> i64 { 19 - 23 }\n";
const REPAIRED: &str = "pub fn answer() -> i64 { 19 + 23 }\n";

async fn through_result(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
    tool: &str,
) -> DemoResult<(RunId, ActivityId, Value, Value)> {
    for _ in 0..100 {
        worker.step().await?;
        let view = agents.actors()?.task(task).await?;
        let Some(run) = view.run_id else {
            continue;
        };
        if let Some(wait) = view.wait {
            assert_eq!(wait.key, "native_tool_approval");
            assert!(matches!(
                wait.payload["tool"]["name"].as_str(),
                Some("replace_text" | "run_tests")
            ));
            // The example owns the fixture and approves only its pinned edit/test operations.
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await?;
        }
        let saved = engine.status(run).await?;
        if let Some(activity) = saved
            .activities()
            .iter()
            .find(|a| a.spec.input["definition"]["id"]["name"] == tool && a.result.is_some())
        {
            return Ok((
                run,
                activity.id,
                activity.result.clone().unwrap(),
                saved
                    .activities()
                    .last()
                    .ok_or("missing continuation")?
                    .spec
                    .input
                    .clone(),
            ));
        }
        if !saved.record().status.is_unfinished() {
            return Err(format!("demo stopped: {:?}", saved.record().error).into());
        }
    }
    Err(format!("{tool} result was not saved").into())
}
#[tokio::main]
async fn main() -> DemoResult<()> {
    let db = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/workspace-search-demo/demo.db".into()),
    );
    let parent = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let source = parent.join(format!("search-source-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(source.join("src"))?;
    fs::create_dir(source.join("tests"))?;
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"search_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )?;
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"search_fixture\"\nversion = \"0.1.0\"\n",
    )?;
    fs::write(source.join("src/lib.rs"), ORIGINAL)?;
    fs::write(source.join("README.md"), "Search demo project\n")?;
    fs::write(
        source.join("tests/contract.rs"),
        "#[test]\nfn fixed_contract() { assert_eq!(search_fixture::answer(), 42); }\n",
    )?;
    let (engine, agents) = local_app(&db).await?;
    let reviewer = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
        "goal":"Review the saved repair evidence","target":{"provider":"scripted","model":"reviewer","endpoint":""},
        "max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"finish","summary":"Reviewed the function and passing tests."}]
    }),4,Default::default()).await?;
    let config = json!({
        "goal":"Find the answer function, repair it, test, and review","target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":6,"max_tokens":1000000,"max_output_tokens":512,
        "context":{"max_input_bytes":20000,"max_observations":3},
        "goal_budget":{"max_model_calls":7,"max_tokens":1000000},
        "workers":[{"name":"reviewer","agent_id":reviewer,"description":"Reviews supplied saved evidence"}],
        "delegation":{"max_tasks":1,"max_depth":1},
        "tools":[{"name":"search_text","version":1},{"name":"read_file","version":2},{"name":"replace_text","version":1},{"name":"run_tests","version":1}],
        "writable_paths":["src/lib.rs"],
        "test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000},
        "script":[{"kind":"call_tool","tool":"search_text","arguments":{"query":"fn answer","path":"src"}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"19 - 23","new":"19 + 23"}},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},
            {"kind":"delegate_task","worker":"reviewer","goal":"Review saved function repair src/lib.rs:19 + 23 and the passing fixed tests."},
            {"kind":"finish","summary":"Found, repaired, tested, and reviewed the function."}]
    });
    let config_path = db.with_extension("planner.json");
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source.clone()),
            config,
            18,
            Default::default(),
        )
        .await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Find the answer function, repair it, test, and review."}),
        })
        .await?;
    let workspace = PathBuf::from(
        agents.actors()?.status(actor).await?.record().config["input"]["workspace"]
            .as_str()
            .ok_or("missing managed root")?,
    );
    let mut worker = engine.worker().await?;
    let (run, search, original, next) =
        through_result(&engine, &agents, &mut worker, task, "search_text").await?;
    assert_eq!(original["truncated"], false);
    assert_eq!(
        original["matches"]
            .as_array()
            .ok_or("missing matches")?
            .len(),
        1
    );
    assert_eq!(original["matches"][0]["path"], "src/lib.rs");
    assert_eq!(original["matches"][0]["snippet"], ORIGINAL.trim_end());
    // Add a current match after the saved search: reopening must not refresh its result.
    fs::write(
        workspace.join("src/later.txt"),
        "fn answer added after saved search\n",
    )?;
    let usage = json!(engine.status(run).await?.record().usage);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let mut worker = engine.worker().await?;
    let restored = engine.status(run).await?;
    assert_eq!(
        restored
            .activities()
            .last()
            .ok_or("missing saved next request")?
            .spec
            .input,
        next
    );
    assert_eq!(json!(restored.record().usage), usage);
    let (_, tests, passing, next) =
        through_result(&engine, &agents, &mut worker, task, "run_tests").await?;
    assert_eq!(passing["ok"], true);
    assert_eq!(passing["exit_code"], 0);
    assert_eq!(fs::read_to_string(workspace.join("src/lib.rs"))?, REPAIRED);
    let usage = json!(engine.status(run).await?.record().usage);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let mut worker = engine.worker().await?;
    let restored = engine.status(run).await?;
    assert_eq!(
        restored
            .activities()
            .last()
            .ok_or("missing saved test continuation")?
            .spec
            .input,
        next
    );
    assert_eq!(json!(restored.record().usage), usage);
    for _ in 0..100 {
        if !worker.step().await? && !worker.has_active_runs().await? {
            break;
        }
    }
    let view = agents.actors()?.task(task).await?;
    assert_eq!(view.status, AgentTaskStatus::Succeeded);
    let saved = engine.status(run).await?;
    assert_eq!(
        saved
            .activities()
            .iter()
            .find(|a| a.id == search)
            .ok_or("missing saved search")?
            .result,
        Some(original)
    );
    assert_eq!(
        saved
            .attempts()
            .iter()
            .filter(|a| a.activity_id == search)
            .count(),
        1
    );
    assert_eq!(
        saved
            .attempts()
            .iter()
            .filter(|a| a.activity_id == tests)
            .count(),
        1
    );
    assert_eq!(
        saved
            .activities()
            .iter()
            .filter(|a| a.spec.input["definition"]["id"]["name"] == "search_text")
            .count(),
        1
    );
    assert_eq!(fs::read_to_string(source.join("src/lib.rs"))?, ORIGINAL);
    assert_eq!(
        saved.record().state["state"]["context"]["test_status"]["stale"],
        false
    );
    let inspection = agents
        .actors()?
        .inspect_task(task, Default::default())
        .await?;
    assert_eq!(inspection.summary.visible_usage.admitted_model_calls, 7);
    assert_eq!(inspection.summary.succeeded, 2);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"restart_count":2,"saved_search_reused":true,
        "search_activity":search,"search_attempts":1,"passing_exit_code":passing["exit_code"],"test_executions":1,
        "model_calls":inspection.summary.visible_usage.admitted_model_calls,"status":view.status,"task":task,"run":run,
        "planner_config":config_path}))?
    );
    Ok(())
}
