//! Create a missing Rust module, recover an uncommitted creation, test, and review.
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
const MODULE: &str = "pub fn add(left: i32, right: i32) -> i32 { left + right }\n";
const LIB: &str = "// No module yet.\n";

async fn until_creation_wait(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
) -> DemoResult<(RunId, HumanWait)> {
    for _ in 0..100 {
        worker.step().await?;
        let view = agents.actors()?.task(task).await?;
        if let Some(wait) = view.wait {
            let run = view.run_id.ok_or("missing Run")?;
            if wait.payload["tool"]["name"] == "create_file" {
                return Ok((run, wait));
            }
            assert_eq!(wait.payload["tool"]["name"], "run_tests");
            // This example owns the fixture and approves its fixed test command.
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await?;
        }
    }
    Err("creation approval was not reached".into())
}
async fn through_passing_tests(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
    run: RunId,
) -> DemoResult<Value> {
    for _ in 0..100 {
        worker.step().await?;
        let view = agents.actors()?.task(task).await?;
        if let Some(wait) = view.wait {
            assert_eq!(wait.key, "native_tool_approval");
            // Approvals apply only to this owned demo's exact pinned edit/test calls.
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await?;
        }
        let saved = engine.status(run).await?;
        let tests: Vec<_> = saved
            .activities()
            .iter()
            .filter(|a| {
                a.spec.input["definition"]["id"]["name"] == "run_tests" && a.result.is_some()
            })
            .collect();
        if tests.len() == 2 {
            return Ok(tests[1].result.clone().unwrap());
        }
        if !saved.record().status.is_unfinished() {
            return Err(format!("demo stopped: {:?}", saved.record().error).into());
        }
    }
    Err("passing tests were not reached".into())
}

#[tokio::main]
async fn main() -> DemoResult<()> {
    let db = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/workspace-create-demo/demo.db".into()),
    );
    let parent = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let source = parent.join(format!("create-source-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(source.join("src"))?;
    fs::create_dir(source.join("tests"))?;
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"creation_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )?;
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"creation_fixture\"\nversion = \"0.1.0\"\n",
    )?;
    fs::write(source.join("src/lib.rs"), LIB)?;
    fs::write(
        source.join("tests/contract.rs"),
        "#[test]\nfn fixed_contract() { assert_eq!(creation_fixture::arithmetic::add(19, 23), 42); }\n",
    )?;
    let (engine, agents) = local_app(&db).await?;
    let reviewer = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
        "goal":"Review created module and saved test evidence","target":{"provider":"scripted","model":"reviewer","endpoint":""},
        "max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"finish","summary":"Reviewed the created module and passing test evidence."}]
    }),4,Default::default()).await?;
    let config = json!({
        "goal":"Create the missing arithmetic module, test, and review","target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":7,"max_tokens":1000000,"max_output_tokens":512,
        "context":{"max_input_bytes":16000,"max_observations":2},
        "goal_budget":{"max_model_calls":8,"max_tokens":1000000},
        "workers":[{"name":"reviewer","agent_id":reviewer,"description":"Review supplied module and test evidence"}],
        "delegation":{"max_tasks":1,"max_depth":1},
        "tools":[{"name":"create_file","version":1},{"name":"replace_text","version":1},{"name":"run_tests","version":1},{"name":"read_file","version":2}],
        "writable_paths":["src/arithmetic.rs","src/lib.rs"],
        "test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000},
        "script":[
            {"kind":"call_tool","tool":"run_tests","arguments":{}},
            {"kind":"call_tool","tool":"create_file","arguments":{"path":"src/arithmetic.rs","content":MODULE}},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":LIB,"new":"pub mod arithmetic;\n"}},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"src/arithmetic.rs"}},
            {"kind":"delegate_task","worker":"reviewer","goal":"Review the saved passing test status and the created arithmetic module: pub fn add(left: i32, right: i32) -> i32 { left + right }"},
            {"kind":"finish","summary":"Created, tested, and reviewed the missing module."}]
    });
    let config_path = db.with_extension("planner.json");
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source.clone()),
            config,
            22,
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
            payload: json!({"goal":"Create the missing arithmetic module, test, and review."}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let (run, wait) = until_creation_wait(&engine, &agents, &mut worker, task).await?;
    let before = engine.status(run).await?;
    let failed = before
        .activities()
        .iter()
        .find(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
        .ok_or("missing initial tests")?
        .result
        .clone()
        .ok_or("missing initial failure")?;
    assert_eq!(failed["ok"], false);
    assert_eq!(failed["exit_code"], 101);
    engine
        .signal(run, wait.id, json!({"approved":true}))
        .await?;
    let creation = worker.admit().await?.ok_or("missing creation")?;
    let output = worker.execute(creation.clone()).await?;
    assert_eq!(
        output.result().ok_or("missing creation result")?["already_applied"],
        false
    );
    let workspace = PathBuf::from(
        creation.input["workspace"]
            .as_str()
            .ok_or("missing workspace")?,
    );
    let target = workspace.join("src/arithmetic.rs");
    let modified = fs::metadata(&target)?.modified()?;
    let journal = workspace.join(format!(".act0r-tools/{}.json", creation.activity_id));
    let journal_bytes = fs::read(&journal)?;
    // Deliberately stop after file publication, before its result commits to SQLite.
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let mut worker = engine.worker().await?;
    assert_eq!(
        engine.status(run).await?.record().status,
        RunStatus::Blocked
    );
    assert!(worker.admit().await?.is_none());
    assert_eq!(fs::read_to_string(&target)?, MODULE);
    // The fixture owns the stopped worker and has inspected its exact created file.
    engine.reconcile(run,ReconciliationRequest {activity_id:creation.activity_id,attempt_id:creation.attempt_id,
        decision:ReconciliationDecision::Retry {},evidence:"Stopped worker; verified the exact created arithmetic module and authorize journal verification.".into()}).await?;
    let retry = worker.admit().await?.ok_or("missing authorized retry")?;
    assert_eq!(retry.activity_id, creation.activity_id);
    assert_eq!(retry.input, creation.input);
    assert_ne!(retry.attempt_id, creation.attempt_id);
    let output = worker.execute(retry.clone()).await?;
    assert_eq!(
        output.result().ok_or("missing retry result")?["already_applied"],
        true
    );
    assert_eq!(fs::metadata(&target)?.modified()?, modified);
    assert_eq!(fs::read(&journal)?, journal_bytes);
    worker.complete(retry, output).await?;
    assert_eq!(
        engine.status(run).await?.record().state["state"]["context"]["test_status"]["stale"],
        true
    );
    let passed = through_passing_tests(&engine, &agents, &mut worker, task, run).await?;
    assert_eq!(passed["ok"], true);
    assert_eq!(passed["exit_code"], 0);
    let saved = engine.status(run).await?;
    assert_eq!(
        saved.record().state["state"]["context"]["test_status"]["stale"],
        false
    );
    let next = saved.activities().last().unwrap().spec.input.clone();
    let usage = json!(saved.record().usage);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let mut worker = engine.worker().await?;
    let saved = engine.status(run).await?;
    assert_eq!(saved.activities().last().unwrap().spec.input, next);
    assert_eq!(json!(saved.record().usage), usage);
    for _ in 0..100 {
        if !worker.step().await? && !worker.has_active_runs().await? {
            break;
        }
    }
    let view = agents.actors()?.task(task).await?;
    assert_eq!(view.status, AgentTaskStatus::Succeeded);
    let saved = engine.status(run).await?;
    let creations: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.input["definition"]["id"]["name"] == "create_file")
        .collect();
    assert_eq!(creations.len(), 1);
    assert_eq!(
        saved
            .attempts()
            .iter()
            .filter(|a| a.activity_id == creation.activity_id)
            .count(),
        2
    );
    let tests: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
        .collect();
    assert_eq!(tests.len(), 2);
    for test in &tests {
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == test.id)
                .count(),
            1
        );
    }
    let read = saved
        .activities()
        .iter()
        .find(|a| a.spec.input["definition"]["id"]["name"] == "read_file")
        .ok_or("missing read")?;
    assert_eq!(
        read.result.as_ref().ok_or("missing read result")?["text"],
        MODULE
    );
    assert_eq!(fs::read_to_string(source.join("src/lib.rs"))?, LIB);
    assert!(!source.join("src/arithmetic.rs").exists());
    let tree = agents
        .actors()?
        .inspect_task(task, Default::default())
        .await?;
    assert_eq!(tree.summary.visible_usage.admitted_model_calls, 8);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"task":task,"run":run,"status":view.status,"restart_count":2,
        "creation_activity":creation.activity_id,"creation_attempts":2,"created_file_rewritten_on_retry":false,"reconciliation_verified":true,
        "failed_exit_code":failed["exit_code"],"passed_exit_code":passed["exit_code"],"test_executions":2,"model_calls":8,"planner_config":config_path})
        )?
    );
    Ok(())
}
