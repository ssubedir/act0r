//! Long native repair with bounded context, real tests, and restart at both test results.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, *},
};
use serde_json::{Value, json};
use std::{fs, path::PathBuf};

async fn through_test(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
    count: usize,
) -> Result<(RunId, Value), Box<dyn std::error::Error>> {
    for _ in 0..100 {
        worker.step().await?;
        let view = agents.actors()?.task(task).await?;
        if let Some(wait) = view.wait {
            assert_eq!(wait.key, "native_tool_approval");
            // This demo owns the exact fixture and explicitly approves its pinned calls.
            engine
                .signal(
                    view.run_id.ok_or("missing Run")?,
                    wait.id,
                    json!({"approved":true}),
                )
                .await?;
        }
        if let Some(run) = view.run_id {
            let saved = engine.status(run).await?;
            let tests: Vec<_> = saved
                .activities()
                .iter()
                .filter(|a| {
                    a.spec.input["definition"]["id"]["name"] == "run_tests" && a.result.is_some()
                })
                .collect();
            if tests.len() == count {
                return Ok((run, tests.last().unwrap().result.clone().unwrap()));
            }
        }
    }
    Err("test result was not reached".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/workspace-tests-demo/demo.db".into()),
    );
    let parent = database
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    fs::create_dir_all(parent)?;
    let source = parent.join(format!("test-source-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(source.join("src"))?;
    fs::create_dir(source.join("tests"))?;
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"native_test_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )?;
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"native_test_fixture\"\nversion = \"0.1.0\"\n",
    )?;
    fs::write(source.join("src/lib.rs"), "pub fn value() -> u32 { 0 }\n")?;
    fs::write(
        source.join("EVIDENCE.md"),
        "bounded evidence 🙂\\\n".repeat(140),
    )?;
    fs::write(
        source.join("tests/contract.rs"),
        "#[test]\nfn fixed_contract() { assert_eq!(native_test_fixture::value(), 2); }\n",
    )?;
    let (engine, agents) = local_app(&database).await?;
    let reviewer=agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
        "goal":"Review supplied test evidence","target":{"provider":"scripted","model":"reviewer","endpoint":""},
        "max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"finish","summary":"Reviewed the saved passing test evidence."}]
    }),4,Default::default()).await?;
    let evidence =
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"EVIDENCE.md"}});
    let mut script = vec![evidence.clone(); 10];
    script.extend([
        json!({"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"{ 0 }","new":"{ 1 }"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
        json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"{ 1 }","new":"{ 2 }"}}),
        json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
    ]);
    script.extend(vec![evidence; 10]);
    script.extend([
        json!({"kind":"delegate_task","worker":"reviewer","goal":"Review the passing test status retained by the planner after its test output left the window."}),
        json!({"kind":"finish","summary":"Repaired, tested, and reviewed the project."}),
    ]);
    let config = json!({
        "goal":"Repair and test the project","target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":27,"max_tokens":1000000,"max_output_tokens":512,
        "context":{"version":1,"max_input_bytes":10000,"max_observations":2},
        "goal_budget":{"max_model_calls":28,"max_tokens":1000000},
        "workers":[{"name":"reviewer","agent_id":reviewer,"description":"Review saved test evidence"}],
        "delegation":{"max_tasks":1,"max_depth":1},
        "tools":[{"name":"read_file","version":2},{"name":"replace_text","version":1},{"name":"run_tests","version":1}],
        "writable_paths":["src/lib.rs"],
        "test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000},
        "script":script
    });
    let config_path = database.with_extension("planner.json");
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    let actor = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source.clone()),
            config,
            64,
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
            payload: json!({"goal":"Repair, test, and review the project."}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let (run, failed) = through_test(&engine, &agents, &mut worker, task, 1).await?;
    assert_eq!(failed["ok"], false);
    assert_ne!(failed["exit_code"], 0);
    assert!(
        failed["stdout"]
            .as_str()
            .ok_or("missing stdout")?
            .contains("fixed_contract")
    );
    let saved = engine.status(run).await?;
    let next = saved.activities().last().unwrap().spec.input.clone();
    let usage = json!(saved.record().usage);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await?;
    let mut worker = engine.worker().await?;
    let saved = engine.status(run).await?;
    assert_eq!(saved.activities().last().unwrap().spec.input, next);
    assert_eq!(json!(saved.record().usage), usage);
    let (_, passed) = through_test(&engine, &agents, &mut worker, task, 2).await?;
    assert_eq!(passed["ok"], true);
    assert_eq!(passed["exit_code"], 0);
    let saved = engine.status(run).await?;
    let next = saved.activities().last().unwrap().spec.input.clone();
    let usage = json!(saved.record().usage);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&database).await?;
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
    let tests: Vec<_> = saved
        .activities()
        .iter()
        .filter(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
        .collect();
    assert_eq!(tests.len(), 2);
    let context = &saved.record().state["state"]["context"];
    assert_eq!(context["test_status"]["passed"], true);
    assert_eq!(context["test_status"]["stale"], false);
    assert_eq!(context["test_status"]["activity_id"], json!(tests[1].id));
    assert!(
        context["selection"]["omitted_observations"]
            .as_u64()
            .unwrap()
            >= 24
    );
    assert!(context["observations"].as_array().unwrap().len() <= 2);
    assert!(
        !context["observations"]
            .to_string()
            .contains("fixed_contract")
    );
    for model in saved
        .activities()
        .iter()
        .filter(|a| a.spec.handler.name == "agent.model")
    {
        let request: agent::ModelRequest = serde_json::from_value(model.spec.input.clone())?;
        let selection = request.context.as_ref().ok_or("missing selection")?;
        assert_eq!(selection.input_bytes, request.context_input_bytes());
        assert!(selection.input_bytes <= 10000);
        assert!(selection.included_observations <= 2);
    }
    for activity in &tests {
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == activity.id)
                .count(),
            1
        );
    }
    assert_eq!(
        fs::read_to_string(source.join("src/lib.rs"))?,
        "pub fn value() -> u32 { 0 }\n"
    );
    let child = saved
        .record()
        .delegations
        .first()
        .ok_or("missing review")?
        .id;
    assert_eq!(
        agents.actors()?.task(child).await?.status,
        AgentTaskStatus::Succeeded
    );
    let tree = agents
        .actors()?
        .inspect_task(task, Default::default())
        .await?;
    assert_eq!(tree.summary.visible_usage.admitted_model_calls, 28);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"task":task,"review_task":child,"status":view.status,
        "restart_verified":true,"test_executions":2,"model_calls":28,"context":context["selection"],"test_status":context["test_status"],"failed_exit_code":failed["exit_code"],
        "passed_exit_code":passed["exit_code"],"failed_elapsed_ms":failed["elapsed_ms"],"passed_elapsed_ms":passed["elapsed_ms"],
        "planner_config":config_path})
        )?
    );
    Ok(())
}
