//! Search, repair, run real Rust tests, and export a task's saved patch across two restarts.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, change_review::*, *},
};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
};
type DemoResult<T> = Result<T, Box<dyn std::error::Error>>;
async fn through(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
    target: &str,
) -> DemoResult<ActivityExecution> {
    for _ in 0..200 {
        worker.advance_work().await?;
        if let Some(call) = worker.admit().await? {
            if call.handler == handler(target) {
                return Ok(call);
            }
            let result = worker.execute(call.clone()).await?;
            worker.complete(call, result).await?;
        }
        let task = agents.actors()?.task(task).await?;
        if let (Some(run), Some(wait)) = (task.run_id, task.wait) {
            // This demo owns the tiny fixture and approves its fixed edit/test operations.
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await?;
        }
    }
    Err(format!("{target} checkpoint was not reached").into())
}
#[tokio::main]
async fn main() -> DemoResult<()> {
    let db = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/workspace-changes-demo/demo.db".into()),
    );
    let parent = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let source = parent.join(format!("review-source-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(source.join("src"))?;
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"review_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )?;
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"review_fixture\"\nversion = \"0.1.0\"\n",
    )?;
    let original = "pub fn answer() -> i64 { 19 - 23 }\n#[cfg(test)]\nmod tests { #[test] fn contract() { assert_eq!(super::answer(), 42); } }\n";
    fs::write(source.join("src/lib.rs"), original)?;
    let (engine, agents) = local_app(&db).await?;
    let actor=agents.create_registered_actor(DefinitionId::new("tool",1),Some(source.clone()),json!({
        "goal":"Find and repair answer(), test, and save a patch","target":{"provider":"scripted","model":"review","endpoint":""},
        "max_model_calls":5,"max_tokens":1000000,"max_output_tokens":512,"change_review":true,
        "tools":[{"name":"search_text","version":1},{"name":"replace_text","version":1},{"name":"create_file","version":1},{"name":"run_tests","version":1}],
        "writable_paths":["src/"],"test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000},
        "script":[{"kind":"call_tool","tool":"search_text","arguments":{"query":"fn answer","path":"src"}},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"19 - 23","new":"19 + 23"}},
            {"kind":"call_tool","tool":"create_file","arguments":{"path":"src/notes.txt","content":"Repaired addition and checked the contract.\n"}},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},
            {"kind":"finish","summary":"Repaired and tested the saved changes."}]
    }),30,Default::default()).await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Repair and export a tested patch."}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    let baseline = through(&engine, &agents, &mut worker, task, "baseline").await?;
    worker
        .complete(baseline.clone(), worker.execute(baseline.clone()).await?)
        .await?;
    let next = engine
        .status(baseline.run_id)
        .await?
        .activities()
        .last()
        .ok_or("missing first model request")?
        .spec
        .input
        .clone();
    assert_eq!(
        engine
            .status(baseline.run_id)
            .await?
            .record()
            .usage
            .admitted_model_calls,
        0
    );
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let mut worker = engine.worker().await?;
    assert_eq!(
        engine
            .status(baseline.run_id)
            .await?
            .activities()
            .last()
            .ok_or("missing saved first model request")?
            .spec
            .input,
        next
    );
    let final_call = through(&engine, &agents, &mut worker, task, "changes").await?;
    worker
        .complete(
            final_call.clone(),
            worker.execute(final_call.clone()).await?,
        )
        .await?;
    let saved = agents.actors()?.task_changes(task).await?;
    let artifact = saved.changes.as_ref().ok_or("missing saved changes")?;
    assert_eq!(artifact.test_status, TestEvidenceStatus::Matching);
    assert_eq!(
        artifact.tests.as_ref().ok_or("missing tests")?.result["ok"],
        true
    );
    assert_eq!(artifact.files.len(), 2);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let mut worker = engine.worker().await?;
    for _ in 0..50 {
        if !worker.step().await? && !worker.has_active_runs().await? {
            break;
        }
    }
    let restored = agents.actors()?.task_changes(task).await?;
    assert_eq!(json!(saved), json!(restored));
    let artifact = restored.changes.ok_or("missing restored changes")?;
    let output = db.with_extension("patch");
    fs::write(&output, artifact.patch())?;
    assert_eq!(fs::read_to_string(source.join("src/lib.rs"))?, original);
    let run = engine.status(final_call.run_id).await?;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(
        run.activities()
            .iter()
            .filter(|a| a.spec.input["definition"]["id"]["name"] == "run_tests")
            .count(),
        1
    );
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"task":task,"run":final_call.run_id,"patch":output,"source":source,"changed_files":artifact.files.len(),"test_status":artifact.test_status,"tests_ok":true,"restarts":2,"model_calls":run.record().usage.admitted_model_calls,"source_unchanged":true,"status":run.record().status})
        )?
    );
    Ok(())
}
