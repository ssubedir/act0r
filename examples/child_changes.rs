//! Delegate a real Rust repair, restart at durable import approval, integrate, and verify.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, change_review::TestEvidenceStatus, child_changes::*, *},
};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
async fn drive(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    child: AgentId,
    task: MessageId,
    stop_import: bool,
) -> Result<()> {
    for _ in 0..300 {
        worker.step().await?;
        for task in agents.actors()?.tasks(child).await? {
            if let (Some(run), Some(wait)) = (task.run_id, task.wait) {
                if wait.key != "native_tool_approval" {
                    return Err("unexpected worker wait".into());
                }
                engine
                    .signal(run, wait.id, json!({"approved":true}))
                    .await?;
            }
        }
        let view = agents.actors()?.task(task).await?;
        if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
            if wait.key == APPROVAL_KEY && stop_import {
                return Ok(());
            }
            if wait.key != "native_tool_approval" {
                return Err("unexpected planner wait".into());
            }
            // This example owns the fixture and its pinned Cargo test command.
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await?;
        }
        if view.status.is_terminal() {
            return Ok(());
        }
    }
    Err("planner did not finish".into())
}
#[tokio::main]
async fn main() -> Result<()> {
    let db = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/child-changes-demo/demo.db".into()),
    );
    let parent = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let source = parent.join(format!("handoff-source-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(source.join("src"))?;
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"handoff_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )?;
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"handoff_fixture\"\nversion = \"0.1.0\"\n",
    )?;
    let original = "pub fn answer() -> i64 { 19 - 23 }\n#[cfg(test)]\nmod tests { #[test] fn contract() { assert_eq!(super::answer(), 42); } }\n";
    fs::write(source.join("src/lib.rs"), original)?;
    let source = fs::canonicalize(source)?;
    let (engine, agents) = local_app(&db).await?;
    let test_command = json!({"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000});
    let child=agents.create_registered_actor(DefinitionId::new("tool",1),Some(source.clone()),json!({
        "goal":"Repair and verify", "target":{"provider":"scripted","model":"repairer","endpoint":""},
        "max_model_calls":4,"max_tokens":1000000,"max_output_tokens":512,"change_review":true,
        "tools":[{"name":"replace_text","version":1},{"name":"create_file","version":1},{"name":"run_tests","version":1}],
        "writable_paths":["src/"],"test_command":test_command,
        "script":[{"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"19 - 23","new":"19 + 23"}},
            {"kind":"call_tool","tool":"create_file","arguments":{"path":"src/notes.txt","content":"Worker repair evidence\n"}},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},{"kind":"finish","summary":"Repaired and tested."}]
    }),40,Default::default()).await?;
    let planner=agents.create_registered_actor(DefinitionId::new("planner",3),Some(source.clone()),json!({
        "goal":"Delegate, integrate, verify and deliver", "target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":5,"max_tokens":1000000,"max_output_tokens":512,
        "workers":[{"name":"repairer","agent_id":child,"description":"Repair Rust code and publish a patch."}],"delegation":{"max_tasks":1,"max_depth":2},
        "change_review":true,"child_changes":true,"writable_paths":["src/"],
        "tools":[{"name":"run_tests","version":1}],"test_command":test_command,
        "script":[{"kind":"delegate_task","worker":"repairer","goal":"Repair and verify the answer"},
            {"kind":"inspect_child_changes","worker":"repairer","offset":0},
            {"kind":"import_child_changes","worker":"repairer"},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},
            {"kind":"finish","summary":"Integrated and verified the worker repair."}]
    }),40,Default::default()).await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: planner,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Repair this project"}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    drive(&engine, &agents, &mut worker, child, task, true).await?;
    let view = agents.actors()?.task(task).await?;
    let run_id = view.run_id.ok_or("missing planner run")?;
    let wait = view.wait.ok_or("missing durable approval")?;
    let inspection: Inspection = serde_json::from_value(wait.payload["inspection"].clone())?;
    assert_eq!(inspection.preview.test_status, TestEvidenceStatus::Matching);
    assert_eq!(
        inspection
            .preview
            .tests
            .as_ref()
            .ok_or("missing child tests")?
            .result["ok"],
        true
    );
    println!(
        "Child {} published {} files; planner waits at {}.",
        inspection.task_id,
        inspection.preview.files.len(),
        wait.id
    );
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let saved = agents.actors()?.task(task).await?;
    assert_eq!(
        saved.wait.as_ref().ok_or("approval lost on restart")?.id,
        wait.id
    );
    engine
        .signal(run_id, wait.id, json!({"approved":true}))
        .await?;
    let mut worker = engine.worker().await?;
    drive(&engine, &agents, &mut worker, child, task, false).await?;
    let final_run = engine.status(run_id).await?;
    assert_eq!(final_run.record().status, RunStatus::Succeeded);
    let artifact = agents
        .actors()?
        .task_changes(task)
        .await?
        .changes
        .ok_or("missing combined patch")?;
    assert_eq!(artifact.test_status, TestEvidenceStatus::Matching);
    assert_eq!(
        artifact
            .tests
            .as_ref()
            .ok_or("missing parent tests")?
            .result["ok"],
        true
    );
    assert_eq!(fs::read_to_string(source.join("src/lib.rs"))?, original);
    let output = parent.join(format!("combined-{task}.patch"));
    fs::write(&output, artifact.patch())?;
    println!(
        "Restarted, approved, imported and passed combined Rust tests. Planner: {} calls; child: 4 calls.",
        final_run.record().usage.admitted_model_calls
    );
    println!(
        "Final artifact: {} changed files, tests {:?}. Patch: {}",
        artifact.files.len(),
        artifact.test_status,
        fs::canonicalize(output)?.display()
    );
    println!(
        "Source remains at {}. Apply the final artifact explicitly with actor task preview-changes {} --target PROJECT.",
        source.display(),
        task
    );
    Ok(())
}
