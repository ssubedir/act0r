//! A real Rust repair, saved review, two restarts, explicit approval, and project application.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, patch_application::ApplicationStatus, *},
};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
async fn finish_task(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
) -> Result<()> {
    for _ in 0..200 {
        worker.step().await?;
        let view = agents.actors()?.task(task).await?;
        if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
            // The example owns this fixture and approves its pinned edits and test command.
            if wait.key != "native_tool_approval" {
                return Err("unexpected task approval".into());
            }
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await?;
        }
        if view.status.is_terminal() {
            return Ok(());
        }
    }
    Err("actor task did not finish".into())
}
async fn activity(worker: &mut Worker) -> Result<ActivityExecution> {
    for _ in 0..100 {
        worker.advance_work().await?;
        if let Some(call) = worker.admit().await? {
            return Ok(call);
        }
    }
    Err("application activity was not admitted".into())
}
#[tokio::main]
async fn main() -> Result<()> {
    let db = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/workspace-apply-demo/demo.db".into()),
    );
    let parent = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let source = parent.join(format!("apply-source-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(source.join("src"))?;
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"apply_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )?;
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"apply_fixture\"\nversion = \"0.1.0\"\n",
    )?;
    let original = "pub fn answer() -> i64 { 19 - 23 }\n#[cfg(test)]\nmod tests { #[test] fn contract() { assert_eq!(super::answer(), 42); } }\n";
    fs::write(source.join("src/lib.rs"), original)?;
    let source = fs::canonicalize(source)?;
    let (engine, agents) = local_app(&db).await?;
    let actor = agents.create_registered_actor(DefinitionId::new("tool",1),Some(source.clone()),json!({
        "goal":"Repair and test the answer","target":{"provider":"scripted","model":"apply","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,"change_review":true,
        "tools":[{"name":"replace_text","version":1},{"name":"create_file","version":1},{"name":"run_tests","version":1}],
        "writable_paths":["src/"],"test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000},
        "script":[{"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"19 - 23","new":"19 + 23"}},
            {"kind":"call_tool","tool":"create_file","arguments":{"path":"src/notes.txt","content":"Reviewed repair.\n"}},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},{"kind":"finish","summary":"Repaired and tested."}]
    }),30,Default::default()).await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Repair and test"}),
        })
        .await?;
    let mut worker = engine.worker().await?;
    finish_task(&engine, &agents, &mut worker, task).await?;
    let changes = agents
        .actors()?
        .task_changes(task)
        .await?
        .changes
        .ok_or("missing reviewed artifact")?;
    assert_eq!(
        changes
            .tests
            .as_ref()
            .ok_or("missing test evidence")?
            .result["ok"],
        true
    );
    assert_eq!(fs::read_to_string(source.join("src/lib.rs"))?, original);
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let applications = agents.actors()?.patch_applications()?;
    let application = applications.preview(task, source.clone()).await?;
    let mut worker = engine.worker().await?;
    let prepare = activity(&mut worker).await?;
    worker
        .complete(prepare.clone(), worker.execute(prepare).await?)
        .await?;
    let saved = applications.status(application).await?;
    assert_eq!(saved.status, RunStatus::Waiting);
    assert!(
        !saved
            .preview
            .as_ref()
            .ok_or("missing preview")?
            .has_conflicts()
    );
    assert_eq!(fs::read_to_string(source.join("src/lib.rs"))?, original);
    drop(worker);
    drop(applications);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let applications = agents.actors()?.patch_applications()?;
    assert_eq!(json!(saved), json!(applications.status(application).await?));
    // Explicit approval of this example's owned project, after inspecting its saved preview.
    applications.approve(application, true).await?;
    let mut worker = engine.worker().await?;
    let write = activity(&mut worker).await?;
    let result = worker.execute(write.clone()).await?;
    worker.complete(write.clone(), result.clone()).await?;
    worker.complete(write, result).await?;
    let completed = applications.status(application).await?;
    assert_eq!(
        completed
            .report
            .as_ref()
            .ok_or("missing application receipt")?
            .status,
        ApplicationStatus::Applied
    );
    assert_eq!(
        fs::read_to_string(source.join("src/lib.rs"))?,
        original.replace("19 - 23", "19 + 23")
    );
    assert_eq!(
        fs::read_to_string(source.join("src/notes.txt"))?,
        "Reviewed repair.\n"
    );
    assert_eq!(
        engine
            .status(application)
            .await?
            .record()
            .usage
            .admitted_model_calls,
        0
    );
    // Independently check the resulting target project; this is demonstration verification.
    let mut command = Command::new("cargo");
    command
        .args(["test", "--locked", "--offline"])
        .current_dir(&source);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let checked = command.output()?;
    if !checked.status.success() {
        return Err(String::from_utf8_lossy(&checked.stderr).into_owned().into());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"task_id":task,"application_run_id":application,"target":source,"changed_files":changes.files.len(),"test_status":changes.test_status,"target_tests_ok":true,"restarts":2,"model_calls":4,"application_model_calls":0,"status":completed.report.ok_or("missing receipt")?.status})
        )?
    );
    Ok(())
}
