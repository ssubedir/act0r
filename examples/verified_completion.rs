//! A JSON-configured team repairs modules; a refused combined finish drives a fresh repair.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{
        actors::*, child_changes::APPROVAL_KEY, completion::CompletionStatus, teams::TeamSpec, *,
    },
};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn drive(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    actors: &[AgentId],
    task: MessageId,
    stop_on_refusal: bool,
    overlapping: &mut bool,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        for actor in actors {
            for view in agents.actors()?.tasks(*actor).await? {
                if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
                    if wait.key != "native_tool_approval" && wait.key != APPROVAL_KEY {
                        return Err("unexpected wait".into());
                    }
                    // This demo owns the fixture, edit permissions, and pinned Cargo commands.
                    engine
                        .signal(run, wait.id, json!({"approved":true}))
                        .await?;
                }
                if view.status == AgentTaskStatus::Blocked {
                    return Err(
                        "an interrupted unsafe operation needs operator reconciliation".into(),
                    );
                }
            }
        }
        let mut progressed = worker.advance_work().await?;
        if let Some(first) = worker.admit().await? {
            if let Some(second) = worker.admit().await? {
                assert_ne!(first.run_id, second.run_id);
                *overlapping = true;
                let (a, b) = tokio::join!(
                    worker.execute(first.clone()),
                    worker.execute(second.clone())
                );
                worker.complete(first, a?).await?;
                worker.complete(second, b?).await?;
            } else {
                let outcome = worker.execute(first.clone()).await?;
                worker.complete(first, outcome).await?;
            }
            progressed = true;
        }
        let saved = agents.actors()?.task_changes(task).await?;
        if stop_on_refusal
            && saved
                .completion
                .as_ref()
                .is_some_and(|r| r.status == CompletionStatus::Refused)
        {
            return Ok(());
        }
        if agents.actors()?.task(task).await?.status.is_terminal() {
            return Ok(());
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    Err("demo timed out".into())
}
#[tokio::main]
async fn main() -> Result<()> {
    let db = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/verified-completion-demo/demo.db".into()),
    );
    let folder = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(folder)?;
    let source = folder.join(format!("verified-source-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(source.join("src"))?;
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"verified_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )?;
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"verified_fixture\"\nversion = \"0.1.0\"\n",
    )?;
    let lib = "pub mod a;\npub mod b;\npub fn combined() -> i64 { a::answer() - b::answer() }\n#[cfg(test)] mod tests { #[test] fn integrated_contract() { assert_eq!(super::combined(), 55); } }\n";
    fs::write(source.join("src/lib.rs"), lib)?;
    for (name, expression, expected) in [("a", "19 - 23", 42), ("b", "5 - 8", 13)] {
        fs::write(
            source.join(format!("src/{name}.rs")),
            format!(
                "pub fn answer() -> i64 {{ {expression} }}\n#[cfg(test)] mod tests {{ #[test] fn contract() {{ assert_eq!(super::answer(), {expected}); }} }}\n"
            ),
        )?;
    }
    let source = fs::canonicalize(source)?;
    let (engine, agents) = local_app(&db).await?;
    let mut workers = vec![];
    for (name, path, old, new, filter) in [
        ("first", "src/a.rs", "19 - 23", "19 + 23", Some("a::")),
        ("second", "src/b.rs", "5 - 8", "5 + 8", Some("b::")),
        (
            "integration",
            "src/lib.rs",
            "a::answer() - b::answer()",
            "a::answer() + b::answer()",
            None,
        ),
    ] {
        let mut argv = vec!["cargo", "test", "--locked", "--offline"];
        if let Some(filter) = filter {
            argv.push(filter);
        }
        let config = json!({
            "goal":format!("Repair {path}"),"target":{"provider":"scripted","model":name,"endpoint":""},
            "max_model_calls":3,"max_tokens":1000000,"max_output_tokens":512,"scripted_delay_ms":20,
            "change_review":true,"completion_policy":"require_passing_tests","accept_workspace_handoff":true,
            "writable_paths":[path],"tools":[{"name":"replace_text","version":1},{"name":"run_tests","version":1}],
            "test_command":{"argv":argv,"timeout_ms":60000},
            "script":[{"kind":"call_tool","tool":"replace_text","arguments":{"path":path,"old":old,"new":new}},
                {"kind":"call_tool","tool":"run_tests","arguments":{}},{"kind":"finish","summary":format!("Verified {path}")}]
        });
        workers.push(json!({"name":name,"description":format!("Repairs {path}"),
            "adapter":{"name":"tool","version":1},"attempt_limit":40,"config":config}));
    }
    let planner_config = json!({
        "goal":"Repair modules and verify the integrated project","target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":12,"max_tokens":2000000,"max_output_tokens":512,
        "delegation":{"max_tasks":3,"max_depth":2,"max_parallel_tasks":2,"shared_budget":{"max_model_calls":21,"max_tokens":4000000}},
        "goal_budget":{"max_model_calls":21,"max_tokens":4000000},
        "workspace_handoff":true,"child_changes":true,"change_review":true,"completion_policy":"require_passing_tests",
        "writable_paths":["src/"],"tools":[{"name":"run_tests","version":1}],
        "test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000},
        "script":[
            {"kind":"delegate_tasks","tasks":[{"worker":"first","goal":"Repair module a"},{"worker":"second","goal":"Repair module b"}]},
            {"kind":"inspect_child_changes","worker":"first","offset":0},{"kind":"import_child_changes","worker":"first"},
            {"kind":"inspect_child_changes","worker":"second","offset":0},{"kind":"import_child_changes","worker":"second"},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},{"kind":"finish","summary":"Module tests passed; finish the project"},
            {"kind":"delegate_task","worker":"integration","goal":"Repair the combined test failure against the integrated code"},
            {"kind":"inspect_child_changes","worker":"integration","offset":0},{"kind":"import_child_changes","worker":"integration"},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},{"kind":"finish","summary":"Combined Rust tests passed"}
        ]
    });
    let manifest = folder.join(format!("team-{}.json", uuid::Uuid::new_v4()));
    fs::write(
        &manifest,
        serde_json::to_vec_pretty(&json!({"version":1,
        "name":format!("verified-{}",uuid::Uuid::new_v4()),"source":source,
        "planner":{"attempt_limit":100,"config":planner_config},"workers":workers}))?,
    )?;
    let spec: TeamSpec = serde_json::from_slice(&fs::read(&manifest)?)?;
    let team = agents.teams()?.create(spec.clone()).await?;
    let actors: Vec<_> = team.members.iter().map(|m| m.agent_id).collect();
    let task = agents
        .teams()?
        .submit(
            team.id,
            MessageId::new(),
            "Repair the integrated project".into(),
        )
        .await?;
    println!(
        "Team: {}\nConfiguration: {}",
        team.id,
        fs::canonicalize(&manifest)?.display()
    );
    let mut overlapping = false;
    let mut worker = engine.worker().await?;
    drive(
        &engine,
        &agents,
        &mut worker,
        &actors,
        task,
        true,
        &mut overlapping,
    )
    .await?;
    let refused = agents.actors()?.task_changes(task).await?;
    let report = refused.completion.as_ref().ok_or("missing refusal")?;
    assert_eq!(report.status, CompletionStatus::Refused);
    let run_id = refused.run_id.ok_or("missing planner Run")?;
    let before = engine.status(run_id).await?;
    assert_eq!(before.record().delegations.len(), 2);
    assert!(before.record().result.is_none());
    assert!(overlapping, "module actors must actually overlap");
    for child in &before.record().delegations {
        let artifact = agents.actors()?.task_changes(child.id).await?;
        assert_eq!(
            artifact
                .completion
                .ok_or("missing child assessment")?
                .status,
            CompletionStatus::Verified
        );
    }
    let continuation = before
        .activities()
        .last()
        .ok_or("missing continuation")?
        .spec
        .input
        .clone();
    println!(
        "Both module tests passed, but combined tests failed. Finish was refused. Restarting with saved feedback."
    );
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let same_team = agents.teams()?.create(spec).await?;
    assert_eq!(same_team.id, team.id);
    assert_eq!(
        same_team
            .members
            .iter()
            .map(|m| m.agent_id)
            .collect::<Vec<_>>(),
        actors
    );
    let restored = engine.status(run_id).await?;
    assert_eq!(
        restored
            .activities()
            .last()
            .ok_or("lost continuation")?
            .spec
            .input,
        continuation
    );
    assert_eq!(json!(restored.record().usage), json!(before.record().usage));
    let mut worker = engine.worker().await?;
    drive(
        &engine,
        &agents,
        &mut worker,
        &actors,
        task,
        false,
        &mut overlapping,
    )
    .await?;
    let final_run = engine.status(run_id).await?;
    assert_eq!(final_run.record().status, RunStatus::Succeeded);
    assert_eq!(final_run.record().usage.admitted_model_calls, 12);
    let repaired_input = final_run.record().delegations[2]
        .workspace_input
        .as_ref()
        .ok_or("missing fresh repair input")?;
    assert_eq!(repaired_input.snapshot_digest, report.workspace_digest);
    let final_artifact = agents.actors()?.task_changes(task).await?;
    assert_eq!(
        final_artifact
            .completion
            .ok_or("missing final assessment")?
            .status,
        CompletionStatus::Verified
    );
    let changes = final_artifact.changes.ok_or("missing combined patch")?;
    assert_eq!(changes.files.len(), 3);
    assert_eq!(
        changes.tests.as_ref().ok_or("missing final tests")?.result["ok"],
        true
    );
    assert_eq!(fs::read_to_string(source.join("src/lib.rs"))?, lib);
    let output = folder.join(format!("verified-{task}.patch"));
    fs::write(&output, changes.patch())?;
    println!(
        "Fresh repair actor fixed the integrated code; planner retested and completed verified. 21 model calls across the goal."
    );
    println!(
        "Task: {task}\nPatch: {}",
        fs::canonicalize(output)?.display()
    );
    Ok(())
}
