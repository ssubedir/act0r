//! Two isolated repair actors share a snapshot, restart at a partial join, and return patches.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, change_review::TestEvidenceStatus, child_changes::APPROVAL_KEY, *},
};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn approve_owned_fixture(
    engine: &Engine,
    agents: &AgentService,
    actors: &[AgentId],
) -> Result<()> {
    for actor in actors {
        for task in agents.actors()?.tasks(*actor).await? {
            if let (Some(run), Some(wait)) = (task.run_id, task.wait) {
                if wait.key == "answer_clarification" {
                    continue;
                }
                if wait.key != "native_tool_approval" && wait.key != APPROVAL_KEY {
                    return Err("unexpected wait".into());
                }
                // This example owns every fixture file and the pinned Cargo commands.
                engine
                    .signal(run, wait.id, json!({"approved":true}))
                    .await?;
            }
        }
    }
    Ok(())
}
async fn step(worker: &mut Worker, overlapping: &mut bool) -> Result<bool> {
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
    Ok(progressed)
}
async fn drive(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    actors: &[AgentId],
    task: MessageId,
    partial_join: bool,
    overlapping: &mut bool,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        approve_owned_fixture(engine, agents, actors).await?;
        let progressed = step(worker, overlapping).await?;
        let view = agents.actors()?.task(task).await?;
        let run = engine
            .status(view.run_id.ok_or("missing planner Run")?)
            .await?;
        if partial_join
            && run.record().delegations.len() == 2
            && run.record().delegations[0].outcome.is_some()
        {
            let second = agents
                .actors()?
                .task(run.record().delegations[1].id)
                .await?;
            if second
                .wait
                .as_ref()
                .is_some_and(|w| w.key == "answer_clarification")
            {
                return Ok(());
            }
        }
        if view.status.is_terminal() {
            return Ok(());
        }
        for actor in actors {
            for task in agents.actors()?.tasks(*actor).await? {
                if task.status == AgentTaskStatus::Blocked {
                    return Err(format!(
                        "Run {:?} requires reconciliation; inspect its saved status and journal",
                        task.run_id
                    )
                    .into());
                }
            }
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    Err("group did not finish".into())
}
#[tokio::main]
async fn main() -> Result<()> {
    let db = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .unwrap_or_else(|| ".build/workspace-handoff-groups-demo/demo.db".into()),
    );
    let folder = db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(folder)?;
    let source = folder.join(format!("group-source-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(source.join("src"))?;
    fs::write(
        source.join("Cargo.toml"),
        "[package]\nname = \"group_handoff_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )?;
    fs::write(
        source.join("Cargo.lock"),
        "version = 4\n\n[[package]]\nname = \"group_handoff_fixture\"\nversion = \"0.1.0\"\n",
    )?;
    fs::write(source.join("src/lib.rs"), "pub mod a;\npub mod b;\n")?;
    let originals = [
        "pub fn answer() -> i64 { 19 - 23 }\n#[cfg(test)] mod tests { #[test] fn contract() { assert_eq!(super::answer(), 42); } }\n",
        "pub fn answer() -> i64 { 5 - 8 }\n#[cfg(test)] mod tests { #[test] fn contract() { assert_eq!(super::answer(), 13); } }\n",
    ];
    for (index, name) in ["a", "b"].iter().enumerate() {
        fs::write(source.join(format!("src/{name}.rs")), originals[index])?;
    }
    let source = fs::canonicalize(source)?;
    let (engine, agents) = local_app(&db).await?;
    let mut children = vec![];
    for (index, name) in ["a", "b"].iter().enumerate() {
        let mut script = vec![];
        if index == 1 {
            script.push(
                json!({"kind":"ask_human","question":"Continue module b repair after review?"}),
            );
        }
        script.extend([
            json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":format!("src/{name}.rs"),"old":if index == 0 {"20 - 22"} else {"6 - 7"},"new":if index == 0 {"20 + 22"} else {"6 + 7"}}}),
            json!({"kind":"call_tool","tool":"run_tests","arguments":{}}),
            json!({"kind":"finish","summary":format!("Repaired and verified module {name}")}),
        ]);
        children.push(agents.create_registered_actor(DefinitionId::new("tool",1),Some(source.clone()),json!({
            "goal":format!("Repair module {name}"),"target":{"provider":"scripted","model":name,"endpoint":""},
            "max_model_calls":4,"max_tokens":1000000,"max_output_tokens":512,"scripted_delay_ms":20,
            "change_review":true,"accept_workspace_handoff":true,"writable_paths":[format!("src/{name}.rs")],
            "tools":[{"name":"replace_text","version":1},{"name":"run_tests","version":1}],
            "test_command":{"argv":["cargo","test","--locked","--offline",format!("{name}::")],"timeout_ms":60000},"script":script
        }),40,Default::default()).await?);
    }
    let planner = agents.create_registered_actor(DefinitionId::new("planner",3),Some(source.clone()),json!({
        "goal":"Repair both modules and verify the combined project","target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":9,"max_tokens":1000000,"max_output_tokens":512,
        "workers":[{"name":"first","agent_id":children[0],"description":"repair a"},{"name":"second","agent_id":children[1],"description":"repair b"}],
        "delegation":{"max_tasks":2,"max_depth":2,"max_parallel_tasks":2,"shared_budget":{"max_model_calls":16,"max_tokens":2000000}},
        "workspace_handoff":true,"child_changes":true,"change_review":true,"writable_paths":["src/"],
        "tools":[{"name":"replace_text","version":1},{"name":"run_tests","version":1}],
        "test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000},
        "script":[
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/a.rs","old":"19 - 23","new":"20 - 22"}},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/b.rs","old":"5 - 8","new":"6 - 7"}},
            {"kind":"delegate_tasks","tasks":[{"worker":"first","goal":"Repair module a in current code"},{"worker":"second","goal":"Repair module b in current code"}]},
            {"kind":"inspect_child_changes","worker":"first","offset":0},{"kind":"import_child_changes","worker":"first"},
            {"kind":"inspect_child_changes","worker":"second","offset":0},{"kind":"import_child_changes","worker":"second"},
            {"kind":"call_tool","tool":"run_tests","arguments":{}},{"kind":"finish","summary":"Both patches imported; combined tests passed"}
        ]
    }),80,Default::default()).await?;
    let task = agents
        .actors()?
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: planner,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"repair both modules"}),
        })
        .await?;
    let mut overlapping = false;
    let mut worker = engine.worker().await?;
    let actors = [planner, children[0], children[1]];
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
    let view = agents.actors()?.task(task).await?;
    let run_id = view.run_id.ok_or("missing planner Run")?;
    let before = engine.status(run_id).await?;
    assert!(before.record().delegations[0].outcome.is_some());
    assert!(before.record().delegations[1].outcome.is_none());
    assert!(before.record().task_groups[0].joined_at.is_none());
    let inputs: Vec<_> = before
        .record()
        .delegations
        .iter()
        .map(|d| d.workspace_input.clone().expect("missing handed workspace"))
        .collect();
    assert_eq!(inputs[0].snapshot_digest, inputs[1].snapshot_digest);
    assert_eq!(inputs[0].snapshot_id, inputs[1].snapshot_id);
    assert_ne!(inputs[0].workspace, inputs[1].workspace);
    assert!(
        overlapping,
        "the demo must admit activities from independent Runs concurrently"
    );
    let second = agents.actors()?.task(inputs[1].task_id).await?;
    let second_run = second.run_id.ok_or("missing second Run")?;
    let wait = second.wait.ok_or("missing second human wait")?;
    println!(
        "One snapshot, two isolated copies. First patch saved; second child waits durably. Restarting."
    );
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let saved = engine.status(run_id).await?;
    assert_eq!(
        json!(saved.record().delegations),
        json!(before.record().delegations)
    );
    assert_eq!(
        agents
            .actors()?
            .task(inputs[1].task_id)
            .await?
            .wait
            .ok_or("lost wait")?
            .id,
        wait.id
    );
    engine
        .signal(second_run, wait.id, json!({"answer":"continue"}))
        .await?;
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
    let artifact = agents
        .actors()?
        .task_changes(task)
        .await?
        .changes
        .ok_or("missing combined artifact")?;
    assert_eq!(artifact.files.len(), 2);
    assert_eq!(artifact.test_status, TestEvidenceStatus::Matching);
    assert_eq!(
        artifact
            .tests
            .as_ref()
            .ok_or("missing combined tests")?
            .result["ok"],
        true
    );
    for (index, input) in inputs.iter().enumerate() {
        let child = agents
            .actors()?
            .task_changes(input.task_id)
            .await?
            .changes
            .ok_or("missing child artifact")?;
        assert_eq!(child.baseline_digest, input.snapshot_digest);
        assert_eq!(child.files.len(), 1);
        assert_eq!(child.test_status, TestEvidenceStatus::Matching);
        assert_eq!(
            child.tests.as_ref().ok_or("missing child tests")?.result["ok"],
            true
        );
        let pinned = agents.actors()?.status(children[index]).await?;
        let default = PathBuf::from(
            pinned.record().config["input"]["workspace"]
                .as_str()
                .ok_or("missing default workspace")?,
        );
        for (i, name) in ["a", "b"].iter().enumerate() {
            assert_eq!(
                fs::read_to_string(source.join(format!("src/{name}.rs")))?,
                originals[i]
            );
            assert_eq!(
                fs::read_to_string(default.join(format!("src/{name}.rs")))?,
                originals[i]
            );
        }
    }
    let output = folder.join(format!("combined-{task}.patch"));
    fs::write(&output, artifact.patch())?;
    println!(
        "Restored the partial join, imported both patches separately, and passed combined Rust tests."
    );
    println!(
        "Planner: {} calls; children: 3 and 4 calls. Final patch: {}",
        final_run.record().usage.admitted_model_calls,
        fs::canonicalize(output)?.display()
    );
    Ok(())
}
