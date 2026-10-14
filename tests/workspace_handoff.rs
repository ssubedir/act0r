//! Public native actor contract: current code, isolated task inputs, durable receipt replay.
use act0r::{
    Engine, Worker,
    adapters::{
        candidates::LocalCandidates, sqlite::SqliteStore, workspace::LocalTools,
        workspace_handoff::LocalWorkspaceHandoff,
    },
    application::{AgentService, ports::*},
    bootstrap::local_app,
    domain::{actors::*, workspace_handoff::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

fn config() -> Value {
    json!({"goal":"repair", "target":{"provider":"scripted","model":"handoff","endpoint":""},
        "max_model_calls":16,"max_tokens":1000000,"max_output_tokens":512,"change_review":true})
}
struct Fixture {
    engine: Engine,
    agents: AgentService,
    child: AgentId,
    parent: AgentId,
    task: MessageId,
    source: PathBuf,
}
async fn setup(root: &Path, mode: &str) -> Fixture {
    let source = root.join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/lib.rs"), "initial café\n").unwrap();
    fs::write(source.join("README.md"), "original\n").unwrap();
    let (engine, agents) = local_app(root.join("app.db")).await.unwrap();
    let mut child = config();
    child["accept_workspace_handoff"] = json!(mode != "disabled_child");
    child["tools"] = json!([{"name":"replace_text","version":1},{"name":"read_file","version":2}]);
    child["writable_paths"] = json!(["src/"]);
    child["script"] = json!([
        {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
        {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"current café","new":"fixed café"}},
        {"kind":"finish","summary":"fixed"}]);
    if matches!(mode, "busy" | "capacity" | "scoped_busy") {
        child["script"].as_array_mut().unwrap().insert(
            0,
            json!({"kind":"ask_human","question":"Continue this task?"}),
        );
    }
    let child = agents
        .create_registered_actor(
            DefinitionId::new("tool", 1),
            Some(source.clone()),
            child,
            40,
            if mode == "capacity" {
                AgentQueuePolicy { task_capacity: 1 }
            } else {
                Default::default()
            },
        )
        .await
        .unwrap();
    let mut parent = config();
    parent["workspace_handoff"] = json!(mode != "disabled_parent");
    parent["child_changes"] = json!(true);
    parent["workers"] = json!([{"name":"repairer","agent_id":child,"description":"return repair"}]);
    parent["delegation"] = json!({"max_tasks":2,"max_depth":2});
    parent["tools"] = json!([{"name":"replace_text","version":1}]);
    parent["writable_paths"] = json!(["src/", "README.md"]);
    parent["script"] = json!([
        {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"initial café","new":"current café"}},
        {"kind":"delegate_task","worker":"repairer","goal":"repair current code"},
        {"kind":"inspect_child_changes","worker":"repairer","offset":0},
        {"kind":"import_child_changes","worker":"repairer"},
        {"kind":"finish","summary":"integrated"}]);
    if matches!(mode, "twice" | "scoped_twice") {
        parent["script"] = json!([
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"initial café","new":"current café"}},
            {"kind":"delegate_task","worker":"repairer","goal":"repair current code"},
            {"kind":"inspect_child_changes","worker":"repairer","offset":0},
            {"kind":"import_child_changes","worker":"repairer"},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"fixed café","new":"current café"}},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"README.md","old":"original","new":"integrated"}},
            {"kind":"delegate_task","worker":"repairer","goal":"repair combined code"},
            {"kind":"inspect_child_changes","worker":"repairer","offset":0},
            {"kind":"import_child_changes","worker":"repairer"},
            {"kind":"finish","summary":"integrated twice"}]);
    }
    if matches!(mode, "scoped" | "scoped_twice" | "scoped_busy") {
        parent["delegation_write_scopes"] = json!(true);
        parent["script"][1]["writable_paths"] = json!(["src/lib.rs"]);
    }
    let parent = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source.clone()),
            parent,
            if mode == "attempts" { 4 } else { 80 },
            Default::default(),
        )
        .await
        .unwrap();
    let task = agents.actors().unwrap().send(message(parent, json!({"goal":"repair","workspace":"forged","workspace_input":{"workspace":"forged"},"writable_paths":["/"]}))).await.unwrap();
    Fixture {
        engine,
        agents,
        child,
        parent,
        task,
        source,
    }
}
fn message(actor: AgentId, payload: Value) -> AgentMessage {
    AgentMessage {
        id: MessageId::new(),
        recipient: actor,
        sender: None,
        correlation_id: None,
        kind: "task".into(),
        payload,
    }
}

#[tokio::test]
async fn scoped_handoff_replays_exact_paths_verifies_task_authority_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "scoped").await;
    let pinned = f
        .agents
        .actors()
        .unwrap()
        .status(f.child)
        .await
        .unwrap()
        .record()
        .config
        .clone();
    let mut worker = f.engine.worker().await.unwrap();
    let call = drive(&f, &mut worker, Some(handler("prepare")))
        .await
        .unwrap();
    let outcome = worker.execute(call.clone()).await.unwrap();
    let port = adapter(dir.path()).await;
    assert_eq!(port.execute(call.clone()).await.unwrap(), outcome);
    for paths in [json!(["src/"]), json!(["README.md"]), json!(["outside.rs"])] {
        let mut forged = call.clone();
        forged.input["writable_paths"] = paths;
        assert!(
            matches!(port.execute(forged).await.unwrap(), Outcome::Success(v) if v["ok"] == false)
        );
    }
    worker.complete(call, outcome).await.unwrap();
    let verify = drive(&f, &mut worker, Some(handler("verify")))
        .await
        .unwrap();
    let mut forged = verify.clone();
    forged.input["request"]["writable_paths"] = json!(["src/"]);
    assert!(matches!(
        port.execute(forged).await.unwrap(),
        Outcome::Failure(_)
    ));
    let outcome = worker.execute(verify.clone()).await.unwrap();
    worker.complete(verify, outcome).await.unwrap();
    drop(port);
    drop(worker);
    let Fixture {
        engine,
        agents,
        child,
        parent,
        task,
        source,
    } = f;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
    let f = Fixture {
        engine,
        agents,
        child,
        parent,
        task,
        source,
    };
    let mut worker = f.engine.worker().await.unwrap();
    drive(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    let prepared = run.record().delegations[0]
        .workspace_input
        .as_ref()
        .unwrap();
    assert_eq!(
        prepared.request.writable_paths,
        Some(vec!["src/lib.rs".into()])
    );
    let task = f
        .agents
        .actors()
        .unwrap()
        .task(prepared.task_id)
        .await
        .unwrap();
    assert_eq!(task.task.workspace_input.as_ref(), Some(prepared));
    let child_run = f.engine.status(task.run_id.unwrap()).await.unwrap();
    assert_eq!(
        child_run.record().input["input"]["writable_paths"],
        json!(["src/lib.rs"])
    );
    assert_eq!(
        f.agents
            .actors()
            .unwrap()
            .status(f.child)
            .await
            .unwrap()
            .record()
            .config,
        pinned
    );
    assert_eq!(
        fs::read_to_string(workspace(&run).join("src/lib.rs")).unwrap(),
        "fixed café\n"
    );
}

#[tokio::test]
async fn task_scopes_do_not_leak_to_later_delegations_or_public_task_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "scoped_twice").await;
    let mut worker = f.engine.worker().await.unwrap();
    drive(&f, &mut worker, None).await;
    let parent = parent_run(&f).await;
    assert_eq!(parent.record().status, RunStatus::Succeeded);
    for (index, delegation) in parent.record().delegations.iter().enumerate() {
        let task = f
            .agents
            .actors()
            .unwrap()
            .task(delegation.id)
            .await
            .unwrap();
        let run = f.engine.status(task.run_id.unwrap()).await.unwrap();
        assert_eq!(
            run.record().input["input"]["writable_paths"],
            json!([if index == 0 { "src/lib.rs" } else { "src/" }])
        );
    }
    let public = f.agents.actors().unwrap().send(message(f.child, json!({"goal":"ordinary","writable_paths":["README.md"],"workspace_input":parent.record().delegations[0].workspace_input}))).await.unwrap();
    for _ in 0..100 {
        worker.advance_work().await.unwrap();
        let task = f.agents.actors().unwrap().task(public).await.unwrap();
        if let Some(id) = task.run_id {
            let run = f.engine.status(id).await.unwrap();
            assert!(task.task.workspace_input.is_none());
            assert_eq!(
                run.record().input["input"]["writable_paths"],
                json!(["src/"])
            );
            assert!(run.record().input["input"].get("workspace_input").is_none());
            return;
        }
        worker.step().await.unwrap();
    }
    panic!("ordinary task was not launched");
}

#[tokio::test]
async fn out_of_scope_files_in_a_saved_child_patch_cannot_be_imported() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "scoped").await;
    let mut worker = f.engine.worker().await.unwrap();
    let capture = drive(&f, &mut worker, Some(change_review::handler("changes")))
        .await
        .unwrap();
    let child = f.engine.status(capture.run_id).await.unwrap();
    // Commands are not OS sandboxes: reproduce an out-of-scope process side effect.
    fs::write(
        workspace(&child).join("src/unassigned.rs"),
        "unexpected process output\n",
    )
    .unwrap();
    let outcome = worker.execute(capture.clone()).await.unwrap();
    worker.complete(capture, outcome).await.unwrap();
    drive(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert!(
        run.activities()
            .iter()
            .any(|a| a.spec.handler == child_changes::handler("inspect")
                && a.result.as_ref().is_some_and(|r| r["ok"] == false
                    && r.to_string().contains("outside its task writable paths")))
    );
    assert!(
        !run.activities()
            .iter()
            .any(|a| a.spec.handler == child_changes::handler("import"))
    );
    assert_eq!(
        fs::read_to_string(workspace(&run).join("src/lib.rs")).unwrap(),
        "current café\n"
    );
    assert!(!workspace(&run).join("src/unassigned.rs").exists());
}

#[test]
fn scopes_narrow_portable_permissions_and_optional_handoff_is_omitted() {
    use act0r::domain::tools::validate_write_scope;
    let allowed = vec!["src/".into(), "Cargo.toml".into()];
    for paths in [
        vec!["src/lib.rs".into()],
        vec!["src/nested/".into()],
        vec!["Cargo.toml".into()],
    ] {
        validate_write_scope(&paths, &allowed).unwrap();
    }
    for paths in [
        vec![],
        vec!["src".into()],
        vec!["src2/x.rs".into()],
        vec!["../src/x.rs".into()],
        vec!["src\\x.rs".into()],
        vec!["src/../README.md".into()],
        vec!["Cargo.toml/".into()],
        vec!["src/lib.rs".into(), "src/lib.rs".into()],
    ] {
        assert!(validate_write_scope(&paths, &allowed).is_err(), "{paths:?}");
    }
    assert!(validate_write_scope(&["src/".into()], &["src/lib.rs".into()]).is_err());
    let action = json!({"kind":"delegate_task","worker":"coder","goal":"repair"});
    let decoded: agent::AgentAction = serde_json::from_value(action.clone()).unwrap();
    assert_eq!(json!(decoded), action);
    let request = json!({"workspace":"saved", "recipient":AgentId::new(),"goal":"repair"});
    assert_eq!(
        json!(serde_json::from_value::<PrepareRequest>(request.clone()).unwrap()),
        request
    );
}

#[tokio::test]
async fn invalid_model_scopes_return_feedback_without_preparing_a_child_and_old_schemas_stay_pinned()
 {
    for mode in ["success", "scoped"] {
        let dir = tempfile::tempdir().unwrap();
        let f = setup(dir.path(), mode).await;
        let mut worker = f.engine.worker().await.unwrap();
        let call = drive(&f, &mut worker, Some(DefinitionId::new("agent.model", 1)))
            .await
            .unwrap();
        let variants = call.input["schema"]["properties"]["action"]["anyOf"]
            .as_array()
            .unwrap();
        assert_eq!(
            variants
                .iter()
                .any(|v| v["properties"].get("writable_paths").is_some()),
            mode == "scoped"
        );
        let run = parent_run(&f).await;
        let definitions =
            serde_json::from_value(run.record().input["input"]["tool_definitions"].clone())
                .unwrap();
        let workflow = planner::PlannerWorkflow::new(definitions);
        for action in [
            json!({"kind":"delegate_task","worker":"repairer","goal":"repair","writable_paths":["outside.rs"]}),
            json!({"kind":"delegate_tasks","tasks":[{"worker":"repairer","goal":"repair","writable_paths":["src/../README.md"]}]}),
        ] {
            let transition = workflow
                .transition(
                    &run.record().state["state"],
                    &WorkflowEvent::ActivityCompleted {
                        activity_id: call.activity_id,
                        handler: call.handler.clone(),
                        result: json!({"action":action}),
                    },
                )
                .unwrap();
            assert_eq!(transition.commands.len(), 1);
            let Command::Schedule(next) = &transition.commands[0] else {
                panic!("invalid scope should request correction")
            };
            assert_eq!(next.handler, DefinitionId::new("agent.model", 1));
            assert!(
                next.input["prompt"]
                    .as_str()
                    .unwrap()
                    .contains("invalid_delegation_scope")
            );
        }
        assert!(run.record().delegations.is_empty());
    }
}
async fn approvals(f: &Fixture) {
    for actor in [f.parent, f.child] {
        for view in f.agents.actors().unwrap().tasks(actor).await.unwrap() {
            if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
                if wait.key == "answer_clarification" {
                    continue;
                }
                assert!(
                    wait.key == "native_tool_approval"
                        || wait.key == act0r::domain::child_changes::APPROVAL_KEY
                );
                f.engine
                    .signal(run, wait.id, json!({"approved":true}))
                    .await
                    .unwrap();
            }
        }
    }
}
async fn drive(
    f: &Fixture,
    worker: &mut Worker,
    stop: Option<DefinitionId>,
) -> Option<ActivityExecution> {
    for _ in 0..400 {
        worker.advance_work().await.unwrap();
        approvals(f).await;
        if let Some(call) = worker.admit().await.unwrap() {
            if stop.as_ref() == Some(&call.handler) {
                return Some(call);
            }
            let result = worker.execute(call.clone()).await.unwrap();
            worker.complete(call, result).await.unwrap();
        }
        if f.agents
            .actors()
            .unwrap()
            .task(f.task)
            .await
            .unwrap()
            .status
            .is_terminal()
        {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("did not settle");
}
async fn parent_run(f: &Fixture) -> Run {
    f.engine
        .status(
            f.agents
                .actors()
                .unwrap()
                .task(f.task)
                .await
                .unwrap()
                .run_id
                .unwrap(),
        )
        .await
        .unwrap()
}
fn workspace(run: &Run) -> PathBuf {
    PathBuf::from(run.record().input["input"]["workspace"].as_str().unwrap())
}
async fn adapter(root: &Path) -> LocalWorkspaceHandoff {
    let store = Arc::new(SqliteStore::open(root.join("app.db")).await.unwrap());
    let managed = root.join(".act0r/workspaces");
    LocalWorkspaceHandoff::new(
        store,
        Arc::new(LocalCandidates::new(
            root.join(".act0r/snapshots"),
            managed.clone(),
        )),
        Arc::new(LocalTools::new(managed)),
    )
}

#[tokio::test]
async fn current_parent_code_is_the_child_baseline_and_import_keeps_worker_pins() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "success").await;
    let pinned = f
        .agents
        .actors()
        .unwrap()
        .status(f.child)
        .await
        .unwrap()
        .record()
        .config
        .clone();
    let base = PathBuf::from(pinned["input"]["workspace"].as_str().unwrap());
    let mut worker = f.engine.worker().await.unwrap();
    drive(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(
        run.record().status,
        RunStatus::Succeeded,
        "{:?}",
        run.record().error
    );
    assert_eq!(
        fs::read_to_string(workspace(&run).join("src/lib.rs")).unwrap(),
        "fixed café\n"
    );
    assert_eq!(
        fs::read_to_string(base.join("src/lib.rs")).unwrap(),
        "initial café\n"
    );
    assert_eq!(
        fs::read_to_string(f.source.join("src/lib.rs")).unwrap(),
        "initial café\n"
    );
    assert_eq!(
        f.agents
            .actors()
            .unwrap()
            .status(f.child)
            .await
            .unwrap()
            .record()
            .config,
        pinned
    );
    let child = f
        .agents
        .actors()
        .unwrap()
        .tasks(f.child)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let prepared = child.task.workspace_input.unwrap();
    assert_eq!(prepared.task_id, run.record().delegations[0].id);
    assert_ne!(PathBuf::from(&prepared.workspace), base);
    let child_run = f.engine.status(child.run_id.unwrap()).await.unwrap();
    assert_eq!(
        child_run.record().input["input"]["writable_paths"],
        pinned["input"]["writable_paths"]
    );
    assert_eq!(child_run.activities()[0].spec.handler, handler("verify"));
    assert_eq!(child_run.record().usage.admitted_model_calls, 3);
    let changes = f
        .agents
        .actors()
        .unwrap()
        .task_changes(prepared.task_id)
        .await
        .unwrap()
        .changes
        .unwrap();
    assert_eq!(changes.baseline_digest, prepared.snapshot_digest);
    assert_eq!(changes.files.len(), 1);
    assert!(
        changes.patch().contains("-current café") && !changes.patch().contains("-initial café")
    );
    let models: Vec<_> = child_run
        .activities()
        .iter()
        .filter(|a| a.spec.handler.name == "agent.model")
        .collect();
    assert!(models.iter().all(|a| {
        !serde_json::to_string(&a.spec.input)
            .unwrap()
            .contains("worker_pin")
    }));
}
#[tokio::test]
async fn successive_tasks_on_one_actor_get_distinct_current_workspaces() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "twice").await;
    let mut worker = f.engine.worker().await.unwrap();
    drive(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(
        run.record().status,
        RunStatus::Succeeded,
        "{:?}",
        run.record().error
    );
    let tasks = f.agents.actors().unwrap().tasks(f.child).await.unwrap();
    assert_eq!(tasks.len(), 2);
    let first = tasks
        .iter()
        .find(|t| t.task.id == run.record().delegations[0].id)
        .unwrap();
    let second = tasks
        .iter()
        .find(|t| t.task.id == run.record().delegations[1].id)
        .unwrap();
    let a = first.task.workspace_input.as_ref().unwrap();
    let b = second.task.workspace_input.as_ref().unwrap();
    assert_ne!(a.workspace, b.workspace);
    assert_ne!(a.snapshot_id, b.snapshot_id);
    assert_eq!(
        fs::read_to_string(Path::new(&a.workspace).join("README.md")).unwrap(),
        "original\n"
    );
    assert_eq!(
        fs::read_to_string(Path::new(&b.workspace).join("README.md")).unwrap(),
        "integrated\n"
    );
    assert_eq!(
        fs::read_to_string(workspace(&run).join("src/lib.rs")).unwrap(),
        "fixed café\n"
    );
    // Ordinary later messages still use the worker's creation-time workspace.
    let ordinary = f
        .agents
        .actors()
        .unwrap()
        .send(message(
            f.child,
            json!({"goal":"ordinary","workspace_input":json!(b)}),
        ))
        .await
        .unwrap();
    for _ in 0..100 {
        worker.step().await.unwrap();
        approvals(&f).await;
        if f.agents
            .actors()
            .unwrap()
            .task(ordinary)
            .await
            .unwrap()
            .status
            .is_terminal()
        {
            break;
        }
    }
    let task = f.agents.actors().unwrap().task(ordinary).await.unwrap();
    assert!(task.task.workspace_input.is_none());
    let ordinary_run = f.engine.status(task.run_id.unwrap()).await.unwrap();
    assert!(
        ordinary_run.record().input["input"]
            .get("workspace_input")
            .is_none()
    );
    assert_eq!(
        ordinary_run.activities()[0].spec.handler,
        change_review::handler("baseline")
    );
}
#[tokio::test]
async fn preparation_receipt_replays_exact_input_after_parent_drift_and_checks_bindings() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "success").await;
    let mut worker = f.engine.worker().await.unwrap();
    let call = drive(&f, &mut worker, Some(handler("prepare")))
        .await
        .unwrap();
    let port = adapter(dir.path()).await;
    let Outcome::Success(first) = port.execute(call.clone()).await.unwrap() else {
        panic!()
    };
    let prepared: PreparedWorkspace = serde_json::from_value(first["prepared"].clone()).unwrap();
    let target = workspace(&parent_run(&f).await);
    fs::write(target.join("src/lib.rs"), "later parent state\n").unwrap();
    assert_eq!(
        port.execute(call.clone()).await.unwrap(),
        Outcome::Success(first.clone())
    );
    assert_eq!(
        fs::read_to_string(Path::new(&prepared.workspace).join("src/lib.rs")).unwrap(),
        "current café\n"
    );
    let mut changed = call.clone();
    changed.input["goal"] = json!("different");
    assert!(matches!(port.execute(changed).await.unwrap(),Outcome::Success(v) if v["ok"] == false));
    let mut forged = call.clone();
    forged.input["workspace"] = json!("unrelated");
    assert!(matches!(port.execute(forged).await.unwrap(),Outcome::Success(v) if v["ok"] == false));
    // Restore parent so the child patch may be imported; preparation stays identical.
    fs::write(target.join("src/lib.rs"), "current café\n").unwrap();
    worker
        .complete(call, Outcome::Success(first))
        .await
        .unwrap();
    drive(&f, &mut worker, None).await;
    assert_eq!(parent_run(&f).await.record().status, RunStatus::Succeeded);
}
#[tokio::test]
async fn snapshot_and_queued_copy_tampering_stop_before_the_first_child_model_call() {
    for mode in ["snapshot", "copy", "receipt"] {
        let dir = tempfile::tempdir().unwrap();
        let f = setup(dir.path(), "success").await;
        let mut worker = f.engine.worker().await.unwrap();
        let call = drive(&f, &mut worker, Some(handler("verify")))
            .await
            .unwrap();
        let prepared: PreparedWorkspace = serde_json::from_value(call.input.clone()).unwrap();
        let path = if mode == "copy" {
            PathBuf::from(&prepared.workspace).join("src/lib.rs")
        } else if mode == "receipt" {
            dir.path()
                .join(".act0r/snapshots")
                .join(prepared.snapshot_id.to_string())
                .join(".act0r-tools/handoff.json")
        } else {
            dir.path()
                .join(".act0r/snapshots")
                .join(prepared.snapshot_id.to_string())
                .join("src/lib.rs")
        };
        fs::write(path, if mode == "receipt" { "{}" } else { "tampered" }).unwrap();
        let outcome = worker.execute(call.clone()).await.unwrap();
        assert!(matches!(outcome, Outcome::Failure(_)), "{outcome:?}");
        worker.complete(call.clone(), outcome).await.unwrap();
        drive(&f, &mut worker, None).await;
        let child = f.engine.status(call.run_id).await.unwrap();
        assert_eq!(child.record().status, RunStatus::Failed);
        assert_eq!(child.record().usage.admitted_model_calls, 0);
    }
}
#[tokio::test]
async fn disabled_handoff_uses_the_worker_workspace_and_worker_opt_in_is_required() {
    for mode in ["disabled_parent", "disabled_child", "attempts"] {
        let dir = tempfile::tempdir().unwrap();
        let f = setup(dir.path(), mode).await;
        let mut worker = f.engine.worker().await.unwrap();
        drive(&f, &mut worker, None).await;
        let run = parent_run(&f).await;
        if mode == "disabled_parent" {
            assert!(!run.activities().iter().any(|a| is_handler(&a.spec.handler)));
            let child = f
                .agents
                .actors()
                .unwrap()
                .tasks(f.child)
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert!(child.task.workspace_input.is_none());
        } else {
            assert!(run.record().delegations.is_empty());
            assert!(
                f.agents
                    .actors()
                    .unwrap()
                    .tasks(f.child)
                    .await
                    .unwrap()
                    .is_empty()
            );
            if mode == "attempts" {
                assert_eq!(run.record().status, RunStatus::BudgetExhausted);
            } else {
                assert!(
                    run.activities()
                        .iter()
                        .any(|a| a.spec.handler == handler("prepare")
                            && a.result.as_ref().unwrap()["ok"] == false)
                );
            }
        }
    }
}
#[tokio::test]
async fn task_authority_and_saved_worker_permissions_cannot_be_forged() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "success").await;
    let mut worker = f.engine.worker().await.unwrap();
    let call = drive(&f, &mut worker, Some(handler("verify")))
        .await
        .unwrap();
    let port = adapter(dir.path()).await;
    let mut changed = call.clone();
    changed.input["worker_pin"] = json!("f".repeat(64));
    assert!(matches!(
        port.execute(changed).await.unwrap(),
        Outcome::Failure(_)
    ));
    let mut changed = call.clone();
    changed.run_id = RunId::new();
    assert!(matches!(
        port.execute(changed).await.unwrap(),
        Outcome::Failure(_)
    ));
    let mut changed = call.clone();
    changed.input["workspace"] = json!(workspace(&parent_run(&f).await));
    assert!(matches!(
        port.execute(changed).await.unwrap(),
        Outcome::Failure(_)
    ));
    let outcome = worker.execute(call.clone()).await.unwrap();
    assert_eq!(outcome, Outcome::Success(json!({"verified":true})));
    worker
        .complete(call.clone(), outcome.clone())
        .await
        .unwrap();
    drive(&f, &mut worker, None).await;
    // Verification replay must preserve the child's edited workspace.
    assert_eq!(port.execute(call).await.unwrap(), outcome);
}

#[tokio::test]
async fn goal_pause_and_cancellation_hold_publication_after_files_are_prepared() {
    for mode in ["pause", "cancel"] {
        let dir = tempfile::tempdir().unwrap();
        let f = setup(dir.path(), "success").await;
        let mut worker = f.engine.worker().await.unwrap();
        let call = drive(&f, &mut worker, Some(handler("prepare")))
            .await
            .unwrap();
        let outcome = worker.execute(call.clone()).await.unwrap();
        assert!(matches!(outcome, Outcome::Success(_)));
        if mode == "pause" {
            f.agents.actors().unwrap().pause_goal(f.task).await.unwrap();
        } else {
            f.agents
                .actors()
                .unwrap()
                .cancel_goal(f.task)
                .await
                .unwrap();
        }
        worker.complete(call, outcome).await.unwrap();
        for _ in 0..6 {
            worker.advance_work().await.unwrap();
            assert!(worker.admit().await.unwrap().is_none());
        }
        assert!(
            f.agents
                .actors()
                .unwrap()
                .tasks(f.child)
                .await
                .unwrap()
                .is_empty()
        );
        if mode == "pause" {
            f.agents
                .actors()
                .unwrap()
                .resume_goal(f.task)
                .await
                .unwrap();
            drive(&f, &mut worker, None).await;
            assert_eq!(parent_run(&f).await.record().status, RunStatus::Succeeded);
        } else {
            assert_eq!(parent_run(&f).await.record().status, RunStatus::Cancelled);
        }
    }
}
#[tokio::test]
async fn runtime_input_and_unsupported_handoff_configurations_are_rejected_before_allocating() {
    let dir = tempfile::tempdir().unwrap();
    let (_, agents) = local_app(dir.path().join("app.db")).await.unwrap();
    let source = dir.path().join("source");
    fs::create_dir(&source).unwrap();
    for mode in ["runtime", "no_review", "no_source", "no_parent_source"] {
        let mut cfg = config();
        let adapter = if mode == "no_parent_source" {
            cfg["workspace_handoff"] = json!(true);
            cfg["workers"] =
                json!([{"name":"worker","agent_id":AgentId::new(),"description":"worker"}]);
            cfg["delegation"] = json!({"max_tasks":2,"max_depth":2});
            DefinitionId::new("planner", 3)
        } else {
            cfg["accept_workspace_handoff"] = json!(true);
            if mode == "runtime" {
                cfg["workspace_input"] = json!({});
            }
            if mode == "no_review" {
                cfg["change_review"] = json!(false);
            }
            DefinitionId::new("tool", 1)
        };
        let src = (!matches!(mode, "no_source" | "no_parent_source")).then(|| source.clone());
        assert!(
            agents
                .create_registered_actor(adapter, src, cfg, 20, Default::default())
                .await
                .is_err()
        );
    }
    assert!(!dir.path().join(".act0r/workspaces").exists());
}

async fn queue_at(f: &Fixture, worker: &mut Worker, target: DefinitionId) -> ActivityExecution {
    for _ in 0..300 {
        worker.advance_work().await.unwrap();
        approvals(f).await;
        for actor in [f.parent, f.child] {
            for task in f.agents.actors().unwrap().tasks(actor).await.unwrap() {
                if let Some(id) = task.run_id {
                    let run = f.engine.status(id).await.unwrap();
                    if let Some(a) = run
                        .activities()
                        .iter()
                        .find(|a| a.spec.handler == target && a.status == ActivityStatus::Queued)
                    {
                        return ActivityExecution {
                            run_id: id,
                            activity_id: a.id,
                            attempt_id: AttemptId::new(),
                            ordinal: 1,
                            handler: a.spec.handler.clone(),
                            input: a.spec.input.clone(),
                        };
                    }
                }
            }
        }
        if let Some(call) = worker.admit().await.unwrap() {
            let outcome = worker.execute(call.clone()).await.unwrap();
            worker.complete(call, outcome).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("checkpoint not queued");
}
#[tokio::test]
async fn killed_hosts_replay_preparation_and_verification_receipts_without_duplicate_tasks() {
    for checkpoint in ["prepare", "verify"] {
        let dir = tempfile::tempdir().unwrap();
        let f = setup(dir.path(), "success").await;
        let mut worker = f.engine.worker().await.unwrap();
        let queued = queue_at(&f, &mut worker, handler(checkpoint)).await;
        let parent = workspace(&parent_run(&f).await);
        let task = f.task;
        let child = f.child;
        let parent_id = f.parent;
        let source = f.source.clone();
        drop(worker);
        drop(f);
        let marker = dir.path().join("executed.json");
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args([
            "--ignored",
            "--exact",
            "workspace_handoff_host_fixture",
            "--nocapture",
        ])
        .env("ACT0R_HANDOFF_DB", dir.path().join("app.db"))
        .env("ACT0R_HANDOFF_MARKER", &marker)
        .env("ACT0R_HANDOFF_CHECKPOINT", checkpoint)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x08000000);
        }
        let mut host = cmd.spawn().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !marker.exists() && std::time::Instant::now() < deadline {
            if host.try_wait().unwrap().is_some() {
                panic!("handoff host exited before checkpoint");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if !marker.exists() {
            host.kill().unwrap();
            host.wait().unwrap();
            panic!("checkpoint timeout");
        }
        host.kill().unwrap();
        host.wait().unwrap();
        let (call, saved): (ActivityExecution, Outcome) =
            serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
        assert_eq!(call.activity_id, queued.activity_id);
        if checkpoint == "prepare" {
            fs::write(parent.join("README.md"), "changed after snapshot\n").unwrap();
        }
        let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
        let f = Fixture {
            engine,
            agents,
            child,
            parent: parent_id,
            task,
            source,
        };
        let mut worker = f.engine.worker().await.unwrap();
        let retry = drive(&f, &mut worker, Some(handler(checkpoint)))
            .await
            .unwrap();
        assert_eq!(retry.activity_id, call.activity_id);
        assert_eq!(retry.ordinal, 2);
        assert_eq!(worker.execute(retry.clone()).await.unwrap(), saved);
        worker.complete(retry, saved).await.unwrap();
        drive(&f, &mut worker, None).await;
        assert_eq!(parent_run(&f).await.record().status, RunStatus::Succeeded);
        let children = f.agents.actors().unwrap().tasks(child).await.unwrap();
        assert_eq!(children.len(), 1);
        if checkpoint == "prepare" {
            let p = children[0].task.workspace_input.as_ref().unwrap();
            assert_eq!(
                fs::read_to_string(Path::new(&p.workspace).join("README.md")).unwrap(),
                "original\n"
            );
        }
    }
}
#[tokio::test]
#[ignore = "killed host fixture used by the consumer test"]
async fn workspace_handoff_host_fixture() {
    let db = PathBuf::from(std::env::var_os("ACT0R_HANDOFF_DB").unwrap());
    let marker = PathBuf::from(std::env::var_os("ACT0R_HANDOFF_MARKER").unwrap());
    let checkpoint = std::env::var("ACT0R_HANDOFF_CHECKPOINT").unwrap();
    let (engine, _agents) = local_app(db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..200 {
        worker.advance_work().await.unwrap();
        if let Some(call) = worker.admit().await.unwrap() {
            assert_eq!(call.handler, handler(&checkpoint));
            let saved = worker.execute(call.clone()).await.unwrap();
            assert!(matches!(saved, Outcome::Success(_)), "{saved:?}");
            fs::write(&marker, serde_json::to_vec(&(call, saved)).unwrap()).unwrap();
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("handoff not admitted");
}

#[tokio::test]
async fn accepted_handoff_survives_queueing_behind_another_task_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "scoped_busy").await;
    let ordinary = f
        .agents
        .actors()
        .unwrap()
        .send(message(f.child, json!({"goal":"busy ordinary task"})))
        .await
        .unwrap();
    let mut worker = f.engine.worker().await.unwrap();
    let call = drive(&f, &mut worker, Some(handler("prepare")))
        .await
        .unwrap();
    let outcome = worker.execute(call.clone()).await.unwrap();
    worker.complete(call, outcome).await.unwrap();
    for _ in 0..20 {
        worker.advance_work().await.unwrap();
    }
    let queued = f
        .agents
        .actors()
        .unwrap()
        .tasks(f.child)
        .await
        .unwrap()
        .into_iter()
        .find(|t| t.task.id != ordinary)
        .unwrap();
    assert_eq!(queued.status, AgentTaskStatus::Queued);
    assert!(queued.run_id.is_none());
    let input = queued.task.workspace_input.unwrap();
    assert_eq!(
        input.request.writable_paths,
        Some(vec!["src/lib.rs".into()])
    );
    assert_eq!(
        fs::read_to_string(Path::new(&input.workspace).join("src/lib.rs")).unwrap(),
        "current café\n"
    );
    let task = f.task;
    let child = f.child;
    let parent = f.parent;
    let source = f.source.clone();
    drop(worker);
    drop(f);
    let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
    let f = Fixture {
        engine,
        agents,
        child,
        parent,
        task,
        source,
    };
    let mut worker = f.engine.worker().await.unwrap();
    for _ in 0..300 {
        for view in f.agents.actors().unwrap().tasks(child).await.unwrap() {
            if let (Some(run), Some(wait)) = (view.run_id, view.wait)
                && wait.key == "answer_clarification"
            {
                f.engine
                    .signal(run, wait.id, json!({"answer":"continue"}))
                    .await
                    .unwrap();
            }
        }
        worker.step().await.unwrap();
        approvals(&f).await;
        if f.agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status
            .is_terminal()
        {
            break;
        }
    }
    assert_eq!(parent_run(&f).await.record().status, RunStatus::Succeeded);
    let task = f
        .agents
        .actors()
        .unwrap()
        .task(input.task_id)
        .await
        .unwrap();
    assert_eq!(task.task.workspace_input.as_ref(), Some(&input));
    assert_eq!(
        f.engine
            .status(task.run_id.unwrap())
            .await
            .unwrap()
            .record()
            .input["input"]["writable_paths"],
        json!(["src/lib.rs"])
    );
    assert_eq!(
        workspace(&f.engine.status(task.run_id.unwrap()).await.unwrap()),
        PathBuf::from(&input.workspace)
    );
    assert!(
        f.agents
            .actors()
            .unwrap()
            .task(ordinary)
            .await
            .unwrap()
            .task
            .workspace_input
            .is_none()
    );
}
#[tokio::test]
async fn prepared_handoff_still_honors_worker_mailbox_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "capacity").await;
    f.agents
        .actors()
        .unwrap()
        .send(message(f.child, json!({"goal":"occupy capacity"})))
        .await
        .unwrap();
    let mut worker = f.engine.worker().await.unwrap();
    drive(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().delegations.len(), 1);
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["error"]["code"],
        "queue_full"
    );
    let tasks = f.agents.actors().unwrap().tasks(f.child).await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert!(tasks[0].task.workspace_input.is_none());
}
#[tokio::test]
async fn published_snapshot_is_reused_when_preparation_receipt_has_not_been_saved() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "success").await;
    let mut worker = f.engine.worker().await.unwrap();
    let call = drive(&f, &mut worker, Some(handler("prepare")))
        .await
        .unwrap();
    let port = adapter(dir.path()).await;
    let Outcome::Success(first) = port.execute(call.clone()).await.unwrap() else {
        panic!()
    };
    let input: PreparedWorkspace = serde_json::from_value(first["prepared"].clone()).unwrap();
    let receipt = dir
        .path()
        .join(".act0r/snapshots")
        .join(input.snapshot_id.to_string())
        .join(".act0r-tools/handoff.json");
    fs::remove_file(receipt).unwrap();
    fs::write(
        workspace(&parent_run(&f).await).join("README.md"),
        "later source\n",
    )
    .unwrap();
    assert_eq!(
        port.execute(call.clone()).await.unwrap(),
        Outcome::Success(first.clone())
    );
    assert_eq!(
        fs::read_to_string(Path::new(&input.workspace).join("README.md")).unwrap(),
        "original\n"
    );
    worker
        .complete(call, Outcome::Success(first))
        .await
        .unwrap();
    drive(&f, &mut worker, None).await;
    assert_eq!(parent_run(&f).await.record().status, RunStatus::Succeeded);
}

#[tokio::test]
async fn a_verification_file_alone_does_not_authorize_drift_and_baseline_must_match() {
    for checkpoint in ["before_sql", "baseline"] {
        let dir = tempfile::tempdir().unwrap();
        let f = setup(dir.path(), "success").await;
        let mut worker = f.engine.worker().await.unwrap();
        let call = drive(&f, &mut worker, Some(handler("verify")))
            .await
            .unwrap();
        let input: PreparedWorkspace = serde_json::from_value(call.input.clone()).unwrap();
        let outcome = worker.execute(call.clone()).await.unwrap();
        assert_eq!(outcome, Outcome::Success(json!({"verified":true})));
        let interrupted = if checkpoint == "baseline" {
            worker.complete(call, outcome).await.unwrap();
            drive(&f, &mut worker, Some(change_review::handler("baseline")))
                .await
                .unwrap()
        } else {
            call
        };
        fs::write(
            Path::new(&input.workspace).join("src/lib.rs"),
            "drift after verification\n",
        )
        .unwrap();
        let result = worker.execute(interrupted.clone()).await.unwrap();
        if checkpoint == "before_sql" {
            assert!(matches!(result, Outcome::Failure(_)), "{result:?}");
        } else {
            assert!(matches!(result, Outcome::Success(_)));
        }
        worker.complete(interrupted.clone(), result).await.unwrap();
        drive(&f, &mut worker, None).await;
        let child = f.engine.status(interrupted.run_id).await.unwrap();
        assert_eq!(child.record().status, RunStatus::Failed);
        assert_eq!(child.record().usage.admitted_model_calls, 0);
    }
}

#[tokio::test]
async fn a_public_task_cannot_acquire_trusted_input_by_claiming_the_prepared_identity() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "success").await;
    let mut worker = f.engine.worker().await.unwrap();
    let call = drive(&f, &mut worker, Some(handler("prepare")))
        .await
        .unwrap();
    let outcome = worker.execute(call.clone()).await.unwrap();
    let Outcome::Success(value) = &outcome else {
        panic!()
    };
    let input: PreparedWorkspace = serde_json::from_value(value["prepared"].clone()).unwrap();
    // Deliberately use the exact envelope the native publisher would otherwise deduplicate.
    f.agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            id: input.task_id,
            recipient: f.child,
            sender: None,
            correlation_id: Some(input.task_id),
            kind: "task".into(),
            payload: json!({"goal":input.request.goal}),
        })
        .await
        .unwrap();
    worker.complete(call, outcome).await.unwrap();
    drive(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["error"]["code"],
        "workspace_handoff_rejected"
    );
    let public = f
        .agents
        .actors()
        .unwrap()
        .task(input.task_id)
        .await
        .unwrap();
    assert!(public.task.workspace_input.is_none());
    if let Some(id) = public.run_id {
        assert!(
            f.engine.status(id).await.unwrap().record().input["input"]
                .get("workspace_input")
                .is_none()
        );
    }
}
