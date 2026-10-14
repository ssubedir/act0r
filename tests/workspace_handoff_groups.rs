//! Parallel actors consume one immutable input, retain partial results, and import exact patches.
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
    time::{Duration, Instant},
};

struct Fixture {
    engine: Engine,
    agents: AgentService,
    children: [AgentId; 2],
    parent: AgentId,
    task: MessageId,
    source: PathBuf,
}
fn config() -> Value {
    json!({"goal":"repair", "target":{"provider":"scripted","model":"group-handoff","endpoint":""},
        "max_model_calls":16,"max_tokens":1000000,"max_output_tokens":512,"change_review":true})
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
async fn setup(root: &Path, mode: &str) -> Fixture {
    let source = root.join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/a.rs"), "initial café\n").unwrap();
    fs::write(source.join("src/b.rs"), "initial b\n").unwrap();
    fs::write(source.join("README.md"), "original\n").unwrap();
    let (engine, agents) = local_app(root.join("app.db")).await.unwrap();
    let mut children = vec![];
    for index in 0..2 {
        let mut child = config();
        child["accept_workspace_handoff"] = json!(mode != "unsupported" || index == 0);
        child["tools"] = json!([{"name":"replace_text","version":1}]);
        let path = if index == 0 || mode == "conflict" {
            "src/a.rs"
        } else {
            "src/b.rs"
        };
        child["writable_paths"] = json!([path]);
        child["script"] = json!([
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":path,"old":if index == 0 || mode == "conflict" {"current café"} else {"current b"},"new":if index == 0 {"fixed a"} else {"fixed b"}}},
            {"kind":"finish","summary":format!("fixed {index}")}
        ]);
        if index == 1 && matches!(mode, "held" | "capacity" | "deadline") {
            child["script"].as_array_mut().unwrap().insert(
                0,
                json!({"kind":"ask_human","question":"Continue second repair?"}),
            );
        }
        children.push(
            agents
                .create_registered_actor(
                    DefinitionId::new("tool", 1),
                    Some(source.clone()),
                    child,
                    40,
                    if mode == "capacity" && index == 1 {
                        AgentQueuePolicy { task_capacity: 1 }
                    } else {
                        Default::default()
                    },
                )
                .await
                .unwrap(),
        );
    }
    let mut parent = config();
    parent["workspace_handoff"] = json!(true);
    parent["child_changes"] = json!(true);
    parent["workers"] = json!([
        {"name":"first","agent_id":children[0],"description":"repair a"},
        {"name":"second","agent_id":children[1],"description":"repair b"}
    ]);
    parent["delegation"] = json!({"max_tasks":4,"max_depth":2,"max_parallel_tasks":2,"shared_budget":{"max_model_calls":20,"max_tokens":2000000}});
    if mode == "budget" {
        parent["delegation"]["shared_budget"]["max_model_calls"] = json!(3);
    }
    if mode == "deadline" {
        parent["delegation"]["timeout_ms"] = json!(5000);
    }
    parent["tools"] = json!([{"name":"replace_text","version":1}]);
    parent["writable_paths"] = json!(["src/", "README.md"]);
    parent["script"] = json!([
        {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/a.rs","old":"initial café","new":"current café"}},
        {"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/b.rs","old":"initial b","new":"current b"}},
        {"kind":"delegate_tasks","tasks":[{"worker":"first","goal":"repair a using current code"},{"worker":"second","goal":"repair b using current code"}]}
    ]);
    if !matches!(mode, "unsupported" | "capacity" | "budget" | "deadline") {
        parent["script"].as_array_mut().unwrap().extend([
            json!({"kind":"inspect_child_changes","worker":"first","offset":0}),
            json!({"kind":"import_child_changes","worker":"first"}),
            json!({"kind":"inspect_child_changes","worker":"second","offset":0}),
            json!({"kind":"import_child_changes","worker":"second"}),
        ]);
    }
    parent["script"]
        .as_array_mut()
        .unwrap()
        .push(json!({"kind":"finish","summary":"handled group"}));
    let parent = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(source.clone()),
            parent,
            80,
            Default::default(),
        )
        .await
        .unwrap();
    let task = agents
        .actors()
        .unwrap()
        .send(message(
            parent,
            json!({"goal":"repair both","workspace_input":{"workspace":"forged"}}),
        ))
        .await
        .unwrap();
    Fixture {
        engine,
        agents,
        children: [children[0], children[1]],
        parent,
        task,
        source,
    }
}
async fn approvals(f: &Fixture) {
    for actor in [f.parent, f.children[0], f.children[1]] {
        for view in f.agents.actors().unwrap().tasks(actor).await.unwrap() {
            if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
                if wait.key == "answer_clarification" {
                    continue;
                }
                assert!(
                    wait.key == "native_tool_approval" || wait.key == child_changes::APPROVAL_KEY
                );
                f.engine
                    .signal(run, wait.id, json!({"approved":true}))
                    .await
                    .unwrap();
            }
        }
    }
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
async fn until(
    f: &Fixture,
    worker: &mut Worker,
    stop: Option<DefinitionId>,
) -> Option<ActivityExecution> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        worker.advance_work().await.unwrap();
        approvals(f).await;
        if let Some(call) = worker.admit().await.unwrap() {
            if stop.as_ref() == Some(&call.handler) {
                return Some(call);
            }
            let outcome = worker.execute(call.clone()).await.unwrap();
            worker.complete(call, outcome).await.unwrap();
        }
        let view = f.agents.actors().unwrap().task(f.task).await.unwrap();
        if view.status.is_terminal() {
            return None;
        }
        assert_ne!(
            view.status,
            AgentTaskStatus::Blocked,
            "{:?}",
            parent_run(f).await.record().error
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!(
        "group did not settle: {:?}",
        parent_run(f).await.record().status
    );
}
async fn adapter(root: &Path) -> LocalWorkspaceHandoff {
    let managed = root.join(".act0r/workspaces");
    LocalWorkspaceHandoff::new(
        Arc::new(SqliteStore::open(root.join("app.db")).await.unwrap()),
        Arc::new(LocalCandidates::new(
            root.join(".act0r/snapshots"),
            managed.clone(),
        )),
        Arc::new(LocalTools::new(managed)),
    )
}
fn prepared(outcome: &Outcome) -> PreparedGroup {
    let Outcome::Success(value) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(value["ok"], true, "{value}");
    serde_json::from_value(value["prepared"].clone()).unwrap()
}
async fn reopen(f: Fixture, root: &Path) -> Fixture {
    let Fixture {
        engine,
        agents,
        children,
        parent,
        task,
        source,
    } = f;
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(root.join("app.db")).await.unwrap();
    Fixture {
        engine,
        agents,
        children,
        parent,
        task,
        source,
    }
}

#[tokio::test]
async fn group_uses_one_latest_snapshot_two_isolated_copies_and_individual_imports() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "success").await;
    let mut worker = f.engine.worker().await.unwrap();
    let call = until(&f, &mut worker, Some(handler("prepare_group")))
        .await
        .unwrap();
    let outcome = worker.execute(call.clone()).await.unwrap();
    let group = prepared(&outcome);
    group.validate().unwrap();
    assert_ne!(group.workspaces[0].task_id, group.workspaces[1].task_id);
    assert_ne!(group.workspaces[0].workspace, group.workspaces[1].workspace);
    assert_eq!(
        group.workspaces[0].snapshot_id,
        group.workspaces[1].snapshot_id
    );
    assert_eq!(
        group.workspaces[0].snapshot_digest,
        group.workspaces[1].snapshot_digest
    );
    assert_eq!(
        fs::read_dir(dir.path().join(".act0r/snapshots"))
            .unwrap()
            .count(),
        1
    );
    for input in &group.workspaces {
        assert_eq!(
            fs::read_to_string(Path::new(&input.workspace).join("src/a.rs")).unwrap(),
            "current café\n"
        );
        assert_eq!(
            fs::read_to_string(Path::new(&input.workspace).join("src/b.rs")).unwrap(),
            "current b\n"
        );
    }
    worker.complete(call, outcome).await.unwrap();
    until(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(
        run.record().status,
        RunStatus::Succeeded,
        "{:?}",
        run.record().error
    );
    assert_eq!(run.record().task_groups[0].id, group.id().unwrap());
    assert_eq!(
        run.record().task_groups[0].children,
        group
            .workspaces
            .iter()
            .map(|p| p.task_id)
            .collect::<Vec<_>>()
    );
    assert!(run.record().task_groups[0].joined_at.is_some());
    assert_eq!(
        fs::read_to_string(workspace(&run).join("src/a.rs")).unwrap(),
        "fixed a\n"
    );
    assert_eq!(
        fs::read_to_string(workspace(&run).join("src/b.rs")).unwrap(),
        "fixed b\n"
    );
    for (index, input) in group.workspaces.iter().enumerate() {
        let view = f
            .agents
            .actors()
            .unwrap()
            .task(input.task_id)
            .await
            .unwrap();
        assert_eq!(view.task.workspace_input.as_ref(), Some(input));
        let child = f.engine.status(view.run_id.unwrap()).await.unwrap();
        assert_eq!(workspace(&child), PathBuf::from(&input.workspace));
        assert_eq!(
            child.record().input["input"]["writable_paths"],
            json!([if index == 0 { "src/a.rs" } else { "src/b.rs" }])
        );
        let changes = f
            .agents
            .actors()
            .unwrap()
            .task_changes(input.task_id)
            .await
            .unwrap()
            .changes
            .unwrap();
        assert_eq!(changes.baseline_digest, input.snapshot_digest);
        assert_eq!(changes.files.len(), 1);
        assert_eq!(
            fs::read_to_string(Path::new(&input.workspace).join(if index == 0 {
                "src/b.rs"
            } else {
                "src/a.rs"
            }))
            .unwrap(),
            if index == 0 {
                "current b\n"
            } else {
                "current café\n"
            }
        );
        let pins = f
            .agents
            .actors()
            .unwrap()
            .status(f.children[index])
            .await
            .unwrap();
        let default = PathBuf::from(pins.record().config["input"]["workspace"].as_str().unwrap());
        assert_eq!(
            fs::read_to_string(default.join("src/a.rs")).unwrap(),
            "initial café\n"
        );
    }
    assert_eq!(
        fs::read_to_string(f.source.join("src/a.rs")).unwrap(),
        "initial café\n"
    );
    assert_eq!(
        run.activities()
            .iter()
            .filter(|a| a.spec.handler == child_changes::handler("import"))
            .count(),
        2
    );
}

#[tokio::test]
async fn partial_results_and_a_human_wait_survive_restart_then_join_once() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "held").await;
    let mut worker = f.engine.worker().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let wait = loop {
        assert!(Instant::now() < deadline);
        worker.step().await.unwrap();
        approvals(&f).await;
        let run = parent_run(&f).await;
        if run.record().delegations.len() == 2 && run.record().delegations[0].outcome.is_some() {
            let view = f
                .agents
                .actors()
                .unwrap()
                .task(run.record().delegations[1].id)
                .await
                .unwrap();
            if let Some(wait) = view.wait {
                break (view.run_id.unwrap(), wait);
            }
        }
    };
    let before = parent_run(&f).await;
    assert!(before.record().delegations[1].outcome.is_none());
    assert!(before.record().task_groups[0].joined_at.is_none());
    drop(worker);
    let f = reopen(f, dir.path()).await;
    let saved = parent_run(&f).await;
    assert_eq!(
        json!(saved.record().delegations),
        json!(before.record().delegations)
    );
    f.engine
        .signal(wait.0, wait.1.id, json!({"answer":"continue"}))
        .await
        .unwrap();
    let mut worker = f.engine.worker().await.unwrap();
    until(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    let history = f.engine.history(run.record().id).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    for actor in f.children {
        assert_eq!(
            f.agents.actors().unwrap().tasks(actor).await.unwrap().len(),
            1
        );
    }
}

#[tokio::test]
async fn group_preparation_replays_after_parent_drift_and_a_partially_published_copy_set() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "success").await;
    let mut worker = f.engine.worker().await.unwrap();
    let call = until(&f, &mut worker, Some(handler("prepare_group")))
        .await
        .unwrap();
    let port = adapter(dir.path()).await;
    let outcome = port.execute(call.clone()).await.unwrap();
    let group = prepared(&outcome);
    let snapshot = dir
        .path()
        .join(".act0r/snapshots")
        .join(group.workspaces[0].snapshot_id.to_string());
    fs::remove_file(snapshot.join(".act0r-tools/handoff-group.json")).unwrap();
    let second = PathBuf::from(&group.workspaces[1].workspace);
    assert!(
        fs::canonicalize(&second)
            .unwrap()
            .starts_with(fs::canonicalize(dir.path()).unwrap())
    );
    fs::remove_dir_all(second).unwrap();
    fs::write(
        workspace(&parent_run(&f).await).join("README.md"),
        "later parent\n",
    )
    .unwrap();
    assert_eq!(port.execute(call.clone()).await.unwrap(), outcome);
    assert_eq!(
        fs::read_to_string(Path::new(&group.workspaces[1].workspace).join("README.md")).unwrap(),
        "original\n"
    );
    assert_eq!(port.execute(call.clone()).await.unwrap(), outcome);
    worker.complete(call, outcome).await.unwrap();
    until(&f, &mut worker, None).await;
    assert_eq!(parent_run(&f).await.record().status, RunStatus::Succeeded);
}

#[tokio::test]
async fn overlapping_patches_report_conflicts_without_overwriting_the_first_import() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "conflict").await;
    let mut worker = f.engine.worker().await.unwrap();
    until(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(workspace(&run).join("src/a.rs")).unwrap(),
        "fixed a\n"
    );
    assert_eq!(
        run.activities()
            .iter()
            .filter(|a| a.spec.handler == child_changes::handler("import"))
            .count(),
        1
    );
    let inspections: Vec<_> = run
        .activities()
        .iter()
        .filter(|a| a.spec.handler == child_changes::handler("inspect"))
        .collect();
    assert_eq!(inspections.len(), 2);
    assert!(
        inspections[1].result.as_ref().unwrap()["inspection"]["preview"]["files"]
            .as_array()
            .unwrap()
            .iter()
            .any(|file| file["conflict"].is_string())
    );
}

#[tokio::test]
async fn worker_opt_in_is_checked_for_every_member_before_any_child_is_published() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "unsupported").await;
    let mut worker = f.engine.worker().await.unwrap();
    until(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert!(run.record().delegations.is_empty());
    let prep = run
        .activities()
        .iter()
        .find(|a| a.spec.handler == handler("prepare_group"))
        .unwrap();
    assert_eq!(prep.result.as_ref().unwrap()["ok"], false);
    for actor in f.children {
        assert!(
            f.agents
                .actors()
                .unwrap()
                .tasks(actor)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn a_full_mailbox_rejects_one_prepared_member_and_keeps_the_successful_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "capacity").await;
    f.agents
        .actors()
        .unwrap()
        .send(message(f.children[1], json!({"goal":"occupy mailbox"})))
        .await
        .unwrap();
    let mut worker = f.engine.worker().await.unwrap();
    until(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "succeeded"
    );
    assert_eq!(
        run.record().delegations[1].outcome.as_ref().unwrap()["status"],
        "rejected"
    );
    assert_eq!(
        run.record().delegations[1].outcome.as_ref().unwrap()["error"]["code"],
        "queue_full"
    );
    assert!(run.record().task_groups[0].joined_at.is_some());
}

#[tokio::test]
async fn shared_model_allowance_cannot_be_reset_by_handed_off_copies() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "budget").await;
    let mut worker = f.engine.worker().await.unwrap();
    until(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::BudgetExhausted);
    assert_eq!(run.record().usage.admitted_model_calls, 3);
    for input in &run.record().delegations {
        let child = f.agents.actors().unwrap().task(input.id).await.unwrap();
        assert_eq!(child.status, AgentTaskStatus::BudgetExhausted);
        assert_eq!(
            f.engine
                .status(child.run_id.unwrap())
                .await
                .unwrap()
                .record()
                .usage
                .admitted_model_calls,
            0
        );
    }
}

#[tokio::test]
async fn partial_mailbox_publication_survives_restart_without_duplicate_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "success").await;
    let mut worker = f.engine.worker().await.unwrap();
    let call = until(&f, &mut worker, Some(handler("prepare_group")))
        .await
        .unwrap();
    let outcome = worker.execute(call.clone()).await.unwrap();
    let group = prepared(&outcome);
    worker.complete(call, outcome).await.unwrap();
    for _ in 0..10 {
        worker.advance_work().await.unwrap();
        if f.agents
            .actors()
            .unwrap()
            .tasks(f.children[0])
            .await
            .unwrap()
            .len()
            == 1
        {
            break;
        }
    }
    assert_eq!(
        f.agents
            .actors()
            .unwrap()
            .tasks(f.children[0])
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        f.agents
            .actors()
            .unwrap()
            .tasks(f.children[1])
            .await
            .unwrap()
            .is_empty()
    );
    drop(worker);
    let f = reopen(f, dir.path()).await;
    let mut worker = f.engine.worker().await.unwrap();
    until(&f, &mut worker, None).await;
    assert_eq!(parent_run(&f).await.record().status, RunStatus::Succeeded);
    for input in group.workspaces {
        let tasks = f
            .agents
            .actors()
            .unwrap()
            .tasks(input.request.recipient)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task.id, input.task_id);
        assert_eq!(tasks[0].task.workspace_input.as_ref(), Some(&input));
    }
}

#[tokio::test]
async fn prepared_groups_obey_goal_holds_and_cancellation_before_publication() {
    for mode in ["pause", "cancel"] {
        let dir = tempfile::tempdir().unwrap();
        let f = setup(dir.path(), "success").await;
        let mut worker = f.engine.worker().await.unwrap();
        let call = until(&f, &mut worker, Some(handler("prepare_group")))
            .await
            .unwrap();
        let outcome = worker.execute(call.clone()).await.unwrap();
        prepared(&outcome);
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
        drop(worker);
        let f = reopen(f, dir.path()).await;
        let mut worker = f.engine.worker().await.unwrap();
        for _ in 0..8 {
            worker.advance_work().await.unwrap();
            assert!(worker.admit().await.unwrap().is_none());
        }
        for actor in f.children {
            assert!(
                f.agents
                    .actors()
                    .unwrap()
                    .tasks(actor)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        if mode == "pause" {
            f.agents
                .actors()
                .unwrap()
                .resume_goal(f.task)
                .await
                .unwrap();
            until(&f, &mut worker, None).await;
            assert_eq!(parent_run(&f).await.record().status, RunStatus::Succeeded);
        } else {
            assert_eq!(parent_run(&f).await.record().status, RunStatus::Cancelled);
        }
    }
}

#[tokio::test]
async fn a_modified_group_copy_fails_before_its_model_without_discarding_the_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "capacity").await; // This mode finishes after inspecting the joined outcomes.
    let mut worker = f.engine.worker().await.unwrap();
    let call = until(&f, &mut worker, Some(handler("prepare_group")))
        .await
        .unwrap();
    let outcome = worker.execute(call.clone()).await.unwrap();
    let group = prepared(&outcome);
    fs::write(
        Path::new(&group.workspaces[1].workspace).join("src/a.rs"),
        "tampered\n",
    )
    .unwrap();
    worker.complete(call, outcome).await.unwrap();
    until(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "succeeded"
    );
    let second = f
        .agents
        .actors()
        .unwrap()
        .task(group.workspaces[1].task_id)
        .await
        .unwrap();
    assert_eq!(second.status, AgentTaskStatus::Failed);
    assert_eq!(
        f.engine
            .status(second.run_id.unwrap())
            .await
            .unwrap()
            .record()
            .usage
            .admitted_model_calls,
        0
    );
}

#[tokio::test]
async fn changed_member_order_and_forged_group_bindings_are_rejected_before_publication() {
    for mode in ["order", "digest", "task", "request", "parent"] {
        let dir = tempfile::tempdir().unwrap();
        let f = setup(dir.path(), "success").await;
        let mut worker = f.engine.worker().await.unwrap();
        let call = until(&f, &mut worker, Some(handler("prepare_group")))
            .await
            .unwrap();
        let outcome = worker.execute(call.clone()).await.unwrap();
        let mut group = prepared(&outcome);
        match mode {
            "order" => group.workspaces.swap(0, 1),
            "digest" => group.workspaces[1].snapshot_digest = "a".repeat(64),
            "task" => group.workspaces[1].task_id = group.workspaces[0].task_id,
            "request" => {
                group.request.tasks[1].goal = "another goal".into();
                group.workspaces[1].request.goal = "another goal".into();
            }
            "parent" => {
                group.parent_run_id = RunId::new();
                for p in &mut group.workspaces {
                    p.parent_run_id = group.parent_run_id;
                }
            }
            _ => unreachable!(),
        }
        worker
            .complete(call, Outcome::Success(json!({"ok":true,"prepared":group})))
            .await
            .unwrap();
        let run = parent_run(&f).await;
        assert_eq!(run.record().status, RunStatus::Failed, "{mode}");
        assert_eq!(
            run.record().error.as_ref().unwrap().code,
            "invalid_workflow"
        );
        assert!(run.record().delegations.is_empty());
        for actor in f.children {
            assert!(
                f.agents
                    .actors()
                    .unwrap()
                    .tasks(actor)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }
}

#[tokio::test]
async fn saved_group_deadline_expires_while_the_worker_is_down_and_retains_the_completed_child() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "deadline").await;
    let mut worker = f.engine.worker().await.unwrap();
    let stop = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(Instant::now() < stop);
        worker.step().await.unwrap();
        approvals(&f).await;
        let run = parent_run(&f).await;
        if run
            .record()
            .delegations
            .first()
            .is_some_and(|d| d.outcome.is_some())
        {
            break;
        }
    }
    let before = parent_run(&f).await;
    let deadline = before.record().task_groups[0].deadline_at.unwrap();
    assert_eq!(
        before.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "succeeded"
    );
    assert!(before.record().delegations[1].outcome.is_none());
    drop(worker);
    let f = reopen(f, dir.path()).await;
    // The persisted UTC deadline, rather than a sleep created after recovery, governs expiry.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    if deadline >= now {
        tokio::time::sleep(Duration::from_millis((deadline - now + 20) as u64)).await;
    }
    let mut worker = f.engine.worker().await.unwrap();
    until(&f, &mut worker, None).await;
    let run = parent_run(&f).await;
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.record().task_groups[0].deadline_at, Some(deadline));
    assert_eq!(
        run.record().delegations[0].outcome.as_ref().unwrap()["status"],
        "succeeded"
    );
    assert_eq!(
        run.record().delegations[1].outcome.as_ref().unwrap()["error"]["code"],
        "delegation_timeout"
    );
}

#[tokio::test]
async fn a_killed_host_reuses_the_prepared_group_and_publishes_each_child_once() {
    let dir = tempfile::tempdir().unwrap();
    let f = setup(dir.path(), "success").await;
    let mut worker = f.engine.worker().await.unwrap();
    let queued = loop {
        worker.advance_work().await.unwrap();
        approvals(&f).await;
        let run = parent_run(&f).await;
        if let Some(activity) = run.activities().iter().find(|a| {
            a.spec.handler == handler("prepare_group") && a.status == ActivityStatus::Queued
        }) {
            break activity.id;
        }
        if let Some(call) = worker.admit().await.unwrap() {
            let outcome = worker.execute(call.clone()).await.unwrap();
            worker.complete(call, outcome).await.unwrap();
        }
    };
    let parent_workspace = workspace(&parent_run(&f).await);
    let Fixture {
        engine,
        agents,
        children,
        parent,
        task,
        source,
    } = f;
    drop(worker);
    drop(agents);
    drop(engine);
    let marker = dir.path().join("prepared.json");
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.args([
        "--ignored",
        "--exact",
        "workspace_handoff_group_host_fixture",
        "--nocapture",
    ])
    .env("ACT0R_GROUP_HANDOFF_DB", dir.path().join("app.db"))
    .env("ACT0R_GROUP_HANDOFF_MARKER", &marker)
    .stdout(std::process::Stdio::null())
    .stderr(std::process::Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000);
    }
    let mut host = cmd.spawn().unwrap();
    let stop = Instant::now() + Duration::from_secs(30);
    while !marker.exists() && Instant::now() < stop {
        assert!(
            host.try_wait().unwrap().is_none(),
            "host exited before preparation"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if !marker.exists() {
        host.kill().unwrap();
        host.wait().unwrap();
        panic!("preparation timeout");
    }
    host.kill().unwrap();
    host.wait().unwrap();
    let (call, outcome): (ActivityExecution, Outcome) =
        serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
    assert_eq!(call.activity_id, queued);
    let group = prepared(&outcome);
    fs::write(
        parent_workspace.join("README.md"),
        "changed after snapshot\n",
    )
    .unwrap();
    let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
    let f = Fixture {
        engine,
        agents,
        children,
        parent,
        task,
        source,
    };
    let mut worker = f.engine.worker().await.unwrap();
    let retry = until(&f, &mut worker, Some(handler("prepare_group")))
        .await
        .unwrap();
    assert_eq!(retry.activity_id, call.activity_id);
    assert_eq!(retry.ordinal, 2);
    assert_eq!(worker.execute(retry.clone()).await.unwrap(), outcome);
    worker.complete(retry, outcome).await.unwrap();
    until(&f, &mut worker, None).await;
    assert_eq!(parent_run(&f).await.record().status, RunStatus::Succeeded);
    for input in group.workspaces {
        assert_eq!(
            f.agents
                .actors()
                .unwrap()
                .tasks(input.request.recipient)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            fs::read_to_string(Path::new(&input.workspace).join("README.md")).unwrap(),
            "original\n"
        );
    }
}

#[tokio::test]
#[ignore = "killed host fixture invoked by the consumer test"]
async fn workspace_handoff_group_host_fixture() {
    let db = PathBuf::from(std::env::var_os("ACT0R_GROUP_HANDOFF_DB").unwrap());
    let marker = PathBuf::from(std::env::var_os("ACT0R_GROUP_HANDOFF_MARKER").unwrap());
    let (engine, _agents) = local_app(db).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..300 {
        worker.advance_work().await.unwrap();
        if let Some(call) = worker.admit().await.unwrap() {
            assert_eq!(call.handler, handler("prepare_group"));
            let outcome = worker.execute(call.clone()).await.unwrap();
            prepared(&outcome);
            let staging = marker.with_extension("tmp");
            fs::write(&staging, serde_json::to_vec(&(call, outcome)).unwrap()).unwrap();
            fs::rename(staging, &marker).unwrap();
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("group was not admitted");
}
