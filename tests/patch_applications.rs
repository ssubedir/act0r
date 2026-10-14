//! Public actor-to-reviewed-project application, using real files and durable Runs.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, patch_application::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

async fn task(root: &Path, policy: &str) -> (Engine, AgentService, MessageId, PathBuf) {
    let source = root.join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/lib.rs"), "first\r\nold café\r\nlast").unwrap();
    fs::write(source.join("unrelated.txt"), "keep this\n").unwrap();
    let (engine, agents) = local_app(root.join("app.db")).await.unwrap();
    let mut config = json!({"goal":"Repair", "target":{"provider":"scripted","model":"apply","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,"change_review":true,
        "tools":[{"name":"replace_text","version":1},{"name":"create_file","version":1}],"writable_paths":["src/"],
        "script":[{"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"old café","new":"new café"}},
            {"kind":"call_tool","tool":"create_file","arguments":{"path":"src/new.txt","content":"new file"}},
            {"kind":"finish","summary":"Repaired"}]});
    if policy == "planner" {
        let reviewer = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({"goal":"Review","target":{"provider":"scripted","model":"review","endpoint":""},"max_model_calls":1,"max_tokens":10000,"max_output_tokens":512,"script":[{"kind":"finish","summary":"Reviewed"}]}),5,Default::default()).await.unwrap();
        config["workers"] = json!([{"name":"reviewer","agent_id":reviewer,"description":"Review"}]);
        config["delegation"] = json!({"max_tasks":1,"max_depth":1});
    }
    let actor = agents
        .create_registered_actor(
            DefinitionId::new(policy, if policy == "planner" { 3 } else { 1 }),
            Some(source.clone()),
            config,
            20,
            Default::default(),
        )
        .await
        .unwrap();
    let task = agents
        .actors()
        .unwrap()
        .send(AgentMessage {
            id: MessageId::new(),
            recipient: actor,
            sender: None,
            correlation_id: None,
            kind: "task".into(),
            payload: json!({"goal":"Repair"}),
        })
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..100 {
        worker.step().await.unwrap();
        let view = agents.actors().unwrap().task(task).await.unwrap();
        if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
            engine
                .signal(run, wait.id, json!({"approved":true}))
                .await
                .unwrap();
        }
        if view.status.is_terminal() {
            break;
        }
    }
    assert!(
        agents
            .actors()
            .unwrap()
            .task_changes(task)
            .await
            .unwrap()
            .changes
            .is_some()
    );
    drop(worker);
    (engine, agents, task, fs::canonicalize(source).unwrap())
}
async fn next(worker: &mut Worker) -> ActivityExecution {
    for _ in 0..100 {
        worker.advance_work().await.unwrap();
        if let Some(call) = worker.admit().await.unwrap() {
            return call;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("activity was not admitted");
}
async fn execute(worker: &mut Worker) -> ActivityExecution {
    let call = next(worker).await;
    let outcome = worker.execute(call.clone()).await.unwrap();
    worker.complete(call.clone(), outcome).await.unwrap();
    call
}
fn untouched(source: &Path) {
    assert_eq!(
        fs::read_to_string(source.join("src/lib.rs")).unwrap(),
        "first\r\nold café\r\nlast"
    );
    assert!(!source.join("src/new.txt").exists());
}
#[tokio::test]
async fn both_policies_apply_immutable_saved_bytes_after_workspace_removal_and_two_restarts() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents, task, source) = task(dir.path(), policy).await;
        let artifact = agents
            .actors()
            .unwrap()
            .task_changes(task)
            .await
            .unwrap()
            .changes
            .unwrap();
        let ws = PathBuf::from(
            engine.status(artifact.run_id).await.unwrap().record().input["input"]["workspace"]
                .as_str()
                .unwrap(),
        );
        fs::rename(&ws, ws.with_extension("removed")).unwrap();
        // Unrelated target drift is preserved; the first slice compares affected files only.
        fs::write(source.join("unrelated.txt"), "user's later change\n").unwrap();
        let app = agents.actors().unwrap().patch_applications().unwrap();
        assert!(app.preview(task, PathBuf::from("relative")).await.is_err());
        let id = app.preview(task, source.clone()).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        let preparation = execute(&mut worker).await;
        assert_eq!(preparation.handler, handler("prepare"));
        let saved = app.status(id).await.unwrap();
        assert_eq!(saved.status, RunStatus::Waiting);
        assert_eq!(saved.wait.as_ref().unwrap().key, APPROVAL_KEY);
        assert_eq!(saved.preview.as_ref().unwrap().files.len(), 2);
        assert!(!source.join(".act0r-tools").exists());
        untouched(&source);
        worker.step().await.unwrap();
        assert_eq!(engine.status(id).await.unwrap().attempts().len(), 1);
        assert!(
            engine
                .signal(
                    id,
                    saved.wait.as_ref().unwrap().id,
                    json!({"approved":"yes"})
                )
                .await
                .is_err()
        );
        assert_eq!(json!(saved), json!(app.status(id).await.unwrap()));
        drop(worker);
        drop(app);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
        let app = agents.actors().unwrap().patch_applications().unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert_eq!(json!(saved), json!(app.status(id).await.unwrap()));
        app.approve(id, true).await.unwrap();
        app.approve(id, true).await.unwrap();
        assert!(app.approve(id, false).await.is_err());
        let write = next(&mut worker).await;
        assert_eq!(write.handler, handler("write"));
        assert_eq!(write.input["input"]["changes"], json!(artifact));
        assert_eq!(write.handler.version, 1);
        let result = worker.execute(write.clone()).await.unwrap();
        worker
            .complete(write.clone(), result.clone())
            .await
            .unwrap();
        worker.complete(write, result).await.unwrap();
        assert_eq!(
            fs::read_to_string(source.join("src/lib.rs")).unwrap(),
            "first\r\nnew café\r\nlast"
        );
        assert_eq!(
            fs::read_to_string(source.join("src/new.txt")).unwrap(),
            "new file"
        );
        assert_eq!(
            fs::read_to_string(source.join("unrelated.txt")).unwrap(),
            "user's later change\n"
        );
        let completed = app.status(id).await.unwrap();
        assert_eq!(
            completed.report.as_ref().unwrap().status,
            ApplicationStatus::Applied
        );
        assert_eq!(
            engine
                .status(id)
                .await
                .unwrap()
                .record()
                .usage
                .admitted_model_calls,
            0
        );
        drop(worker);
        drop(app);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(dir.path().join("app.db")).await.unwrap();
        let _worker = engine.worker().await.unwrap();
        assert_eq!(
            json!(completed),
            json!(
                agents
                    .actors()
                    .unwrap()
                    .patch_applications()
                    .unwrap()
                    .status(id)
                    .await
                    .unwrap()
            )
        );
    }
}
#[tokio::test]
async fn preview_and_post_approval_conflicts_refuse_every_project_write() {
    for before_preview in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents, task, source) = task(dir.path(), "tool").await;
        let app = agents.actors().unwrap().patch_applications().unwrap();
        let id = app.preview(task, source.clone()).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        if before_preview {
            fs::write(source.join("src/new.txt"), "user file").unwrap();
        }
        execute(&mut worker).await;
        if !before_preview {
            app.approve(id, true).await.unwrap();
            fs::write(source.join("src/new.txt"), "user file").unwrap();
            execute(&mut worker).await;
        }
        let report = app.status(id).await.unwrap().report.unwrap();
        assert_eq!(report.status, ApplicationStatus::Conflicted);
        assert!(report.applied_files.is_empty());
        assert_eq!(report.conflicts[0].path, "src/new.txt");
        assert_eq!(
            fs::read_to_string(source.join("src/lib.rs")).unwrap(),
            "first\r\nold café\r\nlast"
        );
        assert_eq!(
            fs::read_to_string(source.join("src/new.txt")).unwrap(),
            "user file"
        );
        assert!(app.approve(id, false).await.is_err());
    }
}
#[tokio::test]
async fn rejection_pause_cancellation_and_attempt_cap_keep_target_unchanged() {
    for control in ["reject", "pause", "cancel", "limit"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents, task, source) = task(dir.path(), "tool").await;
        let app = agents.actors().unwrap().patch_applications().unwrap();
        let id = if control == "limit" {
            let artifact = agents
                .actors()
                .unwrap()
                .task_changes(task)
                .await
                .unwrap()
                .changes
                .unwrap();
            engine
                .start(
                    workflow(),
                    json!(ApplicationInput {
                        task_id: task,
                        target: source.to_string_lossy().into_owned(),
                        changes: artifact
                    }),
                    1,
                )
                .await
                .unwrap()
        } else {
            app.preview(task, source.clone()).await.unwrap()
        };
        let mut worker = engine.worker().await.unwrap();
        execute(&mut worker).await;
        match control {
            "reject" => {
                app.approve(id, false).await.unwrap();
                assert_eq!(
                    app.status(id).await.unwrap().report.unwrap().status,
                    ApplicationStatus::Rejected
                );
            }
            "pause" => {
                engine.pause(id).await.unwrap();
                app.approve(id, true).await.unwrap();
                worker.step().await.unwrap();
                assert_eq!(engine.status(id).await.unwrap().attempts().len(), 1);
                assert_eq!(app.status(id).await.unwrap().status, RunStatus::Paused);
            }
            "cancel" => {
                engine.cancel(id).await.unwrap();
                assert!(app.approve(id, true).await.is_err());
            }
            "limit" => {
                app.approve(id, true).await.unwrap();
                for _ in 0..10 {
                    worker.step().await.unwrap();
                }
                assert_eq!(
                    app.status(id).await.unwrap().status,
                    RunStatus::BudgetExhausted
                );
            }
            _ => unreachable!(),
        }
        untouched(&source);
        assert!(!source.join(".act0r-tools").exists());
    }
}
#[tokio::test]
async fn competing_previews_cannot_overwrite_the_first_application() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, task, source) = task(dir.path(), "tool").await;
    let app = agents.actors().unwrap().patch_applications().unwrap();
    let a = app.preview(task, source.clone()).await.unwrap();
    let b = app.preview(task, source.clone()).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    execute(&mut worker).await;
    execute(&mut worker).await;
    app.approve(a, true).await.unwrap();
    app.approve(b, true).await.unwrap();
    execute(&mut worker).await;
    execute(&mut worker).await;
    assert_eq!(
        app.status(a).await.unwrap().report.unwrap().status,
        ApplicationStatus::Applied
    );
    assert_eq!(
        app.status(b).await.unwrap().report.unwrap().status,
        ApplicationStatus::Conflicted
    );
    assert_eq!(
        fs::read_to_string(source.join("src/new.txt")).unwrap(),
        "new file"
    );
}
fn cli(root: &Path, args: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_act0r"));
    command
        .current_dir(root)
        .arg("--db")
        .arg(root.join("app.db"))
        .arg("--json")
        .args(args);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    command.output().unwrap()
}
#[tokio::test]
async fn cli_creates_inspects_approves_and_rejects_saved_application_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents, task, source) = task(dir.path(), "tool").await;
    drop(agents);
    drop(engine);
    let output = cli(
        dir.path(),
        &[
            "actor",
            "task",
            "preview-changes",
            &task.to_string(),
            "--target",
            "source",
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let id: RunId = serde_json::from_slice::<Value>(&output.stdout).unwrap()["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    untouched(&source);
    assert!(
        !cli(
            dir.path(),
            &["actor", "task", "apply-changes", &id.to_string()]
        )
        .status
        .success()
    );
    assert!(
        cli(dir.path(), &["worker", "run", "--until-idle"])
            .status
            .success()
    );
    let output = cli(
        dir.path(),
        &["actor", "task", "application", &id.to_string()],
    );
    let view: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(view["status"], "waiting");
    assert_eq!(view["preview"]["files"].as_array().unwrap().len(), 2);
    assert!(
        cli(
            dir.path(),
            &["actor", "task", "apply-changes", &id.to_string()]
        )
        .status
        .success()
    );
    untouched(&source);
    assert!(
        cli(dir.path(), &["worker", "run", "--until-idle"])
            .status
            .success()
    );
    let output = cli(
        dir.path(),
        &["actor", "task", "application", &id.to_string()],
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["report"]["status"],
        "applied"
    );
    // A separate target copy allows a distinct preview to be rejected.
    fs::create_dir_all(dir.path().join("other/src")).unwrap();
    fs::write(
        dir.path().join("other/src/lib.rs"),
        "first\r\nold café\r\nlast",
    )
    .unwrap();
    let output = cli(
        dir.path(),
        &[
            "actor",
            "task",
            "preview-changes",
            &task.to_string(),
            "--target",
            "other",
        ],
    );
    let other = serde_json::from_slice::<Value>(&output.stdout).unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        cli(dir.path(), &["worker", "run", "--until-idle"])
            .status
            .success()
    );
    assert!(
        cli(dir.path(), &["actor", "task", "reject-changes", &other])
            .status
            .success()
    );
    assert_eq!(
        serde_json::from_slice::<Value>(
            &cli(dir.path(), &["actor", "task", "application", &other]).stdout
        )
        .unwrap()["report"]["status"],
        "rejected"
    );
    assert!(
        !cli(
            dir.path(),
            &[
                "actor",
                "task",
                "preview-changes",
                &MessageId::new().to_string(),
                "--target",
                "source"
            ]
        )
        .status
        .success()
    );
}
#[tokio::test]
async fn binary_missing_parent_case_alias_and_managed_targets_are_refused() {
    for error in [
        "binary",
        "missing_parent",
        "case_alias",
        "managed",
        "root_replaced",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents, task, source) = task(dir.path(), "tool").await;
        let app = agents.actors().unwrap().patch_applications().unwrap();
        let id = app.preview(task, source.clone()).await.unwrap();
        match error {
            "binary" => fs::write(source.join("src/lib.rs"), [0, 255]).unwrap(),
            "missing_parent" => fs::rename(source.join("src"), source.join("gone")).unwrap(),
            "case_alias" => {
                fs::rename(source.join("src/lib.rs"), source.join("src/LIB.rs")).unwrap()
            }
            "managed" => {
                fs::create_dir(source.join(".act0r-tools")).unwrap();
                fs::write(source.join(".act0r-tools/managed"), "act0r workspace v1\n").unwrap();
            }
            _ => {}
        }
        let mut worker = engine.worker().await.unwrap();
        execute(&mut worker).await;
        if error == "managed" {
            assert_eq!(app.status(id).await.unwrap().status, RunStatus::Failed);
        } else if error == "root_replaced" {
            app.approve(id, true).await.unwrap();
            fs::rename(&source, source.with_extension("old")).unwrap();
            fs::create_dir_all(source.join("src")).unwrap();
            fs::write(source.join("src/lib.rs"), "first\r\nold café\r\nlast").unwrap();
            execute(&mut worker).await;
            assert_eq!(app.status(id).await.unwrap().status, RunStatus::Blocked);
            untouched(&source);
        } else {
            assert_eq!(
                app.status(id).await.unwrap().report.unwrap().status,
                ApplicationStatus::Conflicted
            );
            assert!(!source.join("src/new.txt").exists());
        }
    }
}
