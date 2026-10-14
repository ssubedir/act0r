use act0r::{
    adapters::{
        native_tools::RunCommandTool,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::ports::{NativeTool, WorkspaceCommandRunner, WorkspaceFactory},
    domain::{agent::AgentAction, native_context::NativeContextState, tools::*, *},
};
use serde_json::json;
use std::{fs, path::Path, sync::Arc, time::Duration};

fn command(fixture: &str, timeout_ms: u32) -> WorkspaceTestCommand {
    WorkspaceTestCommand {
        argv: vec![
            std::env::current_exe()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "--ignored".into(),
            "--exact".into(),
            fixture.into(),
            "--nocapture".into(),
        ],
        timeout_ms,
    }
}

#[test]
fn catalog_pins_selection_and_refuses_model_arguments_or_missing_host_context() {
    let tool = RunCommandTool::new(Arc::new(LocalTools::new("unused".into()))).definition();
    let commands: ProjectCommands = [("format".into(), command("unused", 1000))].into();
    let arguments = json!({"name":"format"});
    let Command::WaitForInput { payload, .. } = tool
        .request_call_with_commands(&arguments, Some("managed"), &[], None, &commands)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        payload["project_command"],
        json!({"name":"format","command":commands["format"]})
    );
    assert!(payload.get("test_command").is_none());
    let Command::Schedule(spec) = tool
        .schedule_call_with_commands(&arguments, Some("managed"), &[], None, &commands)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(spec.handler, DefinitionId::new("native.tool", 6));
    assert_eq!(spec.input["project_command"], payload["project_command"]);
    assert_eq!(spec.recovery, RecoveryPolicy::RequiresReconciliation);
    for args in [
        json!({"name":"missing"}),
        json!({"name":"format","argv":["injected"]}),
        json!({"name":"format","timeout_ms":1}),
        json!({"name":"format","workspace":"outside"}),
    ] {
        assert!(
            tool.request_call_with_commands(&args, Some("managed"), &[], None, &commands)
                .is_err()
        );
    }
    assert!(
        tool.request_call_with_context(&arguments, Some("managed"), &[], None)
            .is_err()
    );
    assert!(
        tool.schedule_call_with_commands(&arguments, None, &[], None, &commands)
            .is_err()
    );
    assert!(
        validate_tool_project_commands(std::slice::from_ref(&tool), &ProjectCommands::new())
            .is_err()
    );
    assert!(validate_tool_project_commands(&[], &commands).is_err());
    for name in ["", "bad name", "../format", &"n".repeat(65)] {
        assert!(validate_project_commands(&[(name.into(), command("unused", 1))].into()).is_err());
    }
    let large: ProjectCommands = (0..17)
        .map(|i| (format!("c{i}"), command("unused", 1)))
        .collect();
    assert!(validate_project_commands(&large).is_err());
    let selected = NamedWorkspaceCommand {
        name: "other".into(),
        command: commands["format"].clone(),
    };
    assert!(validate_project_command(&tool, &arguments, Some(&selected)).is_err());
    let mut context = NativeContextState::default();
    let tests = act0r::adapters::native_tools::RunTestsTool::new(Arc::new(LocalTools::new(
        "unused".into(),
    )))
    .definition();
    context
        .record_tool(
            &tests,
            ActivityId::new(),
            &AgentAction::CallTool {
                tool: "run_tests".into(),
                arguments: json!({}),
            },
            &json!({"ok":true,"exit_code":0}),
        )
        .unwrap();
    context
        .record_tool(
            &tool,
            ActivityId::new(),
            &AgentAction::CallTool {
                tool: "run_command".into(),
                arguments,
            },
            &json!({"ok":false,"exit_code":7}),
        )
        .unwrap();
    assert!(context.test_status.unwrap().stale);
}

async fn workspace() -> (tempfile::TempDir, LocalTools, String) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("README.md"), "before").unwrap();
    let managed = root.path().join("managed");
    let copy = LocalWorkspaces::new(managed.clone())
        .prepare(source)
        .await
        .unwrap();
    (root, LocalTools::new(managed), copy)
}

#[tokio::test]
async fn nonzero_command_output_is_bounded_and_source_stays_unchanged() {
    let (root, runner, copy) = workspace().await;
    let result = runner
        .run_command(
            copy.clone(),
            NamedWorkspaceCommand {
                name: "format".into(),
                command: command("format_fixture", 10_000),
            },
            ActivityId::new(),
        )
        .await
        .unwrap();
    assert_eq!(result["ok"], false);
    assert_eq!(result["exit_code"], 7);
    assert_eq!(result["command_name"], "format");
    assert_eq!(result["stdout_truncated"], true);
    assert_eq!(result["stderr_truncated"], true);
    assert!(result["stdout"].as_str().unwrap().len() <= 16_384);
    assert_eq!(
        fs::read_to_string(Path::new(&copy).join("README.md")).unwrap(),
        "formatted"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("source/README.md")).unwrap(),
        "before"
    );
}

#[test]
#[ignore = "invoked as a pinned native command"]
fn format_fixture() {
    use std::io::Write;
    fs::write("README.md", "formatted").unwrap();
    std::io::stdout().write_all(&vec![b'x'; 40_000]).unwrap();
    std::io::stderr().write_all(&vec![b'y'; 40_000]).unwrap();
    std::process::exit(7);
}

#[test]
#[ignore = "invoked as a pinned native command"]
#[allow(clippy::zombie_processes)] // Deliberate background child tests owner/job cleanup.
fn tree_fixture() {
    let mut process = std::process::Command::new(std::env::current_exe().unwrap());
    process.args(["--ignored", "--exact", "descendant_fixture", "--nocapture"]);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        process.creation_flags(0x08000000);
    }
    let _child = process.spawn().unwrap();
    while !Path::new("descendant-ready").exists() {
        std::thread::sleep(Duration::from_millis(5));
    }
    if Path::new("parent-exits").exists() {
        return;
    }
    std::thread::sleep(Duration::from_secs(30));
}

#[test]
#[ignore = "invoked by the process-tree fixture"]
fn descendant_fixture() {
    fs::write("heartbeat", "alive").unwrap();
    fs::write("descendant-ready", std::process::id().to_string()).unwrap();
    loop {
        fs::write("heartbeat", "alive").unwrap();
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[tokio::test]
async fn timeout_and_parent_exit_stop_descendants_and_release_workspace() {
    for exits in [false, true] {
        let (_root, runner, copy) = workspace().await;
        if exits {
            fs::write(Path::new(&copy).join("parent-exits"), "").unwrap();
        }
        let result = runner
            .run_command(
                copy.clone(),
                NamedWorkspaceCommand {
                    name: "build".into(),
                    command: command("tree_fixture", if exits { 10_000 } else { 1500 }),
                },
                ActivityId::new(),
            )
            .await;
        if exits {
            assert_eq!(result.unwrap()["ok"], true);
        } else {
            assert!(result.is_err());
        }
        assert!(Path::new(&copy).join("descendant-ready").exists());
        let heartbeat = Path::new(&copy).join("heartbeat");
        fs::remove_file(&heartbeat).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !heartbeat.exists(),
            "descendant still writes after command settles"
        );
        // Reacquire workspace ownership for a new command after cleanup.
        let result = runner
            .run_command(
                copy,
                NamedWorkspaceCommand {
                    name: "next".into(),
                    command: command("passing_fixture", 10_000),
                },
                ActivityId::new(),
            )
            .await
            .unwrap();
        assert_eq!(result["ok"], true);
    }
}

#[test]
#[ignore = "invoked as a pinned native command"]
fn passing_fixture() {
    println!("passed");
}

async fn descendant_ready(copy: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !Path::new(copy).join("descendant-ready").exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "descendant did not start"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn assert_descendant_stopped(copy: &str) {
    let heartbeat = Path::new(copy).join("heartbeat");
    // Give asynchronous OS termination a bounded grace after dropping a future/job.
    tokio::time::sleep(Duration::from_millis(100)).await;
    fs::remove_file(&heartbeat).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !heartbeat.exists(),
        "descendant still writes after owner stopped"
    );
}
#[tokio::test]
async fn dropping_command_future_stops_its_process_tree() {
    let (_root, runner, copy) = workspace().await;
    let task_copy = copy.clone();
    let task = tokio::spawn(async move {
        runner
            .run_command(
                task_copy,
                NamedWorkspaceCommand {
                    name: "build".into(),
                    command: command("tree_fixture", 30_000),
                },
                ActivityId::new(),
            )
            .await
    });
    descendant_ready(&copy).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_descendant_stopped(&copy).await;
}
#[cfg(windows)]
#[tokio::test]
async fn abrupt_windows_host_exit_closes_the_job_and_stops_descendants() {
    use std::os::windows::process::CommandExt;
    let (_root, _runner, copy) = workspace().await;
    let mut host = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "host_fixture", "--nocapture"])
        .current_dir(&copy)
        .creation_flags(0x08000000)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    descendant_ready(&copy).await;
    host.kill().unwrap();
    host.wait().unwrap();
    assert_descendant_stopped(&copy).await;
}
#[cfg(windows)]
#[test]
#[ignore = "invoked by abrupt host exit test"]
fn host_fixture() {
    let copy = std::env::current_dir().unwrap();
    let runner = LocalTools::new(copy.parent().unwrap().to_path_buf());
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(runner.run_command(
            copy.to_string_lossy().into_owned(),
            NamedWorkspaceCommand {
                name: "build".into(),
                command: command("tree_fixture", 30_000),
            },
            ActivityId::new(),
        ))
        .unwrap();
}
