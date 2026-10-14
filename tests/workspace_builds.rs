use act0r::{
    adapters::workspace::{LocalTools, LocalWorkspaces},
    application::ports::{ToolRunner, WorkspaceFactory, WorkspaceTestRunner},
    domain::{ActivityId, agent::*, tools::WorkspaceTestCommand},
};
use std::{fs, path::Path, time::Duration};

#[tokio::test]
async fn project_test_builds_ignore_the_parent_target_and_project_cache_override() {
    let directory = tempfile::tempdir().unwrap();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--ignored",
            "--exact",
            "workspace_build_child",
            "--nocapture",
        ])
        .env("ACT0R_WORKSPACE_BUILD_TEST_ROOT", directory.path())
        .env("CARGO_TARGET_DIR", directory.path().join("parent-cache"))
        .env("OPENAI_API_KEY", "test-fixture-openai")
        .env("FIXTURE_API_KEY", "test-fixture-provider")
        .kill_on_drop(true);
    #[cfg(windows)]
    child.creation_flags(0x08000000);
    let output = tokio::time::timeout(Duration::from_secs(30), child.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!directory.path().join("parent-cache").exists());
    assert!(!directory.path().join("project-cache").exists());
}

#[tokio::test]
#[ignore = "subprocess workspace build isolation fixture"]
async fn workspace_build_child() {
    let Ok(root) = std::env::var("ACT0R_WORKSPACE_BUILD_TEST_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let managed = root.join("managed");
    let factory = LocalWorkspaces::new(managed.clone());
    let tools = LocalTools::new(managed);
    let argv = vec![
        "cargo".into(),
        "test".into(),
        "--offline".into(),
        "--quiet".into(),
    ];
    for passed in [true, false] {
        let source = root.join(if passed { "correct" } else { "incorrect" });
        fs::create_dir_all(source.join("src")).unwrap();
        fs::create_dir(source.join(".cargo")).unwrap();
        fs::write(
            source.join("Cargo.toml"),
            "[package]\nname = \"workspace_build_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
        ).unwrap();
        fs::write(
            source.join("src/lib.rs"),
            format!("#[test]\nfn verifies_this_workspace() {{ assert!(std::env::var_os(\"OPENAI_API_KEY\").is_none()); assert!(std::env::var_os(\"FIXTURE_API_KEY\").is_none()); assert!({passed}); }}\n"),
        )
        .unwrap();
        // Cargo configuration must not redirect a managed workspace into a shared cache.
        fs::write(
            source.join(".cargo/config.toml"),
            format!(
                "[build]\ntarget-dir = \"{}\"\n",
                root.join("project-cache")
                    .to_string_lossy()
                    .replace('\\', "/")
            ),
        )
        .unwrap();
        let workspace = factory.prepare(source).await.unwrap();
        let result = if passed {
            tools
                .execute(
                    ToolRequest {
                        workspace: workspace.clone(),
                        action: AgentAction::RunTests,
                        test_command: argv.clone(),
                        writable_paths: vec![],
                    },
                    ActivityId::new(),
                )
                .await
                .unwrap()
        } else {
            WorkspaceTestRunner::run_tests(
                &tools,
                workspace.clone(),
                WorkspaceTestCommand {
                    argv: argv.clone(),
                    timeout_ms: 10_000,
                },
                ActivityId::new(),
            )
            .await
            .unwrap()
        };
        assert_eq!(result["ok"], passed, "{result}");
        assert_eq!(result["exit_code"], if passed { 0 } else { 101 });
        assert!(Path::new(&workspace).join("target/debug/deps").is_dir());
        // Generated artifacts stay hidden from model-visible workspace inspection.
        let listed = tools
            .execute(
                ToolRequest {
                    workspace,
                    action: AgentAction::ListFiles,
                    test_command: vec![],
                    writable_paths: vec![],
                },
                ActivityId::new(),
            )
            .await
            .unwrap();
        assert!(
            listed["files"]
                .as_array()
                .unwrap()
                .iter()
                .all(|path| !path.as_str().unwrap().starts_with("target/"))
        );
    }
}
