use act0r::{
    adapters::project_detection::LocalProjectDetection,
    application::{ProjectDetectionService, ports::ProjectDetectionPort},
    domain::project_detection::*,
};
use serde_json::{Value, json};
use std::{fs, path::Path, sync::Arc};

async fn detect(root: &Path) -> Value {
    serde_json::to_value(
        ProjectDetectionService::new(Arc::new(LocalProjectDetection))
            .detect(DetectionRequest {
                workspace: root.to_string_lossy().into_owned(),
            })
            .await
            .unwrap(),
    )
    .unwrap()
}
fn manifest(root: &Path, text: &str) {
    fs::write(root.join("Cargo.toml"), text).unwrap();
}

#[tokio::test]
async fn detection_only_reads_files_and_ignores_metadata_commands() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("src")).unwrap();
    let text = "[package]\nname='sample'\nversion='0.1.0'\nbuild='build.rs'\n[package.metadata.act0r]\ncommands={test=['powershell','malicious']}\n";
    manifest(root.path(), text);
    fs::write(
        root.path().join("build.rs"),
        "compile_error!(\"must never be executed by detection\");",
    )
    .unwrap();
    fs::write(
        root.path().join("Cargo.lock"),
        "a regular lockfile is detected without reading its contents",
    )
    .unwrap();
    let result = detect(root.path()).await;
    assert_eq!(result["kind"], "rust_package");
    assert_eq!(
        result["suggested"]["coding"]["writable_paths"],
        json!(["src/"])
    );
    assert_eq!(
        result["suggested"]["coding"]["test_command"]["argv"],
        json!(["cargo", "test", "--locked", "--offline"])
    );
    assert_eq!(
        result["suggested"]["coding"]["project_commands"]["format"]["argv"],
        json!(["cargo", "fmt"])
    );
    assert_eq!(
        result["suggested"]["coding"]["project_commands"]["lint"]["argv"],
        json!(["cargo", "clippy", "--locked", "--offline"])
    );
    assert_eq!(
        fs::read_to_string(root.path().join("Cargo.toml")).unwrap(),
        text
    );
    assert!(!root.path().join("target").exists());
    assert_eq!(result, detect(root.path()).await);
}

#[tokio::test]
async fn custom_source_paths_and_missing_lockfile_produce_explicit_reviewable_scopes() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("library")).unwrap();
    fs::write(root.path().join("library/lib.rs"), "").unwrap();
    fs::write(root.path().join("main.rs"), "").unwrap();
    manifest(
        root.path(),
        "[package]\nname='custom'\n[lib]\npath='./library/lib.rs'\n[[bin]]\nname='app'\npath='main.rs'\n[[bin]]\nname='again'\npath='main.rs'\n",
    );
    let result = detect(root.path()).await;
    assert_eq!(
        result["suggested"]["coding"]["writable_paths"],
        json!(["library/lib.rs", "main.rs", "Cargo.lock"])
    );
    assert_eq!(
        result["suggested"]["coding"]["test_command"]["argv"],
        json!(["cargo", "test", "--offline"])
    );
    assert!(!root.path().join("Cargo.lock").exists());
    assert!(
        result["notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n.as_str().unwrap().contains("Cargo.lock"))
    );
}

#[tokio::test]
async fn unsupported_layouts_leave_manual_settings_available() {
    let root = tempfile::tempdir().unwrap();
    assert_eq!(detect(root.path()).await["kind"], "unrecognized");
    for text in [
        "[workspace]\nmembers=['crates/*']\n",
        "[package]\nname='root'\n[workspace]\nmembers=['child']\n",
        "[package]\nname='member'\nworkspace='..'\n",
    ] {
        manifest(root.path(), text);
        let result = detect(root.path()).await;
        assert_eq!(result["kind"], "rust_workspace");
        assert!(result["suggested"].is_null());
    }
    manifest(root.path(), "[package]\nname='empty'\n");
    assert!(detect(root.path()).await["suggested"].is_null());
}

#[tokio::test]
async fn malformed_oversized_and_escaping_manifests_are_refused() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("src")).unwrap();
    for text in [
        "[package\n",
        "[package]\nname='bad name'\n",
        "[package]\nname='escape'\n[lib]\npath='../outside.rs'\n",
        "[package]\nname='private'\n[lib]\npath='.env'\n",
    ] {
        manifest(root.path(), text);
        assert!(
            ProjectDetectionService::new(Arc::new(LocalProjectDetection))
                .detect(DetectionRequest {
                    workspace: root.path().to_string_lossy().into()
                })
                .await
                .is_err()
        );
    }
    manifest(root.path(), &"#".repeat(MAX_MANIFEST_BYTES as usize + 1));
    assert!(
        LocalProjectDetection
            .inspect(root.path().to_string_lossy().into())
            .await
            .is_err()
    );
    manifest(root.path(), "[package]\nname='wrong_lock'\n");
    fs::create_dir(root.path().join("Cargo.lock")).unwrap();
    assert!(
        LocalProjectDetection
            .inspect(root.path().to_string_lossy().into())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn linked_source_directories_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    manifest(root.path(), "[package]\nname='linked'\n");
    #[cfg(unix)]
    std::os::unix::fs::symlink(outside.path(), root.path().join("src")).unwrap();
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let status = std::process::Command::new("cmd")
            .args(["/d", "/c", "mklink", "/J"])
            .arg(root.path().join("src"))
            .arg(outside.path())
            .creation_flags(0x08000000)
            .output()
            .unwrap();
        assert!(status.status.success());
    }
    assert!(
        LocalProjectDetection
            .inspect(root.path().to_string_lossy().into())
            .await
            .is_err()
    );
}
