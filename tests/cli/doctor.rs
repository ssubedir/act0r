//! Spawn the real CLI with an isolated environment; diagnostics must not execute work.
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn command(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_act0r"));
    command.current_dir(directory).arg("--json").arg("doctor");
    for name in [
        "ACT0R_MODEL",
        "ACT0R_MODEL_ENDPOINT",
        "OPENAI_API_KEY",
        "ACT0R_MCP_CONFIG",
        "ACT0R_MCP_FILESYSTEM_CONFIG",
    ] {
        command.env_remove(name);
    }
    command
}
fn report(output: &Output, ready: bool) -> Value {
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        panic!(
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert_eq!(value["ready"], ready, "{value}");
    assert_eq!(output.status.success(), ready, "{value}");
    assert_eq!(value["scope"], "local_configuration");
    value
}
fn check<'a>(value: &'a Value, name: &str) -> &'a Value {
    value["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("missing {name}: {value}"))
}
fn local(command: &mut Command) -> &mut Command {
    command.args([
        "--model",
        "local-test",
        "--endpoint",
        "http://127.0.0.1:1/v1/chat/completions",
    ])
}
fn mcp(program: &Path) -> Value {
    json!({"replace_inspection":true,"servers":[{
        "name":"files","command":{"program":program,"args":["--ignored", "--exact", "doctor::doctor_mcp_start_fixture"]},
        "tools":[{"name":"read_text_file","read_only":true,"path_parameters":["path"]}]
    }]})
}

#[test]
fn doctor_reports_missing_hosted_key_and_does_not_create_the_default_database() {
    let directory = tempfile::tempdir().unwrap();
    let output = command(directory.path())
        .env("OPENAI_API_KEY", " \t ")
        .output()
        .unwrap();
    let value = report(&output, false);
    assert_eq!(value["model"]["name"], "moonshotai/kimi-k3");
    assert_eq!(value["model"]["action_format"], "function_calling");
    assert_eq!(check(&value, "credentials")["status"], "error");
    assert!(
        check(&value, "credentials")["fix"]
            .as_str()
            .unwrap()
            .contains("OPENAI_API_KEY")
    );
    assert_eq!(check(&value, "database")["path"], ".act0r/chat.db");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn doctor_checks_local_configuration_without_http_mcp_or_persistent_files() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let config = root.join("host.json");
    let mut config_value = mcp(&std::env::current_exe().unwrap());
    let marker = root.join("server-started");
    config_value["servers"][0]["env"] = json!({"ACT0R_DOCTOR_TEST_MARKER":marker});
    fs::write(&config, serde_json::to_vec(&config_value).unwrap()).unwrap();
    fs::create_dir(root.join("project")).unwrap();
    fs::write(
        root.join("project/README.md"),
        "leave the project unchanged",
    )
    .unwrap();
    let output = command(root)
        .args([
            "--db",
            "missing/nested/chat.db",
            "--project",
            "project",
            "--model",
            "local",
            "--endpoint",
            &endpoint,
        ])
        .env("ACT0R_MCP_CONFIG", &config)
        .output()
        .unwrap();
    let value = report(&output, true);
    assert_eq!(check(&value, "credentials")["status"], "ok");
    assert_eq!(
        check(&value, "mcp_config")["path"],
        config.to_str().unwrap()
    );
    assert_eq!(check(&value, "mcp_server:files")["status"], "ok");
    assert_eq!(check(&value, "project")["status"], "ok");
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert!(!marker.exists());
    assert!(!root.join("missing").exists());
    assert!(!root.join(".act0r").exists());
    assert_eq!(
        fs::read_to_string(root.join("project/README.md")).unwrap(),
        "leave the project unchanged"
    );
    assert_eq!(fs::read_dir(root).unwrap().count(), 2);
    assert_eq!(fs::read_dir(root.join("project")).unwrap().count(), 1);
}

#[test]
#[ignore = "MCP startup trap: doctor must never launch this process"]
fn doctor_mcp_start_fixture() {
    fs::write(
        std::env::var_os("ACT0R_DOCTOR_TEST_MARKER").unwrap(),
        "started",
    )
    .unwrap();
}

#[test]
fn doctor_checks_http_token_configuration_without_contacting_the_mcp_endpoint() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let config = root.join("mcp.json");
    let mut value = json!({"servers":[{"name":"records","http":{
        "url":format!("http://{}/mcp",listener.local_addr().unwrap()),
        "bearer_token_env":"ACT0R_DOCTOR_HTTP_TOKEN"},
        "tools":[{"name":"lookup","read_only":true,"requires_workspace":false}]}]});
    fs::write(&config, serde_json::to_vec(&value).unwrap()).unwrap();
    for token in [None, Some(" "), Some("doctor-http-secret")] {
        let mut command = command(root);
        local(&mut command)
            .env("ACT0R_MCP_CONFIG", &config)
            .env_remove("ACT0R_DOCTOR_HTTP_TOKEN");
        if let Some(token) = token {
            command.env("ACT0R_DOCTOR_HTTP_TOKEN", token);
        }
        let output = command.output().unwrap();
        let ready = token == Some("doctor-http-secret");
        let result = report(&output, ready);
        assert_eq!(
            check(&result, "mcp_server:records")["status"],
            if ready { "ok" } else { "error" }
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("doctor-http-secret"));
    }
    value["servers"][0]["http"]["url"] = json!("https://service.invalid/mcp?token=embedded-secret");
    fs::write(&config, serde_json::to_vec(&value).unwrap()).unwrap();
    let output = local(&mut command(root))
        .env("ACT0R_MCP_CONFIG", &config)
        .output()
        .unwrap();
    assert_eq!(
        check(&report(&output, false), "mcp_config")["status"],
        "error"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("embedded-secret"));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(fs::read_dir(root).unwrap().count(), 1);
}

#[test]
fn doctor_uses_chat_model_environment_and_explicit_overrides_without_exposing_keys() {
    let directory = tempfile::tempdir().unwrap();
    let output = command(directory.path())
        .env("ACT0R_MODEL", "environment-model")
        .env(
            "ACT0R_MODEL_ENDPOINT",
            "https://hosted.invalid/v1/chat/completions",
        )
        .env("OPENAI_API_KEY", "secret-doctor-sentinel")
        .output()
        .unwrap();
    let value = report(&output, true);
    assert_eq!(value["model"]["name"], "environment-model");
    assert_eq!(
        value["model"]["endpoint"],
        "https://hosted.invalid/v1/chat/completions"
    );
    assert_eq!(value["model"]["api_key_present"], true);
    assert_eq!(value["model"]["action_format"], "json_schema");
    assert_eq!(value["model"]["request_timeout_ms"], 90000);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("secret-doctor-sentinel"));

    let output = local(&mut command(directory.path()))
        .arg("--action-format")
        .arg("function-calling")
        .env("ACT0R_MODEL", "ignored")
        .env("ACT0R_MODEL_ENDPOINT", "invalid")
        .output()
        .unwrap();
    let value = report(&output, true);
    assert_eq!(value["model"]["name"], "local-test");
    assert_eq!(value["model"]["action_format"], "function_calling");
    assert_eq!(value["model"]["api_key_present"], false);

    let output = command(directory.path())
        .args(["--model", " \t "])
        .env("OPENAI_API_KEY", "test")
        .output()
        .unwrap();
    let value = report(&output, false);
    assert_eq!(check(&value, "model")["status"], "error");
    assert!(value["model"]["name"].is_null());
}

#[test]
fn doctor_reports_valid_deadlines_and_rejects_out_of_range_values_before_startup() {
    let directory = tempfile::tempdir().unwrap();
    for timeout in ["1", "300000", "600000"] {
        let output = local(&mut command(directory.path()))
            .args(["--model-timeout-ms", timeout])
            .output()
            .unwrap();
        let value = report(&output, true);
        assert_eq!(
            value["model"]["request_timeout_ms"],
            timeout.parse::<u32>().unwrap()
        );
    }
    for timeout in ["0", "600001", "-1", "forever"] {
        let output = command(directory.path())
            .args(["--model-timeout-ms", timeout])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
    assert!(directory.path().read_dir().unwrap().next().is_none());
}

#[test]
fn doctor_rejects_unsafe_or_invalid_endpoints_without_echoing_credentials() {
    let directory = tempfile::tempdir().unwrap();
    for endpoint in [
        "invalid-secret-doctor-sentinel",
        "http://remote.invalid/v1/chat/completions",
        "https://user:private-password@host.invalid/v1/chat/completions",
        "https://host.invalid/v1/chat/completions?key=private-password",
        "https://host.invalid/v1/chat/completions#private-password",
        "file:///private-password",
    ] {
        let output = command(directory.path())
            .arg("--endpoint")
            .arg(endpoint)
            .env("OPENAI_API_KEY", "secret-doctor-sentinel")
            .output()
            .unwrap();
        let value = report(&output, false);
        assert_eq!(check(&value, "endpoint")["status"], "error");
        assert!(value["model"]["endpoint"].is_null());
        for bytes in [&output.stdout, &output.stderr] {
            let text = String::from_utf8_lossy(bytes);
            assert!(!text.contains("private-password"), "{text}");
            assert!(!text.contains("secret-doctor-sentinel"), "{text}");
        }
    }
    for host in ["localhost", "127.0.0.1", "[::1]"] {
        let endpoint = format!("http://{host}:1/v1/chat/completions");
        report(
            &command(directory.path())
                .args(["--endpoint", &endpoint])
                .output()
                .unwrap(),
            true,
        );
    }
}

#[test]
fn doctor_checks_mcp_override_schema_executables_and_bridge_even_when_other_checks_fail() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::create_dir(root.join(".act0r")).unwrap();
    let adjacent = root.join(".act0r/mcp.json");
    fs::write(&adjacent, "{not json").unwrap();
    let output = local(&mut command(root)).output().unwrap();
    assert_eq!(
        check(&report(&output, false), "mcp_config")["status"],
        "error"
    );

    let selected = root.join("selected.json");
    fs::write(
        &selected,
        serde_json::to_vec(&mcp(&std::env::current_exe().unwrap())).unwrap(),
    )
    .unwrap();
    let output = local(&mut command(root))
        .env("ACT0R_MCP_CONFIG", &selected)
        .output()
        .unwrap();
    assert_eq!(
        check(&report(&output, true), "mcp_config")["path"],
        selected.to_str().unwrap()
    );

    let invalid = [
        json!({"secret-unrecognized-field":"private-config-value"}),
        json!({"replace_inspection":true,"servers":[]}),
        {
            let mut config = mcp(&root.join("missing-program"));
            config["servers"][0]["env"] = json!({"TOKEN":"private-config-value"});
            config
        },
        {
            let mut config = mcp(&std::env::current_exe().unwrap());
            config["servers"][0]["tools"][0]["read_only"] = json!(false);
            config
        },
    ];
    for config in invalid {
        fs::write(&selected, serde_json::to_vec(&config).unwrap()).unwrap();
        let output = command(root)
            .env("ACT0R_MCP_CONFIG", &selected)
            .env(
                "ACT0R_MCP_FILESYSTEM_CONFIG",
                root.join("missing-bridge.json"),
            )
            .output()
            .unwrap();
        let value = report(&output, false);
        for name in ["credentials", "mcp_config", "mcp_file_reader"] {
            assert_eq!(check(&value, name)["status"], "error");
        }
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(!text.contains("private-config-value"));
        assert!(!text.contains("secret-unrecognized-field"));
    }
    fs::write(&selected, " ".repeat(65537)).unwrap();
    let output = local(&mut command(root))
        .env("ACT0R_MCP_CONFIG", &selected)
        .output()
        .unwrap();
    assert_eq!(
        check(&report(&output, false), "mcp_config")["status"],
        "error"
    );
    fs::remove_file(&adjacent).unwrap();
    fs::write(
        &selected,
        serde_json::to_vec(&json!({"program":std::env::current_exe().unwrap(),"args":[]})).unwrap(),
    )
    .unwrap();
    let output = local(&mut command(root))
        .env("ACT0R_MCP_FILESYSTEM_CONFIG", &selected)
        .output()
        .unwrap();
    assert_eq!(
        check(&report(&output, true), "mcp_file_reader")["status"],
        "ok"
    );
    assert!(!root.join(".act0r/chat.db").exists());
    assert_eq!(fs::read_dir(root.join(".act0r")).unwrap().count(), 0);
}

#[test]
fn doctor_checks_database_placement_project_access_and_workspace_obstructions() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::create_dir(root.join("project")).unwrap();
    let output = local(&mut command(root))
        .args(["--project", "project", "--db", "project/state.db"])
        .output()
        .unwrap();
    assert_eq!(check(&report(&output, false), "project")["status"], "error");
    assert!(!root.join("project/state.db").exists());
    let output = local(&mut command(root))
        .args(["--project", "project", "--db", "project/.act0r/state.db"])
        .output()
        .unwrap();
    report(&output, true);
    assert_eq!(fs::read_dir(root.join("project")).unwrap().count(), 0);

    fs::write(root.join("file"), "obstruction").unwrap();
    let output = local(&mut command(root))
        .args(["--project", "file", "--db", "file/state.db"])
        .output()
        .unwrap();
    let value = report(&output, false);
    assert_eq!(check(&value, "project")["status"], "error");
    assert_eq!(check(&value, "database")["status"], "error");
    let output = local(&mut command(root))
        .args(["--db", "project"])
        .output()
        .unwrap();
    assert_eq!(
        check(&report(&output, false), "database")["status"],
        "error"
    );
    let output = local(&mut command(root))
        .args(["--project", "absent"])
        .output()
        .unwrap();
    assert_eq!(check(&report(&output, false), "project")["status"], "error");

    fs::create_dir_all(root.join(".act0r/.act0r")).unwrap();
    fs::write(root.join(".act0r/.act0r/workspaces"), "obstruction").unwrap();
    let output = local(&mut command(root)).output().unwrap();
    assert_eq!(
        check(&report(&output, false), "workspaces")["status"],
        "error"
    );
    assert_eq!(
        fs::read_to_string(root.join(".act0r/.act0r/workspaces")).unwrap(),
        "obstruction"
    );
}

#[test]
fn doctor_never_opens_sqlite_or_touches_worker_locks_and_preserves_existing_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::create_dir(root.join(".act0r")).unwrap();
    let db = root.join(".act0r/chat.db");
    fs::write(
        &db,
        "not SQLite; diagnostics must not initialize or migrate this",
    )
    .unwrap();
    let lock = root.join(".act0r/chat.db.worker.lock");
    fs::write(&lock, "retain lock bytes").unwrap();
    let held = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock)
        .unwrap();
    held.lock().unwrap();
    let output = local(&mut command(root)).output().unwrap();
    report(&output, true);
    drop(held);
    assert_eq!(
        fs::read_to_string(&db).unwrap(),
        "not SQLite; diagnostics must not initialize or migrate this"
    );
    assert_eq!(fs::read_to_string(&lock).unwrap(), "retain lock bytes");
    assert_eq!(fs::read_dir(root.join(".act0r")).unwrap().count(), 2);
    let original = fs::metadata(&db).unwrap().permissions();
    let mut readonly = original.clone();
    readonly.set_readonly(true);
    fs::set_permissions(&db, readonly).unwrap();
    let output = local(&mut command(root)).output().unwrap();
    fs::set_permissions(&db, original).unwrap();
    assert_eq!(
        check(&report(&output, false), "database")["status"],
        "error"
    );
}

#[test]
fn doctor_human_output_has_fixes_and_redacts_values_in_paths_and_model_names() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .current_dir(directory.path())
        .args([
            "doctor",
            "--model",
            "prefix-secret-marker",
            "--db",
            "secret-marker/state.db",
        ])
        .env("OPENAI_API_KEY", "secret-marker")
        .env_remove("ACT0R_MODEL_ENDPOINT")
        .env_remove("ACT0R_MCP_CONFIG")
        .env_remove("ACT0R_MCP_FILESYSTEM_CONFIG")
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("prefix-[REDACTED]"));
    assert!(!text.contains("secret-marker"));
    assert!(text.contains("Local setup checks passed"));
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);

    let output = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .current_dir(directory.path())
        .arg("doctor")
        .env_remove("OPENAI_API_KEY")
        .env_remove("ACT0R_MODEL")
        .env_remove("ACT0R_MODEL_ENDPOINT")
        .env_remove("ACT0R_MCP_CONFIG")
        .env_remove("ACT0R_MCP_FILESYSTEM_CONFIG")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("[error] credentials"));
    assert!(text.contains("Fix: Set OPENAI_API_KEY"));
}

#[cfg(unix)]
#[test]
fn doctor_refuses_nonexecutable_servers_and_dangling_workspace_links() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let program = root.join("not-executable");
    fs::write(&program, "unused").unwrap();
    fs::set_permissions(&program, fs::Permissions::from_mode(0o644)).unwrap();
    let config = root.join("mcp.json");
    fs::write(&config, serde_json::to_vec(&mcp(&program)).unwrap()).unwrap();
    fs::create_dir_all(root.join(".act0r/.act0r")).unwrap();
    symlink("missing", root.join(".act0r/.act0r/workspaces")).unwrap();
    let output = local(&mut command(root))
        .env("ACT0R_MCP_CONFIG", &config)
        .output()
        .unwrap();
    let value = report(&output, false);
    assert_eq!(check(&value, "mcp_server:files")["status"], "error");
    assert_eq!(check(&value, "workspaces")["status"], "error");
}
