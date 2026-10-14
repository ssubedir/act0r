use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Command, Output},
};

fn cli(database: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(database)
        .arg("--json")
        .args(arguments)
        .output()
        .unwrap()
}
fn success(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn cli_discovers_native_tools_and_executes_a_pinned_tool_actor() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("native-tools.db");
    let tools = success(cli(&db, &["actor", "tool", "list"]));
    let catalog = tools.as_array().unwrap();
    let identities: std::collections::BTreeSet<_> =
        catalog.iter().map(|tool| tool["id"].to_string()).collect();
    assert_eq!(identities.len(), catalog.len());
    let tool = |name: &str, version: u32| {
        tools
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["id"] == json!({"name":name,"version":version}))
            .unwrap()
    };
    tool("echo", 1);
    tool("sum", 1);
    tool("read_file", 2);
    assert_eq!(tool("list_files", 2)["requires_workspace"], true);
    assert_eq!(tool("list_files", 2)["reports_errors"], true);
    assert_eq!(tool("replace_text", 1)["requires_writable_paths"], true);
    assert_eq!(tool("create_file", 1)["requires_writable_paths"], true);
    assert_eq!(tool("create_file", 1)["reports_errors"], true);
    assert_eq!(tool("run_command", 1)["requires_project_command"], true);
    assert_eq!(tool("run_command", 1)["requires_approval"], true);
    assert_eq!(tool("search_text", 1)["requires_workspace"], true);
    assert_eq!(tool("search_text", 1)["reports_errors"], true);
    assert_eq!(tool("search_text", 1)["requires_approval"], false);
    assert_eq!(tool("search_text", 1)["recovery"], "SafeToRepeat");
    for name in ["replace_text", "create_file", "run_tests"] {
        assert_eq!(tool(name, 1)["requires_approval"], true);
        assert_eq!(tool(name, 1)["recovery"], "RequiresReconciliation");
    }
    assert_eq!(tool("run_tests", 1)["requires_test_command"], true);
    for name in ["list_results", "read_result"] {
        assert_eq!(tool(name, 1)["reports_errors"], true);
        assert_eq!(tool(name, 1)["requires_approval"], false);
        assert_eq!(tool(name, 1)["recovery"], "SafeToRepeat");
        assert!(tool(name, 1).get("requires_workspace").is_none());
    }
    let config =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/configs/tool-agent-scripted.json");
    let actor = success(cli(
        &db,
        &[
            "actor",
            "agent",
            "create",
            "tool",
            "--config-file",
            config.to_str().unwrap(),
        ],
    ))["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let state = success(cli(&db, &["actor", "status", &actor]));
    assert_eq!(
        state["agent"]["config"]["input"]["tool_definitions"][0]["id"]["name"],
        "sum"
    );
    assert_eq!(
        state["agent"]["config"]["input"]["context"],
        json!({"version":1,"max_input_bytes":65536,"max_observations":16})
    );
    let input = dir.path().join("task.json");
    std::fs::write(&input, br#"{"goal":"Add 19 and 23"}"#).unwrap();
    let task = success(cli(
        &db,
        &[
            "actor",
            "send",
            &actor,
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    let id = task["message_id"].as_str().unwrap();
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let completed = success(cli(&db, &["actor", "task", "status", id]));
    assert_eq!(completed["status"], "succeeded");
    assert_eq!(completed["admitted_attempts"], 3);
    assert_eq!(completed["usage"]["admitted_model_calls"], 2);
    let history = success(cli(
        &db,
        &["run", "history", completed["run_id"].as_str().unwrap()],
    ));
    assert!(history.to_string().contains("native.tool"));
}

#[test]
fn cli_native_context_reports_eviction_and_preserves_full_tool_history() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("native-context.db");
    let file = dir.path().join("config.json");
    let task_file = dir.path().join("task.json");
    let mut script: Vec<Value> = (0..20).map(|i| json!({"kind":"call_tool","tool":"echo","arguments":{"text":format!("complete evidence {i} 🙂")}})).collect();
    script.push(json!({"kind":"finish","summary":"Done"}));
    std::fs::write(
        &file,
        serde_json::to_vec(&json!({
            "goal":"Read evidence","target":{"provider":"scripted","model":"context","endpoint":""},
            "max_model_calls":21,"max_tokens":1000000,"max_output_tokens":512,
            "context":{"max_input_bytes":6000,"max_observations":2},
            "tools":[{"name":"echo","version":1}],"script":script
        }))
        .unwrap(),
    )
    .unwrap();
    let created = success(cli(
        &db,
        &[
            "actor",
            "agent",
            "create",
            "tool",
            "--config-file",
            file.to_str().unwrap(),
            "--attempt-limit",
            "64",
        ],
    ));
    std::fs::write(
        &task_file,
        br#"{"goal":"Inspect","context":{"max_observations":1}}"#,
    )
    .unwrap();
    let task = success(cli(
        &db,
        &[
            "actor",
            "send",
            created["agent_id"].as_str().unwrap(),
            "--input-file",
            task_file.to_str().unwrap(),
        ],
    ));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let view = success(cli(
        &db,
        &[
            "actor",
            "task",
            "status",
            task["message_id"].as_str().unwrap(),
        ],
    ));
    assert_eq!(view["status"], "succeeded");
    let run_id = view["run_id"].as_str().unwrap();
    let run = success(cli(&db, &["run", "status", run_id]));
    let context = &run["record"]["state"]["state"]["context"];
    assert_eq!(context["selection"]["total_observations"], 20);
    assert_eq!(context["selection"]["included_observations"], 2);
    assert_eq!(context["selection"]["omitted_observations"], 18);
    assert_eq!(context["selection"]["policy"]["max_input_bytes"], 6000);
    assert_eq!(context["observations"].as_array().unwrap().len(), 2);
    let tools: Vec<_> = run["activities"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["spec"]["handler"]["name"] == "native.tool")
        .collect();
    assert_eq!(tools.len(), 20);
    assert_eq!(tools[0]["result"]["text"], "complete evidence 0 🙂");
    let text = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&db)
        .args(["run", "status", run_id])
        .output()
        .unwrap();
    assert!(text.status.success());
    let text = String::from_utf8_lossy(&text.stdout);
    assert!(text.contains("2 of 20 observations included, 18 omitted"));
    let history = success(cli(&db, &["run", "history", run_id]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    assert_eq!(history, success(cli(&db, &["run", "history", run_id])));
}

#[test]
fn cli_native_actor_lists_saved_results_and_restarts_without_replaying_tools() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("recall.db");
    let cfg = dir.path().join("config.json");
    let input = dir.path().join("input.json");
    std::fs::write(&cfg, serde_json::to_vec(&json!({
        "goal":"Find saved evidence","target":{"provider":"scripted","model":"recall","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
        "context":{"max_input_bytes":12000,"max_observations":1},
        "tools":[{"name":"echo","version":1},{"name":"list_results","version":1}],
        "script":[{"kind":"call_tool","tool":"echo","arguments":{"text":"original saved evidence 🙂"}},
            {"kind":"call_tool","tool":"list_results","arguments":{"tool":"echo","limit":1}},
            {"kind":"finish","summary":"Found original evidence."}]
    })).unwrap()).unwrap();
    let actor = success(cli(
        &db,
        &[
            "actor",
            "agent",
            "create",
            "tool",
            "--config-file",
            cfg.to_str().unwrap(),
        ],
    ));
    std::fs::write(
        &input,
        br#"{"goal":"Find evidence","run_id":"forged","tools":[]}"#,
    )
    .unwrap();
    let task = success(cli(
        &db,
        &[
            "actor",
            "send",
            actor["agent_id"].as_str().unwrap(),
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let view = success(cli(
        &db,
        &[
            "actor",
            "task",
            "status",
            task["message_id"].as_str().unwrap(),
        ],
    ));
    assert_eq!(view["status"], "succeeded");
    let run_id = view["run_id"].as_str().unwrap();
    let run = success(cli(&db, &["run", "status", run_id]));
    let tools: Vec<_> = run["activities"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["spec"]["handler"]["name"] == "native.tool")
        .collect();
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[1]["spec"]["handler"]["version"], 3);
    assert_eq!(
        tools[1]["result"]["results"][0]["activity_id"],
        tools[0]["id"]
    );
    assert_eq!(tools[1]["result"]["results"].as_array().unwrap().len(), 1);
    let prompt = run["activities"].as_array().unwrap().last().unwrap()["spec"]["input"]["prompt"]
        .as_str()
        .unwrap();
    assert!(!prompt.contains("original saved evidence"));
    let history = success(cli(&db, &["run", "history", run_id]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    assert_eq!(success(cli(&db, &["run", "history", run_id])), history);
    assert_eq!(success(cli(&db, &["run", "status", run_id])), run);
}

#[test]
fn cli_workspace_tools_pin_a_copy_and_inspect_relative_files() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("workspace.db");
    let source = dir.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("README.md"), "Copied project evidence.\n").unwrap();
    std::fs::write(source.join(".env"), "private").unwrap();
    let config = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/configs/workspace-agent-scripted.json");
    let args = [
        "actor",
        "agent",
        "create",
        "tool",
        "--source",
        source.to_str().unwrap(),
        "--config-file",
        config.to_str().unwrap(),
    ];
    let created = success(cli(&db, &args));
    let actor = created["agent_id"].as_str().unwrap();
    let state = success(cli(&db, &["actor", "status", actor]));
    let root = state["agent"]["config"]["input"]["workspace"]
        .as_str()
        .unwrap();
    assert_ne!(Path::new(root), source);
    assert!(Path::new(root).join("README.md").exists());
    assert!(!Path::new(root).join(".env").exists());
    std::fs::write(source.join("README.md"), "Changed original").unwrap();
    let input = dir.path().join("task.json");
    std::fs::write(
        &input,
        br#"{"goal":"Inspect this project","workspace":"forged-root"}"#,
    )
    .unwrap();
    let submitted = success(cli(
        &db,
        &[
            "actor",
            "send",
            actor,
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    let task = submitted["message_id"].as_str().unwrap();
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let completed = success(cli(&db, &["actor", "task", "status", task]));
    assert_eq!(completed["status"], "succeeded");
    let run = success(cli(
        &db,
        &["run", "status", completed["run_id"].as_str().unwrap()],
    ));
    let tools: Vec<_> = run["activities"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["spec"]["handler"]["name"] == "native.tool")
        .collect();
    assert_eq!(tools.len(), 2);
    for tool in &tools {
        assert_eq!(tool["spec"]["handler"]["version"], 3);
        assert_eq!(tool["spec"]["input"]["workspace"], root);
    }
    assert_eq!(tools[0]["result"]["files"], json!(["README.md"]));
    assert_eq!(tools[1]["result"]["text"], "Copied project evidence.\n");
    assert_eq!(tools[1]["result"]["truncated"], false);
}

#[test]
fn cli_expected_tool_error_is_saved_feedback_for_a_corrected_read() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("feedback-cli.db");
    let source = dir.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("README.md"), "Saved project evidence.\n").unwrap();
    let mut cfg: Value = serde_json::from_str(include_str!(
        "../examples/configs/workspace-agent-scripted.json"
    ))
    .unwrap();
    cfg["script"] = json!([{"kind":"call_tool","tool":"read_file","arguments":{"path":"missing.md"}},
        {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
        {"kind":"finish","summary":"Corrected the read."}]);
    let file = dir.path().join("config.json");
    std::fs::write(&file, serde_json::to_vec(&cfg).unwrap()).unwrap();
    let actor = success(cli(
        &db,
        &[
            "actor",
            "agent",
            "create",
            "tool",
            "--source",
            source.to_str().unwrap(),
            "--config-file",
            file.to_str().unwrap(),
        ],
    ));
    let input = dir.path().join("task.json");
    std::fs::write(&input, br#"{"goal":"Inspect the README"}"#).unwrap();
    let submitted = success(cli(
        &db,
        &[
            "actor",
            "send",
            actor["agent_id"].as_str().unwrap(),
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    let task = submitted["message_id"].as_str().unwrap();
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let completed = success(cli(&db, &["actor", "task", "status", task]));
    assert_eq!(completed["status"], "succeeded");
    assert_eq!(completed["admitted_attempts"], 5);
    assert_eq!(completed["usage"]["admitted_model_calls"], 3);
    let run_id = completed["run_id"].as_str().unwrap();
    let run = success(cli(&db, &["run", "status", run_id]));
    assert_eq!(
        run["activities"][1]["result"]["error"]["code"],
        "file_not_found"
    );
    assert_eq!(run["activities"][1]["status"], "succeeded");
    assert_eq!(
        run["activities"][3]["result"]["text"],
        "Saved project evidence.\n"
    );
    let history = success(cli(&db, &["run", "history", run_id]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    assert_eq!(history, success(cli(&db, &["run", "history", run_id])));
}

#[test]
fn cli_native_edits_wait_for_saved_approval_and_return_the_changed_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("native-edit.db");
    let source = dir.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("README.md"), "Original project evidence.\n").unwrap();
    let config = dir.path().join("edit.json");
    std::fs::write(&config,serde_json::to_vec(&json!({
        "goal":"Edit the README","target":{"provider":"scripted","model":"edit","endpoint":""},
        "max_model_calls":3,"max_tokens":300000,"max_output_tokens":512,
        "tools":[{"name":"replace_text","version":1},{"name":"read_file","version":2}],"writable_paths":["README.md"],
        "script":[{"kind":"call_tool","tool":"replace_text","arguments":{"path":"README.md","old":"Original","new":"Updated"}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}, {"kind":"finish","summary":"Updated the README"}]
    })).unwrap()).unwrap();
    let actor = success(cli(
        &db,
        &[
            "actor",
            "agent",
            "create",
            "tool",
            "--source",
            source.to_str().unwrap(),
            "--config-file",
            config.to_str().unwrap(),
        ],
    ));
    let actor_id = actor["agent_id"].as_str().unwrap();
    let input = dir.path().join("goal.json");
    std::fs::write(
        &input,
        br#"{"goal":"Edit the README","writable_paths":["other/"]}"#,
    )
    .unwrap();
    let submitted = success(cli(
        &db,
        &[
            "actor",
            "send",
            actor_id,
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    let task = submitted["message_id"].as_str().unwrap();
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let waiting = success(cli(&db, &["actor", "task", "status", task]));
    assert_eq!(waiting["status"], "waiting");
    assert_eq!(
        waiting["wait"]["payload"]["writable_paths"],
        json!(["README.md"])
    );
    let run = waiting["run_id"].as_str().unwrap();
    let wait = waiting["wait"]["id"].as_str().unwrap();
    success(cli(&db, &["run", "approve", run, wait]));
    success(cli(&db, &["run", "approve", run, wait]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let done = success(cli(&db, &["actor", "task", "status", task]));
    assert_eq!(done["status"], "succeeded");
    assert_eq!(done["usage"]["admitted_model_calls"], 3);
    assert_eq!(done["admitted_attempts"], 5);
    let state = success(cli(&db, &["run", "status", run]));
    assert_eq!(state["activities"][1]["spec"]["handler"]["version"], 4);
    assert_eq!(state["activities"][1]["result"]["already_applied"], false);
    assert_eq!(
        state["activities"][3]["result"]["text"],
        "Updated project evidence.\n"
    );
    assert_eq!(
        std::fs::read_to_string(source.join("README.md")).unwrap(),
        "Original project evidence.\n"
    );
}

#[test]
fn cli_native_creation_pins_permissions_and_keeps_saved_file_results_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("create.db");
    let source = dir.path().join("source");
    std::fs::create_dir_all(source.join("src")).unwrap();
    let file = dir.path().join("create.json");
    let task_file = dir.path().join("goal.json");
    std::fs::write(&file,serde_json::to_vec(&json!({
        "goal":"Create a module","target":{"provider":"scripted","model":"create","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
        "tools":[{"name":"create_file","version":1},{"name":"read_file","version":2}],"writable_paths":["src/new.rs"],
        "script":[{"kind":"call_tool","tool":"create_file","arguments":{"path":"src/new.rs","content":"pub fn value() -> u32 { 42 }\n"}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"src/new.rs"}}, {"kind":"finish","summary":"Created the module."}]
    })).unwrap()).unwrap();
    let actor = success(cli(
        &db,
        &[
            "actor",
            "agent",
            "create",
            "tool",
            "--source",
            source.to_str().unwrap(),
            "--config-file",
            file.to_str().unwrap(),
        ],
    ));
    std::fs::write(
        &task_file,
        br#"{"goal":"Create","writable_paths":["other/"],"workspace":"forged"}"#,
    )
    .unwrap();
    let task = success(cli(
        &db,
        &[
            "actor",
            "send",
            actor["agent_id"].as_str().unwrap(),
            "--input-file",
            task_file.to_str().unwrap(),
        ],
    ));
    let task_id = task["message_id"].as_str().unwrap();
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let waiting = success(cli(&db, &["actor", "task", "status", task_id]));
    assert_eq!(
        waiting["wait"]["payload"]["writable_paths"],
        json!(["src/new.rs"])
    );
    assert_eq!(waiting["wait"]["payload"]["tool"]["name"], "create_file");
    let workspace =
        std::path::PathBuf::from(waiting["wait"]["payload"]["workspace"].as_str().unwrap());
    assert!(!workspace.join("src/new.rs").exists());
    let run_id = waiting["run_id"].as_str().unwrap();
    let wait_id = waiting["wait"]["id"].as_str().unwrap();
    success(cli(&db, &["run", "approve", run_id, wait_id]));
    success(cli(&db, &["run", "approve", run_id, wait_id]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let view = success(cli(&db, &["actor", "task", "status", task_id]));
    assert_eq!(view["status"], "succeeded");
    let saved = success(cli(&db, &["run", "status", run_id]));
    assert_eq!(saved["activities"][1]["spec"]["handler"]["version"], 4);
    assert_eq!(saved["activities"][1]["result"]["already_applied"], false);
    assert_eq!(
        saved["activities"][3]["result"]["text"],
        "pub fn value() -> u32 { 42 }\n"
    );
    std::fs::write(workspace.join("src/new.rs"), "changed after completion").unwrap();
    let history = success(cli(&db, &["run", "history", run_id]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    assert_eq!(success(cli(&db, &["run", "history", run_id])), history);
    assert_eq!(success(cli(&db, &["run", "status", run_id])), saved);
    assert_eq!(
        std::fs::read_to_string(workspace.join("src/new.rs")).unwrap(),
        "changed after completion"
    );
    assert!(!source.join("src/new.rs").exists());
}

#[test]
fn cli_native_search_saves_scoped_matches_across_a_durable_human_wait() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("search.db");
    let source = dir.path().join("source");
    std::fs::create_dir_all(source.join("src")).unwrap();
    std::fs::write(source.join("src/lib.rs"), "pub fn answer() -> i64 { 42 }\n").unwrap();
    let config = dir.path().join("search.json");
    std::fs::write(&config, serde_json::to_vec(&json!({
        "goal":"Find a function","target":{"provider":"scripted","model":"search","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "tools":[{"name":"search_text","version":1},{"name":"read_file","version":2}],
        "script":[{"kind":"call_tool","tool":"search_text","arguments":{"query":"fn answer","path":"src","max_matches":null,"max_files":null}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}},
            {"kind":"ask_human","question":"Continue with saved function evidence?"},
            {"kind":"finish","summary":"Used the saved search and read."}]
    })).unwrap()).unwrap();
    let actor = success(cli(
        &db,
        &[
            "actor",
            "agent",
            "create",
            "tool",
            "--source",
            source.to_str().unwrap(),
            "--config-file",
            config.to_str().unwrap(),
        ],
    ));
    let goal = dir.path().join("goal.json");
    std::fs::write(&goal, br#"{"goal":"Find","workspace":"forged"}"#).unwrap();
    let task = success(cli(
        &db,
        &[
            "actor",
            "send",
            actor["agent_id"].as_str().unwrap(),
            "--input-file",
            goal.to_str().unwrap(),
        ],
    ));
    let task_id = task["message_id"].as_str().unwrap();
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let waiting = success(cli(&db, &["actor", "task", "status", task_id]));
    let run = waiting["run_id"].as_str().unwrap();
    let wait = waiting["wait"]["id"].as_str().unwrap();
    let saved = success(cli(&db, &["run", "status", run]));
    let search = &saved["activities"][1];
    assert_eq!(
        search["spec"]["handler"],
        json!({"name":"native.tool","version":3})
    );
    assert_eq!(search["result"]["matches"][0]["path"], "src/lib.rs");
    assert_eq!(search["result"]["matches"][0]["line"], 1);
    assert_eq!(search["result"]["truncated"], false);
    let workspace = Path::new(search["spec"]["input"]["workspace"].as_str().unwrap());
    std::fs::write(workspace.join("src/lib.rs"), "changed after saved read").unwrap();
    success(cli(&db, &["worker", "run", "--until-idle"]));
    assert_eq!(success(cli(&db, &["run", "status", run])), saved);
    let answer = dir.path().join("answer.json");
    std::fs::write(&answer, br#"{"answer":"continue"}"#).unwrap();
    success(cli(
        &db,
        &[
            "run",
            "signal",
            run,
            wait,
            "--input-file",
            answer.to_str().unwrap(),
        ],
    ));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    assert_eq!(
        success(cli(&db, &["actor", "task", "status", task_id]))["status"],
        "succeeded"
    );
    let complete = success(cli(&db, &["run", "status", run]));
    assert_eq!(complete["activities"][1], saved["activities"][1]);
    assert_eq!(complete["activities"][3], saved["activities"][3]);
    assert_eq!(
        std::fs::read_to_string(source.join("src/lib.rs")).unwrap(),
        "pub fn answer() -> i64 { 42 }\n"
    );
}

#[test]
#[ignore = "invoked as a pinned subprocess by the CLI native-test check"]
fn native_test_cli_process_fixture() {
    let text = std::fs::read_to_string("README.md").unwrap();
    println!("Fixed test inspected: {text}");
    assert_eq!(
        text, "Correct project evidence.\n",
        "fixed test expected corrected evidence"
    );
}

#[test]
fn cli_native_tests_pin_the_command_and_save_nonzero_exit_as_feedback() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("native-tests.db");
    let source = dir.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("README.md"), "Original project evidence.\n").unwrap();
    let config = dir.path().join("tests.json");
    let pinned = json!({"argv":[std::env::current_exe().unwrap().to_string_lossy(),"--ignored","--exact","native_test_cli_process_fixture","--nocapture"],"timeout_ms":10000});
    std::fs::write(&config,serde_json::to_vec(&json!({
        "goal":"Test the project","target":{"provider":"scripted","model":"tests","endpoint":""},
        "max_model_calls":2,"max_tokens":300000,"max_output_tokens":512,
        "tools":[{"name":"run_tests","version":1}],"test_command":pinned,
        "script":[{"kind":"call_tool","tool":"run_tests","arguments":{}},{"kind":"finish","summary":"Reported saved test failure"}]
    })).unwrap()).unwrap();
    let actor = success(cli(
        &db,
        &[
            "actor",
            "agent",
            "create",
            "tool",
            "--source",
            source.to_str().unwrap(),
            "--config-file",
            config.to_str().unwrap(),
        ],
    ));
    let input = dir.path().join("goal.json");
    std::fs::write(
        &input,
        br#"{"goal":"Test it","test_command":{"argv":["forged"]}}"#,
    )
    .unwrap();
    let submitted = success(cli(
        &db,
        &[
            "actor",
            "send",
            actor["agent_id"].as_str().unwrap(),
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    let task = submitted["message_id"].as_str().unwrap();
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let waiting = success(cli(&db, &["actor", "task", "status", task]));
    assert_eq!(waiting["wait"]["payload"]["test_command"], pinned);
    assert_eq!(waiting["wait"]["payload"]["arguments"], json!({}));
    let run = waiting["run_id"].as_str().unwrap();
    let wait = waiting["wait"]["id"].as_str().unwrap();
    success(cli(&db, &["run", "approve", run, wait]));
    success(cli(&db, &["run", "approve", run, wait]));
    success(cli(
        &db,
        &[
            "worker",
            "run",
            "--until-idle",
            "--activity-timeout-ms",
            "10",
        ],
    ));
    let done = success(cli(&db, &["actor", "task", "status", task]));
    assert_eq!(done["status"], "succeeded");
    assert_eq!(done["usage"]["admitted_model_calls"], 2);
    assert_eq!(done["admitted_attempts"], 3);
    let state = success(cli(&db, &["run", "status", run]));
    let tests = &state["activities"][1];
    assert_eq!(
        tests["spec"]["handler"],
        json!({"name":"native.tool","version":5})
    );
    assert_eq!(tests["spec"]["input"]["test_command"], pinned);
    assert_eq!(tests["result"]["ok"], false);
    assert_ne!(tests["result"]["exit_code"], 0);
    assert!(
        tests["result"]["stdout"]
            .as_str()
            .unwrap()
            .contains("Original project evidence")
    );
    assert!(
        tests["result"]["stderr"]
            .as_str()
            .unwrap()
            .contains("fixed test expected corrected evidence")
    );
    assert!(tests["result"]["elapsed_ms"].is_u64());
    let history = success(cli(&db, &["run", "history", run]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    assert_eq!(history, success(cli(&db, &["run", "history", run])));
}

#[test]
fn cli_pauses_and_resumes_goals_without_waiting_for_held_work() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("pause-cli.db");
    let config = dir.path().join("answer.json");
    let input = dir.path().join("input.json");
    std::fs::write(&config,serde_json::to_vec(&json!({"goal":"Review","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"ask_human","question":"Continue?"},{"kind":"finish","summary":"Reviewed"}]})).unwrap()).unwrap();
    std::fs::write(&input, br#"{"goal":"Review this"}"#).unwrap();
    let actor = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "answer",
            "--config-file",
            config.to_str().unwrap(),
        ],
    ));
    let submitted = success(cli(
        &database,
        &[
            "actor",
            "send",
            actor["agent_id"].as_str().unwrap(),
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    let root = submitted["message_id"].as_str().unwrap();
    let paused = success(cli(&database, &["actor", "task", "pause-goal", root]));
    assert_eq!(paused["paused"], true);
    assert_eq!(paused["root_task_id"], root);
    assert_eq!(
        success(cli(&database, &["actor", "task", "pause-goal", root])),
        paused
    );
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let held = success(cli(&database, &["actor", "task", "inspect", root]));
    assert_eq!(held["nodes"][0]["execution_held"], true);
    assert_eq!(held["nodes"][0]["run_id"], Value::Null);
    assert_eq!(held["summary"]["visible_usage"]["admitted_model_calls"], 0);
    let pretty = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&database)
        .args(["actor", "task", "inspect", root])
        .output()
        .unwrap();
    assert!(pretty.status.success());
    let text = String::from_utf8(pretty.stdout).unwrap();
    assert!(text.contains("Goal paused at"));
    assert!(text.contains("Admission: held by goal pause"));
    let resumed = success(cli(&database, &["actor", "task", "resume-goal", root]));
    assert_eq!(resumed["paused"], false);
    assert_eq!(resumed["pause"], paused["pause"]);
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let waiting = success(cli(&database, &["actor", "task", "inspect", root]));
    let node = &waiting["nodes"][0];
    assert_eq!(node["reason"]["code"], "human_input");
    success(cli(&database, &["actor", "task", "pause-goal", root]));
    std::fs::write(&input, br#"{"answer":"Yes"}"#).unwrap();
    success(cli(
        &database,
        &[
            "run",
            "signal",
            node["run_id"].as_str().unwrap(),
            node["wait"]["id"].as_str().unwrap(),
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let held = success(cli(&database, &["actor", "task", "inspect", root]));
    assert_eq!(held["summary"]["visible_usage"]["admitted_model_calls"], 1);
    let resumed = success(cli(&database, &["actor", "task", "resume-goal", root]));
    assert_eq!(resumed["revision"], 4);
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let done = success(cli(&database, &["actor", "task", "inspect", root]));
    assert_eq!(done["summary"]["succeeded"], 1);
    assert_eq!(done["summary"]["visible_usage"]["admitted_model_calls"], 2);
    assert_eq!(done["nodes"][0]["execution_held"], false);
    assert_eq!(
        success(cli(&database, &["actor", "task", "resume-goal", root])),
        resumed
    );
    success(cli(&database, &["actor", "task", "cancel-goal", root]));
    assert!(
        !cli(&database, &["actor", "task", "resume-goal", root])
            .status
            .success()
    );
}

#[test]
fn cli_cancels_a_goal_from_its_child_and_preserves_the_first_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("cancel-goal-cli.db");
    let config_file = dir.path().join("config.json");
    let input_file = dir.path().join("input.json");
    std::fs::write(&input_file, br#"{"goal":"Review"}"#).unwrap();
    std::fs::write(&config_file, serde_json::to_vec(&json!({
        "goal":"Review", "target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"ask_human","question":"Continue?"},{"kind":"finish","summary":"Reviewed"}]
    })).unwrap()).unwrap();
    let assistant = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "answer",
            "--config-file",
            config_file.to_str().unwrap(),
        ],
    ));
    std::fs::write(&config_file, serde_json::to_vec(&json!({
        "goal":"Plan", "target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "workers":[{"name":"reviewer","agent_id":assistant["agent_id"],"description":"Reviews"}],
        "delegation":{"max_tasks":2,"max_depth":2},
        "script":[{"kind":"delegate_task","worker":"reviewer","goal":"Review"},{"kind":"finish","summary":"Done"}]
    })).unwrap()).unwrap();
    let planner = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "planner",
            "--config-file",
            config_file.to_str().unwrap(),
        ],
    ));
    let submitted = success(cli(
        &database,
        &[
            "actor",
            "send",
            planner["agent_id"].as_str().unwrap(),
            "--input-file",
            input_file.to_str().unwrap(),
        ],
    ));
    let root = submitted["message_id"].as_str().unwrap();
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let before = success(cli(&database, &["actor", "task", "inspect", root]));
    let child = before["nodes"][1]["task_id"].as_str().unwrap();
    let cancelled = success(cli(&database, &["actor", "task", "cancel-goal", child]));
    assert_eq!(cancelled["receipt"]["root_task_id"], root);
    assert_eq!(cancelled["receipt"]["requested_task_id"], child);
    // A new process finishes cancellation and consumes the saved child reply.
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let again = success(cli(&database, &["actor", "task", "cancel-goal", root]));
    assert_eq!(again["receipt"], cancelled["receipt"]);
    assert_eq!(again["settled"], true);
    assert_eq!(again["pending_tasks"], 0);
    let inspected = success(cli(&database, &["actor", "task", "inspect", child]));
    assert_eq!(inspected["goal_cancellation"], cancelled["receipt"]);
    assert_eq!(inspected["summary"]["stopped"], 2);
    assert_eq!(inspected["summary"]["settled"], true);
    assert_eq!(
        inspected["summary"]["visible_usage"],
        before["summary"]["visible_usage"]
    );
    let pretty = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&database)
        .args(["actor", "task", "inspect", root])
        .output()
        .unwrap();
    assert!(pretty.status.success());
    assert!(
        String::from_utf8(pretty.stdout)
            .unwrap()
            .contains("Goal cancellation requested")
    );

    let independent = success(cli(
        &database,
        &[
            "actor",
            "send",
            assistant["agent_id"].as_str().unwrap(),
            "--input-file",
            input_file.to_str().unwrap(),
        ],
    ));
    let other = independent["message_id"].as_str().unwrap();
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let waiting = success(cli(&database, &["actor", "task", "inspect", other]));
    assert!(waiting.get("goal_cancellation").is_none());
    let node = &waiting["nodes"][0];
    assert_eq!(node["reason"]["code"], "human_input");
    std::fs::write(&input_file, br#"{"answer":"Yes"}"#).unwrap();
    success(cli(
        &database,
        &[
            "run",
            "signal",
            node["run_id"].as_str().unwrap(),
            node["wait"]["id"].as_str().unwrap(),
            "--input-file",
            input_file.to_str().unwrap(),
        ],
    ));
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let done = success(cli(&database, &["actor", "task", "inspect", other]));
    assert_eq!(done["summary"]["succeeded"], 1);
}

#[test]
fn cli_inspects_a_goal_across_a_delegated_wait_and_completion() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("inspect-cli.db");
    let answer_file = dir.path().join("answer.json");
    let planner_file = dir.path().join("planner.json");
    let input_file = dir.path().join("input.json");
    let signal_file = dir.path().join("signal.json");
    std::fs::write(&answer_file,serde_json::to_vec(&json!({"goal":"Review","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"ask_human","question":"May I continue?"},{"kind":"finish","summary":"Reviewed"}]})).unwrap()).unwrap();
    std::fs::write(&input_file, br#"{"goal":"Original goal"}"#).unwrap();
    std::fs::write(&signal_file, br#"{"answer":"Yes"}"#).unwrap();
    let answer = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "answer",
            "--config-file",
            answer_file.to_str().unwrap(),
        ],
    ));
    std::fs::write(&planner_file,serde_json::to_vec(&json!({"goal":"Plan","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "goal_budget":{"max_model_calls":8,"max_tokens":100000},
        "workers":[{"name":"reviewer","agent_id":answer["agent_id"],"description":"Reviews"}],
        "delegation":{"max_tasks":2,"max_depth":2,"shared_budget":{"max_model_calls":8,"max_tokens":100000}},
        "script":[{"kind":"delegate_task","worker":"reviewer","goal":"Review"},{"kind":"finish","summary":"Completed"}]})).unwrap()).unwrap();
    let planner = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "planner",
            "--config-file",
            planner_file.to_str().unwrap(),
        ],
    ));
    let submitted = success(cli(
        &database,
        &[
            "actor",
            "send",
            planner["agent_id"].as_str().unwrap(),
            "--input-file",
            input_file.to_str().unwrap(),
        ],
    ));
    let root = submitted["message_id"].as_str().unwrap();
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let waiting = success(cli(&database, &["actor", "task", "inspect", root]));
    assert_eq!(waiting["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(waiting["nodes"][0]["goal"], "Original goal");
    assert_eq!(waiting["nodes"][1]["reason"]["code"], "human_input");
    assert_eq!(
        waiting["summary"]["visible_usage"]["admitted_model_calls"],
        2
    );
    assert_eq!(waiting["budgets"].as_array().unwrap().len(), 2);
    let leaf = &waiting["nodes"][1];
    let child = leaf["task_id"].as_str().unwrap();
    let from_child = success(cli(&database, &["actor", "task", "inspect", child]));
    assert_eq!(from_child["root_task_id"], root);
    assert_eq!(from_child["nodes"], waiting["nodes"]);
    let pretty = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&database)
        .args(["actor", "task", "inspect", root])
        .output()
        .unwrap();
    assert!(pretty.status.success());
    let text = String::from_utf8(pretty.stdout).unwrap();
    for expected in [
        "[root]",
        "[delegated]",
        "May I continue?",
        "Shared budget scopes",
        "2 model calls",
    ] {
        assert!(text.contains(expected), "{text}");
    }
    assert_eq!(text.matches("member Runs").count(), 2);
    let partial = success(cli(
        &database,
        &["actor", "task", "inspect", child, "--max-nodes", "1"],
    ));
    assert_eq!(partial["requested_task_visible"], false);
    assert_eq!(partial["truncations"][0]["reason"], "max_nodes");
    assert_eq!(partial["summary"]["settled"], false);
    assert!(
        !cli(
            &database,
            &["actor", "task", "inspect", root, "--max-nodes", "0"]
        )
        .status
        .success()
    );
    success(cli(
        &database,
        &[
            "run",
            "signal",
            leaf["run_id"].as_str().unwrap(),
            leaf["wait"]["id"].as_str().unwrap(),
            "--input-file",
            signal_file.to_str().unwrap(),
        ],
    ));
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let done = success(cli(&database, &["actor", "task", "inspect", child]));
    assert_eq!(done["summary"]["settled"], true);
    assert_eq!(done["summary"]["succeeded"], 2);
    assert_eq!(done["summary"]["visible_usage"]["admitted_model_calls"], 4);
    assert_eq!(done["nodes"][0]["result"]["answer"], "Completed");
}

#[test]
fn cli_exposes_goal_scope_remaining_usage_and_exhaustion() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("goal-cli.db");
    let worker_config = directory.path().join("answer.json");
    let planner_config = directory.path().join("planner.json");
    let task_file = directory.path().join("task.json");
    std::fs::write(&worker_config,serde_json::to_vec(&json!({"goal":"Answer","target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"script":[{"kind":"finish","summary":"Result"}]})).unwrap()).unwrap();
    std::fs::write(&task_file, b"{}").unwrap();
    let assistant = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "answer",
            "--config-file",
            worker_config.to_str().unwrap(),
        ],
    ));
    std::fs::write(&planner_config,serde_json::to_vec(&json!({"goal":"Ask the assistant","target":{"provider":"scripted","model":"fixture","endpoint":""},"max_model_calls":4,"max_tokens":100000,"max_output_tokens":512,
        "goal_budget":{"max_model_calls":1,"max_tokens":100000},"workers":[{"name":"assistant","agent_id":assistant["agent_id"],"description":"A permitted worker"}],"delegation":{"max_tasks":2,"max_depth":2},
        "script":[{"kind":"delegate_task","worker":"assistant","goal":"Help"},{"kind":"finish","summary":"Handled"}]})).unwrap()).unwrap();
    let actor = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "planner",
            "--config-file",
            planner_config.to_str().unwrap(),
        ],
    ));
    let submitted = success(cli(
        &database,
        &[
            "actor",
            "send",
            actor["agent_id"].as_str().unwrap(),
            "--input-file",
            task_file.to_str().unwrap(),
        ],
    ));
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let task = success(cli(
        &database,
        &[
            "actor",
            "task",
            "status",
            submitted["message_id"].as_str().unwrap(),
        ],
    ));
    assert_eq!(task["status"], "budget_exhausted");
    let scope = &task["shared_budgets"][0];
    assert_eq!(scope["record"]["kind"], "goal");
    assert_eq!(scope["record"]["usage"]["admitted_model_calls"], 1);
    assert_eq!(scope["remaining_model_calls"], 0);
    assert_eq!(scope["remaining_tokens"], 99980);
    assert_eq!(scope["members"].as_array().unwrap().len(), 2);
    assert_eq!(scope["record"]["exhaustion"]["reason"], "admission_denied");
    let run_id = task["run_id"].as_str().unwrap();
    let run = success(cli(&database, &["run", "status", run_id]));
    assert_eq!(run["record"]["error"]["code"], "goal_model_budget");
    assert_eq!(run["shared_budgets"][0], *scope);
    let pretty = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&database)
        .args(["run", "status", run_id])
        .output()
        .unwrap();
    assert!(pretty.status.success());
    let text = String::from_utf8(pretty.stdout).unwrap();
    assert!(text.contains("Shared goal budget"));
    assert!(text.contains("0 calls and 99980 tokens remaining"));
}

#[test]
fn cli_supervision_pauses_recovers_and_exposes_failure_history() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("supervision-cli.db");
    let config_path = directory.path().join("answer.json");
    let input_path = directory.path().join("task.json");
    // An empty answer is an invalid live decision, so each task fails deterministically.
    std::fs::write(&config_path,serde_json::to_vec(&json!({"goal":"Answer","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"supervision":{"max_consecutive_failures":1},
        "script":[{"kind":"finish","summary":""}]})).unwrap()).unwrap();
    std::fs::write(&input_path, br#"{"goal":"A valid goal"}"#).unwrap();
    let actor = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "answer",
            "--config-file",
            config_path.to_str().unwrap(),
        ],
    ));
    let actor_id = actor["agent_id"].as_str().unwrap();
    let mut ids = vec![];
    for _ in 0..2 {
        ids.push(
            success(cli(
                &database,
                &[
                    "actor",
                    "send",
                    actor_id,
                    "--input-file",
                    input_path.to_str().unwrap(),
                ],
            ))["message_id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let health = success(cli(
        &database,
        &["actor", "supervision", "status", actor_id],
    ));
    assert_eq!(health["status"], "paused");
    assert_eq!(health["supervision"]["consecutive_failures"], 1);
    assert_eq!(health["supervision"]["failures"][0]["task_id"], ids[0]);
    assert_eq!(
        success(cli(&database, &["actor", "task", "status", &ids[0]]))["status"],
        "failed"
    );
    assert!(success(cli(&database, &["actor", "task", "status", &ids[1]]))["run_id"].is_null());
    let resume = cli(&database, &["actor", "resume", actor_id]);
    assert!(!resume.status.success());
    assert!(String::from_utf8_lossy(&resume.stderr).contains("explicit supervision recovery"));
    let recovered = success(cli(
        &database,
        &["actor", "supervision", "recover", actor_id],
    ));
    assert_eq!(recovered["status"], "ready");
    assert_eq!(recovered["supervision"]["recoveries"], 1);
    assert_eq!(recovered["supervision"]["consecutive_failures"], 0);
    success(cli(
        &database,
        &["actor", "supervision", "recover", actor_id],
    ));
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let health = success(cli(
        &database,
        &["actor", "supervision", "status", actor_id],
    ));
    assert_eq!(health["status"], "paused");
    assert_eq!(health["supervision"]["recoveries"], 1);
    assert_eq!(
        health["supervision"]["failures"].as_array().unwrap().len(),
        2
    );
    assert_eq!(
        success(cli(&database, &["actor", "task", "status", &ids[1]]))["status"],
        "failed"
    );
    assert_eq!(
        success(cli(&database, &["actor", "status", actor_id]))["runs"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    std::fs::write(&config_path,br#"{"workflow":{"name":"demo","version":1},"input":{"message":"Hello"},"attempt_limit":5}"#).unwrap();
    let generic = success(cli(
        &database,
        &[
            "actor",
            "create",
            "workflow_worker",
            "--config-file",
            config_path.to_str().unwrap(),
            "--max-consecutive-failures",
            "2",
        ],
    ));
    let health = success(cli(
        &database,
        &[
            "actor",
            "supervision",
            "status",
            generic["agent_id"].as_str().unwrap(),
        ],
    ));
    assert_eq!(
        health["supervision"]["policy"]["max_consecutive_failures"],
        2
    );
}

#[test]
fn cli_native_planner_follow_ups_show_parent_and_chain_limit_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("planner-follow-ups-cli.db");
    let config_path = directory.path().join("agent.json");
    let task_path = directory.path().join("task.json");
    std::fs::write(&config_path,serde_json::to_vec(&json!({"goal":"Assist","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"script":[{"kind":"finish","summary":"Blue"}]})).unwrap()).unwrap();
    let worker = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "answer",
            "--config-file",
            config_path.to_str().unwrap(),
        ],
    ));
    std::fs::write(&config_path,serde_json::to_vec(&json!({"goal":"Plan a review","target":{"provider":"scripted","model":"fixture","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"memory":{},"workers":[{"name":"assistant","agent_id":worker["agent_id"],"description":"An assistant"}],
        "delegation":{"max_tasks":2,"max_depth":2},"follow_up":{"max_follow_ups":1},
        "script":[{"kind":"schedule_follow_up","summary":"Saved blue","goal":"Review blue","after_ms":5}]})).unwrap()).unwrap();
    let actor = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "planner",
            "--config-file",
            config_path.to_str().unwrap(),
            "--task-capacity",
            "1",
        ],
    ));
    let actor_id = actor["agent_id"].as_str().unwrap();
    let root_id = uuid::Uuid::new_v4().to_string();
    std::fs::write(&task_path, br#"{"goal":"Remember blue"}"#).unwrap();
    success(cli(
        &database,
        &[
            "actor",
            "send",
            actor_id,
            "--input-file",
            task_path.to_str().unwrap(),
            "--message-id",
            &root_id,
        ],
    ));
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let parent = success(cli(&database, &["actor", "task", "status", &root_id]));
    assert_eq!(parent["status"], "succeeded");
    assert_eq!(parent["follow_up"]["status"], "registered");
    let future_id = parent["follow_up"]["reminder_id"].as_str().unwrap();
    let reminder = success(cli(&database, &["actor", "reminder", "status", future_id]));
    assert_eq!(reminder["status"], "published");
    assert_eq!(reminder["origin"]["task_id"], root_id);
    let child = success(cli(&database, &["actor", "task", "status", future_id]));
    assert_eq!(child["status"], "succeeded");
    assert_eq!(child["task"]["follow_up_origin"]["root_task_id"], root_id);
    assert_eq!(child["task"]["follow_up_origin"]["depth"], 1);
    assert_eq!(child["follow_up"]["error"], "chain_limit");
    assert_eq!(
        success(cli(&database, &["actor", "reminder", "list", actor_id]))
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        success(cli(&database, &["actor", "memory", "status", actor_id]))["memory"]["revision"],
        2
    );
    success(cli(&database, &["worker", "run", "--until-idle"]));
    assert_eq!(
        success(cli(&database, &["actor", "task", "list", actor_id]))
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn cli_reminders_preserve_deadlines_and_cancellation_then_publish_one_task() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("reminders-cli.db");
    let config_path = directory.path().join("answer.json");
    let input_path = directory.path().join("task.json");
    std::fs::write(&config_path, serde_json::to_vec(&json!({
        "goal":"Answer the assigned task","target":{"provider":"scripted","model":"reminder","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"memory":{},
        "script":[{"kind":"finish","summary":"Blue"}]
    })).unwrap()).unwrap();
    std::fs::write(&input_path, br#"{"goal":"Remember blue"}"#).unwrap();
    let actor = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "answer",
            "--config-file",
            config_path.to_str().unwrap(),
        ],
    ));
    let actor_id = actor["agent_id"].as_str().unwrap();
    let message_id = uuid::Uuid::new_v4().to_string();
    let schedule = [
        "actor",
        "reminder",
        "schedule",
        actor_id,
        "--input-file",
        input_path.to_str().unwrap(),
        "--message-id",
        &message_id,
    ];
    assert!(!cli(&database, &schedule).status.success());
    assert!(
        !cli(
            &database,
            &[
                schedule.as_slice(),
                &["--at-unix-ms", "0", "--after-ms", "50"]
            ]
            .concat()
        )
        .status
        .success()
    );
    let delayed = [schedule.as_slice(), &["--after-ms", "60000"]].concat();
    let saved = success(cli(&database, &delayed));
    assert_eq!(saved["id"], message_id);
    assert_eq!(saved["status"], "scheduled");
    assert_eq!(
        saved["due_at"].as_i64().unwrap() - saved["created_at"].as_i64().unwrap(),
        60000
    );
    assert_eq!(saved, success(cli(&database, &delayed)));
    assert_eq!(
        saved,
        success(cli(
            &database,
            &["actor", "reminder", "status", &message_id]
        ))
    );
    assert!(
        !cli(&database, &["actor", "task", "status", &message_id])
            .status
            .success()
    );
    assert!(
        !cli(
            &database,
            &[
                "actor",
                "send",
                actor_id,
                "--input-file",
                input_path.to_str().unwrap(),
                "--message-id",
                &message_id
            ]
        )
        .status
        .success()
    );
    assert!(
        !cli(
            &database,
            &[schedule.as_slice(), &["--after-ms", "1"]].concat()
        )
        .status
        .success()
    );
    let cancelled = success(cli(
        &database,
        &["actor", "reminder", "cancel", &message_id],
    ));
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(
        cancelled,
        success(cli(
            &database,
            &["actor", "reminder", "cancel", &message_id]
        ))
    );
    assert_eq!(cancelled, success(cli(&database, &delayed)));
    assert_eq!(
        success(cli(&database, &["actor", "reminder", "list", actor_id])),
        json!([cancelled])
    );

    // Paused actors retain overdue reminders and do not keep --until-idle alive.
    assert!(
        cli(&database, &["actor", "pause", actor_id])
            .status
            .success()
    );
    let overdue = success(cli(
        &database,
        &[
            "actor",
            "reminder",
            "schedule",
            actor_id,
            "--input-file",
            input_path.to_str().unwrap(),
            "--at-unix-ms",
            "-1",
        ],
    ));
    let overdue_id = overdue["id"].as_str().unwrap();
    success(cli(&database, &["worker", "run", "--until-idle"]));
    assert_eq!(
        overdue,
        success(cli(&database, &["actor", "reminder", "status", overdue_id]))
    );
    assert!(
        !cli(&database, &["actor", "task", "status", overdue_id])
            .status
            .success()
    );
    assert!(
        cli(&database, &["actor", "resume", actor_id])
            .status
            .success()
    );
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let published = success(cli(&database, &["actor", "reminder", "status", overdue_id]));
    assert_eq!(published["status"], "published");
    assert_eq!(published["due_at"], -1);
    assert_eq!(
        success(cli(&database, &["actor", "task", "status", overdue_id]))["status"],
        "succeeded"
    );
    assert_eq!(
        published,
        success(cli(&database, &["actor", "reminder", "cancel", overdue_id]))
    );
    success(cli(&database, &["worker", "run", "--until-idle"]));
    assert_eq!(
        success(cli(&database, &["actor", "task", "list", actor_id]))
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        success(cli(&database, &["actor", "memory", "status", actor_id]))["memory"]["revision"],
        1
    );
    assert!(
        !cli(&database, &["actor", "task", "status", &message_id])
            .status
            .success()
    );
}

#[test]
fn cli_memory_status_and_clear_persist_across_processes() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("memory-cli.db");
    let config_path = directory.path().join("answer.json");
    let input_path = directory.path().join("task.json");
    let mut config = json!({"goal":"Remember the task","target":{"provider":"scripted","model":"memory","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,"memory":{},"script":[{"kind":"finish","summary":"Blue"}]});
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let actor = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "answer",
            "--config-file",
            config_path.to_str().unwrap(),
        ],
    ));
    let id = actor["agent_id"].as_str().unwrap();
    for goal in ["Remember blue", "Recall the color"] {
        std::fs::write(
            &input_path,
            serde_json::to_vec(&json!({"goal":goal})).unwrap(),
        )
        .unwrap();
        success(cli(
            &database,
            &[
                "actor",
                "send",
                id,
                "--input-file",
                input_path.to_str().unwrap(),
            ],
        ));
        success(cli(&database, &["worker", "run", "--until-idle"]));
    }
    let memory = success(cli(&database, &["actor", "memory", "status", id]));
    assert_eq!(memory["memory"]["revision"], 2);
    assert_eq!(memory["memory"]["entries"].as_array().unwrap().len(), 2);
    assert!(memory["retained_bytes"].as_u64().unwrap() > 0);
    let cleared = success(cli(&database, &["actor", "memory", "clear", id]));
    assert_eq!(cleared["memory"]["generation"], 1);
    assert_eq!(cleared["memory"]["entries"], json!([]));
    assert_eq!(
        cleared,
        success(cli(&database, &["actor", "memory", "status", id]))
    );
    std::fs::write(
        &input_path,
        serde_json::to_vec(&json!({"goal":"Fresh task"})).unwrap(),
    )
    .unwrap();
    success(cli(
        &database,
        &[
            "actor",
            "send",
            id,
            "--input-file",
            input_path.to_str().unwrap(),
        ],
    ));
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let fresh = success(cli(&database, &["actor", "memory", "status", id]));
    assert_eq!(fresh["memory"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(fresh["memory"]["entries"][0]["goal"], "Fresh task");
    config.as_object_mut().unwrap().remove("memory");
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let disabled = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "answer",
            "--config-file",
            config_path.to_str().unwrap(),
        ],
    ));
    let disabled_id = disabled["agent_id"].as_str().unwrap();
    assert!(
        success(cli(&database, &["actor", "memory", "status", disabled_id]))["memory"].is_null()
    );
    assert!(
        !cli(&database, &["actor", "memory", "clear", disabled_id])
            .status
            .success()
    );
}

#[tokio::test]
async fn cli_worker_accepts_positive_concurrency_and_rejects_zero_before_execution() {
    use act0r::{
        bootstrap::local_engine,
        domain::{DefinitionId, RunStatus},
    };
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("concurrent-cli.db");
    let engine = local_engine(&database).await.unwrap();
    let mut ids = vec![];
    for message in ["first", "second"] {
        ids.push(
            engine
                .start(
                    DefinitionId::new("demo", 1),
                    json!({"message":message,"delay_ms":10}),
                    5,
                )
                .await
                .unwrap(),
        );
    }
    let rejected = cli(
        &database,
        &[
            "worker",
            "run",
            "--until-idle",
            "--max-concurrent-activities",
            "0",
        ],
    );
    assert!(!rejected.status.success());
    for id in &ids {
        assert_eq!(
            engine
                .status(*id)
                .await
                .unwrap()
                .record()
                .allowance
                .admitted,
            0
        );
    }
    assert_eq!(
        success(cli(
            &database,
            &[
                "worker",
                "run",
                "--until-idle",
                "--max-concurrent-activities",
                "2"
            ]
        )),
        json!({"worker":"stopped"})
    );
    for id in ids {
        let run = engine.status(id).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Succeeded);
        assert_eq!(run.record().allowance.admitted, 3);
    }
}

#[test]
fn cli_discovers_one_planner_and_defaults_to_it_for_task_groups() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("planner-cli.db");
    let registrations = success(cli(&database, &["actor", "agent", "list"]));
    let planners: Vec<_> = registrations
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["adapter"]["name"] == "planner")
        .collect();
    assert_eq!(planners.len(), 1);
    assert_eq!(planners[0]["adapter"]["version"], 3);
    assert_eq!(planners[0]["capabilities"]["checkpoints"], "model_and_tool");
    assert_eq!(planners[0]["capabilities"]["task_groups"], true);
    let answer_path = directory.path().join("answer.json");
    std::fs::write(&answer_path, serde_json::to_vec(&json!({
        "goal":"Answer the assigned question.","target":{"provider":"scripted","model":"answer","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"finish","summary":"Answered."}]
    })).unwrap()).unwrap();
    let mut workers = vec![];
    for name in ["a", "b"] {
        let actor = success(cli(
            &database,
            &[
                "actor",
                "agent",
                "create",
                "answer",
                "--config-file",
                answer_path.to_str().unwrap(),
            ],
        ));
        workers.push(json!({"name":name,"agent_id":actor["agent_id"],"description":name}));
    }
    let config_path = directory.path().join("planner.json");
    std::fs::write(&config_path, serde_json::to_vec(&json!({
        "goal":"Combine both results.","target":{"provider":"scripted","model":"planner","endpoint":""},
        "max_model_calls":3,"max_tokens":100000,"max_output_tokens":512,
        "tools":[{"name":"sum","version":1}],
        "workers":workers,"delegation":{"max_tasks":2,"max_depth":1,"max_parallel_tasks":2},
        "script":[{"kind":"call_tool","tool":"sum","arguments":{"left":19,"right":23}},
            {"kind":"delegate_tasks","tasks":[{"worker":"a","goal":"First question"},{"worker":"b","goal":"Second question"}]},
            {"kind":"finish","summary":"Combined results."}]
    })).unwrap()).unwrap();
    let retired = cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "planner",
            "--adapter-version",
            "1",
            "--config-file",
            config_path.to_str().unwrap(),
        ],
    );
    assert!(!retired.status.success());
    let actor = success(cli(
        &database,
        &[
            "actor",
            "agent",
            "create",
            "planner",
            "--config-file",
            config_path.to_str().unwrap(),
        ],
    ));
    let actor_id = actor["agent_id"].as_str().unwrap();
    let status = success(cli(&database, &["actor", "status", actor_id]));
    assert_eq!(
        status["agent"]["config"]["registration"]["adapter"]["version"],
        3
    );
    let task_path = directory.path().join("task.json");
    std::fs::write(&task_path, "{}").unwrap();
    let task = success(cli(
        &database,
        &[
            "actor",
            "send",
            actor_id,
            "--input-file",
            task_path.to_str().unwrap(),
        ],
    ));
    let worker = cli(
        &database,
        &[
            "worker",
            "run",
            "--until-idle",
            "--max-concurrent-activities",
            "2",
        ],
    );
    assert!(
        worker.status.success(),
        "{}",
        String::from_utf8_lossy(&worker.stderr)
    );
    let done = success(cli(
        &database,
        &[
            "actor",
            "task",
            "status",
            task["message_id"].as_str().unwrap(),
        ],
    ));
    assert_eq!(done["status"], "succeeded");
    assert_eq!(done["task_groups"].as_array().unwrap().len(), 1);
    assert_eq!(done["group_children"].as_array().unwrap().len(), 2);
    assert!(done["task_groups"][0]["joined_at"].is_number());
    let run = success(cli(
        &database,
        &["run", "status", done["run_id"].as_str().unwrap()],
    ));
    let tools: Vec<_> = run["activities"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["spec"]["handler"]["name"] == "native.tool")
        .collect();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["result"], json!({"sum":42}));
}

#[test]
fn cli_search_explores_distinct_actions_and_returns_a_verified_patch() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("search.db");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let config = root.join("examples/configs/search-scripted.json");
    let source = root.join("examples/benchmark-cases/addition/source");
    let started = success(cli(
        &database,
        &[
            "search",
            "start",
            "--source",
            source.to_str().unwrap(),
            "--config-file",
            config.to_str().unwrap(),
        ],
    ));
    let id = started["search_id"].as_str().unwrap();
    assert!(!cli(&database, &["search", "result", id]).status.success());
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let status = success(cli(&database, &["search", "status", id]));
    assert_eq!(status["search"]["phase"], "completed");
    assert_eq!(status["search"]["verified"], true);
    assert_eq!(status["task"]["usage"]["admitted_model_calls"], 6);
    let result = success(cli(&database, &["search", "result", id]));
    assert_eq!(result["evaluation"]["passed"], true);
    assert!(
        result["evaluation"]["diff"][0]["patch"]
            .as_str()
            .unwrap()
            .contains("+    left + right")
    );
}

#[tokio::test]
async fn cli_creates_inspects_and_resumes_durable_branches() {
    use act0r::{
        bootstrap::local_app,
        domain::{CheckpointId, ForkId, task::TaskConfig},
    };
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("branches.db");
    let (engine, agents) = local_app(&database).await.unwrap();
    let mut config: TaskConfig =
        serde_json::from_str(include_str!("../examples/configs/task-scripted.json")).unwrap();
    config.candidates = 1;
    let task_id = agents
        .tasks()
        .unwrap()
        .start(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/coding-task"),
            config,
        )
        .await
        .unwrap();
    let parent = agents
        .tasks()
        .unwrap()
        .status(task_id)
        .await
        .unwrap()
        .record()
        .candidates[0]
        .run_id;
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..6 {
        assert!(worker.step().await.unwrap());
    }
    engine.pause(parent).await.unwrap();
    drop(worker);
    let parent_id = parent.to_string();
    let checkpoint_id = CheckpointId::new().to_string();
    let checkpoint = success(cli(
        &database,
        &[
            "checkpoint",
            "create",
            &parent_id,
            "--checkpoint-id",
            &checkpoint_id,
        ],
    ));
    assert_eq!(checkpoint["status"], "preparing");
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let checkpoint = success(cli(&database, &["checkpoint", "status", &checkpoint_id]));
    assert_eq!(checkpoint["status"], "ready");
    assert_eq!(checkpoint["seed"]["origin"]["attempts"], 6);
    let fork_id = ForkId::new().to_string();
    let planned = success(cli(
        &database,
        &[
            "fork",
            "create",
            &checkpoint_id,
            "--candidates",
            "2",
            "--fork-id",
            &fork_id,
        ],
    ));
    assert_eq!(planned["status"], "preparing");
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let fork = success(cli(&database, &["fork", "status", &fork_id]));
    assert_eq!(fork["status"], "ready");
    assert_eq!(fork["run_ids"], planned["run_ids"]);
    // Finished children remain selectable when the user retires the paused parent.
    success(cli(&database, &["run", "cancel", &parent_id]));
    let task = success(cli(&database, &["task", "status", &task_id.to_string()]));
    assert_eq!(task["status"], "succeeded");
    assert_eq!(task["attempts"], 14);
    assert_eq!(task["usage"]["admitted_model_calls"], 7);
    assert_eq!(task["candidates"].as_array().unwrap().len(), 3);
    let result = success(cli(&database, &["task", "result", &task_id.to_string()]));
    assert!(
        result["evaluation"]["diff"][0]["patch"]
            .as_str()
            .unwrap()
            .contains("+    left + right")
    );
    let repeated = success(cli(
        &database,
        &[
            "fork",
            "create",
            &checkpoint_id,
            "--candidates",
            "2",
            "--fork-id",
            &fork_id,
        ],
    ));
    assert_eq!(repeated["status"], "ready");
    assert_eq!(repeated["run_ids"], planned["run_ids"]);
}

#[test]
fn cli_benchmark_runs_equal_budget_trials_and_exports_a_persisted_report() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("benchmark.db");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let suite = root.join("examples/configs/benchmark-scripted.json");
    let started = success(cli(
        &database,
        &[
            "benchmark",
            "start",
            "--suite-file",
            suite.to_str().unwrap(),
            "--case",
            "clamp",
            "--repetitions",
            "2",
        ],
    ));
    let id = started["benchmark_id"].as_str().unwrap();
    assert_eq!(
        success(cli(&database, &["benchmark", "status", id]))["trials"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let report = success(cli(&database, &["benchmark", "report", id]));
    assert_eq!(report["status"], "completed");
    assert_eq!(report["summary"]["arms"][0]["successes"], 2);
    assert_eq!(report["summary"]["arms"][1]["successes"], 2);
    assert_eq!(report["summary"]["arms"][0]["model_calls"], 6);
    assert_eq!(report["summary"]["arms"][1]["model_calls"], 18);
    assert_eq!(report["trials"][0]["candidates"], 1);
    assert_eq!(report["trials"][1]["candidates"], 3);
    assert_eq!(report["trials"][2]["candidates"], 3);
    assert_eq!(report["trials"][3]["candidates"], 1);
    let output = directory.path().join("reports/pilot.json");
    success(cli(
        &database,
        &[
            "benchmark",
            "report",
            id,
            "--output",
            output.to_str().unwrap(),
        ],
    ));
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(output).unwrap()).unwrap(),
        report
    );
    let output = directory.path().join("reports/pilot.md");
    let saved = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&database)
        .args([
            "benchmark",
            "report",
            id,
            "--output",
            output.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        saved.status.success(),
        "{}",
        String::from_utf8_lossy(&saved.stderr)
    );
    let markdown = std::fs::read_to_string(output).unwrap();
    assert!(markdown.contains("100.0%"));
    assert!(markdown.contains("+    value.max(min).min(max)"));
    assert!(markdown.contains("Final tests SHA-256"));
    assert!(
        !cli(
            &database,
            &[
                "benchmark",
                "start",
                "--suite-file",
                suite.to_str().unwrap(),
                "--case",
                "unknown"
            ]
        )
        .status
        .success()
    );
}

#[test]
fn cli_runs_three_candidates_and_returns_the_recorded_verified_patch() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("tasks.db");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let config = root.join("examples/configs/task-scripted.json");
    let source = root.join("examples/coding-task");
    let started = success(cli(
        &database,
        &[
            "task",
            "start",
            "--source",
            source.to_str().unwrap(),
            "--config-file",
            config.to_str().unwrap(),
        ],
    ));
    let id = started["task_id"].as_str().unwrap();
    let initial = success(cli(&database, &["task", "status", id]));
    assert_eq!(initial["candidates"].as_array().unwrap().len(), 3);
    assert!(initial["selected"].is_null());
    assert!(!cli(&database, &["task", "result", id]).status.success());
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let status = success(cli(&database, &["task", "status", id]));
    assert_eq!(status["status"], "succeeded");
    assert_eq!(status["attempts"], 30);
    assert_eq!(status["usage"]["admitted_model_calls"], 15);
    let result = success(cli(&database, &["task", "result", id]));
    assert_eq!(result["run_id"], status["selected"]);
    assert_eq!(result["evaluation"]["passed"], true);
    assert_eq!(result["evaluation"]["tests"]["exit_code"], 0);
    assert!(
        result["evaluation"]["diff"][0]["patch"]
            .as_str()
            .unwrap()
            .contains("+    left + right")
    );
    let human = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&database)
        .args(["run", "status", result["run_id"].as_str().unwrap()])
        .output()
        .unwrap();
    assert!(human.status.success());
    assert!(
        String::from_utf8(human.stdout)
            .unwrap()
            .contains("Context:")
    );
}

#[test]
fn cli_exports_three_strategy_benchmarks_with_frozen_search_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("search-benchmark.db");
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/configs/benchmark-search-scripted.json");
    let started = success(cli(
        &database,
        &[
            "benchmark",
            "start",
            "--suite-file",
            suite.to_str().unwrap(),
            "--case",
            "addition",
        ],
    ));
    let id = started["benchmark_id"].as_str().unwrap();
    let initial = success(cli(&database, &["benchmark", "status", id]));
    assert_eq!(initial["trials"].as_array().unwrap().len(), 3);
    assert_eq!(initial["trials"][2]["strategy"], "bounded_search");
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let report = success(cli(&database, &["benchmark", "report", id]));
    assert_eq!(report["status"], "completed");
    assert_eq!(report["summary"]["arms"][2]["successes"], 1);
    assert_eq!(report["summary"]["arms"][2]["model_calls"], 6);
    assert_eq!(report["trials"][2]["metrics"]["search"]["verified"], true);
    let output = directory.path().join("search-report.md");
    let saved = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&database)
        .args([
            "benchmark",
            "report",
            id,
            "--output",
            output.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        saved.status.success(),
        "{}",
        String::from_utf8_lossy(&saved.stderr)
    );
    let markdown = std::fs::read_to_string(output).unwrap();
    assert!(markdown.contains("bounded search"));
    assert!(markdown.contains("feedback_cost v1"));
    assert!(markdown.contains("New charged tokens"));
}

#[test]
fn cli_runs_demo_and_exposes_result_and_history() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("cli.db");
    let input = directory.path().join("input.json");
    std::fs::write(&input, json!({"message": "  hoss  "}).to_string()).unwrap();
    let started = success(cli(
        &database,
        &[
            "run",
            "start",
            "demo",
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    let id = started["run_id"].as_str().unwrap();
    assert_eq!(
        success(cli(&database, &["run", "status", id]))["record"]["status"],
        "queued"
    );
    success(cli(&database, &["worker", "run", "--until-idle"]));
    let status = success(cli(&database, &["run", "status", id]));
    assert_eq!(status["record"]["status"], "succeeded");
    assert_eq!(
        status["record"]["result"],
        json!({"text": "hoss", "tool": "echo"})
    );
    assert_eq!(status["record"]["allowance"]["admitted"], 3);
    let history = success(cli(&database, &["run", "history", id]));
    assert_eq!(
        history
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt_admitted")
            .count(),
        3
    );
    assert_eq!(
        history.as_array().unwrap().last().unwrap()["kind"],
        "run_succeeded"
    );
    // Starting another worker must not replay terminal runs.
    success(cli(&database, &["worker", "run", "--until-idle"]));
    assert_eq!(history, success(cli(&database, &["run", "history", id])));
}

#[test]
fn cli_agent_approval_signal_and_lifecycle_commands_complete_coding_task() {
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("agent.db");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = root.join("examples/coding-task");
    let config = root.join("examples/configs/agent-scripted.json");
    let started = success(cli(
        &db,
        &[
            "agent",
            "start",
            "--source",
            source.to_str().unwrap(),
            "--config-file",
            config.to_str().unwrap(),
        ],
    ));
    let id = started["run_id"].as_str().unwrap();
    success(cli(&db, &["run", "pause", id]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    assert_eq!(
        success(cli(&db, &["run", "status", id]))["record"]["status"],
        "paused"
    );
    success(cli(&db, &["run", "resume", id]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let status = success(cli(&db, &["run", "status", id]));
    let wait = status["record"]["wait"]["id"].as_str().unwrap();
    assert_eq!(status["record"]["status"], "waiting");
    success(cli(&db, &["run", "approve", id, wait]));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let status = success(cli(&db, &["run", "status", id]));
    let test_wait = status["record"]["wait"]["id"].as_str().unwrap();
    let input = directory.path().join("approval.json");
    std::fs::write(&input, "{\"approved\":true}").unwrap();
    success(cli(
        &db,
        &[
            "run",
            "signal",
            id,
            test_wait,
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let status = success(cli(&db, &["run", "status", id]));
    assert_eq!(status["record"]["status"], "succeeded");
    assert_eq!(status["record"]["usage"]["admitted_model_calls"], 5);
    assert_eq!(status["record"]["workflow"]["version"], 2);
    assert_eq!(
        status["record"]["state"]["context"]["selection"]["policy"]["max_input_bytes"],
        65536
    );
    let text = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&db)
        .args(["run", "status", id])
        .output()
        .unwrap();
    assert!(text.status.success());
    assert!(String::from_utf8_lossy(&text.stdout).contains("Context:"));
    let history = success(cli(&db, &["run", "history", id]));
    success(cli(&db, &["run", "approve", id, wait]));
    assert_eq!(history, success(cli(&db, &["run", "history", id])));
    let started = success(cli(
        &db,
        &[
            "agent",
            "start",
            "--source",
            source.to_str().unwrap(),
            "--config-file",
            config.to_str().unwrap(),
        ],
    ));
    let id = started["run_id"].as_str().unwrap();
    success(cli(&db, &["run", "cancel", id]));
    assert_eq!(
        success(cli(&db, &["run", "status", id]))["record"]["status"],
        "cancelled"
    );
}

#[test]
fn invalid_input_and_unknown_run_report_failure() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("cli.db");
    let input = directory.path().join("invalid.json");
    std::fs::write(&input, "{}").unwrap();
    let output = cli(
        &database,
        &[
            "run",
            "start",
            "demo",
            "--input-file",
            input.to_str().unwrap(),
        ],
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("missing field"));
    let id = act0r::RunId::new().to_string();
    let output = cli(&database, &["run", "status", &id]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("was not found"));
}

#[test]
fn cli_waits_for_durable_timer_and_records_firing() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("timer.db");
    let input = directory.path().join("timer.json");
    std::fs::write(
        &input,
        json!({"message": "timer", "wait_ms": 25}).to_string(),
    )
    .unwrap();
    let started = success(cli(
        &database,
        &[
            "run",
            "start",
            "timer_demo",
            "--input-file",
            input.to_str().unwrap(),
        ],
    ));
    let id = started["run_id"].as_str().unwrap();
    success(cli(
        &database,
        &["worker", "run", "--until-idle", "--poll-ms", "5"],
    ));
    let status = success(cli(&database, &["run", "status", id]));
    assert_eq!(status["record"]["status"], "succeeded");
    assert_eq!(status["record"]["allowance"]["admitted"], 1);
    assert_eq!(status["timers"][0]["status"], "fired");
    let history = success(cli(&database, &["run", "history", id]));
    assert_eq!(
        history
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "timer_fired")
            .count(),
        1
    );
}

#[tokio::test]
async fn cli_reconciles_verified_test_output_and_rejects_conflicting_decisions() {
    use act0r::domain::{
        ReconciliationDecision, ReconciliationRequest,
        agent::{AgentAction, AgentConfig},
    };
    let directory = tempfile::tempdir().unwrap();
    let db = directory.path().join("reconcile.db");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = directory.path().join("source");
    std::fs::create_dir_all(source.join("src")).unwrap();
    std::fs::create_dir_all(source.join("tests")).unwrap();
    for path in ["Cargo.toml", "tests/add.rs"] {
        std::fs::copy(
            root.join("examples/coding-task").join(path),
            source.join(path),
        )
        .unwrap();
    }
    std::fs::write(
        source.join("src/lib.rs"),
        "pub fn add(left: i32, right: i32) -> i32 { left + right }\n",
    )
    .unwrap();
    let mut cfg: AgentConfig = serde_json::from_slice(
        &std::fs::read(root.join("examples/configs/agent-scripted.json")).unwrap(),
    )
    .unwrap();
    cfg.require_approval = false;
    cfg.repeat_safe_tests = false;
    cfg.script = vec![
        AgentAction::RunTests,
        AgentAction::Finish {
            summary: "Verified the fixed tests.".into(),
        },
    ];
    let (engine, agents) = act0r::bootstrap::local_app(&db).await.unwrap();
    let id = agents.start(source, cfg, 10).await.unwrap();
    let mut worker = engine.worker().await.unwrap();
    worker.step().await.unwrap();
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    let result = outcome.result().unwrap().clone();
    assert_eq!(result["ok"], true);
    assert_eq!(result["exit_code"], 0);
    // Lose the result before persistence, then recover in a separate CLI process.
    drop(worker);
    success(cli(&db, &["worker", "run", "--until-idle"]));
    let id = id.to_string();
    let status = success(cli(&db, &["run", "status", &id]));
    assert_eq!(status["record"]["status"], "blocked");
    let text = Command::new(env!("CARGO_BIN_EXE_act0r"))
        .arg("--db")
        .arg(&db)
        .args(["run", "status", &id])
        .output()
        .unwrap();
    assert!(text.status.success());
    let text = String::from_utf8_lossy(&text.stdout);
    assert!(text.contains(&execution.activity_id.to_string()));
    assert!(text.contains(&execution.attempt_id.to_string()));
    assert!(text.contains("decision-file"));
    let mut decision = ReconciliationRequest {
        activity_id: execution.activity_id,
        attempt_id: execution.attempt_id,
        decision: ReconciliationDecision::Accept { result },
        evidence:
            "Captured the completed cargo test output; exit code 0 and all fixed tests passed."
                .into(),
    };
    let file = directory.path().join("decision.json");
    std::fs::write(&file, serde_json::to_vec(&decision).unwrap()).unwrap();
    let args = [
        "run",
        "reconcile",
        &id,
        "--decision-file",
        file.to_str().unwrap(),
    ];
    assert_eq!(
        success(cli(&db, &args))["action"],
        "reconciliation_recorded"
    );
    let history = success(cli(&db, &["run", "history", &id]));
    success(cli(&db, &args));
    assert_eq!(history, success(cli(&db, &["run", "history", &id])));
    decision.decision = ReconciliationDecision::Retry {};
    std::fs::write(&file, serde_json::to_vec(&decision).unwrap()).unwrap();
    let conflict = cli(&db, &args);
    assert!(!conflict.status.success());
    assert!(String::from_utf8_lossy(&conflict.stderr).contains("conflicting reconciliation"));
    success(cli(&db, &["worker", "run", "--until-idle"]));
    assert_eq!(
        success(cli(&db, &["run", "status", &id]))["record"]["status"],
        "succeeded"
    );
}
