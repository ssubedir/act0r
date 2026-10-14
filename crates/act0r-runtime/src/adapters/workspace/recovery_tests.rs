//! Real interrupted native writes/tests with scoped HTTP receipts and ordered harness turns.
use super::*;
use crate::{
    Engine,
    adapters::http_api,
    application::AgentService,
    bootstrap::local_app,
    domain::{harness::*, tools::*, *},
};
use reqwest::{Client, StatusCode};
use std::{
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::oneshot, task::JoinHandle};

struct Executor {
    inner: Arc<dyn ActivityExecutor>,
    kind: String,
    stop: Option<PathBuf>,
}
impl ActivityExecutor for Executor {
    fn supports(&self, id: &DefinitionId) -> bool {
        self.inner.supports(id)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if execution.handler == DefinitionId::new("agent.model", 1) {
                let request: ModelRequest = serde_json::from_value(execution.input)?;
                let action = if request.turn == 0 {
                    let arguments = match self.kind.as_str() {
                        "replace_text" => json!({"path":"src/a.txt","old":"old","new":"new"}),
                        "create_file" => json!({"path":"src/new.txt","content":"created"}),
                        _ => json!({}),
                    };
                    json!({"kind":"call_tool","tool":self.kind,"arguments":arguments})
                } else if request.turn == 1 && self.kind != "run_tests" {
                    json!({"kind":"call_tool","tool":"run_tests","arguments":{}})
                } else {
                    json!({"kind":"finish","summary":"Verified fixture"})
                };
                return Ok(Outcome::Success(json!({"action":action,"request_id":null})));
            }
            let freeze = tools::is_native_tool_handler(&execution.handler)
                && execution.input["definition"]["id"]["name"] == self.kind;
            let outcome = self.inner.execute(execution).await?;
            if freeze && let Some(marker) = &self.stop {
                assert!(matches!(outcome, Outcome::Success(_)));
                fs::write(marker, "effect finished; SQLite receipt not saved")?;
                std::future::pending::<()>().await;
            }
            Ok(outcome)
        })
    }
}
async fn app(root: &Path, stop: bool) -> (Engine, AgentService) {
    let (mut engine, agents) = local_app(root.join("recovery.db")).await.unwrap();
    engine.executor = Arc::new(Executor {
        inner: engine.executor.clone(),
        kind: fs::read_to_string(root.join("tool-kind")).unwrap(),
        stop: stop.then(|| root.join("effect-finished")),
    });
    (engine, agents)
}
struct Server {
    url: String,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), Error>>,
}
impl Server {
    async fn start(agents: AgentService) -> Self {
        let listener = http_api::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(http_api::serve(
            listener,
            Arc::new(agents),
            Default::default(),
            async {
                let _ = stopped.await;
                Ok(())
            },
        ));
        Self { url, stop, task }
    }
    async fn close(self) {
        let _ = self.stop.send(());
        self.task.await.unwrap().unwrap();
    }
}
async fn get(client: &Client, url: &str) -> Value {
    let response = client.get(url).send().await.unwrap();
    let status = response.status();
    let value = response.json().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{value}");
    value
}
async fn post(client: &Client, url: &str, body: &Value, status: StatusCode) -> Value {
    let response = client.post(url).json(body).send().await.unwrap();
    let actual = response.status();
    let value = response.json().await.unwrap();
    assert_eq!(actual, status, "{value}");
    value
}
async fn settle(engine: &Engine) {
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..100 {
        if !worker.step().await.unwrap() {
            return;
        }
    }
    panic!("fixture did not settle");
}
async fn approve(engine: &Engine, agents: &AgentService, receipt: &HarnessReceipt) {
    let inbox = agents
        .teams()
        .unwrap()
        .tasks()
        .unwrap()
        .inbox(receipt.team_id, receipt.task_id, Default::default())
        .await
        .unwrap();
    let item = inbox.items.iter().find(|item| item.actionable).unwrap();
    agents
        .teams()
        .unwrap()
        .tasks()
        .unwrap()
        .respond(
            receipt.team_id,
            receipt.task_id,
            item.wait.id,
            HumanResponse::Approval { approved: true },
        )
        .await
        .unwrap();
    // Do not execute here: the child worker must be killed before persisting its receipt.
    let run = engine.status(item.run_id).await.unwrap();
    assert!(run.record().status.is_active());
}
fn test_command() -> WorkspaceTestCommand {
    WorkspaceTestCommand {
        argv: vec![
            std::env::current_exe().unwrap().to_string_lossy().into(),
            "--ignored".into(),
            "--exact".into(),
            "adapters::workspace::recovery_tests::project_test_command".into(),
            "--nocapture".into(),
        ],
        timeout_ms: 10000,
    }
}
async fn fixture(root: &Path, kind: &str) -> (Engine, AgentService, HarnessReceipt, RunId, String) {
    fs::write(root.join("tool-kind"), kind).unwrap();
    let source = root.join("project");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/a.txt"), "old").unwrap();
    let (engine, agents) = app(root, false).await;
    let input:HarnessRequest=serde_json::from_value(json!({"message_id":MessageId::new(),"prompt":"Repair fixture","workspace":source.to_string_lossy(),
        "target":{"provider":"openai","model":"unused","endpoint":"http://127.0.0.1:1234/v1/chat/completions"},"coding":{"writable_paths":["src/"],"test_command":test_command()}})).unwrap();
    let receipt = agents
        .teams()
        .unwrap()
        .harness()
        .unwrap()
        .start(input)
        .await
        .unwrap();
    settle(&engine).await;
    let run = agents
        .actors()
        .unwrap()
        .task(receipt.task_id)
        .await
        .unwrap()
        .run_id
        .unwrap();
    let team = agents.teams().unwrap().get(receipt.team_id).await.unwrap();
    let workspace = team.members[0].workspace.as_ref().unwrap().path.clone();
    approve(&engine, &agents, &receipt).await;
    (engine, agents, receipt, run, workspace)
}
#[tokio::test]
#[ignore = "child fixture executes a real native effect before its worker is killed"]
async fn interrupted_tool_worker() {
    let root = std::env::current_dir().unwrap();
    let (engine, _) = app(&root, true).await;
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..100 {
        assert!(worker.step().await.unwrap());
    }
    panic!("boundary not reached");
}
#[test]
#[ignore = "fixed project command executes only in a managed test workspace"]
fn project_test_command() {
    let root = std::env::current_dir().unwrap();
    assert!(root.join(PRIVATE).join("managed").is_file());
    let path = root.join(PRIVATE).join("test-count");
    let count = fs::read_to_string(&path)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    fs::write(path, (count + 1).to_string()).unwrap();
    println!("fixed test command passed");
}
async fn kill(root: &Path) {
    let marker = root.join("effect-finished");
    let _ = fs::remove_file(&marker);
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--ignored",
            "--exact",
            "adapters::workspace::recovery_tests::interrupted_tool_worker",
            "--nocapture",
        ])
        .current_dir(root)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = command.spawn().unwrap();
    let start = Instant::now();
    while !marker.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("worker exited early {status}");
        }
        if start.elapsed() > Duration::from_secs(30) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("boundary timeout");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
}
fn recovery_base(server: &Server, receipt: &HarnessReceipt) -> String {
    format!(
        "{}/v1/harness/sessions/{}/turns/{}",
        server.url, receipt.session_id, receipt.task_id
    )
}
fn decision(recovery: &Value, action: &str) -> Value {
    json!({"activity_id":recovery["activity_id"],"attempt_id":recovery["attempt_id"],"operation_stopped":true,"evidence":"Worker and fixed test process exited; inspected managed copy.","decision":if action=="retry" {json!({"action":"retry"})} else {json!({"action":"fail","message":"Keep existing changes for manual review."})}})
}

#[tokio::test]
async fn killed_edits_and_tests_recover_with_exact_receipts_across_restarts() {
    for kind in ["replace_text", "create_file", "run_tests"] {
        let dir = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(dir.path()).unwrap();
        let (engine, agents, receipt, run, workspace) = fixture(&root, kind).await;
        let follow = agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .append(
                receipt.session_id,
                HarnessTurnRequest {
                    message_id: MessageId::new(),
                    prompt: "Follow-up must wait".into(),
                },
            )
            .await
            .unwrap();
        drop(agents);
        drop(engine);
        kill(&root).await;
        let (engine, agents) = app(&root, false).await;
        settle(&engine).await;
        assert!(
            agents
                .actors()
                .unwrap()
                .task(follow.task_id)
                .await
                .unwrap()
                .run_id
                .is_none()
        );
        agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .tool_recoveries(receipt.session_id, receipt.task_id)
            .await
            .unwrap();
        let server = Server::start(agents).await;
        let client = Client::builder().no_proxy().build().unwrap();
        let base = recovery_base(&server, &receipt);
        let page = get(&client, &format!("{base}/tool-recovery")).await;
        let recovery = &page["items"][0]["recovery"];
        assert_eq!(page["items"][0]["run_id"], json!(run));
        assert_eq!(recovery["retry_allowed"], true);
        assert_eq!(
            recovery["inspection"]["state"],
            if kind == "run_tests" {
                "test_outcome_unknown"
            } else {
                "already_applied"
            }
        );
        let mutation = format!("{base}/runs/{run}/reconciliation");
        let body = decision(recovery, "retry");
        for bad in [
            json!({"operation_stopped":false}),
            json!({"evidence":" "}),
            json!({"evidence":"🦀".repeat(4097)}),
            json!({"decision":{"action":"accept","result":{"ok":true}}}),
            json!({"argv":["other"]}),
        ] {
            let status = if bad.get("argv").is_some() || bad.get("decision").is_some() {
                StatusCode::UNPROCESSABLE_ENTITY
            } else {
                StatusCode::BAD_REQUEST
            };
            let mut invalid = body.clone();
            invalid
                .as_object_mut()
                .unwrap()
                .extend(bad.as_object().unwrap().clone());
            post(&client, &mutation, &invalid, status).await;
        }
        let wrong = format!(
            "{}/v1/harness/sessions/{}/turns/{}/runs/{run}/reconciliation",
            server.url,
            SessionId::new(),
            receipt.task_id
        );
        post(&client, &wrong, &body, StatusCode::NOT_FOUND).await;
        post(
            &client,
            &format!(
                "{}/v1/harness/sessions/{}/turns/{}/runs/{run}/reconciliation",
                server.url, receipt.session_id, follow.task_id
            ),
            &body,
            StatusCode::NOT_FOUND,
        )
        .await;
        let (a, b) = tokio::join!(
            post(&client, &mutation, &body, StatusCode::OK),
            post(&client, &mutation, &body, StatusCode::OK)
        );
        assert_eq!(a, b);
        let mut changed = body.clone();
        changed["evidence"] = json!("Changed notes");
        post(&client, &mutation, &changed, StatusCode::CONFLICT).await;
        server.close().await;
        drop(engine);
        // A second real interruption gets a new attempt, but the old saved packet is still just an acknowledgement.
        kill(&root).await;
        let (engine, agents) = app(&root, false).await;
        settle(&engine).await;
        let server = Server::start(agents).await;
        let base = recovery_base(&server, &receipt);
        let mutation = format!("{base}/runs/{run}/reconciliation");
        let page = get(&client, &format!("{base}/tool-recovery")).await;
        let second = &page["items"][0]["recovery"];
        assert_ne!(second["attempt_id"], body["attempt_id"]);
        post(&client, &mutation, &body, StatusCode::OK).await;
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Blocked
        );
        if kind == "run_tests" {
            assert_eq!(
                fs::read_to_string(Path::new(&workspace).join(PRIVATE).join("test-count")).unwrap(),
                "2"
            );
        }
        post(
            &client,
            &mutation,
            &decision(second, "retry"),
            StatusCode::OK,
        )
        .await;
        settle(&engine).await;
        if kind != "run_tests" {
            let inbox = get(
                &client,
                &format!(
                    "{}/v1/teams/{}/tasks/{}/inbox",
                    server.url, receipt.team_id, receipt.task_id
                ),
            )
            .await;
            let wait = &inbox["items"][0]["wait"]["id"];
            post(
                &client,
                &format!(
                    "{}/v1/teams/{}/tasks/{}/waits/{}/responses",
                    server.url,
                    receipt.team_id,
                    receipt.task_id,
                    wait.as_str().unwrap()
                ),
                &json!({"kind":"approval","approved":true}),
                StatusCode::OK,
            )
            .await;
            settle(&engine).await;
        }
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Succeeded
        );
        if kind == "run_tests" {
            assert_eq!(
                fs::read_to_string(Path::new(&workspace).join(PRIVATE).join("test-count")).unwrap(),
                "3"
            );
        }
        post(&client, &mutation, &body, StatusCode::OK).await;
        let page = get(&client, &format!("{base}/tool-recovery")).await;
        assert_eq!(
            page["items"][0]["reconciliation_receipts"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert!(page["items"][0]["recovery"].is_null());
        server.close().await;
        let (_, agents) = app(&root, false).await;
        assert!(
            agents
                .actors()
                .unwrap()
                .task(follow.task_id)
                .await
                .unwrap()
                .run_id
                .is_some()
        );
        assert_eq!(
            fs::read_to_string(root.join("project/src/a.txt")).unwrap(),
            "old"
        );
    }
}

#[tokio::test]
async fn recovery_keeps_pause_and_failure_preserves_effects_before_releasing_followups() {
    for kind in ["replace_text", "create_file", "run_tests"] {
        let dir = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(dir.path()).unwrap();
        let (engine, agents, receipt, run, workspace) = fixture(&root, kind).await;
        drop(agents);
        drop(engine);
        kill(&root).await;
        let (engine, agents) = app(&root, false).await;
        settle(&engine).await;
        let follow = agents
            .teams()
            .unwrap()
            .harness()
            .unwrap()
            .append(
                receipt.session_id,
                HarnessTurnRequest {
                    message_id: MessageId::new(),
                    prompt: "Continue after settlement".into(),
                },
            )
            .await
            .unwrap();
        agents
            .actors()
            .unwrap()
            .pause_goal(receipt.task_id)
            .await
            .unwrap();
        let server = Server::start(agents).await;
        let client = Client::builder().no_proxy().build().unwrap();
        let base = recovery_base(&server, &receipt);
        let url = format!("{base}/runs/{run}/reconciliation");
        let page = get(&client, &format!("{base}/tool-recovery")).await;
        let recovery = &page["items"][0]["recovery"];
        assert_eq!(recovery["paused"], true);
        let body = decision(recovery, "retry");
        post(&client, &url, &body, StatusCode::OK).await;
        settle(&engine).await;
        let held = engine.status(run).await.unwrap();
        assert!(held.record().status.is_active());
        assert!(
            held.activities()
                .iter()
                .all(|activity| activity.active_attempt.is_none())
        );
        server.close().await;
        drop(engine);
        let (engine, agents) = app(&root, false).await;
        agents
            .actors()
            .unwrap()
            .resume_goal(receipt.task_id)
            .await
            .unwrap();
        drop(agents);
        drop(engine);
        kill(&root).await;
        let (engine, agents) = app(&root, false).await;
        settle(&engine).await;
        assert!(
            agents
                .actors()
                .unwrap()
                .task(follow.task_id)
                .await
                .unwrap()
                .run_id
                .is_none()
        );
        let saved = engine.status(run).await.unwrap();
        let activity = saved.attempts().last().unwrap().activity_id;
        if kind != "run_tests" {
            let journal = Path::new(&workspace)
                .join(PRIVATE)
                .join(format!("{activity}.json"));
            fs::write(&journal, "corrupt journal").unwrap();
        }
        let server = Server::start(agents).await;
        let base = recovery_base(&server, &receipt);
        let url = format!("{base}/runs/{run}/reconciliation");
        let page = get(&client, &format!("{base}/tool-recovery")).await;
        let recovery = &page["items"][0]["recovery"];
        if kind != "run_tests" {
            assert_eq!(recovery["retry_allowed"], false);
            assert!(recovery["inspection_error"].is_string());
        }
        let fail = decision(recovery, "fail");
        let mut invalid = fail.clone();
        invalid["decision"]["message"] = json!(" ");
        post(&client, &url, &invalid, StatusCode::BAD_REQUEST).await;
        post(&client, &url, &fail, StatusCode::OK).await;
        settle(&engine).await;
        assert_eq!(
            engine.status(run).await.unwrap().record().status,
            RunStatus::Failed
        );
        post(&client, &url, &fail, StatusCode::OK).await;
        server.close().await;
        let (_, agents) = app(&root, false).await;
        assert!(
            agents
                .actors()
                .unwrap()
                .task(follow.task_id)
                .await
                .unwrap()
                .run_id
                .is_some()
        );
        match kind {
            "replace_text" => assert_eq!(
                fs::read_to_string(Path::new(&workspace).join("src/a.txt")).unwrap(),
                "new"
            ),
            "create_file" => assert_eq!(
                fs::read_to_string(Path::new(&workspace).join("src/new.txt")).unwrap(),
                "created"
            ),
            _ => assert_eq!(
                fs::read_to_string(Path::new(&workspace).join(PRIVATE).join("test-count")).unwrap(),
                "2"
            ),
        }
    }
}

#[tokio::test]
async fn journal_inspection_is_read_only_and_never_infers_a_creation_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(source.join("src/a.txt"), "old").unwrap();
    let managed = dir.path().join("workspaces");
    let workspace = LocalWorkspaces::new(managed.clone())
        .prepare(source)
        .await
        .unwrap();
    let tools = Arc::new(LocalTools::new(managed));
    let definition =
        crate::adapters::native_tools::ReplaceTextTool::new(tools.clone()).definition();
    let edit = NativeToolRequest {
        definition,
        arguments: json!({"path":"src/a.txt","old":"old","new":"new"}),
        workspace: Some(workspace.clone()),
        writable_paths: vec!["src/".into()],
        test_command: None,
        project_command: None,
    };
    let activity = ActivityId::new();
    let root = Path::new(&workspace);
    let journal = root.join(PRIVATE).join(format!("{activity}.json"));
    let first = tools.inspect_tool(activity, edit.clone()).await.unwrap();
    assert_eq!(first.state, tool_recovery::ToolEffectState::NotStarted);
    assert!(!journal.exists());
    assert!(!root.join(PRIVATE).join("ownership.lock").exists());
    tools
        .replace_text(
            workspace.clone(),
            vec!["src/".into()],
            serde_json::from_value(edit.arguments.clone()).unwrap(),
            activity,
        )
        .await
        .unwrap();
    let modified = fs::metadata(root.join("src/a.txt"))
        .unwrap()
        .modified()
        .unwrap();
    let applied = tools.inspect_tool(activity, edit.clone()).await.unwrap();
    assert_eq!(
        applied.state,
        tool_recovery::ToolEffectState::AlreadyApplied
    );
    assert_eq!(
        modified,
        fs::metadata(root.join("src/a.txt"))
            .unwrap()
            .modified()
            .unwrap()
    );
    fs::write(root.join("src/a.txt"), "old").unwrap();
    assert_eq!(
        tools
            .inspect_tool(activity, edit.clone())
            .await
            .unwrap()
            .state,
        tool_recovery::ToolEffectState::Ready
    );
    fs::write(root.join("src/a.txt"), "third party").unwrap();
    assert!(
        tools
            .inspect_tool(activity, edit.clone())
            .await
            .unwrap()
            .conflict
            .is_some()
    );
    let mut changed = edit.clone();
    changed.arguments["new"] = json!("other");
    assert!(tools.inspect_tool(activity, changed).await.is_err());
    fs::remove_file(&journal).unwrap();
    assert!(
        tools
            .inspect_tool(activity, edit.clone())
            .await
            .unwrap()
            .conflict
            .is_some()
    );
    fs::write(&journal, "broken").unwrap();
    assert!(tools.inspect_tool(activity, edit.clone()).await.is_err());
    fs::remove_file(root.join("src/a.txt")).unwrap();
    assert!(tools.inspect_tool(activity, edit).await.is_err());
    let create = NativeToolRequest {
        definition: crate::adapters::file_creation::CreateFileTool::new(tools.clone()).definition(),
        arguments: json!({"path":"src/new.txt","content":"created"}),
        workspace: Some(workspace.clone()),
        writable_paths: vec!["src/".into()],
        test_command: None,
        project_command: None,
    };
    let activity = ActivityId::new();
    assert_eq!(
        tools
            .inspect_tool(activity, create.clone())
            .await
            .unwrap()
            .state,
        tool_recovery::ToolEffectState::NotStarted
    );
    fs::write(root.join("src/new.txt"), "created").unwrap();
    assert!(
        tools
            .inspect_tool(activity, create.clone())
            .await
            .unwrap()
            .conflict
            .is_some()
    );
    fs::remove_file(root.join("src/new.txt")).unwrap();
    tools
        .create_file(
            workspace.clone(),
            vec!["src/".into()],
            serde_json::from_value(create.arguments.clone()).unwrap(),
            activity,
        )
        .await
        .unwrap();
    assert_eq!(
        tools
            .inspect_tool(activity, create.clone())
            .await
            .unwrap()
            .state,
        tool_recovery::ToolEffectState::AlreadyApplied
    );
    fs::remove_file(root.join("src/new.txt")).unwrap();
    assert_eq!(
        tools
            .inspect_tool(activity, create.clone())
            .await
            .unwrap()
            .state,
        tool_recovery::ToolEffectState::Ready
    );
    let mut refused = create;
    refused.arguments["path"] = json!(".act0r-tools/managed");
    assert!(tools.inspect_tool(activity, refused).await.is_err());
    let tests = NativeToolRequest {
        definition: crate::adapters::native_tools::RunTestsTool::new(tools.clone()).definition(),
        arguments: json!({}),
        workspace: Some(workspace.clone()),
        writable_paths: vec![],
        test_command: Some(test_command()),
        project_command: None,
    };
    assert_eq!(
        tools
            .inspect_tool(ActivityId::new(), tests)
            .await
            .unwrap()
            .state,
        tool_recovery::ToolEffectState::TestOutcomeUnknown
    );
    assert!(!root.join(PRIVATE).join("test-count").exists());
}

#[tokio::test]
async fn recovery_refuses_other_activities_and_exhausted_admission_without_losing_failure() {
    let dir = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(dir.path()).unwrap();
    let (engine, agents, receipt, run, _) = fixture(&root, "run_tests").await;
    drop(agents);
    drop(engine);
    kill(&root).await;
    let (engine, agents) = app(&root, false).await;
    settle(&engine).await;
    let server = Server::start(agents).await;
    let client = Client::builder().no_proxy().build().unwrap();
    let base = recovery_base(&server, &receipt);
    let url = format!("{base}/runs/{run}/reconciliation");
    let page = get(&client, &format!("{base}/tool-recovery")).await;
    let recovery = &page["items"][0]["recovery"];
    let body = decision(recovery, "retry");
    let saved = engine.status(run).await.unwrap();
    let model = saved
        .attempts()
        .iter()
        .find(|attempt| {
            saved.activities().iter().any(|activity| {
                activity.id == attempt.activity_id && activity.spec.handler.name == "agent.model"
            })
        })
        .unwrap();
    let mut unsupported = body.clone();
    unsupported["activity_id"] = json!(model.activity_id);
    unsupported["attempt_id"] = json!(model.id);
    post(&client, &url, &unsupported, StatusCode::BAD_REQUEST).await;
    let mut unknown = body.clone();
    unknown["attempt_id"] = json!(AttemptId::new());
    post(&client, &url, &unknown, StatusCode::NOT_FOUND).await;
    post(
        &client,
        &format!("{base}/runs/{}/reconciliation", RunId::new()),
        &body,
        StatusCode::NOT_FOUND,
    )
    .await;
    engine
        .store
        .mutate(
            run,
            Box::new(|run| {
                run.record.allowance.limit = run.record.allowance.admitted;
                Ok(None)
            }),
        )
        .await
        .unwrap();
    let page = get(&client, &format!("{base}/tool-recovery")).await;
    assert_eq!(page["items"][0]["recovery"]["retry_allowed"], false);
    post(&client, &url, &body, StatusCode::BAD_REQUEST).await;
    let after = engine.status(run).await.unwrap();
    assert_eq!(after.record().status, RunStatus::Blocked);
    assert!(after.record().reconciliation_receipts.is_empty());
    assert_eq!(
        after.record().allowance.admitted,
        saved.record().allowance.admitted
    );
    post(&client, &url, &decision(recovery, "fail"), StatusCode::OK).await;
    server.close().await;
}
