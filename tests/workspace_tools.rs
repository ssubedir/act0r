use act0r::{
    Engine, Error, Worker,
    adapters::{
        agent_executor::AgentExecutor,
        agents::{AnswerAgentAdapter, PlannerAgentAdapter, ToolAgentAdapter},
        demo::DemoExecutor,
        file_lock::FileOwnership,
        model::ModelRouter,
        native_tools::{
            EchoTool, ReadFileTool, ReplaceTextTool, RunTestsTool,
            editable_workspace_tool_registry, workspace_tool_registry,
        },
        sqlite::SqliteStore,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{ActorService, AgentRegistry, AgentService, ports::*},
    domain::{actors::*, agent::*, tools::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

#[path = "workspace_tools/native_tests.rs"]
mod native_tests;
#[path = "workspace_tools/reads.rs"]
mod reads;

struct ManualClock(AtomicI64);
impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

struct FaultReader(&'static str);
impl WorkspaceReader for FaultReader {
    fn inspect(&self, _: String, _: WorkspaceReadAction, _: ActivityId) -> PortFuture<'_, Value> {
        Box::pin(async {
            Err(match self.0 {
                "rejected" => ToolError::new("permission_denied", "choose a permitted file").into(),
                "busy" => Error::Ownership("another tool owns the workspace".into()),
                "transient" => std::io::Error::from(std::io::ErrorKind::Interrupted).into(),
                _ => Error::Unavailable("invalid managed workspace marker".into()),
            })
        })
    }
}
#[tokio::test]
async fn reader_rejections_temporary_failures_and_runtime_authority_remain_distinct() {
    for mode in [
        "rejected",
        "busy",
        "transient",
        "authority",
        "scope",
        "input",
    ] {
        let tool = Arc::new(ReadFileTool::new(Arc::new(FaultReader(mode))));
        let definition = tool.definition();
        let registry = act0r::application::ToolRegistry::new(vec![tool]).unwrap();
        let Command::Schedule(spec) = definition
            .schedule_call_in(
                &json!({"path":"README.md"}),
                Some("runtime-pinned-workspace"),
            )
            .unwrap()
        else {
            panic!("expected read")
        };
        let mut execution = ActivityExecution {
            run_id: RunId::new(),
            activity_id: ActivityId::new(),
            attempt_id: AttemptId::new(),
            ordinal: 1,
            handler: spec.handler,
            input: spec.input,
        };
        if mode == "scope" {
            execution.input["workspace"] = Value::Null;
        }
        if mode == "input" {
            execution.input["arguments"]["workspace"] = json!("model-forged-scope");
        }
        let result = registry.execute(execution).await.unwrap();
        if mode == "rejected" {
            assert_eq!(
                result.result().unwrap()["error"]["code"],
                "permission_denied"
            );
        } else {
            let Outcome::Failure(failure) = result else {
                panic!("execution error became feedback")
            };
            assert_eq!(failure.retryable, matches!(mode, "busy" | "transient"));
            assert_eq!(
                failure.code,
                match mode {
                    "busy" => "workspace_busy",
                    "transient" => "workspace_io",
                    "scope" => "tool_workspace",
                    "input" => "tool_input",
                    _ => "workspace_read",
                }
            );
        }
    }
}

#[tokio::test]
async fn workspace_feedback_preserves_file_refusals_and_size_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let source = source(dir.path());
    fs::write(source.join("raw.bin"), [255, 254]).unwrap();
    fs::write(source.join("large.txt"), vec![b'x'; 1_048_577]).unwrap();
    let outside = dir.path().join("outside.txt");
    fs::write(&outside, "outside evidence").unwrap();
    let app = open(
        &dir.path().join("feedback-refusals.db"),
        Arc::new(Calls::default()),
        None,
    )
    .await;
    for (path, code) in [
        ("../outside.txt", "path_refused"),
        (".env", "path_refused"),
        (".act0r-tools/managed", "path_refused"),
        ("C:outside.txt", "path_refused"),
        ("bad\0path", "path_refused"),
        ("missing.md", "file_not_found"),
        ("src", "not_a_file"),
        ("raw.bin", "invalid_utf8"),
        ("large.txt", "file_limit"),
    ] {
        let mut cfg = feedback_config();
        cfg["script"] = json!([{"kind":"call_tool","tool":"read_file","arguments":{"path":path}},
            {"kind":"finish","summary":"Explained the saved refusal."}]);
        let actor = create(&app, "tool", Some(source.clone()), cfg, 10).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(view.status, AgentTaskStatus::Succeeded);
        let run = app.engine.status(view.run_id.unwrap()).await.unwrap();
        assert_eq!(
            run.activities()[1].result.as_ref().unwrap()["error"]["code"],
            code
        );
        assert!(!json!(run).to_string().contains("outside evidence"));
    }
    assert_eq!(fs::read_to_string(&outside).unwrap(), "outside evidence");
    for i in 0..1001 {
        fs::write(source.join(format!("file{i}.txt")), "x").unwrap();
    }
    let mut cfg = feedback_config();
    cfg["script"] = json!([{"kind":"call_tool","tool":"list_files","arguments":{}},{"kind":"finish","summary":"Listing was too large."}]);
    let actor = create(&app, "tool", Some(source), cfg, 10).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    assert_eq!(view.status, AgentTaskStatus::Succeeded);
    assert_eq!(
        app.engine
            .status(view.run_id.unwrap())
            .await
            .unwrap()
            .activities()[1]
            .result
            .as_ref()
            .unwrap()["error"]["code"],
        "listing_limit"
    );
}
#[derive(Default)]
struct Calls {
    requests: Mutex<Vec<ToolRequest>>,
    ledger: Option<PathBuf>,
}
struct Inspector {
    inner: LocalTools,
    calls: Arc<Calls>,
}
impl ToolRunner for Inspector {
    fn execute(&self, request: ToolRequest, id: ActivityId) -> PortFuture<'_, Value> {
        self.inner.execute(request, id)
    }
}

#[tokio::test]
async fn workspace_scope_validation_and_ownership_contention_precede_inspection() {
    let dir = tempfile::tempdir().unwrap();
    let source = source(dir.path());
    let managed = dir.path().join("managed");
    let workspace = LocalWorkspaces::new(managed.clone())
        .prepare(source)
        .await
        .unwrap();
    let tools = workspace_tool_registry(Arc::new(LocalTools::new(managed))).unwrap();
    let definition = tools
        .pin(&[DefinitionId::new("read_file", 2)])
        .unwrap()
        .remove(0);
    let Command::Schedule(spec) = definition
        .schedule_call_in(&json!({"path":"README.md"}), Some(&workspace))
        .unwrap()
    else {
        panic!("expected read intent")
    };
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(Path::new(&workspace).join(".act0r-tools/ownership.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    let execution = ActivityExecution {
        run_id: RunId::new(),
        activity_id: ActivityId::new(),
        attempt_id: AttemptId::new(),
        ordinal: 1,
        handler: spec.handler,
        input: spec.input,
    };
    let mut incompatible = execution.clone();
    incompatible.handler = DefinitionId::new("native.tool", 1);
    assert!(tools.execute(incompatible).await.is_err());
    let mut missing_scope = execution.clone();
    missing_scope
        .input
        .as_object_mut()
        .unwrap()
        .remove("workspace");
    assert!(
        matches!(tools.execute(missing_scope).await.unwrap(), Outcome::Failure(failure) if failure.code == "tool_workspace" && !failure.retryable)
    );
    let mut forged_argument = execution.clone();
    forged_argument.input["arguments"]["workspace"] = json!(workspace);
    assert!(
        matches!(tools.execute(forged_argument).await.unwrap(), Outcome::Failure(failure) if failure.code == "tool_input" && !failure.retryable)
    );
    let Outcome::Failure(failure) = tools.execute(execution.clone()).await.unwrap() else {
        panic!("expected busy failure")
    };
    assert_eq!(failure.code, "workspace_busy");
    assert!(failure.retryable);
    drop(lock);
    assert!(
        matches!(tools.execute(execution).await.unwrap(), Outcome::Success(result) if result["text"] == "Original project evidence.\n")
    );
}
impl WorkspaceReader for Inspector {
    fn read_files(
        &self,
        workspace: String,
        batch: WorkspaceReadBatch,
        id: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            self.calls.requests.lock().unwrap().push(ToolRequest {
                workspace: workspace.clone(),
                action: AgentAction::CallTool {
                    tool: "read_files".into(),
                    arguments: json!(batch),
                },
                test_command: vec![],
                writable_paths: vec![],
            });
            self.inner.read_files(workspace, batch, id).await
        })
    }
    fn search_text(
        &self,
        workspace: String,
        search: WorkspaceSearch,
        id: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            self.calls.requests.lock().unwrap().push(ToolRequest {
                workspace: workspace.clone(),
                action: AgentAction::CallTool {
                    tool: "search_text".into(),
                    arguments: json!(search),
                },
                test_command: vec![],
                writable_paths: vec![],
            });
            if let Some(path) = &self.calls.ledger {
                use std::io::Write;
                let mut ledger = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                writeln!(ledger, "search {id}")?;
            }
            self.inner.search_text(workspace, search, id).await
        })
    }
    fn inspect(
        &self,
        workspace: String,
        action: WorkspaceReadAction,
        id: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            let agent_action = match &action {
                WorkspaceReadAction::ListFiles => AgentAction::ListFiles,
                WorkspaceReadAction::ReadFile { path } => {
                    AgentAction::ReadFile { path: path.clone() }
                }
            };
            self.calls.requests.lock().unwrap().push(ToolRequest {
                workspace: workspace.clone(),
                action: agent_action,
                test_command: vec![],
                writable_paths: vec![],
            });
            if let Some(path) = &self.calls.ledger {
                let mut ledger = fs::read_to_string(path).unwrap_or_default();
                ledger.push_str(&format!(
                    "{} {}\n",
                    if matches!(action, WorkspaceReadAction::ReadFile { .. }) {
                        "read"
                    } else {
                        "list"
                    },
                    id
                ));
                fs::write(path, ledger)?;
            }
            self.inner.inspect(workspace, action, id).await
        })
    }
}
impl WorkspaceWriter for Inspector {
    fn create_file(
        &self,
        workspace: String,
        writable_paths: Vec<String>,
        create: WorkspaceCreate,
        id: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            self.calls.requests.lock().unwrap().push(ToolRequest {
                workspace: workspace.clone(),
                writable_paths: writable_paths.clone(),
                test_command: vec![],
                action: AgentAction::CallTool {
                    tool: "create_file".into(),
                    arguments: json!(create),
                },
            });
            if let Some(path) = &self.calls.ledger {
                use std::io::Write;
                let mut ledger = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                writeln!(ledger, "create {id}")?;
            }
            self.inner
                .create_file(workspace, writable_paths, create, id)
                .await
        })
    }
    fn replace_text(
        &self,
        workspace: String,
        writable_paths: Vec<String>,
        edit: WorkspaceEdit,
        id: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            self.calls.requests.lock().unwrap().push(ToolRequest {
                workspace: workspace.clone(),
                writable_paths: writable_paths.clone(),
                test_command: vec![],
                action: AgentAction::ReplaceText {
                    path: edit.path.clone(),
                    old: edit.old.clone(),
                    new: edit.new.clone(),
                },
            });
            if let Some(path) = &self.calls.ledger {
                let mut ledger = fs::read_to_string(path).unwrap_or_default();
                ledger.push_str(&format!("edit {id}\n"));
                fs::write(path, ledger)?;
            }
            self.inner
                .replace_text(workspace, writable_paths, edit, id)
                .await
        })
    }
}
impl WorkspaceTestRunner for Inspector {
    fn run_tests(
        &self,
        workspace: String,
        command: WorkspaceTestCommand,
        id: ActivityId,
    ) -> PortFuture<'_, Value> {
        Box::pin(async move {
            self.calls.requests.lock().unwrap().push(ToolRequest {
                workspace: workspace.clone(),
                action: AgentAction::RunTests,
                test_command: command.argv.clone(),
                writable_paths: vec![],
            });
            if let Some(path) = &self.calls.ledger {
                use std::io::Write;
                let mut ledger = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                writeln!(ledger, "tests {id}")?;
            }
            self.inner.run_tests(workspace, command, id).await
        })
    }
}
struct App {
    engine: Engine,
    agents: AgentService,
    clock: Arc<ManualClock>,
}
async fn open(db: &Path, calls: Arc<Calls>, models: Option<Arc<dyn ModelClient>>) -> App {
    let clock = Arc::new(ManualClock(AtomicI64::new(1000)));
    let store = Arc::new(SqliteStore::open(db).await.unwrap());
    let root = db.parent().unwrap().join("workspaces");
    let workspaces = Arc::new(LocalWorkspaces::new(root.clone()));
    let inspector = Arc::new(Inspector {
        inner: LocalTools::new(root),
        calls,
    });
    let tools = Arc::new(
        act0r::adapters::native_tools::agent_tool_registry(
            inspector.clone(),
            inspector.clone(),
            inspector.clone(),
            store.clone(),
        )
        .unwrap(),
    );
    let registry = Arc::new(
        AgentRegistry::new(vec![
            Arc::new(AnswerAgentAdapter),
            Arc::new(ToolAgentAdapter::new(tools.clone()).with_workspaces(workspaces.clone())),
            Arc::new(PlannerAgentAdapter::new(tools.clone()).with_workspaces(workspaces.clone())),
        ])
        .unwrap(),
    );
    let engine = Engine::new(
        store.clone(),
        Arc::new(
            AgentExecutor::new(
                models.unwrap_or_else(|| Arc::new(ModelRouter::new(None).unwrap())),
                inspector,
                Arc::new(DemoExecutor),
            )
            .with_native_tools(tools.clone()),
        ),
        clock.clone(),
        Arc::new(FileOwnership::new(db)),
        registry.workflows(),
    )
    .unwrap();
    let actors = Arc::new(
        ActorService::new(
            engine.clone(),
            store.clone(),
            vec![Arc::new(RegisteredAgentDefinition)],
        )
        .unwrap()
        .with_agent_registry(registry.clone())
        .with_inspection_reader(store.clone())
        .with_goal_control(store),
    );
    let engine = engine.with_coordinator(actors.clone());
    let agents = AgentService::new(engine.clone(), workspaces)
        .with_actors(actors)
        .with_agent_registry(registry)
        .with_tool_registry(tools);
    App {
        engine,
        agents,
        clock,
    }
}
fn source(root: &Path) -> PathBuf {
    let source = root.join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::create_dir_all(source.join("target")).unwrap();
    fs::write(source.join("README.md"), "Original project evidence.\n").unwrap();
    fs::write(
        source.join("src/lib.rs"),
        "pub fn durable() -> bool { true }\n",
    )
    .unwrap();
    fs::write(source.join(".env"), "private-source-data").unwrap();
    fs::write(source.join("target/generated.txt"), "generated").unwrap();
    source
}
fn config() -> Value {
    json!({"goal":"Inspect the project","target":{"provider":"scripted","model":"inspect","endpoint":""},
        "max_model_calls":5,"max_tokens":300000,"max_output_tokens":512,"max_input_bytes":65536,
        "tools":[{"name":"list_files","version":2},{"name":"read_file","version":2}],
        "script":[{"kind":"call_tool","tool":"list_files","arguments":{}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
            {"kind":"finish","summary":"Inspected the saved project."}]})
}
async fn assistant(app: &App) -> AgentId {
    app.agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({
        "goal":"Review supplied evidence","target":{"provider":"scripted","model":"assistant","endpoint":""},
        "max_model_calls":2,"max_tokens":100000,"max_output_tokens":512,
        "script":[{"kind":"ask_human","question":"Continue?"},{"kind":"finish","summary":"Reviewed."}]}),10,AgentQueuePolicy::default()).await.unwrap()
}
async fn create(
    app: &App,
    policy: &str,
    source: Option<PathBuf>,
    mut cfg: Value,
    attempts: u32,
) -> AgentId {
    if policy == "planner" {
        let child = assistant(app).await;
        cfg["target"]["model"] = json!("planner");
        cfg["workers"] = json!([{"name":"reviewer","agent_id":child,"description":"Review supplied file evidence"}]);
        cfg["delegation"] = json!({"max_tasks":1,"max_depth":1});
    }
    app.agents
        .create_registered_actor(
            DefinitionId::new(policy, if policy == "planner" { 3 } else { 1 }),
            source,
            cfg,
            attempts,
            AgentQueuePolicy::default(),
        )
        .await
        .unwrap()
}
async fn send(app: &App, actor: AgentId) -> MessageId {
    app.agents.actors().unwrap().send(AgentMessage { id:MessageId::new(),sender:None,recipient:actor,correlation_id:None,
        kind:"task".into(),payload:json!({"goal":"Inspect the saved project","workspace":"forged-root","writable_paths":["src/"],"tools":[{"name":"sum","version":1}]}) }).await.unwrap()
}
async fn settle(worker: &mut Worker) {
    for _ in 0..100 {
        if !worker.step().await.unwrap() && !worker.has_active_runs().await.unwrap() {
            return;
        }
    }
    panic!("worker did not settle");
}
async fn model_once(worker: &mut Worker) -> ActivityExecution {
    for _ in 0..100 {
        if !worker.advance_work().await.unwrap() {
            break;
        }
    }
    let model = worker.admit().await.unwrap().unwrap();
    assert_eq!(model.handler, DefinitionId::new("agent.model", 1));
    worker
        .complete(model.clone(), worker.execute(model.clone()).await.unwrap())
        .await
        .unwrap();
    model
}
async fn tool_once(worker: &mut Worker) -> ActivityExecution {
    let call = worker.admit().await.unwrap().unwrap();
    assert_eq!(call.handler, DefinitionId::new("native.tool", 3));
    worker
        .complete(call.clone(), worker.execute(call.clone()).await.unwrap())
        .await
        .unwrap();
    call
}

fn feedback_config() -> Value {
    let mut cfg = config();
    cfg["tools"] = json!([{"name":"list_files","version":2},{"name":"read_file","version":2}]);
    cfg["script"] = json!([{"kind":"call_tool","tool":"read_file","arguments":{"path":"missing.md"}},
        {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
        {"kind":"finish","summary":"Used saved feedback to inspect the project."}]);
    cfg
}

async fn feedback_once(worker: &mut Worker) -> ActivityExecution {
    let call = worker.admit().await.unwrap().unwrap();
    assert_eq!(call.handler, DefinitionId::new("native.tool", 3));
    worker
        .complete(call.clone(), worker.execute(call.clone()).await.unwrap())
        .await
        .unwrap();
    call
}

#[tokio::test]
async fn expected_error_and_exact_correction_request_survive_restart_for_both_policies() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("feedback.db");
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), None).await;
        let actor = create(
            &app,
            policy,
            Some(source(dir.path())),
            feedback_config(),
            12,
        )
        .await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let error = feedback_once(&mut worker).await;
        let before = app.engine.status(error.run_id).await.unwrap();
        let result = before.activities()[1].result.clone().unwrap();
        assert_eq!(result["ok"], false);
        assert_eq!(result["error"]["code"], "file_not_found");
        assert_eq!(before.activities()[1].status, ActivityStatus::Succeeded);
        assert!(before.record().error.is_none());
        let next = before.activities().last().unwrap().spec.input.clone();
        assert!(next["prompt"].as_str().unwrap().contains("file_not_found"));
        let root = error.input["workspace"].as_str().unwrap();
        assert!(!next["prompt"].as_str().unwrap().contains(root));
        fs::write(
            Path::new(root).join("missing.md"),
            "new evidence must not replace a saved error",
        )
        .unwrap();
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), None).await;
        let mut worker = app.engine.worker().await.unwrap();
        let restored = app.engine.status(error.run_id).await.unwrap();
        assert_eq!(restored.activities().last().unwrap().spec.input, next);
        assert_eq!(restored.record().input, before.record().input);
        assert_eq!(json!(restored.record().usage), json!(before.record().usage));
        assert_eq!(restored.activities()[1].result, Some(result));
        settle(&mut worker).await;
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .status,
            AgentTaskStatus::Succeeded
        );
        let run = app.engine.status(error.run_id).await.unwrap();
        assert_eq!(calls.requests.lock().unwrap().len(), 2);
        assert_eq!(
            run.activities()[3].result.as_ref().unwrap()["text"],
            "Original project evidence.\n"
        );
        assert_eq!(
            run.attempts()
                .iter()
                .filter(|a| a.activity_id == error.activity_id)
                .count(),
            1
        );
        assert_eq!(run.record().usage.admitted_model_calls, 3);
    }
}

struct FeedbackReviewModel;
impl ModelClient for FeedbackReviewModel {
    fn generate(&self, request: ModelRequest, _: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
            let action = if request.target.model == "assistant" {
                if request.turn == 0 {
                    AgentAction::AskHuman {
                        question: "Review the saved evidence?".into(),
                    }
                } else {
                    AgentAction::Finish {
                        summary: prompt["goal"].as_str().unwrap().to_owned(),
                    }
                }
            } else {
                match request.turn {
                    0 => AgentAction::CallTool {
                        tool: "read_file".into(),
                        arguments: json!({"path":"missing.md"}),
                    },
                    1 => {
                        assert_eq!(
                            prompt["observations"][0]["result"]["error"]["code"],
                            "file_not_found"
                        );
                        AgentAction::CallTool {
                            tool: "read_file".into(),
                            arguments: json!({"path":"README.md"}),
                        }
                    }
                    2 => AgentAction::DelegateTask {
                        writable_paths: None,
                        worker: "reviewer".into(),
                        goal: format!(
                            "Review {}",
                            prompt["observations"][1]["result"]["text"]
                                .as_str()
                                .unwrap()
                        ),
                    },
                    _ => {
                        assert_eq!(prompt["observations"][2]["result"]["status"], "succeeded");
                        AgentAction::Finish {
                            summary: "Recovered from a missing file and reviewed saved evidence."
                                .into(),
                        }
                    }
                }
            };
            Ok(ModelReply {
                decision: Ok(ModelDecision {
                    action,
                    request_id: None,
                    diagnostics: None,
                }),
                usage: Some(TokenUsage {
                    input_tokens: 10,
                    output_tokens: 10,
                }),
                retryable: false,
                diagnostics: None,
                retry_after: None,
            })
        })
    }
}

#[tokio::test]
async fn planner_corrects_a_read_then_delegates_saved_evidence_across_error_read_and_human_wait_restarts()
 {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("feedback-review.db");
    let calls = Arc::new(Calls::default());
    let models = Arc::new(FeedbackReviewModel);
    let app = open(&db, calls.clone(), Some(models.clone())).await;
    let mut cfg = feedback_config();
    cfg["goal_budget"] = json!({"max_model_calls":6,"max_tokens":300000});
    let actor = create(&app, "planner", Some(source(dir.path())), cfg, 12).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let error = feedback_once(&mut worker).await;
    let root = error.input["workspace"].as_str().unwrap().to_owned();
    fs::write(
        Path::new(&root).join("missing.md"),
        "must not refresh committed error",
    )
    .unwrap();
    drop(worker);
    drop(app);
    let app = open(&db, calls.clone(), Some(models.clone())).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let read = feedback_once(&mut worker).await;
    let next = app
        .engine
        .status(read.run_id)
        .await
        .unwrap()
        .activities()
        .last()
        .unwrap()
        .spec
        .input
        .clone();
    fs::write(
        Path::new(&root).join("README.md"),
        "must not refresh committed read",
    )
    .unwrap();
    drop(worker);
    drop(app);
    let app = open(&db, calls.clone(), Some(models.clone())).await;
    let mut worker = app.engine.worker().await.unwrap();
    assert_eq!(
        app.engine
            .status(read.run_id)
            .await
            .unwrap()
            .activities()
            .last()
            .unwrap()
            .spec
            .input,
        next
    );
    settle(&mut worker).await;
    let child = app
        .agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .delegation
        .unwrap();
    assert_eq!(child.goal, "Review Original project evidence.\n");
    let waiting = app.agents.actors().unwrap().task(child.id).await.unwrap();
    let wait = waiting.wait.unwrap();
    drop(worker);
    drop(app);
    let app = open(&db, calls.clone(), Some(models)).await;
    let mut worker = app.engine.worker().await.unwrap();
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .task(child.id)
            .await
            .unwrap()
            .wait
            .unwrap()
            .id,
        wait.id
    );
    app.engine
        .signal(
            waiting.run_id.unwrap(),
            wait.id,
            json!({"answer":"Continue"}),
        )
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status,
        AgentTaskStatus::Succeeded
    );
    assert_eq!(calls.requests.lock().unwrap().len(), 2);
    let inspection = app
        .agents
        .actors()
        .unwrap()
        .inspect_task(task, Default::default())
        .await
        .unwrap();
    assert_eq!(inspection.nodes.len(), 2);
    assert_eq!(inspection.summary.visible_usage.admitted_model_calls, 6);
    let run = app.engine.status(error.run_id).await.unwrap();
    for id in [error.activity_id, read.activity_id] {
        assert_eq!(
            run.attempts()
                .iter()
                .filter(|a| a.activity_id == id)
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn copied_workspace_and_saved_reads_survive_restart_without_refreshing_source_or_results() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("reads.db");
        let source = source(dir.path());
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), None).await;
        let actor = create(&app, policy, Some(source.clone()), config(), 12).await;
        let actor_before = app
            .agents
            .actors()
            .unwrap()
            .status(actor)
            .await
            .unwrap()
            .record()
            .config
            .clone();
        let root = actor_before["input"]["workspace"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_ne!(Path::new(&root), source);
        assert!(!Path::new(&root).join(".env").exists());
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        let model = model_once(&mut worker).await;
        let req: ModelRequest = serde_json::from_value(model.input).unwrap();
        assert!(!req.prompt.contains(&root));
        let listing = tool_once(&mut worker).await;
        assert_eq!(listing.input["workspace"], root);
        assert_eq!(
            app.engine
                .status(listing.run_id)
                .await
                .unwrap()
                .activities()[1]
                .result,
            Some(json!({"ok":true,"files":["README.md","src/lib.rs"]}))
        );
        fs::write(source.join("README.md"), "Changed source after creation").unwrap();
        model_once(&mut worker).await;
        let read = tool_once(&mut worker).await;
        let before = app.engine.status(read.run_id).await.unwrap();
        let next = before.activities().last().unwrap().spec.input.clone();
        let pins = before.record().input.clone();
        let usage = json!(before.record().usage);
        assert!(
            next["prompt"]
                .as_str()
                .unwrap()
                .contains("Original project evidence.")
        );
        fs::write(
            Path::new(&root).join("README.md"),
            "Changed managed file after saved read",
        )
        .unwrap();
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), None).await;
        let mut worker = app.engine.worker().await.unwrap();
        let saved = app.engine.status(read.run_id).await.unwrap();
        assert_eq!(saved.activities().last().unwrap().spec.input, next);
        assert_eq!(saved.record().input, pins);
        assert_eq!(json!(saved.record().usage), usage);
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .status(actor)
                .await
                .unwrap()
                .record()
                .config,
            actor_before
        );
        settle(&mut worker).await;
        assert_eq!(calls.requests.lock().unwrap().len(), 2);
        let run = app.engine.status(read.run_id).await.unwrap();
        assert_eq!(run.record().status, RunStatus::Succeeded);
        assert_eq!(
            run.attempts()
                .iter()
                .filter(|a| a.activity_id == read.activity_id)
                .count(),
            1
        );
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .status,
            AgentTaskStatus::Succeeded
        );
    }
}

struct ReviewModel;
impl ModelClient for ReviewModel {
    fn generate(&self, request: ModelRequest, _: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
            let action = if request.target.model == "assistant" {
                if request.turn == 0 {
                    AgentAction::AskHuman {
                        question: "Continue?".into(),
                    }
                } else {
                    AgentAction::Finish {
                        summary: prompt["goal"].as_str().unwrap().to_owned(),
                    }
                }
            } else {
                match request.turn {
                    0 => AgentAction::CallTool {
                        tool: "list_files".into(),
                        arguments: json!({}),
                    },
                    1 => AgentAction::CallTool {
                        tool: "read_file".into(),
                        arguments: json!({"path":prompt["observations"][0]["result"]["files"][0]}),
                    },
                    2 => AgentAction::DelegateTask {
                        writable_paths: None,
                        worker: "reviewer".into(),
                        goal: format!(
                            "Review {}",
                            prompt["observations"][1]["result"]["text"]
                                .as_str()
                                .unwrap()
                        ),
                    },
                    _ => {
                        assert_eq!(prompt["observations"][2]["result"]["status"], "succeeded");
                        AgentAction::Finish {
                            summary: "Reviewed saved project evidence.".into(),
                        }
                    }
                }
            };
            Ok(ModelReply {
                decision: Ok(ModelDecision {
                    action,
                    request_id: None,
                    diagnostics: None,
                }),
                usage: Some(TokenUsage {
                    input_tokens: 10,
                    output_tokens: 10,
                }),
                retryable: false,
                diagnostics: None,
                retry_after: None,
            })
        })
    }
}
#[tokio::test]
async fn planner_delegates_from_saved_file_contents_and_recovers_a_child_wait_under_one_goal_budget()
 {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("review.db");
    let source = source(dir.path());
    let calls = Arc::new(Calls::default());
    let models = Arc::new(ReviewModel);
    let app = open(&db, calls.clone(), Some(models.clone())).await;
    let mut cfg = config();
    cfg["goal_budget"] = json!({"max_model_calls":6,"max_tokens":300000});
    let actor = create(&app, "planner", Some(source), cfg, 12).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    tool_once(&mut worker).await;
    model_once(&mut worker).await;
    let read = tool_once(&mut worker).await;
    let root = read.input["workspace"].as_str().unwrap();
    fs::write(
        Path::new(root).join("README.md"),
        "Fresh content must not replace saved evidence",
    )
    .unwrap();
    drop(worker);
    drop(app);
    let app = open(&db, calls.clone(), Some(models.clone())).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let child = app
        .agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .delegation
        .unwrap();
    assert_eq!(child.goal, "Review Original project evidence.\n");
    let waiting = app.agents.actors().unwrap().task(child.id).await.unwrap();
    let wait = waiting.wait.unwrap();
    drop(worker);
    drop(app);
    let app = open(&db, calls.clone(), Some(models)).await;
    let mut worker = app.engine.worker().await.unwrap();
    let child = app.agents.actors().unwrap().task(child.id).await.unwrap();
    assert_eq!(child.wait.as_ref().unwrap().id, wait.id);
    app.engine
        .signal(child.run_id.unwrap(), wait.id, json!({"answer":"Continue"}))
        .await
        .unwrap();
    settle(&mut worker).await;
    assert_eq!(calls.requests.lock().unwrap().len(), 2);
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status,
        AgentTaskStatus::Succeeded
    );
    let tree = app
        .agents
        .actors()
        .unwrap()
        .inspect_task(task, Default::default())
        .await
        .unwrap();
    assert_eq!(tree.summary.visible_usage.admitted_model_calls, 6);
    assert_eq!(tree.nodes.len(), 2);
}

#[tokio::test]
async fn file_paths_and_private_directories_cannot_escape_the_pinned_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("paths.db");
    let source = source(dir.path());
    let outside = dir.path().join("outside.txt");
    fs::write(&outside, "outside evidence").unwrap();
    let calls = Arc::new(Calls::default());
    let app = open(&db, calls.clone(), None).await;
    for path in [
        "../outside.txt".to_owned(),
        outside.to_string_lossy().into_owned(),
        ".env".into(),
        ".act0r-tools/managed".into(),
        "target/generated.txt".into(),
        "src/../../outside.txt".into(),
        "C:outside.txt".into(),
        r"\\host\share\outside.txt".into(),
        r"\outside.txt".into(),
        r"src\lib.rs".into(),
        r"src\..\README.md".into(),
        r"src\../README.md".into(),
        r".act0r-tools\managed".into(),
        String::new(),
        "bad\0path".into(),
    ] {
        let mut cfg = config();
        cfg["script"] = json!([{"kind":"call_tool","tool":"read_file","arguments":{"path":path}},{"kind":"finish","summary":"Invalid"}]);
        let actor = create(&app, "tool", Some(source.clone()), cfg, 10).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(view.status, AgentTaskStatus::Succeeded);
        let run = app.engine.status(view.run_id.unwrap()).await.unwrap();
        let refusal = run.activities()[1].result.as_ref().unwrap();
        assert_eq!(refusal["ok"], false, "accepted {path:?}");
        assert_eq!(refusal["error"]["code"], "path_refused", "{path:?}");
        assert!(!json!(run).to_string().contains("outside evidence"));
    }
    assert_eq!(fs::read_to_string(outside).unwrap(), "outside evidence");
}

#[tokio::test]
async fn workspace_selection_is_runtime_owned_and_scope_is_required_only_for_workspace_tools() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("config.db");
    let source = source(dir.path());
    let app = open(&db, Arc::new(Calls::default()), None).await;
    for policy in ["tool", "planner"] {
        let mut cfg = config();
        if policy == "planner" {
            cfg["workers"] = json!([{"name":"reviewer","agent_id":assistant(&app).await,"description":"Reviewer"}]);
            cfg["delegation"] = json!({"max_tasks":1,"max_depth":1});
        }
        let id = DefinitionId::new(policy, if policy == "planner" { 3 } else { 1 });
        assert!(
            app.agents
                .create_registered_actor(
                    id.clone(),
                    None,
                    cfg.clone(),
                    10,
                    AgentQueuePolicy::default()
                )
                .await
                .is_err()
        );
        cfg["workspace"] = json!(source);
        assert!(
            app.agents
                .create_registered_actor(
                    id,
                    Some(source.clone()),
                    cfg,
                    10,
                    AgentQueuePolicy::default()
                )
                .await
                .is_err()
        );
    }
    let mut cfg = config();
    cfg["tools"] = json!([{"name":"echo","version":1}]);
    cfg["script"] = json!([{"kind":"call_tool","tool":"echo","arguments":{"text":"Unscoped"}},{"kind":"finish","summary":"Done"}]);
    let actor = create(&app, "tool", Some(source), cfg, 10).await;
    send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    let call = worker.admit().await.unwrap().unwrap();
    assert_eq!(call.handler, DefinitionId::new("native.tool", 1));
    assert!(call.input.get("workspace").is_none());
    assert!(call.input["definition"].get("requires_workspace").is_none());
    worker
        .complete(call.clone(), worker.execute(call).await.unwrap())
        .await
        .unwrap();
    settle(&mut worker).await;
    let definition = EchoTool.definition();
    assert!(json!(definition).get("requires_workspace").is_none());
    let old: NativeToolRequest =
        serde_json::from_value(json!({"definition":definition,"arguments":{"text":"old"}}))
            .unwrap();
    assert!(old.workspace.is_none());
    assert!(json!(old).get("workspace").is_none());
}

#[tokio::test]
async fn scoped_reads_keep_byte_utf8_file_listing_and_context_bounds_explicit() {
    for mode in ["utf8", "binary", "large", "listing", "context"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("bounds.db");
        let source = source(dir.path());
        match mode {
            "binary" => fs::write(source.join("README.md"), [255, 254]).unwrap(),
            "large" => fs::write(source.join("README.md"), vec![b'x'; 1_048_577]).unwrap(),
            "listing" => {
                for i in 0..1001 {
                    fs::write(source.join(format!("file{i:04}.txt")), "x").unwrap();
                }
            }
            _ => fs::write(source.join("README.md"), "😀".repeat(6000)).unwrap(),
        }
        let app = open(&db, Arc::new(Calls::default()), None).await;
        let mut cfg = config();
        cfg["script"] = json!([{"kind":"call_tool","tool":if mode=="listing" {"list_files"} else {"read_file"},
            "arguments":if mode=="listing" {json!({})} else {json!({"path":"README.md"})}},{"kind":"finish","summary":"Done"}]);
        if mode == "context" {
            cfg["max_input_bytes"] = json!(4096);
        }
        let actor = create(&app, "tool", Some(source), cfg, 10).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        let run = app.engine.status(view.run_id.unwrap()).await.unwrap();
        if matches!(mode, "utf8" | "context") {
            let result = run.activities()[1].result.as_ref().unwrap();
            assert_eq!(result["truncated"], true);
            assert_eq!(result["text"].as_str().unwrap().len(), 16384);
            if mode == "context" {
                assert_eq!(run.record().error.as_ref().unwrap().code, "context_limit");
            } else {
                assert_eq!(view.status, AgentTaskStatus::Succeeded);
            }
        } else {
            assert_eq!(view.status, AgentTaskStatus::Succeeded);
            let refusal = run.activities()[1].result.as_ref().unwrap();
            assert_eq!(refusal["ok"], false);
            assert_eq!(
                refusal["error"]["code"],
                match mode {
                    "binary" => "invalid_utf8",
                    "large" => "file_limit",
                    "listing" => "listing_limit",
                    _ => unreachable!(),
                }
            );
        }
    }
}

#[tokio::test]
async fn scoped_tool_outcomes_commit_while_goal_pause_or_cancellation_holds_further_work() {
    for (cancel, feedback) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("controls.db");
        let source = source(dir.path());
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), None).await;
        let mut cfg = if feedback {
            feedback_config()
        } else {
            config()
        };
        cfg["script"] = json!([
            {"kind":"call_tool","tool":"read_file","arguments":{"path":if feedback { "missing.md" } else { "README.md" }}},{"kind":"finish","summary":"Done"}]);
        let actor = create(&app, "tool", Some(source), cfg, 10).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let call = worker.admit().await.unwrap().unwrap();
        let outcome = worker.execute(call.clone()).await.unwrap();
        if cancel {
            app.agents
                .actors()
                .unwrap()
                .cancel_goal(task)
                .await
                .unwrap();
        } else {
            app.agents.actors().unwrap().pause_goal(task).await.unwrap();
        }
        worker.complete(call.clone(), outcome).await.unwrap();
        settle(&mut worker).await;
        let saved = app.engine.status(call.run_id).await.unwrap();
        assert_eq!(saved.activities()[1].status, ActivityStatus::Succeeded);
        if feedback {
            assert_eq!(
                saved.activities()[1].result.as_ref().unwrap()["error"]["code"],
                "file_not_found"
            );
        } else {
            assert_eq!(
                saved.activities()[1].result.as_ref().unwrap()["text"],
                "Original project evidence.\n"
            );
        }
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), None).await;
        let mut worker = app.engine.worker().await.unwrap();
        if !cancel {
            app.agents
                .actors()
                .unwrap()
                .resume_goal(task)
                .await
                .unwrap();
        }
        settle(&mut worker).await;
        assert_eq!(calls.requests.lock().unwrap().len(), 1);
        assert_eq!(
            app.engine
                .status(call.run_id)
                .await
                .unwrap()
                .record()
                .status,
            if cancel {
                RunStatus::Cancelled
            } else {
                RunStatus::Succeeded
            }
        );
    }
}

fn directory_link(target: &Path, link: &Path) {
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, link).unwrap();
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let output = ProcessCommand::new("cmd")
            .args(["/d", "/c", "mklink", "/J"])
            .arg(link)
            .arg(target)
            .creation_flags(0x08000000)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
#[tokio::test]
async fn linked_sources_nested_paths_and_replaced_workspace_roots_are_refused() {
    for mode in ["source", "root", "read", "list"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("links.db");
        let source = source(dir.path());
        let outside = dir.path().join("foreign");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("secret.txt"), "foreign evidence").unwrap();
        let app = open(&db, Arc::new(Calls::default()), None).await;
        if mode == "source" {
            directory_link(&outside, &source.join("linked"));
            assert!(
                app.agents
                    .create_registered_actor(
                        DefinitionId::new("tool", 1),
                        Some(source),
                        config(),
                        10,
                        AgentQueuePolicy::default()
                    )
                    .await
                    .is_err()
            );
            continue;
        }
        let mut cfg = config();
        cfg["script"] = json!([{"kind":"call_tool","tool":if mode=="list" {"list_files"}else{"read_file"},
            "arguments":if mode=="list" {json!({})}else {json!({"path":if mode=="read" {"linked/secret.txt"}else{"README.md"}})}},{"kind":"finish","summary":"Invalid"}]);
        let actor = create(&app, "tool", Some(source.clone()), cfg, 10).await;
        let state = app.agents.actors().unwrap().status(actor).await.unwrap();
        let root = PathBuf::from(
            state.record().config["input"]["workspace"]
                .as_str()
                .unwrap(),
        );
        if mode == "root" {
            let other = create(&app, "tool", Some(source), config(), 10).await;
            let state = app.agents.actors().unwrap().status(other).await.unwrap();
            let other_root = PathBuf::from(
                state.record().config["input"]["workspace"]
                    .as_str()
                    .unwrap(),
            );
            fs::write(other_root.join("README.md"), "foreign evidence").unwrap();
            fs::rename(&root, root.with_extension("original")).unwrap();
            directory_link(&other_root, &root);
        } else {
            directory_link(&outside, &root.join("linked"));
        }
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        let run = app.engine.status(view.run_id.unwrap()).await.unwrap();
        if mode == "root" {
            assert_eq!(view.status, AgentTaskStatus::Failed);
            assert_eq!(run.record().error.as_ref().unwrap().code, "workspace_read");
        } else {
            assert_eq!(view.status, AgentTaskStatus::Succeeded);
            let refusal = run.activities()[1].result.as_ref().unwrap();
            assert_eq!(refusal["ok"], false);
            assert_eq!(refusal["error"]["code"], "path_refused");
        }
        assert!(!json!(run).to_string().contains("foreign evidence"));
    }
}

#[tokio::test]
async fn attempt_and_model_caps_preserve_completed_file_work() {
    for mode in ["attempt", "model"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("budget.db");
        let source = source(dir.path());
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), None).await;
        let mut cfg = config();
        cfg["script"] = json!([
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},{"kind":"finish","summary":"Done"}]);
        if mode == "model" {
            cfg["max_model_calls"] = json!(1);
        }
        let actor = create(
            &app,
            "tool",
            Some(source),
            cfg,
            if mode == "attempt" { 1 } else { 10 },
        )
        .await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let view = app.agents.actors().unwrap().task(task).await.unwrap();
        assert_eq!(view.status, AgentTaskStatus::BudgetExhausted);
        assert_eq!(
            calls.requests.lock().unwrap().len(),
            usize::from(mode == "model")
        );
        if mode == "model" {
            assert_eq!(
                app.engine
                    .status(view.run_id.unwrap())
                    .await
                    .unwrap()
                    .activities()[1]
                    .status,
                ActivityStatus::Succeeded
            );
        }
    }
}

#[tokio::test]
#[ignore = "child process fixture invoked by the workspace recovery test"]
async fn workspace_crash_fixture() {
    let Ok(root) = std::env::var("ACT0R_WORKSPACE_CRASH_DIR") else {
        return;
    };
    let root = PathBuf::from(root);
    let stage = std::env::var("ACT0R_WORKSPACE_CRASH_STAGE").unwrap();
    let source = source(&root);
    let calls = Arc::new(Calls {
        requests: Mutex::new(vec![]),
        ledger: Some(root.join("reads.log")),
    });
    let feedback = stage.starts_with("feedback_");
    let models: Arc<dyn ModelClient> = if feedback {
        Arc::new(FeedbackReviewModel)
    } else {
        Arc::new(ReviewModel)
    };
    let mut cfg = if feedback {
        feedback_config()
    } else {
        config()
    };
    if feedback {
        cfg["goal_budget"] = json!({"max_model_calls":6,"max_tokens":300000});
    }
    let app = open(&root.join("crash.db"), calls, Some(models)).await;
    let actor = create(&app, "planner", Some(source), cfg, 12).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    if stage == "feedback_admitted" {
        let call = worker.admit().await.unwrap().unwrap();
        assert_eq!(call.handler, DefinitionId::new("native.tool", 3));
        assert_eq!(
            worker.execute(call).await.unwrap().result().unwrap()["ok"],
            false
        );
    } else if feedback && stage != "feedback_intent" {
        feedback_once(&mut worker).await;
        if matches!(stage.as_str(), "feedback_read_saved" | "feedback_waiting") {
            model_once(&mut worker).await;
            feedback_once(&mut worker).await;
        }
        if stage == "feedback_waiting" {
            settle(&mut worker).await;
        }
    } else if !feedback && stage != "intent" {
        tool_once(&mut worker).await;
    }
    if matches!(stage.as_str(), "read_saved" | "waiting") {
        model_once(&mut worker).await;
        tool_once(&mut worker).await;
    }
    if stage == "waiting" {
        settle(&mut worker).await;
    }
    fs::write(
        root.join("ready.json"),
        serde_json::to_vec(&json!({"task":task,"actor":actor})).unwrap(),
    )
    .unwrap();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn kill_workspace_fixture(root: &Path, stage: &str) -> Value {
    let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
    command
        .args(["--ignored", "--exact", "workspace_crash_fixture"])
        .env("ACT0R_WORKSPACE_CRASH_DIR", root)
        .env("ACT0R_WORKSPACE_CRASH_STAGE", stage)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !root.join("ready.json").exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "fixture exited: {stage}"
        );
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("fixture timed out: {stage}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    serde_json::from_slice(&fs::read(root.join("ready.json")).unwrap()).unwrap()
}

#[tokio::test]
async fn killed_workers_recover_error_intent_uncommitted_feedback_saved_correction_and_delegated_wait()
 {
    for stage in [
        "feedback_intent",
        "feedback_admitted",
        "feedback_saved",
        "feedback_read_saved",
        "feedback_waiting",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let ready = kill_workspace_fixture(root, stage).await;
        let task = serde_json::from_value(ready["task"].clone()).unwrap();
        let calls = Arc::new(Calls {
            requests: Mutex::new(vec![]),
            ledger: Some(root.join("reads.log")),
        });
        let app = open(
            &root.join("crash.db"),
            calls,
            Some(Arc::new(FeedbackReviewModel)),
        )
        .await;
        let parent = app.agents.actors().unwrap().task(task).await.unwrap();
        let run = app.engine.status(parent.run_id.unwrap()).await.unwrap();
        let workspace = Path::new(run.record().input["input"]["workspace"].as_str().unwrap());
        if matches!(
            stage,
            "feedback_saved" | "feedback_read_saved" | "feedback_waiting"
        ) {
            fs::write(
                workspace.join("missing.md"),
                "must not replace a saved error",
            )
            .unwrap();
        }
        if matches!(stage, "feedback_read_saved" | "feedback_waiting") {
            fs::write(workspace.join("README.md"), "must not replace a saved read").unwrap();
        }
        let mut worker = app.engine.worker().await.unwrap();
        if stage == "feedback_admitted" {
            assert!(worker.admit().await.unwrap().is_none());
            app.clock.0.store(1100, Ordering::SeqCst);
        }
        settle(&mut worker).await;
        let child = app
            .agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .delegation
            .unwrap();
        assert_eq!(child.goal, "Review Original project evidence.\n");
        let waiting = app.agents.actors().unwrap().task(child.id).await.unwrap();
        app.engine
            .signal(
                waiting.run_id.unwrap(),
                waiting.wait.unwrap().id,
                json!({"answer":"Continue"}),
            )
            .await
            .unwrap();
        settle(&mut worker).await;
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .status,
            AgentTaskStatus::Succeeded
        );
        let ledger = fs::read_to_string(root.join("reads.log")).unwrap();
        let lines: Vec<_> = ledger.lines().collect();
        assert_eq!(
            lines.len(),
            if stage == "feedback_admitted" { 3 } else { 2 }
        );
        if stage == "feedback_admitted" {
            assert_eq!(lines[0], lines[1]);
        }
        let inspection = app
            .agents
            .actors()
            .unwrap()
            .inspect_task(task, Default::default())
            .await
            .unwrap();
        assert_eq!(inspection.summary.visible_usage.admitted_model_calls, 6);
        assert_eq!(
            app.engine
                .status(parent.run_id.unwrap())
                .await
                .unwrap()
                .activities()[1]
                .result
                .as_ref()
                .unwrap()["error"]["code"],
            "file_not_found"
        );
    }
}

#[tokio::test]
async fn killed_workers_preserve_scoped_intents_file_results_and_delegated_waits() {
    for stage in ["intent", "list_saved", "read_saved", "waiting"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "workspace_crash_fixture"])
            .env("ACT0R_WORKSPACE_CRASH_DIR", root)
            .env("ACT0R_WORKSPACE_CRASH_STAGE", stage)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !root.join("ready.json").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "fixture exited: {stage}"
            );
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("fixture timed out: {stage}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let ready: Value =
            serde_json::from_slice(&fs::read(root.join("ready.json")).unwrap()).unwrap();
        let task = serde_json::from_value(ready["task"].clone()).unwrap();
        let calls = Arc::new(Calls {
            requests: Mutex::new(vec![]),
            ledger: Some(root.join("reads.log")),
        });
        let app = open(&root.join("crash.db"), calls, Some(Arc::new(ReviewModel))).await;
        let parent = app.agents.actors().unwrap().task(task).await.unwrap();
        if matches!(stage, "read_saved" | "waiting") {
            let run = app.engine.status(parent.run_id.unwrap()).await.unwrap();
            fs::write(
                Path::new(run.record().input["input"]["workspace"].as_str().unwrap())
                    .join("README.md"),
                "New content after process kill",
            )
            .unwrap();
        }
        let mut worker = app.engine.worker().await.unwrap();
        settle(&mut worker).await;
        let child = app
            .agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .delegation
            .unwrap();
        assert_eq!(child.goal, "Review Original project evidence.\n");
        let waiting = app.agents.actors().unwrap().task(child.id).await.unwrap();
        app.engine
            .signal(
                waiting.run_id.unwrap(),
                waiting.wait.unwrap().id,
                json!({"answer":"Continue"}),
            )
            .await
            .unwrap();
        settle(&mut worker).await;
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .status,
            AgentTaskStatus::Succeeded
        );
        let ledger = fs::read_to_string(root.join("reads.log")).unwrap();
        assert_eq!(
            ledger.lines().filter(|l| l.starts_with("read ")).count(),
            1,
            "read repeated: {stage}"
        );
        assert_eq!(
            ledger.lines().filter(|l| l.starts_with("list ")).count(),
            1,
            "listing repeated: {stage}"
        );
    }
}

fn edit_config() -> Value {
    let mut cfg = feedback_config();
    cfg["tools"] = json!([{"name":"read_file","version":2},{"name":"replace_text","version":1}]);
    cfg["writable_paths"] = json!(["README.md"]);
    cfg["script"] = json!([
        {"kind":"call_tool","tool":"replace_text","arguments":{"path":"README.md","old":"Original project evidence.","new":"Updated project evidence."}},
        {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}},
        {"kind":"finish","summary":"Updated and inspected the project."}]);
    cfg
}
async fn approval(app: &App, task: MessageId) -> (RunId, HumanWait) {
    let view = app.agents.actors().unwrap().task(task).await.unwrap();
    (view.run_id.unwrap(), view.wait.unwrap())
}
async fn approve_edit(app: &App, task: MessageId, approved: bool) {
    let (run, wait) = approval(app, task).await;
    assert_eq!(wait.key, "native_tool_approval");
    assert_eq!(wait.payload["writable_paths"], json!(["README.md"]));
    app.engine
        .signal(run, wait.id, json!({"approved":approved}))
        .await
        .unwrap();
}
async fn native_once(worker: &mut Worker) -> (ActivityExecution, Outcome) {
    let call = worker.admit().await.unwrap().unwrap();
    assert!(is_native_tool_handler(&call.handler));
    let outcome = worker.execute(call.clone()).await.unwrap();
    worker
        .complete(call.clone(), outcome.clone())
        .await
        .unwrap();
    (call, outcome)
}

#[tokio::test]
async fn writable_permissions_are_bounded_pinned_and_absent_from_inspection_intents() {
    let dir = tempfile::tempdir().unwrap();
    let app = open(
        &dir.path().join("permission.db"),
        Arc::new(Calls::default()),
        None,
    )
    .await;
    let src = source(dir.path());
    for paths in [
        json!([]),
        json!(["../README.md"]),
        json!(["/README.md"]),
        json!(["C:/README.md"]),
        json!(["src\\lib.rs"]),
        json!(["src//lib.rs"]),
        json!(["src/./lib.rs"]),
        json!(["src/*"]),
        json!(["README.md", "README.md"]),
        json!(["src./"]),
        json!(["x".repeat(4097)]),
        json!(vec!["a"; 129]),
        json!(
            (0..5)
                .map(|i| format!("{i}{}", "a".repeat(4000)))
                .collect::<Vec<_>>()
        ),
    ] {
        let mut cfg = edit_config();
        cfg["writable_paths"] = paths.clone();
        assert!(
            app.agents
                .create_registered_actor(
                    DefinitionId::new("tool", 1),
                    Some(src.clone()),
                    cfg,
                    10,
                    Default::default()
                )
                .await
                .is_err(),
            "accepted {paths}"
        );
    }
    let mut cfg = config();
    cfg["writable_paths"] = json!(["README.md"]);
    assert!(
        app.agents
            .create_registered_actor(
                DefinitionId::new("tool", 1),
                Some(src.clone()),
                cfg,
                10,
                Default::default()
            )
            .await
            .is_err()
    );
    assert!(
        !dir.path().join("workspaces").exists(),
        "invalid configuration allocated a copy"
    );
    let actor = create(&app, "tool", Some(src), edit_config(), 10).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    let model = model_once(&mut worker).await;
    let prompt: Value = serde_json::from_str(model.input["prompt"].as_str().unwrap()).unwrap();
    assert_eq!(prompt["writable_paths"], json!(["README.md"]));
    approve_edit(&app, task, true).await;
    let (write, _) = native_once(&mut worker).await;
    assert_eq!(write.handler, DefinitionId::new("native.tool", 4));
    assert_eq!(write.input["writable_paths"], json!(["README.md"]));
    assert!(write.input["arguments"].get("writable_paths").is_none());
    model_once(&mut worker).await;
    let (read, _) = native_once(&mut worker).await;
    assert!(read.input.get("writable_paths").is_none());
    let old: ToolDefinition = serde_json::from_value(json!(EchoTool.definition())).unwrap();
    assert!(!old.requires_writable_paths);
    assert!(json!(old).get("requires_writable_paths").is_none());
}

#[tokio::test]
async fn native_edits_refuse_expected_problems_without_journaling_or_widening_permissions() {
    let dir = tempfile::tempdir().unwrap();
    let src = source(dir.path());
    fs::write(src.join("ambiguous.txt"), "old old").unwrap();
    fs::write(src.join("raw.bin"), [255]).unwrap();
    fs::write(src.join("limit.txt"), vec![b'x'; 1_048_576]).unwrap();
    let root = dir.path().join("workspaces");
    let workspace = LocalWorkspaces::new(root.clone())
        .prepare(src)
        .await
        .unwrap();
    let registry = editable_workspace_tool_registry(
        Arc::new(LocalTools::new(root.clone())),
        Arc::new(LocalTools::new(root)),
    )
    .unwrap();
    let definition = registry
        .definition(&DefinitionId::new("replace_text", 1))
        .unwrap();
    for (path, old, new, code) in [
        ("../outside.txt", "old", "new", "path_refused"),
        ("src\\lib.rs", "old", "new", "path_refused"),
        (".act0r-tools/managed", "old", "new", "path_refused"),
        (".env", "old", "new", "path_refused"),
        ("src/lib.rs", "old", "new", "path_not_writable"),
        ("missing.md", "old", "new", "file_not_found"),
        ("README.md", "", "new", "empty_old_text"),
        ("README.md", "missing", "new", "text_not_found"),
        ("ambiguous.txt", "old", "new", "text_not_unique"),
        ("raw.bin", "old", "new", "invalid_utf8"),
        ("src", "old", "new", "not_a_file"),
        ("limit.txt", "x", "new", "text_not_unique"),
    ] {
        let Command::Schedule(spec) = definition
            .schedule_call_with_permissions(
                &json!({"path":path,"old":old,"new":new}),
                Some(&workspace),
                &[
                    "README.md".into(),
                    "ambiguous.txt".into(),
                    "raw.bin".into(),
                    "missing.md".into(),
                    "limit.txt".into(),
                ],
            )
            .unwrap()
        else {
            panic!()
        };
        let id = ActivityId::new();
        let execution = ActivityExecution {
            run_id: RunId::new(),
            activity_id: id,
            attempt_id: AttemptId::new(),
            ordinal: 1,
            handler: spec.handler,
            input: spec.input,
        };
        let outcome = registry.execute(execution).await.unwrap();
        assert_eq!(outcome.result().unwrap()["error"]["code"], code, "{path}");
        assert!(
            !Path::new(&workspace)
                .join(format!(".act0r-tools/{id}.json"))
                .exists()
        );
    }
    assert_eq!(
        fs::read_to_string(Path::new(&workspace).join("README.md")).unwrap(),
        "Original project evidence.\n"
    );
    // A directory prefix permits this file without exposing the model to permission arguments.
    let tool = ReplaceTextTool::new(Arc::new(LocalTools::new(dir.path().join("workspaces"))));
    let Command::Schedule(spec) = tool
        .definition()
        .schedule_call_with_permissions(
            &json!({"path":"src/lib.rs","old":"true","new":"false"}),
            Some(&workspace),
            &["src/".into()],
        )
        .unwrap()
    else {
        panic!()
    };
    let mut execution = ActivityExecution {
        run_id: RunId::new(),
        activity_id: ActivityId::new(),
        attempt_id: AttemptId::new(),
        ordinal: 1,
        handler: spec.handler,
        input: spec.input,
    };
    for mode in ["scope", "permissions", "input", "handler"] {
        let mut invalid = execution.clone();
        match mode {
            "scope" => invalid.input["workspace"] = Value::Null,
            "permissions" => invalid.input["writable_paths"] = json!([]),
            "input" => invalid.input["arguments"]["writable_paths"] = json!(["src/"]),
            _ => invalid.handler = DefinitionId::new("native.tool", 3),
        }
        if mode == "handler" {
            assert!(registry.execute(invalid).await.is_err());
        } else {
            assert!(matches!(
                registry.execute(invalid).await.unwrap(),
                Outcome::Failure(_)
            ));
        }
    }
    assert_eq!(
        registry
            .execute(execution.clone())
            .await
            .unwrap()
            .result()
            .unwrap()["already_applied"],
        false
    );
    execution.attempt_id = AttemptId::new();
    execution.ordinal = 2;
    assert_eq!(
        registry
            .execute(execution.clone())
            .await
            .unwrap()
            .result()
            .unwrap()["already_applied"],
        true
    );
    fs::write(Path::new(&workspace).join("src/lib.rs"), "third-party edit").unwrap();
    assert!(
        registry.execute(execution.clone()).await.is_err(),
        "journal conflict became feedback"
    );
    assert_eq!(
        fs::read_to_string(Path::new(&workspace).join("src/lib.rs")).unwrap(),
        "third-party edit"
    );
    fs::remove_file(Path::new(&workspace).join("src/lib.rs")).unwrap();
    assert!(
        registry.execute(execution).await.is_err(),
        "missing journaled target became feedback"
    );
}

#[tokio::test]
async fn edit_approval_and_completed_result_survive_restart_for_both_native_policies() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("approved.db");
        let src = source(dir.path());
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), None).await;
        let actor = create(&app, policy, Some(src.clone()), edit_config(), 12).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let (run, wait) = approval(&app, task).await;
        assert!(worker.admit().await.unwrap().is_none());
        assert!(calls.requests.lock().unwrap().is_empty());
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), None).await;
        let mut worker = app.engine.worker().await.unwrap();
        assert_eq!(approval(&app, task).await.1.id, wait.id);
        approve_edit(&app, task, true).await;
        app.engine
            .signal(run, wait.id, json!({"approved":true}))
            .await
            .unwrap();
        let (edit, outcome) = native_once(&mut worker).await;
        assert_eq!(outcome.result().unwrap()["already_applied"], false);
        worker.complete(edit.clone(), outcome).await.unwrap();
        let before = app.engine.status(run).await.unwrap();
        let next = before.activities().last().unwrap().spec.input.clone();
        let usage = json!(before.record().usage);
        let managed = PathBuf::from(edit.input["workspace"].as_str().unwrap());
        assert_eq!(
            fs::read_to_string(managed.join("README.md")).unwrap(),
            "Updated project evidence.\n"
        );
        assert_eq!(
            fs::read_to_string(src.join("README.md")).unwrap(),
            "Original project evidence.\n"
        );
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), None).await;
        let mut worker = app.engine.worker().await.unwrap();
        let saved = app.engine.status(run).await.unwrap();
        assert_eq!(saved.activities().last().unwrap().spec.input, next);
        assert_eq!(json!(saved.record().usage), usage);
        settle(&mut worker).await;
        let saved = app.engine.status(run).await.unwrap();
        assert_eq!(saved.record().status, RunStatus::Succeeded);
        assert_eq!(
            saved.activities()[3].result.as_ref().unwrap()["text"],
            "Updated project evidence.\n"
        );
        assert_eq!(calls.requests.lock().unwrap().len(), 2);
        assert_eq!(
            saved
                .attempts()
                .iter()
                .filter(|a| a.activity_id == edit.activity_id)
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn interrupted_edits_require_reconciliation_and_authorized_retries_use_the_saved_journal() {
    for conflict in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("uncertain.db");
        let src = source(dir.path());
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), None).await;
        let actor = create(&app, "tool", Some(src), edit_config(), 12).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        approve_edit(&app, task, true).await;
        let edit = worker.admit().await.unwrap().unwrap();
        let outcome = worker.execute(edit.clone()).await.unwrap();
        assert_eq!(outcome.result().unwrap()["already_applied"], false);
        let path = Path::new(edit.input["workspace"].as_str().unwrap()).join("README.md");
        if conflict {
            fs::write(&path, "external change after uncertain edit").unwrap();
        }
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), None).await;
        let mut worker = app.engine.worker().await.unwrap();
        let blocked = app.engine.status(edit.run_id).await.unwrap();
        assert_eq!(blocked.record().status, RunStatus::Blocked);
        assert_eq!(
            blocked.activities()[1].status,
            ActivityStatus::Indeterminate
        );
        assert!(worker.admit().await.unwrap().is_none());
        assert_eq!(calls.requests.lock().unwrap().len(), 1);
        let request = ReconciliationRequest { activity_id:edit.activity_id, attempt_id:edit.attempt_id,
            decision:ReconciliationDecision::Retry {}, evidence:"The stopped worker released ownership; authorize journal verification of this edit.".into() };
        app.engine
            .reconcile(edit.run_id, request.clone())
            .await
            .unwrap();
        app.engine.reconcile(edit.run_id, request).await.unwrap();
        let retry = worker.admit().await.unwrap().unwrap();
        assert_eq!(retry.activity_id, edit.activity_id);
        assert_ne!(retry.attempt_id, edit.attempt_id);
        assert_eq!(retry.input, edit.input);
        if conflict {
            assert!(worker.execute(retry.clone()).await.is_err());
            worker
                .complete(
                    retry,
                    Outcome::Uncertain("journal verification found conflicting content".into()),
                )
                .await
                .unwrap();
            app.agents
                .actors()
                .unwrap()
                .cancel_goal(task)
                .await
                .unwrap();
            settle(&mut worker).await;
            let blocked = app.engine.status(edit.run_id).await.unwrap();
            assert_eq!(blocked.record().status, RunStatus::Blocked);
            assert_eq!(
                blocked.activities()[1].status,
                ActivityStatus::Indeterminate
            );
            assert_eq!(
                fs::read_to_string(path).unwrap(),
                "external change after uncertain edit"
            );
            assert!(
                !blocked
                    .activities()
                    .iter()
                    .any(|a| a.result.as_ref().is_some_and(|r| r["ok"] == false))
            );
        } else {
            let outcome = worker.execute(retry.clone()).await.unwrap();
            assert_eq!(outcome.result().unwrap()["already_applied"], true);
            worker.complete(retry, outcome).await.unwrap();
            settle(&mut worker).await;
            assert_eq!(
                app.engine
                    .status(edit.run_id)
                    .await
                    .unwrap()
                    .record()
                    .status,
                RunStatus::Succeeded
            );
            assert_eq!(
                fs::read_to_string(path).unwrap(),
                "Updated project evidence.\n"
            );
        }
    }
}

#[tokio::test]
async fn edit_denial_and_goal_controls_preserve_files_and_durable_admission() {
    for policy in ["tool", "planner"] {
        for mode in [
            "deny",
            "pause",
            "cancel",
            "inflight_pause",
            "inflight_cancel",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("controls.db");
            let calls = Arc::new(Calls::default());
            let app = open(&db, calls.clone(), None).await;
            let actor = create(&app, policy, Some(source(dir.path())), edit_config(), 12).await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            model_once(&mut worker).await;
            let (run, wait) = approval(&app, task).await;
            let root = PathBuf::from(wait.payload["workspace"].as_str().unwrap());
            let inflight = if mode.starts_with("inflight") {
                approve_edit(&app, task, true).await;
                let execution = worker.admit().await.unwrap().unwrap();
                let outcome = worker.execute(execution.clone()).await.unwrap();
                Some((execution, outcome))
            } else {
                None
            };
            if mode.contains("pause") {
                app.agents.actors().unwrap().pause_goal(task).await.unwrap();
            }
            if mode.contains("cancel") {
                app.agents
                    .actors()
                    .unwrap()
                    .cancel_goal(task)
                    .await
                    .unwrap();
            }
            if let Some((execution, outcome)) = inflight {
                worker.complete(execution, outcome).await.unwrap();
            } else if !mode.contains("cancel") {
                approve_edit(&app, task, mode != "deny").await;
            }
            settle(&mut worker).await;
            if mode.contains("pause") {
                assert!(worker.admit().await.unwrap().is_none());
                assert_eq!(
                    calls.requests.lock().unwrap().len(),
                    usize::from(mode.starts_with("inflight"))
                );
            }
            drop(worker);
            drop(app);
            let app = open(&db, calls.clone(), None).await;
            let mut worker = app.engine.worker().await.unwrap();
            if mode.contains("pause") {
                app.agents
                    .actors()
                    .unwrap()
                    .resume_goal(task)
                    .await
                    .unwrap();
            }
            settle(&mut worker).await;
            assert_eq!(
                app.engine.status(run).await.unwrap().record().status,
                if mode.contains("cancel") {
                    RunStatus::Cancelled
                } else {
                    RunStatus::Succeeded
                }
            );
            let changed = mode.contains("pause") || mode.starts_with("inflight");
            assert_eq!(
                fs::read_to_string(root.join("README.md")).unwrap(),
                if changed {
                    "Updated project evidence.\n"
                } else {
                    "Original project evidence.\n"
                }
            );
            assert_eq!(
                calls
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|r| matches!(r.action, AgentAction::ReplaceText { .. }))
                    .count(),
                usize::from(changed)
            );
        }
    }
}

#[tokio::test]
async fn edit_results_survive_model_attempt_and_context_limits() {
    for policy in ["tool", "planner"] {
        for mode in ["attempt", "model", "context"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("limits.db");
            let calls = Arc::new(Calls::default());
            let app = open(&db, calls.clone(), None).await;
            let src = source(dir.path());
            let mut cfg = edit_config();
            if mode == "model" {
                cfg["max_model_calls"] = json!(1);
            }
            if mode == "context" {
                let baseline = create(&app, policy, Some(src.clone()), cfg.clone(), 12).await;
                send(&app, baseline).await;
                let mut worker = app.engine.worker().await.unwrap();
                let execution = model_once(&mut worker).await;
                let request: ModelRequest = serde_json::from_value(execution.input).unwrap();
                cfg["max_input_bytes"] = json!(request.context_input_bytes());
                drop(worker);
            }
            let actor = create(
                &app,
                policy,
                Some(src),
                cfg,
                if mode == "attempt" { 1 } else { 12 },
            )
            .await;
            let task = send(&app, actor).await;
            let mut worker = app.engine.worker().await.unwrap();
            model_once(&mut worker).await;
            approve_edit(&app, task, true).await;
            settle(&mut worker).await;
            let run = app
                .engine
                .status(
                    app.agents
                        .actors()
                        .unwrap()
                        .task(task)
                        .await
                        .unwrap()
                        .run_id
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                run.record().status,
                if mode == "context" {
                    RunStatus::Failed
                } else {
                    RunStatus::BudgetExhausted
                }
            );
            assert_eq!(
                calls.requests.lock().unwrap().len(),
                usize::from(mode != "attempt")
            );
            if mode != "attempt" {
                assert_eq!(run.activities()[1].status, ActivityStatus::Succeeded);
                assert_eq!(run.activities()[1].result.as_ref().unwrap()["ok"], true);
            }
            if mode == "context" {
                assert_eq!(run.record().error.as_ref().unwrap().code, "context_limit");
            }
        }
    }
}

#[tokio::test]
async fn journaled_edit_completion_and_next_request_commit_atomically_without_repeating_the_write()
{
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");
    let calls = Arc::new(Calls::default());
    let app = open(&db, calls.clone(), None).await;
    let actor = create(&app, "tool", Some(source(dir.path())), edit_config(), 12).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    approve_edit(&app, task, true).await;
    let execution = worker.admit().await.unwrap().unwrap();
    let outcome = worker.execute(execution.clone()).await.unwrap();
    let before = json!(app.engine.status(execution.run_id).await.unwrap());
    let history = json!(app.engine.history(execution.run_id).await.unwrap());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_edit_continuation BEFORE INSERT ON activities WHEN json_extract(NEW.payload,'$.spec.handler.name')='agent.model' BEGIN SELECT RAISE(ABORT,'test edit result rollback'); END").execute(&pool).await.unwrap();
    assert!(
        worker
            .complete(execution.clone(), outcome.clone())
            .await
            .is_err()
    );
    assert_eq!(
        json!(app.engine.status(execution.run_id).await.unwrap()),
        before
    );
    assert_eq!(
        json!(app.engine.history(execution.run_id).await.unwrap()),
        history
    );
    assert_eq!(
        fs::read_to_string(
            Path::new(execution.input["workspace"].as_str().unwrap()).join("README.md")
        )
        .unwrap(),
        "Updated project evidence.\n"
    );
    sqlx::query("DROP TRIGGER reject_edit_continuation")
        .execute(&pool)
        .await
        .unwrap();
    worker
        .complete(execution.clone(), outcome.clone())
        .await
        .unwrap();
    worker.complete(execution.clone(), outcome).await.unwrap();
    settle(&mut worker).await;
    assert_eq!(
        calls
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| matches!(r.action, AgentAction::ReplaceText { .. }))
            .count(),
        1
    );
    let run = app.engine.status(execution.run_id).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert_eq!(run.activities().len(), 5);
}
#[tokio::test]
async fn expected_edit_feedback_survives_restart_and_a_corrected_edit_needs_its_own_approval() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("correction.db");
        let calls = Arc::new(Calls::default());
        let app = open(&db, calls.clone(), None).await;
        let mut cfg = edit_config();
        cfg["script"] = json!([
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"README.md","old":"wrong old text","new":"Updated project evidence."}},
            {"kind":"call_tool","tool":"replace_text","arguments":{"path":"README.md","old":"Original project evidence.","new":"Updated project evidence."}},
            {"kind":"call_tool","tool":"read_file","arguments":{"path":"README.md"}}, {"kind":"finish","summary":"Corrected the edit"}]);
        let actor = create(&app, policy, Some(source(dir.path())), cfg, 12).await;
        let task = send(&app, actor).await;
        let mut worker = app.engine.worker().await.unwrap();
        model_once(&mut worker).await;
        let (_, first_wait) = approval(&app, task).await;
        approve_edit(&app, task, true).await;
        let (error, result) = native_once(&mut worker).await;
        assert_eq!(result.result().unwrap()["error"]["code"], "text_not_found");
        let next = app
            .engine
            .status(error.run_id)
            .await
            .unwrap()
            .activities()
            .last()
            .unwrap()
            .spec
            .input
            .clone();
        drop(worker);
        drop(app);
        let app = open(&db, calls.clone(), None).await;
        let mut worker = app.engine.worker().await.unwrap();
        assert_eq!(
            app.engine
                .status(error.run_id)
                .await
                .unwrap()
                .activities()
                .last()
                .unwrap()
                .spec
                .input,
            next
        );
        model_once(&mut worker).await;
        let (_, second_wait) = approval(&app, task).await;
        assert_ne!(first_wait.id, second_wait.id);
        assert!(worker.admit().await.unwrap().is_none());
        assert_eq!(
            fs::read_to_string(
                Path::new(second_wait.payload["workspace"].as_str().unwrap()).join("README.md")
            )
            .unwrap(),
            "Original project evidence.\n"
        );
        approve_edit(&app, task, true).await;
        settle(&mut worker).await;
        let saved = app.engine.status(error.run_id).await.unwrap();
        assert_eq!(saved.record().status, RunStatus::Succeeded);
        assert_eq!(saved.activities()[1].result, result.result().cloned());
        assert_eq!(saved.record().usage.admitted_model_calls, 4);
        assert_eq!(calls.requests.lock().unwrap().len(), 3);
    }
}

#[tokio::test]
async fn native_edit_bounds_locks_and_tampered_journals_keep_execution_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let src = source(dir.path());
    let root = dir.path().join("workspaces");
    let workspace = LocalWorkspaces::new(root.clone())
        .prepare(src)
        .await
        .unwrap();
    let tools = Arc::new(LocalTools::new(root));
    let registry = editable_workspace_tool_registry(tools.clone(), tools).unwrap();
    let definition = registry
        .definition(&DefinitionId::new("replace_text", 1))
        .unwrap();
    assert!(
        definition
            .schedule_call_with_permissions(
                &json!({"path":"README.md","old":"Original","new":"x".repeat(65_536)}),
                Some(&workspace),
                &["README.md".into()]
            )
            .is_err()
    );
    let Command::Schedule(spec) = definition
        .schedule_call_with_permissions(
            &json!({"path":"README.md","old":"Original","new":"Updated"}),
            Some(&workspace),
            &["README.md".into()],
        )
        .unwrap()
    else {
        panic!()
    };
    let mut execution = ActivityExecution {
        run_id: RunId::new(),
        activity_id: ActivityId::new(),
        attempt_id: AttemptId::new(),
        ordinal: 1,
        handler: spec.handler,
        input: spec.input,
    };
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(Path::new(&workspace).join(".act0r-tools/ownership.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    assert!(
        matches!(registry.execute(execution.clone()).await.unwrap(),Outcome::Failure(failure) if failure.code=="workspace_busy" && failure.retryable)
    );
    drop(lock);
    registry.execute(execution.clone()).await.unwrap();
    execution.input["arguments"]["new"] = json!("Reused with another intent");
    assert!(registry.execute(execution.clone()).await.is_err());
    execution.input["arguments"]["new"] = json!("Updated");
    execution.input["writable_paths"] = json!(["README.md", "src/"]);
    assert!(registry.execute(execution.clone()).await.is_err());
    execution.input["writable_paths"] = json!(["README.md"]);
    let journal =
        Path::new(&workspace).join(format!(".act0r-tools/{}.json", execution.activity_id));
    let original = fs::read(&journal).unwrap();
    let mut value: Value = serde_json::from_slice(&original).unwrap();
    value["content"] = json!("tampered content");
    fs::write(&journal, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(registry.execute(execution.clone()).await.is_err());
    fs::write(&journal, original).unwrap();
    // The original hash lets an authorized retry publish a saved intent whose file publication was interrupted.
    fs::write(
        Path::new(&workspace).join("README.md"),
        "Original project evidence.\n",
    )
    .unwrap();
    assert_eq!(
        registry.execute(execution).await.unwrap().result().unwrap()["already_applied"],
        false
    );
    fs::write(
        Path::new(&workspace).join("limit.txt"),
        format!("a{}", "x".repeat(1_048_575)),
    )
    .unwrap();
    let Command::Schedule(spec) = definition
        .schedule_call_with_permissions(
            &json!({"path":"limit.txt","old":"a","new":"aa"}),
            Some(&workspace),
            &["limit.txt".into()],
        )
        .unwrap()
    else {
        panic!()
    };
    let result = registry
        .execute(ActivityExecution {
            run_id: RunId::new(),
            activity_id: ActivityId::new(),
            attempt_id: AttemptId::new(),
            ordinal: 1,
            handler: spec.handler,
            input: spec.input,
        })
        .await
        .unwrap();
    assert_eq!(result.result().unwrap()["error"]["code"], "file_limit");
}

#[tokio::test]
async fn native_edits_refuse_linked_targets_and_replaced_managed_roots() {
    for mode in ["nested", "root"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("workspaces");
        let workspace = LocalWorkspaces::new(root.clone())
            .prepare(source(dir.path()))
            .await
            .unwrap();
        let tools = Arc::new(LocalTools::new(root));
        let registry = editable_workspace_tool_registry(tools.clone(), tools).unwrap();
        let foreign = dir.path().join("foreign");
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join("README.md"), "foreign evidence").unwrap();
        let path = if mode == "nested" {
            directory_link(&foreign, &Path::new(&workspace).join("linked"));
            "linked/README.md"
        } else {
            fs::rename(&workspace, Path::new(&workspace).with_extension("original")).unwrap();
            directory_link(&foreign, Path::new(&workspace));
            "README.md"
        };
        let definition = registry
            .definition(&DefinitionId::new("replace_text", 1))
            .unwrap();
        let Command::Schedule(spec) = definition
            .schedule_call_with_permissions(
                &json!({"path":path,"old":"foreign evidence","new":"unsafe change"}),
                Some(&workspace),
                &["linked/".into(), "README.md".into()],
            )
            .unwrap()
        else {
            panic!()
        };
        let outcome = registry
            .execute(ActivityExecution {
                run_id: RunId::new(),
                activity_id: ActivityId::new(),
                attempt_id: AttemptId::new(),
                ordinal: 1,
                handler: spec.handler,
                input: spec.input,
            })
            .await;
        if mode == "nested" {
            assert_eq!(
                outcome.unwrap().result().unwrap()["error"]["code"],
                "path_refused"
            );
        } else {
            assert!(outcome.is_err());
        }
        assert_eq!(
            fs::read_to_string(foreign.join("README.md")).unwrap(),
            "foreign evidence"
        );
    }
}

struct EditReviewModel;
impl ModelClient for EditReviewModel {
    fn generate(&self, request: ModelRequest, _: ActivityId) -> PortFuture<'_, ModelReply> {
        Box::pin(async move {
            let prompt: Value = serde_json::from_str(&request.prompt).unwrap();
            let action = if request.target.model == "assistant" {
                if request.turn == 0 {
                    AgentAction::AskHuman {
                        question: "Approve the review?".into(),
                    }
                } else {
                    AgentAction::Finish {
                        summary: prompt["goal"].as_str().unwrap().into(),
                    }
                }
            } else {
                match request.turn {
                    0 => AgentAction::CallTool {
                        tool: "read_file".into(),
                        arguments: json!({"path":"README.md"}),
                    },
                    1 => AgentAction::CallTool {
                        tool: "replace_text".into(),
                        arguments: json!({"path":"README.md",
                        "old":prompt["observations"][0]["result"]["text"].as_str().unwrap().trim_end(),"new":"Updated project evidence."}),
                    },
                    2 => {
                        assert_eq!(prompt["observations"][1]["result"]["ok"], true);
                        AgentAction::CallTool {
                            tool: "read_file".into(),
                            arguments: json!({"path":"README.md"}),
                        }
                    }
                    3 => AgentAction::DelegateTask {
                        writable_paths: None,
                        worker: "reviewer".into(),
                        goal: format!(
                            "Review {}",
                            prompt["observations"][2]["result"]["text"]
                                .as_str()
                                .unwrap()
                        ),
                    },
                    _ => {
                        assert_eq!(prompt["observations"][3]["result"]["status"], "succeeded");
                        AgentAction::Finish {
                            summary: "Edited and reviewed saved evidence.".into(),
                        }
                    }
                }
            };
            Ok(ModelReply {
                decision: Ok(ModelDecision {
                    action,
                    request_id: None,
                    diagnostics: None,
                }),
                usage: Some(TokenUsage {
                    input_tokens: 10,
                    output_tokens: 10,
                }),
                retryable: false,
                diagnostics: None,
                retry_after: None,
            })
        })
    }
}

#[tokio::test]
async fn planner_edits_and_delegates_saved_changed_evidence_across_write_read_and_review_restarts()
{
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("edit-review.db");
    let calls = Arc::new(Calls::default());
    let app = open(&db, calls.clone(), Some(Arc::new(EditReviewModel))).await;
    let mut cfg = edit_config();
    cfg["goal_budget"] = json!({"max_model_calls":7,"max_tokens":300000});
    let actor = create(&app, "planner", Some(source(dir.path())), cfg, 15).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    native_once(&mut worker).await;
    model_once(&mut worker).await;
    approve_edit(&app, task, true).await;
    let (edit, _) = native_once(&mut worker).await;
    let next = app
        .engine
        .status(edit.run_id)
        .await
        .unwrap()
        .activities()
        .last()
        .unwrap()
        .spec
        .input
        .clone();
    drop(worker);
    drop(app);
    let app = open(&db, calls.clone(), Some(Arc::new(EditReviewModel))).await;
    let mut worker = app.engine.worker().await.unwrap();
    assert_eq!(
        app.engine
            .status(edit.run_id)
            .await
            .unwrap()
            .activities()
            .last()
            .unwrap()
            .spec
            .input,
        next
    );
    model_once(&mut worker).await;
    let (read, _) = native_once(&mut worker).await;
    fs::write(
        Path::new(read.input["workspace"].as_str().unwrap()).join("README.md"),
        "Later content cannot replace saved review evidence",
    )
    .unwrap();
    drop(worker);
    drop(app);
    let app = open(&db, calls.clone(), Some(Arc::new(EditReviewModel))).await;
    let mut worker = app.engine.worker().await.unwrap();
    settle(&mut worker).await;
    let child = app
        .agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .delegation
        .unwrap();
    assert_eq!(child.goal, "Review Updated project evidence.\n");
    let waiting = app.agents.actors().unwrap().task(child.id).await.unwrap();
    let wait = waiting.wait.unwrap();
    drop(worker);
    drop(app);
    let app = open(&db, calls.clone(), Some(Arc::new(EditReviewModel))).await;
    let mut worker = app.engine.worker().await.unwrap();
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .task(child.id)
            .await
            .unwrap()
            .wait
            .unwrap()
            .id,
        wait.id
    );
    app.engine
        .signal(
            waiting.run_id.unwrap(),
            wait.id,
            json!({"review":"Accepted"}),
        )
        .await
        .unwrap();
    settle(&mut worker).await;
    let tree = app
        .agents
        .actors()
        .unwrap()
        .inspect_task(task, Default::default())
        .await
        .unwrap();
    assert_eq!(tree.nodes.len(), 2);
    assert_eq!(tree.summary.visible_usage.admitted_model_calls, 7);
    assert_eq!(
        app.agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status,
        AgentTaskStatus::Succeeded
    );
    let run = app.engine.status(edit.run_id).await.unwrap();
    assert_eq!(
        run.attempts()
            .iter()
            .filter(|a| a.activity_id == edit.activity_id)
            .count(),
        1
    );
    assert_eq!(calls.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
#[ignore = "child process fixture invoked by native edit recovery test"]
async fn workspace_edit_crash_fixture() {
    let Ok(root) = std::env::var("ACT0R_EDIT_CRASH_DIR") else {
        return;
    };
    let root = PathBuf::from(root);
    let stage = std::env::var("ACT0R_EDIT_CRASH_STAGE").unwrap();
    let calls = Arc::new(Calls {
        requests: Mutex::new(vec![]),
        ledger: Some(root.join("operations.log")),
    });
    let app = open(
        &root.join("edit-crash.db"),
        calls,
        Some(Arc::new(EditReviewModel)),
    )
    .await;
    let mut cfg = edit_config();
    cfg["goal_budget"] = json!({"max_model_calls":7,"max_tokens":300000});
    let actor = create(&app, "planner", Some(source(&root)), cfg, 16).await;
    let task = send(&app, actor).await;
    let mut worker = app.engine.worker().await.unwrap();
    model_once(&mut worker).await;
    native_once(&mut worker).await;
    model_once(&mut worker).await;
    let mut edit = None;
    if stage != "approval" {
        approve_edit(&app, task, true).await;
        if stage != "intent" {
            let execution = worker.admit().await.unwrap().unwrap();
            if stage != "admitted" {
                let outcome = worker.execute(execution.clone()).await.unwrap();
                if stage != "applied" {
                    worker.complete(execution.clone(), outcome).await.unwrap();
                }
            }
            edit = Some(execution);
        }
        if matches!(stage.as_str(), "read_saved" | "review_wait") {
            model_once(&mut worker).await;
            native_once(&mut worker).await;
        }
        if stage == "review_wait" {
            settle(&mut worker).await;
        }
    }
    fs::write(
        root.join("edit-ready.json"),
        serde_json::to_vec(&json!({"task":task,"actor":actor,"edit":edit})).unwrap(),
    )
    .unwrap();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::test]
async fn killed_native_edit_workers_preserve_approval_intent_journal_results_and_delegated_wait() {
    for stage in [
        "approval",
        "intent",
        "admitted",
        "applied",
        "saved",
        "read_saved",
        "review_wait",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "workspace_edit_crash_fixture"])
            .env("ACT0R_EDIT_CRASH_DIR", root)
            .env("ACT0R_EDIT_CRASH_STAGE", stage)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let start = Instant::now();
        while !root.join("edit-ready.json").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "edit fixture exited: {stage}"
            );
            if start.elapsed() > Duration::from_secs(25) {
                child.kill().unwrap();
                panic!("edit fixture timeout: {stage}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let ready: Value =
            serde_json::from_slice(&fs::read(root.join("edit-ready.json")).unwrap()).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let calls = Arc::new(Calls {
            requests: Mutex::new(vec![]),
            ledger: Some(root.join("operations.log")),
        });
        let app = open(
            &root.join("edit-crash.db"),
            calls,
            Some(Arc::new(EditReviewModel)),
        )
        .await;
        let task: MessageId = serde_json::from_value(ready["task"].clone()).unwrap();
        let actor: AgentId = serde_json::from_value(ready["actor"].clone()).unwrap();
        let managed = PathBuf::from(
            app.agents
                .actors()
                .unwrap()
                .status(actor)
                .await
                .unwrap()
                .record()
                .config["input"]["workspace"]
                .as_str()
                .unwrap(),
        );
        if matches!(stage, "read_saved" | "review_wait") {
            fs::write(managed.join("README.md"), "Changed after saved read").unwrap();
        }
        let mut worker = app.engine.worker().await.unwrap();
        if stage == "approval" {
            approve_edit(&app, task, true).await;
        }
        if matches!(stage, "admitted" | "applied") {
            let execution: ActivityExecution =
                serde_json::from_value(ready["edit"].clone()).unwrap();
            let run = app.engine.status(execution.run_id).await.unwrap();
            assert_eq!(run.record().status, RunStatus::Blocked);
            assert!(worker.admit().await.unwrap().is_none());
            app.engine
                .reconcile(
                    execution.run_id,
                    ReconciliationRequest {
                        activity_id: execution.activity_id,
                        attempt_id: execution.attempt_id,
                        decision: ReconciliationDecision::Retry {},
                        evidence: "Killed worker has exited; authorize the bounded journal retry."
                            .into(),
                    },
                )
                .await
                .unwrap();
            let retry = worker.admit().await.unwrap().unwrap();
            assert_eq!(retry.activity_id, execution.activity_id);
            assert_eq!(retry.input, execution.input);
            let outcome = worker.execute(retry.clone()).await.unwrap();
            assert_eq!(
                outcome.result().unwrap()["already_applied"],
                stage == "applied"
            );
            worker.complete(retry, outcome).await.unwrap();
        }
        settle(&mut worker).await;
        let child = app
            .agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .delegation
            .unwrap();
        assert_eq!(child.goal, "Review Updated project evidence.\n");
        let waiting = app.agents.actors().unwrap().task(child.id).await.unwrap();
        app.engine
            .signal(
                waiting.run_id.unwrap(),
                waiting.wait.unwrap().id,
                json!({"review":"Accepted"}),
            )
            .await
            .unwrap();
        settle(&mut worker).await;
        assert_eq!(
            app.agents
                .actors()
                .unwrap()
                .task(task)
                .await
                .unwrap()
                .status,
            AgentTaskStatus::Succeeded
        );
        let tree = app
            .agents
            .actors()
            .unwrap()
            .inspect_task(task, Default::default())
            .await
            .unwrap();
        assert_eq!(tree.summary.visible_usage.admitted_model_calls, 7);
        let ledger = fs::read_to_string(root.join("operations.log")).unwrap();
        assert_eq!(
            ledger.lines().filter(|l| l.starts_with("read ")).count(),
            2,
            "repeated read: {stage}"
        );
        assert_eq!(
            ledger.lines().filter(|l| l.starts_with("edit ")).count(),
            if stage == "applied" { 2 } else { 1 },
            "unexpected edit executions: {stage}"
        );
        if stage == "applied" {
            let ids: Vec<_> = ledger.lines().filter(|l| l.starts_with("edit ")).collect();
            assert_eq!(ids[0], ids[1]);
        }
    }
}
