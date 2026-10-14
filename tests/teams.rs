//! Public team setup, atomic publication and existing actor execution contracts.
use act0r::{
    Engine, Error,
    adapters::{
        agents::{AnswerAgentAdapter, PlannerAgentAdapter, ToolAgentAdapter},
        candidates::LocalCandidates,
        sqlite::SqliteStore,
        team_workspaces::LocalTeamWorkspaces,
    },
    application::{AgentRegistry, TeamService, ToolRegistry, ports::*},
    bootstrap::local_app,
    domain::{actors::*, teams::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

fn actor_config(name: &str, script: Vec<Value>) -> Value {
    json!({"goal":name,"target":{"provider":"scripted","model":name,"endpoint":""},
        "max_model_calls":4,"max_tokens":500000,"max_output_tokens":512,"script":script})
}
fn spec(source: Option<&Path>) -> TeamSpec {
    let finish = json!({"kind":"finish","summary":"Done"});
    let mut planner = actor_config(
        "planner",
        vec![
            json!({"kind":"delegate_tasks","tasks":[{"worker":"reader","goal":"Inspect the project"},{"worker":"reviewer","goal":"Review the result"}]}),
            finish.clone(),
        ],
    );
    planner["delegation"] = json!({"max_tasks":2,"max_depth":2,"max_parallel_tasks":2});
    let mut reader = actor_config("reader", vec![finish.clone()]);
    if source.is_some() {
        reader["tools"] = json!([{"name":"read_file","version":2}]);
    }
    serde_json::from_value(json!({"version":1,"name":"sample-team","source":source.map(|p|p.to_string_lossy()),
        "planner":{"config":planner},"workers":[
        {"name":"reader","description":"Project reader","adapter":{"name":"tool","version":1},"config":reader},
        {"name":"reviewer","description":"Independent reviewer","adapter":{"name":"answer","version":1},"config":actor_config("reviewer",vec![finish])}
    ]})).unwrap()
}
fn source(root: &Path) -> PathBuf {
    let path = root.join("project");
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("README.md"), "Initial project\n").unwrap();
    fs::write(path.join(".env"), "EXCLUDED=secret\n").unwrap();
    fs::canonicalize(path).unwrap()
}
async fn pool(db: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(db))
        .await
        .unwrap()
}
async fn sql(db: &Path, statement: &'static str) {
    let pool = pool(db).await;
    sqlx::raw_sql(statement).execute(&pool).await.unwrap();
    pool.close().await;
}
async fn count(db: &Path, table: &str) -> i64 {
    let pool = pool(db).await;
    let query = match table {
        "agent_teams" => "SELECT COUNT(*) FROM agent_teams",
        "agent_actors" => "SELECT COUNT(*) FROM agent_actors",
        "agent_team_members" => "SELECT COUNT(*) FROM agent_team_members",
        "agent_mailbox" => "SELECT COUNT(*) FROM agent_mailbox",
        "agent_tasks" => "SELECT COUNT(*) FROM agent_tasks",
        _ => panic!("unknown fixture table"),
    };
    let result = sqlx::query_scalar(query).fetch_one(&pool).await.unwrap();
    pool.close().await;
    result
}
fn ids(record: &TeamRecord) -> Vec<AgentId> {
    record.members.iter().map(|m| m.agent_id).collect()
}
fn workspace_adapter(root: &Path) -> Arc<LocalTeamWorkspaces> {
    Arc::new(LocalTeamWorkspaces::new(
        root.join(".act0r/teams"),
        Arc::new(LocalCandidates::new(
            root.join(".act0r/snapshots"),
            root.join(".act0r/workspaces"),
        )),
    ))
}
struct InterruptedWorkspaces {
    inner: Arc<LocalTeamWorkspaces>,
    calls: AtomicUsize,
    interrupt: usize,
    marker: Option<PathBuf>,
}
impl TeamWorkspaces for InterruptedWorkspaces {
    fn lock_setup(&self, id: TeamId) -> PortFuture<'_, Box<dyn TeamSetupGuard>> {
        self.inner.lock_setup(id)
    }
    fn prepare_member(
        &self,
        team: TeamId,
        actor: AgentId,
        source: String,
    ) -> PortFuture<'_, TeamWorkspace> {
        Box::pin(async move {
            let prepared = self.inner.prepare_member(team, actor, source).await?;
            if self.calls.fetch_add(1, Ordering::SeqCst) == self.interrupt {
                if let Some(marker) = &self.marker {
                    fs::write(marker, serde_json::to_vec(&prepared)?)?;
                    loop {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                return Err(Error::Unavailable(
                    "interrupted after workspace publication".into(),
                ));
            }
            Ok(prepared)
        })
    }
}
async fn interrupted_service(
    db: &Path,
    engine: Engine,
    root: &Path,
    interrupt: usize,
    marker: Option<PathBuf>,
) -> TeamService {
    let tools = Arc::new(ToolRegistry::new(vec![]).unwrap());
    let registry = Arc::new(
        AgentRegistry::new(vec![
            Arc::new(AnswerAgentAdapter),
            Arc::new(ToolAgentAdapter::new(tools.clone())),
            Arc::new(PlannerAgentAdapter::new(tools)),
        ])
        .unwrap(),
    );
    // These fixtures use no tools so their contracts equal the normal local bootstrap.
    TeamService::new(
        engine,
        Arc::new(SqliteStore::open(db).await.unwrap()),
        Arc::new(InterruptedWorkspaces {
            inner: workspace_adapter(root),
            calls: AtomicUsize::new(0),
            interrupt,
            marker,
        }),
        registry,
    )
}
fn setup_spec(source: &Path) -> TeamSpec {
    let mut spec = spec(Some(source));
    spec.workers[0]
        .config
        .as_object_mut()
        .unwrap()
        .remove("tools");
    spec
}

#[tokio::test]
async fn validates_entire_configuration_before_persisting_intents_or_copying_files() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("teams.db");
    let src = source(root.path());
    let (_, agents) = local_app(&db).await.unwrap();
    let base = json!(spec(Some(&src)));
    let mut invalid = Vec::new();
    for (key, value) in [
        ("version", json!(2)),
        ("name", json!("../team")),
        ("workers", json!([])),
        ("source", json!("relative")),
    ] {
        let mut cfg = base.clone();
        cfg[key] = value;
        invalid.push(cfg);
    }
    for (key, value) in [
        ("attempt_limit", json!(0)),
        ("queue_policy", json!({"task_capacity":0})),
    ] {
        let mut cfg = base.clone();
        cfg["workers"][1][key] = value;
        invalid.push(cfg);
    }
    for (key, value) in [
        ("workspace", Value::Null),
        ("actor_memory", json!({})),
        ("tool_definitions", json!([])),
        ("max_model_calls", json!(0)),
        ("memory", json!({"max_entries":0})),
        ("tools", json!([{"name":"missing","version":1}])),
    ] {
        let mut cfg = base.clone();
        cfg["workers"][1]["config"][key] = value;
        invalid.push(cfg);
    }
    let mut cfg = base.clone();
    cfg["planner"]["config"]["workers"] = json!([]);
    invalid.push(cfg);
    let mut cfg = base.clone();
    cfg["workers"][1]["name"] = json!("reader");
    invalid.push(cfg);
    let mut cfg = base.clone();
    cfg["workers"][1]["adapter"] = json!({"name":"coding","version":1});
    invalid.push(cfg);
    let mut cfg = base.clone();
    cfg["workers"][1]["config"]["invented"] = json!(true);
    invalid.push(cfg);
    for value in invalid {
        let spec: TeamSpec = serde_json::from_value(value.clone()).unwrap();
        assert!(
            agents.teams().unwrap().create(spec).await.is_err(),
            "accepted {value}"
        );
        assert_eq!(count(&db, "agent_teams").await, 0);
        assert_eq!(count(&db, "agent_actors").await, 0);
        assert!(!root.path().join(".act0r").exists());
    }
    let mut too_large = base.clone();
    too_large["workers"][0]["description"] = json!("x".repeat(MAX_TEAM_SPEC_BYTES));
    assert!(
        agents
            .teams()
            .unwrap()
            .create(serde_json::from_value(too_large).unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn workspace_boundary_interruptions_reuse_all_ids_and_one_original_snapshot() {
    for boundary in 0..2 {
        let root = tempfile::tempdir().unwrap();
        let db = root.path().join("teams.db");
        let src = source(root.path());
        let (engine, agents) = local_app(&db).await.unwrap();
        let manifest = setup_spec(&src);
        let service = interrupted_service(&db, engine.clone(), root.path(), boundary, None).await;
        assert!(service.create(manifest.clone()).await.is_err());
        let saved = service.list().await.unwrap().teams.remove(0);
        let before = service.get(saved.id).await.unwrap();
        assert_eq!(before.status, TeamStatus::Preparing);
        assert!(before.error.as_ref().unwrap().contains("interrupted"));
        assert_eq!(count(&db, "agent_actors").await, 0);
        assert_eq!(
            before
                .members
                .iter()
                .filter(|m| m.workspace.is_some())
                .count(),
            boundary
        );
        assert!(
            service
                .submit(saved.id, MessageId::new(), "Do work".into())
                .await
                .is_err()
        );
        let member = &before.members[0];
        assert!(
            agents
                .actors()
                .unwrap()
                .create_with_policies(
                    member.agent_id,
                    RegisteredAgentDefinition.id(),
                    member.pinned_config.clone(),
                    member.queue_policy,
                    None
                )
                .await
                .is_err()
        );
        let mut worker = engine.worker().await.unwrap();
        assert!(!worker.step().await.unwrap());
        drop(worker);
        drop(service);
        drop(agents);
        drop(engine);
        fs::write(src.join("README.md"), "Changed after snapshot\n").unwrap();
        let (_, agents) = local_app(&db).await.unwrap();
        let ready = agents.teams().unwrap().resume(saved.id).await.unwrap();
        assert_eq!(ids(&before), ids(&ready));
        assert_eq!(ready.spec, manifest);
        assert_eq!(ready.status, TeamStatus::Ready);
        assert!(ready.error.is_none());
        assert_eq!(count(&db, "agent_actors").await, 3);
        let mut paths = Vec::new();
        let mut digests = Vec::new();
        for (old, new) in before.members.iter().zip(&ready.members) {
            assert_eq!(old.pinned_config, new.pinned_config);
            if let Some(workspace) = &new.workspace {
                assert_eq!(
                    fs::read_to_string(Path::new(&workspace.path).join("README.md")).unwrap(),
                    "Initial project\n"
                );
                assert!(!Path::new(&workspace.path).join(".env").exists());
                paths.push(&workspace.path);
                digests.push(&workspace.snapshot_digest);
            }
        }
        assert_eq!(paths.len(), 2);
        assert_ne!(paths[0], paths[1]);
        assert_eq!(digests[0], digests[1]);
        let replay = agents.teams().unwrap().create(manifest).await.unwrap();
        assert_eq!(ids(&replay), ids(&ready));
        assert_eq!(count(&db, "agent_team_members").await, 3);
    }
}

#[tokio::test]
async fn actor_and_readiness_publication_roll_back_together_then_resume() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("teams.db");
    let src = source(root.path());
    let (_, agents) = local_app(&db).await.unwrap();
    sql(&db,"CREATE TRIGGER refuse_team_actor BEFORE INSERT ON agent_actors WHEN (SELECT COUNT(*) FROM agent_actors)=1 BEGIN SELECT RAISE(ABORT,'publication fault'); END;").await;
    let manifest = spec(Some(&src));
    assert!(
        agents
            .teams()
            .unwrap()
            .create(manifest.clone())
            .await
            .is_err()
    );
    let team = agents
        .teams()
        .unwrap()
        .list()
        .await
        .unwrap()
        .teams
        .remove(0);
    let pending = agents.teams().unwrap().get(team.id).await.unwrap();
    assert_eq!(count(&db, "agent_actors").await, 0);
    assert_eq!(pending.status, TeamStatus::Preparing);
    assert_eq!(
        pending
            .members
            .iter()
            .filter(|m| m.workspace.is_some())
            .count(),
        2
    );
    let view = agents.teams().unwrap().status(team.id).await.unwrap();
    assert!(
        view.members
            .iter()
            .all(|m| m.status.is_none() && m.queue.is_none())
    );
    sql(&db, "DROP TRIGGER refuse_team_actor;").await;
    let ready = agents.teams().unwrap().create(manifest).await.unwrap();
    assert_eq!(ids(&pending), ids(&ready));
    assert_eq!(count(&db, "agent_actors").await, 3);
    assert!(
        agents
            .teams()
            .unwrap()
            .status(team.id)
            .await
            .unwrap()
            .members
            .iter()
            .all(|m| m.status == Some(AgentStatus::Ready))
    );
}

#[tokio::test]
async fn concurrent_creation_is_idempotent_and_changed_configuration_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("teams.db");
    let src = source(root.path());
    let (_, a) = local_app(&db).await.unwrap();
    let (_, b) = local_app(&db).await.unwrap();
    let manifest = spec(Some(&src));
    let (first, second) = tokio::join!(
        a.teams().unwrap().create(manifest.clone()),
        b.teams().unwrap().create(manifest.clone())
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(ids(&first), ids(&second));
    assert_eq!(count(&db, "agent_teams").await, 1);
    assert_eq!(count(&db, "agent_actors").await, 3);
    assert_eq!(
        fs::read_dir(root.path().join(".act0r/workspaces"))
            .unwrap()
            .count(),
        2
    );
    let mut changed = manifest;
    changed.workers[0].config["max_tokens"] = json!(499999);
    assert!(
        a.teams()
            .unwrap()
            .create(changed)
            .await
            .unwrap_err()
            .to_string()
            .contains("different pinned")
    );
    assert_eq!(
        ids(&a.teams().unwrap().get(first.id).await.unwrap()),
        ids(&first)
    );
}

#[tokio::test]
async fn resume_refuses_damaged_unpublished_copies_and_missing_pinned_tools() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("teams.db");
    let src = source(root.path());
    let (engine, agents) = local_app(&db).await.unwrap();
    sql(&db,"CREATE TRIGGER refuse_ready BEFORE UPDATE ON agent_teams WHEN NEW.status='ready' BEGIN SELECT RAISE(ABORT,'readiness fault'); END;").await;
    assert!(
        agents
            .teams()
            .unwrap()
            .create(spec(Some(&src)))
            .await
            .is_err()
    );
    let team = agents
        .teams()
        .unwrap()
        .list()
        .await
        .unwrap()
        .teams
        .remove(0);
    let pending = agents.teams().unwrap().get(team.id).await.unwrap();
    assert_eq!(count(&db, "agent_actors").await, 0);
    sql(&db, "DROP TRIGGER refuse_ready;").await;
    let tools = Arc::new(ToolRegistry::default());
    let registry = Arc::new(
        AgentRegistry::new(vec![
            Arc::new(AnswerAgentAdapter),
            Arc::new(ToolAgentAdapter::new(tools.clone())),
            Arc::new(PlannerAgentAdapter::new(tools)),
        ])
        .unwrap(),
    );
    let missing = TeamService::new(
        engine,
        Arc::new(SqliteStore::open(&db).await.unwrap()),
        workspace_adapter(root.path()),
        registry,
    );
    assert!(
        missing
            .resume(team.id)
            .await
            .unwrap_err()
            .to_string()
            .contains("not registered")
    );
    let file = Path::new(&pending.members[0].workspace.as_ref().unwrap().path).join("README.md");
    fs::write(&file, "Unexpected change\n").unwrap();
    assert!(agents.teams().unwrap().resume(team.id).await.is_err());
    assert_eq!(count(&db, "agent_actors").await, 0);
    fs::write(file, "Initial project\n").unwrap();
    let ready = agents.teams().unwrap().resume(team.id).await.unwrap();
    assert_eq!(ids(&ready), ids(&pending));
}

#[tokio::test]
async fn submission_obeys_capacity_and_identity_and_checks_every_member_binding() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("teams.db");
    let (_, agents) = local_app(&db).await.unwrap();
    let mut manifest = spec(None);
    manifest.planner.queue_policy.task_capacity = 1;
    let team = agents.teams().unwrap().create(manifest).await.unwrap();
    let task = MessageId::new();
    agents
        .teams()
        .unwrap()
        .submit(team.id, task, "Inspect".into())
        .await
        .unwrap();
    agents
        .teams()
        .unwrap()
        .submit(team.id, task, "Inspect".into())
        .await
        .unwrap();
    assert!(matches!(
        agents
            .teams()
            .unwrap()
            .submit(team.id, MessageId::new(), "Another".into())
            .await,
        Err(Error::QueueFull { .. })
    ));
    assert!(
        agents
            .teams()
            .unwrap()
            .submit(team.id, task, "Changed".into())
            .await
            .is_err()
    );
    assert_eq!(count(&db, "agent_mailbox").await, 1);
    assert_eq!(count(&db, "agent_tasks").await, 1);
    let worker = team.members[1].agent_id;
    let original = agents
        .actors()
        .unwrap()
        .status(worker)
        .await
        .unwrap()
        .record()
        .config
        .clone();
    let p = pool(&db).await;
    sqlx::query("UPDATE agent_actors SET payload=json_set(payload,'$.config.input.max_tokens',12) WHERE id=?").bind(worker.to_string()).execute(&p).await.unwrap();
    assert!(
        agents
            .teams()
            .unwrap()
            .submit(team.id, task, "Inspect".into())
            .await
            .unwrap_err()
            .to_string()
            .contains("pinned actor")
    );
    assert!(agents.teams().unwrap().resume(team.id).await.is_err());
    sqlx::query("UPDATE agent_actors SET payload=json_set(payload,'$.config',json(?)) WHERE id=?")
        .bind(serde_json::to_string(&original).unwrap())
        .bind(worker.to_string())
        .execute(&p)
        .await
        .unwrap();
    p.close().await;
    assert_eq!(
        agents
            .teams()
            .unwrap()
            .status(team.id)
            .await
            .unwrap()
            .members[0]
            .queue
            .as_ref()
            .unwrap()
            .outstanding,
        1
    );
}

#[tokio::test]
async fn teams_use_existing_parallel_tasks_durable_human_waits_and_persistent_actor_memory() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("teams.db");
    let src = source(root.path());
    let (engine, agents) = local_app(&db).await.unwrap();
    let mut manifest = setup_spec(&src);
    for worker in &mut manifest.workers {
        worker.config["script"] = json!([{"kind":"ask_human","question":"Continue?"},{"kind":"finish","summary":"Reviewed"}]);
        worker.config["memory"] = json!({});
    }
    let team = agents
        .teams()
        .unwrap()
        .create(manifest.clone())
        .await
        .unwrap();
    let task = MessageId::new();
    agents
        .teams()
        .unwrap()
        .submit(team.id, task, "Review the project".into())
        .await
        .unwrap();
    let mut worker = engine.worker().await.unwrap();
    let mut waits = Vec::new();
    for _ in 0..120 {
        worker.step().await.unwrap();
        waits.clear();
        for member in &team.members[1..] {
            for view in agents
                .actors()
                .unwrap()
                .tasks(member.agent_id)
                .await
                .unwrap()
            {
                if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
                    waits.push((view.task.id, run, wait.id));
                }
            }
        }
        if waits.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(waits.len(), 2);
    let root_run = agents
        .actors()
        .unwrap()
        .task(task)
        .await
        .unwrap()
        .run_id
        .unwrap();
    assert_eq!(
        engine
            .status(root_run)
            .await
            .unwrap()
            .record()
            .task_groups
            .len(),
        1
    );
    let before = agents
        .actors()
        .unwrap()
        .status(team.members[0].agent_id)
        .await
        .unwrap();
    let file = Path::new(&team.members[0].workspace.as_ref().unwrap().path).join("README.md");
    fs::write(&file, "Working project\n").unwrap();
    drop(worker);
    drop(agents);
    drop(engine);
    let (engine, agents) = local_app(&db).await.unwrap();
    let same = agents.teams().unwrap().create(manifest).await.unwrap();
    assert_eq!(ids(&same), ids(&team));
    assert_eq!(
        json!(before),
        json!(
            agents
                .actors()
                .unwrap()
                .status(team.members[0].agent_id)
                .await
                .unwrap()
        )
    );
    assert_eq!(fs::read_to_string(file).unwrap(), "Working project\n");
    let mut worker = engine.worker().await.unwrap();
    for (task, run, wait) in &waits {
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .task(*task)
                .await
                .unwrap()
                .wait
                .unwrap()
                .id,
            *wait
        );
        engine
            .signal(*run, *wait, json!({"answer":"continue"}))
            .await
            .unwrap();
    }
    for _ in 0..160 {
        worker.step().await.unwrap();
        if agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status
            .is_terminal()
        {
            for _ in 0..20 {
                worker.advance_work().await.unwrap();
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(
        agents.actors().unwrap().task(task).await.unwrap().status,
        AgentTaskStatus::Succeeded
    );
    for member in &team.members[1..] {
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .tasks(member.agent_id)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .memory(member.agent_id)
                .await
                .unwrap()
                .unwrap()
                .entries
                .len(),
            1
        );
    }
    let saved_run = engine.status(root_run).await.unwrap();
    assert_eq!(saved_run.record().usage.admitted_model_calls, 2);
    assert_eq!(saved_run.record().task_groups[0].children.len(), 2);
    assert_eq!(
        engine
            .history(root_run)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == "task_group_joined")
            .count(),
        1
    );
    agents
        .teams()
        .unwrap()
        .submit(team.id, task, "Review the project".into())
        .await
        .unwrap();
    assert_eq!(count(&db, "agent_tasks").await, 3);
}

#[test]
fn cli_resolves_source_against_manifest_and_retries_submission() {
    let root = tempfile::tempdir().unwrap();
    let src = source(root.path());
    let db = root.path().join("teams.db");
    let config = root.path().join("team.json");
    let mut manifest = json!(setup_spec(&src));
    manifest["source"] = json!("project");
    fs::write(&config, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    let cli = |args: &[&str]| {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"))
            .arg("--db")
            .arg(&db)
            .arg("--json")
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let created = cli(&["team", "create", "--config-file", config.to_str().unwrap()]);
    let id = created["id"].as_str().unwrap();
    assert_eq!(created["status"], "ready");
    assert_eq!(created, cli(&["team", "resume", id]));
    assert_eq!(created, cli(&["team", "status", id]));
    let task = MessageId::new().to_string();
    let submit = [
        "team",
        "submit",
        id,
        "--goal",
        "Inspect",
        "--message-id",
        task.as_str(),
    ];
    assert_eq!(cli(&submit), cli(&submit));
    assert_eq!(cli(&["team", "list"])["teams"][0]["id"], id);
    manifest["workers"][0]["unknown"] = json!(true);
    fs::write(config, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(serde_json::from_value::<TeamSpec>(manifest).is_err());
}

#[tokio::test]
#[ignore = "child process held at a durable team setup boundary"]
async fn team_setup_kill_fixture() {
    let root = PathBuf::from(std::env::var_os("ACT0R_TEAM_KILL_ROOT").unwrap());
    let db = root.join("teams.db");
    let (engine, agents) = local_app(&db).await.unwrap();
    let boundary: usize = std::env::var("ACT0R_TEAM_KILL_BOUNDARY")
        .unwrap()
        .parse()
        .unwrap();
    let manifest = setup_spec(&fs::canonicalize(root.join("project")).unwrap());
    if boundary < 2 {
        let service =
            interrupted_service(&db, engine, &root, boundary, Some(root.join("copied.json"))).await;
        service.create(manifest).await.unwrap();
    } else {
        let team = agents.teams().unwrap().create(manifest).await.unwrap();
        fs::write(
            root.join("copied.json"),
            serde_json::to_vec(&team.members[0].workspace).unwrap(),
        )
        .unwrap();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
#[tokio::test]
async fn process_kills_before_receipts_and_after_ready_replay_without_duplicates() {
    for boundary in 0..3 {
        let root = tempfile::tempdir().unwrap();
        let src = source(root.path());
        let db = root.path().join("teams.db");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "team_setup_kill_fixture",
                "--nocapture",
            ])
            .env("ACT0R_TEAM_KILL_ROOT", root.path())
            .env("ACT0R_TEAM_KILL_BOUNDARY", boundary.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !root.path().join("copied.json").exists() && Instant::now() < deadline {
            assert!(child.try_wait().unwrap().is_none(), "fixture exited early");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let copied = root.path().join("copied.json").exists();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(copied, "fixture timed out");
        let (_, agents) = local_app(&db).await.unwrap();
        let team = agents
            .teams()
            .unwrap()
            .list()
            .await
            .unwrap()
            .teams
            .remove(0);
        let pending = agents.teams().unwrap().get(team.id).await.unwrap();
        assert_eq!(
            pending
                .members
                .iter()
                .filter(|m| m.workspace.is_some())
                .count(),
            boundary
        );
        assert_eq!(
            count(&db, "agent_actors").await,
            if boundary == 2 { 3 } else { 0 }
        );
        let physical: TeamWorkspace =
            serde_json::from_slice(&fs::read(root.path().join("copied.json")).unwrap()).unwrap();
        // Once captured, even a missing original source cannot change the saved setup snapshot.
        fs::rename(src, root.path().join("moved-project")).unwrap();
        let ready = agents.teams().unwrap().resume(team.id).await.unwrap();
        assert_eq!(ids(&ready), ids(&pending));
        assert_eq!(
            ready.members[if boundary == 1 { 1 } else { 0 }]
                .workspace
                .as_ref(),
            Some(&physical)
        );
        assert_eq!(count(&db, "agent_actors").await, 3);
        assert_eq!(
            fs::read_dir(root.path().join(".act0r/workspaces"))
                .unwrap()
                .count(),
            2
        );
    }
}
