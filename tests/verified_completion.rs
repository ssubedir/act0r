//! Public actor contract: verified candidates, durable refusals, and explicit partial work.
use act0r::{
    Engine, Worker,
    application::AgentService,
    bootstrap::local_app,
    domain::{actors::*, change_review::*, completion::*, *},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

fn source(root: &Path, mode: &str) -> PathBuf {
    let source = root.join("source");
    fs::create_dir_all(source.join("src")).unwrap();
    fs::write(
        source.join("src/lib.rs"),
        "pub fn answer() -> i64 { 19 - 23 }\n",
    )
    .unwrap();
    fs::write(source.join("README.md"), "Original\n").unwrap();
    fs::write(source.join("test-mode"), mode).unwrap();
    source
}
fn repair() -> Value {
    json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"19 - 23","new":"19 + 23"}})
}
fn tests() -> Value {
    json!({"kind":"call_tool","tool":"run_tests","arguments":{}})
}
fn finish() -> Value {
    json!({"kind":"finish","summary":"Verified repair"})
}
fn incomplete() -> Value {
    json!({"kind":"finish_incomplete","summary":"Unable to verify this repair within the remaining budget"})
}
fn config(script: Vec<Value>) -> Value {
    json!({"goal":"Repair and verify","memory":{},"target":{"provider":"scripted","model":"completion","endpoint":""},
        "max_model_calls":12,"max_tokens":2000000,"max_output_tokens":512,"change_review":true,"completion_policy":"require_passing_tests",
        "tools":[{"name":"replace_text","version":1},{"name":"run_tests","version":1}],"writable_paths":["src/"],
        "test_command":{"argv":[std::env::current_exe().unwrap(),"--ignored","--exact","completion_test_fixture","--nocapture"],"timeout_ms":10000},"script":script})
}
async fn create(agents: &AgentService, source: PathBuf, policy: &str, mut cfg: Value) -> AgentId {
    if policy == "planner" {
        let reviewer = agents.create_registered_actor(DefinitionId::new("answer",1),None,json!({"goal":"Review","target":{"provider":"scripted","model":"reviewer","endpoint":""},"max_model_calls":1,"max_tokens":100000,"max_output_tokens":512,"script":[{"kind":"finish","summary":"Reviewed"}]}),4,Default::default()).await.unwrap();
        cfg["workers"] = json!([{"name":"reviewer","agent_id":reviewer,"description":"Reviews"}]);
        cfg["delegation"] = json!({"max_tasks":1,"max_depth":1});
    }
    agents
        .create_registered_actor(
            DefinitionId::new(policy, if policy == "planner" { 3 } else { 1 }),
            Some(source),
            cfg,
            80,
            Default::default(),
        )
        .await
        .unwrap()
}
async fn send(agents: &AgentService, actor: AgentId) -> MessageId {
    agents.actors().unwrap().send(AgentMessage {id:MessageId::new(),recipient:actor,sender:None,correlation_id:None,kind:"task".into(),payload:json!({"goal":"Repair","completion_policy":null,"test_command":{"argv":["forged"]}})}).await.unwrap()
}
async fn approve(engine: &Engine, agents: &AgentService, task: MessageId) {
    let view = agents.actors().unwrap().task(task).await.unwrap();
    if let (Some(run), Some(wait)) = (view.run_id, view.wait) {
        assert_eq!(wait.key, "native_tool_approval");
        engine
            .signal(run, wait.id, json!({"approved":true}))
            .await
            .unwrap();
    }
}
async fn until(
    engine: &Engine,
    agents: &AgentService,
    worker: &mut Worker,
    task: MessageId,
    target: &str,
) -> ActivityExecution {
    for _ in 0..300 {
        worker.advance_work().await.unwrap();
        approve(engine, agents, task).await;
        if let Some(call) = worker.admit().await.unwrap() {
            if call.handler.name == target || call.input["definition"]["id"]["name"] == target {
                return call;
            }
            let result = worker.execute(call.clone()).await.unwrap();
            worker.complete(call, result).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("{target} not admitted");
}
async fn settle(engine: &Engine, agents: &AgentService, worker: &mut Worker, task: MessageId) {
    for _ in 0..300 {
        worker.step().await.unwrap();
        approve(engine, agents, task).await;
        if agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status
            .is_terminal()
        {
            for _ in 0..16 {
                if !worker.advance_work().await.unwrap() {
                    break;
                }
            }
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("task did not finish");
}
fn workspace(run: &Run) -> PathBuf {
    PathBuf::from(run.record().input["input"]["workspace"].as_str().unwrap())
}

#[test]
#[ignore = "subprocess for the pinned completion test command"]
fn completion_test_fixture() {
    use std::io::Write;
    let root = std::env::current_dir().unwrap();
    let mut ledger = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join(".act0r-tools/tests"))
        .unwrap();
    writeln!(ledger, "test").unwrap();
    let mode = fs::read_to_string(root.join("test-mode")).unwrap();
    if mode == "mutate" {
        fs::write(root.join("README.md"), "Changed by tests\n").unwrap();
    }
    assert_ne!(mode, "fail", "expected failing test evidence");
    assert!(
        fs::read_to_string(root.join("src/lib.rs"))
            .unwrap()
            .contains("19 + 23")
    );
}

#[tokio::test]
async fn both_policies_verify_exact_final_capture_once_across_restart_and_export_without_live_workspace()
 {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("completion.db");
        let src = source(dir.path(), "pass");
        let (engine, agents) = local_app(&db).await.unwrap();
        let actor = create(
            &agents,
            src.clone(),
            policy,
            config(vec![repair(), tests(), finish()]),
        )
        .await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let capture = until(&engine, &agents, &mut worker, task, "workspace.changes").await;
        let outcome = worker.execute(capture.clone()).await.unwrap();
        let before = engine.status(capture.run_id).await.unwrap();
        let ws = workspace(&before);
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        let recovered = until(&engine, &agents, &mut worker, task, "workspace.changes").await;
        assert_eq!(recovered.activity_id, capture.activity_id);
        let replayed = worker.execute(recovered.clone()).await.unwrap();
        assert_eq!(replayed.result(), outcome.result());
        worker
            .complete(recovered.clone(), replayed.clone())
            .await
            .unwrap();
        let saved = engine.status(capture.run_id).await.unwrap();
        assert_eq!(saved.record().status, RunStatus::Succeeded);
        assert_eq!(saved.record().usage.admitted_model_calls, 3);
        assert_eq!(
            saved.record().result.as_ref().unwrap()["completion"]["status"],
            "verified"
        );
        let revision = saved.record().revision;
        worker.complete(recovered, replayed).await.unwrap();
        assert_eq!(
            engine
                .status(capture.run_id)
                .await
                .unwrap()
                .record()
                .revision,
            revision
        );
        settle(&engine, &agents, &mut worker, task).await;
        assert_eq!(
            agents
                .actors()
                .unwrap()
                .memory(actor)
                .await
                .unwrap()
                .unwrap()
                .revision,
            1
        );
        let view = agents.actors().unwrap().task_changes(task).await.unwrap();
        assert_eq!(
            view.completion.as_ref().unwrap().status,
            CompletionStatus::Verified
        );
        assert_eq!(
            view.completion.as_ref().unwrap().activity_id,
            capture.activity_id
        );
        assert_eq!(
            view.changes.as_ref().unwrap().test_status,
            TestEvidenceStatus::Matching
        );
        assert_eq!(
            fs::read_to_string(ws.join(".act0r-tools/tests"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert!(
            fs::read_to_string(src.join("src/lib.rs"))
                .unwrap()
                .contains("19 - 23")
        );
        fs::rename(&ws, ws.with_extension("retired")).unwrap();
        let exported = std::process::Command::new(env!("CARGO_BIN_EXE_act0r"))
            .args([
                "--db",
                db.to_str().unwrap(),
                "--json",
                "actor",
                "task",
                "changes",
                &task.to_string(),
            ])
            .output()
            .unwrap();
        assert!(
            exported.status.success(),
            "{}",
            String::from_utf8_lossy(&exported.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&exported.stdout).unwrap(),
            json!(view)
        );
    }
}

#[tokio::test]
async fn missing_failed_and_stale_tests_refuse_success_then_save_explicit_incomplete_patch() {
    for policy in ["tool", "planner"] {
        for (mode, reason, script) in [
            (
                "pass",
                CompletionReason::TestsNotRun,
                vec![repair(), finish(), incomplete()],
            ),
            (
                "fail",
                CompletionReason::TestsFailed,
                vec![repair(), tests(), finish(), incomplete()],
            ),
            (
                "mutate",
                CompletionReason::TestsStale,
                vec![repair(), tests(), finish(), incomplete()],
            ),
            (
                "pass",
                CompletionReason::TestsStale,
                vec![
                    repair(),
                    tests(),
                    json!({"kind":"call_tool","tool":"replace_text","arguments":{"path":"src/lib.rs","old":"pub fn","new":"pub(crate) fn"}}),
                    finish(),
                    incomplete(),
                ],
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("completion.db");
            let (engine, agents) = local_app(&db).await.unwrap();
            let actor = create(&agents, source(dir.path(), mode), policy, config(script)).await;
            let task = send(&agents, actor).await;
            let mut worker = engine.worker().await.unwrap();
            let capture = until(&engine, &agents, &mut worker, task, "workspace.changes").await;
            let result = worker.execute(capture.clone()).await.unwrap();
            worker
                .complete(capture.clone(), result.clone())
                .await
                .unwrap();
            let refused = agents.actors().unwrap().task_changes(task).await.unwrap();
            assert_eq!(
                refused.completion.as_ref().unwrap().status,
                CompletionStatus::Refused
            );
            assert_eq!(refused.completion.as_ref().unwrap().reason, Some(reason));
            let run = engine.status(capture.run_id).await.unwrap();
            assert!(run.record().result.is_none());
            let next = run.activities().last().unwrap().spec.input.clone();
            assert!(
                next["prompt"]
                    .as_str()
                    .unwrap()
                    .contains(&serde_json::to_string(&reason).unwrap())
            );
            assert!(next["schema"].to_string().contains("finish_incomplete"));
            assert!(
                !next["instructions"]
                    .as_str()
                    .unwrap()
                    .contains("not a requirement to finish")
            );
            let calls = run.record().usage.admitted_model_calls;
            drop(worker);
            drop(agents);
            drop(engine);
            let (engine, agents) = local_app(&db).await.unwrap();
            let mut worker = engine.worker().await.unwrap();
            worker.complete(capture.clone(), result).await.unwrap();
            let recovered = engine.status(capture.run_id).await.unwrap();
            assert_eq!(recovered.activities().last().unwrap().spec.input, next);
            assert_eq!(recovered.record().usage.admitted_model_calls, calls);
            settle(&engine, &agents, &mut worker, task).await;
            let view = agents.actors().unwrap().task_changes(task).await.unwrap();
            assert_eq!(view.run_status, Some(RunStatus::Failed));
            assert_eq!(
                view.completion.as_ref().unwrap().status,
                CompletionStatus::Incomplete
            );
            assert_eq!(view.completion.as_ref().unwrap().reason, Some(reason));
            assert!(!view.changes.unwrap().files.is_empty());
            let stopped = engine.status(capture.run_id).await.unwrap();
            assert_eq!(
                stopped.record().error.as_ref().unwrap().code,
                "coding_incomplete"
            );
            assert!(stopped.record().result.is_none());
            assert!(stopped.record().follow_up.is_none());
            assert_eq!(
                agents.actors().unwrap().task(task).await.unwrap().status,
                AgentTaskStatus::Failed
            );
            assert_eq!(
                agents
                    .actors()
                    .unwrap()
                    .memory(actor)
                    .await
                    .unwrap()
                    .unwrap()
                    .revision,
                0
            );
            assert!(
                agents
                    .actors()
                    .unwrap()
                    .patch_applications()
                    .unwrap()
                    .preview(task, dir.path().canonicalize().unwrap())
                    .await
                    .is_err()
            );
        }
    }
}

#[tokio::test]
async fn refusal_allows_repair_and_retest_without_resetting_model_or_goal_allowances() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
        // The first test really fails. The actor tries finish, receives refusal, and repairs it.
        let mut cfg = config(vec![tests(), finish(), repair(), tests(), finish()]);
        cfg["max_model_calls"] = json!(5);
        if policy == "planner" {
            cfg["goal_budget"] = json!({"max_model_calls":5,"max_tokens":2000000});
        }
        let actor = create(&agents, source(dir.path(), "pass"), policy, cfg).await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&engine, &agents, &mut worker, task).await;
        let view = agents.actors().unwrap().task_changes(task).await.unwrap();
        assert_eq!(view.completion.unwrap().status, CompletionStatus::Verified);
        let run = engine.status(view.run_id.unwrap()).await.unwrap();
        assert_eq!(run.record().usage.admitted_model_calls, 5);
        assert_eq!(
            run.activities()
                .iter()
                .filter(|a| a.spec.handler == handler("changes"))
                .count(),
            2
        );
        assert_eq!(
            fs::read_to_string(workspace(&run).join(".act0r-tools/tests"))
                .unwrap()
                .lines()
                .count(),
            2
        );
    }
}

#[tokio::test]
async fn repeated_finish_exhausts_original_budget_and_preserves_refused_candidate() {
    for policy in ["tool", "planner"] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
        let mut cfg = config(vec![finish(), finish(), finish()]);
        cfg["max_model_calls"] = json!(2);
        let actor = create(&agents, source(dir.path(), "pass"), policy, cfg).await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        settle(&engine, &agents, &mut worker, task).await;
        let view = agents.actors().unwrap().task_changes(task).await.unwrap();
        assert_eq!(view.run_status, Some(RunStatus::BudgetExhausted));
        assert_eq!(view.completion.unwrap().status, CompletionStatus::Refused);
        assert!(view.changes.is_some());
        let run = engine.status(view.run_id.unwrap()).await.unwrap();
        assert_eq!(run.record().usage.admitted_model_calls, 2);
        assert!(run.record().result.is_none());
    }
}

#[tokio::test]
async fn unknown_test_results_do_not_count_as_passing_evidence() {
    for result in [
        json!({"exit_code":0}),
        json!({"ok":true}),
        json!({"ok":true,"exit_code":"0"}),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
        let actor = create(
            &agents,
            source(dir.path(), "pass"),
            "tool",
            config(vec![repair(), tests(), finish(), incomplete()]),
        )
        .await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let call = until(&engine, &agents, &mut worker, task, "run_tests").await;
        // Custom hosts can deliver malformed successful activity output; completion fails closed.
        worker
            .complete(call, Outcome::Success(result))
            .await
            .unwrap();
        let capture = until(&engine, &agents, &mut worker, task, "workspace.changes").await;
        let outcome = worker.execute(capture.clone()).await.unwrap();
        worker.complete(capture, outcome).await.unwrap();
        let view = agents.actors().unwrap().task_changes(task).await.unwrap();
        assert_eq!(
            view.completion.unwrap().reason,
            Some(CompletionReason::UnknownTestResult)
        );
        settle(&engine, &agents, &mut worker, task).await;
    }
}

#[tokio::test]
async fn configuration_requires_review_and_pinned_tests_and_disabled_actors_keep_their_schema() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
    let src = source(dir.path(), "pass");
    for field in ["change_review", "test_command", "tools"] {
        let mut cfg = config(vec![finish()]);
        cfg[field] = match field {
            "change_review" => json!(false),
            "tools" => json!([{"name":"replace_text","version":1}]),
            _ => Value::Null,
        };
        assert!(
            agents
                .create_registered_actor(
                    DefinitionId::new("tool", 1),
                    Some(src.clone()),
                    cfg,
                    20,
                    Default::default()
                )
                .await
                .is_err()
        );
    }
    let mut cfg = config(vec![finish()]);
    cfg.as_object_mut().unwrap().remove("completion_policy");
    let actor = create(&agents, src.clone(), "tool", cfg.clone()).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let model = until(&engine, &agents, &mut worker, task, "agent.model").await;
    assert!(
        !model.input["schema"]
            .to_string()
            .contains("finish_incomplete")
    );
    let result = worker.execute(model.clone()).await.unwrap();
    worker.complete(model, result).await.unwrap();
    settle(&engine, &agents, &mut worker, task).await;
    let view = agents.actors().unwrap().task_changes(task).await.unwrap();
    assert_eq!(view.run_status, Some(RunStatus::Succeeded));
    assert!(view.completion.is_none());
    cfg["script"] = json!([incomplete()]);
    assert!(
        agents
            .create_registered_actor(
                DefinitionId::new("tool", 1),
                Some(src),
                cfg,
                20,
                Default::default()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn unverified_follow_up_is_refused_and_cannot_publish_a_future_task() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
    let mut cfg = config(vec![
        repair(),
        json!({"kind":"schedule_follow_up","summary":"Unverified repair","goal":"Verify later","after_ms":1}),
        incomplete(),
    ]);
    cfg["follow_up"] = json!({"max_follow_ups":1,"min_delay_ms":1,"max_delay_ms":1000});
    let actor = create(&agents, source(dir.path(), "pass"), "planner", cfg).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, task).await;
    let view = agents.actors().unwrap().task(task).await.unwrap();
    assert!(view.follow_up.is_none());
    let run = engine.status(view.run_id.unwrap()).await.unwrap();
    assert_eq!(
        run.record().error.as_ref().unwrap().code,
        "coding_incomplete"
    );
    assert!(run.record().follow_up.is_none());
    assert_eq!(
        agents.actors().unwrap().tasks(actor).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn verified_follow_up_retains_assessment_and_durable_intent() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
    let mut cfg = config(vec![
        repair(),
        tests(),
        json!({"kind":"schedule_follow_up","summary":"Verified repair","goal":"Review later","after_ms":60000}),
    ]);
    cfg["follow_up"] = json!({"max_follow_ups":1,"min_delay_ms":1,"max_delay_ms":60000});
    let actor = create(&agents, source(dir.path(), "pass"), "planner", cfg).await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    settle(&engine, &agents, &mut worker, task).await;
    let view = agents.actors().unwrap().task(task).await.unwrap();
    assert!(view.follow_up.is_some());
    let run = engine.status(view.run_id.unwrap()).await.unwrap();
    assert_eq!(run.record().status, RunStatus::Succeeded);
    assert!(run.record().follow_up.is_some());
    assert_eq!(
        run.record().result.as_ref().unwrap()["completion"]["status"],
        "verified"
    );
}

#[tokio::test]
async fn incomplete_child_retains_patch_but_parent_cannot_import_it() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
    let src = source(dir.path(), "pass");
    let child = create(
        &agents,
        src.clone(),
        "tool",
        config(vec![repair(), incomplete()]),
    )
    .await;
    let mut cfg = config(vec![
        json!({"kind":"delegate_task","worker":"repairer","goal":"Repair"}),
        json!({"kind":"inspect_child_changes","worker":"repairer","offset":0}),
        json!({"kind":"import_child_changes","worker":"repairer"}),
        incomplete(),
    ]);
    cfg["child_changes"] = json!(true);
    cfg["workers"] = json!([{"name":"repairer","agent_id":child,"description":"Repair"}]);
    cfg["delegation"] = json!({"max_tasks":1,"max_depth":1});
    let parent = agents
        .create_registered_actor(
            DefinitionId::new("planner", 3),
            Some(src),
            cfg,
            80,
            Default::default(),
        )
        .await
        .unwrap();
    let task = send(&agents, parent).await;
    let mut worker = engine.worker().await.unwrap();
    for _ in 0..300 {
        worker.step().await.unwrap();
        for view in agents.actors().unwrap().tasks(child).await.unwrap() {
            approve(&engine, &agents, view.task.id).await;
        }
        approve(&engine, &agents, task).await;
        if agents
            .actors()
            .unwrap()
            .task(task)
            .await
            .unwrap()
            .status
            .is_terminal()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let view = agents.actors().unwrap().task_changes(task).await.unwrap();
    assert_eq!(view.run_status, Some(RunStatus::Failed));
    assert!(view.changes.unwrap().files.is_empty());
    let run = engine.status(view.run_id.unwrap()).await.unwrap();
    assert!(
        !run.activities()
            .iter()
            .any(|a| a.spec.handler == child_changes::handler("import"))
    );
    let inspect = run
        .activities()
        .iter()
        .find(|a| a.spec.handler == child_changes::handler("inspect"))
        .unwrap();
    assert_eq!(inspect.result.as_ref().unwrap()["ok"], false);
    assert_eq!(
        inspect.result.as_ref().unwrap()["error"]["code"],
        "child_changes_unavailable"
    );
    let child = agents
        .actors()
        .unwrap()
        .task_changes(run.record().delegations[0].id)
        .await
        .unwrap();
    assert_eq!(
        child.completion.unwrap().status,
        CompletionStatus::Incomplete
    );
    assert_eq!(child.changes.unwrap().files.len(), 1);
}

#[tokio::test]
async fn final_hash_check_detects_external_drift_after_passing_tests() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
    let actor = create(
        &agents,
        source(dir.path(), "pass"),
        "tool",
        config(vec![repair(), tests(), finish(), incomplete()]),
    )
    .await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let capture = until(&engine, &agents, &mut worker, task, "workspace.changes").await;
    let run = engine.status(capture.run_id).await.unwrap();
    fs::write(
        workspace(&run).join("README.md"),
        "External edit after tests\n",
    )
    .unwrap();
    let outcome = worker.execute(capture.clone()).await.unwrap();
    worker.complete(capture, outcome).await.unwrap();
    let view = agents.actors().unwrap().task_changes(task).await.unwrap();
    assert_eq!(
        view.completion.unwrap().reason,
        Some(CompletionReason::TestsStale)
    );
    assert_eq!(view.changes.unwrap().tests.unwrap().result["ok"], true);
    settle(&engine, &agents, &mut worker, task).await;
}

#[tokio::test]
async fn goal_hold_survives_refused_completion_restart_and_cancellation_prevents_repair() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("completion.db");
        let (engine, agents) = local_app(&db).await.unwrap();
        let actor = create(
            &agents,
            source(dir.path(), "pass"),
            "tool",
            config(vec![finish(), repair(), tests(), finish()]),
        )
        .await;
        let task = send(&agents, actor).await;
        let mut worker = engine.worker().await.unwrap();
        let capture = until(&engine, &agents, &mut worker, task, "workspace.changes").await;
        agents.actors().unwrap().pause_goal(task).await.unwrap();
        let outcome = worker.execute(capture.clone()).await.unwrap();
        worker.complete(capture.clone(), outcome).await.unwrap();
        let before = engine.status(capture.run_id).await.unwrap();
        assert!(before.record().result.is_none());
        assert!(worker.admit().await.unwrap().is_none());
        let next = before.activities().last().unwrap().spec.input.clone();
        drop(worker);
        drop(agents);
        drop(engine);
        let (engine, agents) = local_app(&db).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        assert!(worker.admit().await.unwrap().is_none());
        assert_eq!(
            engine
                .status(capture.run_id)
                .await
                .unwrap()
                .activities()
                .last()
                .unwrap()
                .spec
                .input,
            next
        );
        if cancel {
            agents.actors().unwrap().cancel_goal(task).await.unwrap();
        } else {
            agents.actors().unwrap().resume_goal(task).await.unwrap();
        }
        settle(&engine, &agents, &mut worker, task).await;
        let view = agents.actors().unwrap().task_changes(task).await.unwrap();
        assert_eq!(
            view.run_status,
            Some(if cancel {
                RunStatus::Cancelled
            } else {
                RunStatus::Succeeded
            })
        );
        assert_eq!(
            view.completion.unwrap().status,
            if cancel {
                CompletionStatus::Refused
            } else {
                CompletionStatus::Verified
            }
        );
        let run = engine.status(capture.run_id).await.unwrap();
        assert_eq!(
            run.record().usage.admitted_model_calls,
            if cancel { 1 } else { 4 }
        );
    }
}

#[tokio::test]
async fn a_capture_completed_under_cancellation_does_not_reuse_an_older_assessment() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
    let actor = create(
        &agents,
        source(dir.path(), "pass"),
        "tool",
        config(vec![repair(), finish(), incomplete()]),
    )
    .await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let first = until(&engine, &agents, &mut worker, task, "workspace.changes").await;
    let outcome = worker.execute(first.clone()).await.unwrap();
    worker.complete(first.clone(), outcome).await.unwrap();
    let last = until(&engine, &agents, &mut worker, task, "workspace.changes").await;
    let outcome = worker.execute(last.clone()).await.unwrap();
    engine.cancel(last.run_id).await.unwrap();
    worker.complete(last.clone(), outcome).await.unwrap();
    let view = agents.actors().unwrap().task_changes(task).await.unwrap();
    assert_eq!(view.run_status, Some(RunStatus::Cancelled));
    assert_eq!(view.changes.unwrap().activity_id, last.activity_id);
    assert!(view.completion.is_none());
}

#[test]
fn verified_evidence_requires_the_exact_pinned_command_and_success_flags() {
    let command = tools::WorkspaceTestCommand {
        argv: vec!["test-project".into()],
        timeout_ms: 1000,
    };
    let mut changes = WorkspaceChangeSet {
        version: 1,
        run_id: RunId::new(),
        activity_id: ActivityId::new(),
        baseline_digest: "a".repeat(64),
        workspace_digest: "b".repeat(64),
        files: vec![],
        changed_bytes: 0,
        test_status: TestEvidenceStatus::Matching,
        tests: Some(TestEvidence {
            activity_id: ActivityId::new(),
            command: command.clone(),
            before_digest: "b".repeat(64),
            after_digest: "b".repeat(64),
            result: json!({"ok":true,"exit_code":0}),
        }),
    };
    changes.validate().unwrap();
    assert_eq!(
        assess(&changes, &command, false).status,
        CompletionStatus::Verified
    );
    changes.tests.as_mut().unwrap().command.argv = vec!["different-tests".into()];
    assert_eq!(
        assess(&changes, &command, false).reason,
        Some(CompletionReason::WrongTestCommand)
    );
    changes.tests.as_mut().unwrap().command = command.clone();
    changes.tests.as_mut().unwrap().result = json!({"ok":true,"exit_code":1});
    assert_eq!(
        assess(&changes, &command, false).reason,
        Some(CompletionReason::TestsFailed)
    );
    assert_eq!(
        assess(&changes, &command, true).status,
        CompletionStatus::Incomplete
    );
    assert!(act0r::domain::completion::incomplete("").is_err());
    assert!(act0r::domain::completion::incomplete(&"x".repeat(60_001)).is_err());
}

#[tokio::test]
async fn responses_and_chat_receive_refusal_and_can_choose_explicit_incomplete() {
    use act0r::domain::agent::ModelProtocol;
    use std::{
        io::{Read, Write},
        net::TcpListener,
    };
    for policy in ["tool", "planner"] {
        for protocol in [ModelProtocol::Responses, ModelProtocol::ChatCompletions] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let mut requests = vec![];
                for action in [finish(), incomplete()] {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    let mut bytes = vec![];
                    let mut buffer = [0; 4096];
                    let (offset, length) = loop {
                        let count = stream.read(&mut buffer).unwrap();
                        assert!(count > 0);
                        bytes.extend_from_slice(&buffer[..count]);
                        if let Some(position) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                            let header = String::from_utf8_lossy(&bytes[..position]).to_lowercase();
                            let length = header
                                .lines()
                                .find_map(|line| {
                                    line.strip_prefix("content-length:")
                                        .map(|v| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap();
                            break (position + 4, length);
                        }
                    };
                    while bytes.len() < offset + length {
                        let count = stream.read(&mut buffer).unwrap();
                        assert!(count > 0);
                        bytes.extend_from_slice(&buffer[..count]);
                    }
                    requests.push(
                        serde_json::from_slice::<Value>(&bytes[offset..offset + length]).unwrap(),
                    );
                    let text = json!({"action":action}).to_string();
                    let body = match protocol {
                        ModelProtocol::Responses => json!({"status":"completed","usage":{"input_tokens":20,"output_tokens":10},"output":[{"content":[{"type":"output_text","text":text}]}]}),
                        ModelProtocol::ChatCompletions => json!({"usage":{"prompt_tokens":20,"completion_tokens":10},"choices":[{"finish_reason":"stop","message":{"content":text}}]}),
                    }.to_string();
                    write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
                }
                requests
            });
            let dir = tempfile::tempdir().unwrap();
            let (engine, agents) = local_app(dir.path().join("completion.db")).await.unwrap();
            let mut cfg = config(vec![]);
            cfg["target"] = json!({"provider":"openai","model":"fixture","protocol":protocol,"endpoint":format!("http://{address}/v1/{}",if matches!(protocol,ModelProtocol::Responses) {"responses"} else {"chat/completions"})});
            let actor = create(&agents, source(dir.path(), "pass"), policy, cfg).await;
            let task = send(&agents, actor).await;
            let mut worker = engine.worker().await.unwrap();
            settle(&engine, &agents, &mut worker, task).await;
            let requests = server.join().unwrap();
            let schema = if matches!(protocol, ModelProtocol::Responses) {
                &requests[0]["text"]["format"]["schema"]
            } else {
                &requests[0]["response_format"]["json_schema"]["schema"]
            };
            assert!(schema.to_string().contains("finish_incomplete"));
            assert!(requests[1].to_string().contains("tests_not_run"));
            let view = agents.actors().unwrap().task_changes(task).await.unwrap();
            assert_eq!(view.run_status, Some(RunStatus::Failed));
            assert_eq!(
                view.completion.unwrap().status,
                CompletionStatus::Incomplete
            );
        }
    }
}

#[tokio::test]
#[ignore = "spawned and killed by completion checkpoint recovery test"]
async fn completion_crash_fixture() {
    let root = PathBuf::from(std::env::var("ACT0R_COMPLETION_CRASH_ROOT").unwrap());
    let stage = std::env::var("ACT0R_COMPLETION_CRASH_STAGE").unwrap();
    let (engine, agents) = local_app(root.join("completion.db")).await.unwrap();
    let actor = create(
        &agents,
        source(&root, "pass"),
        "tool",
        config(if stage == "refused" {
            vec![finish(), repair(), tests(), finish()]
        } else {
            vec![repair(), tests(), finish()]
        }),
    )
    .await;
    let task = send(&agents, actor).await;
    let mut worker = engine.worker().await.unwrap();
    let call = until(&engine, &agents, &mut worker, task, "workspace.changes").await;
    let outcome = worker.execute(call.clone()).await.unwrap();
    if stage == "refused" {
        worker
            .complete(call.clone(), outcome.clone())
            .await
            .unwrap();
    }
    let saved = engine.status(call.run_id).await.unwrap();
    fs::write(root.join("ready.json"),serde_json::to_vec(&json!({"task":task,"call":call,"outcome":outcome,"next":saved.activities().last().unwrap().spec.input,"usage":saved.record().usage})).unwrap()).unwrap();
    let _keep = (engine, agents, worker);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn killed_hosts_replay_capture_or_saved_refusal_without_repeating_tests_or_resetting_usage() {
    use std::{
        process::{Command, Stdio},
        time::Instant,
    };
    for stage in ["captured", "refused"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "completion_crash_fixture",
                "--nocapture",
            ])
            .env("ACT0R_COMPLETION_CRASH_ROOT", root)
            .env("ACT0R_COMPLETION_CRASH_STAGE", stage)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn().unwrap();
        let start = Instant::now();
        while !root.join("ready.json").exists() {
            assert!(child.try_wait().unwrap().is_none(), "crash fixture exited");
            if start.elapsed() > Duration::from_secs(30) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("fixture timed out");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let saved: Value =
            serde_json::from_slice(&fs::read(root.join("ready.json")).unwrap()).unwrap();
        let task: MessageId = serde_json::from_value(saved["task"].clone()).unwrap();
        let call: ActivityExecution = serde_json::from_value(saved["call"].clone()).unwrap();
        let (engine, agents) = local_app(root.join("completion.db")).await.unwrap();
        let mut worker = engine.worker().await.unwrap();
        let recovered = engine.status(call.run_id).await.unwrap();
        assert_eq!(json!(recovered.record().usage), saved["usage"]);
        assert_eq!(
            recovered.activities().last().unwrap().spec.input,
            saved["next"]
        );
        settle(&engine, &agents, &mut worker, task).await;
        let view = agents.actors().unwrap().task_changes(task).await.unwrap();
        assert_eq!(view.completion.unwrap().status, CompletionStatus::Verified);
        let final_run = engine.status(call.run_id).await.unwrap();
        assert_eq!(
            final_run.record().usage.admitted_model_calls,
            if stage == "captured" { 3 } else { 4 }
        );
        assert_eq!(
            fs::read_to_string(workspace(&final_run).join(".act0r-tools/tests"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        if stage == "captured" {
            let outcome: Outcome = serde_json::from_value(saved["outcome"].clone()).unwrap();
            assert_eq!(
                json!(view.changes.unwrap()),
                outcome.result().unwrap().clone()
            );
        }
    }
}
