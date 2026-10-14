//! Recovery driver for the example. SQLite remains the workflow authority.
use super::*;
use sha2::{Digest, Sha256};
use std::io::Write;

fn read(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

// Publish the identity before sending its HTTP request. A torn file is an error,
// never a reason to generate a replacement identity or decision.
fn create(path: &Path, value: &Value) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    Ok(())
}

pub(super) fn checkpoint(app: &App, receipt: &Value, phase: &str) -> Value {
    json!({"status":"checkpoint","phase":phase,"directory":app.directory,
        "database":app.directory.join("demo.db"),"session":receipt})
}

async fn validate(app: &App, request: &Value, receipt: &Value) -> Result<()> {
    let request: act0r::domain::harness::HarnessRequest = serde_json::from_value(request.clone())?;
    let source = fs::canonicalize(app.directory.join("project"))?;
    if request
        .workspace
        .as_deref()
        .map(fs::canonicalize)
        .transpose()?
        .as_ref()
        != Some(&source)
        || receipt["session_id"] != json!(request.message_id)
        || receipt["task_id"] != json!(request.message_id)
        || request.coding.as_ref().is_none_or(|c| {
            c.coder_count != 2
                || c.writable_paths != ["src/api.rs", "src/consumer.rs"]
                || c.test_command.argv
                    != [
                        "cargo",
                        "test",
                        "--locked",
                        "--offline",
                        "--test",
                        "contracts",
                    ]
                || c.test_command.timeout_ms != 120000
                || !c.project_commands.is_empty()
        })
    {
        return Err(
            "resume requires the original dependent-coding fixture and exact session receipt"
                .into(),
        );
    }
    for (path, contents) in FIXTURE
        .iter()
        .filter(|(p, _)| !matches!(*p, "src/api.rs" | "src/consumer.rs"))
    {
        if fs::read_to_string(source.join(path))? != *contents {
            return Err("resume refused changed protected fixture files".into());
        }
    }
    let team = app
        .agents
        .teams()?
        .get(receipt["team_id"].as_str().ok_or("missing team")?.parse()?)
        .await?;
    if team
        .spec
        .source
        .as_deref()
        .map(fs::canonicalize)
        .transpose()?
        .as_ref()
        != Some(&source)
        || team
            .members
            .iter()
            .any(|m| m.pinned_config["input"]["target"] != json!(request.target))
    {
        return Err("saved request differs from the original team model or workspace pins".into());
    }
    let task = app.agents.actors()?.task(request.message_id).await?;
    if task.task.agent_id != team.planner()?.agent_id {
        return Err("saved receipt differs from the original planner task".into());
    }
    let parent = app
        .engine
        .status(task.run_id.ok_or("saved session has no planner Run")?)
        .await?;
    if parent.record().input["input"]["goal"] != request.prompt {
        return Err("saved request differs from the original planner goal".into());
    }
    Ok(())
}

async fn guide(
    client: &Client,
    app: &App,
    receipt: &Value,
    options: &Options,
) -> Result<Option<Value>> {
    let (wait, path) = match (&options.wait_id, &options.guidance_file) {
        (None, None) => return Ok(None),
        (Some(wait), Some(path)) => (wait, path),
        _ => return Err("guidance requires both --wait-id and --guidance-file".into()),
    };
    let answer = fs::read_to_string(path)?;
    if answer.trim().is_empty() || answer.len() > 65536 {
        return Err("guidance must be nonblank and at most 65536 UTF-8 bytes".into());
    }
    let body = json!({"kind":"answer","answer":answer});
    let journal = app.directory.join(format!("guidance-{wait}.json"));
    if journal.exists() {
        let saved = read(&journal)?;
        if saved["wait_id"] != json!(wait) || saved["session"] != *receipt || saved["body"] != body
        {
            return Err("guidance conflicts with the saved decision for this exact wait".into());
        }
    } else {
        let inbox = get(client, &format!("{}/inbox", app.goal(receipt))).await?;
        let item = inbox["items"]
            .as_array()
            .ok_or("missing inbox items")?
            .iter()
            .find(|i| i["wait"]["id"] == json!(wait))
            .ok_or("the exact guidance wait is not pending")?;
        if item["kind"] != "guidance" || item["actionable"] != true {
            return Err("the exact wait is not actionable guidance; holds and recovery remain authoritative".into());
        }
        create(
            &journal,
            &json!({"session":receipt,"wait_id":wait,"task_id":item["task_id"],"body":body}),
        )?;
    }
    let url = format!("{}/waits/{wait}/responses", app.goal(receipt));
    let first = post(client, &url, &body).await?;
    let duplicate = post(client, &url, &body).await?;
    if duplicate["replayed"] != true {
        return Err("guidance response did not retain its exact receipt".into());
    }
    eprintln!(
        "Guidance saved for wait {wait}; exact retry acknowledged. Restarting before model continuation."
    );
    Ok(Some(
        json!({"wait_id":wait,"response":first,"duplicate":duplicate}),
    ))
}

async fn continue_saved(
    client: &Client,
    app: &App,
    receipt: &Value,
    options: &Options,
) -> Result<Value> {
    let mut worker = app.engine.worker().await?;
    let work = get(client, &format!("{}/delegated-work", app.turn(receipt))).await?;
    let task = app
        .agents
        .actors()?
        .task(receipt["task_id"].as_str().ok_or("missing task")?.parse()?)
        .await?;
    let parent = app
        .engine
        .status(task.run_id.ok_or("missing planner Run")?)
        .await?;
    let declarations = &parent.record().delegations;
    if declarations.len() != 2
        || declarations[0]
            .initial_dependency
            .as_ref()
            .is_none_or(|d| d.prerequisite_task_id != declarations[1].id)
    {
        return Err(
            "resume requires the original single group with its saved initial dependency".into(),
        );
    }
    let transferred = parent.activities().iter().any(|a| {
        a.spec.handler == act0r::domain::worker_changes::handler("import")
            && a.status == act0r::domain::ActivityStatus::Succeeded
    });
    if !task.status.is_terminal() && !transferred {
        let approving = drive(client, app, &mut worker, receipt, Phase::TransferApproval).await?;
        let held = assert_held(app, &approving).await?;
        let prerequisite = verified(app, &approving["groups"][0]["children"][1]).await?;
        let inbox = get(client, &format!("{}/inbox", app.goal(receipt))).await?;
        let approval = inbox["items"]
            .as_array()
            .ok_or("missing inbox")?
            .iter()
            .find(|i| i["wait"]["payload"]["worker_transfer"] == true)
            .ok_or("missing transfer approval")?;
        save(
            &app.directory.join("checkpoint-approval.json"),
            &json!({"delegated_work":approving,"receiver":held,"approval":approval,"prerequisite":prerequisite}),
        )?;
        if options.stop_after == Some(StopAfter::TransferApproval) {
            return Ok(checkpoint(app, receipt, "transfer_approval"));
        }
        reply(client, app, receipt, approval).await?;
    }
    let completed = if task.status == act0r::domain::actors::AgentTaskStatus::Succeeded {
        work
    } else {
        drive(client, app, &mut worker, receipt, Phase::Complete).await?
    };
    let previous = app.directory.join("report.json");
    let restarts = if previous.exists() {
        read(&previous)?["restarts"].as_u64().unwrap_or(0) as u32
    } else {
        0
    };
    let mut report = finish(
        client,
        app,
        &mut worker,
        receipt,
        &completed,
        None,
        options.stop_after,
        restarts,
    )
    .await?;
    report["resumed"] = json!(true);
    if read(&app.directory.join("request.json"))?["target"]["model"] == "dependent-coding-script" {
        report["mode"] = json!("scripted");
    }
    report["resume_restarts_verified"] =
        json!(fs::read_dir(app.directory.join("resume-restarts"))?.count());
    if report["status"] == "verified" {
        save(&app.directory.join("report.json"), &report)?;
    }
    Ok(report)
}

async fn owned_runs(
    client: &Client,
    app: &App,
    receipt: &Value,
    work: &Value,
    root: RunId,
) -> Result<Vec<RunId>> {
    let mut ids = vec![root];
    ids.extend(
        work["groups"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|g| g["children"].as_array().into_iter().flatten())
            .map(id)
            .collect::<Result<Vec<_>>>()?,
    );
    let applications = get(client, &format!("{}/applications", app.turn(receipt))).await?;
    if applications["truncated"] == true {
        return Err("demo application inspection was truncated; inspect it explicitly".into());
    }
    ids.extend(
        applications["items"]
            .as_array()
            .ok_or("missing applications")?
            .iter()
            .map(id)
            .collect::<Result<Vec<_>>>()?,
    );
    Ok(ids)
}

async fn snapshot(client: &Client, app: &App, receipt: &Value) -> Result<Value> {
    let task = app
        .agents
        .actors()?
        .task(receipt["task_id"].as_str().ok_or("missing task")?.parse()?)
        .await?;
    let work = get(client, &format!("{}/delegated-work", app.turn(receipt))).await?;
    let ids = owned_runs(
        client,
        app,
        receipt,
        &work,
        task.run_id.ok_or("missing planner Run")?,
    )
    .await?;
    let mut runs = Vec::new();
    for run in ids {
        let saved = app.engine.status(run).await?;
        runs.push(json!({"run_id":run,"status":saved.record().status,"revision":saved.record().revision,
            "usage":saved.record().usage,"wait_id":saved.record().wait.as_ref().map(|w|w.id),
            "record_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(saved.record())?)),
            "activities_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(saved.activities())?))}));
    }
    Ok(json!({"session":receipt,"runs":runs}))
}

async fn outcome(
    client: &Client,
    app: &App,
    receipt: &Value,
    result: &Result<Value>,
    guidance: &Option<Value>,
) -> Result<()> {
    let task = app
        .agents
        .actors()?
        .task(receipt["task_id"].as_str().ok_or("missing task")?.parse()?)
        .await?;
    let work = get(client, &format!("{}/delegated-work", app.turn(receipt))).await?;
    let inbox = get(client, &format!("{}/inbox", app.goal(receipt))).await?;
    let mut runs = Vec::new();
    let ids = owned_runs(
        client,
        app,
        receipt,
        &work,
        task.run_id.ok_or("missing planner Run")?,
    )
    .await?;
    for run in ids {
        let saved = app.engine.status(run).await?;
        runs.push(json!({"run_id":run,"status":saved.record().status,"error":saved.record().error,"usage":saved.record().usage}));
    }
    let needs_guidance = inbox["items"]
        .as_array()
        .is_some_and(|items| items.iter().any(|i| i["kind"] == "guidance"));
    let status = match result {
        Ok(report) => report["status"].as_str().unwrap_or("checkpoint"),
        Err(_) if needs_guidance => "needs_guidance",
        Err(_) if runs.iter().any(|r| r["status"] == "blocked") => "needs_recovery",
        Err(_)
            if runs.iter().any(|r| {
                r["error"]["code"]
                    .as_str()
                    .is_some_and(|c| c.starts_with("model_"))
            }) =>
        {
            "model_failure"
        }
        Err(_) => "stopped",
    };
    let report = json!({"status":status,"session":receipt,"directory":app.directory,"database":app.directory.join("demo.db"),
        "guidance":guidance,"error":result.as_ref().err().map(ToString::to_string),"runs":runs,"task":task,"delegated_work":work,"inbox":inbox});
    let attempts = app.directory.join("resume-attempts");
    fs::create_dir_all(&attempts)?;
    create(
        &attempts.join(format!("{}.json", uuid::Uuid::new_v4())),
        &report,
    )?;
    save(&app.directory.join("resume-report.json"), &report)?;
    eprintln!(
        "Resume outcome: {status}. Evidence: {}",
        app.directory.join("resume-report.json").display()
    );
    Ok(())
}

pub(super) async fn run(options: Options) -> Result<Value> {
    if options.target_config.is_some() {
        return Err("resume cannot replace the saved model configuration".into());
    }
    let folder = fs::canonicalize(options.resume.as_ref().ok_or("missing resume folder")?)?;
    let db = folder.join("demo.db");
    if !db.is_file() {
        return Err("resume requires an existing demo.db; no new database is created".into());
    }
    let request = read(&folder.join("request.json"))?;
    let receipt = read(&folder.join("session.json"))?;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    let app = App::open(&db).await?;
    validate(&app, &request, &receipt).await?;
    eprintln!(
        "Continuing saved session {} in {}",
        receipt["session_id"],
        folder.display()
    );
    let guidance = match guide(&client, &app, &receipt, &options).await {
        Ok(guidance) => guidance,
        Err(error) => {
            let result = Err(error);
            outcome(&client, &app, &receipt, &result, &None).await?;
            app.close().await?;
            return result;
        }
    };
    let before = snapshot(&client, &app, &receipt).await?;
    app.close().await?;
    let app = App::open(&db).await?;
    let after = snapshot(&client, &app, &receipt).await?;
    if before != after {
        return Err(
            "saved Run pins, waits, usage or activities changed across the resume restart".into(),
        );
    }
    let restarts = folder.join("resume-restarts");
    fs::create_dir_all(&restarts)?;
    create(
        &restarts.join(format!("{}.json", uuid::Uuid::new_v4())),
        &json!({"before":before,"after":after}),
    )?;
    let result = continue_saved(&client, &app, &receipt, &options).await;
    outcome(&client, &app, &receipt, &result, &guidance).await?;
    app.close().await?;
    result
}

pub(super) async fn apply(
    client: &Client,
    app: &App,
    worker: &mut Worker,
    receipt: &Value,
    stop_after: Option<StopAfter>,
) -> Result<Value> {
    let base = format!("{}/applications", app.turn(receipt));
    let journal = app.directory.join("application-request.json");
    let existing = get(client, &base).await?;
    let items = existing["items"]
        .as_array()
        .ok_or("missing application list")?;
    if existing["truncated"] == true || items.len() > 1 {
        return Err(
            "demo requires one final application; inspect the existing applications".into(),
        );
    }
    let request = if journal.exists() {
        read(&journal)?
    } else {
        let id = if let Some(item) = items.first() {
            id(item)?
        } else {
            RunId::new()
        };
        let request = json!({"application_id":id});
        create(&journal, &request)?;
        request
    };
    let application_id: RunId = request["application_id"]
        .as_str()
        .ok_or("missing application identity")?
        .parse()?;
    if items
        .first()
        .is_some_and(|i| i["run_id"] != json!(application_id))
    {
        return Err(
            "saved application identity conflicts with the existing final application".into(),
        );
    }
    if items.is_empty() {
        check_original(&app.directory.join("project"))?;
    }
    post(client, &base, &request).await?;
    let url = format!("{base}/{application_id}");
    for _ in 0..40 {
        let view = get(client, &url).await?;
        match view["status"].as_str() {
            Some("succeeded") if view["report"]["status"] == "applied" => return Ok(view),
            Some("waiting") => {
                if view["preview"]["test_status"] != "matching" {
                    return Err("final application lacks matching saved tests".into());
                }
                check_original(&app.directory.join("project"))?;
                if stop_after == Some(StopAfter::ApplicationApproval) {
                    return Ok(checkpoint(app, receipt, "application_approval"));
                }
                let decision = json!({"wait_id":view["wait"]["id"],"approved":true});
                post(client, &format!("{url}/responses"), &decision).await?;
                post(client, &format!("{url}/responses"), &decision).await?;
            }
            Some("failed" | "cancelled" | "blocked" | "paused") => {
                return Err(
                    "final application needs inspection or explicit recovery; it was not retried"
                        .into(),
                );
            }
            _ => {}
        }
        if !worker.step().await? {
            return Err("final application is idle and needs explicit inspection".into());
        }
    }
    Err("final application exceeded its activity bound".into())
}
