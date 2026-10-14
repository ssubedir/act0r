//! Create a team over HTTP, restart at saved waits, and reconnect its event stream.
use act0r::{
    Engine,
    adapters::{
        http_api::{self, ApiOptions},
        tokio_worker::{self, WorkerOptions},
    },
    application::AgentService,
    bootstrap::local_app,
    domain::{MessageId, teams::TeamSpec},
};
use reqwest::Client;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{sync::oneshot, task::JoinHandle};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
struct Api {
    url: String,
    stop: oneshot::Sender<()>,
    task: JoinHandle<std::result::Result<(), act0r::Error>>,
}
impl Api {
    async fn start(agents: Arc<AgentService>) -> Result<Self> {
        let listener = http_api::bind("127.0.0.1:0".parse()?).await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(http_api::serve(
            listener,
            agents,
            ApiOptions::default(),
            async {
                let _ = stopped.await;
                Ok(())
            },
        ));
        Ok(Self { url, stop, task })
    }
    async fn close(self) -> Result<()> {
        let _ = self.stop.send(());
        self.task.await??;
        Ok(())
    }
}
async fn post(client: &Client, url: &str, input: &impl serde::Serialize) -> Result<Value> {
    Ok(client
        .post(url)
        .json(input)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}
async fn get(client: &Client, url: &str) -> Result<Value> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}
async fn work(engine: &Engine) -> Result<()> {
    tokio_worker::run(
        engine.worker().await?,
        WorkerOptions {
            until_idle: true,
            max_concurrent_activities: 2,
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}
async fn history(client: &Client, root: &str) -> Result<(Vec<Value>, String)> {
    let mut events = Vec::new();
    let mut after = None::<String>;
    loop {
        let mut request = client
            .get(format!("{root}/events"))
            .query(&[("limit", "7")]);
        if let Some(after) = &after {
            request = request.query(&[("after", after)]);
        }
        let page: Value = request.send().await?.error_for_status()?.json().await?;
        assert!(page["oversized_event"].is_null());
        events.extend(page["events"].as_array().unwrap().iter().cloned());
        after = Some(page["next_cursor"].as_str().unwrap().into());
        if page["has_more"] == false {
            return Ok((events, after.unwrap()));
        }
    }
}
async fn first_frame(client: &Client, root: &str, cursor: &str) -> Result<Value> {
    let mut response = client
        .get(format!("{root}/events/stream"))
        .header("last-event-id", cursor)
        .send()
        .await?
        .error_for_status()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        bytes.extend_from_slice(&chunk);
        assert!(bytes.len() < 20 * 1024 * 1024);
        if let Some(end) = bytes.windows(2).position(|w| w == b"\n\n") {
            let frame = std::str::from_utf8(&bytes[..end])?;
            assert!(
                frame
                    .lines()
                    .any(|l| l == "event: page" || l == "event:page")
            );
            let page: Value = serde_json::from_str(
                frame
                    .lines()
                    .find_map(|l| l.strip_prefix("data:"))
                    .unwrap()
                    .trim_start(),
            )?;
            assert_eq!(
                frame
                    .lines()
                    .find_map(|l| l.strip_prefix("id:"))
                    .unwrap()
                    .trim_start(),
                page["next_cursor"].as_str().unwrap()
            );
            return Ok(page);
        }
    }
    Err("event stream ended before its checkpoint".into())
}
async fn respond_saved_waits(
    client: &Client,
    root: &str,
    waiting: &Value,
    replayed: bool,
) -> Result<()> {
    for item in waiting["items"].as_array().unwrap() {
        let response = match item["kind"].as_str().unwrap() {
            "question" => json!({"kind":"answer","answer":"Review durability"}),
            "tool_approval" => json!({"kind":"approval","approved":true}),
            _ => panic!("unexpected wait"),
        };
        let url = format!(
            "{root}/waits/{}/responses",
            item["wait"]["id"].as_str().unwrap()
        );
        assert_eq!(post(client, &url, &response).await?["replayed"], replayed);
        assert_eq!(post(client, &url, &response).await?["replayed"], true);
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let db = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| ".build/http-api-demo/demo.db".into()),
    );
    if let Some(parent) = db.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    let source = std::fs::canonicalize(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/team-inbox-source"),
    )?;
    let original = std::fs::read_to_string(source.join("README.md"))?;
    let mut spec: TeamSpec = serde_json::from_str(include_str!("configs/team-inbox.json"))?;
    spec.name = "http-restart-demo".into();
    spec.source = Some(source.to_string_lossy().into_owned());
    let (engine, agents) = local_app(&db).await?;
    let agents = Arc::new(agents);
    let api = Api::start(agents.clone()).await?;
    let team = post(&client, &format!("{}/v1/teams", api.url), &spec).await?;
    let team_id = team["id"].as_str().unwrap();
    let task_id = MessageId::new();
    post(
        &client,
        &format!("{}/v1/teams/{team_id}/tasks", api.url),
        &json!({"message_id":task_id,"goal":"Coordinate the reviewed edit"}),
    )
    .await?;
    work(&engine).await?;
    let root = format!("{}/v1/teams/{team_id}/tasks/{task_id}", api.url);
    let waiting = get(&client, &format!("{root}/inbox")).await?;
    assert_eq!(waiting["items"].as_array().unwrap().len(), 2);
    let (mut delivered, cursor) = history(&client, &root).await?;
    let before = delivered.len();
    assert!(
        first_frame(&client, &root, &cursor).await?["events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let cursor_file = db.with_extension("cursor");
    std::fs::write(&cursor_file, format!("{cursor}\n"))?;
    let paused = post(
        &client,
        &format!("{root}/pause"),
        &json!({"expected_revision":0}),
    )
    .await?;
    assert_eq!(paused["control"]["revision"], 1);
    let held_usage = get(&client, &root).await?["task"]["summary"]["visible_usage"].clone();
    respond_saved_waits(&client, &root, &waiting, false).await?;
    api.close().await?;
    drop(agents);
    drop(engine);

    let (engine, agents) = local_app(&db).await?;
    let api = Api::start(Arc::new(agents)).await?;
    let root = format!("{}/v1/teams/{team_id}/tasks/{task_id}", api.url);
    let restored = get(&client, &format!("{root}/inbox")).await?;
    assert_eq!(restored["goal_paused"], true);
    assert!(restored["items"].as_array().unwrap().is_empty());
    assert_eq!(
        get(&client, &format!("{root}/controls")).await?["pause"],
        paused["control"]
    );
    respond_saved_waits(&client, &root, &waiting, true).await?;
    work(&engine).await?;
    assert_eq!(
        get(&client, &root).await?["task"]["summary"]["visible_usage"],
        held_usage
    );
    let resumed = post(
        &client,
        &format!("{root}/resume"),
        &json!({"expected_revision":1}),
    )
    .await?;
    assert_eq!(resumed["control"]["revision"], 2);
    let stale = client
        .post(format!("{root}/pause"))
        .json(&json!({"expected_revision":0}))
        .send()
        .await?;
    assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        post(
            &client,
            &format!("{root}/resume"),
            &json!({"expected_revision":1})
        )
        .await?["control"],
        resumed["control"]
    );
    work(&engine).await?;
    let page = first_frame(
        &client,
        &root,
        std::fs::read_to_string(&cursor_file)?.trim(),
    )
    .await?;
    assert_eq!(page["has_more"], false);
    let missed = page["events"].as_array().unwrap();
    delivered.extend(missed.iter().cloned());
    assert_eq!(delivered, history(&client, &root).await?.0);
    let completed = get(&client, &root).await?;
    assert_eq!(completed["task"]["summary"]["succeeded"], 3);
    assert_eq!(
        completed["task"]["summary"]["visible_usage"]["admitted_model_calls"],
        6
    );
    let editor = team["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == "editor")
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(
            PathBuf::from(editor["workspace"]["path"].as_str().unwrap()).join("README.md")
        )?,
        original.replace("Draft", "Reviewed")
    );
    assert_eq!(std::fs::read_to_string(source.join("README.md"))?, original);
    std::fs::write(
        &cursor_file,
        format!("{}\n", page["next_cursor"].as_str().unwrap()),
    )?;
    let cancelled_task = MessageId::new();
    post(
        &client,
        &format!("{}/v1/teams/{team_id}/tasks", api.url),
        &json!({"message_id":cancelled_task,"goal":"Cancel a waiting review"}),
    )
    .await?;
    work(&engine).await?;
    let cancel_root = format!("{}/v1/teams/{team_id}/tasks/{cancelled_task}", api.url);
    assert_eq!(
        get(&client, &format!("{cancel_root}/inbox")).await?["items"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let cancelled_usage =
        get(&client, &cancel_root).await?["task"]["summary"]["visible_usage"].clone();
    let cancellation = post(&client, &format!("{cancel_root}/cancel"), &json!({})).await?;
    api.close().await?;
    drop(engine);
    let (engine, agents) = local_app(&db).await?;
    let api = Api::start(Arc::new(agents)).await?;
    work(&engine).await?;
    let cancel_root = format!("{}/v1/teams/{team_id}/tasks/{cancelled_task}", api.url);
    let cancelled = get(&client, &format!("{cancel_root}/controls")).await?;
    assert_eq!(
        cancelled["cancellation"]["receipt"],
        cancellation["receipt"]
    );
    assert_eq!(cancelled["cancellation"]["settled"], true);
    assert_eq!(
        get(&client, &cancel_root).await?["task"]["summary"]["visible_usage"],
        cancelled_usage
    );
    api.close().await?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"team_id":team_id,"task_id":task_id,"http_restart_verified":true,"response_receipts_restored":2,
        "goal_hold_restored":true,"no_calls_while_held":true,"stale_control_refused":true,"resumed_revision":2,
        "cancellation_survived_restart":true,"cancellation_settled":true,
        "events_before_disconnect":before,"missed_events_recovered":missed.len(),"events_total":delivered.len(),"recovered_exact_history":true,"successful_tasks":3,"model_calls":6,"cursor_file":cursor_file})
        )?
    );
    Ok(())
}
