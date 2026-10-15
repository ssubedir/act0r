//! Concise terminal projections of committed goal history; never drives execution.
use super::Output;
use crate::{
    Error,
    application::{TeamEventService, TeamTimelineEvent},
    domain::{
        ActivityId, MessageId, conversation::ContinuousConversation, harness::HarnessReceipt,
        timeline::*,
    },
};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};

const RECAP_LIMIT: usize = 24;
const PAGES_PER_POLL: usize = 4;

#[derive(Default)]
pub(super) struct ActivityFeed {
    goals: Vec<GoalFeed>,
    observed: HashSet<MessageId>,
    recap: bool,
}
struct GoalFeed {
    scope: HarnessReceipt,
    cursor: Option<EventCursor>,
    tools: HashMap<ActivityId, Tool>,
    models: HashSet<ActivityId>,
    recap_until: Option<u64>,
    recent: VecDeque<Value>,
    retired: bool,
    disabled: bool,
}
struct Tool {
    name: String,
    label: String,
}
impl ActivityFeed {
    pub(super) fn new(resuming: bool) -> Self {
        Self {
            recap: resuming,
            ..Default::default()
        }
    }
    /// Return whether unread pages remain, so piped EOF drains the feed before exit.
    pub(super) async fn refresh(
        &mut self,
        service: &TeamEventService,
        conversation: &ContinuousConversation,
        output: &Output,
    ) -> Result<bool, Error> {
        let scope = &conversation.active;
        if self.recap {
            // Reopening shows the active goal's recap rather than replaying every old turn.
            self.observed.extend(
                conversation
                    .turns
                    .iter()
                    .filter(|turn| turn.turn.settled)
                    .map(|turn| turn.turn.task_id),
            );
            self.add(scope.clone(), true);
            self.observed.insert(scope.task_id);
            self.recap = false;
        }
        for turn in &conversation.turns {
            if self.observed.insert(turn.turn.task_id) {
                self.add(
                    HarnessReceipt {
                        session_id: turn.session_id,
                        team_id: turn.team_id,
                        task_id: turn.turn.task_id,
                        sequence: turn.turn.sequence,
                    },
                    false,
                );
            }
        }
        if !self
            .goals
            .iter()
            .any(|goal| goal.scope.task_id == scope.task_id)
        {
            self.observed.insert(scope.task_id);
            self.add(scope.clone(), false);
        }
        let mut pending = false;
        for goal in &mut self.goals {
            if goal.retired || goal.disabled {
                continue;
            }
            let more = goal.poll(service, output).await?;
            pending |= more;
            let settled = conversation
                .turns
                .iter()
                .find(|turn| turn.turn.task_id == goal.scope.task_id)
                .is_none_or(|turn| turn.turn.settled);
            if !more && settled && goal.scope.task_id != scope.task_id {
                goal.retired = true;
                goal.tools.clear();
                goal.models.clear();
            }
        }
        Ok(pending)
    }
    fn add(&mut self, scope: HarnessReceipt, recap: bool) {
        self.goals.push(GoalFeed {
            scope,
            cursor: None,
            tools: HashMap::new(),
            models: HashSet::new(),
            recap_until: recap.then_some(u64::MAX),
            recent: VecDeque::new(),
            retired: false,
            disabled: false,
        });
    }
}
impl GoalFeed {
    async fn poll(&mut self, service: &TeamEventService, output: &Output) -> Result<bool, Error> {
        let mut max_bytes = 1024 * 1024;
        for _ in 0..PAGES_PER_POLL {
            let page = service
                .read(
                    self.scope.team_id,
                    self.scope.task_id,
                    EventQuery {
                        after: self.cursor,
                        limit: 128,
                        max_bytes,
                    },
                )
                .await?;
            if self.recap_until == Some(u64::MAX) {
                self.recap_until = Some(page.snapshot_position);
            }
            for item in &page.events {
                if self.recap_until.is_some_and(|until| item.position > until) {
                    self.finish_recap(output)?;
                }
                if let Some(update) = self.project(item) {
                    if self.recap_until.is_some() {
                        if self.recent.len() == RECAP_LIMIT {
                            self.recent.pop_front();
                        }
                        self.recent.push_back(update);
                    } else {
                        emit(output, &update, false)?;
                    }
                }
            }
            // Advance only after successfully emitting/retaining the complete page.
            self.cursor = Some(page.next_cursor);
            if let Some(oversized) = page.oversized_event {
                if oversized.required_bytes <= u64::from(MAX_EVENT_PAGE_BYTES) {
                    max_bytes = oversized.required_bytes as u32;
                    continue;
                }
                self.finish_recap(output)?;
                output.emit("activity_unavailable", json!({"task_id": self.scope.task_id, "event_id": oversized.event_id}),
                    "Activity display stopped at an oversized saved event; work remains available through run history.")?;
                self.disabled = true;
                return Ok(false);
            }
            if self
                .recap_until
                .is_some_and(|until| page.next_cursor.position() >= until)
            {
                self.finish_recap(output)?;
            }
            if !page.has_more {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn finish_recap(&mut self, output: &Output) -> Result<(), Error> {
        if self.recap_until.take().is_some() {
            for update in self.recent.drain(..) {
                emit(output, &update, true)?;
            }
        }
        Ok(())
    }
    fn project(&mut self, item: &TeamTimelineEvent) -> Option<Value> {
        let event = &item.event;
        let payload = &event.payload;
        let id = event.activity_id;
        let mut tool_name = None;
        let (status, message) = match event.kind.as_str() {
            "activity_scheduled" => {
                if payload["handler"]["name"] == "native.tool" {
                    let input = &payload["input"];
                    let name =
                        concise(input["definition"]["id"]["name"].as_str().unwrap_or("tool"));
                    let arguments = &input["arguments"];
                    let target = arguments["path"]
                        .as_str()
                        .or_else(|| arguments["name"].as_str());
                    let action = match name.as_str() {
                        "read_file" | "read_files" => "Reading files",
                        "list_files" => "Listing files",
                        "search_text" => "Searching project",
                        "replace_text" | "apply_patch" => "Editing files",
                        "run_tests" => "Running tests",
                        "run_command" => "Running project command",
                        _ if input["definition"]["external"].is_object() => "MCP tool",
                        _ => "Tool",
                    };
                    let label = format!(
                        "{action}: {name}{}",
                        target.map_or(String::new(), |value| format!(" · {}", concise(value)))
                    );
                    self.tools.insert(id?, Tool { name, label });
                }
                return None;
            }
            "attempt_admitted" => {
                let attempt = payload["ordinal"].as_u64().unwrap_or(1);
                let label = if payload["handler"]["name"] == "agent.model" {
                    self.models.insert(id?);
                    "Requesting model decision".to_owned()
                } else {
                    let tool = self.tools.get(&id?)?;
                    tool_name = Some(tool.name.clone());
                    tool.label.clone()
                };
                if attempt > 1 {
                    ("retrying", format!("{label} (attempt {attempt})"))
                } else {
                    ("started", label)
                }
            }
            "attempt_finished" => {
                if !id.is_some_and(|id| self.models.contains(&id) || self.tools.contains_key(&id)) {
                    return None;
                }
                let tool = id.and_then(|id| self.tools.get(&id));
                tool_name = tool.map(|tool| tool.name.clone());
                let label = tool.map_or("Model request", |tool| tool.label.as_str());
                let outcome = &payload["outcome"];
                match outcome["kind"].as_str()? {
                    "success" | "success_with_usage" => {
                        let result = if outcome["kind"] == "success" {
                            &outcome["value"]
                        } else {
                            &outcome["value"]["result"]
                        };
                        if let Some(id) = id {
                            self.models.remove(&id);
                        }
                        let tool = tool?;
                        let failed = result["ok"] == false
                            || (tool.name == "run_tests"
                                && result["exit_code"].as_i64().is_some_and(|code| code != 0));
                        let message = if tool.name == "run_tests" && result["exit_code"].is_number()
                        {
                            if failed {
                                "Tests failed".to_owned()
                            } else {
                                "Tests passed".to_owned()
                            }
                        } else {
                            format!(
                                "{label} — {}",
                                if failed {
                                    "reported a failure"
                                } else {
                                    "completed"
                                }
                            )
                        };
                        if let Some(id) = id {
                            self.tools.remove(&id);
                        }
                        (if failed { "failed" } else { "completed" }, message)
                    }
                    "failure" | "failure_with_usage" => {
                        let failure = if outcome["kind"] == "failure" {
                            &outcome["value"]
                        } else {
                            &outcome["value"]["failure"]
                        };
                        (
                            "failed",
                            format!(
                                "{label} — failed ({})",
                                concise(failure["code"].as_str().unwrap_or("error"))
                            ),
                        )
                    }
                    "uncertain" => (
                        "uncertain",
                        format!("{label} — outcome uncertain; reconciliation required"),
                    ),
                    _ => return None,
                }
            }
            "delegation_requested" => (
                "delegated",
                format!(
                    "Assigned work to {}",
                    concise(
                        payload["delegation"]["recipient"]
                            .as_str()
                            .unwrap_or("worker")
                    )
                ),
            ),
            "delegation_completed" => (
                "worker_finished",
                format!(
                    "Worker task {}: {}",
                    payload["task_id"].as_str().unwrap_or("unknown"),
                    concise(payload["outcome"]["status"].as_str().unwrap_or("finished"))
                ),
            ),
            _ => return None,
        };
        Some(
            json!({"event_id":event.id,"position":item.position,"task_id":item.task_id,
            "run_id":event.run_id,"activity_id":id,"attempt_id":event.attempt_id,
            "at":event.at,"actor_id":item.actor_id,"member":item.member,"kind":event.kind,"status":status,
            "tool":tool_name,"message":message}),
        )
    }
}
fn concise(value: &str) -> String {
    // Treat all names and paths as untrusted terminal text. Never print arguments/results.
    let mut text = String::new();
    for ch in value.chars().take(160) {
        for escaped in ch.escape_debug() {
            text.push(escaped);
        }
    }
    if value.chars().nth(160).is_some() {
        text.push('…');
    }
    text
}
fn emit(output: &Output, update: &Value, replayed: bool) -> Result<(), Error> {
    let mut data = update.clone();
    data["replayed"] = json!(replayed);
    let member = concise(update["member"].as_str().unwrap_or("worker"));
    output.emit(
        "activity",
        data,
        &format!(
            "{}[{member}] {}",
            if replayed { "Recent · " } else { "" },
            update["message"].as_str().unwrap_or("Activity")
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{AgentId, AttemptId, EventId, HistoryEvent, RunId, SessionId, TeamId};

    fn feed() -> GoalFeed {
        let mut feed = ActivityFeed::default();
        feed.add(
            HarnessReceipt {
                session_id: SessionId::new(),
                team_id: TeamId::new(),
                task_id: MessageId::new(),
                sequence: 1,
            },
            false,
        );
        feed.goals.pop().unwrap()
    }
    fn event(activity: ActivityId, kind: &str, payload: Value) -> TeamTimelineEvent {
        TeamTimelineEvent {
            position: 1,
            task_id: MessageId::new(),
            actor_id: AgentId::new(),
            member: Some("worker-1".into()),
            event: HistoryEvent {
                id: EventId::new(),
                run_id: RunId::new(),
                sequence: 1,
                kind: kind.into(),
                payload_version: 1,
                payload,
                activity_id: Some(activity),
                attempt_id: Some(AttemptId::new()),
                at: 1,
            },
        }
    }
    #[test]
    fn interleaved_worker_tools_keep_identity_and_do_not_expose_contents_or_terminal_controls() {
        let mut feed = feed();
        let mcp = ActivityId::new();
        let tests = ActivityId::new();
        feed.project(&event(
            mcp,
            "activity_scheduled",
            json!({"handler":{"name":"native.tool"},"input":{
            "definition":{"id":{"name":"mcp_files_read_text_file"},"external":{}},
            "arguments":{"path":"src/\u{1b}[2J\nmodule.rs", "secret":"never-print"}}}),
        ));
        feed.project(&event(
            tests,
            "activity_scheduled",
            json!({"handler":{"name":"native.tool"},"input":{
            "definition":{"id":{"name":"run_tests"}},"arguments":{}}}),
        ));
        let start = feed
            .project(&event(
                mcp,
                "attempt_admitted",
                json!({"handler":{"name":"native.tool"},"ordinal":1}),
            ))
            .unwrap();
        assert!(start["message"].as_str().unwrap().contains("MCP tool"));
        assert!(!start.to_string().contains("never-print"));
        assert!(
            !start["message"]
                .as_str()
                .unwrap()
                .contains(['\u{1b}', '\n'])
        );
        feed.project(&event(
            tests,
            "attempt_admitted",
            json!({"handler":{"name":"native.tool"},"ordinal":1}),
        ));
        let failure = feed
            .project(&event(
                mcp,
                "attempt_finished",
                json!({"outcome":{"kind":"success","value":{"ok":false,"content":"never-print"}}}),
            ))
            .unwrap();
        assert_eq!(failure["tool"], "mcp_files_read_text_file");
        assert_eq!(failure["status"], "failed");
        assert!(!failure.to_string().contains("never-print"));
        let passed_event = event(
            tests,
            "attempt_finished",
            json!({"outcome":{"kind":"success","value":{"ok":true,"exit_code":0,"stdout":"never-print"}}}),
        );
        let passed = feed.project(&passed_event).unwrap();
        assert_eq!(passed["message"], "Tests passed");
        assert_eq!(passed["activity_id"], tests.to_string());
        assert_eq!(passed["task_id"], passed_event.task_id.to_string());
        assert_eq!(passed["actor_id"], passed_event.actor_id.to_string());
        assert_eq!(passed["event_id"], passed_event.event.id.to_string());
        assert!(!passed.to_string().contains("never-print"));
        let failed_test = ActivityId::new();
        feed.project(&event(
            failed_test,
            "activity_scheduled",
            json!({"handler":{"name":"native.tool"},"input":{
            "definition":{"id":{"name":"run_tests"}},"arguments":{}}}),
        ));
        let failed = feed
            .project(&event(
                failed_test,
                "attempt_finished",
                json!({"outcome":{"kind":"success","value":{
            "ok":false,"exit_code":1,"stdout":"never-print"}}}),
            ))
            .unwrap();
        assert_eq!(failed["message"], "Tests failed");
        assert_eq!(failed["status"], "failed");
    }
    #[test]
    fn retries_only_appear_on_admission_and_unknown_handlers_are_not_model_requests() {
        let mut feed = feed();
        let model = ActivityId::new();
        let started = feed
            .project(&event(
                model,
                "attempt_admitted",
                json!({"handler":{"name":"agent.model"},"ordinal":1}),
            ))
            .unwrap();
        assert_eq!(started["status"], "started");
        let failure = feed
            .project(&event(
                model,
                "attempt_finished",
                json!({"outcome":{"kind":"failure_with_usage","value":{
            "failure":{"code":"model_http_status","retryable":true,"message":"never-print"}}}}),
            ))
            .unwrap();
        assert_eq!(failure["status"], "failed");
        assert!(!failure.to_string().contains("never-print"));
        let retry = feed
            .project(&event(
                model,
                "attempt_admitted",
                json!({"handler":{"name":"agent.model"},"ordinal":2}),
            ))
            .unwrap();
        assert_eq!(retry["status"], "retrying");
        let unknown = event(
            ActivityId::new(),
            "attempt_finished",
            json!({"outcome":{"kind":"failure","value":{"code":"unrelated"}}}),
        );
        assert!(feed.project(&unknown).is_none());
        let uncertain = feed
            .project(&event(
                model,
                "attempt_finished",
                json!({"outcome":{"kind":"uncertain","value":"never-print"}}),
            ))
            .unwrap();
        assert_eq!(uncertain["status"], "uncertain");
        assert!(!uncertain.to_string().contains("never-print"));
    }
}
