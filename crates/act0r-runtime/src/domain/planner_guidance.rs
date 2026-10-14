//! A bounded guidance decision within the planner's existing child join.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const WAKE_KEY: &str = "planner_guidance";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GuidanceTarget {
    pub name: String,
    pub task_id: MessageId,
    pub run_id: RunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause: Option<planner_pause::GuidancePause>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlannerGuidance {
    pub instructions: Vec<steering::SteeringInstruction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<worker_messages::PeerInstruction>,
    pub targets: Vec<GuidanceTarget>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<planner_pause::WorkerDependency>,
}
impl PlannerGuidance {
    pub(crate) fn validate(&self) -> Result<(), DomainError> {
        if self.targets.is_empty()
            || self.targets.len() > 2
            || (self.instructions.is_empty()
                && self.findings.is_empty()
                && self.dependencies.is_empty())
            || self.dependencies.len() > 2
            || self.dependencies.iter().enumerate().any(|(i, d)| {
                d.validate().is_err()
                    || d.closed
                    || d.outcome.is_none()
                    || d.review_timer_id.is_none()
                    || !self.targets.iter().any(|t| {
                        t.name == d.worker && t.task_id == d.task_id && t.run_id == d.run_id
                    })
                    || self.dependencies[..i]
                        .iter()
                        .any(|p| p.activity_id == d.activity_id || p.task_id == d.task_id)
            })
            || self.instructions.len() > steering::MAX_MESSAGES
            || self.findings.len() > worker_messages::MAX_MESSAGES
            || self.findings.iter().enumerate().any(|(i, e)| {
                !e.needs_coordination
                    || e.question_id.is_some()
                    || e.sequence == 0
                    || !matches!(e.sender.as_str(), "coder" | "coder2")
                    || e.text.trim().is_empty()
                    || e.text.len() > worker_messages::MAX_TEXT_BYTES
                    || self.findings[..i]
                        .iter()
                        .any(|p| p.sequence >= e.sequence || p.message_id == e.message_id)
            })
            || self.targets.iter().enumerate().any(|(i, t)| {
                !matches!(t.name.as_str(), "coder" | "coder2")
                    || t.pause.as_ref().is_some_and(|p| {
                        p.revision > 9_007_199_254_740_991 || p.planner_paused && p.revision == 0
                    })
                    || self.targets[..i]
                        .iter()
                        .any(|p| p.name == t.name || p.task_id == t.task_id || p.run_id == t.run_id)
            })
            || self.instructions.iter().enumerate().any(|(i, e)| {
                e.sequence == 0
                    || e.text.trim().is_empty()
                    || e.text.len() > steering::MAX_TEXT_BYTES
                    || self.instructions[..i]
                        .iter()
                        .any(|p| p.sequence >= e.sequence || p.message_id == e.message_id)
            })
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuideWorkers {
    pub coder: Option<String>,
    pub coder2: Option<String>,
}
impl GuideWorkers {
    pub fn selected(&self) -> impl Iterator<Item = (&str, &str)> {
        [
            ("coder", self.coder.as_deref()),
            ("coder2", self.coder2.as_deref()),
        ]
        .into_iter()
        .filter_map(|(name, text)| text.map(|text| (name, text)))
    }
    pub fn validate(&self) -> Result<(), tools::ToolError> {
        self.selected().try_for_each(|(name, text)| {
            worker_messages::SendMessage {
                recipient: name.into(),
                text: text.into(),
                needs_coordination: false,
            }
            .validate()
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GuidanceReceipt {
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub session_id: SessionId,
    pub turn_id: MessageId,
    pub request: GuideWorkers,
    pub deliveries: Vec<worker_messages::WorkerMessageReceipt>,
    pub saved_at: i64,
}
impl Run {
    pub(crate) fn dependencies_allowed(&self) -> bool {
        self.pending_delegation().is_some()
            && !self.record.controls.cancel_requested
            && self
                .steering_native_state()
                .is_ok_and(|(s, _)| s["config"]["worker_dependencies"] == true)
    }
    pub(crate) fn queue_dependency_wake(
        &mut self,
        activity_id: ActivityId,
        now: i64,
    ) -> Result<(), DomainError> {
        if !self.dependencies_allowed() {
            return Err(DomainError::InvalidState);
        }
        self.timers.push(DurableTimer {
            id: TimerId::new(),
            run_id: self.record.id,
            scheduling_revision: self.record.revision,
            command_ordinal: self.timers.len() as u32,
            due_at: now,
            status: TimerStatus::Scheduled,
            fired_at: None,
            payload: json!({"key":WAKE_KEY,"dependency_id":activity_id}),
        });
        self.event(
            "planner_dependency_ready",
            json!({"dependency_id":activity_id}),
            None,
            None,
            now,
        );
        Ok(())
    }
    pub(crate) fn worker_pauses_allowed(&self) -> bool {
        self.steering_native_state()
            .is_ok_and(|(s, _)| s["config"]["worker_pause_limit"].as_u64().unwrap_or(0) > 0)
    }
    pub(crate) fn findings_allowed(&self) -> bool {
        self.pending_delegation().is_some()
            && !self.record.controls.cancel_requested
            && self
                .steering_native_state()
                .is_ok_and(|(s, _)| planner::accepts_findings(s))
    }
    fn finding_in_current_join(&self, task: MessageId) -> bool {
        self.pending_delegation().is_some_and(|join| {
            self.record.delegations.iter().any(|d| {
                d.id == task
                    && (d.id == join.id || d.group_id.is_some() && d.group_id == join.group_id)
            })
        })
    }
    /// The note and this wake are saved with the recipient Run in the same transaction.
    pub(crate) fn queue_finding_wake(
        &mut self,
        sequence: u64,
        now: i64,
    ) -> Result<(), DomainError> {
        if !self.findings_allowed() {
            return Err(DomainError::SessionConflict(
                "This planner does not accept coordination findings during its current child join."
                    .into(),
            ));
        }
        self.timers.push(DurableTimer {
            id: TimerId::new(),
            run_id: self.record.id,
            scheduling_revision: self.record.revision,
            command_ordinal: self.timers.len() as u32,
            due_at: now,
            status: TimerStatus::Scheduled,
            fired_at: None,
            payload: json!({"key":WAKE_KEY,"finding_sequence":sequence}),
        });
        self.event(
            "planner_finding_queued",
            json!({"sequence":sequence}),
            None,
            None,
            now,
        );
        Ok(())
    }
    pub(crate) fn queue_guidance_wake(&mut self, sequence: u64, now: i64) {
        if self.pending_delegation().is_none()
            || !self
                .steering_native_state()
                .is_ok_and(|(s, _)| planner::accepts_guidance(s))
        {
            return;
        }
        self.timers.push(DurableTimer {
            id: TimerId::new(),
            run_id: self.record.id,
            scheduling_revision: self.record.revision,
            command_ordinal: self.timers.len() as u32,
            due_at: now,
            status: TimerStatus::Scheduled,
            fired_at: None,
            payload: json!({"key":WAKE_KEY,"sequence":sequence}),
        });
        self.event(
            "planner_guidance_queued",
            json!({"sequence":sequence}),
            None,
            None,
            now,
        );
    }
    pub(crate) fn prepare_guidance_timer(
        &mut self,
        id: TimerId,
        targets: Vec<GuidanceTarget>,
        dependencies: Vec<planner_pause::WorkerDependency>,
    ) {
        let through = self
            .timers
            .iter()
            .filter(|t| t.status == TimerStatus::Scheduled && t.payload["key"] == WAKE_KEY)
            .filter_map(|t| t.payload["sequence"].as_u64())
            .max()
            .unwrap_or(0);
        let findings_through = self
            .timers
            .iter()
            .filter(|t| t.status == TimerStatus::Scheduled && t.payload["key"] == WAKE_KEY)
            .filter_map(|t| t.payload["finding_sequence"].as_u64())
            .max()
            .unwrap_or(0);
        let guidance = PlannerGuidance {
            instructions: steering::instructions(&self.record.steering)
                .into_iter()
                .filter(|i| {
                    i.sequence > self.record.guidance_handled_through && i.sequence <= through
                })
                .collect(),
            findings: worker_messages::instructions(&self.record.peer_messages)
                .into_iter()
                .filter(|i| {
                    i.needs_coordination
                        && i.sequence > self.record.findings_handled_through
                        && i.sequence <= findings_through
                        && self.finding_in_current_join(i.sender_task_id)
                })
                .collect(),
            targets,
            dependencies,
        };
        if let Some(t) = self
            .timers
            .iter_mut()
            .find(|t| t.id == id && t.payload["key"] == WAKE_KEY)
        {
            t.payload["guidance"] = json!(guidance);
            t.payload["through"] = json!(through);
            t.payload["findings_through"] = json!(findings_through);
        }
    }
}
