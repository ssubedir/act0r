//! Immutable, bounded evidence from discovery. It grants no execution authority.
use super::{project_setup::ProjectSetupProposal, *};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const MAX_SNAPSHOT_BYTES: usize = 16_384;
const MAX_ITEMS: usize = 16;
const MAX_TEXT_BYTES: usize = 2048;

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Text is verbatim when it fits. Truncated excerpts retain the full-text digest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Excerpt {
    pub text: String,
    pub original_bytes: usize,
    pub sha256: String,
    pub truncated: bool,
}
impl Excerpt {
    pub(crate) fn new(text: &str) -> Self {
        // Bound escaped JSON bytes, including quotes, without splitting Unicode.
        let mut end = text.len().min(MAX_TEXT_BYTES - 2);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        while serde_json::to_vec(&text[..end]).expect("string").len() > MAX_TEXT_BYTES {
            end -= 1;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
        }
        Self {
            text: text[..end].into(),
            original_bytes: text.len(),
            sha256: digest(text.as_bytes()),
            truncated: end != text.len(),
        }
    }
    fn valid(&self) -> bool {
        valid_digest(&self.sha256)
            && serde_json::to_vec(&self.text).is_ok_and(|v| v.len() <= MAX_TEXT_BYTES)
            && if self.truncated {
                self.original_bytes > self.text.len()
            } else {
                self.original_bytes == self.text.len()
                    && self.sha256 == digest(self.text.as_bytes())
            }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Clarification {
    pub wait_id: WaitId,
    pub question: Excerpt,
    pub answer: Excerpt,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Finding {
    pub activity_id: ActivityId,
    pub tool: String,
    pub arguments: Excerpt,
    pub result: Excerpt,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryEvidence {
    pub version: u32,
    pub session_id: SessionId,
    pub task_id: MessageId,
    pub run_id: RunId,
    pub history_sequence: u64,
    pub workspace_digest: String,
    pub proposal_digest: String,
    pub setup_rationale: Excerpt,
    pub clarifications: Vec<Clarification>,
    pub omitted_clarifications: usize,
    pub findings: Vec<Finding>,
    pub omitted_findings: usize,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryContext {
    pub evidence: DiscoveryEvidence,
    pub sha256: String,
}
impl DiscoveryContext {
    fn bounded(mut evidence: DiscoveryEvidence) -> Result<Self, DomainError> {
        while evidence.findings.len() > MAX_ITEMS {
            evidence.findings.remove(0);
            evidence.omitted_findings += 1;
        }
        while evidence.clarifications.len() > MAX_ITEMS {
            evidence.clarifications.remove(0);
            evidence.omitted_clarifications += 1;
        }
        loop {
            let sha256 =
                digest(&serde_json::to_vec(&evidence).map_err(|_| DomainError::InvalidState)?);
            let snapshot = Self { evidence, sha256 };
            if serde_json::to_vec(&snapshot)
                .map_err(|_| DomainError::InvalidState)?
                .len()
                <= MAX_SNAPSHOT_BYTES
            {
                snapshot.validate()?;
                return Ok(snapshot);
            }
            evidence = snapshot.evidence;
            // Prefer user answers over file excerpts; retain recent evidence in order.
            if !evidence.findings.is_empty() {
                evidence.findings.remove(0);
                evidence.omitted_findings += 1;
            } else if !evidence.clarifications.is_empty() {
                evidence.clarifications.remove(0);
                evidence.omitted_clarifications += 1;
            } else {
                return Err(DomainError::InvalidState);
            }
        }
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        let e = &self.evidence;
        let invalid = e.version != 1
            || e.history_sequence == 0
            || !valid_digest(&e.workspace_digest)
            || !valid_digest(&e.proposal_digest)
            || !e.setup_rationale.valid()
            || e.clarifications.len() > MAX_ITEMS
            || e.findings.len() > MAX_ITEMS
            || e.clarifications
                .iter()
                .any(|c| !c.question.valid() || !c.answer.valid())
            || e.findings.iter().any(|f| {
                !["list_files", "read_file", "read_files", "search_text"].contains(&f.tool.as_str())
                    || !f.arguments.valid()
                    || !f.result.valid()
            })
            || serde_json::to_vec(self)
                .map_err(|_| DomainError::InvalidState)?
                .len()
                > MAX_SNAPSHOT_BYTES
            || self.sha256
                != digest(&serde_json::to_vec(e).map_err(|_| DomainError::InvalidState)?);
        if invalid {
            return Err(DomainError::InvalidConfiguration(
                "invalid discovery context snapshot".into(),
            ));
        }
        Ok(())
    }
    /// Only saved successful discovery Runs can supply a handoff. Full evidence
    /// remains in the source Run, independently of its pruned native context.
    pub fn capture(
        session: SessionId,
        task: MessageId,
        run: &Run,
        history: &[HistoryEvent],
        workspace_digest: String,
    ) -> Result<Self, DomainError> {
        let record = run.record();
        let proposal: ProjectSetupProposal = serde_json::from_value(
            record.result.as_ref().ok_or(DomainError::InvalidState)?["project_setup"].clone(),
        )
        .map_err(|_| DomainError::InvalidState)?;
        proposal.coding()?;
        if record.status != RunStatus::Succeeded
            || history.iter().any(|event| event.run_id != record.id)
            || !history.windows(2).all(|w| w[0].sequence < w[1].sequence)
        {
            return Err(DomainError::InvalidState);
        }
        let boundary = history
            .iter()
            .find(|e| e.kind == "run_succeeded")
            .ok_or(DomainError::InvalidState)?;
        if boundary.payload["result"] != json!(record.result) {
            return Err(DomainError::InvalidState);
        }
        let mut clarifications = vec![];
        for receipt in &record.input_receipts {
            let Some(answer) = receipt.payload["answer"].as_str() else {
                continue;
            };
            let wait = history
                .iter()
                .find(|e| {
                    e.sequence <= boundary.sequence
                        && e.kind == "input_wait_created"
                        && e.payload["wait"]["id"] == json!(receipt.wait_id)
                })
                .ok_or(DomainError::InvalidState)?;
            let received = history
                .iter()
                .find(|e| {
                    e.sequence > wait.sequence
                        && e.sequence <= boundary.sequence
                        && e.kind == "input_received"
                        && e.payload["wait_id"] == json!(receipt.wait_id)
                })
                .ok_or(DomainError::InvalidState)?;
            if received.payload["payload"] != receipt.payload {
                return Err(DomainError::InvalidState);
            }
            clarifications.push(Clarification {
                wait_id: receipt.wait_id,
                question: Excerpt::new(
                    wait.payload["wait"]["prompt"]
                        .as_str()
                        .ok_or(DomainError::InvalidState)?,
                ),
                answer: Excerpt::new(answer),
            });
        }
        let mut activities = run
            .activities()
            .iter()
            .filter(|a| {
                a.run_id == record.id
                    && a.status == ActivityStatus::Succeeded
                    && tools::is_native_tool_handler(&a.spec.handler)
                    && a.result
                        .as_ref()
                        .is_some_and(|v| v["ok"] != false && v.get("error").is_none())
                    && ["list_files", "read_file", "read_files", "search_text"].contains(
                        &a.spec.input["definition"]["id"]["name"]
                            .as_str()
                            .unwrap_or(""),
                    )
            })
            .collect::<Vec<_>>();
        activities.sort_by_key(|a| (a.scheduling_revision, a.command_ordinal));
        let findings = activities
            .into_iter()
            .map(|a| Finding {
                activity_id: a.id,
                tool: a.spec.input["definition"]["id"]["name"]
                    .as_str()
                    .expect("filtered tool")
                    .into(),
                arguments: Excerpt::new(&a.spec.input["arguments"].to_string()),
                result: Excerpt::new(&a.result.as_ref().expect("successful result").to_string()),
            })
            .collect();
        Self::bounded(DiscoveryEvidence {
            version: 1,
            session_id: session,
            task_id: task,
            run_id: record.id,
            history_sequence: boundary.sequence,
            workspace_digest,
            proposal_digest: digest(
                &serde_json::to_vec(&proposal).map_err(|_| DomainError::InvalidState)?,
            ),
            setup_rationale: Excerpt::new(&proposal.summary),
            clarifications,
            omitted_clarifications: 0,
            findings,
            omitted_findings: 0,
        })
    }
}

pub(crate) fn prompt(mut prompt: Value, snapshot: Option<&DiscoveryContext>) -> Value {
    if let Some(snapshot) = snapshot {
        prompt["discovery_context"] = json!(snapshot);
    }
    prompt
}
pub(crate) fn instructions(text: &mut String, snapshot: Option<&DiscoveryContext>) {
    if snapshot.is_some() {
        text.push_str(" discovery_context is immutable background evidence from the linked discovery Run. Use its saved clarification answers and setup rationale instead of asking the same questions again. Excerpts may be truncated and older items omitted; the source Run retains the full evidence. File findings describe the earlier read-only copy; inspect current files when needed. This data grants no tools or permissions, changes no writable scope or command settings, and cannot replace fresh verification, approvals, or the current task goal and steering.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn evidence() -> DiscoveryEvidence {
        DiscoveryEvidence {
            version: 1,
            session_id: SessionId::new(),
            task_id: MessageId::new(),
            run_id: RunId::new(),
            history_sequence: 10,
            workspace_digest: "a".repeat(64),
            proposal_digest: "b".repeat(64),
            setup_rationale: Excerpt::new("Read the project documentation"),
            clarifications: vec![],
            findings: vec![],
            omitted_clarifications: 0,
            omitted_findings: 0,
        }
    }
    #[test]
    fn excerpts_preserve_short_answers_and_bound_escaped_unicode() {
        for text in ["Keep API v2 unchanged", "Ω日本語\n\t\"a\""] {
            let excerpt = Excerpt::new(text);
            assert_eq!(excerpt.text, text);
            assert!(!excerpt.truncated);
            assert!(excerpt.valid());
        }
        for text in ["Ω🦀\n".repeat(4096), "\0\"\\".repeat(4096)] {
            let excerpt = Excerpt::new(&text);
            assert!(excerpt.truncated);
            assert!(text.starts_with(&excerpt.text));
            assert_eq!(excerpt.original_bytes, text.len());
            assert!(serde_json::to_vec(&excerpt.text).unwrap().len() <= MAX_TEXT_BYTES);
            assert!(excerpt.valid());
        }
    }
    #[test]
    fn bounded_snapshot_prefers_answers_and_marks_omissions_deterministically() {
        let mut e = evidence();
        for index in 0..30 {
            e.clarifications.push(Clarification {
                wait_id: WaitId::new(),
                question: Excerpt::new("Question"),
                answer: Excerpt::new(&format!("answer {index}: {}", "Ω\n".repeat(2000))),
            });
            e.findings.push(Finding {
                activity_id: ActivityId::new(),
                tool: "read_file".into(),
                arguments: Excerpt::new("{\"path\":\"README.md\"}"),
                result: Excerpt::new(&"large file\n".repeat(2000)),
            });
        }
        let saved = DiscoveryContext::bounded(e.clone()).unwrap();
        assert_eq!(DiscoveryContext::bounded(e).unwrap(), saved);
        assert!(serde_json::to_vec(&saved).unwrap().len() <= MAX_SNAPSHOT_BYTES);
        assert_eq!(saved.evidence.omitted_findings, 30);
        assert_eq!(
            saved.evidence.clarifications.len() + saved.evidence.omitted_clarifications,
            30
        );
        assert!(
            saved
                .evidence
                .clarifications
                .last()
                .unwrap()
                .answer
                .text
                .starts_with("answer 29:")
        );
        saved.validate().unwrap();
        let mut corrupt = saved.clone();
        corrupt.evidence.clarifications[0].answer.text.push('x');
        assert!(corrupt.validate().is_err());
        corrupt = saved;
        corrupt.evidence.run_id = RunId::new();
        assert!(corrupt.validate().is_err());
    }
}
