//! Durable model allowances shared by delegated Runs and continuing goals.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedBudgetKind {
    #[default]
    Delegation,
    Goal,
}
impl SharedBudgetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delegation => "delegation",
            Self::Goal => "goal",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SharedBudgetExhaustion {
    pub run_id: RunId,
    pub recorded_at: i64,
    pub reason: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SharedBudgetRecord {
    pub root_run_id: RunId,
    #[serde(default)]
    pub kind: SharedBudgetKind,
    pub usage: UsageLedger,
    pub exhaustion: Option<SharedBudgetExhaustion>,
    pub created_at: i64,
    pub updated_at: i64,
}
#[derive(Clone, Debug)]
pub struct SharedBudget {
    pub(crate) record: SharedBudgetRecord,
}
impl SharedBudget {
    pub fn new(root_run_id: RunId, budget: UsageBudget, now: i64) -> Result<Self, DomainError> {
        Self::with_kind(root_run_id, SharedBudgetKind::Delegation, budget, now)
    }
    pub fn with_kind(
        root_run_id: RunId,
        kind: SharedBudgetKind,
        budget: UsageBudget,
        now: i64,
    ) -> Result<Self, DomainError> {
        budget.validate()?;
        Ok(Self {
            record: SharedBudgetRecord {
                root_run_id,
                kind,
                usage: UsageLedger {
                    budget: Some(budget),
                    ..Default::default()
                },
                exhaustion: None,
                created_at: now,
                updated_at: now,
            },
        })
    }
    pub fn record(&self) -> &SharedBudgetRecord {
        &self.record
    }
    /// Run admission and this check belong to the same atomic unit of work.
    pub fn permits_admission(&self, before: &RunRecord, proposed: &RunRecord) -> bool {
        if before.id != proposed.id {
            return false;
        }
        if proposed.usage.admitted_model_calls == before.usage.admitted_model_calls {
            return true;
        }
        let Some(calls) = proposed
            .usage
            .admitted_model_calls
            .checked_sub(before.usage.admitted_model_calls)
        else {
            return false;
        };
        let Some(tokens) = proposed
            .usage
            .charged_tokens()
            .checked_sub(before.usage.charged_tokens())
        else {
            return false;
        };
        self.record.exhaustion.is_none()
            && self.record.usage.budget.as_ref().is_some_and(|budget| {
                budget.max_model_calls.is_none_or(|limit| {
                    self.record
                        .usage
                        .admitted_model_calls
                        .checked_add(calls)
                        .is_some_and(|n| n <= limit)
                }) && budget.max_tokens.is_none_or(|limit| {
                    self.record
                        .usage
                        .charged_tokens()
                        .checked_add(tokens)
                        .is_some_and(|n| n <= limit)
                })
            })
    }
    pub(crate) fn exhaust(&mut self, run_id: RunId, now: i64, reason: &str) {
        if self.record.exhaustion.is_none() {
            self.record.exhaustion = Some(SharedBudgetExhaustion {
                run_id,
                recorded_at: now,
                reason: reason.into(),
            });
            self.record.updated_at = now;
        }
    }
    /// Sum authoritative member ledgers once; child receipts never add a second charge.
    pub(crate) fn refresh(
        &mut self,
        runs: &[RunRecord],
        changed: RunId,
        now: i64,
    ) -> Result<(), DomainError> {
        let root = runs
            .iter()
            .find(|r| r.id == self.record.root_run_id)
            .ok_or(DomainError::InvalidState)?;
        let pinned = match self.record.kind {
            SharedBudgetKind::Delegation => root
                .delegation_budget
                .as_ref()
                .and_then(|d| d.shared_budget.as_ref()),
            SharedBudgetKind::Goal => root.goal_budget.as_ref(),
        };
        if pinned != self.record.usage.budget.as_ref() {
            return Err(DomainError::InvalidState);
        }
        let mut usage = UsageLedger {
            budget: self.record.usage.budget.clone(),
            ..Default::default()
        };
        let mut seen = std::collections::HashSet::new();
        for run in runs {
            if !seen.insert(run.id) {
                return Err(DomainError::InvalidState);
            }
            usage.admitted_model_calls = usage
                .admitted_model_calls
                .saturating_add(run.usage.admitted_model_calls);
            usage.actual_input_tokens = usage
                .actual_input_tokens
                .saturating_add(run.usage.actual_input_tokens);
            usage.actual_output_tokens = usage
                .actual_output_tokens
                .saturating_add(run.usage.actual_output_tokens);
            usage.held_tokens = usage.held_tokens.saturating_add(run.usage.held_tokens);
            usage.uncertain_calls = usage
                .uncertain_calls
                .saturating_add(run.usage.uncertain_calls);
        }
        let budget = usage.budget.as_ref().ok_or(DomainError::InvalidState)?;
        let over = budget.exceeded(usage.admitted_model_calls, usage.charged_tokens());
        self.record.usage = usage;
        self.record.updated_at = now;
        if over {
            self.exhaust(changed, now, "reported_usage_exceeded");
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct SharedBudgetMember {
    pub run_id: RunId,
    pub agent_id: Option<AgentId>,
    pub task_id: Option<MessageId>,
    pub status: RunStatus,
    pub usage: UsageLedger,
}
#[derive(Clone, Debug, Serialize)]
pub struct SharedBudgetView {
    pub record: SharedBudgetRecord,
    pub members: Vec<SharedBudgetMember>,
    pub remaining_model_calls: Option<u32>,
    pub remaining_tokens: Option<u64>,
}
