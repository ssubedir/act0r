//! Controls for one persisted task-ownership tree, separate from individual Run controls.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalCancellationReceipt {
    pub root_task_id: MessageId,
    pub requested_task_id: MessageId,
    pub requested_at: i64,
}
impl GoalCancellationReceipt {
    pub fn new(root_task_id: MessageId, requested_task_id: MessageId, requested_at: i64) -> Self {
        Self {
            root_task_id,
            requested_task_id,
            requested_at,
        }
    }
    /// An admitted unsafe operation must settle or be reconciled before Run cancellation.
    pub fn protects_reconciliation(run: &Run) -> bool {
        run.record().status == RunStatus::Blocked
            || run.activities().iter().any(|a| {
                a.status == ActivityStatus::Running
                    && a.spec.recovery == RecoveryPolicy::RequiresReconciliation
            })
    }
    pub fn cancel_run(&self, run: &mut Run, now: i64) -> Result<(), DomainError> {
        if (run.record().status.is_unfinished() || run.needs_worker_recovery())
            && !Self::protects_reconciliation(run)
        {
            run.cancel(now.max(self.requested_at))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct GoalCancellationView {
    pub receipt: GoalCancellationReceipt,
    pub pending_tasks: u64,
    pub pending_reminders: u64,
    pub pending_intents: u64,
    pub blocked_tasks: u64,
    pub settled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalPauseReceipt {
    pub requested_task_id: MessageId,
    pub requested_at: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalPauseRecord {
    pub root_task_id: MessageId,
    pub paused: bool,
    pub revision: u64,
    pub pause: Option<GoalPauseReceipt>,
    pub resume: Option<GoalPauseReceipt>,
}
pub struct GoalPause {
    pub(crate) record: GoalPauseRecord,
}
/// The caller's observed revision, required for reversible operator decisions.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalPausePrecondition {
    pub expected_revision: u64,
}
impl GoalPause {
    pub fn new(root: MessageId) -> Self {
        Self {
            record: GoalPauseRecord {
                root_task_id: root,
                paused: false,
                revision: 0,
                pause: None,
                resume: None,
            },
        }
    }
    pub fn record(&self) -> &GoalPauseRecord {
        &self.record
    }
    /// A retry may acknowledge only the immediately following matching transition.
    /// Later controls cannot be overwritten, even when they selected the same state.
    pub fn control_if_revision(
        &mut self,
        requested: MessageId,
        paused: bool,
        precondition: GoalPausePrecondition,
        now: i64,
    ) -> Result<bool, DomainError> {
        let expected = precondition.expected_revision;
        if self.record.revision != expected {
            if self.record.paused == paused && expected.checked_add(1) == Some(self.record.revision)
            {
                return Ok(false);
            }
            return Err(DomainError::GoalRevisionConflict {
                expected_revision: expected,
                current_revision: self.record.revision,
            });
        }
        self.control(requested, paused, now)
    }
    /// Repeating the current control preserves the first receipt, including across clock changes.
    pub fn control(
        &mut self,
        requested: MessageId,
        paused: bool,
        now: i64,
    ) -> Result<bool, DomainError> {
        if self.record.paused == paused {
            return Ok(false);
        }
        self.record.revision = self
            .record
            .revision
            .checked_add(1)
            .ok_or(DomainError::InvalidState)?;
        self.record.paused = paused;
        let receipt = GoalPauseReceipt {
            requested_task_id: requested,
            requested_at: now,
        };
        if paused {
            self.record.pause = Some(receipt);
            self.record.resume = None;
        } else {
            self.record.resume = Some(receipt);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod revision_tests {
    use super::*;
    fn precondition(expected_revision: u64) -> GoalPausePrecondition {
        GoalPausePrecondition { expected_revision }
    }
    #[test]
    fn latest_transition_retries_preserve_receipts_and_older_matching_states_conflict() {
        let root = MessageId::new();
        let child = MessageId::new();
        let mut hold = GoalPause::new(root);
        assert!(
            hold.control_if_revision(child, true, precondition(0), 100)
                .unwrap()
        );
        let paused = hold.record().clone();
        assert!(
            !hold
                .control_if_revision(root, true, precondition(0), 200)
                .unwrap()
        );
        assert_eq!(hold.record(), &paused);
        assert!(
            hold.control_if_revision(root, false, precondition(1), 300)
                .unwrap()
        );
        let resumed = hold.record().clone();
        assert!(
            !hold
                .control_if_revision(child, false, precondition(1), 400)
                .unwrap()
        );
        assert_eq!(hold.record(), &resumed);
        assert!(
            hold.control_if_revision(root, true, precondition(2), 500)
                .unwrap()
        );
        let latest = hold.record().clone();
        for (expected, desired) in [(0, true), (1, false), (9, false), (u64::MAX, true)] {
            assert!(matches!(
                hold.control_if_revision(root, desired, precondition(expected), 600),
                Err(DomainError::GoalRevisionConflict {expected_revision, current_revision:3}) if expected_revision==expected
            ));
            assert_eq!(hold.record(), &latest);
        }
    }
    #[test]
    fn no_op_keeps_revision_zero_and_revision_overflow_leaves_the_hold_intact() {
        let root = MessageId::new();
        let mut hold = GoalPause::new(root);
        assert!(
            !hold
                .control_if_revision(root, false, precondition(0), 100)
                .unwrap()
        );
        assert_eq!(hold.record().revision, 0);
        assert!(hold.record().resume.is_none());
        hold.record.revision = u64::MAX;
        let before = hold.record().clone();
        assert!(matches!(
            hold.control_if_revision(root, true, precondition(u64::MAX), 200),
            Err(DomainError::InvalidState)
        ));
        assert_eq!(hold.record(), &before);
    }
}
