use serde::{Deserialize, Serialize};

/// Each absent/null limit is unlimited; numeric limits remain pinned and positive.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageBudget {
    pub max_model_calls: Option<u32>,
    pub max_tokens: Option<u64>,
}
impl UsageBudget {
    pub fn validate(&self) -> Result<(), super::DomainError> {
        if self.max_model_calls == Some(0) || self.max_tokens == Some(0) {
            return Err(super::DomainError::InvalidConfiguration(
                "usage limits must be positive numbers or null for unlimited".into(),
            ));
        }
        Ok(())
    }
    pub fn exceeded(&self, calls: u32, tokens: u64) -> bool {
        self.max_model_calls.is_some_and(|limit| calls > limit)
            || self.max_tokens.is_some_and(|limit| tokens > limit)
    }
    pub fn remaining_model_calls(&self, usage: &UsageLedger) -> Option<u32> {
        self.max_model_calls
            .map(|limit| limit.saturating_sub(usage.admitted_model_calls))
    }
    pub fn remaining_tokens(&self, usage: &UsageLedger) -> Option<u64> {
        self.max_tokens
            .map(|limit| limit.saturating_sub(usage.charged_tokens()))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelReservation {
    pub input_tokens: u64,
    pub output_tokens: u32,
}
impl ModelReservation {
    pub fn total(&self) -> u64 {
        self.input_tokens
            .saturating_add(u64::from(self.output_tokens))
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}
impl TokenUsage {
    pub fn total(&self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UsageLedger {
    pub budget: Option<UsageBudget>,
    pub admitted_model_calls: u32,
    pub actual_input_tokens: u64,
    pub actual_output_tokens: u64,
    pub held_tokens: u64,
    pub uncertain_calls: u32,
}
impl UsageLedger {
    /// A branch retains its prefix for per-run limits; shared tasks count only its suffix.
    pub(crate) fn since(&self, prefix: &Self) -> Option<Self> {
        Some(Self {
            budget: self.budget.clone(),
            admitted_model_calls: self
                .admitted_model_calls
                .checked_sub(prefix.admitted_model_calls)?,
            actual_input_tokens: self
                .actual_input_tokens
                .checked_sub(prefix.actual_input_tokens)?,
            actual_output_tokens: self
                .actual_output_tokens
                .checked_sub(prefix.actual_output_tokens)?,
            held_tokens: self.held_tokens.checked_sub(prefix.held_tokens)?,
            uncertain_calls: self.uncertain_calls.checked_sub(prefix.uncertain_calls)?,
        })
    }
    pub fn charged_tokens(&self) -> u64 {
        self.actual_input_tokens
            .saturating_add(self.actual_output_tokens)
            .saturating_add(self.held_tokens)
    }
    pub fn can_reserve(&self, reservation: &ModelReservation) -> bool {
        self.budget.as_ref().is_some_and(|b| {
            b.max_model_calls
                .is_none_or(|limit| self.admitted_model_calls < limit)
                && b.max_tokens.is_none_or(|limit| {
                    self.charged_tokens()
                        .checked_add(reservation.total())
                        .is_some_and(|total| total <= limit)
                })
        })
    }
    pub(crate) fn reserve(&mut self, reservation: &ModelReservation) {
        self.admitted_model_calls = self.admitted_model_calls.saturating_add(1);
        self.held_tokens = self.held_tokens.saturating_add(reservation.total());
    }
    pub(crate) fn reconcile(
        &mut self,
        reservation: &ModelReservation,
        usage: Option<&TokenUsage>,
    ) -> bool {
        let Some(usage) = usage else {
            self.uncertain_calls = self.uncertain_calls.saturating_add(1);
            return false;
        };
        self.held_tokens = self.held_tokens.saturating_sub(reservation.total());
        self.actual_input_tokens = self.actual_input_tokens.saturating_add(usage.input_tokens);
        self.actual_output_tokens = self
            .actual_output_tokens
            .saturating_add(usage.output_tokens);
        usage.input_tokens > reservation.input_tokens
            || usage.output_tokens > u64::from(reservation.output_tokens)
            || self.budget.as_ref().is_some_and(|b| {
                b.max_tokens
                    .is_some_and(|limit| self.charged_tokens() > limit)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independent_limits_and_unlimited_usage_keep_uncertain_reservations() {
        let reservation = ModelReservation {
            input_tokens: 20,
            output_tokens: 10,
        };
        for (budget, admitted) in [
            (UsageBudget::default(), true),
            (
                UsageBudget {
                    max_model_calls: Some(2),
                    max_tokens: None,
                },
                false,
            ),
            (
                UsageBudget {
                    max_model_calls: None,
                    max_tokens: Some(100),
                },
                true,
            ),
            (
                UsageBudget {
                    max_model_calls: None,
                    max_tokens: Some(99),
                },
                false,
            ),
        ] {
            let mut usage = UsageLedger {
                budget: Some(budget),
                admitted_model_calls: 2,
                held_tokens: 70,
                ..Default::default()
            };
            assert_eq!(usage.can_reserve(&reservation), admitted);
            if admitted {
                usage.reserve(&reservation);
                assert!(!usage.reconcile(&reservation, None));
                assert_eq!(usage.held_tokens, 100);
                assert_eq!(usage.uncertain_calls, 1);
                let restored: UsageLedger =
                    serde_json::from_value(serde_json::to_value(&usage).unwrap()).unwrap();
                assert_eq!(restored.budget, usage.budget);
                assert_eq!(restored.charged_tokens(), 100);
            }
        }
        assert!(!UsageLedger::default().can_reserve(&reservation));
    }

    #[test]
    fn unlimited_counters_saturate_without_creating_a_limit_or_wrapping() {
        let mut usage = UsageLedger {
            budget: Some(UsageBudget::default()),
            admitted_model_calls: u32::MAX,
            held_tokens: u64::MAX,
            uncertain_calls: u32::MAX,
            ..Default::default()
        };
        let reservation = ModelReservation {
            input_tokens: 100,
            output_tokens: 100,
        };
        assert!(usage.can_reserve(&reservation));
        usage.reserve(&reservation);
        assert!(!usage.reconcile(&reservation, None));
        assert_eq!(usage.admitted_model_calls, u32::MAX);
        assert_eq!(usage.held_tokens, u64::MAX);
        assert_eq!(usage.uncertain_calls, u32::MAX);
        assert!(usage.can_reserve(&reservation));
    }
}
