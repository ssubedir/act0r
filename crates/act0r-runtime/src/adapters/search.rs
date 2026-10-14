use crate::{
    application::ports::*,
    domain::{search::*, *},
};
use std::sync::Arc;

pub struct FeedbackCostStrategy;
impl SearchStrategy for FeedbackCostStrategy {
    fn id(&self) -> DefinitionId {
        DefinitionId::new("feedback_cost", 1)
    }
    fn select(&self, search: &SearchRecord) -> Result<Option<RunId>, DomainError> {
        if search.nodes.iter().any(|n| n.result.is_none()) {
            return Err(DomainError::InvalidState);
        }
        Ok(search
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.result.as_ref().unwrap().feedback_passed)
            .min_by_key(|(i, n)| {
                let r = n.result.as_ref().unwrap();
                (r.charged_tokens, r.model_calls, r.attempts, *i)
            })
            .map(|(_, n)| n.run_id))
    }
}
pub struct SearchExecutor {
    proposals: Arc<dyn ProposalClient>,
    fallback: Arc<dyn ActivityExecutor>,
}
impl SearchExecutor {
    pub fn new(proposals: Arc<dyn ProposalClient>, fallback: Arc<dyn ActivityExecutor>) -> Self {
        Self {
            proposals,
            fallback,
        }
    }
}
impl ActivityExecutor for SearchExecutor {
    fn request_timeout_ms(&self, execution: &ActivityExecution) -> Option<u64> {
        if execution.handler == DefinitionId::new("search.propose", 1) {
            let request: ProposalRequest = serde_json::from_value(execution.input.clone()).ok()?;
            (request.model.target.provider == "openai")
                .then_some(u64::from(request.model.target.policy.request_timeout_ms))
        } else {
            self.fallback.request_timeout_ms(execution)
        }
    }
    fn supports(&self, handler: &DefinitionId) -> bool {
        *handler == DefinitionId::new("search.propose", 1)
            || *handler == DefinitionId::new("search.action", 1)
            || self.fallback.supports(handler)
    }
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            if execution.handler == DefinitionId::new("search.action", 1) {
                return Ok(Outcome::Success(execution.input));
            }
            if execution.handler != DefinitionId::new("search.propose", 1) {
                return self.fallback.execute(execution).await;
            }
            let request: ProposalRequest = serde_json::from_value(execution.input)?;
            let reply = self
                .proposals
                .propose(request.clone(), execution.activity_id)
                .await?;
            let mut diagnostics = reply.diagnostics;
            let result = reply.actions.and_then(|actions| {
                validate_proposals(&actions, request.width)
                    .map(|()| serde_json::json!({"actions": actions}))
                    .map_err(|e| e.to_string())
            });
            if result.is_err()
                && let Some(diagnostics) = &mut diagnostics
                && diagnostics.category == RequestCategory::Success
            {
                diagnostics.category = RequestCategory::InvalidAction;
            }
            let code = diagnostics
                .as_ref()
                .map_or("proposal_response", |d| d.category.failure_code());
            let result = result.map(|mut result| {
                if let Some(diagnostics) = &diagnostics {
                    result["diagnostics"] = serde_json::to_value(diagnostics).unwrap();
                }
                result
            });
            Ok(match (result, reply.usage) {
                (Ok(result), Some(usage)) => Outcome::SuccessWithUsage { result, usage },
                (Ok(result), None) => Outcome::Success(result),
                (Err(message), Some(usage)) => Outcome::FailureWithUsage {
                    failure: Failure::new(code, message, reply.retryable)
                        .with_request(diagnostics, reply.retry_after),
                    usage,
                },
                (Err(message), None) => Outcome::Failure(
                    Failure::new(code, message, reply.retryable)
                        .with_request(diagnostics, reply.retry_after),
                ),
            })
        })
    }
}
