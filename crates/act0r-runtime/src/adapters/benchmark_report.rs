use crate::domain::benchmark::BenchmarkRecord;
use std::fmt::Write;

pub fn markdown(record: &BenchmarkRecord) -> String {
    fn cell(value: &str) -> String {
        value.replace('|', "\\|").replace(['\r', '\n'], " ")
    }
    let mut result = format!(
        "# Benchmark: {}\n\nID: `{}`  \nStatus: `{:?}`  \nModel: {} / {}\n\n",
        cell(&record.config.name),
        record.id,
        record.status,
        cell(&record.config.agent.target.provider),
        cell(&record.config.agent.target.model)
    );
    let budget = &record.config.budget;
    let _ = writeln!(
        result,
        "Each trial has the same caps: {} attempts, {} model calls, {} tokens. Repetitions: {}. Actual usage can differ between arms.\n",
        budget.max_attempts, budget.max_model_calls, budget.max_tokens, record.config.repetitions
    );
    if let Some(search) = &record.config.search {
        let _ = writeln!(
            result,
            "Bounded search: width {}, warmup {} tool steps, rollout {} tool steps. Proposal calls and shared-prefix work count toward the same caps.\n",
            search.width, search.warmup_steps, search.rollout_steps
        );
        if search.version == 2 {
            result.push_str("Search v2 checks stale feedback at rollout boundaries. Each automatic check consumes an activity attempt without another model call.\n\n");
        }
    }
    result.push_str("Summary includes completed trials. Charged tokens include held uncertain reservations. Wall time includes preparation and downtime.\n\n| Strategy | Completed | Successes | Success rate | Model calls | Charged tokens | Held tokens | Attempts | Mean wall ms |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
    for arm in &record.summary.arms {
        let _ = writeln!(
            result,
            "| {} | {}/{} | {} | {} | {} | {} | {} | {} | {} |",
            arm.strategy.label(arm.candidates),
            arm.completed,
            arm.trials,
            arm.successes,
            arm.success_rate
                .map_or_else(|| "pending".into(), |v| format!("{:.1}%", v * 100.0)),
            arm.model_calls,
            arm.charged_tokens,
            arm.held_tokens,
            arm.attempts,
            arm.mean_elapsed_ms
                .map_or_else(|| "pending".into(), |v| format!("{v:.1}"))
        );
    }
    result.push_str("\n## Cases\n\n| Case | Strategy | Completed | Successes | Success rate | Model calls | Charged tokens | Held tokens | Attempts | Mean wall ms | Baseline SHA-256 | Final tests SHA-256 |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
    for case in &record.summary.cases {
        for arm in &case.arms {
            let _ = writeln!(
                result,
                "| {} | {} | {}/{} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                cell(&case.name),
                arm.strategy.label(arm.candidates),
                arm.completed,
                arm.trials,
                arm.successes,
                arm.success_rate
                    .map_or_else(|| "pending".into(), |v| format!("{:.1}%", v * 100.0)),
                arm.model_calls,
                arm.charged_tokens,
                arm.held_tokens,
                arm.attempts,
                arm.mean_elapsed_ms
                    .map_or_else(|| "pending".into(), |v| format!("{v:.1}")),
                case.baseline_digest,
                case.final_tests_digest
            );
        }
    }
    result.push_str("\n## Trials and selected patches\n");
    for trial in &record.trials {
        let case = &record.config.cases[trial.case_index as usize];
        let _ = writeln!(
            result,
            "\n### Trial {}: {}, repetition {}, {}\n\nTask: `{}`  \nStatus: `{:?}`\n",
            trial.ordinal + 1,
            cell(&case.name),
            trial.repetition + 1,
            trial.strategy.label(trial.candidates),
            trial.task_id,
            trial.status
        );
        if let Some(metrics) = &trial.metrics {
            let _ = writeln!(
                result,
                "Success: {}. Attempts: {}. Calls: {}. Reported input/output tokens: {}/{}. Held tokens: {}. Wall ms: {}.\n",
                metrics.success,
                metrics.attempts,
                metrics.usage.admitted_model_calls,
                metrics.usage.actual_input_tokens,
                metrics.usage.actual_output_tokens,
                metrics.usage.held_tokens,
                metrics.elapsed_ms
            );
            if let Some(error) = &metrics.error {
                let _ = writeln!(result, "Diagnostic: {}\n", cell(error));
            }
            for failure in &metrics.failures {
                let _ = writeln!(
                    result,
                    "Run `{}`: {}: {}\n",
                    failure.run_id,
                    cell(&failure.failure.code),
                    cell(&failure.failure.message)
                );
                if let Some(request) = &failure.failure.request {
                    let _ = writeln!(
                        result,
                        "Request: {:?}. Latency: {} ms. HTTP status: {}. Provider request ID: {}.\n",
                        request.category,
                        request.elapsed_ms,
                        request
                            .http_status
                            .map_or_else(|| "none".into(), |v| v.to_string()),
                        cell(request.request_id.as_deref().unwrap_or("none"))
                    );
                }
            }
            if let Some(search) = &metrics.search {
                let _ = writeln!(
                    result,
                    "Search: `{}`. Policy: {} v{}. Phase: {:?}. Verified: {}.\n\n| Branch | Action | Visits | Reward | Feedback passed | New calls | New charged tokens | New attempts |\n| --- | --- | --- | --- | --- | --- | --- | --- |",
                    search.id,
                    cell(&search.config.strategy.name),
                    search.config.strategy.version,
                    search.phase,
                    search.verified
                );
                for node in &search.nodes {
                    if let Some(score) = &node.result {
                        let _ = writeln!(
                            result,
                            "| {} | {} | {} | {} | {} | {} | {} | {} |",
                            node.run_id,
                            cell(&serde_json::to_string(&node.action).unwrap()),
                            node.visits,
                            node.reward,
                            score.feedback_passed,
                            score.model_calls,
                            score.charged_tokens,
                            score.attempts
                        );
                    }
                }
                result.push('\n');
                for node in &search.nodes {
                    if let Some(feedback) = node
                        .result
                        .as_ref()
                        .and_then(|score| score.automatic_feedback.as_ref())
                    {
                        let _ = writeln!(
                            result,
                            "\nBranch `{}` automatic feedback: {:?}. Activity: {}. Fixed tests exit code: {}.\n",
                            node.run_id,
                            feedback.phase,
                            feedback
                                .activity_id
                                .map_or_else(|| "none".into(), |id| id.to_string()),
                            feedback.result.as_ref().map_or_else(
                                || "none".into(),
                                |result| result["exit_code"].to_string()
                            )
                        );
                    }
                    if let Some(error) = node
                        .result
                        .as_ref()
                        .and_then(|score| score.terminal_error.as_ref())
                    {
                        let _ = writeln!(
                            result,
                            "\nBranch `{}` diagnostic: {}\n",
                            node.run_id,
                            cell(error)
                        );
                    }
                }
                result.push('\n');
            }
            if let Some(candidate) = &metrics.selected {
                let _ = writeln!(
                    result,
                    "Selected run: `{}`  \nWorkspace: {}\n",
                    candidate.run_id,
                    cell(&candidate.workspace)
                );
                if let Some(evaluation) = &candidate.evaluation {
                    let _ = writeln!(
                        result,
                        "Candidate SHA-256: `{}`. Fixed tests exit code: {}.\n",
                        evaluation.candidate_digest, evaluation.tests["exit_code"]
                    );
                    for file in &evaluation.diff {
                        for line in file.patch.lines() {
                            let _ = writeln!(result, "    {line}");
                        }
                        result.push('\n');
                    }
                }
            }
        }
    }
    result
}
