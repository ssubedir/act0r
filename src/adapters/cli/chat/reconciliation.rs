//! Explicit operator decisions for saved MCP attempts; no service calls from this UI.
use super::*;
use crate::{
    application::ToolRecoveries,
    domain::{AttemptId, tool_recovery::*},
};

pub(super) fn show(
    view: &ToolRecoveries,
    displayed: &mut Displayed,
    output: &Output,
    force: bool,
) -> Result<(), Error> {
    for item in &view.items {
        if let Some(recovery) = &item.recovery
            && external_action(&recovery.request)
            && (force || displayed.tool_recoveries.insert(recovery.attempt_id))
        {
            let arguments = recovery
                .request
                .definition
                .external
                .as_ref()
                .unwrap()
                .arguments(&recovery.request.arguments);
            let mut data = json!(item);
            data["arguments"] = arguments.clone();
            output.emit("tool_recovery", data, &format!(
                "\nMCP action outcome unknown: {}\n{}\nRun {} · attempt {}\nArguments:\n{}\nInspect the service and confirm the previous operation stopped before deciding.\nUse /reconcile {} retry --stopped EVIDENCE, fail --stopped EVIDENCE, or accept --stopped RESULT_JSON -- EVIDENCE.{}{}",
                recovery.request.definition.id.name, diagnostic(&recovery.request.definition.description), item.run_id, recovery.attempt_id,
                serde_json::to_string_pretty(&arguments)?, recovery.attempt_id,
                if recovery.paused {"\nWork is paused; a saved decision will not release the pause."} else {""},
                if recovery.retry_allowed {""} else {"\nRetry is unavailable; inspect the saved limits and pinned tool contract."},
            ))?;
        }
        for receipt in &item.reconciliation_receipts {
            if force
                || displayed
                    .reconciliation_receipts
                    .insert(receipt.request.attempt_id)
            {
                output.emit(
                    "reconciliation",
                    json!({"task_id":item.task_id,"run_id":item.run_id,"receipt":receipt}),
                    &format!(
                        "Saved reconciliation for attempt {}: {}.",
                        receipt.request.attempt_id,
                        match receipt.request.decision {
                            crate::domain::ReconciliationDecision::Accept { .. } =>
                                "verified result accepted",
                            crate::domain::ReconciliationDecision::Retry {} => "retry authorized",
                            crate::domain::ReconciliationDecision::Fail { .. } => "marked failed",
                        }
                    ),
                )?;
            }
        }
    }
    if force
        && !view.items.iter().any(|item| {
            item.recovery
                .as_ref()
                .is_some_and(|r| external_action(&r.request))
        })
    {
        output.note("No MCP actions currently require reconciliation.")?;
    }
    if view.truncated {
        output.note("Recovery list is truncated; inspect run status for older owned work.")?;
    }
    Ok(())
}

fn usage() -> Error {
    Error::Unavailable("Usage: /reconcile ATTEMPT_ID retry|fail --stopped EVIDENCE, or /reconcile ATTEMPT_ID accept --stopped RESULT_JSON -- EVIDENCE. Inspect the service first; --stopped confirms the previous operation has stopped.".into())
}
fn word<'a>(text: &mut &'a str) -> Option<&'a str> {
    *text = text.trim_start();
    let end = text.find(char::is_whitespace).unwrap_or(text.len());
    if end == 0 {
        return None;
    }
    let token = &text[..end];
    *text = &text[end..];
    Some(token)
}
fn parse(mut text: &str) -> Result<(AttemptId, ToolRecoveryDecision, String), Error> {
    let attempt_id: AttemptId = word(&mut text)
        .ok_or_else(usage)?
        .parse()
        .map_err(|_| usage())?;
    let action = word(&mut text).ok_or_else(usage)?;
    if word(&mut text) != Some("--stopped") {
        return Err(usage());
    }
    let (decision, evidence) = match action {
        "retry" => (ToolRecoveryDecision::Retry {}, text.trim()),
        "fail" => (
            ToolRecoveryDecision::Fail {
                message: text.trim().into(),
            },
            text.trim(),
        ),
        "accept" => {
            let mut stream =
                serde_json::Deserializer::from_str(text.trim_start()).into_iter::<Value>();
            let result = stream.next().ok_or_else(usage)?.map_err(|_|Error::Unavailable("Verified result must be one complete JSON value, followed by -- and inspection evidence.".into()))?;
            let trailing = text.trim_start()[stream.byte_offset()..].trim_start();
            let evidence = trailing
                .strip_prefix("--")
                .filter(|rest| rest.starts_with(char::is_whitespace))
                .ok_or_else(usage)?
                .trim();
            (ToolRecoveryDecision::Accept { result }, evidence)
        }
        _ => return Err(usage()),
    };
    Ok((attempt_id, decision, evidence.into()))
}

pub(super) async fn handle(
    text: &str,
    agents: &AgentService,
    scope: &HarnessReceipt,
    displayed: &mut Displayed,
    output: &Output,
) -> Result<(), Error> {
    let teams = agents.teams()?;
    let harness = teams.harness()?;
    let view = harness
        .tool_recoveries(scope.session_id, scope.task_id)
        .await?;
    if text.trim().is_empty() {
        return show(&view, displayed, output, true);
    }
    let (attempt_id, decision, evidence) = parse(text)?;
    let mut target = None;
    for item in &view.items {
        if let Some(recovery) = &item.recovery
            && recovery.attempt_id == attempt_id
            && external_action(&recovery.request)
        {
            if matches!(decision, ToolRecoveryDecision::Retry {}) && !recovery.retry_allowed {
                return Err(Error::Unavailable("Retry is unavailable for this saved attempt; restore its pinned contract and inspect the saved limits.".into()));
            }
            target = Some((item.run_id, recovery.activity_id));
            break;
        }
        if let Some(receipt) = item
            .reconciliation_receipts
            .iter()
            .find(|r| r.request.attempt_id == attempt_id)
        {
            target = Some((item.run_id, receipt.request.activity_id));
            break;
        }
    }
    let (run, activity_id) = target.ok_or_else(||Error::Unavailable("No owned MCP recovery or saved decision matches this attempt. Use /reconcile to inspect current work.".into()))?;
    let request = ToolRecoveryRequest {
        activity_id,
        attempt_id,
        decision,
        evidence,
        operation_stopped: true,
    };
    harness
        .reconcile_tool(scope.session_id, scope.task_id, run, request.clone())
        .await?;
    displayed.responses.remove(&scope.task_id);
    displayed.progress.clear();
    displayed.reconciliation_turn = None;
    output.emit("reconciliation_saved", json!({"task_id":scope.task_id,"run_id":run,"request":request}), "Reconciliation saved for the exact interrupted attempt. Existing pauses and limits still apply.")
}
