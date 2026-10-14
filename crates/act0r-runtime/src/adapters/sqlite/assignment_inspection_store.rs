use super::*;
use crate::domain::{assignment_inspection::*, tools::*};
use serde_json::json;

fn output(receipt: InspectionReceipt) -> Result<NativeToolResult, Error> {
    if receipt.result["ok"] == false {
        Ok(NativeToolResult::Rejected(serde_json::from_value(
            receipt.result["error"].clone(),
        )?))
    } else {
        Ok(NativeToolResult::Success(receipt.result))
    }
}
fn refused(code: &str, message: &str) -> serde_json::Value {
    json!({"ok":false,"error":ToolError::new(code,message)})
}
impl AssignmentInspectionPort for SqliteStore {
    fn inspect_assignment(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
        Box::pin(async move {
            let request: InspectAssignment = serde_json::from_value(call.arguments.clone())?;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            let saved: Option<String> = sqlx::query_scalar(
                "SELECT payload FROM harness_assignment_inspections WHERE activity_id=?",
            )
            .bind(call.activity_id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
            if let Some(saved) = saved {
                let receipt: InspectionReceipt = serde_json::from_str(&saved)?;
                if receipt.run_id != call.run_id || receipt.request != request {
                    return Err(DomainError::ConflictingMessage.into());
                }
                tx.commit().await.map_err(storage)?;
                return output(receipt);
            }
            let source = Self::load(&mut tx, call.run_id).await?;
            let activity = source
                .activities()
                .iter()
                .find(|a| a.id == call.activity_id && a.status == ActivityStatus::Running)
                .ok_or(DomainError::InvalidState)?;
            let pinned: NativeToolRequest = serde_json::from_value(activity.spec.input.clone())?;
            let state = if source.record().state["config"].is_object() {
                &source.record().state
            } else {
                &source.record().state["state"]
            };
            if !planner::assignment_inspection_admitted(state, &pinned.definition, &call.arguments)
                || pinned.arguments != call.arguments
                || activity.active_attempt != Some(call.attempt_id)
                || activity.spec.handler != pinned.definition.handler()
                || call.workspace.is_some()
                || !call.writable_paths.is_empty()
                || call.test_command.is_some()
                || call.project_command.is_some()
                || !source.attempts().iter().any(|a| {
                    a.id == call.attempt_id
                        && a.activity_id == call.activity_id
                        && a.status == AttemptStatus::Running
                })
            {
                return Err(DomainError::InvalidState.into());
            }
            let root: String =
                sqlx::query_scalar("SELECT request_id FROM agent_runs WHERE run_id=?")
                    .bind(call.run_id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(storage)?;
            let root: MessageId = root.parse().map_err(|_| DomainError::InvalidState)?;
            let session: Option<String> =
                sqlx::query_scalar("SELECT session_id FROM harness_turns WHERE task_id=?")
                    .bind(root.to_string())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage)?;
            let result = if let Some(session) = session {
                let session: SessionId = session.parse().map_err(|_| DomainError::InvalidState)?;
                Self::assignment_snapshot(&mut tx, &source, session, root, &request).await?
            } else {
                refused(
                    "assignment_scope",
                    "Inspection requires a harness planner's current coding join.",
                )
            };
            let receipt = InspectionReceipt {
                activity_id: call.activity_id,
                run_id: call.run_id,
                request,
                result,
            };
            sqlx::query("INSERT INTO harness_assignment_inspections(activity_id,run_id,payload) VALUES(?,?,?)")
                .bind(call.activity_id.to_string()).bind(call.run_id.to_string()).bind(serde_json::to_string(&receipt)?)
                .execute(&mut *tx).await.map_err(storage)?;
            tx.commit().await.map_err(storage)?;
            output(receipt)
        })
    }
}
impl SqliteStore {
    async fn assignment_snapshot(
        conn: &mut SqliteConnection,
        source: &Run,
        session: SessionId,
        root: MessageId,
        request: &InspectAssignment,
    ) -> Result<serde_json::Value, Error> {
        if !matches!(request.worker.as_str(), "coder" | "coder2") {
            return Ok(refused(
                "assignment_worker",
                "Choose coder or coder2 in the current suspended join.",
            ));
        }
        if source.record().controls.cancel_requested
            || Self::goal_receipt_for_task(conn, root).await?.is_some()
        {
            return Ok(refused(
                "assignment_stopped",
                "The goal is stopping; no new inspection was captured.",
            ));
        }
        let team_id: String = sqlx::query_scalar("SELECT team_id FROM harness_sessions WHERE id=?")
            .bind(session.to_string())
            .fetch_one(&mut *conn)
            .await
            .map_err(storage)?;
        let team = Self::load_team(
            conn,
            team_id.parse().map_err(|_| DomainError::InvalidState)?,
        )
        .await?;
        let Some(member) = team.members.iter().find(|m| m.name == request.worker) else {
            return Ok(refused(
                "assignment_unavailable",
                "That worker is not in this session.",
            ));
        };
        let (task_id, expected_run) = if let Some(guidance) = &source.record().planner_guidance {
            let target = guidance
                .targets
                .iter()
                .find(|t| t.name == request.worker)
                .map(|t| (t.task_id, Some(t.run_id)))
                .or_else(|| {
                    guidance
                        .dependencies
                        .iter()
                        .find(|d| d.prerequisite == request.worker)
                        .map(|d| (d.prerequisite_task_id, Some(d.prerequisite_run_id)))
                });
            let Some(target) = target else {
                return Ok(refused(
                    "assignment_unavailable",
                    "That worker was not bound to this guidance step.",
                ));
            };
            target
        } else if let Some(question) = &source.record().planner_question {
            let requester = source
                .record()
                .delegations
                .iter()
                .find(|d| d.id == question.child_task_id)
                .ok_or(DomainError::InvalidState)?;
            let target = source.record().delegations.iter().find(|d| {
                d.recipient == member.agent_id
                    && if let Some(group) = requester.group_id {
                        d.group_id == Some(group)
                    } else {
                        d.id == requester.id
                    }
            });
            let Some(target) = target else {
                return Ok(refused(
                    "assignment_unavailable",
                    "That worker is outside the current question's suspended join.",
                ));
            };
            (target.id, None)
        } else {
            return Err(DomainError::InvalidState.into());
        };
        let (task, delegation, run) =
            Self::harness_worker_assignment(conn, session, root, task_id).await?;
        if task.record().agent_id != member.agent_id {
            return Err(DomainError::InvalidState.into());
        }
        let Some(run) = run else {
            return Ok(refused(
                "assignment_unstarted",
                "That exact assignment has not started.",
            ));
        };
        if expected_run.is_some_and(|id| id != run.record().id) {
            return Err(DomainError::InvalidState.into());
        }
        // Verify the saved native coding binding, including its handoff, even after completion.
        let actor = Self::load_actor(conn, task.record().agent_id).await?;
        let pinned: actors::RegisteredAgentConfig =
            serde_json::from_value(actor.record().config.clone())?;
        let binding = run.task_inspection_binding(task.record(), &pinned).ok();
        if binding.as_ref().is_none_or(|(input, _)| {
            input.parent_run_id != source.record().id
                || delegation.workspace_input.as_ref() != Some(input)
                || !run.activities().iter().any(|a| {
                    a.spec.handler == workspace_handoff::handler("verify")
                        && a.spec.input == json!(input)
                        && a.status == ActivityStatus::Succeeded
                        && a.result == Some(json!({"verified":true}))
                })
        }) {
            return Ok(refused(
                "assignment_unavailable",
                "This assignment has no verified native coding binding.",
            ));
        }
        let r = run.record();
        let state = if r.state["config"].is_object() {
            &r.state
        } else {
            &r.state["state"]
        };
        let completed: Vec<_> = run
            .activities()
            .iter()
            .filter(|a| {
                tools::is_native_tool_handler(&a.spec.handler)
                    && a.status == ActivityStatus::Succeeded
            })
            .collect();
        let results: Vec<_> = completed.iter().rev().take(3).map(|a| json!({
            "activity_id":a.id,"tool":a.spec.input["definition"]["id"],"result":snippet(&json!(a.result).to_string(), 768)
        })).collect();
        let messages: Vec<_> = r.peer_messages.iter().rev().take(4).map(|m| json!({
            "message_id":m.message_id,"sender":m.sender,"sender_task_id":m.sender_task_id,"question_id":m.question_id,
            "sequence":m.sequence,"application":m.application,"text":snippet(&m.text,384)
        })).collect();
        let sent: Vec<String> = sqlx::query_scalar("SELECT json_object('message_id',message_id,'recipient',json_extract(payload,'$.request.recipient'),'text',substr(json_extract(payload,'$.request.text'),1,384),'text_bytes',length(CAST(json_extract(payload,'$.request.text') AS BLOB)),'error',json_extract(payload,'$.error')) FROM harness_worker_messages WHERE sender_task_id=? AND sender_run_id=? ORDER BY rowid DESC LIMIT 5")
            .bind(task_id.to_string()).bind(r.id.to_string()).fetch_all(&mut *conn).await.map_err(storage)?;
        let sent_more = sent.len() > 4;
        let sent = sent
            .into_iter()
            .take(4)
            .map(|s| serde_json::from_str::<serde_json::Value>(&s))
            .collect::<Result<Vec<_>, _>>()?;
        let sent: Vec<_> = sent
            .into_iter()
            .map(|mut m| {
                let text = m["text"].as_str().unwrap_or("");
                let mut preview = snippet(text, 384);
                preview["bytes"] = m["text_bytes"].clone();
                preview["truncated"] = json!(
                    preview["text"].as_str().unwrap().len() as u64
                        != m["text_bytes"].as_u64().unwrap_or(0)
                );
                m["text"] = preview;
                m.as_object_mut().unwrap().remove("text_bytes");
                m
            })
            .collect();
        let question = if let Some(wait) = &r.worker_reply_wait {
            let saved: String = sqlx::query_scalar("SELECT payload FROM harness_worker_questions WHERE question_id=? AND requester_run_id=?")
                .bind(wait.question_id.to_string()).bind(r.id.to_string()).fetch_one(&mut *conn).await.map_err(storage)?;
            let q: worker_questions::WorkerQuestion = serde_json::from_str(&saved)?;
            Some(
                json!({"question_id":q.question_id,"recipient":q.recipient,"recipient_task_id":q.recipient_task_id,"deadline_at":q.deadline_at,"text":snippet(&q.text,512),"resolved":q.outcome.is_some()}),
            )
        } else {
            None
        };
        let wait = r.wait.as_ref().map(|w| json!({"wait_id":w.id,"kind":HumanWaitKind::for_key(&w.key),"key":w.key,"prompt":snippet(&w.prompt,512),"tool":w.payload["tool"]["name"]}));
        let mut snapshot = json!({"ok":true,"assignment":{
            "worker":request.worker,"task_id":task_id,"run_id":r.id,"history_sequence":r.history_sequence,
            "captured_at":crate::adapters::system_clock::SystemClock.now_ms(),"goal":snippet(&delegation.goal,512),
            "status":actors::AgentTaskStatus::from_run(r),"task_status":task.record().status,
            "held":Self::task_execution_held(conn,task_id).await?,"controls":r.controls,
            "pending_wait":wait,"worker_reply_wait":r.worker_reply_wait,"pending_worker_question":question,"needs_recovery":run.needs_worker_recovery(),
            "test_status":state["context"]["test_status"],"recent_results":results,"results_omitted":completed.len().saturating_sub(3),
            "received_messages":messages,"messages_omitted":r.peer_messages.len().saturating_sub(4),"sent_messages":sent,"sent_messages_truncated":sent_more,
            "in_flight":run.activities().iter().filter(|a| a.status==ActivityStatus::Running).map(|a| json!({"activity_id":a.id,"handler":a.spec.handler,"tool":a.spec.input["definition"]["id"]})).collect::<Vec<_>>(),
            "evidence_truncated":false
        }});
        if let Some(hold) = Self::planner_hold_for_task(conn, task_id).await? {
            snapshot["assignment"]["planner_pause"] = json!({"paused":hold.paused,"revision":hold.revision,"planner_run_id":hold.planner_run_id,"activity_id":hold.activity_id});
        }
        if source.record().planner_guidance.as_ref().is_some_and(|g| {
            g.dependencies
                .iter()
                .any(|d| d.prerequisite_task_id == task_id)
        }) && let Some(outcome) = source
            .record()
            .delegations
            .iter()
            .find(|d| d.id == task_id)
            .and_then(|d| d.outcome.as_ref())
        {
            snapshot["assignment"]["terminal_outcome"] = json!({
                "status":outcome["status"],
                "result":if outcome["result"].is_null(){serde_json::Value::Null}else{snippet(&outcome["result"].to_string(),1024)},
                "error":if outcome["error"].is_null(){serde_json::Value::Null}else{snippet(&outcome["error"].to_string(),1024)}
            });
        }
        // Escaping can expand excerpts; retain identities/status even for pathological text.
        while serde_json::to_vec(&snapshot)?.len() > MAX_OUTPUT_BYTES {
            snapshot["assignment"]["evidence_truncated"] = json!(true);
            let key = ["sent_messages", "received_messages", "recent_results"]
                .into_iter()
                .find(|k| {
                    snapshot["assignment"][k]
                        .as_array()
                        .is_some_and(|a| !a.is_empty())
                })
                .ok_or(DomainError::InvalidState)?;
            snapshot["assignment"][key].as_array_mut().unwrap().pop();
            match key {
                "recent_results" => {
                    snapshot["assignment"]["results_omitted"] =
                        json!(snapshot["assignment"]["results_omitted"].as_u64().unwrap() + 1)
                }
                "received_messages" => {
                    snapshot["assignment"]["messages_omitted"] =
                        json!(snapshot["assignment"]["messages_omitted"].as_u64().unwrap() + 1)
                }
                _ => snapshot["assignment"]["sent_messages_truncated"] = json!(true),
            }
        }
        Ok(snapshot)
    }
}
