# Local HTTP API

<a id="http-api"></a>

Existing coding turns can review and update writable paths, verification and named commands under `/project-setup-review`, with explicit `/inspection` and durable `/cancellation`. Approved settings enter a fresh stage; earlier actor settings stay pinned. See [project setup review](harness.md#project-setup-review).

Reviewed source refresh uses the physical turn's `/project-refresh` routes: `GET` eligibility, `GET /inspection` file differences, `POST` the exact request and review digest, and `POST /cancellation` an unadmitted request ID. The response retains the canonical conversation and a new physical coding stage. See [project refresh](harness.md#project-refresh).

Reusable project settings use `GET /v1/harness/project-profiles`, `GET /v1/harness/project-profiles/{id}` and `POST /v1/harness/project-profiles/{id}`. Schema 32 stores independent profile revisions and exact mutation receipts. Session starts continue to submit actual configuration snapshots through their existing route. See [project profiles](harness.md#project-profiles).

Task inspection includes each launched native node's optional `project_instructions` snapshot. New project sessions save root `AGENTS.md` text and SHA-256, or a missing/ refusal status, when the task launches. See [project instructions](harness.md#project-instructions).

Goal task inspection optionally includes a native planner's saved `plan` on its node: revision, explanation and bounded checklist steps. New harness sessions enable planning; see [durable task plans](planner.md#task-plans).

The HTTP adapter exposes native conversations, teams, goal inboxes and committed events. Actor transitions and Run execution continue through the existing worker; the API routes do not drive an agent loop. With `--with-worker`, the same process also hosts that worker and owns its lease. SQLite stores durable sessions, turns, titles, archive state, pagination, memory-clear receipts, summaries, compaction policies and launch gates in the shared baseline. Existing actor/Run evidence stays available.

<a id="http-api--start-the-api-and-worker"></a>

### Start the API and worker

Host the API and worker with the CLI:

```powershell
New-Item -ItemType Directory -Force .act0r | Out-Null
cargo run --locked --offline -- --db .act0r/act0r.db api serve --with-worker
```

`--with-worker` acquires ownership before startup is announced. A conflicting worker refuses startup. The hosted worker defaults to two concurrent activities; `--max-concurrent-activities N` selects a positive limit and requires `--with-worker`. Ctrl+C signals both adapters, closes live streams, drains accepted HTTP requests and persists remaining activity uncertainty under the worker’s existing five-second grace period. If either adapter exits, its sibling is signaled and drained too.

To use separate processes, run these in separate terminals against the same database:

```powershell
cargo run --locked --offline -- --db .build/api.db api serve --bind 127.0.0.1:8080
cargo run --locked --offline -- --db .build/api.db worker run --max-concurrent-activities 2
```

Create `.build` first if needed. The default listener is `127.0.0.1:8080`; `[::1]:8080` also works. `--bind 127.0.0.1:0` selects an available port. With global `--json`, startup prints `{"api":"listening","url":"http://127.0.0.1:PORT"}` and flushes it. Ctrl+C stops admission of HTTP requests, closes SSE streams and drains accepted requests. Workflow waits, timers and worker execution remain durable independently of the API connection.

This version trusts local clients and requires a loopback bind address. Requests must use a loopback or `localhost` Host with the listener's port. Browser Origin must match the API origin; cross-origin fetch requests are refused, and CORS access is not enabled. It is a local transport, without user accounts or bearer authentication. Native clients can omit Origin. The server exposes JSON/SSE routes under `/v1`; it serves no frontend or static assets.

<a id="http-api--version-1-routes"></a>

### Version-1 routes

`T`, `G`, `W`, `S`, `R` and `V` below are team, physical task, wait, session, application Run and project verification Run UUIDs. A descendant task handle resolves the original goal through persisted ownership. Wait IDs must come from that goal's saved inbox; correlation IDs and caller-supplied payloads cannot establish ownership.

| Method and path | Request | Result |
| --- | --- | --- |
| `GET /v1/health` | None | Process liveness and `api_version: 1`; this does not certify worker or model health. |
| `GET /v1/harness/config` | None | Initial model target, `api_key_available` boolean, default read-only access and optional `coding_defaults`; no key is returned. |
| `GET /v1/harness/sessions` | `archived`, `limit`, `after` | Bounded session catalog page with saved titles, metadata revisions and `next_after`; default unarchived, 50 items. |
| `GET /v1/harness/sessions/S/metadata` | None | Saved title, archive state, revision and exact archive eligibility. |
| `GET /v1/harness/sessions/S/summary` | None | Latest summary, pending Run/status/usage and bounded source preview. See [session summaries](state.md#session-summaries). |
| `POST /v1/harness/sessions/S/summary` | `{"request_id":"UUID","expected_revision":N,"through_sequence":N}` | Admit a durable summary Run (202); exact retries recover its original receipt. Sources must be settled and match the reviewed boundary. |
| `POST /v1/harness/sessions/S/summary/policy` | `{"request_id":"UUID","expected_revision":N,"enabled":true}` | Save opt-in automatic compaction (200); exact retries return the original decision. Disabling releases failed holds after any admitted summary settles. |
| `GET /v1/harness/sessions/S/memory` | None | Current memory for every session actor: policy, entries, bytes, revision, generation and omissions; `memory: null` when disabled. |
| `GET /v1/harness/sessions/S/turns/G/memory` | None | Exact launch snapshots for the first 128 owned actor Runs, including delegated and continuing tasks, with a truncation flag. A queued turn has no Runs yet. |
| `GET /v1/harness/sessions/S/turns/G/recovery` | None | Saved failed model checkpoint, eligibility, usage and recovery receipts. See [turn recovery](recovery.md#turn-recovery). |
| `POST /v1/harness/sessions/S/turns/G/recovery` | Exact request/Run/activity/attempt IDs and expected history sequence | Admit one additional model attempt (202), retaining context, completed tools and charges. Exact retries return the original durable receipt. |
| `POST /v1/harness/sessions/S/actors/A/memory/clear` | `{"request_id":"UUID","expected_revision":N,"expected_generation":N}` | Clear this actor's memory atomically with a durable receipt. Exact retries return the original receipt with `replayed: true`; changed IDs/scopes or stale counters conflict. Saved history and Run snapshots remain intact. See [actor memory](actor-model.md#actor-memory). |
| `POST /v1/harness/sessions/S/metadata` | Revision-checked rename/archive/restore | Persist one decision with a latest retry receipt; archive requires settled work. See [session organization](harness.md#sessions). |
| `POST /v1/harness/sessions` | HarnessRequest JSON | Prepare native actors and admit turn 1; `202` with stable `session_id`, `team_id`, `task_id` and `sequence`. |
| `GET /v1/harness/sessions/S` | Optional positive `before` sequence | Up to 32 saved turns in chronological order, pinned model/project, and `next_before` for older history. |
| `GET /v1/harness/sessions/S/turns/T/steering` | None | Active root planner eligibility and ordered durable instructions with delivery receipts. |
| `GET /v1/harness/sessions/S/turns/T/permissions` | None | Current session test permission, recent decision receipts, and the selected root planner's eligible test wait. |
| `POST /v1/harness/sessions/S/permissions` | Request ID, expected revision, and typed allow/revoke action | Save or revoke permission for the exact pinned test command; retries acknowledge the original receipt. See [session permissions](recovery.md#session-permissions). |
| `POST /v1/harness/sessions/S/turns/T/steering` | `{"message_id":"UUID","run_id":"UUID","text":"..."}` | Save an instruction for the next fresh model request; exact identity retries recover the original receipt. See [turn steering](recovery.md#turn-steering). |
| `GET /v1/harness/sessions/S/turns/T/workers/C/steering` | None | Exact delegated coding task eligibility and ordered durable instruction history. |
| `POST /v1/harness/sessions/S/turns/T/workers/C/steering` | `{"message_id":"UUID","run_id":"CHILD_RUN_UUID","text":"..."}` | Guide this physical coding task's next fresh request; saved receipts survive restart and remain scoped to this task. |
| `GET /v1/harness/sessions/S/turns/T/workers/C/recovery` | None | Exact pending coding-worker checkpoint, retry/accept eligibility, target, usage and latest 32 decisions. Also included in delegated work. |
| `POST /v1/harness/sessions/S/turns/T/workers/C/recovery` | Request ID, Run/activity/attempt IDs, expected history sequence, `decision:"retry"` or `"accept_failure"` | Save one scoped [worker recovery](recovery.md#worker-recovery) decision; `202` with a durable receipt. Parent joins wait until recovery settles. |
| `GET /v1/harness/sessions/S/turns/T/workers/C/cancellation` | None | Exact assignment stop eligibility, current status and latest 32 saved receipts; also included in delegated work. |
| `POST /v1/harness/sessions/S/turns/T/workers/C/cancellation` | `{"request_id":"UUID"}` | Durably [stop this assignment](recovery.md#worker-cancellation) and its descendants; `202` with an exact receipt. Siblings continue; admitted unsafe work retains reconciliation. |
| `GET /v1/harness/sessions/S/turns/T/workers/C/pause` | None | Assignment hold revision, effective/ancestor holds, admitted operation count, eligibility and latest 32 receipts; also included in delegated work. |
| `POST /v1/harness/sessions/S/turns/T/workers/C/pause` | `{"request_id":"UUID","expected_revision":0,"paused":true}` | Durably [pause or resume this assignment](recovery.md#worker-pause); `202` with an exact receipt. Resume uses `paused:false` and releases only this hold. |
| `POST /v1/harness/sessions/S/turns` | `{"message_id":"UUID","prompt":"..."}` | Admit a follow-up with the same actors/configuration; `202` with a stable receipt. |
| `GET /v1/harness/storage` | None | Bounded storage report, visible cache bytes, advisory eligibility and inspection errors. |
| `POST /v1/harness/storage/cleanup` | `{"session_id":"UUID","agent_id":"UUID","fingerprint":"SHA256"}` | Recheck saved state and remove only the reviewed managed Cargo cache; `409 storage_conflict` for protected/changed work. |
| `GET /v1/harness/sessions/S/turns/G/changes` | None | Coding turn's saved patch, actual tests, completion assessment and Run status. |
| `GET /v1/harness/sessions/S/turns/G/tool-recovery` | None | Latest 128 owned blocked/reconciled Runs, exact native tool requests, read-only evidence, receipts and truncation flag. |
| `GET /v1/harness/sessions/S/turns/G/child-import-recovery` | None | Owned uncertain planner child imports, approved artifacts, file journal progress, current hashes and saved recovery receipts. |
| `POST /v1/harness/sessions/S/turns/G/runs/R/child-import-reconciliation` | Exact activity/attempt, retry or fail, evidence, `operation_stopped:true` | Save recovery of the same approved child import; identical receipts replay after restart. |
| `POST /v1/harness/sessions/S/turns/G/runs/R/reconciliation` | Exact activity/attempt, retry or fail, evidence, `operation_stopped: true` | Recover an interrupted native edit/create/test call; `200` with `run_id` and `attempt_id`. |
| `GET /v1/harness/sessions/S/turns/G/delegated-work` | None | Consistent saved assignments, writable scopes, group joins, child results, file/test summaries, message delivery history, planner and sibling questions/replies/deadlines and exact artifact review/import receipts under this turn. Works for read-only and coding sessions. |
| `GET /v1/harness/sessions/S/turns/G/applications` | None | Latest 20 application identities/statuses and a truncation flag. |
| `POST /v1/harness/sessions/S/turns/G/applications` | `{"application_id":"UUID"}` | `202` with `run_id`; identical retries recover the same durable preview. |
| `GET /v1/harness/sessions/S/turns/G/applications/R` | None | Saved preview, exact approval wait, report/error, recovery inspection and saved reconciliation receipts. |
| `POST /v1/harness/sessions/S/turns/G/applications/R/responses` | `{"wait_id":"UUID","approved":true}` | Save approval/rejection of that exact preview; duplicate decisions retain their receipt. |
| `POST /v1/harness/sessions/S/turns/G/applications/R/reconciliation` | Exact activity/attempt, retry or fail decision, inspection evidence | Save a recovery decision for an interrupted patch write; `200` with `run_id` and `attempt_id`. |
| `GET /v1/harness/sessions/S/turns/G/applications/R/verifications` | None | Latest 20 compact project verification identities/statuses/outcomes and a truncation flag. |
| `POST /v1/harness/sessions/S/turns/G/applications/R/verifications` | `{"verification_id":"UUID"}` | `202` with `run_id`; atomically pin the applied target and session's reviewed verifier. Exact retries recover the same Run. |
| `GET /v1/harness/sessions/S/turns/G/applications/R/verifications/V` | None | Frozen command/snapshot, approval wait, actual result/error and recovery receipts. |
| `POST /v1/harness/sessions/S/turns/G/applications/R/verifications/V/responses` | `{"wait_id":"UUID","approved":true}` | Approve/reject the exact project verification; duplicate decisions retain their receipt. |
| `POST /v1/harness/sessions/S/turns/G/applications/R/verifications/V/reconciliation` | Exact activity/attempt, retry/fail, evidence, `operation_stopped:true` | Recover an interrupted verification command; arbitrary result acceptance and changed commands/targets are refused. |
| `GET /v1/harness/sessions/S/turns/G/applications/R/verifications/V/repair` | None | Eligibility and saved preparation/receipt for repairing a settled failed verification. |
| `POST /v1/harness/sessions/S/turns/G/applications/R/verifications/V/repair` | `{"request_id":"UUID"}` | `202` with the new physical receipt and canonical `conversation_id`; exact retries recover one fresh repair stage. |
| `POST /v1/harness/sessions/S/turns/G/applications/R/verifications/V/repair/cancellation` | The same `{"request_id":"UUID"}` | Cancel an unadmitted preparation durably; an already admitted repair uses normal task controls. |
| `GET /v1/teams` | None | Existing bounded team list, at most 100 entries, with truncation reported. |
| `POST /v1/teams` | [TeamSpec](teams.md#agent-teams) JSON | Named team creation or identical retry; returns its current team/member view. |
| `GET /v1/teams/T` | None | Team/member status and queues. |
| `POST /v1/teams/T/resume` | No body required | Resume saved setup; return the team/member view. |
| `POST /v1/teams/T/tasks` | `{"message_id":"UUID","goal":"..."}` | `202` with stable `team_id` and `task_id`. |
| `GET /v1/teams/T/tasks` | Goal list query | Bounded persisted planner-root summaries, newest first, with a restartable pagination cursor. |
| `GET /v1/teams/T/tasks/G` | Inspection query | Existing goal status, graph, usage, controls and saved outcomes. |
| `GET /v1/teams/T/tasks/G/inbox` | Inspection query | Exact saved human questions/tool/import requests and stable wait IDs. |
| `GET /v1/teams/T/tasks/G/controls` | None | Current pause record (revision zero initially) and exact cancellation pending/blocked counts. |
| `POST /v1/teams/T/tasks/G/pause` | `{"expected_revision":0}` | Durable goal hold receipt with `control` and `replayed`. |
| `POST /v1/teams/T/tasks/G/resume` | `{"expected_revision":1}` | Release the goal hold under the same revision contract. |
| `POST /v1/teams/T/tasks/G/cancel` | No body required | Permanent cancellation receipt and current cleanup/reconciliation counts. |
| `POST /v1/teams/T/tasks/G/waits/W/responses` | HumanResponse JSON | Durable response receipt, including `replayed`. |
| `GET /v1/teams/T/tasks/G/events` | Event query | One bounded committed history page. |
| `GET /v1/teams/T/tasks/G/events/stream` | Event query, optional `Last-Event-ID` | SSE pages with reconnectable goal cursors. |

Team creation and human response routes return `200` on success, including idempotent retries. Team names are durable creation keys: retry with the same complete configuration to recover the original identities. A changed configuration requires a new name. HTTP TeamSpec source paths must be absolute; the API has no configuration filename from which to resolve a relative source. Interrupted preparation remains inspectable in the team list and can be resumed by saved ID or an identical creation request.

<a id="http-api--prompt-first-harness"></a>

#### Prompt-first harness

```json
{
  "message_id": "11111111-1111-4111-8111-111111111111",
  "prompt": "Explain how durable actors recover after restart",
  "target": {
    "provider": "openai",
    "protocol": "chat_completions",
    "model": "your-loaded-model-id",
    "endpoint": "http://127.0.0.1:1234/v1/chat/completions"
  },
  "workspace": null
}
```

The client-generated UUID identifies an immutable session: prompt, target and optional absolute workspace path are pinned in its assembly intent before actors or mailbox messages are created. Retry the complete identical body after a lost response or restart. An interrupted setup is resumed; changed configuration under that UUID is refused. Admission creates a planner and helper through the existing team application service. Execution remains owned by the worker. Use the returned team/task IDs with the existing status, inbox, control and event routes.

For automatic project work, send `"discover_project":true,"auto_execute":true` with the folder and goal. The worker continues from inspection into coding using the agent's validated setup, without a setup review request. Routine scoped work runs in the isolated copy; source application remains explicit. Both flags are pinned, and omission retains manual behavior for API callers. Conversation reads include `auto_execute` and an optional saved `setup_error`. See [automatic project work](harness.md#automatic-project-work).

Prompts must be nonblank and fit within 65,536 UTF-8 bytes as a `{"goal":"..."}` envelope. Model IDs are at most 256 UTF-8 bytes; endpoints and workspace paths at most 4,096 bytes. Targets require provider `openai` and HTTPS or loopback HTTP without credentials, query parameters or fragments. Existing model-policy validation applies. Missing workspace means no filesystem tools. Without `coding`, a folder enables `list_files@2`, `read_file@2` and `search_text@1` on read-only isolated copies.

New sessions default to unlimited model calls and total tokens. An optional `"budget":{"max_model_calls":null,"max_tokens":null}` object sets independent positive numeric limits or `null` for unlimited; omitted fields are unlimited. The same limits are pinned in actor Runs and shared across each turn's planner, descendants and retries. Actual and uncertain usage remain durable; unlimited remaining counters serialize as `null`. Existing sessions and exact retries retain their original saved limits. The 2,048-token per-response cap, total execution-attempt allowances and delegation bounds (four tasks, depth two) remain separate. Each turn retains its own pause/resume/stop controls and durable human decisions. See [execution budgets](state.md#goal-budgets).

For coding, include an absolute `workspace` and `"coding":{"writable_paths":["src/"],"test_command":{"argv":["cargo","test","--locked","--offline"],"timeout_ms":60000}}`. This pins approved `replace_text@1`, `create_file@1`, `run_tests@1`, change review and required fresh passing tests on the planner. Calls and tokens also default to unlimited; the planner retains its separate 128 execution-attempt allowance. The helper retains read-only tools and receives a current-code snapshot for each delegated task. Both session discovery and conversation reads return the pinned `coding` profile, or `null` for existing read-only sessions. Follow-up bodies cannot change it.

New Code planners enable `delegation_write_scopes`. Model delegations may supply `writable_paths` for one task or each group member, narrowing native edit tools within the planner and worker pins. Omission inherits worker permissions; saved sessions retain their original schema. The delegated-work read returns `write_scope: {source: "task" | "worker", paths: [...], paths_truncated: boolean}` or `null` when unavailable. Paths are bounded to 32 entries; a worker scope with no paths describes read-only native tools. Full task scopes remain in accepted `workspace_input.request.writable_paths`. Pinned commands are not OS sandboxes; out-of-scope files in a scoped child patch refuse import.

`coding.coder_count` accepts `1` (default when omitted) or `2`. Two adds the distinct `coder2` native tool actor and enables a two-child task group. Each coder inherits the pinned permissions, model and shared turn allowance. Both receive independent copies of the same current-code snapshot. The planner joins saved results, imports successful patches with individual approval and requires fresh combined tests. Conflicting before hashes refuse overwrite. The count is returned in `coding` when two and also saved by project profiles; old one-coder responses can omit it. Changing it on an exact start retry conflicts; follow-up bodies cannot change session configuration. Actual overlap requires two worker activity slots (`api serve --with-worker --max-concurrent-activities 2` or a separate worker with that option). See [parallel coding](coordination.md#harness-coding-delegation--optional-parallel-coding).

Add optional `coding.project_commands`, for example `{"format":{"argv":["cargo","fmt"],"timeout_ms":60000}}`, to pin `run_command@1` on the planner. The agent selects only a configured name. Each call saves an exact approval, activity intent, and bounded process result. Every admitted command clears previous test verification; only fresh `run_tests` can verify completion. Catalogs stay fixed across follow-up turns and are included in session/conversation reads. Interrupted commands use the existing `tool-recovery` and reconciliation endpoints with `command_outcome_unknown`. See [project commands](tools.md#project-commands).

Coding routes require a root turn belonging to that exact session. Preview creation binds the caller's application UUID, successful verified artifact and original project target in one SQLite transaction. Identical concurrent or restarted requests preserve one Run and its saved activities; another task/artifact under the same UUID conflicts. Failed, unverified and empty patches are definite client refusals, and a harness turn admits at most 20 previews. Application reads/decisions verify the Run's task and target scope. Approvals identify the exact saved wait; repeated decisions are idempotent and changed decisions conflict. No model chooses the application target. These operations reuse the existing [patch application workflow](tools.md#patch-application) and the shared baseline.

An application blocked on an uncertain write returns `recovery` with the exact `activity_id`, latest interrupted `attempt_id`, interruption diagnostic, pause flag, advisory `retry_allowed`, and read-only `inspection`. Inspection verifies the saved preparation/journal bindings and reports each file's `pending`, `intended` or `applied` progress, before/after/current hashes and conflicts. It creates no journal or target files. An unreadable or invalid journal becomes `inspection_error`; the Run remains inspectable and failure can still be recorded. Current hashes are observations; resumed execution checks them again under the existing project lock. A saved final receipt describes the completed operation even if the project has changed since.

Recovery bodies are `{"activity_id":"UUID","attempt_id":"UUID","decision":{"action":"retry"},"evidence":"Stopped the old worker and inspected the files."}` or use `"decision":{"action":"fail","message":"Reason for retaining the partial state."}`. Evidence and failure messages must be nonblank and at most 16,384 UTF-8 bytes. Unknown fields, target changes and `accept` decisions are refused. Retry schedules the same approved write with its original journal, permissions and limits; fail records `reconciliation_failed` without undoing effects. Confirm the previous worker has stopped before resuming. No filesystem execution runs in the API request.

The engine atomically persists the exact decision, receipt, history and continuation. The interrupted attempt ID is the idempotency key: an identical full body acknowledges its original decision after restart, completion, or a later interruption. Changed notes/action for an already resolved attempt conflict; replaying an old receipt never resolves a newer interruption. Read `reconciliation_receipts` on the application to inspect the recorded decisions. Existing holds, attempt limits, worker ownership and conflict refusal remain in force.

Project verification requires an owned **Applied** application and uses a separate Run `V` under that application's `/verifications` routes. The caller supplies only its stable UUID; the command, timeout, original folder and affected hashes are derived from the saved session/application in the admission transaction. One unfinished run and 20 total runs per application bound admission. Pending work holds archive and cleanup. No model calls are made. Passing process output proves verification only when both project snapshots match the approved manifest; failed tests and changed inputs retain distinct saved reports. Unknown process outcomes require the existing retry/fail recovery body with `operation_stopped:true`. A completed filesystem receipt acknowledges a lost SQLite completion without rerunning the command. See [project verification](harness.md#project-verification) for snapshot bounds, exact bodies and point-in-time evidence semantics.

Verification repair accepts only a real saved failed result against stable inputs on the latest settled turn. It derives the feedback, model and reviewed project settings from owned records, saves setup intent, checks current source identity and hashes, then atomically admits a new coding stage linked to the canonical conversation. Each prepared copy must match the failed verification's digest. Pending setup holds new turns, archive and cleanup; cancellation releases only unadmitted preparation. Passing/changed/rejected/uncertain results, changed source and caller overrides are refused. See [verification repair](harness.md#verification-repair).

Native tool recovery uses the `/tool-recovery` read and `/runs/R/reconciliation` mutation. The read discovers at most 128 blocked/reconciled Runs from the turn's authoritative ownership tree, independent of graph display bounds, and reports `truncated` if more exist. Each item has `task_id`, `run_id`, `status`, optional `recovery` and `reconciliation_receipts`. Recovery includes the entire saved `NativeToolRequest`, exact activity/attempt IDs, diagnostic, pause flag, advisory retry availability, and read-only filesystem inspection or inspection error. Only approved version-1 `replace_text`, `create_file` and `run_tests` calls qualify. Other activities retain CLI recovery.

Send the same retry/fail/evidence fields as above, plus required `"operation_stopped":true`. The API rejects false/missing confirmation for both decisions: settling a Run may release later turns while an old process could still affect its workspace. Confirm independently that the previous operation and any test subprocess have exited. No endpoint changes the original tool, permissions, workspace, argv or deadline, and `accept` cannot manufacture a tool or passing test result. Exact receipts reuse the engine's existing transaction; all accepted packets contain the validated stopped assertion.

Edit/creation inspection validates activity-bound journals and current content, with `not_started`, `ready`, `already_applied` or `conflicted` states. Journaled writes are acknowledged on retry without republishing matching content. Creation with an existing target and no journal conflicts. Inspection creates no locks or files; execution checks again under the existing lock. Interrupted tests report `test_outcome_unknown`: no test result journal exists, so retry runs the exact saved command anew and persists its new result. Failure preserves changes and permits later turns after the current ownership tree settles. Pause, cancellation and admission limits retain their core semantics.

Follow-up admission commits the conversation sequence, prompt, task handle and mailbox envelope in one SQLite transaction. Identical retries return the same sequence and task; changed text or another session under the same message ID is refused. The existing planner queue capacity (65 outstanding tasks) bounds admission, including held turns. A queued turn starts only after all earlier turns' owned tasks, delegations, reminders, follow-ups and actor completion receipts settle. A question, pause or blocked reconciliation therefore holds later turns across restart. Select the earlier turn to answer, resume or reconcile it. Stopping a turn does not stop the conversation or other turns.

New conversations pin bounded native actor memory: up to eight successful tasks and 32,768 serialized bytes per actor. The snapshot is taken when each turn launches and then stays pinned to its Run. Oversized entries are omitted whole and older entries are evicted; failed/stopped turns remain in saved history but do not enter this success memory. The conversation history is durable independently of this context window. History pages expose at most 8,192 UTF-8 prompt bytes and 16,384 response bytes per turn, with explicit truncation flags; complete evidence remains accessible through task inspection. `before=next_before` reads older turns without including concurrent new arrivals.

Sessions configured without conversation continuation retain their actor pins and exact creation retries. Their `can_continue` flag is false; start a new session to use conversation memory. **New session** creates fresh native actors and context; model/project settings apply to new sessions only. Coding starts now assemble planner, helper and coder, with approved child imports and fresh combined verification. The request shape and routes are unchanged. See [coding delegation](coordination.md#harness-coding-delegation).

Defaults use NVIDIA's `moonshotai/kimi-k3` chat-completions endpoint with `action_format: "function_calling"`. `ACT0R_MODEL` and `ACT0R_MODEL_ENDPOINT` override initial defaults; other model/endpoint combinations keep structured JSON. This recommendation applies only to the config endpoint's fresh target. API requests that omit `action_format` retain JSON semantics, and saved sessions and retries keep their original pins. The model worker reads `OPENAI_API_KEY` for all compatible hosted endpoints, including NVIDIA; the config endpoint's availability flag describes the API process environment. Session history includes bounded 1,024-byte prompt previews and interrupted setup, ordered newest first. `root_status` describes the physical root; inspect the full owned task graph to determine whether all work has settled.

Task submission requires a client-generated message UUID. Reuse it with the identical goal after a timeout, lost response or API restart. A changed envelope under that ID is refused. The submitted goal proceeds only when the worker processes it.

Human responses use the existing tagged contract:

```json
{"kind":"answer","answer":"Review durability"}
```

```json
{"kind":"approval","approved":true}
```

Use `approved: false` to deny the saved operation. The response must match the saved wait kind. Answers retain the 65536-byte UTF-8 domain limit. The receipt, history and continuation commit atomically; identical retries return `replayed: true`, including after completion/restart. Conflicting replies are refused. Goal ownership, pauses, cancellations, budgets and pinned tool/import contracts retain their existing behavior. See [team inbox](teams.md#team-inbox).

<a id="http-api--powershell-client"></a>

### PowerShell client

```powershell
$base = 'http://127.0.0.1:8080/v1'
$spec = Get-Content examples/configs/team-inbox.json -Raw | ConvertFrom-Json
$spec.source = (Resolve-Path examples/team-inbox-source).Path
$team = Invoke-RestMethod -Method Post "$base/teams" -ContentType application/json `
    -Body ($spec | ConvertTo-Json -Depth 50 -Compress)
$messageId = [guid]::NewGuid().ToString()
$goal = @{message_id=$messageId; goal='Coordinate a reviewed edit'} | ConvertTo-Json
$task = Invoke-RestMethod -Method Post "$base/teams/$($team.id)/tasks" `
    -ContentType application/json -Body $goal
$root = "$base/teams/$($team.id)/tasks/$($task.task_id)"
$inbox = Invoke-RestMethod "$root/inbox"
$inbox.items | Select-Object member,kind,actionable,wait
# Once a saved wait is visible, select its exact ID and response kind:
$question = $inbox.items | Where-Object kind -eq question | Select-Object -First 1
$reply = @{kind='answer'; answer='Review durability'} | ConvertTo-Json
Invoke-RestMethod -Method Post "$root/waits/$($question.wait.id)/responses" `
    -ContentType application/json -Body $reply
```

The worker runs asynchronously. Poll inbox/status until the expected waits are visible before selecting one. Inspect the saved approval payload, then post `{"kind":"approval","approved":true}` to that wait's response route.

<a id="http-api--discover-saved-goals"></a>

### Discover saved goals

`GET /v1/teams/T/tasks` accepts `limit` (default 25, range 1–100) and an optional opaque `after` cursor. Unknown/duplicate fields, malformed cursors, another database/team scope and cursors ahead of committed mailbox order are refused. The team must be ready and its saved actor pins coherent.

The response contains `team`, `query`, `snapshot_position`, `items`, `next_cursor` and `has_more`. Each item has `task_id`, `goal_preview`, `goal_truncated`, `root_status`, `created_at`, `updated_at`, `goal_paused` and `goal_cancelled`. Previews contain at most 1024 UTF-8 bytes and never split a character. The adapter reads bounded scalar projections instead of loading result bodies or complete Run payloads.

Ordering uses durable mailbox sequence, independent of timestamps. The first page captures a sequence watermark; subsequent pages preserve that membership after restart and exclude later submissions. Item statuses and controls are current at each page's read snapshot. Start without `after` to refresh membership. Empty/final pages return `next_cursor: null` and `has_more: false`. Identical submission retries add no goal or ordering change.

Only tasks whose authoritative ownership root belongs to the team's planner appear. Delegated workers, same-planner follow-ups and unrelated worker roots are excluded. The list's `root_status` describes the physical root's execution; it does not claim the entire goal has settled. Open `/tasks/G` for the bounded whole-goal graph, waits, results and usage.

Rust callers use `agents.teams()?.goals()?.list(team, GoalListQuery)`. `TeamGoalListService` depends on the read-only `TeamGoalListPort`; the SQLite adapter resolves pins, scope, cursor and page in one snapshot. Bootstrap installs it with `with_goal_listing`, preserving saved actor and Run payloads.

<a id="http-api--durable-goal-controls"></a>

### Durable goal controls

Read `/controls` to obtain the goal's current `pause.revision`. Pause and resume require a JSON `expected_revision` observed by the caller; missing or unknown fields are refused. Team ownership, cancellation and the revision check share the same write transaction as the hold. Any saved descendant task handle resolves the canonical goal, independently of inspection limits. Another team's tasks and unrelated worker submissions cannot establish this authority.

Each actual pause/resume transition increments the existing revision once. A retry using the original expected revision can acknowledge the immediately following matching transition without rewriting its receipt (`replayed: true`). A request selecting the current state at the current revision also makes no change. Once a newer transition has occurred, an older precondition returns `409 goal_revision_conflict` with `expected_revision` and `current_revision`; even an old pause matching a later pause is refused. Fetch `/controls`, review the current state, and issue a new decision explicitly. Do not automatically replace the revision on a failed request, as that could reverse another operator's action.

```powershell
$controls = Invoke-RestMethod "$root/controls"
$pauseRequest = @{expected_revision=$controls.pause.revision} | ConvertTo-Json
$paused = Invoke-RestMethod -Method Post "$root/pause" -ContentType application/json -Body $pauseRequest
# Retry a lost response with the identical action and body.
$resumeRequest = @{expected_revision=$paused.control.revision} | ConvertTo-Json
$resumed = Invoke-RestMethod -Method Post "$root/resume" -ContentType application/json -Body $resumeRequest
$cancelled = Invoke-RestMethod -Method Post "$root/cancel"
```

Pause gates new activities, timers, child publication, joins and follow-ups across the owned goal. Already admitted operations can finish and record results/usage. Human answers and approvals can be saved while held; their continuation waits for resume. The hold survives API and worker restart. Deadlines remain absolute UTC timestamps, and resume preserves budgets, receipts, individual actor/Run pauses, supervision and reconciliation blocks. These are the existing [goal pause rules](recovery.md#goal-pause).

Cancellation commits its irreversible receipt before bounded cleanup. It takes precedence over a pause and refuses later pause/resume with `409 state_conflict`. Identical cancellation retries through any owned handle preserve the first requesting handle and timestamp. The response includes `pending_tasks`, `pending_reminders`, `pending_intents`, `blocked_tasks` and `settled`. `/controls` reads these exact counts in one snapshot, without running cleanup or using display graph bounds. The worker continues pending cleanup. If cleanup fails after the receipt commits, the HTTP request can return `500`, while `/controls` still shows the effective cancellation; retrying cannot reopen execution.

Completed results and charged usage are retained. An admitted uncertain operation or blocked Run remains protected until it can be reconciled, so cancellation may stay unsettled. Independent goals keep progressing. New input to a cancelled wait is refused; a saved human-response receipt can still replay. See [goal cancellation](recovery.md#goal-cancellation) for the full execution and reconciliation rules.

Goal controls retain their own saved receipts. SSE continues to represent committed Run history; read `/controls` or task status to refresh the current hold and cancellation state.

<a id="http-api--bounded-queries-and-streaming"></a>

### Bounded queries and streaming

Inspection endpoints accept `max_nodes` (default 128, range 1–512) and `max_depth` (default 16, range 0–128). Partial views retain explicit truncation markers.

Event endpoints accept `after`, `limit` (default 100, range 1–512) and `max_bytes` (default 262144, range 1–16777216). Unknown or duplicate query parameters are refused. Pages use the same stored-history/envelope byte accounting and scope validation as [CLI goal events](teams.md#team-events); ownership traversal is not truncated by the event page count.

SSE validates the initial scope, cursor and limits before returning HTTP 200. Each `event: page` contains the complete `TeamEventPage` JSON; its `id:` is `next_cursor`. The first page is sent even if caught up, providing a checkpoint. Further pages contain saved events; empty reads over unrelated history stay quiet. Backlog pages drain without a polling delay. Keepalive comments carry no event ID and do not advance progress.

Reconnect using the last fully processed page's cursor in `Last-Event-ID` or the URL's `after` query. `Last-Event-ID` takes precedence when both are supplied, supporting EventSource reconnection from a URL with an older initial cursor. Cursors bind the saved database stream, team and canonical goal, so they work across API ports/restarts and cannot cross goal scope. This is at-least-once delivery: a disconnect before processing/checkpointing can replay a page, and stable event IDs support deduplication. Receiving a frame is not a processing acknowledgement.

```powershell
curl.exe -N "$root/events/stream"
curl.exe -N -H "Last-Event-ID: CURSOR" "$root/events/stream"
```

Once headers are sent, failures arrive as `event: api_error` containing the normal error envelope, without an ID. The stream then closes. A client should handle this event explicitly, close automatic retries, and resolve the reported problem. Oversized history produces a page retaining the cursor before that event, followed by `event_too_large` with its ID, position, required byte allowance and last safe cursor. Increase `max_bytes` and reconnect; events exceeding the maximum allowance need separate Run-history inspection. No event is silently skipped.

Streams read lazily under transport backpressure; there is no detached producer or unbounded page queue. By default each API instance allows 32 concurrent streams, polling caught-up goals every 250 ms and sending keepalive comments every 15 seconds. Capacity is released when disconnect/closure is observed. Regular status and response requests do not consume stream slots. Graceful shutdown signals idle streams to close before waiting for HTTP connections to drain.

<a id="http-api--errors-limits-and-durability"></a>

### Errors, limits and durability

API and extractor failures use `{"error":{"code":"...","message":"...","status":400}}`, optionally with `details`. Internal storage/I/O/serialization failures return a generic `internal_error`; diagnostic details go to the server log. HTTP protocol errors produced before routing remain the transport library's responsibility.

| Status | Examples |
| --- | --- |
| `400` | Malformed path/query, invalid limits/cursor, blank goal/answer, mismatched wait kind, invalid configuration. |
| `403` | Refused browser origin. |
| `404` | Missing team/task/wait or unknown route. |
| `405` | Unsupported route method. |
| `408` | Request deadline exceeded; retry using the same durable identity/response. |
| `409` | Conflicting message/response, stale goal control revision, cancelled-goal pause/resume, or invalid saved state. |
| `413` | JSON body exceeds 1 MiB, including whitespace. |
| `415` | A JSON mutation route lacks an application/json content type. |
| `422` | JSON cannot deserialize into its typed request, including missing/unknown fields. |
| `429` | Actor queue full or concurrent SSE stream limit reached. |
| `500` | Internal service failure. |
| `503` | Application service/registration/scope unavailable or API shutting down. |

Responses use `Cache-Control: no-store`; model inputs, outcomes, exact human requests and workspace details remain visible to authorized local operators through the existing projections. A 30-second deadline covers request parsing and handler work, including an unfinished request body. It stops waiting on the HTTP request; committed workflow progress remains saved. Team preparation may have durable partial setup, which an identical retry or `/resume` recovers. A transport timeout does not authorize retrying tools or resetting budgets. Long-lived SSE bodies are governed by stream closure/shutdown rather than the initial request deadline.

<a id="http-api--rust-boundary-and-verification"></a>

### Rust boundary and verification

`adapters::http_api::bind` validates loopback before opening a listener. `serve(listener, Arc<AgentService>, ApiOptions, shutdown)` supplies routing, extraction, bounded requests, structured errors, SSE and graceful shutdown. Programmatic options allow 1–60000 ms event polling, 1–128 streams and 1–300000 ms request deadlines. The adapter calls `TeamService`, `TeamTaskService`, `TeamGoalControlService` and `TeamEventService`; it performs no SQLite access or execution admission. HTTP types stay out of domain/application contracts. Routing and streaming use [Axum](https://docs.rs/axum/0.8.9/axum/) and its [SSE response](https://docs.rs/axum/0.8.9/axum/response/sse/struct.Sse.html).

`agents.teams()?.controls()?` exposes `status(team, task)`, `pause(team, task, GoalPausePrecondition)`, `resume(...)` and `cancel(team, task)` for Rust callers. `TeamService::with_goal_controls(actors)` reuses an installed `ActorService` and its `GoalControlPort`; `bootstrap::local_app` wires it automatically. Storage implements scoped snapshot/transaction methods on that existing port, and the pure `GoalPause` aggregate owns revision admission and retry policy. The worker's existing cancellation hooks finish cleanup. The baseline and saved actor/Run/goal payloads are preserved.

```powershell
cargo test --locked --offline --test http_api
cargo run --locked --offline --example http_api -- .build/http-api-demo/demo.db
```

The standalone demo creates/submits through HTTP, pauses at two waits, saves their idempotent answers/approvals, restarts the API and worker, verifies no further model calls while held, then resumes. Recovered SSE history must match a fresh full read, with three successful tasks and six model calls. A second waiting goal is cancelled and stays cancelled after another restart without additional spending. The integration suite kills/restarts separate API and worker processes, verifies `Last-Event-ID` recovery, stale revisions, ownership isolation, individual Run pauses, blocked-work counts, and receipt/cleanup rollback. Other checks cover duplicate/conflicting replies, pagination, slow/oversized bodies, shutdown, stream capacity/disconnect, byte limits, structured errors, and loopback/origin admission.
