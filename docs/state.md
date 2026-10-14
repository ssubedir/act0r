# Context, budgets and storage

Saved context, summaries, usage accounting, task inspection and storage cleanup.

- [Bounded context management](#context-management)
- [Bounded context for native actors](#native-context)
- [Durable conversation summaries](#session-summaries)
- [Harness storage inspection and cleanup](#storage)
- [Durable goal budgets](#goal-budgets)
- [Goal-level task inspection](#task-inspection)

<a id="context-management"></a>

## Bounded context management

This page describes coding workflows. Native tool and planner actors now use the same policy/report format with their own domain selector; see [native actor context](#native-context) for creation defaults, advisory test status, and compatibility with existing native pins.

Agent workflow version 2 selects a deterministic suffix of recent complete observations. The goal, instructions, permissions, fixed test command, pending action, test status, and observation-range metadata accompany every model request. The newest observation is essential: no string slicing or silent truncation changes its meaning.

<a id="context-management--configuration"></a>

### Configuration

```json
{
  "context": {
    "version": 1,
    "max_input_bytes": 65536,
    "max_observations": 16
  }
}
```

These are the defaults. Omitting `context` or individual fields fills them before a new run's configuration is saved. Policy version 1 is the supported selection algorithm; positive byte and observation limits are required. Unknown fields and unsupported policy versions are rejected. Policies are immutable for a run.

`max_input_bytes` counts UTF-8 bytes of the saved instructions, serialized JSON prompt, and compact action schema. Chat Completions additionally counts the schema and separator appended to the system message. The domain and adapter share that system-message construction, and HTTP contract tests compare measured bytes with the actual transmitted input material. The limit excludes the remaining HTTP envelope, model name, output allowance, fixture script, and selection-report metadata outside the prompt. It is not a provider tokenizer or a guarantee that a particular model's token window will fit; configure it for your model. `max_output_tokens`, `max_tokens`, and model-call/attempt limits continue to apply separately.

<a id="context-management--selection-and-failure-behavior"></a>

### Selection and failure behavior

The selector starts with the newest `max_observations` observations, then removes the oldest remaining observation until the complete input fits. It preserves chronological order and never skips a newer observation to retain an older one. Previously omitted observations stay outside the live state; a later smaller result does not restore them. No extra model calls or generated summaries are used.

Each selection records its policy, measured input bytes, total observation count, first included observation's one-based sequence, included count, and omitted count. Observations include tool results, human answers, approval denials, and feedback for premature finish actions. Test-success state remains explicit even if the corresponding test output leaves the window; finishing still requires successful fixed tests after the final edit.

If the fixed material or newest complete observation cannot fit, no new model activity is scheduled. Starting a run rejects the configuration with an essential-context diagnostic. Overflow following a completed model/tool activity saves that activity's outcome and usage and fails the run with `context_limit`, including required and allowed bytes. Reconciliation acceptance similarly preserves its receipt and verified result before reporting an invalid continuation; context overflow uses `context_limit`. An oversized human answer is rejected transactionally and leaves its wait pending so a smaller answer can be submitted. Limits cannot be raised on an existing run; start a new run with suitable configuration after inspecting a stopped run.

<a id="context-management--durability-and-inspection"></a>

### Durability and inspection

Live `AgentState.observations` contains the retained suffix. Original outcomes remain on activities and attempts and in ordered history; human-input payloads and receipts also remain recorded. Selection changes and the next model intent use the existing aggregate transaction. A database rollback cannot commit a trimmed state without its corresponding model intent.

Each model activity saves the exact prompt, instructions, schema, target, output cap, and selection report in its input. Retrying or restarting that activity reuses this input instead of recomputing context from current files, a modified config, or a new selection algorithm. Uncertain attempts retain usage reservations as before; new reservations use the complete measured input material plus framing headroom, including the Chat schema copy.

`run status RUN_ID` prints the most recently scheduled selection's bytes and included/omitted counts. `--json run status` exposes `/record/state/context/selection`; `--json run history` contains model `activity_scheduled` events with `/payload/input/prompt` and `/payload/input/context`. Original results can be inspected in activity/attempt records and `attempt_finished` history. The model currently accesses current workspace files through its tools; historical retrieval is not a new model tool in this milestone.

This bounds model input and the live observation list. The database, audit history, human-input/reconciliation receipts, and workspace artifacts continue to grow; retention and cleanup are separate work.

<a id="context-management--compatibility-and-verification"></a>

### Compatibility and verification

`AgentService::start` creates agent workflow version 2 with bounded context. This is the only coding workflow registered by the composition root. Direct integrations must register the matching workflow definition.

```text
cargo test --test context --test agent --test model_adapter --test cli --locked
```

Tests exercise byte and count eviction with complete Unicode/escaped JSON results, exact limit boundaries, mandatory-context overflow, policy validation/defaults, approval/input handling, test-state preservation, and both protocol byte counts. A 28-turn scripted coding run keeps at most three observations, completes fixed tests, and retries an interrupted model activity using identical saved input while retaining original outcomes. The existing real worker-kill test verifies saved input is unchanged after process recovery. These are correctness checks rather than claims about model quality under a smaller context window.

<a id="native-context"></a>

## Bounded context for native actors

New `tool` and `planner` actors pin a deterministic context policy at creation. Each model request includes the goal, instructions, permitted actions, writable paths, fixed test command, actor memory snapshot, planner workers and execution policies, and a suffix of complete recent observations. Context selection stays in the domain; adapters choose configuration defaults, and the existing Run transaction saves the selected state and next model intent together.

<a id="native-context--configuration"></a>

### Configuration

```json
{
  "max_input_bytes": 65536,
  "context": {
    "version": 1,
    "max_input_bytes": 65536,
    "max_observations": 16
  }
}
```

When `context` is omitted for a newly created native actor, its byte cap inherits the top-level `max_input_bytes`, and its observation cap defaults to 16. An explicit partial `context` uses policy defaults of version 1, 65536 bytes, and 16 observations for missing fields. The effective byte cap is the smaller of the context and top-level caps. Positive limits are required; unknown fields and unsupported versions are rejected before copying a source workspace. Task payloads can replace the goal but cannot override the pinned policy.

The byte count includes UTF-8 instructions, the serialized JSON prompt, and the compact action schema. Chat Completions also counts the schema copy and separator in the system message. It excludes the rest of the HTTP envelope, fixture script, output allowance, and selection report outside the prompt. It bounds input material rather than provider tokens. Model-call, token, output, and activity-attempt limits apply separately.

<a id="native-context--selection-and-saved-evidence"></a>

### Selection and saved evidence

Tool results, human clarifications, approval denials, and planner child replies share one chronological observation window. A joined child group is one complete observation. Selection starts with the newest `max_observations` entries, then removes the oldest remaining entry until the complete request fits. It never slices a result, skips a newer entry to retain an older one, or adds a summarization model call. Omitted entries do not return to live state on later turns.

The newest complete observation and fixed material are essential. If they cannot fit, the engine saves the completed activity result and usage, then fails the Run with `context_limit` instead of scheduling another model request. Initial overflow rejects the Run. An oversized human reply is rejected transactionally, preserving its wait for a smaller reply. Start a new actor or Run with suitable limits when a pinned policy is insufficient.

`test_status` retains the latest completed test activity ID, `passed`, `exit_code`, and `stale`. The full test output can leave the window while these fields remain. A successful permitted write marks the status stale; a later completed test refreshes it. Rejected writes and approval denials do not mark it stale. A tool result without a recognizable `ok` boolean has unknown success. This evidence is advisory: native policies do not enforce a passing-test gate before finishing. Changes made outside the runtime's write tools are not tracked by the stale flag.

<a id="native-context--durability-and-inspection"></a>

### Durability and inspection

Live `context.observations` contains only the selected suffix. Full tool/model outcomes stay on activities, attempts, and ordered history. Human replies and child outcomes also remain in durable records. This bounds model input and live observations; it does not bound database or workspace growth.

Every model activity saves its exact prompt, instructions, schema, target, output cap, and selection report. Retrying an uncertain model call reuses that request, including its selected observations and test status. Restarts, human waits, and goal holds do not rebuild it from current files or consume a fresh selection. Existing usage reservations and uncertainty charges continue to apply. A transaction rollback cannot save trimmed context without its corresponding next request.

`run status RUN_ID` prints the latest selection's measured bytes, included count, total count, and omitted count. For registered actors, `--json run status` exposes `/record/state/state/context/selection` and `/record/state/state/context/test_status`. Raw native workflows expose `/record/state/context`. Model activities contain `/spec/input/context` and the exact prompt; `--json run history` exposes the corresponding scheduled intents and original outcomes. Actors with the opt-in [result recall tools](planner.md#result-recall) can retrieve bounded pages of earlier completed native results in their own Run without rerunning the original tool. Recalled test evidence remains historical and does not refresh compact test status.

<a id="native-context--compatibility"></a>

### Compatibility

The tool adapter uses version 1 and the planner uses version 3. New native actors pin their context policy at creation; restart recovery uses the saved policy and exact pending model inputs. The model-only `answer` adapter can opt into this policy explicitly.

The coding workflow has its own existing [context implementation](#context-management), using the same version-1 policy and selection report. Its successful-test finish gate remains specific to coding workflows.

<a id="native-context--verification"></a>

### Verification

```powershell
cargo test --test native_context --test native_tools --test workspace_tools --test cli --locked
cargo run --locked --example workspace_tests -- .build/native-context-demo/demo.db
```

The deterministic demo uses a small Rust project and real Cargo tests: ten evidence reads, read/edit, failed tests, repair, passing tests, ten more reads, and delegated review. It reopens SQLite after both test results, bounds every planner request to 10000 bytes and two observations, preserves the passing test status after output eviction, and verifies exactly two test executions and 28 model calls across planner and reviewer.

Tests cover complete Unicode and escaped results, byte/count eviction, exact boundaries, essential overflow, defaults and validation, task override refusal, clarifications and groups, test-state staleness, completion rollback, CLI inspection, and SQLite restoration. Long tool/planner repairs retry interrupted model calls with identical requests and preserved usage. Real worker-kill fixtures stop after a saved test, an admitted model call, an evicted test output, and a child's durable human wait. These checks establish execution correctness; they do not measure model quality under a smaller window.

<a id="session-summaries"></a>

## Durable conversation summaries

In a settled harness session, use the summary API below to review the source-turn range, pinned model and execution limits, then submit the exact summary request. This starts a separate durable Run; the HTTP request only admits it. Run the API with `--with-worker`, or keep the existing durable worker running. Summary inspection exposes the saved text, source task IDs, Run, status and usage.

Each batch includes complete, contiguous turns after the previous successful summary, up to 32 turns and 48 KiB of serialized UTF-8 source data. Prompts, terminal responses/errors and statuses are captured without display truncation. If a whole turn cannot fit, the request is refused with a visible reason; it is never skipped. A further explicit summary folds the previous summary into the next batch. The previous Run link preserves that ancestry.

<a id="session-summaries--automatic-summarization"></a>

### Automatic summarization

Enable automatic summarization through the session summary-policy API below. Existing and new sessions default to off. Before launching a queued turn, the runtime summarizes when unsummarized earlier history reaches six turns or 24 KiB of prompt/terminal response text. This byte threshold is a conservative history heuristic, not a provider token count. It leaves the two most recent turns unsummarized; at least three earlier turns must exist. It does not make background model calls while a conversation is idle.

The entire earlier ownership tree must settle, including delegated work, durable waits, reminders, and completion receipts. Future turns stay in their durable mailboxes. Admission saves the source boundary, held task, policy revision, immutable model input and summary Run atomically. Restart recovery reuses that saved Run. Unrelated sessions and completion messages can progress. Source batches use the same bounds as explicit summaries; a large backlog may need several batches before the next turn launches.

On failure, the last successful summary remains and queued work stays held. Use the summary API to review and submit the same frozen sources under a new request ID. The worker does not retry failed summary Runs automatically. Disabling automatic summarization releases the hold. Turning the policy off during an admitted summary lets that Run settle first. An oversized complete source that cannot fit also holds work with a visible reason; turning off the setting releases it without skipping source data.

At future Run launch, memory entries whose tasks belong to turns covered by the selected summary are omitted from that Run's snapshot. Recent entries and entries unrelated to covered turns stay in order. Actor-owned memory, full history, older Run snapshots, memory revision and generation remain saved; the snapshot's omitted-entry count includes this compaction. This applies to explicit summaries too, including delegated tasks and earlier summary batches.

Automatic summaries have their own durable usage accounting, copy the session's original model and optional budget limits, and count retries. Unlimited call and token limits remain unlimited. They do not share or consume another turn's goal budget.

The summary text is limited to 8,192 UTF-8 bytes, the response to 2,048 tokens, and the complete model input (instructions, schema and prompt) to 65,536 bytes. The summary uses the session's original model target and optional call/token limits in its own Run. Null limits remain unlimited. Its model activity reuses the normal provider protocol, timeout, retry and usage accounting. It cannot call tools, delegate, ask a question or modify project files.

<a id="session-summaries--durable-boundaries"></a>

### Durable boundaries

Admission requires a settled session, including descendants, durable waits, reminders and completion receipts. SQLite atomically records the request receipt, immutable source/model input, queued Run and pending summary head. Fresh turns and archival wait while this summary is pending. Exact replays of existing turn submissions remain available.

The worker publishes a successful summary in the same transaction that saves the model outcome, Run history and reconciled usage. A failed, cancelled or exhausted Run clears the pending head and retains the last successful summary. Its sources can be reviewed again under a new request ID and current revision. Retries reuse the pinned input. An uncommitted external response may repeat after a crash; a committed response is reused without another model call.

When a future planner or native worker launches, the runtime selects the saved summary in the actor/mailbox transaction and pins it as `session_summary` in that Run's input. The model receives that summary alongside its bounded recent actor memory and current observations. Summary bytes count toward existing prompt limits and usage reservations. Oversized essential context fails explicitly. Historical Runs, their prompts, full conversation history and actor configuration stay intact. Actors and caller-supplied task data cannot install a summary snapshot. A summary never enters a turn that it already covers.

Conversation summaries and actor memory are separate. Clearing an actor's successful-result memory retains the conversation summary. Start a new session for fresh conversation context. Saved source data and model-produced summaries are historical input, not instructions or evidence of current filesystem state.

<a id="session-summaries--api"></a>

### API

- `GET /v1/harness/sessions/S/summary` returns the head revision, latest successful snapshot, pending flag, latest attempt/status/usage, next source references, reviewed boundary, remaining-turn count, blocking reason, model and budget. The `automatic` object includes the opt-in setting and its separate revision, thresholds, held turn, source boundary, summary Run and failure reason.
- `POST /v1/harness/sessions/S/summary/policy` accepts exactly `{"request_id":"UUID","expected_revision":0,"enabled":true}`. HTTP 200 returns the committed policy revision. Exact retries return the original receipt without reapplying it; changed bodies/scopes and stale revisions conflict.
- `POST /v1/harness/sessions/S/summary` accepts exactly `{"request_id":"UUID","expected_revision":0,"through_sequence":4}` and returns HTTP 202 with the admitted Run ID. Use the boundary from the GET preview.
- `GET /v1/harness/sessions/S/turns/T/memory` includes each launched Run's `session_summary` snapshot when present, in addition to its actor memory.

A request UUID identifies one decision globally. An exact retry returns the original Run receipt with `replayed: true`, including after restart or later summaries. Changed bodies/scopes, stale revisions, changed source boundaries, active work and archived sessions return a conflict. Clients should save the exact reviewed request before sending it and retry that body after an unknown acknowledgement.

<a id="session-summaries--local-checks"></a>

### Local checks

```powershell
cargo test --locked --offline --test session_summary
cargo test --locked --offline --test http_api summary::
cargo test --locked --offline --test http_api automatic_summary::
```

The HTTP tests use a loopback model fixture. They cover restart/idempotence, planner/helper snapshots, incremental batches, complete Unicode sources, concurrent admission, model failure, transaction rollback/recovery, scope conflicts without changing saved Runs.

<a id="storage"></a>

## Harness storage inspection and cleanup

Use `storage report` to inspect database/WAL/shared-memory bytes, visible managed workspace bytes, generated Cargo cache bytes and currently eligible bytes. Review an eligible workspace's exact path, file/byte counts and fingerprint before calling `storage clean` as shown below.

The first slice covers up to 64 member workspaces from the latest 64 saved harness sessions. Totals cover successfully inspected listed copies; truncation and refused inspections are explicit. Sizes are logical file bytes, not guaranteed physical disk savings. Snapshots, handoff copies, other runtime artifacts, unrelated projects and repository development builds are outside this report. Use `cargo clean` separately for repository development output.

<a id="storage--cli-and-http"></a>

### CLI and HTTP

Use the same database as the harness. Inspect first, then select the session ID, actor ID and fingerprint from an eligible item:

```powershell
act0r --db .act0r/act0r.db storage report
act0r --db .act0r/act0r.db storage clean --session SESSION_ID --agent AGENT_ID --fingerprint REVIEWED_FINGERPRINT
```

`GET /v1/harness/storage` returns the report. `POST /v1/harness/storage/cleanup` accepts exactly `session_id`, `agent_id` and `fingerprint`; callers cannot supply filesystem paths. Success returns `agent_id`, `removed_bytes`, `removed_files` and `already_absent`. A changed preview or newly protected execution returns `409 storage_conflict`. Refresh and make a new decision. Ordinary loopback/origin checks and strict JSON handling apply.

<a id="storage--eligibility-and-recovery"></a>

### Eligibility and recovery

Cleanup targets only a recognized Cargo `target/` directory directly inside a runtime-marked managed workspace. Fresh caches need Cargo's `CACHEDIR.TAG` signature and valid `.rustc_info.json`. Paths, ancestors and entries cannot traverse symlinks, Windows reparse points or special files. Cache-contained runtime-private directories or database files refuse cleanup. Scans allow at most 200,000 entries and 128 directory levels per tree; saved cleanup inventories are capped at 32 MiB. Refused inspections have unknown sizes and no cleanup action.

The application checks the entire saved session ownership state, independent of display limits. Incomplete setup, queued work, reminders, durable waits, actor holds, unconsumed completion receipts, blocked/unfinished Runs and pending patch applications protect the copies. Direct advanced Runs referencing the same workspace also protect it. The cleanup operation acquires SQLite write admission exclusion and workspace ownership, then rechecks eligibility and the reviewed cache. New turns and worker writes wait until deletion ends. HTTP cancellation cannot release protection while the owned filesystem operation is running; a large cleanup can briefly delay unrelated database writes.

Before deletion, the adapter syncs the exact cache inventory and fingerprint to `.act0r-tools/cache-cleanup.json`. If a process stops after some removals, retry uses that saved inventory. Remaining files must still match their original path, type, length and modification time; new or changed files refuse resumption. Empty-directory and final-receipt boundaries are recoverable. A compact completed receipt remains after successful cleanup. Retrying after the cache is absent reports `already_absent`; it does not claim old byte savings again. Regenerated caches require a new preview.

Saved source files, task results, patches, approvals, budgets, original projects and runtime journals are retained. Future tests rebuild caches; existing saved test results remain historical evidence. No automatic retention, session deletion or database VACUUM is performed. This is a trusted local maintenance operation, not an OS sandbox against unrelated processes changing files during removal.

<a id="storage--architecture-and-validation"></a>

### Architecture and validation

SQLite busy/locked responses are transient `storage_busy` errors (`503` over HTTP). The hosted worker retries scheduling after contention and retains each exact finished outcome while retrying its commit, without executing the external activity again. Other storage errors remain failures. A cleanup held beyond the normal five-second SQLite timeout must not stop unrelated worker execution.

`StorageService` coordinates domain report/request types through `StorageStorePort`, `CacheCleanupGuard` and `StorageFilesPort`. SQLite supplies authoritative state and admission exclusion; the local filesystem adapter supplies bounded inspection, workspace ownership and journaled deletion. The CLI and HTTP API share this service. No schema migration or automatic retention policy is required.

Tests cover protected work, stale previews, wrong scopes/extra fields, pending applications, uncertain external Runs, concurrent follow-up admission, cancellation of a waiting request, retained source/evidence, real Windows junction refusal, missing Cargo markers, exact requests and HTTP acknowledgement replay. A child process exits after a synced cleanup intent and first removal; reopening resumes the remaining cache and refuses new files. A separate check covers the final-unlink/receipt boundary. Contention tests hold write admission beyond SQLite's busy timeout, verifying both continued hosted execution and a single committed attempt for an already finished activity.

<a id="goal-budgets"></a>

## Durable goal budgets

Harness sessions default to **unlimited model calls and total tokens**. Configure a positive integer for either limit or `null` for unlimited. New sessions pin that choice for the planner and helper, and each conversation turn starts its own shared goal allowance. Delegated children and retries charge that turn's allowance. Existing sessions retain their saved numeric limits.

The harness HTTP request accepts an optional `budget`:

```json
"budget": {"max_model_calls": null, "max_tokens": null}
```

Omitting the object also defaults to unlimited for a new session. Either field can independently be a positive integer or `null`; omitted fields are unlimited. Zero, negative, fractional, string and unknown-field values are rejected. An exact retry of an older saved request recovers its previous configuration and receipt; it cannot replace saved limits. Follow-up requests supply only their prompt and message identity.

On the native planner, configure an allowance for one goal:

```json
"goal_budget": {"max_model_calls": 20, "max_tokens": 20000}
```

This allowance covers the original task, its scheduled follow-ups, delegated children, and those children's follow-ups and descendants. It is independent of the per-Run limits (`max_model_calls` and `max_tokens`) and the optional `delegation.shared_budget`. Every applicable numeric limit must admit a model request. Each call/token limit accepts a positive integer or `null` for unlimited.

Unlimited budgets still record every admitted call, actual tokens, held reservations and uncertain usage. CLI inspection labels unlimited allowances explicitly; JSON remaining counters are `null` for those dimensions. Numeric limits keep the same accounting, exhaustion latch and atomic admission checks. Per-response output/context bounds, per-activity retries, total execution-attempt allowances, delegation capacity and lifecycle controls remain independent of these two usage limits. Harness requests pin separate total execution-attempt allowances for each Run.

Each independently submitted task starts a fresh goal scope, identified by its root Run ID and `kind: "goal"`. Scheduling a follow-up inherits the source's goal scopes rather than creating fresh ones. A delegated planner can add a tighter goal allowance for its own task, follow-ups, and descendants. That tighter scope remains alongside all ancestor goal scopes. Manual reminders and separately submitted tasks are independent goals.

Goal identity and membership come from persisted task ownership and follow-up provenance. Model actions and task payloads cannot select another scope, replace its limits, or reset usage. A continuation's Run, goal membership, actor state, mailbox receipt, and task handle commit in one transaction before external activity admission.

<a id="goal-budgets--accounting-and-exhaustion"></a>

### Accounting and exhaustion

Goal scopes use the same `SharedBudget` aggregate and reservation rules as [delegation budgets](coordination.md#delegation-budgets). Admission reserves one model call and the request's conservative input/output token allowance atomically with its Run, attempt, activity, and history. Reported usage replaces that attempt's token reservation. Missing usage and interrupted calls retain their uncertain reservations; retries require another admission. Duplicate outcomes do not charge twice. Cancellation, pause, supervision recovery, memory clearing, and worker restart never refund admitted calls or clear exhaustion.

When a goal cannot admit a request, that requesting Run becomes `budget_exhausted` with `goal_model_budget`. No new attempt or external request is created. The scope saves the first exhaustion receipt and rejects later admissions even if an outstanding request subsequently reports lower usage. Already admitted work can settle normally. Completed tasks and their results stay intact, and unrelated goals on the same actor can run normally. Budget exhaustion is neutral to actor supervision.

Exhaustion does not cancel every sibling, revoke an in-flight request, or remove already scheduled reminders. A saved follow-up may still publish and create a Run; its next model admission checks the exhausted scope and ends without dispatch. This preserves task receipts and the existing durable reminder and cancellation contracts.

<a id="goal-budgets--inspection-and-local-verification"></a>

### Inspection and local verification

`Engine::shared_budgets(run_id)`, `actor task status TASK_ID`, and `run status RUN_ID` expose all scopes, member Runs, usage, `remaining_model_calls`, `remaining_tokens`, and exhaustion. JSON includes `record.kind` to distinguish a goal scope from a delegation scope at the same root. Human-readable Run status also prints the scope kind and remaining allowance.

[`actor task inspect TASK_ID`](#task-inspection) shows the whole goal tree and reports each scope once with its exact member count and remaining allowance. It distinguishes visible Run usage from shared totals, including when query limits omit descendants.

Numeric remaining counters mean unspent allowance, not a guarantee that another request will fit. A scope can latch exhaustion with positive remaining tokens when the next reservation is too large. Overlapping scopes each sum unique member Runs; adding overlapping scope totals together would double count usage.

```powershell
cargo test --locked --test goal_budgets
cargo run --locked --example goal_budgets -- .build/goal-budgets-demo/demo.db
```

The offline scripted example spends three calls on a planner and its child, restarts with two calls remaining, then stops a follow-up after its delegation spends the remaining allowance. The original successful result is preserved. Tests additionally cover nested planners, uncertain tokens, duplicate outcomes, rollback of scope/membership writes, process kills after registration, Run creation, admission, and exhaustion.

<a id="goal-budgets--domain-and-storage-boundaries"></a>

### Domain and storage boundaries

`Run` pins the configured goal allowance; `SharedBudget` owns admission rules, usage aggregation, and the exhaustion latch. Actor continuation selection stays in the application layer. The `ExecutionStore` and actor ports require atomic scope membership and accounting; the SQLite adapter implements those transactions using the existing budget tables. The baseline includes a scope kind to their keys so a Run can anchor both kinds. Existing delegation payloads default to `delegation`, and existing Runs and actors are not rewritten or assigned retroactive goal budgets. The planner remains the same adapter/workflow version.

This slice limits model calls and tokens. It does not track money, tool-call budgets, elapsed time, or lifetime spending across independent goals.

<a id="task-inspection"></a>

## Goal-level task inspection

Launched native planner and answering nodes expose an optional `project_instructions` snapshot from their saved Run input. It contains loaded root `AGENTS.md` text and SHA-256, or a saved missing/refusal status. Queued tasks omit this field until activation. See [project instructions](harness.md#project-instructions) for scope and bounds.

Native planner nodes optionally include `plan` with the latest saved checklist revision, explanation and bounded steps. It comes from the same Run snapshot as status and waits. See [durable task plans](planner.md#task-plans).

Inspect a submitted task, a delegated child, or a follow-up reminder ID to read the entire original goal:

```powershell
cargo run --locked -- actor task inspect TASK_ID
cargo run --locked -- --json actor task inspect TASK_ID
cargo run --locked -- actor task inspect TASK_ID --max-nodes 128 --max-depth 16
```

The readable output shows the original request and its tree of forwarded tasks, delegated children, task groups, and follow-ups. Each node includes its actor, own Run when present, goal, saved status, wait or stop reason, human question and wait ID, result/error, and execution usage. JSON exposes the same projection through `nodes`, `edges`, `budgets`, `summary`, and `truncations`. IDs and deadlines remain available in full; readable goal, reason, and result previews are bounded and escaped. Deadlines use UTC Unix milliseconds.

After [whole-goal cancellation](recovery.md#goal-cancellation), the view also includes the optional `goal_cancellation` receipt and a readable header. Physical statuses and reasons remain authoritative, including blocked Runs awaiting reconciliation and successful tasks whose future follow-ups were cancelled.

After [whole-goal pause/resume](recovery.md#goal-pause), optional `goal_pause` contains the saved control and receipts. Pending nodes expose `execution_held`; readable output marks admission holds without replacing their physical status, human question, or deadline. Cancellation takes precedence over the retained pause audit.

<a id="task-inspection--what-the-view-means"></a>

### What the view means

Ownership comes from persisted coordinator parents, native delegation intents, and follow-up provenance. Inspection starts at the original root even when called on a descendant. Independent public submissions and manual reminders remain separate goals, including submissions to the same actor or with the same correlation ID. Task payloads cannot select a parent.

Nodes represent physical task requests. A coordinator forwarding a task has no own Run; its child owns the execution and usage. Delegations saved before publication and follow-ups awaiting registration appear with `accepted: false`. Saved publication rejections, cancelled reminders, and follow-up chain-limit refusals stay visible with their reasons. A future reminder remains `scheduled` until its task is published.

The view reads durable state; it cannot determine whether an admitted activity is still running in another process. `activity_in_flight` means admission was saved without an outcome. Other reasons distinguish human input, pending child replies, activity queue/retry deadlines, durable timers, cancellation, Run pause/block, actor mailbox pause/block, supervision, and terminal results.

Mailbox control and execution control are separate. Pausing an actor leaves its active Run's saved status intact. The node reports `actor.status` alongside its execution status; a human wait is retained in `wait` even if the Run itself is paused. Supervision includes the causal task and Run IDs. Inspection neither recovers nor resumes work.

`summary.pending`, `succeeded`, and `stopped` count visible requests, including unpublished intents. `settled` requires every request to be terminal and the graph to be complete. It can be true when some requests failed, were rejected, exhausted a budget, or were cancelled; inspect the counts and individual outcomes to assess success.

<a id="task-inspection--bounds-and-accounting"></a>

### Bounds and accounting

Defaults are 128 nodes and depth 16, with the root at depth zero. Allowed values are 1–512 nodes and depth 0–128. A stable breadth-first read preserves each task group's request order. Omitted direct children produce a `truncations` entry with a limit reason and count; descendants of omitted nodes are not counted. A partial graph always has `settled: false`. `requested_task_visible` reports whether the requested descendant fits within the root-based limits. Cyclic ownership or ancestry exceeding 512 links fails explicitly.

`summary.visible_usage` sums each visible Run once, including held tokens and uncertain calls. It covers only the shown Runs. The top-level `budgets` list reports each persisted `(root_run_id, kind)` scope referenced by a visible Run once, with exact usage, remaining allowance, exhaustion, and member count across all its Runs, including omitted nodes. Scopes belonging only to omitted Runs are outside the view. Nodes reference their scopes through `budget_scopes`. Goal and delegation scopes can overlap; their totals describe separate limits and must not be summed as spending. A settled goal can retain uncertain charges.

<a id="task-inspection--rust-api-and-architecture"></a>

### Rust API and architecture

```rust,ignore
use act0r::application::TaskInspectionOptions;

let view = agents.actors()?
    .inspect_task(task_id, TaskInspectionOptions::default())
    .await?;
```

`TaskInspectionService` assembles an application read model through the dedicated `TaskInspectionReadPort`. The SQLite adapter reads ownership links, execution facts, budget summaries, and any goal cancellation receipt in one consistent transaction without changing actors, Runs, receipts, reservations, or history. It does not load full attempt/history collections or budget member lists. The baseline includes execution records, ownership links and goal-cancellation receipts.

`bootstrap::local_app` wires the reader automatically. Custom applications can use `ActorService::with_inspection_reader(store.clone())`, or call `TaskInspectionService::new(store)` directly. The standalone query service does not require installed workflow definitions, a worker, model credentials, or external I/O.

<a id="task-inspection--verify-locally"></a>

### Verify locally

```powershell
cargo test --locked --test task_inspection
cargo test --locked --test cli cli_inspects_a_goal_across_a_delegated_wait_and_completion
cargo run --locked --example task_inspection -- .build/task-inspection-demo/demo.db
```

The scripted demo delegates to a child that asks a human question, saves the whole goal, restarts, verifies identical inspection, answers the saved wait, and inspects both completed results. Tests also cover unpublished requests, partial task groups, follow-ups, refusal/cancellation, goal exhaustion, mailbox/Run controls, supervision, durable timers, in-flight reservations, ownership isolation, query limits, corrupt cycles, and unchanged database contents after inspection.
