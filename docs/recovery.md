# Controls, permissions and recovery

Durable holds, cancellation, model recovery, tool reconciliation and explicit guidance.

- [Durable goal cancellation](#goal-cancellation)
- [Durable goal pause and resume](#goal-pause)
- [Manual reconciliation](#reconciliation)
- [Failed turn recovery](#turn-recovery)
- [Durable turn steering](#turn-steering)
- [Durable coding worker recovery](#worker-recovery)
- [Stop one delegated assignment](#worker-cancellation)
- [Pause and resume one delegated assignment](#worker-pause)
- [Clarification and stalled assignments](#worker-clarification)
- [Durable session and task permissions](#session-permissions)

<a id="goal-cancellation"></a>

## Durable goal cancellation

Native team goals expose ownership-scoped cancellation and exact pending/blocked counts through the [local HTTP API](api.md#http-api--durable-goal-controls). Rust callers use `agents.teams()?.controls()?.cancel(team_id, task_id)`. Both transports reuse the existing durable receipt, admission guards and bounded actor cleanup.

Cancel an original task and all work owned by it, using any task, delegated child, or follow-up handle:

```powershell
cargo run --locked -- --db .build/agent.db actor task cancel-goal TASK_ID
cargo run --locked -- --db .build/agent.db actor task inspect TASK_ID
```

The command saves a permanent cancellation receipt before cleaning up work. The JSON response contains `receipt`, `pending_tasks`, `pending_reminders`, `pending_intents`, `blocked_tasks`, and `settled`. Repeating the command through any descendant retains the first requested task and UTC Unix millisecond timestamp. There is no undo; submit a new independent task to begin another goal.

<a id="goal-cancellation--ownership-and-completion"></a>

### Ownership and completion

Ownership follows saved coordinator forwarding, native delegation, and planner follow-up provenance. Cancellation includes unpublished child intents, queued or consumed pending tasks, active Runs, scheduled reminders, and follow-ups awaiting registration. It works without an optional goal budget. Independent public submissions and manual reminders remain separate goals, even on the same actor or with the same correlation ID. Payload fields cannot choose ownership.

The command performs up to 64 cleanup transactions. The worker continues the remainder and propagates saved terminal replies to ancestors. A large tree can therefore return `settled: false` while cancellation is already effective. Cancellation has no inspection node/depth limit. `pending_tasks` counts accepted requests that have not settled; `blocked_tasks` is a subset. `pending_reminders` counts scheduled reminders, and `pending_intents` counts unpublished native delegations and follow-ups awaiting registration. Settlement requires all these pending counts to reach zero, including ancestor replies.

Each admission transaction reads the receipt: cancelled goals cannot admit activities, fire workflow timers, launch pending Runs, publish children or reminders, or register an executable follow-up. Persisting an in-flight outcome can still record further workflow intent; the receipt prevents its execution and the worker settles it. If publication wins before cancellation, the newly accepted descendant belongs to the same goal and is included in cleanup. If cancellation wins, publication is refused. Process restart cannot reopen admission.

Completed results, history, memory, and budget charges remain intact. A successful task that scheduled a follow-up stays successful while its future reminder is cancelled. Cancellation of an already admitted model or safe tool call is cooperative: the saved outcome and actual usage remain available, and no further work is admitted. Cancellation cannot undo an external effect that has already occurred.

<a id="goal-cancellation--unsafe-operations-and-blocked-work"></a>

### Unsafe operations and blocked work

An admitted activity with `RequiresReconciliation` must settle before Run cancellation. The goal receipt is effective immediately, but the worker leaves that Run's cancellation control unset while its outcome is unresolved. If interruption or an uncertain outcome blocks the Run, its reservations and reconciliation requirement remain saved. The goal reports the blocked task and remains unsettled.

Inspect and reconcile through the existing Run API. Accepting a verified outcome can preserve a successful result. An authorized retry, or resuming a Run after restoring a missing implementation, reaches the goal admission guard and is cancelled before another attempt. Reconciliation evidence and uncertain charges remain recorded; cancellation does not refund them.

Consumed pending tasks use their actor definition's existing pure cancellation hook to remove saved FIFO state and release capacity atomically. A custom application must install the exact actor definition and implement that hook. Cleanup errors leave the committed goal receipt in place; restoring the definition or correcting storage failure allows cleanup to continue. Native actor definitions provide these hooks.

<a id="goal-cancellation--rust-api-and-architecture"></a>

### Rust API and architecture

```rust,ignore
let cancellation = agents.actors()?.cancel_goal(task_id).await?;
let inspection = agents.actors()?
    .inspect_task(task_id, act0r::application::TaskInspectionOptions::default())
    .await?;
```

`GoalCancellationReceipt` owns the irreversible request and the policy for preserving reconciliation. `ActorService` coordinates cleanup through a dedicated `GoalControlPort`; actor cancellation transitions and Run execution remain in their existing domains. `bootstrap::local_app` wires the port automatically. Custom applications opt in with `ActorService::with_goal_control(store.clone())` and keep that service installed as the Engine's work coordinator.

SQLite adds the receipt table and derived ownership views. The receipt and each admission guard use write transactions; cleanup saves cancellation, actor state, capacity release, and replies together using existing adapters. Goal inspection includes optional `goal_cancellation` JSON and a readable receipt header. It continues to report saved physical statuses and reasons, so an unsafe blocked Run remains visibly blocked. A bounded partial inspection can be unsettled even when the cancellation command's complete pending counts have reached zero.

<a id="goal-cancellation--verify-locally"></a>

### Verify locally

```powershell
cargo test --locked --test goal_cancellation
cargo test --locked --test cli cli_cancels_a_goal_from_its_child_and_preserves_the_first_receipt
cargo run --locked --example goal_cancellation -- .build/goal-cancellation-demo/demo.db
```

The scripted demo creates a delegated human wait and a successful child with a future follow-up, cancels through the reminder handle, restarts, and verifies that both pending branches stay stopped while a separate goal completes. Tests also cover admission races, paused pending queues, retained in-flight results, unsafe reconciliation, blocked restoration, trees beyond inspection limits, ownership isolation, migration, independent receipt/cleanup rollback, and process kills after receipt, cleanup, and safe/unsafe admission.

<a id="goal-pause"></a>

## Durable goal pause and resume

Native team goals also expose revision-checked pause/resume through the [local HTTP API](api.md#http-api--durable-goal-controls). Team ownership and the revision gate commit atomically; delayed retries cannot undo newer controls. Rust callers use `agents.teams()?.controls()?` with `GoalPausePrecondition { expected_revision }`. CLI commands below retain their existing direct operator contract.

Hold execution for an original task and its descendants through any task, delegated intent, or follow-up handle:

```powershell
cargo run --locked -- --db .build/agent.db actor task pause-goal TASK_ID
cargo run --locked -- --db .build/agent.db actor task inspect TASK_ID
cargo run --locked -- --db .build/agent.db actor task resume-goal TASK_ID
```

Each command returns JSON with `root_task_id`, `paused`, `revision`, `pause`, and `resume`. Receipts contain the requesting handle and UTC Unix millisecond timestamp. Repeating the current operation preserves its first receipt, including across restart and clock changes. A new pause/resume cycle increments the revision and saves new receipts. Resuming a goal that has never been paused returns an unchanged default record without writing one.

<a id="goal-pause--admission-and-retained-progress"></a>

### Admission and retained progress

The goal's pause commits in one write transaction. Activity admission, timer firing, native delegation publication/joins/deadline processing, reminder publication, and task mailbox consumption read the hold in their own transactions. If admission wins before pause, that operation may finish. If pause wins, no new external operation is admitted. The hold covers existing and newly saved descendants through authoritative ownership, without inspection graph limits or a required goal budget.

Already admitted outcomes, usage reconciliation, follow-up registration, and terminal replies can still commit. Human answers can be saved while held; the resulting continuation waits for resume. Internal completion transitions may prepare a queued Run for an already consumed pending request; that Run's activities remain held. Successful children retain their results while a held parent's join waits. Completed requests show their original status, result, and spending.

Queued task envelopes retain their FIFO order on the actor. A held earlier task prevents a later task on that actor from passing it, while runtime replies can release existing work. Other actors keep progressing. An actor that has finished its part of a held goal can handle independent submissions; matching correlation IDs or payload parent fields do not merge their ownership. Manual reminders remain independent goals.

`worker run --until-idle` exits when only held, waiting, individually paused, or blocked work remains. Admitted operations are still drained normally. Startup recovery inspects all unfinished Runs, including held ones: interrupted safe attempts retain their retry deadlines and conservative charges; interrupted unsafe attempts remain blocked for reconciliation.

<a id="goal-pause--resume-and-individual-controls"></a>

### Resume and individual controls

Resuming releases only the goal hold. It never clears an actor mailbox pause, supervision pause, an individual Run pause, or a reconciliation block. Those controls retain their existing recovery APIs. Context, saved answers, attempt counts, model/token reservations, and shared budget scopes remain unchanged by either command.

Deadlines remain absolute UTC timestamps. Pause does not freeze elapsed time or move retry, workflow timer, reminder, or delegation deadlines. After resume, overdue work becomes eligible under its original rules. An expired delegation can time out before child publication; a child that completed before the deadline retains the existing completion ordering. Capacity and individual controls can still defer eligible work.

[Goal cancellation](#goal-cancellation) takes precedence and can clean up a paused goal. Pause and resume then reject the cancelled goal; neither can reopen it. The last pause record remains as audit data alongside the cancellation receipt.

<a id="goal-pause--rust-api-inspection-and-architecture"></a>

### Rust API, inspection, and architecture

```rust,ignore
let pause = agents.actors()?.pause_goal(task_id).await?;
let resume = agents.actors()?.resume_goal(task_id).await?;
```

`GoalPause` owns the reversible control and idempotent receipt policy. `ActorService` uses `GoalControlPort`; SQLite supplies the atomic gate and saved ownership projection. Individual Agent and Run aggregates keep their controls. The execution store exposes runnable work separately from the unfinished Runs needed for recovery. `bootstrap::local_app` wires these ports automatically.

Goal inspection includes optional `goal_pause` and per-node `execution_held`, while retaining physical statuses, wait IDs, errors, and stop reasons. Readable output shows the saved goal control and admission hold. Successful or cancelled requests are not labelled held. A cancellation receipt takes precedence over a retained pause record.

The baseline includes the pause table and derived scheduling views. Existing actors, task envelopes, Runs, cancellation receipts, and accounting payloads are preserved.

<a id="goal-pause--verify-locally"></a>

### Verify locally

```powershell
cargo test --locked --test goal_pause
cargo test --locked --test cli cli_pauses_and_resumes_goals_without_waiting_for_held_work
cargo run --locked --example goal_pause -- .build/goal-pause-demo/demo.db
```

The scripted demo pauses a delegated human wait and scheduled follow-up, stops through the saved deadline, restarts, accepts the answer while held, runs an independent goal, and resumes both branches to completion. Tests cover FIFO and idle detection, late follow-up registration, independent reminders, in-flight results, individual controls, unsafe reconciliation, large trees, ownership isolation, migration, transaction rollback, and process kills after pause, resume, timer creation, and safe/unsafe admission.

<a id="reconciliation"></a>

## Manual reconciliation

An interrupted `RequiresReconciliation` activity becomes indeterminate and blocks its run. `run status` exposes the saved input, handler, activity ID, and latest interrupted attempt ID. An operator inspects the external effects and resolves that specific attempt with `run reconcile RUN_ID --decision-file decision.json`, or `Engine::reconcile` from Rust.

<a id="reconciliation--decision-contract"></a>

### Decision contract

```json
{
  "activity_id": "ACTIVITY_UUID",
  "attempt_id": "ATTEMPT_UUID",
  "decision": { "action": "retry" },
  "evidence": "The external operation has stopped; verified that repeating it is appropriate."
}
```

- `accept` includes `result`, the verified JSON value expected by the handler's workflow continuation. It marks the activity succeeded and delivers the ordinary `ActivityCompleted` event without executing the operation again. An invalid continuation preserves the result and receipt and fails with `invalid_workflow`, matching ordinary completion behavior. Acceptance needs the run's pinned workflow implementation.
- `retry` authorizes one new attempt using the existing activity ID, input, handler, recovery policy, and limits. It queues the activity immediately; the worker performs normal admission. Authorization does not reserve usage itself, increase limits, reset counters, or promise admission if the run's shared allowance changes before dispatch. Exhausted allowances reject authorization and leave the run blocked. The next uncertain outcome blocks again.
- `fail` includes a nonempty `message`. It marks the activity and run failed with `reconciliation_failed`, and schedules no continuation. It makes no assertion that external effects were undone.

All requests require nonblank `evidence` of at most 16,384 UTF-8 bytes; failure messages have the same bound. Unknown JSON fields are rejected. Evidence is an operator assertion; the harness cannot independently establish the state of arbitrary external services or terminate surviving processes. Inspect the effects and ensure the operation has stopped before accepting or repeating it.

<a id="reconciliation--consistency-and-audit"></a>

### Consistency and audit

`Run::reconcile` contains the synchronous domain decision. `Engine::reconcile` invokes it through the application-owned `ExecutionStore::mutate` port. The SQLite adapter commits the receipt, activity result or retry, run state, ordered `activity_reconciled` event, and any continuation in one IMMEDIATE transaction. No separate scheduler or persistence mechanism is added.

The interrupted attempt remains `Interrupted` with its original `Outcome::Uncertain`; reconciliation never rewrites it as a provider-reported success. A receipt on `RunRecord` stores the full request and recording time, and history attributes the decision to the activity and attempt. Original diagnostics remain in preceding history. Evidence is not an authenticated operator identity.

The attempt ID is the idempotency key within the run. The identical full request is a no-op, including after a restart or after a later attempt has blocked or completed. A changed action, result, evidence, or activity ID for that already resolved attempt is a conflict. A receipt for an earlier attempt does not resolve a later interruption. Concurrent requests serialize through the same transaction boundary.

New decisions require a blocked run with an `uncertain_outcome` diagnostic, an indeterminate activity with no active attempt, and the matching latest interrupted attempt with an uncertain outcome. Running, cancelled, failed, budget-exhausted, and implementation-unavailable runs cannot be reopened through this API. Retry/fail can record a decision without the workflow implementation; acceptance requires its exact definition version. The worker still checks handler compatibility before execution.

Pause flags survive reconciliation. Acceptance or retry can leave the run paused until `run resume`; a terminal result remains terminal. Admission counters and uncertain token reservations are retained. Manual reconciliation does not recover provider billing information or refund model usage.

Receipts are a defaulted additive field in existing run JSON, so schema version 3 needs no migration. Existing runs deserialize with an empty receipt list. Model [context selection](state.md#context-management) is bounded for new agent runs; retention of receipts, audit history, and workspace artifacts remains future work.

<a id="reconciliation--verification"></a>

### Verification

The local API offers scoped retry/fail recovery for coding-session native `replace_text`, `create_file` and `run_tests` activities. Its API validates the exact session/root-turn/owned Run, requires explicit stopped-operation confirmation, and reuses this transaction/receipt contract. It shows read-only journal/hash evidence for writes and unknown outcome for interrupted tests. Acceptance remains a CLI operation. See [HTTP tool recovery](api.md#http-api--prompt-first-harness).

```text
cargo test --test reconciliation --test cli --locked
```

Tests kill a child worker after a synced external file effect but before result persistence, confirm it blocks after recovery, accept the inspected result, and finish without repeating the effect. They also verify failed-commit rollback, restart deduplication, concurrent conflicts, retry and model limits, pause, and CLI resolution using actual fixed-test output. Process-kill verification covers application crashes rather than physical power loss.

<a id="turn-recovery"></a>

## Failed turn recovery

After automatic model retries stop, the harness recovery API permits an explicit retry for a settled root planner turn with a transient provider failure or invalid/incomplete model response. Recovery sends the exact saved model request with one additional attempt. It resumes the same Run at the failed model checkpoint; completed tools, delegated results, actor context, permissions and model configuration stay pinned.

Original failed activities, attempts, diagnostics and history remain saved. Each recovery adds a new model activity and a `turn_recovery_requested` event. Successful recovery updates the existing task result and records its answer in actor memory once. Repeated failed recoveries require new explicit decisions.

All previous calls, actual tokens and conservative charges remain counted. Run attempt limits, Run model/token budgets and shared goal/delegation budgets still apply. Unlimited defaults stay unlimited. An outstanding provider `Retry-After` deadline is retained. Recovery does not execute a model call inside the HTTP request.

`GET /v1/harness/sessions/S/turns/T/recovery` returns the exact failed Run/activity/attempt IDs, history sequence, safe diagnostic, target, usage, eligibility reason and the latest 32 recovery receipts.

Send the reviewed identity to the same path with `POST`:

```json
{
  "request_id": "a fresh UUID",
  "run_id": "the saved Run UUID",
  "activity_id": "the failed model activity UUID",
  "attempt_id": "the failed attempt UUID",
  "expected_history_sequence": 31
}
```

Admission commits the new activity, actor's active request, task lifecycle, fresh completion envelope and exact decision receipt in one SQLite transaction. An exact retry returns the original receipt with `replayed: true`, even after restart or completion. A changed body/scope under that decision ID or a competing decision against the same failed checkpoint conflicts. Clients should persist the exact decision before sending it. Replaying that decision recovers an unknown acknowledgement; a new recovery decision resumes execution.

Queued later turns remain held behind the recovered turn until its entire ownership graph and completion receipts settle. Recovery is refused once later turns have launched, the planner has completed newer advanced work, or a summary has admitted/covered the failed turn, because those requests already consumed that history. In that case, send a follow-up. Archived sessions, held/busy planners, unsettled descendants, cancellation, unsafe/indeterminate tools, missing implementations and exhausted budgets also prevent admission. Configuration/authentication errors and refusals need their own correction and a new request. Child-agent and arbitrary workflow recovery are outside this initial scope.

Local HTTP fixtures verify exact request reuse, completed file-read reuse, duplicate decisions, stale/conflicting scope, transaction rollback, restart after admission, repeated failures, finite budgets and later-turn/summary boundaries. No hosted model is needed.

<a id="turn-steering"></a>

## Durable turn steering

New Code sessions also [route planner guidance to active coding workers](planner.md#planner-guidance) while their original child join stays open. Saved worker messages show delivery separately from the root instruction's Pending/Applied status.

Use the steering API below to guide an unfinished native root planner. For example, send “Focus on the API first” while it works. This instruction belongs to the selected turn and its existing Run; it does not create a conversation turn or change the actor's permissions, workspace, model or budget.

Instructions are saved in arrival order. Clients should persist the exact message identity, Run and text before sending. An identical retry after reload, lost acknowledgement, completion or restart recovers the original receipt. Reusing that identity with changed text or another scope conflicts.

**Pending** means the instruction is saved but has not entered a model request. At the next fresh model admission, the runtime rebuilds the planner request with all this turn's instructions, uses its normal bounded context selection, and commits the input, usage reservation and **Applied** delivery together. Applied instructions stay in that turn's future model context. Applied means included in a saved request, not a guarantee that the model followed it. Previous requests, completed tools and history remain saved.

An admitted model/tool call continues under its original request. Automatic retries, interrupted safe-repeat attempts and explicit failed-turn recovery replay their original inputs. Pending instructions wait for the next fresh model decision. They do not answer a clarification, approve a tool, interrupt a child or reopen a terminal turn. Resolve existing waits separately. Goal and Run pauses retain pending instructions until resume. Cancellation and budget checks remain authoritative; refused admission does not mark instructions applied.

If the turn finishes without another model request, pending instructions show **Not applied · turn finished**. They remain inspectable and do not leak into another queued conversation turn. Sending new instructions after completion or cancellation is refused. A queued turn without a Run must launch first.

<a id="turn-steering--guide-an-individual-coding-worker"></a>

### Guide an individual coding worker

Use the worker steering API on an active native coding assignment. Send a focused instruction such as “Keep the existing API.” The runtime verifies the task's saved ownership, accepted workspace handoff and pinned native registration. The child must have started and verified its handoff. Read-only helpers and external adapters do not accept worker guidance through this endpoint.

Worker instructions use the same Pending/Applied contract, ordering, limits, bounded context selection and atomic admission as planner instructions. They belong to that physical task and Run, independently of the actor's mailbox. Parallel siblings, the parent planner and later assignments on the same actor receive none of this guidance. Applied instructions stay in this task's future model context, including decisions following tool results or human answers. Existing waits still need their separate response, and instructions grant no permissions.

The assignment retains **Saved worker instructions** after completion. Pending entries then show **Not applied · worker finished**. Pauses retain the queue; cancellation and exhausted budgets cannot claim delivery. Already admitted requests and automatic or interrupted safe-repeat retries retain their original input. An exact receipt replay can still recover its acknowledgement after the task ends; a new instruction cannot reopen it.

Each planner turn or worker task accepts up to 16 messages, 4096 UTF-8 bytes per message and 16384 serialized instruction-context bytes in total. Instructions are fixed context, so older observations can leave the recent window to make room. The pinned input-byte limit still applies; an instruction that cannot fit current context is refused. If future observations cannot fit alongside saved instructions, execution blocks visibly and retains the pending queue and original checkpoint.

<a id="turn-steering--api-and-persistence"></a>

### API and persistence

`GET /v1/harness/sessions/S/turns/T/steering` returns the Run identity, `can_send`, `blocked_reason`, `terminal` and ordered `items`. Each item includes its exact request, sequence, saved time and optional application with the first receiving model activity and admission time.

Send to the same path with `POST`:

```json
{"message_id":"a fresh UUID","run_id":"the current Run UUID","text":"Focus on the API first"}
```

HTTP 202 saves an immutable admission receipt. Exact replay sets `replayed: true`; use GET for current delivery status. Session/turn/Run checks, the Run queue, history and message receipt commit in one SQLite transaction. A completion racing admission either refuses the new message or leaves a saved, unconsumed instruction. The baseline stores identity receipts. Explicit model-recovery activities preserve their saved inputs. Domain decisions own ordering and request preparation; the harness application port, SQLite adapter and HTTP adapter retain the existing inward dependency direction.

For a delegated coding task `C`, use `GET` or `POST /v1/harness/sessions/S/turns/T/workers/C/steering`. POST uses the same strict three-field body; `run_id` must be the child's current Run. GET returns the child's `task_id` and instruction history. The turn's `delegated-work` snapshot also includes `steering` on each coding assignment, from the same read transaction as its other saved facts. Message identities are checked across both planner and worker receipts, so an acknowledgement cannot be reused for another task or turn.

The baseline includes an indexed worker receipt table with session, root turn, physical task and Run foreign keys. Existing Runs, actor pins, model requests, permissions and root receipts remain unchanged.

Local integration tests exercise ordered duplicate/restart delivery, frozen admitted inputs and retries, pause with saved human answers, terminal/cancelled work, strict scope and byte/count limits, transactional rollback, budget refusal, tool-result reuse, explicit failed-turn recovery. Worker tests also cover parallel sibling isolation, later assignments on the same actor.

<a id="worker-recovery"></a>

## Durable coding worker recovery

A native coding assignment with a verified workspace handoff can hold a recoverable failed model step before its actor publishes completion. The physical task projects as `needs_recovery`; its parent delegation and parallel group remain unresolved. The actor owns the same request throughout the hold, so later tasks on that actor remain queued.

The Run stores the exact failed activity and attempt plus a recovery checkpoint. Existing Runs deserialize with no checkpoint. The baseline includes durable decision receipts without rebuilding or resetting saved actor configuration. Existing coding sessions can use recovery for newly observed eligible failures; previously delivered failures stay terminal.

Eligible errors match root model recovery: transient provider transport/HTTP errors and invalid or incomplete model responses, from `agent.model` v1 with `SafeToRepeat`. Automatic retries settle first. Configuration, authentication/refusal, tool, context and uncertain side-effect failures do not enter this flow. Root planner recovery and interrupted tool reconciliation retain their separate contracts.

<a id="worker-recovery--decisions"></a>

### Decisions

The assignment offers **Retry saved step** and **Accept failure**. Retry clones only the failed model activity into a new activity with one attempt, the same input/target/reservation and `preserve_input`. Provider Retry-After retains its original absolute deadline. Completed tools, workflow state, workspace contents, steering and Run/shared usage are retained. Pending instructions wait for a later fresh model decision. Another eligible failure creates a new checkpoint that requires a new decision.

Accept failure saves the decision without modifying the original failure. Normal actor completion then releases one final failure to the parent, including accumulated usage. Intermediate failures and explicit retries do not increment actor completion or supervision counts. A parent can choose subsequent work using the final outcome; acceptance does not synthesize a successful result.

Both choices require the exact current session, root turn, physical child task, Run, activity, attempt and history sequence. The adapter checks the saved parent delegation, verified handoff and native coding registration. Decisions and Run changes commit under one SQLite writer transaction. Duplicate request IDs replay the exact receipt even after restart or later completion; changed payloads or scope are refused. Stale checkpoints cannot authorize a later failed attempt.

Retry obeys existing attempt, per-Run and shared budgets. Usage is never reset. Accept failure remains available with exhausted budgets. Archived sessions require restoration; paused assignments and ancestor goals require resuming before a recovery decision. Cancellation and saved delegation deadlines clear pending recovery through normal task cancellation, allowing joins to settle. A recovery hold keeps the conversation unsettled and prevents later turns, summaries and workspace cleanup from consuming it.

<a id="worker-recovery--local-api"></a>

### Local API

`GET /v1/harness/sessions/{session}/turns/{turn}/workers/{task}/recovery` returns pending state, checkpoint, model target, usage, decision eligibility/reasons and the latest 32 receipts. The same view is included in the consistent delegated-work snapshot.

`POST` to that route returns `202` with an immutable receipt. Strict request fields:

```json
{
  "request_id": "<new UUID reused after an unknown acknowledgement>",
  "run_id": "<saved Run>",
  "activity_id": "<failed model activity>",
  "attempt_id": "<failed attempt>",
  "expected_history_sequence": 42,
  "decision": "retry"
}
```

The other decision is `accept_failure`. Extra fields, injected model settings, results, permissions or writable paths are rejected. `409` means refresh and inspect the current saved assignment before making a new decision.

<a id="worker-recovery--verification"></a>

### Verification

```powershell
cargo test --locked --offline --test http_api worker_recovery::
```

Tests exercise exact retry after completed tools, pending steering, repeated failure, restart, duplicate/concurrent decisions, stale/wrong scopes, transactional rollback, parallel joins, finite budgets, pause/cancel, excluded failures.

<a id="worker-cancellation"></a>

## Stop one delegated assignment

The planner can also request a scoped stop using [`stop_worker`](planner.md#planner-stops) during guidance/finding coordination in new Code sessions. It uses the same durable cancellation holds and cleanup rules.

Use **Stop assignment** on a worker card in **Delegated work**, then confirm the selected goal. This permanently stops that physical task. Its parent, siblings, actor identity and later assignments retain their existing controls. Queued, active, waiting and **Needs recovery** assignments are eligible, including read-only helpers. Finished assignments have no Stop button. Paused goals can still receive a stop; remaining work continues when the goal resumes.

The application verifies the session, root turn, authoritative ownership and saved parent delegation. It roots the existing cancellation admission hold at the selected assignment and its descendants. This uses the existing actor/Run ports and cancellation rules; the original goal is not cancelled.

Queued work is removed from its mailbox or accepted queue without launching a Run. Active work stops at a safe boundary. An admitted model request can finish and save its outcome and usage, but cannot schedule further execution. An admitted unsafe edit, command or test must settle first. Interrupted effects remain blocked for explicit reconciliation, with the stop still saved. A reconciliation retry cannot execute another operation under that hold. Recording a reconciliation failure retains the failed outcome instead of inventing a successful cancellation.

The parent receives one final outcome through the existing delegation join. Stopping a member of a parallel group retains other members' results and waits; the group joins once every member settles. Partial edits stay in the isolated worker workspace, and saved tools, results, instructions and usage remain available. Stop does not undo files or import a worker patch into the original project. A finished assignment cannot be retroactively cancelled.

The saved cancellation projection distinguishes pending admitted work, reconciliation and settled cancellation. Its receipt remains after restart.

<a id="worker-cancellation--http-and-retry-contract"></a>

### HTTP and retry contract

`GET /v1/harness/sessions/S/turns/T/workers/C/cancellation` reads eligibility, current status, the saved stop state and the latest 32 receipts. This projection is also included in the delegated-work response.

`POST` to that route accepts exactly:

```json
{"request_id":"<new UUID reused after an unknown acknowledgement>"}
```

It returns `202` with a receipt binding that identity to the exact session, turn and physical assignment. The task ID is sufficient for queued-to-running transitions; the request never targets an actor's current task. The receipt and cancellation hold commit in one SQLite writer transaction. The baseline includes the receipt table, retaining existing saved tasks, actor pins and usage.

Exact retries replay the original receipt after restart, completion or archive. Reusing the identity for another assignment is refused. Fresh requests for a finished, already-stopping or archived assignment return `409`. Restore an archived session to make a new decision. Wrong ownership is refused. No model, tool, permission, workspace or result overrides are accepted.

<a id="worker-cancellation--verify-locally"></a>

### Verify locally

```powershell
cargo test --locked --offline --test http_api worker_cancellation::
```

Deterministic local fixtures cover parallel joins, queued removal, admitted models, unsafe filesystem edits and reconciliation, pauses, held model recovery, restart and duplicate requests, changed scopes, receipt rollback and reuse of the same actor for later work. They require no provider key.

<a id="worker-pause"></a>

## Pause and resume one delegated assignment

Use **Pause assignment** in **Delegated work** to hold one physical task and its descendants while siblings continue. **Resume assignment** releases that same hold. Queued, active, waiting, blocked and Needs recovery assignments are eligible, including read-only helpers. Finished and stopping assignments have no actionable pause/resume control. Archived sessions require restoration.

New Code planners can also save their own [assignment holds](planner.md#planner-pauses). Their `pause_worker` / `resume_worker` decisions are independent of these manual controls. Manual Resume releases only your hold; a planner hold remains until the planner resumes it or the assignment stops. Send **Guide this turn** when its dependency is ready. Worker cards and Planner coordination show ownership.

The hold uses the existing durable goal admission gate, rooted at the selected assignment rather than its original goal. It does not pause the worker actor or change its identity, pinned configuration or later assignments. The parent keeps waiting for the existing delegation join; a pause creates no terminal outcome. A held queued task retains its mailbox capacity and FIFO order, so later tasks on the same actor cannot pass it.

If pause commits first, new activity admission, retries, timer firing, owned reminder publication and descendant delegation execution remain held. Operations already admitted can finish and save results and usage. The saved pause projection reports admitted operations until they settle. Human answers, approvals and eligible guidance can be saved while held; their continuation or delivery waits for resume. Queued assignments without a Run cannot yet receive worker guidance.

Resume preserves the same Run, workspace, completed tools, context, steering, test evidence and charged/reserved usage. It releases only this assignment's hold. Whole-goal or ancestor-assignment holds, actor/Run pauses, supervision, recovery decisions and reconciliation blocks remain authoritative. Whole-goal resume likewise cannot clear an assignment hold. A model recovery decision requires releasing the assignment and ancestor holds first. An uncertain edit stays blocked after resume; explicitly reconciled work still waits if held.

Deadlines remain absolute. Pause never adds time to retry, timer, reminder or delegation deadlines. An unpaused parent can enforce an expired delegation deadline and cancel its paused child. **Stop assignment** and whole-goal Stop take precedence, retaining unsafe-operation reconciliation and saved results.

<a id="worker-pause--api-and-exact-retries"></a>

### API and exact retries

`GET /v1/harness/sessions/S/turns/T/workers/C/pause` returns the saved assignment `control` (including its revision), effective `execution_held`, `inherited_held`, count of `admitted_operations` across its subtree, eligibility and latest 32 receipts. The same view appears under `pause` in each delegated-work card. Physical task status is retained separately from the admission hold. Terminal assignments report no effective hold, even if old pause records remain saved.

`POST` accepts exactly:

```json
{"request_id":"<new UUID>","expected_revision":0,"paused":true}
```

Use `paused:false` and the newly observed revision to resume. It returns `202` with an immutable receipt binding the request to the exact session, root turn, physical assignment and resulting control. The nullable Run ID supports queued tasks. Ownership and the saved parent delegation are verified under the same SQLite writer transaction as the hold transition and receipt insertion.

Actual transitions increment the assignment's own revision once. Fresh requests with stale revisions or the already-selected state return `409`; refresh and review before making another decision. Never automatically replace a saved request's revision. Clients should persist the exact request identity and body before sending, and reuses them after reload or an unknown acknowledgement.

Exact retries return the original receipt with `replayed:true`, even after a newer transition, completion, archive or restart. Replaying an old pause receipt does not reapply its hold. Changed bodies or scopes under the same request ID are refused. Nonnil request identities and safe integer revisions are required; unknown fields, workspace/permission/model overrides and root-task targets are rejected.

The baseline includes the receipt table. Existing task, actor, Run and goal control records keep their original contents. DDD boundaries remain inward: the domain owns request/receipt types and reversible revision policy, the harness service uses its persistence port, and SQLite/HTTP supply adapters. Scheduling continues through the existing actor and Run gates.

<a id="worker-pause--verify-locally"></a>

### Verify locally

```powershell
cargo test --locked --offline --test http_api worker_pause::
```

Local deterministic model fixtures cover parallel progress and joins, queued mailboxes, admitted models, unsafe edits/reconciliation, guidance and answers, model recovery, independent goal/actor holds, deadline expiry, exact/stale retries, receipt rollback, later assignments on the same actor and schema upgrade. No hosted provider key is needed. Historical manual validation used a disposable database and project to verify Pause, restart and Resume.

<a id="worker-clarification"></a>

## Clarification and stalled assignments

New native tool actors pin a clarification policy at creation. Their instructions lead with carrying out the assigned goal, inspecting available evidence and requesting permitted tools. `ask_human` is for essential missing information. Reading a permitted file and requesting an edit or fixed test do not need an extra confirmation question: the runtime creates the normal exact approval wait when the selected tool requires it.

The saved NVIDIA trial that motivated this change contained both previous human answers in the worker's model inputs. The model nevertheless kept asking for permission to start. The earlier tool policy began with “Answer the user's question.” The new instructions address this ambiguity; they do not guarantee that a particular model will make useful decisions.

<a id="worker-clarification--when-a-worker-needs-guidance"></a>

### When a worker needs guidance

The default policy permits two ordinary clarification waits. Selecting a third consecutive `ask_human` action without native tool feedback saves a **Needs guidance** wait. It retains the exact physical assignment and Run, workspace, context, results, test status and usage. No next model activity is scheduled until a human supplies guidance. Other assignments continue and the parent retains its original delegation join.

This is a deterministic count of consecutive question actions, including different wording. It does not classify semantic duplicates or detect all forms of model loops. A completed native tool result, including expected rejection feedback, or a tool approval denial resets the counter. Requesting a tool or merely saving its approval does not reset it before tool feedback is committed. Human clarification answers, planner messages and ordinary steering alone do not reset it. Explicit guidance does reset it.

The saved guidance wait includes recent question/answer pairs and the latest question. Respond with a focused instruction through its exact inbox wait ID. Guidance enters the next bounded model input. Each wait admits at most one continuation; exact retries return the original receipt and changed answers conflict. Repeated stalling requires a new decision. Assignment cancellation remains available.

This wait suspends workflow progression through the existing input boundary. It is independent of manual and planner assignment holds, actor pauses and whole-goal pauses. Guidance can be saved under a hold, but execution still waits for that hold to be released. The guidance form explains this. Absolute delegation deadlines and cancellation retain their normal authority.

Token and model-call budgets remain unlimited by default. The clarification policy does not change budgets, grant permissions, approve tools, invent test results or automatically fail a stalled assignment.

<a id="worker-clarification--domain-and-http-contracts"></a>

### Domain and HTTP contracts

Tool actor configuration can pin:

```json
{"clarification_policy":{"version":1,"max_consecutive_questions":3}}
```

Version 1 accepts limits from 1 through 8; the default is 3. The runtime pins this default only when creating a new tool actor with a tool catalog. Task payloads cannot override it. Existing actor/session/Run pins without the policy retain their previous instructions and question behavior, including after restart and follow-up. Use a new session to obtain the new defaults.

The domain owns the policy, counter and transition rules. Application services reuse the existing human-decision persistence port. SQLite commits the wait or guidance observation and its model continuation atomically. Schema 50 prevents older hosts from opening checkpoints whose guidance behavior they do not know; it adds no new table, queue, scheduler or agent policy.

The normal team inbox exposes `kind: "guidance"`, wait key `native_guidance`, and a payload containing `code: "repeated_questions"`, the count, latest question and up to two available recent clarification observations. The exact answers remain in history if bounded context evicts them. Delegated work adds `guidance_wait_id` only for an assignment with this saved wait.

Use the existing response endpoint and exact wait identity:

```text
POST /v1/teams/TEAM/tasks/TURN/waits/WAIT/responses
```

```json
{"kind":"answer","answer":"Read src/api.rs, implement the assigned change and run the pinned tests."}
```

The existing scoped ownership, nonblank/65,536-byte answer bound, total model input bound and exact retry receipts apply. An invalid or oversized answer preserves the original wait. An approval response cannot resolve guidance. Stopped assignments reject new input.

The dependent coding example leaves this wait saved and exits with diagnostic evidence rather than automatically answering it. Inspect its saved database through the API to review and guide the assignment.

<a id="worker-clarification--local-verification"></a>

### Local verification

```powershell
cargo test --locked --offline --test clarification
cargo test --locked --offline --test http_api worker_clarification::
cargo test --locked --offline --test dependent_coding_demo
```

Tests cover saved answer inclusion, bounded-context eviction, tool approvals and expected feedback, invalid guard state, exact concurrent responses, restart before and after guidance, a separate assignment hold, independent sibling progress, one original group join, scoped refusals, oversized guidance, cancellation and unlimited budgets. The HTTP fixture uses saved read evidence and explicit incomplete completion; the separate dependent coding demo verifies real tests and final patch application.

On 12 October 2026, the four domain/compatibility checks, 244 active HTTP tests, the native tool/context/storage/migration suites, the two coding-demo tests, 84 JavaScript tests and all-target Clippy with warnings denied passed. A browser fixture restored the saved guidance wait, submitted a focused instruction, observed the worker's file read and verified one original group join. That fixture intentionally finishes incomplete after inspection; it does not claim a successful code repair.

The NVIDIA rerun received the new task-focused instructions but still chose three consecutive questions. It retained a `native_guidance` wait with `needs_guidance: true`; the dependent consumer remained held at zero model calls. The example exited with saved diagnostic evidence instead of continuing the question loop. This validates the live safeguard, while end-to-end hosted coding success remains unverified.

<a id="session-permissions"></a>

## Durable session and task permissions

Manual coding sessions ask before each native test run by default. Approve the exact pending wait for one execution, or use the permissions API below to grant subsequent matching requests. Permission inspection exposes the command, timeout, isolated workspace and recent decisions. The same API supports revocation.

A grant belongs to one session and its pinned planner actor. It matches the complete native `run_tests` v1 contract, the managed workspace path, empty tool arguments, and the exact executable/argument array and timeout. Changed contracts or commands do not match. Helpers, other sessions, file edits, child imports and patch application require their existing authority and approvals. Actor configurations and model requests never gain permission fields.

<a id="session-permissions--delegated-worker-edits"></a>

### Delegated worker edits

Manual delegated coding workers ask before each edit by default. When a worker has **explicit saved task writable paths**, the permissions API can grant native edit access for that task. This approves the current request and saves authority for later pinned `replace_text` v1 and `create_file` v1 calls within those paths. File reads stay available under the normal tool contract. Workers with inherited writable paths, read-only helpers, root planners, and other tasks cannot receive this grant.

The saved grant binds the session, root turn, physical task, Run, actor, complete accepted workspace handoff (including snapshot identity, worker configuration pin and assigned paths), and complete edit tool contracts. The worker checks those bindings and the exact pending approval payload again before each automatic approval. Model text and HTTP clients cannot supply alternative paths, workspaces or tools. Actor defaults and pinned model requests remain unchanged.

Use the permissions API to inspect a turn's worker grants or revoke an active grant. The permission survives restart and expires as soon as the task or its parent ends or is cancelled. A later task on the same actor needs a new permission. Expired and revoked grants remain visible with their saved provenance; immutable decision receipts preserve earlier grants. These permissions cover native edits only. Tests, named commands, child patch imports and application to the source project keep their separate approvals. This is not an OS sandbox for subprocesses.

Grant, current approval, continuation and receipt commit in one SQLite transaction. Automatic approval and revocation use the same write boundary. A request authorized before revocation can still settle; revocation restores approval for subsequent requests. Paused goals do not receive automatic approvals. A human can save a grant while paused, and its authorized work stays held until resume. An interrupted edit still requires the existing reconciliation flow.

<a id="session-permissions--delegated-worker-tests"></a>

### Delegated worker tests

At a delegated coder's native test approval, approve the pending wait for one execution or use the task-test permission API for subsequent matching tests. This grant belongs to the physical task and Run, its pinned actor, accepted workspace handoff and isolated workspace, and the complete `run_tests` v1 contract, empty arguments, executable/argv and timeout. Both explicit task write scopes and inherited paths qualify; this permission grants no edit authority.

The permission survives restart, can be revoked through the permissions API, and expires when the task or its parent ends, is cancelled or times out. A later task on the same actor needs a new grant. The planner's session test grant and other workers' grants do not cover this task. Edits, named project commands (even with the same argv), child imports and source-project application retain separate approvals. Test commands remain subprocesses rather than an OS sandbox.

Granting saves the permission, current approval, continuation, session revision and immutable receipt in one transaction. Automatic test authorization rechecks the accepted ownership, actor/handoff pins and exact saved pending request, then commits `task_test_authorized` evidence with the approval and continuation. Paused goals remain held. Revocation serializes with authorization; an already authorized request may settle, while later tests ask again. Interrupted execution still requires reconciliation. Test results and workspace fingerprints retain their normal completion checks; child tests never replace fresh parent tests after imports.

Exact old grant/revoke retries return their original receipts without reapplying an old grant or revoking a replacement. Active, revoked and expired grants are shown for the selected turn, with command, timeout, workspace and task provenance.

The worker resolves a matching saved test wait in a SQLite transaction. It reads current permission, verifies session/turn/actor ownership and pinned contracts, saves an approval receipt, records `test_command_authorized` with the grant identity and exact request, and schedules the existing workflow continuation together. Test execution still saves its normal activity input, attempts, output and result; workspace fingerprints and passing-test completion requirements remain in force. Recovery of uncertain test execution still needs reconciliation.

Revocation and automatic authorization serialize in the same database. If revocation commits first, a pending test remains a human approval wait. If authorization commits first, that already authorized request can execute and settle, including after a restart. Revocation prevents subsequent automatic approvals; use Pause or Stop to control remaining work. Paused goals do not execute automatically authorized tests. An explicit grant can be saved while a goal is paused; work stays held until resumed. Cancelled or archived work cannot receive a fresh grant, and revocation remains available.

Permission changes have stable client request identities and expected policy revisions. Exact retries return the original immutable receipt, even after completion or a later revocation. They never reapply the old policy. Changed bodies/scopes for the same identity, stale revisions, replaced waits and stale grant identities are conflicts. Grant, approval, Run continuation, policy and receipt commit together or roll back together. Clients should persist the full decision before sending it.

<a id="session-permissions--http-and-rust-ports"></a>

### HTTP and Rust ports

`GET /v1/harness/sessions/S/turns/T/permissions` returns current `state` (`revision`, optional `test_command` grant), `archived`, the latest 20 permission decision receipts (newest first), and an optional authoritative `eligible_wait` for the selected root planner turn. It also returns `eligible_edit_waits` and `task_edits` for that turn, with each edit grant's `active`, `expired` or `revoked` status. Both task lists are limited to 128 entries; `task_edits_truncated` reports additional saved work. The full receipt history stays in SQLite.

`POST /v1/harness/sessions/S/permissions` accepts one strict request:

```json
{
  "request_id": "CLIENT_UUID",
  "expected_revision": 0,
  "action": {
    "kind": "allow_test_command",
    "task_id": "TURN_UUID",
    "run_id": "RUN_UUID",
    "wait_id": "WAIT_UUID"
  }
}
```

The command and workspace are derived from the saved wait and actor pins; the HTTP body cannot supply alternatives. Revoke with `action: {"kind":"revoke_test_command","grant_id":"SAVED_GRANT_UUID"}` and a new request identity/current revision. Responses include the saved request, resulting state, timestamp and `replayed` flag.

For task edits use `action: {"kind":"allow_task_edits","task_id":"CHILD_TASK_UUID","run_id":"CHILD_RUN_UUID","wait_id":"WAIT_UUID"}` from an eligible edit wait, or `action: {"kind":"revoke_task_edits","grant_id":"SAVED_GRANT_UUID"}`. Every permission kind shares the session's policy revision. Task decisions also return the saved `task_edit` grant. An exact old retry returns its original receipt without reapplying or revoking a newer grant.

`HarnessService::permissions` and `change_permissions` use `HarnessStorePort`. The domain owns policy revision/admission and Run approval matching. `ExecutionStore::authorize_test_wait` gives the worker an atomic persistence boundary; its default implementation performs no automatic approval. Bootstrap installs the SQLite implementation. The baseline includes permission state and immutable receipts; the default policy uses per-call approval and retains saved waits.

`ExecutionStore::authorize_task_edit_wait` supplies the equivalent task edit boundary, also defaulting to no automatic approval. The baseline includes indexed per-task grants. Existing saved waits, test grants, receipts, actor pins and model requests are preserved.

Task tests use `action: {"kind":"allow_task_tests","task_id":"CHILD_TASK_UUID","run_id":"CHILD_RUN_UUID","wait_id":"WAIT_UUID"}` or `action: {"kind":"revoke_task_tests","grant_id":"SAVED_GRANT_UUID"}` with the same shared session revision. Clients cannot supply commands, timeouts, workspaces or paths. Receipts include `task_test` for task-specific grants. The selected turn's GET view adds `eligible_task_test_waits`, `task_tests` and `task_tests_truncated`, bounded to 128 entries per list. `ExecutionStore::authorize_task_test_wait` defaults to no automatic approval. The baseline includes a separate indexed task test grant table without changing actor pins, model schemas, existing grants, receipts or waits.

<a id="session-permissions--verification"></a>

### Verification

```text
cargo test --locked --offline --test http_api permissions::
cargo test --locked --offline --test http_api task_edit_permissions::
cargo test --locked --offline --test http_api task_test_permissions::
```

Integration checks run a real local test command, restart before its execution, authorize later turns, revoke and restart again, and verify unchanged source files. Further checks cover exact retries, concurrent duplicate grants, changed identities/scopes, stale revisions, rejected supplied commands, cancellation, transaction rollback, paused execution, mismatched workspace/timeout/tool contract/actor/session bindings.

Task permission checks exercise native text edits and file creation, restart before execution, out-of-scope requests, separate tests, replacement grants, expiry and reuse of the same worker by a later task. They also cover inherited scopes, cancellation, identity/revision conflicts, grant and automatic-approval rollback, paused restart recovery, changed saved bindings, a revocation race.

Task test checks execute a real pinned local command through restart, automatic repeats, revocation and replacement, with separate edit/command/planner/later-task approvals. Additional checks cover inherited scopes, cancellation, strict bodies and stale identities, grant and automatic-approval rollback, paused restart, tampered command/timeout/workspace/contracts/handoff bindings, a revocation race.
