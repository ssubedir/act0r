# Coding and worker coordination

Delegation, isolated workspaces, dependencies, messages and reviewed patch transfers.

- [Native coding delegation in the harness](#harness-coding-delegation)
- [Native agent delegation](#delegation)
- [Shared delegation budgets](#delegation-budgets)
- [Durable native task groups](#task-groups)
- [Initial dependencies between coding assignments](#initial-dependencies)
- [Durable workspace inputs for delegated tasks](#workspace-handoff)
- [Durable child artifact handoff](#child-changes)
- [Durable worker messages](#worker-messages)
- [Durable worker questions and replies](#worker-questions)
- [Durable planner reactions to worker findings](#worker-findings)
- [Durable assignment dependencies](#worker-dependencies)
- [Approved patch handoff between coding workers](#worker-changes)

<a id="harness-coding-delegation"></a>

## Native coding delegation in the harness

A new harness request with `workspace` and `coding` automatically assembles three native actors: a planner, a read-only helper and one coder by default. No team JSON is needed. Saved project profiles use the same coding request settings. Read-only sessions continue with planner and helper.

The planner can edit directly or delegate a focused coding task to `coder`. The coder uses the existing `tool` v1 actor with its own durable mailbox, bounded successful-task memory and serialized task execution. It inherits the session's model target, writable paths, fixed test command and named project commands. Its default workspace is distinct from the planner and helper copies.

New coding planners can supply optional per-task `writable_paths` on an individual delegation or each group member. For example, assign `src/api.rs` to `coder` and `src/store.rs` to `coder2`. The scope narrows the child's native editing tools within both actors' pinned permissions while retaining reads of project context. Omission inherits worker permissions. The saved handoff and child Run preserve the scope across restart; later tasks and worker defaults stay independent. See [task scopes](#workspace-handoff--per-task-writable-paths).

The **Delegated work** panel displays task-specific or inherited writable paths. Use **Guide worker** on an active assignment to save instructions for that task's next fresh model request. Its Pending/Applied history survives restart and remains separate from siblings and later tasks on the same actor. See [durable worker guidance](recovery.md#turn-steering--guide-an-individual-coding-worker). Test and project commands are not OS sandboxes; a scoped patch containing an out-of-scope file is refused at import. Fresh child and combined parent tests, individual import approval, and conflict checks remain required.

New coding sessions give coders [durable worker messages](#worker-messages). They can share findings with their parent or a sibling in the same parallel group; accepted notes enter the recipient's next fresh model call and keep their physical task binding across pauses and restart. Delegated-work cards show message and delivery history. Notes do not grant permissions or replace patch review and combined tests.

New two-coder sessions also support [durable sibling questions](#worker-questions). An accepted question suspends its requester until the pinned sibling replies, the original deadline expires, or the sibling finishes/stops. Waiting releases execution capacity; circular waits are refused and existing holds remain.

<a id="harness-coding-delegation--repair-and-review"></a>

### Repair and review

1. The planner saves a `delegate_task` decision naming `coder` and a bounded goal.
2. [Workspace handoff](#workspace-handoff) captures the planner's current code and creates an isolated task copy. The accepted task pins that saved input.
3. The coder reads/edits its task copy and runs its fixed tests. Editing and test execution retain their normal durable approval waits. Completion requires fresh passing tests against its captured final code. At the test wait, the human can allow the exact pinned test command for this physical task. Permission survives restart, is revocable and expires when the task ends; later tasks and parent tests keep separate approval. See [task permissions](recovery.md#session-permissions--delegated-worker-tests).
4. The planner uses `inspect_child_changes` to inspect that successful direct child's immutable patch and test evidence, then `import_child_changes` to request approval. API inspection returns the saved patch page, changed files and evidence, with **Import reviewed patch** and **Decline import** controls.
5. Approval authorizes the exact inspected artifact into the planner's copy through the existing import journal. Conflicts refuse overwrite; declining supplies saved feedback. The planner runs fresh combined tests before its final completion. Child tests cannot satisfy this parent completion gate.
6. The final planner artifact contains both its own changes and imported repairs relative to the turn's original baseline. Applying it to the user's project still uses the separate reviewed application flow.

<a id="harness-coding-delegation--optional-parallel-coding"></a>

### Optional parallel coding

For a new coding session, set `coding.coder_count: 2`; omission or `1` keeps one coder. Other values are refused before creating a session. This choice is saved with project profiles and pinned for every follow-up in the session.

Two-coder mode adds a distinct `coder2` actor alongside `coder` and enables `delegation.max_parallel_tasks: 2`. The worker descriptions guide the planner to assign distinct files in each goal and use `delegate_tasks` for independent subtasks. Dependent tasks can still use sequential `delegate_task` decisions. New sessions can [declare one dependency upfront](#initial-dependencies) with `depends_on` on the receiving `delegate_tasks` member. That worker prepares its workspace and original baseline, then waits before its first model call. The same planner reviews the prerequisite outcome, requests patch transfer approval and explicitly resumes the receiver. A consolidated receiver patch includes its prerequisite's changes; import it once and run fresh parent tests.

Both children get separate copies of the same saved snapshot of the planner's current code. Each actor serializes its own mailbox tasks; different actors can execute concurrently. Their all-finished join saves results in request order, including failed children, before the planner continues. See [task groups](#task-groups).

The planner inspects and requests approval for each successful patch separately. Imports check the saved before hashes against current parent files. Overlapping edits refuse overwrite rather than merging automatically. The planner must handle that feedback and run fresh tests against the combined workspace. Parallel mode permits delegation; it does not guarantee a model will split a request or assign nonoverlapping files. Existing conflict checks remain decisive.

`worker run --max-concurrent-activities 2` allows two concurrent activities; its default is one. `api serve --with-worker` defaults to two activity slots; `--max-concurrent-activities 1` serializes its worker.

Parallel calls can increase simultaneous provider requests and hit rate limits; normal durable provider retries and shared budget accounting still apply.

Delegation is a model decision; there is no guarantee every coding request delegates. Planner and each coder have 128 total activity attempts each; the existing four-task/two-depth delegation guard remains. Model-call and token budgets default to unlimited, or share the configured turn allowance across planner and descendants. The coder does not own a new goal budget that could reset that allowance.

<a id="harness-coding-delegation--recovery-and-compatibility"></a>

### Recovery and compatibility

New coding workers can use `ask_worker` with `recipient: "parent"` to get a decision during delegation. The existing planner answers one saved question at a time, can inspect its own managed code with up to three read/list/search tools, and returns to its original single/group join. Saved read evidence survives restart; it does not include workers' in-progress isolated files. Questions cannot grant tool permissions or replace child tests, patch inspection or imports. This works with one or two coders; [question deadlines and recovery](#worker-questions--planner-coordination) remain durable across restart, pause and stop.

During questions and user-guidance routing, new Code sessions let the same planner inspect up to three [saved assignment snapshots](planner.md#assignment-inspection). The evidence includes status, waits, recent tool/test results and messages from the exact current tasks. Inspection retains isolation, approval and test gates.

Child task identity, workspace inputs, tool approvals, results and import waits survive runtime restart. A goal pause holds descendant execution even when an approval is saved; resume continues the same task. Stop cancels owned child work and prevents pending edits from dispatching. Already admitted effects retain the engine's existing cancellation and reconciliation semantics.

An interrupted unsafe edit/import requires explicit reconciliation. The saved per-file journal exposes the exact approved import and its conflicts. Native tool and final project application recovery use the same Run recovery boundary. See [child imports](#child-changes), [tool recovery](harness.md#coding) and [reconciliation](recovery.md#reconciliation).

Existing sessions retain their saved actor pins. A pending start request from before this change can be retried with the same message ID without adding a coder or changing its planner schema. Existing one-coder starts retry exactly without adding `coder2` or group actions; changing the worker count under the same message ID is refused. Follow-ups reuse the saved assembly; start a new coding session to enable two coders. No SQLite migration, new scheduler, planner version or external framework is introduced. Assembly belongs to the harness domain and existing application services supply the ports/adapters.

Existing one- and two-coder sessions also retain their unscoped planner schemas on exact start retries. New coding sessions pin `delegation_write_scopes: true`; follow-ups reuse that saved choice.

<a id="harness-coding-delegation--verify-locally"></a>

### Verify locally

```powershell
cargo test --locked --offline --test harness
cargo test --locked --offline --test http_api harness_coder
cargo test --locked --offline --test http_api parallel_coding
cargo test --locked --offline --test child_changes --test workspace_handoff --test verified_completion
```

Local compatible-model fixtures execute real Rust tests without a provider key. They check current-code input, stable child identity across pause/restart, immutable import approval recovery, refused premature parent completion, combined tests, cancellation, shared call/token exhaustion and source preservation. Assembly checks cover explicit command/permission pins, workspace isolation and exact retries of current assemblies. Existing native handoff/import suites also exercise killed-host recovery boundaries. Parallel harness tests require both model HTTP calls to overlap, reopen a paused partial join with a saved human answer, retain actor/task/snapshot identities, import disjoint patches, refuse overlapping patches, and run real combined Rust tests.

<a id="delegation"></a>

## Native agent delegation

This document describes the native planner's single-child delegation and the shared lifecycle. [Task groups](#task-groups) enable bounded fan-out and ordered durable joins in the same planner, using the same lifecycle, deadlines, and shared budget ports.

The current planner can also [call native tools directly](planner.md#planner-tools) and use saved results to choose child tasks. Tool and worker allowlists are pinned separately; approvals and tool attempts use the existing Run engine.

A native `planner` agent can ask another registered actor to solve a task, wait durably, and continue with its saved result. This is self-contained Rust using the existing SQLite mailbox and Run engine. Each actor still processes messages serially and owns a stable identity; each submitted task has its own Run. The planner waits without holding an executing activity.

<a id="delegation--restart-demo"></a>

### Restart demo

No model server or credentials are required:

```powershell
cargo run --locked --example actor_delegation -- .build/actor-delegation-demo/demo.db
```

The planner chooses the answering actor, which asks for human clarification. The example closes and reopens the app, verifies the same child task ID, supplies the clarification, and finishes both tasks. Output includes parent and child handles, results, and separate usage ledgers. It also writes a usable `planner.json` next to the database, referencing the answering actor it created. Process-kill tests separately cover the committed intent and published child boundaries.

<a id="delegation--cli"></a>

### CLI

Create the permitted worker first, then pin its ID in the planner configuration:

```powershell
cargo build --locked
$db = '.\delegation.db'
$answerId = (& .\target\debug\act0r.exe --db $db actor agent create answer --config-file .\examples\configs\answer-scripted.json --attempt-limit 10).Trim()
@{
    goal = 'Explain durable recovery with help from the assistant.'
    target = @{ provider = 'scripted'; model = 'planner-fixture'; endpoint = '' }
    max_model_calls = 4
    max_tokens = 50000
    max_output_tokens = 512
    max_input_bytes = 65536
    workers = @(@{ name = 'assistant'; agent_id = $answerId; description = 'Explains durable execution.' })
    delegation = @{ max_tasks = 2; max_depth = 1 }
    script = @(
        @{ kind = 'delegate_task'; worker = 'assistant'; goal = 'Explain how durable agents recover.' }
        @{ kind = 'finish'; summary = 'The assistant explained recovery after clarification.' }
    )
} | ConvertTo-Json -Depth 8 | Set-Content .\planner.json
$plannerId = (& .\target\debug\act0r.exe --db $db actor agent create planner --config-file .\planner.json --attempt-limit 10 --task-capacity 2).Trim()
@{ goal = 'Explain durable recovery through the assistant.' } | ConvertTo-Json | Set-Content .\goal.json
$taskId = (& .\target\debug\act0r.exe --db $db actor send $plannerId --input-file .\goal.json).Trim()
.\target\debug\act0r.exe --db $db worker run --until-idle
$parent = (& .\target\debug\act0r.exe --db $db actor task status $taskId | ConvertFrom-Json)
$child = (& .\target\debug\act0r.exe --db $db actor task status $parent.delegation.id | ConvertFrom-Json)
@{ answer = 'A short explanation, please.' } | ConvertTo-Json | Set-Content .\clarification.json
.\target\debug\act0r.exe --db $db run signal $child.run_id $child.wait.id --input-file .\clarification.json
.\target\debug\act0r.exe --db $db worker run --until-idle
.\target\debug\act0r.exe --db $db actor task status $taskId
```

The parent has a `delegation` field while awaiting a child. Its `run_id`, status, and usage describe its own execution. Inspect `delegation.id` to see the child's execution and any human wait; a child wait cannot be answered by signalling the parent. After completion, `--json run status RUN_ID` exposes all saved requests and outcomes under `record.delegations`; history includes `delegation_requested` and `delegation_completed`.

To use a real model, replace the planner's `target` with your existing model target and remove `script`. The same Responses/Chat Completions adapter and provider retry policy apply. Workers independently pin their model and tool configuration. This feature has scripted and local conformance coverage; the new planner has not been validated against a live provider.

<a id="delegation--policy-and-limits"></a>

### Policy and limits

`planner` uses `finish`, `ask_human`, or `delegate_task` with a permitted worker name and a nonempty goal. Setting `delegation.max_parallel_tasks` also enables `delegate_tasks` for a bounded group. The worker allowlist contains 1–16 unique names, actor IDs, and descriptions. Creation checks that every worker exists and has a compatible registered integration. Delegation supplies only a goal; it cannot replace the child's model settings, tools, permissions, or budgets. Coding, answering, and other planners can be workers. Planner creation rejects `--source`.

There is one pending child at a time. The parent continues only after the child has a terminal task status. Its next saved model request includes the child's status, result, error, and usage. Rejections and failures are observations the model can handle; they do not imply parent success. Context includes the goal, worker descriptions, observations, instructions, and action schema, including Chat Completions' extra schema copy. An oversized continuation fails with `context_limit` and preserves the child result and existing usage.

`delegation.max_tasks` must be 1–64 and `max_depth` 1–8. Each planner limits its own requested children, including rejected requests. The root also caps child submissions across its descendants. Queue and cycle rejections can consume this allowance. Every ancestor's depth limit applies, so a permissive descendant cannot widen it. A direct child has depth 1. The runtime rejects delegation to the same actor, its actor ancestry, or a target that would create an active actor wait cycle.

Each Run retains its own attempt, model-call, and token budgets. An optional `delegation.shared_budget` also limits model calls and tokens across the planner and all descendants, with inherited admission checks, persisted exhaustion, and combined usage inspection. Without it, model/token budgets remain independent. Root delegation count/depth limits bound child submissions; each worker's pinned configuration bounds its execution. See [shared delegation budgets](#delegation-budgets).

Children obey the existing recipient task capacity. A full queue produces a saved `rejected` outcome with `error.code = "queue_full"`; no child mailbox entry, task handle, Run, or usage charge is created. The parent may choose another action. The runtime does not automatically retry a rejected delegation.

<a id="delegation--controls-and-recovery"></a>

### Controls and recovery

Pausing the parent before publication prevents child creation until resume. Pausing after publication lets the child continue independently. A completed child result can be recorded while the parent remains paused, with the next model activity awaiting resume. Mailbox pause and Run pause retain their existing separate meanings.

`actor task cancel PARENT_TASK_ID` or `run cancel PARENT_RUN_ID` propagates cooperative cancellation to the published child and any descendants. The parent stays `cancelling` and retains its task slot until the child reaches a terminal outcome. Cancellation before publication records a cancelled request without creating child work. In-flight operations retain admitted charges. An uncertain child remains blocked for reconciliation; parent cancellation cannot bypass that requirement or undo an external effect.

The model decision and stable delegation ID commit before publication. In a later transaction, the runtime inserts the child mailbox envelope, task handle, and ownership link together. Restart reuses that ID. When the child is terminal, saving the parent receipt and scheduling its continuation share one transaction. Failed commits roll back for retry; duplicate delivery does not create another child or continue the parent twice.

The baseline includes `agent_delegations` and prevents older binaries from discarding delegation state. Missing or changed planner implementations block before publication or continuation while retaining the intent and child result; restore the exact implementation and use `run resume` to continue.

<a id="delegation--durable-delegation-deadlines"></a>

### Durable delegation deadlines

Set an optional positive `timeout_ms` in the planner's pinned delegation configuration:

```json
"delegation": { "max_tasks": 4, "max_depth": 2, "timeout_ms": 30000 }
```

Each delegation saves an absolute UTC deadline in milliseconds when its intent commits. The duration includes publication delay, queueing, execution, and child human-input waits. Restart preserves the same deadline and child ID. Omission keeps the existing wait without a deadline. Each new delegation gets its own deadline; this is not a lifetime or model/token budget for the parent.

Expiry first saves a `delegation_timed_out` history event and `timed_out_at` receipt, then cooperatively cancels the published child and its descendants. Expiry before publication creates no child. While cancellation is pending, task inspection reports `cancelling` and retains capacity. Once the child is terminal, the planner receives a saved outcome with `status = "cancelled"` and `error.code = "delegation_timeout"`. Its actual child status, result, error, and usage are retained under `child_outcome`; reported and uncertain charges remain intact. The planner can explain the failure or choose another permitted action within its remaining limits. Expiry does not automatically retry the child or cancel the parent goal.

A child completion timestamp saved at or before the deadline wins if expiry has not already committed, even when the parent reads it after downtime. A committed expiry remains authoritative through restart and backward clock changes. A later completion remains in `child_outcome` while the parent sees the timeout. Saving the final timeout outcome and scheduling the next parent activity share the existing atomic reply transaction.

The clock keeps running while the parent is paused. Expiry can cancel its child and record the outcome, while the next parent model call waits for resume. Future deadlines count as pending work, so `worker run --until-idle` waits for completion or expiry. Deadline checks happen at worker boundaries; an executing model request or host process may finish or time out before the worker handles expiry. The deadline cannot forcibly interrupt external effects. A blocked child Run still requires restoration or reconciliation; the worker stops polling after recording expiry when operator action is required. An unconsumed child with no Run can be cancelled even when its integration is unavailable.

Missing planner implementations do not stop deadline recording or child cancellation. The parent retains its pending outcome until its pinned implementation is restored and resumed. The baseline stores delegation deadlines and expiry records.

Try the offline-expiry example without credentials:

```powershell
cargo run --locked --example actor_deadlines -- .build/actor-deadlines-demo/demo.db
```

It closes the worker while the child waits for clarification, reopens after the saved deadline, cancels the child, and lets the planner handle the timeout. Output includes the original deadline, the single expiry receipt, both handles, and retained usage. `actor task status` exposes pending deadline fields under `delegation`; `--json run status RUN_ID` exposes completed timeout evidence under `record.delegations`.

<a id="delegation--ddd-boundaries-and-verification"></a>

### DDD boundaries and verification

The Run aggregate owns the delegation budget, intent, stable ID, and completion receipt. `PlannerWorkflow` is a pure model policy returning commands. `ActorService` resolves compatible workflow definitions and coordinates dispatch, delivery, and cancellation through the `ActorStore` port. SQLite supplies atomic mailbox admission and ownership links. Coordinator forwarding uses its task parent chain; native delegation links a parent Run to an independent child task. One local worker and SQLite remain the runtime; distributed delivery and external framework hosting are later work.

```powershell
cargo test --locked --test delegation
cargo test --locked --test delegation_deadlines
cargo run --locked --example actor_delegation -- .build/actor-delegation-demo/demo.db
```

Tests cover restart during a child human wait, stable process-kill checkpoints, duplicate delivery, transactional publication/reply rollback, queue rejections, count/depth inheritance, wait cycles, pause before and after publication, queued and active cancellation, uncertain child reconciliation, coding child results, oversized continuation context, missing-version restoration.

Deadline checks cover offline expiry, backward clock changes, completion races including equality, paused parents, nested and in-flight cancellation, unadmitted children, retained usage and reconciliation, missing integrations, transactional expiry/cancellation rollback, and process kills before publication, during the wait, and after expiry commits.

<a id="delegation-budgets"></a>

## Shared delegation budgets

A planner can pin a model-call and token allowance for its Run and all delegated descendants. Each task submitted to that planner gets a fresh allowance, identified by its root Run ID. Calls in the planner, answering workers, coding workers, and nested planners count together. Existing per-Run allowances also apply.

To keep one allowance across scheduled follow-ups as well, configure a [durable goal budget](state.md#goal-budgets). Goal and delegation scopes can coexist at the same root; inspection distinguishes them with `record.kind`. Follow-ups inherit goal scopes, while each new Run can pin its own delegation scope.

<a id="delegation-budgets--configuration"></a>

### Configuration

Add `shared_budget` to the planner's existing delegation configuration:

```json
{
  "delegation": {
    "max_tasks": 4,
    "max_depth": 2,
    "timeout_ms": 30000,
    "shared_budget": {
      "max_model_calls": 20,
      "max_tokens": 20000
    }
  }
}
```

Each shared allowance accepts a positive number or `null` for unlimited; omitted fields are unlimited. Omitting the entire `shared_budget` object preserves the existing independent Run budgets and serialized capability pins. Delegation count/depth limits, queue capacity, deadlines, and attempt limits retain their separate rules. The shared budget covers model activities admitted with usage reservations through the harness; it does not impose a tool-call, dollar, lifetime-actor, or elapsed-time allowance. Usage and uncertain reservations remain recorded without a cap; unlimited remaining counters serialize as `null`.

Every child inherits all of its parent's shared allowances when its Run is created. This membership commits with the Run, actor execution link, and mailbox transition, before any activity can be admitted. A nested planner can pin an additional allowance for its own subtree; all ancestor and per-Run limits still apply. Repeated child tasks retain their earlier charges in the same root ledger. Separate goals on the same actor have separate root ledgers.

<a id="delegation-budgets--admission-completion-and-exhaustion"></a>

### Admission, completion, and exhaustion

Admission reserves a model call and the request's conservative input/output token maximum. SQLite checks each inherited allowance in the same transaction that saves the Run reservation, activity, attempt, and history. A denied admission creates no attempt, model execution, or usage charge. The requesting Run ends with `budget_exhausted` and `error.code = "shared_model_budget"`; its history includes `shared_budget_exhausted` and the relevant root Run ID.

The first denied admission saves an exhaustion receipt for that allowance. This receipt prevents further model admissions under that allowance, even if a different request would fit its remaining tokens. Pause/resume, child completion, and restart do not reset the receipt or counters. A tighter nested allowance can exhaust without exhausting an ancestor allowance; the outer planner can handle that child failure while it still has budget. When the root allowance exhausts, parent continuations that need a model call also end with the saved budget failure.

Reported usage replaces the held reservation with actual input/output tokens. Missing usage and interrupted calls keep their reservations and count as uncertain calls. Retries reserve another call and tokens, including failures that consumed the original request. Duplicate completion and child-result delivery do not add charges again. Cancellation and deadline expiry do not refund admitted work; an in-flight completion still reconciles its existing reservation.

Provider reports can exceed a reservation. Actual reported usage remains visible even when it exceeds the shared cap, and an exhaustion receipt records `reported_usage_exceeded`. The cap prevents admission based on saved reservations; it cannot undo provider work or force a provider to obey its declared output limit. Already admitted effects and uncertain tool operations retain the existing completion and operator-reconciliation contract.

Shared ledgers sum each member Run once. Child outcome receipts carry usage for observation, but are not added again to the total. Overlapping ancestor/subtree ledgers are separate limits over some of the same Runs; do not sum those ledgers to calculate goal cost.

<a id="delegation-budgets--inspection-and-restart-example"></a>

### Inspection and restart example

No server or API key is required:

```powershell
cargo run --locked --example actor_budgets -- .build/actor-budgets-demo
```

The example closes and reopens the app during a child clarification wait, preserving the same task IDs, allowance membership, and usage. It compares three- and four-call caps. The child needs two calls and the planner needs two; with a three-call cap the child answer is saved and the parent cannot admit its final call. With four calls both tasks succeed. Both cases release task capacity and print the ledgers and handles.

`actor task status TASK_ID` and `actor task list ACTOR_ID` include `shared_budgets` when the task belongs to an allowance. Each entry contains a `record` with `root_run_id`, combined `usage`, and the first `exhaustion` receipt, plus `members` with agent/task/Run IDs, status, and individual usage. Each member's usage budget describes its per-Run limits; the combined record describes the shared limits.

`run status RUN_ID` prints combined usage and limits. `--json run status RUN_ID` exposes the same views under `shared_budgets` beside the Run's own `record`. The Rust API provides `Engine::shared_budgets(run_id)` and `AgentTaskView::shared_budgets`. Runs without a configured or inherited allowance retain their previous JSON shape.

The baseline stores persisted budget records and indexed member links.

<a id="delegation-budgets--ddd-boundaries-and-checks"></a>

### DDD boundaries and checks

`SharedBudget` owns aggregate accounting, admission rules, and the first exhaustion receipt. `Run` owns the requesting task's durable budget failure and per-Run usage. The application-owned `ExecutionStore` port defines atomic mutations and consistent usage views. SQLite persists allowance membership and refreshes combined usage from authoritative member ledgers within that unit of work. CLI and native agents use the application services.

```powershell
cargo test --locked --test delegation_budgets
```

Tests cover nested and repeated delegation, tighter subtree caps, token denial before dispatch, paused waits across restart, held/uncertain retry charges, duplicate delivery, admission/completion/membership/exhaustion rollback, cancellation and deadlines, provider overruns, separate goals, unknown-effect reconciliation, and killed workers after parent or child admission.

<a id="task-groups"></a>

## Durable native task groups

The native planner can delegate to several worker actors and continue once every child has a saved terminal outcome. The join returns results in request order, even when children finish out of order. It preserves successes, failures, rejections, cancellations, and budget exhaustion so the planner can decide how to proceed. Single-child delegation and task groups use the same planner implementation.

<a id="task-groups--run-locally"></a>

### Run locally

```powershell
cargo run --locked --example actor_groups -- .build/actor-groups-demo/demo.db
```

The scripted example uses two answering actors and two activity slots. One child finishes while the other asks for clarification. It reopens the database, answers the saved wait, and joins the original children once. The planner and both workers use five model calls under one shared allowance. No model server or API key is needed.

The example writes `planner.json` beside its database. Use that configuration with the same database to create another planner through the CLI:

```powershell
cargo build --locked
.\target\debug\act0r.exe --db .build/actor-groups-demo/demo.db actor agent list
$plannerId = (& .\target\debug\act0r.exe --db .build/actor-groups-demo/demo.db actor agent create planner --config-file .build/actor-groups-demo/planner.json).Trim()
'{}' | Set-Content .build/actor-groups-demo/task.json
$taskId = (& .\target\debug\act0r.exe --db .build/actor-groups-demo/demo.db actor send $plannerId --input-file .build/actor-groups-demo/task.json).Trim()
.\target\debug\act0r.exe --db .build/actor-groups-demo/demo.db worker run --until-idle --max-concurrent-activities 2
.\target\debug\act0r.exe --db .build/actor-groups-demo/demo.db actor task status $taskId
```

The first child again waits for clarification. Inspect its task ID from `task_groups[0].children`, then use the existing `run signal RUN_ID WAIT_ID --input-file answer.json` command and rerun the worker. The Rust example performs these steps automatically.

<a id="task-groups--configuration-and-decisions"></a>

### Configuration and decisions

Keep the existing planner configuration and worker allowlist. Add an explicit group bound:

```json
"delegation": {
  "max_tasks": 8,
  "max_depth": 3,
  "max_parallel_tasks": 4,
  "timeout_ms": 60000,
  "shared_budget": { "max_model_calls": 20, "max_tokens": 200000 }
}
```

`max_parallel_tasks` must be 2 through 16 and no larger than `max_tasks`. Each group contains at least two children, up to the configured bound, with distinct resolved actor IDs. Different worker aliases cannot submit two group tasks to the same actor. All goals must be nonempty, and their combined UTF-8 size is at most 60,000 bytes. Omitting `max_parallel_tasks` leaves group delegation disabled. Single-child delegation remains available in the same planner.

The additional model action is:

```json
{
  "action": {
    "kind": "delegate_tasks",
    "tasks": [
      { "worker": "recovery", "goal": "Explain crash recovery." },
      { "worker": "budgets", "goal": "Explain shared accounting." }
    ]
  }
}
```

Worker names resolve only through the planner's pinned allowlist. The saved next model request includes one observation with the original action, `group_id`, and ordered `results`; each result carries the child task ID, Run ID if created, terminal status, result, error, and usage. A failed child contributes a result rather than failing the join automatically. The planner's next request still obeys its input byte bound and model/token allowances.

<a id="task-groups--persistence-and-execution"></a>

### Persistence and execution

Run owns the group, stable child IDs, and join receipt. Creating the group commits every child intent and history event with the model result. The coordinator publishes each intent through the existing ActorStore port using that child's stable task ID. Publication and recipient capacity admission are atomic per child; queue-full or missing-worker rejection becomes that child's saved result.

Each child owns a normal actor task and Run. Actors serialize their tasks, while the local worker can execute activities from different child Runs concurrently. `--max-concurrent-activities` controls actual execution capacity separately from the group's size. A limit of one processes a group serially. Waiting for human input or for a join uses no activity slot.

With `workspace_handoff: true`, opted-in native tool children receive independent copies of one snapshot of the planner's current code. Preparation preserves stable group and child identities across restart, including partially created copies. The planner can inspect and import successful child artifacts individually, then test the combined workspace. See [durable workspace handoff](#workspace-handoff) and `cargo run --locked --example workspace_handoff_groups`.

Each delivered child outcome commits with parent history. The last outcome, `joined_at`, ordered join history, and parent continuation commit in one transaction. Repeated identical outcomes are harmless; conflicting outcomes are rejected. A paused parent can collect results and save its join, but its next activity waits for resume. Restart uses the saved group, child IDs, outcomes, and continuation rather than creating replacement children.

Optional `timeout_ms` resolves to one absolute UTC deadline when the group is created. Every child inherits that deadline across publication, actor queueing, execution, human waits, parent pause, and worker downtime. A child's saved completion before expiry remains successful while unfinished siblings can time out. Expiry requests cooperative cancellation and retains late results and usage. Parent cancellation propagates to every unfinished child and releases the parent's actor capacity only after all children become terminal.

An uncertain effect awaiting reconciliation is not terminal. Deadline expiry or cancellation cannot bypass it or manufacture a result. The join remains open until the operator reconciles the blocked child. Ancestor checks and a wait graph containing every pending branch prevent delegation cycles.

Existing root task/depth bounds apply to nested groups. Children inherit shared budget membership when their Runs are created; admissions check ancestor and subtree allowances in the same SQLite transaction. Parallel children cannot independently reserve the same remaining calls or tokens. Interrupted attempts keep uncertain charges, and actual provider usage above a reservation remains visible. [Shared budget rules](#delegation-budgets) describe exhaustion and reconciliation.

<a id="task-groups--compatibility-and-inspection"></a>

### Compatibility and inspection

Only one native planner adapter and workflow are installed. The current version 3 declares `task_groups` and model-and-tool checkpoints, supporting optional [direct native tools](planner.md#planner-tools). The CLI selects the latest installed adapter when `--adapter-version` is omitted; specifying a version pins that exact installed implementation. The same planner handles single-child delegation and groups.

Missing or changed workflow implementations block execution without rewriting saved requests, waits, results or usage. Restore the exact implementation and use `run resume`.

The baseline stores `agent_task_groups` and optional group membership on `agent_delegations`. Group waits, receipts, identities, budgets and version pins persist across restart.

`actor task status/list` includes `task_groups` and `group_children`, including partial and terminal outcomes. JSON Run status includes those groups and every delegation. Text Run status shows group ID, completed/total counts, join time, deadline, and child task IDs. History records `task_group_created`, each `delegation_completed`, and one `task_group_joined`.

<a id="task-groups--verification-and-scope"></a>

### Verification and scope

```powershell
cargo test --locked --offline --test task_groups
```

Tests cover overlapping children, ordered results, duplicate/conflicting delivery, pause and restart, human waits, failed/rejected children, shared call/token reservations, nested groups, cancellation, deadlines, reconciliation, wait cycles through a later branch, missing adapter restoration, and transactional rollback. Process-kill checks stop workers before publication, after partial publication, with both children running, after one result, after the join, and with an expired deadline.

This version supports one outstanding group per Run and waits for all terminal outcomes. First-success joins, fail-fast joins, repeated tasks to one worker within a group, distributed ownership, and external framework adapters remain later work. Scripted tests verify the action schema and engine behavior; live-provider group decisions have not been validated.

<a id="initial-dependencies"></a>

## Initial dependencies between coding assignments

For a complete runnable API-to-consumer example with real Cargo tests, two restarts, approved transfer and final application, see the [dependent coding demo](validation.md#dependent-coding-demo).

New two-coder Code sessions enable `initial_dependencies` on their existing planner. A planner can declare one dependency while assigning the original group, before either worker starts model or tool work:

```json
{"action":{"kind":"delegate_tasks","tasks":[{"worker":"coder","goal":"Implement the UI using the completed API; read the transferred API files before editing the UI.","writable_paths":["src/"],"depends_on":"coder2"},{"worker":"coder2","goal":"Implement and verify the API in src/api.rs.","writable_paths":["src/api.rs"]}]}}
```

`depends_on` names the prerequisite member of that same group. Omit it on independent assignments. This first version supports two distinct coders and one edge; self-dependencies, missing/non-coding prerequisites and circular declarations are refused before publishing tasks. Existing task, attempt, depth, model-call, token and absolute deadline limits still apply.

<a id="initial-dependencies--initial-hold-and-preparation"></a>

### Initial hold and preparation

The prepared group binds the declaration to both physical task IDs and the parent's preparation activity. The initial hold projection commits with the group intent in SQLite, before task publication or worker Run allocation. Restart and exact activity replay retain that declaration and those identities.

The receiver can execute only its pinned workspace verification and baseline reads while preparing. It saves its original baseline before accepting any patch. Its first model activity remains queued without spending model calls or tokens. The scheduler query and transactional admission guard both enforce this restriction. Normal manual, whole-goal, actor and Run pauses also apply to preparation.

Once both worker Runs exist and the receiver's baseline is ready, the existing work coordinator atomically converts the declaration into a planner-owned hold and an exact [worker dependency](#worker-dependencies). No model call can enter between baseline completion and that conversion. A prerequisite already settled before conversion queues the same durable review wake immediately. Pending initial edges participate in sibling-question cycle detection too.

**Delegated work** shows the held receiver and its prerequisite task during preparation. After conversion, the existing planner hold and **Planner coordination** dependency history show its saved state. Reloads and inspection make no model calls.

<a id="initial-dependencies--review-transfer-and-release"></a>

### Review, transfer and release

The prerequisite continues independently. Success, failure, stop or the saved delegation deadline wakes the same planner Run through the original child join. The planner reviews the saved outcome and chooses what to do with the receiver.

For a successful prerequisite, the existing [approved patch handoff](#worker-changes) lets the planner inspect the immutable source patch, request **Transfer reviewed patch** approval, transfer it into the held receiver, then explicitly select `resume_worker` and `guide_workers`. A declaration does not grant transfer approval, widen writable paths or automatically release a hold. Denial, path refusal and file conflicts preserve the hold and supply saved feedback.

The receiver's original task scope must already include the prerequisite files that will transfer. Its resumed work reads those files, applies its own changes and runs fresh tests. The original baseline lets its final patch include both sets of changes. The planner imports this consolidated downstream patch once and runs fresh parent tests before final completion. The user's project retains the separate final patch review/application flow.

A failed, stopped or timed-out prerequisite cannot supply a patch. The planner can guide/resume the receiver with a revised approach or stop that assignment. Manual and ancestor holds survive a planner resume. If the prerequisite ends before allocating a Run, the work coordinator stops the still-unstarted receiver through normal task cancellation and retains both outcomes in the original join; there is no exact source Run or artifact to review. Whole-goal stop prevents fresh work and preserves the existing cancellation and reconciliation rules.

<a id="initial-dependencies--compatibility-and-local-checks"></a>

### Compatibility and local checks

The baseline includes the initial declaration projection and a read-only blocked-activity view. Domain code validates the bounded declaration and binds prepared task identities. SQLite implements the existing execution and work-coordination ports for atomic admission, hold conversion and durable wakeups. The existing planner, actors, scheduler, patch journal and approval/reconciliation paths execute the workflow.

Older sessions omit `initial_dependencies` and retain their saved descriptions, schemas, configuration and Run state across exact start retries, follow-ups and restart. Restart the API and create a new Code session with **Two coding workers** to enable the declaration. Describe the dependency in the request; delegation remains a model decision.

```powershell
cargo test --locked --offline --test http_api initial_dependencies
```

Local scripted fixtures cover publication and approval restarts, zero receiver model calls before release, early prerequisite completion, fresh downstream and parent tests, denial, scope refusal, failure/stop/deadlines, independent manual holds, mixed question cycles, queued cancellation, invalid declarations and old-session exact retries. They use no hosted provider or API key.

<a id="workspace-handoff"></a>

## Durable workspace inputs for delegated tasks

A planner can give a worker an isolated snapshot of its current code for each delegated task. The worker retains its actor identity, durable mailbox, model, tools, default writable paths, test command, budgets, and memory policy. A task may narrow its native editing permissions. Ordinary tasks continue using its creation-time workspace and permissions. This supports a repair loop: integrate patches, test the combined code, delegate another fix against that code, inspect and import the returned patch, then test again.

Enable `workspace_handoff: true` on a native `planner` v3 actor with a source workspace. Enable `accept_workspace_handoff: true` and `change_review: true` on each native `tool` v1 worker with its own source workspace. Both individual `delegate_task` actions and [parallel task groups](#task-groups) support handoff. Set `delegation.max_parallel_tasks` to enable groups (2–16, no larger than `max_tasks`).

```json
{
  "kind": "delegate_task",
  "worker": "repairer",
  "goal": "Fix the failing combined tests using the current workspace"
}
```

There is no new model action or workspace argument. The planner's pinned configuration selects its workspace and permitted workers. Missing or unsupported workers and disabled worker opt-in return saved preparation feedback without launching a task. Existing model schemas and default delegation behavior remain compatible when the feature is disabled.

For a group, use the existing `delegate_tasks` action. The runtime captures current code once and forks a separate managed copy for each distinct worker:

```json
{
  "kind": "delegate_tasks",
  "tasks": [
    { "worker": "first", "goal": "Repair module a using the current code" },
    { "worker": "second", "goal": "Repair module b using the current code" }
  ]
}
```

All workers must accept handoff. A preparation refusal launches none of the group; after preparation, publication admits each member independently, so a full mailbox can reject one child while siblings proceed. Actual concurrency still depends on the worker's `--max-concurrent-activities` setting. Each actor processes one task at a time.

<a id="workspace-handoff--per-task-writable-paths"></a>

### Per-task writable paths

New harness Code sessions enable `delegation_write_scopes: true` on the planner. Custom native planners can opt in with that flag and `workspace_handoff: true`. Supply `writable_paths` on `delegate_task`, or on individual `delegate_tasks` members:

```json
{
  "kind": "delegate_tasks",
  "tasks": [
    { "worker": "coder", "goal": "Repair the API", "writable_paths": ["src/api.rs"] },
    { "worker": "coder2", "goal": "Repair storage", "writable_paths": ["src/store.rs"] }
  ]
}
```

An omitted scope inherits the worker's pinned permissions. An explicit scope requires 1–128 distinct portable relative files or directory prefixes ending in `/`, with at most 16 KiB of path text. Every entry must be contained in both the planner's and worker's pinned writable paths. Empty, duplicate, absolute, traversing, and broader entries are refused. Omit the scope for read-only helpers. Invalid model scopes return saved feedback; invalid worker scopes refuse preparation before copying or publishing any child.

The exact scope is part of the preparation request, filesystem receipt, accepted task and child Run. Only that Run's native editing permissions are narrowed; the registered worker configuration and digest stay unchanged. Verification checks those effective permissions before the baseline and first model call. Retry and restart preserve the same scope. Later tasks independently inherit their own scope or the worker defaults; public task payloads cannot set it.

Workers can read the rest of their project copy. Pinned test and project commands are subprocesses, not OS filesystem sandboxes. If a command changes a file outside a child's supplied scope, its saved patch cannot be imported, even when the planner could otherwise write that file. Approval and conflict checks still apply, and the parent must test integrated changes. Scopes may overlap; they do not reserve files or eliminate import conflicts.

Disable the capability to omit scoped delegation actions. Current coding sessions enable scopes without adding another planner implementation.

<a id="workspace-handoff--preparation-publication-and-recovery"></a>

### Preparation, publication, and recovery

`workspace.handoff.prepare` v1 runs under the planner's managed workspace ownership lock. It publishes an immutable snapshot, forks a managed copy under a stable activity-derived identity, and saves a preparation receipt. The snapshot manifest stays on disk; the activity result contains a compact reference binding the parent Run, preparation activity, task identity, recipient, goal, snapshot digest, task workspace, and worker configuration digest. Model requests do not contain this internal reference or the full manifest.

`workspace.handoff.prepare_group` v1 resolves every worker's authority and configuration in one SQLite read snapshot, then holds the planner's workspace lock while publishing one input snapshot and all copies. The preparation activity fixes the group ID and snapshot ID. Each request index derives a stable child task ID and copy identity; the group receipt binds the ordered goals, worker pins, common digest, and distinct copies. The model result, group, and all child intents commit together before publication begins. If copying stops partway through, retry reuses the published snapshot and existing copies, creates missing copies, and publishes the receipt. It never captures newer code for a remaining sibling. A saved receipt replays the same references.

Publication atomically writes the child mailbox message, physical task, trusted workspace reference, and delegation link in SQLite. Publication refuses a prepared task ID already held by an accepted request. Count/depth limits, cycles, capacity, shared budgets, goal holds, cancellation, and deadlines continue using the existing delegation machinery. Worker opt-in and pinned configuration are rechecked at publication and task launch. A paused goal holds publication; cancellation before publication launches no child. Preparation consumes an activity attempt without charging model usage. As before, the child deadline is established when the delegation intent is saved; preparation precedes that intent.

Only the accepted task record supplies `workspace_input` to the child's Run. Actor creation rejects that runtime field. Public message fields cannot select a workspace or widen permissions. The reference survives mailbox queueing and worker restart, including when the worker is finishing another task. The actor's pinned default workspace is never changed, and separate delegated tasks receive separate copies.

`workspace.handoff.verify` v1 executes before the child's change-review baseline and first model call. It checks the accepted physical task, worker pins, saved preparation, immutable snapshot, managed marker and fork origin, and copied file manifest. A mismatched queued copy, snapshot, or receipt fails before model execution. A saved verification file with no SQLite completion still requires the copy to match on recovery. Once that checkpoint is committed, its receipt replays without resetting a workspace the child has already edited. The child also checks its captured baseline digest against the input snapshot before starting the model.

All three activities are `SafeToRepeat`. Restart after filesystem publication but before SQLite completion reuses the same snapshot, copies, and identities; it does not capture newer planner code or publish replacement tasks. Partial mailbox publication and partial terminal results recover through the existing group contract, with one saved join in request order. Snapshot publication is staged, and receipts use the existing atomic file writer. Recovery is still bounded by the normal retry and total attempt allowances. A crash before snapshot publication can capture the current workspace on retry; the durable input is fixed at snapshot publication. Excluded files and snapshot limits follow the existing workspace rules: up to 10,000 entries and 64 MiB, without links, private runtime files, Git metadata, secrets excluded by name, or generated build directories. A group can retain up to 16 full copies; receipt reads are bounded to 8 MiB.

Local artifacts are stored beside the database:

- `.act0r/snapshots/PREPARE_ACTIVITY_ID/`, with manifest and `handoff.json` in `.act0r-tools/`.
- `.act0r/workspaces/PREPARE_ACTIVITY_ID/`, with managed/fork markers and `handoff-verified.json` in `.act0r-tools/`.

For groups, the common snapshot stores `handoff-group.json`; copies live at `.act0r/workspaces/CHILD_TASK_ID/`. Each copy has its own verification receipt. Child verification checks membership in the complete saved group before baseline capture or model execution.

Snapshots and copies are retained for recovery and inspection. Automatic retention cleanup is a later feature.

<a id="workspace-handoff--returning-and-integrating-patches"></a>

### Returning and integrating patches

The worker's `change_review` baseline is the handed-off code, so its returned patch contains only its task's changes. Enable [child patch imports](#child-changes) on the parent to inspect and explicitly approve each successful child's artifact. Disjoint files import separately; changes to a file already modified by another import produce conflict feedback without overwriting it. Import still requires exact before hashes and the parent's writable paths; it does not merge overlapping edits automatically. Child tests verify its own copy. Run the parent's pinned tests after imports to verify combined code. The parent's final change set remains relative to its original task baseline and includes its earlier changes plus imported fixes.

<a id="workspace-handoff--ports-and-adapters"></a>

### Ports and adapters

`domain::workspace_handoff` defines pure individual/group request and reference validation, deterministic member identities, and the child's verification checkpoint. The existing planner owns preparation and delegation transitions; Run owns the group and its join. `WorkspaceHandoffReadPort` resolves authority and pins in SQLite snapshots; `WorkspaceHandoffPort` executes preparation and verification. `LocalWorkspaceHandoff` reuses `LocalCandidates` snapshot/fork operations and `LocalTools` ownership locks. Actor services overlay trusted workspace inputs immediately before starting the Run. Local bootstrap installs both ports through the existing executor. Optional JSON fields require no SQLite schema migration or second planner implementation. Existing individual preparation and verification receipts remain readable.

```powershell
cargo run --locked --example workspace_handoff
cargo run --locked --example workspace_handoff_groups
cargo test --locked --test workspace_handoff --test workspace_handoff_groups --test task_groups
```

The example edits the planner's Rust code after creating the worker, hands off that latest code, executes real Cargo tests in the child, restarts at import approval, imports the saved patch, and passes combined Cargo tests. The worker's default workspace and original source remain unchanged. Scripted decisions require no provider key.

The group example edits two modules after worker creation and admits activities from two child Runs concurrently. One worker verifies and publishes its module patch while the second waits for human input. It restarts with that partial join, answers the saved wait, imports each patch under its own approval, and passes combined Cargo tests. Each child's fixed command verifies its module independently. Original source and worker default workspaces remain unchanged.

Consumer tests cover current-code baselines, repeated delegations through one actor, queueing behind an active task across restart, mailbox capacity, unchanged pins/default workspaces, forged task messages, exact receipt replay after parent drift, recovery with a published snapshot before a preparation receipt, snapshot/copy/receipt tampering, disabled compatibility, attempt caps, goal holds/cancellation, invalid configurations, and real host kills after both preparation and verification effects but before SQLite completion.

Group checks cover a common latest-code baseline, distinct task copies and permissions, separate imports and overlapping-file conflicts, exact replay after a partial copy set and parent drift, partial mailbox publication, partial results and human waits across restart, member order and identity binding, tampering before model execution, worker opt-in, mailbox rejection with a successful sibling, shared budget exhaustion, durable deadline expiry during downtime, goal holds/cancellation, and a real host kill after filesystem preparation but before SQLite completion.

<a id="child-changes"></a>

## Durable child artifact handoff

A native planner can delegate repairs to a worker, inspect the worker's immutable patch and test evidence, request approval to import it into its own managed workspace, and run combined tests before publishing its final patch. Every step belongs to the original planner Run, including the approval wait and journaled write. Goal holds, cancellation, and attempt limits apply through the existing engine.

Enable `child_changes: true` and `change_review: true` on a `planner` v3 actor. Supply a source workspace and nonempty `writable_paths` at creation. The runtime pins the managed target and worker identities; model actions and task messages cannot choose another target or expand permissions. Enable `change_review: true` on each worker that will supply artifacts. The planner can pin `run_tests` and a test command without also permitting editing tools.

```json
{
  "kind": "inspect_child_changes",
  "worker": "repairer",
  "offset": 0
}
```

Inspection selects the **latest delegated task** for that pinned worker. It must be a successful, completed direct child with a saved change artifact. An unrelated task, grandchild, failed task, timed-out child, or missing artifact cannot supply an import. Task replies already provide task/Run IDs; inspection resolves those ownership links in one SQLite snapshot and returns the exact artifact activity ID and inspection ID. There is no live read of the child's current workspace.

If the child's saved workspace input supplies per-task `writable_paths`, every changed file must fit that scope before an inspection can authorize import. An out-of-scope process side effect refuses the whole patch, even if the parent allows the file. Pinned commands are not OS sandboxes. Unscoped tasks retain their existing import behavior. See [task scopes](#workspace-handoff--per-task-writable-paths).

The result includes an 8 KiB patch page, byte offset, `next_offset`, saved test status, test command/success/exit metadata, and bounded file summaries. Follow `next_offset` to inspect additional pages; offsets must be UTF-8 boundaries. Metadata truncation is explicit. Full hashes and test output remain in the saved inspection; the full source patch and evidence are available through `actor task changes CHILD_TASK_ID --json`. First-slice limits are 128 affected files, 1 MiB per changed file, the existing 4 MiB patch limit, and 32 MiB staged original/replacement content. Files must be UTF-8 text with portable relative paths and existing parent directories.

`matching` describes workspace hashes, and can accompany failed tests. Inspect `tests.ok` and `tests.exit_code` as well as test status before choosing to import.

```json
{
  "kind": "import_child_changes",
  "worker": "repairer"
}
```

Import requires the latest inspection to belong to the same worker and have no conflicts. It saves a `child_changes_approval` wait containing that exact inspection, then schedules `planner.child_changes.import` v1 only after `{"approved": true}`. Rejection is saved feedback, clears the inspection, and lets the planner continue. Missing/conflicting inspections produce feedback without scheduling a write. A new delegation invalidates the inspection; an import consumes it.

Inspect and approve with the existing Run CLI. Approval submits the decision; the worker executes it:

```powershell
cargo run --locked -- --db agent.db actor task status PARENT_TASK_ID --json
cargo run --locked -- --db agent.db run status PARENT_RUN_ID
cargo run --locked -- --db agent.db run approve PARENT_RUN_ID WAIT_ID
cargo run --locked -- --db agent.db worker run --until-idle
cargo run --locked -- --db agent.db actor task changes PARENT_TASK_ID --output combined.patch
```

Use `run approve PARENT_RUN_ID WAIT_ID --deny` to reject. The Rust host uses `engine.signal(parent_run_id, wait_id, json!({"approved": true_or_false})).await?`. The saved wait ID and decision receipts prevent changed or stale approvals from authorizing another artifact.

<a id="child-changes--exact-import-and-recovery"></a>

### Exact import and recovery

Inspection reconstructs the child's canonical saved patch against matching parent bytes and persists both original and replacement content, the target directory identity, and the preview. Every affected path must satisfy the parent's pinned writable paths. Exact before hashes are required, including absence for additions. Conflicting parent edits are reported; this slice does not attempt textual merging. Unrelated parent changes are preserved. Links/reparse points, case aliases, private paths, and unsupported content are refused by the shared patch adapter.

The importer reuses [patch application](tools.md#patch-application)'s per-file intent journal under the **same managed workspace ownership lock** as native tools. The external operator application API continues to refuse managed actor targets. Inspection receipts are under `.act0r/applications/INSPECTION_ACTIVITY_ID/inspection.json`; `prepared.json` binds staged bytes and pinned managed permissions; `write.json` binds the parent Run, import activity, and exact approved preparation. A finished receipt replays its saved report without rereading mutable child/parent project files.

Inspection is `SafeToRepeat`; import is `RequiresReconciliation`. An interrupted import blocks the parent. Inspect `run status PARENT_RUN_ID`, then use the existing [reconciliation workflow](recovery.md#reconciliation) to explicitly accept, retry, or fail the interrupted attempt. Retry continues only the saved operation: pending files require before hashes; intended files can have before or after hashes; applied files require after hashes. Partial conflicts block again. There is no automatic rollback or forced overwrite. Cancellation of an already admitted write can leave a prefix applied, just as with other unsafe operations. The journal remains available.

Imports charge no model allowance; inspecting, deciding, and continuing still use ordinary model turns. An import invalidates any earlier parent test status. Child test evidence describes the child's workspace, not the integrated parent. Run the planner's pinned `run_tests` after import; final `change_review` compares its own original baseline and publishes the combined patch with matching/stale test evidence. Applying that final artifact to the user's project remains an explicit separate operator operation.

<a id="child-changes--browser-recovery"></a>

### Harness API recovery

In a coding session, child-import recovery inspection exposes the exact approved child artifact and interrupted activity/attempt, plus each file's saved `pending`, `intended` or `applied` progress and current hashes. Restarting the runtime records an abandoned unsafe attempt; inspection reads its saved receipts without creating files, taking a workspace lock, selecting another child, or refreshing the patch from the child's live workspace.

Confirm the old worker has stopped and submit inspection notes with a retry or fail decision. Retry continues the original operation under its pinned writable paths, target identity, hash checks and attempt allowance. A completed receipt acknowledges its original result without repeating writes. Conflicts and unreadable receipts disable resume; correct the managed copy and refresh, or record failure with a reason. Failure retains any partial imports. Recovery never rolls back files or supplies a fabricated success/test result.

Clients should save exact recovery decisions before submission. The server persists their receipts in the existing Run transaction. Exact activity/attempt/decision/evidence retries acknowledge the original receipt after reload, worker restart, completion or a newer interruption. Changed decisions conflict; an older receipt cannot release a newer uncertain attempt. Goal pause keeps its hold. Stopped goals refuse resume while failure can settle the uncertain operation; the existing transactional cancellation guard prevents new writes even if a stale client submits retry. Combined-workspace tests remain required after a recovered import.

The session/turn-scoped API uses `GET /v1/harness/sessions/S/turns/T/child-import-recovery` and `POST /v1/harness/sessions/S/turns/T/runs/R/child-import-reconciliation`. The read is bounded to the latest 128 owned blocked/reconciled Runs, independent of graph display limits, with an explicit truncation flag. Bodies use the native recovery contract: exact activity/attempt IDs, `decision` with `retry` or `fail`, `evidence`, and required `operation_stopped: true`. Unknown fields, target overrides and `accept` are refused. See [HTTP API](api.md#http-api).

<a id="child-changes--ports-and-adapters"></a>

### Ports and adapters

`domain::child_changes` defines bounded inspection, pinned target permissions, and checkpoint contracts; the existing planner owns the action/approval transitions. `ChildChangesReadPort` resolves direct-child ownership and the committed artifact in one snapshot. `ChildChangesPort` supplies inspection and import activities. `SqliteStore` and `LocalChildChanges` implement these ports; `LocalPatchApplications` supplies the shared filesystem journal. Local bootstrap installs them through `AgentExecutor`. This adds no actor implementation, scheduler, or SQLite migration; disabled planners retain their previous action schema.

`ChildImportRecoveryService` verifies the saved native planner invocation and permissions. `ChildImportInspectionPort` exposes read-only managed import inspection through `LocalChildChanges`, sharing the existing journal validator with external applications while preserving their refusal of managed targets. API recovery adds no second scheduler, journal or SQLite schema.

```powershell
cargo run --locked --example child_changes
cargo test --locked --test child_changes
```

Consumer tests cover approval/restart, immutable artifacts after child workspace removal, rejection, missing/failed children, permission refusal, target drift, bounded UTF-8 pages and receipt replay, stale parent evidence, goal pause/resume/cancel, attempt caps, and disabled planner compatibility. A real host is killed after import effects and before SQLite completion; recovery blocks the parent until explicit reconciliation and then replays the finished receipt. The shared journal's existing killed-host tests exercise per-file partial-write boundaries. Local HTTP fixtures verify the new actions and exact pinned schema through both Responses and Chat Completions. The runnable example executes Cargo tests in both child and parent workspaces; no live provider key is required.

<a id="worker-messages"></a>

## Durable worker messages

New Code planners also [route user guidance to active assignments](planner.md#planner-guidance) through saved notes from `parent`. This runs while the original child join stays open; workers receive the notes at their next fresh model call.

New Code sessions give each coder `send_worker_message` v2. It saves informational notes for the delegating parent or a coding sibling in the same parallel group. The parent reads ordinary notes after its existing all-finished join. A marked [`needs_coordination` finding](#worker-findings) durably wakes the same planner to inspect evidence and route guidance during that join.

For a question that requires an answer, new coding workers use [`ask_worker` / `reply_worker`](#worker-questions). These create a separate durable planner or sibling wait with a deadline; ordinary notes still do not create waits.

```json
{"kind":"call_tool","tool":"send_worker_message","arguments":{"recipient":"coder2","text":"The response field must remain named id."}}
```

Use `recipient: "parent"` to report a finding to the planner. Sibling names are `coder` and `coder2`; a sibling must be a different actor in the sender's saved group and have a started, unfinished assignment. Unknown names, self, helpers, finished/stopped recipients and not-yet-started recipients produce saved `ok:false` tool feedback. A new send requires a new model decision; there is no automatic polling loop for another actor to start.

<a id="worker-messages--delivery-and-recovery"></a>

### Delivery and recovery

SQLite resolves ownership from the accepted physical task and saved parent delegation, never model-supplied actor IDs, correlation IDs or file paths. Each accepted note pins the recipient task and Run, so it cannot reach a later assignment on the same actor or another session turn.

The sending activity ID is the message identity. Its receipt and the recipient's task-scoped inbox commit in one immediate transaction before the tool returns. The sender's activity completion then uses its normal Run journal. If a process stops between those commits, repeating the activity returns its original accepted/refused result without publishing another note. Changed arguments or another sender Run under that identity conflict.

At the recipient's next unattempted model admission, notes enter a separate `peer_messages` array. Admission, context selection, usage reservation and the delivery marker commit together. Notes remain fixed context on subsequent calls; the first-inclusion receipt is recorded once. Human instructions retain their separate `steering` array, identities and history. Peer notes are task data and cannot grant approvals, broaden writable paths, renew budgets, supply test evidence or create assignments.

Admitted requests, automatic retries and explicit saved model-checkpoint retries keep their exact input; notes arriving later wait for a fresh request. Assignment and goal pauses hold admission independently and retain original deadlines. Unsafe tool reconciliation still blocks execution. Finishing or stopping before a fresh call retains unread notes as history; it does not restart the recipient.

<a id="worker-messages--inspection-and-bounds"></a>

### Inspection and bounds

**Delegated work → Worker messages** shows sent/received notes, refusals, pinned task identities and Pending / In context / Not delivered status. The existing `GET .../turns/T/delegated-work` projection includes scoped `messages` on each child independently of the event tail. Parent-directed notes remain visible on the sender's card.

Each physical assignment allows 32 send decisions before capacity feedback. Each recipient accepts at most 16 notes and 16 KiB of serialized peer context. Text accepts 1..2048 UTF-8 bytes. The pinned total model-input limit still applies alongside guidance and observations. Refusal does not reset attempt allowances, usage, shared budgets or joins. History shows at most 64 receipts per child with explicit truncation.

Existing sessions and exact start retries retain their original tool pins; start a new Code session to enable messages. Read-only sessions and external-framework contracts retain their current behavior.

The domain owns validation, inbox state and model inclusion. The native tool uses `WorkerMessagePort`; SQLite supplies ownership, atomic persistence and receipt reads. HTTP inspection reuses the delegated-work port. Schema 39 adds indexed receipts, without another scheduler or planner.

<a id="worker-messages--verify-locally"></a>

### Verify locally

```powershell
cargo test --locked --offline --test http_api worker_messages
```

Tests use a local compatible-model fixture with no provider key. They cover peer/parent delivery, joins, pauses and restart, rollback, duplicate/changed sends, interrupted sender completion, stopped recipients, bounds, exact saved model recovery.

<a id="worker-questions"></a>

## Durable worker questions and replies

New Code sessions install `ask_worker` and `reply_worker` v2 on coding workers. Workers can ask their direct planner using `recipient: "parent"`, including in one-coder sessions. The planner answers one saved question at a time while its original child join stays open. See [planner coordination](#worker-questions--planner-coordination). In two-coder mode a worker can ask its started sibling in the same parallel group:

```json
{"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":"coder2","text":"Which fields should the API return?","timeout_ms":60000}}
```

The recipient's `peer_messages` entry includes `question_id`, the requester's name and physical task, text, and `deadline_at`. It answers with:

```json
{"kind":"call_tool","tool":"reply_worker","arguments":{"question_id":"<saved question UUID>","text":"Return id and display_name."}}
```

An accepted ask completes its tool activity and creates a Run wait backed by a persisted durable timer. No executing activity or tool promise remains held. The actor retains ownership of this task; later mailbox tasks do not bypass it. A reply, timeout, or closed recipient records an outcome and schedules the requester's next fresh model decision with a correlated `worker_reply` observation. It does not finish the task or join the planner's group automatically.

<a id="worker-questions--scope-and-authority"></a>

### Scope and authority

Recipients are the enabled direct parent planner or another started coder in this exact saved group. Helper, self, other groups, later assignments, and not-yet-started recipients produce saved tool feedback. Informational findings still use `send_worker_message`.

Only the pinned recipient task and Run can answer. A fresh reply must commit strictly before the original deadline; the first accepted reply wins. Unknown IDs, closed questions, and duplicate new replies are refused. Open questions, including those whose sending tool completion is not yet saved, form the wait graph. A circular wait is refused without suspending its sender.

Questions and replies are untrusted task data. They cannot grant approval, broaden writable paths, reset usage, supply passing-test evidence, share files, or create tasks. Answer incoming questions before creating dependent waits. A question does not interrupt an admitted model request, exact saved retry, human wait, recovery hold, or reconciliation. Its inbox entry enters the recipient's next fresh model call through the existing admission boundary.

<a id="worker-questions--durability-and-control"></a>

### Durability and control

The asking activity ID is the question ID. SQLite saves the pinned question, recipient inbox entry, and send receipt in one immediate transaction. A reply outcome and its activity receipt also commit together. A logical activity replays its original acknowledgement after restart or a changed retry attempt. Changed arguments or another Run under that identity conflict.

These commits precede ordinary tool completion. Recovery can replay either tool without repeating its effect. A reply arriving before the requester's wait exists stays saved and wakes the subsequent wait. Timer firing, reply observation and continuation commit together. The timer retains its original deadline even when a reply wakes it early. Normal safe-tool retry delays and attempt limits apply.

Timeouts are 1000..300000 milliseconds, resolved once at question commit. Pause and downtime do not extend them. Outcomes can be saved while execution is held; model admission waits for assignment, goal and Run holds to clear. An answer committed before the deadline wins even when read after downtime. A late reply cannot replace a committed timeout or closed-task outcome. Stopping the requester closes its question and prevents continuation; stopping or finishing the recipient releases the wait with feedback. Delegation deadlines and reconciliation remain authoritative.

Each assignment permits eight ask decisions, including refusals. Text accepts 1..2048 UTF-8 bytes. Questions share the existing 16-note / 16-KiB peer context bound. Existing attempt, input and shared usage limits remain in force; waiting consumes no new model-call or token allowance.

<a id="worker-questions--inspection-and-compatibility"></a>

### Inspection and compatibility

**Delegated work → Worker questions** shows pending questions, exact task IDs, deadlines, answers, timeouts, closed recipients/requesters, and saved refusals. The existing delegated-work API includes scoped `questions.items` and `questions.receipts`, each limited to 64 rows with explicit truncation. History is read in the same snapshot as assignment state, independently of the event tail.

Existing sessions and exact start retries retain their saved catalogs. Start a new Code session for planner questions, or select two workers for sibling questions. A one-coder session has no eligible sibling. The baseline includes questions and receipts; existing Run fields default to no peer wait. Domain code owns waits and cycle rules, `WorkerMessagePort` exposes native execution, and SQLite supplies scoped atomic persistence and timer wakeups.

```powershell
cargo test --locked --offline --test http_api worker_questions
```

Local model fixtures verify correlated continuation, concurrent calls, rollback, restart before either tool completion, replies before wait creation, deadlines across pause/downtime, late replies, circular waits and stopped siblings.

<a id="worker-questions--planner-coordination"></a>

### Planner coordination

```json
{"kind":"call_tool","tool":"ask_worker","arguments":{"recipient":"parent","text":"Should a missing item return 404 or an empty response?","timeout_ms":60000}}
```

New Code sessions pin `reply_worker` v2 on their planner. An accepted question pins the original requester and parent physical tasks and Runs. SQLite commits the question, inbox entry, immediate durable wakeup and activity receipt together. The original task IDs, delegation group and isolated workspaces remain intact. An interrupted asking tool can replay its acknowledgement without another wakeup.

The parent's wakeup enters its existing Run at a quiescent child join. Its saved delegation action moves into `suspended_delegation`; `coordinating_question` contains the exact question and deadline. New Code sessions also pin `question_inspection_limit: 3`. The restricted model schema permits `reply_worker` with that question ID and the installed `list_files` v2, `read_file` v2 and `search_text` v1 tools while inspections remain. The domain and Run admission checks enforce the same scope independently of the supplied schema.

The planner can answer immediately from its saved goal, project instructions and context, or inspect its managed workspace first. This is the parent's current integrated code; the worker's isolated in-progress files are not visible. Each selected read counts toward the per-question limit, including a refused read. Safe retries retain the same logical read and count. At zero remaining the only model action is the exact reply or an available assignment inspection. The prompt reports used/remaining inspections and exposes only the available coordination tools. Model calls, tokens and tool attempts retain the existing Run and shared goal allowances.

New Code sessions also permit up to three independent [assignment inspections](planner.md#assignment-inspection) for committed status, waits, tool/test evidence and messages from the exact coders in this suspended join. These reads observe saved facts from the child's isolated task, including a child that just finished. They do not inspect its mutable files or approve its waits.

Each read uses the normal native execution port, pinned tool limits and saved activity journal. Completed evidence survives restart and enters the next model decision; an interrupted read recovers with its original identity and input. Missing files and refused paths supply saved feedback so a later read can correct them within the same limit. File text and question text remain untrusted data. The planner should cite relevant relative paths and explain uncertainty when evidence is insufficient. Coordination cannot edit files, run commands or tests, import patches, grant permissions, ask a human, delegate, finish, or create another wait. Replies cannot bypass required tests or approvals.

The reply uses the existing idempotent question receipt, then its normal tool completion restores the original delegation action and child join. Another pending question can run afterward; there is never a second planner Run or a concurrent model request in that Run. Normal model-call, token and attempt accounting applies to the extra decision. The requester waits without holding an executing activity.

Child completions arriving during the reply step commit their terminal outcomes without running the planner's suspended continuation. After the step settles, an already-ready single/group join resumes once with all saved outcomes in their original order. Otherwise the parent returns to waiting for its children. Questions do not finish workers, import patches, or count as child completion.

Run and whole-goal pauses hold the parent's wakeup and execution independently. Absolute question deadlines continue through pauses and downtime. An expired or closed question is skipped before its wakeup spends a model call; an already admitted reply can finish, but a late or closed reply supplies saved refusal feedback and cannot replace the outcome. Stop closes questions using the same task/goal guards and preserves admitted work, usage and reconciliation semantics. For inspection-enabled policies, an expired question or stopped requester ends coordination at the next fresh activity boundary without spending another model call. An admitted read/model result settles and retains its usage and evidence, then returns to the original join. Saved retries and recovery inputs keep their normal lifecycle; a previously committed reply remains authoritative. Pending child outcomes join once when coordination ends. Whole-goal stop waits for an admitted read to settle and does not dispatch a reply afterward.

The restricted reply cannot create an outgoing question or delegation wait; sibling wait-cycle checks remain authoritative.

Question admission checks the full pinned planner input allowance, including the question, current guidance and peer context, before saving its wakeup. An oversized question context returns saved tool feedback rather than starting a reply step that cannot fit. Stopping the whole goal also waits for an admitted planner model/tool attempt to settle before completing cancellation; its result and usage remain accounted, and no reply continuation is dispatched afterward.

The baseline stores coordination checkpoints, planner wakeups, inspection limits and per-question counts. Current workers use `ask_worker` and `reply_worker` v2; parent and sibling questions use the same contracts. Delegated-work inspection exposes saved questions, answers, reads and coordination progress.

Local compatible-model tests cover all three reads, correcting a refused read, saved evidence and uncommitted read recovery, parent/child workspace isolation, the three-read limit, refused edits, shared budget exhaustion, deadlines over a paused restart, stopped requesters, child completions during an admitted read, and whole-goal stop.

<a id="worker-findings"></a>

## Durable planner reactions to worker findings

New Code sessions pin `send_worker_message` v2 on their coders and `worker_findings: true` on their existing planner. A coder can report a finding that affects other active assignments:

```json
{"kind":"call_tool","tool":"send_worker_message","arguments":{"recipient":"parent","text":"The client requires display_name; both implementations must preserve it.","needs_coordination":true}}
```

The report saves a task-scoped parent note and an immediate durable wakeup in one SQLite transaction. The sender continues without a reply wait. Omitted, false, or null flags keep ordinary notes informational. Only `recipient: "parent"` with an enabled direct planner and an open exact coding assignment accepts the flag; sibling flags and unavailable planners return saved refusal feedback. Use `ask_worker` when the sender needs an answer before proceeding.

At the next available child join, the planner coalesces pending marked findings and pending user guidance into one restricted coordination step. It stays in the same Run, retains the original single/group delegation, and receives `coordinating_guidance.findings` with the reports' exact message and sender task identities. User guidance keeps its separate `instructions` field and cursor. Findings remain untrusted task data and cannot grant permissions, answer human waits, approve tools, change budgets or replace required tests.

The planner may use up to three saved `inspect_assignment` reads and two [`stop_worker` attempts](planner.md#planner-stops) for exact captured assignments, then calls `guide_workers` once with focused messages for affected active coders. Both messages null acknowledges a batch that needs no guidance. Saved routing receipts and worker inboxes commit atomically; safe retries replay their original deliveries. The planner returns to its original join, preserving any child outcomes that arrived during coordination. No additional planner or scheduler is created. The usual model/token/attempt limits apply to each extra decision.

Findings arriving after a batch has been captured queue a subsequent bounded step. Handled reports and user guidance are not routed again. Planner questions and finding/guidance steps execute serially. Assignment and goal pauses retain notes and wakes across restart; stop prevents fresh coordination and preserves already admitted work, usage and effect reconciliation.

A reporting child that finishes before its batch can still inform an active sibling in the original group. Once that join closes, its pending reports cannot wake coordination for a later assignment on the same actor. If all targets have closed, the wake settles without an extra model call. Existing message bounds remain in force: 32 sends per assignment, 16 received notes and 16 KiB of peer context per recipient, and 2048 UTF-8 bytes per report. There is no polling loop.

**Delegated work → Planner coordination** restores each finding's review from SQLite. It shows queued reports, the captured batch of findings and user guidance, up to three saved assignment snapshots, and the planner's routing decision. Both messages null displays **No guidance needed**. Refusals retain their saved reason; failed, stopped, blocked and budget-exhausted reviews retain their progress. Wakes that settle without a review display **Skipped before review**. Reports remain bound to their original assignments even when the same actors run later tasks.

Each guidance delivery identifies its exact destination task and Run, and shows **Pending next model request**, **In model context**, or **Assignment closed before inclusion**. An inclusion marker names the actual model activity; it does not claim the worker has followed the guidance or completed its work. A routing refusal's text preview is capped at 2048 UTF-8 bytes with a truncation label; its original receipt remains saved. A routing receipt can be saved before native-tool completion, so review progress and tool status appear separately. Inspection evidence is the immutable captured snapshot, not a later live worker state. **View planner review** on a worker message opens its associated batch.

The existing `GET /v1/harness/sessions/{session}/turns/{turn}/delegated-work` response includes a `coordination` projection for coding turns with a planner Run. It reads the same SQLite transaction as the assignment view, links batch timer identities and history intervals to exact activity receipts, and excludes worker question inspections and other turns. It shows the latest 64 marked parent receipts and 32 reviews with an explicit `truncated` flag. Older records remain saved; an omitted review is never reported as newly queued. Reload, reconnect and restart need no model calls, scheduler actions or additional writes.

The baseline stores the finding cursor alongside message, Run, timer and guidance receipts. Start retries, follow-ups and restart use the saved current contracts.

```powershell
cargo test --locked --offline --test http_api worker_findings
```

Tests use local model fixtures and cover atomic rollback, interrupted sender completion, receipt replay, coalescing, saved inspection, later reports, pauses and restart, questions, closed senders and joins, budgets, whole-goal stop, nullable/ordinary flags and sibling refusals. Coordination view tests also cover coalesced evidence, exact delivery and first inclusion, no-op receipts before tool completion, question isolation, pause/restart, read-only refreshes, queued follow-ups, stopped reviews and skipped closed joins.

<a id="worker-dependencies"></a>

## Durable assignment dependencies

New two-coder Code sessions can also [declare dependencies upfront](#initial-dependencies) using `depends_on` on a `delegate_tasks` member. The waiting worker saves its workspace baseline, then holds before its first model call and enters the same exact dependency review and approved patch handoff described here.

New Code sessions support `wait_for_worker` v1 during planner coordination. Choose two coding assignments captured in the same parallel join:

```json
{"action":{"kind":"call_tool","tool":"wait_for_worker","arguments":{"worker":"coder","dependency":"coder2","reason":"The UI implementation needs the API assignment's final contract."}}}
```

This saves a planner-owned hold on `coder` and a dependency on `coder2`'s exact physical task and Run in one SQLite transaction. The prerequisite continues. The action shares the four-attempt pause/resume allowance per coordination step, including refusals. Finish the step with `guide_workers`. One-coder sessions have no eligible sibling dependency.

When the prerequisite settles, the existing child-completion transaction saves its outcome and an immediate durable wake for the same planner Run. No polling or additional planner is introduced. If the prerequisite finished between the captured coordination step and registration, registration saves the wake itself. Logical activity retries replay their original receipt without another hold, dependency or wake. Original ordered joins still settle once.

<a id="worker-dependencies--reviewing-and-releasing-work"></a>

### Reviewing and releasing work

`coordinating_guidance.dependencies` supplies the dependency ID, exact waiting and prerequisite task/Run IDs, owning planner, hold revision and terminal status. The planner can use `inspect_assignment` on that completed prerequisite to read its bounded saved evidence, then explicitly `resume_worker`, route guidance or `stop_worker` on the waiting assignment. Success, failure, cancellation and saved delegation timeout all wake review. A recoverable model failure remains unsettled until recovery or cancellation supplies a final child outcome. Dependency inspections include bounded final result/error snippets from the parent's committed child outcome, including its saved timeout result.

New Code sessions also support [approved worker patch handoff](#worker-changes). Use `inspect_worker_changes` and `import_worker_changes` to review and transfer a successful prerequisite's immutable patch into its held dependent assignment. The transfer has its own human approval, preserves holds and requires fresh tests.

The wake does not release a hold, import files, grant approval or establish passing parent tests. The planner decides what to do with the saved evidence and closes each review with `guide_workers`. If it leaves the hold in place, later human guidance or a marked finding can request another coordination step.

Manual assignment/goal holds and actor/Run pauses remain independent. Admitted effects may settle while a hold applies; uncertain effects still require explicit reconciliation. Pausing the planner or whole goal retains the completion wake across restart. Resume preserves context, workspace edits, usage, permissions and absolute delegation deadlines.

Resuming early closes the dependency. A later prerequisite completion cannot recreate its hold or trigger dependency review. A waiting assignment finishing, stopping or timing out also closes its dependency when its final outcome is saved. Replaying an old registration after resume or a later assignment never reapplies the hold. Self-dependencies, cross-join targets and circular waits are refused; cycle detection includes unresolved sibling questions in either action order.

<a id="worker-dependencies--harness-and-compatibility"></a>

### Harness and compatibility

Use **Guide this turn** during two-worker Code execution to ask the planner to hold one assignment for its sibling. A worker's marked finding can also initiate coordination. **Delegated work → Planner coordination** shows saved dependencies, their prerequisite outcomes, captured reviews, reasons and hold release status. Refreshing this view is read-only and makes no model calls. It shows the latest 64 dependency records with an explicit partial-history marker.

The baseline includes dependency records while retaining the existing planner hold, receipt, timer and admission contracts. Existing sessions keep their pinned configuration and schemas on upgrade, exact start retry, follow-up and restart. Restart the API and create a new Code session to enable `wait_for_worker`.

```powershell
cargo test --locked --offline --test http_api worker_dependencies
```

Local deterministic fixtures cover atomic registration and replay, recovery after restart, success/failure/stop outcomes, completion during registration, early release, later assignments, manual holds, original deadlines, scope and cycle refusals, bounded actions. No hosted model is needed.

<a id="worker-changes"></a>

## Approved patch handoff between coding workers

New Code sessions enable `worker_changes` for the existing planner. During a successful `wait_for_worker` dependency review, the planner can inspect the prerequisite's immutable patch against the waiting worker's managed copy:

```json
{"action":{"kind":"inspect_worker_changes","worker":"coder","offset":0}}
```

`worker` names the **receiving assignment**, not the prerequisite. The runtime resolves both physical tasks, Runs, source actor, immutable artifact and target permissions from the captured dependency. The model cannot choose another task, directory or permission scope. Failed, stopped or timed-out prerequisites cannot supply a patch. Both tasks must belong to the original parallel join.

Inspection shows an 8 KiB patch page, saved source tests and a hash-checked preview. Follow `next_offset` to review subsequent pages. There are three inspection attempts per coordination step. Larger patches remain available through `actor task changes SOURCE_TASK_ID --json`; the approval card identifies that exact source task and artifact. Patch contents are untrusted task data.

After a conflict-free inspection, the planner can request one import approval:

```json
{"action":{"kind":"import_worker_changes","worker":"coder"}}
```

The harness shows **Transfer reviewed patch** with source/receiving task IDs, artifact identity, changed paths and saved test evidence. Approval is durable and specific to that inspection. Denial changes no files and keeps the hold. The approval is for this transfer; it grants no later edit or command permission.

<a id="worker-changes--held-assignments-and-file-conflicts"></a>

### Held assignments and file conflicts

The receiving worker must retain the same planner-owned dependency hold and hold revision. Already admitted work must settle first, including descendant work. Uncertain effects and unfinished test checkpoints prevent a fresh handoff. The receiver's accepted task workspace and effective writable paths are checked again at admission and immediately before a new write. Stopping the goal or receiver, closing the dependency, archiving the session or reaching the original deadline prevents a fresh transfer. Already admitted writes may settle.

The receiving task must already permit every transferred path. For example, an API-to-UI dependency needs the receiving task's scope to include the API files as well as its UI files. A UI-only scope refuses API edits; approval does not widen it. Source patches containing files outside the source task's scope also refuse handoff. There is no implicit merge or overwrite: every affected file must match the saved source before hash, and its directories must already exist. Changes after review produce conflict feedback and preserve the changed files.

<a id="worker-changes--resume-and-combined-verification"></a>

### Resume and combined verification

Import leaves the dependency hold active. The planner explicitly chooses whether to `resume_worker`, `guide_workers` or `stop_worker` after reviewing the result. Manual assignment/goal holds and actor/Run pauses remain independent.

Earlier receiving-worker test evidence is invalidated in the same SQLite transaction that admits the import, before any filesystem write. This includes an import that later conflicts or is interrupted. The resumed worker should read the transferred files and run its pinned tests against its final workspace. Source test evidence cannot verify the receiving or parent workspace.

The receiver retains its original baseline and edits. Its final patch includes both prerequisite changes and its own work. The planner imports this consolidated downstream patch **once**, then runs fresh parent tests. Separately importing the same prerequisite files first produces a normal hash conflict. The user's project is updated through the existing final patch review and application flow.

<a id="worker-changes--restart-and-recovery"></a>

### Restart and recovery

Inspection and approved bytes use the existing child-import preparation receipts and filesystem journal. Inspection is safe to repeat; writes require explicit reconciliation after an interrupted outcome. **Interrupted child imports** shows the receiver workspace, exact approved artifact and per-file progress for these transfers too. Resume continues only the saved operation while its hold and permissions remain valid. A finished receipt replays its original result without writing again, including after the receiver has progressed. Missing/corrupt receipts or conflicting pending files disable recovery until corrected or failed. Partial writes remain visible; recovery does not roll them back.

**Delegated work → Planner coordination** restores the latest 64 inspection/import records and their exact dependencies, file summaries and outcomes. Reads make no model calls. Receiver history records admission and completion; test invalidation and completion history commit atomically with the parent activity checkpoints.

`domain::worker_changes` and the existing planner own policy and transitions. `WorkerChangesReadPort` resolves authority through SQLite. `LocalChildChanges` reuses managed workspace locks, staged immutable bytes and `LocalPatchApplications`. There is no new actor, scheduler or database schema. Older sessions keep their original configuration and model schemas through exact start retries, follow-ups and restart. Restart the API and create a new two-worker Code session to enable the feature. During execution, **Guide this turn** can request a dependency and approved patch handoff.

```powershell
cargo test --locked --offline --test http_api worker_changes
```

Deterministic local fixtures cover approval/restart, full source-to-receiver-to-parent verification, denial, changed-file conflicts, narrowed permissions, stale task/hold and artifact identities, cancellation, interrupted write recovery, receipt replay, fresh-test enforcement, independent manual holds and old-session compatibility. They use a local scripted HTTP model; no provider key or hosted model is needed.
