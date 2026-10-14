# Planner actions

Planning, saved evidence, guidance and control of individual assignments.

- [Native tools in the durable planner](#planner-tools)
- [Durable task plans](#task-plans)
- [Native planner follow-ups](#planner-follow-ups)
- [Planner guidance during delegated work](#planner-guidance)
- [Inspect active worker assignments](#assignment-inspection)
- [Planner-controlled assignment stops](#planner-stops)
- [Planner-controlled assignment pause and resume](#planner-pauses)
- [Durable native result recall](#result-recall)

<a id="planner-tools"></a>

## Native tools in the durable planner

The existing planner can call an allowed native tool, use its saved result to delegate to workers, ask for clarification, join a task group, schedule a follow-up, or finish. Each decision and tool result passes through the existing Run journal. One planner implementation is installed: `planner` v3, `planner_agent` v3, and `registered_agent.planner` v3, declaring model-and-tool checkpoints.

Coding planners can use [named project commands](tools.md#project-commands) for host-configured format, lint, and build steps. `run_command@1` takes only a name, needs exact approval, saves bounded output, and invalidates prior passing test evidence.

<a id="planner-tools--try-it-locally"></a>

### Try it locally

Use `update_plan` to save a [durable task checklist](#task-plans) before multi-step work and revise its remaining steps as work progresses. The saved plan is fixed model context and visible through task inspection; it does not change tool permissions or completion requirements.

No model server or credentials are required:

```powershell
cargo run --locked --example planner_tools -- .build/planner-tools-demo/demo.db
cargo test --locked --test native_tools --test task_groups --test cli
```

The example calculates 19 + 23 with `sum`, saves the tool result and next model request, closes the runtime, and reopens the database. It verifies the same request, delegates an explanation to an answering actor, and finishes with one tool attempt and four model calls across both actors. It writes a planner configuration beside the database. Scripted actions make the demo deterministic; a separate model fixture reads the saved tool result to construct the delegated goal and restarts again during the child's human wait.

<a id="planner-tools--configure-allowed-tools-and-workers"></a>

### Configure allowed tools and workers

Add a versioned tool allowlist to ordinary planner configuration:

```json
"tools": [{ "name": "sum", "version": 1 }]
```

`tools` defaults to empty. The planner still requires 1–16 permitted worker aliases and its existing delegation limits. Workers and tools have separate allowlists; adding a worker does not grant a tool. `actor tool list` discovers installed contracts. Create a planner with `actor agent create planner --config-file PLANNER.json`; omission of `--adapter-version` selects v3.

The runtime resolves and saves exact `tool_definitions` once at actor creation. Callers cannot supply those pins. Submitted tasks replace the goal without changing tools, workers, or permissions. Follow-ups retain the same catalog and select actor memory through the existing runtime contract. A new task may intentionally request the same tool again; restart of a saved result does not repeat it.

The model sees permitted descriptions and typed argument schemas alongside its worker aliases. For example:

```json
{"action":{"kind":"sum","left":19,"right":23}}
```

New tool-enabled planners pin `model_contract_version: 4`. Listed tool names become action kinds, with argument fields directly in the action. The model adapter normalizes them to the existing internal `call_tool` decision using only the current saved request's catalog and action schema. Direct planner actions such as `delegate_task`, `inspect_child_changes`, `update_plan` and `schedule_follow_up` retain their formats. Restricted coordination requests expose only their currently permitted actions, including single-action schemas. Tool observations and pending actions use the same flat format before context selection; durable decisions, intents and approval payloads remain canonical.

Model contract 4 is the only supported native contract. Omitting the version selects it; other versions are rejected. Flat catalogs reserve planner action names and parameter name `kind`. Restart and later tasks retain the saved catalog and exact pending requests. See the [focused planner probe](validation.md#planner-dispatch-probe).

Compatible Chat Completions targets can also pin [`action_format: "function_calling"`](providers.md#model-function-calling). Each current action becomes an API function; one returned call normalizes into the same decision and native tool intent. JSON schema remains the default.

The planner validates the selected tool and arguments before producing a `native.tool` activity intent: v1 for unscoped tools, v2 for scoped tools without feedback, and v3 for [feedback-capable tools](tools.md#tool-feedback). To enable `list_files`/`read_file`, add their versioned allowlist entries and supply `--source PROJECT` at actor creation; tool v2 returns expected inspection errors as observations. The managed copy remains pinned across tasks and follow-ups. Completion adds `{ "action": ..., "result": ... }` to the same ordered observations used for clarification and child replies. The next model request includes that evidence, allowing the model to correct a rejected read or choose a delegated goal from the result. Responses and Chat Completions use the existing structured JSON adapters; dispatch belongs to the runtime.

<a id="planner-tools--durable-execution-and-controls"></a>

### Durable execution and controls

The planner can also [replace text in permitted workspace files](tools.md#workspace-edits). Add `replace_text` v1, supply `--source PROJECT`, and configure `writable_paths` at actor creation. Every edit waits for durable approval; permission-aware activities use `native.tool` v4. The same planner consumes saved edit feedback/results and can delegate from a subsequent saved read. Interrupted writes require reconciliation, with hash-checked journal recovery on an authorized retry.

Tool contracts, primitive input validation, bounded outputs, retries, idempotency keys, approval waits, and uncertain-effect reconciliation follow the [native tool contract](tools.md#native-tools). Both answering and planner policies share these pure helpers and the same executor. Approval uses a saved `native_tool_approval` wait; denial is saved as an observation before the next decision.

If a pinned tool is missing or changed, the planner blocks before admitting further model or tool work. Queued actor messages remain queued; active Run requests, results, waits, and usage remain saved. Restore the exact tool contract and resume the blocked actor or Run. Changes to tools outside the actor's allowlist do not affect its saved catalog.

Tools consume the Run's attempt allowance. Model calls retain per-Run, inherited delegation, and whole-goal budgets. Direct tools do not consume a delegation slot or depth. Complete observations count toward the next request's input-byte limit; exceeding that limit records `context_limit` while retaining the successful tool result. Known tool failures follow their pinned retry policy; uncertain effects require the configured recovery procedure.

Goal pause/cancellation, individual actor/Run controls, task groups, memory, supervision, and durable follow-ups use existing runtime gates. Already admitted outcomes can commit while new admission is held. Approval signals may be persisted during a hold; execution waits for release. Lifecycle controls cannot bypass reconciliation.

<a id="planner-tools--compatibility-and-ddd-boundaries"></a>

### Compatibility and DDD boundaries

The planner registration uses version 3. Coding, model-only answering and tool-answering registrations retain their current versions. Unknown definitions block execution; install the exact matching implementation before resuming. Native model contract 4 runs inside the same planner implementation.

`domain::planner` owns decisions, pending actions, and observations. `domain::tools` owns pure catalog validation, schemas, approval commands, and activity specifications. `application::ToolRegistry` resolves exact contracts and dispatches through the `NativeTool` port. `PlannerAgentAdapter` pins the catalog and supplies the workflow; bootstrap shares the registry with `AgentExecutor`. Custom hosts should install `PlannerAgentAdapter::new(tools.clone())` and `AgentExecutor::with_native_tools(tools)` together.

Tests cover dynamic tool-to-child data flow across two restarts, ordered groups, goal limits, follow-up holds and memory, malformed actions, queued/active contract restoration, approvals, retry deadlines, transaction rollback, uncertain effects, provider HTTP fixtures, CLI execution, and killed workers at tool intent, saved result, approval, and unsafe-effect boundaries. The focused probe separately records local and hosted planner outcomes.

<a id="task-plans"></a>

## Durable task plans

The native planner can keep a short, agent-reported checklist for a physical task Run. The harness shows it in **Task plan**, above live progress. Simple answers can finish without creating a plan. Planning uses the existing native planner.

New harness sessions enable the actor-pinned `task_planning: true` setting. Existing sessions and other planners default to disabled, preserving their saved configuration and prompt size. Start a new harness session to use plans, or enable the setting when creating a planner through the native API/CLI. Each update is a model decision and counts toward the normal call/token budget.

The model selects a structured action:

```json
{"action":{"kind":"update_plan","expected_revision":0,"explanation":"Inspect the project before implementing","steps":[{"id":"inspect","title":"Inspect the project","status":"in_progress"},{"id":"implement","title":"Implement the change","status":"pending"}]}}
```

The next model prompt includes `task_plan` with its revision, explanation and steps. It is fixed context, retained even when older tool observations are trimmed. An absent plan is `null` and has expected revision zero. Each accepted update increments the plan revision. Updates replace the full checklist; stable IDs identify retained steps. Unfinished work can be renamed, reordered, removed, or replaced when steering changes the task. Every completed step must remain with its exact ID, title and status. To revisit finished work, add a new step.

Plans have 1–16 steps, unique IDs of 1–32 ASCII letters/digits/underscores/hyphens, single-line titles of 1–256 UTF-8 bytes, and a single-line explanation of 1–512 UTF-8 bytes. At most one step is `in_progress`; the other statuses are `pending`, `completed` and `blocked`. Invalid updates leave the plan unchanged and return `invalid_plan_update` feedback to the next model request. They do not publish a plan event.

The plan lives in the planner's saved workflow state. Its checkpoint, `task_plan_updated` history event, model result and next activity intent commit through the same Run persistence boundary. Duplicate activity completion cannot advance its revision or publish another update. SQLite requires no new table or migration. Older checkpoints without a plan remain valid.

Task inspection exposes a bounded `plan` on each native planner node, for direct and registered planner invocations. Read the plan from a consistent task snapshot; SSE can prompt clients to refresh that snapshot. Restart restores the saved checklist. Steering reaches the next fresh model decision with the saved plan. A new conversation turn starts with no checklist; earlier plans remain inspectable.

Steps describe the agent's assessment, not verified execution. Marking a step completed grants no tool permission, launches no child task, supplies no test evidence and changes no Run status. Existing approval, cancellation, pause, budget and verified-completion rules still govern work. Unfinished steps remain saved when a turn stops or completes.

```powershell
cargo test --locked --offline --test task_plans
cargo test --locked --offline --test http_api task_plans
```

Tests cover context trimming, checkpoints, bounded validation, completed step preservation, stale revisions, duplicate completion, HTTP inspection, restart recovery, steering, follow-up isolation, and refusal to treat a completed checklist as passing test evidence.

<a id="planner-follow-ups"></a>

## Native planner follow-ups

The existing native planner can finish a task and request one future task on its own actor. Its model decision, stable reminder ID, UTC deadline, and chain history are saved before registration. The follow-up uses ordinary [actor reminders](actor-model.md#actor-reminders), mailbox capacity, sequential execution, and memory selected at Run launch.

<a id="planner-follow-ups--opt-in"></a>

### Opt in

Add this policy to a registered planner actor's configuration:

```json
"follow_up": { "max_follow_ups": 1, "min_delay_ms": 1000, "max_delay_ms": 86400000 }
```

Omitting it disables scheduling. `max_follow_ups` accepts 1–64; delay bounds must be positive, ordered, and fit supported timestamps. Omitted delay bounds default to 1 ms and 24 hours. The policy is pinned with the actor. The runtime supplies `follow_up_context`; callers cannot set it in configuration or override it through task payloads. Generic workflow workers cannot enable this feature.

When the chain has room, the model schema includes this terminal action:

```json
{
  "kind": "schedule_follow_up",
  "summary": "The result is saved; review it later.",
  "goal": "Review the earlier result against the current environment.",
  "after_ms": 60000
}
```

`summary` finishes the current task successfully. `goal` is nonempty and at most 60000 UTF-8 bytes. `after_ms` must satisfy the configured bounds; it resolves once from the saved decision transition timestamp. Retries or delayed registration do not move the deadline. The model cannot choose another actor or change the follow-up's model, tools, permissions, memory policy, or per-Run budget. Delegation and task groups continue through the same planner implementation and registration.

The prompt reports the chain root, depth, remaining follow-ups, and delay policy. The root has depth zero. Each follow-up inherits the same root and policy and increments depth; a fresh Run does not reset the allowance. Every task can request at most one successor, so the chain contains at most the configured number of follow-ups. A separately submitted public task or manual reminder starts an independent root.

At the limit the schema omits scheduling and asks the model to finish. If a model nevertheless supplies an otherwise valid scheduling action, its answer still finishes and a durable refusal records `chain_limit`; no reminder or successor task is created. Invalid delay, empty goal/summary, disabled scheduling, and malformed responses follow the existing failure contract. Per-Run model/token limits still apply; the chain limit bounds task count. Opt in with `goal_budget` to [pool a durable model/token allowance](state.md#goal-budgets) across the entire chain and its delegated descendants. Follow-up Runs inherit their source's goal scopes; they never renew those allowances.

<a id="planner-follow-ups--inspect-and-cancel"></a>

### Inspect and cancel

Create and send tasks through the existing CLI. After execution, inspect the source task:

```powershell
.\target\debug\act0r.exe --db $db actor task status $parentMessageId
.\target\debug\act0r.exe --db $db actor reminder status $reminderId
.\target\debug\act0r.exe --db $db actor reminder cancel $reminderId
.\target\debug\act0r.exe --db $db actor task status $reminderId
```

The source task and Run expose `follow_up`: reminder ID, goal, due time, origin, `pending`/`registered`/`rejected` status, refusal code, and settlement time. `pending` is an intent awaiting registration. `registered` means a reminder exists; publication and execution have separate lifecycles. `rejected` means no successor will be published; explicit cancellation retains a cancelled reminder receipt. Parent Run/task IDs, chain root, child depth, and policy also appear in the reminder's `origin` and the published task's `follow_up_origin`.

The existing 64-pending-reminder bound applies. When full, registration preserves the parent's successful answer and records `reminder_capacity`. Releasing capacity later does not silently retry that finished request. Task capacity applies at publication; full or paused actors keep registered reminders scheduled until eligible.

Cancellation before the successful model transition prevents any follow-up intent. Once that transition wins, the parent is finished and the durable scheduling request remains. Cancel its reminder before publication; afterward use `actor task cancel REMINDER_ID`. Cancelling the successor preserves parent success and its registration receipt. Clearing memory preserves chain history. Reminder/intent IDs remain reserved against early direct sends and reuse after refusal or cancellation.

Use `actor task cancel-goal TASK_ID` to [cancel the entire original goal](recovery.md#goal-cancellation) through a source, descendant, or reminder handle. Its saved receipt stops all owned future follow-ups and pending children across restart, while retaining successful source results and charges.

Use `actor task pause-goal TASK_ID` to [hold the goal and its follow-ups durably](recovery.md#goal-pause). An admitted model outcome can finish and register its reminder while held; publication waits for `resume-goal`, using the original deadline and allowance.

`actor reminder cancel` also accepts a pending intent ID before registration, including while the worker is stopped. Cancellation saves the reminder's cancelled receipt and the source intent's `cancelled` refusal together, without consuming a pending reminder slot. Restart cannot revive that request.

The worker's normal polling drives registration and publication. `worker run --until-idle` waits for an eligible future reminder; pauses, blocked actors, and unavailable task capacity retain the existing idle rules. Registration completes even while the actor mailbox is paused, so its cancellation and inspection remain available.

<a id="planner-follow-ups--recovery-and-ddd-boundaries"></a>

### Recovery and DDD boundaries

Run owns the scheduling context, model choice, and follow-up intent. Actor owns pending reminders. Accepted task records own provenance, read atomically when creating the next Run, including tasks queued behind other work. `ActorService` enriches native invocation inputs through the ActorStore port. SQLite implements the transaction; the domain does not query the database or run clocks itself.

Successful completion notification atomically registers the reminder, saves the Run's receipt and history, updates the Actor, finishes the parent task receipt, publishes its completion message, and marks the notification consumed. An interrupted transaction commits all of these or none. Source business results remain unchanged by registration or refusal. The existing completion mailbox turn appends memory and launches any next queued task atomically.

Registration time belongs to the follow-up receipt. The source Run keeps its original terminal completion time, so delayed registration cannot turn an on-time delegated result into a parent deadline failure.

The current database version protects optional Run, reminder, and task metadata and indexes reserved intent IDs and queued provenance. Existing disabled planners, saved requests, memory, waits, manual reminders, registration pins, and usage keep their behavior without an eager rewrite. This extends the current planner.

Run the scripted demo without credentials:

```powershell
cargo run --locked --example planner_follow_ups -- .build/planner-follow-ups-demo/demo.db
cargo test --locked --test planner_follow_ups
```

The planner finishes and schedules a review, stops through the deadline, and recovers one successful follow-up with saved memory. Its script deliberately asks again at the chain limit to demonstrate refusal. Tests cover chains, queued provenance, payload forgery, cancellation, capacity refusal, rollback at each registration write, migration, and process kills after intent, registration, publication, Run creation, and model admission.

<a id="planner-guidance"></a>

## Planner guidance during delegated work

New Code sessions can route **Guide this turn** instructions to active coding assignments while the planner waits for results. Send “Keep the existing API” while workers are coding. The existing planner Run wakes at its next quiescent child join, runs a bounded coordination step, saves worker messages, and returns to the original join.

Workers can also queue this bounded step by reporting an actionable [`needs_coordination` finding](coordination.md#worker-findings) to their enabled parent. Pending marked findings and user guidance coalesce, retain separate saved cursors and identities, and stay scoped to the original child join.

The routing decision uses `guide_workers` v1. New sessions also permit up to three [saved assignment inspections](#assignment-inspection) and up to two [`stop_worker` attempts](#planner-stops) before routing. Stops bind to a captured task and Run and leave sibling assignments running. Up to four [`pause_worker` / `resume_worker` attempts](#planner-pauses) can hold and release a captured assignment under the planner's own ownership. Your manual holds stay independent, and original deadlines still apply. Its `coder` and `coder2` arguments are focused messages of 1..2048 UTF-8 bytes, or null when that worker needs no guidance. Both null is valid. Only started, verified, unfinished native coding assignments are offered. The snapshot includes exact physical task and Run identities. A closed assignment receives saved refusal feedback; a later assignment on that actor never receives its message.

Messages appear in **Delegated work → Worker messages** as notes from `parent`. Each selected worker receives the note in its next fresh model call. Pending and Applied retain the existing peer-message meanings. An admitted call, saved retry, recovery input or human wait retains its existing contract. A note cannot answer a human question, approve a tool, change permissions, reset budgets or replace passing-test evidence. Answer a worker's existing wait separately.

Root instructions arriving before the wake are coalesced in order. Exact HTTP retries do not create another wake. A later instruction queues another bounded step. Worker questions and guidance run serially in the same planner Run; guidance already included in a question reply still has its routing wake saved. The original tasks, group, workspaces and ordered outcomes remain saved. Children completing during coordination join once after it ends.

Run and whole-goal pauses hold new execution. Cancellation and shared model, token and attempt allowances retain authority. Each coalesced step consumes one model decision and one native tool activity, plus any bounded inspection or stop decisions and reads; resuming the join needs no model call. Existing 16-message/16-KiB peer-context limits apply to each worker. Unavailable/full recipients produce saved feedback while other selected workers can still receive guidance.

SQLite commits the root instruction, receipt and immediate durable timer together. Routing commits its immutable aggregate receipt and all accepted worker inbox updates in one transaction. Interrupted tool completion replays that receipt without duplicate messages. Compact tool results report delivery metadata; full messages and refusals remain inspectable in saved history. The baseline includes the indexed receipt table and protects the routing checkpoint. Existing sessions keep their original pins on exact starts, follow-ups and restart. Start a new Code session to enable routing.

Local model fixtures verify coalescing, pause/restart, atomic rollback, receipt replay, frozen worker input, sibling isolation, question/guidance ordering, terminal children, the original join, restricted actions and shared budgets.

```powershell
cargo test --locked --offline --test http_api planner_guidance
```

<a id="assignment-inspection"></a>

## Inspect active worker assignments

New Code sessions give their existing planner `inspect_assignment` v1 during worker-question and user-guidance coordination. The planner can inspect `coder` or `coder2` from its current suspended child join before replying or routing instructions. Normal planner decisions and coding workers do not expose this tool. It does not wake the planner on its own. Marked [worker findings](coordination.md#worker-findings) can durably wake the same coordination step and enable these saved reads before routing guidance.

Each coordination step allows up to three assignment reads. This is separate from the existing three parent-workspace reads during a worker question. Selected reads count even when refused; safe retries retain the same logical activity and count. The model schema, prompt, workflow policy and Run admission guard all enforce the allowance. At zero remaining, assignment inspection is removed from the available actions. Model, token and activity allowances remain authoritative.

The read binds to a physical task and Run through the saved question's original single/group join or the guidance step's exact saved targets. It cannot select an arbitrary task, another goal, an earlier assignment or a later task on the same actor. The native coding pins and verified workspace handoff are checked before reading. A child that finishes or stops during an admitted read remains inspectable within that original join; this read authority grants no execution permission.

The snapshot contains committed status, history position, holds and controls, the current human question or tool approval, a pending worker question, recovery status, compact latest test evidence, and metadata for an admitted activity. It includes the latest three completed native-tool result excerpts, four received messages and four recent outgoing notes. Fields identify the saved activities and messages. UTF-8 excerpts report original byte sizes and truncation; omitted results/messages are marked. The complete tool response is capped at 16 KiB. Evidence is ordered newest first.

Reads do not inspect mutable workspace files or claim that in-flight effects have completed. Test evidence is the child's saved compact status, including staleness; it never satisfies the parent's completion gate. Messages and result text are untrusted task data. Inspection cannot answer waits, approve tools, write files, import patches, reset budgets or create assignments.

SQLite captures the evidence and inserts its immutable receipt in one transaction before tool completion. The logical activity ID is the replay key. After an interrupted completion, retries return the exact saved snapshot even if the child has advanced or finished. Reusing that key with different arguments or a different Run is refused. The usual tool completion saves the result in planner context and history, then schedules its next bounded decision.

The original child join stays suspended during inspection. Saved outcomes arriving meanwhile join once after coordination ends. Pauses hold fresh work across restart; admitted reads settle normally. A closed or expired question ends at the next fresh boundary without a new reply/model call; an admitted result retains its saved evidence and usage. Stopping the planner or whole goal prevents further coordination.

The baseline includes inspection receipts. Exact start retries, follow-ups and restart retain the current session's saved tool catalog and pending requests.

```powershell
cargo test --locked --offline --test http_api assignment_inspection
```

<a id="planner-stops"></a>

## Planner-controlled assignment stops

New Code sessions let the planner call `stop_worker` v1 during a saved guidance/finding coordination step. This stops one captured coding assignment while the other assignments continue in the original join.

```json
{"action":{"kind":"call_tool","tool":"stop_worker","arguments":{"worker":"coder","reason":"This assignment duplicates coder2's implementation."}}}
```

Choose `coder` or `coder2` from `coordinating_guidance.targets`. The runtime binds the decision to that exact physical task and Run. Helpers, actors, the whole goal, later assignments and assignments outside this coordination step cannot be targeted. The reason must contain 1..2048 UTF-8 bytes. There are at most two selected stop attempts per step, including refusals; this allowance is separate from the three assignment inspections. The saved checkpoint and native activity admission both enforce the restriction.

The planner receives `stop_requested: true` after SQLite atomically saves the assignment cancellation hold, ordinary cancellation receipt and planner stop receipt. This reports acceptance of the request. Existing durable cancellation cleanup settles the assignment, including after restart. Interrupted native tool completion replays the same logical activity receipt, without another hold or stop. Changed arguments or a different planner Run conflict.

The hold prevents further execution in that assignment subtree. Admitted work may settle and save its result and usage. An unsafe edit, command or test with an uncertain result still requires explicit reconciliation. Stop does not undo files, import a patch or erase completed results. A finished or already stopping assignment receives a saved refusal, without changing its outcome. Whole-goal stop and archive rules remain authoritative.

After inspecting evidence or stopping an assignment, the planner calls `guide_workers` to close coordination, using null for a stopped worker. It may still guide the remaining worker. The original tasks, group and ordered results remain saved; outcomes arriving during coordination join once after it closes. Run and goal pauses hold new model/tool admission. Shared budgets still apply to every model decision; unlimited budgets do not remove the per-step bound.

**Delegated work → Planner coordination** shows the reason, exact task and Run, tool receipt status, and current settlement status. `Stopping` and `Stop pending · needs reconciliation` remain distinct from `Stopped`. Refused requests also remain visible. Reading this view makes no model calls.

The baseline includes indexed planner stop receipts and protects extended planner checkpoints. Existing sessions retain their original tool definitions and schemas on exact start retries, follow-ups and restart. Start a new Code session to enable this tool.

```powershell
cargo test --locked --offline --test http_api planner_stops
```

<a id="planner-pauses"></a>

## Planner-controlled assignment pause and resume

New Code sessions offer `pause_worker` and `resume_worker` v1 during a saved guidance/finding coordination step. These actions hold and release one captured coding assignment while its siblings continue in the original join.

```json
{"action":{"kind":"call_tool","tool":"pause_worker","arguments":{"worker":"coder","reason":"Wait for coder2 to confirm the API contract."}}}
```

```json
{"action":{"kind":"call_tool","tool":"resume_worker","arguments":{"worker":"coder","reason":"The sibling's finding confirms the API; continue the saved implementation."}}}
```

Choose `coder` or `coder2` from `coordinating_guidance.targets`. The runtime binds each decision to the captured physical task and Run, not the worker actor or a later assignment. Helpers, queued assignments without a Run, other joins and the whole goal cannot be targeted. Each reason needs 1..2048 UTF-8 bytes. `worker_pausing` reports a shared allowance of four selected pause/resume attempts per coordination step, including refusals. Inspections and stops have their own allowances. The saved checkpoint, action schema and native activity admission enforce the restriction independently.

<a id="planner-pauses--ownership-and-execution"></a>

### Ownership and execution

SQLite saves the planner-owned hold and immutable activity receipt atomically. The hold has its own revision and owning planner Run. `resume_worker` releases only that owner's hold. Manual assignment, ancestor goal/assignment and actor/Run pauses remain authoritative. Manual **Resume assignment** likewise cannot release a planner hold. Repeated requests for the same state receive a saved refusal, while interrupted completion replays the exact logical activity receipt. Replaying an old pause after a newer resume never reapplies the pause. Changed arguments, action or planner Run under the same activity conflict.

The existing admission gate holds tools, model calls, retries, durable timers, reminders and descendant tasks. Already admitted operations may finish and save their results and usage; the view distinguishes **Pausing · admitted work settling** from **Paused by planner**. Context, completed tools, workspace, permissions, approvals, usage and the original delegation join stay saved. Resume creates no replacement task or actor.

Deadlines remain absolute. A parent can enforce an expired delegation deadline and stop a held child. Assignment and whole-goal Stop take precedence. An uncertain edit or command still requires explicit reconciliation after resume; resuming does not approve work, undo files, import a patch or clear a recovery decision. Completed, stopping and archived assignments cannot receive fresh pause/resume transitions.

<a id="planner-pauses--continuing-coordination"></a>

### Continuing coordination

Use [`wait_for_worker`](coordination.md#worker-dependencies) when one captured assignment depends on another's completion. It saves the hold and dependency together and wakes this planner when the prerequisite settles, including failure or stop. The planner reviews the outcome before explicitly releasing its hold. This shares the four pause/resume attempts; ordinary pauses retain their original behavior.

The captured `targets[].pause` reports planner, manual and inherited holds and the planner hold revision. `inspect_assignment` also reports the current planner hold identity and revision when present. Each tool acknowledgement reports the saved control and whether independent assignment/ancestor holds still prevent execution. These are snapshots, not promises that the worker is currently executing.

Finish each coordination step with `guide_workers`, using null where no worker guidance is needed. Arrange for a sibling to report a marked finding when the dependency is ready, or send **Guide this turn** to request resumption. A pause does not create a polling loop or a new wake timer; pausing every worker without a way forward can leave the join waiting for human guidance. Original outcomes still join once, including outcomes arriving during coordination.

**Delegated work → Planner coordination** restores reasons, exact assignments, receipt status, current hold state and independent holds without making model calls. Earlier pause decisions show **Pause released later** after resumption. Worker cards explain planner ownership separately from your manual controls.

The baseline includes planner pause receipts and separate planner holds in the SQLite admission view. Exact start retries, follow-ups and restart preserve saved tool definitions and manual holds.

```powershell
cargo test --locked --offline --test http_api planner_pauses
```

The deterministic local fixtures verify atomic rollback, replay after restart, sibling progress, exact joins, independent manual/goal/Run holds, admitted model settlement, unsafe edit reconciliation, original deadlines, scoped refusals, action limits, later actor assignments. No hosted model or API key is required.

<a id="result-recall"></a>

## Durable native result recall

Native `tool` and `planner` actors can retrieve earlier tool results after bounded context has evicted them. `list_results` discovers saved activity IDs; `read_result` returns bounded pages of the original serialized JSON. Both execute as ordinary durable tool activities. They read saved evidence without dispatching the original tool or inspecting current workspace files.

<a id="result-recall--configuration"></a>

### Configuration

Add either or both versioned tools to the actor's allowlist:

```json
{
  "tools": [
    { "name": "run_tests", "version": 1 },
    { "name": "list_results", "version": 1 },
    { "name": "read_result", "version": 1 }
  ],
  "context": { "max_input_bytes": 16000, "max_observations": 1 }
}
```

These entries belong inside a complete tool/planner configuration with its goal, target, and budgets. `run_tests` also needs a source workspace, a pinned test command, and approval. Recall itself requires no workspace, writable paths, or approval. Installing the tools does not grant them to existing actors; contracts are resolved and pinned when an actor is created.

<a id="result-recall--discover-results"></a>

### Discover results

```json
{"kind":"call_tool","tool":"list_results","arguments":{"tool":"run_tests","after":null,"limit":8}}
```

The result contains `ok:true`, `results`, and `next_after`. Each entry has an `activity_id`, versioned `tool` ID, `result_bytes`, and the original result's boolean `ok` flag (or null if absent). It includes no contents or original input arguments. Entries use scheduling order. Pass the returned `next_after` as `after` to continue; null means the last page. `tool` is an optional exact tool-name filter. `limit` defaults to 8 and accepts 1..16.

<a id="result-recall--read-original-json"></a>

### Read original JSON

```json
{"kind":"call_tool","tool":"read_result","arguments":{"activity_id":"SAVED_ACTIVITY_UUID","offset":0,"max_bytes":2048}}
```

Successful output has this shape:

```json
{
  "ok": true,
  "format": "json",
  "source": {
    "activity_id": "SAVED_ACTIVITY_UUID",
    "tool": { "name": "run_tests", "version": 1 },
    "result_bytes": 42,
    "ok": false
  },
  "offset": 0,
  "next_offset": null,
  "complete": true,
  "text": "{\"exit_code\":101,\"ok\":false,\"stdout\":\"failed\"}"
}
```

`text` is serialized JSON, including its quotes and escapes. The example's UUID and byte count are illustrative. A page may end inside the JSON document, so it is not necessarily independently parseable. Follow `next_offset`, concatenate `text` in order, then parse the assembled JSON. Offsets count UTF-8 bytes; the reader defers an incomplete final Unicode character and returns the actual next boundary. `complete` is true only when one page contains the entire document. A final partial page has `next_offset:null` and `complete:false`.

`offset` defaults to zero. `max_bytes` defaults to 2048 and accepts 4..4096 raw JSON bytes. The serialized response may be larger because it contains metadata and escapes `text` again. The read tool's output cap is 32768 bytes; listing is capped at 8192. Choose a page size that fits the actor's pinned context. An essential observation that cannot fit still produces the existing `context_limit`; recall does not expand context limits.

The outer `ok:true` means retrieval succeeded. `source.ok:false` and the JSON in `text` can describe a failed test or a tool rejection. Recalling old tests does not refresh `test_status`, clear its stale flag, or prove that current files pass. The original activity ID can come from `list_results` or compact `test_status`.

Optional arguments may be omitted or null. Strict provider schemas require all property keys and use null for optional fields. Unknown fields, including `run_id` and workspace overrides, are refused.

<a id="result-recall--scope-and-recovery"></a>

### Scope and recovery

Execution supplies the Run and recall activity IDs through the existing `NativeToolCall`; model arguments cannot choose them. Sources must be completed native tool activities in that same Run. Model responses, recall results, failed execution attempts, and unresolved effects are excluded. Operator-accepted reconciliation results are eligible after acceptance commits. Previous actor tasks and parent/child Runs are outside this scope; delegation can pass chosen recalled evidence through its normal goal or reply.

Each query sees only results committed before its own saved recall intent. The reader checks ordered history, so a tool queued earlier but completed later remains excluded. Retrying an interrupted query keeps that boundary and its original arguments. A newly scheduled page has its own boundary; activity IDs remain stable, and continuation skips entries at or before its cursor.

Expected query problems become bounded saved `ok:false` feedback: `result_input` for invalid arguments, `result_offset` for invalid byte boundaries/ranges, and `result_unavailable` for an inaccessible source or cursor. Foreign, missing, model, and recall activity IDs receive the same unavailable message. Storage or invalid trusted execution-context failures retain ordinary execution-error semantics.

Recall uses `SafeToRepeat`, the existing retry policy, and feedback-capable `native.tool` v3. Its intent, bounded result, and next model request use existing Run transactions. Tool attempts consume the Run's attempt budget; subsequent model decisions consume their usual model/token and goal budgets. Holds and cancellation still apply. No extra model call, queue, scheduler, planner, or database migration is introduced. SQLite uses the shared baseline.

<a id="result-recall--ports-and-adapters"></a>

### Ports and adapters

| Layer | Responsibility |
| --- | --- |
| `domain::result_recall` | Typed scope, queries, metadata, limits, and UTF-8 page validation |
| `application::ports::ResultRecallReadPort` | Read completed results within the trusted Run and intent boundary |
| `adapters::sqlite::SqliteStore` | Snapshot queries, history guards, metadata listing, bounded JSON byte slices |
| `adapters::result_recall` | Native tool contracts, arguments, results, and expected feedback |
| `adapters::native_tools::agent_tool_registry` | Compose the full host catalog with workspace and recall ports |

Custom hosts can install `result_recall_tools(reader)` in their `ToolRegistry`. The earlier local, inspection, editable, and tested registry constructors retain their original catalogs. Custom persistence adapters must implement the same scope and completion-boundary guarantees. Workflow policies remain synchronous and perform no storage I/O.

<a id="result-recall--verification"></a>

### Verification

```powershell
cargo test --test result_recall --locked
cargo test --test workspace_tools recall --locked
cargo run --locked --example result_recall -- .build/result-recall-demo/demo.db
```

The deterministic example uses a real dependency-free Rust project and Cargo tests. A planner saves one failed test, evicts its output through later observations, restarts, discovers and reads that failure, restarts again, then delegates review of the recalled evidence. Every planner request fits 16000 bytes and one observation. Current managed files are changed after the test; recall still returns the exact historical failure. A fixture execution ledger proves the original test ran once. The source project stays intact. The planner and reviewer use 12 model calls total; no provider credential is needed.

Tests cover JSON objects, arrays, scalar values and null; exact Unicode/escape paging; list cursors and filters; access refusals; post-intent completion exclusion; reconciled acceptance; read-only behavior; completion rollback and duplicate delivery; both native policies; holds, budgets and delegation; CLI discovery/execution; and workers killed before dispatch, after admission, after return, after commit, and in a delegated human wait. Local verification establishes execution correctness, not live-model recall quality or physical power-loss recovery.
