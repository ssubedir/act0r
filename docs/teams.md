# Native teams and event streams

Advanced team configuration, task inboxes and reconnectable committed history.

- [Native agent teams](#agent-teams)
- [Human decisions for team tasks](#team-inbox)
- [Durable goal events and live watching](#team-events)

<a id="agent-teams"></a>

## Native agent teams

A version-1 JSON configuration registers one native planner v3 and 1..16 named tool v1 or answer v1 workers. The team owns their assembly and stable identities. Each actor retains its existing durable mailbox, serialized transitions, memory and supervision; each task runs through the existing Run engine. Team setup adds no scheduler or agent loop.

<a id="agent-teams--run-a-team"></a>

### Run a team

The included [agent-team.json](../examples/configs/agent-team.json) needs no model server. Its scripted planner delegates to two answer workers and combines their results.

```powershell
$team = cargo run --locked --offline -- --db teams.db team create --config-file examples/configs/agent-team.json
$task = cargo run --locked --offline -- --db teams.db team submit $team --goal "Design a durable actor workflow"
cargo run --locked --offline -- --db teams.db worker run --until-idle --max-concurrent-activities 2
```

The worker exits when work completes or stops at a durable wait. Inspect the saved team and task afterward:

```powershell
cargo run --locked --offline -- --db teams.db --json team status $team
cargo run --locked --offline -- --db teams.db --json actor task status $task
cargo run --locked --offline -- --db teams.db team list
```

`team create` and `team resume TEAM_ID` print the team ID; global `--json` returns member IDs, setup status, prepared workspaces, actor status and queue counts. `team submit` prints a physical task ID, which works with ordinary task inspection, goal controls, change review and patch export. Use `--message-id UUID` to retry an identical submission without another mailbox entry or task. A changed goal under the same message ID is refused. The planner's pinned queue capacity applies; duplicate retries consume no additional slot. List returns the newest 100 teams and explicitly marks truncation.

Use `team task status TEAM_ID TASK_ID` for the named goal tree and `team task inbox TEAM_ID TASK_ID` for pending human decisions. `team task answer` and `team task approve` resolve exact durable wait IDs across workers, descendants and follow-ups. See the [team inbox guide](#team-inbox) for restart behavior, approvals/denials and a runnable sample.

<a id="agent-teams--configuration"></a>

### Configuration


Top-level fields are `version`, `name`, optional `source`, `planner` and `workers`. Team and worker names contain 1..64 ASCII letters, digits, hyphens or underscores. Worker names are unique; `planner` is reserved. Descriptions are bounded to 4096 bytes; the complete configuration is limited to 1 MiB. Unknown configuration fields are rejected.

The planner object contains `config`, optional `attempt_limit` (default 100), and optional `queue_policy` (default `{"task_capacity":65}`). Workers use the same fields plus `name`, `description` and an explicitly versioned `adapter`. The planner adapter is fixed at v3.

Each `config` is the existing native adapter configuration. Models, context limits, tool allowlists, writable paths, fixed test commands, completion requirements, memory retention, supervision, delegation and follow-up policies remain actor policies. The planner's `workers` field is generated from named workers and their saved IDs. Do not supply runtime-owned `workers`, `workspace`, `workspace_input`, `actor_memory`, `follow_up_context` or `tool_definitions`. Worker adapter versions must be explicit. A team file grants no tools beyond each member's own allowlist.

The CLI resolves `source` relative to the configuration file and saves its canonical absolute path. Rust callers supply an absolute source path. With source, planner and tool workers get independent managed copies from one common immutable snapshot. Answer workers have no workspace. Without source, tools requiring a workspace, handoff and coding completion policies are refused. Existing copy bounds and exclusions apply, including `.git`, `.env`, build output and runtime-private folders.

For coding teams, select `{"name":"tool","version":1}` workers and configure their normal `tools`, `writable_paths` and `test_command`. Enable `change_review` and `completion_policy: "require_passing_tests"` to require verified completion. Enable `accept_workspace_handoff` on tool workers and `workspace_handoff` on the planner to receive fresh isolated input per delegated task. A planner with `child_changes: true` can inspect and approve imports before testing combined code. These policies are opt-in; see [handoff](coordination.md#workspace-handoff), [imports](coordination.md#child-changes) and [verified completion](tools.md#verified-completion).

The scripted sample executes fixed actions. For a live model, select a supported model target on each member and remove its `script`; existing provider key environment settings apply. For LM Studio the target is:

```json
{ "provider": "openai", "protocol": "chat_completions", "model": "your-loaded-model", "endpoint": "http://localhost:1234/v1/chat/completions" }
```

<a id="agent-teams--durable-setup"></a>

### Durable setup

Creation validates and pins every member before storing an intent or allocating a copy. SQLite saves the raw configuration, exact registrations/tool contracts, budgets/policies and all actor IDs under a unique team name. A per-team filesystem lock serializes setup across processes without holding a database transaction while copying files.

The first copy publishes one immutable source snapshot under the team ID. Member workspaces use saved actor IDs as stable copy identities. Filesystem publication is atomic; retries reuse saved snapshots and verify unpublished copies against them. Workspace paths and the common snapshot digest are checkpointed in SQLite. All initialized actor records and team readiness then commit in one transaction. Actors cannot execute or receive work before this final publication. Their IDs are reserved against ordinary actor creation during setup.

If interrupted, the team remains `preparing`; recoverable setup errors are saved for inspection. Repeat `team create` with the same name and configuration, or use the saved ID from `team list`:

```powershell
cargo run --locked --offline -- --db teams.db team resume TEAM_ID
```

Resume uses saved configuration and contracts. It refuses missing/changed installed contracts and conflicting workspace bytes instead of silently repinning or overwriting them. After a snapshot is saved, resume can finish even if the original project has moved. `team resume` avoids rereading or resolving the original JSON file. Setup recovery is explicitly invoked; the ordinary worker executes ready teams.

A ready team is immutable. Repeated creation/resume retains actor IDs, workspace edits, mailbox/task history, memory, actor pause/supervision state and Run checkpoints. Changed configuration under the same name is refused; choose a new name for another configuration. `ready` describes completed assembly, while member statuses describe their current operational state. Team submission checks every member's actor binding in the same SQLite transaction as mailbox admission and task-handle creation.

<a id="agent-teams--rust-api-and-verification"></a>

### Rust API and verification

Use the [team inbox](#team-inbox) for named goal progress and durable human decisions. The [goal timeline](#team-events) adds `team task events` and `team task watch`, with bounded committed-history pages and reconnectable cursors across the planner, owned workers and follow-ups.

The [local HTTP API](api.md#http-api) exposes the same team and goal services under `/v1`, including idempotent submission and reconnectable SSE. It runs alongside the existing worker and retains the native actor/Run execution contract.

```rust,ignore
let (engine, agents) = act0r::bootstrap::local_app("teams.db").await?;
let spec: act0r::domain::teams::TeamSpec = serde_json::from_slice(&bytes)?;
let team = agents.teams()?.create(spec).await?;
let task = agents.teams()?.submit(team.id, act0r::domain::MessageId::new(), goal).await?;
let view = agents.teams()?.status(team.id).await?;
let saved_configuration = agents.teams()?.get(team.id).await?;
```

`domain::teams` owns configuration constraints, pinned assembly and projections. `TeamService` coordinates validation, intents, preparation, publication and admission. `TeamStore` supplies persistence; SQLite adds team intents and identity reservations without rewriting existing actor/Run payloads. `TeamWorkspaces` supplies setup ownership and stable preparation; the local adapter reuses existing snapshots and managed-workspace code. Pure `AgentAdapter::pin_config` validates preparation without filesystem effects. Custom adapters remain unaffected and are outside team v1.

```powershell
cargo test --locked --offline --test teams
cargo run --locked --offline --example verified_completion
```

The verified-completion demo now writes and loads a JSON team with a planner and three coding workers. Two module repairs overlap, pass their tests and are imported. Combined tests fail and completion is refused. The demo restarts, reuses the saved team without resetting actors, delegates a fresh integrated repair, retests and exports the verified patch. It uses real Cargo tests and a 21-model-call goal budget; its scripted fixture approves its own tool/import waits.

Team tests cover full preflight rejection, configuration/identity conflicts, concurrent creation, workspace replay, publication rollback, contract restoration, damaged copies, queue/duplicate admission, member binding changes, durable parallel human waits, actor memory, migration preservation, CLI paths and process-kill recovery. These are local application-crash checks; distributed team ownership and external-framework workers remain future work.

<a id="team-inbox"></a>

## Human decisions for team tasks

The team task inbox gathers durable questions, native-tool approvals and child-patch import approvals across one planner goal. Named members, physical task/Run IDs, stable wait IDs and exact saved requests are shown together. Each Run still owns its wait, input receipt and continuation. The inbox is a read projection and an application service over the existing runtime.

<a id="team-inbox--try-the-cli"></a>

### Try the CLI

This scripted sample needs no model server. Its editor requests approval to change `Draft` to `Reviewed` in a managed README. Its reviewer asks which review to perform. Both workers pause independently.

```powershell
$team = cargo run --locked --offline -- --db inbox.db team create --config-file examples/configs/team-inbox.json
$task = cargo run --locked --offline -- --db inbox.db team submit $team --goal "Coordinate a reviewed edit"
cargo run --locked --offline -- --db inbox.db worker run --until-idle --max-concurrent-activities 2
cargo run --locked --offline -- --db inbox.db team task status $team $task
cargo run --locked --offline -- --db inbox.db team task inbox $team $task
```

Status includes the goal tree, member-to-actor mapping, progress, saved results, errors, pause/cancellation state and usage. Visible usage counts unique Runs; shared budget scopes retain their exact persisted totals even when the graph is truncated. Inbox prints each full saved payload, including tool arguments, managed workspace and permissions, or the inspected child artifact/import preview. Prompts are JSON-escaped; payloads are pretty JSON without text clipping.

Use the exact IDs from the inbox to record one decision:

```powershell
cargo run --locked --offline -- --db inbox.db team task answer $team $task QUESTION_WAIT_ID --answer "Review durability"
cargo run --locked --offline -- --db inbox.db team task approve $team $task TOOL_WAIT_ID
# To refuse a tool or child import instead:
# cargo run --locked --offline -- --db inbox.db team task approve $team $task TOOL_WAIT_ID --deny
cargo run --locked --offline -- --db inbox.db worker run --until-idle --max-concurrent-activities 2
cargo run --locked --offline -- --db inbox.db team task status $team $task
```

Each command starts a new process and reads the same database. You can stop between any of these steps: saved waits, requests and accepted decisions survive restart. A reply schedules its continuation; run the worker again to execute it. The editor changes its own managed copy; the source and planner's separate copy retain their original bytes in this sample.

Global `--json` supplies structured status, inbox or response acknowledgements. For a longer answer, use `--answer-file FILE` instead of `--answer`. The file is UTF-8 text, not a JSON signal. Answers must contain nonblank text and fit within 65536 UTF-8 bytes. File bytes, including trailing newlines, are preserved and participate in duplicate detection. A workflow's own pinned context bounds still apply.

<a id="team-inbox--authority-and-durability"></a>

### Authority and durability

- A team task is a goal rooted at that team's saved planner actor, including top-level tasks submitted through the ordinary actor API. A root or owned descendant/follow-up handle resolves the same goal. Direct unrelated worker tasks are excluded.
- Team readiness, pinned actor bindings, task ownership and all view facts are read in one SQLite snapshot. Correlation IDs, task payloads and member names do not establish goal ownership.
- Responses resolve the exact wait ID through authoritative goal links in the same IMMEDIATE transaction as the Run mutation. They do not trust a prior inbox view or its display limits.
- The Run's existing input receipt, history event and continuation commit together. An identical retry returns `replayed: true` without rewriting the Run, even after completion/restart or cancellation. A conflicting reply is refused. Receipts created by ordinary `run signal`/`run approve` commands are recognized when their payloads match exactly.
- An answer only resolves a native question. An approval/denial only resolves a native tool or child-import approval. Unknown, stale and other-goal waits are refused. Unsupported waits can appear as non-actionable items; uncertain operations remain subject to `run reconcile`.
- Goal or task cancellation refuses new input. Run and goal pauses remain in force after recording a response; recording a decision does not resume execution. Existing attempt/model/token caps and tool/import contracts remain authoritative.

Status and inbox default to 128 graph nodes and depth 16. `--max-nodes` accepts 1..512; `--max-depth` accepts 0..128. A partial view has explicit `truncations` in JSON and `PARTIAL INBOX`/`PARTIAL VIEW` in text. An empty partial inbox does not establish that the goal has no pending decisions. Exact reply lookup is independent of these display limits. Items report `actionable`, `unavailable_reason` and `execution_held`; a paused goal may record replies while retaining its admission hold.

<a id="team-inbox--rust-api-and-boundaries"></a>

### Rust API and boundaries

```rust,ignore
use act0r::{application::TaskInspectionOptions, domain::HumanResponse};
let tasks = agents.teams()?.tasks()?;
let status = tasks.status(team_id, task_id, TaskInspectionOptions::default()).await?;
let inbox = tasks.inbox(team_id, task_id, TaskInspectionOptions::default()).await?;
let receipt = tasks.respond(team_id, task_id, wait_id,
    HumanResponse::Answer { answer: "Review durability".into() }).await?;
```

`domain::human` defines supported decision kinds and response validation. `TeamTaskService` assembles views and invokes Run behavior. `TeamTaskInboxPort` provides consistent reads and an atomic scoped mutation; SQLite implements it using existing team, ownership, Run and input-receipt records. `TeamService::with_task_inbox` installs the port; `local_app` wires it by default. CLI formatting remains an adapter. The inbox introduced no schema or saved actor/Run payload changes.

Use the [goal timeline](#team-events) to read or watch the committed events behind these waits and responses. The baseline includes their shared history index; cursor files reconnect a watcher after execution has progressed while it was disconnected.

```powershell
cargo test --locked --offline --test team_inbox
cargo run --locked --offline --example team_inbox
```

The automated demo pauses an editor and reviewer, reopens the database, verifies identical waits/requests, records answers and approvals through the inbox, and completes the goal in six model calls. Integration tests cover named members, exact arguments, partial views, goal/correlation boundaries, concurrent duplicates/conflicts, transaction rollback, response replay, restart after completion, cancellations/pauses, denials, planner follow-ups/imports, unsupported/blocked waits and the multi-process CLI flow.

<a id="team-events"></a>

## Durable goal events and live watching

`team task events` reads committed Run history across one native team's goal. `team task watch` follows the same stream and reconnects from a saved cursor. Planner decisions, activity admission/results, delegation, timers, human waits/replies and Run completion retain their original event IDs and per-Run sequences. Each item adds a persistent stream position, task ID, actor ID and optional team member name.

The timeline follows persisted forwarding, delegation and follow-up ownership, using the same authority as the [team inbox](#team-inbox). A descendant handle resolves the original goal. Correlation IDs, message senders and user-supplied payloads do not establish ownership. Other goals submitted to the same actors stay separate; a cursor for another goal, team or database stream is refused.

<a id="team-events--cli"></a>

### CLI

The same bounded pages and scoped cursors are also available through the [local HTTP API](api.md#http-api), with SSE page IDs and `Last-Event-ID` reconnection.

```powershell
cargo run --locked --offline -- --db .build/team.db --json team task events TEAM_ID TASK_ID --limit 100
cargo run --locked --offline -- --db .build/team.db --json team task events TEAM_ID TASK_ID --after CURSOR
cargo run --locked --offline -- --db .build/team.db --json team task watch TEAM_ID TASK_ID --cursor-file .build/goal.cursor
```

Run the worker in a separate terminal. Watching does not acquire the executor lease, admit work, answer questions or fire timers. It can coexist with the worker and other independent readers. Use Ctrl+C to stop; durable waits and future follow-ups mean the watcher stays open even when current execution is idle or finished.

`events --json` emits one JSON page. `watch --json` emits newline-delimited compact JSON pages: an initial page/checkpoint, then pages containing saved events. Text output shows identity, position, sequence, kind, timestamp and an escaped payload preview capped at 240 characters. JSON retains full event payloads. Output can include saved prompts, model inputs/results and workspace details; choose its destination accordingly.

The shared options are:

| Option | Default | Contract |
| --- | --- | --- |
| `--after CURSOR` | Beginning | Read events strictly after this cursor; cannot combine with a watch cursor file. |
| `--limit N` | 100 | 1–512 events per page. |
| `--max-bytes N` | 262144 | 1–16777216 stored-event/envelope bytes per page. |
| `watch --cursor-file PATH` | None | Load a saved cursor, or begin at the start if the file does not exist; save after output flush. The parent directory must exist. |
| `watch --poll-ms N` | 250 | 1–60000 milliseconds between caught-up reads; backlog pages drain immediately. |

An existing cursor file must contain a single versioned cursor (at most 256 UTF-8 bytes including whitespace). Malformed and wrong-scope files are refused without overwriting them. Each active cursor file has a companion `.lock` file; another watcher using it is refused. The lock is released when the process exits or is killed. Use separate files for independent consumers. Regular files are required; symlink/reparse cursor and lock files are refused.

<a id="team-events--pages-and-recovery"></a>

### Pages and recovery

A page contains `events`, `event_bytes`, `snapshot_position`, `next_cursor`, `has_more` and `oversized_event`, plus its team, canonical root and query. Continue with `next_cursor` while `has_more` is true. An empty caught-up page still returns a valid cursor. Positions are globally ordered within the database, so a single goal can have gaps from unrelated history; they are not event counts or timestamps.

Limits count UTF-8 bytes of each stored HistoryEvent JSON plus its compact timeline envelope. Page metadata and pretty-print whitespace are excluded. Noncompact JSON is counted conservatively. The adapter first loads at most `limit + 1` small headers, then bodies that fit. Goal ownership traversal still depends on the size of the saved graph; event page limits do not truncate that authority graph.

If an event fits by itself but not in the remaining page allowance, it appears on the next page. If one event exceeds `max_bytes`, the page reports its ID, position and `required_bytes`; the cursor stops before it. Increase the byte limit and retry. The watcher flushes/checkpoints any preceding events, then exits with an error so it cannot spin or silently drop that event. An event exceeding the maximum page allowance requires separate Run-history inspection or a future large-event transport.

The watch cursor file is replaced atomically after the page is written and stdout is flushed. A failed write/flush never advances the checkpoint. If the process dies after output but before saving the cursor, reconnecting may replay a page: delivery is at least once, and stable event IDs support deduplication. Flushing stdout does not acknowledge processing by a downstream consumer. A consumer needing its own processing acknowledgement should use the Rust page API and checkpoint after processing. This is not an exactly-once or physical power-loss delivery guarantee.

Quiet reads can advance the in-memory cursor over unrelated events without emitting a page or rewriting the file. Reconnecting from the last output checkpoint safely scans those gaps again. New child/follow-up Runs publish their ownership links and initial history together, at later committed positions.

The opaque version-1 cursor binds a database stream UUID, team, canonical goal and integer position. Parsing rejects unknown versions, malformed fields and out-of-range positions; reading also rejects cursors ahead of the committed stream. It is a checkpoint, not a signed authentication token. These APIs use the local database's goal authority and do not introduce network authentication.

<a id="team-events--rust-and-storage-boundaries"></a>

### Rust and storage boundaries

```rust,ignore
use act0r::domain::timeline::EventQuery;

let events = agents.teams()?.events()?;
let page = events.read(team_id, task_id, EventQuery::default()).await?;
// Process this page before persisting its next_cursor.
let next = events.read(team_id, task_id, EventQuery {
    after: Some(page.next_cursor),
    ..Default::default()
}).await?;
```

The domain defines `EventCursor` and bounded `EventQuery`. `TeamEventService` uses the application-owned `TeamTaskEventsPort` and returns `TeamEventPage`; custom hosts install it with `TeamService::with_task_events`. SQLite supplies one read snapshot for team/root validation, committed high-water position, owned event headers and bodies. The CLI owns polling, presentation, shutdown and local checkpoint files. Execution continues through the existing actor and Run services.

The baseline includes a saved stream identity and `history_positions` index. A history-insert trigger allocates an explicit AUTOINCREMENT position in the same transaction as history, Run changes and continuations. Rollback removes all of them together. SQLite serializes writers; explicit saved positions remain stable through reopen and VACUUM. Existing history, Run and actor JSON payloads are preserved. Every new event appends to the persisted order.

This first slice exposes Run history. A queued mailbox request with no Run, team setup state, and goal-level pause/cancellation receipts without a corresponding Run event do not generate synthetic timeline events. Use `team task status`, `team task inbox`, and `actor task` controls for those projections. Actor/Run waits remain durable; the watcher is a reconnectable reader of their saved progress. Distributed tracing, remote streaming and history retention/compaction are later work.

<a id="team-events--verification"></a>

### Verification

```powershell
cargo test --locked --offline --test team_events
cargo test --locked --offline --lib adapters::cli::team_events
cargo run --locked --offline --example team_events -- .build/team-events-demo/demo.db
```

The scripted demo runs two native workers concurrently, disconnects at their approval/question waits, reopens the database, completes execution and reconnects from its cursor. Paginated recovered history must equal a fresh full read, with three successful tasks and six model calls. The CLI test kills a watcher, restarts execution, then starts a new watcher with the same file and verifies all missed events. Other checks cover ownership isolation, follow-ups, simultaneous execution/reads, byte limits, rollback, migration, output failures, file locking and VACUUM.
