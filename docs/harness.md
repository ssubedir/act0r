# Using the harness

Start with the CLI, then use these sections for harness service/API contracts, project setup, saved sessions and verification. Dated browser trials below record historical validation from before the UI was removed.

- [Run from the CLI](#cli)
- [Chat with the harness](#chat)
- [Automatic project work](#automatic-project-work)
- [Continuous Auto conversations](#continuous-conversations)
- [General project onboarding](#project-onboarding)
- [Review and update project setup](#project-setup-review)
- [Continue from the current project](#project-refresh)
- [Detect Rust project settings](#project-detection)
- [Saved project profiles](#project-profiles)
- [Durable project instructions](#project-instructions)
- [Verify an applied project](#project-verification)
- [Fix a project verification failure](#verification-repair)
- [Saved session organization](#sessions)

<a id="cli"></a>

## Run from the CLI

Build and explore commands from the repository root:

```sh
cargo build --locked
cargo run --locked -- --help
cargo run --locked -- actor --help
cargo run --locked -- team --help
```

Chat defaults to `.act0r/chat.db`; other commands default to `act0r.db`. Use global `--db PATH` to select another database; missing parent directories are created. Global `--json` exposes structured results for scripts. Credentials come from the launching environment; set `OPENAI_API_KEY` for hosted compatible model targets. Loopback model servers can run without authentication. Chat takes model flags or environment defaults; actor/team model targets and tool policies live in JSON configuration. See [models](providers.md) and [MCP setup](mcp.md).

The built executable is `target/debug/act0r` on Unix and `target/debug/act0r.exe` on Windows. No frontend build or JavaScript launcher is required. Node is needed only for Node-based tools, fixtures and docs checks.

<a id="chat"></a>

### Chat with the harness

`act0r chat` runs a durable conversation directly through the harness services and hosts the existing worker. Give it an optional project folder and enter a task; it discovers the project's setup, works in an isolated copy, runs the selected verification and presents the saved patch. No team JSON or HTTP server is needed. Omit the folder for general agent conversations.

```sh
act0r chat --project ./my-project
act0r chat --project ./my-project "Fix the failing tests"
act0r chat --list
act0r chat --resume SESSION_ID
```

The session ID and an exact resume command, including the absolute database path, are printed when the request is saved. `--db PATH` selects the database, as with other CLI commands. The default `.act0r/chat.db` is excluded from project copies, so `--project .` can work in the current directory. Explicit database paths must be outside the project or beneath its excluded `.act0r/` or `.build/` directories. Use the printed `--db` path for lower-level actor/Run commands too. Resuming restores the canonical conversation, original model, project settings, questions and approvals, including discovery's coding continuation. Completed model/tool results are reused. The terminal displays the newest 32 conversation turns; all history remains durable.

For new conversations, `ACT0R_MODEL` and `ACT0R_MODEL_ENDPOINT` supply the same defaults as the API. Explicit `--model` and `--endpoint` override them. The endpoint is the full Chat Completions URL, not just its `/v1` base. `--action-format function-calling` selects native model functions; `--action-format json-schema` selects structured JSON. The tested NVIDIA Kimi default uses function calling automatically. Set `OPENAI_API_KEY` for hosted compatible endpoints; loopback models can run without a key. Model/project overrides are refused with `--resume`, because existing sessions retain their saved pins. Token and call budgets remain unlimited by default.

```sh
act0r chat --project ./my-project \
  --model YOUR_LOCAL_MODEL \
  --endpoint http://127.0.0.1:1234/v1/chat/completions \
  --action-format function-calling
```

Enter follow-up requests as ordinary text. When a question appears, the next submitted message answers its saved wait; approvals accept `yes` or `no`. Routine Auto edits, configured commands, tests and conflict-free worker imports run in the isolated copy. Applying to the original folder always uses the existing separate preview and approval workflow.

When stdin, stdout and stderr are terminals and `TERM` is not `dumb`, chat uses [Reedline](https://github.com/nushell/reedline) to edit one complete message. Enter adds a newline; Ctrl+S sends the draft. Alt+Enter also sends when your terminal forwards that shortcut; some terminal hosts reserve it, so Ctrl+S is the portable choice. Cursor movement, deletion and session-local input history are available. Multiline paste stays in the draft until you send it, and activity updates print above the draft. For example, a pasted code block containing a line such as `/retry` remains part of that message, rather than becoming a separate command.

Submitted messages and their input handoff are bounded. If a previous submission has not yet been consumed, chat keeps the next draft in the editor and asks you to send it again. Unsent drafts and the most recent 100 history entries stay in memory; they are not saved to another history file. Ctrl+C exits with admitted work saved and discards any unsent draft. Ctrl+D at an empty draft also exits. The terminal is restored on exit, errors and worker shutdown. The CLI keeps its ordinary line reader for pipes, redirected streams, `TERM=dumb` and `--json`; those modes retain one request or reply per input line and emit no editor controls.

Activity updates identify the planner or worker and show model requests, tool starts/completions, MCP calls, test outcomes, retries and worker assignment results. They come from committed history; a start confirms admission, and a completion confirms a saved result. Tool refusals and nonzero test exits remain visible as failures. Names and path previews are bounded and escaped; updates omit file contents, model prompts, tool output and full arguments.

Resuming prints the active goal's most recent 24 activity updates with a `Recent` prefix, then follows new activity. Reading the saved timeline uses bounded pages and does not rerun work or make model calls. Older details remain available through `team task events` or `run history`. Large backlogs drain across polls so terminal input remains responsive; an event beyond the timeline's maximum page size stops only the activity display and reports its identity.

| Command | Behavior |
| --- | --- |
| `/status` | Inspect the current turn and owned tasks |
| `/retry` | Retry an eligible failed model request once at its saved checkpoint |
| `/steer MESSAGE` | Save guidance for the active planner task's next fresh model request |
| `/patch` | Display its saved patch and completion assessment |
| `/apply` | Prepare an application preview, then ask for approval |
| `/pause` | Save an admission hold for the current goal |
| `/resume` | Release that goal's saved hold |
| `/stop` | Save cancellation for the current turn and its owned work |
| `/exit` | Leave the conversation and drain the worker |
| `/help` | List these commands |

Use `/steer` to correct work already underway instead of queuing another request:

```text
/steer Keep the public API unchanged. Only modify src/math.mjs.
```

The acknowledgement confirms the instruction was saved. Delivery status is shown automatically and restored after restart; `/status` includes the current task's steering history. `pending` means it is waiting for a fresh model request, `delivered` means it was included in that request, and `not_delivered` means the task ended before delivery. Delivery records admission of the model request, not proof that the model followed the instruction. An in-flight request or its exact retry keeps its saved input.

Guidance belongs to the active planner task. It does not create a follow-up, answer a pending question, release a pause or grant tools, permissions or budgets. Auto discovery and coding have separate tasks; their guidance stays scoped to the task that accepted it. A finished task refuses new guidance. Existing runtime limits allow 16 instructions per task, each containing 1..4096 UTF-8 bytes, within 16 KiB of serialized steering context. See [durable steering](recovery.md#turn-steering) for recovery and delivery rules.

After automatic model retries stop, chat displays the saved failure and recovery eligibility. Use `/retry` for an eligible transient provider failure or invalid/incomplete model response. Each command admits one additional model attempt in the same task and Run, using the exact saved request. Completed reads, edits, tests and worker results remain saved; a successful recovery prints the recovered answer. A failed recovery requires another explicit `/retry`. Provider retry deadlines and all existing budget charges still apply. `chat --resume SESSION_ID /retry` also works with piped input or after restarting the CLI.

`/retry` rechecks eligibility when entered and reports the reason when recovery is unavailable. Running work, questions, pauses, cancellation, unsafe tool outcomes, exhausted budgets, later work that consumed the failed history and archived sessions retain their existing guards. Configuration/authentication errors require correction and a new request; restart the CLI to reload changed environment settings. Refusals require a revised request. `/retry` does not answer questions, release pauses, grant permissions or start another conversation turn. See [failed turn recovery](recovery.md#turn-recovery) for the full contract.

Ctrl+C also drains the worker. Questions, approvals and deadlines survive exit; interrupted external effects retain their recovery policy and may require [explicit reconciliation](recovery.md#reconciliation). A blocked task reports its reason and Run ID for inspection with `run status`. Another worker cannot own the database simultaneously; `chat --list` is read-only and can run alongside one.

Input can come from stdin. After EOF, the CLI processes eligible work until it settles or needs a human decision, pause release or recovery; then it exits with the session saved. Supply a reply directly when resuming a pending question or approval:

```sh
act0r chat --resume SESSION_ID "Keep the public API unchanged"
act0r chat --resume SESSION_ID yes
```

Global `--json` emits one JSON object per line with `event` and `data` fields, including `session`, `status`, `question`, `approval`, `response`, `patch` and `saved`. `steering_saved` acknowledges durable guidance; `steering` reports its delivery state. Both include the saved entry, message identity and `status`. There is no prose on stdout in this mode. Send answers after the corresponding decision event; future ordinary requests use the existing durable turn queue. Invalid replies or steering produce `input_error` and leave pending work available. Input lines are bounded to 65,536 UTF-8 bytes.

Interactive drafts have the same 65,536-byte UTF-8 input limit, applied to the entire message including newlines. Domain limits for goals, steering and answers still apply. Piped EOF waits for the saved conversation response before exiting when the worker finishes between polls.

`activity` carries `event_id`, global timeline `position`, task/actor/Run/activity/attempt identities, `member`, timestamp `at`, saved event `kind`, display `status`, optional `tool`, concise `message` and `replayed`. Replayed entries retain their original event IDs; consumers can use these to recognize the recap across process restarts. Each committed event is displayed once per CLI process. `retrying` appears only after a new attempt is admitted; a failure alone does not promise another attempt. `activity_unavailable` reports an event too large for the timeline reader. Raw saved history remains available separately.

`recovery` reports the saved failed checkpoint, diagnostic, `retry_allowed`, `blocked_reason` and recovery receipts; `/status` includes the same view under `recovery` for a failed turn. `recovery_saved` acknowledges durable retry admission, including the exact decision identity and resumed activity ID. Admission can wait for a saved provider deadline; it does not mean the model request has started. A recovered turn emits a new `response` under its original task ID, so JSON consumers must allow an updated answer after recovery.

### Submit and inspect actor work

The [team CLI](teams.md#agent-teams--run-a-team) creates native planner/worker actors from JSON and submits goals. The included scripted example needs no key:

```powershell
$team = cargo run --locked -- team create --config-file examples/configs/agent-team.json
$task = cargo run --locked -- team submit $team --goal "Design a durable actor workflow"
cargo run --locked -- worker run --until-idle --max-concurrent-activities 2
cargo run --locked -- team task status $team $task
cargo run --locked -- team task inbox $team $task
```

In a POSIX shell:

```sh
team=$(cargo run --locked -- team create --config-file examples/configs/agent-team.json)
task=$(cargo run --locked -- team submit "$team" --goal "Design a durable actor workflow")
cargo run --locked -- worker run --until-idle --max-concurrent-activities 2
cargo run --locked -- team task status "$team" "$task"
cargo run --locked -- team task inbox "$team" "$task"
```

`worker run` without `--until-idle` keeps processing saved work. Ctrl+C drains in-flight activities; start the worker against the same database to continue. Timers, questions and approvals remain durable when no process is running. `team task answer` and `team task approve` target exact saved wait IDs; `team task watch` follows committed events. See [human decisions](teams.md#team-inbox).

<a id="coding"></a>

### Code in a project

Use `actor coding-worker --source PATH --config-file FILE` for a persistent coding actor, or a configured [coding team](teams.md#agent-teams--configuration) for planner delegation. Writable paths, model target and verifier are pinned in the configuration. Work happens in isolated managed copies. Inspect tasks and changes before [applying a patch](tools.md#patch-application) to the original project. Interrupted external effects require [reconciliation](recovery.md#reconciliation).

For the full automatic discovery, coding, verification and follow-up flow, use [`act0r chat --project PATH`](#chat). The Rust harness service and [local HTTP API](api.md#http-api--prompt-first-harness) expose the same workflows. `cargo run --locked --example auto_conversation` demonstrates recovery with a local fixture model.

```sh
cargo run --locked -- api serve --with-worker
```

The optional API listens on loopback at `127.0.0.1:8080` and exposes JSON/SSE under `/v1`. It serves no UI. Omit `--with-worker` when a separate worker already owns the database. Hosted API workers default to two activity slots; use `--max-concurrent-activities 1` to serialize activities.

<a id="automatic-project-work"></a>

## Automatic project work

Auto requests provide a folder, task and model target to the harness service. The agent inspects the project, selects its setup, edits an isolated copy and runs its checks. Set `discover_project: true` and `auto_execute: true` in an API request. The result includes the verified patch for review before application to the source.

The agent still asks for essential missing information. Pause, stop, saved questions and uncertain external effects retain their existing durable behavior. Commands execute on the API host in the managed project copy. Applying changes to the original folder remains a separate explicit decision.

<a id="automatic-project-work--execution"></a>

### Execution

The client sends `discover_project: true` and `auto_execute: true` with the folder, goal and model. The native planner inspects project files and produces its typed setup internally. The worker then uses the existing deterministic coding-start identity, saved discovery context, remaining budget and team/mailbox admission. The transition runs without a connected client or a client-generated approval.

The resulting planner and coder pin `auto_execute`. Known scoped native edits, tests and named commands schedule through the same durable activity contract. Inspected conflict-free worker patches can be imported into the managed copy without a human handoff. Original tool definitions, validation, recovery policies, immutable change artifacts and fresh parent test requirements remain in effect. No additional planner or execution engine is introduced.

All paths and commands come from the existing validated project setup. This change automates configuration and routine decisions; it does not add new tools. Analysis-only requests can finish during inspection. Missing verification information can use a normal saved question instead of a configuration wizard.

<a id="automatic-project-work--restart-and-failure"></a>

### Restart and failure

Successful discovery remains discoverable until its coding continuation commits. The worker reuses any prepared actors and copies after restart, and publishes the continuation, task and mailbox receipt through the existing atomic admission. Pause, cancellation and archive prevent new automatic admission, including a final transactional check after team preparation. Exact accepted retries retain their receipt and make no additional model calls.

An unrecoverable setup failure is saved once and displayed as Needs attention. It does not stop unrelated requests or repeatedly attempt the same preparation. Resolve the reported folder/configuration problem and start a new request. SQLite protects these pins and saved errors. Older sessions omit the flag and keep their original manual setup, approvals and immutable history.

<a id="automatic-project-work--interface"></a>

### Interface

Use [`act0r chat --project PATH`](#chat), the Rust harness service or `POST /v1/harness/sessions` for Auto requests. Saved human questions and answers remain available through the goal inbox and conversation projections. See [API routes](api.md#http-api--prompt-first-harness).

<a id="automatic-project-work--checks"></a>

### Checks

```powershell
cargo test --locked --offline --test http_api automatic_project
```

Local fixtures use real Node contracts with both structured JSON and native function calls. They exercise a saved human answer, interrupted continuation publication, restart, reused prepared actors, automatic named commands/edits/tests, explicit source application, same-actor follow-ups, pause/stop, persisted setup failure and automatic worker-patch import followed by fresh parent verification. No hosted model is required.

The isolated browser trial selected a folder and submitted one request without opening manual setup. Seven local model calls produced the repaired module, passing five real Node contracts and a syntax check. The original source remained unchanged for result review. Reload restored the same conversation, result and verified patch without additional calls; internal setup and activity logs stayed out of the main flow. Screenshot and state evidence are saved locally under `.build/auto-project-ui/b78b0e35-1c12-4fde-ac7e-39c0da88afad/`.

<a id="automatic-project-work--nvidia-acceptance-trial"></a>

### NVIDIA acceptance trial

On October 13, 2026, NVIDIA `moonshotai/kimi-k3` exercised the normal HTTP API and worker with native function calling, `discover_project: true` and `auto_execute: true`. The isolated project was a dependency-free JavaScript issue search API on Windows with Node 24.12.0. Its label matching and pagination bugs failed 10 of 12 tests before the trial.

Kimi discovered the setup, selected `node --test test/query.test.mjs`, and edited only `src/filter.mjs` and `src/query.mjs`. The repair requires every selected label to match, filters before pagination, and computes totals and next-page indicators from the full filtered collection. All 12 tests passed in the managed copy, after application to the generated source, and in a fresh follow-up verification. Tests, fixtures, documentation and package metadata remained unchanged.

The trial also verified:

- A separate process resumed after the first committed edit with identical saved actors, Runs, activities, history, usage and conversation.
- Runtime reopening preserved the pending application and source-verification approvals. The trial driver approved these generated-project operations through the ordinary durable responses after checking the verified two-file patch.
- The follow-up reused the same coding team and session. All five actor identities remained unchanged, a new test activity passed, and the follow-up made no edits.
- Completion reopening and exact request replay preserved the saved evidence. Repeating the completed trial in another process made no additional model calls.
- No setup submission or routine tool approval was needed. Runtime token and call allowances stayed unlimited; the trial itself had a separate 40-attempt guard.

The result used 18 admitted model attempts: 15 completed actions and three 90-second timeouts recovered by the existing retry policy. No response-format or runtime changes were required for this case.

Local evidence is retained under `.build/kimi-auto-trial/d3befb5b-d408-4931-adfa-a51064a1bb22/`. It includes the exact credential-free model requests, database, before/after verification artifacts, restart checkpoints, `acceptance-summary.json`, `evidence.json` and `report.json`. The generated fixture and temporary driver remain under `.build/kimi-auto-trial/`; the temporary Cargo example entry point was removed after validation.

<a id="continuous-conversations"></a>

## Continuous Auto conversations

New Auto requests manage setup and routine work without a configuration review. The execution scopes below remain internal, and the main conversation shows the request, questions and result. Manual controls are under details. See [automatic project work](#automatic-project-work).

The setup and refresh APIs can create reviewed fresh stages within the same conversation. Their durable links retain prior evidence while follow-ups route to the latest active stage. The conversation projection exposes discovery, coding, verification and terminal outcomes from committed execution evidence.

An explicit `coding_incomplete` outcome displays **Incomplete**.

Discovery and coding still own separate native actors, Runs, workspaces, permissions and histories. Accepting setup pins the original goal, model, remaining budget and discovery context in coding. The conversation link connects those execution scopes; it grants no capabilities and performs no model calls. See [project onboarding](#project-onboarding) for discovery and reviewed settings.

<a id="continuous-conversations--follow-ups-and-controls"></a>

### Follow-ups and controls

During discovery, answer a question or guide the current request. A setup proposal needs review before a new follow-up can be admitted. A completed analysis-only discovery can accept another request without coding.

After setup acceptance, follow-ups submitted to the original conversation reach its coding planner. They retain the existing editable copy, pinned settings and normal per-turn budgets. Queued follow-ups appear after current work; the earliest unfinished turn stays current while an approval, question or recovery is pending. Once it settles, the next queued turn becomes current. Exact message retries locate their original physical receipt, including after a stage transition or archive.

Controls, answers, permissions, worker operations and patch application continue to use exact physical session/team/task/Run identities. Selecting earlier inspection does not apply its controls to coding. A follow-up always targets current execution, even while earlier work is being inspected. Pending client requests retain their original journaled body and identity across reload.

Titles and archival belong to the visible conversation. Archive is unavailable while any linked coding work, patch application, summary or setup recovery remains unfinished. Archiving prevents new follow-ups through either the original or coding session and new patch applications. Original admitted receipts remain recoverable. Applying a verified patch to the original folder still requires its separate review and approval.

Failed original-project verification can also start a fresh **Repair** coding stage in this same conversation. Its own actors use the checked project snapshot and saved failure feedback; follow-ups reach the latest repair stage. Earlier coding copies and controls retain their identities. See [verification repair](#verification-repair).

For linked sessions, metadata endpoints return the canonical conversation's scope and revision. A restore also releases older independently archived coding stages.

<a id="continuous-conversations--api-and-storage"></a>

### API and storage

`GET /v1/harness/conversations` returns the grouped sidebar, with the usual archived, limit and exclusive `after` cursor query. Filtering happens before the page limit; coding continuations do not consume visible entries. Its cursor stream is distinct from the physical session catalog. The entry's `session_id` is the original conversation; `execution_session_id`, `team_id` and `task_id` locate execution.

`GET /v1/harness/sessions/{session}/conversation` accepts either the original or a linked coding session. It returns the canonical `session_id`, requested scope, current stage, `active` and `selected` physical receipts and chronological turn previews. Optional `task` selects historical execution after ownership validation. Each preview includes its own session/team/task identities, discovery or coding phase, whether the initial coding request repeats discovery's goal, and saved clarifications. Clients can omit that repeated prompt while retaining both stages' results.

Pages contain at most 32 execution turns, with existing 8 KiB request and 16 KiB response previews. Each turn can include its last 16 human question/answer pairs; text fits 2048 escaped JSON bytes per excerpt and marks truncation and omissions. Full evidence remains in the Run. `next_before` is a serialized `a1:CONVERSATION:SOURCE_SEQUENCE:PHASE:TURN_SEQUENCE` cursor, passed as `before`. The composite boundary preserves ordering when coding is admitted and rejects another conversation's cursor. `active` and `selected` remain available when their turns are outside the current preview page.

Existing physical conversation and task endpoints remain available. `POST /v1/harness/sessions/{session}/turns` resolves the current linked coding scope and admits the message atomically, returning its original physical harness receipt. The usual owned task endpoints still handle all execution mutations.

SQLite schema **56** adds `harness_project_continuations` with foreign keys to the original and coding sessions/turns. The coding session, first turn, actor task, mailbox message and continuation link commit in one transaction. A failed link write rolls admission back; retry reuses the already prepared team and immutable settings. No new queue, agent policy or workflow engine is introduced.

Schema **57** adds stable continuation positions and durable verification repair preparations. Existing cursor positions stay fixed; additional repair phases range through 64 and are labelled `repair`. One canonical conversation links all stages, and exact follow-up replay still finds its original physical receipt.

Upgrade backfills only deterministic, previously accepted coding starts that match their saved successful setup source, goal, project and model. It preserves actors, Runs, workspaces and context pins, and makes no model calls. A previously prepared but unadmitted start resumes using its original configuration. The latest coding stage's older archive state and title are carried into the visible conversation when needed; changed metadata increments its revision and invalidates stale organization decisions. Actor and execution records stay fixed.

<a id="continuous-conversations--verification"></a>

### Verification

```powershell
cargo test --locked --offline --test http_api continuous_conversation
cargo test --locked --offline --test sessions
cargo test --locked --offline --lib conversation
```

Regression validation passed 350 Rust tests across the library, all HTTP workflows, harness/session admission, teams, storage, native contracts and setup, plus 89 UI tests. All-target Clippy, formatting, syntax and diff checks passed. Tests exercise a real approved verifier, durable questions and verification over restart, queued follow-ups, cross-scope refusals, exact retry, archive guards, atomic rollback, multiple history pages.

Browser verification used an isolated generated text project and local mock model. It kept one sidebar entry and the original conversation URL through discovery, question/answer, reviewed setup, edits, real Python verification and completion. The API restarted at a saved test approval, retaining the stage and earlier answer. An old discovery URL restored coding. Historical inspection offered its own controls, and a follow-up reached the same coding actors. Source files stayed unchanged; no NVIDIA calls were made. Evidence and screenshots are in `.build/continuous-browser/a3bd70ff-bc61-478e-a8a5-e3235ad6af1a/`.

The [repeatable Auto trial](validation.md#auto-conversation-demo) subsequently completed this full flow with NVIDIA Kimi in 13 calls on a generated JavaScript project: actual Node verification, approved application, four restart checks and a verified follow-up, all in one visible conversation. Terminal replay made no further calls or application. The example also runs with a local provider for regression checks.

<a id="project-onboarding"></a>

## General project onboarding

New Auto requests continue through setup automatically. The manual review flow below documents saved/manual discovery requests and advanced API use. See [automatic project work](#automatic-project-work) for the primary flow.

Submit a discovery request with an absolute project folder and goal. The native planner inspects a read-only managed copy using list/read/search tools, root AGENTS.md and the project's own documentation, manifests and scripts. This path requires no Cargo manifest and has no language or framework allowlist. It uses your selected model and supports structured JSON or native function calling.

An analysis-only request can finish with an answer. When edits are needed, the planner proposes writable paths, one verification command and optional named commands. It must record a successful project inspection before publishing a proposal. Invalid setup receives saved feedback for correction. Missing information uses an ordinary durable question; discovery, questions, answers, model usage and recorded reads survive restart. Discovery has no edit, process or delegation capability. A proposal alone neither grants permission nor runs its commands.

For manual discovery, review the saved proposal and send the accepted paths, commands, timeouts and worker count through the setup API below. The server starts one stable coding session for that discovery task, carrying the original goal, model, remaining budget, saved answers, setup rationale and successful inspections. Repeated identical reviews return the original receipt; changed settings conflict after acceptance. Discovery usage remains charged. Project profiles can retain reviewed settings for later requests.

The service exposes [one continuous Auto conversation](#continuous-conversations) through inspection, setup review, coding and verification. Internally, a read-only discovery session is followed by a separate coding execution session with durable links. Existing actors, older session settings and pending requests retain their original configuration. The coding session gets a fresh copy of the selected source folder when admitted; discovery is evidence about the earlier copy, not a lock on your project's contents. Accepted setup is not proof that commands exist or pass. The coding planner can inspect again and receives actual verification feedback. Existing command/edit approvals, isolated worker assignments, child patch import, fresh final verification and separate application to the original folder still apply.

Command arrays can name project-appropriate executables and scripts; the runtime does not insert a shell. Discovery receives the host OS. On Windows, configure an explicit shell invocation when a `.cmd`/`.bat` command requires it. Commands need to be available on the API host. If a project has no known verification method, the planner should ask instead of inventing one. Native file tools operate on permitted text files; this does not add arbitrary binary editing or a process sandbox. See [project commands](tools.md#project-commands) for process and recovery semantics.

A request without coding or discovery stays read-only. Explicit `coding` configuration skips discovery when the setup is already known. The optional [Rust detector](#project-detection) is advisory; Auto requires no Cargo manifest.

<a id="project-onboarding--api-and-architecture"></a>

### API and architecture

Start through the existing `POST /v1/harness/sessions`, adding `"discover_project": true`, a `workspace` and the usual goal/model/budget. Omit `coding`; combining pre-granted coding settings with discovery is refused. Omission or false retains the existing direct session contract. Conversation views expose `discover_project`.

The discovery action is `propose_project_setup` with a typed `proposal`:

```json
{
  "summary": "README documents the verification procedure",
  "writable_paths": ["app.py"],
  "test_command": {"argv": ["python", "-m", "pytest"], "timeout_ms": 60000},
  "commands": []
}
```

Named entries in `commands` contain `name`, `argv` and `timeout_ms`. A bounded explanation, distinct names, portable scopes and the existing command limits are validated in the domain. These values are suggestions until reviewed.

`GET /v1/harness/sessions/{session}/turns/{task}/project-setup` verifies ownership and returns the immutable proposal, acceptance availability, stable coding session identity and any accepted receipt/configuration. Reads start no model calls. `POST` to that same route accepts exactly `{"coding": HarnessCoding}`. Folder, goal, model, budgets and destination identity cannot be supplied or overridden. It returns the ordinary harness start receipt with HTTP 202. Invalid setup or unfinished/exhausted discovery is 400; unknown fields are 422; changed accepted review is 409. Clients should persist the exact review identity and body before sending it.

The domain owns proposals, validation, deterministic continuation identity and remaining-budget calculation. The existing planner policy gains an opt-in discovery schema and recorded-inspection guard. The application reads owned Run evidence through existing actor/execution/harness ports and assembles the reviewed session through the existing durable team/mailbox boundaries. HTTP and JavaScript remain adapters. SQLite protects the checkpoint/config contract and persists links between discovery and coding, using the existing queue and planner.

<a id="project-onboarding--discovery-context-handoff"></a>

### Discovery context handoff

The server derives `discovery_context` from the owned successful discovery Run, its input receipts, wait/input history and successful list/read/search activities. This includes answers no longer present in the planner's recent observation window. The review request still accepts only `coding`; callers cannot replace the source context, goal, target or budget through that route. Preview and acceptance require no additional model calls or reads of the original project.

The snapshot records discovery session/task/Run IDs, the successful history boundary, the earlier managed workspace digest, proposal digest and a SHA-256 of its evidence. It contains at most 16 KiB of serialized JSON, up to 16 question/answer pairs and 16 inspection findings. Each text excerpt fits 2048 escaped JSON bytes. Short answers are copied verbatim. Longer text retains its original byte count and digest with an explicit `truncated` flag. Overflow drops older file findings before older answers and records omission counts. Full evidence remains in the source Run.

The same snapshot is pinned before coding team registration in the planner, helper and each coder. It is fixed prompt material, included in context byte accounting and usage reservation, and survives task retries, child assignments and process restart. Accepted reviews return the existing receipt. Partially prepared starts reuse their original settings and snapshot; older starts without this field keep their original configuration and prompts. No extra summary model, planner, queue or storage table is introduced.

Discovery context is historical task data. The coding copy may differ from the earlier discovery copy, and excerpts may be incomplete. Models receive instructions to use saved answers while inspecting current files when needed. Reviewed coding settings remain the authority for paths and commands; snapshot content cannot waive edit/command approvals or satisfy fresh verification.

<a id="project-onboarding--verification"></a>

### Verification

```powershell
cargo test --locked --offline --test project_setup
cargo test --locked --offline --test http_api project_setup
cargo test --locked --offline --lib discovery_context
```

Local provider fixtures cover native function calls and structured JSON, custom projects without a Cargo manifest, JavaScript/Python/Makefile layouts, premature proposal refusal, durable questions/reads over restart, edited review, concurrent duplicates, unchanged original model/goal, remaining budgets and accepted receipt recovery. An actual approved custom verification subprocess validates an edited text project and publishes a verified patch while leaving the source unchanged. Handoff tests evict a clarification from discovery's observation window, restart before review and again before coding, then inspect actual coordinator and coder provider requests to prove the exact saved answer and snapshot survive. They also cover immutable reviewed commands, unlimited budgets, snapshot bounds/digests, Unicode/escaped excerpts and recovery of partially prepared coding starts. The ignored child verifier test is invoked by its parent fixture. No hosted model or external framework is needed for these checks.

Discovery-context validation passed 325 Rust tests covering the library, all HTTP workflows, proposals, team admission, storage contracts and native model pins, plus 88 UI tests. Final handoff fixtures also reject cross-Run history and changed answer evidence. All-target Clippy, formatting, JavaScript syntax and diff checks passed. These checks used local providers; no new NVIDIA calls were made.

On 13 October 2026, browser verification started a generated JavaScript project using only Auto, its folder and a goal. Four local fixture model calls inspected the project and proposed its Node test. The proposal survived backend restart; review retained an edited 45-second timeout and two coding workers, and saved a reusable profile. Four further calls completed an approved edit, a real passing Node contract test and a verified one-file patch. Original source stayed unchanged. Reopening discovery showed its accepted coding-session link without more calls. The isolated evidence is in `.build/project-onboarding-ui/34f402fe-63e3-40e9-a144-58f5862c44d7/`.

Regression validation passed 923 Rust tests across all targets and 88 UI tests, with all-target Clippy, Rust formatting, JavaScript syntax and diff checks passing. The initial all-target run stopped at a future-schema fixture that still treated 54 as unsupported. After updating that fixture to 55, the corrected target and all remaining targets/examples passed; preceding targets retained their passing results. Eighty declared child/manual fixtures were ignored by the ordinary runner; parent recovery tests invoke their required child fixtures explicitly. The browser and provider fixtures in that validation were local. The later [integrated Auto trial](validation.md#auto-conversation-demo) completed discovery, reviewed coding, application and a verified follow-up with NVIDIA Kimi in 13 calls, including saved-boundary and separate-process recovery.

<a id="project-setup-review"></a>

## Review and update project setup

New Auto requests manage initial setup automatically. The advanced setup API allows a settled coding conversation to review different writable paths, verifier and named commands for its next request. Approval creates a fresh isolated copy with the accepted settings. Earlier actors, workspaces, approvals, results and usage retain their original settings; unapplied edits are not merged. See [automatic project work](#automatic-project-work) for initial setup.

<a id="project-setup-review--what-can-change"></a>

### What can change

Only writable paths, the fixed verifier (executable/argument array and timeout) and named commands are editable. Models, model action format, worker count, budgets, actor policies and other saved configuration remain pinned. Unchanged settings are a no-op refusal; use normal project refresh to keep the same setup. Paths and commands use the existing language-neutral coding validation, including portable relative paths, bounded catalogs and explicit executable arguments.

The fresh editable planner and coding workers receive the approved paths and commands. Read-only helpers retain their capabilities. Adding a nonempty named command catalog enables the existing `run_command` tool for editable actors; removing that catalog removes its dispatch capability. Execution still uses the existing native tool approval and recovery contracts. Previous test evidence does not verify the new snapshot or command. Old permission grants keep their original physical session/task bindings and are not transferred to new actors.

Historical discovery and previous-turn excerpts remain bounded saved evidence. The fresh request explicitly makes the reviewed new settings authoritative over earlier setup evidence. No additional planner or model execution engine is added. Default call/token budgets remain unlimited; saved explicit limits remain pinned.

<a id="project-setup-review--durable-preparation"></a>

### Durable preparation

The existing [project refresh](#project-refresh) boundaries apply: the latest coding turn and all earlier conversation work, questions, applications, verifications and recovery must be settled, and the conversation must be active. Source and old-workspace identities and complete manifest digests bind inspection to this physical turn. Either copy changing after review refuses admission.

Before preparing actors, SQLite saves the exact approved settings, prompt, request ID, review digest, derived assembly and stage position. An identical setup-update request resumes that preparation after API restart with the saved Current/Proposed settings and request. Exact retries reuse the same actors, copies and first task. Altered settings under an existing ID conflict. Settings and model calls cannot slip into the original actors.

Preparation holds follow-ups, archive and cache cleanup. Cancellation of setup-update preparation durably releases that hold and prevents new task admission even when setup was running. Cancelled IDs cannot be revived. Prepared copies and receipts remain available; inspect again with a new ID for another attempt. After admission, use ordinary Pause/Stop controls. Exact terminal replay returns the saved receipt after later work, archive or project changes.

Copies are checked under workspace ownership and must match the reviewed source. Previously used prepared actors are refused. Stage linking, task/mailbox admission and the receipt commit together. Filesystem work stays outside SQLite transactions. External writers do not participate in these locks; repeated scans refuse observed drift, while later edits remain protected by ordinary patch application checks.

<a id="project-setup-review--api-and-architecture"></a>

### API and architecture

All routes use the physical source scope: `/v1/harness/sessions/S/turns/T/project-setup-review`

| Method and suffix | Contract |
| --- | --- |
| `GET` | Current `coding` settings, eligibility, saved preparation/request, error and receipt. No filesystem scan. |
| `GET /inspection` | Source/copy differences and review digest, plus current `coding`. No model calls or preparation hold. |
| `POST` | `{"request_id":"UUID","review_digest":"SHA256","prompt":"next request","coding":{"writable_paths":["src/","lib/"],"test_command":{"argv":["node","tests.mjs"],"timeout_ms":10000},"project_commands":{}}}` → `202` physical receipt and canonical `conversation_id`. |
| `POST /cancellation` | `{"request_id":"UUID"}` → saved cancellation acknowledgement. |

The current `coder_count` must be retained when present; omission defaults to one. Folder/model/budget/worker overrides and unknown fields are refused. Normal `/project-refresh` POST bodies still accept only request ID, digest and prompt.

Pure `project_setup` rules derive reviewed settings and revise only editable actors. `project_refresh` owns the exact optional coding approval in its existing intent; application services coordinate team setup and inspection through existing ports. SQLite schema **59** protects this additional checkpoint field. It adds no table or policy version, preserves older pending refreshes without overrides and keeps existing conversation cursor positions. An updated stage projects as `setup_update`; physical identities and control scopes remain unchanged.

<a id="project-setup-review--checks"></a>

### Checks

```powershell
cargo test --locked --offline --lib project_setup::review_tests
cargo test --locked --offline --test http_api project_setup_update
cargo test --locked --offline --test http_api project_refresh
```

Local generated JavaScript fixtures update an initially unpermitted folder, create a new module, run an approved new named command and pass ten real Node contracts with the new verifier. The patch is reviewed, applied and verified in the original folder; follow-ups retain the new setup. Tests cover saved setup across restart, concurrent exact admission, unchanged earlier actors and read-only helpers, scope and override refusal, invalid/no-op/count changes, cancellation, source drift, terminal replay. No hosted model is needed for these checks.

The local browser trial also completed this flow with ten scripted model calls: reviewed paths, verifier and named command, invalidated approval after a form edit, reloaded a pending test approval, applied the patch and passed all ten contracts in the original folder. Its unrelated `settings.json` retained `offset: 5`. After stopping the fixture worker and API, a fresh API process restored the same conversation, verified application and new setup (including the 12000 ms verifier timeout). Evidence and a screenshot are in the ignored local directory `.build/project-setup-ui/ff644de7-f068-436d-b1ed-329274a3cd72/`.

<a id="project-refresh"></a>

## Continue from the current project

To change writable paths, the verifier or named commands, use the separate [Review project setup](#project-setup-review) flow. Ordinary refresh continues to preserve those settings.

Ordinary coding follow-ups reuse their managed workspace. After editing the original project or switching branches, inspect current source differences with the project-refresh API below. It reports up to 200 added, modified and deleted paths with an omitted count. Approving the reviewed next request creates a fresh isolated copy with the same models, scopes, verifier, commands, workers and budgets. Earlier work remains inspectable; unapplied workspace edits are not merged.

The model receives the next request plus bounded excerpts of the previous request and saved result, source scope and digest. Excerpts retain full-text hashes and explicit truncation. Initial discovery context remains pinned when present. Visible history is retained; every earlier model input is not replayed. Edits, tests, application and source verification retain their existing approvals. Earlier passing tests do not verify this fresh snapshot.

<a id="project-refresh--snapshot-and-durability-boundaries"></a>

### Snapshot and durability boundaries

Review hashes both included manifests twice and captures their directory identities without model calls or commands. A scope-bound review digest covers both folders, identities and complete manifest digests. Either copy changing after review refuses admission. Snapshot exclusions and bounds match [project verification](#project-verification). No Git or Cargo layout is required.

The latest coding turn must be settled. Queued turns, active actors, questions, summaries, applications, verifications and recovery must settle first. Restore archived conversations before starting new work; discovery retains its setup flow.

Before setup, SQLite saves the exact request, reviewed snapshot, derived team specification, bounded context and stage position. Repeating the saved refresh request reuses those records after reload, lost acknowledgement or backend restart. Source and previous-workspace ownership guards cover setup and admission. All prepared copies are hashed under workspace locks and must match the reviewed source. Used actors cannot qualify as fresh prepared actors. The first task, mailbox delivery, physical session, conversation link and receipt commit together.

Preparation holds follow-ups, archive and cache cleanup. Cancelling refresh preparation releases that hold and prevents task admission, including during concurrent setup. Prepared copies and receipts remain saved. Cancelled IDs cannot be revived; inspect again with a new ID. Once admitted, use ordinary Pause/Stop. Exact completed retries return the same receipt after later turns, archive or source edits. Changed prompts under the same ID and cross-scope reuse conflict.

External editors do not participate in harness ownership locks. Repeated scans refuse observed drift but cannot exclude an actively racing external writer. Later source edits remain protected by patch application conflicts. Refresh does not merge conflicting edits or broaden permissions.

<a id="project-refresh--api-and-architecture"></a>

### API and architecture

Routes use the physical source scope: `/v1/harness/sessions/S/turns/T/project-refresh`

| Method and suffix | Contract |
| --- | --- |
| `GET` | Eligibility, reason, saved preparation/request, diagnostic and receipt. No filesystem scan. |
| `GET /inspection` | Explicit scan returning scoped review digest, source/copy digests and bounded file differences. No preparation hold. |
| `POST` | `{"request_id":"UUID","review_digest":"SHA256","prompt":"next request"}` → `202` physical receipt and canonical `conversation_id`. |
| `POST /cancellation` | `{"request_id":"UUID"}` → saved cancellation acknowledgement. |

Unknown fields, nil IDs, folder/command/model overrides, stale reviews and historical-source starts are refused. Review alone is repeatable and read-only; durability begins with the saved request before actor setup.

The pure `project_refresh` domain owns review fingerprints, bounded differences and context. Harness services coordinate existing team setup and inspection ports. The local adapter supplies snapshots and ownership guards; SQLite schema **58** stores preparations and extends continuation kinds without moving old cursors. Repair and refresh share stage guards and preparation exclusions. Actor/Run boundaries and the existing scheduler remain intact.

<a id="project-refresh--checks"></a>

### Checks

```powershell
cargo test --locked --offline --test http_api project_refresh
cargo test --locked --offline --test http_api verification_repair
cargo test --locked --offline --test http_api continuous_conversation
```

Local fixtures use generated JavaScript and real Node contracts: changed settings, added/deleted files, fresh actors, setup across restart, concurrent exact admission, unchanged earlier actors, reviewed application, passing source verification and latest-stage follow-ups. Other checks cover stale reviews, scope and override refusal, cancellation, pending-work guards and terminal replay. No hosted model is required.

The browser trial completed an Auto refresh from changed settings through edit and test approvals, reviewed patch application and **Applied · verified**, exit 0 with five passing Node contracts. Reload retained the exact test approval. A separate API host reopened the same database without a worker, restoring one sidebar conversation, the human request and refreshed stage. Model calls stayed at nine, and the edited settings remained intact. Both isolated hosts and the test browser tab were closed. Evidence and the restored review screenshot are in ignored `.build/project-refresh-ui/0289b476-7ccc-475b-95fc-97a1fc063243/`.

<a id="project-detection"></a>

## Detect Rust project settings

This optional Rust shortcut remains available alongside the general [Auto folder-and-goal flow](#project-onboarding). Auto does not require this detector or a Cargo manifest.

Call the project-detection API below with an absolute folder path. Individual Cargo packages receive advisory writable paths and test, format, lint and build commands. Review the values before using them in a profile or session.

Detection saves no profile, starts no work and executes no commands. Unsupported projects can use Auto discovery or explicit settings. Later manifest changes do not rewrite saved profiles or existing session configuration.

<a id="project-detection--suggestions-and-scope"></a>

### Suggestions and scope

The reader parses only the selected folder's root `Cargo.toml` with a TOML parser. It inspects metadata for `src/`, `Cargo.lock` and declared `[lib].path` / `[[bin]].path` files, without reading source contents. These conventions follow Cargo's [manifest](https://doc.rust-lang.org/cargo/reference/manifest.html) and [target](https://doc.rust-lang.org/cargo/reference/cargo-targets.html) formats. Manifest metadata cannot supply executable commands or permissions.

- An existing `src/` directory is suggested. Declared library/binary files outside it are suggested individually when they exist. Test, example and benchmark directories are not automatically granted as broad scopes.
- With a regular `Cargo.lock`, tests use `cargo test --locked --offline`; lint and build use the same flags with `clippy` / `build`.
- Without a lockfile, `--locked` is omitted and the suggested scope includes `Cargo.lock` because Cargo may create it. This is shown in the review notes.
- Formatting uses `cargo fmt`. Its timeout is 60 seconds; tests, lint and build use 120 seconds. These commands are editable before use. Offline execution needs cached dependencies, and formatting/linting need their Rust components.

This first version supports individual packages, including custom source paths. Virtual workspaces, explicit workspace members and packages declaring an external workspace return a manual-setup message. Parent workspace configuration, dependency resolution, tool installation and compilation are not checked. Missing declared sources are reported for review. This is an advisory setup preview, not Cargo's full manifest validation or a guarantee that the suggested commands will pass.

The root manifest is a regular UTF-8 file of at most 64 KiB. Source paths reuse portable-path and scope limits (128 entries / 16 KiB); root escapes, excluded private/cache paths, symlinks and Windows reparse points are refused. No recursive scan, Cargo subprocess, build script or dependency download occurs during detection. The returned manifest SHA-256 identifies the bytes read; it does not lock project contents or become an execution permission.

<a id="project-detection--http-api-and-architecture"></a>

### HTTP API and architecture

`POST /v1/harness/project-detection` is a read-only inspection with a strict body:

```json
{"workspace":"C:/projects/my-crate"}
```

The result contains `workspace`, `kind` (`rust_package`, `rust_workspace` or `unrecognized`), `manifest_sha256`, `suggested` (a `ProjectSettings` object or null) and `notes`. A missing manifest or unsupported layout returns 200 with no suggested setup. Invalid paths/manifests return 400; malformed or unknown request fields return 422. Detection has no durable decision identity because it changes no stored state. Reviewed profile changes use the existing durable profile endpoint.

The domain builds bounded suggestions from `RustProjectFacts`. The application's `ProjectDetectionService` validates requests and reads through `ProjectDetectionPort`. The local adapter owns filesystem access and TOML parsing on a blocking task. HTTP and the JavaScript dialog adapt that service; profile storage and actor/Run configuration reuse existing contracts. There is no SQLite migration or new agent policy.

<a id="project-detection--verification"></a>

### Verification

```powershell
cargo test --locked --offline --test project_detection
cargo test --locked --offline --test http_api project_detection
```

Fixtures cover standard/custom layouts, metadata command rejection, no file/process effects, absent lockfiles, invalid/oversized manifests, escaping paths and linked directories (including Windows junctions). HTTP checks save an edited suggestion, create a session, change the manifest and reopen the database while retaining the reviewed profile/session configuration. Request validation rejects mismatched folders and malformed suggestions and preserves editable snapshots and worker count.

The focused checks passed: five filesystem detection tests, two detection HTTP tests, two existing profile HTTP regressions and all 87 UI tests. Rust formatting, all-target Clippy with warnings denied, JavaScript syntax and diff checks passed.

On 13 October 2026, the browser detected a generated Cargo project, filled the form, retained two coders, saved edited file scopes/tests as a profile and restored that profile after backend restart. Changing folders cleared stale suggestions; a folder without a manifest retained manual settings. No Runs or model calls were created and all source files remained unchanged. Evidence and screenshots are in `.build/project-detection-ui/0bbcc25e-4651-413c-b887-612054da93a6/`.

<a id="project-profiles"></a>

## Saved project profiles

Auto mode can [discover a project from its goal](#project-onboarding). Profiles store reusable explicit read-only or coding settings: a name, absolute folder, writable paths, verifier, named commands and one or two coding workers. Models and budgets remain separate. Omitted `coding.coder_count` selects one worker; `2` enables [parallel coding](coordination.md#harness-coding-delegation--optional-parallel-coding).

Use the profile API below to list, create, revise or remove settings. Profiles persist in SQLite. A session request pins a configuration snapshot; editing or removing its profile does not change existing actors, pending requests or source files. Select current profile values explicitly when starting another request.

Saving/selecting never reads a directory, copies files, starts work, runs a process, or grants tool permission. Folder existence is checked when session setup copies it. Existing edit, command, test and patch-application approvals remain in force. Test permission belongs to one session and cannot be granted through a profile. Verified completion still requires fresh passing tests.

<a id="project-profiles--http-api"></a>

### HTTP API

`GET /v1/harness/project-profiles` returns `{items, capacity}` with active profiles ordered by name and identity, up to 100 active profiles per database. `GET /v1/harness/project-profiles/{id}` returns `{id, revision, deleted, settings}`.

`POST /v1/harness/project-profiles/{id}` uses a caller-chosen nonzero UUID:

```json
{
  "request_id": "22222222-2222-4222-8222-222222222222",
  "expected_revision": 0,
  "action": {
    "kind": "save",
    "settings": {
      "name": "My Rust project",
      "workspace": "C:/projects/example",
      "coding": {
        "writable_paths": ["src/"],
        "test_command": {"argv": ["cargo", "test", "--locked"], "timeout_ms": 60000},
        "project_commands": {"format": {"argv": ["cargo", "fmt"], "timeout_ms": 60000}}
      }
    }
  }
}
```

Omit `coding` for read-only mode. Configuration follows existing [coding](#coding) and [named-command](tools.md#project-commands) contracts. Names are trimmed, nonblank, 1–256 UTF-8 bytes, without control characters. Active names are unique under SQLite's ASCII case-insensitive comparison. Settings fit within 65536 serialized UTF-8 JSON bytes. Unknown fields are refused.

Revision zero creates; updates use the GET revision. Removal uses `{request_id, expected_revision, action: {kind: "remove"}}` on the same route. Success returns `{profile, replayed}`. Stale revision, duplicate name, full catalog, changed request identity or removed identity returns 409 `project_profile_conflict`. Missing/removed GET returns 404 `project_profile_not_found`. Invalid settings return 400; malformed/unknown fields return 422. Mutation revisions are 0–9007199254740990.

Each mutation atomically saves configuration and a decision receipt. Exact identity retries return the original receipt even after later edits or removal; they never roll configuration back. Use GET for current state. Removed identities remain tombstones and cannot be reused. New profiles use new identities. Receipts and tombstones remain saved for acknowledgement recovery. Concurrent edits share the revision guard and serialized transaction; one wins and the other reloads. Clients should save exact profile decisions before sending them and retain unknown outcomes for an identical retry after reconnection.

<a id="project-profiles--architecture-and-checks"></a>

### Architecture and checks

The domain owns bounded settings and revision rules. `ProjectProfileService` uses `ProjectProfileStorePort`; SQLite implements atomic storage and HTTP adapt the service. The baseline includes two independent tables without rewriting actor, Run or session pins. No new worker or agent policy is involved.

```powershell
cargo test --locked --offline --test project_profiles
cargo test --locked --offline --test http_api project_profiles
```

Local fixtures cover revision races, receipt replay, rollback, capacity, invalid configuration, session pins after profile edits, removal/restart and exact request snapshots.

<a id="project-instructions"></a>

## Durable project instructions

New harness sessions with a project folder enable root `AGENTS.md` guidance for their native planner and helper. Add conventions to this UTF-8 file before starting a session, for example:

```markdown
# Project conventions

Use DDD with ports and adapters. Keep filesystem and model I/O in adapters.
Prefer small changes and use the configured test command to verify them.
```

The runtime reads only `AGENTS.md` at the root of the actor's **managed project copy**, when a physical task launches. It does not discover instructions in parent folders or nested directories. Project copies are created at session setup; editing the original folder later does not refresh an existing copy. Start a new session to pick up original-folder changes. An approved edit to the managed copy takes effect for subsequent tasks, never for an already launched task.

Each launch saves one `project_instructions` snapshot in its Run input and native configuration. Loaded snapshots contain exact text and its SHA-256 hash. Missing files save `{"status":"missing"}`. Refused files save a reason without any partial text: `too_large`, `invalid_utf8`, `unsafe_path`, `not_a_file`, or `unreadable`. Empty UTF-8 files load as empty text; line endings and BOM bytes are preserved. The content limit is **16 KiB of UTF-8 bytes**, including a BOM if present. The local adapter validates managed-workspace membership and refuses observed links/reparse points and nonregular files, following the same trusted-host storage assumptions as native file reads. This is not a filesystem sandbox.

Mailbox launch commits the snapshot with the Run and first activity, before any model call. Queued-turn validation does not read guidance. A turn queued behind earlier work selects its snapshot only when it actually starts. Delegated helpers select their own snapshot from their task's project copy, including a prepared workspace handoff when configured. Restart, pause/resume, steering, model retries, and failed-model recovery use the saved snapshot. They do not reread the file. Duplicate session/turn admission does not replace saved launch contents.

Guidance is fixed model context outside the observations trimmed by native context selection. Conversation summaries cannot replace it. Its serialized text and instructions count toward the normal model input-byte and token allowances. If mandatory context does not fit, normal context-limit handling applies; guidance is never silently cut to fit. A missing/refused file does not block work, and its status remains inspectable.

Project text supplies conventions, subordinate to the current user goal and steering. It cannot change the response schema, grant tools, change writable paths or the test command, bypass approvals, waive verified-completion tests, or alter execution budgets. These remain host-pinned runtime contracts. Actor creation and task messages cannot supply `project_instructions` contents. Native tool/planner actor definitions can opt in with `load_project_instructions: true` and a source workspace; the application installs the `ProjectInstructionsPort` reader.

The harness task inspection API includes `project_instructions` on each launched native node.

DDD boundaries: the domain defines and validates bounded snapshots and prompt semantics; actor scheduling selects them through `ProjectInstructionsPort`; the local workspace adapter performs the read; existing SQLite Run persistence supplies the durable transaction. No new table, migration, approval type, or tool is needed.

<a id="project-verification"></a>

## Verify an applied project

After an approved patch reports **Applied**, create a project verification through the application API below. The worker captures the original project's file hashes and saves an approval wait. Review the exact folder, command and snapshot, then approve or reject the exact saved verification wait. The command is the coding session's reviewed verifier, including its original argv and timeout. Neither a client nor a model can substitute another command or target for this operation.

This is a separate durable Run using the existing engine. It makes no model calls and does not change the completed coding task's result. Applying a patch still checks affected-file hashes without executing tests; project verification is the explicit next operation. It runs in the original folder, so the reviewed command can produce files and other ordinary process effects there.

<a id="project-verification--saved-outcomes"></a>

### Saved outcomes

| Saved status | Evidence |
| --- | --- |
| **Applied · verified** | The real command exited with code 0 and the project matched the approved snapshot both before and after execution. |
| **Applied · verification failed** | The real command returned a nonzero result against that same snapshot. Its output and exit status remain saved. |
| **Applied · project changed** | The project drifted before execution, changed during the command, or could not be captured afterward. This never asserts verification, even if a process returned 0. |
| **Applied · verification rejected** | The saved approval was declined; no verification process was started. |
| **Applied · verification needs recovery** | An admitted process has no certain saved outcome and requires an explicit recovery decision. |

The result includes the checked time, command, bounded stdout/stderr, truncation, elapsed time and observed snapshots. A process that changes tracked source cannot produce a verified result. Drift detected before process admission runs no command. Preparation failures remain visible as failed Runs rather than fabricated reports. A workflow Run can succeed while its report says `failed`, `changed` or `rejected`: inspect `report.status` to determine whether the project was verified.

For a settled `failed` report, the repair API can start a fresh isolated repair in this conversation with saved process feedback and the same reviewed settings. It checks that the original project still matches, retains earlier work and requires the usual edit/test/application approvals. See [verification repair](#verification-repair).

A saved result describes that point in time. Later edits do not rewrite historical evidence, and reopening a completed verification does not rescan or rerun tests. Create another verification to check the current project. There can be one unfinished verification and at most 20 saved verifications per application.

<a id="project-verification--snapshot-and-process-boundaries"></a>

### Snapshot and process boundaries

Preparation pins the applied target's directory identity and confirms all affected files still have their applied hashes. It records a sorted SHA-256 file manifest and scans twice to refuse observed changes during capture. Execution rechecks the same directory and full manifest under the ownership lock shared with patch applications, then captures the project again after the command.

Snapshots use the existing workspace exclusions: `.git`, `.act0r`, `.act0r-tools`, `.build`, `target`, `node_modules`, `.aws`, `.env` and `.env.*` at every level. They admit at most 10,000 entries and 64 MiB of included file content, with a 4 MiB serialized snapshot bound. Receipts retain hashes and metadata rather than another copy of project source. Links/reparse points, ambiguous names and special files are refused. Generated outputs outside those exclusions count as project changes.

The manifest is content evidence, not a hermetic dependency or environment proof. External editors do not participate in the harness lock. Observed drift is refused; the lock cannot prevent an actively racing external writer.

The existing process runner executes argv directly, strips environment variables ending in `_API_KEY`, bounds output and enforces the reviewed timeout with process-tree cleanup. It retains its existing Cargo output-directory handling when the reviewed executable is Cargo; other executables remain general. There is no implicit shell or new command permission.

<a id="project-verification--restart-and-recovery"></a>

### Restart and recovery

Preparation publishes a synced receipt before SQLite completion. Restarts and lost acknowledgements retain the exact snapshot, Run and wait ID. Approval is saved before the worker executes it, so the API and worker can restart between those steps without another decision.

Execution uses `RequiresReconciliation`. Interruption, timeout or failure to save a certain outcome blocks the Run instead of automatically repeating a command. Verification recovery inspection exposes the exact saved command, activity/attempt and current inspection. Confirm the previous worker and any test process have stopped, then submit inspection notes with a retry or fail decision. Failure also requires a reason. The API accepts no invented result or arbitrary passing output.

Retry runs the same approved command only while the original snapshot still matches. A completed process report published before a lost SQLite completion is acknowledged from its receipt without rerunning the process, even if the project has changed afterward. Exact recovery decisions replay their original receipt; an earlier decision cannot resolve a later attempt. Pauses and execution-attempt limits remain enforced.

Pending and blocked verifications prevent session archive and cleanup, including the coding stage of an Auto conversation. Terminal reads and exact start retries remain available after archive; restore the conversation to create a new run.

<a id="project-verification--http-and-rust-boundaries"></a>

### HTTP and Rust boundaries

Use the physical coding session `S`, turn `T`, applied patch Run `A`, and a client-generated verification UUID `V`. All routes below are under `/v1/harness/sessions/S/turns/T/applications/A/verifications`:

| Method and suffix | Body / result |
| --- | --- |
| `GET` | Latest 20 compact identities, Run statuses and outcomes; `truncated` is explicit. |
| `POST` | `{"verification_id":"V"}` → `202 {"run_id":"V"}`. |
| `GET /V` | Frozen input/snapshot, approval wait, saved result/error and recovery receipts. |
| `POST /V/responses` | `{"wait_id":"UUID","approved":true}`; use `false` to reject. |
| `POST /V/reconciliation` | Exact activity/attempt, retry/fail decision, inspection notes and `"operation_stopped":true`. |

Unknown body fields are refused. Admission atomically derives the target, applied hashes and command from owned saved records. Exact concurrent or restarted starts reuse one Run; cross-session/task/application access is refused. Requests cannot override argv, folder, snapshot or timeout. Recovery uses the existing `ToolRecoveryRequest` contract and engine transaction.

`domain::project_verification` owns the pure workflow and evidence validation. `ProjectVerificationService` and the harness own admission, inspection and exact decisions. `ProjectVerificationPort` and `ProjectVerificationInspectionPort` keep filesystem/process operations outside those services; `LocalProjectVerification` implements both. SQLite stores existing Runs and receipts. Bootstrap installs `project.verify` v1 and its prepare/run handlers without another actor version or database migration. Filesystem receipts live under `.act0r/project-verifications/` beside the database.

<a id="project-verification--checks"></a>

### Checks

```powershell
cargo test --locked --offline --lib project_verification
cargo test --locked --offline --test http_api project_verification
cargo test --locked --offline --test auto_conversation_demo
```

Coverage includes exact concurrent admission and scope refusal, archive guards, approval across restart, real passing/nonzero commands, drift before execution, tracked changes during execution, rejection, credential stripping, timeout, stopped confirmation and lost SQLite completion recovered without a second process invocation. The [Auto trial](validation.md#auto-conversation-demo) uses the public verification routes and can exit at `--stop-after project-verification-approval`.

A local browser check saved approval with no worker, restarted with a worker and restored **Applied · verified**, exit 0 and `5 contracts passed`, retaining seven model calls. Completing its follow-up retained the normal ten-call trial total and all five restart checks. Evidence is under ignored `.build/project-verification-ui/e73fc33a-2384-414d-9241-401074501515/`. The browser's screenshot service was unavailable; `browser-verified-state.txt` retains the observed accessible result. No hosted model calls were made for this feature's validation.

<a id="verification-repair"></a>

## Fix a project verification failure

When project verification records a failed result, use the repair API below. The harness starts a new coding stage in the same visible conversation. It uses a fresh isolated copy of the original folder, the saved failure, and the source session's reviewed model, writable paths, test command, named commands, worker count and budget settings. Follow-ups then reach this new coding stage.

The repair receives the failing exit status, stdout/stderr excerpts, original request, command, checked time, verification/application identities and project digest. Excerpts preserve complete text when it fits; larger text retains a full-text SHA-256 hash and explicit truncation. The complete process output stays in the original verification. Output is evidence and grants no tool permission. Edits, tests, patch application and original-project verification retain their existing approval steps. Starting repair performs setup/admission; its model calls execute through the durable worker.

<a id="verification-repair--fresh-project-inputs"></a>

### Fresh project inputs

A repair can start only from a settled verification whose actual report is `failed`, against stable inputs. Passing, rejected, changed-input and uncertain outcomes cannot manufacture failure feedback for a repair. The source must be the conversation's latest settled turn, with earlier work, applications, verifications and summaries settled. An archived conversation must be restored first.

The original folder's identity and full included manifest must still match the failed verification. The check runs before and after actor setup. Every prepared team copy must have the same immutable snapshot digest before mailbox admission. Their live files are hashed again under workspace ownership locks held through admission. Prepared actors that already received other work are refused. This catches drift while copying and prevents reuse of a stale session copy. There is no refresh or overwrite of existing actors: earlier workspace files, history, approvals, results and charges remain inspectable.

If the project changed, no repair task is admitted. Cancel any saved preparation and verify the current project again. If an affected patch file has changed too, the original application/verification hash guards still refuse it; resolve that application state or start a new reviewed project request. This first repair slice does not merge conflicting external edits or grant additional write paths. Normal snapshot exclusions and bounds apply; see [project verification](#project-verification).

Once a fresh isolated copy is admitted, later external edits are preserved by the ordinary patch-application conflict checks. They do not rewrite the saved repair input or authorize a different command.

<a id="verification-repair--durable-preparation-and-retry"></a>

### Durable preparation and retry

Before preparing actors, SQLite saves the client request ID, exact derived team specification, feedback prompt, source scope, checked digest and conversation stage position. Team setup reuses its existing stable IDs and snapshot/copy receipts. A lost acknowledgement or API restart can continue that same preparation. The request, first task, mailbox admission, conversation link and repair receipt commit together. Duplicate clicks create one repair.

One saved repair request belongs to each verification. An identical replay returns the same receipt after completion, archive or later project changes, with no new task or model admission. Another request ID under that verification conflicts. The saved receipt identifies the admitted repair stage.

An unadmitted preparation holds new follow-ups, archive and cache cleanup. If preparation cannot continue, a preparation cancellation durably releases that hold. Cancellation is checked in the admission transaction, including when setup was running concurrently. It creates no repair task and changes no project files. Setup receipts and any prepared copies remain available for inspection. Cancelled request IDs cannot be resurrected; use a new verification before another repair. Once admitted, use the repair request's ordinary Pause/Stop controls.

The new stage uses the same configured per-turn allowance; old usage is retained on the old Runs. Call/token defaults remain unlimited. Repair does not automatically retry model calls or accept uncertain process results. Existing worker recovery, durable questions and approvals retain their exact scopes.

<a id="verification-repair--conversation-and-api"></a>

### Conversation and API

Old URLs still locate the canonical conversation. Saved turns label the new work **Repair · turn N**, with its own physical session/team/task identities. Selecting earlier work keeps controls bound to that historical scope. Further repairs form ordered stages; the conversation permits at most 64 coding continuations. Existing `a1` cursor positions remain unchanged, while later repair phases extend through
64. Historical cursors exclude later repairs when reading older pages.

All routes are under the physical source scope:

`/v1/harness/sessions/S/turns/T/applications/A/verifications/V/repair`

| Method and suffix | Body / result |
| --- | --- |
| `GET` | Eligibility/reason, saved request ID, preparing/cancelled state, setup diagnostic and optional admitted receipt. |
| `POST` | `{"request_id":"UUID"}` → `202` with physical `session_id`, `team_id`, `task_id`, `sequence` and canonical `conversation_id`. |
| `POST /cancellation` | The same `{"request_id":"UUID"}` → a saved cancellation acknowledgement. |

Unknown fields, nil request IDs and caller-supplied prompts, commands, folders or model settings are refused. Ownership is checked against the saved session, application and verification. Eligibility reads committed records; execution freshness is checked again during the POST. Cross-scope access is refused. No filesystem or model work runs in a SQLite transaction.

`domain::verification_repair` constructs bounded feedback and the unchanged reviewed configuration. Harness application services coordinate verification inspection, existing team setup and the `HarnessStorePort` admission boundary. SQLite schema **57** adds durable preparation receipts and stable stage positions to conversation links. Restart preserves actor/Run records, accepted coding links and cursor positions. No agent policy version or new execution engine is introduced.

<a id="verification-repair--validation"></a>

### Validation

```powershell
cargo test --locked --offline --test http_api verification_repair
cargo test --locked --offline --test http_api continuous_conversation
cargo test --locked --offline --test teams --test sessions --test storage
```

The generated JavaScript fixture first passes five real Node contracts in its managed copy. A subsequent user edit to a protected settings file makes the applied original fail. Repair preserves that settings edit in a fresh copy, receives actual assertion feedback, changes only its reviewed module, and passes all five contracts both in the copy and after reviewed application. Follow-up uses the new actors; exact replay adds no calls. Other checks cover saved setup across restart, concurrent admission, wrong-scope/override refusal, drift during copying, cancellation before publication, pending-work guards, successive repair stages and stable historical paging. All providers in these checks are local.

The browser trial used the same generated fixture, clicked through repair, edit/test approvals, application and original-project verification, then reopened its database in a separate API host. One sidebar entry retained both stages, **Repair · turn 1** restored with a concise request heading, and the saved result showed **Applied · verified**, exit 0 and `5 contracts passed`. The settings edit remained intact. Model calls stayed at seven across that restart. Evidence is under ignored `.build/verification-repair-ui/61bb651a-ea56-4bc4-b69d-4c65e0ea6930/`. Screenshot capture was unavailable; `browser-restored-state.txt` retains the observed result. Both isolated browser hosts were stopped after verification.

<a id="sessions"></a>

## Saved session organization

Session listing supports active/archived filters and bounded cursor pagination. Active means unarchived, including completed work. Metadata updates save titles, archive or restore a session. Titles support up to 256 UTF-8 bytes, are trimmed and contain no control characters. They do not replace the original request or change pinned model, tool, permission or actor configuration.

<a id="sessions--http-and-native-rust"></a>

### HTTP and native Rust

`GET /v1/harness/sessions` accepts `archived` (default `false`), `limit` (1–50, default 50) and `after`. The response keeps `items` and `truncated`, and adds `next_after`. Each item includes `title`, `archived` and `metadata_revision`. Pages follow immutable catalog creation order, descending. Appending a turn or renaming a session does not reorder it. Cursors bind to the database identity, visibility filter, creation watermark and exclusive position; they survive restart and VACUUM. New sessions are excluded from an existing page traversal. Archive/restore changes the visibility filter live; refresh the latest page to see those changes. The API refuses unknown fields, invalid limits and cursors from another database/filter or ahead of the catalog.

`GET /v1/harness/sessions/S/metadata` returns `session_id`, `title`, `archived`, `revision` and `archive_blocked_reason`. This also works for interrupted actor setup. `POST` to that same route accepts one of these strict bodies:

```json
{"expected_revision":0,"kind":"rename","title":"Project repair"}
{"expected_revision":1,"kind":"archive"}
{"expected_revision":2,"kind":"restore"}
```

Each accepted decision advances metadata revision once, including a no-op, and saves its exact request. The latest identical retry returns `replayed: true`. A delayed request after another decision or automatic restoration returns `409 session_conflict`; inspect the current state and make a new decision. Archive eligibility is rechecked in the write transaction. Clients should save the exact session, title, operation and revision before sending. Retry that same body after an unknown acknowledgement; do not substitute a newer revision. The response includes the metadata view and `replayed`.

Native Rust hosts use `teams.harness()?.sessions_page(query)`, `session_metadata(id)` and `update_session(id, request)` through the existing application-owned harness port. Domain metadata rules remain independent of SQLite and HTTP. The baseline includes the session catalog, bounded metadata and latest receipt, with admission triggers.

<a id="sessions--verification"></a>

### Verification

`cargo test --locked --offline --test sessions --test http_api --test harness --test storage --test storage_contract` covers title bounds and strict requests, restart receipts, delayed decisions, archive/admission races, pending applications, uncertain Runs, reminders, advanced automatic restoration, retained source/evidence. Pagination checks cross-database/filter/future cursors, concurrent creation, follow-up ordering, restart and VACUUM.
