# Workspace tools and effects

Prefer MCP for inspection. Native effects retain their existing approvals, journals and reconciliation contracts.

- [Native tools for durable actors](#native-tools)
- [Add application-owned native tools](#custom-tools)
- [Durable tool error feedback](#tool-feedback)
- [Portable workspace tools for native actors](#workspace-tools)
- [Durable native workspace edits](#workspace-edits)
- [Durable native file creation](#workspace-creation)
- [Bounded workspace search for native actors](#workspace-search)
- [Durable native test execution](#workspace-tests)
- [Saved changes for native actor tasks](#workspace-changes)
- [Apply a reviewed task to a project](#patch-application)
- [Verified completion for native actors](#verified-completion)
- [Durable named project commands](#project-commands)

<a id="native-tools"></a>

## Native tools for durable actors

The `tool` v1 agent uses the existing answering policy with an explicitly allowed native tool catalog. It can request one listed tool, finish, or ask for human input, using one durable Run per actor task. Model-only `answer` v1 actors retain their registrations, behavior, and prompts. The current [planner v3](planner.md#planner-tools) also accepts optional native tools and shares their execution contract.

Native workspace capabilities include [inspection](#workspace-tools), [approved edits](#workspace-edits), and [fixed test execution](#workspace-tests). `run_tests` pins its command at actor creation and saves failed tests as feedback for a repair.

<a id="native-tools--try-it-locally"></a>

### Try it locally

No model server or API key is needed:

```powershell
cargo run --locked --example native_tools -- .build/native-tools-demo/demo.db
cargo test --locked --test native_tools
```

The example calls `sum` with 19 and 23, saves the result, closes the runtime, and reopens the database. It verifies the exact saved next model request, completes the task, and reports one tool attempt. Process-kill tests cover saved intent, saved result, approval waits, and an effect whose result was never committed.

Create and run a tool actor through the CLI:

```powershell
cargo build --locked
$db = '.\native-tools.db'
.\target\debug\act0r.exe --db $db actor tool list
$actor = (& .\target\debug\act0r.exe --db $db actor agent create tool --config-file .\examples\configs\tool-agent-scripted.json).Trim()
@{ goal = 'Add 19 and 23 using the sum tool.' } | ConvertTo-Json | Set-Content .\tool-goal.json
$task = (& .\target\debug\act0r.exe --db $db actor send $actor --input-file .\tool-goal.json).Trim()
.\target\debug\act0r.exe --db $db worker run --until-idle
.\target\debug\act0r.exe --db $db actor task inspect $task
```

`actor tool list` returns JSON containing installed versioned contracts. Local bootstrap provides `echo` (string `text`), `sum` (signed 64-bit integers `left` and `right`), and the [workspace inspection tools](#workspace-tools) `list_files` and `read_file`. These are repeat-safe and require no approval. It also installs [replace_text](#workspace-edits), which requires pinned writable paths, durable approval, and reconciliation of interrupted writes. Workspace tools require a runtime-pinned source copy. Sum overflow and out-of-range arguments produce non-retryable known failures.

The full catalog also includes [run_tests](#workspace-tests) for approved fixed process execution and [list_results/read_result](planner.md#result-recall) for bounded historical retrieval within the current Run. Recall is repeat-safe, requires no workspace or approval, and returns expected query refusals as saved feedback.

[create_file](#workspace-creation) creates a new UTF-8 file under pinned writable paths and durable approval. Its parent must exist; existing targets are refused. Interrupted publication requires explicit reconciliation and a validated journal retry.

[search_text](#workspace-search) finds literal matching lines through the read-only workspace port. It returns bounded paths, line numbers, and snippets with explicit scan/output limits. Saved searches survive restart and can be recalled without repeating them.

<a id="native-tools--configuration-and-pinning"></a>

### Configuration and pinning

The workspace catalog also includes [`read_files` v1](#workspace-tools--durable-batched-reads), which returns several bounded file reads in one saved activity. Fresh harness actors receive it automatically; explicitly configured actors must allow it.

Tool actors use answering configuration plus a versioned allowlist:

```json
"tools": [{ "name": "sum", "version": 1 }]
```

The adapter resolves this list once at actor creation and saves the exact contracts in `tool_definitions`. Callers cannot supply these runtime pins during creation. Each task receives that catalog. A submitted task can replace the goal; it cannot add tools or change contracts. An allowlist supports at most 32 distinct names. Installing a tool does not grant it to every actor.

The model sees only allowed tools, descriptions, and argument schemas. New `tool` actors pin `model_contract_version: 4` and return a flat action such as `{"kind":"sum","left":19,"right":23}` inside the usual `action` envelope. The model adapter uses the saved catalog to normalize it into internal `{"kind":"call_tool","tool":"sum","arguments":{"left":19,"right":23}}`. The runtime then validates arguments and scope before scheduling a tool activity. Tool-enabled [planners](planner.md#planner-tools) use the same flat contract. Responses and Chat Completions use the existing structured JSON adapters; provider automatic tool dispatch remains disabled. See [contract trials and NVIDIA evidence](validation.md#tool-dispatch-probe--pinned-native-contract-trials).

Native contracts reserve action kinds `finish`, `ask_human` and `finish_incomplete`, and argument name `kind`. Model contract 4 is the only supported native format. Restart and later tasks use the saved tool catalog.

Contracts support flat objects with string, integer, number, boolean and bounded string-array parameters. `ToolValueType::StringArray { max_items }` accepts a nonempty list of strings with an explicit limit of 1–32 items; `read_files` pins eight. Required fields, optional nullable fields, and unknown-field rejection are explicit. Other nested objects/arrays and arbitrary JSON Schema keywords remain unsupported. Input/output limits measure serialized UTF-8 JSON bytes; each must be positive and at most 1 MiB. Strict model schemas require all property keys, using null for optional parameters.

If an allowed tool is missing or its declared contract differs on restart, its Run blocks before new model/tool admission. Queued mailbox turns also block without rejecting tasks, creating a Run, or replacing the pinned catalog. Restore the exact contract and use `actor resume AGENT_ID` for a blocked mailbox and `run resume RUN_ID` for a blocked Run; pins, requests, usage, and results remain intact. Versions identify compatibility; they do not hash executable code. Applications must supply the same registry to their workflow adapter and executor.

<a id="native-tools--durability-controls-and-limits"></a>

### Durability, controls, and limits

A model decision commits before its `native.tool` activity executes: v1 for unscoped tools, v2 for scoped tools without feedback, v3 for [feedback-capable tools](#tool-feedback), and v4 for [tools requiring writable paths](#workspace-edits). Arguments, definition, retry/recovery policies, scoped permissions, result, and next model request use the existing transactional journal. The activity ID is the stable idempotency key across retries; each execution gets a distinct attempt ID. Tool implementations receive both IDs and the Run ID.

Saved results are reused after restart. An effect that succeeds before its result commits remains uncertain. `SafeToRepeat` permits another attempt; `RequiresReconciliation` blocks until an operator accepts a verified result, authorizes retry, or fails it through the existing reconciliation API. Lifecycle controls cannot bypass uncertainty. Cancellation does not undo external effects.

A definition with `requires_approval: true` creates a persisted `native_tool_approval` wait before tool scheduling. Use `run approve RUN_ID WAIT_ID`, or add `--deny`. Identical signals are idempotent; conflicting or stale signals are refused. Approval can be saved during a goal pause; execution waits for the relevant holds to be released. Denial becomes a saved observation for the next model decision.

Tool attempts count against the Run's total attempt allowance. Model calls/tokens retain per-Run and inherited goal/delegation allowances. Tools issuing their own model requests need additional usage instrumentation; this tool port does not report model tokens. Existing worker concurrency, timeouts, actor FIFO/capacity, supervision, memory, goal controls, and reconciliation apply. Admitted results can commit while further admission is paused.

Explicit known failures use the pinned retry policy and eventually fail the Run. An adapter `Error` signals possible uncertainty. Oversized successful outputs produce a non-retryable `tool_output_limit` failure without putting the oversized value into history. Complete observations remain in saved context; if the next model request exceeds its byte limit, the Run records `context_limit` and retains the tool result.

A pinned `reports_errors: true` contract can instead return `NativeToolResult::Rejected(ToolError)` for an expected tool-level problem. The registry saves bounded `ok:false` feedback and the agent can adjust its next action. Existing execution failures, retries, and uncertain-effect reconciliation keep their behavior. Local `list_files`/`read_file` v2 opt in; their v1 contracts remain installed for existing pins. See [tool feedback](#tool-feedback).

Tool-enabled answering shares the answering implementation and declares model-and-tool checkpoints. No schema migration or additional scheduler is required. Only trusted native Rust implementations are installed by the host application; tool declarations are not an execution sandbox.

<a id="native-tools--rust-api-and-ddd-boundaries"></a>

### Rust API and DDD boundaries

| Layer | Responsibility |
| --- | --- |
| `domain::tools` | Serializable contracts, requests, outcomes, and pure validation |
| `domain::answer` | Shared policy, optional catalog, approval waits, observations, and tool intents |
| `application::ports::NativeTool` | One asynchronous operation with stable execution identities |
| `application::ToolRegistry` | Validate registrations, resolve pins, dispatch calls, enforce byte bounds |
| `adapters::agents::ToolAgentAdapter` | Pin actor allowlists and supply the tool-capable answering workflow |
| Native adapters and bootstrap | Typed implementations and composition with the existing engine |

Implement `NativeTool::definition` and `execute`, then construct `ToolRegistry::new(Vec<Arc<dyn NativeTool>>)`. Install `ToolAgentAdapter::new(registry.clone())` in AgentRegistry and connect the same registry with `AgentExecutor::with_native_tools`. Expose discovery through `AgentService::with_tool_registry`. Workflows remain synchronous and perform no I/O; external calls execute after intent persistence.

For an application-owned tool on the default host, use the validated `ToolDefinition::builder(name, version, recovery)` and `bootstrap::local_app_with_tools(database, tools)`. The hook retains built-ins and shares the extended registry across actor adapters, execution, and discovery; actors still need explicit allowlists. See [custom tool authoring and the copyable example](#custom-tools).

Native actors with a managed workspace can opt into [saved task changes](#workspace-changes) using `"change_review": true`. Baseline/final capture and test fingerprints use durable activities; `actor task changes TASK_ID --output review.patch` exports the committed patch and its inspection reports whether saved tests match the captured content.

Tests cover allowlists, forged configuration, primitive/byte validation, shared allowances, context failure, approvals and duplicate signals, contract restoration, retry deadlines and execution identities, transaction rollback, local model HTTP contracts, memory, CLI, and process kills. Live-provider validation of the new tool actions remains separate from the local HTTP fixtures.

<a id="custom-tools"></a>

## Add application-owned native tools

A Rust host can define, install, and enable a custom tool using public APIs. `ToolDefinition::builder` validates its versioned contract; `local_app_with_tools` installs it alongside the built-ins and shares one registry across actor adapters, execution, and discovery. No runtime source changes are needed for the tool implementation.

<a id="custom-tools--run-the-example"></a>

### Run the example

```powershell
cargo run --locked --example custom_tools
cargo test --locked --test tool_extensions
```

The [word-count implementation](../examples/custom_tools/word_count.rs) lives in application code. The [host example](../examples/custom_tools.rs) enables it alongside built-in `sum`, saves the word count, closes the runtime, and registers a fresh implementation when reopening the database. It verifies the exact saved next request and usage, finishes using `sum`, and reports one custom execution, zero custom executions after restart, and one saved tool attempt. No model server or credentials are needed. The counter is demo instrumentation; persisted progress belongs to the harness.

<a id="custom-tools--1-define-a-contract"></a>

### 1. Define a contract

Construct and retain the definition when constructing your tool:

```rust
use act0r::domain::{RecoveryPolicy, tools::{ToolDefinition, ToolValueType}};

let definition = ToolDefinition::builder("word_count", 1, RecoveryPolicy::SafeToRepeat)
    .description("Count whitespace-separated words in the supplied text.")
    .required_parameter("text", ToolValueType::String, "Text to count.")
    .byte_limits(4096, 256)
    .build()?;
```

The recovery policy is mandatory at construction. `SafeToRepeat` is appropriate for this pure calculation. An external effect needs repeat safety, an idempotency scheme using the stable activity ID, or `RequiresReconciliation`. A durable SQLite result does not make an external operation exactly once.

The builder produces the existing `ToolDefinition`, preserving its serialization and validation rules. Defaults are 4 KiB input, 8 KiB output, the existing retry policy (three attempts, 100 ms initial delay, 5 s maximum delay), and false for all approval, scope, and feedback flags. A nonempty description is required. `build()` rejects invalid identifiers/versions, duplicate parameter declarations, more than 32 parameters, invalid byte limits/retries, and inconsistent workspace requirements.

| Builder method | Purpose |
| --- | --- |
| `required_parameter(name, type, description)` | Required argument |
| `optional_parameter(name, type, description)` | Argument that may be omitted or null |
| `byte_limits(input, output)` | Serialized UTF-8 JSON limits, each 1 byte–1 MiB |
| `retry_policy(policy)` | Known-failure retry attempts and delays |
| `requires_approval(bool)` | Durable approval before execution |
| `requires_workspace(bool)` | Runtime-pinned managed workspace |
| `requires_writable_paths(bool)` | Runtime-pinned writable paths; also declare workspace |
| `requires_test_command(bool)` | Host-pinned fixed command; also declare workspace |
| `reports_errors(bool)` | Allow expected refusals as saved model feedback |

Parameter types include string, integer, number, boolean and `ToolValueType::StringArray { max_items }`. String arrays must be nonempty and set an explicit maximum of 1–32 items. Strict model schemas require optional keys with nullable values; ordinary validated calls may omit them. Other nested objects and arrays remain unsupported. The builder does not automatically enable permissions or change recovery policy when setting other flags.

<a id="custom-tools--2-implement-one-operation"></a>

### 2. Implement one operation

Implement `application::ports::NativeTool`:

```rust
fn definition(&self) -> ToolDefinition {
    self.definition.clone()
}

fn execute(&self, call: NativeToolCall) -> PortFuture<'_, NativeToolResult> {
    Box::pin(async move {
        // Decode call.arguments and use your injected application ports/clients.
        Ok(NativeToolResult::Success(serde_json::json!({"words": 6})))
    })
}
```

This snippet illustrates the interface; the linked implementation performs the actual typed decoding and counting. The registry validates the pinned input schema and byte limit before dispatch. `NativeToolCall` supplies the Run, stable activity ID, per-attempt ID, arguments, and any declared host-pinned workspace/permissions/command. These scope fields are separate from model arguments.

Return `Success(JSON)` for a completed result. With `reports_errors(true)`, `Rejected(ToolError)` saves bounded expected refusal feedback for another agent decision. `Failure(Failure)` reports a known execution failure under the retry policy. Returning an adapter `Error` indicates possible uncertainty, handled according to the pinned recovery policy. See [native tool execution](#native-tools) and [reconciliation](recovery.md#reconciliation).

Reuse application ports for existing interactions. Add a new application-owned port when introducing a different external capability, and inject its adapter into your tool. Workflows perform no I/O; the tool runs after its activity intent is saved. Side-effect recovery remains the tool author's responsibility. Tools issuing their own model requests need usage instrumentation beyond this tool port.

<a id="custom-tools--3-install-and-allow-the-tool"></a>

### 3. Install and allow the tool

```rust
use act0r::bootstrap::local_app_with_tools;
use std::sync::Arc;

let tool = Arc::new(WordCountTool::new()?);
let (engine, agents) = local_app_with_tools("agent.db", vec![tool]).await?;
```

`WordCountTool` is your application type from the example. The hook accepts `Vec<Arc<dyn NativeTool>>` and retains the default local catalog. Duplicate `(name, version)` registrations, including collisions with built-ins, fail; tools cannot override installed contracts. Different versions of a name can coexist in the registry. A single actor allowlist still supports only one version per name.

Enable the tool in the actor configuration:

```json
"tools": [{ "name": "word_count", "version": 1 }]
```

Installation makes it discoverable through `agents.tool_definitions()` but does not grant it to every actor. Task payloads cannot change allowlists, contracts, or pinned scope. Both `tool` v1 and `planner` v3 actors use the same extended catalog and durable execution contract.

For a fully custom composition root, `ToolRegistry::new(tools)?.with_tools(additional_tools)?` extends a startup catalog without replacement. Supply the returned registry to your actor adapters, executor, and tool-discovery service as described in [native tools](#native-tools). `local_app_with_tools` does that wiring for the default local host. Existing `local_app`/`local_engine` and the shipped CLI retain their default catalog; the CLI does not load arbitrary Rust files or dynamic plugins.

<a id="custom-tools--recovery-versions-and-verification"></a>

### Recovery, versions, and verification

Reinstall the exact versioned contracts and compatible implementations on every restart. Missing or changed definitions block pending work before admission. Restore the original tool and use the existing Run/actor resume controls; saved inputs and usage remain intact. Definition versions identify contracts, not executable code hashes. Publish a new version when changing a contract and retain implementations needed by older Runs.

Results, approvals, retries, bounded context, actor/goal controls, and activity allowances use the existing engine. A saved custom result is reused after restart. An uncommitted repeat-safe call can retry with the same activity ID and a new attempt. An uncertain reconciliation-required call blocks for operator inspection. No new workflow versions or SQLite migration are needed.

Consumer tests exercise the example through public APIs, builder/pin compatibility, nullable schemas, duplicate/invalid contracts, retained built-ins, explicit actor schemas, both policies, saved results and duplicate completion, missing/changed contract restoration, durable approval/denial and simulated uncertain outcomes, and killed hosts at admission and saved-result boundaries. Local process-kill validation covers Windows application crashes; Unix execution and power-loss validation remain separate work.

<a id="tool-feedback"></a>

## Durable tool error feedback

Native tools can declare `reports_errors: true` and return an expected rejection as saved feedback. Tool-enabled answering and planner actors then choose another permitted action, ask for clarification, delegate, or finish. A missing project file can lead to a corrected read while using the same Run and its existing budgets.

<a id="tool-feedback--try-it-locally"></a>

### Try it locally

No model server or credentials are required:

```powershell
cargo run --locked --example tool_feedback -- .build/tool-feedback-demo/demo.db
cargo test --locked --test native_tools --test workspace_tools --test cli
```

The demo saves a missing-file error and the exact next model request, creates that previously missing file in the managed workspace, and restarts. The saved error remains unchanged. The planner reads the correct README, delegates a review, and finishes with two file attempts and five model calls across both actors. Its scripted actions are deterministic; a separate model fixture in the tests consumes the error to choose a corrected read and derives the delegated goal from the saved file contents. That fixture also resumes a child's human wait across restart.

Select the new workspace tool contracts at actor creation:

```json
"tools": [
  { "name": "list_files", "version": 2 },
  { "name": "read_file", "version": 2 }
]
```

Supply `--source PROJECT` with `actor agent create tool` or `actor agent create planner`. [workspace-agent-scripted.json](../examples/configs/workspace-agent-scripted.json) now selects v2. Actor creation pins the exact contracts and managed workspace. Model actions still use the tool name and relative arguments, without choosing a version or workspace root.

<a id="tool-feedback--results-and-execution-failures"></a>

### Results and execution failures

An expected rejection becomes an observation such as:

```json
{
  "ok": false,
  "error": {
    "code": "file_not_found",
    "message": "the requested path does not exist in the managed workspace"
  }
}
```

The runtime completes the activity successfully because it obtained and saved a known reply. Its result records the rejected operation with `ok:false`. The answering policy appends it to `tool_observations`; the planner appends it to `observations`, alongside the selected action. Feedback and the next model request commit together. Model instructions describe how to use errors as data when an allowed tool declares feedback support.

| Tool return | Runtime behavior |
| --- | --- |
| `NativeToolResult::Success(value)` | Save the bounded result and continue |
| `NativeToolResult::Rejected(ToolError)` | Validate the declared contract and bounded error, save `ok:false`, and continue |
| `NativeToolResult::Failure(failure)` | Use the pinned retry policy; non-retryable or exhausted failures terminate the Run |
| `NativeTool::execute` returns `Err(Error)` | Preserve the existing uncertain-outcome recovery/reconciliation procedure |

Only report `Rejected` for a known tool-level problem. Unknown effects still require the existing recovery procedure. Runtime capability checks, missing/changed catalog pins, unallowed tools, invalid argument schemas, absent workspace scope, and output-contract violations retain their existing execution safeguards. A rejection cannot grant permissions, replace workspace authority, or authorize retries of an uncertain effect.

<a id="tool-feedback--workspace-classification"></a>

### Workspace classification

Both workspace tool versions use the same filesystem implementation and read-only port. V2 reports these expected problems:

| Code | Meaning |
| --- | --- |
| `file_not_found` | A requested inspection path does not exist |
| `permission_denied` | Filesystem permissions deny inspection |
| `not_a_directory` | A requested path component is not a directory |
| `not_a_file` | The requested read path is not a regular file |
| `path_refused` | A relative-path rule, private-directory rule, or link/reparse-point check refuses inspection |
| `invalid_utf8` | File contents are not UTF-8 text |
| `file_limit` | The file exceeds the bounded read size |
| `listing_limit` | The project contains more than the permitted number of listed files |

These messages are bounded and omit absolute filesystem paths and file contents. The read/list limits, copy exclusions, ownership lock, and path checks still apply. Ownership contention remains retryable `workspace_busy`. Interrupted, would-block, and timed-out inspection I/O are retryable `workspace_io` failures for v2. Other inspection I/O errors remain execution failures. Invalid managed roots, markers, or lock authority are execution failures; they are outside model-controlled file selection.

<a id="tool-feedback--bounds-controls-and-recovery"></a>

### Bounds, controls, and recovery

`ToolError` requires a nonempty ASCII identifier of at most 64 bytes and a nonempty message of at most 4 KiB of UTF-8. The entire serialized observation must also fit the tool's pinned `max_output_bytes`. Invalid or undeclared feedback records a non-retryable `tool_feedback_contract` failure; a result exceeding its serialized bound records `tool_output_limit`. Oversized error data does not enter saved history.

Each feedback activity consumes an attempt. Further model decisions consume normal per-Run and inherited goal/delegation allowances. Context-byte limits include the complete error observation. If correction cannot be admitted, the completed error remains saved; feedback does not reset limits or add automatic retries.

Pause and cancellation hold further work using the existing gates. An admitted feedback outcome can still commit during a hold. Duplicate completion does not append another observation or model request. Completed errors and reads survive restart even if files later change. An interrupted repeat-safe inspection whose result was never committed may execute again; its activity ID remains stable and its attempt ID changes. Process-kill tests verify that only this uncommitted work repeats.

<a id="tool-feedback--compatibility-and-ddd-boundaries"></a>

### Compatibility and DDD boundaries

Feedback-capable inspection tools use `native.tool` v3; [tools requiring writable paths](#workspace-edits) use v4 and also support feedback. Executors lacking the appropriate handler block before new execution. Unscoped tools use handler v1; host-defined scoped tools without feedback use handler v2. `reports_errors` selects structured tool feedback. Echo and sum v1 retain their current behavior.

Local bootstrap installs the version-2 `list_files` and `read_file` contracts. Expected inspection errors return structured feedback so the actor can choose its next action. `ListFilesTool::new` and `ReadFileTool::new` provide these contracts.

`domain::tools` owns `ToolError`, pure validation, the contract flag, and activity-handler selection. `application::ToolRegistry` normalizes typed feedback through the existing execution port and enforces output bounds. `WorkspaceReader` returns typed `Error::ToolRejected` for expected inspection problems; the native adapter maps it to `Rejected` only for a feedback-capable contract. `domain::answer` and `domain::planner` consume ordinary saved observations without filesystem I/O. The local filesystem adapter supplies the classification. No scheduler, persistence tables, or schema migration are added; SQLite uses the shared baseline and agent registrations remain tool v1/planner v3/answer v1.

Tests cover both policies, corrected reads and dynamic delegation, exact requests/results/usage across restart, a durable human wait, error/output/context limits, model caps, path and private-file refusal, UTF-8/file/listing limits, temporary failure classification, unchanged retries and reconciliation, contract restoration, rollback, controls, local Responses and Chat Completions HTTP fixtures, CLI execution, and killed workers before execution, after an uncommitted error, after committed error/read results, and during delegated waits. Verification runs on Windows. Live-provider decisions and a Unix test run remain unverified.

<a id="workspace-tools"></a>

## Portable workspace tools for native actors

The `tool` v1 and `planner` v3 actors can inspect a project with `list_files`, `read_file` and `read_files`. These tools use Rust filesystem APIs and structured JSON on Windows and Unix. They do not invoke a shell or select between `ls`, `dir`, and PowerShell commands.

For writes, explicitly allow [replace_text](#workspace-edits) and pin writable paths at actor creation. It uses a separate workspace-writing port, durable approval, and the existing edit journal; inspection tools retain their read-only contract.

For locating code, explicitly allow [search_text](#workspace-search). It searches the same managed copy and saves bounded matching paths, line numbers, and snippets through the existing tool activity contract.

<a id="workspace-tools--try-it-locally"></a>

### Try it locally

No model server or credentials are required:

```powershell
cargo run --locked --example workspace_tools -- .build/workspace-tools-demo/demo.db
cargo run --locked --example tool_feedback -- .build/tool-feedback-demo/demo.db
cargo test --locked --test workspace_tools --test cli
```

The demo creates a small source project, pins a managed copy to a planner, lists files, and reads its README. After saving the read result and the exact next model request, it changes the managed file, closes the runtime, and reopens the database. The planner then delegates a review to an answering actor and finishes. Assertions verify one attempt per file activity and five model calls across the goal. Scripted actions make this demonstration deterministic. A separate model fixture in the tests derives the delegated goal from saved file contents and restarts again during the child's human wait.

Create a tool actor through the CLI:

```powershell
cargo build --locked
$db = '.\workspace-tools.db'
.\target\debug\act0r.exe --db $db actor tool list
$actor = (& .\target\debug\act0r.exe --db $db actor agent create tool --source .\YOUR_PROJECT --config-file .\examples\configs\workspace-agent-scripted.json).Trim()
@{ goal = 'Inspect the project README.' } | ConvertTo-Json | Set-Content .\workspace-goal.json
$task = (& .\target\debug\act0r.exe --db $db actor send $actor --input-file .\workspace-goal.json).Trim()
.\target\debug\act0r.exe --db $db worker run --until-idle
.\target\debug\act0r.exe --db $db actor task inspect $task
```

The supplied fixture expects `README.md` and selects workspace tool v2, which reports [expected errors as saved feedback](#tool-feedback). The `workspace_tools` demo uses the same v2 pins; `tool_feedback` demonstrates v2 correction and delegation across restart. For a live model, configure the existing model target and omit `script`. Planner configuration also needs its permitted worker aliases and delegation limits; the demos write examples beside their databases. `actor agent create planner --source PROJECT --config-file PLANNER.json` selects the current planner.

<a id="workspace-tools--workspace-ownership-and-tool-arguments"></a>

### Workspace ownership and tool arguments

Actor creation copies `--source` once into a runtime-managed directory beside the database, under `.act0r/workspaces`. The runtime saves its canonical path in actor configuration. Source changes do not refresh that copy. Later tasks and follow-ups retain the same managed workspace; create another actor to use a new source snapshot.

Add a versioned tool allowlist:

```json
"tools": [
  { "name": "list_files", "version": 2 },
  { "name": "read_file", "version": 2 }
]
```

Actors with workspace tools require a source at creation. Callers cannot supply `workspace` or `tool_definitions` pins. Task payloads can replace the goal but cannot change the workspace, tools, or permissions. The model sees the read-only tool descriptions and schemas; it does not receive the absolute workspace path. Calls use these actions:

```json
{"kind":"call_tool","tool":"list_files","arguments":{}}
{"kind":"call_tool","tool":"read_file","arguments":{"path":"src/lib.rs"}}
```

The root is runtime context, outside model arguments. Unscoped tools such as `echo` and `sum` receive no workspace even when their actor has one. Model-only `answer` actors continue to reject workspace configuration and source preparation.

<a id="workspace-tools--durable-batched-reads"></a>

### Durable batched reads

Fresh harness sessions allow `read_files` v1 during discovery and coding. The agent can inspect several known files in one model decision and one saved tool activity:

```json
{"kind":"call_tool","tool":"read_files","arguments":{"paths":["README.md","src/lib.rs","tests/contracts.rs"]}}
```

Explicit actor configurations can allow `{"name":"read_files","version":1}`. Native function calling exposes the same bounded string-array parameter; flat JSON actions use `{"kind":"read_files","paths":["README.md","src/lib.rs"]}`.

The batch accepts 1–8 paths of at most 4 KiB each and returns `files` in request order, including duplicates. Each entry has `path` and either `ok:true`, `text`, `truncated`, or `ok:false` with a structured `error`. A missing, private, linked, oversized or non-UTF-8 file does not discard successful entries. Top-level `ok:true` means the batch completed; inspect each file's `ok` before using its contents as evidence.

Each file is limited to 1 MiB, with at most 16 KiB of UTF-8 text returned. The batch shares a 64 KiB text allowance in request order; later entries may have empty, truncated text. `text_bytes`, top-level `truncated` and `limits` describe the returned evidence. Argument JSON is limited to 32 KiB and result JSON to 512 KiB, including escaping. Limits end at character boundaries.

Batch evidence remains subject to the actor's pinned model-context cap. If the newest complete batch cannot fit, its full result is saved and the Run reports `context_limit`, following the existing native-context contract. Use a suitable context cap for actors expected to consume large batches.

The local adapter holds the existing managed-workspace ownership lock for the whole batch. Invalid runtime authority and ownership/transient execution errors retain their existing failure/retry behavior. File refusals are per-file feedback. The workspace reader port has an optional `read_files` capability for other hosts.

The batch result commits together. Restart after that commit reuses the saved contents and errors, even if files subsequently change. An interrupted read before its result commits may repeat under `SafeToRepeat`. It is not a filesystem snapshot against processes that bypass the managed-workspace lock.

SQLite protects the new bounded string-list contracts from older hosts. Existing sessions and prepared coding continuations retain their original tool catalogs on exact retries. Coordination inspection limits retain their existing single-file tools. No additional configuration is required.

```powershell
cargo test --locked --offline --test workspace_tools reads::
cargo test --locked --offline --test http_api automatic_project_batched_reads
```

<a id="workspace-tools--filesystem-and-output-bounds"></a>

### Filesystem and output bounds

The tools reuse the coding adapter's managed-workspace marker, ownership lock, path validation, and bounded inspection:

- Copying accepts at most 10,000 entries and 64 MiB, rejecting included symlinks, Windows reparse points, and special files.
- Copy and listing exclude `.git`, `.act0r`, `.act0r-tools`, `.build`, `target`, `node_modules`, `.aws`, `.env`, and `.env.*` at every level. These names are compared case-insensitively, with trailing dots/spaces normalized.
- Reads accept permitted relative file paths using `/` separators on every host. Absolute paths (including Windows UNC paths), backslashes, parent traversal, drive-qualified paths, excluded components, and linked components are refused. A replaced workspace root that is a symlink or reparse point is also refused.
- `list_files` returns sorted relative paths using `/`, with a limit of 1,000 files. Exceeding the limit fails explicitly.
- `read_file` accepts UTF-8 files up to 1 MiB and returns at most 16 KiB of text, ending at a character boundary. The `truncated` flag reports whether text was shortened.
- `list_files` and `read_file` inputs are bounded to 4 KiB of serialized JSON; outputs to 128 KiB, allowing JSON escaping within the text bound. Saved observations still count toward the next model request's configured context-byte limit.

Example results:

```json
{"ok":true,"files":["README.md","src/lib.rs"]}
{"ok":true,"path":"README.md","text":"Project evidence.\n","truncated":false}
```

Workspace tool v2 reports missing files, invalid paths, binary files, and inspection limits as bounded `{ "ok": false, "error": ... }` observations so the model can choose another permitted action. Ownership contention remains a known retryable `workspace_busy` failure; invalid runtime workspace authority and other execution errors retain their safeguards. See [tool feedback](#tool-feedback) for classification, limits, and recovery. Neither tool exposes file writes or commands. Native adapters remain trusted host code, with the existing filesystem checks rather than an OS execution sandbox.

<a id="workspace-tools--durability-and-compatibility"></a>

### Durability and compatibility

The workflow saves the tool definition, relative arguments, and pinned workspace before execution. Feedback-capable inspection contracts use `native.tool` v3. An executor lacking the appropriate handler blocks before new execution. Unscoped requests without feedback remain `native.tool` v1 and omit optional workspace metadata, preserving existing serialized contracts. Current tool definitions stay pinned across restart.

Results and the next model request commit through the existing Run journal. Restart reuses a saved result even if the file is later changed or removed. A read interrupted before its result commits can execute again because it is repeat-safe; that new attempt may observe changed file contents. The guarantee concerns committed results, not an exactly-once filesystem read.

Goal and actor/Run controls, attempt allowances, model/token budgets, context bounds, and delegation waits use the existing scheduler. Admitted read outcomes can commit during pause or cancellation while further admission stays held. Shared goal accounting includes delegated model calls. Missing or changed pinned tool contracts block without repinning, copying source again, or rewriting saved requests/results. Configuration checks perform no filesystem I/O; actual filesystem access belongs to a tool attempt.

Registrations remain `tool` v1, `planner` v3, and `answer` v1. SQLite uses the shared baseline; no persistence tables or scheduler are added.

<a id="workspace-tools--rust-api-and-ddd-boundaries"></a>

### Rust API and DDD boundaries

| Layer | Responsibility |
| --- | --- |
| `domain::tools` | Workspace-read actions, scoped contracts/requests, pure validation and activity intents |
| `domain::answer` / `domain::planner` | Decisions and saved observations using the pinned actor workspace |
| `application::ports::WorkspaceReader` | Read-only asynchronous project inspection |
| `application::ToolRegistry` | Exact catalog pins, scoped dispatch and output bounds |
| `adapters::workspace::LocalWorkspaces` / `LocalTools` | Source preparation and portable filesystem access |
| Native tool/agent adapters and bootstrap | Read-only tool implementations and dependency composition |

Custom hosts construct `workspace_tool_registry(Arc<dyn WorkspaceReader>)`, then install `ToolAgentAdapter::new(tools.clone()).with_workspaces(factory.clone())` and/or `PlannerAgentAdapter::new(tools.clone()).with_workspaces(factory)`. Supply the same registry to `AgentExecutor::with_native_tools`. `LocalTools` implements the existing coding `ToolRunner`, the read-only `WorkspaceReader`, and the separate `WorkspaceWriter`; inspection exposes only list/read actions and preserves typed failures. Local bootstrap now uses `editable_workspace_tool_registry(reader, writer)`, whose added write tool still requires explicit actor permission. `local_tool_registry()` remains available for hosts wanting only echo/sum.

Tests cover tool/planner source pinning, saved results and exact next requests across restart, dynamic tool-to-child evidence, durable human waits, ownership contention, path/private-file refusal, symlinks/reparse points, input/output/context bounds, attempt/model limits, goal pause/cancellation, CLI execution, and killed workers at intent, list-result, read-result, and delegated-wait boundaries. The implementation and Windows junction tests have been exercised on Windows; Unix-specific link fixtures are conditional. Live-provider decisions and a Unix test run remain unverified.

<a id="workspace-edits"></a>

## Durable native workspace edits

`tool` v1 and `planner` v3 actors can call `replace_text` v1 alongside the existing read tools. Each edit requires saved approval, applies only within the actor's managed source copy and pinned writable paths, and uses the coding adapter's edit journal. A completed result survives restart. An interrupted attempt without a saved result requires explicit reconciliation, even if the file appears to contain the replacement.

For new files, opt into [create_file](#workspace-creation), which shares the write port, permissions, approvals, and recovery lifecycle. `replace_text` continues to require an existing file.

<a id="workspace-edits--try-it-locally"></a>

### Try it locally

No model server or credentials are required:

```powershell
cargo run --locked --example workspace_edits -- .build/workspace-edits-demo/demo.db
cargo test --locked --test workspace_tools --test native_tools --test cli
```

The scripted planner reads a README, waits for approval, and edits it. The example deliberately closes the runtime after the write but before saving its result. Restart blocks the uncertain edit. The fixture inspects the changed file, explicitly authorizes a journal retry through the ordinary reconciliation API, and verifies `already_applied: true` with the original activity ID and a new attempt ID. It saves that result, reopens again, verifies the exact next request and usage, reads the changed file, delegates review, and finishes. Six model calls span both actors; the edit has two attempts. The source remains unchanged.

The demo makes an explicit operator decision because it controls this deterministic fixture and knows the stopped worker has exited. Ordinary workers never authorize reconciliation automatically. Separate tests kill real worker processes at approval, queued intent, admitted intent, applied-but-uncommitted edit, committed edit, committed read, and delegated human wait boundaries. Another test constructs its replacement and delegated goal from saved read observations rather than a scripted goal.

<a id="workspace-edits--actor-configuration"></a>

### Actor configuration

Supply `--source PROJECT` at actor creation and add these fields to tool or planner configuration:

```json
"tools": [
  { "name": "read_file", "version": 2 },
  { "name": "replace_text", "version": 1 }
],
"writable_paths": ["README.md", "src/"]
```

The runtime saves the permissions with the actor configuration. Tasks may replace the goal but cannot change permissions, tools, contracts, or the managed root. Later tasks and planner follow-ups retain the same workspace and permissions. Delegated workers use their own configuration; a parent does not grant them its write permissions.

Permissions use exact relative file names or directory prefixes ending in `/`. `src/` covers files below `src`, without covering `src-other`. There must be 1–128 distinct entries, each at most 4096 UTF-8 bytes before its optional trailing slash, totaling at most 16 KiB. Use `/` separators. Empty, absolute, drive-qualified, parent/current-directory, empty-component, control-character, wildcard, backslash, and trailing-dot/space components are refused. Write tools require a nonempty allowlist; read-only configurations reject unused writable permissions. Private-path and link checks still run during execution, regardless of the allowlist.

The model sees writable relative paths in its saved prompt and returns only relative tool arguments:

```json
{"kind":"call_tool","tool":"replace_text","arguments":{"path":"README.md","old":"Original project evidence.","new":"Updated project evidence."}}
```

`old` must be nonempty and have exactly one match under Rust's non-overlapping text matching. `new` may be empty. Existing files must be UTF-8 and both original and replacement must fit 1 MiB. The serialized argument object is limited to 64 KiB; serialized results are limited to 8 KiB. This tool changes existing files; it does not create files or expose command execution. Reads retain their existing truncation flag, so a large file may require a narrower replacement chosen from inspected text.

[workspace-edit-agent-scripted.json](../examples/configs/workspace-edit-agent-scripted.json) supplies a deterministic tool actor configuration. Its source README must contain `Original project evidence.`. Create the actor with:

```powershell
cargo build --locked
.\target\debug\act0r.exe --db .\edits.db actor agent create tool --source .\YOUR_PROJECT --config-file .\examples\configs\workspace-edit-agent-scripted.json
```

Send a task and run the worker as described in [workspace tools](#workspace-tools). `worker run --until-idle` exits at the approval wait. Inspect the task to obtain its Run and wait IDs, then use `run approve RUN_ID WAIT_ID` and run the worker again. Add `--deny` to save denial as an observation. Planner configuration also requires the existing worker aliases and delegation limits; the demo writes a complete planner configuration beside its database. Live model configuration uses the existing model target and omits `script`.

<a id="workspace-edits--approval-errors-and-controls"></a>

### Approval, errors, and controls

Every native replacement uses `requires_approval: true`. The `native_tool_approval` payload saves the exact tool, relative arguments, recovery policy, managed root, and writable paths. Approval survives restart, identical signals are idempotent, and approval of one edit does not approve a corrected edit with a new activity. A signal can be saved while the goal is paused; execution waits for the hold to be released.

Before a filesystem journal exists, expected refusals become bounded saved `ok:false` observations through the [tool feedback contract](#tool-feedback):

| Code | Refusal |
| --- | --- |
| `path_refused` | Invalid, private, or linked path |
| `path_not_writable` | File falls outside the configured writable paths |
| `file_not_found`, `not_a_directory`, `not_a_file` | Requested target is unavailable or not a regular file |
| `permission_denied` | Inspection permissions refuse the target |
| `invalid_utf8` | Existing file is not UTF-8 |
| `empty_old_text` | Old text is empty |
| `text_not_found` | Old text has no match |
| `text_not_unique` | Old text has multiple non-overlapping matches |
| `file_limit` | Original or resulting file exceeds the size limit |

The agent may correct its arguments, read another permitted file, ask for clarification, delegate, or finish. A corrected edit creates another durable approval wait and consumes normal model/attempt allowances. Refused edits do not create a filesystem edit journal or modify the target.

Ownership contention is a known retryable `workspace_busy` failure: this invocation has not acquired the workspace or changed anything. Invalid runtime authority, invalid journals, unclassified I/O, and write errors remain execution errors rather than model feedback. They use the edit's `RequiresReconciliation` policy. Once a journal exists, a missing, oversized, refused, or otherwise unverifiable target is also an execution error, since a previous attempt may have published the replacement.

Existing goal/actor/Run pause and cancellation, context limits, model/token allowances, and total attempt caps apply. Outcomes admitted before a hold can commit while further admission is held. Budget/context exhaustion retains a completed edit and its recorded result. Cancellation does not undo edits or discard an indeterminate activity requiring reconciliation.

<a id="workspace-edits--filesystem-journal-and-recovery"></a>

### Filesystem journal and recovery

The shared workspace ownership lock serializes cooperating tools. Before file publication, the adapter writes an activity-keyed journal under `.act0r-tools`, containing a hash of the complete tool request (including permissions), the original and replacement content hashes, and the replacement content. Journal and target writes use a temporary file, file synchronization, and replacement. Unix additionally synchronizes the parent directory. This is separate from the SQLite result transaction; there is no atomic transaction spanning both stores.

On an authorized retry with the same activity ID:

1. Verify the input hash and bounded journal content/hash.
2. If the current file matches the replacement hash, return `already_applied: true` without writing it again.
3. If it matches the original hash, publish the saved replacement and return `already_applied: false`.
4. If it matches neither hash, preserve the file and return an execution error requiring reconciliation.

Changed input or permissions, a corrupt/link journal, or an unverifiable target cannot become fresh expected feedback. After inspecting the external effect and ensuring the interrupted worker has stopped, an operator can use the existing `run reconcile` API to accept a verified result, authorize the bounded journal retry, or fail the Run. See [reconciliation instructions](recovery.md#reconciliation).

Successfully recording the result, appending its observation, and scheduling the exact next model request are one existing SQLite transaction. Duplicate completion does not create another observation/request. Restart reuses committed results even if files subsequently change. Activity IDs remain stable across interrupted attempts; attempt IDs and usage charges remain distinct.

These checks coordinate tools in this runtime; they are not an OS sandbox against independent processes modifying the workspace. Process-kill verification establishes application-crash recovery, not physical-power-loss guarantees.

<a id="workspace-edits--ddd-boundaries-and-compatibility"></a>

### DDD boundaries and compatibility

Coding-session recovery inspection exposes exact saved `replace_text`/`create_file` requests and journal/hash evidence through a separate read-only inspection port. Retry/fail decisions use the existing Run reconciliation transaction and require stopped-operation confirmation and notes. Retry acknowledges journaled matching content without rewriting it; creation never treats a matching pre-existing file without an activity journal as success. See [tool recovery](harness.md#coding).

`domain::tools` owns the serializable `WorkspaceEdit`, the `requires_writable_paths` contract flag, pure permission validation, and scoped approval/activity commands. `AnswerConfig` and `PlannerConfig` persist writable paths; their policies select them without filesystem access. `application::ports::WorkspaceWriter` performs one journaled replacement independently of the read-only `WorkspaceReader`. `ToolRegistry` validates exact contracts, scoped permissions, arguments, and bounded results. `ReplaceTextTool` adapts typed feedback and execution errors; `LocalTools` implements the filesystem behavior using the same journal as the coding adapter.

Custom hosts can retain `workspace_tool_registry(reader)` for inspection only, or install `editable_workspace_tool_registry(reader, writer)`. Connect that registry to the tool/planner adapters and executor as in [workspace tools](#workspace-tools). Local bootstrap installs the full `agent_tool_registry` with inspection, editing, creation, tests, and recall; actor allowlists determine access. `local_tool_registry()` still supplies only echo/sum.

Write-capable activities use `native.tool` v4, preventing executors without the permission-aware handler from admitting them. Handler versions describe distinct capabilities: unscoped tools, workspace reads, feedback, writes, tests and named commands. `requires_writable_paths` defaults to false and is omitted when false; empty configuration/request writable paths are likewise omitted. Registrations stay tool v1/planner v3/answer v1. SQLite uses the shared baseline; no tables, scheduler, or second planner are added.

Verification covers both native policies, writable-path validation and scope isolation, expected corrections with independent approvals, denial and duplicate delivery, restart and real process kills, exact requests/results/usage, journal identity/content tampering, original/replacement/conflicting hashes, transaction rollback, goal controls, attempt/model/context limits, CLI, and delegation from saved changed-file evidence through a durable child wait. Tests run on Windows; live model decisions and Unix execution remain separate validation work.

<a id="workspace-creation"></a>

## Durable native file creation

`tool` v1 and `planner` v3 actors can opt into `create_file` v1 to create a new UTF-8 file in their managed workspace. Each call uses actor-pinned writable paths, a durable approval wait, and an activity-keyed filesystem journal. It refuses existing targets, including files whose contents already match the request. Parent directories must already exist.

<a id="workspace-creation--try-it-locally"></a>

### Try it locally

```powershell
cargo run --locked --example workspace_create -- .build/workspace-create-demo/demo.db
cargo test --test workspace_tools creation --locked
cargo test --test cli creation --locked
```

The scripted example starts with a Rust project missing a module. It runs failing tests, creates the module, deliberately stops after file publication without saving the result, then reopens SQLite. The fixture inspects its exact file and explicitly authorizes journal recovery. The retry returns `already_applied:true` without rewriting the file. It updates the module reference, runs passing tests, restarts again, reads the file, and delegates review. The planner and reviewer use eight model calls in total. No provider credential is required.

The example controls the stopped worker and fixture. Ordinary recovery requires an operator to inspect the external effect before accepting a result or authorizing retry; workers do not make that decision automatically.

<a id="workspace-creation--configuration-and-arguments"></a>

### Configuration and arguments

Provide a source project with `--source`, then include creation in the actor configuration:

```json
{
  "tools": [
    { "name": "create_file", "version": 1 },
    { "name": "read_file", "version": 2 },
    { "name": "replace_text", "version": 1 }
  ],
  "writable_paths": ["src/"]
}
```

These entries belong in a complete tool/planner configuration with its goal, model target, and budgets. Permissions can be exact relative file names or directory prefixes ending in `/`. A task payload can replace the goal but cannot change the tools, permissions, or managed root. Delegated actors use their own pins. See [workspace edits](#workspace-edits) for permission bounds and approval commands.

The model returns:

```json
{
  "kind": "call_tool",
  "tool": "create_file",
  "arguments": {
    "path": "src/arithmetic.rs",
    "content": "pub fn add(a: i32, b: i32) -> i32 { a + b }\n"
  }
}
```

The runtime supplies the workspace, permissions, and activity/attempt IDs separately. Unknown arguments and wrong primitive types are rejected by the existing tool schema. Both fields are required strings; content may be empty. Native arguments are capped at 65536 serialized UTF-8 JSON bytes, including path, contents, quotes, and escaping. The workspace adapter separately caps file content at 1 MiB. A successful result is bounded to 8192 bytes and contains `ok:true`, relative `path`, UTF-8 `bytes`, and `already_applied`.

Relative paths use `/` separators. Creation rejects absolute/drive paths, parent/current-directory components, private directories, links/reparse points, invalid portable components, and Windows device-name aliases on all platforms. It never creates parent directories or replaces a target. Use `read_file` and `replace_text` for an existing file.

<a id="workspace-creation--approval-feedback-and-controls"></a>

### Approval, feedback, and controls

Every creation saves a `native_tool_approval` wait containing the exact content, path, permissions, workspace, and recovery policy. Approval survives restart. Identical signals are idempotent; another corrected call gets its own wait. A goal hold can retain an approval while admission remains stopped.

Expected refusals before a journal exists become bounded saved feedback:

| Code | Refusal |
| --- | --- |
| `file_exists` | Any existing target, even identical contents or a directory |
| `parent_not_found` | A parent directory is missing |
| `not_a_directory` | A parent component is not a directory |
| `path_refused` | Invalid, private, linked, or reserved path |
| `path_not_writable` | Target falls outside pinned permissions |
| `permission_denied` | Filesystem permissions refuse preflight inspection |
| `file_limit` | Content exceeds the workspace adapter's bound |

Refused creation does not write a journal, create a file, or create directories. The model can choose another permitted path, inspect existing content, ask for clarification, or finish. A denied approval likewise preserves the filesystem. Lock contention is a known retryable `workspace_busy` failure.

Once a journal exists, invalid journals, changed inputs/permissions, unavailable parents, linked or conflicting targets, and write failures retain execution-error semantics. An interrupted `RequiresReconciliation` activity blocks even when admission saved before file execution began. Cancellation does not undo a file or resolve uncertainty.

Completed creation marks existing compact `test_status` stale through the current native context policy. Rejected calls and approval denials do not. A later completed test refreshes that evidence. Model/token, goal, context, and activity-attempt limits still apply; exhaustion retains the created file and saved result.

<a id="workspace-creation--journal-and-recovery"></a>

### Journal and recovery

The workspace ownership lock serializes cooperating tools. Before publication, `.act0r-tools/ACTIVITY_ID.json` saves a versioned creation intent with the complete request hash (including workspace and permissions), content hash, and exact contents. The journal records that the target was absent at preflight. It is synchronized before target publication.

The adapter stages and synchronizes a complete temporary file in the target's existing parent, then publishes it without replacing an existing destination. If a file appears between preflight and publication, it remains intact and the journaled operation requires reconciliation. Unix also synchronizes the parent directory. Some supported filesystem implementations may leave an extra temporary name if interrupted during publication; the journal and target are the recovery evidence.

On an operator-authorized retry using the same activity ID:

1. Validate the journal's operation/version, request identity, bounded contents, and content hash.
2. If the target is a regular file matching the saved content hash, return `already_applied:true` without writing it.
3. If the target is absent and its parent remains valid, publish the saved contents and return `already_applied:false`.
4. Preserve a conflicting target and require reconciliation.

A fresh activity always refuses an existing target. Matching bytes establish a saved creation's desired state only when its validated journal exists. A retry of a saved intent may publish an absent target again if an operator authorizes it after inspecting the current state.

Filesystem publication and SQLite completion are separate operations. SQLite atomically saves the result, context/test-status change, and next model request. Duplicate completion adds no continuation. A restart reuses committed results even if the file later changes. Tools coordinate within the runtime; they do not provide an OS sandbox against unrelated processes changing the workspace.

<a id="workspace-creation--ports-adapters-and-compatibility"></a>

### Ports, adapters, and compatibility

`domain::tools::WorkspaceCreate` carries only path and UTF-8 content. `WorkspaceWriter::create_file` is the application-owned write port; existing edit-only implementations retain a default unsupported method. `CreateFileTool` declares and validates the native contract. `LocalTools` supplies journaled filesystem creation in its workspace adapter. The shared answer/planner policies continue to schedule ordinary `call_tool` activities and perform no filesystem I/O.

The full `agent_tool_registry` and local bootstrap install creation, while previous local, inspection, editable, and tested registry constructors retain their catalogs. Custom hosts can explicitly install `CreateFileTool::new(writer)` in their tool registry after implementing its write port. Installing a capability does not grant it to existing actor allowlists.

Creation uses the existing permission-aware `native.tool` v4 handler and current tool v1/planner v3 adapters. Existing edit journals, tool contracts, saved requests, and workflow versions remain supported. SQLite uses the shared baseline. There is no additional planner or scheduler.

<a id="workspace-creation--verification"></a>

### Verification

Checks cover both policies, exact permissions and approvals, denial/holds/cancellation, initial refusals without journals, empty/Unicode contents, size bounds, overwrite refusal at publication, journal identity/hash tampering, absent/matching/conflicting targets, linked parents/leaves/journals/roots, saved reads and delegated waits, completion rollback and duplicate delivery, budget/context exhaustion, compact test-state staleness, and CLI restart behavior.

Real process-kill fixtures stop at approval, queued intent, admission, applied-but-uncommitted creation, saved creation, saved read, and a child's durable human wait. Recovery retains IDs and usage, preserves committed results, and verifies that matching journal retries leave the file's modification time unchanged. These checks validate application-crash recovery on Windows; physical power-loss and Unix execution remain separate validation work.

<a id="workspace-search"></a>

## Bounded workspace search for native actors

`search_text` v1 lets `tool` v1 and `planner` v3 actors find code through case-sensitive literal matching. It uses the runtime-pinned managed workspace, the read-only workspace port, and existing durable native tool activities. No shell or external search service is needed.

<a id="workspace-search--try-it-locally"></a>

### Try it locally

```powershell
cargo run --locked --example workspace_search
cargo test --locked --test workspace_tools native_tests::search
```

The scripted example finds a Rust function, saves its search, restarts, reads the returned path, approves a repair, and runs real fixed Cargo tests. A second restart preserves the passing test evidence before delegated review. It checks one search attempt, unchanged original source, and saved search evidence describing the earlier code. No model server or credentials are needed. Integration tests additionally use a model fixture that chooses its read/edit path from the actual search result and recalls an evicted search.

<a id="workspace-search--configuration-and-arguments"></a>

### Configuration and arguments

Add the capability to a complete tool/planner actor configuration:

```json
"tools": [{ "name": "search_text", "version": 1 }]
```

Create the actor with `actor agent create tool --source PROJECT --config-file CONFIG.json` (or `planner`). Source copying and ownership follow [workspace inspection](#workspace-tools). Installing search does not add it to existing actor allowlists. Task payloads cannot replace the managed root.

The model can request:

```json
{
  "kind": "call_tool",
  "tool": "search_text",
  "arguments": {
    "query": "fn answer",
    "path": "src",
    "max_matches": 20,
    "max_files": 200
  }
}
```

`query` is required: 1–256 UTF-8 bytes without NUL, CR, or LF. Regex metacharacters are literal. Matching is case-sensitive, with no Unicode normalization. Each matching line appears once even if it contains several occurrences. Its first occurrence determines the snippet. Line numbers start at one; CRLF terminators are removed.

`path` selects one permitted relative file or directory with `/` separators. Omit it or use null for the workspace root. Empty strings, `.`, parent/absolute/drive paths, private components, and Windows device-name aliases are refused. Omit or use null for limits: `max_matches` defaults to 20 (allowed 1–100), and `max_files` defaults to 200 (allowed 1–1000). Unknown arguments are rejected. Input JSON is limited to 8 KiB.

<a id="workspace-search--results-and-bounds"></a>

### Results and bounds

```json
{
  "ok": true,
  "matches": [
    { "path": "src/lib.rs", "line": 1, "snippet": "pub fn answer() -> i64 { 19 - 23 }", "snippet_truncated": false }
  ],
  "files_considered": 1,
  "files_scanned": 1,
  "bytes_read": 35,
  "skipped": { "binary": 0, "oversized": 0, "unsupported": 0 },
  "truncated": false,
  "limits_hit": []
}
```

`files_considered` counts regular files examined, including skipped binary/oversized files. `files_scanned` counts files read and accepted as UTF-8; a match/output limit can stop searching partway through the last file. `bytes_read` includes binary content and bounded probes of growing files.

| Bound | Behavior |
| --- | --- |
| 1 MiB per file | Larger files are skipped and counted as `oversized` |
| 8 MiB total bytes read | Stops with `limits_hit: ["bytes"]` |
| Configured regular-file count | Stops before another file with `files` |
| Configured matching-line count | Stops when another matching line would exceed it with `matches` |
| 512 UTF-8 bytes per snippet | Keeps the first match and reports `snippet_truncated` |
| 32 KiB serialized result | Stops before another match exceeds the output allowance with `output` |
| 10,000 directory entries | Stops with `entries`; an incomplete directory is discarded |
| Directory depth 32 from the root | Skips further traversal with `depth` |

Traversal sorts each completely collected directory and visits entries in that order. Entry accounting includes excluded and unsupported names, bounding traversal independently of file count. Private/generated paths (`.git`, `.act0r`, `.act0r-tools`, `.build`, `target`, `node_modules`, `.aws`, `.env`, and `.env.*`) follow existing case-insensitive exclusions. Invalid UTF-8 and text containing NUL count as binary. Special files, non-UTF-8 filenames, and nonportable discovered paths count as unsupported and are not opened. An encountered symlink or reparse point in permitted traversal refuses the search rather than following it.

Check `truncated`, `limits_hit`, and `skipped` before treating an empty result as evidence of absence. A truncated result contains earlier matches but does not establish completeness. Narrow the scope to resolve broad-search limits. Use `read_file` before editing; snippets and saved results are historical observations.

<a id="workspace-search--durability-and-controls"></a>

### Durability and controls

The pinned contract uses `native.tool` v3, `reports_errors: true`, and `SafeToRepeat`. It requires no approval and receives no writable paths or test command. Expected invalid query/limit/path and inspection refusals become saved `ok:false` feedback. Authority errors remain execution failures; ownership contention and temporary I/O retain retry semantics. Read-only search creates no filesystem journal.

SQLite atomically saves the result, context selection, and exact next model request. Restarts reuse committed results even when files change. Duplicate completion adds no continuation. An interrupted **uncommitted** search may safely rerun after its saved retry deadline, retaining the activity ID with a new attempt; it can observe newer content. Saved search is not a filesystem snapshot. Runtime tools share workspace ownership, but unrelated processes can still change files during traversal; this is not an OS sandbox.

[Bounded context](state.md#native-context) applies to complete search observations. A result that cannot fit alongside essential prompt material remains saved while the Run fails with `context_limit`; tighten search limits or increase the pinned context. Opt-in [result recall](planner.md#result-recall) retrieves earlier searches within the same Run without repeating them. Search does not invalidate or refresh test status. Actor FIFO, goal/Run controls, delegation budgets, activity attempts, and recovery retain their contracts.

<a id="workspace-search--rust-ports-and-compatibility"></a>

### Rust ports and compatibility

`domain::tools::WorkspaceSearch` holds serializable arguments and pure validation. `WorkspaceReader::search_text` is the read-only application boundary. Its default reports unsupported search, keeping existing inspection-only custom adapters source-compatible. Custom hosts install `SearchTextTool::new(reader)` after implementing the port.

The full `agent_tool_registry` and local bootstrap include search. Earlier inspection/edit/test constructors retain their catalogs. `LocalTools` implements bounded traversal behind the port and shares managed-root validation, exclusion rules, and locking. Workflow versions, existing tool pins, and SQLite remain unchanged.

Tests cover literal/Unicode matching, scoped/empty results, all scan/output limits, private/generated/binary/oversized exclusions, linked paths, argument/scope/authority refusals, ownership contention, both policies, real fixed-process tests, recalled evicted search results, pause/cancel/exhaustion, completion rollback/duplicates, CLI discovery, and killed workers before and after committed results. Process-kill checks run locally on Windows; Unix execution and physical power-loss validation remain separate work.

<a id="workspace-tests"></a>

## Durable native test execution

`tool` v1 and the existing `planner` v3 can use `run_tests` v1 alongside native file inspection and editing. The host pins one executable, argument vector, and timeout at actor creation. Each call requires a saved approval. Completed test output survives restart and nonzero exit status becomes feedback for the next model decision.

<a id="workspace-tests--try-it"></a>

### Try it

```powershell
cargo run --locked --example workspace_tests -- .build/workspace-tests-demo/demo.db
cargo test --locked --test workspace_tools native_tests
cargo test --locked --test cli cli_native_tests
```

The demo creates a tiny dependency-free Rust project and runs actual `cargo test --locked --offline` processes. Its scripted planner reads, edits, receives a failing test result, repairs, and receives a passing result. Ten evidence reads precede the repair and ten follow it, exercising a 10000-byte/two-observation context window. It restarts after each saved test result, verifies the next request and usage, retains compact test status after output eviction, delegates review, and finishes. Both test activities have one attempt; 28 model calls cover planner and reviewer. The original source stays unchanged. The demo explicitly approves the calls for its own fixture; ordinary workers stop for approval.

Integration checks also choose a corrective edit from saved failure output, construct the delegated goal from saved passing output, and preserve a delegated human wait across restart. Parent tests kill real worker processes at approval, queued intent, admitted intent, during a subprocess, after execution without commit, and after result commit. Ignored fixtures are invoked by those parent tests.

<a id="workspace-tests--configure-an-actor"></a>

### Configure an actor

Supply a source directory at actor creation and add these fields to its tool or planner configuration:

```json
"tools": [
  { "name": "read_file", "version": 2 },
  { "name": "replace_text", "version": 1 },
  { "name": "run_tests", "version": 1 }
],
"writable_paths": ["src/"],
"test_command": {
  "argv": ["cargo", "test", "--locked", "--offline"],
  "timeout_ms": 60000
}
```

`test_command` is required when a selected tool needs it and rejected when unused. `argv` contains 1–128 strings totaling at most 16 KiB, requires a nonblank executable, and refuses NUL bytes. Empty arguments after the executable are allowed. `timeout_ms` defaults to 60,000 and accepts 1–600,000 ms. Validation precedes workspace copying. Existing configs and tools omit the new optional fields and retain their exact pins and prompts.

The model calls only:

```json
{"kind":"call_tool","tool":"run_tests","arguments":{}}
```

The command is visible in the saved model prompt and approval payload, but the model cannot supply or override it. Task messages may replace the goal; they do not replace commands, timeouts, tool allowlists, writable paths, or workspace roots. Later tasks and planner follow-ups retain the actor configuration. Delegated actors use their own configuration. No shell is inserted: pass the executable and each argument separately. Bare executable names resolve through the host's PATH; use an absolute executable path when that matters.

Use the existing CLI sequence: `actor agent create tool --source PROJECT --config-file CONFIG`, `actor send`, then `worker run --until-idle`. Inspect `actor task status` to obtain Run and wait IDs. Approve with `run approve RUN_ID WAIT_ID`, or deny with `--deny`, then run the worker again. `actor tool list` includes the new contract. Planner actors also require their existing worker aliases and delegation limits; the demo saves a full configuration beside its database.

<a id="workspace-tests--saved-results-and-bounds"></a>

### Saved results and bounds

A completed process returns:

```json
{
  "ok": false,
  "exit_code": 101,
  "stdout": "...",
  "stderr": "...",
  "output_truncated": false,
  "stdout_truncated": false,
  "stderr_truncated": false,
  "elapsed_ms": 250
}
```

`ok` reports exit success; `exit_code` may be null when the OS supplies no code. A failed test is a successful activity with an `ok:false` result. It consumes its activity attempt and the next model call consumes the usual model/token allowances. The agent can repair, request another test run with a new approval, ask for help, or explain the failure.

Each pipe is drained concurrently and retains at most 16 KiB of raw bytes. Invalid UTF-8 is replaced; serialized results allow 256 KiB to accommodate replacement characters and JSON escaping. Truncation is explicit per pipe and in aggregate. Context limits still apply to the saved output in the next model request; a context failure preserves the test result. [Bounded native context](state.md#native-context) keeps compact latest-test evidence when older full output leaves the window, and marks it stale after a successful write. Elapsed time uses a monotonic clock and measures spawn, execution, and pipe collection. It is result metadata, not a durable workflow timer.

<a id="workspace-tests--recovery-controls-and-process-limits"></a>

### Recovery, controls, and process limits

Intent commits before dispatch through `native.tool` v5. Approval pins the exact tool, empty arguments, recovery policy, managed workspace, and command. Result, workflow observation, history, and next activity commit together. A transaction failure can be retried with the same completion without rerunning the process. Duplicate approvals and completions are idempotent. Restart reuses committed results and exact pending requests.

Tests declare `RequiresReconciliation`: test programs can create files or other effects. An admitted attempt without a saved result becomes indeterminate and blocks its Run. Spawn/I/O errors and timeouts retain uncertainty. Inspect effects and verify the prior process has stopped before using `run reconcile` to accept a verified result, fail, or authorize one retry. A retry retains the activity ID, command, workspace, and existing budgets, with a new attempt ID. The engine never infers a result from current files or automatically reruns an uncertain test.

The adapter holds workspace ownership throughout execution. Lock contention proves no dispatch and follows the existing bounded retry policy. The pinned timeout covers process and pipe collection; on timeout or collection failure the adapter stops the command process tree before releasing the workspace lock. Normal parent exit also stops background children before output collection completes. The CLI accounts for that deadline when choosing its outer activity timeout. Native Windows processes are hidden. Environment variables ending in `_API_KEY` are removed from the child environment, matching existing coding tests.

These are trusted host processes, with the host user's filesystem/network authority. Managed-directory checks and edit allowlists do not sandbox a test program. Other environment variables remain inherited. Windows uses a job assigned before the suspended command resumes; closing its owning handle kills descendants, including after abrupt host exit. Unix uses a separate process group with cleanup on normal exit, timeout, errors, or future drop; deliberate detachment and abrupt host SIGKILL still need operator cleanup. See [process lifetime and named commands](#project-commands--recovery-and-process-lifetime). Unknown effects always require inspection and reconciliation.

Goal/Run pause and cancellation act at activity boundaries. A saved approval during a pause does not dispatch until resumed. In-flight results can commit under a hold or cancellation; later work obeys those controls. Attempt limits, model/token limits, shared planner goal allowances, and input context limits remain enforced. Completed results survive exhaustion and restart.

<a id="workspace-tests--ports-and-adapters"></a>

### Ports and adapters

Coding-session recovery inspection exposes interrupted `run_tests` calls. The exact pinned argv/deadline and interrupted attempt remain inspectable. There is no saved output for an abandoned attempt, so the view reports an unknown test outcome. Both retry and failure require confirmation that the previous operation and any test process have stopped. Retry executes the same command again and records a new attempt/output; the API cannot accept invented test results. Saved decisions survive reload/restart and cannot authorize a later interrupted attempt. See [tool recovery](harness.md#coding).

The domain owns `WorkspaceTestCommand`, validation, versioned tool requirements, and approval/activity decisions. `WorkspaceTestRunner` is a separate application port; `RunTestsTool` maps it into the native registry. `LocalTools` provides managed-directory validation, shared workspace ownership, bounded subprocess capture, and timeout handling. The composition root installs all three workspace capabilities using `tested_workspace_tool_registry`; inspection-only and edit-only registry constructors remain available to custom hosts. SQLite uses the shared baseline; no additional queue or planner was introduced.

<a id="workspace-changes"></a>

## Saved changes for native actor tasks

Native `tool` v1 and `planner` v3 actors can save a task-specific workspace patch with test evidence. Enable `"change_review": true` in the actor configuration and provide the usual source workspace and tool allowlist. The runtime captures a baseline before the first model call and saves the final change set after the model chooses to finish, including a finish that schedules a follow-up.

```json
{
  "change_review": true
}
```

This is an additional field in the existing actor configuration, not a standalone configuration. It requires a managed workspace. Task messages cannot change the pinned setting, workspace, or tool permissions. Existing actors without this setting retain their requests, activity sequence, workflow versions, and recovery behavior.

<a id="workspace-changes--run-the-example"></a>

### Run the example

```powershell
cargo run --locked --example workspace_changes -- .build/workspace-changes-demo/demo.db
cargo test --locked --test workspace_changes
```

The [example](../examples/workspace_changes.rs) creates a tiny Rust project, captures its baseline, restarts, searches for a function, repairs it, creates a text file, and runs the fixed Rust tests. It saves a patch, restarts again, verifies the exact artifact, and exports it beside the database. It uses the scripted model and actual `cargo test --locked --offline`; no model service or credentials are needed. The fixture owns and approves its edit/test operations. The original source stays unchanged.

<a id="workspace-changes--inspect-and-export"></a>

### Inspect and export

Use the physical actor task handle printed when submitting the task:

```powershell
cargo run --locked -- --db agent.db actor task changes TASK_ID
cargo run --locked -- --db agent.db --json actor task changes TASK_ID
cargo run --locked -- --db agent.db actor task changes TASK_ID --output review.patch
```

Text output shows the changed-file count, changed-byte count, test evidence status, and unified patch. JSON includes the task and Run identities, current Run status, pinned feature selection, saved artifact, and any execution error. `--output` writes the exact saved patch to a new file; its parent must exist and an existing destination is refused. The export never applies changes to the original project.

The read API is `agents.actors()?.task_changes(task_id).await?`. `WorkspaceChangeSet::patch()` concatenates its saved per-file patches in sorted path order. Reading or exporting uses one SQLite snapshot and no live filesystem inspection. Deleting or editing the workspace after completion cannot rewrite the saved artifact. A hash-matching test status refers to the captured workspace contents, not the workspace's current contents.

Each task gets a baseline when its Run executes, even when several queued tasks use one actor's persistent workspace. A second task starts from the changes left by the first; its artifact contains only its own observed differences. Delegated tasks and follow-ups have their own physical task handles. Use [goal inspection](state.md#task-inspection) to find them; this first slice does not merge multiple workspaces into one patch.

A successfully completed task's artifact can also be [applied through a separate durable preview and approval](#patch-application). The application checks target hashes and journals per-file progress; reading/exporting the patch still performs no target writes.

<a id="workspace-changes--test-evidence"></a>

### Test evidence

For built-in `run_tests` v1, durable fingerprint activities bracket the approved fixed command. The tool's completed result, activity ID, exact host-pinned command, and hashes remain saved while the loop continues. Finalization compares those hashes with the captured workspace:

| Status | Meaning |
| --- | --- |
| `not_run` | No completed built-in test call was recorded in this Run |
| `matching` | Included project content had the same hash before the latest test, after it, and at final capture |
| `stale` | A test changed included project content, or content changed before final capture |

`matching` describes the content relationship. Check `tests.result.ok` and `exit_code` for success; a failed command can have matching evidence. Only the latest completed built-in test call is attached. Hashing uses the same project-file inclusion rules as snapshots, excluding private, dependency, and build directories. External dependencies, environment variables, and ignored files are outside that hash. Arbitrary custom test tools are not automatically qualified. This attaches existing test evidence and performs no independent final test execution.

<a id="workspace-changes--durable-checkpoints-and-limits"></a>

### Durable checkpoints and limits

Opt in to [verified completion](#verified-completion) with `"completion_policy": "require_passing_tests"` to require matching, successful pinned tests before `finish` or `schedule_follow_up` can succeed. Refusal saves a candidate and feedback for another model turn; explicit `finish_incomplete` saves partial work with a failure reason. The optional inspection report distinguishes verified, refused, and incomplete captures. Ordinary change review retains its existing behavior.

The pure policies issue `workspace.baseline` v1, `workspace.fingerprint` v1, and `workspace.changes` v1 activities. They use existing intent/result transactions, controls, retry deadlines, and attempt limits. They reserve no model calls or tokens. Budget for two additional activities per completed test plus baseline and final capture; retries consume further attempts.

The local adapter holds existing workspace ownership, verifies the managed root, and reuses the coding-task snapshot, manifest, and unified-diff implementations. Baselines use immutable snapshot directories keyed by the baseline activity ID. Private receipts bind operation, Run, activity, exact request, and result. A retry after publication reuses the receipt rather than inspecting a later workspace state. Final artifacts live in committed activity results; no new SQLite table or migration is needed.

Snapshots and fingerprints preserve the existing 10,000-entry/64 MiB project bounds, private exclusions, and link/reparse-point refusals. Changed files must have portable paths and UTF-8 text without NUL bytes, with a 1 MiB limit per before/after file. Total patch text is at most 4 MiB and the serialized artifact is capped at 32 MiB. Added, changed, and removed files carry before/after SHA-256 hashes. Empty added or removed files use Git mode headers. Permissions, executable bits, and explicit rename tracking are outside this content-only patch contract. Unsupported or excessive changes fail capture rather than exporting a partial patch.

Baseline/fingerprint/final capture is repeat-safe under its saved receipt. Interrupted test execution retains `RequiresReconciliation`; restart cannot silently rerun it to obtain a patch. Existing [reconciliation](recovery.md#reconciliation), approvals, goal holds/cancellation, and allowances apply. A task that stops before final capture has no final artifact; inspection exposes its state/error. An already-admitted capture can still save its outcome at the existing control boundary.

<a id="workspace-changes--ports-and-verification"></a>

### Ports and verification

`domain::change_review` owns change-set validation, test evidence, and shared deterministic checkpoints. `WorkspaceChangesPort` owns filesystem execution; `LocalWorkspaceChanges` implements it through existing workspace and snapshot adapters. `WorkspaceChangesReadPort` exposes committed per-task artifacts; `ActorService` and the CLI use its SQLite implementation. The default bootstrap wires both ports. A custom host installs the execution port with `AgentExecutor::with_workspace_changes` and the read port with `ActorService::with_changes_reader`.

Consumer checks cover both native policies, queued task baselines, immutable results and duplicate completion, matching/failed/stale/absent evidence, source mutation by tests, holds/cancellation/attempt exhaustion, unsupported content, CLI export and overwrite refusal, and killed hosts before SQLite completion of baseline/fingerprint/final capture and after artifact completion. Killed test attempts require explicit acceptance of verified output. Local process-kill checks validate Windows application crashes; power-loss and Unix validation remain separate work.

<a id="patch-application"></a>

## Apply a reviewed task to a project

A successfully completed native actor task with `change_review: true` can be applied to an explicitly chosen target project. Application creates a separate durable Run. Its worker prepares an immutable preview, waits for explicit approval, and applies the saved patch through a per-file journal. The original actor task and artifact remain unchanged.

<a id="patch-application--cli"></a>

### CLI

```powershell
# Creates an application Run and prints APPLICATION_RUN_ID.
cargo run --locked -- --db agent.db actor task preview-changes TASK_ID --target .\project
cargo run --locked -- --db agent.db worker run --until-idle
cargo run --locked -- --db agent.db actor task application APPLICATION_RUN_ID

# Approves the saved preview; the worker executes the application.
cargo run --locked -- --db agent.db actor task apply-changes APPLICATION_RUN_ID
cargo run --locked -- --db agent.db worker run --until-idle
cargo run --locked -- --db agent.db actor task application APPLICATION_RUN_ID
```

`actor task reject-changes APPLICATION_RUN_ID` finishes a waiting preview as `rejected`. Commands support `--json`. A Run can succeed with an application report of `conflicted` or `rejected`; inspect `report.status` to distinguish these from `applied`. Conflicts show affected paths, expected hashes, observed hashes, and reasons. A preview with conflicts schedules no writes and cannot be approved. Resolve the conflict and create another preview.

The Rust API is `actors.patch_applications()?`, returning `PatchApplicationService`. Use `preview(task_id, absolute_target).await?`, `status(application_run_id).await?`, and `approve(application_run_id, true_or_false).await?`. The CLI resolves relative targets against its current directory before persisting them. A Rust host must provide an absolute UTF-8 target path.

Try the provider-free Rust example and focused checks:

```powershell
cargo run --locked --example workspace_apply
cargo test --locked --lib patch_applications --test patch_applications
```

<a id="patch-application--preview-and-conflicts"></a>

### Preview and conflicts

Preparation uses the committed task artifact, never the mutable actor workspace. It reconstructs the canonical saved patch against matching target bytes, checks hunk context/counts and before/after SHA-256 hashes, and persists the original and replacement text in a synced preparation receipt. Removing the agent workspace or baseline after task completion does not prevent application. Retrying preparation reuses its exact saved preview; subsequent target drift is checked when applying.

Only affected files must match the task's baseline. Unrelated target changes remain untouched. Existing-file edits/deletions require the exact before hash; additions require an absent target. Empty files, CRLF, UTF-8, and missing final newlines preserve their exact bytes. A matching replacement already present before application is a conflict, rather than evidence that this operation ran.

The target must be an existing ordinary directory and cannot itself be a managed actor workspace. File parents must already exist. Absolute/traversing/private file paths, case aliases, links/reparse points, nonregular files, binary text, oversized content, and unsupported patch encodings are refused. Changed files remain limited to 1 MiB each; preparation permits 32 MiB of original/replacement content and a 64 MiB serialized receipt. Existing review bounds still apply. The first slice does not create parent directories, merge task artifacts, or resolve textual conflicts.

Preview includes the original task's latest saved test evidence, including its actual result. `matching` still describes the captured actor workspace, and may accompany failed tests. Application checks affected-file hashes; it does not establish that the entire target or its external environment matches the tested workspace, and does not rerun tests. Choose whether that evidence is sufficient before approving.

After a harness application reports **Applied**, project verification is available as a separate durable API operation. It reviews the session's pinned verifier against a frozen original-project snapshot, then saves its actual output, exit status and before/after hashes. See [project verification](harness.md#project-verification) for approval and recovery.

<a id="patch-application--journal-and-interruption"></a>

### Journal and interruption

`workspace.apply` v1 schedules `workspace.apply.prepare` v1 with `SafeToRepeat`, then a durable `patch_application_approval` wait. Approval schedules `workspace.apply.write` v1 with `RequiresReconciliation`. No model allowance is charged; normal Run attempt limits and pause/cancel controls remain in force. This separate operation has its own Run, rather than inheriting an already finished actor goal's lifecycle or compute budget.

Apply takes a project ownership lock shared by applications targeting the same project, rechecks the target directory identity, and checks every affected file before the first project-file write. Conflicts at that point return `conflicted` with no project-file changes. Approved execution creates `.act0r-tools/patch-application.lock` in the target for ownership; preview does not write inside the target.

Original/replacement bytes are retained under `.act0r/applications/PREPARATION_ACTIVITY_ID/prepared.json` beside the database. `write.json` binds the application Run, write activity, and exact approved request. It saves whole-operation intent, then each file's `pending`, `intended`, or `applied` progress. Original bytes remain available for operator inspection. File replacements use synced temporary files and atomic replacement; additions refuse overwrites. Existing-file permissions are preserved during replacement. Deletion unlinks the named file. Completion verifies affected hashes and publishes a final immutable receipt before saving the SQLite activity result.

An interrupted write blocks the Run. Use `run status APPLICATION_RUN_ID` and the journal to inspect the latest interrupted attempt, then the existing [reconciliation API](recovery.md#reconciliation). A `retry` decision explicitly permits the adapter to continue the same saved operation: pending files must have before hashes, intended files may have before or after hashes, and applied files must have after hashes. Conflicting partial state blocks again without overwriting the conflict. Once the final receipt has been published, an authorized retry returns that receipt even if files have subsequently changed; the receipt records the completed operation, not the target's current state.

This is a recoverable multi-file operation, not a filesystem transaction: interruption, cancellation of an admitted attempt, or I/O failure can leave a prefix applied. There is no automatic rollback or force-overwrite command. Reconciliation failure or cancellation preserves the journal and existing effects. The project lock coordinates harness applications; external editors do not participate. Checks refuse observed drift, but do not provide protection against an actively racing directory replacement. Windows directory identity uses creation time; Unix uses device/inode. Changes to executable bits, explicit renames, and other metadata remain outside the content-only artifact contract.

Coding sessions expose write recovery through the [harness API](api.md#http-api--prompt-first-harness). Application status includes the latest interrupted write attempt, read-only journal/hash inspection and durable decision receipts. A separate `PatchApplicationInspectionPort` reads saved preparation and journal state without creating directories or scheduling work; filesystem inspection failures stay visible without hiding the Run. API retry/fail decisions call the existing Engine reconciliation transaction and identify the exact interrupted attempt. The API does not accept arbitrary replacement results. Inspection is advisory; execution rechecks all affected hashes, preserves journal-owned applied files and retains saved final receipts.

<a id="patch-application--ddd-boundaries-and-verification"></a>

### DDD boundaries and verification

`domain::patch_application` owns the pure preview/approval/application workflow and validation. `PatchApplicationService` loads committed task changes through `WorkspaceChangesReadPort` and uses the existing Engine/ExecutionStore transactions. `PatchApplicationPort` owns preparation and journaled filesystem execution; `LocalPatchApplications` implements it. Local bootstrap installs the port and workflow through `AgentExecutor`. No new scheduler, actor policy version, or SQLite migration is needed.

Consumer checks exercise tool and planner actors, workspace removal, approval/rejection, exact saved inputs/results, restart, conflicts before and after approval, competing previews, bounds/path refusals, pause/cancel/attempt caps, and CLI use. Real killed hosts cover preparation publication, whole-operation intent, file intent, file effect before progress persistence, file progress persistence, and final receipt before SQLite completion. Partial-state conflicts require explicit resolution and another reconciliation decision. Local process-kill coverage concerns Windows application crashes; physical power loss and Unix execution remain separate validation.

<a id="verified-completion"></a>

## Verified completion for native actors

Native `tool` v1 and `planner` v3 actors can require test verification before successful completion. Add these fields to the existing actor configuration and provide a source workspace when creating the actor:

```json
{
  "completion_policy": "require_passing_tests",
  "change_review": true,
  "tools": [{ "name": "run_tests", "version": 1 }],
  "test_command": {
    "argv": ["cargo", "test", "--locked", "--offline"],
    "timeout_ms": 60000
  }
}
```

Include other permitted tools, writable paths, model settings, budgets, and planner workers as usual. The adapter validates the requirements before allocating the managed workspace. The policy and command are pinned at actor creation; task messages and model actions cannot disable them or supply different tests. Actors without this optional field keep their existing schemas, requests, and completion behavior. The `coding` adapter keeps its existing completion rules.

<a id="verified-completion--what-qualifies-as-success"></a>

### What qualifies as success

After `finish` or `schedule_follow_up`, the existing durable `workspace.changes` checkpoint captures the candidate patch. Successful completion requires all of the following:

- This Run completed the permitted built-in `run_tests` v1 activity with the exact pinned executable, arguments, and timeout.
- Its latest result has `ok: true` and integer `exit_code: 0`.
- Included project content has identical hashes before the test, after the test, and at the final capture.

The check attaches existing evidence; it does not launch another test automatically. A failed test may have matching hashes and still refuse completion. Missing or malformed success flags are unknown. A test that changes included content makes its own evidence stale. Edits or child imports after testing also require fresh evidence. Child test results verify the child's candidate; they cannot satisfy the parent's combined-workspace check.

The policy verifies the chosen command and captured project content. Test selection and coverage remain the host's responsibility. Environment variables, external dependencies, private files, and excluded build/dependency directories are outside the existing snapshot hash. A verified report refers to the immutable captured artifact; later manual changes to the live workspace do not change it. Applying a patch to another project remains a separate approved operation.

<a id="verified-completion--durable-refusal-and-incomplete-work"></a>

### Durable refusal and incomplete work

A failed completion check saves the candidate and a structured `refused` assessment, then supplies bounded feedback to the next model request. Reasons are `tests_not_run`, `tests_stale`, `wrong_test_command`, `tests_failed`, or `unknown_test_result`. The actor may repair, retest, delegate against its current code, or ask for clarification. Its existing Run, model turn, context window, attempt limit, goal allowance, and shared delegation budget continue; refusal does not reset them.

The enabled action schema also permits:

```json
{
  "kind": "finish_incomplete",
  "summary": "The repair is saved, but the combined tests still fail."
}
```

The summary must be nonempty and at most 60000 UTF-8 bytes. This action captures the final patch and stops the Run as `failed` with error code `coding_incomplete` and the summary as its message. The assessment says `incomplete`, and its reason describes any test deficit. It publishes no successful result or follow-up, adds no successful actor memory, and cannot supply an importable child artifact or an operator-applicable successful patch. The saved patch can still be inspected or exported for manual review.

Budget exhaustion and cancellation retain their existing statuses. If they occur after a refusal, the earlier immutable candidate remains inspectable with its refusal report. A stop before any capture has no patch. Context limits still apply when preparing the next request. An already admitted capture may finish under cancellation; if its policy continuation did not execute, it has no completion assessment. Inspection never attaches an older candidate's assessment to it.

<a id="verified-completion--inspect-and-run"></a>

### Inspect and run

`agents.actors()?.task_changes(task_id).await?` and `actor task changes TASK_ID` with the global `--json` option expose the optional `completion` report alongside the patch and current Run status. Reports bind the capture activity ID, workspace digest, and test activity ID. Text inspection prints the status and reason. `--output review.patch` can export a refused or incomplete candidate; the report and Run status distinguish it from verified success. Earlier captures and exact next model requests remain in durable Run history.

```powershell
cargo run --locked --offline --example verified_completion
cargo test --locked --offline --test verified_completion
```

The scripted example uses real Rust tests and needs no model service. It writes and loads a [native team](teams.md#agent-teams) JSON configuration, reusing the same team and actor IDs after restart. Two module actors execute concurrently and pass their own filtered tests. The planner imports both patches, fails the combined test, and attempts to finish. The runtime refuses it and saves the feedback. After restart, a third repair actor receives a fresh snapshot of the integrated code, fixes the combined contract, and returns a verified patch. The planner imports it, retests, and succeeds under one 21-call goal allowance. The example owns and approves its fixture operations and exports the final patch beside its database.

<a id="verified-completion--domain-and-adapter-boundaries"></a>

### Domain and adapter boundaries

`domain::completion` owns pure evidence assessment, configuration requirements, and the incomplete action contract. `domain::change_review` makes the authoritative decision inside the existing capture continuation. Native tool and planner policies handle the internal refusal event through their existing context and model scheduling. Run applies a deliberate terminal failure only at a quiescent boundary.

Filesystem execution remains behind `WorkspaceChangesPort` and the existing test port. `WorkspaceChangesReadPort` projects the saved report and artifact through SQLite without inspecting the live workspace. Capture outcome, assessment, and next command commit in the existing transaction; repeat-safe capture receipts and ordinary unsafe-test reconciliation remain in force. Optional state/config fields require no migration, new planner, scheduler, or actor implementation.

Consumer tests exercise both policies, failed/missing/stale/unknown evidence, external drift before capture, pinned configuration, bounded incomplete summaries, budget exhaustion, goal holds and cancellation, patch retention and import refusal, actor memory, follow-up gating, Responses and Chat Completions fixtures, duplicate outcomes, and killed hosts before capture commit or after refusal commit. These are local Windows application-crash checks; power-loss durability and Unix execution remain separate validation.

<a id="project-commands"></a>

## Durable named project commands

Coding sessions can give the native planner `run_command` v1 alongside `run_tests` v1. Configure names and fixed executable/argument arrays when attaching a project; the agent selects a name, then waits for your approval. This enables independent format, lint, and build steps in the session's isolated project copy.

<a id="project-commands--configure-a-session"></a>

### Configure a session

Set `project_commands` in a coding actor configuration, or `coding.project_commands` in a harness request. For example:

```json
{
  "format": { "argv": ["cargo", "fmt"], "timeout_ms": 60000 },
  "lint": { "argv": ["cargo", "clippy", "--locked", "--offline"], "timeout_ms": 120000 },
  "build": { "argv": ["cargo", "build", "--locked", "--offline"], "timeout_ms": 120000 }
}
```

Configure commands appropriate to your project and host. Executables run directly with each argument passed separately; the runtime adds no shell. Bare names resolve through the host's PATH. Windows `.cmd`/`.bat` scripts require an explicitly configured shell invocation, whose entire argument vector is approved. Use an absolute executable path when resolution must be fixed.

The HTTP session request puts this object under `coding.project_commands`, next to `writable_paths` and the existing mandatory `test_command`. For a custom native planner/tool actor, include `{"name":"run_command","version":1}` in `tools` and put `project_commands` in its configuration. A nonempty catalog and that tool are required together. The default harness helper stays read-only and receives no command capability. Follow-up messages cannot replace the catalog.

Limits: at most 16 names, each 1–64 ASCII letters/digits/underscores/hyphens; the serialized catalog fits 32 KiB. Each command has 1–128 argv entries totaling at most 16 KiB, a nonblank executable, no NUL bytes, and a timeout of 1–600000 ms (default 60000). Invalid configuration is rejected before workspace copying. Omitting the catalog preserves existing tool pins, prompts, and session retry behavior.

<a id="project-commands--model-actions-approval-and-results"></a>

### Model actions, approval, and results

The model receives the pinned catalog and a schema limiting the name to its configured choices. It calls:

```json
{"kind":"call_tool","tool":"run_command","arguments":{"name":"format"}}
```

Extra model arguments, unknown names, and missing pinned context are refused. Approval saves the selected name, exact argv/deadline, managed workspace, and reconciliation policy. The saved approval exposes those values. Approvals are per call; the session's optional test-command permission does not authorize project commands. Denial does not dispatch a process or invalidate test evidence.

After approval, the runtime saves one `native.tool` v6 activity intent containing the resolved selection, then executes through `WorkspaceCommandRunner`. Exit code, bounded stdout/stderr, truncation flags, elapsed milliseconds, and `command_name` are saved with the observation and next model request. Nonzero exit is completed `ok:false` feedback, allowing repair. Output bounds match [native tests](#workspace-tests): 16 KiB retained per pipe, concurrent draining, explicit truncation, and a 256 KiB serialized result ceiling. Model context and usage limits still apply.

Every admitted command may change files, even if it fails or exits without changing the captured digest. Scheduling it clears verified test evidence; its completed result marks compact `test_status` stale. Only a fresh, passing `run_tests` result matching the final workspace capture can verify successful completion. Format/lint/build output never substitutes for tests. Change review captures the resulting project edits; applying a patch to the original project still needs separate approval.

<a id="project-commands--recovery-and-process-lifetime"></a>

### Recovery and process lifetime

The catalog, pending approval, exact activity request, and completed result survive restart. Duplicate approval/completion delivery keeps the original intent/result. A completed command is never replayed to rebuild its observation.

An admitted execution without a saved result, spawn/I/O failure, or timeout becomes indeterminate under `RequiresReconciliation`. Recovery inspection reports an unknown command outcome and the exact saved request. Confirm the prior operation and processes stopped, inspect any partial effects, then retry that same command or mark the turn failed. Retry creates a new attempt under the existing activity and budgets. Recovery cannot supply invented output or infer success from current files. Saved decision receipts remain idempotent across restart; an old decision cannot resolve a later interruption.

Commands and tests hold workspace ownership while running and during explicit cleanup. On Windows the runner starts the process suspended, assigns it to a job with no breakaway flags and `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, then resumes it. Timeout, collection error, normal parent exit, and closing the owning job stop descendants; explicit cleanup waits for the job's active count to reach zero. Abrupt host exit also closes its non-inherited job handle. This follows Microsoft's [job object lifetime rules](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects).

On Unix the runner creates a separate process group and kills that group on timeout, collection error, normal parent exit, or future drop. Detached processes that deliberately leave the group and abrupt host `SIGKILL` require operator inspection and cleanup. The worker gives process execution a cleanup grace beyond its configured deadline. Pause/cancel retain existing activity-boundary behavior: already admitted operations settle; subsequent dispatch obeys holds and cancellation.

These are trusted local programs with the host user's filesystem/network authority. Writable-path permissions govern native file tools, not arbitrary effects of executable code. Process lifetime management is not a sandbox. Environment variables ending in `_API_KEY` are removed from the child environment; other variables are inherited, and Cargo output goes to the managed copy's own `target` directory.

<a id="project-commands--verify-locally"></a>

### Verify locally

```powershell
cargo test --locked --test project_commands
cargo test --locked --test http_api project_commands
```

Fixtures use a local mock model and test executables, with no external model calls. Checks cover strict configuration/model arguments, exact approvals and results across restart, nonzero output bounds, required retesting, interrupted exact retries, descendant cleanup, cancelled futures, and abrupt Windows host exit. The Windows execution path is tested on the development host; Unix process-group execution requires a Unix host to validate.
