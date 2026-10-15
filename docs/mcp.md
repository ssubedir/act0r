# MCP tools

<a id="mcp-tools"></a>

Configure local stdio servers and their explicitly allowed tools once on the API/worker host. act0r discovers their names, descriptions, input schemas and optional output schemas through MCP and pins them into newly created tool/planner actors according to their workspace requirements. With `replace_inspection: true`, workspace actors use the MCP inspection tools directly instead of the built-in `read_file`, `read_files`, `list_files` and `search_text` tools. This includes the harness's project inspection, discovery and coding actors. Adding a supported tool requires configuration rather than a Rust wrapper.

For service queries or other tools that do not need project files, set `requires_workspace: false` in that tool's allowlist policy:

```json
{"name":"lookup","read_only":true,"requires_workspace":false}
```

These tools are available to the planner in ordinary `act0r chat` without `--project`, and to fresh tool actors with or without a project. Each call runs in an empty temporary working directory with no project context; an exact `{workspace}` command argument receives that temporary directory. File and directory parameter policies must be empty. The same durable activity records save contracts, arguments and results for restart recovery. Omitting `requires_workspace` retains the existing project requirement. Existing actors keep their pinned catalogs; changing a saved tool's policy or contract requires restoring its original definition to recover that work. General tools do not replace project inspection: `replace_inspection: true` still requires at least one workspace tool.

For service actions that make changes, explicitly configure both policies:

```json
{"name":"update_record","read_only":false,"requires_workspace":false}
```

Each action requires human approval, including in Auto mode. The CLI displays the server/tool identity and exact normalized arguments; absent optional fields represented by null in model requests are omitted before review and dispatch. The saved approval authorizes only that pending action. Denial returns to the agent without dispatch, and a later call needs a new approval. MCP actions currently receive no project authority; use native workspace tools for project mutations. An empty working directory is not an OS sandbox: configured server programs and their credentials remain trusted host configuration.

The project-discovery planner retains its inspection-only catalog. General service tools are added to normal planners and tool actors, including the coding phase after discovery.

Contracts, approvals, attempts and successful results are durable. A committed result is reused after restart. If execution is interrupted, or a dispatched action returns a connection, protocol, tool, schema or output-limit error, the runtime blocks with `uncertain_outcome` because effects may already exist. It does not automatically repeat that action. Inspect the service and ensure the previous operation has stopped, then use `run status RUN_ID` and `run reconcile RUN_ID --decision-file decision.json`. A decision identifies the saved `activity_id` and `attempt_id`, includes inspection `evidence`, and chooses `accept` with the verified MCP result (`ok`, `content`, and any `structuredContent`), `retry`, or `fail` with a message. Retrying is an explicit operator decision; MCP does not provide an exactly-once guarantee. A timeout or refusal before dispatch has no action effects and follows the normal failure policy.

Install Node.js 22 or later, ripgrep (`rg`), and the pinned MCP servers once on the host:

```powershell
npm.cmd install --prefix .act0r/mcp-filesystem --save-exact --ignore-scripts --no-audit --no-fund @modelcontextprotocol/server-filesystem@1.0.0 @atef_andrus/mcp-ripgrep@1.2.0
```

Save this host configuration as `.act0r/mcp.json`. Set `program` to the absolute Node executable and the first argument to the installed `.act0r/mcp-filesystem/node_modules/@modelcontextprotocol/server-filesystem/dist/index.js` path:

```json
{
  "replace_inspection": true,
  "servers": [{
    "name": "files",
    "command": {
      "program": "C:/path/to/node.exe",
      "args": ["C:/path/to/server-filesystem/dist/index.js", "{workspace}"],
      "timeout_ms": 30000
    },
    "tools": [
      {"name": "read_text_file", "read_only": true, "path_parameters": ["path"]},
      {"name": "read_multiple_files", "read_only": true, "path_parameters": ["paths"]},
      {"name": "list_directory", "read_only": true, "directory_parameters": ["path"]},
      {"name": "directory_tree", "read_only": true, "directory_parameters": ["path"]},
      {"name": "search_files", "read_only": true, "directory_parameters": ["path"]}
    ]
  }, {
    "name": "code",
    "command": {
      "program": "C:/path/to/node.exe",
      "args": ["C:/path/to/mcp-ripgrep/dist/index.js", "--allow-dir", "{workspace}", "--max-result-chars", "8000", "--max-output-bytes", "1048576"],
      "timeout_ms": 30000
    },
    "tools": [{"name": "search", "read_only": true, "directory_parameters": ["path"]}]
  }]
}
```

Start `act0r chat`, the CLI worker or the API: the host loads `mcp.json` alongside its database automatically. Chat defaults to `.act0r/chat.db`, so it loads the `.act0r/mcp.json` configuration above. Other CLI commands default to `act0r.db` in the current directory, with adjacent `mcp.json`; pass `--db .act0r/chat.db` to inspect or run the chat database from those commands. `ACT0R_MCP_CONFIG` may select another file and takes precedence; a separately launched worker using the same database picks up the same adjacent file. `actor tool list` includes the discovered `mcp_files_*` tools. Fresh workspace actors receive them without duplicate built-in inspection choices or steering instructions. No additional team imports are required. Existing actors retain their saved tool sets when configuration or the inspection mode changes. Built-in tools stay registered for their recovery. Omitting `replace_inspection` selects an additive tool catalog; an absent configuration retains local inspection.

Use [`act0r doctor`](harness.md#doctor) to check this configuration and its executable paths without starting servers or opening the database. It defaults to the chat database location too; use the same `--db` override as your intended command. Server handshakes and advertised tool contracts are checked by normal startup, rather than doctor.

The reference server's `search_files` finds filenames. The [ripgrep MCP server](https://github.com/AtefAndrus/mcp-ripgrep) supplies content search as `mcp_code_search`; its entry point is `.act0r/mcp-filesystem/node_modules/@atef_andrus/mcp-ripgrep/dist/index.js`. No search-specific Rust adapter is needed. The server's additional tools, including search/replace, are not allowlisted. This project inspection configuration enables only reads; edits, tests and approved patch application retain their existing journaled implementations.

Each selected tool needs an explicit `read_only` host policy; server annotations do not grant permissions or suppress action approval. Names default to `mcp_SERVER_TOOL`. An optional `alias` may select a shorter identifier beginning with `mcp_`; collisions fail startup. A configuration supports up to four servers and eight tools. The command runs directly with a cleared environment; an exact `{workspace}` argument is replaced with the scoped project root. For a filesystem server, supply that placeholder as its allowed-directory argument. A trusted server that does not need a directory argument may omit it; its working directory is still the scoped project copy.

A server may declare an explicit `env` object, with at most eight bounded literal values. For example, add `"env": {"PATH": "C:/path/to/ripgrep-bin"}` beside the `code` server's `command` when `rg.exe` is not on the launching host's PATH. The host otherwise inherits only OS process essentials. Explicit environment values are part of the opaque binding hash; changing them blocks recovery of the affected tool until its original binding is restored. The local installation can keep a copy of an existing `rg.exe` in `.act0r/mcp-filesystem/bin` and point this override there without changing the system PATH.

`path_parameters` identifies top-level string or string-array file arguments. The host accepts only existing relative regular files, rejects private/generated paths and links, limits each file to 1 MiB and each activity to eight paths, then sends their canonical managed paths. Arguments omitted or explicitly nullable remain consistent with the original schema. Declare every file-path parameter for a filesystem tool; configured path arguments support files rather than directory traversal. The server executable and tool policies are trusted host configuration.

`directory_parameters` identifies top-level directory arguments with the same string/string-array forms. Use `.` for the project root or an existing relative subdirectory. Before calling a directory tool, the host prepares a disposable project-only snapshot using the existing 10,000-entry / 64 MiB workspace-copy limits. Private/generated paths are excluded and links are refused, so recursive server inspection cannot read runtime journals or excluded files. File arguments on that same tool are mapped into the snapshot too. The server root and working directory point to the snapshot, which is removed after completion, deadline or cancellation. Returned absolute paths identify temporary files; use project-relative paths in subsequent calls. Complete outputs remain bounded by the saved 64 KiB tool contract.

The original schemas are saved in `ToolDefinition.external`, alongside an opaque hash of the selected server command and tool policy. They validate arguments and successful structured results; a separate strict model projection represents absent optional fields with null. Schemas must fit 32 KiB, have depth at most 32, and use self-contained declared object parameters. References and pattern properties are rejected; schema validation cannot retrieve network or filesystem resources. `kind` is reserved at the top level. The adapter supports text and structured JSON results, preserves their MCP fields, and adds `ok`; read-only `isError` results become saved `ok: false` observations. Complete results exceeding 64 KiB are never silently truncated; read-only calls fail and actions require reconciliation.

Startup discovery invokes no tools and uses an empty temporary root. Each activity then starts its own scoped server and rechecks the selected contract before dispatch. Changes found on restart block saved work: restore the original configuration and server contract, then resume the Run with the existing controls. A change discovered between startup and dispatch refuses that call with a saved execution error. A binding hash identifies configuration, not executable contents: compatible server implementations remain the host's responsibility. Adding another allowed tool does not change existing tool bindings. SQLite migration 62 guards the new serialized contracts without rewriting existing catalogs, activities or results.

Run `cargo test --locked --test mcp_catalog --test mcp_inspection -- --include-ignored` with Node.js for the configuration, schema, MCP-only discovery, harness, CLI, snapshot cleanup and restart tests. The metadata example, `cargo run --locked --example mcp_catalog -- METADATA_CONFIG`, expects a configuration whose first allowed tool is `get_file_info` with `path_parameters: ["path"]`; it changes the file after saving its result and reopens the runtime to verify the original result and exact continuation with one tool attempt. Rust hosts can use `bootstrap::local_app_with_mcp_tools(database, config)` or install the implementations returned by `mcp::discover_tools` in their own composition root. Both reuse the `NativeTool` port and durable activity registry.

<a id="mcp-tools--compatibility-file-read-bridge"></a>

### Compatibility file-read bridge

The earlier file-read bridge can offload `read_file` and `read_files` while preserving those exact action names and observation formats for existing integrations. New MCP-first setup uses the single configuration above and exposes server tools directly.

The adapter implements `WorkspaceReader` and uses the [official Rust MCP client](https://github.com/modelcontextprotocol/rust-sdk) for initialization, tool discovery and `tools/call` over stdio. SQLite still saves the tool intent, arguments, attempt and bounded result before the actor continues. A committed result is reused after restart; an interrupted, uncommitted read can repeat under the existing retry policy.

This first integration calls only `read_text_file`. A batched activity uses one server connection and reads each permitted file in request order. Listing, search, edits and commands retain their existing local implementations. Server-advertised write tools and annotations do not grant additional agent capabilities.

<a id="mcp-tools--compatibility-bridge-setup"></a>

### Compatibility bridge setup

For integrations that require the earlier `read_file`/`read_files` action names, set up the [reference filesystem server](https://github.com/modelcontextprotocol/servers/tree/main/src/filesystem) bridge from the repository root in PowerShell:

```powershell
New-Item -ItemType Directory -Force .act0r/mcp-filesystem | Out-Null
npm.cmd install --prefix .act0r/mcp-filesystem --save-exact --no-audit --no-fund @modelcontextprotocol/server-filesystem@1.0.0

$mcpConfig = @{
  program = (Get-Command node.exe).Source
  args = @((Resolve-Path .act0r/mcp-filesystem/node_modules/@modelcontextprotocol/server-filesystem/dist/index.js).Path)
  timeout_ms = 30000
} | ConvertTo-Json
$mcpConfigPath = Join-Path (Get-Location) '.act0r/mcp-filesystem.json'
[System.IO.File]::WriteAllText($mcpConfigPath, $mcpConfig, [System.Text.UTF8Encoding]::new($false))
$env:ACT0R_MCP_FILESYSTEM_CONFIG = $mcpConfigPath
```

Start `cargo run --locked -- worker run` or `cargo run --locked -- api serve --with-worker` from that shell, using the intended `--db PATH`. A separately launched worker needs the same environment variable. The server package is installed locally and pinned; activities do not download or install packages.

The configuration contains an absolute executable path, a bounded argument list and an optional 1–60000 ms deadline (default 30000 ms). On Unix use an absolute Node executable and server entry point with the same JSON fields. Do not put an allowed directory in `args`: act0r appends the actor's managed project copy for each invocation. The model cannot choose the command or root. The executable and its arguments are trusted host configuration, rather than an OS sandbox for arbitrary server code.

With no configuration, the existing local reader is used. A configured server failure produces saved feedback or an execution failure; it does not silently switch readers. Removing the environment variable restores the local implementation of the same contracts.

Rust hosts can inject the adapter explicitly with `bootstrap::local_app_with_mcp(database, config)` or supply `McpWorkspaceReader` when composing their own tool registry. The environment-selected adapter is installed by `local_app`, `local_engine` and `local_app_with_tools`.

<a id="mcp-tools--scope-and-results"></a>

### Scope and results

The adapter checks the managed-workspace marker, takes the existing workspace lock, rejects private/generated paths, traversal and links, and checks file sizes before sending a path. It gives each activity its own server process rooted at that workspace, so concurrent actors cannot change one another's roots. Only required OS environment values are inherited; model credentials and `NODE_OPTIONS` are removed. The process tree is owned by the activity and terminated on completion, deadline or cancellation.

Tool definitions, IDs and observation formats are unchanged. Files remain limited to 1 MiB, text to 16 KiB per file, and batches to eight files / 64 KiB total text. Order, duplicates, per-file errors and explicit truncation are preserved. The reference server replaces invalid UTF-8 with `�`; the adapter checks local bytes only when that character appears, preserving the existing strict UTF-8 behavior without rejecting a valid replacement character.

Tool discovery verifies the required `read_text_file(path: string)` interface before dispatch. Incompatible interfaces and malformed results fail execution; `isError` becomes bounded `mcp_read_refused` feedback. Connection loss and deadlines use the existing durable retries. The configuration is host wiring, not saved model input: install a compatible implementation on every restart, as with the local reader.

MCP write operations are deferred. Persisting a result alone cannot make an interrupted external write exactly once; those tools need explicit idempotency or reconciliation before they can replace the existing journaled writes.

<a id="mcp-tools--exercise-the-normal-auto-conversation"></a>

### Exercise the normal Auto conversation

The [Auto conversation trial](validation.md#auto-conversation-demo) uses the normal host wiring, including `ACT0R_MCP_CONFIG`. Allow `read_text_file` with `path_parameters: ["path"]` in the configuration above, then prepare its generated JavaScript project:

```powershell
$env:ACT0R_MCP_CONFIG = (Resolve-Path .act0r/mcp.json).Path
cargo run --locked --offline --example auto_conversation -- `
  --target-config examples/configs/benchmark-nvidia-agent.json --prepare-only
# Inspect GENERATED_DIRECTORY/first-request.json before making provider calls.
cargo run --locked --offline --example auto_conversation -- `
  --resume GENERATED_DIRECTORY --stop-after question
```

Keep the same host configuration when resuming. Inspect the saved native-tool activities and subsequent model observations to confirm which tools the model actually selected. In MCP-first mode, fresh actors have only the configured external inspection tools, so no steering is needed to choose MCP. Sessions configured with an additive catalog can also select built-in readers. The earlier NVIDIA trial below explicitly exercised MCP in such a session using the existing [turn steering](recovery.md#turn-steering) endpoint; its saved actors retain that catalog.

Resume with `--stop-after test-approval` to leave a separate executable boundary after discovery and the repair. The saved MCP activity IDs, pinned definitions, arguments, results and attempt counts must survive subsequent restarts unchanged. Then resume without a stop option to complete verification, approved application and the follow-up. Reopening the completed trial must add no calls or application. This exercises the configuration-driven tools; the existing file-read bridge can instead offload `read_file` and `read_files` without changing the model's chosen action names.

<a id="mcp-tools--nvidia-auto-trial--14-october-2026"></a>

#### NVIDIA Auto trial · 14 October 2026

NVIDIA `moonshotai/kimi-k3` completed the existing Auto conversation in **12 native-function model calls**, with two successful `mcp_files_read_text_file` activities against the installed reference filesystem server. Discovery initially selected the existing `list_files` and `read_files` actions. A saved steering instruction at its compatibility question then requested MCP reads of `README.md` and `src/math.mjs`. Both returned text and structured JSON successfully, including nullable `head`/`tail` arguments. `get_file_info` was advertised but not selected. This establishes explicit MCP selection through the real model and harness; it does not establish a default model preference for MCP.

The isolated repair, applied project and unchanged follow-up each passed all five real Node contracts. The trial retained one conversation, one approved application and the original compatibility answer. All five restart comparisons passed: question at three model calls, test approval at eight, application and project-verification approvals at nine, and completion at twelve. The executable also exited at the question and test-approval boundaries before resuming the same actors.

The two committed MCP activity records were compared in full before the test-approval resume, after completion and after another terminal invocation. IDs, arguments, schema/server pins, results and one attempt per call stayed identical. Terminal replay retained twelve model calls and byte-identical execution evidence, application receipt and project-verification report; no activity had a repeated attempt. The trial used the existing runtime without implementation changes.

Evidence is under `.build/auto-mcp-live/e39d62f8-62c0-42f6-b579-2e5f061e9b03/`: exact model requests, `host-mcp-config.json`, saved steering request/receipt, the standard Auto reports, `mcp-before-resume.json` and `mcp-replay-check.json`. These generated artifacts stay ignored.

<a id="mcp-tools--mcp-first-local-trial--14-october-2026"></a>

#### MCP-first local trial · 14 October 2026

The same Auto trial completed with the local native-function provider and installed reference filesystem MCP server in ten model calls, without steering. Discovery, coding and follow-up performed three MCP file reads and zero built-in inspection activities. The repair, applied project and follow-up each passed five Node contracts. All five restart comparisons passed; a separate executable resume at test approval preserved the original MCP activities. Terminal replay retained identical execution evidence, application and original-project verification receipts, with no repeated attempts. The earlier completed NVIDIA session also reopened with its original twelve calls and saved catalogs intact.

A separate scripted actor exercised the installed server's directory listing, directory tree, filename search and batched reads, then another actor used the installed ripgrep MCP server to find the `add` implementation by file contents. Each of these five activities succeeded with one attempt. Content search used only configured schemas and the generic directory scope; it needed no new Rust search implementation. The local host configuration allowlists six read-only tools across the two servers.

Evidence remains under `.build/mcp-first-auto/7c7fd39d-9446-4452-aafc-d2c5bdf9482a/` and `.build/mcp-first-inspection/b7ba33b7-955d-4641-b417-67e4962454f0/`. The Node-backed tests additionally cover discarded/reclaimed directory snapshots, cancellation, explicit environment binding changes, MCP-only setup discovery and saved native/external catalogs across host mode changes.

The final two-server configuration repeated the complete Auto flow and terminal replay under `.build/mcp-first-auto-full/d6bc2bf0-9318-4fa8-847e-c7f13e5deca7/`, again retaining ten calls, three MCP reads, five verified restarts and unchanged terminal evidence. Every workspace actor pinned all six external tools and none of the four built-in inspection tools.

<a id="mcp-tools--mcp-first-nvidia-trial--14-october-2026"></a>

#### MCP-first NVIDIA trial · 14 October 2026

NVIDIA `moonshotai/kimi-k3` completed a fresh Auto conversation in **16 native-function model calls**, including failed provider attempts and one exact model recovery. The database loaded its adjacent `mcp.json`; all five workspace actors pinned the six configured MCP tools and none of the built-in inspection tools. Discovery selected directory-tree inspection and batched reads without steering, and coding selected an MCP source read before repairing the module.

At the saved test approval, an explicit turn instruction requested a content search to exercise the ripgrep server. Kimi called `mcp_code_search` with `path: "."`, literal `function add`, `glob: "*.mjs"`, five-result and 1,000-character limits. The real server returned the repaired implementation at `src/math.mjs:1`; the disposable inspection snapshot was removed. This demonstrates the model's configured content-search interface with guidance, rather than an unguided preference for that tool. Directory listing and filename search were advertised but not selected in this trial.

The five committed MCP activities were one directory tree, one batched read, two single-file reads and one content search. Each succeeded in one attempt, with zero built-in inspection activities. The isolated repair, applied project and follow-up each passed all five real Node contracts. Documentation, package metadata and tests retained their original bytes; the follow-up made no edits. One reviewed application and its original-project verification remained in the same conversation.

The provider produced three 90-second timeouts and one invalid model action. Automatic retries recovered the question and content-search requests. The post-question request exhausted its automatic retries, then the existing turn-recovery API admitted one additional attempt with identical saved input and target. Its duplicate HTTP submission replayed the same receipt. Recovery retained model configuration, completed tools, permissions and usage. Recorded usage was 71,180 input and 1,989 output tokens; three timed-out attempts retained 81,356 held tokens and three uncertain calls.

All five restart comparisons passed: question at four calls, test approval at ten, application and project-verification approvals at thirteen, and completion at sixteen. Separate executable invocations resumed the question and test waits. MCP records already committed at each checkpoint stayed identical as work continued. A further terminal invocation added no model calls and retained byte-identical execution evidence, application receipt, project-verification report and applied source. Native edit/test activities also retained one attempt each. These checks cover saved waits and graceful runtime restarts.

Evidence remains under `.build/auto-mcp-first-live/e7013cfd-69ec-4ad0-a29d-057df3ffb01a/`, including exact requests, host configuration, recovery and steering packets/receipts, MCP checkpoint snapshots, the standard Auto reports and `mcp-replay-check.json`. These generated artifacts remain ignored. This trial used the existing runtime without implementation changes and covers the generated five-contract JavaScript project; provider retries were necessary.

<a id="mcp-tools--verify-locally"></a>

### Verify locally

No model endpoint or API key is needed:

```powershell
cargo test --locked --test mcp_tools
# Optional transport/restart/cancellation tests use Node.js and a controllable local MCP peer:
cargo test --locked --test mcp_tools -- --ignored
# Exercise the installed reference filesystem server and restart with the saved result:
cargo run --locked --example mcp_reads -- .act0r/mcp-filesystem.json
```

The example creates a fresh generated project and database under `.build/mcp-reads-demo`, saves a batch containing reads and refusals, reopens the runtime, and verifies its exact saved result and continuation with one tool attempt. The integration tests also cover interrupted reads, process cleanup, catalog pagination, malformed responses, UTF-8, byte limits and scope refusal before dispatch.
