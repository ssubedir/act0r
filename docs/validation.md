# Reproducible trials and model probes

Run local fixtures without credentials. Hosted model trials are explicit opt-ins and can send generated project context to the selected provider.

- [Repeatable Auto conversation trial](#auto-conversation-demo)
- [Two dependent coding workers: runnable harness demo](#dependent-coding-demo)
- [Native and standalone tool-dispatch probe](#tool-dispatch-probe)
- [Planner import and parent-test probe](#planner-dispatch-probe)

<a id="auto-conversation-demo"></a>

## Repeatable Auto conversation trial

Run a generated JavaScript project through the normal harness HTTP API:

```powershell
cargo run --locked --offline --example auto_conversation
```

The default uses a local mock Chat Completions provider and native function calls. Node must be on PATH. The fixture has no dependencies or install step: five built-in assertions check addition with positive, negative, zero and fractional inputs. The example changes only its generated project under `.build/auto-conversation/`. It does not introduce another planner, tool, runtime policy or storage schema.

The same trial supports host-configured MCP tools through adjacent `mcp.json` or the `ACT0R_MCP_CONFIG` override. See [MCP Auto verification](mcp.md#mcp-tools--exercise-the-normal-auto-conversation) for configuration and restart evidence. A [fresh MCP-first NVIDIA trial](mcp.md#mcp-tools--mcp-first-nvidia-trial--14-october-2026) completed with a six-tool external inspection catalog, verified content search, exact model recovery and unchanged terminal replay.

The request starts with only a folder, goal and model. Discovery reads project documentation, asks a compatibility question and proposes the writable module and Node verifier. The driver submits its saved fixture answer, reviews setup through the public route, and accepts the same reviewed configuration idempotently. Coding then uses the normal isolated workspace, edit/test/import approvals and fresh completion evidence. A separate approved application changes the generated source, then a separate durable verification reviews and runs the pinned Node command in that original folder. Its saved report must show passing output and matching project snapshots. An independent Node observer checks the result too. A follow-up targets the original conversation, reuses its coding actors and editable copy, and needs a new passing test activity with no further edits. The reviewer keeps the discovered module and exact Node verifier, pins a 60-second timeout and omits optional named commands. Optional command suggestions remain in the saved proposal and grant no execution capability by themselves.

The trial reopens the API and engine at a discovery question, the first test approval, application approval, project-verification approval and completion. Each of these five reopens compares committed actors, Runs, activities, usage, history and the grouped conversation before any work continues. These are graceful runtime restarts at saved boundaries. They are not abrupt process-kill or power-loss tests.

<a id="auto-conversation-demo--hosted-model-and-resume"></a>

### Hosted model and resume

Prepare the exact first NVIDIA request without a provider call:

```powershell
cargo run --locked --offline --example auto_conversation -- `
  --target-config examples/configs/benchmark-nvidia-agent.json --prepare-only
```

Inspect the printed directory's `first-request.json`, `session-request.json` and `project/` files. The configured target is pinned to native function calling for this example. Then continue that same database with your configured `OPENAI_API_KEY`:

```powershell
cargo run --locked --offline --example auto_conversation -- --resume GENERATED_DIRECTORY
```

`--target-config` may also start a hosted trial immediately. It reads the target only; the example supplies its own generated goal and project. All native roles use that pinned target. Model payloads include the generated fixture's reads, compatibility answer, reviewed discovery context, tool/test feedback and follow-up. Provider credentials remain in the installed model adapter and are absent from exported request bodies. Node processes omit environment variables ending in `_API_KEY`.

Runtime token/call budgets remain unlimited. An example-only guard stops before another model admission after 40 calls across its database. Activity and elapsed time guards also retain saved work. Unsupported waits, incomplete outcomes, unexpected setup, provider failures and recovery requirements produce a saved `needs_inspection` report rather than invented success or automatic reconciliation. The fixture answer is predefined; this driver is not a general human-answer bot.

To leave the executable entirely and resume in a separate invocation, use `--stop-after question`, `setup-review`, `test-approval`, `application-approval` or `project-verification-approval`. The result prints `checkpoint` and its generated directory. Resume without the stop option to continue. Exact session, setup, answer, application, verification and follow-up identities are saved before sending their HTTP requests. A torn journal is an error; the example does not replace it with a new operation.

Reopening a completed trial recovers those exact receipts and makes no model calls or second application/verification execution. Protected project documentation, metadata and tests must retain their original bytes. Keep the generated directory at its original absolute path: workspace/model pins belong to its original actors.

<a id="auto-conversation-demo--evidence-and-checks"></a>

### Evidence and checks

The directory retains `demo.db`, exact serialized `request-ACTIVITY.json` bodies, `evidence.json`, saved setup/decisions, `restart-*.json` comparisons, verified coding and follow-up changes, the application receipt, `project-verification.json` with actual original-project output/snapshots, independent `applied-tests.log`, the initial failing `baseline-tests.log`, final conversation and `report.json`. Raw evidence remains under ignored `.build/`; generated builds and task data should not be committed.

```powershell
cargo test --locked --offline --test auto_conversation_demo
cargo clippy --locked --offline --all-targets -- -D warnings
```

The local regression checks completion and exact terminal replay, each checkpoint through a separate driver invocation, refusal of changed protected files and zero-call preparation followed by native-function execution using the original session. The ten-call local path verifies the repair and its follow-up with separate real Node subprocesses, one application, one durable original-project verification and one visible conversation. The verification adds no model calls. See [project verification](harness.md#project-verification).

<a id="auto-conversation-demo--nvidia-trial--13-october-2026"></a>

### NVIDIA trial · 13 October 2026

NVIDIA `moonshotai/kimi-k3` completed the full Auto flow in **13 model calls** with native functions, zero repeated attempts and unlimited runtime call/token budgets: seven discovery calls, three repair/verification calls and three follow-up calls. Its saved compatibility answer was present in the actual coding and follow-up model requests. The managed repair, independently applied source and follow-up each passed all five real Node contracts. Documentation, metadata and tests kept their original bytes; the follow-up produced zero further changes.

The first trial stopped at setup review because the example initially rejected every optional named command. Kimi had proposed the correct module and Node verifier plus a redundant named copy of that verifier. The review guard now omits optional commands and pins its single verifier. The regression provider exercises that case. Continuing the original database preserved all seven discovery calls, the original model, answer and actors.

The trial reopened at the saved question (3 calls), test approval (9), application approval (10) and completion (13). It also exited its executable at test approval and application approval, resuming each in a separate invocation. A further terminal invocation retained 13 calls and the same one application. Recorded usage was 45,683 input and 1,450 output tokens, with no held or uncertain charges at completion. These results cover the generated five-contract project.

Historical browser inspection, before UI removal, restored active coding and the completed follow-up from the original conversation URL. One sidebar entry retained discovery, its question and answer, coding results and the verified follow-up; the mode stayed Auto. Completed turns offered no pause/stop actions. A small UI correction removes an empty patch download and application-approval hint from turns with zero changed files. Historical inspection also distinguishes a saved later follow-up from queued or running work, preserving its correct status and review action.

Evidence, exact requests and screenshots are under `.build/auto-conversation/fdc3b5cc-53d1-4a3a-b381-b3195a70e723/`. Final validation includes three local smoke tests, all-target Clippy, formatting, 89 UI tests, JavaScript syntax and diff checks. The local executable also completed across a separate-process test-approval stop in `.build/auto-process-restart/cd333afb-2914-405f-8071-f51bb520f10f/`.

<a id="dependent-coding-demo"></a>

## Two dependent coding workers: runnable harness demo

Run a complete API-to-consumer repair through the public local HTTP harness, including durable waiting, reviewed patch transfer, two restarts, meaningful tests and a separately approved final application.

From the repository root:

```powershell
cargo run --locked --offline --example dependent_coding
```

No provider key is needed for this default run. An example-local scripted Chat Completions server returns deterministic decisions; the installed harness still assembles the planner and coders, records model activities, enforces permissions, executes real tools and tests, and processes the saved approvals. The example does not implement another workflow engine or change the runtime's adapters.

Each fresh invocation creates a directory under `.build/dependent-coding-demo/`. Use `--output C:/path/to/demo-runs` to choose another parent. Existing runs are retained. The generated projects have no external Rust dependencies and disable debug symbols and incremental compilation to keep their builds small.

<a id="dependent-coding-demo--project-and-workflow"></a>

### Project and workflow

The source fixture is [dependent-coding-project](../examples/dependent-coding-project). It contains an API that incorrectly uses only a user's first name, a consumer with duplicated but correct formatting, and eight protected contract tests. Tests cover full names, whitespace, a missing name component, Unicode, and the anonymous fallback. The original project fails its API tests; the consumer's existing behavior already passes.

1. One request creates a new two-coder Code session. The consumer assignment is `coder`, with `depends_on: "coder2"`; `coder2` repairs the API. Each receives a separate managed copy and the same fixed Cargo test command.
2. The consumer saves its original baseline, then waits before its first model call. The API coder requests its normal saved edit approval. The example closes the HTTP API and worker, reopens the same database and checks the task IDs, baseline, usage and exact edit approval identity.
3. After the API repair passes all eight tests, the same planner inspects its immutable patch for transfer to the held consumer. The example restarts again at **Transfer reviewed patch** approval and checks the saved approval and API artifact. The consumer still has zero model calls.
4. The exact approval is saved, and a repeated HTTP response receives its existing receipt. The planner transfers the patch and explicitly resumes the consumer. The consumer reads the transferred API, replaces duplicated formatting with `crate::api::display_name(user)` and runs fresh tests.
5. The consumer's patch includes both API and consumer files against its original baseline. The planner imports that consolidated patch once, runs fresh tests and completes. The example checks one original group join, one dependency binding, distinct test activities and no repeated source model calls.
6. Until this point the generated source project is unchanged. A separate public application preview and saved approval apply the final immutable patch to that project. An independent Cargo test run verifies the applied code, while the protected tests, manifest and lockfile remain unchanged.

After application, an additional disposable project copies only the consumer and substitutes an API that returns distinctive names. Its test checks that the greeting follows API output, rejecting duplicated formatting while accepting fully qualified calls, imports and equivalent Rust expressions. This check does not modify the saved workspaces, applied project or protected contract tests.

The receiving task permits both `src/api.rs` and `src/consumer.rs` so the API patch can transfer. The source task permits only `src/api.rs`. Approval does not widen either scope. See [initial dependencies](coordination.md#initial-dependencies) and [worker changes](coordination.md#worker-changes) for the runtime contract.

Running this demonstration authorizes its driver to answer questions and approve the generated fixture's pinned edits, tests, reviewed transfers and final application. Decisions use exact public HTTP wait IDs and do not create lasting session permissions. An unavailable or unsupported wait stops the driver.

<a id="dependent-coding-demo--inspect-the-result"></a>

### Inspect the result

The command prints the run directory and a JSON report. A successful scripted run uses 17 model calls: four API calls, four consumer calls and nine planner calls. These are fixed scripted decisions, not a real-model efficiency estimate.

Saved files include:

- `report.json`: status, session and database identities, counts and verification.
- `checkpoint-wait.json` and `checkpoint-approval.json`: saved state at each restart.
- `test-evidence.json`: separate API, consumer and parent artifacts and test results.
- `combined.patch`: the verified two-file parent patch.
- `baseline-tests.log` and `applied-tests.log`: the failing baseline and final passing tests.
- `consumer-dependency-tests.log`: the independent check that greetings use API output.
- `completed-work.json`: the restored delegated-work and planner coordination view.
- `model-requests.json`: scripted prompts, showing the consumer read after transfer.
- `model-request-ACTIVITY_ID.json`: exact HTTP bodies from the model serializer for either scripted or hosted mode, without credentials.
- `session.json` and `request.json`: durable session receipt and original request.
- `request-nvidia.json`: the same goal configured for NVIDIA and a separate, untouched `project-nvidia/` copy. It contains no credential.

To inspect the saved session after the demo exits, use the IDs from `session.json`:

```powershell
cargo run --locked --offline --bin act0r -- --db "GENERATED_FOLDER/demo.db" team task status TEAM_ID TASK_ID
```

The same saved state is available through the local API's JSON routes.

The two restart checks close and reopen the API/worker at committed waits in the same example process. They demonstrate restoration of saved workflow state; they do not simulate a killed process or physical power loss. Existing runtime process-kill and reconciliation fixtures cover interrupted activities separately.

<a id="dependent-coding-demo--try-nvidia-or-another-configured-model"></a>

### Try NVIDIA or another configured model

Keep `OPENAI_API_KEY` in the launching environment, as for the normal harness. Opt into hosted calls explicitly:

```powershell
cargo run --locked --offline --example dependent_coding -- --target-config examples/configs/benchmark-nvidia-agent.json
```

`--target-config` loads only the JSON `target` from that file (or a standalone ModelTarget object). It creates another fresh project and Code session, using the same goal, permissions, test command, restart checks and approval driver. The existing NVIDIA target retains its provider request/retry policy. Cargo's `--offline` prevents dependency downloads; this opt-in mode still calls the configured model endpoint. Credentials remain in the Rust model adapter and are excluded from saved requests and test process environments.

To use native API functions for the planner and both workers, make a copy of the target configuration and set `target.action_format` to `"function_calling"` (or `action_format` on a standalone target), then pass that file to `--target-config`. Compatible Chat Completions targets and flat contract 4 support this mode. The saved configuration remains immutable on resume; the demo needs no additional transport flag. See [function calling](providers.md#model-function-calling).

A real model must choose the declared delegation, transfer and release actions; the driver does not inject them or fall back to scripted replies. Successful live runs must satisfy the same assertions, but their call counts can differ. The request includes a complete first-action example. The runner checks that the saved group actually contains the dependency before admitting coder model work; a dependency mentioned only in goal text fails that check. It also stops if one assignment asks more than two questions in a driving phase, retaining its answers and diagnostic instead of approving a repeated-question loop indefinitely. These are example checks; they do not change harness budgets or agent policy. An example-only guard also stops before a 25th model call in any one role, while allowing non-model effects to settle after its 24th call. It saves `model_call_guard` in `failed-checkpoint.json` and preserves queued work. Runtime token and call budgets remain unlimited. Successful reports include separate `source_usage`, `receiver_usage` and `planner_usage` alongside their call counts.

The driver also stops if the planner proposes another delegation after the original assignments exist, or the saved session already contains additional groups or assignments. `failed-checkpoint.json` records `repeated_delegation` and the exact parsed proposal before more work is admitted. Resume retains the same guard and does not discard the offending decision or supply a replacement. The runtime still supports multiple groups for other workloads; this fixture requires exactly one. A refused finish must lead to actual parent tests or explicit incomplete completion, rather than repeating the initial example.

New tool actors also have a [durable clarification safeguard](recovery.md#worker-clarification). Their third consecutive question saves a **Needs guidance** wait. This example retains that wait and exits for inspection; it does not automatically guide a stalled assignment. Inspect its saved inbox and use the guidance/resume command below to continue the Run.

Provider failures, incompatible decisions or missing expected checkpoints leave the database for inspection and exit unsuccessfully. `failed-checkpoint.json` captures terminal or idle-checkpoint evidence when available. A model failure still needs the harness's explicit recovery decision. Each phase has a 30-minute elapsed limit and a 1,000-progress-step guard; session token/call budgets retain the normal unlimited defaults.

You can also submit `request-nvidia.json` to an independently running API's `POST /v1/harness/sessions` and handle approvals through its goal inbox. Use the goal and pinned settings from that saved request. Manual API execution does not perform the example's automatic restart checks or approvals.

<a id="dependent-coding-demo--resume-a-saved-demo"></a>

### Resume a saved demo

Continue a previously generated directory, using its existing `demo.db`, `request.json` and `session.json`:

```powershell
cargo run --locked --offline --example dependent_coding -- --resume GENERATED_FOLDER
```

The driver reads the existing session; it does not submit another request, copy the source again or change any actor, model or permission pin. An existing initial dependency and the generated protected fixture files must match. The consumer retains its original baseline and Run, and its model work remains held until the planner performs the approved transfer and explicit release.

For **Needs guidance**, read the exact wait ID and saved questions in the `inbox.items` of `failed-checkpoint.json` / `resume-report.json`. Write a focused instruction to a UTF-8 file, then target that one wait:

```powershell
cargo run --locked --offline --example dependent_coding -- --resume GENERATED_FOLDER --wait-id WAIT_ID --guidance-file guidance.txt
```

For example: “Call read_file for src/api.rs, implement the assigned repair, then run the pinned tests. The runtime handles the exact edit and test approvals.” The text must be nonblank and at most 65,536 UTF-8 bytes; the harness also checks its total saved context bound. Guidance adds no permission and cannot resolve an ordinary question or approval. Both flags are required together.

Before sending guidance, the driver saves its exact wait identity and body in `guidance-WAIT_ID.json`. A retry with the same text acknowledges that wait's original receipt. Changed text for that identity conflicts. If the worker stalls again, its new guidance wait needs another explicit wait ID and decision; old guidance is never applied automatically to that later wait.

Every continuation closes and reopens the API/worker before admitting model work, including immediately after guidance. `resume-restarts/*.json` compares hashes of the complete saved Run records and activity lists, with readable usage, Run and wait identities. These are committed-state restart checks, not process-kill or power-loss simulations. Use one demo driver at a time and stop any separate worker operating this database while demonstrating this comparison.

To stop deliberately at another committed approval:

```powershell
cargo run --locked --offline --example dependent_coding -- --resume GENERATED_FOLDER --stop-after transfer-approval
cargo run --locked --offline --example dependent_coding -- --resume GENERATED_FOLDER --stop-after application-approval
```

Omit `--stop-after` on the next invocation to proceed. It also works on fresh invocations. For an intermediate transfer checkpoint, use a configured model server that stays available: the default scripted server exists only during the fresh invocation. Resume preserves its original endpoint and cannot retarget it with `--target-config`. A default scripted run stopped at final application approval can resume without model calls because all coding work is complete.

Final application reuses `application-request.json` and the public application receipt, including an application already created before that file existed. Reopening an applied result checks its saved report and reruns the independent fixture tests; it does not create another application or repeat model calls. Interrupted or failed tools, transfers, applications and model decisions retain their normal explicit recovery requirements. The driver never supplies recovery decisions, releases independent holds or replaces a torn decision file.

`resume-report.json` records the latest outcome: `verified`, `checkpoint`, `needs_guidance`, `needs_recovery`, `model_failure` or `stopped`. It includes current scoped task/inbox projections, Run errors and usage. Each invocation also retains an immutable report under `resume-attempts/`. Invalid local configuration can fail before these reports are available. Successful resumed reports add `resumed: true` and the number of checked `resume_restarts_verified`; `restarts` retains the original completed demo's automatic checkpoint count, or zero when that fresh execution did not finish.

Invoking resume authorizes the same generated fixture decisions as a fresh run: ordinary clarification answers, pinned edits/tests, reviewed patch transfers and final application. Hosted mode still sends its saved context and explicit guidance to the original provider using your configured environment key.

<a id="dependent-coding-demo--verify-locally"></a>

### Verify locally

```powershell
cargo test --locked --offline --test dependent_coding_demo
cargo clippy --locked --offline --example dependent_coding --test dependent_coding_demo -- -D warnings
```

The six integration tests use temporary directories and local model fixtures, including real tests and final project application. They need no provider key, and temporary fixtures are removed after each check. Resume checks cover two successive guidance waits, exact receipt replay, stale/conflicting/invalid input, unchanged physical assignments, both approval checkpoints, one final application, saved model failures and refusal of changed model or protected fixture pins. Native-function coverage also compares all 17 exported wire bodies to actual HTTP requests, verifies dependency ordering and final application, and reopens the terminal result without additional calls. Dependency-check regressions accept both imported and qualified API calls and reject duplicated formatting.

<a id="dependent-coding-demo--recorded-validation"></a>

### Recorded validation

On 12 October 2026, the scripted command completed with 17 model calls, two restarts, one dependency binding, one group join and eight passing contracts in each API, consumer and parent workspace. The separately applied project passed the same eight tests. Two demo integration tests, the eight existing initial dependency tests, and all-target Clippy with warnings denied passed.

Live trials used the existing NVIDIA `moonshotai/kimi-k3` configuration. An initial trial omitted `depends_on`, retaining the dependency only in goal text. The explicit first-action example established the correct durable declaration: the consumer stayed held at zero model calls and recovered after restart. The API worker subsequently kept choosing `ask_human` despite saved answers, including with the simpler goal that uses normal edit approval. The final trial stopped under the repeated-question guard after three API calls and one planner call; it did not transfer or apply a patch. An end-to-end NVIDIA success has not been established for this demo. The saved local database and checkpoint expose those decisions for inspection; the scripted result verifies the runtime flow without claiming real-model planning reliability.

The resume suite subsequently passed all four local tests. Its stalled local worker continued after explicit guidance, retained the original two assignments, transferred and imported each patch once, and passed eight contracts in the API, consumer, parent and applied project. Replay after completion made no model calls and retained one final application. Targeted Clippy with warnings denied and formatting checks also passed.

After explicit approval for its saved context and guidance, the NVIDIA trial continued the same session and physical worker Runs. The exact guidance receipt replayed once and complete saved Run/activity hashes matched across restart. Guidance appeared in each of its four additional saved model inputs. The first decision attempted to finish with a placeholder summary before any tool work; the fresh-test gate refused it. Three subsequent decisions chose `ask_human` instead of `call_tool`, so the runtime saved another `native_guidance` wait. The source Run retained eight admitted calls in total (including a pre-resume repair attempt); the parent retained one and the consumer retained zero. No patch was transferred or applied. The example exited with `needs_guidance` in `resume-report.json`. This confirms live resume and guard behavior; end-to-end real-model coding completion remains unverified.

The subsequent flat native model-contract change passed the smaller NVIDIA read/edit/test probe. A fresh local dependent-worker demo using the new contract also completed with two restarts, the consumer held before its first model call, one approved consolidated patch and eight passing tests in each worker, parent and applied project. Its local HTTP fixture emits each worker's pinned wire format. This local result does not establish an end-to-end NVIDIA result for the larger demo.

<a id="dependent-coding-demo--follow-up-nvidia-trial"></a>

### Follow-up NVIDIA trial

A fresh hosted attempt with the flat worker contract repaired the API in four model calls and passed all eight contracts. The consumer remained held at zero calls across the first restart. NVIDIA then rejected the dependency-review request with HTTP 400. Replaying the exact saved body returned `Grammar error: anyOf must be an array`. Mutable indexing had inserted `"anyOf": null` while checking whether the single guidance action needed wrapping. Coordination schema composition now uses an existing-key lookup for inspection, stop and pause tools. Saved admitted requests retain their original contents; the next trial used a fresh session with the corrected schema.

That corrected trial completed the original API worker in four calls and its consumer in five, with eight fresh passing tests each. Both committed-wait restarts passed. The planner inspected and approved transfer into the held consumer, explicitly resumed it, routed guidance, joined the original group and imported only its consolidated two-file patch. It then chose `finish`, claiming fresh parent tests that had never run. The capture recorded `tests_not_run` and refused completion. Its next decision repeated the initial delegation, creating an unwanted second group. The process was stopped after detection, with 24 admitted calls across the planner, original workers and extra assignments; one model attempt remained interrupted. No final application was created, and every generated source-project file still matched the fixture. The database retains the refused completion, original artifacts and duplicate work.

The retained corrected trial is `.build/dependent-coding-demo/ccc55a50-619a-4796-9205-947005510f26/`, with a read-only assessment in `trial-assessment.json`; its status is `stopped`, not `verified`. This shows actual worker dispatch, durable ordering, restoration and reviewed integration. Reliable parent test dispatch and end-to-end hosted completion remain unverified. The latest local fixture reproduces the premature finish and second delegation, checks the refusal, stops before another group is prepared, and verifies that resume makes no more model calls or application.

All four demo tests and 244 HTTP regression tests passed. The demo also checks schema combinators recursively in its actual saved model requests, including dependency coordination. Formatting and all-target Clippy with warnings denied passed.

<a id="dependent-coding-demo--native-function-trial-13-october-2026"></a>

### Native function trial, 13 October 2026

A fresh NVIDIA `moonshotai/kimi-k3` trial used native function calls throughout and completed both workers, the planner and approved application. Its 18 model activities all succeeded in one attempt each. The API worker used four calls, the consumer five and the planner nine. The consumer stayed held at zero calls until the reviewed API transfer and explicit resume. The original edit and transfer waits survived the two automatic restart checks.

The API, consumer and integrated parent each passed all eight protected tests with separate fresh evidence. The parent imported one consolidated two-file patch and completed with verified tests. The generated project remained unchanged until its separate approved application; that project then passed the same eight tests. Protected files remained unchanged, one original group joined and one dependency binding was preserved.

The initial driver exited after application because its source-text assertion required a particular `let name = crate::api::display_name(user);` line. Kimi correctly imported `display_name` and called it inside `format!`. The behavioral dependency check replaced that assertion. Reopening the same terminal session reused its existing application, passed both independent checks and returned `verified`, with no additional model calls. The first invocation's transcript is retained in `.build/kimi-dependent-functions-trial.log`; the recheck is in `.build/kimi-dependent-functions-reopen.log`. Its resumed report records one additional fingerprint-verified restart; the `restarts` counter is zero because the first invocation did not write a successful report, despite completing its two automatic checkpoint checks.

Evidence: `.build/dependent-coding-demo/5e8012f1-dd25-4bf7-aa88-f260f44b33a8/`. Exact wire requests, immutable patches, approvals, tests and usage remain saved.

| Role | Model calls | Input tokens | Output tokens |
| --- | ---: | ---: | ---: |
| API worker | 4 | 11,980 | 403 |
| Consumer worker | 5 | 16,688 | 552 |
| Planner | 9 | 51,793 | 1,032 |

All runtime budgets stayed unlimited. This is one successful hosted integration fixture, not a measured reliability rate across projects. The earlier incomplete JSON-mode trials remain recorded above. All six demo tests, all eight planner probe tests, formatting and all-target Clippy with warnings denied passed.

<a id="tool-dispatch-probe"></a>

## Native and standalone tool-dispatch probe

Compare the existing native `tool` actor and standalone `coding` actor on one small generated Rust repair, before trying a multi-worker coding session.

```powershell
cargo run --locked --offline --example tool_dispatch_probe
```

The default uses a local scripted Chat Completions server. It returns decisions; the installed actor runtime still saves model activities, requests edit/test approvals and executes real tools. Both arms read `src/lib.rs`, replace incorrect subtraction with addition, run three protected contracts and finish. Separate databases and managed copies prevent one arm's wait from affecting the other. The generated source copies remain unchanged.

Both arms use the same goal, fixture, model target, response limit, context bounds, HTTP adapter and outer `action` envelope. Model-call and token budgets are unlimited. The native actor enables only `read_file` v2, `replace_text` v1 and `run_tests` v1; the standalone actor retains its existing flat action catalog. Both contracts use tool names directly as action kinds with fields alongside `kind`. Instructions, observations and completion gates differ, so this comparison does not isolate schema structure alone.

<a id="tool-dispatch-probe--configured-models-and-exact-prepared-requests"></a>

### Configured models and exact prepared requests

```powershell
cargo run --locked --offline --example tool_dispatch_probe -- --target-config examples/configs/benchmark-nvidia-agent.json --prepare-only
cargo run --locked --offline --example tool_dispatch_probe -- --resume GENERATED_FOLDER
```

Preparation saves both tasks, the native baseline and their first outgoing JSON bodies, with zero admitted model calls. Resume opens the original databases and verifies actor, task, Run, model/configuration and fixture pins before execution. It preserves completed results and stops again at unanswered questions. A completed probe can be reopened without more model calls. The default ephemeral server does not support a later invocation; resume requires a configured-target probe.

Omit `--prepare-only` to prepare and execute in one invocation. A configured model receives the generated task, action schema, file contents and subsequent saved tool/test feedback. Authentication comes from the existing environment configuration. Request exports contain JSON bodies, not headers or API keys.

Each new invocation creates a retained UUID directory beneath `.build/tool-dispatch-probe/`; `--output` selects another parent. Its files are:

- `manifest.json`: original task and actor configuration identities.
- `native/first-request.json` and `coding/first-request.json`: exact first bodies.
- Each arm's `request-ACTIVITY_ID.json`: outgoing body for each logical decision.
- Each arm's `evidence.json`: saved Run, requests, parsed decisions, errors, activity results, attempts, usage and exact approval receipts.
- Each arm's `report.json`, plus the combined `report.json`: dispatch and test assessment with the saved task/wait and workspace.

Exports use the HTTP adapter's actual serializer. Repeated attempts retain the same logical request. Invalid response text is not retained by the adapter; evidence includes its saved diagnostics and known usage instead. These files are diagnostic snapshots; SQLite remains the workflow authority.

Running the probe authorizes approval of its generated fixture's pinned edits and fixed `cargo test --locked --offline --test contracts` command. Every approval uses the exact durable wait; it does not create a lasting grant. Questions and other waits stop an arm without injected guidance, while the second arm runs independently. Blocked or failed tasks remain available for inspection. An example-only guard stops further admissions after eight model calls or fifteen minutes per arm; it does not change runtime budgets or cancel saved work.

`verified` requires completed tasks, successful saved read/edit/test activities in order, exactly one edit, three passing real tests and unchanged protected files. A provider returning a `finish` summary without this evidence cannot pass. The CLI returns failure after saving the comparison when either arm is unverified.

Local tests cover real dispatch and request-body equivalence, the first-question stop, malformed-response retries, preparation without egress, pin validation and resuming the original tasks. Hosted outcomes are provider observations, not a guarantee for other tasks or models.

<a id="tool-dispatch-probe--nvidia-observation-12-october-2026"></a>

### NVIDIA observation, 12 October 2026

One approved prepared probe used `moonshotai/kimi-k3` at the configured NVIDIA Chat Completions endpoint with the same target policy for both arms:

| Actor | Model calls | Actual tools | Outcome |
| --- | ---: | --- | --- |
| Native | 2 | None | Premature `finish` refused by the fresh-test gate, then `ask_human` with question `placeholder`; the original task remains waiting. |
| Standalone coding | 4 | `read_file`, approved `replace_text`, approved `run_tests` | Three contracts passed and the original task succeeded. |

Both budgets remained unlimited and both generated source copies were unchanged. The native model said it was starting a read in its finish summary, but no read activity existed. This distinguishes claimed actions from runtime execution. The standalone result demonstrates actual tool dispatch using the same model transport. The comparison points toward native contract compatibility; schema, instructions, prompt layout and model variability remain possible explanations. It does not establish a particular schema keyword as the cause. The larger NVIDIA dependent-worker demo was not rerun because the native small case did not succeed. Local scripted execution succeeds in both arms.

<a id="tool-dispatch-probe--pinned-native-contract-trials"></a>

### Pinned native contract trials

Native tool and planner actors use model contract 4: actual tool names as action kinds, with argument fields directly in the action. The runtime normalizes these responses to canonical `call_tool` decisions using the saved catalog. Dispatch, argument validation, scopes, approvals, tests and completion gates remain authoritative. Tool names must not collide with control actions; parameter name `kind` is reserved. Earlier development contract variants and probe switches have been removed.

One approved NVIDIA comparison using this contract completed both native and standalone read/edit/test sequences in four calls each, passing three protected contracts. Source copies stayed unchanged and budgets remained unlimited. This is one integration result, not a reliability measurement or proof of why the model selected valid actions. The [planner probe](#planner-dispatch-probe) checks delegation, reviewed imports and fresh parent tests separately.

<a id="planner-dispatch-probe"></a>

## Planner import and parent-test probe

This small trial checks delegation, reviewed import and parent verification. By default, one deterministic native worker repairs addition in a generated Rust file and produces a verified immutable patch using real tools and three protected tests. Use `--live-worker` to run the repair worker with the configured model as well. The HTTP planner must delegate once, inspect and approve importing that patch, request its own fixed parent `run_tests`, and finish with verified evidence. The generated source stays unchanged; no final application is performed.

```powershell
cargo run --locked --offline --example planner_dispatch_probe
cargo test --locked --offline --test planner_dispatch_probe --test planner_model_contract
```

The default uses a local HTTP fixture for planner responses. It deliberately claims completion once after importing, before parent tests exist. The installed completion gate refuses with `tests_not_run`. After restart the fixture selects the real parent test tool and finishes only after all three contracts pass. The driver reopens the runtime at import approval, refused completion (if one occurs), and each distinct parent test approval. It verifies the complete saved checkpoint before continuing: actor pins, wait identities, requests, results and usage. This proves local execution/recovery, not hosted model reasoning.

<a id="planner-dispatch-probe--configured-model-and-saved-continuation"></a>

### Configured model and saved continuation

```powershell
cargo run --locked --offline --example planner_dispatch_probe -- --target-config examples/configs/benchmark-nvidia-agent.json --prepare-only
cargo run --locked --offline --example planner_dispatch_probe -- --resume GENERATED_FOLDER
```

Preparation creates an isolated UUID directory beneath `.build/planner-dispatch-probe/` and saves the first exact HTTP body with zero admitted model calls. Only the planner uses the configured target by default; the repair worker remains deterministic. Resume opens the original task and checks its actor, worker, Run, model, workspace and fixture pins. It cannot replace the target or contract version. A completed trial can be reopened without additional model calls.

To exercise both roles with the same pinned native function target:

```powershell
cargo run --locked --offline --example planner_dispatch_probe -- --target-config examples/configs/benchmark-nvidia-agent.json --action-format function_calling --live-worker --prepare-only
cargo run --locked --offline --example planner_dispatch_probe -- --resume GENERATED_FOLDER
```

`--live-worker` requires `--target-config` and is selected only when creating a fresh trial. The worker retains the same target and action format as the planner, an empty script, a 2,048-token output limit, its original one-file writable scope and the fixed test command. Resume derives its mode from the saved actor; neither role can be retargeted. The driver also restarts at the model worker's first edit approval. Use `--stop-after worker-edit-approval` to leave that exact wait for a later invocation. Existing scripted-worker probes remain resumable.

`4` uses the fresh flat planner contract. Both retain one planner implementation, the same worker, approvals, tests and completion gate. Instructions, action ordering and observations also differ, so results cannot isolate flattening as the sole cause. Model-call and token budgets remain unlimited.

`--action-format function_calling` selects native API functions for a fresh Chat Completions target with flat contract 4. JSON schema remains the default, or can be selected with `--action-format json_schema`. The format is saved in the target and cannot be replaced on resume. Local fixtures exercise both transports, malformed-function retries with charged usage, and refusal recovery. See [native function calling](providers.md#model-function-calling).

`--stop-after import-approval` or `--stop-after refusal` exits at that committed checkpoint. A later `--resume` verifies it before proceeding. A hosted model may request tests correctly and never encounter refusal; reports distinguish that from verified refusal recovery. The default ephemeral fixture cannot resume in a later invocation; use a configured persistent endpoint for that case.

Running the probe approves only this generated fixture's fixed edit, reviewed one-file import and `cargo test --locked --offline --test contracts` command. The runtime saves and checks each exact approval wait. The driver never injects a model decision, answers a question, runs tests on behalf of a finish, or applies the final patch to the source. Questions and recovery/blocked states stop for inspection. Example-only guards stop before a ninth model call in either role, after fifteen minutes per drive segment, or at a second delegation proposal. Non-model effects can settle after the eighth call. These guards do not change runtime budgets or general planner policy; token and call budgets stay unlimited.

<a id="planner-dispatch-probe--evidence-and-verification"></a>

### Evidence and verification

- `manifest.json`: original identities, complete planner and worker pins.
- `first-request.json`, `request-ACTIVITY_ID.json`: bodies from the actual HTTP serializer.
- `worker-request-ACTIVITY_ID.json`: model-worker bodies from that same serializer.
- `probe.db`: authoritative Runs, activities, approval waits, artifacts and usage.
- `evidence.json`: parent and child state, requests, canonical parsed actions and results.
- `checkpoint-KIND.json`, `checkpoints.json`: committed checkpoints and restart verification.
- `restart-pending.json`: a checkpoint awaiting verification, removed after successful reopen.
- `report.json`: actual import/test/completion evidence, separate planner/worker usage and call counts, and remaining saved task state.

Exports contain request bodies, not headers or credentials. Expected output is `verified` only when the sole child has verified completion, its one-file import actually succeeded, the latest parent test activity passed all three contracts and was scheduled after that import, and the parent completion report cites that same latest test activity. Earlier failed or stale tests remain in history and cannot verify imported changes. Summary claims cannot satisfy these checks. Protected files and generated source are checked on every reopen. Failed or stalled trials retain their original database and pins and return `not_verified`; the CLI exits with failure after saving evidence.

Local tests cover both wire contracts, refusal recovery through three restarts, zero-call preparation, exact exported request equivalence, process-boundary continuation at approvals/refusal, and question/duplicate-delegation stops. Model-worker fixtures cover separate requests/usage, its exact edit approval across an invocation boundary, first-question and eight-call stops, and a failed baseline test followed by import and fresh passing tests. Both parent test waits are checked across restart independently. Parser tests cover restricted single-tool coordination and direct planner actions in both Responses and Chat Completions protocols. Migration tests preserve old requests verbatim under SQLite.

<a id="planner-dispatch-probe--nvidia-observations-12-october-2026"></a>

### NVIDIA observations, 12 October 2026

Two fresh configured trials used `moonshotai/kimi-k3` at the NVIDIA Chat Completions endpoint. Each stopped after one successful HTTP response whose selected action was `finish_incomplete`, with no delegation, worker execution, import or parent test activity. The first described itself as awaiting initial delegation. The second used shorter flat execution instructions and described a plan to delegate, import and test while acknowledging that no actions ran. The runtime saved an incomplete result and the probe returned `not_verified`.

Evidence is retained in the ignored directories:

- `.build/planner-dispatch-probe/55f3c76e-1500-40df-941f-7e0d4c045277/`
- `.build/planner-dispatch-probe/3754401e-5cb7-4d7b-be18-6c8ecfc12487/`

Both generated sources remained unchanged, and model-call/token budgets stayed unlimited. Neither trial reached a restart checkpoint. Local HTTP fixtures pass the complete workflow with the current flat contract, including refusal and restart recovery, but hosted planner completion remains unverified. No hosted nested control was run for this fixture, and these outcomes do not identify schema structure as the cause. The larger two-worker NVIDIA trial was not repeated after these small failures.

<a id="planner-dispatch-probe--native-function-trial-13-october-2026"></a>

### Native function trial, 13 October 2026

A subsequent Kimi trial using native function calls completed the same planner fixture in five model calls: delegate, inspect, import, fresh parent tests and verified finish. All three parent contracts passed; both approval checkpoints survived restart. No model retries or completion refusals occurred. Reopening the completed trial preserved verification and its five-call count. The worker remained deterministic and the generated source stayed unchanged. Evidence: `.build/planner-dispatch-probe/03b56b7e-752d-40e8-820b-fc6d7c36d7eb/`.

<a id="planner-dispatch-probe--both-roles-use-kimi-13-october-2026"></a>

### Both roles use Kimi, 13 October 2026

A fresh native-function trial used Kimi for both roles: six planner calls and four repair-worker calls, with no retries. The worker read, repaired and tested the actual file and returned a verified patch. The planner also requested a failing baseline test before importing, then ran all three tests successfully after import. Its runtime completion report cites that fresh passing activity. Worker edit, baseline parent test and import approval survived three restarts. Protected files in both workspaces and the original source remained unchanged.

The initial probe report falsely rejected the result because it required exactly one parent test activity. The checker now requires the latest passing test after the sole import, matching the existing runtime completion evidence. Reopening the same saved terminal run returned `verified` without any additional model calls. The original report is archived as `.build/kimi-live-worker-initial-report.json`; the authoritative database was not rewritten to change its outcome.

Evidence: `.build/planner-dispatch-probe/cbacfa6e-e825-4e62-bba6-42f11aa207e3/`. Planner usage was 15,284 input and 479 output tokens; worker usage was 5,957 input and 273 output tokens. The live run predates the per-wait restart improvement: its final test approval was saved and approved, but was not another restart checkpoint. The local baseline fixture separately checks both test approvals. This establishes one hosted coding workflow, not a reliability rate across tasks.
