# Search and benchmark experiments

Optional experimental strategies. The current product focus is the native durable actor harness; MCTS remains deferred.

- [Snapshot-backed coding tasks](#coding-tasks)
- [Durable coding benchmarks](#benchmarks)
- [Durable checkpoints and forks](#branching)
- [Durable bounded search](#search)
- [Multi-file search benchmarks](#multifile-benchmarks)

<a id="coding-tasks"></a>

## Snapshot-backed coding tasks

A coding task creates 1–16 independent candidate runs from one recorded baseline. Each candidate uses the bounded agent-v2 loop and owns its own workspace, activities, context, waits, and history. The task owns shared admission limits and the recorded comparison result. One local worker executes activities across candidates; individual candidate steps can interleave. [Durable checkpoints and forks](#branching) can add continuations from paused progress, up to 16 planned slots, while charging the shared prefix once.

<a id="coding-tasks--start-inspect-and-retrieve"></a>

### Start, inspect, and retrieve

```powershell
cargo build --locked
$taskId = (& .\target\debug\act0r.exe --db .\tasks.db task start --source .\examples\coding-task --config-file .\examples\configs\task-scripted.json).Trim()
.\target\debug\act0r.exe --db .\tasks.db worker run --until-idle
.\target\debug\act0r.exe --db .\tasks.db task status $taskId
.\target\debug\act0r.exe --db .\tasks.db task result $taskId
```

All commands support `--json`. Status includes the task budget, usage, candidate run IDs and workspaces, verification results, and selection. Result prints the selected workspace, recorded test output, and unified patch. It reports an error while there is no selected solution. The source project remains unchanged.

The example makes three candidates with the same deterministic fixture model, so identical patches demonstrate recovery and comparison mechanics. Replace `agent.target` with the target from `agent-lmstudio.json` to use the local model. All candidates use the same goal, model configuration, tools, and tests. Model variability can produce different candidates; this milestone has no strategy-specific prompts or search tree. A live [benchmark pilot](#benchmarks) exercised one and three LM Studio candidates but stopped on model request failures before final verification; successful multi-candidate execution with a real model remains unverified.

<a id="coding-tasks--configuration"></a>

### Configuration

[task-scripted.json](../examples/configs/task-scripted.json) has four fields:

| Field | Meaning |
| --- | --- |
| `agent` | Existing AgentConfig, including model, goal, fixed command, writable paths, context policy, per-candidate limits, approval and test recovery policy. Its workspace is replaced during preparation. |
| `candidates` | Positive count, at most 16. Each candidate starts from the same baseline. |
| `budget` | Shared `max_attempts` (1–10,000), `max_model_calls`, and `max_tokens`. |
| `protected_paths` | Required exact relative files or prefixes ending in `/` containing tests and build configuration. Every rule must match a baseline file and must be disjoint from writable paths. |

Use `/` separators and ordinary relative paths. Path checks treat case aliases conservatively. Files outside `agent.writable_paths` are also frozen during verification. The example protects `Cargo.toml`, `Cargo.lock`, and `tests/`, and permits editing `src/lib.rs`. Choose protected paths that actually contain the task's correctness checks. Keep generated files under excluded build directories and pin dependencies; a new lockfile outside writable paths fails verification.

If `agent.require_approval` is true, edits, agent test calls, and final verification each create persisted approval waits. Inspect a candidate with `run status RUN_ID`, then use `run approve RUN_ID WAIT_ID` or `--deny`. Denied final verification records an unverified candidate. Invalid approval payloads leave the wait intact. Pause/resume/cancel, human answers, and manual reconciliation operate on the candidate run IDs. A task stays `waiting` while a candidate needs input, is paused, or requires reconciliation, after other active candidates finish.

<a id="coding-tasks--snapshot-and-verification-contract"></a>

### Snapshot and verification contract

Preparation copies the source once into `.act0r/snapshots/TASK_ID/` beside the database. A versioned, sorted manifest records relative file paths, byte lengths, and SHA-256 hashes; its digest identifies the baseline. Published directories contain complete copies. Forks use stable candidate run IDs and retain their origin metadata; repeating a fork for the same identity returns its existing workspace. Source changes after capture cannot affect later forks. The adapter verifies baseline content before forks and evaluations and rejects changed content.

The copy policy retains the existing exclusions for Git/runtime metadata, build/dependency output, `.aws`, and `.env` files. Links, reparse points, special files, ambiguous names, and case-aliased files are rejected. Snapshots cap at 10,000 entries and 64 MiB. The manifest describes file contents and paths; filesystem permissions and empty directories are not part of the comparison digest.

A candidate's finish action still requires successful agent tests after its final edit. It then schedules `coding.evaluate` as a separate durable activity. Verification compares the candidate against the baseline, rejects protected or unwritable changes, records a diff, and applies the permitted changes to a fresh temporary copy of the baseline. It reruns the saved command there, checks that evaluation inputs and candidate content did not change during tests, and records bounded stdout/stderr, exit status, and baseline/candidate digests.

Diffs support UTF-8 files up to 1 MiB per changed file and 4 MiB of total patch text. A diff hunk spans the changed region with three lines of surrounding context; its byte score counts removed and added lines. Unsupported or excessive diffs produce a failed verification. A completed candidate run can have failed verification; consult its evaluation rather than treating run completion as proof of correctness. The final result's `tests_passed` reflects verification.

After all candidates finish or stop, the task selects a passing candidate with the smallest changed-byte score; candidate ordinal breaks ties. Selection is deterministic and persisted with the candidate states, evaluations, diffs, and usage. If none passes, the task records `no_solution`. Recorded patches are the tested artifacts. A later manual edit of a candidate workspace does not alter the saved patch or result.

Fixed tests establish correctness only to the extent of their checks. Ordinary tasks rerun their protected feedback tests. [Benchmark tasks](#benchmarks) additionally pin a separate final-test snapshot and command. Final files must be protected, unwritable, and absent from the baseline; they are added only to the clean verification copy. Their digest is recorded in the evaluation and checked during selection. Candidate workspaces and model prompts never receive those files.

<a id="coding-tasks--shared-budgets-and-recovery"></a>

### Shared budgets and recovery

Task accounting sums persisted candidate ledgers. Every admission checks the proposed attempt/model reservation against the shared task allowance inside the same SQLite IMMEDIATE transaction that saves the run and attempt. A rejected admission stores `budget_exhausted` with diagnostic `task_budget` and publishes no execution intent or reservation. Concurrent admissions cannot independently spend the same allowance. Per-candidate limits still apply.

All model, tool, and final-verification attempts consume the shared attempt allowance, including retries and interruptions. Known model usage replaces its reservation; uncertain or missing usage keeps it charged. Recovery never resets task allowance or candidate history. Input reservations are byte-based estimates, so actual provider usage can exceed an estimate; reported usage is retained and later model admissions stop as appropriate. This is admission accounting, not an absolute provider billing guarantee.

Candidate completion, task ledger updates, verification results, and selection commit together. Duplicate candidate completions do not add charges or choose again. Task creation also commits all candidate runs and memberships together, after workspace preparation. A crash or failed transaction during preparation can leave unreferenced filesystem copies; no partial task or candidate runs become visible in SQLite. Preparation has no resumable request key or automatic artifact cleanup yet.

Starting the usual worker recovers all unfinished candidate runs. Interrupted model requests retain their saved prompt and uncertain reservation. Verification obeys `agent.repeat_safe_tests`: repeat-safe tests can retry under the existing limits; other interrupted tests become indeterminate and require manual reconciliation. A reconciliation result for `coding.evaluate` must have the full CandidateEvaluation shape, including verified test results and patch/digest evidence. Authorizing a retry is often easier when that evidence was lost. Never infer that a test process stopped solely from a worker timeout.

The baseline stores task membership, benchmark jobs, checkpoint and fork preparation, bounded search and auxiliary task runs. Optional final-test, branch, and search fields default to absent. Ordinary agent-v1/v2 runs and coding-candidate v1 retain their definitions and behavior; seeded manual fork continuations use coding-candidate v2. Search's proposal run shares the task admission budget and has its own durable ledger.

<a id="coding-tasks--ports-adapters-and-verification"></a>

### Ports, adapters, and verification

`CodingTask` owns task configuration, admission policy, candidate identity, accounting, and selection. `CandidateWorkflow` composes pinned agent-v2 transitions with verification. `TaskService` coordinates snapshot/fork preparation and atomic task creation through `CandidateWorkspaces` and `TaskStore`. Domain/application code contains no filesystem, SQLx, Tokio, or HTTP types. The SQLite adapter implements both store contracts and applies task policy inside run mutations; custom stores must preserve that combined unit of work.

Tests cover three candidates, isolated edits, source changes after capture, idempotent forks, snapshot tampering, persisted selection, failed protected tests, fresh verification, shared limits, concurrent admissions, rollback of both accounting and attempts, atomic task creation, approval/paused waits, and real killed workers at model admission and after verification execution. Non-repeat-safe verification requires explicit reconciliation before retry. Run:

```text
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --test tasks --locked
```

Tests run trusted host code in copied workspaces. They can access host resources, and direct-child termination does not guarantee termination of descendants. The managed workspace policy supplies file isolation; it does not provide an OS security sandbox. Process-kill tests establish application-crash recovery, not physical power-loss behavior. Automatic retention, MCTS policy/statistics, and distributed workers remain later milestones.

<a id="benchmarks"></a>

## Durable coding benchmarks

The benchmark runner compares one candidate, three independent candidates, and bounded search on frozen coding cases. All arms use the same model, context policy, tools, feedback tests, and total attempt/model/token caps. Version 1 retains the two independent arms; version 2 adds search. This supplies a baseline for deeper expansion and MCTS.

<a id="benchmarks--run-the-pilot"></a>

### Run the pilot

The [multi-file suite](#multifile-benchmarks) adds three projects that require two module repairs, failed-feedback warmup, competing branch outcomes, and three repetitions per case. Use `examples/configs/benchmark-multifile-scripted.json` for 27 local trials or `examples/configs/benchmark-multifile-nvidia-pilot.json` for a small live pilot. Per-case Markdown summaries expose success rates, usage, attempts, and mean wall time alongside frozen input digests. Its first live result verified one candidate and recorded shared-budget and search-action failures; the guide preserves the exact settings and limitations.

The five bundled cases cover addition, clamping, Unicode palindromes, upper medians, and whitespace-separated words. The scripted model makes known repairs to demonstrate execution and reporting:

Feedback and final verification pin `CARGO_TARGET_DIR` to the current managed workspace's `target` directory. Candidate branches and the temporary held-out verification workspace compile separately, even when the harness environment or project Cargo configuration selects a shared build directory. Dependency downloads can still use the inherited Cargo home. Build artifacts are excluded from snapshots, patch comparisons, and model-visible file listings.

```powershell
cargo build --locked
$benchmarkId = (& .\target\debug\act0r.exe --db .\benchmarks.db benchmark start --suite-file .\examples\configs\benchmark-scripted.json).Trim()
.\target\debug\act0r.exe --db .\benchmarks.db worker run --until-idle
.\target\debug\act0r.exe --db .\benchmarks.db benchmark status $benchmarkId
.\target\debug\act0r.exe --db .\benchmarks.db benchmark report $benchmarkId --output .\benchmark-report.md
.\target\debug\act0r.exe --db .\benchmarks.db --json benchmark report $benchmarkId --output .\benchmark-report.json
```

The scripted pilot passes all five cases in both arms. One candidate uses 15 model calls across the suite; three candidates use 45. Equal caps permit different actual usage. These fixture results establish runner behavior; they do not show a quality benefit from additional candidates.

To use LM Studio, select the model and endpoint in [benchmark-lmstudio-agent.json](../examples/configs/benchmark-lmstudio-agent.json), then start a fresh benchmark:

```powershell
$benchmarkId = (& .\target\debug\act0r.exe --db .\lmstudio-benchmarks.db benchmark start --suite-file .\examples\configs\benchmark-scripted.json --agent-config-file .\examples\configs\benchmark-lmstudio-agent.json --repetitions 2).Trim()
.\target\debug\act0r.exe --db .\lmstudio-benchmarks.db worker run --until-idle
.\target\debug\act0r.exe --db .\lmstudio-benchmarks.db benchmark report $benchmarkId
```

The agent override replaces the suite's agent configuration. Each case still supplies its goal; fixture scripts are used only by the scripted provider. Add `--case clamp` to run one case. An unknown case is rejected. Version-1 repetitions alternate one/three then three/one; version 2 rotates all three strategies. Trials within a job execute sequentially; other jobs and ordinary runs can share the worker.

A live clamp pilot with `prism-ml/bonsai-27b` exercised both arms. Neither reached final verification: repeated model requests failed before responses were recorded under the adapter's 45-second timeout. The one-candidate arm admitted 5 calls and the three-candidate arm admitted 14. Both kept their uncertain reservations charged and saved completed unsuccessful trials. This validates live failure accounting, while successful benchmark execution with LM Studio and repeated quality comparisons remain to be established.

<a id="benchmarks--nvidia-hosted-model"></a>

### NVIDIA hosted model

The NVIDIA configuration in [benchmark-nvidia-agent.json](../examples/configs/benchmark-nvidia-agent.json) uses `moonshotai/kimi-k3` at `https://integrate.api.nvidia.com/v1/chat/completions`; the `openai` provider label selects the compatible request adapter. It keeps the LM Studio output cap, context policy, and shared budget caps, with an explicit 90-second request / 10-second connect policy and two attempts. [Provider reliability](providers.md#provider-reliability) describes pinned limits, saved diagnostics, and durable `Retry-After` backoff. Earlier pilots below predate that policy. NVIDIA's [Kimi K3 reference](https://docs.api.nvidia.com/nim/reference/moonshotai-kimi-k3-infer) documents reasoning controls but does not list the adapter's `response_format` field. The live endpoint accepted that field and completed a structured-action coding run; this does not establish that it enforces every schema constraint.

An earlier version-1 live clamp pilot produced these results:

| Candidates | Verified success | Model calls | Reported input/output tokens | Held tokens | Wall time |
| --- | --- | --- | --- | --- | --- |
| 1 | Yes | 5 | 3,876 / 145 | 7,647 | 12.2 seconds |
| 3 | No | 6 | 3,457 / 421 | 14,928 | 18.2 seconds |

The single candidate passed both feedback and hidden final tests. In the three-candidate arm, one response had no structured text, another failed action JSON validation, and the last candidate exhausted retries on HTTP 429 rate limits. Uncertain charges remain in both ledgers. A separate synthetic compatibility probe preceded the benchmark and is excluded from its trial counters. These results establish an end-to-end success and expose response-format and rate-limit handling concerns; one repetition does not establish a reliable comparison of strategies or providers.

A version-2 clamp pilot on 2026-10-07 used one repetition, 12 model calls / 60 attempts / 150000 tokens per strategy, width 3, warmup 2, rollout 4, and a 10000 ms serial request interval:

| Strategy | Verified success | Model calls / attempts | Held tokens | Wall time |
| --- | --- | --- | --- | --- |
| One independent candidate | No | 2 / 2 | 14538 | 100.6 seconds |
| Three independent candidates | No | 6 / 6 | 43614 | 270.3 seconds |
| Bounded search | No | 2 / 2 | 14538 | 90.7 seconds |

All requests failed before model responses were recorded under the adapter's 45-second request limit; reported token usage was unavailable. Search ended during warmup, before proposal generation. The saved report demonstrates completed unsuccessful trials and conservative charges, but does not validate live expansion or establish a quality comparison. That milestone stopped after this pilot. Benchmark ID: `013126d4-5312-43f8-a4e4-25754026da4a`. Local artifacts are in `.build/search-benchmark-validation/`: `nvidia-report.json`, `nvidia-report.md`, and `nvidia-task-diagnostics.json` (ignored runtime artifacts).

After provider reliability was implemented, one fresh clamp pilot on 2026-10-07 used the same shared caps and search settings, with the pinned 90000 ms request / 10000 ms connect policy, two attempts, and 10000 ms serial pacing:

| Strategy | Verified success | Model calls / attempts | Reported input/output tokens | Held tokens | Wall time |
| --- | --- | --- | --- | --- | --- |
| One independent candidate | Yes | 4 / 8 | 3881 / 157 | 0 | 234.0 seconds |
| Three independent candidates | Yes | 10 / 19 | 9728 / 2523 | 0 | 467.2 seconds |
| Bounded search | Yes | 4 / 12 | 3689 / 131 | 0 | 158.4 seconds |

All 18 HTTP requests returned 200 with usage and safe request IDs, taking 33.7–88.5 seconds. The one-agent arm's four responses took 53–58 seconds, exceeding the old request limit. One independent candidate reached the 2048-token output cap and stopped with `model_incomplete`; the other two passed final tests. Its receipt retained category, status, latency, provider ID, and reported usage. No transport timeout or 429 occurred, so provider-advised backoff was verified with local HTTP fixtures rather than this live pilot.

Search completed one live proposal call, saved two distinct actions (`run_tests` and `read_file`), executed isolated continuations, and recorded one visit and passing feedback for each. It selected the branch with no additional model calls and verified its patch against the frozen held-out tests. This validates live expansion and reporting on this fixture; one repetition of a three-line task does not establish a strategy ranking or general provider reliability. No additional pilot was started. Benchmark ID: `8edd5787-eb2f-4a86-a634-b4cd6124cabc`. Artifacts are in `.build/provider-reliability-validation/`: `nvidia-pilot.json`, `nvidia-report.json`, `nvidia-report.md`, `nvidia-request-diagnostics.json`, and `validation.json` (ignored local artifacts).

To supply a saved Windows user key to the current worker process without including it in a configuration file:

```powershell
$env:OPENAI_API_KEY = [Environment]::GetEnvironmentVariable("OPENAI_API_KEY", "User")
$benchmarkId = (& .\target\debug\act0r.exe --db .\nvidia-benchmarks.db benchmark start --suite-file .\examples\configs\benchmark-scripted.json --agent-config-file .\examples\configs\benchmark-nvidia-agent.json --case clamp).Trim()
.\target\debug\act0r.exe --db .\nvidia-benchmarks.db worker run --until-idle
.\target\debug\act0r.exe --db .\nvidia-benchmarks.db benchmark report $benchmarkId
Remove-Item Env:\OPENAI_API_KEY
```

<a id="benchmarks--suite-configuration"></a>

### Suite configuration

[benchmark-scripted.json](../examples/configs/benchmark-scripted.json) is a version-1 suite:

| Field | Meaning |
| --- | --- |
| `name`, `version` | Saved experiment name and suite format version. |
| `repetitions` | 1–20 repetitions per case and arm. At most 200 total trials and 50 cases. |
| `agent` | Existing AgentConfig, including model, output cap, context, writable paths, approval policy, and feedback command. |
| `budget` | Identical total attempt, model-call, and token admission caps for each arm. A candidate's model/token caps are raised to these shared caps so a single candidate can use the whole allowance. |
| `protected_paths` | Frozen tests and build configuration, disjoint from writable paths. |
| `final_test_command` | Pinned command for independent final verification. |
| `cases` | Distinct names, goals, source paths, final-test source paths, and optional fixture action scripts. |

Paths resolve relative to the suite file; absolute paths are also accepted. Each source root contains a runnable project with feedback tests. Its separate final-test root contains additional files at their intended project-relative paths, such as `tests/held_out.rs`. The two roots must be separate, with neither nested inside the other. Final files must be protected, unwritable, and absent from the source project.

Starting a benchmark freezes both inputs into hashed snapshots beside the database. Editing the original source or final tests later does not change the saved experiment. Models receive the goal and feedback observations. Final tests are absent from candidate workspaces and model prompts. After a candidate finishes, verification applies its permitted patch to a clean baseline, adds the frozen final files, and executes the pinned final command. The evaluation saves both input digests, the candidate digest, test output, and patch. A feedback pass alone cannot count as benchmark success.

<a id="benchmarks--reports-and-recovery"></a>

### Reports and recovery

<a id="benchmarks--three-strategy-suites"></a>

#### Three-strategy suites

[benchmark-search-scripted.json](../examples/configs/benchmark-search-scripted.json) is the version-2 five-case fixture:

```powershell
$benchmarkId = (& .\target\debug\act0r.exe --db .\search-benchmarks.db benchmark start --suite-file .\examples\configs\benchmark-search-scripted.json --case addition).Trim()
.\target\debug\act0r.exe --db .\search-benchmarks.db worker run --until-idle
.\target\debug\act0r.exe --db .\search-benchmarks.db benchmark report $benchmarkId --output .\search-benchmark-report.md
```

Version 2 requires `search` with `width` (2–4), `warmup_steps` (0–16), and `rollout_steps` (1–16), pinning the `feedback_cost` v1 policy. Cases may provide `search_script` and `scripted_proposals` for fixtures; live agent overrides ignore these fixtures. Search shares the case goal, model, context, tools, permissions, feedback command, frozen final tests, and total admission caps. There are at most 200 trials: five cases allow 13 repetitions with three arms. Arm order rotates over repetitions; version-1 plans retain their original order.

Warmup, proposals, failures, retries, and rollouts charge the shared task ledger, with the prefix counted once. Only the selected branch runs held-out verification after feedback is frozen; hidden-test failure does not trigger another selection. Independent arms measure whether any candidate verifies; search measures its selected candidate. Equal caps do not make those selection rules identical.

For NVIDIA, add `--agent-config-file .\examples\configs\benchmark-nvidia-agent.json --case clamp` at start and configure the process key as described above. A serial worker example adds pacing:

```powershell
cargo run --locked --example benchmark_worker -- .\search-benchmarks.db 10000
```

The optional interval is 0–60000 milliseconds between model-request starts, defaulting to 10000. It also waits before the first request after restart. The helper paces the existing worker; durable admissions, retry deadlines, outcomes, and recovery remain engine operations. An interrupted admitted request keeps a conservative reservation, including during the pacing wait. Wall time includes pacing; this does not guarantee provider quota availability.

<a id="benchmarks--frozen-reports"></a>

#### Frozen reports

`benchmark status` exposes strategies, task/search IDs, progression, and summaries. `--json` includes initial run IDs and completed search evidence. Reports contain overall and per-case success rates, model calls, charged and held tokens, attempts, elapsed time, patches, and saved run failures. Controller retirement cancellations are omitted from failure diagnostics. Search evidence includes proposals, visits, rewards, feedback, and rollout usage. Summaries include completed trials only; inspect the active task for current usage. Charged tokens include uncertain reservations, with reported usage retained separately. Admission caps do not guarantee provider billing.

Elapsed time uses UTC timestamps from the start of trial preparation through task completion. It includes setup and downtime; clock reversal clamps negative durations to zero. It measures workflow wall time rather than model latency. A selected patch is saved with its verified result, so later workspace edits do not alter the report.

Every planned trial has stable task, initial run, and optional search IDs before preparation starts. The worker persists `preparing`, then publishes the task graph, optional search, memberships, and trial activation in one transaction. Restarting reuses identities and frozen inputs. Preparation failures have zero activity charges; storage failures leave preparation resumable. Independent completion updates accounting, metrics, and summaries together. Search completion waits for the controller's terminal receipt, which commits with frozen evidence, final task usage, metrics, and summaries. Duplicate completions cannot add statistics or charges.

Restart the usual worker to continue an interrupted benchmark. Candidate approvals, human-input waits, pause, cancellation, and reconciliation use the existing run commands. A waiting trial stops that job's progression; `--until-idle` exits when only waits remain. Inspect candidate run IDs with `task status TASK_ID` and resolve their waits before restarting. Initial input capture can leave unreferenced snapshots if starting fails before its database commit; artifact retention remains future work.

<a id="benchmarks--architecture-and-verification"></a>

### Architecture and verification

`Benchmark` owns the plan, progression, input identities, and statistics. `BenchmarkService` coordinates `CandidateWorkspaces`, `TaskService`, `SearchService`, and `BenchmarkStore` through the existing worker. `Search` owns expansion/selection, `CodingTask` owns admission, and `Run` owns execution. SQLite observes task and search changes inside their transactions. Custom stores must preserve the combined task/run/search/benchmark unit of work. Domain and application contracts contain no SQLx, Tokio, HTTP, or filesystem implementation types.

Three-arm suites use the shared baseline for benchmarks, branch preparation and search. New optional evidence and strategy fields default to absent or independent and are omitted when absent on old records. Manual forks remain rejected; search controllers own their pinned expansion plans.

```text
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --test benchmarks --locked
```

Checks cover both versions, five cases, proposal accounting, equal caps, hidden tests, false positives, budget exhaustion, frozen inputs, activation/terminal-search rollback, waits, duplicate completion, arm order, and report exports. Process-kill tests cover preparation, search publication, and an admitted proposal whose uncertain reservation remains in benchmark metrics. Existing search tests cover expansion, scoring, and final verification interruptions.

The suite runs trusted host code in copied directories, using the existing workspace protections rather than an OS sandbox. Process-kill checks establish application-crash recovery; they do not simulate physical power loss. This small pilot needs more realistic cases and repeated real-model trials before drawing conclusions about search quality or enabling MCTS.

<a id="branching"></a>

## Durable checkpoints and forks

Coding candidates can continue from a frozen workspace and saved agent state. [Bounded search](#search) now uses this foundation to propose alternatives and compare continuations; MCTS remains later work. Standalone `agent start` runs are not supported by this first version; branches belong to a coding task so they share its budget and final verification baseline.

<a id="branching--run-the-local-example"></a>

### Run the local example

The deterministic example needs no model server or API key. Use a fresh database so its worker has only this task to schedule:

```powershell
cargo run --locked --example branching -- .build/branch-demo/demo.db
```

One candidate lists files, reads the source, and fixes addition. The example pauses it, captures a checkpoint, creates two continuations, and cancels the paused parent. Both children run tests and finish. The task selects a verified patch containing the shared edit.

The shared prefix uses six activity attempts and three model calls. Each child executes four new attempts and two new model calls. Task totals are 14 attempts and seven calls. Each child's cumulative ledger shows ten attempts and five calls, including its inherited prefix. The two workspaces are independent copies; edits in one do not affect the other or the source project.

<a id="branching--cli-control"></a>

### CLI control

Pause a candidate at an activity boundary before capturing it. The eligible boundary has exactly one saved, unattempted next model request and no pending tool action, approval, input wait, timer, verification, or active/indeterminate execution. An initial candidate before its first model call is eligible too.

```powershell
.\target\debug\act0r.exe --db .\tasks.db run pause $parentRunId
$checkpointId = (& .\target\debug\act0r.exe --db .\tasks.db checkpoint create $parentRunId).Trim()
.\target\debug\act0r.exe --db .\tasks.db worker run --until-idle
.\target\debug\act0r.exe --db .\tasks.db checkpoint status $checkpointId
$forkId = (& .\target\debug\act0r.exe --db .\tasks.db fork create $checkpointId --candidates 2).Trim()
.\target\debug\act0r.exe --db .\tasks.db worker run --until-idle
.\target\debug\act0r.exe --db .\tasks.db fork status $forkId
.\target\debug\act0r.exe --db .\tasks.db run cancel $parentRunId
.\target\debug\act0r.exe --db .\tasks.db task status $taskId
.\target\debug\act0r.exe --db .\tasks.db task result $taskId
```

Creation commands persist preparation requests. The owned worker captures files and publishes the checkpoint or all child runs atomically. Inspect status for `preparing`, `ready`, or `failed`; a fork requires a ready checkpoint. If pausing caught a tool or other ineligible boundary, checkpoint creation explains the restriction. Resume the candidate and pause at a suitable boundary before trying again.

Keep the parent paused until checkpoint capture becomes ready. Capture pins both its workflow revision and history sequence; resuming and pausing during capture invalidates the request even if no workflow step completed. After capture, the checkpoint is reusable while its task remains open. Forking leaves the parent paused. Resume it to pursue another continuation, or cancel it to let the task finish when the children finish. A paused parent keeps the task waiting for user control.

All commands support `--json`. Task status includes fork plans and each child's origin. Run history contains `run_forked` with the checkpoint, parent revision/history sequence, and inherited ledger. Ancestor activity IDs, attempts, and history remain under the ancestor run; children get fresh identities and local execution records.

For automation, supply UUIDs with `--checkpoint-id` and `--fork-id` and retry with the same ID after an uncertain response. Identical requests return the persisted receipt, including after completion; conflicting parameters fail. A different ID requests new work. Failed preparations remain inspectable; correcting the cause requires a new request ID.

<a id="branching--state-verification-and-budgets"></a>

### State, verification, and budgets

A checkpoint pins the agent's bounded context, observations, turn, configuration, exact next model request, cumulative usage, and workspace manifest. Children start with that request rather than reconstructing the prompt or replaying completed model/tool calls. Later requests use the normal bounded-context agent loop. Children and nested forks preserve cumulative model/token/attempt limits.

Task accounting charges the original prefix once, then each branch's new work. It subtracts each child's frozen inherited counters when rebuilding the aggregate ledger. Missing or uncertain prefix usage stays charged; nesting cannot refund it. Admission still checks the shared task budget in the same transaction that records an attempt.

The final verification baseline remains the task's original snapshot, even when a child starts from a later checkpoint. A selected patch therefore includes all shared-prefix edits. Protected tests and configuration must remain unchanged at capture, and the copied child workspace must match the checkpoint manifest before publication. Existing final verification rebuilds the complete permitted patch on a clean original baseline and runs the fixed tests.

`TaskConfig.candidates` specifies the initial root count. Actual membership grows through persisted fork plans. The initial limit is 16 planned candidate slots per task; failed plans retain their reserved ordinal ranges. Benchmark tasks reject manual forks; version-2 search arms permit only controller-owned expansion within the pinned search configuration.

<a id="branching--architecture-and-recovery"></a>

### Architecture and recovery

The `Checkpoint` domain aggregate owns eligibility and its immutable continuation. `CodingTask` owns fork planning, membership, and unique-work accounting. `BranchService` coordinates the `BranchStore` and `CandidateWorkspaces` ports. The local filesystem adapter captures and verifies workspaces; SQLite owns the atomic publication of child runs, memberships, task counters, and fork status. A composite work coordinator shares the existing owned worker with benchmark preparation. Domain decisions perform no filesystem or database I/O.

The baseline stores checkpoint and fork indexes, search jobs and auxiliary task runs. Optional branch/origin/fork fields default to absent. Ordinary root candidates retain `coding.candidate` version 1; seeded manual children use version 2. Both continue through the same pinned agent-v2 transitions. Search-owned candidates use a separate definition and controller-owned expansion plan.

Preparation intent is durable before filesystem work. Stable artifact and child IDs let a restarted worker reuse complete copies. A failed database transaction publishes no partial child graph or additional charges; transient storage errors leave preparation pending for retry. Detected invalid boundaries, tampered files, and filesystem preparation failures become explicit failed receipts. A crash after publication resumes those same child runs. Unreferenced filesystem copies can remain after failures; retention and cleanup are later work.

Run the checks locally:

```powershell
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --test branches --locked
```

Branch tests cover saved-request/context reuse, independent edits, nested budgets with uncertain prefix usage, invalid boundaries, source changes during capture, tamper detection, concurrent slot planning, fixed benchmark guards, transactional rollback, CLI operation, and workers killed before checkpoint publication, before fork publication, and after fork commit. Process-kill tests establish application-crash recovery; the existing trusted-host execution and physical power-loss limits still apply.

<a id="search"></a>

## Durable bounded search

Search automates one checkpoint expansion into two to four distinct next tool actions. It runs bounded continuations, records feedback and usage once per branch, selects a passing continuation, and verifies its complete patch. Search v2 adds a durable feedback check when a child reaches its rollout boundary without a current test result. The `SearchStrategy` port supplies `feedback_cost` v1; MCTS and repeated tree expansion remain future work.

<a id="search--local-demo"></a>

### Local demo

No server or API key is needed:

```powershell
cargo run --locked --example search -- .build/search-demo/demo.db
```

The scripted fixture inspects addition, proposes multiplication, addition, and returning the left operand, then runs feedback tests in three independent workspaces. Addition is selected and passes separately frozen final tests. The task uses six total model calls and 18 activity attempts: two warmup calls, one proposal call, one continuation call per child, and one final verification attempt. Shared warmup work is charged once. The scripted numbers verify execution mechanics, not a real-model performance claim.

The same product is available through the CLI:

```powershell
cargo build --locked
$searchId = (& .\target\debug\act0r.exe --db .\search.db search start --source .\examples\benchmark-cases\addition\source --config-file .\examples\configs\search-scripted.json).Trim()
.\target\debug\act0r.exe --db .\search.db worker run --until-idle
.\target\debug\act0r.exe --db .\search.db search status $searchId
.\target\debug\act0r.exe --db .\search.db search result $searchId
```

Status lists the search phase, strategy version, checkpoint, proposed actions, branch IDs, visits, rewards, feedback results, and shared task usage. `--json search status` returns both the persisted search and task records. Result is available only after successful final verification; it contains the complete patch relative to the task's original baseline.

For automatic feedback, start a fresh search with [search-feedback.json](../examples/configs/search-feedback.json):

```powershell
$searchId = (& .\target\debug\act0r.exe --db .\feedback.db search start --source .\examples\benchmark-cases\addition\source --config-file .\examples\configs\search-feedback.json).Trim()
.\target\debug\act0r.exe --db .\feedback.db worker run --until-idle
.\target\debug\act0r.exe --db .\feedback.db search status $searchId
.\target\debug\act0r.exe --db .\feedback.db search result $searchId
```

This version-2 fixture allows only one edit step per child and three total model calls. The controller checks all three edited workspaces without asking the model for another action, records rewards `[0,1,0]`, and verifies addition with the separately frozen final tests. It uses 15 activity attempts: four warmup attempts, one proposal, three saved action deliveries, three edits, three automatic feedback checks, and one final verification. The same one-step bound in v1 leaves all three branches unverified.

<a id="search--configuration"></a>

### Configuration

See [search-scripted.json](../examples/configs/search-scripted.json) for v1 and [search-feedback.json](../examples/configs/search-feedback.json) for v2. `task` is a normal TaskConfig with exactly one initial root; model, tools, approvals, permissions, and admission budgets retain their existing contracts. Other fields:

| Field | Meaning |
| --- | --- |
| `version` | `1` keeps the original bounded rollout behavior; `2` evaluates stale feedback at the final rollout boundary. Existing saved searches keep their pinned version. |
| `strategy` | Defaults to `{ "name": "feedback_cost", "version": 1 }`. The matching implementation must be available on restart. |
| `width` | Maximum distinct proposals, between 2 and 4. Fewer than two, duplicates, `finish`, and `ask_human` expansion actions fail validation. |
| `warmup_steps` | Completed root tool steps before capture, between 0 and 16. |
| `rollout_steps` | Maximum completed tool steps in each child, between 1 and 16. A branch can stop sooner when feedback passes or the agent finishes. Model/retry/attempt budgets also bound paths that do not complete tools. |
| `scripted_proposals` | Distinct fixture actions, used only by the scripted provider. Remove this field when using a live provider. |
| `final_tests` | Optional separately frozen test source and fixed command. Its source path is relative to the configuration file's directory. Without it, clean final verification reruns the ordinary fixed feedback tests. |

For LM Studio or NVIDIA, copy the target configuration from the existing agent/benchmark examples into `task.agent.target` and remove `scripted_proposals`. The proposal adapter makes one schema-constrained request returning an `actions` array through Responses or Chat Completions. Ordinary continuation requests still return one action. The production proposal request, output allowance, selected context, and reservation persist before dispatch. Both proposal HTTP formats are exercised against local mock servers. A live NVIDIA clamp pilot completed expansion, isolated continuations, and held-out verification; [benchmark results](#benchmarks) record its settings and limitations.

<a id="search--feedback-and-final-evaluation"></a>

### Feedback and final evaluation

Each proposal seeds a child's first action directly from the saved proposal outcome. Replaying that local decision has no model-call charge. Subsequent actions use the existing model/tool loop and bounded context. Children inherit the checkpoint's context and cumulative usage, with independent workspace copies.

After a bounded continuation, a branch receives reward 1 when its feedback tests passed after its last edit, or 0 otherwise. Visits and the frozen result are recorded once under the child run ID. The default policy selects among passing branches by new charged tokens, model calls, activity attempts, then proposal order. It does not use model confidence or held-out results. There is no automatic model judge.

In v2, the controller tracks feedback freshness independently of the bounded prompt. Completed edits invalidate it; a completed agent test action refreshes it, including a failing result. Reads leave it unchanged. At a child's final paused boundary, stale feedback schedules one `search.feedback` activity with the pinned public test command. A fresh agent result is reused. The automatic check is an evaluation phase outside `rollout_steps`: it charges an activity attempt and ordinary retry costs, but consumes no model call or token reservation. If the shared budget cannot admit it, the branch stops with budget failure evidence and receives zero reward. Terminal failures, cancelled runs, user pauses, human waits, and reconciliation blocks are not resumed to force a check.

The result commits before scoring and is copied into `nodes[].result.automatic_feedback`, including its activity ID, phase, and tool output. A failed public test gets zero reward; denied approval gets a saved `denied` receipt without executing the command. Status and JSON reports expose these receipts. Automatic feedback never uses held-out tests or modifies the saved rollout's step count.

Selection retires the root and other children, supersedes the winner's unused next model request, and schedules clean final verification in one transaction. Protected paths and the original task baseline still determine the complete patch. Held-out files never enter candidate workspaces or model prompts, and their tests run only for the selected candidate after all search feedback is frozen. If those tests fail, search completes without a verified solution; it does not try other branches using that failure as feedback. This keeps final evaluation separate from planning.

If no branch passes feedback before its bound or budget, the task reports no solution with saved branch evidence. Insufficient warmup, short rollouts, poor proposals, or weak feedback tests can limit quality. [Version-2 benchmarks](#benchmarks) compare search with one and three independent candidates under equal total caps, freezing search evidence with final metrics. Version-1 benchmark plans retain their two arms.

<a id="search--budgets-controls-and-recovery"></a>

### Budgets, controls, and recovery

The [multi-file suite](#multifile-benchmarks) captures failed feedback before fixture repairs, explores alternative edits, and requires corrections in two modules. It runs three repeated trials per case and preserves both passing and failing branch evidence. A live NVIDIA pilot also expanded the unrepaired failed-feedback checkpoint and recovered from HTTP 429; its branches remained unverified after an invalid action and exhausted rollout bounds. The guide records these outcomes separately from the successful scripted checks.

Proposal generation is a durable auxiliary run linked to the same CodingTask. Its model usage, attempts, retries, and uncertain reservations count alongside the root and each child's new work. SQLite checks task admission inside the same transaction that records any attempt, including proposals. Creating children cannot reset inherited allowances. Provider-reported usage can exceed reserved estimates; existing admission-accounting limits apply.

Agent decisions and proposal requests share the pinned `agent.target.policy` for request/connect timeouts and attempt limits. HTTP receipts expose safe status, latency, and request IDs; 429/5xx retry advice becomes a persisted deadline. Restart preserves the deadline and usage charge. See [provider reliability](providers.md#provider-reliability).

Search candidates yield at completed tool boundaries using a planner pause flag. A user pause remains distinct and prevents automatic resume. Existing `run pause/resume/cancel`, approval, human-input, and reconciliation commands work with IDs shown in search status. `worker run --until-idle` exits when remaining work needs user input or reconciliation. Cancellation of a branch becomes recorded failure evidence; cancelling the root before capture ends preparation. Manual forks of a search-owned task are rejected because the controller owns its bounded membership plan.

Automatic checks honor `require_approval` with a persisted `search_feedback` input wait and honor `repeat_safe_tests` with the existing recovery policy. When tests are safe to repeat, interruption before result persistence retries the same logical activity and charges another attempt. Otherwise recovery blocks for operator reconciliation. An accepted result flows through the same completion transition. A saved completed result is never re-executed after restart; duplicate scoring cannot increase visits. Resuming a user pause after feedback completion keeps the planner boundary intact until scoring and selection.

Search, task, and root creation commit together. The checkpoint ID, proposal run ID, fork ID, and child IDs persist before their work. Checkpoint/workspace publication reuses the existing durable branch service. Proposal completion, scores, selection, and verification survive restarts. A lost proposal response leaves an uncertain reservation; its bounded retry costs another call. Stable node receipts prevent duplicate rewards or visits, and selection cannot schedule verification twice.

The DDD boundary is `Search` for search phase, proposed alternatives, feedback eligibility, immutable rollout results and selection; `CodingTask` for shared membership, admission, and usage; and `Run` for durable execution and controls. `SearchService` coordinates the `SearchStore`, `SearchStrategy`, `ProposalClient`, and workspace/branch ports. SQLite atomically retires an unused model request, saves feedback intent, schedules execution or approval, and refreshes task state. The existing tool adapter executes the fixed tests. Domain decisions perform no filesystem, HTTP, or database I/O.

The baseline stores search jobs and auxiliary-run membership. Search v2 uses `coding.search_candidate` workflow v2; the v1 definition stays registered. Search markers carry their selected strategy version. No schema migration is needed for automatic feedback. Ordinary agent and candidate definitions and manual forks keep their behavior. For version-2 benchmark plans, set `search.version` to `2` to opt in; omission retains search v1. Unreferenced filesystem artifacts can remain after preparation failures; retention is later work.

<a id="search--verification"></a>

### Verification

```powershell
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --test search --locked
cargo test --test search_feedback --locked
```

Checks cover distinct action seeding, prefix and proposal accounting, uncertain charges, feedback false positives, separate held-out verification, approvals and user pauses, atomic proposal/score/selection rollback, idempotent contributions, CLI operation, and both provider HTTP formats. Process-kill tests stop workers after proposal admission, after execution before result persistence, after expansion planning, after a score commit, and after final selection. Existing trusted-host, descendant-process, and physical power-loss limitations still apply.

Automatic feedback checks add one-step rollout repairs, freshness reuse, model-cap independence, attempt-cap enforcement, persisted approvals and denial, pause/cancel during evaluation, atomic intent/result/score rollback, duplicate completion, and all three multi-file cases with two-step search rollouts. Process-kill tests cover feedback intent, execution before completion persistence, saved completion, saved score, and unsafe-test reconciliation.

<a id="multifile-benchmarks"></a>

## Multi-file search benchmarks

[benchmark-multifile-scripted.json](../examples/configs/benchmark-multifile-scripted.json) adds three dependency-free Rust projects to the version-2 benchmark runner. Each has a public API module and two implementation modules with independent defects. Correct solutions must repair both modules and pass feedback plus separately frozen held-out tests.

| Case | Required behavior | Competing fixture repairs |
| --- | --- | --- |
| `retry-schedule` | Capped exponential delay and a saturating retry deadline, respecting the attempt limit. | Linear delay, exponential delay, or an immediate maximum delay. |
| `path-routing` | Collapse repeated slash separators without changing segments; match prefixes on segment boundaries. | One-pass separator replacement, full segment normalization, or case folding. |
| `config-overlay` | Split assignments at the first equals sign; overrides win over defaults, including empty values. | Split at the last equals sign, the first equals sign, or only a spaced delimiter. |

The goals specify the complete public behavior. Held-out tests add unseen values and boundaries, rather than undisclosed requirements. Feedback exercises both defects. The initial projects compile but fail feedback; they are intentionally broken benchmark inputs.

<a id="multifile-benchmarks--repeatable-local-run"></a>

### Repeatable local run

```powershell
cargo build --locked
$benchmarkId = (& .\target\debug\act0r.exe --db .\multifile.db benchmark start --suite-file .\examples\configs\benchmark-multifile-scripted.json).Trim()
.\target\debug\act0r.exe --db .\multifile.db worker run --until-idle
.\target\debug\act0r.exe --db .\multifile.db benchmark report $benchmarkId --output .\multifile-report.md
.\target\debug\act0r.exe --db .\multifile.db --json benchmark report $benchmarkId --output .\multifile-report.json
```

The default is three repetitions of all three cases and three strategies: 27 trials. Arm order rotates over repetitions. Each strategy receives identical total caps of 24 model calls, 80 activity attempts, and 150000 tokens. `src/` is writable; tests and Cargo metadata are protected. Add `--case path-routing --repetitions 1` for a smaller run.

Every fixture first reads both implementation modules and runs failing feedback. Search captures that unchanged workspace after three completed tool steps, then proposes three different repairs to the first module. Each branch applies its saved proposal, repairs the second module, and reruns feedback within three tool steps. Only the middle proposal passes. Warmup and proposal usage count once in the shared ledger; every branch's new work counts toward the same caps. Final verification runs only after feedback-based selection.

| Strategy | Calls per case/repetition | Attempts | Charged fixture tokens | Verified solution |
| --- | --- | --- | --- | --- |
| One independent candidate | 7 | 14 | 140 | Yes |
| Three independent candidates | 21 | 42 | 420 | Yes |
| Bounded search | 10 | 26 | 200 | Yes |

The scripted provider returns prescribed actions and a fixed usage receipt. These numbers verify accounting and branch isolation; they are not measurements of model quality or evidence that search outperforms independent execution. Identical scripted repetitions check persistence, aggregation, and ordering. They do not estimate model variability.

Markdown reports include per-case success rates, model calls, charged/held tokens, attempts, and mean wall time. JSON retains each trial's model configuration, frozen digests, selected two-file patch, search nodes, rewards, diagnostics, and usage. A run with a four-call cap saves incomplete repairs and failure evidence without reporting success.

For automatic boundary feedback, copy the suite and set its `search` object to `{ "version": 2, "width": 3, "warmup_steps": 3, "rollout_steps": 2 }`, then start a fresh job. Both module edits now fit the two-step rollout, and the controller schedules fixed feedback tests as a separate durable evaluation before scoring. The search fixture uses seven model calls, 23 attempts, and 140 charged fixture tokens per case/repetition, with rewards `[0,1,0]` and one verified two-file patch. Independent arms retain their settings and accounting. Integration checks exercise all nine trials of one repetition with this configuration. The bundled historical suites omit `search.version`, preserving search v1.

The 2026-10-07 local run completed all 27 trials, with nine verified solutions per strategy and zero held tokens. Across the suite, one candidate used 63 calls / 126 attempts, three candidates used 189 / 378, and search used 90 / 234. The saved report, JSON, database, and inputs are under `.build/multifile-benchmark-validation/` as ignored local artifacts; the commands above reproduce the experiment.

<a id="multifile-benchmarks--live-provider-pilot"></a>

### Live provider pilot

[benchmark-multifile-nvidia-pilot.json](../examples/configs/benchmark-multifile-nvidia-pilot.json) runs one path-routing repetition with identical caps of 12 model calls, 60 activity attempts, and 150000 tokens per strategy. It uses the existing NVIDIA endpoint, Chat Completions, 2048 output tokens, bounded context, and the 90-second request / 10-second connect policy. It permits edits under `src/` so the model can repair either module. The goal, permissions, model settings, feedback, final tests, and total caps are identical across arms. It contains no scripted actions or proposals. The shared budget replaces the per-candidate model-call allowance when each trial is prepared.

```powershell
cargo build --locked --bin act0r --example benchmark_worker
$env:OPENAI_API_KEY = [Environment]::GetEnvironmentVariable('OPENAI_API_KEY', 'User')
try {
    $benchmarkId = (& .\target\debug\act0r.exe --db .\multifile-nvidia.db benchmark start --suite-file .\examples\configs\benchmark-multifile-nvidia-pilot.json).Trim()
    .\target\debug\examples\benchmark_worker.exe .\multifile-nvidia.db 10000
    .\target\debug\act0r.exe --db .\multifile-nvidia.db benchmark report $benchmarkId
} finally {
    Remove-Item Env:OPENAI_API_KEY
}
```

Start with one case and one repetition because live requests can be slow and rate limited. The small pilot's 12-call cap is below the 21 calls needed to complete all three prescribed fixture candidates; interleaved independent runs can stop before any has finished. It deliberately permits budget exhaustion and cannot establish a strategy ranking. For the larger repeated suite, use `benchmark-multifile-scripted.json` with `--agent-config-file` pointing to [benchmark-multifile-nvidia-agent.json](../examples/configs/benchmark-multifile-nvidia-agent.json); its fixture scripts and proposals are ignored by the live provider. Changing caps or settings requires a fresh job; the saved job keeps its pinned inputs. Models may choose a different warmup or proposal sequence from the fixtures. Inspect the saved checkpoint and feedback to establish whether the live expansion occurred before the repair was complete.

<a id="multifile-benchmarks--recorded-live-result-2026-10-07"></a>

#### Recorded live result, 2026-10-07

Job `4b31b420-bee7-484b-b6e2-53f66d0049b4` used the bundled pilot settings, one repetition, serial requests paced at 10000 ms, and separately frozen held-out tests. All three trials completed and retained their reports:

| Strategy | Verified | Model calls | Attempts | Reported input/output tokens | Held tokens | Wall ms |
| --- | --- | --- | --- | --- | --- | --- |
| One independent candidate | Yes | 7 | 14 | 10281 / 431 | 0 | 285820 |
| Three independent candidates | No | 12 | 24 | 13883 / 754 | 0 | 450461 |
| Bounded search | No | 11 | 25 | 16350 / 1796 | 8300 | 399054 |

The single candidate read both modules, observed failing feedback, repaired both, and passed feedback and held-out verification. All three independent candidates stopped on `task_budget`, with no final verification.

Search captured the original baseline digest and failed feedback before any repair. One warmup request returned HTTP 429; its persisted retry completed successfully. The prefix therefore used four calls, including the rejected request. The provider supplied no usage for that rejection, so its 8300-token reservation remains conservatively charged. Across the pilot, all 30 HTTP requests have diagnostic receipts: 29 returned HTTP 200 with reported usage and one returned HTTP 429.

The saved proposals were edits to each implementation module and a feedback run. Each branch scored exactly once with reward zero. The first repaired both modules but stopped on `model_invalid_action` before rerunning feedback. The second spent a step on a nonexistent edit path and reached its three-step bound after repairing both modules. The third spent its first step repeating failing feedback, then repaired both modules at its bound. Neither bounded branch obtained fresh passing feedback after its final edit; no branch was selected for held-out verification. This exposes action failures and insufficient rollout room for these particular sequences, without claiming the unevaluated patches would pass final tests.

These results support larger repeated trials with more room for corrective actions before choosing a search policy or adding MCTS. They do not rank the strategies. The report, JSON, safe request diagnostics, saved checkpoint, search record, database, and validation receipts are under `.build/multifile-benchmark-validation/` as ignored local artifacts. No second live pilot was run to tune these outcomes.

Search v2 now supplies fresh public feedback for paused branches whose last edits reached the rollout bound. It preserves terminal model failures and existing saved v1 jobs. This change has been validated locally with scripted fixtures and crash checks; a fresh live comparison has not been run.

<a id="multifile-benchmarks--architecture-and-recovery-checks"></a>

### Architecture and recovery checks

This suite reuses `Benchmark`, `Search`, `CodingTask`, the model/tool ports, and existing snapshots and transactions. No new scheduler, search policy, database schema, or model-selection heuristic is introduced. The report change renders already saved per-case metrics.

```text
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --test multifile_benchmarks --locked
cargo test --all-targets --locked
```

Checks cover 27 repeated trials, unchanged failed-feedback checkpoints, competing branch results, two-file verified patches, protected and absent held-out tests, per-case aggregate counters, exhausted shared caps, and reopened reports. Process-kill checks stop a worker after an edit reaches the filesystem before its result commits, and after a branch score commits. Restart reuses the edit journal, checkpoint, fork and child identities, retains charges, and cannot count a score twice. Existing trusted-host execution and application-crash recovery limits still apply.

See [benchmark contract](#benchmarks), [bounded search](#search), and [provider reliability](providers.md#provider-reliability).
