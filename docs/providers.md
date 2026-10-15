# Model providers

Native function calling, structured actions, timeouts and provider retries.

[`act0r doctor`](harness.md#doctor) reports the resolved new-chat model, endpoint, action format and API-key presence without making a model request. It can diagnose local configuration errors; it does not test endpoint availability, key validity or model capabilities.

- [Native model function calling](#model-function-calling)
- [Model-provider reliability](#provider-reliability)

<a id="model-function-calling"></a>

## Native model function calling

Saved targets select their model protocol and action format. Actor configurations and HTTP requests that omit `action_format` use structured JSON. Set it explicitly to `function_calling` for compatible Chat Completions servers; the tested NVIDIA `moonshotai/kimi-k3` connection supports this format. [`act0r chat`](harness.md#chat) shares the API's initial model defaults, including function calling for that tested NVIDIA target, and offers `--action-format` for new sessions. Existing sessions, queued requests and follow-ups retain their pinned target and format.

CLI actor and HTTP harness configurations can opt in through the saved target:

```json
"target": {
  "provider": "openai",
  "protocol": "chat_completions",
  "action_format": "function_calling",
  "model": "moonshotai/kimi-k3",
  "endpoint": "https://integrate.api.nvidia.com/v1/chat/completions"
}
```

The existing timeout and retry settings in `target.policy` remain available. Set `OPENAI_API_KEY` in the worker environment for compatible hosted endpoints, including NVIDIA. Credentials stay in the HTTP adapter and are omitted from saved request exports.

Chat defaults to a 90-second request deadline. For a slower endpoint, start a new session with `act0r chat --model-timeout-ms 300000 "Your request"`. The option accepts 1–600000 milliseconds and applies to each model attempt, including receipt of its response body. `act0r doctor --model-timeout-ms 300000` reports the same new-session setting without contacting the provider. Sessions retain their pinned deadline across restart and follow-ups; `--resume` refuses timeout overrides, and `/retry` preserves the failed request's original policy. Actor and HTTP callers can set `target.policy.request_timeout_ms` directly.

<a id="model-function-calling--saved-contract-and-execution"></a>

### Saved contract and execution

Function mode supports compatible Chat Completions endpoints. Each saved model request exposes only its currently allowed actions as named API functions, with `kind` removed from their parameter fields. The request uses `tools`, `tool_choice: "required"` and `parallel_tool_calls: false`, without `response_format`. Control functions such as `finish` and `ask_human` remain available only when the saved workflow schema permits them.

The adapter accepts exactly one modern `tool_calls` entry, validates its name, required and allowed fields against the exact saved schema, and reconstructs the existing model action. Native tool selections normalize to canonical `call_tool` decisions. The domain still validates argument types, permissions, worker aliases and workflow state before creating an activity. Approvals, dispatch, effects, results, usage and completion evidence use the existing Run journal. The provider does not execute tools, and no model loop runs inside the adapter.

Scoped and unscoped delegation share one function definition; the original saved schema alternatives remain authoritative on receipt. Restricted worker coordination exposes only the functions currently permitted. Native tool and planner catalogs require flat model contract 4 in function mode. Newly created native actors already pin that version. Saved nested actors continue using their original JSON contract; changing a target requires a new actor/session.

Malformed arguments, unknown functions, extra fields, duplicate top-level parameters, multiple calls and text-only responses cannot dispatch an action. Known provider usage remains charged. Format corrections follow the saved bounded retry policy and durable deadline, without switching formats. Truncation and refusal retain their existing diagnostic categories.

SQLite prevents older binaries from opening the new target pins. Omitted `action_format` preserves old serialized target payloads and request bodies. Function-mode context accounting includes function schema and instruction overhead. Responses and search proposals retain JSON mode.

<a id="model-function-calling--repeat-the-focused-planner-trial"></a>

### Repeat the focused planner trial

```powershell
cargo run --locked --offline --example planner_dispatch_probe -- --action-format function_calling
cargo test --locked --offline --test model_function_calling --test planner_dispatch_probe

# Prepare the exact NVIDIA request with zero model calls.
cargo run --locked --offline --example planner_dispatch_probe -- --target-config examples/configs/benchmark-nvidia-agent.json --action-format function_calling --prepare-only
# Resume the printed directory, preserving the original actor and target pins.
cargo run --locked --offline --example planner_dispatch_probe -- --resume GENERATED_FOLDER
```

The [planner probe](validation.md#planner-dispatch-probe) uses a deterministic native repair worker by default and three real protected Rust tests. Add `--live-worker` when preparing a configured trial to use that same model for the worker, with separate request and usage exports. Import, edit and test approvals are restricted to that generated fixture; the original source stays unchanged.

<a id="model-function-calling--kimi-observation-13-october-2026"></a>

### Kimi observation, 13 October 2026

One live trial with `moonshotai/kimi-k3` at NVIDIA completed in five model calls: delegate to the repairer, inspect its patch, import it, request fresh parent `run_tests`, and finish. All three parent contracts passed, the final completion report cited that same test activity, and both import/test approval checkpoints retained their exact saved state across restart. There were no model retries. Reopening the completed trial preserved verification and the five-call count.

Evidence is retained beneath the ignored directory `.build/planner-dispatch-probe/03b56b7e-752d-40e8-820b-fc6d7c36d7eb/`, including exact wire bodies, canonical decisions, usage, checkpoints and `report.json`. Both protected files and the generated source remained unchanged. The planner used 11,053 input tokens and 390 output tokens; runtime token/call budgets stayed unlimited.

This first trial verifies one live planner workflow with a deterministic worker. The earlier two JSON trials returned `finish_incomplete` immediately; the successful function trial also changes transport instructions, so the results do not isolate one schema feature as the cause. Kimi requested tests correctly and never encountered a completion refusal; local fixtures separately verify refusal recovery through three restarts.

NVIDIA's [Kimi API reference](https://docs.api.nvidia.com/nim/reference/moonshotai-kimi-k3-infer) documents OpenAI-compatible function definitions. Provider behavior and support still vary by model and deployment.

<a id="model-function-calling--hosted-workers-and-integration-13-october-2026"></a>

### Hosted workers and integration, 13 October 2026

The optional model-worker probe completed with Kimi for both roles: six planner calls, four worker calls and three passing parent tests after approved import. The worker's edit approval also survived restart. A legitimate failing baseline test exposed an overly strict example check; replaying its saved evidence with the corrected freshness check verified completion without new model calls. See [both-role probe evidence](validation.md#planner-dispatch-probe--both-roles-use-kimi-13-october-2026).

The [two-worker demo](validation.md#dependent-coding-demo--native-function-trial-13-october-2026) then used native functions for every role and completed in 18 calls: four API, five consumer and nine planner calls, with zero retries. The dependency hold, approved transfer, explicit release and consolidated import all ran through the existing runtime. Eight fresh tests passed in each worker and the parent; the separately approved applied project also passed all eight tests. A disposable dependency check verifies that the consumer uses API output, accepting Kimi's imported function call as well as the fully qualified spelling.

The original terminal Runs and single application reopened with no new model calls or application. These are two successful hosted fixtures; they do not establish general planning reliability. Existing JSON-mode failures remain recorded, and default runtime budgets remain unlimited.

<a id="model-function-calling--normal-browser-workflow-13-october-2026"></a>

### Historical browser validation, 13 October 2026

Before the UI was removed, a separate browser trial submitted an ordinary Code request through the harness with two workers, the generated API/consumer project and the fixed eight-test command. The prompt described the repair and dependency in prose; it supplied no first-action JSON or scripted model replies. Kimi saved its plan, read both files, delegated both assignments, transferred the verified API patch into the held consumer, explicitly resumed it, imported the consolidated patch and ran fresh parent tests before verified completion. The browser reviewed each exact edit/test/transfer/import approval and a separate application to the generated project. Each worker, the parent and an independent run in the applied project passed all eight protected tests.

The API edit and reviewed transfer approvals each survived a backend process restart. Complete saved Run/activity/task payloads, including model pins, matched before and after reopening, and the consumer retained zero calls until the transfer and release. An explicit browser switch to JSON during the trial did not retarget the running session: all subsequent saved requests kept native functions. Reload after application retained the completed result and one application without new model calls. All three agent budgets remained unlimited; protected files stayed unchanged.

The trial used 23 calls with no retries: 14 planner, four API-worker and five consumer-worker calls. The planner also managed its durable checklist and read the project itself, so this differs from the more prescribed demo's 18 calls. Evidence is retained in `.build/nvidia-ui-check/5e9b6cf8-5d2d-420f-ae3d-89632f6f62ce/`, including saved requests, restart checkpoints, test results and browser screenshots. The 86 UI tests, targeted Rust default-selection test, two HTTP harness regressions, formatting and all-target Clippy passed. This remains a single fixture result.

<a id="provider-reliability"></a>

## Model-provider reliability

The harness also supports [explicit failed-turn recovery](recovery.md#turn-recovery) after automatic retries stop. It adds one model attempt at the saved checkpoint while retaining previous diagnostics, tools and charges.

Agent decisions and search proposals use the same HTTP adapter and durable activity retry rules. Configure limits in `agent.target.policy`; the engine pins them in the run and every saved model request.

```json
{
  "provider": "openai",
  "protocol": "chat_completions",
  "model": "moonshotai/kimi-k3",
  "endpoint": "https://integrate.api.nvidia.com/v1/chat/completions",
  "policy": {
    "request_timeout_ms": 90000,
    "connect_timeout_ms": 10000,
    "max_attempts": 2,
    "retry_delay_ms": 1000,
    "max_retry_delay_ms": 60000
  }
}
```

The `openai` provider label selects an OpenAI-compatible adapter. Both Responses and Chat Completions support this policy. The request timeout includes connection, response headers, and body reads. The connect timeout bounds connection establishment separately. Neither changes an already saved request when a configuration file is edited.

<a id="provider-reliability--limits-and-compatibility"></a>

### Limits and compatibility

| Setting | Default | Accepted values |
| --- | --- | --- |
| `request_timeout_ms` | 45000 | 1–600000 ms |
| `connect_timeout_ms` | 45000 | Positive, at most the request timeout |
| `max_attempts` | 2 | 1–8, including the first and interrupted attempts |
| `retry_delay_ms` | 500 | 0 through `max_retry_delay_ms` |
| `max_retry_delay_ms` | 2000 | At most 86400000 ms |

Omitting `policy` preserves the previous limits and serialized request shape. A partial policy inherits other defaults; lowering the request timeout also requires lowering the default connect timeout. New diagnostic and retry-advice fields are optional. Saved requests remain pinned across restart.

The CLI worker's `--activity-timeout-ms` is a base timeout. For HTTP model decisions and proposals, its effective timeout is at least the pinned request limit plus a 1000 ms completion margin. This lets the adapter return its receipt instead of being cancelled first by the worker. Other activities retain the base timeout. Shutdown grace still applies on shutdown; killing a worker before its receipt commits keeps the request uncertain.

<a id="provider-reliability--structured-output"></a>

### Structured output

JSON-mode Chat requests include the complete action schema in the system message as well as `response_format`. The instructions require the entire JSON envelope and forbid prose, Markdown, and extra fields. The added instructions count toward context limits and model reservations.

Compatible Chat Completions targets may instead pin [`action_format: "function_calling"`](#model-function-calling). That mode exposes the current saved actions as named API functions and requires one returned call. It retains canonical decisions, approvals, usage reconciliation and durable retry deadlines. Unknown/malformed/multiple calls are rejected; invalid function formats can retry within the same saved allowance. Omission retains JSON mode and its existing serialized payloads. Responses and search proposals continue using JSON.

Compatible servers can still wrap JSON in a code fence. The adapter accepts plain JSON or one complete outer fence with a `json` (case-insensitive) or empty language label. It then applies the same strict deserialization. It never searches prose or reasoning for an embedded action, joins multiple objects, invents missing fields, or removes unsupported action fields. Refusals, provider tool dispatch, and incomplete responses still fail before action parsing.

Invalid output receipts distinguish malformed JSON, with line and column, from JSON that does not match the required envelope and action fields. They retain reported usage and the request diagnostics without saving rejected response text or echoing provider-controlled values. A complete response whose action or proposal envelope fails strict deserialization can retry through the durable activity scheduler. Each retry sends the same pinned request; no rejected text is extracted into an action or added to the prompt. The saved `max_attempts` policy (two by default), model-call caps, and token budgets bound these retries; use `max_attempts: 1` to stop after the first response. A stopped session retains its original error and a new request gets a new identity.

An empty or null Chat Completions `content` is incomplete output, even when the provider reports `finish_reason: stop`. The adapter never treats `reasoning_content` as an executable action. This distinguishes an absent final answer from JSON that fails action validation.

<a id="provider-reliability--durable-backoff"></a>

### Durable backoff

HTTP 429 and 5xx responses are retryable, as are complete responses with invalid action or proposal JSON. Other HTTP statuses, refusals, incomplete output, malformed HTTP response bodies, multiple choices, and provider tool dispatch stop that activity. Transport, connection, and request-timeout failures can retry within the same caps. The adapter performs one request per admitted activity attempt; it has no internal retry loop. Exhausting attempts keeps the last formatting error as the terminal failure; exhausting a usage cap prevents another HTTP request and stops with `budget_exhausted`.

Local retry delay grows exponentially up to `max_retry_delay_ms`. For 429 and 5xx responses, a valid `Retry-After` header adds a minimum deadline: the later of local backoff and the provider's advice. The adapter accepts integer seconds and HTTP dates, following [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after). Malformed and negative values are ignored; past dates cannot shorten local backoff. Large delays saturate rather than overflow. Provider advice can exceed the local delay cap, so the engine does not retry earlier than requested.

Failure receipt, finished attempt, usage reconciliation, history, and queued `ready_at` commit together. The scheduler admits the retry only when that saved UTC deadline is due. Restart does not recalculate the deadline or sleep from scratch. Forward or backward system-clock changes affect when UTC deadlines become due; this is the same clock contract as durable timers.

Every retry reserves another model call and attempt under the existing run and shared task budgets. A response with reported usage reconciles its reservation, including an error response. Missing usage retains a conservative reservation and increments uncertain calls. Backoff never refills a budget. Once attempt or usage limits prevent admission, no request is dispatched.

<a id="provider-reliability--request-receipts"></a>

### Request receipts

Each HTTP attempt records a category, elapsed milliseconds, stable client request ID, optional HTTP status, and optional provider request ID. Categories include `success`, `configuration`, `timeout`, `connection`, `transport`, `http_status`, `response_read`, `response_too_large`, `invalid_json`, `invalid_action`, `incomplete`, and `refusal`. Connection failures cover DNS, TCP, proxy, and TLS errors. A timeout may occur before headers or while reading the body; the latter retains the received status and ID. Elapsed time measures that attempt's HTTP operation, separate from benchmark wall time.

Provider IDs come from `x-request-id`, `request-id`, or `x-correlation-id`, falling back to a successful response's `id`. They are limited to 128 ASCII letters, digits, and `-_.:`; values containing the configured API key are dropped. Failure messages use fixed adapter-owned text. API keys, raw transport errors, and provider error bodies are not saved in diagnostic receipts. Successful structured actions and observations remain part of the normal agent history.

```powershell
.\target\debug\act0r.exe --db .\agent.db run status RUN_ID
.\target\debug\act0r.exe --db .\agent.db --json run status RUN_ID
.\target\debug\act0r.exe --db .\agent.db run history RUN_ID
```

Human status shows retry deadlines and completed request receipts. JSON exposes failure `request`/`retry_after`, successful result `diagnostics`, and every completed attempt. Benchmark reports freeze safe diagnostics alongside trial failures. [Benchmark configuration and live pilot results](research.md#benchmarks).

<a id="provider-reliability--verification"></a>

### Verification

```text
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

Local HTTP fixtures cover both protocols, success and error usage, timeouts before headers and during body reads, connection failure, invalid responses, safe IDs, and both header formats. Integration checks cover agent and proposal format recovery after reopening the database, repeated invalid output at the attempt limit, model caps after formatting failures and 429, a worker base timeout shorter than the model limit, transaction rollback, duplicate delivery, deadline overflow, and a killed worker after a committed retry. Process-kill checks demonstrate application-crash recovery, not physical power-loss behavior.

A single live NVIDIA clamp pilot passed held-out verification in all three benchmark arms, including proposal expansion and isolated continuations. Its 18 HTTP requests had saved status, latency, IDs, and usage; one candidate stopped on incomplete output. No 429 or transport timeout occurred in that pilot. [Exact settings, results, and limitations](research.md#benchmarks) keep this evidence separate from the local backoff and crash checks.
