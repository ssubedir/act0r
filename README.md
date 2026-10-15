# act0r

[![Linux CI](https://img.shields.io/github/actions/workflow/status/ssubedir/act0r/ci.yml?branch=main&label=Linux%20CI&logo=github&style=flat-square)](https://github.com/ssubedir/act0r/actions/workflows/ci.yml) [![Rust 1.94+](https://img.shields.io/badge/Rust-1.94%2B-dc6b3f?logo=rust&logoColor=white&style=flat-square)](Cargo.toml) [![License: MIT](https://img.shields.io/badge/License-MIT-2563eb?style=flat-square)](LICENSE) [![Active development](https://img.shields.io/badge/Status-active_development-d97706?style=flat-square)](docs/roadmap.md)

> [!IMPORTANT]
> **Active development:** APIs, configuration and database schemas may change. This project is experimental and is not yet production-ready.

**An AI agent harness with a Rust runtime and CLI, built on durable actors and workflows.**

[Quickstart](#run-the-cli) · [Examples](examples/README.md) · [Documentation](docs/README.md) · [Architecture](docs/architecture.md) · [Roadmap](docs/roadmap.md) · [Contributing](CONTRIBUTING.md)

act0r gives agents persistent identities, message-based coordination and recoverable task execution. Agents can delegate work, exchange results, use models and tools, wait for human input, and continue saved work after a worker restart. SQLite records their state, accepted messages, execution checkpoints and completed activity results.

The intended final harness combines **durable actors + Monte Carlo tree search (MCTS)** with interchangeable models and tools. Durable actors provide persistent execution and coordination; MCTS is planned to explore and evaluate alternative action sequences. Current development focuses on the durable actor foundation; full MCTS integration remains future work.

The current implementation includes native planner, answering and tool-enabled agents, a Rust CLI and a local HTTP API. Project discovery and coding provide an end-to-end workflow: inspect a folder, edit an isolated copy, run the project's checks and produce a patch for review.

act0r is in early development. Execution is local, with one worker owner per database and configurable concurrency across independent work.

## The actor model

The [actor model](https://en.wikipedia.org/wiki/Actor_model) organizes concurrent computation around independent actors. An actor has an address, private state and behavior for handling messages. In response to a message, it can send messages to other actors, create actors and determine its behavior for future messages. Actors communicate asynchronously rather than directly changing one another's state.

For AI agents, this gives each participant its own identity and responsibilities. A planner can send an assignment to a worker, receive a question or result, and coordinate the next step. Other actors can make progress while one task waits for a model, tool or human response.

## How act0r makes actors durable

act0r adds persistent mailboxes, serialized state transitions and durable task execution to this model. An actor keeps its identity across successive tasks; each task's execution has a separate **Run**. The worker consumes saved mailbox messages and advances Runs through recorded activities and waits.

| Concept | In act0r |
| --- | --- |
| Actor | A stable `AgentId`, saved state, versioned behavior and mailbox |
| Message | A saved envelope with sender, recipient, message ID and optional correlation ID |
| Task execution | A `RunId` with model/tool activities, results, retries and durable waits |
| Coordination | Task delegation, correlated replies, worker messages and dependency joins |
| Recovery | Reload saved state and resume eligible work using committed results |

Mailbox consumption is ordered per actor. Native agents execute one task at a time, while the worker can run activities for independent actors concurrently. The runtime manages these logical actors without assigning each a dedicated OS thread. Actor state, processed-message receipts, outgoing messages and Run intents commit together at mailbox transition boundaries.

A committed model or tool result is reused after restart. Timers retain saved deadlines, and questions or approvals retain their wait identities. An external operation interrupted before its result was recorded can have an unknown outcome; its recovery policy determines whether it can repeat or needs explicit reconciliation. Durable messaging does not guarantee exactly-once effects in external systems.

See the [actor guide](docs/actor-model.md#actor-model) for the execution model and [architecture](docs/architecture.md) for persistence boundaries. Domain policies and application services use ports; SQLite, models, MCP tools, workspace operations and transports implement adapters.

## What it does

- **Persistent agents:** durable mailboxes, bounded task queues, retained memory, reminders and failure supervision.
- **Recoverable workflows:** saved model/tool results, retries, durable timers, human questions, approvals and explicit recovery for uncertain effects.
- **Agent coordination:** sequential or parallel delegation, worker messages, questions, dependency waits and per-assignment pause/stop controls.
- **Project work:** automatic discovery, scoped edits in isolated copies, pinned verification commands, immutable patches and reviewed application to source.
- **Configurable tools and models:** MCP inspection tools, native journaled mutations and commands, and compatible model endpoints with structured JSON or native function calling.
- **Conversation state:** ordered follow-ups, bounded context, optional summaries and recorded model-call/token usage with optional limits.
- **Inspection and control:** CLI status/history, goal inboxes, pause/resume/stop, JSON output and local HTTP/SSE routes.

## Current scope

The CLI's `chat` command starts or resumes a durable conversation, discovers an optional project's setup and drives coding through verification and patch review. Actor, team, Run, worker and inspection commands remain available. The same harness services support the Rust API and local HTTP JSON/SSE routes; the HTTP server serves no UI.

The runtime assumes trusted local callers. Managed project copies isolate workspace changes, but project commands and MCP servers are not OS sandboxes. Only the current development database schema is supported; older development databases have no upgrade path.

Distributed execution, external agent-framework adapters and full MCTS are future work. See the [roadmap](docs/roadmap.md) and [recovery guarantees](docs/architecture.md#timers-and-external-effects).

## Run the CLI

Install Rust **1.94+** and the platform C build tools needed by SQLite. Node.js **22+** is needed for Node-based MCP servers and the local Auto trial.

From a checkout:

```sh
cargo build --locked
cargo run --locked -- --help
```

The first build may download Cargo dependencies. Set `OPENAI_API_KEY` for a hosted compatible model, then start a conversation:

```sh
cargo run --locked -- chat --project ./my-project
# An optional first request can be supplied directly:
cargo run --locked -- chat --project ./my-project "Fix the failing tests"
# Restore the model, project, conversation and pending decisions from SQLite:
cargo run --locked -- chat --resume SESSION_ID
```

Omit `--project` for a general conversation. The harness automatically handles project discovery and routine scoped work in its managed copy. Answer questions normally; `/steer MESSAGE` saves guidance for the current task's next fresh model request. `/patch` shows saved changes and `/apply` prepares their reviewed application to the original folder. `/exit` or Ctrl+C leaves work saved. Use `chat --list` to find sessions. See the [chat CLI guide](docs/harness.md#chat) for models, controls and piped input.

Live activity shows model requests, tools, MCP calls, worker assignments and test results as they are saved. Resuming restores a short recap before following new work.

In an interactive terminal, paste and edit a multiline draft before sending it: Enter adds a line, and Ctrl+S sends the whole message. Alt+Enter also sends when supported by your terminal. Activity updates keep your draft visible. Pipes and `--json` retain one input per line.

When automatic model retries stop, `/retry` can resume an eligible failure using the saved request and completed work. Chat explains when a failure needs another action.

Run a scripted actor team without a model server:

```sh
team=$(cargo run --locked -- team create --config-file examples/configs/agent-team.json)
task=$(cargo run --locked -- team submit "$team" --goal "Design a durable actor workflow")
cargo run --locked -- worker run --until-idle --max-concurrent-activities 2
cargo run --locked -- team task status "$team" "$task"
```

The sample uses fixed responses to demonstrate delegation and message handling. For PowerShell commands, see the [CLI guide](docs/harness.md#cli).

Chat defaults to `.act0r/chat.db` in the current directory, keeping runtime state out of project inspection. Other commands default to `act0r.db`; use `--db PATH` to select the chat database for lower-level inspection or choose another database. The printed resume command includes its absolute database path. Ctrl+C drains the worker; restart against the same database to resume saved work. Use global `--json` for machine-readable output.

For live compatible models, including NVIDIA, set `OPENAI_API_KEY` in the worker environment and configure the model target in JSON. Loopback servers such as LM Studio can run without a key. See the [CLI guide](docs/harness.md#cli), [team configuration](docs/teams.md#agent-teams--configuration), [model providers](docs/providers.md) and [MCP setup](docs/mcp.md).

## Try recovery without an API key

From the repository root, with Node on PATH:

```sh
cargo run --locked --example auto_conversation
```

This uses a local fixture model and a generated JavaScript project. It discovers the project, repairs a function, runs five real Node contracts, applies the reviewed patch, verifies a follow-up and compares five restart boundaries. It validates the workflow mechanics; it does not measure LLM coding quality.

Browse the [examples](examples/README.md) for the Rust API, actors, MCP and multi-worker demonstrations, or the [trial guide](docs/validation.md) for hosted model runs and their limits.

## Repository map

| Path | Purpose |
| --- | --- |
| `crates/act0r-runtime/` | Runtime library: domain, application ports, SQLite, models, MCP and workspace effects |
| `crates/act0r-runtime/schema/` | Single current database baseline |
| `src/adapters/` | Product HTTP and CLI entry points |
| `src/lib.rs` | Public Rust API re-exporting the runtime |
| `examples/` | Runnable demos, shared configuration and project fixtures |
| `tests/` | Domain, integration and process-recovery checks |
| `docs/` | Topic guides, architecture and reproducible trials |

The [architecture guide](docs/architecture.md) explains the consistency boundaries and recovery guarantees. Completed activities reuse saved results; an interrupted external effect may need explicit reconciliation.

## Develop

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets -- --test-threads 2
node scripts/check-docs.mjs
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the complete checks and optional MCP tests. The [documentation index](docs/README.md) and [roadmap](docs/roadmap.md) cover the current capabilities and next priorities.

## License

[MIT](LICENSE).
