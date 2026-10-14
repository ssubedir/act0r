# Examples

Run these from the repository root. Cargo downloads dependencies on the first build; add `--offline` once cached. Default demonstrations use scripted or loopback model fixtures. Hosted model endpoints require an explicit configuration.

| Start here | Command | What it shows |
| --- | --- | --- |
| Actor model | `cargo run --locked --example actors` | Stable actors, persistent mailboxes, correlated replies and restart recovery |
| Rust API | `cargo run --locked --example library` | Starting and driving a durable workflow |
| Full Auto conversation | `cargo run --locked --example auto_conversation` | Generated JS repair, real Node tests, reviewed application, follow-up and restart checks |
| Dependent coding | `cargo run --locked --example dependent_coding` | Two workers, dependency handoff and combined verification |
| Local HTTP API | `cargo run --locked --example http_api` | HTTP decisions, event reconnection and recovery |

Auto needs Node.js on PATH. Dependent coding uses Cargo for its generated Rust fixture. Read the [trial guide](../docs/validation.md) before running hosted variants, especially preparation/resume and approval boundaries.

The actor example is the smallest starting point for the [actor model](../docs/actor-model.md#actor-model). It creates a coordinator and worker, commits a task message and Run, closes the application and reopens the same database before delivering the result. No model server or API key is needed. The Auto example is a terminal driver for the high-level harness/API workflow; the CLI's actor/team commands are separate entry points into the same runtime.

## MCP

[Configure an installed MCP server](../docs/mcp.md) before running:

```sh
cargo run --locked --example mcp_reads -- PATH_TO_FILESYSTEM_BRIDGE_CONFIG
cargo run --locked --example mcp_catalog -- PATH_TO_METADATA_CONFIG
```

The metadata example expects `get_file_info` as its first allowed tool. The normal Auto example uses the current host MCP catalog. See the MCP guide for the different configuration shapes and exact requirements.

## Other examples

The remaining named examples provide focused recovery demonstrations for [actors](../docs/actor-model.md), [worker coordination](../docs/coordination.md), [planner actions](../docs/planner.md), [workspace effects](../docs/tools.md), [teams](../docs/teams.md) and [search experiments](../docs/research.md). They remain runnable with `cargo run --locked --example NAME`.

Shared JSON configurations live in [configs/](configs/). Fixture paths inside those configurations resolve relative to that directory. For example:

```sh
cargo run --locked --example auto_conversation -- --target-config examples/configs/benchmark-nvidia-agent.json --prepare-only
```

Review the generated request before resuming a hosted trial. Set provider keys in the environment; leave credentials out of configuration. `coding-task/`, `dependent-coding-project/` and `benchmark-cases/` are intentionally broken or frozen project fixtures used by demos/tests, with independent Cargo manifests. Generated databases, workspaces and reports belong in ignored `.build/`.
