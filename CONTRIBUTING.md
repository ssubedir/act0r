# Contributing

Start with the [README](README.md), [architecture](docs/architecture.md) and [documentation index](docs/README.md). This is a Rust workspace with a CLI and local HTTP API. Keep changes focused and explain their observable behavior and validation.

## Local setup

Install Rust 1.94+ with rustfmt and Clippy, C build tools for bundled SQLite, and Node.js 22+ for Node-based MCP servers, fixtures and documentation checks. Current CI uses Rust 1.99.0 and Node 24 on Linux (`ubuntu-latest`). `cargo fetch --locked` prepares dependencies; use `--offline` on subsequent Cargo commands when they are cached.

Run `cargo run --locked -- --help` from the repository root. The CLI defaults to `act0r.db`; global `--db PATH` selects a database. Credentials come from the launching environment. The application does not automatically load `.env` files. Local databases, MCP installations, generated trials and build outputs are ignored by Git.

## Validate a change

Run these from the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets -- --test-threads 2
node scripts/check-docs.mjs
```

Tests use temporary databases, local provider fixtures and generated projects. They require no hosted-model key. Some tests launch subprocesses, bind loopback sockets or compile fixture projects; allow those operations in the test environment. Keep builds in one Cargo target directory. Set `CARGO_BUILD_JOBS=2` if parallel compilation uses too much memory. CI also sets `RUST_TEST_THREADS=2`: subprocess fixtures include deliberate short deadlines, so avoid oversubscribing the host.

CI disables debug information and incremental compilation for development/test builds, and strips dependency debug information from linked binaries. These settings also reach generated Cargo fixtures and reduce disk usage when building all integration tests and examples. Local builds keep the workspace's normal profile settings.

With Node on PATH, run the additional MCP transport and recovery checks:

```sh
cargo test --locked --test mcp_tools --test mcp_catalog --test mcp_inspection -- --include-ignored
```

Do not run every ignored test in the repository directly. Other ignored tests are crash/child-process fixtures invoked by their parent recovery tests. Hosted-model demonstrations are separate opt-in experiments; see [validation](docs/validation.md). Keep raw requests and reports in ignored `.build/`, and document actual outcomes and limitations.

## Code boundaries

- Domain policies decide transitions without I/O or infrastructure types.
- Application services coordinate persistence and external work through ports.
- Adapters implement storage, processes, transport and tool execution.
- The composition root wires adapters; workflows reuse the existing scheduler.

Prefer configured MCP tools for new inspection capabilities. Native mutations and commands must preserve their approval, journal and reconciliation contracts. Keep current saved actor and tool contracts stable across restart. Earlier development versions are unsupported. Schema SQL belongs in `crates/act0r-runtime/schema/baseline.sql`. Only the current development schema is supported. Add explicit migrations when future schema changes need to preserve existing databases. See the [storage architecture](docs/architecture.md#local-runtime-and-storage).

## Documentation and examples

Keep each prose paragraph on one source line and let the editor soft-wrap it. Preserve the structure of code blocks, lists and tables.

Keep the main README short. Update the relevant topic guide and its table of contents. Existing explicit section IDs are stable links; preserve them when editing or moving sections. The local link checker also validates fragments. Shared example JSON belongs in `examples/configs/`; project and benchmark fixtures stay with their demos and tests.

Include a reproducer and relevant test results in a pull request. Report failures with provider/model, OS, safe diagnostics and the steps needed to reproduce; omit credentials and private project content.
