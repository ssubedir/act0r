# Roadmap

The intended final harness combines **durable actors + Monte Carlo tree search (MCTS)** with interchangeable models, tools and environments. Durable actors provide persistent execution and coordination; MCTS is planned to explore and evaluate alternative action sequences. Current development focuses on the local, self-contained actor runtime, CLI and a simple folder-and-request workflow.

## Current foundation

- Stable native actors, durable mailboxes and task lifecycle controls.
- SQLite-backed activities, timers, waits, usage accounting and restart recovery.
- Automatic project discovery, isolated coding, verification and reviewed application.
- Prompt-first CLI conversations with saved questions, approvals and session resume.
- Local setup diagnostics for models, credentials, MCP and project/runtime paths.
- Native worker coordination and persistent conversations.
- Configuration-driven MCP inspection tools.

## Next priorities

1. Make fresh-checkout setup and ordinary project tasks reliable and easy to diagnose.
2. Exercise the existing harness on more real projects and compatible models.
3. Prefer existing MCP servers over maintaining new inspection tools in Rust.
4. Improve implementation boundaries where real usage exposes complexity.

## Later work

External agent-framework adapters, cross-language worker protocols, distributed ownership and additional storage backends need separate compatibility evidence. SQLite remains the supported baseline. A future UI or desktop package can use the existing HTTP API; current development focuses on the CLI.

Search experiments remain available for comparison, but full MCTS is deferred. A strategy must improve outcomes under equal total budgets and preserve durable state/environment recovery before it becomes a product default. See [research](research.md) and [validation](validation.md).
