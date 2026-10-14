pub mod agent_executor;
pub mod agents;
pub mod assignment_inspection;
pub mod benchmark_report;
pub mod candidates;
pub mod child_changes;
pub mod demo;
pub mod file_creation;
pub mod file_lock;
pub mod mcp;
pub mod model;
pub mod native_tools;
pub mod operator;
pub mod patch_applications;
pub mod planner_pause;
pub mod planner_stop;
mod process_tree;
pub mod project_detection;
pub mod project_instructions;
pub mod project_verification;
pub mod result_recall;
pub mod search;
pub mod sqlite;
pub mod storage;
pub mod system_clock;
pub mod team_workspaces;
pub mod tokio_worker;
pub mod worker_messages;
pub mod workspace;
pub mod workspace_changes;
pub mod workspace_handoff;
pub mod workspace_reads;
pub mod workspace_search;

// Crash tests exercise the product HTTP adapter against private runtime fault hooks.
// Reuse its source only in tests; the runtime has no production HTTP dependency.
#[cfg(test)]
#[path = "../../../../src/adapters/http_api/mod.rs"]
#[expect(
    unused_imports,
    dead_code,
    reason = "runtime fault tests reuse the routes but do not host a worker alongside the API"
)]
pub(crate) mod http_api;
