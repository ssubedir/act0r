use crate::{
    Engine, Error,
    adapters::{
        agent_executor::AgentExecutor,
        agents::{AnswerAgentAdapter, CodingAgentAdapter, PlannerAgentAdapter, ToolAgentAdapter},
        candidates::{CandidateExecutor, LocalCandidates},
        demo::{DemoExecutor, DemoWorkflow, TimerDemoWorkflow},
        file_lock::FileOwnership,
        model::ModelRouter,
        search::{FeedbackCostStrategy, SearchExecutor},
        sqlite::SqliteStore,
        system_clock::SystemClock,
        workspace::{LocalTools, LocalWorkspaces},
    },
    application::{
        ActorService, AgentRegistry, AgentService, BenchmarkService, BranchService, Coordinators,
        SearchService, TaskService, TeamService,
        ports::{NativeTool, WorkspaceReader},
    },
};
use std::{path::Path, sync::Arc};

pub async fn local_engine(database: impl AsRef<Path>) -> Result<Engine, Error> {
    Ok(local_app(database).await?.0)
}
pub async fn local_app(database: impl AsRef<Path>) -> Result<(Engine, AgentService), Error> {
    local_app_with_tools(database, vec![]).await
}
/// Add application-owned native tools to the built-in local catalog. Reinstall the same
/// versioned contracts on restart; installation does not grant actor allowlist access.
pub async fn local_app_with_tools(
    database: impl AsRef<Path>,
    additional_tools: Vec<Arc<dyn NativeTool>>,
) -> Result<(Engine, AgentService), Error> {
    let database = database.as_ref();
    local_app_with_read_tools(
        database,
        additional_tools,
        crate::adapters::mcp::McpFilesystemConfig::from_environment()?,
        crate::adapters::mcp::McpConfig::for_database(database)?,
    )
    .await
}
/// Host-selected MCP reader for the built-in native file tools. Saved activity contracts
/// and observations are unchanged; reinstallation must retain their documented semantics.
pub async fn local_app_with_mcp(
    database: impl AsRef<Path>,
    config: crate::adapters::mcp::McpFilesystemConfig,
) -> Result<(Engine, AgentService), Error> {
    local_app_with_read_tools(database, vec![], Some(config), Default::default()).await
}
/// Discover host-allowed read-only MCP tools and pin them into new workspace actors.
pub async fn local_app_with_mcp_tools(
    database: impl AsRef<Path>,
    config: crate::adapters::mcp::McpConfig,
) -> Result<(Engine, AgentService), Error> {
    local_app_with_read_tools(database, vec![], None, config).await
}
async fn local_app_with_read_tools(
    database: impl AsRef<Path>,
    additional_tools: Vec<Arc<dyn NativeTool>>,
    mcp: Option<crate::adapters::mcp::McpFilesystemConfig>,
    catalog: crate::adapters::mcp::McpConfig,
) -> Result<(Engine, AgentService), Error> {
    let store = Arc::new(SqliteStore::open(database).await?);
    let workspaces = store
        .database_path()
        .parent()
        .ok_or_else(|| Error::Unavailable("database parent unavailable".into()))?
        .join(".act0r")
        .join("workspaces");
    let ownership = Arc::new(FileOwnership::new(store.database_path()));
    let candidates = Arc::new(LocalCandidates::new(
        workspaces.parent().unwrap().join("snapshots"),
        workspaces.clone(),
    ));
    let workspace_factory = Arc::new(LocalWorkspaces::new(workspaces.clone()));
    let workspace_tools = Arc::new(LocalTools::new(workspaces.clone()));
    let replace_inspection = catalog.replace_inspection;
    let (mcp_tools, unavailable_tools) = crate::adapters::mcp::discover_available_tools(
        catalog,
        workspace_tools.clone(),
        &workspaces,
    )
    .await?;
    let (workspace_tools_ids, general_tools_ids): (Vec<_>, Vec<_>) = mcp_tools
        .iter()
        .map(|tool| tool.definition())
        .partition(|tool| tool.requires_workspace);
    let workspace_defaults = workspace_tools_ids
        .into_iter()
        .map(|tool| tool.id)
        .collect();
    let general_defaults = general_tools_ids.into_iter().map(|tool| tool.id).collect();
    let workspace_reader: Arc<dyn WorkspaceReader> = match mcp {
        Some(config) => Arc::new(crate::adapters::mcp::McpWorkspaceReader::new(
            workspace_tools.clone(),
            config,
        )?),
        None => workspace_tools.clone(),
    };
    let native_tools = Arc::new(
        crate::adapters::native_tools::agent_tool_registry(
            workspace_reader,
            workspace_tools.clone(),
            workspace_tools.clone(),
            store.clone(),
        )?
        .with_tools(vec![Arc::new(
            crate::adapters::native_tools::RunCommandTool::new(workspace_tools.clone()),
        )])?
        .with_tools(vec![Arc::new(
            crate::adapters::worker_messages::SendWorkerMessageTool::new(store.clone()),
        )])?
        .with_tools(vec![
            Arc::new(crate::adapters::worker_messages::WorkerQuestionTool::new(
                store.clone(),
                false,
            )),
            Arc::new(crate::adapters::worker_messages::WorkerQuestionTool::new(
                store.clone(),
                true,
            )),
        ])?
        .with_tools(vec![Arc::new(
            crate::adapters::worker_messages::GuideWorkersTool::new(store.clone()),
        )])?
        .with_tools(vec![Arc::new(
            crate::adapters::assignment_inspection::InspectAssignmentTool::new(store.clone()),
        )])?
        .with_tools(vec![Arc::new(
            crate::adapters::planner_stop::StopWorkerTool::new(store.clone()),
        )])?
        .with_tools(vec![
            Arc::new(crate::adapters::planner_pause::WaitForWorkerTool::new(
                store.clone(),
            )),
            Arc::new(crate::adapters::planner_pause::WorkerPauseTool::new(
                store.clone(),
                true,
            )),
            Arc::new(crate::adapters::planner_pause::WorkerPauseTool::new(
                store.clone(),
                false,
            )),
        ])?
        .with_tools(additional_tools)?
        .with_tools(mcp_tools)?
        .with_unavailable_tools(unavailable_tools)?
        .with_workspace_defaults(workspace_defaults)?
        .with_general_defaults(general_defaults)?
        .with_external_inspection(replace_inspection)?,
    );
    let registry = Arc::new(AgentRegistry::new(vec![
        Arc::new(CodingAgentAdapter::new(workspace_factory.clone())),
        Arc::new(AnswerAgentAdapter),
        Arc::new(
            PlannerAgentAdapter::new(native_tools.clone())
                .with_workspaces(workspace_factory.clone()),
        ),
        Arc::new(
            ToolAgentAdapter::new(native_tools.clone()).with_workspaces(workspace_factory.clone()),
        ),
    ])?);
    let mut definitions: Vec<Arc<dyn crate::domain::WorkflowDefinition>> = vec![
        Arc::new(DemoWorkflow),
        Arc::new(TimerDemoWorkflow),
        Arc::new(crate::domain::session_summary::SessionSummaryWorkflow),
        Arc::new(crate::domain::patch_application::PatchApplicationWorkflow),
        Arc::new(crate::domain::project_verification::ProjectVerificationWorkflow),
        Arc::new(crate::domain::agent::AgentWorkflow),
        Arc::new(crate::domain::task::CandidateWorkflow),
        Arc::new(crate::domain::task::BranchCandidateWorkflow),
        Arc::new(crate::domain::search::SearchCandidateWorkflow),
        Arc::new(crate::domain::search::FeedbackSearchCandidateWorkflow),
        Arc::new(crate::domain::search::ProposalWorkflow),
    ];
    definitions.extend(registry.workflows());
    let applications = Arc::new(
        crate::adapters::patch_applications::LocalPatchApplications::new(
            workspaces.parent().unwrap().join("applications"),
        ),
    );
    let child_changes = Arc::new(
        crate::adapters::child_changes::LocalChildChanges::new(
            store.clone(),
            applications.clone(),
            workspace_tools.clone(),
        )
        .with_worker_changes(store.clone()),
    );
    let project_verification = Arc::new(
        crate::adapters::project_verification::LocalProjectVerification::new(
            workspaces.parent().unwrap().join("project-verifications"),
            applications.clone(),
        ),
    );
    let engine = Engine::new(
        store.clone(),
        Arc::new(
            AgentExecutor::new(
                Arc::new(ModelRouter::from_environment()?),
                workspace_tools.clone(),
                Arc::new(SearchExecutor::new(
                    Arc::new(ModelRouter::from_environment()?),
                    Arc::new(CandidateExecutor::new(
                        candidates.clone(),
                        Arc::new(DemoExecutor),
                    )),
                )),
            )
            .with_workspace_changes(Arc::new(
                crate::adapters::workspace_changes::LocalWorkspaceChanges::new(
                    workspaces.parent().unwrap().join("reviews"),
                    workspaces.clone(),
                ),
            ))
            .with_child_changes(child_changes.clone())
            .with_workspace_handoff(Arc::new(
                crate::adapters::workspace_handoff::LocalWorkspaceHandoff::new(
                    store.clone(),
                    candidates.clone(),
                    workspace_tools.clone(),
                ),
            ))
            .with_patch_applications(applications.clone())
            .with_project_verification(project_verification.clone())
            .with_native_tools(native_tools.clone()),
        ),
        Arc::new(SystemClock),
        ownership,
        definitions,
    )?;
    let tasks = TaskService::new(engine.clone(), store.clone(), candidates.clone());
    let branches = Arc::new(BranchService::new(
        engine.clone(),
        store.clone(),
        store.clone(),
        candidates.clone(),
    ));
    let search = Arc::new(SearchService::new(
        engine.clone(),
        store.clone(),
        store.clone(),
        branches.clone(),
        candidates.clone(),
        Arc::new(FeedbackCostStrategy),
    ));
    let benchmarks = Arc::new(BenchmarkService::new(
        engine.clone(),
        tasks.clone(),
        search.clone(),
        store.clone(),
        candidates.clone(),
    ));
    let actors = Arc::new(
        ActorService::new(
            engine.clone(),
            store.clone(),
            vec![
                Arc::new(crate::domain::actors::CoordinatorAgent),
                Arc::new(crate::domain::actors::WorkflowWorkerAgent),
                Arc::new(crate::domain::actors::RegisteredAgentDefinition),
            ],
        )?
        .with_agent_registry(registry.clone())
        .with_project_instructions(workspace_tools.clone())
        .with_inspection_reader(store.clone())
        .with_changes_reader(store.clone())
        .with_application_inspector(applications)
        .with_goal_control(store.clone()),
    );
    let teams = Arc::new(
        TeamService::new(
            engine.clone(),
            store.clone(),
            Arc::new(crate::adapters::team_workspaces::LocalTeamWorkspaces::new(
                workspaces.parent().unwrap().join("teams"),
                candidates,
            )),
            registry.clone(),
        )
        .with_task_inbox(store.clone())
        .with_goal_listing(store.clone())
        .with_harness_store(store.clone())
        .with_project_profiles(store.clone())
        .with_project_detection(Arc::new(
            crate::adapters::project_detection::LocalProjectDetection,
        ))
        .with_harness_coding(actors.clone())
        .with_harness_recovery(native_tools.clone(), workspace_tools)
        .with_harness_import_recovery(child_changes)
        .with_project_verification(project_verification)
        .with_goal_controls(actors.clone())
        .with_task_events(store.clone()),
    );
    let engine = engine.with_coordinator(Arc::new(Coordinators::new(vec![
        branches.clone(),
        search.clone(),
        benchmarks.clone(),
        actors.clone(),
        teams.clone(),
    ])));
    let agents = AgentService::new(engine.clone(), workspace_factory)
        .with_storage(crate::application::StorageService::new(
            store.clone(),
            Arc::new(crate::adapters::storage::LocalStorage::new(
                workspaces,
                store.database_path().to_path_buf(),
            )),
        ))
        .with_tasks(tasks)
        .with_benchmarks(benchmarks)
        .with_branches(branches)
        .with_search(search)
        .with_actors(actors)
        .with_teams(teams)
        .with_agent_registry(registry)
        .with_tool_registry(native_tools);
    Ok((engine, agents))
}
