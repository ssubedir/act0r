use super::Error;
use crate::domain::*;
use std::{future::Future, pin::Pin};

pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;
/// Persist an assignment admission hold and its planner decision as one transaction.
pub trait PlannerPausePort: Send + Sync {
    fn control_worker_pause(
        &self,
        call: tools::NativeToolCall,
        paused: bool,
    ) -> PortFuture<'_, tools::NativeToolResult>;
}
pub trait PlannerStopPort: Send + Sync {
    fn stop_worker(&self, call: tools::NativeToolCall) -> PortFuture<'_, tools::NativeToolResult>;
}
/// Read committed assignment evidence and persist the exact observation for safe replay.
pub trait AssignmentInspectionPort: Send + Sync {
    fn inspect_assignment(
        &self,
        call: tools::NativeToolCall,
    ) -> PortFuture<'_, tools::NativeToolResult>;
}
/// Commit scoped notes/questions/replies and their exact activity receipts atomically.
pub trait WorkerMessagePort: Send + Sync {
    fn guide_workers(&self, call: tools::NativeToolCall)
    -> PortFuture<'_, tools::NativeToolResult>;
    fn worker_question(
        &self,
        call: tools::NativeToolCall,
        reply: bool,
    ) -> PortFuture<'_, tools::NativeToolResult>;
    fn send_worker_message(
        &self,
        call: tools::NativeToolCall,
    ) -> PortFuture<'_, tools::NativeToolResult>;
}
/// Commit configuration revisions and exact decision receipts atomically.
pub trait ProjectProfileStorePort: Send + Sync {
    fn project_profiles(&self) -> PortFuture<'_, project_profiles::ProjectProfiles>;
    fn project_profile(
        &self,
        id: ProjectProfileId,
    ) -> PortFuture<'_, project_profiles::ProjectProfile>;
    fn change_project_profile(
        &self,
        id: ProjectProfileId,
        request: project_profiles::ProfileRequest,
    ) -> PortFuture<'_, project_profiles::ProfileReceipt>;
}
/// Inspect a selected folder without executing commands or changing persisted state.
pub trait ProjectDetectionPort: Send + Sync {
    fn inspect(&self, workspace: String) -> PortFuture<'_, project_detection::RustProjectFacts>;
}
/// Hold authoritative admission exclusion until the owned filesystem operation ends.
pub trait CacheCleanupGuard: Send {
    fn workspace(&self) -> &storage::StorageWorkspace;
    fn finish(self: Box<Self>) -> PortFuture<'static, ()>;
}
pub trait StorageStorePort: Send + Sync {
    fn storage_workspaces(&self) -> PortFuture<'_, (Vec<storage::StorageWorkspace>, bool)>;
    fn lock_cache_cleanup(
        &self,
        session: SessionId,
        agent: AgentId,
    ) -> PortFuture<'_, Box<dyn CacheCleanupGuard>>;
}
/// Synchronous filesystem operations run on an owned blocking task, with bounded scans.
pub trait StorageFilesPort: Send + Sync {
    fn database_bytes(&self) -> Result<u64, Error>;
    fn inspect(&self, workspace: &str) -> Result<storage::WorkspaceUsage, Error>;
    fn clean(&self, workspace: &str, fingerprint: &str) -> Result<(u64, u64, bool), Error>;
}
/// Persist conversation order and mailbox admission in one transaction.
pub trait HarnessStorePort: Send + Sync {
    fn pending_auto_setup(&self) -> PortFuture<'_, Option<conversation::ProjectContinuation>>;
    fn auto_setup_failed(
        &self,
        source: conversation::ProjectContinuation,
        error: String,
        now: i64,
    ) -> PortFuture<'_, ()>;
    fn refresh_context(
        &self,
        scope: project_refresh::RefreshScope,
    ) -> PortFuture<'_, project_refresh::RefreshContext>;
    fn project_refresh(
        &self,
        scope: project_refresh::RefreshScope,
    ) -> PortFuture<'_, project_refresh::RefreshView>;
    fn refresh_intent(
        &self,
        scope: project_refresh::RefreshScope,
        request: MessageId,
    ) -> PortFuture<'_, Option<project_refresh::RefreshIntent>>;
    fn begin_project_refresh(
        &self,
        scope: project_refresh::RefreshScope,
        request: project_refresh::RefreshRequest,
        snapshot: project_refresh::RefreshSnapshot,
    ) -> PortFuture<'_, project_refresh::RefreshIntent>;
    fn admit_project_refresh(
        &self,
        intent: project_refresh::RefreshIntent,
        team: TeamId,
        now: i64,
    ) -> PortFuture<'_, harness::HarnessReceipt>;
    fn cancel_project_refresh(
        &self,
        scope: project_refresh::RefreshScope,
        request: MessageId,
    ) -> PortFuture<'_, ()>;
    fn project_refresh_error(
        &self,
        scope: project_refresh::RefreshScope,
        request: MessageId,
        error: String,
    ) -> PortFuture<'_, ()>;
    fn verification_repair(
        &self,
        scope: verification_repair::RepairScope,
    ) -> PortFuture<'_, verification_repair::RepairView>;
    fn begin_verification_repair(
        &self,
        scope: verification_repair::RepairScope,
        request: verification_repair::RepairRequest,
    ) -> PortFuture<'_, verification_repair::RepairIntent>;
    fn admit_verification_repair(
        &self,
        intent: verification_repair::RepairIntent,
        team: TeamId,
        now: i64,
    ) -> PortFuture<'_, harness::HarnessReceipt>;
    fn cancel_verification_repair(
        &self,
        scope: verification_repair::RepairScope,
        request: verification_repair::RepairRequest,
    ) -> PortFuture<'_, ()>;
    fn verification_repair_error(
        &self,
        scope: verification_repair::RepairScope,
        request: MessageId,
        error: String,
    ) -> PortFuture<'_, ()>;
    fn continuous_conversation(
        &self,
        session: SessionId,
        before: Option<conversation::ConversationCursor>,
        task: Option<MessageId>,
    ) -> PortFuture<'_, conversation::ContinuousConversation>;
    fn conversation_sessions(
        &self,
        query: sessions::SessionListQuery,
    ) -> PortFuture<'_, harness::HarnessSessions>;
    fn start_project_coding_turn(
        &self,
        team: TeamId,
        turn: harness::HarnessTurnRequest,
        source: conversation::ProjectContinuation,
        now: i64,
    ) -> PortFuture<'_, harness::HarnessReceipt>;
    fn worker_pause(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> PortFuture<'_, worker_pause::WorkerPause>;
    /// Save the revision-checked subtree hold and exact receipt in one transaction.
    fn control_assignment_pause(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: worker_pause::AssignmentPauseRequest,
        now: i64,
    ) -> PortFuture<'_, worker_pause::AssignmentPauseReceipt>;
    fn worker_cancellation(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> PortFuture<'_, worker_cancellation::WorkerCancellation>;
    /// Save the scoped receipt and existing task cancellation in one transaction.
    fn stop_assignment(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: worker_cancellation::StopAssignmentRequest,
        now: i64,
        decision: ActorCancellation,
    ) -> PortFuture<'_, worker_cancellation::StopAssignmentReceipt>;
    /// Turn ownership, delegation joins, child artifacts and review receipts in one snapshot.
    fn harness_delegated_work(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, super::DelegatedWorkView>;
    fn session_permissions(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, permissions::SessionPermissions>;
    fn change_permissions(
        &self,
        session: SessionId,
        request: permissions::PermissionRequest,
        now: i64,
        decision: TestPermissionDecision,
        edit_decision: TaskEditPermissionDecision,
        task_test_decision: TaskTestPermissionDecision,
    ) -> PortFuture<'_, permissions::PermissionReceipt>;
    fn turn_steering(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, steering::TurnSteering>;
    fn worker_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> PortFuture<'_, steering::TurnSteering>;
    fn send_worker_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: steering::SteeringRequest,
        now: i64,
    ) -> PortFuture<'_, steering::SteeringReceipt>;
    fn send_steering(
        &self,
        session: SessionId,
        turn: MessageId,
        request: steering::SteeringRequest,
        now: i64,
    ) -> PortFuture<'_, steering::SteeringReceipt>;
    fn turn_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, turn_recovery::TurnRecovery>;
    fn worker_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
    ) -> PortFuture<'_, worker_recovery::WorkerRecovery>;
    fn decide_worker_recovery(
        &self,
        session: SessionId,
        turn: MessageId,
        task: MessageId,
        request: worker_recovery::WorkerRecoveryRequest,
        now: i64,
        check: RecoveryCheck,
    ) -> PortFuture<'_, worker_recovery::WorkerRecoveryReceipt>;
    /// Check ownership/order and commit Run, actor, task, notification and receipt together.
    fn retry_turn(
        &self,
        session: SessionId,
        turn: MessageId,
        request: turn_recovery::RetryTurnRequest,
        now: i64,
        check: RecoveryCheck,
    ) -> PortFuture<'_, turn_recovery::RetryTurnReceipt>;
    fn change_summary_policy(
        &self,
        session: SessionId,
        request: session_summary::SummaryPolicyRequest,
    ) -> PortFuture<'_, session_summary::SummaryPolicyReceipt>;
    fn session_summary(
        &self,
        session: SessionId,
    ) -> PortFuture<'_, session_summary::SessionSummary>;
    fn start_session_summary(
        &self,
        session: SessionId,
        request: session_summary::SummaryRequest,
        now: i64,
        factory: SummaryRunFactory,
    ) -> PortFuture<'_, session_summary::SummaryReceipt>;
    /// Membership and actor state are read from one consistent snapshot.
    fn session_memory_actors(
        &self,
        session: SessionId,
    ) -> PortFuture<'_, Vec<(teams::TeamMember, actors::Agent)>>;
    fn turn_memory(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, session_memory::TurnMemory>;
    /// Scope, exact retry receipt, memory decision and actor state commit together.
    fn clear_session_memory(
        &self,
        session: SessionId,
        agent: AgentId,
        request: session_memory::ClearMemoryRequest,
        now: i64,
        decision: ActorMemoryDecision,
    ) -> PortFuture<'_, session_memory::ClearMemoryReceipt>;
    /// Exact immutable ownership, independent of display graph bounds.
    fn harness_tool_run(
        &self,
        session: SessionId,
        turn: MessageId,
        run: RunId,
    ) -> PortFuture<'_, (MessageId, Run)>;
    /// Bounded runs with blocked execution or reconciliation receipts under this root turn.
    fn harness_tool_runs(
        &self,
        session: SessionId,
        turn: MessageId,
    ) -> PortFuture<'_, (Vec<(MessageId, Run)>, bool)>;
    fn harness_turn_context(
        &self,
        session: SessionId,
        task: MessageId,
    ) -> PortFuture<'_, harness::HarnessTurnContext>;
    /// Bind a client-identified application Run to a verified session artifact atomically.
    fn create_harness_application(
        &self,
        session: SessionId,
        task: MessageId,
        run: Run,
    ) -> PortFuture<'_, RunId>;
    fn harness_applications(
        &self,
        session: SessionId,
        task: MessageId,
    ) -> PortFuture<'_, harness::HarnessApplications>;
    fn create_project_verification(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
        run: Run,
    ) -> PortFuture<'_, RunId>;
    fn project_verifications(
        &self,
        session: SessionId,
        task: MessageId,
        application: RunId,
    ) -> PortFuture<'_, project_verification::VerificationPage>;
    fn harness_sessions(
        &self,
        query: sessions::SessionListQuery,
    ) -> PortFuture<'_, harness::HarnessSessions>;
    fn session_metadata(&self, session: SessionId)
    -> PortFuture<'_, sessions::SessionMetadataView>;
    fn update_session(
        &self,
        session: SessionId,
        request: sessions::SessionUpdate,
    ) -> PortFuture<'_, sessions::SessionUpdateReceipt>;
    fn harness_conversation(
        &self,
        session: SessionId,
        before: Option<u64>,
    ) -> PortFuture<'_, harness::HarnessConversation>;
    fn start_harness_turn(
        &self,
        team: TeamId,
        turn: harness::HarnessTurnRequest,
        now: i64,
    ) -> PortFuture<'_, harness::HarnessReceipt>;
    fn append_harness_turn(
        &self,
        session: SessionId,
        turn: harness::HarnessTurnRequest,
        now: i64,
    ) -> PortFuture<'_, harness::HarnessReceipt>;
}
/// Resolve authority and worker pins atomically, without copying files in a database transaction.
pub trait WorkspaceHandoffReadPort: Send + Sync {
    fn handoff_worker(
        &self,
        parent: RunId,
        request: workspace_handoff::PrepareRequest,
    ) -> PortFuture<'_, actors::RegisteredAgentConfig>;
    fn handoff_group_workers(
        &self,
        parent: RunId,
        request: workspace_handoff::PrepareGroupRequest,
    ) -> PortFuture<'_, Vec<actors::RegisteredAgentConfig>>;
    /// Return whether the child's verification checkpoint has committed in SQLite.
    fn verify_workspace_input(
        &self,
        child: RunId,
        input: workspace_handoff::PreparedWorkspace,
    ) -> PortFuture<'_, bool>;
}
pub trait WorkspaceHandoffPort: Send + Sync {
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome>;
}
/// Resolve only a completed, successful direct child under the parent's pinned permissions.
/// Ownership, completion, and the immutable artifact must be read in one snapshot.
pub trait ChildChangesReadPort: Send + Sync {
    fn child_changes(
        &self,
        parent: RunId,
        request: child_changes::InspectRequest,
    ) -> PortFuture<'_, change_review::TaskChanges>;
}
/// Inspect or journal an approved child import as activities within the parent Run.
pub trait ChildChangesPort: Send + Sync {
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome>;
}
/// Resolve and recheck the exact dependency hold, immutable source and receiver permissions.
pub trait WorkerChangesReadPort: Send + Sync {
    fn worker_changes(
        &self,
        parent: RunId,
        request: worker_changes::InspectWorkerChanges,
    ) -> PortFuture<'_, (child_changes::InspectRequest, change_review::TaskChanges)>;
    fn validate_worker_import(
        &self,
        parent: RunId,
        inspection: child_changes::Inspection,
    ) -> PortFuture<'_, ()>;
}
/// Inspect the exact approved child artifact and managed workspace journal without writes.
pub trait ChildImportInspectionPort: Send + Sync {
    fn inspect_import(
        &self,
        run: RunId,
        activity: ActivityId,
        request: child_changes::Inspection,
    ) -> PortFuture<'_, patch_application::ApplicationInspection>;
}
/// Prepare immutable target bytes or execute a journaled, explicitly approved application.
/// Partial writes must report an uncertain outcome rather than permit automatic retry.
pub trait PatchApplicationPort: Send + Sync {
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome>;
}
/// Read journal progress and target hashes without creating files or scheduling execution.
pub trait PatchApplicationInspectionPort: Send + Sync {
    fn inspect(
        &self,
        run: RunId,
        activity: ActivityId,
        request: patch_application::WriteRequest,
    ) -> PortFuture<'_, patch_application::ApplicationInspection>;
}
pub trait ProjectVerificationPort: Send + Sync {
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome>;
}
pub trait ProjectVerificationInspectionPort: Send + Sync {
    fn inspect_refresh(
        &self,
        source: String,
        workspace: String,
    ) -> PortFuture<'_, (project_refresh::RefreshSnapshot, Vec<task::SnapshotFile>)> {
        let _ = (source, workspace);
        Box::pin(async {
            Err(Error::Unavailable(
                "project refresh inspection is not installed".into(),
            ))
        })
    }
    fn lock_refresh(
        &self,
        snapshot: project_refresh::RefreshSnapshot,
    ) -> PortFuture<'_, Box<dyn OwnershipGuard>> {
        let _ = snapshot;
        Box::pin(async {
            Err(Error::Unavailable(
                "project refresh inspection is not installed".into(),
            ))
        })
    }
    fn lock_repair_copies(
        &self,
        copies: Vec<String>,
        digest: String,
    ) -> PortFuture<'_, Box<dyn OwnershipGuard>> {
        let _ = (copies, digest);
        Box::pin(async {
            Err(Error::Unavailable(
                "repair copy inspection is not installed".into(),
            ))
        })
    }
    fn inspect(
        &self,
        activity: ActivityId,
        request: project_verification::VerificationRequest,
    ) -> PortFuture<'_, project_verification::VerificationInspection>;
}
pub trait ToolRecoveryInspectionPort: Send + Sync {
    fn inspect_tool(
        &self,
        activity: ActivityId,
        request: tools::NativeToolRequest,
    ) -> PortFuture<'_, tool_recovery::ToolInspection>;
}
/// Task-scoped workspace capture, hashing, and immutable artifact publication.
/// Execution identities are supplied by the engine; implementations must reuse saved receipts.
pub trait WorkspaceChangesPort: Send + Sync {
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, serde_json::Value>;
}
/// Read only committed artifacts for the requested physical actor task, in one snapshot.
pub trait WorkspaceChangesReadPort: Send + Sync {
    fn task_changes(&self, task: MessageId) -> PortFuture<'_, change_review::TaskChanges>;
}
/// Read all ownership links, execution facts, and budget summaries in one consistent snapshot.
/// Bounds apply to the graph; truncated graphs must be explicitly marked. This never mutates work.
pub trait TaskInspectionReadPort: Send + Sync {
    fn task_inspection_snapshot(
        &self,
        id: MessageId,
        options: super::TaskInspectionOptions,
    ) -> PortFuture<'_, super::TaskInspectionSnapshot>;
}
/// The receipt commits before cleanup. All execution/publication paths must honour it atomically.
pub trait GoalControlPort: Send + Sync {
    /// Team ownership and control facts must come from one snapshot, without graph limits.
    fn team_goal_controls(
        &self,
        team: TeamId,
        task: MessageId,
    ) -> PortFuture<'_, super::TeamGoalControls> {
        let _ = (team, task);
        Box::pin(async {
            Err(Error::Unavailable(
                "team goal controls are not installed".into(),
            ))
        })
    }
    /// Check team ownership, cancellation and revision atomically with the saved hold.
    fn control_team_goal_pause(
        &self,
        team: TeamId,
        task: MessageId,
        paused: bool,
        precondition: goal::GoalPausePrecondition,
        now: i64,
    ) -> PortFuture<'_, super::TeamGoalPauseReceipt> {
        let _ = (team, task, paused, precondition, now);
        Box::pin(async {
            Err(Error::Unavailable(
                "team goal controls are not installed".into(),
            ))
        })
    }
    /// Ownership and the irreversible receipt commit in the same transaction.
    fn request_team_goal_cancellation(
        &self,
        team: TeamId,
        task: MessageId,
        now: i64,
    ) -> PortFuture<'_, goal::GoalCancellationReceipt> {
        let _ = (team, task, now);
        Box::pin(async {
            Err(Error::Unavailable(
                "team goal controls are not installed".into(),
            ))
        })
    }
    /// Change only the goal admission hold; never toggle individual actor/Run controls.
    fn control_goal_pause(
        &self,
        id: MessageId,
        paused: bool,
        now: i64,
    ) -> PortFuture<'_, goal::GoalPauseRecord>;
    fn goal_pause(&self, id: MessageId) -> PortFuture<'_, goal::GoalPauseRecord>;
    fn request_goal_cancellation(
        &self,
        id: MessageId,
        now: i64,
    ) -> PortFuture<'_, goal::GoalCancellationReceipt>;
    fn goal_cancellation(&self, root: MessageId) -> PortFuture<'_, goal::GoalCancellationView>;
    /// Apply one existing task/reminder cancellation transaction; blocked Runs stay protected.
    fn advance_goal_cancellation(
        &self,
        now: i64,
        decision: ActorCancellation,
    ) -> PortFuture<'_, bool>;
    fn goal_cancellation_work_pending(&self) -> PortFuture<'_, bool>;
}
/// Integration supplies pure workflow behavior and optional workspace preparation.
/// Model/tool I/O stays behind the existing execution ports.
pub trait AgentAdapter: Send + Sync {
    fn registration(&self) -> integration::AgentRegistration;
    fn definition(&self) -> std::sync::Arc<dyn WorkflowDefinition>;
    /// Validate installed dependencies against saved configuration before a mailbox turn.
    /// No preparation, external I/O, or repinning occurs here.
    fn check_config(&self, _config: &serde_json::Value) -> Result<(), Error> {
        Ok(())
    }
    /// Validate and pin configuration without I/O or allocating a workspace.
    fn pin_config(
        &self,
        _config: serde_json::Value,
        _workspace: Option<String>,
    ) -> Result<serde_json::Value, Error> {
        Err(Error::Unavailable(
            "adapter does not support team configuration".into(),
        ))
    }
    fn prepare(
        &self,
        config: serde_json::Value,
        source: Option<std::path::PathBuf>,
    ) -> PortFuture<'_, serde_json::Value>;
}
/// Setup intents precede filesystem effects. All member actors and readiness publish together.
pub trait TeamStore: Send + Sync {
    fn register_team(&self, record: teams::TeamRecord) -> PortFuture<'_, teams::TeamRecord>;
    fn team(&self, id: TeamId) -> PortFuture<'_, teams::TeamRecord>;
    fn team_named(&self, name: String) -> PortFuture<'_, Option<teams::TeamRecord>>;
    fn save_team_workspace(
        &self,
        id: TeamId,
        actor: AgentId,
        workspace: teams::TeamWorkspace,
        now: i64,
    ) -> PortFuture<'_, ()>;
    fn team_setup_error(&self, id: TeamId, error: Option<String>, now: i64) -> PortFuture<'_, ()>;
    fn publish_team(
        &self,
        id: TeamId,
        actors: Vec<actors::Agent>,
        now: i64,
    ) -> PortFuture<'_, teams::TeamRecord>;
    fn submit_team(
        &self,
        id: TeamId,
        message: actors::AgentMessage,
        now: i64,
    ) -> PortFuture<'_, MessageId>;
    fn team_view(&self, id: TeamId) -> PortFuture<'_, teams::TeamView>;
    fn list_teams(&self) -> PortFuture<'_, teams::TeamList>;
}
pub trait TeamSetupGuard: Send + Sync {}
/// A team task view uses one snapshot. A response resolves ownership and the exact
/// current wait (or saved input receipt) in the same transaction as the Run mutation.
pub trait TeamTaskInboxPort: Send + Sync {
    fn team_task_snapshot(
        &self,
        team: TeamId,
        task: MessageId,
        options: super::TaskInspectionOptions,
    ) -> PortFuture<'_, super::TeamTaskSnapshot>;
    fn respond_team_wait(
        &self,
        team: TeamId,
        task: MessageId,
        wait: WaitId,
        decision: TeamWaitDecision,
    ) -> PortFuture<'_, super::TeamWaitReceipt>;
}
/// True means an identical input receipt already exists; replay must not rewrite the Run.
pub type TeamWaitDecision = Box<dyn FnOnce(&mut Run) -> Result<bool, DomainError> + Send>;
pub type TestPermissionDecision =
    Box<dyn Fn(&mut Run, &permissions::TestPermission) -> Result<(), Error> + Send>;
pub type TaskEditPermissionDecision =
    Box<dyn Fn(&mut Run, &permissions::TaskEditPermission) -> Result<(), Error> + Send>;
pub type TaskTestPermissionDecision =
    Box<dyn Fn(&mut Run, &permissions::TaskTestPermission) -> Result<(), Error> + Send>;
/// Resolve stream/cursor authority and all event page facts in one read snapshot.
/// New Run creation, ownership publication and indexed history commit together.
pub trait TeamTaskEventsPort: Send + Sync {
    fn team_task_events(
        &self,
        team: TeamId,
        task: MessageId,
        query: timeline::EventQuery,
    ) -> PortFuture<'_, super::TeamEventPage>;
}
/// Read bounded root summaries in one snapshot; cursors preserve membership across restarts.
pub trait TeamGoalListPort: Send + Sync {
    fn team_goals(
        &self,
        team: TeamId,
        query: goal_list::GoalListQuery,
    ) -> PortFuture<'_, super::TeamGoalPage>;
}
/// Stable team snapshot and member workspace identities; replay verifies unpublished copies.
pub trait TeamWorkspaces: Send + Sync {
    fn lock_setup(&self, id: TeamId) -> PortFuture<'_, Box<dyn TeamSetupGuard>>;
    fn prepare_member(
        &self,
        team: TeamId,
        actor: AgentId,
        source: String,
    ) -> PortFuture<'_, teams::TeamWorkspace>;
}
pub type Decision =
    Box<dyn FnOnce(&mut Run) -> Result<Option<ActivityExecution>, DomainError> + Send>;
pub type TimerDecision = Box<dyn FnOnce(&mut Run, TimerId) -> Result<bool, DomainError> + Send>;

pub struct ActorEffects {
    pub state: serde_json::Value,
    pub messages: Vec<actors::AgentMessage>,
    pub runs: Vec<(Run, actors::AgentRun)>,
}
/// Select bounded guidance from host-owned storage at launch. The caller persists
/// the returned snapshot atomically with the Run; model messages cannot supply it.
pub trait ProjectInstructionsPort: Send + Sync {
    fn capture_project_instructions(
        &self,
        workspace: &str,
    ) -> project_instructions::ProjectInstructions;
}
pub enum ActorDecisionOutcome {
    Apply(ActorEffects),
    Reject(String),
    Block(String),
}
/// Trusted scheduling context, read atomically with the mailbox turn.
#[derive(Default)]
pub struct ActorTaskOrigins {
    pub summarized_memory:
        std::collections::HashMap<MessageId, std::collections::HashSet<MessageId>>,
    pub session_summaries: std::collections::HashMap<MessageId, session_summary::SummarySnapshot>,
    pub follow_ups: std::collections::HashMap<MessageId, follow_up::FollowUpOrigin>,
    pub workspace_inputs:
        std::collections::HashMap<MessageId, workspace_handoff::PreparedWorkspace>,
}
pub type SummaryRunFactory =
    Box<dyn FnOnce(session_summary::SummaryInput) -> Result<Run, Error> + Send>;
pub type RecoveryCheck = Box<dyn FnOnce(&Run) -> Result<(), Error> + Send>;
pub type ActorDecision = Box<
    dyn FnOnce(&actors::Agent, &actors::AgentMessage, &ActorTaskOrigins) -> ActorDecisionOutcome
        + Send,
>;
pub type ActorMemoryDecision =
    Box<dyn FnOnce(&actors::Agent) -> Result<serde_json::Value, Error> + Send>;
pub type ActorCancellation = Box<
    dyn Fn(
            &actors::Agent,
            &[actors::AgentMessage],
            Option<&serde_json::Value>,
        ) -> Result<serde_json::Value, Error>
        + Send,
>;
pub enum DelegationProgress {
    Idle,
    Changed,
    CancelChild(MessageId),
}
pub enum DelegationEvent {
    BeforePublication,
    DeadlineElapsed { child_completed_at: Option<i64> },
    ChildCompleted(serde_json::Value),
}
pub type DelegationDecision =
    Box<dyn Fn(&mut Run, MessageId, DelegationEvent) -> Result<(), DomainError> + Send + Sync>;
/// A mailbox turn commits its receipt, agent state, outgoing messages and new runs together.
/// Native child Run creation also commits inherited shared-budget membership.
pub trait ActorStore: Send + Sync {
    fn schedule_actor_reminder(
        &self,
        _message: actors::AgentMessage,
        _deadline: TimerDeadline,
        _now: i64,
    ) -> PortFuture<'_, actors::ActorReminder> {
        Box::pin(async {
            Err(Error::Unavailable(
                "actor reminders are not supported by this store".into(),
            ))
        })
    }
    fn actor_reminder(&self, _id: MessageId) -> PortFuture<'_, actors::ActorReminder> {
        Box::pin(async {
            Err(Error::Unavailable(
                "actor reminders are not supported by this store".into(),
            ))
        })
    }
    fn actor_reminders(&self, _id: AgentId) -> PortFuture<'_, Vec<actors::ActorReminder>> {
        Box::pin(async {
            Err(Error::Unavailable(
                "actor reminders are not supported by this store".into(),
            ))
        })
    }
    fn cancel_actor_reminder(
        &self,
        _id: MessageId,
        _now: i64,
    ) -> PortFuture<'_, actors::ActorReminder> {
        Box::pin(async {
            Err(Error::Unavailable(
                "actor reminders are not supported by this store".into(),
            ))
        })
    }
    /// Publication commits actor revision, reminder receipt, mailbox entry and task handle.
    fn publish_due_actor_reminder(&self, _now: i64) -> PortFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    /// Publish a child request or deliver its terminal reply in one local transaction.
    fn advance_delegation(
        &self,
        _now: i64,
        _decision: DelegationDecision,
    ) -> PortFuture<'_, DelegationProgress> {
        Box::pin(async { Ok(DelegationProgress::Idle) })
    }
    fn create_actor(&self, agent: actors::Agent) -> PortFuture<'_, ()>;
    fn actor(&self, id: AgentId) -> PortFuture<'_, actors::Agent>;
    fn actor_queue(&self, id: AgentId) -> PortFuture<'_, actors::AgentQueue>;
    fn mailbox(&self, id: AgentId) -> PortFuture<'_, Vec<actors::MailboxEntry>>;
    fn actor_runs(&self, id: AgentId) -> PortFuture<'_, Vec<actors::AgentRun>>;
    /// New tasks reserve capacity atomically with acceptance; duplicate envelopes reserve nothing.
    /// Replies and runtime notifications do not consume task capacity.
    fn enqueue_message(&self, message: actors::AgentMessage, now: i64)
    -> PortFuture<'_, MessageId>;
    /// Created Runs inherit delegation and follow-up goal scopes from persisted task ownership.
    /// Membership commits with the Run, task handle, actor state, and mailbox receipt.
    fn process_message(
        &self,
        now: i64,
        decision: ActorDecision,
        summary_factory: Option<SummaryRunFactory>,
    ) -> PortFuture<'_, bool>;
    fn notify_run_completion(&self, now: i64) -> PortFuture<'_, bool>;
    fn actor_work_pending(&self) -> PortFuture<'_, bool>;
    fn control_actor(&self, id: AgentId, paused: bool, now: i64) -> PortFuture<'_, ()>;
    /// Reset a supervision pause and enqueue a durable wakeup in the same transaction.
    fn recover_actor_supervision(&self, _id: AgentId, _now: i64) -> PortFuture<'_, ()> {
        Box::pin(async {
            Err(Error::Unavailable(
                "actor supervision recovery is not supported by this store".into(),
            ))
        })
    }
    /// Clear actor-owned memory against current state, atomically with its revision.
    fn clear_actor_memory(
        &self,
        _id: AgentId,
        _now: i64,
        _decision: ActorMemoryDecision,
    ) -> PortFuture<'_, ()> {
        Box::pin(async {
            Err(Error::Unavailable(
                "actor memory controls are not supported by this store".into(),
            ))
        })
    }
    fn actor_task(&self, id: MessageId) -> PortFuture<'_, actors::AgentTaskView>;
    fn actor_tasks(&self, id: AgentId) -> PortFuture<'_, Vec<actors::AgentTaskView>>;
    /// Cancellation, queue removal, run control, and any saved reply commit together.
    fn cancel_actor_task(
        &self,
        id: MessageId,
        now: i64,
        decision: ActorCancellation,
    ) -> PortFuture<'_, actors::AgentTaskView>;
}

/// Decisions run synchronously inside an atomic unit of work. No external I/O.
/// For linked candidate runs, task admission checks and refreshed accounting must
/// commit with the run, attempts, history, and continuation. A benchmark task's
/// trial completion and comparison statistics belong to that same unit of work.
/// Configured delegation and continuing-goal allowances check admission and refresh reservations,
/// reconciled usage, and exhaustion atomically with each member Run mutation.
pub trait ExecutionStore: Send + Sync {
    /// Resolve one task test wait and save its authorization and continuation atomically.
    fn authorize_task_test_wait(
        &self,
        _decision: TaskTestPermissionDecision,
    ) -> PortFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    /// Resolve one matching native test wait and save its authorization atomically.
    fn authorize_test_wait(&self, _decision: TestPermissionDecision) -> PortFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    /// Resolve one scoped native edit wait and save permission use and continuation atomically.
    fn authorize_task_edit_wait(
        &self,
        _decision: TaskEditPermissionDecision,
    ) -> PortFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    /// Consistent snapshots of all inherited allowances and their per-Run ledgers.
    fn shared_budgets(&self, _id: RunId) -> PortFuture<'_, Vec<shared_budget::SharedBudgetView>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn create(&self, run: Run) -> PortFuture<'_, ()>;
    fn mutate(&self, id: RunId, decision: Decision) -> PortFuture<'_, Option<ActivityExecution>>;
    /// Select eligible work atomically, excluding Runs with an activity already running.
    /// Different Runs may have concurrent work; each admission still checks shared allowances.
    fn mutate_due(&self, now: i64, decision: Decision)
    -> PortFuture<'_, Option<ActivityExecution>>;
    fn get(&self, id: RunId) -> PortFuture<'_, Run>;
    fn mutate_due_timer(&self, now: i64, decision: TimerDecision) -> PortFuture<'_, bool>;
    fn history(&self, id: RunId) -> PortFuture<'_, Vec<HistoryEvent>>;
    fn active_runs(&self) -> PortFuture<'_, Vec<RunId>>;
    /// Recovery includes held Runs; idle detection excludes holds without admitted operations.
    fn runnable_runs(&self) -> PortFuture<'_, Vec<RunId>> {
        self.active_runs()
    }
}
pub trait ActivityExecutor: Send + Sync {
    /// Optional request deadline; workers allow a completion margin outside it.
    fn request_timeout_ms(&self, _execution: &ActivityExecution) -> Option<u64> {
        None
    }
    fn supports(&self, handler: &DefinitionId) -> bool;
    fn execute(&self, execution: ActivityExecution) -> PortFuture<'_, Outcome>;
}
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}
pub trait ModelClient: Send + Sync {
    fn generate(
        &self,
        request: agent::ModelRequest,
        activity: ActivityId,
    ) -> PortFuture<'_, agent::ModelReply>;
}
pub trait ProposalClient: Send + Sync {
    fn propose(
        &self,
        request: search::ProposalRequest,
        activity: ActivityId,
    ) -> PortFuture<'_, search::ProposalReply>;
}
/// Pure, versioned selection. No external execution or persistence.
pub trait SearchStrategy: Send + Sync {
    fn id(&self) -> DefinitionId;
    fn select(&self, search: &search::SearchRecord) -> Result<Option<RunId>, DomainError>;
}
pub trait SearchStore: Send + Sync {
    /// Search, task, and its root candidate commit together.
    fn create_search(
        &self,
        search: search::Search,
        task: task::CodingTask,
        root: Run,
    ) -> PortFuture<'_, ()>;
    fn search(&self, id: SearchId) -> PortFuture<'_, search::Search>;
    fn active_searches(&self) -> PortFuture<'_, Vec<SearchId>>;
    fn checkpoint_requested(&self, id: SearchId, now: i64) -> PortFuture<'_, ()>;
    /// Proposal run, task admission membership, and search phase commit together.
    fn begin_proposals(&self, id: SearchId, now: i64) -> PortFuture<'_, ()>;
    /// Stable fork plan and proposed action identities are saved before filesystem work.
    fn begin_rollouts(&self, id: SearchId, now: i64) -> PortFuture<'_, ()>;
    /// Superseding the unadmitted model request, feedback intent, and task state commit together.
    fn begin_feedback(&self, id: SearchId, run: RunId, now: i64) -> PortFuture<'_, ()>;
    fn score_rollout(&self, id: SearchId, run: RunId, now: i64) -> PortFuture<'_, ()>;
    /// Selection, final verification intent, and retirement of other branches commit together.
    fn select_search(&self, id: SearchId, run: Option<RunId>, now: i64) -> PortFuture<'_, ()>;
    fn finish_search(&self, id: SearchId, error: Option<String>, now: i64) -> PortFuture<'_, ()>;
}
pub trait WorkspaceFactory: Send + Sync {
    fn prepare(&self, source: std::path::PathBuf) -> PortFuture<'_, String>;
}
pub trait TaskStore: Send + Sync {
    /// Task, candidate runs, and membership are committed together.
    fn create_task(&self, task: task::CodingTask, runs: Vec<Run>) -> PortFuture<'_, ()>;
    fn task(&self, id: TaskId) -> PortFuture<'_, task::CodingTask>;
}
pub trait BenchmarkStore: Send + Sync {
    fn create_benchmark(&self, job: benchmark::Benchmark) -> PortFuture<'_, ()>;
    fn benchmark(&self, id: BenchmarkId) -> PortFuture<'_, benchmark::Benchmark>;
    fn active_benchmarks(&self) -> PortFuture<'_, Vec<BenchmarkId>>;
    fn begin_trial(&self, id: BenchmarkId, task: TaskId, now: i64) -> PortFuture<'_, ()>;
    /// Task, runs, optional search, memberships, and trial intent commit together.
    fn activate_trial(
        &self,
        id: BenchmarkId,
        task: task::CodingTask,
        runs: Vec<Run>,
        search: Option<search::Search>,
    ) -> PortFuture<'_, ()>;
    fn fail_preparation(
        &self,
        id: BenchmarkId,
        task: TaskId,
        message: String,
        now: i64,
    ) -> PortFuture<'_, ()>;
}
pub trait WorkCoordinator: Send + Sync {
    fn advance(&self) -> PortFuture<'_, bool>;
    fn has_pending(&self) -> PortFuture<'_, bool>;
}
pub trait BranchStore: Send + Sync {
    /// Persist the source state and continuation under a caller-stable identity.
    fn begin_checkpoint(
        &self,
        id: CheckpointId,
        run: RunId,
        now: i64,
    ) -> PortFuture<'_, branch::Checkpoint>;
    fn checkpoint(&self, id: CheckpointId) -> PortFuture<'_, branch::Checkpoint>;
    fn pending_checkpoints(&self) -> PortFuture<'_, Vec<CheckpointId>>;
    /// Check the pinned parent revision and history sequence in the publication transaction.
    fn publish_checkpoint(
        &self,
        id: CheckpointId,
        snapshot: task::WorkspaceSnapshot,
        now: i64,
    ) -> PortFuture<'_, ()>;
    fn fail_checkpoint(&self, id: CheckpointId, error: String, now: i64) -> PortFuture<'_, ()>;
    /// Reserve stable child identities and task membership slots atomically.
    /// Reject fixed benchmark arms; retries must return the same plan or a conflict.
    fn begin_fork(
        &self,
        id: ForkId,
        checkpoint: CheckpointId,
        count: u32,
        now: i64,
    ) -> PortFuture<'_, branch::ForkRecord>;
    fn fork_record(&self, id: ForkId) -> PortFuture<'_, branch::ForkRecord>;
    fn pending_forks(&self) -> PortFuture<'_, Vec<ForkId>>;
    /// Child runs, membership, task accounting, and the preparation receipt commit together.
    fn publish_fork(&self, id: ForkId, runs: Vec<Run>, now: i64) -> PortFuture<'_, ()>;
    fn fail_fork(&self, id: ForkId, error: String, now: i64) -> PortFuture<'_, ()>;
}
pub trait CandidateWorkspaces: Send + Sync {
    fn snapshot(
        &self,
        source: std::path::PathBuf,
        id: TaskId,
    ) -> PortFuture<'_, task::WorkspaceSnapshot>;
    fn fork(&self, snapshot: task::WorkspaceSnapshot, id: RunId) -> PortFuture<'_, String>;
    /// Capture a stable managed workspace under a reusable artifact identity.
    /// Verify protected/unwritable paths against the task's original baseline.
    fn capture_checkpoint(
        &self,
        input: task::CandidateInput,
        artifact: TaskId,
    ) -> PortFuture<'_, task::WorkspaceSnapshot> {
        let _ = (input, artifact);
        Box::pin(async {
            Err(Error::Unavailable(
                "checkpoint capture is not supported".into(),
            ))
        })
    }
    /// Before publication, verify new or reused copies still match the snapshot.
    fn fork_checkpoint(
        &self,
        snapshot: task::WorkspaceSnapshot,
        id: RunId,
    ) -> PortFuture<'_, String> {
        self.fork(snapshot, id)
    }
    fn freeze_case(
        &self,
        source: std::path::PathBuf,
        final_source: std::path::PathBuf,
        baseline_id: TaskId,
        final_id: TaskId,
    ) -> PortFuture<'_, (task::WorkspaceSnapshot, task::WorkspaceSnapshot)> {
        let _ = (source, final_source, baseline_id, final_id);
        Box::pin(async {
            Err(Error::Unavailable(
                "separate benchmark snapshots are not supported".into(),
            ))
        })
    }
    fn branch_snapshot(
        &self,
        snapshot: task::WorkspaceSnapshot,
        id: TaskId,
    ) -> PortFuture<'_, task::WorkspaceSnapshot> {
        let _ = (snapshot, id);
        Box::pin(async {
            Err(Error::Unavailable(
                "snapshot branching is not supported".into(),
            ))
        })
    }
    fn evaluate(
        &self,
        request: task::EvaluationRequest,
        activity: ActivityId,
    ) -> PortFuture<'_, task::CandidateEvaluation>;
}
pub trait ToolRunner: Send + Sync {
    fn execute(
        &self,
        request: agent::ToolRequest,
        activity: ActivityId,
    ) -> PortFuture<'_, serde_json::Value>;
}
/// Native tool implementations declare a stable contract and perform one operation per activity.
/// Return Rejected for an expected problem under a reports_errors contract, or Failure for
/// execution failures using the pinned retry policy. An Error means the outcome may be uncertain.
pub trait NativeTool: Send + Sync {
    fn definition(&self) -> tools::ToolDefinition;
    fn execute(&self, call: tools::NativeToolCall) -> PortFuture<'_, tools::NativeToolResult>;
}
/// Read only native results committed before this recall activity's saved intent.
/// The execution-supplied scope is authoritative; queries cannot choose another Run.
pub trait ResultRecallReadPort: Send + Sync {
    fn list_results(
        &self,
        scope: result_recall::RecallScope,
        query: result_recall::ListResultsQuery,
    ) -> PortFuture<'_, result_recall::ResultPage>;
    fn read_result(
        &self,
        scope: result_recall::RecallScope,
        query: result_recall::ReadResultQuery,
    ) -> PortFuture<'_, result_recall::ResultChunk>;
}
/// Read-only inspection. Expected path/content problems use Error::ToolRejected; runtime
/// workspace authority and temporary ownership/I/O failures retain execution-error semantics.
pub trait WorkspaceReader: Send + Sync {
    /// Optional bounded inspection capability; existing readers keep their pinned behavior.
    fn read_files(
        &self,
        _workspace: String,
        _batch: tools::WorkspaceReadBatch,
        _activity: ActivityId,
    ) -> PortFuture<'_, serde_json::Value> {
        Box::pin(async {
            Err(Error::Unavailable(
                "workspace reader does not support batched reads".into(),
            ))
        })
    }
    fn inspect(
        &self,
        workspace: String,
        action: tools::WorkspaceReadAction,
        activity: ActivityId,
    ) -> PortFuture<'_, serde_json::Value>;
    /// Existing inspection-only adapters remain valid; search is an explicit capability.
    fn search_text(
        &self,
        _workspace: String,
        _search: tools::WorkspaceSearch,
        _activity: ActivityId,
    ) -> PortFuture<'_, serde_json::Value> {
        Box::pin(async {
            Err(Error::Unavailable(
                "workspace reader does not support text search".into(),
            ))
        })
    }
}
/// Journaled writes under host-pinned permissions. Expected refusals may use
/// ToolRejected only before a write intent exists. Other errors retain uncertain effects.
pub trait WorkspaceWriter: Send + Sync {
    fn replace_text(
        &self,
        workspace: String,
        writable_paths: Vec<String>,
        edit: tools::WorkspaceEdit,
        activity: ActivityId,
    ) -> PortFuture<'_, serde_json::Value>;
    /// Existing edit-only adapters remain valid; hosts explicitly install creation support.
    fn create_file(
        &self,
        _workspace: String,
        _writable_paths: Vec<String>,
        _create: tools::WorkspaceCreate,
        _activity: ActivityId,
    ) -> PortFuture<'_, serde_json::Value> {
        Box::pin(async {
            Err(Error::Unavailable(
                "workspace writer does not support file creation".into(),
            ))
        })
    }
}

/// Runs only the host-pinned test command in a managed workspace. Nonzero exit
/// status is completed feedback. Errors after dispatch may have external effects.
pub trait WorkspaceTestRunner: Send + Sync {
    fn run_tests(
        &self,
        workspace: String,
        command: tools::WorkspaceTestCommand,
        activity: ActivityId,
    ) -> PortFuture<'_, serde_json::Value>;
}
/// Executes one approved host-pinned project command. Results do not verify tests.
pub trait WorkspaceCommandRunner: Send + Sync {
    fn run_command(
        &self,
        workspace: String,
        command: tools::NamedWorkspaceCommand,
        activity: ActivityId,
    ) -> PortFuture<'_, serde_json::Value>;
}
pub trait OwnershipGuard: Send + Sync {}
pub trait WorkerOwnership: Send + Sync {
    fn acquire(&self) -> Result<Box<dyn OwnershipGuard>, Error>;
}
