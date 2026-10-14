-- Current schema. Apply only when initializing a new database.

-- table: activities
CREATE TABLE "activities" (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES "runs"(id),
    scheduling_revision INTEGER NOT NULL CHECK (scheduling_revision > 0),
    command_ordinal INTEGER NOT NULL CHECK (command_ordinal >= 0),
    status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed','indeterminate')),
    ready_at INTEGER NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    UNIQUE (run_id, scheduling_revision, command_ordinal)
);

-- table: agent_actors
CREATE TABLE agent_actors (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('ready','paused','blocked')),
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: agent_budget_members
CREATE TABLE "agent_budget_members" (
    root_run_id TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'delegation',
    run_id TEXT NOT NULL REFERENCES runs(id),
    PRIMARY KEY(root_run_id,kind,run_id),
    FOREIGN KEY(root_run_id,kind) REFERENCES "agent_budget_scopes"(root_run_id,kind)
);

-- table: agent_budget_scopes
CREATE TABLE "agent_budget_scopes" (
    root_run_id TEXT NOT NULL REFERENCES runs(id),
    kind TEXT NOT NULL DEFAULT 'delegation' CHECK(kind IN ('delegation','goal')),
    payload TEXT NOT NULL,
    PRIMARY KEY(root_run_id,kind)
);

-- table: agent_delegations
CREATE TABLE agent_delegations (
    id TEXT PRIMARY KEY,
    parent_run_id TEXT NOT NULL REFERENCES runs(id),
    root_run_id TEXT REFERENCES runs(id),
    depth INTEGER,
    child_task_id TEXT UNIQUE REFERENCES agent_tasks(id),
    completed INTEGER NOT NULL DEFAULT 0 CHECK(completed IN (0,1)),
    created_at INTEGER NOT NULL
, deadline_at INTEGER, timed_out_at INTEGER, group_id TEXT REFERENCES agent_task_groups(id));

-- table: agent_goal_cancellations
CREATE TABLE agent_goal_cancellations (
    root_task_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: agent_goal_pauses
CREATE TABLE agent_goal_pauses (
    root_task_id TEXT PRIMARY KEY,
    paused INTEGER NOT NULL CHECK(paused IN (0,1)),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: agent_mailbox
CREATE TABLE agent_mailbox (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    recipient TEXT NOT NULL REFERENCES agent_actors(id),
    sender TEXT REFERENCES agent_actors(id),
    status TEXT NOT NULL CHECK (status IN ('queued','processed','rejected')),
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: agent_reminders
CREATE TABLE agent_reminders (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agent_actors(id),
    due_at INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('scheduled','published','cancelled')),
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: agent_runs
CREATE TABLE agent_runs (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    agent_id TEXT NOT NULL REFERENCES agent_actors(id),
    request_id TEXT NOT NULL UNIQUE REFERENCES agent_mailbox(id),
    notified INTEGER NOT NULL CHECK (notified IN (0,1)),
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: agent_task_groups
CREATE TABLE agent_task_groups (
    id TEXT PRIMARY KEY,
    parent_run_id TEXT NOT NULL REFERENCES runs(id),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: agent_tasks
CREATE TABLE agent_tasks (
    id TEXT PRIMARY KEY REFERENCES agent_mailbox(id),
    agent_id TEXT NOT NULL REFERENCES agent_actors(id),
    parent_id TEXT UNIQUE REFERENCES agent_tasks(id),
    run_id TEXT UNIQUE REFERENCES runs(id),
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: agent_team_members
CREATE TABLE agent_team_members (
    team_id TEXT NOT NULL REFERENCES agent_teams(id),
    agent_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    PRIMARY KEY(team_id,name)
);

-- table: agent_teams
CREATE TABLE agent_teams (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('preparing','ready')),
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: attempts
CREATE TABLE "attempts" (
    id TEXT PRIMARY KEY,
    activity_id TEXT NOT NULL REFERENCES "activities"(id),
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    status TEXT NOT NULL CHECK (status IN ('running','succeeded','failed','interrupted')),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    UNIQUE (activity_id, ordinal)
);

-- table: benchmark_jobs
CREATE TABLE benchmark_jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('running', 'waiting', 'completed')),
    created_at INTEGER NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: benchmark_trials
CREATE TABLE benchmark_trials (
    benchmark_id TEXT NOT NULL REFERENCES benchmark_jobs(id),
    task_id TEXT NOT NULL UNIQUE REFERENCES coding_tasks(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (benchmark_id, ordinal)
);

-- table: candidate_checkpoints
CREATE TABLE candidate_checkpoints (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES coding_tasks(id),
    parent_run_id TEXT NOT NULL REFERENCES runs(id),
    status TEXT NOT NULL CHECK (status IN ('preparing', 'ready', 'failed')),
    created_at INTEGER NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: candidate_forks
CREATE TABLE candidate_forks (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES coding_tasks(id),
    checkpoint_id TEXT NOT NULL REFERENCES candidate_checkpoints(id),
    status TEXT NOT NULL CHECK (status IN ('preparing', 'ready', 'failed')),
    created_at INTEGER NOT NULL
);

-- table: coding_tasks
CREATE TABLE coding_tasks (
    id TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: harness_assignment_inspections
CREATE TABLE harness_assignment_inspections (
    activity_id TEXT PRIMARY KEY REFERENCES activities(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_auto_setup_errors
CREATE TABLE harness_auto_setup_errors (
    task_id TEXT PRIMARY KEY REFERENCES harness_turns(task_id),
    message TEXT NOT NULL,
    saved_at INTEGER NOT NULL
);

-- table: harness_automatic_summary_attempts
CREATE TABLE harness_automatic_summary_attempts (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    next_task_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    policy_revision INTEGER NOT NULL,
    through_sequence INTEGER NOT NULL CHECK(through_sequence>0)
);

-- table: harness_initial_dependencies
CREATE TABLE harness_initial_dependencies (
    task_id TEXT PRIMARY KEY REFERENCES agent_delegations(id),
    parent_run_id TEXT NOT NULL REFERENCES runs(id),
    prerequisite_task_id TEXT NOT NULL REFERENCES agent_delegations(id),
    activity_id TEXT NOT NULL UNIQUE REFERENCES activities(id),
    status TEXT NOT NULL CHECK(status IN ('pending','bound','closed')),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_memory_clears
CREATE TABLE harness_memory_clears (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    agent_id TEXT NOT NULL REFERENCES agent_actors(id),
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: harness_permission_receipts
CREATE TABLE harness_permission_receipts (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    revision INTEGER NOT NULL,
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    UNIQUE(session_id, revision)
);

-- table: harness_planner_guidance
CREATE TABLE harness_planner_guidance (
    activity_id TEXT PRIMARY KEY REFERENCES activities(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_planner_holds
CREATE TABLE harness_planner_holds (
    task_id TEXT PRIMARY KEY REFERENCES agent_tasks(id),
    paused INTEGER NOT NULL CHECK(paused IN (0,1)),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_planner_pauses
CREATE TABLE harness_planner_pauses (
    activity_id TEXT PRIMARY KEY REFERENCES activities(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_planner_stops
CREATE TABLE harness_planner_stops (
    activity_id TEXT PRIMARY KEY REFERENCES activities(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_project_continuations
CREATE TABLE "harness_project_continuations" (
    source_session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    source_task_id TEXT NOT NULL UNIQUE REFERENCES harness_turns(task_id),
    coding_session_id TEXT NOT NULL UNIQUE REFERENCES harness_sessions(id),
    coding_task_id TEXT NOT NULL UNIQUE REFERENCES harness_turns(task_id),
    source_sequence INTEGER NOT NULL DEFAULT 1 CHECK(source_sequence>0),
    phase INTEGER NOT NULL DEFAULT 1 CHECK(phase BETWEEN 1 AND 64),
    kind TEXT NOT NULL DEFAULT 'setup' CHECK(kind IN ('setup','repair','refresh')),
    PRIMARY KEY(source_session_id,source_task_id),
    CHECK(source_session_id<>coding_session_id)
);

-- table: harness_project_refreshes
CREATE TABLE harness_project_refreshes (
    request_id TEXT PRIMARY KEY,
    root_session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    source_session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    source_task_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    admitted INTEGER NOT NULL DEFAULT 0 CHECK(admitted IN (0,1)),
    cancelled INTEGER NOT NULL DEFAULT 0 CHECK(cancelled IN (0,1))
);

-- table: harness_session_catalog
CREATE TABLE harness_session_catalog (
    position INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL UNIQUE,
    team_id TEXT NOT NULL UNIQUE REFERENCES agent_teams(id),
    title TEXT,
    archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0,1)),
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
    last_request TEXT
);

-- table: harness_session_permissions
CREATE TABLE harness_session_permissions (
    session_id TEXT PRIMARY KEY REFERENCES harness_sessions(id),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_sessions
CREATE TABLE harness_sessions (
    id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL UNIQUE REFERENCES agent_teams(id),
    can_continue INTEGER NOT NULL CHECK(can_continue IN (0,1))
);

-- table: harness_summary_gates
CREATE TABLE harness_summary_gates (
    session_id TEXT PRIMARY KEY REFERENCES harness_sessions(id),
    next_task_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    policy_revision INTEGER NOT NULL,
    through_sequence INTEGER NOT NULL CHECK(through_sequence>0),
    run_id TEXT REFERENCES runs(id),
    input TEXT,
    blocked_reason TEXT
);

-- table: harness_summary_heads
CREATE TABLE harness_summary_heads (
    session_id TEXT PRIMARY KEY REFERENCES harness_sessions(id),
    revision INTEGER NOT NULL CHECK(revision >= 0),
    latest_run_id TEXT REFERENCES runs(id),
    pending_run_id TEXT REFERENCES runs(id),
    last_run_id TEXT REFERENCES runs(id)
);

-- table: harness_summary_policies
CREATE TABLE harness_summary_policies (
    session_id TEXT PRIMARY KEY REFERENCES harness_sessions(id),
    revision INTEGER NOT NULL CHECK(revision>0),
    enabled INTEGER NOT NULL CHECK(enabled IN (0,1))
);

-- table: harness_summary_policy_requests
CREATE TABLE harness_summary_policy_requests (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    revision INTEGER NOT NULL,
    request TEXT NOT NULL
);

-- table: harness_summary_requests
CREATE TABLE harness_summary_requests (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    run_id TEXT NOT NULL UNIQUE REFERENCES runs(id),
    request TEXT NOT NULL
);

-- table: harness_task_edit_permissions
CREATE TABLE harness_task_edit_permissions (
    task_id TEXT PRIMARY KEY REFERENCES agent_tasks(id),
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES agent_tasks(id),
    grant_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL REFERENCES runs(id),
    revoked INTEGER NOT NULL CHECK(revoked IN (0,1)),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_task_test_permissions
CREATE TABLE harness_task_test_permissions (
    task_id TEXT PRIMARY KEY REFERENCES agent_tasks(id),
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES agent_tasks(id),
    grant_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL REFERENCES runs(id),
    revoked INTEGER NOT NULL CHECK(revoked IN (0,1)),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_turn_recoveries
CREATE TABLE harness_turn_recoveries (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    task_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    activity_id TEXT NOT NULL REFERENCES activities(id),
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    UNIQUE(run_id,activity_id)
);

-- table: harness_turn_steering
CREATE TABLE harness_turn_steering (
    message_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    task_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_turns
CREATE TABLE harness_turns (
    task_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    sequence INTEGER NOT NULL CHECK(sequence>0),
    prompt TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(session_id,sequence)
);

-- table: harness_verification_repairs
CREATE TABLE harness_verification_repairs (
    verification_id TEXT PRIMARY KEY REFERENCES runs(id),
    request_id TEXT NOT NULL UNIQUE,
    root_session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    source_session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    admitted INTEGER NOT NULL DEFAULT 0 CHECK(admitted IN (0,1)),
    cancelled INTEGER NOT NULL DEFAULT 0 CHECK(cancelled IN (0,1))
);

-- table: harness_worker_cancellations
CREATE TABLE harness_worker_cancellations (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    run_id TEXT REFERENCES runs(id),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    CHECK (task_id <> turn_id)
);

-- table: harness_worker_dependencies
CREATE TABLE harness_worker_dependencies (
    activity_id TEXT PRIMARY KEY REFERENCES activities(id),
    planner_run_id TEXT NOT NULL REFERENCES runs(id),
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    prerequisite_task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    closed INTEGER NOT NULL CHECK(closed IN (0,1)),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_worker_messages
CREATE TABLE harness_worker_messages (
    message_id TEXT PRIMARY KEY REFERENCES activities(id),
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    sender_task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    sender_run_id TEXT NOT NULL REFERENCES runs(id),
    recipient_task_id TEXT REFERENCES agent_tasks(id),
    recipient_run_id TEXT REFERENCES runs(id),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_worker_pauses
CREATE TABLE harness_worker_pauses (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    run_id TEXT REFERENCES runs(id),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    CHECK (task_id <> turn_id)
);

-- table: harness_worker_question_receipts
CREATE TABLE harness_worker_question_receipts (
    activity_id TEXT PRIMARY KEY REFERENCES activities(id),
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES agent_tasks(id),
    task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_worker_questions
CREATE TABLE harness_worker_questions (
    question_id TEXT PRIMARY KEY REFERENCES activities(id),
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES agent_tasks(id),
    requester_task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    requester_run_id TEXT NOT NULL REFERENCES runs(id),
    recipient_task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    recipient_run_id TEXT NOT NULL REFERENCES runs(id),
    deadline_at INTEGER NOT NULL,
    resolved INTEGER NOT NULL DEFAULT 0,
    payload TEXT NOT NULL CHECK(json_valid(payload))
);

-- table: harness_worker_recoveries
CREATE TABLE harness_worker_recoveries (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    CHECK (task_id <> turn_id)
);

-- table: harness_worker_steering
CREATE TABLE harness_worker_steering (
    message_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES harness_sessions(id),
    turn_id TEXT NOT NULL REFERENCES harness_turns(task_id),
    task_id TEXT NOT NULL REFERENCES agent_tasks(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    CHECK(task_id <> turn_id)
);

-- table: history
CREATE TABLE "history" (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES "runs"(id),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    UNIQUE (run_id, sequence)
);

-- table: history_positions
CREATE TABLE history_positions (
    position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE REFERENCES history(id),
    run_id TEXT NOT NULL REFERENCES runs(id)
);

-- table: history_stream
CREATE TABLE history_stream (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    id TEXT NOT NULL UNIQUE
);

-- table: project_profile_receipts
CREATE TABLE project_profile_receipts (
    request_id TEXT PRIMARY KEY NOT NULL,
    profile_id TEXT NOT NULL REFERENCES project_profiles(id),
    request TEXT NOT NULL CHECK (json_valid(request)),
    receipt TEXT NOT NULL CHECK (json_valid(receipt))
);

-- table: project_profiles
CREATE TABLE project_profiles (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    deleted INTEGER NOT NULL CHECK (deleted IN (0,1)),
    settings TEXT NOT NULL CHECK (json_valid(settings))
);

-- table: runs
CREATE TABLE "runs" (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed','blocked','budget_exhausted','waiting','paused','cancelled')),
    created_at INTEGER NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: search_jobs
CREATE TABLE search_jobs (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE REFERENCES coding_tasks(id),
    phase TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload))
);

-- table: task_auxiliary_runs
CREATE TABLE task_auxiliary_runs (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    task_id TEXT NOT NULL REFERENCES coding_tasks(id)
);

-- table: task_candidates
CREATE TABLE task_candidates (
    task_id TEXT NOT NULL REFERENCES coding_tasks(id),
    run_id TEXT NOT NULL UNIQUE REFERENCES runs(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (task_id, ordinal)
);

-- table: timers
CREATE TABLE "timers" (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES "runs"(id),
    scheduling_revision INTEGER NOT NULL CHECK (scheduling_revision > 0),
    command_ordinal INTEGER NOT NULL CHECK (command_ordinal >= 0),
    status TEXT NOT NULL CHECK (status IN ('scheduled','fired')),
    due_at INTEGER NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    UNIQUE (run_id, scheduling_revision, command_ordinal)
);

-- index: agent_budget_members_run
CREATE INDEX agent_budget_members_run ON agent_budget_members(run_id);

-- index: agent_delegations_deadlines
CREATE INDEX agent_delegations_deadlines ON agent_delegations(deadline_at)
    WHERE completed=0 AND deadline_at IS NOT NULL AND timed_out_at IS NULL;

-- index: agent_delegations_group
CREATE INDEX agent_delegations_group ON agent_delegations(group_id);

-- index: agent_delegations_pending
CREATE INDEX agent_delegations_pending ON agent_delegations(completed,created_at);

-- index: agent_delegations_root
CREATE INDEX agent_delegations_root ON agent_delegations(root_run_id);

-- index: agent_mailbox_pending
CREATE INDEX agent_mailbox_pending ON agent_mailbox(status,sequence,recipient);

-- index: agent_one_run
CREATE UNIQUE INDEX agent_one_run ON agent_runs(agent_id) WHERE notified=0;

-- index: agent_reminders_agent
CREATE INDEX agent_reminders_agent ON agent_reminders(agent_id,status);

-- index: agent_reminders_due
CREATE INDEX agent_reminders_due ON agent_reminders(status,due_at,agent_id);

-- index: agent_tasks_agent
CREATE INDEX agent_tasks_agent ON agent_tasks(agent_id);

-- index: agent_tasks_follow_up_origin
CREATE INDEX agent_tasks_follow_up_origin ON agent_tasks(agent_id)
    WHERE run_id IS NULL AND json_extract(payload,'$.follow_up_origin') IS NOT NULL;

-- index: agent_tasks_open_agent
CREATE INDEX agent_tasks_open_agent ON agent_tasks(agent_id)
WHERE json_extract(payload, '$.status') NOT IN
    ('succeeded', 'failed', 'rejected', 'budget_exhausted', 'cancelled');

-- index: due_activities_v3
CREATE INDEX due_activities_v3 ON "activities"(status, ready_at, run_id);

-- index: due_timers_v3
CREATE INDEX due_timers_v3 ON "timers"(status, due_at, run_id);

-- index: harness_continuations_source
CREATE INDEX harness_continuations_source ON harness_project_continuations(source_session_id);

-- index: harness_continuations_stage
CREATE UNIQUE INDEX harness_continuations_stage ON harness_project_continuations(source_session_id,source_sequence,phase);

-- index: harness_dependency_holds
CREATE INDEX harness_dependency_holds ON harness_worker_dependencies(task_id,closed);

-- index: harness_dependency_settlement
CREATE INDEX harness_dependency_settlement ON harness_worker_dependencies(planner_run_id,prerequisite_task_id,closed);

-- index: harness_initial_pending
CREATE INDEX harness_initial_pending ON harness_initial_dependencies(status,parent_run_id);

-- index: harness_planner_guidance_turn
CREATE INDEX harness_planner_guidance_turn ON harness_planner_guidance(session_id,turn_id);

-- index: harness_planner_pauses_scope
CREATE INDEX harness_planner_pauses_scope ON harness_planner_pauses(session_id,turn_id,run_id);

-- index: harness_planner_stops_scope
CREATE INDEX harness_planner_stops_scope ON harness_planner_stops(session_id,turn_id,run_id);

-- index: harness_refreshes_pending
CREATE UNIQUE INDEX harness_refreshes_pending ON harness_project_refreshes(root_session_id) WHERE admitted=0 AND cancelled=0;

-- index: harness_refreshes_source
CREATE INDEX harness_refreshes_source ON harness_project_refreshes(source_session_id,source_task_id);

-- index: harness_repairs_pending
CREATE INDEX harness_repairs_pending ON harness_verification_repairs(root_session_id,source_session_id) WHERE admitted=0 AND cancelled=0;

-- index: harness_session_catalog_visibility
CREATE INDEX harness_session_catalog_visibility ON harness_session_catalog(archived,position);

-- index: harness_task_edit_permissions_turn
CREATE INDEX harness_task_edit_permissions_turn ON harness_task_edit_permissions(session_id,turn_id);

-- index: harness_task_test_permissions_turn
CREATE INDEX harness_task_test_permissions_turn ON harness_task_test_permissions(session_id,turn_id);

-- index: harness_turn_recoveries_turn
CREATE INDEX harness_turn_recoveries_turn ON harness_turn_recoveries(task_id);

-- index: harness_turn_steering_turn
CREATE INDEX harness_turn_steering_turn ON harness_turn_steering(task_id);

-- index: harness_worker_cancellation_task
CREATE INDEX harness_worker_cancellation_task ON harness_worker_cancellations(session_id,task_id);

-- index: harness_worker_messages_recipient
CREATE INDEX harness_worker_messages_recipient ON harness_worker_messages(session_id,turn_id,recipient_task_id);

-- index: harness_worker_messages_sender
CREATE INDEX harness_worker_messages_sender ON harness_worker_messages(session_id,turn_id,sender_task_id);

-- index: harness_worker_pause_task
CREATE INDEX harness_worker_pause_task ON harness_worker_pauses(session_id,task_id);

-- index: harness_worker_recovery_task
CREATE INDEX harness_worker_recovery_task ON harness_worker_recoveries(session_id,task_id);

-- index: harness_worker_steering_task
CREATE INDEX harness_worker_steering_task ON harness_worker_steering(task_id);

-- index: history_positions_run
CREATE INDEX history_positions_run ON history_positions(run_id,position);

-- index: planner_question_wakeups
CREATE INDEX planner_question_wakeups ON timers(due_at,run_id)
WHERE status='scheduled' AND json_extract(payload,'$.payload.key')='planner_worker_question';

-- index: project_profile_name
CREATE UNIQUE INDEX project_profile_name ON project_profiles(name COLLATE NOCASE) WHERE deleted=0;

-- index: runs_follow_up_id
CREATE INDEX runs_follow_up_id ON runs(json_extract(payload,'$.follow_up.reminder_id'))
    WHERE json_extract(payload,'$.follow_up.reminder_id') IS NOT NULL;

-- index: worker_question_receipts_task
CREATE INDEX worker_question_receipts_task ON harness_worker_question_receipts(session_id,turn_id,task_id);

-- index: worker_questions_pending
CREATE INDEX worker_questions_pending ON harness_worker_questions(deadline_at) WHERE resolved=0;

-- index: worker_questions_recipient
CREATE INDEX worker_questions_recipient ON harness_worker_questions(session_id,turn_id,recipient_task_id);

-- index: worker_questions_requester
CREATE INDEX worker_questions_requester ON harness_worker_questions(session_id,turn_id,requester_task_id);

-- view: agent_goal_cancelled_requests
CREATE VIEW agent_goal_cancelled_requests AS
WITH RECURSIVE stopped(id,root_task_id) AS (
    SELECT root_task_id,root_task_id FROM agent_goal_cancellations
    UNION
    SELECT o.id,s.root_task_id FROM agent_goal_ownership o JOIN stopped s ON o.parent_id=s.id
)
SELECT id,root_task_id FROM stopped;

-- view: agent_goal_ownership
CREATE VIEW agent_goal_ownership AS
SELECT t.id, COALESCE(t.parent_id, json_extract(t.payload,'$.follow_up_origin.task_id'), a.request_id) AS parent_id
FROM agent_tasks t LEFT JOIN agent_delegations d ON d.child_task_id=t.id
LEFT JOIN agent_runs a ON a.run_id=d.parent_run_id
UNION ALL
SELECT r.id, json_extract(r.payload,'$.origin.task_id')
FROM agent_reminders r WHERE NOT EXISTS(SELECT 1 FROM agent_tasks t WHERE t.id=r.id)
UNION ALL
SELECT d.id, a.request_id FROM agent_delegations d JOIN agent_runs a ON a.run_id=d.parent_run_id
WHERE NOT EXISTS(SELECT 1 FROM agent_tasks t WHERE t.id=d.id)
AND NOT EXISTS(SELECT 1 FROM agent_reminders r WHERE r.id=d.id)
UNION ALL
SELECT json_extract(r.payload,'$.follow_up.reminder_id'), a.request_id
FROM runs r JOIN agent_runs a ON a.run_id=r.id
WHERE json_extract(r.payload,'$.follow_up.reminder_id') IS NOT NULL
AND NOT EXISTS(SELECT 1 FROM agent_tasks t WHERE t.id=json_extract(r.payload,'$.follow_up.reminder_id'))
AND NOT EXISTS(SELECT 1 FROM agent_reminders m WHERE m.id=json_extract(r.payload,'$.follow_up.reminder_id'))
AND NOT EXISTS(SELECT 1 FROM agent_delegations d WHERE d.id=json_extract(r.payload,'$.follow_up.reminder_id'));

-- view: agent_goal_paused_requests
CREATE VIEW agent_goal_paused_requests AS
WITH RECURSIVE roots(root_task_id) AS (
    SELECT root_task_id FROM agent_goal_pauses WHERE paused=1
    UNION
    SELECT task_id FROM harness_planner_holds WHERE paused=1
), held(id,root_task_id) AS (
    SELECT p.root_task_id,p.root_task_id FROM roots p
    WHERE NOT EXISTS(SELECT 1 FROM agent_goal_cancellations c WHERE c.root_task_id=p.root_task_id)
    UNION
    SELECT o.id,h.root_task_id FROM agent_goal_ownership o JOIN held h ON o.parent_id=h.id
)
SELECT id,root_task_id FROM held;

-- view: agent_goal_ready_mailbox
CREATE VIEW agent_goal_ready_mailbox AS
SELECT m.* FROM agent_mailbox m JOIN agent_actors a ON a.id=m.recipient
WHERE m.status='queued' AND a.status='ready' AND (
    json_extract(m.payload,'$.message.kind')<>'task' OR (
        NOT EXISTS(SELECT 1 FROM agent_goal_cancelled_requests s WHERE s.id=m.id)
        AND NOT EXISTS(SELECT 1 FROM agent_goal_paused_requests h WHERE h.id=m.id)
        AND NOT EXISTS(SELECT 1 FROM agent_mailbox earlier JOIN agent_goal_paused_requests h ON h.id=earlier.id
            WHERE earlier.recipient=m.recipient AND earlier.status='queued' AND earlier.sequence<m.sequence
            AND json_extract(earlier.payload,'$.message.kind')='task')
        AND NOT EXISTS(SELECT 1 FROM harness_turns current JOIN harness_turns earlier
            ON earlier.session_id=current.session_id AND earlier.sequence<current.sequence
            JOIN harness_unsettled_turns u ON u.task_id=earlier.task_id WHERE current.task_id=m.id)
        AND NOT EXISTS(SELECT 1 FROM harness_turns t JOIN harness_summary_gates g ON g.session_id=t.session_id WHERE t.task_id=m.id)
        AND NOT EXISTS(SELECT 1 FROM harness_turns t JOIN harness_summary_heads h ON h.session_id=t.session_id WHERE t.task_id=m.id AND h.pending_run_id IS NOT NULL)
    )
);

-- view: harness_initial_blocked_activities
CREATE VIEW harness_initial_blocked_activities AS
SELECT a.id FROM activities a
JOIN agent_runs l ON l.run_id=a.run_id
JOIN harness_initial_dependencies d ON d.task_id=l.request_id
WHERE d.status='pending' AND NOT (
    json_extract(a.payload,'$.spec.handler.version')=1
    AND json_extract(a.payload,'$.spec.handler.name') IN ('workspace.handoff.verify','workspace.baseline')
);

-- view: harness_turn_ownership
CREATE VIEW harness_turn_ownership AS
WITH RECURSIVE owned(id,root_task_id) AS (
    SELECT task_id,task_id FROM harness_turns
    UNION
    SELECT o.id,h.root_task_id FROM agent_goal_ownership o JOIN owned h ON o.parent_id=h.id
)
SELECT id,root_task_id FROM owned;

-- view: harness_unsettled_turns
CREATE VIEW harness_unsettled_turns AS
SELECT h.task_id FROM harness_turns h WHERE
    NOT EXISTS(SELECT 1 FROM agent_tasks t WHERE t.id=h.task_id)
    OR EXISTS(SELECT 1 FROM harness_turn_ownership o JOIN agent_tasks t ON t.id=o.id
        LEFT JOIN runs r ON r.id=t.run_id WHERE o.root_task_id=h.task_id AND (
        (json_extract(t.payload,'$.status') NOT IN ('succeeded','failed','rejected','budget_exhausted','cancelled')
            AND (r.id IS NULL OR r.status NOT IN ('succeeded','failed','budget_exhausted','cancelled')))
        OR r.status NOT IN ('succeeded','failed','budget_exhausted','cancelled')))
    OR EXISTS(SELECT 1 FROM harness_turn_ownership o JOIN agent_reminders m ON m.id=o.id
        WHERE o.root_task_id=h.task_id AND m.status='scheduled')
    OR EXISTS(SELECT 1 FROM harness_turn_ownership o JOIN agent_delegations d ON d.id=o.id
        WHERE o.root_task_id=h.task_id AND d.completed=0)
    OR EXISTS(SELECT 1 FROM harness_turn_ownership o JOIN agent_runs a ON a.request_id=o.id
        JOIN runs r ON r.id=a.run_id LEFT JOIN agent_mailbox m ON m.id=json_extract(a.payload,'$.completion_message_id')
        WHERE o.root_task_id=h.task_id AND (a.notified=0 OR m.status='queued'
            OR json_extract(r.payload,'$.follow_up.status')='pending'));

-- trigger: harness_session_registered
CREATE TRIGGER harness_session_registered AFTER INSERT ON agent_teams
WHEN NEW.name GLOB 'harness-v2-*' AND length(substr(NEW.name,12))=36
AND json_type(NEW.payload,'$.spec.planner.config.goal')='text'
AND json_type(NEW.payload,'$.spec.planner.config.target.model')='text'
BEGIN
    INSERT INTO harness_session_catalog(session_id,team_id) VALUES(substr(NEW.name,12),NEW.id);
END;

-- trigger: harness_session_woken_by_mail
CREATE TRIGGER harness_session_woken_by_mail AFTER INSERT ON agent_mailbox
WHEN NEW.status='queued'
BEGIN
    UPDATE harness_session_catalog SET archived=0,revision=revision+1,last_request=NULL
    WHERE archived=1 AND team_id IN (
        SELECT t.id FROM agent_teams t,json_each(t.payload,'$.members') m
        WHERE json_extract(m.value,'$.agent_id')=NEW.recipient
    );
END;

-- trigger: harness_session_woken_by_reminder
CREATE TRIGGER harness_session_woken_by_reminder AFTER INSERT ON agent_reminders
WHEN NEW.status='scheduled'
BEGIN
    UPDATE harness_session_catalog SET archived=0,revision=revision+1,last_request=NULL
    WHERE archived=1 AND team_id IN (
        SELECT t.id FROM agent_teams t,json_each(t.payload,'$.members') m
        WHERE json_extract(m.value,'$.agent_id')=NEW.agent_id
    );
END;

-- trigger: harness_session_woken_by_run
CREATE TRIGGER harness_session_woken_by_run AFTER INSERT ON runs
WHEN NEW.status NOT IN ('succeeded','failed','budget_exhausted','cancelled')
BEGIN
    UPDATE harness_session_catalog SET archived=0,revision=revision+1,last_request=NULL
    WHERE archived=1 AND (
        EXISTS(SELECT 1 FROM harness_turns ht WHERE ht.session_id=harness_session_catalog.session_id
            AND ht.task_id=json_extract(NEW.payload,'$.input.task_id'))
        OR EXISTS(SELECT 1 FROM agent_teams t,json_each(t.payload,'$.members') m,json_tree(NEW.payload,'$.input') j
            WHERE t.id=harness_session_catalog.team_id AND j.type='text'
            AND j.value=json_extract(m.value,'$.workspace.path'))
    );
END;

-- trigger: history_index_insert
CREATE TRIGGER history_index_insert AFTER INSERT ON history
BEGIN
    INSERT INTO history_positions(event_id,run_id) VALUES(NEW.id,NEW.run_id);
END;

INSERT INTO history_stream(singleton,id) VALUES(1,lower(hex(randomblob(16))));
PRAGMA user_version=62;
