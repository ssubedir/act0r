//! Snapshot/copy receipts belong to activities; actor publication remains a SQLite transaction.
use super::{
    candidates::{LocalCandidates, manifest},
    workspace::{LocalTools, PRIVATE, atomic_write, link, read_with_limit},
};
use crate::{
    application::{Error, ports::*},
    domain::{task::WorkspaceSnapshot, workspace_handoff::*, *},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct LocalWorkspaceHandoff {
    authority: Arc<dyn WorkspaceHandoffReadPort>,
    snapshots: Arc<LocalCandidates>,
    tools: Arc<LocalTools>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Verification {
    run_id: RunId,
    activity_id: ActivityId,
    input: PreparedWorkspace,
}
impl LocalWorkspaceHandoff {
    pub fn new(
        authority: Arc<dyn WorkspaceHandoffReadPort>,
        snapshots: Arc<LocalCandidates>,
        tools: Arc<LocalTools>,
    ) -> Self {
        Self {
            authority,
            snapshots,
            tools,
        }
    }
    fn receipt(&self, snapshot: &WorkspaceSnapshot) -> Result<PathBuf, Error> {
        Ok(self
            .snapshots
            .verify(snapshot)?
            .join(PRIVATE)
            .join("handoff.json"))
    }
    fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Error> {
        if link(&fs::symlink_metadata(path)?) {
            return Err(Error::Unavailable(
                "workspace handoff receipt is a link".into(),
            ));
        }
        Ok(serde_json::from_slice(&read_with_limit(
            path,
            8 * 1024 * 1024,
        )?)?)
    }
    fn saved(&self, input: &PreparedWorkspace) -> Result<WorkspaceSnapshot, Error> {
        input.validate()?;
        let snapshot = self.snapshots.load_snapshot(input.snapshot_id)?;
        let matches = if let Some(index) = input.group_index {
            let group: PreparedGroup = Self::read(
                &self
                    .receipt(&snapshot)?
                    .with_file_name("handoff-group.json"),
            )?;
            group.validate()?;
            group.workspaces.get(index as usize) == Some(input)
        } else {
            Self::read::<PreparedWorkspace>(&self.receipt(&snapshot)?)? == *input
        };
        if snapshot.digest != input.snapshot_digest || !matches {
            return Err(Error::Unavailable(
                "workspace handoff differs from its saved preparation receipt".into(),
            ));
        }
        self.tools.workspace(&input.workspace)?;
        Ok(snapshot)
    }
    async fn prepare(&self, call: &ActivityExecution) -> Result<Value, Error> {
        let request: PrepareRequest = serde_json::from_value(call.input.clone())?;
        let worker = self
            .authority
            .handoff_worker(call.run_id, request.clone())
            .await?;
        let source = self.tools.workspace(&request.workspace)?;
        let _guard = self.tools.lock(&source)?;
        let snapshot_id: TaskId = call
            .activity_id
            .to_string()
            .parse()
            .map_err(|_| DomainError::InvalidState)?;
        // capture reuses an already published snapshot even if the planner subsequently changed.
        let snapshot = self.snapshots.capture(source, snapshot_id, true)?;
        let path = self.receipt(&snapshot)?;
        if path.try_exists()? {
            let input: PreparedWorkspace = Self::read(&path)?;
            if input.parent_run_id != call.run_id
                || input.activity_id != call.activity_id
                || input.request != request
            {
                return Err(Error::Unavailable(
                    "workspace handoff receipt identity differs from this activity".into(),
                ));
            }
            input.check_worker(request.recipient, &worker)?;
            self.saved(&input)?;
            return Ok(json!({"ok":true,"prepared":input}));
        }
        let copy_id = call
            .activity_id
            .to_string()
            .parse()
            .map_err(|_| DomainError::InvalidState)?;
        let workspace = self
            .snapshots
            .fork_checkpoint(snapshot.clone(), copy_id)
            .await?;
        let input = PreparedWorkspace {
            parent_run_id: call.run_id,
            activity_id: call.activity_id,
            task_id: call
                .activity_id
                .to_string()
                .parse()
                .map_err(|_| DomainError::InvalidState)?,
            request,
            workspace,
            snapshot_id,
            snapshot_digest: snapshot.digest,
            worker_pin: worker_pin(&worker)?,
            group_index: None,
        };
        input.validate()?;
        atomic_write(&path, &serde_json::to_vec(&input)?)?;
        Ok(json!({"ok":true,"prepared":input}))
    }
    async fn prepare_group(&self, call: &ActivityExecution) -> Result<Value, Error> {
        let request: PrepareGroupRequest = serde_json::from_value(call.input.clone())?;
        request.validate()?;
        let workers = self
            .authority
            .handoff_group_workers(call.run_id, request.clone())
            .await?;
        if workers.len() != request.tasks.len() {
            return Err(DomainError::InvalidState.into());
        }
        let source = self.tools.workspace(&request.workspace)?;
        let _guard = self.tools.lock(&source)?;
        let snapshot_id = call
            .activity_id
            .to_string()
            .parse()
            .map_err(|_| DomainError::InvalidState)?;
        let snapshot = self.snapshots.capture(source, snapshot_id, true)?;
        let path = self
            .receipt(&snapshot)?
            .with_file_name("handoff-group.json");
        if path.try_exists()? {
            let prepared: PreparedGroup = Self::read(&path)?;
            prepared.validate()?;
            if prepared.parent_run_id != call.run_id
                || prepared.activity_id != call.activity_id
                || prepared.request != request
            {
                return Err(Error::Unavailable(
                    "workspace group receipt differs from this activity".into(),
                ));
            }
            for (input, worker) in prepared.workspaces.iter().zip(&workers) {
                input.check_worker(input.request.recipient, worker)?;
                self.saved(input)?;
            }
            return Ok(json!({"ok":true,"prepared":prepared}));
        }
        let mut workspaces = Vec::with_capacity(request.tasks.len());
        for (index, (task, worker)) in request.tasks.iter().zip(&workers).enumerate() {
            let task_id = member_task_id(call.activity_id, index as u32)?;
            let copy_id = task_id
                .to_string()
                .parse()
                .map_err(|_| DomainError::InvalidState)?;
            let workspace = self
                .snapshots
                .fork_checkpoint(snapshot.clone(), copy_id)
                .await?;
            workspaces.push(PreparedWorkspace {
                parent_run_id: call.run_id,
                activity_id: call.activity_id,
                task_id,
                request: request.member_request(task),
                workspace,
                snapshot_id,
                snapshot_digest: snapshot.digest.clone(),
                worker_pin: worker_pin(worker)?,
                group_index: Some(index as u32),
            });
        }
        let prepared = PreparedGroup {
            parent_run_id: call.run_id,
            activity_id: call.activity_id,
            request,
            workspaces,
        };
        prepared.validate()?;
        atomic_write(&path, &serde_json::to_vec(&prepared)?)?;
        Ok(json!({"ok":true,"prepared":prepared}))
    }
    async fn verify(&self, call: &ActivityExecution) -> Result<Value, Error> {
        let input: PreparedWorkspace = serde_json::from_value(call.input.clone())?;
        let committed = self
            .authority
            .verify_workspace_input(call.run_id, input.clone())
            .await?;
        let snapshot = self.saved(&input)?;
        let root = self.tools.workspace(&input.workspace)?;
        let _guard = self.tools.lock(&root)?;
        let path = root.join(PRIVATE).join("handoff-verified.json");
        if path.try_exists()? {
            let receipt: Verification = Self::read(&path)?;
            if receipt.run_id != call.run_id
                || receipt.activity_id != call.activity_id
                || receipt.input != input
            {
                return Err(Error::Unavailable(
                    "workspace verification receipt identity differs from this activity".into(),
                ));
            }
            // Only a committed checkpoint can have been followed by child edits.
            if committed {
                return Ok(json!({"verified":true}));
            }
        }
        let expected = self
            .snapshots
            .fork_checkpoint(
                snapshot.clone(),
                input
                    .task_id
                    .to_string()
                    .parse()
                    .map_err(|_| DomainError::InvalidState)?,
            )
            .await?;
        if expected != input.workspace || manifest(&root)? != snapshot.files {
            return Err(Error::Unavailable(
                "queued workspace differs from the handed-off snapshot".into(),
            ));
        }
        atomic_write(
            &path,
            &serde_json::to_vec(&Verification {
                run_id: call.run_id,
                activity_id: call.activity_id,
                input,
            })?,
        )?;
        Ok(json!({"verified":true}))
    }
}
impl WorkspaceHandoffPort for LocalWorkspaceHandoff {
    fn execute(&self, call: ActivityExecution) -> PortFuture<'_, Outcome> {
        Box::pin(async move {
            let preparing =
                call.handler == handler("prepare") || call.handler == handler("prepare_group");
            let result = if call.handler == handler("prepare") {
                self.prepare(&call).await
            } else if call.handler == handler("prepare_group") {
                self.prepare_group(&call).await
            } else if call.handler == handler("verify") {
                self.verify(&call).await
            } else {
                Err(Error::Unavailable(
                    "unsupported workspace handoff operation".into(),
                ))
            };
            Ok(match result {
                Ok(result) => Outcome::Success(result),
                Err(Error::Unavailable(message)) if preparing => Outcome::Success(
                    json!({"ok":false,"error":{"code":"workspace_handoff_unavailable","message":message}}),
                ),
                Err(error) => Outcome::Failure(Failure::new(
                    "workspace_handoff",
                    error.to_string(),
                    matches!(
                        error,
                        Error::Io(_) | Error::Ownership(_) | Error::Storage(_)
                    ),
                )),
            })
        })
    }
}
