//! Stable, replayable setup copies built from one immutable team snapshot.
use super::{candidates::LocalCandidates, workspace::link};
use crate::{
    application::{Error, ports::*},
    domain::{teams::TeamWorkspace, *},
};
use std::{
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

pub struct LocalTeamWorkspaces {
    root: PathBuf,
    candidates: Arc<LocalCandidates>,
}
impl LocalTeamWorkspaces {
    pub fn new(root: PathBuf, candidates: Arc<LocalCandidates>) -> Self {
        Self { root, candidates }
    }
}
struct SetupLease {
    _file: File,
}
impl TeamSetupGuard for SetupLease {}
impl TeamWorkspaces for LocalTeamWorkspaces {
    fn lock_setup(&self, id: TeamId) -> PortFuture<'_, Box<dyn TeamSetupGuard>> {
        Box::pin(async move {
            // Check existing ancestors before creating or opening private setup files.
            for parent in self.root.ancestors() {
                if let Ok(metadata) = fs::symlink_metadata(parent)
                    && link(&metadata)
                {
                    return Err(Error::Unavailable(
                        "team setup storage cannot traverse a link".into(),
                    ));
                }
            }
            fs::create_dir_all(&self.root)?;
            let folder = self.root.join(id.to_string());
            if let Ok(metadata) = fs::symlink_metadata(&folder)
                && link(&metadata)
            {
                return Err(Error::Unavailable("invalid team setup folder".into()));
            }
            fs::create_dir_all(&folder)?;
            let path = folder.join("setup.lock");
            if let Ok(metadata) = fs::symlink_metadata(&path)
                && link(&metadata)
            {
                return Err(Error::Unavailable("invalid team setup lock".into()));
            }
            let file = File::options()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)?;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match file.try_lock() {
                    Ok(()) => {
                        return Ok(Box::new(SetupLease { _file: file }) as Box<dyn TeamSetupGuard>);
                    }
                    Err(fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(25)).await
                    }
                    Err(fs::TryLockError::WouldBlock) => {
                        return Err(Error::Ownership(path.display().to_string()));
                    }
                    Err(fs::TryLockError::Error(error)) => return Err(error.into()),
                }
            }
        })
    }
    fn prepare_member(
        &self,
        team: TeamId,
        actor: AgentId,
        source: String,
    ) -> PortFuture<'_, TeamWorkspace> {
        Box::pin(async move {
            let snapshot_id = team
                .to_string()
                .parse::<TaskId>()
                .map_err(|e| Error::Unavailable(e.to_string()))?;
            let copy_id = actor
                .to_string()
                .parse::<RunId>()
                .map_err(|e| Error::Unavailable(e.to_string()))?;
            let snapshot = self
                .candidates
                .capture(PathBuf::from(source), snapshot_id, false)?;
            let path = self
                .candidates
                .fork_checkpoint(snapshot.clone(), copy_id)
                .await?;
            Ok(TeamWorkspace {
                path,
                snapshot_digest: snapshot.digest,
            })
        })
    }
}
