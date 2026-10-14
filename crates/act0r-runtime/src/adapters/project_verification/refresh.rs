use super::*;
use crate::domain::{project_refresh::RefreshSnapshot, task::SnapshotFile};
fn wait_lock(mut acquire: impl FnMut() -> Result<fs::File, Error>) -> Result<fs::File, Error> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match acquire() {
            Ok(lock) => return Ok(lock),
            Err(Error::Ownership(_)) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(25))
            }
            Err(error) => return Err(error),
        }
    }
}

impl LocalProjectVerification {
    fn refresh_tools(&self) -> Result<super::super::workspace::LocalTools, Error> {
        Ok(super::super::workspace::LocalTools::new(
            self.root
                .parent()
                .ok_or(DomainError::InvalidState)?
                .join("workspaces"),
        ))
    }
    fn refresh_source(source: &str) -> Result<PathBuf, Error> {
        let root = safe_directory(Path::new(source))?;
        if root.join(PRIVATE).join("managed").try_exists()? {
            return Err(Error::Unavailable(
                "refresh requires an original project folder".into(),
            ));
        }
        Ok(root)
    }
    pub(super) fn refresh_snapshot(
        &self,
        source: String,
        workspace: String,
    ) -> Result<(RefreshSnapshot, Vec<SnapshotFile>), Error> {
        let root = Self::refresh_source(&source)?;
        let tools = self.refresh_tools()?;
        let copy = tools.workspace(&workspace)?;
        let source_identity = root_identity(&root)?;
        let workspace_identity = root_identity(&copy)?;
        let files = manifest(&root)?;
        let old = manifest(&copy)?;
        if manifest(&root)? != files
            || manifest(&copy)? != old
            || root_identity(&root)? != source_identity
            || root_identity(&copy)? != workspace_identity
        {
            return Err(DomainError::SessionConflict(
                "project or managed copy changed during inspection; inspect again".into(),
            )
            .into());
        }
        let snapshot = RefreshSnapshot {
            source: root.to_string_lossy().into(),
            source_identity,
            workspace,
            workspace_identity,
            source_digest: digest(&files)?,
            workspace_digest: digest(&old)?,
            files,
        };
        snapshot.validate()?;
        Ok((snapshot, old))
    }
    pub(super) fn lock_refresh_snapshot(
        &self,
        snapshot: RefreshSnapshot,
    ) -> Result<Box<dyn OwnershipGuard>, Error> {
        snapshot.validate()?;
        let root = Self::refresh_source(&snapshot.source)?;
        let source_lock = wait_lock(|| self.applications.project_lock(&root))?;
        let tools = self.refresh_tools()?;
        let copy = tools.workspace(&snapshot.workspace)?;
        let copy_lock = wait_lock(|| tools.lock(&copy))?;
        if root_identity(&root)? != snapshot.source_identity
            || root_identity(&copy)? != snapshot.workspace_identity
            || digest(&manifest(&root)?)? != snapshot.source_digest
            || digest(&manifest(&copy)?)? != snapshot.workspace_digest
            || manifest(&root)? != snapshot.files
        {
            return Err(DomainError::SessionConflict(
                "project or managed copy changed since review; cancel preparation and review again"
                    .into(),
            )
            .into());
        }
        Ok(Box::new(RepairCopiesGuard {
            _locks: vec![source_lock, copy_lock],
        }))
    }
}
