use super::{Error, ports::*};
use crate::domain::{DomainError, storage::*};
use std::sync::Arc;

#[derive(Clone)]
pub struct StorageService {
    store: Arc<dyn StorageStorePort>,
    files: Arc<dyn StorageFilesPort>,
}
impl StorageService {
    pub fn new(store: Arc<dyn StorageStorePort>, files: Arc<dyn StorageFilesPort>) -> Self {
        Self { store, files }
    }
    pub async fn report(&self) -> Result<StorageReport, Error> {
        let (owners, truncated) = self.store.storage_workspaces().await?;
        let files = self.files.clone();
        tokio::task::spawn_blocking(move || {
            let database_bytes = files.database_bytes()?;
            let mut report = StorageReport {
                inspection_errors: 0,
                database_bytes,
                visible_workspace_bytes: 0,
                visible_cache_bytes: 0,
                reclaimable_bytes: 0,
                items: Vec::new(),
                truncated,
            };
            for mut owner in owners {
                let mut inspected = true;
                let usage = match files.inspect(&owner.workspace) {
                    Ok(usage) => usage,
                    Err(error) => {
                        inspected = false;
                        report.inspection_errors += 1;
                        owner.blocked_reason = Some(format!("inspection refused: {error}"));
                        WorkspaceUsage::default()
                    }
                };
                let item = StorageItem {
                    owner,
                    usage,
                    inspected,
                };
                report.visible_workspace_bytes += item.usage.workspace_bytes;
                report.visible_cache_bytes += item.usage.cache_bytes;
                report.reclaimable_bytes += item.reclaimable_bytes();
                report.items.push(item);
            }
            Ok(report)
        })
        .await
        .map_err(|e| Error::Unavailable(e.to_string()))?
    }
    pub async fn clean(&self, request: CacheCleanupRequest) -> Result<CacheCleanupResult, Error> {
        if request.fingerprint.len() != 64
            || !request.fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(DomainError::InvalidConfiguration(
                "cleanup requires the reviewed SHA-256 fingerprint".into(),
            )
            .into());
        }
        let guard = self
            .store
            .lock_cache_cleanup(request.session_id, request.agent_id)
            .await?;
        let files = self.files.clone();
        // The blocking operation owns the database guard. Request cancellation cannot release
        // admission protection while filesystem deletion is still executing.
        let (guard, result) = tokio::task::spawn_blocking(move || {
            let result = files
                .clean(&guard.workspace().workspace, &request.fingerprint)
                .map(
                    |(removed_bytes, removed_files, already_absent)| CacheCleanupResult {
                        agent_id: request.agent_id,
                        removed_bytes,
                        removed_files,
                        already_absent,
                    },
                );
            (guard, result)
        })
        .await
        .map_err(|e| Error::Unavailable(e.to_string()))?;
        guard.finish().await?;
        result
    }
}
