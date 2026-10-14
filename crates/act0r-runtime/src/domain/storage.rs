//! Reviewed, bounded removal of generated caches; retained execution evidence is never a target.
use super::{AgentId, SessionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageWorkspace {
    pub session_id: SessionId,
    pub agent_id: AgentId,
    pub member: String,
    pub workspace: String,
    pub blocked_reason: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WorkspaceUsage {
    pub workspace_bytes: u64,
    pub cache_bytes: u64,
    pub cache_files: u64,
    pub fingerprint: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageItem {
    pub inspected: bool,
    #[serde(flatten)]
    pub owner: StorageWorkspace,
    #[serde(flatten)]
    pub usage: WorkspaceUsage,
}
impl StorageItem {
    pub fn reclaimable_bytes(&self) -> u64 {
        if self.owner.blocked_reason.is_none() && self.usage.fingerprint.is_some() {
            self.usage.cache_bytes
        } else {
            0
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageReport {
    pub inspection_errors: u32,
    pub database_bytes: u64,
    pub visible_workspace_bytes: u64,
    pub visible_cache_bytes: u64,
    pub reclaimable_bytes: u64,
    pub items: Vec<StorageItem>,
    pub truncated: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheCleanupRequest {
    pub session_id: SessionId,
    pub agent_id: AgentId,
    pub fingerprint: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheCleanupResult {
    pub agent_id: AgentId,
    pub removed_bytes: u64,
    pub removed_files: u64,
    pub already_absent: bool,
}
