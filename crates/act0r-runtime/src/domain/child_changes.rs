//! A planner imports exactly the immutable artifact it inspected, inside its own Run.
use super::{patch_application::*, *};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const APPROVAL_KEY: &str = "child_changes_approval";
pub const PAGE_BYTES: usize = 8 * 1024;
pub fn handler(name: &str) -> DefinitionId {
    DefinitionId::new(format!("planner.child_changes.{name}"), 1)
}
pub fn is_handler(id: &DefinitionId) -> bool {
    worker_changes::is_handler(id)
        || ["inspect", "import"]
            .iter()
            .any(|name| id == &handler(name))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTarget {
    pub workspace: String,
    pub writable_paths: Vec<String>,
}
impl ManagedTarget {
    pub fn validate(&self) -> Result<(), DomainError> {
        tools::validate_tool_workspace(&[], Some(&self.workspace))?;
        tools::validate_writable_paths(&self.writable_paths)?;
        if self.workspace.trim().is_empty() || self.writable_paths.is_empty() {
            return Err(DomainError::InvalidConfiguration(
                "child imports require a workspace and writable paths".into(),
            ));
        }
        Ok(())
    }
    pub fn permits(&self, path: &str) -> bool {
        self.writable_paths.iter().any(|p| {
            if p.ends_with('/') {
                path.starts_with(p)
            } else {
                path == p
            }
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectRequest {
    pub worker: String,
    pub recipient: AgentId,
    pub target: ManagedTarget,
    /// Byte offset into the saved canonical patch; pages never split a UTF-8 character.
    pub offset: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency: Option<planner_pause::WorkerDependency>,
}
impl InspectRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.target.validate()?;
        if let Some(d) = &self.dependency {
            worker_changes::InspectWorkerChanges {
                dependency: d.clone(),
                offset: self.offset,
            }
            .validate()?;
            if self.worker != d.prerequisite {
                return Err(DomainError::InvalidState);
            }
        }
        if self.worker.is_empty() || self.worker.len() > 64 || self.offset > 4 * 1024 * 1024 {
            return Err(DomainError::InvalidConfiguration(
                "invalid child patch inspection".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inspection {
    pub request: InspectRequest,
    pub task_id: MessageId,
    pub child_run_id: RunId,
    pub artifact_id: ActivityId,
    pub preview: ApplicationPreview,
    pub patch: String,
    pub patch_bytes: u32,
    pub next_offset: Option<u32>,
}
impl Inspection {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.request.validate()?;
        if let Some(d) = &self.request.dependency
            && (d.prerequisite_task_id != self.task_id
                || d.prerequisite_run_id != self.child_run_id
                || d.planner_run_id != self.preview.run_id)
        {
            return Err(DomainError::InvalidState);
        }
        if self.patch.len() > PAGE_BYTES
            || self.request.offset > self.patch_bytes
            || self.patch_bytes > 4 * 1024 * 1024
            || (self.patch.is_empty() && self.request.offset < self.patch_bytes)
            || self.preview.target != self.request.target.workspace
            || self.preview.files.len() > 128
            || self.preview.root_identity.is_empty()
            || self.preview.root_identity.len() > 256
            || self.preview.input_digest.len() != 64
            || !self
                .preview
                .input_digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || self.preview.files.iter().any(|f| {
                !tools::portable_file_path(&f.path)
                    || (!self.request.target.permits(&f.path) && f.conflict.is_none())
                    || f.conflict
                        .as_ref()
                        .is_some_and(|c| c.is_empty() || c.len() > 4096)
                    || [&f.before_sha256, &f.after_sha256, &f.observed_sha256]
                        .iter()
                        .any(|hash| {
                            hash.as_ref().is_some_and(|h| {
                                h.len() != 64 || !h.bytes().all(|b| b.is_ascii_hexdigit())
                            })
                        })
            })
            || self.next_offset
                != ((self.request.offset as usize + self.patch.len()) < self.patch_bytes as usize)
                    .then_some(self.request.offset + self.patch.len() as u32)
            || self.request.offset as usize + self.patch.len() > self.patch_bytes as usize
        {
            return Err(DomainError::InvalidState);
        }
        Ok(())
    }
    /// Keep model context bounded. Full evidence and file hashes stay in the activity receipt.
    pub fn observation(&self) -> Value {
        let mut files = vec![];
        let mut file_bytes = 0;
        for file in &self.preview.files {
            let bytes = serde_json::to_vec(file).map_or(4097, |v| v.len());
            if files.len() == 16 || file_bytes + bytes > 4096 {
                break;
            }
            files.push(file);
            file_bytes += bytes;
        }
        let tests = self.preview.tests.as_ref().map(|t| json!({
            "activity_id":t.activity_id,"command":t.command,"ok":t.result["ok"],"exit_code":t.result["exit_code"],
            "before_digest":t.before_digest,"after_digest":t.after_digest
        }));
        json!({"ok":true,"worker":self.request.worker,"task_id":self.task_id,"run_id":self.child_run_id,
            "artifact_id":self.artifact_id,"inspection_id":self.preview.preparation_id,
            "files":files,"file_count":self.preview.files.len(),
            "files_truncated":self.preview.files.len()>files.len(),"conflicted":self.preview.has_conflicts(),
            "test_status":self.preview.test_status,"tests":tests,"patch":self.patch,
            "offset":self.request.offset,"patch_bytes":self.patch_bytes,"next_offset":self.next_offset})
    }
}
pub fn activity(name: &str, input: Value) -> Command {
    Command::Schedule(ActivitySpec {
        handler: handler(name),
        input,
        retry: RetryPolicy::default(),
        usage: None,
        recovery: if name == "import" {
            RecoveryPolicy::RequiresReconciliation
        } else {
            RecoveryPolicy::SafeToRepeat
        },
    })
}
