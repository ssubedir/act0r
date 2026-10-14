//! Reusable project configuration, independent of session execution and permissions.
use super::{DomainError, MessageId, ProjectProfileId, harness::HarnessCoding};
use serde::{Deserialize, Serialize};

pub const MAX_PROFILES: usize = 100;
pub const MAX_PROFILE_BYTES: usize = 65536;
pub const MAX_REVISION: u64 = 9007199254740990;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSettings {
    pub name: String,
    pub workspace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coding: Option<HarnessCoding>,
}
impl ProjectSettings {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.name.trim().is_empty()
            || self.name.len() > 256
            || self.name.chars().any(char::is_control)
        {
            return Err(DomainError::InvalidConfiguration(
                "profile names require 1..256 UTF-8 bytes without control characters".into(),
            ));
        }
        super::harness::validate_workspace(&self.workspace)?;
        if let Some(coding) = &self.coding {
            coding.validate()?;
        }
        if serde_json::to_vec(self)
            .map_err(|_| DomainError::InvalidState)?
            .len()
            > MAX_PROFILE_BYTES
        {
            return Err(DomainError::InvalidConfiguration(
                "project settings must fit within 65536 UTF-8 JSON bytes".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProfileAction {
    Save { settings: ProjectSettings },
    Remove {},
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileRequest {
    pub request_id: MessageId,
    pub expected_revision: u64,
    pub action: ProfileAction,
}
impl ProfileRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.request_id.to_string() == uuid::Uuid::nil().to_string()
            || self.expected_revision > MAX_REVISION
        {
            return Err(DomainError::InvalidConfiguration(
                "profile requests require a nonzero identity and a supported revision".into(),
            ));
        }
        if let ProfileAction::Save { settings } = &self.action {
            settings.validate()?;
        }
        Ok(())
    }
    pub fn apply(
        &self,
        id: ProjectProfileId,
        current: Option<&ProjectProfile>,
    ) -> Result<ProjectProfile, DomainError> {
        self.validate()?;
        if id.to_string() == uuid::Uuid::nil().to_string() {
            return Err(DomainError::InvalidConfiguration(
                "profile identity must be nonzero".into(),
            ));
        }
        if current.is_some_and(|p| p.deleted)
            || current.map_or(0, |p| p.revision) != self.expected_revision
        {
            return Err(DomainError::ProjectProfileConflict("refresh the profile before changing it; the saved revision changed or the profile was removed".into()));
        }
        let settings = match &self.action {
            ProfileAction::Save { settings } => {
                let mut settings = settings.clone();
                settings.name = settings.name.trim().into();
                settings
            }
            ProfileAction::Remove {} => current
                .ok_or_else(|| {
                    DomainError::ProjectProfileConflict("cannot remove an unsaved profile".into())
                })?
                .settings
                .clone(),
        };
        Ok(ProjectProfile {
            id,
            revision: self.expected_revision + 1,
            settings,
            deleted: matches!(self.action, ProfileAction::Remove {}),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectProfile {
    pub id: ProjectProfileId,
    pub revision: u64,
    pub settings: ProjectSettings,
    pub deleted: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ProfileReceipt {
    pub profile: ProjectProfile,
    pub replayed: bool,
}
#[derive(Debug, Serialize)]
pub struct ProjectProfiles {
    pub items: Vec<ProjectProfile>,
    pub capacity: usize,
}
