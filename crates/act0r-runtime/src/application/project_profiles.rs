use super::{Error, ports::ProjectProfileStorePort};
use crate::domain::{ProjectProfileId, project_profiles::*};
use std::sync::Arc;

pub struct ProjectProfileService {
    store: Arc<dyn ProjectProfileStorePort>,
}
impl ProjectProfileService {
    pub fn new(store: Arc<dyn ProjectProfileStorePort>) -> Self {
        Self { store }
    }
    pub async fn list(&self) -> Result<ProjectProfiles, Error> {
        self.store.project_profiles().await
    }
    pub async fn get(&self, id: ProjectProfileId) -> Result<ProjectProfile, Error> {
        self.store.project_profile(id).await
    }
    pub async fn change(
        &self,
        id: ProjectProfileId,
        request: ProfileRequest,
    ) -> Result<ProfileReceipt, Error> {
        request.validate()?;
        self.store.change_project_profile(id, request).await
    }
}
