use super::{Error, ports::ProjectDetectionPort};
use crate::domain::project_detection::{DetectionRequest, ProjectDetection};
use std::sync::Arc;

pub struct ProjectDetectionService {
    reader: Arc<dyn ProjectDetectionPort>,
}
impl ProjectDetectionService {
    pub fn new(reader: Arc<dyn ProjectDetectionPort>) -> Self {
        Self { reader }
    }
    pub async fn detect(&self, request: DetectionRequest) -> Result<ProjectDetection, Error> {
        request.validate()?;
        Ok(ProjectDetection::from_facts(
            self.reader.inspect(request.workspace).await?,
        )?)
    }
}
