use crate::application::{
    Error,
    ports::{OwnershipGuard, WorkerOwnership},
};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

pub struct FileOwnership {
    path: PathBuf,
}
struct FileGuard {
    _file: File,
}
impl OwnershipGuard for FileGuard {}
impl FileOwnership {
    pub fn new(canonical_database: &Path) -> Self {
        let mut path = canonical_database.as_os_str().to_owned();
        path.push(".worker.lock");
        Self { path: path.into() }
    }
}
impl WorkerOwnership for FileOwnership {
    fn acquire(&self) -> Result<Box<dyn OwnershipGuard>, Error> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(Error::Ownership(self.path.display().to_string()));
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(Error::Io(error)),
        }
        Ok(Box::new(FileGuard { _file: file }))
    }
}
