//! Read only the root guidance file inside a validated managed project copy.
use super::workspace::{LocalTools, link};
use crate::{
    application::ports::ProjectInstructionsPort,
    domain::project_instructions::{FILE_NAME, MAX_BYTES, ProjectInstructions, RefusalReason},
};
use std::{
    fs::{self, File},
    io::{self, Read},
};

impl ProjectInstructionsPort for LocalTools {
    fn capture_project_instructions(&self, workspace: &str) -> ProjectInstructions {
        let refused = |reason| ProjectInstructions::Refused { reason };
        let root = match self.workspace(workspace) {
            Ok(root) => root,
            Err(_) => return refused(RefusalReason::UnsafePath),
        };
        let path = root.join(FILE_NAME);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return ProjectInstructions::Missing {};
            }
            Err(_) => return refused(RefusalReason::Unreadable),
        };
        if link(&metadata) {
            return refused(RefusalReason::UnsafePath);
        }
        if !metadata.is_file() {
            return refused(RefusalReason::NotAFile);
        }
        if metadata.len() > MAX_BYTES as u64 {
            return refused(RefusalReason::TooLarge);
        }
        // The managed copy is trusted host storage, as for existing native reads.
        // Refuse observed path escapes and links before opening; never scan parents
        // or nested instruction files. Read at most the bound plus one sentinel.
        match fs::canonicalize(&path) {
            Ok(resolved) if resolved.parent() == Some(root.as_path()) => {}
            _ => return refused(RefusalReason::UnsafePath),
        }
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(_) => return refused(RefusalReason::Unreadable),
        };
        let mut bytes = Vec::new();
        if file
            .take(MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .is_err()
        {
            return refused(RefusalReason::Unreadable);
        }
        if bytes.len() > MAX_BYTES {
            return refused(RefusalReason::TooLarge);
        }
        let text = match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => return refused(RefusalReason::InvalidUtf8),
        };
        ProjectInstructions::loaded(text).unwrap_or_else(|_| refused(RefusalReason::TooLarge))
    }
}
