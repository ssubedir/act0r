//! Local cache inspection and removal. Only a marked Cargo target inside a managed copy is eligible.
use super::workspace::{LocalTools, atomic_write, link, read_with_limit};
use crate::{
    application::{Error, ports::StorageFilesPort},
    domain::{DomainError, storage::WorkspaceUsage},
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

const MAX_ENTRIES: usize = 200_000;
const TAG: &str = "Signature: 8a477f597d28d172789f06886806bc55";
const JOURNAL: &str = ".act0r-tools/cache-cleanup.json";
const MAX_JOURNAL: u64 = 32 * 1024 * 1024;
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanupIntent {
    version: u32,
    workspace: String,
    fingerprint: String,
    entries: Vec<Entry>,
    complete: bool,
}
struct Inspection {
    usage: WorkspaceUsage,
    entries: Vec<Entry>,
    intent: Option<CleanupIntent>,
}
fn fingerprint(entries: &[Entry]) -> Result<String, Error> {
    let mut digest = Sha256::new();
    for entry in entries {
        digest.update(serde_json::to_vec(entry)?);
        digest.update(b"\n");
    }
    Ok(format!("{:x}", digest.finalize()))
}
pub struct LocalStorage {
    root: PathBuf,
    database: PathBuf,
    tools: LocalTools,
}
impl LocalStorage {
    pub fn new(root: PathBuf, database: PathBuf) -> Self {
        Self {
            tools: LocalTools::new(root.clone()),
            root,
            database,
        }
    }
    fn workspace(&self, value: &str) -> Result<PathBuf, Error> {
        for ancestor in Path::new(value).ancestors() {
            if link(&fs::symlink_metadata(ancestor)?) {
                return Err(Error::Unavailable(
                    "storage cannot traverse links or reparse points".into(),
                ));
            }
        }
        let path = self.tools.workspace(value)?;
        if path.parent() != Some(fs::canonicalize(&self.root)?.as_path()) {
            return Err(DomainError::InvalidState.into());
        }
        Ok(path)
    }
    fn intent(&self, workspace: &Path) -> Result<Option<CleanupIntent>, Error> {
        let journal = workspace.join(JOURNAL);
        match fs::symlink_metadata(&journal) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(meta) if link(&meta) || !meta.is_file() => {
                return Err(Error::Unavailable("invalid cleanup journal".into()));
            }
            Ok(_) => {}
        }
        let saved: CleanupIntent =
            serde_json::from_slice(&read_with_limit(&journal, MAX_JOURNAL)?)?;
        if saved.version != 1
            || saved.workspace != workspace.to_string_lossy()
            || saved.entries.len() > MAX_ENTRIES
            || saved.fingerprint.len() != 64
            || !saved.fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
            || !saved.complete && fingerprint(&saved.entries)? != saved.fingerprint
        {
            return Err(Error::Unavailable(
                "cleanup journal binding is invalid".into(),
            ));
        }
        Ok(Some(saved))
    }
    fn inspect_path(&self, workspace: &Path) -> Result<Inspection, Error> {
        let mut usage = WorkspaceUsage::default();
        let intent = self.intent(workspace)?;
        let mut count = 0;
        scan(
            workspace,
            workspace,
            &mut count,
            &mut usage.workspace_bytes,
            &mut Vec::new(),
        )?;
        let target = workspace.join("target");
        match fs::symlink_metadata(&target) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Inspection {
                    usage,
                    entries: Vec::new(),
                    intent,
                });
            }
            Err(error) => return Err(error.into()),
            Ok(meta) if !meta.is_dir() || link(&meta) => {
                return Err(Error::Unavailable(
                    "Cargo cache is not a regular directory".into(),
                ));
            }
            Ok(_) => {}
        }
        let mut entries = Vec::new();
        let mut count = 0;
        scan(
            &target,
            &target,
            &mut count,
            &mut usage.cache_bytes,
            &mut entries,
        )?;
        usage.cache_files = entries.iter().filter(|e| e.1).count() as u64;
        if entries.iter().any(|e| {
            let name = e.0.to_ascii_lowercase();
            e.0.split('/').any(|part| {
                part.eq_ignore_ascii_case(".act0r") || part.eq_ignore_ascii_case(".act0r-tools")
            }) || [
                ".db",
                ".db-wal",
                ".db-shm",
                ".sqlite",
                ".sqlite-wal",
                ".sqlite-shm",
                ".sqlite3",
                ".sqlite3-wal",
                ".sqlite3-shm",
            ]
            .iter()
            .any(|suffix| name.ends_with(suffix))
        }) {
            return Err(Error::Unavailable(
                "cache contains runtime evidence or a database; cleanup refused".into(),
            ));
        }
        entries.sort();
        if entries.is_empty()
            && let Some(saved) = &intent
        {
            usage.fingerprint = Some(saved.fingerprint.clone());
            return Ok(Inspection {
                usage,
                entries,
                intent,
            });
        }
        if let Some(saved) = intent.as_ref().filter(|i| !i.complete) {
            let original: std::collections::BTreeMap<_, _> =
                saved.entries.iter().map(|e| (&e.0, e)).collect();
            if entries.iter().any(|entry| {
                original.get(&entry.0).is_none_or(|saved| {
                    saved.1 != entry.1 || entry.1 && (saved.2 != entry.2 || saved.3 != entry.3)
                })
            }) {
                return Err(Error::StorageConflict(
                    "interrupted cache contains new or changed files; cleanup refused".into(),
                ));
            }
            usage.fingerprint = Some(saved.fingerprint.clone());
        } else {
            for name in ["CACHEDIR.TAG", ".rustc_info.json"] {
                let meta = fs::symlink_metadata(target.join(name))?;
                if !meta.is_file() || link(&meta) || meta.len() > 64 * 1024 {
                    return Err(Error::Unavailable("Cargo cache marker is invalid".into()));
                }
            }
            if !fs::read_to_string(target.join("CACHEDIR.TAG"))?.starts_with(TAG)
                || !serde_json::from_slice::<serde_json::Value>(&fs::read(
                    target.join(".rustc_info.json"),
                )?)?
                .is_object()
            {
                return Err(Error::Unavailable(
                    "target is not a recognized Cargo cache".into(),
                ));
            }
            usage.fingerprint = Some(fingerprint(&entries)?);
        }
        Ok(Inspection {
            usage,
            entries,
            intent,
        })
    }
}
type Entry = (String, bool, u64, u128);
fn scan(
    root: &Path,
    path: &Path,
    count: &mut usize,
    bytes: &mut u64,
    entries: &mut Vec<Entry>,
) -> Result<(), Error> {
    if path
        .components()
        .count()
        .saturating_sub(root.components().count())
        > 128
    {
        return Err(Error::Unavailable(
            "storage tree exceeds 128 levels; cleanup refused".into(),
        ));
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        *count += 1;
        if *count > MAX_ENTRIES {
            return Err(Error::Unavailable(
                "storage scan exceeds 200,000 entries; cleanup refused".into(),
            ));
        }
        let meta = fs::symlink_metadata(entry.path())?;
        if link(&meta) || !(meta.is_file() || meta.is_dir()) {
            return Err(Error::Unavailable(
                "storage contains a link, reparse point or special file".into(),
            ));
        }
        let relative = entry
            .path()
            .strip_prefix(root)
            .map_err(|_| DomainError::InvalidState)?
            .to_str()
            .ok_or_else(|| Error::Unavailable("storage names must be UTF-8".into()))?
            .replace('\\', "/");
        entries.push((
            relative,
            meta.is_file(),
            if meta.is_file() { meta.len() } else { 0 },
            meta.modified()?
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        ));
        if meta.is_file() {
            *bytes = bytes.saturating_add(meta.len());
        } else {
            scan(root, &entry.path(), count, bytes, entries)?;
        }
    }
    Ok(())
}
fn remove_cache(root: &Path, path: &Path, count: &mut usize) -> Result<(), Error> {
    if path
        .components()
        .count()
        .saturating_sub(root.components().count())
        > 128
    {
        return Err(Error::Unavailable(
            "cache changed beyond cleanup depth bounds".into(),
        ));
    }
    if !path.starts_with(root)
        || link(&fs::symlink_metadata(path)?)
        || fs::canonicalize(path)?.strip_prefix(root).is_err()
    {
        return Err(Error::Unavailable(
            "cache removal escaped its verified root".into(),
        ));
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        *count += 1;
        if *count > MAX_ENTRIES {
            return Err(Error::Unavailable(
                "cache changed beyond cleanup bounds".into(),
            ));
        }
        let meta = fs::symlink_metadata(entry.path())?;
        if link(&meta) {
            return Err(Error::Unavailable("cache changed to contain a link".into()));
        }
        if meta.is_dir() {
            remove_cache(root, &entry.path(), count)?;
        } else if meta.is_file() {
            fs::remove_file(entry.path())?;
            #[cfg(test)]
            if std::env::var_os("ACT0R_STORAGE_CRASH_ROOT").is_some_and(|p| Path::new(&p) == root) {
                std::process::exit(86);
            }
        } else {
            return Err(Error::Unavailable("cache contains a special file".into()));
        }
    }
    // Keep the empty cache root until the completion receipt has been synced.
    if path != root {
        fs::remove_dir(path)?;
    }
    Ok(())
}
impl StorageFilesPort for LocalStorage {
    fn database_bytes(&self) -> Result<u64, Error> {
        let mut bytes: u64 = 0;
        for suffix in ["", "-wal", "-shm"] {
            let path = PathBuf::from(format!("{}{suffix}", self.database.display()));
            match fs::symlink_metadata(path) {
                Ok(meta) if meta.is_file() && !link(&meta) => {
                    bytes = bytes.saturating_add(meta.len())
                }
                Ok(_) => {
                    return Err(Error::Unavailable(
                        "database storage is not a regular file".into(),
                    ));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(bytes)
    }
    fn inspect(&self, workspace: &str) -> Result<WorkspaceUsage, Error> {
        self.inspect_path(&self.workspace(workspace)?)
            .map(|i| i.usage)
    }
    fn clean(&self, workspace: &str, fingerprint: &str) -> Result<(u64, u64, bool), Error> {
        let workspace = self.workspace(workspace)?;
        let _lease = self.tools.lock(&workspace)?;
        let inspected = self.inspect_path(&workspace)?;
        let usage = inspected.usage;
        if usage.fingerprint.is_none() {
            if let Some(mut intent) = inspected
                .intent
                .filter(|i| !i.complete && i.fingerprint == fingerprint)
            {
                intent.complete = true;
                intent.entries.clear();
                atomic_write(&workspace.join(JOURNAL), &serde_json::to_vec(&intent)?)?;
            }
            return Ok((0, 0, true));
        }
        if usage.fingerprint.as_deref() != Some(fingerprint) {
            return Err(Error::StorageConflict(
                "cache changed since review; refresh storage before cleaning".into(),
            ));
        }
        let target = fs::canonicalize(workspace.join("target"))?;
        if target.parent() != Some(workspace.as_path()) {
            return Err(DomainError::InvalidState.into());
        }
        let mut intent = match inspected.intent {
            Some(saved) if !saved.complete => saved,
            _ => CleanupIntent {
                version: 1,
                workspace: workspace.to_string_lossy().into(),
                fingerprint: fingerprint.into(),
                entries: inspected.entries,
                complete: false,
            },
        };
        let bytes = serde_json::to_vec(&intent)?;
        if bytes.len() as u64 > MAX_JOURNAL {
            return Err(Error::Unavailable("cleanup intent exceeds 32 MiB".into()));
        }
        atomic_write(&workspace.join(JOURNAL), &bytes)?;
        remove_cache(&target, &target, &mut 0)?;
        intent.complete = true;
        intent.entries.clear();
        atomic_write(&workspace.join(JOURNAL), &serde_json::to_vec(&intent)?)?;
        fs::remove_dir(&target)?;
        Ok((usage.cache_bytes, usage.cache_files, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(workspace: &Path) {
        fs::create_dir_all(workspace.join(".act0r-tools")).unwrap();
        fs::write(
            workspace.join(".act0r-tools/managed"),
            b"act0r workspace v1\n",
        )
        .unwrap();
        fs::write(workspace.join("keep.txt"), "retained").unwrap();
        fs::create_dir_all(workspace.join("target/debug")).unwrap();
        fs::write(workspace.join("target/CACHEDIR.TAG"), TAG).unwrap();
        fs::write(workspace.join("target/.rustc_info.json"), "{}").unwrap();
        fs::write(workspace.join("target/debug/generated.bin"), "generated").unwrap();
    }
    #[test]
    #[ignore = "child exits immediately after a journaled cache deletion"]
    fn cleanup_crash_fixture() {
        let target = PathBuf::from(std::env::var_os("ACT0R_STORAGE_CRASH_ROOT").unwrap());
        let workspace = target.parent().unwrap();
        let files = LocalStorage::new(
            workspace.parent().unwrap().into(),
            workspace.parent().unwrap().join("fixture.db"),
        );
        let preview = files.inspect(workspace.to_str().unwrap()).unwrap();
        files
            .clean(
                workspace.to_str().unwrap(),
                preview.fingerprint.as_deref().unwrap(),
            )
            .unwrap();
        panic!("fixture did not exit at the first removal");
    }
    fn interrupt(workspace: &Path) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "adapters::storage::tests::cleanup_crash_fixture",
            ])
            .env(
                "ACT0R_STORAGE_CRASH_ROOT",
                fs::canonicalize(workspace.join("target")).unwrap(),
            )
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    #[test]
    fn journaled_cache_cleanup_recovers_after_process_exit_and_refuses_new_files() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fixture(&workspace);
        fs::write(
            workspace.join("target/debug/generated.bin"),
            "new build outputs",
        )
        .unwrap();
        let workspace = fs::canonicalize(workspace).unwrap();
        let files = LocalStorage::new(root.path().into(), root.path().join("fixture.db"));
        let reviewed = files
            .inspect(workspace.to_str().unwrap())
            .unwrap()
            .fingerprint
            .unwrap();
        interrupt(&workspace);
        let saved = files.intent(&workspace).unwrap().unwrap();
        assert!(!saved.complete);
        assert_eq!(saved.fingerprint, reviewed);
        fs::write(workspace.join("target/new.bin"), "new evidence").unwrap();
        assert!(files.clean(workspace.to_str().unwrap(), &reviewed).is_err());
        assert!(workspace.join("target/new.bin").exists());
        fs::remove_file(workspace.join("target/new.bin")).unwrap();
        let reopened = LocalStorage::new(root.path().into(), root.path().join("fixture.db"));
        assert_eq!(
            reopened
                .inspect(workspace.to_str().unwrap())
                .unwrap()
                .fingerprint,
            Some(reviewed.clone())
        );
        reopened
            .clean(workspace.to_str().unwrap(), &reviewed)
            .unwrap();
        assert!(reopened.intent(&workspace).unwrap().unwrap().complete);
        assert!(
            reopened
                .intent(&workspace)
                .unwrap()
                .unwrap()
                .entries
                .is_empty()
        );
        assert!(!workspace.join("target").exists());
        assert_eq!(
            fs::read_to_string(workspace.join("keep.txt")).unwrap(),
            "retained"
        );
        fixture(&workspace);
        let next = reopened
            .inspect(workspace.to_str().unwrap())
            .unwrap()
            .fingerprint
            .unwrap();
        assert_ne!(next, reviewed);
        reopened.clean(workspace.to_str().unwrap(), &next).unwrap();
    }
    #[test]
    fn empty_cache_root_finishes_the_saved_receipt_after_the_last_unlink() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fixture(&workspace);
        let workspace = fs::canonicalize(workspace).unwrap();
        let target = workspace.join("target");
        let files = LocalStorage::new(root.path().into(), root.path().join("fixture.db"));
        let reviewed = files
            .inspect(workspace.to_str().unwrap())
            .unwrap()
            .fingerprint
            .unwrap();
        interrupt(&workspace);
        remove_cache(&target, &target, &mut 0).unwrap();
        assert!(target.exists());
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        assert_eq!(
            files
                .inspect(workspace.to_str().unwrap())
                .unwrap()
                .fingerprint,
            Some(reviewed.clone())
        );
        files.clean(workspace.to_str().unwrap(), &reviewed).unwrap();
        assert!(!target.exists());
        assert!(files.intent(&workspace).unwrap().unwrap().complete);
    }
}
