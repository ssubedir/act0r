use crate::{
    application::{Error, ports::*},
    domain::{
        DomainError,
        project_detection::*,
        tools::{portable_file_path, validate_writable_paths},
    },
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{ErrorKind, Read},
    path::Path,
};

pub struct LocalProjectDetection;
fn invalid(message: &str) -> Error {
    DomainError::InvalidConfiguration(message.into()).into()
}
fn checked(path: &Path) -> Result<Option<fs::Metadata>, Error> {
    match fs::symlink_metadata(path) {
        Ok(meta) if super::workspace::link(&meta) => Err(invalid(
            "project detection refuses symlinks and reparse points",
        )),
        Ok(meta) => Ok(Some(meta)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn inspect(workspace: String) -> Result<RustProjectFacts, Error> {
    let path = Path::new(&workspace);
    for ancestor in path.ancestors() {
        checked(ancestor)?;
    }
    if checked(path)?.is_none_or(|m| !m.is_dir()) {
        return Err(invalid("project folder must exist and be a directory"));
    }
    let root = fs::canonicalize(path)?;
    let mut facts = RustProjectFacts {
        workspace: workspace.clone(),
        manifest_sha256: None,
        package_name: None,
        workspace_layout: false,
        source_paths: vec![],
        lockfile: false,
        notes: vec![],
    };
    let manifest = root.join("Cargo.toml");
    let Some(meta) = checked(&manifest)? else {
        return Ok(facts);
    };
    if !meta.is_file() || meta.len() > MAX_MANIFEST_BYTES {
        return Err(invalid(
            "Cargo.toml must be a regular file within 65536 bytes",
        ));
    }
    let mut file = fs::File::open(&manifest)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("Cargo.toml must be a regular file"));
    }
    let mut bytes = vec![];
    (&mut file)
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(invalid("Cargo.toml grew beyond the detection limit"));
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| invalid("Cargo.toml must be UTF-8"))?;
    let parsed: toml::Table =
        toml::from_str(text).map_err(|_| invalid("Cargo.toml is not valid TOML"))?;
    facts.manifest_sha256 = Some(format!("{:x}", Sha256::digest(&bytes)));
    let package = parsed.get("package").and_then(toml::Value::as_table);
    facts.package_name = package
        .and_then(|p| p.get("name"))
        .and_then(toml::Value::as_str)
        .map(str::to_owned);
    facts.workspace_layout = parsed.get("workspace").is_some_and(|w| {
        package.is_none()
            || w.get("members")
                .and_then(toml::Value::as_array)
                .is_some_and(|m| !m.is_empty())
    }) || package.is_some_and(|p| p.contains_key("workspace"));
    if facts.workspace_layout {
        return Ok(facts);
    }
    if let Some(lock) = checked(&root.join("Cargo.lock"))? {
        if !lock.is_file() {
            return Err(invalid("Cargo.lock must be a regular file when present"));
        }
        facts.lockfile = true;
    }
    if checked(&root.join("src"))?.is_some_and(|m| m.is_dir()) {
        facts.source_paths.push("src/".into());
    }
    let mut paths = vec![];
    if let Some(path) = parsed.get("lib").and_then(|lib| lib.get("path")) {
        paths.push(path);
    }
    if let Some(bins) = parsed.get("bin") {
        let bins = bins
            .as_array()
            .ok_or_else(|| invalid("Cargo bin targets must be an array"))?;
        if bins.len() > 128 {
            return Err(invalid("too many Cargo targets for automatic setup"));
        }
        paths.extend(bins.iter().filter_map(|bin| bin.get("path")));
    }
    for value in paths {
        let value = value
            .as_str()
            .ok_or_else(|| invalid("Cargo target paths must be strings"))?;
        let value = value.strip_prefix("./").unwrap_or(value);
        if !portable_file_path(value)
            || value.split('/').any(super::workspace::excluded)
            || value == "Cargo.toml"
            || value == "Cargo.lock"
        {
            return Err(invalid(
                "Cargo source paths must be portable files inside the project",
            ));
        }
        let mut target = root.clone();
        for part in value.split('/') {
            target.push(part);
            checked(&target)?;
        }
        if checked(&target)?.is_some_and(|m| m.is_file()) {
            if !fs::canonicalize(&target)?.starts_with(&root) {
                return Err(invalid("Cargo target escapes the project"));
            }
            if !value.starts_with("src/") && !facts.source_paths.iter().any(|p| p == value) {
                facts.source_paths.push(value.into());
            }
        } else if facts.notes.len() < 8 {
            facts
                .notes
                .push("A declared source file was not found; review its path manually.".into());
        }
    }
    validate_writable_paths(&facts.source_paths).map_err(Error::from)?;
    Ok(facts)
}
impl ProjectDetectionPort for LocalProjectDetection {
    fn inspect(&self, workspace: String) -> PortFuture<'_, RustProjectFacts> {
        Box::pin(async move {
            tokio::task::spawn_blocking(move || inspect(workspace))
                .await
                .map_err(|_| Error::Unavailable("project detection task failed".into()))?
        })
    }
}
