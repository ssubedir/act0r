//! Read-only project facts become editable suggestions, never execution authority.
use super::{
    DomainError, harness::HarnessCoding, project_profiles::ProjectSettings,
    tools::WorkspaceTestCommand,
};
use serde::{Deserialize, Serialize};

pub const MAX_MANIFEST_BYTES: u64 = 65536;
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetectionRequest {
    pub workspace: String,
}
impl DetectionRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        super::harness::validate_workspace(&self.workspace)
    }
}

/// Filesystem/parser facts supplied by a read-only adapter.
pub struct RustProjectFacts {
    pub workspace: String,
    pub manifest_sha256: Option<String>,
    pub package_name: Option<String>,
    pub workspace_layout: bool,
    pub source_paths: Vec<String>,
    pub lockfile: bool,
    pub notes: Vec<String>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectKind {
    Unrecognized,
    RustPackage,
    RustWorkspace,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectDetection {
    pub workspace: String,
    pub kind: ProjectKind,
    pub manifest_sha256: Option<String>,
    pub suggested: Option<ProjectSettings>,
    pub notes: Vec<String>,
}
impl ProjectDetection {
    pub fn from_facts(facts: RustProjectFacts) -> Result<Self, DomainError> {
        let mut result = Self {
            workspace: facts.workspace.clone(),
            kind: ProjectKind::Unrecognized,
            manifest_sha256: facts.manifest_sha256,
            suggested: None,
            notes: facts.notes,
        };
        if result.manifest_sha256.is_none() {
            result
                .notes
                .push("No root Cargo.toml found. Configure this project manually.".into());
            return Ok(result);
        }
        if facts.workspace_layout {
            result.kind = ProjectKind::RustWorkspace;
            result.notes.push("Cargo workspace detected. Automatic setup currently supports individual packages; configure this workspace manually.".into());
            return Ok(result);
        }
        let Some(name) = facts.package_name else {
            return Err(DomainError::InvalidConfiguration(
                "Cargo.toml needs a package or workspace table".into(),
            ));
        };
        if name.is_empty()
            || name.len() > 256
            || !name
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '-' | '_'))
        {
            return Err(DomainError::InvalidConfiguration(
                "Cargo package name must be a bounded name containing letters, numbers, - or _"
                    .into(),
            ));
        }
        result.kind = ProjectKind::RustPackage;
        if facts.source_paths.is_empty() {
            result.notes.push(
                "No supported source paths found. Enter writable paths and commands manually."
                    .into(),
            );
            return Ok(result);
        }
        let mut writable_paths = facts.source_paths;
        if !facts.lockfile {
            writable_paths.push("Cargo.lock".into());
            result.notes.push("No Cargo.lock found. Suggested scope includes Cargo.lock because Cargo may create it.".into());
        }
        let command = |kind: &str| {
            let mut argv = vec!["cargo".into(), kind.into()];
            if facts.lockfile {
                argv.push("--locked".into());
            }
            argv.push("--offline".into());
            WorkspaceTestCommand {
                argv,
                timeout_ms: 120000,
            }
        };
        let settings = ProjectSettings {
            name,
            workspace: facts.workspace,
            coding: Some(HarnessCoding {
                writable_paths,
                test_command: command("test"),
                coder_count: 1,
                project_commands: [
                    (
                        "format".into(),
                        WorkspaceTestCommand {
                            argv: vec!["cargo".into(), "fmt".into()],
                            timeout_ms: 60000,
                        },
                    ),
                    ("lint".into(), command("clippy")),
                    ("build".into(), command("build")),
                ]
                .into(),
            }),
        };
        settings.validate()?;
        result.suggested = Some(settings);
        result.notes.push(
            "Parent workspace configuration and dependencies are not resolved by detection.".into(),
        );
        result.notes.push("Review source scopes and commands before use. Offline commands require cached dependencies; rustfmt and Clippy must be installed.".into());
        Ok(result)
    }
}
