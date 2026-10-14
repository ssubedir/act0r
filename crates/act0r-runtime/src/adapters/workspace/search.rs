use super::*;
use crate::domain::tools::WorkspaceSearch;
use std::io::Read;

const MAX_BYTES: u64 = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 10_000;
const MAX_DEPTH: usize = 32;
const MAX_JSON: usize = 32_768;
const MAX_SNIPPET: usize = 512;

#[derive(Default, Serialize)]
struct Skipped {
    binary: usize,
    oversized: usize,
    unsupported: usize,
}
#[derive(Serialize)]
struct Hit {
    path: String,
    line: usize,
    snippet: String,
    snippet_truncated: bool,
}
#[derive(Serialize)]
struct SearchResult {
    ok: bool,
    matches: Vec<Hit>,
    files_considered: u32,
    files_scanned: u32,
    bytes_read: u64,
    skipped: Skipped,
    truncated: bool,
    limits_hit: Vec<&'static str>,
}
impl SearchResult {
    fn limit(&mut self, name: &'static str) {
        self.truncated = true;
        if !self.limits_hit.contains(&name) {
            self.limits_hit.push(name);
        }
    }
}
fn permitted(path: &str) -> bool {
    portable_file_path(path)
        && !path
            .split('/')
            .any(|p| excluded(p) || reserved_component(p))
}
fn checked(root: &Path, relative: &str) -> Result<(PathBuf, fs::Metadata), Error> {
    if !permitted(relative) {
        return Err(
            ToolError::new("path_refused", "search requires a permitted relative path").into(),
        );
    }
    let mut path = root.to_path_buf();
    for component in relative.split('/') {
        path.push(component);
        if link(&fs::symlink_metadata(&path).map_err(inspection_io)?) {
            return Err(ToolError::new(
                "path_refused",
                "symlinks and reparse points are not allowed",
            )
            .into());
        }
    }
    let canonical = fs::canonicalize(&path).map_err(inspection_io)?;
    if !canonical.starts_with(root) {
        return Err(ToolError::new(
            "path_refused",
            "search path is outside the managed workspace",
        )
        .into());
    }
    let metadata = fs::symlink_metadata(&canonical).map_err(inspection_io)?;
    Ok((canonical, metadata))
}
fn snippet(line: &str, position: usize) -> (String, bool) {
    let mut start = position.saturating_sub(128);
    while !line.is_char_boundary(start) {
        start += 1;
    }
    let mut end = (start + MAX_SNIPPET).min(line.len());
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    (line[start..end].into(), start > 0 || end < line.len())
}

impl LocalTools {
    pub(super) fn search_sync(
        &self,
        workspace: String,
        search: WorkspaceSearch,
    ) -> Result<Value, Error> {
        search.validate()?;
        let root = self.workspace(&workspace)?;
        let _guard = self.lock(&root)?;
        let mut result = SearchResult {
            ok: true,
            matches: vec![],
            files_considered: 0,
            files_scanned: 0,
            bytes_read: 0,
            skipped: Skipped::default(),
            truncated: false,
            limits_hit: vec![],
        };
        let max_files = search.max_files.unwrap_or(200);
        let max_matches = search.max_matches.unwrap_or(20) as usize;
        // Iterative traversal bounds depth and retained entries independently of file bytes.
        let mut pending = vec![search.path.unwrap_or_default()];
        let mut entries = 0;
        let mut hit_bytes = 0;
        'walk: while let Some(relative) = pending.pop() {
            let (path, metadata) = if relative.is_empty() {
                (root.clone(), fs::symlink_metadata(&root)?)
            } else {
                checked(&root, &relative)?
            };
            if link(&metadata) {
                return Err(ToolError::new(
                    "path_refused",
                    "symlinks and reparse points are not allowed",
                )
                .into());
            }
            if metadata.is_dir() {
                let depth = if relative.is_empty() {
                    0
                } else {
                    relative.split('/').count()
                };
                if depth >= MAX_DEPTH {
                    result.limit("depth");
                    continue;
                }
                let mut children = vec![];
                for entry in fs::read_dir(&path).map_err(inspection_io)? {
                    let entry = entry.map_err(inspection_io)?;
                    if entries == MAX_ENTRIES {
                        // Discard the incomplete directory, avoiding dependence on readdir order.
                        result.limit("entries");
                        break 'walk;
                    }
                    entries += 1;
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else {
                        result.skipped.unsupported += 1;
                        continue;
                    };
                    if excluded(name) {
                        continue;
                    }
                    let child = if relative.is_empty() {
                        name.into()
                    } else {
                        format!("{relative}/{name}")
                    };
                    if !permitted(&child) {
                        result.skipped.unsupported += 1;
                        continue;
                    }
                    children.push(child);
                }
                children.sort();
                pending.extend(children.into_iter().rev());
                continue;
            }
            if !metadata.is_file() {
                // Special files are never opened, including an explicitly requested one.
                result.skipped.unsupported += 1;
                continue;
            }
            if result.files_considered == max_files {
                result.limit("files");
                break;
            }
            result.files_considered += 1;
            if metadata.len() > MAX_FILE {
                result.skipped.oversized += 1;
                continue;
            }
            let remaining = MAX_BYTES - result.bytes_read;
            if remaining == 0 || metadata.len() > remaining {
                result.limit("bytes");
                break;
            }
            let file = File::open(&path).map_err(inspection_io)?;
            let mut bytes = Vec::new();
            (&file)
                .take((MAX_FILE + 1).min(remaining))
                .read_to_end(&mut bytes)
                .map_err(inspection_io)?;
            result.bytes_read += bytes.len() as u64;
            if bytes.len() as u64 > MAX_FILE {
                result.skipped.oversized += 1;
                continue;
            }
            if file.metadata().map_err(inspection_io)?.len() > bytes.len() as u64 {
                result.limit("bytes");
                break;
            }
            let text = match String::from_utf8(bytes) {
                Ok(text) if !text.contains('\0') => text,
                _ => {
                    result.skipped.binary += 1;
                    continue;
                }
            };
            result.files_scanned += 1;
            for (index, line) in text.lines().enumerate() {
                let Some(position) = line.find(search.query.as_str()) else {
                    continue;
                };
                if result.matches.len() == max_matches {
                    result.limit("matches");
                    break 'walk;
                }
                let (snippet, snippet_truncated) = snippet(line, position);
                let hit = Hit {
                    path: relative.clone(),
                    line: index + 1,
                    snippet,
                    snippet_truncated,
                };
                let size = serde_json::to_vec(&hit)?.len() + 1;
                // Reserve fixed metadata overhead, including every limit/skipped counter.
                if hit_bytes + size > MAX_JSON - 1024 {
                    result.limit("output");
                    break 'walk;
                }
                hit_bytes += size;
                result.matches.push(hit);
            }
        }
        Ok(serde_json::to_value(result)?)
    }
}
