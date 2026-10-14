use crate::normalize::canonical_path;

pub fn matches_route(prefix: &str, path: &str) -> bool {
    let prefix = canonical_path(prefix);
    let path = canonical_path(path);
    path.starts_with(&prefix)
}
