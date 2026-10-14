pub fn canonical_path(input: &str) -> String {
    format!("/{}", input.trim_matches('/'))
}
