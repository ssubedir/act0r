use bench_path_routing::{canonical_path, matches_route};

#[test]
fn held_out_unicode_and_literal_segments() {
    assert_eq!(canonical_path("//Å///猫//"), "/Å/猫");
    assert!(matches_route("/Å", "//Å///猫"));
    assert!(!matches_route("/å", "/Å/猫"));
    assert_eq!(canonical_path("/a/../b"), "/a/../b");
}

#[test]
fn held_out_nested_boundaries_and_empty_root() {
    assert!(!matches_route("/api/v1", "/api/v10/users"));
    assert!(matches_route("///", ""));
    assert!(matches_route("", "/nested/path"));
    assert!(!matches_route("/nested", "/"));
    assert!(matches_route("/nested/", "//nested////child"));
}

#[test]
fn held_out_separator_runs_of_different_lengths() {
    for count in 1..=9 {
        let path = format!(
            "{}a{}b{}",
            "/".repeat(count),
            "/".repeat(count),
            "/".repeat(count)
        );
        assert_eq!(canonical_path(&path), "/a/b");
    }
}
