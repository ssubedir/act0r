use bench_path_routing::{canonical_path, matches_route};

#[test]
fn repeated_separators_preserve_all_segments_and_case() {
    assert_eq!(canonical_path("///Api////v1//"), "/Api/v1");
    assert_eq!(canonical_path(""), "/");
}

#[test]
fn route_prefix_must_end_at_a_segment_boundary() {
    assert!(matches_route("/api", "/api"));
    assert!(matches_route("/api", "/api/users"));
    assert!(!matches_route("/api", "/apiary"));
}

#[test]
fn normalization_is_used_by_matching() {
    assert!(matches_route("//Api///v1", "/Api/v1///users/"));
    assert!(!matches_route("/Api", "/api/users"));
    assert!(matches_route("/", "/anything"));
}
