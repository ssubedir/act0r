#[test]
fn feedback() {
    assert_eq!(bench_clamp::clamp(5, 0, 10), 5);
}
