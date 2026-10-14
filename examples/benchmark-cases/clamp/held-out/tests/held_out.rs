#[test]
fn held_out() {
    assert_eq!(bench_clamp::clamp(20, 0, 10), 10);
    assert_eq!(bench_clamp::clamp(-5, 0, 10), 0);
    assert_eq!(bench_clamp::clamp(3, 3, 3), 3);
}
