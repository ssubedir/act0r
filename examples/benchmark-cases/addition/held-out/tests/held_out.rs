#[test]
fn held_out() {
    assert_eq!(bench_addition::add(-2, 3), 1);
    assert_eq!(bench_addition::add(0, 4), 4);
    assert_eq!(bench_addition::add(-7, -5), -12);
}
