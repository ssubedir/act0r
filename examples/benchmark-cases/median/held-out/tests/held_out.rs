#[test]
fn held_out() {
    assert_eq!(bench_median::median(&[9, 1, 4]), Some(4));
    assert_eq!(bench_median::median(&[8, 2]), Some(8));
    assert_eq!(bench_median::median(&[-3, -1, -2]), Some(-2));
    assert_eq!(bench_median::median(&[]), None);
}
