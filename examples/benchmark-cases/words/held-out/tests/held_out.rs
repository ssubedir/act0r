#[test]
fn held_out() {
    assert_eq!(bench_words::count_words(""), 0);
    assert_eq!(bench_words::count_words(" one\t two\nthree  "), 3);
    assert_eq!(bench_words::count_words("one\u{2003}two"), 2);
    assert_eq!(bench_words::count_words(" \t\n"), 0);
}
