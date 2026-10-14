#[test]
fn held_out() {
    assert!(!bench_palindrome::is_palindrome("rust"));
    assert!(bench_palindrome::is_palindrome("été"));
    assert!(bench_palindrome::is_palindrome(""));
    assert!(!bench_palindrome::is_palindrome("Aa"));
}
