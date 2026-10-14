use bench_retry_schedule::{backoff::delay, next_retry};

#[test]
fn held_out_large_attempts_do_not_overflow() {
    assert_eq!(delay(64, 1, u64::MAX), 1_u64 << 63);
    assert_eq!(delay(65, 1, u64::MAX), u64::MAX);
    assert_eq!(delay(u32::MAX, 2, 77), 77);
    assert_eq!(delay(2, u64::MAX, u64::MAX), u64::MAX);
}

#[test]
fn held_out_zero_delay_and_large_clock_values() {
    assert_eq!(next_retry(u64::MAX, 2, 3, 0, 500), Some(u64::MAX));
    assert_eq!(next_retry(u64::MAX - 5, 2, 4, 3, 100), Some(u64::MAX));
    assert_eq!(next_retry(37, 1, 1, 9, 100), None);
}

#[test]
fn held_out_non_power_of_two_caps_and_multiple_attempts() {
    for (attempt, expected) in [(1, 7), (2, 14), (3, 28), (4, 39), (32, 39)] {
        assert_eq!(next_retry(11, attempt, 40, 7, 39), Some(11 + expected));
    }
}
