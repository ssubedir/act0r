use bench_retry_schedule::{backoff::delay, next_retry};

#[test]
fn exponential_delay_and_cap() {
    assert_eq!(delay(1, 100, 1_000), 100);
    assert_eq!(delay(3, 100, 1_000), 400);
    assert_eq!(delay(8, 100, 1_000), 1_000);
}

#[test]
fn deadline_uses_the_current_attempt_and_stops_at_the_limit() {
    assert_eq!(next_retry(1_000, 3, 5, 100, 1_000), Some(1_400));
    assert_eq!(next_retry(1_000, 5, 5, 100, 1_000), None);
}

#[test]
fn deadline_saturates_at_the_clock_limit() {
    assert_eq!(next_retry(u64::MAX - 10, 1, 3, 100, 1_000), Some(u64::MAX));
}
