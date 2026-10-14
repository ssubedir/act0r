use crate::backoff;

pub fn next_retry(
    now_ms: u64,
    attempt: u32,
    max_attempts: u32,
    base_ms: u64,
    max_delay_ms: u64,
) -> Option<u64> {
    if attempt >= max_attempts {
        return None;
    }
    let delay = backoff::delay(attempt, base_ms, max_delay_ms);
    Some(now_ms + delay)
}
