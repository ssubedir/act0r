/// Delay for a retry after a completed, one-based attempt.
pub fn delay(attempt: u32, base_ms: u64, max_delay_ms: u64) -> u64 {
    base_ms.min(max_delay_ms)
}
