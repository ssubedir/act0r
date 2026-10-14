use crate::application::ports::Clock;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
            Err(error) => -(error.duration().as_millis().min(i64::MAX as u128) as i64),
        }
    }
}
