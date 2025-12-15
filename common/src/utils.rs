use prost_types::Timestamp;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn system_time_to_timestamp(t: SystemTime) -> Timestamp {
    let dur = t.duration_since(UNIX_EPOCH).unwrap(); // Handle the error if needed by caller
    Timestamp {
        seconds: dur.as_secs() as i64,
        nanos: dur.subsec_nanos() as i32,
    }
}

pub fn timestamp_to_system_time(ts: &Timestamp) -> SystemTime {
    let dur = Duration::new(ts.seconds as u64, ts.nanos as u32);
    UNIX_EPOCH + dur
}
