use chrono::{DateTime, Utc};

pub trait Clock: Clone {
    fn now(&self) -> DateTime<Utc>;
    fn from_timestamp_secs(seconds: i64) -> Option<DateTime<Utc>>;
    fn from_timestamp_millis(millis: i64) -> Option<DateTime<Utc>>;
}

#[derive(Clone)]
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn from_timestamp_secs(seconds: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_secs(seconds)
    }

    fn from_timestamp_millis(millis: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(millis)
    }
}
