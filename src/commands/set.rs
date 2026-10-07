use std::time::Duration;

use chrono::Utc;

use crate::{commands::Command, parser::RedisReply, state::RedisEntry};

#[derive(Debug)]
pub struct SetCommand {
    key: String,
    value: String,
    duration: Option<Duration>,
}

impl SetCommand {
    pub fn new(key: String, value: String) -> Self {
        SetCommand {
            key,
            value,
            duration: None,
        }
    }

    pub fn set_ex(&mut self, duration: u64) {
        self.duration = Some(Duration::new(duration, 0));
    }

    pub fn set_px(&mut self, duration: u64) {
        self.duration = Some(Duration::from_millis(duration));
    }
}

impl Command for SetCommand {
    fn execute(&self, state: &mut crate::state::StateStore) -> String {
        let status = match state.cache.get(&self.key) {
            Some(entry) => {
                if entry.is_unique {
                    false
                } else {
                    if let Some(expiry_date) = entry.expiry_date
                        && expiry_date <= Utc::now()
                    {
                        false
                    } else {
                        let expiry_date = self.duration.map(|duration| Utc::now() + duration);
                        state.cache.insert(
                            self.key.clone(),
                            RedisEntry::new(self.value.clone(), expiry_date, false),
                        );
                        true
                    }
                }
            }
            _ => {
                let expiry_date = self.duration.map(|duration| Utc::now() + duration);
                state.cache.insert(
                    self.key.clone(),
                    RedisEntry::new(self.value.clone(), expiry_date, false),
                );
                true
            }
        };

        if status {
            RedisReply::SimpleString("OK".to_string()).to_reply()
        } else {
            RedisReply::NullBulkString.to_reply()
        }
    }
}
