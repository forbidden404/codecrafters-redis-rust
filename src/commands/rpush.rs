use chrono::Utc;

use crate::{
    commands::{Command, CommandError},
    parser::{RedisReply, RedisValueRef},
    state::RedisEntry,
};

#[derive(Debug)]
pub struct RPushCommand {
    key: String,
    value: RedisValueRef,
}

impl RPushCommand {
    pub fn new(key: String, value: RedisValueRef) -> Self {
        RPushCommand { key, value }
    }
}

impl Command for RPushCommand {
    fn execute(&self, state: &mut crate::state::StateStore) -> Result<String, CommandError> {
        let mut count: i64 = 0;
        let status = match state.cache.get(&self.key) {
            Some(entry) => {
                if let Some(expiry_date) = entry.expiry_date
                    && expiry_date <= Utc::now()
                    && !matches!(entry.value, RedisValueRef::Array(_))
                {
                    false
                } else {
                    if let RedisValueRef::Array(mut existing) = entry.value.clone()
                        && let RedisValueRef::Array(new_values) = &self.value
                    {
                        existing.extend(new_values.iter().cloned());
                        count = existing.len() as i64;
                        state.cache.insert(
                            self.key.clone(),
                            RedisEntry::new(RedisValueRef::Array(existing), None, None),
                        );
                        true
                    } else {
                        false
                    }
                }
            }
            _ => {
                if let RedisValueRef::Array(values) = &self.value {
                    count = values.len() as i64;
                    state.cache.insert(
                        self.key.clone(),
                        RedisEntry::new(self.value.clone(), None, None),
                    );
                    true
                } else {
                    false
                }
            }
        };

        if status {
            Ok(RedisReply::Int(count).to_reply())
        } else {
            Ok(RedisReply::NullBulkString.to_reply())
        }
    }
}
