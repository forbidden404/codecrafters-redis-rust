use chrono::Utc;

use crate::{
    commands::{Command, CommandError},
    parser::{RedisReply, RedisValueRef},
    state::RedisEntry,
};

#[derive(Debug)]
pub struct LPushCommand {
    key: String,
    value: RedisValueRef,
}

impl LPushCommand {
    pub fn new(key: String, value: RedisValueRef) -> Self {
        LPushCommand { key, value }
    }
}

impl Command for LPushCommand {
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
                    if let RedisValueRef::Array(existing) = entry.value.clone()
                        && let RedisValueRef::Array(mut new_values) = self.value.clone()
                    {
                        new_values.reverse();
                        new_values.extend_from_slice(&existing);
                        count = new_values.len() as i64;
                        state.cache.insert(
                            self.key.clone(),
                            RedisEntry::new(RedisValueRef::Array(new_values.to_vec()), None, None),
                        );
                        true
                    } else {
                        false
                    }
                }
            }
            _ => {
                if let RedisValueRef::Array(mut values) = self.value.clone() {
                    values.reverse();
                    count = values.len() as i64;
                    state.cache.insert(
                        self.key.clone(),
                        RedisEntry::new(RedisValueRef::Array(values.to_vec()), None, None),
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

#[cfg(test)]
mod rpush_command_tests {
    use super::*;

    use crate::commands::{LRangeCommand, utils::redis_value_arr_to_reply};
    use crate::state::StateStore;

    #[test]
    fn test_lpush_prepends() {
        // Arrange
        let mut state = StateStore::new();
        let _ = LPushCommand::new(
            "mylist".to_string(),
            RedisValueRef::Array(vec![
                RedisValueRef::Int(0),
                RedisValueRef::Int(1),
                RedisValueRef::Int(2),
            ]),
        )
        .execute(&mut state)
        .expect("Failed to rpush list");

        // Act
        let value = LRangeCommand::new("mylist".to_string(), 0, -1)
            .execute(&mut state)
            .expect("Failed to get list range");

        // Assert
        assert_eq!(
            value,
            redis_value_arr_to_reply(vec![
                RedisValueRef::Int(2),
                RedisValueRef::Int(1),
                RedisValueRef::Int(0),
            ])
            .unwrap()
            .to_reply()
        );
    }
}
