use std::collections::VecDeque;
use std::collections::hash_map::Entry;

use crate::commands::CommandError;
use crate::commands::utils::redis_value_arr_to_reply;
use crate::{
    commands::Command,
    parser::{RedisReply, RedisValueRef},
};

pub struct LPopCommand {
    key: String,
    quantity: Option<usize>,
}

impl LPopCommand {
    pub fn new(key: String, quantity: Option<usize>) -> Self {
        LPopCommand { key, quantity }
    }
}

impl Command for LPopCommand {
    fn execute(&self, state: &mut crate::state::StateStore) -> Result<String, CommandError> {
        let entry = state.cache.entry(self.key.clone());
        match entry {
            Entry::Occupied(mut entry) => match &mut entry.get_mut().value {
                RedisValueRef::Array(list) => {
                    if let Some(quantity) = self.quantity {
                        let front: VecDeque<RedisValueRef> = list.drain(..quantity).collect();
                        Ok(redis_value_arr_to_reply(front)?.to_reply())
                    } else {
                        if let Some(el) = list.pop_front() {
                            Ok(RedisReply::try_from(el)?.to_reply())
                        } else {
                            Ok(RedisReply::NullBulkString.to_reply())
                        }
                    }
                }
                _ => Err(CommandError::WrongType),
            },
            Entry::Vacant(_) => Ok(RedisReply::NullBulkString.to_reply()),
        }
    }
}

#[cfg(test)]
mod lpop_command_tests {
    use bytes::Bytes;

    use super::*;

    use crate::{commands::rpush::RPushCommand, state::StateStore};

    #[test]
    fn test_lpop_returns_value_when_list_is_populated() {
        // Arrange
        let mut state = StateStore::new();

        let count = RPushCommand::new(
            "list_key".to_string(),
            RedisValueRef::Array(VecDeque::from([
                RedisValueRef::String(Bytes::from_static(b"a")),
                RedisValueRef::String(Bytes::from_static(b"b")),
                RedisValueRef::String(Bytes::from_static(b"c")),
                RedisValueRef::String(Bytes::from_static(b"d")),
            ])),
        )
        .execute(&mut state)
        .expect("Failed to rpush");

        // Act
        let value = LPopCommand::new("list_key".to_string(), None)
            .execute(&mut state)
            .expect("Failed to lpop");

        // Assert
        assert_eq!(count, RedisReply::Int(4).to_reply());
        assert_eq!(value, RedisReply::BulkString("a".to_string()).to_reply())
    }

    #[test]
    fn test_lpop_returns_values_when_list_is_populated() {
        // Arrange
        let mut state = StateStore::new();

        let count = RPushCommand::new(
            "list_key".to_string(),
            RedisValueRef::Array(VecDeque::from([
                RedisValueRef::String(Bytes::from_static(b"a")),
                RedisValueRef::String(Bytes::from_static(b"b")),
                RedisValueRef::String(Bytes::from_static(b"c")),
                RedisValueRef::String(Bytes::from_static(b"d")),
                RedisValueRef::String(Bytes::from_static(b"e")),
                RedisValueRef::String(Bytes::from_static(b"f")),
            ])),
        )
        .execute(&mut state)
        .expect("Failed to rpush");

        // Act
        let value = LPopCommand::new("list_key".to_string(), Some(2))
            .execute(&mut state)
            .expect("Failed to lpop");

        // Assert
        assert_eq!(count, RedisReply::Int(6).to_reply());
        assert_eq!(
            value,
            RedisReply::Array(vec![
                RedisReply::BulkString("a".to_string()),
                RedisReply::BulkString("b".to_string()),
            ])
            .to_reply()
        )
    }

    #[test]
    fn test_lpop_returns_null_when_list_is_empty() {
        // Arrange
        let mut state = StateStore::new();

        let count = RPushCommand::new(
            "list_key".to_string(),
            RedisValueRef::Array(VecDeque::from([])),
        )
        .execute(&mut state)
        .expect("Failed to rpush");

        // Act
        let value = LPopCommand::new("list_key".to_string(), None)
            .execute(&mut state)
            .expect("Failed to lpop");

        // Assert
        assert_eq!(count, RedisReply::Int(0).to_reply());
        assert_eq!(value, RedisReply::NullBulkString.to_reply())
    }
}
