use std::cmp::min;

use crate::{
    commands::{Command, CommandError, utils::redis_value_arr_to_reply},
    parser::{RedisReply, RedisValueRef},
};

#[derive(Debug)]
pub struct LRangeCommand {
    key: String,
    start: i64,
    stop: i64,
}

impl LRangeCommand {
    pub fn new(key: String, start: i64, stop: i64) -> Self {
        LRangeCommand { key, start, stop }
    }
}

impl Command for LRangeCommand {
    fn execute(&self, state: &mut crate::state::StateStore) -> Result<String, CommandError> {
        let Some(entry) = state.cache.get(&self.key) else {
            return Ok(RedisReply::Array(vec![]).to_reply());
        };
        let RedisValueRef::Array(list) = &entry.value else {
            return Ok(RedisReply::Array(vec![]).to_reply());
        };

        if self.start >= list.len() as i64 {
            return Ok(RedisReply::Array(vec![]).to_reply());
        }

        let start = if self.start < 0 {
            list.len() as i64 + self.start
        } else {
            min(self.start, list.len() as i64 - 1)
        };

        let stop = if self.stop < 0 {
            list.len() as i64 + self.stop
        } else {
            min(self.stop, list.len() as i64 - 1)
        };

        if start > stop {
            return Ok(RedisReply::Array(vec![]).to_reply());
        }

        let start = start.clamp(0, (list.len() as i64) - 1) as usize;
        let stop = stop.clamp(0, (list.len() as i64) - 1) as usize;

        Ok(
            redis_value_arr_to_reply(list.iter().skip(start).take(stop - start + 1).cloned())
                .unwrap_or(RedisReply::Array(vec![]))
                .to_reply(),
        )
    }
}

#[cfg(test)]
mod lrange_command_tests {
    use std::collections::VecDeque;

    use super::*;

    use crate::{commands::rpush::RPushCommand, state::StateStore};

    #[test]
    fn test_lrange_basics() {
        // Arrange
        let mut state = StateStore::new();
        let _ = RPushCommand::new(
            "mylist".to_string(),
            RedisValueRef::Array(VecDeque::from([
                RedisValueRef::Int(0),
                RedisValueRef::Int(1),
                RedisValueRef::Int(2),
                RedisValueRef::Int(3),
                RedisValueRef::Int(4),
                RedisValueRef::Int(5),
                RedisValueRef::Int(6),
                RedisValueRef::Int(7),
                RedisValueRef::Int(8),
                RedisValueRef::Int(9),
            ])),
        )
        .execute(&mut state)
        .expect("Failed to rpush list");

        // Act
        let first = LRangeCommand::new("mylist".to_string(), 1, -2)
            .execute(&mut state)
            .expect("Failed to get list range");
        let second = LRangeCommand::new("mylist".to_string(), -3, -1)
            .execute(&mut state)
            .expect("Failed to get list range");
        let third = LRangeCommand::new("mylist".to_string(), 4, 4)
            .execute(&mut state)
            .expect("Failed to get list range");

        // Assert
        assert_eq!(
            first,
            redis_value_arr_to_reply(vec![
                RedisValueRef::Int(1),
                RedisValueRef::Int(2),
                RedisValueRef::Int(3),
                RedisValueRef::Int(4),
                RedisValueRef::Int(5),
                RedisValueRef::Int(6),
                RedisValueRef::Int(7),
                RedisValueRef::Int(8),
            ])
            .unwrap()
            .to_reply()
        );

        assert_eq!(
            second,
            redis_value_arr_to_reply(vec![
                RedisValueRef::Int(7),
                RedisValueRef::Int(8),
                RedisValueRef::Int(9),
            ])
            .unwrap()
            .to_reply()
        );

        assert_eq!(
            third,
            redis_value_arr_to_reply(vec![RedisValueRef::Int(4),])
                .unwrap()
                .to_reply()
        );
    }

    #[test]
    fn test_lrange_inverted_indexes() {
        // Arrange
        let mut state = StateStore::new();
        let _ = RPushCommand::new(
            "mylist".to_string(),
            RedisValueRef::Array(VecDeque::from([
                RedisValueRef::Int(0),
                RedisValueRef::Int(1),
                RedisValueRef::Int(2),
                RedisValueRef::Int(3),
                RedisValueRef::Int(4),
                RedisValueRef::Int(5),
                RedisValueRef::Int(6),
                RedisValueRef::Int(7),
                RedisValueRef::Int(8),
                RedisValueRef::Int(9),
            ])),
        )
        .execute(&mut state)
        .expect("Failed to rpush list");

        // Act
        let value = LRangeCommand::new("mylist".to_string(), 6, 2)
            .execute(&mut state)
            .expect("Failed to get list range");

        // Assert
        assert_eq!(value, redis_value_arr_to_reply(vec![]).unwrap().to_reply());
    }

    #[test]
    fn test_lrange_out_of_range_indexes_include_the_full_list() {
        // Arrange
        let mut state = StateStore::new();
        let _ = RPushCommand::new(
            "mylist".to_string(),
            RedisValueRef::Array(VecDeque::from([
                RedisValueRef::Int(0),
                RedisValueRef::Int(1),
                RedisValueRef::Int(2),
                RedisValueRef::Int(3),
            ])),
        )
        .execute(&mut state)
        .expect("Failed to rpush list");

        // Act
        let value = LRangeCommand::new("mylist".to_string(), -1000, 1000)
            .execute(&mut state)
            .expect("Failed to get list range");

        // Assert
        assert_eq!(
            value,
            redis_value_arr_to_reply(vec![
                RedisValueRef::Int(0),
                RedisValueRef::Int(1),
                RedisValueRef::Int(2),
                RedisValueRef::Int(3),
            ])
            .unwrap()
            .to_reply()
        );
    }

    #[test]
    fn test_lrange_out_of_range_with_negative_end_index() {
        // Arrange
        let mut state = StateStore::new();
        let _ = RPushCommand::new(
            "mylist".to_string(),
            RedisValueRef::Array(VecDeque::from([
                RedisValueRef::Int(0),
                RedisValueRef::Int(1),
                RedisValueRef::Int(2),
                RedisValueRef::Int(3),
            ])),
        )
        .execute(&mut state)
        .expect("Failed to rpush list");

        // Act
        let first = LRangeCommand::new("mylist".to_string(), 0, -4)
            .execute(&mut state)
            .expect("Failed to get list range");

        let second = LRangeCommand::new("mylist".to_string(), 0, -5)
            .execute(&mut state)
            .expect("Failed to get list range");

        // Assert
        assert_eq!(
            first,
            redis_value_arr_to_reply(vec![RedisValueRef::Int(0),])
                .unwrap()
                .to_reply()
        );

        assert_eq!(second, redis_value_arr_to_reply(vec![]).unwrap().to_reply());
    }
}
