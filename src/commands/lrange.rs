use std::cmp::min;

use bytes::Bytes;

use crate::{
    commands::{Command, CommandError},
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
    fn execute(&self, state: &mut crate::state::StateStore) -> String {
        let Some(entry) = state.cache.get(&self.key) else {
            return RedisReply::Array(vec![]).to_reply();
        };
        let RedisValueRef::Array(list) = &entry.value else {
            return RedisReply::Array(vec![]).to_reply();
        };

        if self.start >= list.len() as i64 {
            return RedisReply::Array(vec![]).to_reply();
        }

        let start = if self.start < 0 {
            (list.len() as i64 + self.start) as usize
        } else {
            (min(self.start, list.len() as i64)) as usize
        };

        let stop = if self.stop < 0 {
            (list.len() as i64 + self.stop) as usize
        } else {
            (min(self.stop, list.len() as i64)) as usize
        };

        if start > stop {
            return RedisReply::Array(vec![]).to_reply();
        }

        redis_value_arr_to_reply(list[start..=stop].to_vec())
            .unwrap_or(RedisReply::Array(vec![]))
            .to_reply()
    }
}

fn redis_value_arr_to_reply(arr: Vec<RedisValueRef>) -> Result<RedisReply, CommandError> {
    let replies = arr
        .into_iter()
        .map(|item| -> Result<RedisReply, CommandError> {
            match item {
                RedisValueRef::String(bytes) => Ok(RedisReply::BulkString(stringify(bytes)?)),
                RedisValueRef::Error(bytes) => Ok(RedisReply::Error(stringify(bytes)?)),
                RedisValueRef::Int(i) => Ok(RedisReply::Int(i)),
                RedisValueRef::Array(redis_value_refs) => {
                    redis_value_arr_to_reply(redis_value_refs)
                }
                RedisValueRef::NullArray => Ok(RedisReply::NullBulkString),
                RedisValueRef::NullBulkString => Ok(RedisReply::NullBulkString),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RedisReply::Array(replies))
}

fn stringify(bytes: Bytes) -> Result<String, CommandError> {
    str::from_utf8(&bytes)
        .map_err(|_| CommandError::Utf8ParseFailure)
        .map(|v| v.to_string())
}
