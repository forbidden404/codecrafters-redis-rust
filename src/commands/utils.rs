use bytes::Bytes;

use crate::{
    commands::CommandError,
    parser::{RedisReply, RedisValueRef},
};

pub fn redis_value_arr_to_reply(arr: Vec<RedisValueRef>) -> Result<RedisReply, CommandError> {
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

pub fn stringify(bytes: Bytes) -> Result<String, CommandError> {
    str::from_utf8(&bytes)
        .map_err(|_| CommandError::Utf8ParseFailure)
        .map(|v| v.to_string())
}
