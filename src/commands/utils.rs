use bytes::Bytes;

use crate::{
    commands::CommandError,
    parser::{RedisReply, RedisValueRef},
};

impl TryFrom<RedisValueRef> for RedisReply {
    type Error = CommandError;

    fn try_from(value: RedisValueRef) -> Result<Self, CommandError> {
        match value {
            RedisValueRef::String(bytes) => Ok(RedisReply::BulkString(stringify(bytes)?)),
            RedisValueRef::Error(bytes) => Ok(RedisReply::Error(stringify(bytes)?)),
            RedisValueRef::Int(i) => Ok(RedisReply::Int(i)),
            RedisValueRef::Array(redis_value_refs) => redis_value_arr_to_reply(redis_value_refs),
            RedisValueRef::NullArray => Ok(RedisReply::NullBulkString),
            RedisValueRef::NullBulkString => Ok(RedisReply::NullBulkString),
        }
    }
}

pub fn redis_value_arr_to_reply(
    arr: impl IntoIterator<Item = RedisValueRef>,
) -> Result<RedisReply, CommandError> {
    let replies = arr
        .into_iter()
        .map(RedisReply::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RedisReply::Array(replies))
}

pub fn stringify(bytes: Bytes) -> Result<String, CommandError> {
    str::from_utf8(&bytes)
        .map_err(|_| CommandError::Utf8ParseFailure)
        .map(|v| v.to_string())
}
