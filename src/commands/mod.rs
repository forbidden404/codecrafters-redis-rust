use bytes::Bytes;

use crate::parser::RedisValueRef;

pub enum Command {
    Ping,
    Pong,
    Cmd(String),
    Echo(String),
    Set(String, String),
    Get(String),
}

#[derive(Debug)]
pub enum CommandError {
    UnknownCommand,
    WrongType,
    Utf8ParseFailure,
}

impl Command {
    fn stringify(bytes: Bytes) -> Result<String, CommandError> {
        str::from_utf8(&bytes)
            .map_err(|_| CommandError::Utf8ParseFailure)
            .map(|v| v.to_string())
    }
    fn check_one(values: Vec<RedisValueRef>) -> Result<Self, CommandError> {
        let value = values.first().ok_or(CommandError::WrongType)?;
        match value {
            RedisValueRef::String(bytes) => match &bytes[..] {
                b"PING" => Ok(Command::Ping),
                b"PONG" => Ok(Command::Pong),
                _ => Err(CommandError::UnknownCommand),
            },
            _ => Err(CommandError::WrongType),
        }
    }

    fn check_two(values: Vec<RedisValueRef>) -> Result<Self, CommandError> {
        let value1 = values.first().ok_or(CommandError::WrongType)?;
        let value2 = values.get(1).ok_or(CommandError::WrongType)?;
        match (value1, value2) {
            (RedisValueRef::String(cmd), RedisValueRef::String(arg)) => match &cmd[..] {
                b"ECHO" => Ok(Command::Echo(Self::stringify(arg.clone())?)),
                b"COMMAND" => Ok(Command::Cmd(Self::stringify(arg.clone())?)),
                b"GET" => Ok(Command::Get(Self::stringify(arg.clone())?)),
                _ => Err(CommandError::UnknownCommand),
            },
            _ => Err(CommandError::WrongType),
        }
    }

    fn check_three(values: Vec<RedisValueRef>) -> Result<Self, CommandError> {
        let value1 = values.first().ok_or(CommandError::WrongType)?;
        let value2 = values.get(1).ok_or(CommandError::WrongType)?;
        let value3 = values.get(2).ok_or(CommandError::WrongType)?;
        match (value1, value2, value3) {
            (
                RedisValueRef::String(cmd),
                RedisValueRef::String(arg1),
                RedisValueRef::String(arg2),
            ) => match &cmd[..] {
                b"SET" => Ok(Command::Set(
                    Self::stringify(arg1.clone())?,
                    Self::stringify(arg2.clone())?,
                )),
                _ => Err(CommandError::UnknownCommand),
            },
            _ => Err(CommandError::WrongType),
        }
    }
}

impl TryFrom<RedisValueRef> for Command {
    type Error = CommandError;

    fn try_from(value: RedisValueRef) -> Result<Self, Self::Error> {
        match value {
            RedisValueRef::String(bytes) => match &bytes[..] {
                b"PING" => Ok(Command::Ping),
                b"PONG" => Ok(Command::Pong),
                _ => Err(CommandError::WrongType),
            },
            RedisValueRef::Error(_) => Err(CommandError::WrongType),
            RedisValueRef::Int(_) => Err(CommandError::WrongType),
            RedisValueRef::Array(redis_value_refs) => {
                let count = redis_value_refs.len();
                match count {
                    1 => Self::check_one(redis_value_refs),
                    2 => Self::check_two(redis_value_refs),
                    3 => Self::check_three(redis_value_refs),
                    _ => Err(CommandError::WrongType),
                }
            }
            RedisValueRef::NullArray => Err(CommandError::WrongType),
            RedisValueRef::NullBulkString => Err(CommandError::WrongType),
        }
    }
}
