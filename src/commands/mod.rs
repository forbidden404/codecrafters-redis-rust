use bytes::Bytes;

use crate::{
    commands::{
        cmd::CmdCommand, echo::EchoCommand, get::GetCommand, lrange::LRangeCommand,
        ping::PingCommand, pong::PongCommand, rpush::RPushCommand, set::SetCommand,
    },
    parser::RedisValueRef,
    state::StateStore,
};

mod cmd;
mod echo;
mod get;
mod lrange;
mod ping;
mod pong;
mod rpush;
mod set;

pub trait Command {
    fn execute(&self, state: &mut StateStore) -> String;
}

pub fn try_from(value: RedisValueRef) -> Result<Box<dyn Command>, CommandError> {
    match value {
        RedisValueRef::String(bytes) => match &bytes[..] {
            b"PING" => Ok(Box::new(PingCommand)),
            b"PONG" => Ok(Box::new(PongCommand)),
            _ => Err(CommandError::WrongType),
        },
        RedisValueRef::Error(_) => Err(CommandError::WrongType),
        RedisValueRef::Int(_) => Err(CommandError::WrongType),
        RedisValueRef::Array(redis_value_refs) => check(redis_value_refs),
        RedisValueRef::NullArray => Err(CommandError::WrongType),
        RedisValueRef::NullBulkString => Err(CommandError::WrongType),
    }
}

#[derive(Debug)]
pub enum CommandError {
    UnknownCommand,
    WrongType,
    Utf8ParseFailure,
}

fn stringify(bytes: Bytes) -> Result<String, CommandError> {
    str::from_utf8(&bytes)
        .map_err(|_| CommandError::Utf8ParseFailure)
        .map(|v| v.to_string())
}

fn check(values: Vec<RedisValueRef>) -> Result<Box<dyn Command>, CommandError> {
    let name = values.first().ok_or(CommandError::WrongType)?;
    match name {
        RedisValueRef::String(bytes) => match &bytes[..] {
            b"PING" => Ok(Box::new(PingCommand {})),
            b"PONG" => Ok(Box::new(PongCommand {})),
            b"ECHO" => {
                let RedisValueRef::String(value) = values.get(1).ok_or(CommandError::WrongType)?
                else {
                    return Err(CommandError::WrongType);
                };

                Ok(Box::new(EchoCommand::new(stringify(value.clone())?)))
            }
            b"GET" => {
                let RedisValueRef::String(value) = values.get(1).ok_or(CommandError::WrongType)?
                else {
                    return Err(CommandError::WrongType);
                };

                Ok(Box::new(GetCommand::new(stringify(value.clone())?)))
            }
            b"COMMAND" => {
                let RedisValueRef::String(value) = values.get(1).ok_or(CommandError::WrongType)?
                else {
                    return Err(CommandError::WrongType);
                };

                Ok(Box::new(CmdCommand::new(stringify(value.clone())?)))
            }
            b"SET" => {
                let RedisValueRef::String(key) = values.get(1).ok_or(CommandError::WrongType)?
                else {
                    return Err(CommandError::WrongType);
                };
                let value = values.get(2).ok_or(CommandError::WrongType)?;
                let mut set_command = SetCommand::new(stringify(key.clone())?, value.clone());

                let opt_args = optional_args(values, 3);
                for (arg_k, arg_v) in opt_args {
                    if arg_k == "EX"
                        && let Ok(duration) = arg_v.parse::<u64>()
                    {
                        set_command.set_ex(duration);
                    } else if arg_k == "PX"
                        && let Ok(duration) = arg_v.parse::<u64>()
                    {
                        set_command.set_px(duration);
                    }
                }

                Ok(Box::new(set_command))
            }
            b"RPUSH" if values.len() > 2 => {
                let RedisValueRef::String(key) = values.get(1).ok_or(CommandError::WrongType)?
                else {
                    return Err(CommandError::WrongType);
                };

                let values = values.clone().into_iter().skip(2).collect();

                Ok(Box::new(RPushCommand::new(
                    stringify(key.clone())?,
                    RedisValueRef::Array(values),
                )))
            }
            b"LRANGE" if values.len() == 4 => {
                let RedisValueRef::String(key) = values.get(1).ok_or(CommandError::WrongType)?
                else {
                    return Err(CommandError::WrongType);
                };

                let RedisValueRef::String(start) = values.get(2).ok_or(CommandError::WrongType)?
                else {
                    return Err(CommandError::WrongType);
                };
                let Ok(start) = stringify(start.clone())?.parse::<i64>() else {
                    return Err(CommandError::WrongType);
                };

                let RedisValueRef::String(stop) = values.get(3).ok_or(CommandError::WrongType)?
                else {
                    return Err(CommandError::WrongType);
                };
                let Ok(stop) = stringify(stop.clone())?.parse::<i64>() else {
                    return Err(CommandError::WrongType);
                };

                Ok(Box::new(LRangeCommand::new(
                    stringify(key.clone())?,
                    start,
                    stop,
                )))
            }
            _ => Err(CommandError::UnknownCommand),
        },
        _ => Err(CommandError::WrongType),
    }
}

fn optional_args(list: Vec<RedisValueRef>, starting_index: usize) -> Vec<(String, String)> {
    if starting_index > list.len() {
        return vec![];
    }

    let mut pairs = Vec::new();

    let mut first = None;
    for item in list.iter().skip(starting_index) {
        let RedisValueRef::String(cur) = item else {
            continue;
        };

        let Ok(string) = stringify(cur.clone()) else {
            continue;
        };

        if let Some(fst) = first {
            pairs.push((fst, string));
            first = None;
        } else {
            first = Some(string);
        }
    }

    pairs
}
