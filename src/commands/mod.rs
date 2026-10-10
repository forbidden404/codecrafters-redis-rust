use std::collections::VecDeque;

use crate::{
    clock::SystemClock,
    commands::{
        cmd::CmdCommand,
        echo::EchoCommand,
        get::GetCommand,
        llen::LLenCommand,
        lpop::LPopCommand,
        lpush::LPushCommand,
        lrange::LRangeCommand,
        ping::PingCommand,
        rpop::RPopCommand,
        rpush::RPushCommand,
        set::{ExpiryCondition, SetCommand, SetCondition},
        utils::stringify,
    },
    parser::RedisValueRef,
    state::StateStore,
};

mod cmd;
mod echo;
mod get;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod ping;
mod rpop;
mod rpush;
mod set;
mod utils;

pub trait Command {
    fn execute(&self, state: &mut StateStore) -> Result<String, CommandError>;
}

pub fn try_from(value: RedisValueRef) -> Result<Box<dyn Command>, CommandError> {
    match value {
        RedisValueRef::String(bytes) => match &bytes[..] {
            b"PING" => Ok(Box::new(PingCommand::new(None))),
            _ => Err(CommandError::WrongType),
        },
        RedisValueRef::Error(_) => Err(CommandError::WrongType),
        RedisValueRef::Int(_) => Err(CommandError::WrongType),
        RedisValueRef::Array(redis_value_refs) => check(redis_value_refs),
        RedisValueRef::NullArray => Err(CommandError::WrongType),
        RedisValueRef::NullBulkString => Err(CommandError::WrongType),
    }
}

#[derive(Debug, PartialEq)]
pub enum CommandError {
    UnknownCommand,
    WrongType,
    Utf8ParseFailure,
    Abort,
}

fn get_string_at_index(
    values: &VecDeque<RedisValueRef>,
    index: usize,
) -> Result<String, CommandError> {
    let RedisValueRef::String(value) = values.get(index).ok_or(CommandError::WrongType)? else {
        return Err(CommandError::WrongType);
    };

    stringify(value.clone())
}

fn check(values: VecDeque<RedisValueRef>) -> Result<Box<dyn Command>, CommandError> {
    let name = values.front().ok_or(CommandError::WrongType)?;
    match name {
        RedisValueRef::String(bytes) => match &bytes[..] {
            b"PING" => {
                let message = get_string_at_index(&values, 1).ok();
                Ok(Box::new(PingCommand::new(message)))
            }
            b"ECHO" => {
                let message = get_string_at_index(&values, 1)?;
                Ok(Box::new(EchoCommand::new(message)))
            }
            b"GET" => {
                let key = get_string_at_index(&values, 1)?;
                Ok(Box::new(GetCommand::new(key, SystemClock)))
            }
            b"LLEN" => {
                let key = get_string_at_index(&values, 1)?;
                Ok(Box::new(LLenCommand::new(key)))
            }
            b"LPOP" => {
                let key = get_string_at_index(&values, 1)?;
                let quantity = get_string_at_index(&values, 2)
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok());
                Ok(Box::new(LPopCommand::new(key, quantity)))
            }
            b"RPOP" => {
                let key = get_string_at_index(&values, 1)?;
                let quantity = get_string_at_index(&values, 2)
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok());
                Ok(Box::new(RPopCommand::new(key, quantity)))
            }
            b"COMMAND" => {
                let value = get_string_at_index(&values, 1)?;
                Ok(Box::new(CmdCommand::new(value)))
            }
            b"SET" => {
                let key = get_string_at_index(&values, 1)?;
                let value = values.get(2).ok_or(CommandError::WrongType)?.clone();
                let (expiry, condition, should_get) = parse_extended_string_arguments(values, 3);
                Ok(Box::new(SetCommand::new(
                    key,
                    value,
                    SystemClock,
                    expiry,
                    condition,
                    should_get,
                )))
            }
            b"RPUSH" if values.len() > 2 => {
                let key = get_string_at_index(&values, 1)?;

                let values = values.clone().into_iter().skip(2).collect();

                Ok(Box::new(RPushCommand::new(
                    key,
                    RedisValueRef::Array(values),
                )))
            }
            b"LPUSH" if values.len() > 2 => {
                let key = get_string_at_index(&values, 1)?;

                let values = values.clone().into_iter().skip(2).collect();

                Ok(Box::new(LPushCommand::new(
                    key,
                    RedisValueRef::Array(values),
                )))
            }
            b"LRANGE" if values.len() == 4 => {
                let key = get_string_at_index(&values, 1)?;
                let start = get_string_at_index(&values, 2)?;
                let stop = get_string_at_index(&values, 3)?;

                let Ok(start) = start.parse::<i64>() else {
                    return Err(CommandError::WrongType);
                };

                let Ok(stop) = stop.parse::<i64>() else {
                    return Err(CommandError::WrongType);
                };

                Ok(Box::new(LRangeCommand::new(key, start, stop)))
            }
            _ => Err(CommandError::UnknownCommand),
        },
        _ => Err(CommandError::WrongType),
    }
}

fn parse_extended_string_arguments(
    list: VecDeque<RedisValueRef>,
    start_pos: usize,
) -> (Option<ExpiryCondition>, Option<SetCondition>, bool) {
    if start_pos > list.len() {
        return (None, None, false);
    }

    let mut expiry_condition: Option<ExpiryCondition> = None;
    let mut condition: Option<SetCondition> = None;
    let mut should_get: bool = false;

    let mut iter = list.iter().peekable();

    while let Some(item) = iter.next() {
        let RedisValueRef::String(value) = item else {
            continue;
        };

        let next = iter.peek();

        // Only use this when you know next is some
        let proc_next = |next: Option<&&RedisValueRef>| -> String {
            let Some(next) = next else {
                return "".to_string();
            };

            let RedisValueRef::String(value) = next else {
                return "".to_string();
            };

            str::from_utf8(value).unwrap_or("").to_string()
        };

        match &value[..] {
            b"NX" | b"nx" if condition.is_none() => {
                condition = Some(SetCondition::Nx);
            }
            b"XX" | b"xx" if condition.is_none() => {
                condition = Some(SetCondition::Xx);
            }
            b"IFEQ" | b"ifeq" if condition.is_none() && next.is_some() => {
                condition = Some(SetCondition::Ifeq(proc_next(next)));
            }
            b"IFDEQ" | b"ifdeq" if condition.is_none() && next.is_some() => {
                condition = Some(SetCondition::Ifdeq(proc_next(next)));
            }
            b"IFNE" | b"ifne" if condition.is_none() && next.is_some() => {
                condition = Some(SetCondition::Ifne(proc_next(next)));
            }
            b"IFDNE" | b"ifdne" if condition.is_none() && next.is_some() => {
                condition = Some(SetCondition::Ifdne(proc_next(next)));
            }
            b"GET" | b"get" => {
                should_get = true;
            }
            b"KEEPTTL" | b"keepttl" if expiry_condition.is_none() => {
                expiry_condition = Some(ExpiryCondition::KeepTTL);
            }
            b"EX" | b"ex" if expiry_condition.is_none() && next.is_some() => {
                expiry_condition = proc_next(next).parse::<i64>().ok().map(ExpiryCondition::Ex);
            }
            b"PX" | b"px" if expiry_condition.is_none() && next.is_some() => {
                expiry_condition = proc_next(next).parse::<i64>().ok().map(ExpiryCondition::Px);
            }
            b"EXAT" | b"exat" if expiry_condition.is_none() && next.is_some() => {
                expiry_condition = proc_next(next)
                    .parse::<i64>()
                    .ok()
                    .map(ExpiryCondition::Exat);
            }
            b"PXAT" | b"pxat" if expiry_condition.is_none() && next.is_some() => {
                expiry_condition = proc_next(next)
                    .parse::<i64>()
                    .ok()
                    .map(ExpiryCondition::Pxat);
            }
            _ => {}
        }
    }

    (expiry_condition, condition, should_get)
}
