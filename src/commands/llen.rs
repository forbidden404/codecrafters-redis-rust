use crate::commands::CommandError;
use crate::server::state::StateStore;
use crate::{
    commands::Command,
    parser::{RedisReply, RedisValueRef},
};

pub struct LLenCommand {
    key: String,
}

impl LLenCommand {
    pub fn new(key: String) -> Self {
        LLenCommand { key }
    }
}

impl Command for LLenCommand {
    fn execute(&self, state: &mut StateStore) -> Result<String, CommandError> {
        let Some(entry) = state.cache.get(&self.key) else {
            return Ok(RedisReply::Int(0).to_reply());
        };
        let RedisValueRef::Array(list) = &entry.value else {
            return Err(CommandError::Abort);
        };

        Ok(RedisReply::Int(list.len() as i64).to_reply())
    }
}
