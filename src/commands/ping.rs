use crate::{commands::Command, parser::RedisReply};

pub struct PingCommand;

impl Command for PingCommand {
    fn execute(&self, _state: &mut crate::state::StateStore) -> String {
        RedisReply::SimpleString("PONG".to_string()).to_reply()
    }
}
