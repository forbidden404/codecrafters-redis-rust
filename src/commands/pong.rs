use crate::{commands::Command, parser::RedisReply};

pub struct PongCommand;

impl Command for PongCommand {
    fn execute(&self, _state: &mut crate::state::StateStore) -> String {
        RedisReply::SimpleString("OK".to_string()).to_reply()
    }
}
