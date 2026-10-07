use crate::{commands::Command, parser::RedisReply};

pub struct CmdCommand;

impl CmdCommand {
    pub fn new(_v: String) -> Self {
        CmdCommand {}
    }
}

impl Command for CmdCommand {
    fn execute(&self, _state: &mut crate::state::StateStore) -> String {
        RedisReply::SimpleString("OK".to_string()).to_reply()
    }
}
