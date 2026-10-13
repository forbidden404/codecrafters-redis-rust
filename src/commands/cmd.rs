use crate::{
    commands::{Command, CommandError},
    parser::RedisReply,
    server::state::StateStore,
};

pub struct CmdCommand;

impl CmdCommand {
    pub fn new(_v: String) -> Self {
        CmdCommand {}
    }
}

impl Command for CmdCommand {
    fn execute(&self, _state: &mut StateStore) -> Result<String, CommandError> {
        Ok(RedisReply::SimpleString("OK".to_string()).to_reply())
    }
}
