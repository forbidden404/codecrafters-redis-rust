use crate::{commands::Command, parser::RedisReply};

pub struct EchoCommand {
    value: String,
}

impl EchoCommand {
    pub fn new(value: String) -> Self {
        EchoCommand { value }
    }
}

impl Command for EchoCommand {
    fn execute(&self, _state: &mut crate::state::StateStore) -> String {
        RedisReply::BulkString(self.value.to_string()).to_reply()
    }
}
