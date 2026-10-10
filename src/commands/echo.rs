use crate::{
    commands::{Command, CommandError},
    parser::RedisReply,
};

pub struct EchoCommand {
    value: String,
}

impl EchoCommand {
    pub fn new(value: String) -> Self {
        EchoCommand { value }
    }
}

impl Command for EchoCommand {
    fn execute(&self, _state: &mut crate::state::StateStore) -> Result<String, CommandError> {
        Ok(RedisReply::BulkString(self.value.to_string()).to_reply())
    }
}

#[cfg(test)]
mod echo_command_tests {
    use super::*;
    use crate::state::StateStore;

    #[test]
    fn test_echo_sends_message() {
        // Arrange
        let mut state = StateStore::new();
        let echo_command = EchoCommand::new("Hello World!".to_string());

        // Act
        let reply = echo_command.execute(&mut state);

        // Assert
        assert_eq!(
            reply,
            Ok(RedisReply::BulkString("Hello World!".to_string()).to_reply())
        );
    }
}
