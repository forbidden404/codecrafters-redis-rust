use crate::{
    commands::{Command, CommandError},
    parser::RedisReply,
};

pub struct PingCommand {
    message: Option<String>,
}

impl PingCommand {
    pub fn new(message: Option<String>) -> Self {
        PingCommand { message }
    }
}

impl Command for PingCommand {
    fn execute(&self, _state: &mut crate::state::StateStore) -> Result<String, CommandError> {
        if let Some(message) = &self.message {
            Ok(RedisReply::BulkString(message.clone()).to_reply())
        } else {
            Ok(RedisReply::BulkString("PONG".to_string()).to_reply())
        }
    }
}

#[cfg(test)]
mod ping_command_tests {
    use crate::state::StateStore;

    use super::*;

    #[test]
    fn test_execute_with_no_message() {
        // Arrange
        let ping = PingCommand::new(None);
        let mut state = StateStore::new();

        // Act
        let reply = ping.execute(&mut state);

        // Assert
        assert_eq!(
            reply,
            Ok(RedisReply::BulkString("PONG".to_string()).to_reply())
        );
    }

    #[test]
    fn test_execute_with_message() {
        // Arrange
        let message = String::from("hello world");
        let ping = PingCommand::new(Some(message.clone()));
        let mut state = StateStore::new();

        // Act
        let reply = ping.execute(&mut state);

        // Assert
        assert_eq!(reply, Ok(RedisReply::BulkString(message).to_reply()));
    }
}
