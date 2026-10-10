use crate::clock::Clock;

use crate::commands::CommandError;
use crate::{
    commands::Command,
    parser::{RedisReply, RedisValueRef},
};

pub struct GetCommand<C: Clock> {
    key: String,
    clock: C,
}

impl<C: Clock> GetCommand<C> {
    pub fn new(key: String, clock: C) -> Self {
        GetCommand { key, clock }
    }
}

impl<C: Clock> Command for GetCommand<C> {
    fn execute(&self, state: &mut crate::state::StateStore) -> Result<String, CommandError> {
        let entry = state.cache.get_mut(&self.key);

        let Some(entry) = entry else {
            return Ok(RedisReply::NullBulkString.to_reply());
        };

        if entry.expiry_date.is_some_and(|ex| ex <= self.clock.now()) {
            return Ok(RedisReply::NullBulkString.to_reply());
        }

        let RedisValueRef::String(value) = &entry.value else {
            return Err(CommandError::Abort);
        };

        let Ok(value) = str::from_utf8(value) else {
            return Err(CommandError::Abort);
        };

        entry.last_access_date = Some(self.clock.now());
        Ok(RedisReply::BulkString(value.to_string()).to_reply())
    }
}

#[cfg(test)]
mod get_command_tests {
    use std::cell::RefCell;

    use bytes::Bytes;
    use chrono::{DateTime, Duration, Utc};

    use crate::commands::SetCommand;
    use crate::commands::rpush::RPushCommand;
    use crate::commands::set::ExpiryCondition;
    use crate::state::StateStore;

    use super::*;

    #[derive(Clone)]
    struct MockClock {
        current_time: RefCell<DateTime<Utc>>,
    }

    impl MockClock {
        fn new() -> Self {
            MockClock {
                current_time: RefCell::new(Utc::now()),
            }
        }
        fn advance(&self, duration: Duration) {
            *self.current_time.borrow_mut() += duration;
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> DateTime<Utc> {
            *self.current_time.borrow()
        }

        fn from_timestamp_secs(seconds: i64) -> Option<DateTime<Utc>> {
            DateTime::from_timestamp_secs(seconds)
        }

        fn from_timestamp_millis(millis: i64) -> Option<DateTime<Utc>> {
            DateTime::from_timestamp_millis(millis)
        }
    }

    impl Clock for std::rc::Rc<MockClock> {
        fn now(&self) -> DateTime<Utc> {
            *self.current_time.borrow()
        }

        fn from_timestamp_secs(seconds: i64) -> Option<DateTime<Utc>> {
            DateTime::from_timestamp_secs(seconds)
        }

        fn from_timestamp_millis(millis: i64) -> Option<DateTime<Utc>> {
            DateTime::from_timestamp_millis(millis)
        }
    }

    #[test]
    fn test_get_returns_a_string_if_it_exists() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());
        let set_command = SetCommand::new(
            "key".to_string(),
            RedisValueRef::String(Bytes::from_static(b"value")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        );

        clock.advance(Duration::new(-2, 0).unwrap());
        let get_command = GetCommand::new("key".to_string(), clock);

        let set_reply = set_command.execute(&mut state);
        assert!(set_reply.is_ok());

        // Act
        let reply = get_command.execute(&mut state);

        // Assert
        assert_eq!(
            reply,
            Ok(RedisReply::BulkString("value".to_string()).to_reply())
        );
    }

    #[test]
    fn test_get_returns_nil_if_it_does_not_exist() {
        // Arrange
        let mut state = StateStore::new();
        let clock = MockClock::new();
        let get_command = GetCommand::new("key".to_string(), clock);

        // Act
        let reply = get_command.execute(&mut state);

        // Assert
        assert_eq!(reply, Ok(RedisReply::NullBulkString.to_reply()));
    }

    #[test]
    fn test_get_returns_nil_if_value_is_not_string() {
        // Arrange
        let mut state = StateStore::new();
        let clock = MockClock::new();
        let rpush_command = RPushCommand::new(
            "key".to_string(),
            RedisValueRef::String(Bytes::from_static(b"value")),
        );
        let get_command = GetCommand::new("key".to_string(), clock);

        let rpush_reply = rpush_command.execute(&mut state);
        assert!(rpush_reply.is_ok());

        // Act
        let reply = get_command.execute(&mut state);

        // Assert
        assert_eq!(reply, Ok(RedisReply::NullBulkString.to_reply()));
    }

    #[test]
    fn test_get_returns_nil_if_value_is_expired() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());
        let set_command = SetCommand::new(
            "key".to_string(),
            RedisValueRef::String(Bytes::from_static(b"value")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Ex(10)),
            None,
            false,
        );
        let set_reply = set_command.execute(&mut state);
        assert!(set_reply.is_ok());

        let get_command = GetCommand::new("key".to_string(), std::rc::Rc::clone(&clock));

        // Act
        clock.advance(Duration::new(20, 0).unwrap());
        let reply = get_command.execute(&mut state);

        // Assert
        assert_eq!(reply, Ok(RedisReply::NullBulkString.to_reply()));
    }

    #[test]
    fn test_get_returns_a_string_if_it_exists_and_hasnt_expired() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());
        let set_command = SetCommand::new(
            "key".to_string(),
            RedisValueRef::String(Bytes::from_static(b"value")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Ex(100)),
            None,
            false,
        );
        let set_reply = set_command.execute(&mut state);
        assert!(set_reply.is_ok());

        let get_command = GetCommand::new("key".to_string(), std::rc::Rc::clone(&clock));

        // Act
        clock.advance(Duration::new(20, 0).unwrap());
        let reply = get_command.execute(&mut state);

        // Assert
        assert_eq!(
            reply,
            Ok(RedisReply::BulkString("value".to_string()).to_reply())
        );
    }
}
