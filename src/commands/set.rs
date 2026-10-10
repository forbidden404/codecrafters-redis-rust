use std::time::Duration;

use xxhash_rust::xxh3::xxh3_64;

use crate::{
    clock::Clock,
    commands::{Command, CommandError, get::GetCommand},
    parser::{RedisReply, RedisValueRef},
    state::{RedisEntry, StateStore},
};

#[derive(Debug)]
pub struct SetCommand<C: Clock> {
    key: String,
    value: RedisValueRef,
    clock: C,
    expiry: Option<ExpiryCondition>,
    condition: Option<SetCondition>,
    should_get: bool,
}

#[derive(Debug, Clone)]
pub enum ExpiryCondition {
    Ex(i64),
    Px(i64),
    Exat(i64),
    Pxat(i64),
    KeepTTL,
}

#[derive(Debug, Clone)]
pub enum SetCondition {
    Nx,
    Xx,
    Ifeq(String),
    Ifne(String),
    Ifdeq(String),
    Ifdne(String),
}

impl<C: Clock> SetCommand<C> {
    pub fn new(
        key: String,
        value: RedisValueRef,
        clock: C,
        expiry: Option<ExpiryCondition>,
        condition: Option<SetCondition>,
        should_get: bool,
    ) -> Self {
        SetCommand {
            key,
            value,
            clock,
            expiry,
            condition,
            should_get,
        }
    }
}

impl<C: Clock> Command for SetCommand<C> {
    fn execute(&self, state: &mut StateStore) -> Result<String, CommandError> {
        // Check for expiry validity
        if let Some(ExpiryCondition::Ex(value))
        | Some(ExpiryCondition::Px(value))
        | Some(ExpiryCondition::Exat(value))
        | Some(ExpiryCondition::Pxat(value)) = self.expiry
            && value < 0
        {
            return Ok(RedisReply::NullBulkString.to_reply());
        }

        let mut expiry = self.expiry.clone();

        let mut reply: Option<String> = None;
        // If get flag is set, try to execute the command, and abort if there's an error.
        if self.should_get {
            let get_command = GetCommand::new(self.key.clone(), self.clock.clone());
            reply = Some(get_command.execute(state)?);
        }

        let entry = state.cache.get_mut(&self.key);

        // KEEPTTL is only valid if there's a TTL to keep.
        if matches!(expiry, Some(ExpiryCondition::KeepTTL))
            && entry.as_ref().and_then(|e| e.expiry_date).is_none()
        {
            expiry = None;
        }

        // EX/PX are relative, EXAT/PXAT are absolute.
        // We have checked for the validity of the arguments, so unwrap should be safe.

        let expiry_date = expiry.map(|e| match e {
            ExpiryCondition::Ex(seconds) => self.clock.now() + Duration::new(seconds as u64, 0),
            ExpiryCondition::Px(milliseconds) => {
                self.clock.now() + Duration::from_millis(milliseconds as u64)
            }
            ExpiryCondition::Exat(unix_time_seconds) => {
                C::from_timestamp_secs(unix_time_seconds).unwrap()
            }
            ExpiryCondition::Pxat(unix_time_milliseconds) => {
                C::from_timestamp_millis(unix_time_milliseconds).unwrap()
            }
            ExpiryCondition::KeepTTL => entry
                .as_ref()
                .and_then(|e| e.expiry_date)
                .expect("Failed to get expiry date after keepttl check"),
        });

        // check if NX is set, and entry exists, abort if it checks.
        if matches!(self.condition, Some(SetCondition::Nx)) && entry.is_some() {
            if let Some(reply) = reply {
                return Ok(reply);
            } else {
                return Ok(RedisReply::NullBulkString.to_reply());
            }
        }

        // check if XX is set, or IFEQ/IFDEQ, these require the entry to exist,
        // if it doesn't, abort.
        if (matches!(self.condition, Some(SetCondition::Xx))
            || matches!(
                self.condition,
                Some(SetCondition::Ifeq(_)) | Some(SetCondition::Ifdeq(_))
            ))
            && entry.is_none()
        {
            if let Some(reply) = reply {
                return Ok(reply);
            } else {
                return Ok(RedisReply::NullBulkString.to_reply());
            }
        }

        // handle conditional set operations - only set if key is found and condition is met,
        // otherwise return NullBulkString. If value is not a string, return an error.
        if let Some(entry) = &entry
            && matches!(
                self.condition,
                Some(SetCondition::Ifeq(_))
                    | Some(SetCondition::Ifdeq(_))
                    | Some(SetCondition::Ifne(_))
                    | Some(SetCondition::Ifdne(_))
            )
        {
            if !matches!(entry.value, RedisValueRef::String(_)) {
                return Err(CommandError::Abort);
            }

            let RedisValueRef::String(value) = &entry.value else {
                return Err(CommandError::Abort);
            };

            match (&self.condition, str::from_utf8(value)) {
                (Some(SetCondition::Ifeq(match_value)), Ok(value)) if match_value != value => {
                    if let Some(reply) = reply {
                        return Ok(reply);
                    } else {
                        return Ok(RedisReply::NullBulkString.to_reply());
                    }
                }
                (Some(SetCondition::Ifne(match_value)), Ok(value)) if match_value == value => {
                    if let Some(reply) = reply {
                        return Ok(reply);
                    } else {
                        return Ok(RedisReply::NullBulkString.to_reply());
                    }
                }
                (Some(SetCondition::Ifdeq(match_value)), Ok(value))
                    if xxh3_64(match_value.as_bytes()) != xxh3_64(value.as_bytes()) =>
                {
                    if let Some(reply) = reply {
                        return Ok(reply);
                    } else {
                        return Ok(RedisReply::NullBulkString.to_reply());
                    }
                }
                (Some(SetCondition::Ifdne(match_value)), Ok(value))
                    if xxh3_64(match_value.as_bytes()) == xxh3_64(value.as_bytes()) =>
                {
                    if let Some(reply) = reply {
                        return Ok(reply);
                    } else {
                        return Ok(RedisReply::NullBulkString.to_reply());
                    }
                }
                (_, _) => {}
            }
        }

        // If expiry_date has already passed, we don't need to add the key.
        if let Some(expiry_date) = expiry_date
            && expiry_date < self.clock.now()
        {
            if let Some(reply) = reply {
                return Ok(reply);
            } else {
                return Ok(RedisReply::SimpleString("OK".to_string()).to_reply());
            }
        }

        // Insert or modify the entry in the cache.
        if let Some(entry) = entry {
            entry.value = self.value.clone();
            if entry.expiry_date.is_none() {
                entry.expiry_date = expiry_date;
            }
            entry.last_access_date = Some(self.clock.now());
        } else {
            state.cache.insert(
                self.key.clone(),
                RedisEntry::new(self.value.clone(), expiry_date, None),
            );
        }

        Ok(reply.unwrap_or(RedisReply::SimpleString("OK".to_string()).to_reply()))
    }
}

#[cfg(test)]
mod set_command_tests {
    use std::cell::RefCell;

    use bytes::Bytes;
    use chrono::{DateTime, Duration, Utc};

    use crate::commands::GetCommand;
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
    fn test_set_nx_does_not_overwrite_existing_entry() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let first_reply = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"1")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Nx),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set first value.");

        let second_reply = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"2")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Nx),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set second value.");

        let get_reply = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(
            first_reply,
            RedisReply::SimpleString("OK".to_string()).to_reply()
        );
        assert_eq!(second_reply, RedisReply::NullBulkString.to_reply());
        assert_eq!(
            get_reply,
            RedisReply::BulkString("1".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_xx_only_writes_over_existing_entry() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let first_reply = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"1")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Xx),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Nx),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let second_reply = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"2")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Xx),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let get_reply = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(first_reply, RedisReply::NullBulkString.to_reply());
        assert_eq!(
            second_reply,
            RedisReply::SimpleString("OK".to_string()).to_reply()
        );
        assert_eq!(
            get_reply,
            RedisReply::BulkString("2".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_get_returns_the_old_value() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let old_value = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar2")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            true,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(
            old_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
        assert_eq!(
            new_value,
            RedisReply::BulkString("bar2".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_get_with_no_old_value() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let old_value = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            true,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(old_value, RedisReply::NullBulkString.to_reply());
        assert_eq!(
            new_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_get_with_xx_returns_old_value() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let old_value = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"baz")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Xx),
            true,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(
            old_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
        assert_eq!(
            new_value,
            RedisReply::BulkString("baz".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_get_with_xx_and_no_old_value() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let old_value = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Xx),
            true,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(old_value, RedisReply::NullBulkString.to_reply());
        assert_eq!(new_value, RedisReply::NullBulkString.to_reply());
    }

    #[test]
    fn test_set_get_with_nx_and_no_existing_entry() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let old_value = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Nx),
            true,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(old_value, RedisReply::NullBulkString.to_reply());
        assert_eq!(
            new_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_get_with_nx_with_existing_entry() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let old_value = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"baz")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Nx),
            true,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(
            old_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
        assert_eq!(
            new_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ex_returns_value_when_not_expired() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Ex(10)),
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        clock.advance(Duration::new(5, 0).unwrap());

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(
            new_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ex_returns_null_when_expired() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Ex(10)),
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        clock.advance(Duration::new(11, 0).unwrap());

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(new_value, RedisReply::NullBulkString.to_reply());
    }

    #[test]
    fn test_set_px_returns_value_when_not_expired() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Px(10000)),
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        clock.advance(Duration::new(5, 0).unwrap());

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(
            new_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_px_returns_null_when_expired() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Px(10000)),
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        clock.advance(Duration::new(11, 0).unwrap());

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(new_value, RedisReply::NullBulkString.to_reply());
    }

    #[test]
    fn test_set_exat_returns_value_when_not_expired() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();
        let expiry_timestamp = DateTime::timestamp(&clock.now());

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Exat(expiry_timestamp + 10)),
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        clock.advance(Duration::new(5, 0).unwrap());

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(
            new_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_exat_returns_null_when_expired() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();
        let expiry_timestamp = DateTime::timestamp(&clock.now());

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Exat(expiry_timestamp + 10)),
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        clock.advance(Duration::new(11, 0).unwrap());

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(new_value, RedisReply::NullBulkString.to_reply());
    }

    #[test]
    fn test_set_pxat_returns_value_when_not_expired() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();
        let expiry_timestamp = DateTime::timestamp_millis(&clock.now());

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Pxat(expiry_timestamp + 10000)),
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        clock.advance(Duration::new(5, 0).unwrap());

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(
            new_value,
            RedisReply::BulkString("bar".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_pxat_returns_null_when_expired() {
        // Arrange
        let clock = std::rc::Rc::new(MockClock::new());
        let mut state = StateStore::new();
        let expiry_timestamp = DateTime::timestamp_millis(&clock.now());

        // Act
        let _ = SetCommand::new(
            "foo".to_string(),
            RedisValueRef::String(Bytes::from_static(b"bar")),
            std::rc::Rc::clone(&clock),
            Some(ExpiryCondition::Pxat(expiry_timestamp + 10000)),
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value.");

        clock.advance(Duration::new(11, 0).unwrap());

        let new_value = GetCommand::new("foo".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value.");

        // Assert
        assert_eq!(new_value, RedisReply::NullBulkString.to_reply());
    }

    #[test]
    fn test_set_ifeq_key_exists_and_matches() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let _ = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"hello")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifeq("hello".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::SimpleString("OK".to_string()).to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("world".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ifeq_key_exists_and_does_not_match() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let _ = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"hello")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifeq("different".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::NullBulkString.to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("hello".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ifeq_key_does_not_exist() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifeq("hello".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::NullBulkString.to_reply());
        assert_eq!(value, RedisReply::NullBulkString.to_reply());
    }

    #[test]
    fn test_set_ifne_key_exists_and_matches() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let _ = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"hello")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifne("different".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::SimpleString("OK".to_string()).to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("world".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ifne_key_exists_and_does_not_match() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let _ = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"hello")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifne("hello".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::NullBulkString.to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("hello".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ifne_key_does_not_exist() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifne("hello".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::SimpleString("OK".to_string()).to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("world".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ifdeq_key_exists_and_matches() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let _ = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"hello")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifdeq("hello".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::SimpleString("OK".to_string()).to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("world".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ifdeq_key_exists_and_does_not_match() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let _ = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"hello")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifdeq("different".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::NullBulkString.to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("hello".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ifdeq_key_does_not_exist() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifdeq("hello".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::NullBulkString.to_reply());
        assert_eq!(value, RedisReply::NullBulkString.to_reply());
    }

    #[test]
    fn test_set_ifdne_key_exists_and_matches() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let _ = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"hello")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifdne("different".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::SimpleString("OK".to_string()).to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("world".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ifdne_key_exists_and_does_not_match() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let _ = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"hello")),
            std::rc::Rc::clone(&clock),
            None,
            None,
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifdne("hello".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::NullBulkString.to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("hello".to_string()).to_reply()
        );
    }

    #[test]
    fn test_set_ifdne_key_does_not_exist() {
        // Arrange
        let mut state = StateStore::new();
        let clock = std::rc::Rc::new(MockClock::new());

        // Act
        let reply = SetCommand::new(
            "mykey".to_string(),
            RedisValueRef::String(Bytes::from_static(b"world")),
            std::rc::Rc::clone(&clock),
            None,
            Some(SetCondition::Ifdne("hello".to_string())),
            false,
        )
        .execute(&mut state)
        .expect("Failed to set value");

        let value = GetCommand::new("mykey".to_string(), std::rc::Rc::clone(&clock))
            .execute(&mut state)
            .expect("Failed to get value");

        // Assert
        assert_eq!(reply, RedisReply::SimpleString("OK".to_string()).to_reply());
        assert_eq!(
            value,
            RedisReply::BulkString("world".to_string()).to_reply()
        );
    }
}
