use chrono::Utc;

use crate::{
    commands::Command,
    parser::{RedisReply, RedisValueRef},
};

pub struct GetCommand {
    key: String,
}

impl GetCommand {
    pub fn new(key: String) -> Self {
        GetCommand { key }
    }
}

impl Command for GetCommand {
    fn execute(&self, state: &mut crate::state::StateStore) -> String {
        if let Some(entry) = state.cache.get_mut(&self.key)
            && entry.expiry_date.is_none_or(|ex| ex > Utc::now())
            && let RedisValueRef::String(value) = &entry.value
            && let Ok(value) = str::from_utf8(value)
        {
            entry.last_access_date = Some(Utc::now());
            RedisReply::BulkString(value.to_string()).to_reply()
        } else {
            RedisReply::NullBulkString.to_reply()
        }
    }
}
