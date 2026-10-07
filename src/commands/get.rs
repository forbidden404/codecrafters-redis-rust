use chrono::Utc;

use crate::{commands::Command, parser::RedisReply};

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
        if let Some(entry) = state.cache.get(&self.key)
            && entry.expiry_date.is_none_or(|ex| ex > Utc::now())
        {
            RedisReply::BulkString(entry.value.to_string()).to_reply()
        } else {
            RedisReply::NullBulkString.to_reply()
        }
    }
}
