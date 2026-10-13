use std::collections::HashMap;

use chrono::{DateTime, Utc};
use mio::{Token, net::TcpStream};

use crate::{commands::Command, parser::RedisValueRef};

pub struct RedisEntry {
    pub value: RedisValueRef,
    pub expiry_date: Option<DateTime<Utc>>,
    pub last_access_date: Option<DateTime<Utc>>,
}

impl RedisEntry {
    pub fn new(
        value: RedisValueRef,
        expiry_date: Option<DateTime<Utc>>,
        last_access_date: Option<DateTime<Utc>>,
    ) -> Self {
        RedisEntry {
            value,
            expiry_date,
            last_access_date,
        }
    }
}

pub struct StateStore {
    pub cache: HashMap<String, RedisEntry>,
    connections: HashMap<Token, TcpStream>,
    commands: HashMap<Token, Vec<Box<dyn Command>>>,
}

impl StateStore {
    pub fn new() -> Self {
        StateStore {
            cache: HashMap::new(),
            connections: HashMap::new(),
            commands: HashMap::new(),
        }
    }

    pub fn connection_for_token(&mut self, token: &Token) -> Option<&mut TcpStream> {
        self.connections.get_mut(token)
    }

    pub fn register_connection_to_token(&mut self, connection: TcpStream, token: Token) {
        self.connections.insert(token, connection);
    }

    pub fn deregister_connection_to_token(&mut self, token: &Token) -> Option<TcpStream> {
        self.connections.remove(token)
    }

    pub fn commands_for_token(&mut self, token: &Token) -> Vec<Box<dyn Command>> {
        self.commands.remove(token).unwrap_or(vec![])
    }

    pub fn register_command_for_token(&mut self, command: Box<dyn Command>, token: Token) {
        self.commands.entry(token).or_default().push(command);
    }
}
