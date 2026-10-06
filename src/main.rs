use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::str::from_utf8;

use bytes::BytesMut;
use mio::event::Event;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Registry, Token};

use crate::commands::Command;
use crate::parser::{RedisReply, RedisValueRef, RespParser};

mod commands;
mod event_loop;
mod parser;

const SERVER: Token = Token(0);

fn main() -> io::Result<()> {
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);

    let addr = "127.0.0.1:6379".parse().unwrap();
    let mut server = TcpListener::bind(addr)?;

    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    let mut cache = HashMap::new();
    let mut connections = HashMap::new();
    let mut commands = HashMap::new();
    let mut unique_token = Token(SERVER.0 + 1);

    loop {
        if let Err(err) = poll.poll(&mut events, None) {
            if interrupted(&err) {
                continue;
            }

            return Err(err);
        }

        for event in events.iter() {
            match event.token() {
                SERVER => {
                    let (mut connection, _) = match server.accept() {
                        Ok((connection, address)) => (connection, address),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            break;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    let token = next(&mut unique_token);
                    poll.registry()
                        .register(&mut connection, token, Interest::READABLE)?;

                    connections.insert(token, connection);
                }
                token => {
                    let done = if let Some(connection) = connections.get_mut(&token) {
                        handle_connection_event(
                            poll.registry(),
                            connection,
                            &mut commands,
                            &mut cache,
                            event,
                        )?
                    } else {
                        false
                    };

                    if done && let Some(mut connection) = connections.remove(&token) {
                        poll.registry().deregister(&mut connection)?;
                    }
                }
            }
        }
    }
}

fn next(current: &mut Token) -> Token {
    let next = current.0;
    current.0 += 1;
    Token(next)
}

fn handle_connection_event(
    registry: &Registry,
    connection: &mut TcpStream,
    commands: &mut HashMap<Token, Vec<RedisValueRef>>,
    cache: &mut HashMap<String, String>,
    event: &Event,
) -> io::Result<bool> {
    if event.is_readable() {
        let mut received_data = vec![0; 4096];
        let mut bytes_read = 0;

        loop {
            match connection.read(&mut received_data[bytes_read..]) {
                Ok(n) => {
                    bytes_read += n;
                    if bytes_read == received_data.len() {
                        received_data.resize(received_data.len() + 1024, 0);
                    }
                }
                Err(ref err) if would_block(err) => break,
                Err(ref err) if interrupted(err) => continue,
                Err(err) => return Err(err),
            }
        }

        if bytes_read != 0 {
            let received_data = &received_data[..bytes_read];
            let mut parser = RespParser::new();
            match parser.parse(&mut BytesMut::from(received_data)) {
                Ok(Some(value)) => {
                    commands
                        .entry(event.token())
                        .and_modify(|arr| arr.push(value.clone()))
                        .or_insert(vec![value]);
                }
                Ok(None) => {}
                Err(err) => {
                    println!("{:?}", err)
                }
            }

            if let Ok(str_buf) = from_utf8(received_data) {
                println!("Received data: {}", str_buf.trim_end());

                let interests = Interest::WRITABLE;
                registry.reregister(connection, event.token(), interests)?;
            } else {
                println!("Received (none UTF-8) data: {received_data:?}");
            }
        }
    }

    if event.is_writable()
        && let Some((_, values)) = commands.remove_entry(&event.token())
    {
        for value in values {
            if let Ok(command) = Command::try_from(value) {
                let data = match command {
                    Command::Ping => RedisReply::SimpleString("PONG".to_string()).to_reply(),
                    Command::Pong => RedisReply::SimpleString("OK".to_string()).to_reply(),
                    Command::Cmd(cmd) => {
                        println!("Received COMMAND {}", cmd);
                        RedisReply::SimpleString("OK".to_string()).to_reply()
                    }
                    Command::Echo(echo) => RedisReply::BulkString(echo).to_reply(),
                    Command::Set(key, value) => {
                        cache.insert(key, value);
                        RedisReply::SimpleString("OK".to_string()).to_reply()
                    }
                    Command::Get(key) => {
                        if let Some(entry) = cache.get(&key) {
                            RedisReply::BulkString(entry.clone()).to_reply()
                        } else {
                            RedisReply::NullBulkString.to_reply()
                        }
                    }
                };

                println!("Sending data: {data:?}");
                match connection.write(data.as_bytes()) {
                    Ok(n) if n < data.len() => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(_) => registry.reregister(connection, event.token(), Interest::READABLE)?,
                    Err(ref err) if would_block(err) => {}
                    Err(ref err) if interrupted(err) => {
                        return handle_connection_event(
                            registry, connection, commands, cache, event,
                        );
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }

    Ok(false)
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}
