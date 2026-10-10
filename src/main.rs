use std::io::{self, Read, Write};
use std::str::from_utf8;

use bytes::BytesMut;
use mio::event::Event;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Registry, Token};

use crate::commands::try_from;
use crate::parser::RespParser;
use crate::state::StateStore;

mod clock;
mod commands;
mod event_loop;
mod parser;
mod state;

const SERVER: Token = Token(0);

fn main() -> io::Result<()> {
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);

    let addr = "127.0.0.1:6379".parse().unwrap();
    let mut server = TcpListener::bind(addr)?;

    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    let mut state = StateStore::new();

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

                    state.register_connection_to_token(connection, token);
                }
                token => {
                    let done = handle_connection_event(poll.registry(), &mut state, event)?;

                    if done
                        && let Some(mut connection) = state.deregister_connection_to_token(&token)
                    {
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
    state: &mut StateStore,
    event: &Event,
) -> io::Result<bool> {
    if event.is_readable() {
        let mut received_data = vec![0; 4096];
        let mut bytes_read = 0;

        loop {
            let connection = state
                .connection_for_token(&event.token())
                .expect("No connection for token");

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
                    if let Ok(command) = try_from(value) {
                        state.register_command_for_token(command, event.token());
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    println!("{:?}", err)
                }
            }

            if let Ok(str_buf) = from_utf8(received_data) {
                println!("Received data: {}", str_buf.trim_end());

                let interests = Interest::WRITABLE;
                let connection = state
                    .connection_for_token(&event.token())
                    .expect("No connection for token");
                registry.reregister(connection, event.token(), interests)?;
            } else {
                println!("Received (none UTF-8) data: {received_data:?}");
            }
        }
    }

    if event.is_writable() {
        let commands = state.commands_for_token(&event.token());

        for command in commands {
            if let Ok(data) = command.execute(state) {
                let connection = state
                    .connection_for_token(&event.token())
                    .expect("No connection for token");

                println!("Sending data: {data:?}");
                match connection.write(data.as_bytes()) {
                    Ok(n) if n < data.len() => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(_) => registry.reregister(connection, event.token(), Interest::READABLE)?,
                    Err(ref err) if would_block(err) => {}
                    Err(ref err) if interrupted(err) => {
                        return handle_connection_event(registry, state, event);
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
