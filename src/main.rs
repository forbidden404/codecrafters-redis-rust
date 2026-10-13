use std::io;

use crate::server::Server;

mod commands;
mod event_loop;
mod parser;
mod server;
mod utils;

fn main() -> io::Result<()> {
    let mut server = Server::new();

    if let Err(e) = server.run() {
        println!("{:?}", e);
        return Err(e);
    }

    Ok(())
}
