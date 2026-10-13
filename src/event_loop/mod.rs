use bytes::Bytes;
use mio::Token;

pub struct EventLoop {}

impl EventLoop {
    pub fn new() -> Self {
        Self {}
    }

    pub fn start(&mut self) {}
}

impl EventLoop {
    fn create_file_event(&mut self) {}

    fn create_time_event(&mut self) {}

    fn accept_tcp_handler(&mut self) {}

    fn read_query_from_client(&mut self) {}

    fn send_reply_to_client(&mut self) {}
}

const EL_READABLE: u8 = 0b00000001;
const EL_WRITABLE: u8 = 0b00000010;
const EL_BARRIER: u8 = 0b00000100;

pub struct FileEvent<F: FnMut(&EventLoop, Token, Bytes, u8) -> ()> {
    mask: u8,
    read_file_event_processing: F,
    write_file_event_processing: F,
    token: Token,
    data: Bytes,
}

impl<F: FnMut(&EventLoop, Token, Bytes, u8) -> ()> FileEvent<F> {
    fn new(
        read_file: F,
        write_file: F,
        token: Token,
        data: Bytes,
        readable: bool,
        writable: bool,
        barrier: bool,
    ) -> Self {
        let mut mask: u8 = 0;
        if readable {
            mask |= EL_READABLE;
        }
        if writable {
            mask |= EL_WRITABLE;
        }
        if barrier {
            mask |= EL_BARRIER;
        }

        Self {
            mask,
            read_file_event_processing: read_file,
            write_file_event_processing: write_file,
            token,
            data,
        }
    }
    fn is_readable(&self) -> bool {
        (self.mask & EL_READABLE) != 0
    }

    fn is_writable(&self) -> bool {
        (self.mask & EL_WRITABLE) != 0
    }

    fn is_barrier(&self) -> bool {
        (self.mask & EL_BARRIER) != 0
    }
}

pub struct TimeEvent {}

pub struct FiredEvent {}
