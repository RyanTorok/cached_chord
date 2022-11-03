use std::net::Ipv4Addr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use crate::message::ChordMessage;

const PORT: u16 = 8080;

pub async fn run_inbox(inbox: Sender<ChordMessage>, ip: Ipv4Addr) {
    let listener = TcpListener::bind(format!("{}:{}", ip.to_string(), PORT)).await.expect("Error: could not bind to TCP port.");
    let mut buf = [0u8; 257];
    loop {
        match listener.accept().await {
            Ok((mut socket, _)) => {
                let read = match socket.read(&mut buf).await {
                    Ok(n) => {
                        if n > buf.len() - 1 {
                            panic!("Error: Inbox read buffer is not set large enough. This is a bug.")
                        }
                        inbox.send(match bincode::deserialize(&buf[0..n]) {
                            Ok(msg) => msg,
                            Err(e) => {
                                eprintln!("Error: Could not decode ChordMessage from socket.");
                                continue;
                            }
                        }).await.expect("Error: Could not send decoded ChordMessage to inbox.");
                    }
                    Err(e) => {
                        eprintln!("Error: Could not read from socket for inbox.")
                    }
                };
            }
            Err(e) => {
                eprintln!("Error: Could not accept socket for inbox: {}", e)
            }
        }
    }
}

pub async fn run_outbox(mut outbox: Receiver<ChordMessage>) {
    loop {
        match outbox.recv().await {
            Some(msg) => {
                let mut socket;
                loop {
                    match TcpStream::connect(format!("{}:{}", msg.dest, PORT)).await {
                        Err(e) => {
                            eprintln!("Error: Could not create outgoing socket for outbox -- dest = {}", msg.dest);
                        }
                        Ok(sock) => {
                            socket = sock;
                            break;
                        }
                    }
                }
                socket.write(&*bincode::serialize(&msg)
                    .expect("Error: Could not serialize ChordMessage from outbox.")).await
                    .expect("Error: Could not write serialized ChordMessage to socket.");
                socket.flush().await.expect("Error: Socket flush failed.");
            }
            None => {
                eprintln!("Error: Removed a `None` from outbox.")
            }
        }
    }
}