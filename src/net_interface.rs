use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use crate::Address;
use crate::message::ChordMessage;

const PORT: u16 = 8080;

async fn run_socket_ib(mut socket: TcpStream, inbox: Sender<ChordMessage>) {
   loop {
       let mut size_buf = [0u8; 4];
       match socket.read_exact(&mut size_buf).await {
           Ok(n) => {
               let len = u32::from_be_bytes(size_buf).try_into().expect("Size too large for a usize.");
               //println!("Incoming Message length: {}", len);
               let mut buf = vec![0u8; len];
               match socket.read_exact(&mut buf).await {
                   Ok(n) => {
                       match bincode::deserialize(&buf) {
                           Ok(msg) => {
                               inbox.send(msg).await.expect("Error: Could not send ChordMessage to runner: {}");
                           },
                           Err(e) => {
                               eprintln!("Error: Could not decode ChordMessage from socket: {}", e);
                               continue;
                           }
                       };
                   }
                   Err(e) => {
                       eprintln!("Error: Could not read from socket for inbox.");
                       return;
                   }
               };
           }
           Err(e) => {
               eprintln!("Error: could not read size. {}", e)
           }
       }
   }
}

pub async fn run_inbox(inbox: Sender<ChordMessage>, ip: Ipv4Addr) {
    let listener = TcpListener::bind(format!("{}:{}", ip, PORT)).await.expect("Error: could not bind to TCP port.");
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(run_socket_ib(socket, inbox.clone()));
            }
            Err(e) => {
                eprintln!("Error: Could not accept socket for inbox: {}", e)
            }
        }
    }
}

async fn send_msg(socket: &mut TcpStream, msg: ChordMessage) {
    let ser = bincode::serialize(&msg)
        .expect("Error: Could not serialize ChordMessage from outbox.");
    let len: u32 = ser.len().try_into().expect("ChordMessage too large.");
    //println!("Outgoing message length: {}", len);
    let mut buf = Vec::with_capacity(4 + ser.len());
    len.to_be_bytes().into_iter().chain(ser.into_iter()).for_each(|b| buf.push(b));
    socket.write(&*buf).await
        .expect("Error: Could not write serialized ChordMessage to socket.");
    //socket.flush().await.expect("Error: Socket flush failed.");
}

pub async fn run_outbox(mut outbox: Receiver<ChordMessage>, verbose: bool) {
    let mut sockets: HashMap<Ipv4Addr, TcpStream> = HashMap::new();
    loop {
        match outbox.recv().await {
            Some(msg) => {
                if verbose {
                    println!("Outgoing message to address {:?}: {:?}", msg.dest, msg.content);
                }
                // TODO do something with client replies.
                if !msg.dest.eq(&Address(Ipv4Addr::new(0, 0, 0, 0))) {
                    match sockets.entry(msg.dest.0) {
                        Occupied(mut e) => {
                            send_msg(e.get_mut(), msg).await;
                        },
                        Vacant(e) => {
                            loop {
                                match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(format!("{}:{}", msg.dest, PORT))).await {
                                    Ok(res_socket) => {
                                        match res_socket {
                                            Ok(mut socket) => {
                                                send_msg(&mut socket, msg).await;
                                                e.insert(socket);
                                                break;
                                            }
                                            Err(e) => {
                                                eprintln!("Error: Could not create outgoing socket for outbox (dest = {}) because of error: {}", msg.dest, e);
                                            }
                                        }
                                    }
                                    Err(e) => { eprintln!("Error: Could not create outgoing socket for outbox (dest = {}) because of error: {}", msg.dest, e); }
                                }
                            }
                        }
                    };
                }
            }
            None => {
                eprintln!("Error: Removed a `None` from outbox.")
            }
        }
    }
}