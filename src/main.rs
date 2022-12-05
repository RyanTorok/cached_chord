#![feature(binary_heap_retain)]
#![feature(hash_drain_filter)]
#![allow(dead_code)]
#![allow(unused_variables)]

mod node;
mod cache;
//mod sim;
mod node_runner;
mod message;
mod net_interface;

use std::fmt::{Debug, Display, Formatter};
use std::net::Ipv4Addr;
use std::process::Command;
use std::str::FromStr;
use std::time::Duration;
use clap::Parser;
use crate::node::{CacheType, Distribution};
use crate::net_interface::{run_inbox, run_outbox};
use crate::node_runner::{run_node, send_fix_fingers_triggers, send_heartbeat_triggers, SingleNodeRunner};
use serde::{Serialize, Deserialize};

pub const MASTER_NODE: NodeId = 0;
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
pub const FIX_FINGER_INTERVAL: Duration = Duration::from_millis(500);

pub type NodeId = u32;
pub type ContentStub = u32;
pub type RequestId = u64;
pub type ContentId = (NodeId, ContentStub);

// IPv4 Address
#[derive(Copy, Clone, Serialize, Deserialize, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct Address(Ipv4Addr);

impl Default for Address {
    fn default() -> Self {
        Address(Ipv4Addr::UNSPECIFIED)
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

// I'd like to do u8; 128, but serde doesn't auto-implement traits for arrays longer than 32 elements.
pub type Value = [u32; 32];

/// Simulation of the Chord protocol with caching.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    /// Number of nodes (for simulation), or node number (for distributed)
    #[arg(short)]
    n: u32,

    #[arg(short, long)]
    keys: u64,

    /// Type of cache [none, lru, mru, fifo, lifo, mfu]
    #[arg(short, long)]
    cache: CacheType,

    /// Size of cache (entries)
    #[arg(long)]
    cache_size: usize,

    #[arg(long)]
    master_ip: String,

    /// Number of requests
    #[arg(short, long)]
    requests: u64,

    /// Distribution of requests for nodes [uniform, zipf]
    #[arg(short, long)]
    distribution: Distribution,

    #[arg(long, default_value_t = 1.2)]
    zipf_param: f64,

    #[arg(short, default_value_t = false)]
    verbose: bool,

    #[arg(long)]
    index: u32,

    #[arg(long)]
    total: u32

}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum Mode {
    Distributed,
    Simulation
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let ip = match Ipv4Addr::from_str(String::from_utf8(Command::new("hostname").arg("-I").output()
        .expect("Error: could not read IP address with `hostname -I` command.").stdout)
        .expect("String returned by `hostname -I` was not valid UTF-8.").trim()) {
        Ok(ip) => ip,
        Err(e) => {
            eprintln!("ip address error: {}", e);
            Ipv4Addr::UNSPECIFIED
        }
    };
        //.expect("IP address returned by `hostname -I` was invalid.");
    let (r, inbox, outbox, activation) = SingleNodeRunner::new(
        args.n,
        Address(ip),
        args.keys,
        args.cache,
        args.cache_size,
        Address(Ipv4Addr::from_str(&args.master_ip).expect("Invalid master IP address format.")),
        args.requests,
        args.distribution,
        args.zipf_param,
        args.verbose
    );
    let clone_inbox = inbox.clone();
    let clone_inbox_2 = inbox.clone();
    tokio::spawn(run_inbox(inbox, ip));
    tokio::spawn(run_outbox(outbox, args.verbose));
    tokio::spawn(send_heartbeat_triggers(clone_inbox, HEARTBEAT_INTERVAL));
    tokio::spawn(send_fix_fingers_triggers(clone_inbox_2, FIX_FINGER_INTERVAL));
    tokio::spawn(run_node(r, activation, args.keys, args.cache.to_string(), args.cache_size, args.index, args.total)).await.expect("Error: run_node should never return.");
}
