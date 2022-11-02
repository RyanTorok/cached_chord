#![feature(binary_heap_retain)]
#![allow(dead_code)]
#![allow(unused_variables)]

mod chord;
mod cache;
//mod sim;
mod single_node_runner;
mod message;
mod net_interface;

use clap::Parser;
use crate::chord::{CacheType, Distribution};
use crate::net_interface::{run_inbox, run_outbox};
use crate::single_node_runner::{run_node, SingleNodeRunner};

pub const SUCCESSORS: usize = 32;
pub const MASTER_NODE: NodeId = 0;

pub type NodeId = u32;
pub type ContentStub = u32;
pub type RequestId = u64;
pub type ContentId = (NodeId, ContentStub);

// IPv4 Address
pub type Address = u32;

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
    #[arg(short, long)]
    cache_size: usize,

    /// Number of requests
    #[arg(short, long)]
    requests: u64,

    /// Distribution of requests for nodes [uniform, zipf]
    #[arg(short, long)]
    distribution: Distribution,

    #[arg(long, default_value_t = 1.0)]
    zipf_param: f64

}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum Mode {
    Distributed,
    Simulation
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let ip = Default::default(); // TODO
    let (r, inbox, outbox) = SingleNodeRunner::new(args.n, ip, args.keys, args.cache, args.cache_size, args.requests, args.distribution, args.zipf_param);
    tokio::spawn(run_inbox(inbox));
    tokio::spawn(run_outbox(outbox));
    tokio::spawn(run_node(r));
}
