#![feature(binary_heap_retain)]
#![allow(dead_code)]
#![allow(unused_variables)]

mod chord;
mod cache;
mod sim;
mod single_node_runner;
mod message;

pub type NodeId = u32;
pub type ContentStub = u32;
pub type RequestId = u64;
pub type ContentId = (NodeId, ContentStub);

// IPv4 Address
pub type Address = u32;
pub type Value = [u8; 128];

use clap::Parser;
use crate::chord::{CacheType, Distribution};
use crate::sim::Simulation;
use crate::single_node_runner::SingleNodeRunner;

pub const SUCCESSORS: usize = 32;
pub const MASTER_NODE: NodeId = 0;

/// Simulation of the Chord protocol with caching.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(short, long)]
    mode: Mode,

    /// Number of nodes (for simulation), or node number (for distributed)
    #[arg(short)]
    n: u32,

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

fn main() {
    let args = Args::parse();
    match args.mode {
        Mode::Distributed => {
            let runner = SingleNodeRunner::new(args.n, args.cache, args.cache_size, args.requests, args.distribution, args.zipf_param);
        },
        Mode::Simulation => {
            Simulation::new(args.n, args.cache, args.cache_size, args.requests, args.distribution, args.zipf_param);
        }
    }
}
