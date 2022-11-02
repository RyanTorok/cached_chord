use rand::seq::index::sample;
use crate::{CacheType, Distribution};
use crate::chord::Node;

pub struct Simulation {
    nodes: Vec<Node>,
}

impl Simulation {
    pub fn new(n: u32, cache: CacheType, cache_size: usize, requests: u64, distribution: Distribution, zipf_param: f64) {
        let cap_err = "Fatal: `usize` should not be smaller than u32.";
        let mut nodes = Vec::with_capacity(n.try_into().expect(cap_err));
        let mut rand = rand::thread_rng();
        let mut rand_indices = sample(&mut rand, u32::MAX.try_into().expect(cap_err), n.try_into().expect(cap_err)).into_vec(); 
        rand_indices.sort_unstable();
        let mut rand_indices = rand_indices.iter();
        for _ in 0..n {
            let next_id: usize = *rand_indices.next().expect("Not enough IDs. This is a bug.");
            let i = next_id.try_into().expect("Node ID too big. This is a bug.");
            // Since this is just a simulation, we can just use the node ID as an address.
            let mut node = Node::new(i, i.into());
            node.init_cache(cache, cache_size);
            nodes.push(node);
        }

    }
}