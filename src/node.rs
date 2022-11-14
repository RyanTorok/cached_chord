use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use crate::cache::{Cache, make_cache, default_cache, CacheStats};
use crate::{NodeId, Address, ContentId, Value};
use serde::{Serialize, Deserialize};

pub struct Node {
    id: NodeId,
    address: Address,
    finger_table: BTreeMap<NodeId, (NodeId, Address, Range<NodeId>)>,
    successor: Option<(NodeId, Address)>,
    predecessor: Option<(NodeId, Address)>,
    store: HashMap<ContentId, Value>,
    cache: Box<dyn Cache + Send>,
    active: bool,
    stats: CacheStats
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FindResult {
    Value(Value, Range<NodeId>), // Range indicates set of serviced keys, for populating other nodes' finger tables.
    Redirect(Address),
    NoSuchEntry(Range<NodeId>),
    Error(String),
}

impl Node {
    pub fn new(id: NodeId, address: Address) -> Node {
        Node {
            id, address, finger_table: BTreeMap::new(), successor: None, predecessor: None, store: Default::default(), cache: Box::new(default_cache()), active: false, stats: CacheStats::new()
        }
    }

    pub fn print_status(&self) {
        let key_range = self.store.keys().min().map_or_else(|| None, |min| self.store.keys().max().map(|max| (min.0, max.0)));
        println!("[Node {} status: Address = {}, key_range: {:?} predecessor: {:?}, successor = {:?}, cache_stats: {}]", self.id, self.address, key_range, self.predecessor, self.successor, self.stats)
    }

    pub fn set(&mut self, key: ContentId, value: Value) {
        self.store.insert(key, value);
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn address(&self) -> Address {
        self.address
    }

    pub fn msg_id(&self) -> (NodeId, Address) {
        (self.id(), self.address())
    }

    fn responsible_for(&self, key: ContentId) -> bool {
            match self.predecessor {
                None => false,
                Some(p) => {
                    let us = self.id;
                    let them = p.0;
                    if them < us {
                        // No overflow between them and us
                        them < key.0 && key.0 <= us
                    } else {
                        // them >= us
                        !(us < key.0 && key.0 <= them)
                    }
                }
            }
    }

    // This function takes &mut self because it allows us to avoid using a RefCell inside the caches
    // that update internal state on a read (e.g. LRU)
    pub fn next_finger(&mut self, target: ContentId) -> FindResult {
        if self.responsible_for(target) {
            let key_range = self.predecessor.unwrap().0..self.id;
            match self.store.get(&target) {
                None => FindResult::NoSuchEntry(key_range),
                Some(val) => FindResult::Value(*val, key_range)
            }
        } else if let Some(addr) = self.cache.get(target) {
            // Cache hit -- redirect straight to the node that has the key
            self.stats.hit();
            FindResult::Redirect(addr)
        } else {
            // Cache miss.
            self.stats.miss();
            match self.finger_table.iter().rev().find(|ent| *ent.0 < target.0) {
                None => {
                    // No luck in the finger table (this can happen nominally in some degenerate cases like
                    // a single node or when first initializing the finger table).
                    let err = FindResult::Error("No available finger pointer.".to_string());
                    if let Some(successor) = self.successor {
                        if successor.0 < target.0 {
                            FindResult::Redirect(successor.1)
                        } else {
                            err
                        }
                    } else {
                        err
                    }
                }
                Some(ent) => {
                    FindResult::Redirect(ent.1.1)
                },
            }
        }
    }

    pub fn populate_finger(&mut self, finger_key: NodeId, f_node: NodeId, f_addr: Address, key_range: Range<NodeId>) -> usize {
        self.finger_table.insert(finger_key, (f_node, f_addr, key_range));
        self.finger_table.len()
    }

    pub fn set_successor(&mut self, s: (NodeId, Address)) {
        self.successor = Some(s)
    }

    pub fn successor(&self) -> Option<(NodeId, Address)> {
        self.successor
    }

    pub fn predecessor(&self) -> Option<(NodeId, Address)> {
        self.predecessor
    }

    pub fn set_predecessor(&mut self, s: (NodeId, Address)) {
        self.predecessor = Some(s);
    }

    pub fn offload_keys_before(&mut self, predecessor: NodeId) -> Vec<(ContentId, Value)> {
        // Remove all keys that have a key <= the new predecessor node, modulo the max ID.
        self.store.drain_filter(|(node_id, stub), _| {
            !between_mod_id(predecessor, *node_id, self.id)
        }).collect()
    }

    pub fn init_cache(&mut self, cache_type: CacheType, size: usize) {
        self.cache = make_cache(cache_type, size);
    }

    pub fn cache_key(&mut self, key: ContentId, addr: Address) {
        self.cache.set(key, addr);
    }

    pub fn inactive(&self) -> bool {
        !self.active
    }

    pub fn activate(&mut self) {
        self.active = true;
    }
}

#[derive(clap::ValueEnum, Clone, Debug, Copy)]
pub enum CacheType {
    None,
    LRU,
    MRU,
    FIFO,
    LIFO,
    LFU
}

#[derive(clap::ValueEnum, Clone, Debug, Copy)]
pub enum Distribution {
    Uniform,
    Zipf
}

#[derive(Ord, Eq)]
struct Successor {
    node: NodeId,
    addr: Address
}

impl PartialEq<Self> for Successor {
    fn eq(&self, other: &Self) -> bool {
        self.node.eq(&other.node)
    }
}

impl PartialOrd for Successor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.node.partial_cmp(&other.node)
    }
}

pub fn between_mod_id(lo: NodeId, mid: NodeId, hi: NodeId) -> bool {
    if lo < hi {
        // No overflow between lo and hi
        lo < mid && mid <= hi
    } else {
        // lo >= hi
        !(hi < mid && mid <= lo)
    }
}
