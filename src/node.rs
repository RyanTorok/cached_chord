use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use crate::cache::{Cache, make_cache, default_cache};
use crate::{NodeId, Address, ContentId, Value, SUCCESSORS};
use serde::{Serialize, Deserialize};

pub struct Node {
    id: NodeId,
    address: Address,
    finger_table: BTreeMap<NodeId, (NodeId, Address, Range<NodeId>)>,
    successors: BTreeSet<(NodeId, Address)>,
    predecessor: Option<(NodeId, Address)>,
    store: HashMap<ContentId, Value>,
    cache: Box<dyn Cache + Send>,
    active: bool
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FindResult {
    Value(Value, Range<NodeId>), // Range indicates set of serviced keys, for populating other nodes' finger tables.
    Redirect(Address),
    NoSuchEntry,
    Error(String),
}

impl Node {
    pub fn new(id: NodeId, address: Address) -> Node {
        Node {
            id, address, finger_table: BTreeMap::new(), successors: BTreeSet::new(), predecessor: None, store: Default::default(), cache: Box::new(default_cache()), active: false
        }
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
                        them < p.0 && p.0 <= us
                    } else {
                        // them >= us
                        !(us < p.0 && p.0 <= them)
                    }
                }
            }
    }

    // This function takes &mut self because it allows us to avoid using a RefCell inside the caches
    // that update internal state on a read (e.g. LRU)
    pub fn next_finger(&mut self, target: ContentId) -> FindResult {
        if self.responsible_for(target) {
            match self.store.get(&target) {
                None => FindResult::NoSuchEntry,
                Some(val) => FindResult::Value(*val, self.predecessor.unwrap().0..self.id)
            }
        } else if let Some(addr) = self.cache.get(target) {
            // Cache hit -- redirect straight to the node that has the key
            FindResult::Redirect(addr)
        } else {
            match self.finger_table.iter().rev().find(|ent| *ent.0 < target.0) {
                None => {
                    // No luck in the finger table (this can happen nominally in some degenerate cases like
                    // a single node or when first initializing the finger table).
                    match self.successors.iter().rev().find(|ent | ent.0 < target.0) {
                        None => FindResult::Error("No available finger pointer.".to_string()),
                        Some((_s_node, s_addr)) => {
                            FindResult::Redirect(*s_addr)
                        }
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

    pub fn add_successor(&mut self, s: (NodeId, Address)) {
        self.successors.insert(s);
        while self.successors.len() > SUCCESSORS {
            // Can't be None as long as SUCCESSORS > 0.
            let max = *self.successors.iter().rev().next().unwrap();
            self.successors.remove(&max);
        }
    }

    pub fn nth_successor(&self, n: usize) -> (NodeId, Address) {
        *self.successors.iter().nth(n).expect(&format!("Node {} successor index {} out of bounds (size = {})", self.id(), n, self.successors.len()))
    }

    pub fn predecessor(&self) -> Option<(NodeId, Address)> {
        self.predecessor
    }

    pub fn set_predecessor(&mut self, s: (NodeId, Address)) {
        self.predecessor = Some(s);
    }

    pub fn drain_keys_before(&mut self, predecessor: NodeId) -> Vec<(ContentId, Value)> {
        // Remove all keys that have a key <= the new predecessor node.
        self.store.drain_filter(|(node_id, stub), _| {
            // Determine if the key is between the predecessor and us. Return false if it is, true otherwise.
            let node_id = *node_id;
            if predecessor < self.id {
                // No overflow between predecessor and us.
                !(predecessor < node_id && node_id < self.id)
            } else {
                // Overflow between predecessor and us.
                // If the node is between us and the predecessor (where there's no overflow)
                // than it's NOT between the predecessor and us.
                self.id < node_id && node_id < predecessor
            }
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