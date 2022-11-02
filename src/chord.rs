use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use crate::cache::{Cache, make_cache};
use crate::{NodeId, Address, ContentId, ContentStub, Value, SUCCESSORS};

pub struct Node {
    id: NodeId,
    address: Address,
    finger_table: BTreeMap<NodeId, Address>,
    successors: BTreeSet<(NodeId, Address)>,
    predecessor: Option<(NodeId, Address)>,
    store: HashMap<ContentStub, Value>,
    cache: Option<Box<dyn Cache>>,
}

pub enum FindResult {
    Value(Value),
    Redirect(Address),
    Error(String),
}

impl Node {
    pub fn new(id: NodeId, address: Address) -> Node {
        Node {
            id, address, finger_table: BTreeMap::new(), successors: BTreeSet::new(), predecessor: None, store: Default::default(), cache: None
        }
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

    pub fn next_finger(&self, target: ContentId) -> FindResult {
        if target.0 == self.id {
            match self.store.get(&target.1) {
                None => FindResult::Error("No such object.".to_string()),
                Some(val) => FindResult::Value(*val)
            }
        } else {
            match self.finger_table.iter().rev().find(|ent| *ent.0 < target.0) {
                None => FindResult::Error("No available finger pointer.".to_string()),
                Some(ent) => {
                    FindResult::Redirect(*ent.1)
                },
            }
        }
    }

    pub fn populate_finger(&mut self, node: NodeId, addr: Address) {
        self.finger_table.insert(node, addr);
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

    pub fn init_cache(&mut self, cache_type: CacheType, size: usize) {
        self.cache = Some(make_cache(cache_type, size));
    }

    pub fn cache(&self) -> &Option<Box<dyn Cache>> {
        &self.cache
    }

    pub fn cache_mut(&mut self) -> &mut Option<Box<dyn Cache>> {
        &mut self.cache
    }
}

#[derive(clap::ValueEnum, Clone, Debug, Copy)]
pub enum CacheType {
    LRU,
    MRU,
    FIFO,
    LIFO,
    LFU
}

#[derive(clap::ValueEnum, Clone, Debug)]
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