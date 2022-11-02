use std::collections::VecDeque;
use crate::cache::{Cache, CacheStats, Entry};
use crate::{Address, NodeId};

pub struct LifoCache {
    capacity: usize,
    size: usize,
    store: VecDeque<Entry>,
    stats: CacheStats
}

impl LifoCache {
    pub fn new(capacity: usize) -> LifoCache {
        LifoCache {
            capacity,
            size: 0,
            store: Default::default(),
            stats: Default::default()
        }
    }
}

impl Cache for LifoCache {

    fn get(&mut self, id: NodeId) -> Option<Address> {
        Some(self.store.iter().find(|ent| ent.0.eq(&id) )?.1)
    }

    fn set(&mut self, id: NodeId, address: Address) {
        while self.size >= self.capacity {
            self.store.pop_back();
        }
        self.store.push_back((id, address))
    }
}
