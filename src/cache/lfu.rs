use std::cmp::Ordering;
use std::collections::{BinaryHeap};
use crate::cache::{Cache, CacheStats, Entry};
use crate::{Address, NodeId};

#[derive(Eq, Ord, Clone, Copy)]
struct RankedEntry(u64, Entry);

impl PartialEq<Self> for RankedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialOrd for RankedEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

pub struct LfuCache {
    capacity: usize,
    size: usize,
    store: BinaryHeap<RankedEntry>,
    stats: CacheStats
}

impl LfuCache {
    pub fn new(capacity: usize) -> LfuCache {
        LfuCache {
            capacity,
            size: 0,
            store: Default::default(),
            stats: Default::default()
        }
    }
}

impl Cache for LfuCache {
    fn get(&mut self, id: NodeId) -> Option<Address> {
        let ent = *self.store.iter().find(|ent| (*ent).1.0 == id)?;
        self.store.retain(|e| ent.1.0 != e.1.0);
        self.store.push(RankedEntry(ent.0 + 1, (ent.1.0, ent.1.1)));
        Some(ent.1.1)
    }

    fn set(&mut self, id: NodeId, address: Address) {
        self.store.push(RankedEntry(0, (id, address)))
    }
}
