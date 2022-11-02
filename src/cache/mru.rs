use std::collections::VecDeque;
use crate::cache::{Cache, CacheStats, Entry};
use crate::{Address, ContentId};

pub struct MruCache {
    capacity: usize,
    size: usize,
    store: VecDeque<Entry>,
    stats: CacheStats
}

impl MruCache {
    pub fn new(capacity: usize) -> MruCache {
        MruCache {
            capacity,
            size: 0,
            store: Default::default(),
            stats: Default::default()
        }
    }
}

impl Cache for MruCache {

    fn get(&mut self, id: ContentId) -> Option<Address> {
        let index = self.store.iter().position(|ent| ent.0.eq(&id))?;
        let entry = self.store.remove(index).expect("unreachable");
        let addr = entry.1.clone();
        self.store.push_back(entry);
        Some(addr)
    }

    fn set(&mut self, id: ContentId, address: Address) {
        while self.size >= self.capacity {
            self.store.pop_back();
        }
        self.store.push_back((id, address))
    }
}
