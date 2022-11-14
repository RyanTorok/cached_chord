use std::collections::VecDeque;
use crate::cache::{Cache, Entry};
use crate::{Address, ContentId};

pub struct FifoCache {
    capacity: usize,
    size: usize,
    store: VecDeque<Entry>,
}

impl FifoCache {
    pub fn new(capacity: usize) -> FifoCache {
        FifoCache {
            capacity,
            size: 0,
            store: Default::default(),
        }
    }
}

impl Cache for FifoCache {

    fn get(&mut self, id: ContentId) -> Option<Address> {
        Some(self.store.iter().find(|ent| ent.0.eq(&id) )?.1)
    }

    fn set(&mut self, id: ContentId, address: Address) {
        while self.size >= self.capacity {
            self.store.pop_front();
        }
        self.store.push_back((id, address))
    }
}
