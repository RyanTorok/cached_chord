use std::collections::VecDeque;
use crate::cache::{Cache, Entry};
use crate::{Address, ContentId};

pub struct LifoCache {
    capacity: usize,
    size: usize,
    store: VecDeque<Entry>,
}

impl LifoCache {
    pub fn new(capacity: usize) -> LifoCache {
        LifoCache {
            capacity,
            size: 0,
            store: Default::default(),
        }
    }
}

impl Cache for LifoCache {

    fn get(&mut self, id: ContentId) -> Option<Address> {
        Some(self.store.iter().find(|ent| ent.0.eq(&id) )?.1)
    }

    fn set(&mut self, id: ContentId, address: Address) {
        while self.size >= self.capacity {
            self.store.pop_back();
        }
        self.store.push_back((id, address))
    }
}
