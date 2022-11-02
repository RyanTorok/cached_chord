mod fifo;
mod lru;
mod mru;
mod lifo;
mod lfu;

use crate::{Address, cache::{
    fifo::FifoCache,
    lfu::LfuCache,
    lifo::LifoCache,
    lru::LruCache,
    mru::MruCache
}, chord::CacheType, NodeId};

type Entry = (NodeId, Address);

#[derive(Default)]
struct CacheStats {
    pub hits: u64,
    pub total: u64,
}

pub trait Cache {
    fn get(&mut self, id: NodeId) -> Option<Address>;
    fn set(&mut self, id: NodeId, address: Address);
}

pub fn make_cache(cache_type: CacheType, size: usize) -> Box<dyn Cache> {
    match cache_type {
        CacheType::LRU => Box::new(LruCache::new(size)),
        CacheType::MRU => Box::new(MruCache::new(size)),
        CacheType::FIFO => Box::new(FifoCache::new(size)),
        CacheType::LIFO => Box::new(LifoCache::new(size)),
        CacheType::LFU => Box::new(LfuCache::new(size))
    }
}