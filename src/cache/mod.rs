mod fifo;
mod lru;
mod mru;
mod lifo;
mod lfu;
mod no_cache;

use crate::{Address, cache::{
    fifo::FifoCache,
    lfu::LfuCache,
    lifo::LifoCache,
    lru::LruCache,
    mru::MruCache,
    no_cache::NoCache
}, chord::CacheType, ContentId};

type Entry = (ContentId, Address);

#[derive(Default)]
struct CacheStats {
    pub hits: u64,
    pub total: u64,
}

pub trait Cache {
    fn get(&mut self, id: ContentId) -> Option<Address>;
    fn set(&mut self, id: ContentId, address: Address);
}

pub fn default_cache() -> impl Cache {
    NoCache::new()
}

pub fn make_cache(cache_type: CacheType, size: usize) -> Box<dyn Cache + Send> {
    match cache_type {
        CacheType::None => {Box::new(NoCache::new())}
        CacheType::LRU => Box::new(LruCache::new(size)),
        CacheType::MRU => Box::new(MruCache::new(size)),
        CacheType::FIFO => Box::new(FifoCache::new(size)),
        CacheType::LIFO => Box::new(LifoCache::new(size)),
        CacheType::LFU => Box::new(LfuCache::new(size)),
    }
}