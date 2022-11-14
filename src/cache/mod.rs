mod fifo;
mod lru;
mod mru;
mod lifo;
mod lfu;
mod no_cache;

use std::fmt::{Display, Formatter};
use crate::{Address, cache::{
    fifo::FifoCache,
    lfu::LfuCache,
    lifo::LifoCache,
    lru::LruCache,
    mru::MruCache,
    no_cache::NoCache
}, node::CacheType, ContentId};

type Entry = (ContentId, Address);

#[derive(Default)]
pub struct CacheStats {
    hits: u64,
    total: u64,
}

impl CacheStats {
    pub fn new() -> CacheStats {
        CacheStats { hits: 0, total: 0 }
    }

    pub fn hit(&mut self) {
        self.total += 1;
        self.hits += 1;
    }

    pub fn miss(&mut self) {
        self.total += 1;
    }
}

impl Display for CacheStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("Hit Rate: {}/{} ({:.1}%)", self.hits, self.total, ((self.hits as f64) / (self.total as f64) * 1000.0).round() / 10.0))
    }
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