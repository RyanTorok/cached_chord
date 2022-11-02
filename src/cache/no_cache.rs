use crate::cache::Cache;
use crate::{Address, ContentId};

pub struct NoCache;

impl NoCache {
    pub fn new() -> NoCache {
        NoCache {}
    }
}

impl Cache for NoCache {
    fn get(&mut self, _id: ContentId) -> Option<Address> {
        None
    }

    fn set(&mut self, _id: ContentId, _address: Address) {}
}