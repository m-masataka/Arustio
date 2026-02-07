use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};
use crate::common::Result;

use bytes::Bytes;
use moka::future::Cache;
use uuid::Uuid;

pub const DEFAULT_CAPACITY: u64 = 512 * 1024 * 1024;
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq)]
struct ChunkKey {
    file_id: Uuid,
    index: u64,
}

impl PartialEq for ChunkKey {
    fn eq(&self, other: &Self) -> bool {
        self.file_id == other.file_id && self.index == other.index
    }
}

impl Hash for ChunkKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_id.hash(state);
        self.index.hash(state);
    }
}

pub struct CacheManager {
    cache: Cache<ChunkKey, Arc<Vec<u8>>>,
}

impl CacheManager {
    pub fn new(
        capacity_bytes: u64,
    ) -> Self {
        let capacity = if capacity_bytes == 0 {
            DEFAULT_CAPACITY
        } else {
            capacity_bytes
        };
        Self {
            cache: Cache::builder().max_capacity(capacity).build(),
        }
    }

    pub async fn store_block(&self, file_id: Uuid, index: u64, data: Bytes) -> Result<()> {
        tracing::debug!("Storing block: file_id={}, index={}, size={}", file_id, index, data.len());
        self.cache
            .insert(ChunkKey { file_id, index }, Arc::new(data.to_vec()))
            .await;
        Ok(())
    }

    pub async fn read_block(&self, file_id: Uuid, index: u64) -> Result<Bytes> {
        tracing::debug!("Reading block: file_id={}, index={}", file_id, index);
        match self.cache.get(&ChunkKey { file_id, index }).await {
            Some(chunk) => Ok(Bytes::from(chunk.as_ref().clone())),
            None => Err(crate::common::Error::CacheMiss { file_id, index }),
        }
    }
}
