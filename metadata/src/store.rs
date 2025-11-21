//! RocksDB-based persistent storage for metadata

use common::{Error, Result};
use rocksdb::{DB, Options};
use std::path::Path;
use std::sync::Arc;

/// RocksDB-backed metadata storage
pub struct RocksDbStore {
    db: Arc<DB>,
}

impl RocksDbStore {
    /// Create or open a RocksDB store at the given path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = DB::open(&opts, path)
            .map_err(|e| Error::Storage(format!("Failed to open RocksDB: {}", e)))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .get(key)
            .map_err(|e| Error::Storage(format!("Failed to get from RocksDB: {}", e)))
    }

    /// Put a key-value pair
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .map_err(|e| Error::Storage(format!("Failed to put to RocksDB: {}", e)))
    }

    /// Delete a key
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.db
            .delete(key)
            .map_err(|e| Error::Storage(format!("Failed to delete from RocksDB: {}", e)))
    }

    /// Iterate over all key-value pairs with a given prefix
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let iter = self.db.iterator(rocksdb::IteratorMode::From(prefix, rocksdb::Direction::Forward));

        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item
                .map_err(|e| Error::Storage(format!("Failed to iterate RocksDB: {}", e)))?;

            // Check if key starts with prefix
            if !key.starts_with(prefix) {
                break;
            }

            results.push((key.to_vec(), value.to_vec()));
        }

        Ok(results)
    }
}
