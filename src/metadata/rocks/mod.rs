use std::path::Path;
use std::sync::Arc;
use async_trait::async_trait;
use bincode;
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, WriteBatch};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::{
    metadata::metadata::{MetadataStore, MountInfo},
    metadata::utils::{MOUNT_PREFIX, PATH_PREFIX, kv_key_id, kv_key_mount_path, kv_key_path},
    core::file_metadata::FileMetadata,
    common::{Error, Result},
    block::node::BlockNode,
};

/// RocksDB-backed metadata store for single-node deployments.
#[derive(Clone)]
pub struct RocksMetadataStore {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

impl RocksMetadataStore {
    /// Open or create the RocksDB instance located at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DBWithThreadMode::<MultiThreaded>::open(&opts, path).map_err(map_rocks_err)?;
        let store = Self { db: Arc::new(db) };
        store.ensure_root_directory()?;
        Ok(store)
    }

    fn ensure_root_directory(&self) -> Result<()> {
        if self.get_file_by_path("/")?.is_none() {
            let root = FileMetadata::new_directory("/".to_string(), None);
            self.store_metadata(root)?;
        }
        Ok(())
    }

    fn store_metadata(&self, metadata: FileMetadata) -> Result<()> {
        let encoded = encode_value(&metadata)?;
        let mut wb = WriteBatch::default();
        wb.put(kv_key_path(&metadata.path), encoded.clone());
        wb.put(kv_key_id(&metadata.id.to_string()), encoded);
        self.db.write(wb).map_err(map_rocks_err)
    }

    fn get_file_by_path(&self, path: &str) -> Result<Option<FileMetadata>> {
        let key = kv_key_path(path);
        let value = self.db.get(key).map_err(map_rocks_err)?;
        value
            .map(|bytes| decode_value::<FileMetadata>(&bytes))
            .transpose()
    }

    fn get_file_by_id(&self, id: &Uuid) -> Result<Option<FileMetadata>> {
        let key = kv_key_id(&id.to_string());
        let value = self.db.get(key).map_err(map_rocks_err)?;
        value
            .map(|bytes| decode_value::<FileMetadata>(&bytes))
            .transpose()
    }

    fn delete_metadata(&self, path: &str) -> Result<()> {
        if let Some(existing) = self.get_file_by_path(path)? {
            let mut wb = WriteBatch::default();
            wb.delete(kv_key_path(path));
            wb.delete(kv_key_id(&existing.id.to_string()));
            self.db.write(wb).map_err(map_rocks_err)?;
        }
        Ok(())
    }

    fn list_children_of(&self, parent_id: &Uuid) -> Result<Vec<FileMetadata>> {
        let mut children = Vec::new();
        let mut iter = self
            .db
            .iterator(IteratorMode::From(PATH_PREFIX, Direction::Forward));
        for kv in iter.by_ref() {
            let (raw_key, raw_val) = kv.map_err(map_rocks_err)?;
            if !raw_key.starts_with(PATH_PREFIX) {
                break;
            }
            let meta: FileMetadata = decode_value(&raw_val)?;
            if meta.parent_id.as_ref() == Some(parent_id) {
                children.push(meta);
            }
        }
        Ok(children)
    }

    fn save_mount_internal(&self, mount_info: &MountInfo) -> Result<()> {
        let encoded = encode_value(mount_info)?;
        self.db
            .put(kv_key_mount_path(&mount_info.path), encoded)
            .map_err(map_rocks_err)
    }

    fn get_mount_internal(&self, path: &str) -> Result<Option<MountInfo>> {
        let key = kv_key_mount_path(path);
        let value = self.db.get(key).map_err(map_rocks_err)?;
        value
            .map(|bytes| decode_value::<MountInfo>(&bytes))
            .transpose()
    }

    fn delete_mount_internal(&self, path: &str) -> Result<()> {
        self.db
            .delete(kv_key_mount_path(path))
            .map_err(map_rocks_err)
    }

    fn list_mounts_internal(&self) -> Result<Vec<MountInfo>> {
        let mut mounts = Vec::new();
        let mut iter = self
            .db
            .iterator(IteratorMode::From(MOUNT_PREFIX, Direction::Forward));
        for kv in iter.by_ref() {
            let (raw_key, raw_val) = kv.map_err(map_rocks_err)?;
            if !raw_key.starts_with(MOUNT_PREFIX) {
                break;
            }
            mounts.push(decode_value(&raw_val)?);
        }
        Ok(mounts)
    }
}

#[async_trait]
impl MetadataStore for RocksMetadataStore {
    async fn get(&self, path: &str) -> Result<Option<FileMetadata>> {
        self.get_file_by_path(path)
    }

    async fn put(&self, metadata: FileMetadata) -> Result<()> {
        self.store_metadata(metadata)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.delete_metadata(path)
    }

    async fn list_children(&self, parent_id: &Uuid) -> Result<Vec<FileMetadata>> {
        self.list_children_of(parent_id)
    }

    async fn get_by_id(&self, id: &Uuid) -> Result<Option<FileMetadata>> {
        self.get_file_by_id(id)
    }

    async fn save_mount(&self, mount_info: &MountInfo) -> Result<()> {
        self.save_mount_internal(mount_info)
    }

    async fn get_mount(&self, path: &str) -> Result<Option<MountInfo>> {
        self.get_mount_internal(path)
    }

    async fn delete_mount(&self, path: &str) -> Result<()> {
        self.delete_mount_internal(path)
    }

    async fn list_mounts(&self) -> Result<Vec<MountInfo>> {
        self.list_mounts_internal()
    }

    async fn put_block_node(&self, _block_node: BlockNode) -> Result<()> {
        Err(Error::Internal(
            "put_block_node is not implemented in RocksMetadataStore".to_string(),
        ))
    }

    async fn list_block_nodes(&self) -> Result<Vec<BlockNode>> {
        Err(Error::Internal(
            "list_block_nodes is not implemented in RocksMetadataStore".to_string(),
        ))
    }
}

fn encode_value<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serialize(value)
        .map_err(|e| Error::Serialization(format!("Failed to serialize metadata: {e}")))
}

fn decode_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    bincode::deserialize(bytes)
        .map_err(|e| Error::Serialization(format!("Failed to deserialize metadata: {e}")))
}

fn map_rocks_err(err: rocksdb::Error) -> Error {
    Error::Storage(err.to_string())
}
