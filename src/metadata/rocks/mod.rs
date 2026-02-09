use async_trait::async_trait;
use prost::Message;
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, WriteBatch};
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

use crate::{
    block::node::BlockNode,
    common::{Error, Result},
    core::file_metadata::{FileMetadata, MountInfo},
    meta::{
        BlockNodeInfo as BlockNodeProto, FileMetadata as FileMetadataProto,
        Mount as MountInfoProto, PathConf as PathConfProto,
    },
    metadata::metadata::MetadataStore,
    metadata::utils::{
        MOUNT_PREFIX, PATH_PREFIX, kv_key_id, kv_key_mount_path, kv_key_path, kv_path_conf_key,
    },
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
        let filemeta_proto: FileMetadataProto = metadata.clone().into();
        let mut encoded = Vec::new();
        filemeta_proto
            .encode(&mut encoded)
            .map_err(|e| Error::Internal(format!("Failed to encode FileMetadata: {}", e)))?;
        let mut wb = WriteBatch::default();
        wb.put(kv_key_path(&metadata.path), encoded.clone());
        wb.put(kv_key_id(&metadata.id.to_string()), encoded);
        self.db.write(wb).map_err(map_rocks_err)
    }

    fn get_file_by_path(&self, path: &str) -> Result<Option<FileMetadata>> {
        let key = kv_key_path(path);
        let value = self.db.get(key).map_err(map_rocks_err)?;
        match value {
            Some(bytes) => {
                let filemeta_proto = FileMetadataProto::decode(&bytes[..]).map_err(|e| {
                    Error::Internal(format!("Failed to decode FileMetadata: {}", e))
                })?;
                let file_metadata: FileMetadata = filemeta_proto.try_into().map_err(|e| {
                    Error::Internal(format!(
                        "Failed to convert FileMetadataProto to FileMetadata: {}",
                        e
                    ))
                })?;
                Ok(Some(file_metadata))
            }
            None => Ok(None),
        }
    }

    fn get_file_by_id(&self, id: &Uuid) -> Result<Option<FileMetadata>> {
        let key = kv_key_id(&id.to_string());
        let value = self.db.get(key).map_err(map_rocks_err)?;
        match value {
            Some(bytes) => {
                let filemeta_proto = FileMetadataProto::decode(&bytes[..]).map_err(|e| {
                    Error::Internal(format!("Failed to decode FileMetadata: {}", e))
                })?;
                let file_metadata: FileMetadata = filemeta_proto.try_into().map_err(|e| {
                    Error::Internal(format!(
                        "Failed to convert FileMetadataProto to FileMetadata: {}",
                        e
                    ))
                })?;
                Ok(Some(file_metadata))
            }
            None => Ok(None),
        }
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
            let filemeta_proto = FileMetadataProto::decode(&raw_val[..])
                .map_err(|e| Error::Internal(format!("Failed to decode FileMetadata: {}", e)))?;
            let meta: FileMetadata = filemeta_proto.try_into().map_err(|e| {
                Error::Internal(format!(
                    "Failed to convert FileMetadataProto to FileMetadata: {}",
                    e
                ))
            })?;
            if meta.parent_id.as_ref() == Some(parent_id) {
                children.push(meta);
            }
        }
        Ok(children)
    }

    fn save_mount_internal(&self, mount_info: &MountInfo) -> Result<()> {
        tracing::info!("Saving mount info: {:?}", mount_info);
        let key = kv_key_mount_path(&mount_info.path);
        tracing::info!(
            "save_mount key='{}' bytes={:?}",
            String::from_utf8_lossy(&key),
            key
        );
        let m: MountInfoProto = mount_info.clone().into();
        let mut val = Vec::new();
        m.encode(&mut val)
            .map_err(|e| Error::Internal(format!("Failed to encode Mount entry: {}", e)))?;

        self.db.put(&key, &val).map_err(map_rocks_err)
    }

    fn get_mount_internal(&self, path: &str) -> Result<Option<MountInfo>> {
        let key = kv_key_mount_path(path);
        let value = self.db.get(key).map_err(map_rocks_err)?;
        let mount = match value {
            Some(bytes) => {
                let m_proto = MountInfoProto::decode(&bytes[..])
                    .map_err(|e| Error::Internal(format!("Failed to decode Mount entry: {}", e)))?;
                let mount_info: MountInfo = m_proto.try_into().map_err(|e| {
                    Error::Internal(format!("Failed to convert MountProto to MountInfo: {}", e))
                })?;
                Some(mount_info)
            }
            None => None,
        };
        Ok(mount)
    }

    fn delete_mount_internal(&self, path: &str) -> Result<()> {
        self.db
            .delete(kv_key_mount_path(path))
            .map_err(map_rocks_err)
    }

    fn list_mounts_internal(&self) -> Result<Vec<MountInfo>> {
        tracing::debug!(
            "list_mounts from prefix='{}'",
            String::from_utf8_lossy(MOUNT_PREFIX)
        );
        let mut mounts = Vec::new();
        let mut iter = self.db.prefix_iterator(MOUNT_PREFIX);
        for kv in iter.by_ref() {
            let kv_res = kv.map_err(map_rocks_err);
            let (raw_key, raw_val) = match kv_res {
                Ok(kv) => kv,
                Err(e) => {
                    tracing::error!("Error iterating mounts: {}", e);
                    // Egnore this error and continue
                    continue;
                }
            };
            if !raw_key.starts_with(MOUNT_PREFIX) {
                break;
            }
            let decoded = MountInfoProto::decode(&raw_val[..])
                .map_err(|e| Error::Internal(format!("Failed to decode Mount entry: {}", e)))
                .and_then(|m_proto| {
                    m_proto.try_into().map_err(|e| {
                        Error::Internal(format!("Failed to convert MountProto to MountInfo: {}", e))
                    })
                });
            let mount = match decoded {
                Ok(mount) => mount,
                Err(e) => {
                    tracing::error!("Error decoding mount info: {}", e);
                    // Ignore this error and continue
                    continue;
                }
            };
            mounts.push(mount);
        }
        Ok(mounts)
    }

    fn list_block_nodes_internal(&self) -> Result<Vec<BlockNode>> {
        let mut nodes = Vec::new();
        let mut iter = self
            .db
            .iterator(IteratorMode::From(b"block_node_", Direction::Forward));
        for kv in iter.by_ref() {
            let (raw_key, raw_val) = kv.map_err(map_rocks_err)?;
            if !raw_key.starts_with(b"block_node_") {
                break;
            }
            let blocknode_proto = BlockNodeProto::decode(&raw_val[..])
                .map_err(|e| Error::Internal(format!("Failed to decode BlockNode: {}", e)))?;
            let node: BlockNode = blocknode_proto.try_into().map_err(|e| {
                Error::Internal(format!(
                    "Failed to convert BlockNodeProto to BlockNode: {}",
                    e
                ))
            })?;
            nodes.push(node);
        }
        Ok(nodes)
    }

    fn put_block_node_internal(&self, block_node: BlockNode) -> Result<()> {
        let blocknode_proto: BlockNodeProto = block_node.clone().into();
        let mut encoded = Vec::new();
        blocknode_proto
            .encode(&mut encoded)
            .map_err(|e| Error::Internal(format!("Failed to encode BlockNode: {}", e)))?;
        self.db
            .put(
                format!("block_node_{}", block_node.node_id).as_bytes(),
                encoded,
            )
            .map_err(map_rocks_err)
    }

    fn save_path_conf_internal(&self, conf: &PathConfProto) -> Result<()> {
        let mut encoded = Vec::new();
        conf.encode(&mut encoded)
            .map_err(|e| Error::Internal(format!("Failed to encode PathConf: {}", e)))?;
        self.db
            .put(kv_path_conf_key(&conf.full_path), encoded)
            .map_err(map_rocks_err)
    }

    fn get_path_conf_internal(&self, path: &str) -> Result<Option<PathConfProto>> {
        let value = self.db.get(kv_path_conf_key(path)).map_err(map_rocks_err)?;
        match value {
            Some(bytes) => {
                let conf = PathConfProto::decode(&bytes[..])
                    .map_err(|e| Error::Internal(format!("Failed to decode PathConf: {}", e)))?;
                Ok(Some(conf))
            }
            None => Ok(None),
        }
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

    async fn put_block_node(&self, block_node: BlockNode) -> Result<()> {
        self.put_block_node_internal(block_node)
    }

    async fn list_block_nodes(&self) -> Result<Vec<BlockNode>> {
        self.list_block_nodes_internal()
    }

    async fn set_path_conf(&self, conf: PathConfProto) -> Result<()> {
        self.save_path_conf_internal(&conf)
    }

    async fn get_path_conf(&self, path: &str) -> Result<Option<PathConfProto>> {
        self.get_path_conf_internal(path)
    }
}

fn map_rocks_err(err: rocksdb::Error) -> Error {
    Error::Storage(err.to_string())
}
