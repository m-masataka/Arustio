//! Simple in-memory metadata store for testing and development

use crate::{
    core::file_metadata::MountInfo,
    metadata::{
        metadata::MetadataStore,
        raft::linearizable_read::LinearizableReadHandle,
        raft::rocks_store::RocksStorage,
        utils::{MOUNT_PREFIX, PATH_PREFIX, kv_key_id, kv_key_mount_path, kv_key_path},
    },
    ufs::config::UfsConfig,
    core::file_metadata::FileMetadata,
    block::node::BlockNode,
    common::{
        raft_client::RaftClient,
        {Error, Result},
    },
    meta::{
        FileMetadata as ProtoFileMetadata,
        PutFileMeta,
        MetaCmd,
        meta_cmd,
        Mount,
        Unmount,
    },
};
use prost::Message;
use std::sync::Arc;
use async_trait::async_trait;
use uuid::Uuid;

/// Raft-based metadata store implementation
#[derive(Clone)]
pub struct RaftMetadataStore {
    raft_client: Arc<RaftClient>,
    local_db: Arc<RocksStorage>,
    read_handle: LinearizableReadHandle,
}

impl RaftMetadataStore {
    pub fn new(
        raft_client: RaftClient,
        local_db: RocksStorage,
        read_handle: LinearizableReadHandle,
    ) -> Self {
        Self {
            raft_client: Arc::new(raft_client),
            local_db: Arc::new(local_db),
            read_handle,
        }
    }

    async fn linearized(&self) -> Result<()> {
        self.read_handle.wait().await
    }

    fn decode_file_metadata(bytes: &[u8]) -> Result<FileMetadata> {
        let proto = ProtoFileMetadata::decode(bytes)
            .map_err(|e| Error::Internal(format!("Failed to decode FileMetadata entry: {}", e)))?;
        FileMetadata::try_from(proto)
            .map_err(|e| Error::Internal(format!("Failed to convert FileMetadata: {}", e)))
    }

    fn read_file_metadata(&self, key: &[u8]) -> Result<Option<FileMetadata>> {
        let value = self
            .local_db
            .get_kv_value(key)
            .map_err(|e| Error::Internal(format!("Failed to read metadata: {}", e)))?;
        value
            .map(|bytes| Self::decode_file_metadata(bytes.as_slice()))
            .transpose()
    }
}

#[async_trait]
impl MetadataStore for RaftMetadataStore {
    async fn get(&self, path: &str) -> Result<Option<FileMetadata>> {
        self.linearized().await?;
        let key = kv_key_path(path);
        let meta = self.read_file_metadata(&key)?;
        if meta.is_none() {
            tracing::debug!("No FileMetadata found for path {}", path);
        }
        Ok(meta)
    }

    async fn put(&self, metadata: FileMetadata) -> Result<()> {
        tracing::debug!(
            "Putting FileMetadata for path {}: {:?}",
            metadata.path,
            metadata
        );
        self.raft_client
            .send_command(MetaCmd {
                op: Some(meta_cmd::Op::PutFileMeta(
                    PutFileMeta {
                        full_path: metadata.path.clone(),
                        file_metadata: Some(metadata.into()),
                    },
                )),
            })
            .await
            .map_err(|e| Error::Internal(format!("Failed to send put command: {}", e)))?;
        Ok(())
    }

    async fn delete(&self, _path: &str) -> Result<()> {
        Err(Error::Internal(
            "Delete operation is not implemented for Raft metadata store".into(),
        ))
    }

    async fn list_children(&self, parent_id: &Uuid) -> Result<Vec<FileMetadata>> {
        self.linearized().await?;
        let keys = self
            .local_db
            .list_kv_keys(PATH_PREFIX)
            .map_err(|e| Error::Internal(format!("Failed to list keys: {}", e)))?;
        let mut children = Vec::new();
        for key in keys {
            if let Some(meta) = self.read_file_metadata(key.as_bytes())? {
                if meta.parent_id.as_ref() == Some(parent_id) {
                    children.push(meta);
                }
            }
        }
        Ok(children)
    }

    async fn get_by_id(&self, id: &Uuid) -> Result<Option<FileMetadata>> {
        self.linearized().await?;
        let key = kv_key_id(&id.to_string());
        self.read_file_metadata(&key)
    }

    async fn save_mount(&self, mount_info: &MountInfo) -> Result<()> {
        self.raft_client
            .send_command(MetaCmd {
                op: Some(meta_cmd::Op::Mount(Mount {
                    full_path: mount_info.path.clone(),
                    ufs_config: Some(mount_info.ufs_config.clone().into()),
                    description: "Dummy Description".to_string(),
                })),
            })
            .await
            .map_err(|e| Error::Internal(format!("Failed to send mount command: {}", e)))?;

        Ok(())
    }

    async fn get_mount(&self, path: &str) -> Result<Option<MountInfo>> {
        self.linearized().await?;
        let key = kv_key_mount_path(path);
        let value = self
            .local_db
            .get_kv_value(&key)
            .map_err(|e| Error::Internal(format!("Failed to read mount: {}", e)))?;
        if let Some(value) = value {
            let mount = Mount::decode(value.as_slice())
                .map_err(|e| Error::Internal(format!("Failed to decode mount entry: {}", e)))?;
            let config = mount
                .ufs_config
                .ok_or_else(|| Error::Internal("Missing ufs_config in mount entry".into()))?;
            Ok(Some(MountInfo {
                path: mount.full_path,
                ufs_config: UfsConfig::try_from(config)
                    .map_err(|e| Error::Internal(format!("Failed to convert UfsConfig: {}", e)))?,
                description: None,
            }))
        } else {
            Ok(None)
        }
    }
    async fn delete_mount(&self, path: &str) -> Result<()> {
        self.raft_client
            .send_command(MetaCmd {
                op: Some(meta_cmd::Op::Unmount(Unmount {
                    full_path: path.to_string(),
                })),
            })
            .await
            .map_err(|e| Error::Internal(format!("Failed to send unmount command: {}", e)))
    }

    async fn list_mounts(&self) -> Result<Vec<MountInfo>> {
        self.linearized().await?;
        let keys = self
            .local_db
            .list_kv_keys(MOUNT_PREFIX)
            .map_err(|e| Error::Internal(format!("Failed to list mount keys: {}", e)))?;
        let mut mounts = Vec::new();
        for key in keys {
            if let Some(value) = self
                .local_db
                .get_kv_value(key.as_bytes())
                .map_err(|e| Error::Internal(format!("Failed to get mount value: {}", e)))?
            {
                tracing::debug!("Got mount value for key {}: {:?}", key, value);
                let mount = Mount::decode(value.as_slice())
                    .map_err(|e| Error::Internal(format!("Failed to decode mount entry: {}", e)))?;
                mounts.push(MountInfo {
                    path: mount.full_path,
                    ufs_config: UfsConfig::try_from(mount.ufs_config.ok_or_else(|| {
                        Error::Internal("Missing ufs_config in mount entry".to_string())
                    })?)
                    .map_err(|e| Error::Internal(format!("Failed to convert UfsConfig: {}", e)))?,
                    description: None,
                });
            }
        }
        tracing::debug!("Listing mounts: {:?}", mounts);
        Ok(mounts)
    }

    async fn put_block_node(&self, block_node: BlockNode) -> Result<()> {
        self.raft_client
            .send_command(MetaCmd {
                op: Some(meta_cmd::Op::AddBlockNode(
                    crate::meta::AddBlockNode {
                        node: Some(block_node.into()),
                    },
                )),
            })
            .await
            .map_err(|e| Error::Internal(format!("Failed to send put block node command: {}", e)))?;
        Ok(())
    }

    async fn list_block_nodes(&self) -> Result<Vec<BlockNode>> {
        let nodes_proto = self.raft_client.get_block_nodes().await?;
        let nodes: Vec<BlockNode> = nodes_proto
            .iter()
            .cloned()
            .map(|proto_node| BlockNode::try_from(proto_node))
            .collect::<Result<Vec<BlockNode>>>()?;
        Ok(nodes)
    }
}
