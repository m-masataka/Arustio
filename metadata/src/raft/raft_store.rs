//! Simple in-memory metadata store for testing and development

use crate::metadata::{MetadataStore, MountInfo};
use async_trait::async_trait;
use common::Result;
use common::file_metadata::FileMetadata;
use std::sync::Arc;
use ufs::UfsConfig;
use uuid::Uuid;

// Add import for RaftClient (adjust the path as needed)
use crate::raft::rocks_store::RocksStorage;
use crate::utils::{MOUNT_PREFIX, PATH_PREFIX};
use common::raft_client::RaftClient;

/// In-memory metadata store
#[derive(Clone)]
pub struct RaftMetadataStore {
    raft_client: Arc<RaftClient>,
    local_db: Arc<RocksStorage>,
}

impl RaftMetadataStore {
    pub fn new(raft_client: RaftClient, local_db: RocksStorage) -> Self {
        Self {
            raft_client: Arc::new(raft_client),
            local_db: Arc::new(local_db),
        }
    }
}

#[async_trait]
impl MetadataStore for RaftMetadataStore {
    async fn get(&self, path: &str) -> Result<Option<FileMetadata>> {
        let key = format!(
            "{}{}",
            std::str::from_utf8(PATH_PREFIX).expect("PATH_PREFIX must be valid UTF-8"),
            path
        );
        let value = self
            .local_db
            .get_kv_value(key.as_bytes())
            .map_err(|e| common::Error::Internal(format!("Failed to list mount keys: {}", e)))?;
        if let Some(value) = value {
            let file_metadata: common::meta::FileMetadata =
                prost::Message::decode(value.as_slice()).map_err(|e| {
                    common::Error::Internal(format!("Failed to decode FileMetadata entry: {}", e))
                })?;
            let metadata = FileMetadata::try_from(file_metadata).map_err(|e| {
                common::Error::Internal(format!("Failed to convert FileMetadata: {}", e))
            })?;
            tracing::info!("Got FileMetadata for path {}: {:?}", path, metadata);
            return Ok(Some(metadata));
        } else {
            tracing::info!("No FileMetadata found for path {}", path);
            Ok(None)
        }
    }

    async fn put(&self, metadata: FileMetadata) -> Result<()> {
        tracing::info!(
            "Putting FileMetadata for path {}: {:?}",
            metadata.path,
            metadata
        );
        self.raft_client
            .send_command(common::meta::MetaCmd {
                op: Some(common::meta::meta_cmd::Op::PutFileMeta(
                    common::meta::PutFileMeta {
                        full_path: metadata.path.clone(),
                        file_metadata: Some(metadata.into()),
                    },
                )),
            })
            .await
            .map_err(|e| common::Error::Internal(format!("Failed to send put command: {}", e)))?;
        Ok(())
    }

    async fn delete(&self, _path: &str) -> Result<()> {
        Ok(())
    }

    async fn list_children(&self, _parent_id: &Uuid) -> Result<Vec<FileMetadata>> {
        Ok(vec![])
    }

    async fn get_by_id(&self, id: &Uuid) -> Result<Option<FileMetadata>> {
        Ok(None)
    }

    async fn save_mount(&self, mount_info: &MountInfo) -> Result<()> {
        self.raft_client
            .send_command(common::meta::MetaCmd {
                op: Some(common::meta::meta_cmd::Op::Mount(common::meta::Mount {
                    full_path: mount_info.path.clone(),
                    ufs_config: Some(mount_info.config.clone().into()),
                    description: "Dummy Description".to_string(),
                })),
            })
            .await
            .map_err(|e| common::Error::Internal(format!("Failed to send mount command: {}", e)))?;

        Ok(())
    }

    async fn get_mount(&self, path: &str) -> Result<Option<MountInfo>> {
        Ok(None)
    }
    async fn delete_mount(&self, path: &str) -> Result<()> {
        Ok(())
    }

    async fn list_mounts(&self) -> Result<Vec<MountInfo>> {
        let keys = self
            .local_db
            .list_kv_keys(MOUNT_PREFIX)
            .map_err(|e| common::Error::Internal(format!("Failed to list mount keys: {}", e)))?;
        let mut mounts = Vec::new();
        use prost::Message;
        for key in keys {
            if let Some(value) = self
                .local_db
                .get_kv_value(key.as_bytes())
                .map_err(|e| common::Error::Internal(format!("Failed to get mount value: {}", e)))?
            {
                tracing::info!("Got mount value for key {}: {:?}", key, value);
                let mount = common::meta::Mount::decode(value.as_slice()).map_err(|e| {
                    common::Error::Internal(format!("Failed to decode mount entry: {}", e))
                })?;
                // let mount: common::meta::Mount = prost::Message::decode(value.as_slice())
                //     .map_err(|e| common::Error::Internal(format!("Failed to decode mount entry: {}", e)))?;
                mounts.push(MountInfo {
                    path: mount.full_path,
                    config: UfsConfig::try_from(mount.ufs_config.ok_or_else(|| {
                        common::Error::Internal("Missing ufs_config in mount entry".to_string())
                    })?)
                    .map_err(|e| {
                        common::Error::Internal(format!("Failed to convert UfsConfig: {}", e))
                    })?,
                });
            }
        }
        tracing::info!("Listing mounts: {:?}", mounts);
        Ok(mounts)
    }
}
