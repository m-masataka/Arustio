//! Simple in-memory metadata store for testing and development

use common::Result;
use common::file_metadata::FileMetadata;
use crate::metadata::{MetadataStore, MountInfo};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// In-memory metadata store
pub struct InMemoryMetadataStore {
    data: Arc<RwLock<HashMap<String, FileMetadata>>>,
    mounts: Arc<RwLock<HashMap<String, MountInfo>>>,
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        let mut data = HashMap::new();

        // Initialize with root directory
        let root = FileMetadata::new_directory("/".to_string(), None);
        data.insert("/".to_string(), root);

        Self {
            data: Arc::new(RwLock::new(data)),
            mounts: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
    async fn get(&self, path: &str) -> Result<Option<FileMetadata>> {
        let data = self.data.read().await;
        Ok(data.get(path).cloned())
    }

    async fn put(&self, metadata: FileMetadata) -> Result<()> {
        let mut data = self.data.write().await;
        data.insert(metadata.path.clone(), metadata);
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let mut data = self.data.write().await;
        data.remove(path);
        Ok(())
    }

    async fn list_children(&self, parent_id: &Uuid) -> Result<Vec<FileMetadata>> {
        let data = self.data.read().await;
        let children: Vec<FileMetadata> = data
            .values()
            .filter(|m| m.parent_id.as_ref() == Some(parent_id))
            .cloned()
            .collect();
        Ok(children)
    }

    async fn get_by_id(&self, id: &Uuid) -> Result<Option<FileMetadata>> {
        let data = self.data.read().await;
        Ok(data.values().find(|m| &m.id == id).cloned())
    }

    async fn save_mount(&self, mount_info: &MountInfo) -> Result<()> {
        let mut mounts = self.mounts.write().await;
        mounts.insert(mount_info.path.clone(), mount_info.clone());
        Ok(())
    }

    async fn get_mount(&self, path: &str) -> Result<Option<MountInfo>> {
        let mounts = self.mounts.read().await;
        Ok(mounts.get(path).cloned())
    }

    async fn delete_mount(&self, path: &str) -> Result<()> {
        let mut mounts = self.mounts.write().await;
        mounts.remove(path);
        Ok(())
    }

    async fn list_mounts(&self) -> Result<Vec<MountInfo>> {
        let mounts = self.mounts.read().await;
        Ok(mounts.values().cloned().collect())
    }
}
