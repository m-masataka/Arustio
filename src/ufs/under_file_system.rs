//! UFS (Unified File System) layer
//!
//! This module provides a unified interface to multiple object storage backends
//! using the object_store crate. Supported backends include:
//! - Amazon S3
//! - Google Cloud Storage (GCS)
//! - Azure Blob Storage
//! - Local filesystem (for testing)

use crate::common::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{ObjectMeta, ObjectStore, WriteMultipart, path::Path as ObjectPath};
use std::sync::Arc;

use crate::ufs::config::UfsConfig;
use crate::ufs::store::UnifiedStore;

use futures::stream::BoxStream;

/// Trait for UFS operations
#[async_trait]
pub trait UfsOperations: Send + Sync {
    /// Read an object from UFS
    async fn read(&self, path: &str) -> Result<(ObjectMeta, BoxStream<'static, Result<Bytes>>)>;

    /// Read an object from UFS with range
    async fn read_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes>;

    /// Write an object to UFS
    async fn write(&self, path: &str, data: Bytes) -> Result<()>;

    /// Open a multipart write stream to UFS
    async fn open_multipart_write(&self, path: &str) -> Result<WriteMultipart>;

    /// Delete an object from UFS
    async fn delete(&self, path: &str) -> Result<()>;

    /// Check if an object exists
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Get object size
    async fn size(&self, path: &str) -> Result<u64>;

    /// List objects with a given prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// List objects in a directory (non-recursive)
    /// Returns (files, directories)
    async fn list_dir(&self, prefix: &str) -> Result<(Vec<String>, Vec<String>)>;
}

/// UFS implementation wrapping object_store
pub struct Ufs {
    store: Arc<dyn ObjectStore>,
}

impl Ufs {
    /// Create a new UFS instance from configuration
    pub async fn new(config: UfsConfig) -> Result<Self> {
        let store = UnifiedStore::from_config(config).await?;
        Ok(Self { store })
    }

    /// Create a new UFS instance with a custom object store
    pub fn with_store(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Get Config of UFS
    pub fn get_config(&self) -> String {
        return format!("{:?}", self.store);
    }

    /// Convert a string path to ObjectPath
    fn to_object_path(path: &str) -> ObjectPath {
        ObjectPath::from(path.trim_start_matches('/'))
    }
}

#[async_trait]
impl UfsOperations for Ufs {
    async fn read(&self, path: &str) -> Result<(ObjectMeta, BoxStream<'static, Result<Bytes>>)> {
        use futures::stream::StreamExt;

        let object_path = Self::to_object_path(path);

        let get_result = self
            .store
            .get(&object_path)
            .await
            .map_err(|e| Error::Storage(format!("Failed to get {}: {}", path, e)))?;

        let meta = get_result.meta.clone();

        let stream = get_result
            .into_stream()
            .map(|res| res.map_err(|e| Error::Storage(format!("Failed to read data from: {}", e))))
            .boxed();

        Ok((meta, stream))
    }

    async fn read_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        let object_path = Self::to_object_path(path);

        let data = self
            .store
            .get_range(&object_path, start..end)
            .await
            .map_err(|e| {
                Error::Storage(format!(
                    "Failed to get range {}-{} of {}: {}",
                    start, end, path, e
                ))
            })?;
        Ok(data)
    }

    async fn write(&self, path: &str, data: Bytes) -> Result<()> {
        let object_path = Self::to_object_path(path);
        self.store
            .put(&object_path, data.into())
            .await
            .map_err(|e| Error::Storage(format!("Failed to write {}: {}", path, e)))?;
        Ok(())
    }

    async fn open_multipart_write(&self, path: &str) -> Result<WriteMultipart> {
        // Convert Steram to multipart upload
        let object_path = Self::to_object_path(path);
        let upload = self.store.put_multipart(&object_path).await.unwrap();
        Ok(WriteMultipart::new(upload))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let object_path = Self::to_object_path(path);
        self.store
            .delete(&object_path)
            .await
            .map_err(|e| Error::Storage(format!("Failed to delete {}: {}", path, e)))?;
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let object_path = Self::to_object_path(path);
        match self.store.head(&object_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::Storage(format!(
                "Failed to check existence of {}: {}",
                path, e
            ))),
        }
    }

    async fn size(&self, path: &str) -> Result<u64> {
        let object_path = Self::to_object_path(path);
        let meta =
            self.store.head(&object_path).await.map_err(|e| {
                Error::Storage(format!("Failed to get metadata for {}: {}", path, e))
            })?;
        Ok(meta.size as u64)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let object_path = Self::to_object_path(prefix);
        let list_result = self.store.list(Some(&object_path));

        use futures::stream::TryStreamExt;
        let paths: Vec<String> = list_result
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| Error::Storage(format!("Failed to list {}: {}", prefix, e)))?
            .into_iter()
            .map(|meta| format!("/{}", meta.location))
            .collect();

        Ok(paths)
    }

    async fn list_dir(&self, prefix: &str) -> Result<(Vec<String>, Vec<String>)> {
        let prefix_path = prefix.trim_start_matches('/');
        let prefix_path = if prefix_path.is_empty() || prefix_path == "/" {
            None
        } else {
            Some(ObjectPath::from(prefix_path))
        };

        let list_result = self
            .store
            .list_with_delimiter(prefix_path.as_ref())
            .await
            .map_err(|e| Error::Storage(format!("Failed to list directory {}: {}", prefix, e)))?;

        // Get files
        let files: Vec<String> = list_result
            .objects
            .into_iter()
            .map(|meta| {
                let location = meta.location.to_string();
                // Return just the filename, not the full path
                location.rsplit('/').next().unwrap_or(&location).to_string()
            })
            .collect();

        // Get directories (common prefixes)
        let dirs: Vec<String> = list_result
            .common_prefixes
            .into_iter()
            .map(|path| {
                let path_str = path.to_string();
                // Extract directory name from the path
                path_str
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or(&path_str)
                    .to_string()
            })
            .collect();

        Ok((files, dirs))
    }
}
