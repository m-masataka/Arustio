//! Virtual File System with mount support

use crate::FileSystem;
use async_trait::async_trait;
use bytes::Bytes;
use common::{Error, Result, file_metadata::FileMetadata, file_metadata::FileStatus};
use metadata::metadata::{MetadataStore, MountInfo};
use std::sync::Arc;
use ufs::{Ufs, UfsConfig, UfsOperations};

/// Virtual File System with mount table support
pub struct VirtualFsWithMounts {
    metadata: Arc<dyn MetadataStore>,
}

impl VirtualFsWithMounts {
    /// Create a new Virtual File System with mount support
    pub fn new(metadata: Arc<dyn MetadataStore>) -> Self {
        Self { metadata }
    }

    /// Restore mounts from metadata store
    pub async fn restore_mounts(&self) -> Result<()> {
        // self.mount_table.restore_mounts().await
        Ok(())
    }

    /// Generate a UFS path for a file based on VFS path
    /// Maps VFS path directly to UFS path by removing mount point prefix
    fn generate_ufs_path(&self, vfs_path: &str, mount_path: &str) -> String {
        // Remove mount point from VFS path to get relative path
        if vfs_path == mount_path {
            "/".to_string()
        } else if vfs_path.starts_with(&format!("{}/", mount_path)) {
            // Remove mount path and return relative path without leading slash
            vfs_path[mount_path.len()..]
                .trim_start_matches('/')
                .to_string()
        } else {
            // Fallback: use full path without leading slash
            vfs_path.trim_start_matches('/').to_string()
        }
    }

    /// Get the mount point for a given path
    async fn get_mount_for_path(&self, path: &str) -> Result<(Arc<Ufs>, String, String)> {
        let (ufs, relative_path) = self.resolve(path).await?;

        // Find mount point path
        let mount_points = self
            .metadata
            .list_mounts()
            .await
            .unwrap_or_default()
            .iter()
            .map(|m| m.path.clone())
            .collect::<Vec<String>>();
        let mut mount_path = "/".to_string();
        for mp in mount_points {
            if path == mp || path.starts_with(&format!("{}/", mp)) {
                if mp.len() > mount_path.len() {
                    mount_path = mp;
                }
            }
        }
        tracing::info!("Mount path for {}: {}", path, mount_path);

        Ok((ufs, relative_path, mount_path))
    }

    /// Mount table management
    /// Mount a UFS backend to a VFS path
    pub async fn mount(&self, vfs_path: &str, config: UfsConfig) -> Result<()> {
        let normalized_path = common::normalize_path(vfs_path)?;

        // Check if already mounted
        {
            let mounts = self.metadata.list_mounts().await?;
            for m in mounts.iter() {
                if m.path == normalized_path {
                    return Err(Error::PathAlreadyExists(format!(
                        "Path {} is already mounted",
                        normalized_path
                    )));
                }
            }
        }
        

        // Check if directory or file exists at the mount point
        {
            if let Some(existing) = self.metadata.get(&normalized_path).await? {
                if !existing.is_directory() {
                    return Err(Error::NotADirectory(format!(
                        "Mount point {} is not a directory",
                        normalized_path
                    )));
                }
            }
        }

        // Create Directory metadata if not exists
        if self.metadata.get(&normalized_path).await?.is_none() {
            self.mkdir(&normalized_path).await?;
        }

        // Save to metadata store first
        let mount_info = MountInfo {
            path: normalized_path.clone(),
            config: config.clone(),
        };
        self.metadata.save_mount(&mount_info).await?;

        tracing::info!("Mounted UFS to {}", normalized_path);
        Ok(())
    }

    /// Unmount a VFS path
    pub async fn unmount(&self, vfs_path: &str) -> Result<()> {
        let normalized_path = common::normalize_path(vfs_path)?;

        // Remove from mount table
        self.metadata
            .delete_mount(&normalized_path)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to unmount {}: {}", normalized_path, e))
            })?;
        tracing::info!("Unmounted {}", normalized_path);
        Ok(())
    }

    /// Resolve a VFS path to its mount point and relative path
    /// Returns (UFS instance, relative path within the UFS)
    pub async fn resolve(&self, vfs_path: &str) -> Result<(Arc<Ufs>, String)> {
        tracing::info!("Resolving VFS path: {}", vfs_path);
        let normalized_path = common::normalize_path(vfs_path)?;
        let mounts = self.metadata.list_mounts().await?;
        tracing::info!("Available mounts: {:?}", mounts);

        // Find the longest matching mount point
        let mut best_match: Option<(&String, UfsConfig)> = None;
        let mut best_match_len = 0;

        for m in mounts.iter() {
            if normalized_path == *m.path || normalized_path.starts_with(&format!("{}/", m.path)) {
                if m.path.len() > best_match_len {
                    best_match = Some((&m.path, m.config.clone()));
                    best_match_len = m.path.len();
                }
            }
        }

        match best_match {
            Some((mount_path, ufs_config)) => {
                // Calculate relative path
                let relative_path = if normalized_path == *mount_path {
                    "/".to_string()
                } else {
                    normalized_path[mount_path.len()..].to_string()
                };

                Ok((Arc::new(Ufs::new(ufs_config).await?), relative_path))
            }
            None => Err(Error::PathNotFound(format!(
                "No mount point found for path: {}",
                normalized_path
            ))),
        }
    }

    /// List all mount points
    pub async fn list_mounts(&self) -> Vec<MountInfo> {
        let mounts = self.metadata.list_mounts().await.unwrap_or_default();
        mounts
    }
}

#[async_trait]
impl FileSystem for VirtualFsWithMounts {
    async fn mkdir(&self, path: &str) -> Result<()> {
        let normalized_path = common::normalize_path(path)?;

        // Check if already exists
        if self.metadata.get(&normalized_path).await?.is_some() {
            return Err(Error::PathAlreadyExists(normalized_path));
        }
        
        // Create root directory if not exists
        let root_path = "/";
        if self.metadata.get(&root_path).await?.is_none() {
            let metadata = FileMetadata::new_directory(root_path.to_string(), None);
            self.metadata.put(metadata).await?;
        }

        // Get parent directory
        let parent_id = if let Some(parent_path) = common::parent_path(&normalized_path) {
            let parent = self
                .metadata
                .get(&parent_path)
                .await?
                .ok_or_else(|| Error::PathNotFound(parent_path.clone()))?;

            if !parent.is_directory() {
                return Err(Error::NotADirectory(parent_path));
            }

            Some(parent.id)
        } else {
            None
        };

        // Create directory metadata
        let metadata = FileMetadata::new_directory(normalized_path, parent_id);
        self.metadata.put(metadata).await?;

        Ok(())
    }

    async fn create(&self, path: &str, data: Bytes) -> Result<()> {
        let normalized_path = common::normalize_path(path)?;

        // Check if already exists
        if self.metadata.get(&normalized_path).await?.is_some() {
            return Err(Error::PathAlreadyExists(normalized_path));
        }

        // Get parent directory
        let parent_path = common::parent_path(&normalized_path)
            .ok_or_else(|| Error::InvalidPath("Cannot create file at root".to_string()))?;

        let parent = self
            .metadata
            .get(&parent_path)
            .await?
            .ok_or_else(|| Error::PathNotFound(parent_path.clone()))?;

        if !parent.is_directory() {
            return Err(Error::NotADirectory(parent_path));
        }

        // Resolve to mount point
        match self.get_mount_for_path(&normalized_path).await {
            Ok((ufs, _relative_path, mount_path)) => {
                // Generate UFS path from VFS path (maps directly to S3/MinIO path)
                let ufs_path = self.generate_ufs_path(&normalized_path, &mount_path);

                // TODO: DELETE THIS LOG
                tracing::info!("Creating file at VFS path");

                // Write data to UFS
                ufs.write(&ufs_path, data.clone()).await?;

                // Create file metadata
                let metadata = FileMetadata::new_file(
                    normalized_path,
                    data.len() as u64,
                    Some(parent.id),
                    Some(ufs_path),
                );
                self.metadata.put(metadata).await?;

                // TODO: DELETE THIS LOG
                tracing::info!("Finished creating file at VFS path");
                Ok(())
            }
            Err(Error::PathNotFound(_)) => {
                // No mount for this path: store metadata only
                tracing::warn!(
                    "No mount found for {}, storing metadata without backing UFS data",
                    normalized_path
                );
                let metadata = FileMetadata::new_file(
                    normalized_path,
                    data.len() as u64,
                    Some(parent.id),
                    None,
                );
                self.metadata.put(metadata).await?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let normalized_path = common::normalize_path(path)?;

        let metadata = self
            .metadata
            .get(&normalized_path)
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;

        if !metadata.is_file() {
            return Err(Error::NotAFile(normalized_path));
        }

        let ufs_path = metadata
            .ufs_path
            .ok_or_else(|| Error::Internal("File metadata missing UFS path".to_string()))?;

        // Resolve to mount point to get the right UFS instance
        let (ufs, _relative_path, _mount_path) = self.get_mount_for_path(&normalized_path).await?;

        ufs.read(&ufs_path).await
    }

    async fn write(&self, path: &str, data: Bytes) -> Result<()> {
        let normalized_path = common::normalize_path(path)?;

        let mut metadata = self
            .metadata
            .get(&normalized_path)
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;

        if !metadata.is_file() {
            return Err(Error::NotAFile(normalized_path));
        }

        let ufs_path = metadata
            .ufs_path
            .clone()
            .ok_or_else(|| Error::Internal("File metadata missing UFS path".to_string()))?;

        // Resolve to mount point
        let (ufs, _relative_path, _mount_path) = self.get_mount_for_path(&normalized_path).await?;

        // Write data to UFS
        ufs.write(&ufs_path, data.clone()).await?;

        // Update metadata
        metadata.size = data.len() as u64;
        metadata.modified_at = chrono::Utc::now();
        self.metadata.put(metadata).await?;

        Ok(())
    }

    async fn stat(&self, path: &str) -> Result<FileStatus> {
        let normalized_path = common::normalize_path(path)?;

        let metadata = self
            .metadata
            .get(&normalized_path)
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path))?;

        Ok(FileStatus::from(&metadata))
    }

    async fn list(&self, path: &str) -> Result<Vec<FileStatus>> {
        let normalized_path = common::normalize_path(path)?;

        // Get or create directory metadata
        let dir_metadata = match self.metadata.get(&normalized_path).await? {
            Some(meta) => {
                if !meta.is_directory() {
                    return Err(Error::NotADirectory(normalized_path));
                }
                meta
            }
            None => {
                // Auto-create directory metadata for the path
                let parent_id = if let Some(parent_path) = common::parent_path(&normalized_path) {
                    self.metadata.get(&parent_path).await?.map(|p| p.id)
                } else {
                    None
                };
                let meta = FileMetadata::new_directory(normalized_path.clone(), parent_id);
                self.metadata.put(meta).await?;
                self.metadata.get(&normalized_path).await?.ok_or_else(|| {
                    Error::Internal("Failed to create directory metadata".to_string())
                })?
            }
        };

        // Attempt to resolve a mount; if none, return metadata-only listing.
        match self.get_mount_for_path(&normalized_path).await {
            Ok((ufs, _relative_path, mount_path)) => {
                // Get UFS path prefix for listing
                tracing::info!("Listing directory at VFS path: {}", normalized_path);
                tracing::info!("Resolved to mount path: {}", mount_path);
                let ufs_prefix = self.generate_ufs_path(&normalized_path, &mount_path);

                // List files from UFS
                let (ufs_files, ufs_dirs) = ufs.list_dir(&ufs_prefix).await?;
                tracing::info!("UFS listed files: {:?}, dirs: {:?}", ufs_files, ufs_dirs);

                // Sync with metadata store
                let mut result = Vec::new();
                let mut metadata_names = std::collections::HashSet::new();

                // Get existing metadata children
                let existing_children = self.metadata.list_children(&dir_metadata.id).await?;
                for child in &existing_children {
                    let name = child.path.rsplit('/').next().unwrap_or(&child.path);
                    metadata_names.insert(name.to_string());
                    result.push(FileStatus::from(child));
                }

                // Add files from UFS that are not in metadata
                for filename in ufs_files {
                    if !metadata_names.contains(&filename) {
                        let file_path = if normalized_path == "/" {
                            format!("/{}", filename)
                        } else {
                            format!("{}/{}", normalized_path, filename)
                        };
                        let ufs_file_path = self.generate_ufs_path(&file_path, &mount_path);

                        // Get file size from UFS
                        let size = ufs.size(&ufs_file_path).await.unwrap_or(0);

                        // Create metadata for the file
                        let file_metadata = FileMetadata::new_file(
                            file_path.clone(),
                            size,
                            Some(dir_metadata.id),
                            Some(ufs_file_path),
                        );
                        self.metadata.put(file_metadata.clone()).await?;
                        result.push(FileStatus::from(&file_metadata));
                        metadata_names.insert(filename);
                    }
                }

                // Add directories from UFS that are not in metadata
                for dirname in ufs_dirs {
                    if !metadata_names.contains(&dirname) {
                        let dir_path = if normalized_path == "/" {
                            format!("/{}", dirname)
                        } else {
                            format!("{}/{}", normalized_path, dirname)
                        };

                        // Create metadata for the directory
                        let sub_dir_metadata =
                            FileMetadata::new_directory(dir_path.clone(), Some(dir_metadata.id));
                        self.metadata.put(sub_dir_metadata.clone()).await?;
                        result.push(FileStatus::from(&sub_dir_metadata));
                    }
                }

                Ok(result)
            }
            Err(Error::PathNotFound(_)) => {
                // No mount available; return metadata-only listing
                tracing::info!(
                    "No mount found for {}, returning metadata-only listing",
                    normalized_path
                );
                let existing_children = self.metadata.list_children(&dir_metadata.id).await?;
                Ok(existing_children
                    .into_iter()
                    .map(|child| FileStatus::from(&child))
                    .collect())
            }
            Err(e) => Err(e),
        }
    }

    async fn remove_file(&self, path: &str) -> Result<()> {
        let normalized_path = common::normalize_path(path)?;

        let metadata = self
            .metadata
            .get(&normalized_path)
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;

        if !metadata.is_file() {
            return Err(Error::NotAFile(normalized_path));
        }

        // Delete from UFS
        if let Some(ufs_path) = &metadata.ufs_path {
            let (ufs, _relative_path, _mount_path) =
                self.get_mount_for_path(&normalized_path).await?;
            ufs.delete(ufs_path).await?;
        }

        // Delete metadata
        self.metadata.delete(&normalized_path).await?;

        Ok(())
    }

    async fn remove_dir(&self, path: &str) -> Result<()> {
        let normalized_path = common::normalize_path(path)?;

        let metadata = self
            .metadata
            .get(&normalized_path)
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;

        if !metadata.is_directory() {
            return Err(Error::NotADirectory(normalized_path.clone()));
        }

        // Check if directory is empty
        let children = self.metadata.list_children(&metadata.id).await?;
        if !children.is_empty() {
            return Err(Error::Internal("Directory not empty".to_string()));
        }

        // Delete metadata
        self.metadata.delete(&normalized_path).await?;

        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let normalized_path = common::normalize_path(path)?;
        Ok(self.metadata.get(&normalized_path).await?.is_some())
    }
}
