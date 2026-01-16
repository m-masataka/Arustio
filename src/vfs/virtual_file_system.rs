//! Virtual File System with mount support

use async_stream::try_stream;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use crate::{
    block::node::BlockNode, cache::manager::CacheManager,
    common::{
        Error, Result, file_client::FileClient, normalize_path, parent_path
    }, core::file_metadata::{BlockDesc, FileMetadata}, metadata::metadata::{MetadataStore, MountInfo}, ufs::{
        config::UfsConfig, under_file_system::{Ufs, UfsOperations}
    }, vfs::{
        file_system::FileSystem,
        utils::{get_block_node_pairs, get_node_by_block, plan_chunk_layout},
    }
};

use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use std::sync::Arc;

/// Virtual File System with mount table support
pub struct VirtualFileSystem {
    metadata_store: Arc<dyn MetadataStore>,
    cache: Arc<CacheManager>,
    file_client: FileClient,
}

impl VirtualFileSystem {
    /// Create a new Virtual File System with mount support
    pub fn new(
        metadata_store: Arc<dyn MetadataStore>,
        cache: Arc<CacheManager>,
    ) -> Self {
        Self {
            metadata_store,
            cache,
            file_client: FileClient::new(),
        }
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
            .metadata_store
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
        tracing::debug!("Mount path for {}: {}", path, mount_path);

        Ok((ufs, relative_path, mount_path))
    }

    /// Mount table management
    /// Mount a UFS backend to a VFS path
    pub async fn mount(&self, vfs_path: &str, config: UfsConfig) -> Result<()> {
        let normalized_path = normalize_path(vfs_path)?;

        // Check if already mounted
        {
            let mounts = self.metadata_store.list_mounts().await?;
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
            if let Some(existing) = self.metadata_store.get(&normalized_path).await? {
                if !existing.is_directory() {
                    return Err(Error::NotADirectory(format!(
                        "Mount point {} is not a directory",
                        normalized_path
                    )));
                }
            }
        }

        // Create Directory metadata if not exists
        if self.metadata_store.get(&normalized_path).await?.is_none() {
            self.mkdir(&normalized_path).await?;
        }

        // Save to metadata store first
        let mount_info = MountInfo {
            path: normalized_path.clone(),
            config: config.clone(),
        };
        self.metadata_store.save_mount(&mount_info).await?;

        tracing::info!("Mounted UFS to {}", normalized_path);
        Ok(())
    }

    /// Unmount a VFS path
    pub async fn unmount(&self, vfs_path: &str) -> Result<()> {
        let normalized_path = normalize_path(vfs_path)?;

        // Remove from mount table
        self.metadata_store
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
        tracing::debug!("Resolving VFS path: {}", vfs_path);
        let normalized_path = normalize_path(vfs_path)?;
        let mounts = self.metadata_store.list_mounts().await?;
        tracing::debug!("Available mounts: {:?}", mounts);

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
        let mounts = self.metadata_store.list_mounts().await.unwrap_or_default();
        mounts
    }
}

#[async_trait]
impl FileSystem for VirtualFileSystem {
    async fn mkdir(&self, path: &str) -> Result<()> {
        let normalized_path = normalize_path(path)?;

        // Check if already exists
        if self.metadata_store.get(&normalized_path).await?.is_some() {
            return Err(Error::PathAlreadyExists(normalized_path));
        }

        // Create root directory if not exists
        let root_path = "/";
        if self.metadata_store.get(&root_path).await?.is_none() {
            let metadata = FileMetadata::new_directory(root_path.to_string(), None);
            self.metadata_store.put(metadata).await?;
        }

        // Get parent directory
        let parent_id = if let Some(parent_path) = parent_path(&normalized_path) {
            let parent = self
                .metadata_store
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
        self.metadata_store.put(metadata).await?;

        Ok(())
    }

    async fn read(&self, path: &str) -> Result<(FileMetadata, BoxStream<'static, Result<Bytes>>)> {
        let normalized_path = normalize_path(path)?;
        let block_nodes = self.metadata_store.list_block_nodes().await?;

        if let Some(metadata) = self.metadata_store.get(&normalized_path).await? {
            if !metadata.is_file() {
                return Err(Error::NotAFile(normalized_path));
            }

            tracing::debug!("Attempting to read file {} from cache", normalized_path);
            let blocks = plan_chunk_layout(metadata.size).await;

            let blocks_nodes_pairs: Vec<(BlockDesc, BlockNode)> = get_block_node_pairs(block_nodes, metadata.id, blocks).await?;

            tracing::debug!(
                "Found {} blocks for file {} in cache",
                blocks_nodes_pairs.len(),
                normalized_path
            );
            tracing::warn!("blocks_nodes_pairs: {:?}", blocks_nodes_pairs);

            let file_client = self.file_client.clone();
            let normalized_path = normalized_path.clone();
            let metadata = metadata.clone();
            let blocks_nodes_pairs = blocks_nodes_pairs.clone();

            let stream: BoxStream<'static, Result<Bytes>> = try_stream! {
                for (block_desc, block_node) in blocks_nodes_pairs {
                    tracing::debug!(
                        "Reading chunk {} owned by node {}",
                        metadata.id,
                        block_node.node_id
                    );
                    let node_info = block_node.clone();
                    let dst_url = node_info.url.clone();

                    // Assume read_block returns Result<Option<Bytes>>
                    if let Some(data) = file_client
                        .read_block(
                            &normalized_path,
                            metadata.id,
                            block_desc.clone(),
                            dst_url,
                        )
                        .await?
                    {
                        // Do not wrap in Ok here; yield the bytes directly
                        tracing::debug!(
                            "Successfully read block index {} of file {} from cache node {}",
                            block_desc.index,
                            normalized_path,
                            node_info.node_id
                        );
                        yield data;
                    } else {
                        // Emit an error here so the outer stream produces Err(Error)
                        Err(Error::Internal(format!(
                            "Block data not found for block index {}",
                            block_desc.index
                        )))?;
                    }
                }
            }
            .boxed();

            return Ok((metadata, stream));
        }

        // If not in metadata, read from UFS
        let (ufs, relative_path, _mount_path) = self.get_mount_for_path(&normalized_path).await?;

        let (obj_meta, ufs_read_stream) = ufs.read(&relative_path).await?;
        let new_metadata = FileMetadata::new_file(
            normalized_path.clone(),
            obj_meta.size as u64,
            None,
            Some(relative_path),
        );
        let new_metadata_id = new_metadata.id;

        // Store Cache
        let blocks = plan_chunk_layout(new_metadata.size).await;
        let path_for_cache = normalized_path.clone();
        let blocks_for_cache = blocks.clone();
        let file_id = new_metadata_id;
        let file_client_for_cache = self.file_client.clone();

        let ufs_stream = ufs_read_stream;
        let normalized_path_for_stream = normalized_path.clone();

        // Clone all necessary Arcs and data to move into the stream
        let file_client_for_cache = file_client_for_cache.clone();
        let path_for_cache = path_for_cache.clone();
        let blocks_for_cache = blocks_for_cache.clone();
        let file_id = file_id.clone();
        let normalized_path_for_stream = normalized_path_for_stream.clone();

        let tee_stream: BoxStream<'static, Result<Bytes>> = try_stream! {
            let mut block_idx: usize = 0;
            let mut block_buf = BytesMut::new();
            let mut pos_in_block: u64 = 0;

            let mut ufs_stream = ufs_stream; // shadow to move into stream

            while let Some(chunk) = ufs_stream.next().await {
                let bytes = chunk?; // one chunk read from UFS
                let mut remaining: &[u8] = &bytes;
                while !remaining.is_empty() && block_idx < blocks_for_cache.len() {
                    let block = &blocks_for_cache[block_idx];
                    let block_size = block.size as u64;

                    let space_in_block = (block_size - pos_in_block) as usize;
                    let take_len = remaining.len().min(space_in_block);

                    // Accumulate chunk bytes into block_buf
                    block_buf.extend_from_slice(&remaining[..take_len]);
                    pos_in_block += take_len as u64;
                    remaining = &remaining[take_len..];

                    // Flush to cache once the block buffer is full
                    if pos_in_block == block_size {
                        let data = Bytes::from(block_buf.split().freeze());
                        tracing::debug!(
                            "Writing block {} of file {} to cache",
                            block.index,
                            normalized_path_for_stream
                        );
                        let node = get_node_by_block(block_nodes.clone(), file_id, block).await?;
                        let dst_url = node.url.clone();
                        file_client_for_cache
                            .write_block(
                                &path_for_cache,
                                file_id,
                                block.clone(),
                                data.clone(),
                                dst_url,
                            )
                            .await?;

                        block_idx += 1;
                        pos_in_block = 0;
                    }
                }
                yield bytes;
            }
            // After the loop, flush any partially filled block that remains
            if !block_buf.is_empty() && block_idx < blocks_for_cache.len() {
                let block = &blocks_for_cache[block_idx];
                let data = Bytes::from(block_buf.freeze());
                let node = get_node_by_block(block_nodes, file_id, block).await?;
                let dst_url = node.url.clone();
                file_client_for_cache
                    .write_block(
                        &path_for_cache,
                        file_id,
                        block.clone(),
                        data.clone(),
                        dst_url,
                    )
                    .await?;
            }
        }
        .boxed();
        self.metadata_store.put(new_metadata.clone()).await?;
        Ok((new_metadata, tee_stream))
    }

    async fn read_block(
        &self,
        path: &str,
        file_id: uuid::Uuid,
        block_desc: BlockDesc,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let normalized_path = normalize_path(path)?;

        let (ufs, relative_path, _mount_path) = self.get_mount_for_path(&normalized_path).await?;
        let start = block_desc.range_start;
        let end = block_desc.range_end;

        // Check cache first
        if let Ok(bytes) = self.cache.read_block(file_id, block_desc.index).await {
            let cached_stream = futures::stream::once(async move { Ok(bytes) }).boxed();
            tracing::debug!(
                "Cache hit for block {} of file {}",
                block_desc.index,
                normalized_path
            );
            return Ok(cached_stream);
        }

        // TODO: range_read is not implemented as streaming in UFS yet
        let ufs_bytes = ufs.read_range(&relative_path, start, end).await?;

        let cache_for_store = self.cache.clone();
        let file_id_for_store = file_id;
        let index_for_store = block_desc.index;
        let bytes_for_store = ufs_bytes.clone();

        // Store in cache asynchronously
        tokio::spawn(async move {
            if let Err(e) = cache_for_store
                .store_block(file_id_for_store, index_for_store, bytes_for_store)
                .await
            {
                tracing::warn!(
                    "failed to store block {} of file {} into cache: {}",
                    index_for_store,
                    file_id_for_store,
                    e
                );
            }
        });
        let s = stream::once(async move { Ok(ufs_bytes) }).boxed();
        Ok(s)
    }

    async fn write(&self, path: &str, data: Bytes) -> Result<()> {
        let normalized_path = normalize_path(path)?;

        let mut metadata = self
            .metadata_store
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
        // TODO: Store Cache
        // self.cache_accessor.store_file(metadata.id, data.clone()).await?;
        self.metadata_store.put(metadata).await?;

        Ok(())
    }

    async fn write_block(
        &self,
        path: &str,
        file_id: uuid::Uuid,
        block_desc: BlockDesc,
        data: Bytes,
    ) -> Result<()> {
        let normalized_path = normalize_path(path)?;

        let metadata = self
            .metadata_store
            .get(&normalized_path)
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;

        if !metadata.is_file() {
            return Err(Error::NotAFile(normalized_path));
        }

        let result = self
            .cache
            .store_block(file_id, block_desc.index, data.clone())
            .await?;

        tracing::debug!(
            "Wrote block {} of file {} to cache",
            block_desc.index,
            normalized_path
        );

        // TODO: If needed, write ufs
        Ok(result)
    }

    async fn stat(&self, path: &str) -> Result<FileMetadata> {
        let normalized_path = normalize_path(path)?;

        let metadata = self
            .metadata_store
            .get(&normalized_path)
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path))?;

        Ok(metadata)
    }

    async fn list(&self, path: &str) -> Result<Vec<FileMetadata>> {
        let normalized_path = normalize_path(path)?;

        // Get or create directory metadata
        let dir_metadata = match self.metadata_store.get(&normalized_path).await? {
            Some(meta) => {
                if !meta.is_directory() {
                    return Err(Error::NotADirectory(normalized_path));
                }
                meta
            }
            None => {
                return Err(Error::PathNotFound(normalized_path));
            }
        };

        // Attempt to resolve a mount; if none, return metadata-only listing.
        match self.get_mount_for_path(&normalized_path).await {
            Ok((ufs, _relative_path, mount_path)) => {
                // Get UFS path prefix for listing
                tracing::debug!("Listing directory at VFS path: {}", normalized_path);
                tracing::debug!("Resolved to mount path: {}", mount_path);
                let ufs_prefix = self.generate_ufs_path(&normalized_path, &mount_path);

                // List files from UFS
                let (ufs_files, ufs_dirs) = ufs.list_dir(&ufs_prefix).await?;
                tracing::debug!("UFS listed files: {:?}, dirs: {:?}", ufs_files, ufs_dirs);

                // Sync with metadata store
                let mut result = Vec::new();
                let mut metadata_names = std::collections::HashSet::new();

                // Get existing metadata children
                let existing_children = self.metadata_store.list_children(&dir_metadata.id).await?;
                for child in &existing_children {
                    let name = child.path.rsplit('/').next().unwrap_or(&child.path);
                    metadata_names.insert(name.to_string());
                    result.push(child.clone());
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
                        self.metadata_store.put(file_metadata.clone()).await?;
                        result.push(file_metadata);
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
                        self.metadata_store.put(sub_dir_metadata.clone()).await?;
                        result.push(sub_dir_metadata);
                    }
                }

                Ok(result)
            }
            Err(Error::PathNotFound(_)) => {
                // No mount available; return metadata-only listing
                tracing::debug!(
                    "No mount found for {}, returning metadata-only listing",
                    normalized_path
                );
                let existing_children = self.metadata_store.list_children(&dir_metadata.id).await?;
                Ok(existing_children.into_iter().map(|child| child).collect())
            }
            Err(e) => Err(e),
        }
    }

    async fn remove_file(&self, path: &str) -> Result<()> {
        let normalized_path = normalize_path(path)?;

        let metadata = self
            .metadata_store
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
        self.metadata_store.delete(&normalized_path).await?;
        // TODO: DELETE FROM CACHE
        // self.cache.invalidate_file(metadata.id).await;

        Ok(())
    }

    async fn remove_dir(&self, path: &str) -> Result<()> {
        let normalized_path = normalize_path(path)?;

        let metadata = self
            .metadata_store
            .get(&normalized_path)
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;

        if !metadata.is_directory() {
            return Err(Error::NotADirectory(normalized_path.clone()));
        }

        // Check if directory is empty
        let children = self.metadata_store.list_children(&metadata.id).await?;
        if !children.is_empty() {
            return Err(Error::Internal("Directory not empty".to_string()));
        }

        // Delete metadata
        self.metadata_store.delete(&normalized_path).await?;

        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let normalized_path = normalize_path(path)?;
        Ok(self.metadata_store.get(&normalized_path).await?.is_some())
    }
}
