//! Virtual File System with mount support

use crate::{
    client::{
        cache_client::CacheClient, metadata_client::MetadataClient, utils::plan_chunk_layout,
    },
    common::{Error, Result, block_size_bytes, normalize_path, parent_path},
    core::{
        file_metadata::{BlockDesc, FileMetadata, MountInfo},
        file_system::FileSystem,
    },
    ufs::{
        config::UfsConfig,
        under_file_system::{Ufs, UfsOperations},
    },
};
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug)]
struct HitRateTracker {
    path: String,
    file_id: uuid::Uuid,
    hits: u64,
    misses: u64,
}

impl HitRateTracker {
    fn new(path: String, file_id: uuid::Uuid) -> Self {
        Self {
            path,
            file_id,
            hits: 0,
            misses: 0,
        }
    }
    fn hit(&mut self) {
        self.hits += 1;
    }
    fn miss(&mut self) {
        self.misses += 1;
    }

    fn rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64) / (total as f64)
        }
    }
}

impl Drop for HitRateTracker {
    fn drop(&mut self) {
        let total = self.hits + self.misses;
        if total > 0 {
            tracing::info!(
                path=%self.path,
                file_id=%self.file_id,
                hits=self.hits,
                misses=self.misses,
                hit_rate=self.rate(),
                "cache hit rate (blocks)"
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteType {
    AsyncThrough,
    CacheThrough,
    NoCache,
}

fn parse_write_type(value: &str) -> Result<WriteType> {
    match value.to_ascii_uppercase().as_str() {
        "ASYNC_THROUGH" => Ok(WriteType::AsyncThrough),
        "CACHE_THROUGH" => Ok(WriteType::CacheThrough),
        "NO_CACHE" => Ok(WriteType::NoCache),
        _ => Err(Error::InvalidPath(format!(
            "invalid writetype: {value} (use ASYNC_THROUGH|CACHE_THROUGH|NO_CACHE)"
        ))),
    }
}

/// Virtual File System with mount table support
pub struct VirtualFileSystem {
    metadata_client: MetadataClient,
    cache: Arc<dyn CacheClient>,
}

impl VirtualFileSystem {
    /// Create a new Virtual File System with mount support
    pub fn new(metadata_client: MetadataClient, cache: Arc<dyn CacheClient>) -> Self {
        Self {
            metadata_client,
            cache,
        }
    }

    /// Restore mounts from metadata store
    pub async fn restore_mounts(&self) -> Result<()> {
        // self.mount_table.restore_mounts().await
        Ok(())
    }

    async fn write_type_for_path(&self, path: &str) -> Result<WriteType> {
        let mut current = normalize_path(path)?;
        loop {
            if let Some(conf) = self.metadata_client.get_path_conf(current.clone()).await? {
                if let Some(value) = conf.conf.get("writetype") {
                    return parse_write_type(value);
                }
            }
            if let Some(parent) = parent_path(&current) {
                current = parent;
            } else {
                break;
            }
        }
        Ok(WriteType::CacheThrough)
    }

    pub async fn set_path_conf(&self, path: &str, conf: HashMap<String, String>) -> Result<()> {
        let normalized_path = normalize_path(path)?;
        let meta = self
            .metadata_client
            .get(normalized_path.clone())
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;
        if !meta.is_directory() {
            return Err(Error::NotADirectory(normalized_path));
        }

        if let Some(value) = conf.get("writetype") {
            parse_write_type(value)?;
        }

        let conf = crate::meta::PathConf {
            full_path: normalized_path,
            conf,
        };
        self.metadata_client.set_path_conf(conf).await?;
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
            .metadata_client
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
        tracing::info!("Mounting UFS at {}", normalized_path);

        // Check if already mounted
        {
            let mounts = self.metadata_client.list_mounts().await?;
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
            match self.metadata_client.get(normalized_path.clone()).await {
                Ok(Some(_)) => {
                    return Err(Error::PathAlreadyExists(format!(
                        "Path {} already exists",
                        normalized_path
                    )));
                }
                Ok(None) => {
                    // Path does not exist, which is fine
                }
                Err(e) => {
                    tracing::error!("Error checking path {}: {}", normalized_path, e);
                    return Err(Error::Internal(format!(
                        "Error checking path {}: {}",
                        normalized_path, e
                    )));
                }
            }
        }

        tracing::info!("Mounting UFS to {}", normalized_path);

        // Create Directory metadata if not exists
        if self
            .metadata_client
            .get(normalized_path.clone())
            .await?
            .is_none()
        {
            self.mkdir(&normalized_path).await?;
        }

        tracing::info!("Created mount point directory {}", normalized_path);

        // Save to metadata store first
        let mount_info = MountInfo {
            path: normalized_path.clone(),
            ufs_config: config.clone(),
            description: None,
        };
        self.metadata_client.save_mount(&mount_info).await?;

        tracing::info!("Mounted UFS to {}", normalized_path);
        Ok(())
    }

    /// Unmount a VFS path
    pub async fn unmount(&self, vfs_path: &str) -> Result<()> {
        let normalized_path = normalize_path(vfs_path)?;

        // Remove from mount table
        self.metadata_client
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
        let mounts = self.metadata_client.list_mounts().await?;
        tracing::debug!("Available mounts: {:?}", mounts);

        // Find the longest matching mount point
        let mut best_match: Option<(&String, UfsConfig)> = None;
        let mut best_match_len = 0;

        for m in mounts.iter() {
            if normalized_path == *m.path || normalized_path.starts_with(&format!("{}/", m.path)) {
                if m.path.len() > best_match_len {
                    best_match = Some((&m.path, m.ufs_config.clone()));
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
        let mounts = self.metadata_client.list_mounts().await.unwrap_or_default();
        mounts
    }
}

#[async_trait]
impl FileSystem for VirtualFileSystem {
    async fn mkdir(&self, path: &str) -> Result<()> {
        let normalized_path = normalize_path(path)?;

        // Check if already exists
        if self
            .metadata_client
            .get(normalized_path.clone())
            .await?
            .is_some()
        {
            return Err(Error::PathAlreadyExists(normalized_path));
        }

        // Create root directory if not exists
        let root_path = "/";
        if self
            .metadata_client
            .get(root_path.to_string())
            .await?
            .is_none()
        {
            let metadata = FileMetadata::new_directory(root_path.to_string(), None);
            self.metadata_client.put(metadata).await?;
        }

        // Get parent directory
        let parent_id = if let Some(parent_path) = parent_path(&normalized_path) {
            let parent = self
                .metadata_client
                .get(parent_path.to_string())
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
        self.metadata_client.put(metadata).await?;

        Ok(())
    }

    async fn read(&self, path: &str) -> Result<(FileMetadata, BoxStream<'static, Result<Bytes>>)> {
        let normalized_path = normalize_path(path)?;
        let (ufs, relative_path, _mount_path) = self.get_mount_for_path(&normalized_path).await?;
        let write_type = self.write_type_for_path(&normalized_path).await?;
        let cache_enabled = write_type != WriteType::NoCache;
        if let Some(metadata) = self.metadata_client.get(normalized_path.clone()).await? {
            if !metadata.is_file() {
                return Err(Error::NotAFile(normalized_path));
            }
            let blocks = plan_chunk_layout(metadata.size).await;
            let cache = self.cache.clone();
            if cache_enabled {
                cache.connection_pool().await?;
            }

            let path_for_log = normalized_path.clone();
            let file_id = metadata.id;
            tracing::info!("file_id={}", file_id);
            let stream: BoxStream<'static, Result<Bytes>> = try_stream! {
                // Track hit rate
                let mut tracker = HitRateTracker::new(path_for_log, file_id);
                for block in blocks {
                    if cache_enabled {
                        match cache.read_block(metadata.id, block.index).await {
                            Ok(data) => {
                                tracker.hit();
                                yield data;
                            }
                            Err(Error::CacheMiss { .. }) => {
                                tracker.miss();
                                // Read from UFS
                                let start = block.range_start;
                                let end = block.range_end;
                                let ufs_bytes = ufs.read_range(&relative_path, start, end).await?;
                                // Store in cache asynchronously
                                let cache_for_store = cache.clone();
                                let file_id_for_store = metadata.id;
                                let index_for_store = block.index;
                                let bytes_for_store = ufs_bytes.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = cache_for_store
                                        .write_block(
                                            file_id_for_store,
                                            index_for_store,
                                            bytes_for_store,
                                        )
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
                                yield ufs_bytes;
                            }
                            Err(e) => {
                                tracker.miss();
                                tracing::warn!(
                                    "cache read error for file {} block {}: {}",
                                    metadata.id,
                                    block.index,
                                    e
                                );
                                // Read from UFS
                                let start = block.range_start;
                                let end = block.range_end;
                                let ufs_bytes = ufs.read_range(&relative_path, start, end).await?;
                                // Store in cache asynchronously
                                let cache_for_store = cache.clone();
                                let file_id_for_store = metadata.id;
                                let index_for_store = block.index;
                                let bytes_for_store = ufs_bytes.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = cache_for_store
                                        .write_block(
                                            file_id_for_store,
                                            index_for_store,
                                            bytes_for_store,
                                        )
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
                                yield ufs_bytes;
                            }
                        }
                    } else {
                        tracker.miss();
                        let start = block.range_start;
                        let end = block.range_end;
                        let ufs_bytes = ufs.read_range(&relative_path, start, end).await?;
                        yield ufs_bytes;
                    }
                }
            }
            .boxed();
            return Ok((metadata, stream));
        }

        // If not in metadata, read from UFS
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
        let blocks_for_cache = blocks.clone();
        let file_id = new_metadata_id;
        tracing::info!("file_id={}", file_id);
        let cache_for_store = self.cache.clone();
        if cache_enabled {
            cache_for_store.connection_pool().await?;
        }

        let ufs_stream = ufs_read_stream;
        let normalized_path_for_stream = normalized_path.clone();

        // Clone all necessary Arcs and data to move into the stream
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
                        if cache_enabled {
                            cache_for_store
                                .write_block(file_id, block.clone().index, data.clone())
                                .await?;
                        }

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
                if cache_enabled {
                    cache_for_store
                        .write_block(file_id, block.clone().index, data.clone())
                        .await?;
                }
            }
        }
        .boxed();
        self.metadata_client.put(new_metadata.clone()).await?;
        Ok((new_metadata, tee_stream))
    }

    async fn read_range(&self, path: &str, offset: u64, size: u64) -> Result<Bytes> {
        let normalized_path = normalize_path(path)?;
        let (ufs, relative_path, _mount_path) = self.get_mount_for_path(&normalized_path).await?;
        let write_type = self.write_type_for_path(&normalized_path).await?;
        let cache_enabled = write_type != WriteType::NoCache;

        let metadata = self
            .metadata_client
            .get(normalized_path.clone())
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;
        if !metadata.is_file() {
            return Err(Error::NotAFile(normalized_path));
        }
        if size == 0 || offset >= metadata.size {
            return Ok(Bytes::new());
        }

        let end = std::cmp::min(offset + size, metadata.size);
        let blocks = plan_chunk_layout(metadata.size).await;
        let cache = self.cache.clone();
        if cache_enabled {
            cache.connection_pool().await?;
        }

        let mut out = BytesMut::new();
        for block in blocks {
            if block.range_end <= offset || block.range_start >= end {
                continue;
            }
            let block_bytes = if cache_enabled {
                match cache.read_block(metadata.id, block.index).await {
                    Ok(data) => data,
                    Err(Error::CacheMiss { .. }) => {
                        let ufs_bytes = ufs
                            .read_range(&relative_path, block.range_start, block.range_end)
                            .await?;
                        let cache_for_store = cache.clone();
                        let file_id_for_store = metadata.id;
                        let index_for_store = block.index;
                        let bytes_for_store = ufs_bytes.clone();
                        tokio::spawn(async move {
                            if let Err(e) = cache_for_store
                                .write_block(file_id_for_store, index_for_store, bytes_for_store)
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
                        ufs_bytes
                    }
                    Err(e) => {
                        tracing::warn!(
                            "cache read error for file {} block {}: {}",
                            metadata.id,
                            block.index,
                            e
                        );
                        ufs.read_range(&relative_path, block.range_start, block.range_end)
                            .await?
                    }
                }
            } else {
                ufs.read_range(&relative_path, block.range_start, block.range_end)
                    .await?
            };

            let slice_start = std::cmp::max(offset, block.range_start) - block.range_start;
            let slice_end = std::cmp::min(end, block.range_end) - block.range_start;
            out.extend_from_slice(&block_bytes[slice_start as usize..slice_end as usize]);
        }
        Ok(Bytes::from(out))
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
        self.cache.connection_pool().await?;

        // Check cache first
        match self.cache.read_block(file_id, block_desc.index).await {
            Ok(bytes) => {
                let cached_stream = futures::stream::once(async move { Ok(bytes) }).boxed();
                tracing::debug!(
                    "Cache hit for block {} of file {}",
                    block_desc.index,
                    normalized_path
                );
                return Ok(cached_stream);
            }
            Err(Error::CacheMiss { .. }) => {
                tracing::debug!(
                    "Cache miss for block {} of file {}",
                    block_desc.index,
                    normalized_path
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Cache read error for block {} of file {}: {}",
                    block_desc.index,
                    normalized_path,
                    e
                );
            }
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
                .write_block(file_id_for_store, index_for_store, bytes_for_store)
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

    async fn write(&self, path: &str, mut data: BoxStream<'static, Result<Bytes>>) -> Result<()> {
        let normalized_path = normalize_path(path)?;

        let metadata = self.metadata_client.get(normalized_path.clone()).await?;

        let mut metadata = match metadata {
            Some(meta) => meta,
            None => {
                // Create new file metadata with parent directory
                let parent_id = if let Some(parent_path) = parent_path(&normalized_path) {
                    let parent = self
                        .metadata_client
                        .get(parent_path.clone())
                        .await?
                        .ok_or_else(|| Error::PathNotFound(parent_path.clone()))?;

                    if !parent.is_directory() {
                        return Err(Error::NotADirectory(parent_path));
                    }

                    Some(parent.id)
                } else {
                    None
                };
                FileMetadata::new_file(normalized_path.clone(), 0, parent_id, None)
            }
        };

        if !metadata.is_file() {
            return Err(Error::NotAFile(normalized_path));
        }

        // Resolve to mount point
        let (ufs, relative_path, _mount_path) = self.get_mount_for_path(&normalized_path).await?;

        // Write data to UFS and Cache
        self.cache.connection_pool().await?;
        let cache = self.cache.clone();
        let file_id = metadata.id;
        let mut block_index: u64 = 0;
        let mut buf = BytesMut::new();
        let mut total: u64 = 0;
        let mut writer = ufs.open_multipart_write(&relative_path).await?;
        let block_size = block_size_bytes();

        while let Some(chunk) = data.next().await {
            let bytes = chunk?;
            let mut remaining: &[u8] = &bytes;
            total += bytes.len() as u64;

            writer.write(&bytes);

            while !remaining.is_empty() {
                let space_in_block = block_size - buf.len();
                let take_len = remaining.len().min(space_in_block);
                buf.extend_from_slice(&remaining[..take_len]);
                remaining = &remaining[take_len..];

                if buf.len() == block_size {
                    // Write to Cache
                    cache
                        .write_block(file_id, block_index, Bytes::from(buf.split().freeze()))
                        .await?;
                    block_index += 1;
                }
            }
        }

        // Finish remaining buffer
        if !buf.is_empty() {
            let block_bytes = buf.freeze();
            cache
                .write_block(file_id, block_index, Bytes::from(block_bytes))
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to write final block {} to cache: {}",
                        block_index, e
                    ))
                })?;
        }
        writer
            .finish()
            .await
            .map_err(|e| Error::Internal(format!("Failed to finish write: {}", e)))?;

        metadata.size = total;
        metadata.modified_at = chrono::Utc::now();
        self.metadata_client.put(metadata).await?;

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
            .metadata_client
            .get(normalized_path.clone())
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;

        if !metadata.is_file() {
            return Err(Error::NotAFile(normalized_path));
        }

        self.cache.connection_pool().await?;

        let result = self
            .cache
            .write_block(file_id, block_desc.index, data.clone())
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
            .metadata_client
            .get(normalized_path.clone())
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path))?;

        Ok(metadata)
    }

    async fn list(&self, path: &str) -> Result<Vec<FileMetadata>> {
        let normalized_path = normalize_path(path)?;

        // Get or create directory metadata
        let dir_metadata = match self.metadata_client.get(normalized_path.clone()).await? {
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
                tracing::debug!("ufs config: {:?}", ufs.get_config());

                // List files from UFS
                let (ufs_files, ufs_dirs) = ufs.list_dir(&ufs_prefix).await?;
                tracing::debug!("UFS listed files: {:?}, dirs: {:?}", ufs_files, ufs_dirs);

                // Sync with metadata store
                let mut result = Vec::new();
                let mut metadata_names = std::collections::HashSet::new();

                // Get existing metadata children
                let existing_children =
                    self.metadata_client.list_children(&dir_metadata.id).await?;
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
                        self.metadata_client.put(file_metadata.clone()).await?;
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
                        self.metadata_client.put(sub_dir_metadata.clone()).await?;
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
                let existing_children =
                    self.metadata_client.list_children(&dir_metadata.id).await?;
                Ok(existing_children.into_iter().map(|child| child).collect())
            }
            Err(e) => Err(e),
        }
    }

    async fn remove_file(&self, path: &str) -> Result<()> {
        let normalized_path = normalize_path(path)?;

        let metadata = self
            .metadata_client
            .get(normalized_path.clone())
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
        self.metadata_client.delete(normalized_path.clone()).await?;
        // TODO: DELETE FROM CACHE
        // self.cache.invalidate_file(metadata.id).await;

        Ok(())
    }

    async fn remove_dir(&self, path: &str) -> Result<()> {
        let normalized_path = normalize_path(path)?;

        let metadata = self
            .metadata_client
            .get(normalized_path.clone())
            .await?
            .ok_or_else(|| Error::PathNotFound(normalized_path.clone()))?;

        if !metadata.is_directory() {
            return Err(Error::NotADirectory(normalized_path.clone()));
        }

        // Check if directory is empty
        let children = self.metadata_client.list_children(&metadata.id).await?;
        if !children.is_empty() {
            return Err(Error::Internal("Directory not empty".to_string()));
        }

        // Delete metadata
        self.metadata_client.delete(normalized_path.clone()).await?;

        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let normalized_path = normalize_path(path)?;
        Ok(self
            .metadata_client
            .get(normalized_path.clone())
            .await?
            .is_some())
    }
}
