//! Metadata storage trait
//!
//! This module defines the interface for storing and retrieving file system metadata.
//! The actual implementation can be backed by different storage systems
//! (e.g., RocksDB, distributed Raft-based storage).

use async_trait::async_trait;
use crate::{
    common::Result,
    core::file_metadata::FileMetadata,
    ufs::config::UfsConfig,
    block::node::BlockNode,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Mount point configuration stored in metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountInfo {
    pub path: String,
    pub config: UfsConfig,
}

/// Trait for metadata storage operations
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Get metadata by path
    async fn get(&self, path: &str) -> Result<Option<FileMetadata>>;

    /// Store or update metadata
    async fn put(&self, metadata: FileMetadata) -> Result<()>;

    /// Delete metadata by path
    async fn delete(&self, path: &str) -> Result<()>;

    /// List all children of a directory
    async fn list_children(&self, parent_id: &Uuid) -> Result<Vec<FileMetadata>>;

    /// Get metadata by file ID
    async fn get_by_id(&self, id: &Uuid) -> Result<Option<FileMetadata>>;

    // Mount table persistence methods
    /// Save mount point configuration
    async fn save_mount(&self, mount_info: &MountInfo) -> Result<()>;

    /// Get mount point configuration by path
    async fn get_mount(&self, path: &str) -> Result<Option<MountInfo>>;

    /// Delete mount point configuration
    async fn delete_mount(&self, path: &str) -> Result<()>;

    /// List all mount points
    async fn list_mounts(&self) -> Result<Vec<MountInfo>>;

    /// List Block Nodes
    async fn list_block_nodes(&self) -> Result<Vec<BlockNode>>;

    /// Put Block Node
    async fn put_block_node(&self, block_node: BlockNode) -> Result<()>;
}
