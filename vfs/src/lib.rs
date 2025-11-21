//! Virtual File System layer
//!
//! This module provides a filesystem-like interface on top of the UFS layer,
//! managing metadata and providing common filesystem operations like:
//! - mkdir: Create directories
//! - open/create: Open or create files
//! - read/write: Read and write file contents
//! - ls: List directory contents
//! - stat: Get file/directory status
//! - rm: Remove files and directories

use common::{
    Result,
    file_metadata::FileStatus
};
use async_trait::async_trait;
use bytes::Bytes;

pub mod operations;
mod lib_mount;

pub use lib_mount::VirtualFsWithMounts;

/// Trait for filesystem operations
#[async_trait]
pub trait FileSystem: Send + Sync {
    /// Create a directory
    async fn mkdir(&self, path: &str) -> Result<()>;

    /// Create a file
    async fn create(&self, path: &str, data: Bytes) -> Result<()>;

    /// Read a file
    async fn read(&self, path: &str) -> Result<Bytes>;

    /// Write to a file (overwrites existing content)
    async fn write(&self, path: &str, data: Bytes) -> Result<()>;

    /// Get file/directory status
    async fn stat(&self, path: &str) -> Result<FileStatus>;

    /// List directory contents
    async fn list(&self, path: &str) -> Result<Vec<FileStatus>>;

    /// Remove a file
    async fn remove_file(&self, path: &str) -> Result<()>;

    /// Remove a directory (must be empty)
    async fn remove_dir(&self, path: &str) -> Result<()>;

    /// Check if a path exists
    async fn exists(&self, path: &str) -> Result<bool>;
}

/// Trait for filesystem with mount support
#[async_trait]
pub trait MountableFileSystem: FileSystem {
    /// Mount a UFS backend to a VFS path
    async fn mount(&self, vfs_path: &str, config: ufs::UfsConfig) -> Result<()>;

    /// Unmount a VFS path
    async fn unmount(&self, vfs_path: &str) -> Result<()>;

    /// List all mount points
    async fn list_mounts(&self) -> Vec<String>;
}
