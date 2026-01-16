//! Metadata management layer
//!
//! This module provides distributed metadata storage using:
//! - OpenRaft: Distributed consensus protocol
//! - RocksDB: Local persistent storage
//!
//! The metadata is replicated across multiple nodes for high availability
//! and consistency.

pub mod metadata;
pub mod raft {
    pub mod apply;
    pub mod cluster;
    pub mod linearizable_read;
    pub mod raft_store;
    pub mod raft_transport;
    pub mod rocks_store;
}
pub mod rocks;
pub mod utils;

pub use rocks::RocksMetadataStore;

/// Node ID type for Raft cluster
pub type NodeId = u64;
