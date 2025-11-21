//! Metadata management layer
//!
//! This module provides distributed metadata storage using:
//! - OpenRaft: Distributed consensus protocol
//! - RocksDB: Local persistent storage
//!
//! The metadata is replicated across multiple nodes for high availability
//! and consistency.

// pub mod raft;
pub mod store;
pub mod mem_store;
pub mod metadata;
pub mod raft {
    pub mod raft_store;
    pub mod raft_transport;
    pub mod rocks_store;
    pub mod cluster;
    pub mod apply;
    pub mod utils;
}

// pub use raft::{RaftMetadataStore, RaftNode};
pub use store::RocksDbStore;
pub use mem_store::InMemoryMetadataStore;

/// Node ID type for Raft cluster
pub type NodeId = u64;
