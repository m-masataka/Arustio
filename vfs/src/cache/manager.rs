use std::{
    hash::{Hash, Hasher},
    io::{self, ErrorKind},
    sync::Arc,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use common::{Result, raft_client::RaftClient};
use moka::future::Cache;
use uuid::Uuid;

use crate::cache::{hash_ring::HashRing, node::CacheNode};

pub const DEFAULT_CAPACITY: u64 = 512 * 1024 * 1024;
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq)]
struct ChunkKey {
    file_id: Uuid,
    index: u64,
}

impl PartialEq for ChunkKey {
    fn eq(&self, other: &Self) -> bool {
        self.file_id == other.file_id && self.index == other.index
    }
}

impl Hash for ChunkKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_id.hash(state);
        self.index.hash(state);
    }
}

pub struct CacheManager {
    raft_client: RaftClient,
    self_node: CacheNode,
    ring: ArcSwap<HashRing>,
    cache_nodes: ArcSwap<Vec<CacheNode>>,
    cache: Cache<ChunkKey, Arc<Vec<u8>>>,
}

impl CacheManager {
    pub fn new(
        raft_client: RaftClient,
        self_node: CacheNode,
        ring_nodes: Vec<String>,
        capacity_bytes: u64,
    ) -> Self {
        let initial_ring = HashRing::new(ring_nodes);
        let capacity = if capacity_bytes == 0 {
            DEFAULT_CAPACITY
        } else {
            capacity_bytes
        };
        Self {
            raft_client,
            self_node,
            ring: ArcSwap::from_pointee(initial_ring),
            cache_nodes: ArcSwap::from_pointee(Vec::new()),
            cache: Cache::builder().max_capacity(capacity).build(),
        }
    }

    pub async fn store_block(&self, file_id: Uuid, index: u64, data: Bytes) -> Result<()> {
        self.cache
            .insert(ChunkKey { file_id, index }, Arc::new(data.to_vec()))
            .await;
        Ok(())
    }

    pub async fn read_block(&self, file_id: Uuid, index: u64) -> Result<Bytes> {
        match self.cache.get(&ChunkKey { file_id, index }).await {
            Some(chunk) => Ok(Bytes::from(chunk.as_ref().clone())),
            None => Err(io::Error::new(
                ErrorKind::NotFound,
                format!("Chunk {} of file {} not found in cache", index, file_id),
            ))?,
        }
    }

    pub async fn invalidate_file(&self, file_id: Uuid) {
        let _ = self
            .cache
            .invalidate_entries_if(move |key, _| key.file_id == file_id);
    }

    pub async fn update_ring_nodes(&self) {
        let nodes_proto = self.raft_client.get_cache_nodes().await;
        let cache_nodes = match nodes_proto {
            Ok(ref protos) => {
                let mut nodes = Vec::new();
                for proto in protos {
                    match CacheNode::try_from(proto.clone()) {
                        Ok(node) => nodes.push(node),
                        Err(e) => {
                            tracing::error!("Failed to convert CacheNodeInfo to CacheNode: {}", e);
                        }
                    }
                }
                nodes
            }
            Err(e) => {
                tracing::error!("Failed to fetch cache nodes from Raft: {}", e);
                Vec::new()
            }
        };
        self.cache_nodes.store(Arc::new(cache_nodes.clone()));

        let node_ids = cache_nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect::<Vec<String>>();

        // if node_ids is not contains self.self_node.node_id, join the ring
        if !node_ids.contains(&self.self_node.node_id) {
            tracing::info!(
                "Cache node ID {} not in ring, joining...",
                self.self_node.node_id
            );
            self.join_cache_ring().await;
        }

        let new_ring = HashRing::new(node_ids.clone());
        self.ring.store(Arc::new(new_ring));
    }

    pub async fn join_cache_ring(&self) {
        // TODO: UPDATE heartbeat timestamp periodically
        if let Err(e) = self
            .raft_client
            .send_command(common::meta::MetaCmd {
                op: Some(common::meta::meta_cmd::Op::AddCacheNode(
                    common::meta::AddCacheNode {
                        node: Some(self.self_node.clone().into()),
                    },
                )),
            })
            .await
        {
            tracing::error!("Failed to join cache ring: {}", e);
        }
    }

    pub async fn run_maintenance(&self) {
        self.update_ring_nodes().await;
    }
}
