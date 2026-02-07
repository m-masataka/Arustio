use arc_swap::ArcSwap;
use std::sync::Arc;

use crate::{
    block::{hash_ring::HashRing, node::BlockNode},
    metadata::metadata::MetadataStore,
};

pub const DEFAULT_CAPACITY: u64 = 512 * 1024 * 1024;
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub struct BlockManager {
    self_node: BlockNode,
    metadata_store: Arc<dyn MetadataStore>,
    ring: ArcSwap<HashRing>,
    block_nodes: ArcSwap<Vec<BlockNode>>,
}

impl BlockManager {
    pub fn new(
        metadata_store: Arc<dyn MetadataStore>,
        self_node: BlockNode,
        ring_nodes: Vec<String>,
    ) -> Self {
        let initial_ring = HashRing::new(ring_nodes);
        Self {
            metadata_store,
            self_node,
            ring: ArcSwap::from_pointee(initial_ring),
            block_nodes: ArcSwap::from_pointee(Vec::new()),
        }
    }

    pub async fn update_ring_nodes(&self) {
        let block_nodes = self.metadata_store.list_block_nodes().await;
        let nodes = match block_nodes {
            Err(e) => {
                tracing::error!("Failed to fetch block nodes from Metadata: {}", e);
                return;
            }
            Ok(nodes) => nodes,
        };

        // update block_nodes
        self.block_nodes.store(Arc::new(nodes.clone()));

        let node_ids = nodes
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
            .metadata_store
            .put_block_node(self.self_node.clone())
            .await
        {
            tracing::error!("Failed to join cache ring: {}", e);
        }
    }

    pub async fn run_maintenance(&self) {
        self.update_ring_nodes().await;
    }
}
