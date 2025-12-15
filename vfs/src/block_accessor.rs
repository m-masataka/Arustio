use common::{Result, file_metadata::BlockDesc, raft_client::RaftClient};

use crate::cache::{hash_ring::HashRing, manager::CHUNK_SIZE, node::CacheNode};

pub struct BlockAccessor {
    raft_client: RaftClient,
}

impl BlockAccessor {
    pub fn new(raft_client: RaftClient) -> Self {
        BlockAccessor { raft_client }
    }

    pub async fn get_cache_nodes(&self) -> Result<Vec<CacheNode>> {
        let nodes_proto = self.raft_client.get_cache_nodes().await?;
        let nodes: Vec<CacheNode> = nodes_proto
            .iter()
            .cloned()
            .map(|proto_node| CacheNode::try_from(proto_node))
            .collect::<Result<Vec<CacheNode>>>()?;
        Ok(nodes)
    }

    pub async fn plan_chunk_layout(&self, file_size: u64) -> Vec<BlockDesc> {
        let chunk_count = self.chunk_count(file_size);
        let mut blocks = Vec::new();
        for index in 0..chunk_count {
            blocks.push(BlockDesc {
                index,
                size: if index == chunk_count - 1 {
                    (file_size - index * CHUNK_SIZE as u64) as u32
                } else {
                    CHUNK_SIZE as u32
                },
                range_start: index * CHUNK_SIZE as u64,
                range_end: if index == chunk_count - 1 {
                    file_size
                } else {
                    (index + 1) * CHUNK_SIZE as u64
                },
            });
        }
        blocks
    }

    fn chunk_count(&self, size: u64) -> u64 {
        if size == 0 {
            0
        } else {
            ((size as usize) + CHUNK_SIZE - 1) as u64 / CHUNK_SIZE as u64
        }
    }

    // Get Blocks and their assigned Cache Nodes Pair
    pub async fn get_block_node_pairs(
        &self,
        file_id: uuid::Uuid,
        block_descs: Vec<BlockDesc>,
    ) -> Result<Vec<(BlockDesc, CacheNode)>> {
        let cache_nodes = self.get_cache_nodes().await?;
        if cache_nodes.is_empty() {
            return Err(common::Error::Internal(
                "No cache nodes available".to_string(),
            ));
        }
        let node_ids: Vec<String> = cache_nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect();
        // Select Node
        let hash_ring = HashRing::new(node_ids.clone());

        let mut block_node_pairs = Vec::new();
        for block_desc in block_descs {
            let node_id = hash_ring.owner(file_id, block_desc.index);
            let cache_node = cache_nodes
                .iter()
                .find(|node| node.node_id == node_id)
                .ok_or_else(|| {
                    common::Error::Internal(format!("Cache node not found: {}", node_id))
                })?;
            block_node_pairs.push((block_desc, cache_node.clone()));
        }
        Ok(block_node_pairs)
    }

    pub async fn get_node_by_block(
        &self,
        file_id: uuid::Uuid,
        block_desc: &BlockDesc,
    ) -> Result<CacheNode> {
        let cache_nodes = self.get_cache_nodes().await?;
        if cache_nodes.is_empty() {
            return Err(common::Error::Internal(
                "No cache nodes available".to_string(),
            ));
        }
        let node_ids: Vec<String> = cache_nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect();
        // Select Node
        let hash_ring = HashRing::new(node_ids.clone());
        let node_id = hash_ring.owner(file_id, block_desc.index);
        let cache_node = cache_nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| common::Error::Internal(format!("Cache node not found: {}", node_id)))?;
        Ok(cache_node.clone())
    }
}
