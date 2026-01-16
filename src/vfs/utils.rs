use crate::{
    common::{Result, Error},
    core::file_metadata::BlockDesc,
    block::{hash_ring::HashRing, manager::CHUNK_SIZE, node::BlockNode},
};

pub async fn plan_chunk_layout(file_size: u64) -> Vec<BlockDesc> {
    let chunk_count = chunk_count(file_size);
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

fn chunk_count(size: u64) -> u64 {
    if size == 0 {
        0
    } else {
        ((size as usize) + CHUNK_SIZE - 1) as u64 / CHUNK_SIZE as u64
    }
}

// Get Blocks and their assigned Cache Nodes Pair
pub async fn get_block_node_pairs(
    block_nodes: Vec<BlockNode>,
    file_id: uuid::Uuid,
    block_descs: Vec<BlockDesc>,
) -> Result<Vec<(BlockDesc, BlockNode)>> {
    if block_nodes.is_empty() {
        return Err(Error::Internal(
            "No block nodes available".to_string(),
        ));
    }
    let node_ids: Vec<String> = block_nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect();
    // Select Node
    let hash_ring = HashRing::new(node_ids.clone());

    let mut block_node_pairs = Vec::new();
    for block_desc in block_descs {
        let node_id = hash_ring.owner(file_id, block_desc.index);
        let block_node = block_nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| {
                Error::Internal(format!("Block node not found: {}", node_id))
            })?;
        block_node_pairs.push((block_desc, block_node.clone()));
    }
    Ok(block_node_pairs)
}

pub async fn get_node_by_block(
    block_nodes: Vec<BlockNode>,    
    file_id: uuid::Uuid,
    block_desc: &BlockDesc,
) -> Result<BlockNode> {
    if block_nodes.is_empty() {
        return Err(Error::Internal(
            "No block nodes available".to_string(),
        ));
    }
    let node_ids: Vec<String> = block_nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect();
    // Select Node
    let hash_ring = HashRing::new(node_ids.clone());
    let node_id = hash_ring.owner(file_id, block_desc.index);
    let block_node = block_nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .ok_or_else(|| Error::Internal(format!("Block node not found: {}", node_id)))?;
    Ok(block_node.clone())
}