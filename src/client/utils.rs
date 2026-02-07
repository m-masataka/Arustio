use crate::{block::manager::CHUNK_SIZE, core::file_metadata::BlockDesc};

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
