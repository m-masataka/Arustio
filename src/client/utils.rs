use crate::{common::block_size_bytes, core::file_metadata::BlockDesc};

pub async fn plan_chunk_layout(file_size: u64) -> Vec<BlockDesc> {
    let chunk_size = block_size_bytes() as u64;
    let chunk_count = chunk_count(file_size);
    let mut blocks = Vec::new();
    for index in 0..chunk_count {
        blocks.push(BlockDesc {
            index,
            size: if index == chunk_count - 1 {
                (file_size - index * chunk_size) as u32
            } else {
                chunk_size as u32
            },
            range_start: index * chunk_size,
            range_end: if index == chunk_count - 1 {
                file_size
            } else {
                (index + 1) * chunk_size
            },
        });
    }
    blocks
}

fn chunk_count(size: u64) -> u64 {
    if size == 0 {
        0
    } else {
        let chunk_size = block_size_bytes() as u64;
        ((size as usize) + chunk_size as usize - 1) as u64 / chunk_size
    }
}
