use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct HashRing {
    nodes: Arc<Vec<String>>,
}

impl HashRing {
    pub fn new(mut nodes: Vec<String>) -> Self {
        nodes.sort_unstable();
        Self {
            nodes: Arc::new(nodes),
        }
    }

    pub fn owner(&self, file_id: Uuid, chunk_index: u64) -> String {
        let key = Self::mix(file_id, chunk_index);
        let bucket = jump_consistent_hash(key, self.nodes.len() as i32);
        self.nodes[bucket as usize].clone()
    }

    pub fn mix(file_id: Uuid, chunk: u64) -> u64 {
        let mut hash = file_id.as_u128();
        hash ^= (chunk as u128) << 64;
        hash as u64 ^ (hash >> 64) as u64
    }
}

fn jump_consistent_hash(mut key: u64, num_buckets: i32) -> i32 {
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < num_buckets as i64 {
        b = j;
        key = key.wrapping_mul(2862933555777941757).wrapping_add(1);
        let denom = ((key >> 33) + 1) as f64;
        j = ((b as f64 + 1.0) * ((1u64 << 31) as f64) / denom).floor() as i64;
    }
    b as i32
}
