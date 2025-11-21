use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct RaftClusterState {
    pub leader_id: AtomicU64,
    pub term: AtomicU64,
}

impl RaftClusterState {
    pub fn new() -> Self {
        Self {
            leader_id: AtomicU64::new(0),
            term: AtomicU64::new(0),
        }
    }

    pub fn set_leader(&self, leader_id: u64) {
        self.leader_id.store(leader_id, Ordering::Relaxed);
    }

    pub fn set_term(&self, term: u64) {
        self.term.store(term, Ordering::Relaxed);
    }

    pub fn get_leader(&self) -> (u64, u64) {
        (
            self.leader_id.load(Ordering::Relaxed),
            self.term.load(Ordering::Relaxed),
        )
    }
}