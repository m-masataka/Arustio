use anyhow::Result;
use serde::Deserialize;
use std::fs;
use std::sync::OnceLock;

#[derive(Debug, Deserialize, Clone)]
pub struct PeerConfig {
    pub id: u64,
    pub addr: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    pub self_id: u64,
    pub raft_listen: String,
    pub fs_listen: String,
    pub peers: Vec<PeerConfig>,
    #[serde(default = "default_cache_capacity_bytes")]
    pub cache_capacity_bytes: u64,
    #[serde(default = "default_block_size_bytes")]
    pub block_size_bytes: u64,
    #[serde(default = "default_grpc_chunk_size_bytes")]
    pub grpc_chunk_size_bytes: u64,
}

impl NodeConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let cfg: NodeConfig = toml::from_str(&content)?;
        Ok(cfg)
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub cache_capacity_bytes: u64,
    pub block_size_bytes: u64,
    pub grpc_chunk_size_bytes: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            cache_capacity_bytes: default_cache_capacity_bytes(),
            block_size_bytes: default_block_size_bytes(),
            grpc_chunk_size_bytes: default_grpc_chunk_size_bytes(),
        }
    }
}

impl From<&NodeConfig> for RuntimeConfig {
    fn from(cfg: &NodeConfig) -> Self {
        Self {
            cache_capacity_bytes: cfg.cache_capacity_bytes,
            block_size_bytes: cfg.block_size_bytes,
            grpc_chunk_size_bytes: cfg.grpc_chunk_size_bytes,
        }
    }
}

static RUNTIME_CONFIG: OnceLock<RuntimeConfig> = OnceLock::new();

pub fn set_runtime_config(cfg: RuntimeConfig) {
    let _ = RUNTIME_CONFIG.set(cfg);
}

pub fn runtime_config() -> &'static RuntimeConfig {
    RUNTIME_CONFIG.get_or_init(RuntimeConfig::default)
}

pub fn cache_capacity_bytes() -> u64 {
    runtime_config().cache_capacity_bytes
}

pub fn block_size_bytes() -> usize {
    runtime_config().block_size_bytes as usize
}

pub fn grpc_chunk_size_bytes() -> usize {
    runtime_config().grpc_chunk_size_bytes as usize
}

fn default_cache_capacity_bytes() -> u64 {
    512 * 1024 * 1024
}

fn default_block_size_bytes() -> u64 {
    4 * 1024 * 1024
}

fn default_grpc_chunk_size_bytes() -> u64 {
    1 * 1024 * 1024
}
