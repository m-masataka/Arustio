use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct PeerConfig {
    pub id: u64,
    pub addr: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    pub self_id: u64,
    pub listen: String,
    pub fs_listen: String,
    pub peers: Vec<PeerConfig>,
}

impl NodeConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let cfg: NodeConfig = toml::from_str(&content)?;
        Ok(cfg)
    }
}
