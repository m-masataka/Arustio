use chrono::{DateTime, NaiveDateTime, Utc};
use common::{Error, meta::CacheNodeInfo as ProtoCacheNodeInfo};
use prost_types::Timestamp;

fn prost_to_chrono(ts: &Timestamp) -> DateTime<Utc> {
    NaiveDateTime::from_timestamp_opt(ts.seconds, ts.nanos as u32)
        .unwrap()
        .and_utc()
}

fn chrono_to_prost(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

/// UFS backend type
#[derive(Debug, Clone)]
pub struct CacheNode {
    pub node_id: String,
    pub url: String,
    pub weight: u32,
    pub status: CacheNodeStatus,
    pub last_heartbeat: Option<chrono::DateTime<chrono::Utc>>,
    pub zone: String,
    pub description: String,
}

/// UFS backend type
#[derive(Debug, Clone)]
pub enum CacheNodeStatus {
    Unspecified = 0,
    Up = 1,
    Draining = 2,
    Down = 3,
}

impl From<CacheNode> for ProtoCacheNodeInfo {
    fn from(node: CacheNode) -> Self {
        ProtoCacheNodeInfo {
            node_id: node.node_id,
            url: node.url,
            weight: node.weight,
            status: node.status as i32,
            last_heartbeat: node.last_heartbeat.map(|dt| chrono_to_prost(dt)),
            zone: node.zone,
            description: node.description,
        }
    }
}

impl TryFrom<ProtoCacheNodeInfo> for CacheNode {
    type Error = Error;

    fn try_from(proto: ProtoCacheNodeInfo) -> Result<Self, Self::Error> {
        let last_heartbeat = match proto.last_heartbeat {
            Some(ts) => Some(prost_to_chrono(&ts)),
            None => None,
        };
        let status = match proto.status {
            0 => CacheNodeStatus::Unspecified,
            1 => CacheNodeStatus::Up,
            2 => CacheNodeStatus::Draining,
            3 => CacheNodeStatus::Down,
            _ => {
                return Err(Error::Internal(format!(
                    "Invalid cache node status: {}",
                    proto.status
                )));
            }
        };
        Ok(CacheNode {
            node_id: proto.node_id,
            url: proto.url,
            weight: proto.weight,
            status,
            last_heartbeat,
            zone: proto.zone,
            description: proto.description,
        })
    }
}
