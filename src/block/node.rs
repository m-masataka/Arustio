use chrono::{DateTime, NaiveDateTime, Utc};
use crate::common::Error;
use crate::meta::BlockNodeInfo as ProtoBlockNodeInfo;
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
pub struct BlockNode {
    pub node_id: String,
    pub url: String,
    pub weight: u32,
    pub status: BlockNodeStatus,
    pub last_heartbeat: Option<chrono::DateTime<chrono::Utc>>,
    pub zone: String,
    pub description: String,
}

/// UFS backend type
#[derive(Debug, Clone)]
pub enum BlockNodeStatus {
    Unspecified = 0,
    Up = 1,
    Draining = 2,
    Down = 3,
}

impl From<BlockNode> for ProtoBlockNodeInfo {
    fn from(node: BlockNode) -> Self {
        ProtoBlockNodeInfo {
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

impl TryFrom<ProtoBlockNodeInfo> for BlockNode {
    type Error = Error;

    fn try_from(proto: ProtoBlockNodeInfo) -> Result<Self, Self::Error> {
        let last_heartbeat = match proto.last_heartbeat {
            Some(ts) => Some(prost_to_chrono(&ts)),
            None => None,
        };
        let status = match proto.status {
            0 => BlockNodeStatus::Unspecified,
            1 => BlockNodeStatus::Up,
            2 => BlockNodeStatus::Draining,
            3 => BlockNodeStatus::Down,
            _ => {
                return Err(Error::Internal(format!(
                    "Invalid cache node status: {}",
                    proto.status
                )));
            }
        };
        Ok(BlockNode {
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
