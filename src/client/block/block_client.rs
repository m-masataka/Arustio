use std::{collections::HashMap, sync::Arc};

use crate::{
    block::hash_ring::HashRing,
    blockio::{
        FileId,
        block_node_service_client::BlockNodeServiceClient,
        write_block_request, {ReadBlockRequest, WriteBlockRequest},
    },
    client::{cache_client::CacheClient, metadata_client::MetadataClient},
    common::{Error, Result},
};
use bytes::{Bytes, BytesMut};
use futures::stream;
use tokio::sync::RwLock;
use tonic::{Code, Request, async_trait, transport::Channel};
use uuid::Uuid;

#[derive(Clone)]
pub struct BlockNodeClient {
    metadata: MetadataClient,
    pool: Arc<RwLock<Option<BlockNodePool>>>,
}

#[derive(Clone)]
struct BlockNodePool {
    ring: HashRing,
    clients: HashMap<String, BlockNodeServiceClient<Channel>>,
    fallback_id: String,
}

impl BlockNodeClient {
    pub fn new(metadata: MetadataClient) -> Self {
        Self {
            metadata,
            pool: Arc::new(RwLock::new(None)),
        }
    }

    async fn select_node(
        &self,
        file_id: Uuid,
        index: u64,
    ) -> Result<BlockNodeServiceClient<Channel>> {
        let pool = self.pool.read().await;
        let pool = pool
            .as_ref()
            .ok_or_else(|| Error::Internal("No block nodes available".to_string()))?;
        let node_id = pool.ring.owner(file_id, index);
        if let Some(client) = pool.clients.get(&node_id) {
            return Ok(client.clone());
        }
        if let Some(client) = pool.clients.get(&pool.fallback_id) {
            tracing::warn!(
                "Hash ring selected unknown node_id={}, falling back to {}",
                node_id,
                pool.fallback_id
            );
            return Ok(client.clone());
        }
        return Err(Error::Internal("No block nodes available".to_string()));
    }
}

#[async_trait]
impl CacheClient for BlockNodeClient {
    async fn connection_pool(&self) -> Result<()> {
        // get connection pool to the block node
        let nodes = self.metadata.list_block_nodes().await?;
        if nodes.is_empty() {
            return Err(Error::Internal("No block nodes available".to_string()));
        }
        let mut clients = HashMap::with_capacity(nodes.len());
        let mut node_ids = Vec::with_capacity(nodes.len());
        for node in nodes {
            let client = BlockNodeServiceClient::connect(node.url.clone())
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to connect to block node {}: {}",
                        node.node_id, e
                    ))
                })?;
            node_ids.push(node.node_id.clone());
            clients.insert(node.node_id, client);
        }
        let fallback_id = node_ids[0].clone();
        let ring = HashRing::new(node_ids);
        *self.pool.write().await = Some(BlockNodePool {
            ring,
            clients,
            fallback_id,
        });
        Ok(())
    }

    async fn read_block(&self, file_id: Uuid, index: u64) -> Result<Bytes> {
        // Read block from the block node
        let mut node = self.select_node(file_id, index).await?;
        tracing::debug!("Reading block: file_id={}, index={}", file_id, index);

        let response = node
            .read_block(Request::new(ReadBlockRequest {
                file_id: file_id.to_string(),
                index,
            }))
            .await
            .map_err(|e| {
                tracing::debug!("Failed to read block: {}", e);
                match e.code() {
                    Code::NotFound => Error::CacheMiss { file_id, index },
                    Code::InvalidArgument => {
                        Error::InvalidPath(format!("Invalid block request: {}", e.message()))
                    }
                    Code::PermissionDenied => {
                        Error::PermissionDenied(format!("Permission denied: {}", e.message()))
                    }
                    Code::Unavailable => {
                        Error::Metadata(format!("Block node unavailable: {}", e.message()))
                    }
                    _ => Error::Internal(format!(
                        "Failed to read block: {} ({})",
                        e.message(),
                        e.code()
                    )),
                }
            })?;

        let mut stream = response.into_inner();
        let mut buf = BytesMut::new();
        while let Some(resp) = stream
            .message()
            .await
            .map_err(|e| Error::Internal(format!("Failed to read block stream: {}", e)))?
        {
            buf.extend_from_slice(&resp.data);
        }
        Ok(Bytes::from(buf))
    }

    async fn write_block(&self, file_id: Uuid, index: u64, data: Bytes) -> Result<()> {
        // Write Stream to the block node
        let mut node = self.select_node(file_id, index).await?;
        tracing::debug!(
            "Writing block: file_id={}, index={}, size={}",
            file_id,
            index,
            data.len()
        );

        const CHUNK_SIZE: usize = 1 * 1024 * 1024; // 1 MB
        let first = WriteBlockRequest {
            data: Some(write_block_request::Data::FileId(FileId {
                id: file_id.to_string(),
                index,
            })),
        };

        let data = data.clone();
        let chunks = (0..data.len()).step_by(CHUNK_SIZE).map(move |i| {
            let end = (i + CHUNK_SIZE).min(data.len());
            let piece = data.slice(i..end);

            WriteBlockRequest {
                data: Some(write_block_request::Data::Chunk(piece.to_vec())),
            }
        });

        let outbound = stream::iter(std::iter::once(first).chain(chunks));

        node.write_block(Request::new(outbound))
            .await
            .map_err(|e| match e.code() {
                Code::InvalidArgument => {
                    Error::InvalidPath(format!("Invalid block write: {}", e.message()))
                }
                Code::PermissionDenied => {
                    Error::PermissionDenied(format!("Permission denied: {}", e.message()))
                }
                Code::Unavailable => {
                    Error::Metadata(format!("Block node unavailable: {}", e.message()))
                }
                _ => Error::Internal(format!(
                    "Failed to write block: {} ({})",
                    e.message(),
                    e.code()
                )),
            })
            .and_then(|_response| Ok(()))
    }
}
