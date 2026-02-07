use std::sync::Arc;

use crate::{
    common::{Result, Error},
    client::{
        cache_client::CacheClient,
        metadata_client::MetadataClient,
    },
    blockio::{
        block_node_service_client::BlockNodeServiceClient,{
            ReadBlockRequest,
            WriteBlockRequest,
        },
        write_block_request,
        FileId,
    },
};
use tokio::sync::RwLock;
use tonic::{
    Request,
    async_trait,
    Code,
    transport::Channel,
};
use futures::stream;
use uuid::Uuid;
use bytes::{Bytes, BytesMut};


#[derive(Clone)]
pub struct BlockNodeClient {
    metadata: MetadataClient,
    pool: Arc<RwLock<Vec<BlockNodeServiceClient<Channel>>>>,
}

impl BlockNodeClient {
    pub fn new(metadata: MetadataClient) -> Self {
        Self {
            metadata,
            pool: Arc::new(RwLock::new(Vec::new())),
        }
    }

    async fn select_node(&self, file_id: Uuid, index: u64) -> Result<BlockNodeServiceClient<Channel>> {
        let pool = self.pool.read().await;
        if pool.is_empty() {
            return Err(Error::Internal("No block nodes available".to_string()));
        }
        // Simple round-robin selection
        Ok(pool[0].clone())
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
        let mut list = Vec::with_capacity(nodes.len());
        for node in nodes {
            let client = BlockNodeServiceClient::connect(node.url.clone())
                .await
                .map_err(|e| Error::Internal(format!("Failed to connect to block node {}: {}", node.node_id, e)))?;
            list.push(client);
        }
        *self.pool.write().await = list;
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
                    Code::InvalidArgument => Error::InvalidPath(format!("Invalid block request: {}", e.message())),
                    Code::PermissionDenied => Error::PermissionDenied(format!("Permission denied: {}", e.message())),
                    Code::Unavailable => Error::Metadata(format!("Block node unavailable: {}", e.message())),
                    _ => Error::Internal(format!("Failed to read block: {} ({})", e.message(), e.code())),
                }
            })?;

        let mut stream = response.into_inner();
        let mut buf = BytesMut::new();
        while let Some(resp) = stream.message().await.map_err(|e| {
            Error::Internal(format!("Failed to read block stream: {}", e))
        })? {
            buf.extend_from_slice(&resp.data);
        }
        Ok(Bytes::from(buf))
    }

    async fn write_block(&self, file_id: Uuid, index: u64, data: Bytes) -> Result<()> {
        // Write Stream to the block node
        let mut node = self.select_node(file_id, index).await?;
        tracing::debug!("Writing block: file_id={}, index={}, size={}", file_id, index, data.len());

        const CHUNK_SIZE: usize = 1 * 1024 * 1024; // 1 MB
        let first = WriteBlockRequest {
            data: Some(write_block_request::Data::FileId(FileId { id: file_id.to_string(), index })),
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
        
        node
            .write_block(Request::new(outbound))
            .await.map_err(|e| {
                match e.code() {
                    Code::InvalidArgument => Error::InvalidPath(format!("Invalid block write: {}", e.message())),
                    Code::PermissionDenied => Error::PermissionDenied(format!("Permission denied: {}", e.message())),
                    Code::Unavailable => Error::Metadata(format!("Block node unavailable: {}", e.message())),
                    _ => Error::Internal(format!("Failed to write block: {} ({})", e.message(), e.code())),
                }
            })
            .and_then(|_response| {
                Ok(())
            })
    }
}
