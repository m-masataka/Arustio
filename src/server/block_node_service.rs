use crate::{
    blockio::{
        ReadBlockRequest, ReadBlockResponse, WriteBlockRequest, WriteBlockResponse,
        block_node_service_server::{BlockNodeService, BlockNodeServiceServer},
        write_block_request,
    },
    cache::manager::CacheManager,
    common::{Error as ArustioError, grpc_chunk_size_bytes},
};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

pub struct BlockNodeServiceImpl {
    cache: Arc<CacheManager>,
}

impl BlockNodeServiceImpl {
    pub fn new(cache: Arc<CacheManager>) -> Self {
        Self { cache }
    }

    pub fn into_server(self) -> BlockNodeServiceServer<Self> {
        BlockNodeServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl BlockNodeService for BlockNodeServiceImpl {
    type ReadBlockStream = ReceiverStream<Result<ReadBlockResponse, Status>>;
    async fn read_block(
        &self,
        request: Request<ReadBlockRequest>,
    ) -> Result<Response<Self::ReadBlockStream>, Status> {
        let req = request.into_inner();
        let block_id = req.file_id;
        let offset = req.index as u64;

        tracing::debug!(
            "Received ReadBlock request: block_id={}, offset={}",
            block_id,
            offset,
        );
        let uuid = Uuid::parse_str(&block_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid block ID: {}", e)))?;
        let res_block_data = self.cache.read_block(uuid, offset).await;
        match res_block_data {
            Ok(block_data) => {
                let (tx, rx) = tokio::sync::mpsc::channel(128);
                let data_len = block_data.len();
                let mut range_start = 0usize;
                let mut range_end = data_len;
                if let Some(off) = req.offset {
                    range_start = off as usize;
                }
                if let Some(size) = req.size {
                    range_end = range_start.saturating_add(size as usize);
                }
                if range_start > data_len {
                    return Err(Status::out_of_range("offset out of range"));
                }
                range_end = range_end.min(data_len);

                let chunk_size = grpc_chunk_size_bytes();
                let mut start = range_start;
                while start < range_end {
                    let end = std::cmp::min(start + chunk_size, range_end);
                    let chunk = block_data.slice(start..end);
                    let response = ReadBlockResponse {
                        data: chunk.to_vec(),
                    };
                    tx.send(Ok(response))
                        .await
                        .map_err(|e| Status::internal(format!("Failed to send chunk: {}", e)))?;
                    start = end;
                }
                Ok(Response::new(ReceiverStream::new(rx)))
            }
            Err(ArustioError::CacheMiss { .. }) => Err(Status::not_found("Cache miss")),
            Err(e) => Err(Status::internal(format!("Failed to read block: {}", e))),
        }
    }

    async fn write_block(
        &self,
        request: Request<Streaming<WriteBlockRequest>>,
    ) -> Result<Response<WriteBlockResponse>, Status> {
        let mut stream = request.into_inner();

        // Read first message for metadata
        let first_msg = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("Failed to receive metadata: {}", e)))?
            .ok_or_else(|| Status::invalid_argument("Empty stream"))?;

        let file_id = match first_msg.data {
            Some(write_block_request::Data::FileId(id)) => id,
            _ => {
                return Err(Status::invalid_argument(
                    "First message must contain block ID",
                ));
            }
        };

        tracing::debug!(
            "Received WriteBlock request: block_id={}, index={}",
            file_id.id,
            file_id.index,
        );

        let block_id = Uuid::parse_str(&file_id.id)
            .map_err(|e| Status::invalid_argument(format!("Invalid block ID: {}", e)))?;

        let mut buffer = Vec::new();
        // Read chunks
        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("Failed to receive chunk: {}", e)))?
        {
            match msg.data {
                Some(write_block_request::Data::Chunk(chunk)) => {
                    buffer.extend_from_slice(&chunk);
                }
                _ => return Err(Status::invalid_argument("Unexpected message type")),
            }
        }

        let buffer_len = buffer.len();

        match self
            .cache
            .store_block(block_id, file_id.index, buffer.into())
            .await
        {
            Ok(_) => {
                tracing::debug!("Successfully wrote block {}", block_id);
                let response = WriteBlockResponse {
                    success: true,
                    message: "Block written successfully".to_string(),
                    bytes_written: buffer_len as u64,
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                tracing::error!("Failed to write block {}: {}", block_id, e);
                let response = WriteBlockResponse {
                    success: false,
                    message: format!("Failed to write block: {}", e),
                    bytes_written: 0,
                };
                Ok(Response::new(response))
            }
        }
    }
}
