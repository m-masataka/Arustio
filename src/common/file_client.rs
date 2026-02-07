use futures::stream;
use tonic::Request;

use crate::common::{error::Result, grpc_chunk_size_bytes};
use crate::core::file_metadata::BlockDesc;
use crate::file::{
    ReadBlockRequest, WriteBlockMetadata, WriteBlockRequest,
    file_service_client::FileServiceClient, write_block_request,
};
use std::io;

#[derive(Clone)]
pub struct FileClient {}

impl FileClient {
    pub fn new() -> Self {
        FileClient {}
    }

    // Read block from node via grpc stream
    pub async fn read_block(
        &self,
        path: &str,
        file_id: uuid::Uuid,
        block_desc: BlockDesc,
        dst_url: String,
    ) -> Result<Option<bytes::Bytes>> {
        tracing::debug!("Connecting to block node at: {}", dst_url);
        let mut client = FileServiceClient::connect(dst_url).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to connect to block node: {}", e),
            )
        })?;
        let request = tonic::Request::new(ReadBlockRequest {
            path: path.to_string(),
            file_id: file_id.to_string(),
            block_desc: Some(block_desc.into()),
        });

        let response = client.read_block(request).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to read block from node: {}", e),
            )
        })?;
        let mut stream = response.into_inner();
        let mut data = Vec::new();

        while let Some(resp) = stream.message().await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to read stream: {}", e),
            )
        })? {
            // Assuming the bytes are in a field named `data` in ReadBlockResponse
            data.extend_from_slice(&resp.chunk);
        }

        if data.is_empty() {
            tracing::warn!("Received empty block data for file_id: {}", file_id);
            Ok(None)
        } else {
            tracing::debug!(
                "Successfully read block data for file_id: {}, size: {}",
                file_id,
                data.len()
            );
            Ok(Some(bytes::Bytes::from(data)))
        }
    }

    pub async fn write_block(
        &self,
        path: &str,
        file_id: uuid::Uuid,
        block_desc: BlockDesc,
        data: bytes::Bytes,
        dst_url: String,
    ) -> Result<()> {
        tracing::debug!("Connecting to block node at: {}", dst_url);
        let mut client = FileServiceClient::connect(dst_url).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to connect to block node: {}", e),
            )
        })?;

        let path = path.to_string();
        let file_id_str = file_id.to_string();
        let block_desc_proto: crate::meta::BlockDesc = block_desc.into();
        let total_size = data.len();

        let mut requests = Vec::new();

        // Send Metadata first
        let metadata = WriteBlockRequest {
            data: Some(write_block_request::Data::Metadata(WriteBlockMetadata {
                path: path.clone(),
                file_id: file_id_str.clone(),
                block_desc: Some(block_desc_proto.clone()),
            })),
        };
        requests.push(metadata);

        let mut offset: usize = 0;
        let chunk_size = grpc_chunk_size_bytes();
        while offset < total_size {
            let end = offset + chunk_size.min(total_size - offset);
            let chunk = data.slice(offset..end);
            let chunk_request = WriteBlockRequest {
                data: Some(write_block_request::Data::Chunk(chunk.to_vec())),
            };
            requests.push(chunk_request);
            offset = end;
        }

        let outbound = stream::iter(requests);
        let request = Request::new(outbound);

        let resp = client
            .write_block(request)
            .await
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to write block to node: {}", e),
                )
            })?
            .into_inner();

        tracing::debug!(
            "Successfully wrote block data for file_id: {}, bytes_written={}",
            file_id_str,
            resp.bytes_written,
        );
        Ok(())
    }
}
