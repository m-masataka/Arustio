use crate::meta::Mount;
use crate::mount::{
    ListRequest, ListResponse, MountRequest, MountResponse, UnmountRequest, UnmountResponse,
    mount_service_server::{MountService, MountServiceServer},
};
use crate::file::{
    ReadBlockRequest, ReadBlockResponse,
    CopyFromLocalRequest, CopyFromLocalResponse, ListFilesRequest, ListFilesResponse,
    MkdirRequest, MkdirResponse, ReadRequest, ReadResponse, RemoveDirRequest,
    RemoveDirResponse, RemoveFileRequest, RemoveFileResponse, StatRequest, StatResponse,
    WriteBlockRequest, WriteBlockResponse, WriteFileRequest, WriteFileResponse,
    file_service_server::{FileService, FileServiceServer},
    copy_from_local_request, read_response, write_file_request, write_block_request,
};
use crate::vfs::{file_system::FileSystem, virtual_file_system::VirtualFileSystem};
use crate::ufs::config::UfsConfig;
use std::sync::Arc;
use bytes::Bytes;
use tokio::sync::RwLock;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

pub struct ArustioMountService {
    vfs: Arc<RwLock<VirtualFileSystem>>,
}

impl ArustioMountService {
    pub fn new(vfs: Arc<RwLock<VirtualFileSystem>>) -> Self {
        Self { vfs }
    }

    pub fn into_server(self) -> MountServiceServer<Self> {
        MountServiceServer::new(self)
    }
}

const TRANSPORT_CHUNK_SIZE: usize = 1 * 1024 * 1024;

#[tonic::async_trait]
impl MountService for ArustioMountService {
    async fn mount(
        &self,
        request: Request<MountRequest>,
    ) -> Result<Response<MountResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("Received mount request: path={}, uri={}", req.path, req.uri);

        // Parse URI to UFS config
        let ufs_config = parse_uri_to_config(&req.uri, req.options.clone())
            .map_err(|e| Status::invalid_argument(format!("Invalid URI: {}", e)))?;

        // Mount the filesystem
        let vfs = self.vfs.read().await;

        // Create mount directory if it doesn't exist
        let parent = std::path::Path::new(&req.path)
            .parent()
            .and_then(|p| p.to_str())
            .unwrap_or("/");

        if parent != "/" {
            if let Err(e) = vfs.mkdir(parent).await {
                tracing::warn!("Failed to create parent directory {}: {}", parent, e);
            }
        }

        match vfs.mount(&req.path, ufs_config).await {
            Ok(_) => {
                tracing::info!("Successfully mounted {} to {}", req.uri, req.path);
                Ok(Response::new(MountResponse {
                    success: true,
                    message: format!("Successfully mounted {} to {}", req.uri, req.path),
                }))
            }
            Err(e) => {
                tracing::error!("Failed to mount {} to {}: {}", req.uri, req.path, e);
                Ok(Response::new(MountResponse {
                    success: false,
                    message: format!("Failed to mount: {}", e),
                }))
            }
        }
    }

    async fn unmount(
        &self,
        request: Request<UnmountRequest>,
    ) -> Result<Response<UnmountResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("Received unmount request: path={}", req.path);

        let vfs = self.vfs.read().await;
        match vfs.unmount(&req.path).await {
            Ok(_) => {
                tracing::info!("Successfully unmounted {}", req.path);
                Ok(Response::new(UnmountResponse {
                    success: true,
                    message: format!("Successfully unmounted {}", req.path),
                }))
            }
            Err(e) => {
                tracing::error!("Failed to unmount {}: {}", req.path, e);
                Ok(Response::new(UnmountResponse {
                    success: false,
                    message: format!("Failed to unmount: {}", e),
                }))
            }
        }
    }

    async fn list(&self, _request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        tracing::debug!("Received list mount points request");

        let vfs = self.vfs.read().await;
        let mount_info_list = vfs.list_mounts().await;

        let mut mounts: Vec<Mount> = mount_info_list
            .into_iter()
            .map(|mount| Mount {
                full_path: mount.path.clone(),
                ufs_config: Some(mount.config.clone().into()),
                description: "Mounted UFS".to_string(),
            })
            .collect();

        // Sort by path for consistent output
        mounts.sort_by(|a, b| a.full_path.cmp(&b.full_path));

        Ok(Response::new(ListResponse { mounts }))
    }
}

pub struct ArustioFileService {
    vfs: Arc<RwLock<VirtualFileSystem>>,
}

impl ArustioFileService {
    pub fn new(vfs: Arc<RwLock<VirtualFileSystem>>) -> Self {
        Self { vfs }
    }

    pub fn into_server(self) -> FileServiceServer<Self> {
        FileServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl FileService for ArustioFileService {
    async fn list_files(
        &self,
        request: Request<ListFilesRequest>,
    ) -> Result<Response<ListFilesResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("Received list files request: path={}", req.path);

        let vfs = self.vfs.read().await;
        match vfs.list(&req.path).await {
            Ok(entries) => {
                let entries_proto = entries.into_iter().map(|e| e.into()).collect();
                Ok(Response::new(ListFilesResponse {
                    entries: entries_proto,
                }))
            }
            Err(e) => Err(Status::not_found(format!("Failed to list files: {}", e))),
        }
    }

    async fn stat(&self, request: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("Received stat request: path={}", req.path);

        let vfs = self.vfs.read().await;
        match vfs.stat(&req.path).await {
            Ok(metadata) => {
                let file_metadata_proto = metadata.into();

                Ok(Response::new(StatResponse {
                    entry: Some(file_metadata_proto),
                }))
            }
            Err(e) => Err(Status::not_found(format!(
                "Failed to get file status: {}",
                e
            ))),
        }
    }

    async fn mkdir(
        &self,
        request: Request<MkdirRequest>,
    ) -> Result<Response<MkdirResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("Received mkdir request: path={}", req.path);

        let vfs = self.vfs.read().await;
        match vfs.mkdir(&req.path).await {
            Ok(_) => Ok(Response::new(MkdirResponse {
                success: true,
                message: format!("Successfully created directory {}", req.path),
            })),
            Err(e) => Ok(Response::new(MkdirResponse {
                success: false,
                message: format!("Failed to create directory: {}", e),
            })),
        }
    }

    async fn copy_from_local(
        &self,
        request: Request<tonic::Streaming<CopyFromLocalRequest>>,
    ) -> Result<Response<CopyFromLocalResponse>, Status> {
        let mut stream = request.into_inner();

        // Read first message for metadata
        let first_msg = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("Failed to receive metadata: {}", e)))?
            .ok_or_else(|| Status::invalid_argument("Empty stream"))?;

        let metadata = match first_msg.data {
            Some(copy_from_local_request::Data::Metadata(m)) => m,
            _ => return Err(Status::invalid_argument("First message must be metadata")),
        };

        tracing::debug!(
            "Receiving file upload: path={}, size={} bytes",
            metadata.destination_path,
            metadata.file_size
        );

        let mut buffer = Vec::new();

        // Read chunks
        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("Failed to receive chunk: {}", e)))?
        {
            match msg.data {
                Some(copy_from_local_request::Data::Chunk(chunk)) => {
                    buffer.extend_from_slice(&chunk);
                }
                _ => return Err(Status::invalid_argument("Unexpected message type")),
            }
        }

        // TODO: DELETE THIS LOG
        tracing::debug!("Completed receiving file upload:");

        let _vfs = self.vfs.read().await;
        let bytes_written = buffer.len() as u64;

        // TODO: DELETE THIS LOG
        tracing::debug!("Completed receiving file upload READ:");

        Ok(Response::new(CopyFromLocalResponse {
            success: true,
            message: format!("NOOP file to {}", metadata.destination_path),
            bytes_written,
        }))
    }

    type ReadStream = ReceiverStream<Result<ReadResponse, Status>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let req = request.into_inner();
        tracing::debug!("Received download request: path={}", req.path);

        let vfs = self.vfs.read().await;
        let (file_metadata, mut stream) = vfs
            .read(&req.path)
            .await
            .map_err(|e| Status::not_found(format!("Failed to read file: {}", e)))?;

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            if tx
                .send(Ok(ReadResponse {
                    data: Some(read_response::Data::Metadata(
                        file_metadata.into(),
                    )),
                }))
                .await
                .is_err()
            {
                return;
            }
            while let Some(item) = stream.next().await {
                match item {
                    Ok(chunk) => {
                        let mut offset_in_block = 0u64;
                        let mut buf = vec![0u8; TRANSPORT_CHUNK_SIZE];
                        while offset_in_block < chunk.len() as u64 {
                            let end = std::cmp::min(
                                offset_in_block + TRANSPORT_CHUNK_SIZE as u64,
                                chunk.len() as u64,
                            );
                            let len = (end - offset_in_block) as usize;
                            buf[..len]
                                .copy_from_slice(&chunk[offset_in_block as usize..end as usize]);
                            let response = ReadResponse {
                                data: Some(read_response::Data::Chunk(
                                    buf[..len].to_vec(),
                                )),
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                return;
                            }
                            offset_in_block += len as u64;
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "Error reading file chunk: {}",
                                e
                            ))))
                            .await;
                        break;
                    }
                }
            }
        });
        let out_stream = ReceiverStream::new(rx);

        Ok(Response::new(out_stream))
    }

    type ReadBlockStream = ReceiverStream<Result<ReadBlockResponse, Status>>;
    async fn read_block(
        &self,
        request: Request<ReadBlockRequest>,
    ) -> Result<Response<Self::ReadBlockStream>, Status> {
        let req = request.into_inner();
        tracing::debug!(
            "Received read block request: path={}, file_id={}, block_index={}",
            req.path,
            req.file_id,
            req.block_desc.as_ref().map_or(0, |b| b.index)
        );

        let block_desc = match req.block_desc {
            Some(b) => b.try_into().map_err(|e| {
                Status::invalid_argument(format!("Invalid block description: {}", e))
            })?,
            None => {
                return Err(Status::invalid_argument("Block description is required"));
            }
        };

        let file_id = uuid::Uuid::parse_str(&req.file_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid file ID: {}", e)))?;
        let vfs = self.vfs.read().await;
        let mut stream = vfs
            .read_block(&req.path, file_id, block_desc)
            .await
            .map_err(|e| Status::not_found(format!("Failed to read file block: {}", e)))?;
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(chunk) => {
                        let mut offset_in_block = 0u64;
                        let mut buf = vec![0u8; TRANSPORT_CHUNK_SIZE];
                        while offset_in_block < chunk.len() as u64 {
                            let end = std::cmp::min(
                                offset_in_block + TRANSPORT_CHUNK_SIZE as u64,
                                chunk.len() as u64,
                            );
                            let len = (end - offset_in_block) as usize;
                            buf[..len]
                                .copy_from_slice(&chunk[offset_in_block as usize..end as usize]);
                            let response = ReadBlockResponse {
                                chunk: buf[..len].to_vec(),
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                return;
                            }
                            offset_in_block += len as u64;
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "Error reading file block chunk: {}",
                                e
                            ))))
                            .await;
                        break;
                    }
                }
            }
        });
        let out_stream = ReceiverStream::new(rx);

        Ok(Response::new(out_stream))
    }

    async fn write_file(
        &self,
        request: Request<tonic::Streaming<WriteFileRequest>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let mut stream = request.into_inner();

        // Read first message for metadata
        let first_msg = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("Failed to receive metadata: {}", e)))?
            .ok_or_else(|| Status::invalid_argument("Empty stream"))?;

        let metadata = match first_msg.data {
            Some(write_file_request::Data::Metadata(m)) => m,
            _ => return Err(Status::invalid_argument("First message must be metadata")),
        };

        tracing::debug!("Receiving file write: path={}", metadata.path);

        let mut buffer = Vec::new();

        // Read chunks
        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("Failed to receive chunk: {}", e)))?
        {
            match msg.data {
                Some(write_file_request::Data::Chunk(chunk)) => {
                    buffer.extend_from_slice(&chunk);
                }
                _ => return Err(Status::invalid_argument("Unexpected message type")),
            }
        }

        let vfs = self.vfs.read().await;
        let bytes_written = buffer.len() as u64;

        match vfs.write(&metadata.path, Bytes::from(buffer)).await {
            Ok(_) => Ok(Response::new(WriteFileResponse {
                success: true,
                message: format!("Successfully wrote file {}", metadata.path),
                bytes_written,
            })),
            Err(e) => Ok(Response::new(WriteFileResponse {
                success: false,
                message: format!("Failed to write file: {}", e),
                bytes_written: 0,
            })),
        }
    }

    async fn write_block(
        &self,
        request: Request<tonic::Streaming<WriteBlockRequest>>,
    ) -> Result<Response<WriteBlockResponse>, Status> {
        let mut stream = request.into_inner();

        // Read first message for metadata
        let first_msg = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("Failed to receive metadata: {}", e)))?
            .ok_or_else(|| Status::invalid_argument("Empty stream"))?;

        let metadata = match first_msg.data {
            Some(write_block_request::Data::Metadata(m)) => m,
            _ => return Err(Status::invalid_argument("First message must be metadata")),
        };

        tracing::debug!(
            "Receiving block write: path={}, file_id={}, block_index={}",
            metadata.path,
            metadata.file_id,
            metadata.block_desc.as_ref().map_or(0, |b| b.index)
        );

        let block_desc = match metadata.block_desc {
            Some(b) => b.try_into().map_err(|e| {
                Status::invalid_argument(format!("Invalid block description: {}", e))
            })?,
            None => {
                return Err(Status::invalid_argument("Block description is required"));
            }
        };

        let file_id = uuid::Uuid::parse_str(&metadata.file_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid file ID: {}", e)))?;

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

        let vfs = self.vfs.read().await;
        let bytes_written = buffer.len() as u64;

        match vfs
            .write_block(&metadata.path, file_id, block_desc, Bytes::from(buffer))
            .await
        {
            Ok(_) => Ok(Response::new(WriteBlockResponse {
                success: true,
                message: format!("Successfully wrote block for file_id {}", metadata.file_id),
                bytes_written,
            })),
            Err(e) => Ok(Response::new(WriteBlockResponse {
                success: false,
                message: format!("Failed to write block: {}", e),
                bytes_written: 0,
            })),
        }
    }

    async fn remove_file(
        &self,
        request: Request<RemoveFileRequest>,
    ) -> Result<Response<RemoveFileResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("Received remove file request: path={}", req.path);

        let vfs = self.vfs.read().await;
        match vfs.remove_file(&req.path).await {
            Ok(_) => Ok(Response::new(RemoveFileResponse {
                success: true,
                message: format!("Successfully removed file {}", req.path),
            })),
            Err(e) => Ok(Response::new(RemoveFileResponse {
                success: false,
                message: format!("Failed to remove file: {}", e),
            })),
        }
    }

    async fn remove_dir(
        &self,
        request: Request<RemoveDirRequest>,
    ) -> Result<Response<RemoveDirResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("Received remove directory request: path={}", req.path);

        let vfs = self.vfs.read().await;
        match vfs.remove_dir(&req.path).await {
            Ok(_) => Ok(Response::new(RemoveDirResponse {
                success: true,
                message: format!("Successfully removed directory {}", req.path),
            })),
            Err(e) => Ok(Response::new(RemoveDirResponse {
                success: false,
                message: format!("Failed to remove directory: {}", e),
            })),
        }
    }
}

fn parse_uri_to_config(
    uri: &str,
    options: std::collections::HashMap<String, String>,
) -> Result<UfsConfig, String> {
    if uri.starts_with("s3://") || uri.starts_with("s3a://") {
        let without_prefix = uri
            .strip_prefix("s3://")
            .or_else(|| uri.strip_prefix("s3a://"))
            .unwrap();

        let parts: Vec<&str> = without_prefix.splitn(2, '/').collect();
        let bucket = parts[0].to_string();

        Ok(UfsConfig::S3 {
            bucket,
            region: options
                .get("region")
                .cloned()
                .or_else(|| std::env::var("AWS_REGION").ok())
                .unwrap_or_else(|| "us-east-1".to_string()),
            access_key_id: options
                .get("access_key_id")
                .cloned()
                .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok()),
            secret_access_key: options
                .get("secret_access_key")
                .cloned()
                .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok()),
            endpoint: options
                .get("endpoint")
                .cloned()
                .or_else(|| std::env::var("S3_ENDPOINT").ok()),
        })
    } else if uri.starts_with("gs://") || uri.starts_with("gcs://") {
        let without_prefix = uri
            .strip_prefix("gs://")
            .or_else(|| uri.strip_prefix("gcs://"))
            .unwrap();

        let parts: Vec<&str> = without_prefix.splitn(2, '/').collect();
        let bucket = parts[0].to_string();

        Ok(UfsConfig::Gcs {
            bucket,
            service_account_path: options.get("service_account_path").cloned(),
        })
    } else if uri.starts_with("local://") {
        let path = uri.strip_prefix("local://").unwrap();
        Ok(UfsConfig::Local {
            root_path: path.to_string(),
        })
    } else if uri.starts_with("mem://") || uri == "memory" {
        Ok(UfsConfig::Memory)
    } else {
        Err(format!("Unsupported URI scheme: {}", uri))
    }
}
