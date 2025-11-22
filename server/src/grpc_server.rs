use bytes::Bytes;
use common::file::{
    CopyFromLocalRequest, CopyFromLocalResponse, CopyToLocalRequest, CopyToLocalResponse,
    FileEntry, FileType, ListFilesRequest, ListFilesResponse, MkdirRequest, MkdirResponse,
    ReadFileRequest, ReadFileResponse, RemoveDirRequest, RemoveDirResponse, RemoveFileRequest,
    RemoveFileResponse, StatRequest, StatResponse, WriteFileRequest, WriteFileResponse,
    file_service_server::{FileService, FileServiceServer},
};
use common::meta::Mount;
use common::mount::{
    ListRequest, ListResponse, MountRequest, MountResponse, UnmountRequest, UnmountResponse,
    mount_service_server::{MountService, MountServiceServer},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use vfs::{FileSystem, VirtualFsWithMounts};

pub struct ArustioMountService {
    vfs: Arc<RwLock<VirtualFsWithMounts>>,
}

impl ArustioMountService {
    pub fn new(vfs: Arc<RwLock<VirtualFsWithMounts>>) -> Self {
        Self { vfs }
    }

    pub fn into_server(self) -> MountServiceServer<Self> {
        MountServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl MountService for ArustioMountService {
    async fn mount(
        &self,
        request: Request<MountRequest>,
    ) -> Result<Response<MountResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("Received mount request: path={}, uri={}", req.path, req.uri);

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
        tracing::info!("Received unmount request: path={}", req.path);

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
        tracing::info!("Received list mount points request");

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
    vfs: Arc<RwLock<VirtualFsWithMounts>>,
}

impl ArustioFileService {
    pub fn new(vfs: Arc<RwLock<VirtualFsWithMounts>>) -> Self {
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
        tracing::info!("Received list files request: path={}", req.path);

        let vfs = self.vfs.read().await;
        match vfs.list(&req.path).await {
            Ok(entries) => {
                let file_entries: Vec<FileEntry> = entries
                    .into_iter()
                    .map(|e| FileEntry {
                        name: std::path::Path::new(&e.path)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("")
                            .to_string(),
                        path: e.path.clone(),
                        file_type: match e.file_type {
                            common::file_metadata::FileType::File => FileType::File as i32,
                            common::file_metadata::FileType::Directory => {
                                FileType::Directory as i32
                            }
                        },
                        size: e.size,
                        modified_at: e.modified_at.timestamp(),
                    })
                    .collect();

                Ok(Response::new(ListFilesResponse {
                    entries: file_entries,
                }))
            }
            Err(e) => Err(Status::not_found(format!("Failed to list files: {}", e))),
        }
    }

    async fn stat(&self, request: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("Received stat request: path={}", req.path);

        let vfs = self.vfs.read().await;
        match vfs.stat(&req.path).await {
            Ok(stat) => {
                let entry = FileEntry {
                    name: std::path::Path::new(&stat.path)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("")
                        .to_string(),
                    path: stat.path.clone(),
                    file_type: match stat.file_type {
                        common::file_metadata::FileType::File => FileType::File as i32,
                        common::file_metadata::FileType::Directory => FileType::Directory as i32,
                    },
                    size: stat.size,
                    modified_at: stat.modified_at.timestamp(),
                };

                Ok(Response::new(StatResponse { entry: Some(entry) }))
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
        tracing::info!("Received mkdir request: path={}", req.path);

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
            Some(common::file::copy_from_local_request::Data::Metadata(m)) => m,
            _ => return Err(Status::invalid_argument("First message must be metadata")),
        };

        tracing::info!(
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
                Some(common::file::copy_from_local_request::Data::Chunk(chunk)) => {
                    buffer.extend_from_slice(&chunk);
                }
                _ => return Err(Status::invalid_argument("Unexpected message type")),
            }
        }

        // TODO: DELETE THIS LOG
        tracing::info!("Completed receiving file upload:");

        let vfs = self.vfs.read().await;
        let bytes_written = buffer.len() as u64;

        // TODO: DELETE THIS LOG
        tracing::info!("Completed receiving file upload READ:");

        match vfs
            .create(&metadata.destination_path, Bytes::from(buffer))
            .await
        {
            Ok(_) => Ok(Response::new(CopyFromLocalResponse {
                success: true,
                message: format!(
                    "Successfully uploaded file to {}",
                    metadata.destination_path
                ),
                bytes_written,
            })),
            Err(e) => Ok(Response::new(CopyFromLocalResponse {
                success: false,
                message: format!("Failed to upload file: {}", e),
                bytes_written: 0,
            })),
        }
    }

    type CopyToLocalStream = ReceiverStream<Result<CopyToLocalResponse, Status>>;

    async fn copy_to_local(
        &self,
        request: Request<CopyToLocalRequest>,
    ) -> Result<Response<Self::CopyToLocalStream>, Status> {
        let req = request.into_inner();
        tracing::info!("Received download request: path={}", req.source_path);

        let vfs = self.vfs.read().await;
        let data = vfs
            .read(&req.source_path)
            .await
            .map_err(|e| Status::not_found(format!("Failed to read file: {}", e)))?;

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            // Send metadata first
            let metadata = common::file::CopyToLocalMetadata {
                file_size: data.len() as u64,
            };

            if tx
                .send(Ok(CopyToLocalResponse {
                    data: Some(common::file::copy_to_local_response::Data::Metadata(
                        metadata,
                    )),
                }))
                .await
                .is_err()
            {
                return;
            }

            // Send data in chunks
            const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks
            for chunk in data.chunks(CHUNK_SIZE) {
                let response = CopyToLocalResponse {
                    data: Some(common::file::copy_to_local_response::Data::Chunk(
                        chunk.to_vec(),
                    )),
                };

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type ReadFileStream = ReceiverStream<Result<ReadFileResponse, Status>>;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let req = request.into_inner();
        tracing::info!("Received read file request: path={}", req.path);

        let vfs = self.vfs.read().await;
        let data = vfs
            .read(&req.path)
            .await
            .map_err(|e| Status::not_found(format!("Failed to read file: {}", e)))?;

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            const CHUNK_SIZE: usize = 64 * 1024;
            for chunk in data.chunks(CHUNK_SIZE) {
                let response = ReadFileResponse {
                    chunk: chunk.to_vec(),
                };

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
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
            Some(common::file::write_file_request::Data::Metadata(m)) => m,
            _ => return Err(Status::invalid_argument("First message must be metadata")),
        };

        tracing::info!("Receiving file write: path={}", metadata.path);

        let mut buffer = Vec::new();

        // Read chunks
        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("Failed to receive chunk: {}", e)))?
        {
            match msg.data {
                Some(common::file::write_file_request::Data::Chunk(chunk)) => {
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

    async fn remove_file(
        &self,
        request: Request<RemoveFileRequest>,
    ) -> Result<Response<RemoveFileResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("Received remove file request: path={}", req.path);

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
        tracing::info!("Received remove directory request: path={}", req.path);

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
) -> Result<ufs::UfsConfig, String> {
    if uri.starts_with("s3://") || uri.starts_with("s3a://") {
        let without_prefix = uri
            .strip_prefix("s3://")
            .or_else(|| uri.strip_prefix("s3a://"))
            .unwrap();

        let parts: Vec<&str> = without_prefix.splitn(2, '/').collect();
        let bucket = parts[0].to_string();

        Ok(ufs::UfsConfig::S3 {
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

        Ok(ufs::UfsConfig::Gcs {
            bucket,
            service_account_path: options.get("service_account_path").cloned(),
        })
    } else if uri.starts_with("local://") {
        let path = uri.strip_prefix("local://").unwrap();
        Ok(ufs::UfsConfig::Local {
            root_path: path.to_string(),
        })
    } else if uri.starts_with("mem://") || uri == "memory" {
        Ok(ufs::UfsConfig::Memory)
    } else {
        Err(format!("Unsupported URI scheme: {}", uri))
    }
}
