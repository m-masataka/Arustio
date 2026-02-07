use std::sync::Arc;

use crate::{
    core::file_metadata::MountInfo,
    meta::{
        DeleteRequest, DeleteResponse,
        GetByIdRequest, GetByIdResponse,
        GetRequest, GetResponse,
        PutRequest, PutResponse,
        ListChildrenRequest, ListChildrenResponse,
        GetMountRequest, GetMountResponse,
        PutMountRequest, PutMountResponse,
        DeleteMountRequest, DeleteMountResponse,
        ListMountsRequest, ListMountsResponse,
        ListBlockNodesRequest, ListBlockNodesResponse,
        metadata_service_server::{
            MetadataService, MetadataServiceServer
        }
    },
    metadata::metadata::MetadataStore
};

use tonic::{Request, Response, Status};

pub struct MetadataServiceImpl {
    metadata_store: Arc<dyn MetadataStore>,
}

impl MetadataServiceImpl {
    pub fn new(metadata_store: Arc<dyn MetadataStore>) -> Self {
        MetadataServiceImpl { metadata_store }
    }

    pub fn into_server(self) -> MetadataServiceServer<Self> {
        MetadataServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl MetadataService for MetadataServiceImpl {
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let path = request.into_inner().path;
        // Placeholder logic for fetching metadata by path
        let metadata_opt= self
            .metadata_store
            .get(&path)
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?;

        let response = match metadata_opt {
            None => GetResponse {
                metadata: None,
                found: false,
            },
            Some(metadata) => GetResponse {
                metadata: Some(metadata.into()),
                found: true,
            },
        };
        Ok(Response::new(response))
    }

    async fn get_by_id(
        &self,
        request: Request<GetByIdRequest>,
    ) -> Result<Response<GetByIdResponse>, Status> {
        let id = request.into_inner().id;
        let uuid = uuid::Uuid::parse_str(&id)
            .map_err(|_| Status::invalid_argument("Invalid UUID format"))?;
        // Placeholder logic for fetching metadata by ID
        let metadata_opt= self
            .metadata_store
            .get_by_id(&uuid)
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?;

        let response = match metadata_opt {
            None => GetByIdResponse {
                metadata: None,
                found: false,
            },
            Some(metadata) => GetByIdResponse {
                metadata: Some(metadata.into()),
                found: true,
            },
        };
        Ok(Response::new(response))
    }

    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let metadata = req
            .metadata
            .ok_or_else(|| Status::invalid_argument("Missing metadata"))?
            .try_into()
            .map_err(|e| Status::invalid_argument(format!("Invalid metadata: {}", e)))?;

        // Placeholder logic for storing metadata
        self.metadata_store
            .put(metadata)
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?;

        let response = PutResponse {
            created: true, // Placeholder; determine if it was created or updated
        };
        Ok(Response::new(response))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let path = request.into_inner().path;
        // Placeholder logic for deleting metadata by ID
        self.metadata_store
            .delete(&path)
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?;
        let response = DeleteResponse { deleted: true, }; // Placeholder
        Ok(Response::new(response))
    }

    async fn list_children(
        &self,
        request: Request<ListChildrenRequest>,
    ) -> Result<Response<ListChildrenResponse>, Status> {
        let parent_id_str = request.into_inner().parent_id;
        let parent_id = uuid::Uuid::parse_str(&parent_id_str)
            .map_err(|_| Status::invalid_argument("Invalid UUID format"))?;

        // Placeholder logic for listing children
        let children = self
            .metadata_store
            .list_children(&parent_id)
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?;

        let response = ListChildrenResponse {
            children: children.into_iter().map(|m| m.into()).collect(),
        };
        Ok(Response::new(response))
    }

    async fn put_mount(
        &self,
        request: Request<PutMountRequest>,
    ) -> Result<Response<PutMountResponse>, Status> {
        let req = request.into_inner();
        let mount_metadata: MountInfo = req
            .mount
            .ok_or_else(|| Status::invalid_argument("Missing mount metadata"))?
            .try_into()
            .map_err(|e| Status::invalid_argument(format!("Invalid mount metadata: {}", e)))?;

        // Placeholder logic for storing mount metadata
        self.metadata_store
            .save_mount(&mount_metadata)
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?;

        let response = PutMountResponse {
            created: true, // Placeholder; determine if it was created or updated
        };
        Ok(Response::new(response))
    }

    async fn get_mount(
        &self,
        request: Request<GetMountRequest>,
    ) -> Result<Response<GetMountResponse>, Status> {
        let path = request.into_inner().path;
        // Placeholder logic for fetching mount metadata by path
        let mount_metadata = self
            .metadata_store
            .get_mount(&path)
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?
            .ok_or_else(|| Status::not_found("Mount metadata not found"))?;
        let response = GetMountResponse {
            mount: Some(mount_metadata.into()),
            found: true,
        };
        Ok(Response::new(response))
    }

    async fn delete_mount(
        &self,
        request: Request<DeleteMountRequest>,
    ) -> Result<Response<DeleteMountResponse>, Status> {
        let path = request.into_inner().path;
        // Placeholder logic for deleting mount metadata by path
        self.metadata_store
            .delete_mount(&path)
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?;
        let response = DeleteMountResponse { deleted: true, }; // Placeholder
        Ok(Response::new(response))
    }

    async fn list_mounts(
        &self,
        _request: Request<ListMountsRequest>,
    ) -> Result<Response<ListMountsResponse>, Status> {
        // Placeholder logic for listing all mounts
        let mounts = self
            .metadata_store
            .list_mounts()
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?;

        let response = ListMountsResponse {
            mounts: mounts.into_iter().map(|m| m.into()).collect(),
        };
        Ok(Response::new(response))
    }

    async fn list_block_nodes(
        &self,
        _request: Request<ListBlockNodesRequest>,
    ) -> Result<Response<ListBlockNodesResponse>, Status> {
        let block_nodes = self
            .metadata_store
            .list_block_nodes()
            .await
            .map_err(|e| Status::internal(format!("Metadata store error: {}", e)))?;

        let response = ListBlockNodesResponse {
            nodes: block_nodes.into_iter().map(|bn| bn.into()).collect(),
        };
        Ok(Response::new(response))
    }
}
