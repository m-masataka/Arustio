use crate::{
    core::file_metadata::{
        FileMetadata,
        MountInfo,
    },
    block::node::BlockNode,
    meta::{
        GetRequest,
        ListMountsRequest,
        PutMountRequest,
        DeleteMountRequest,
        ListChildrenRequest,
        metadata_service_client::MetadataServiceClient
    },
    common::error::{Result, Error},
};
use tonic::{
    transport::Channel,
    Request,
};

#[derive(Clone)]
pub struct MetadataClient {
    client: MetadataServiceClient<Channel>,
}

impl MetadataClient {
    pub async fn new(server_addr: String) -> Result<Self> {
        let client = MetadataServiceClient::connect(server_addr)
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to metadata service: {}", e)))?;
        Ok(Self { client })
    }

    pub async fn get(&self, path: String) -> Result<Option<FileMetadata>> {
        let mut client = self.client.clone();
        let request = Request::new(GetRequest { path });
        let response = client.get(request).await
            .map_err(|e| Error::Internal(format!("Failed to get metadata: {}", e)))?;
        let res = response.into_inner();
        if res.found {
            Ok(res.metadata
                .map(|m| 
                    m.try_into()
                )
                .transpose()
                .map_err(|e: String| Error::Internal(e))?
            )
        } else {
            Ok(None)
        }
    }

    pub async fn put(&self, metadata: FileMetadata) -> Result<()> {
        let request = Request::new(crate::meta::PutRequest {
            metadata: Some(metadata.into()),
        });
        let mut client = self.client.clone();
        client.put(request).await
            .map_err(|e| Error::Internal(format!("Failed to put metadata: {}", e)))?;
        Ok(())
    }

    pub async fn delete(&self, path: String) -> Result<()> {
        let request = Request::new(crate::meta::DeleteRequest { path });
        let mut client = self.client.clone();
        client.delete(request).await
            .map_err(|e| Error::Internal(format!("Failed to delete metadata: {}", e)))?;
        Ok(())
    }

    pub async fn list_children(&self, parent_id: &uuid::Uuid) -> Result<Vec<FileMetadata>> {
        let request = Request::new(ListChildrenRequest {
            parent_id: parent_id.to_string(),
        });
        let mut client = self.client.clone();
        let response = client.list_children(request).await
            .map_err(|e| Error::Internal(format!("Failed to list children: {}", e)))?;
        let res = response.into_inner();
        let mut children = Vec::new();
        for child in res.children {
            let file_metadata: FileMetadata = child.try_into()
                .map_err(|e: String| Error::Internal(e))?;
            children.push(file_metadata);
        }
        Ok(children)
    }

    pub async fn list_mounts(&self) -> Result<Vec<MountInfo>> {
        let request = Request::new(ListMountsRequest {});
        let mut client = self.client.clone();
        let response = client.list_mounts(request).await
            .map_err(|e| Error::Internal(format!("Failed to list mounts: {}", e)))?;
        let res = response.into_inner();
        let mount_list = res.mounts;
        let mut mount_list_converted = Vec::new();
        for mount in mount_list {
            let mount_info: MountInfo = mount.try_into()
                .map_err(|e: String| Error::Internal(e))?;
            mount_list_converted.push(mount_info);
        }
        Ok(mount_list_converted)
    }

    pub async fn save_mount(&self, mount: &MountInfo) -> Result<()> {
        let request = Request::new(PutMountRequest {
            mount: Some(mount.clone().into()),
        });
        let mut client = self.client.clone();
        client.put_mount(request).await
            .map_err(|e| Error::Internal(format!("Failed to put mount: {}", e)))?;
        Ok(())
    }

    pub async fn delete_mount(&self, path: &str) -> Result<()> {
        let request = Request::new(DeleteMountRequest {
            path: path.to_string(),
        });
        let mut client = self.client.clone();
        client.delete_mount(request).await
            .map_err(|e| Error::Internal(format!("Failed to delete mount: {}", e)))?;
        Ok(())
    }

    // Block node management
    pub async fn list_block_nodes(&self) -> Result<Vec<BlockNode>> {
        let request = Request::new(crate::meta::ListBlockNodesRequest {});
        let mut client = self.client.clone();
        let response = client.list_block_nodes(request).await
            .map_err(|e| Error::Internal(format!("Failed to list block nodes: {}", e)))?;
        let res = response.into_inner();
        let mut block_nodes = Vec::new();
        for bn in res.nodes {
            let block_node: BlockNode = bn.try_into()
                .map_err(|e| Error::Internal(format!("{:?}", e)))?;
            block_nodes.push(block_node);
        }
        Ok(block_nodes)
    }
}