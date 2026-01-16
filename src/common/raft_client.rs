// create client class
use crate::meta::{
    BlockNodeInfo, FileMetadata, GetBlockNodesRequest, GetFileMetaRequest, GetLeaderRequest,
    MetaCmd, meta_api_client::MetaApiClient,
};
use std::io;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone)]
pub struct RaftClient {
    bootstrap_address: String,
}
use super::Result;

impl RaftClient {
    pub fn new<S: Into<String>>(bootstrap_address: S) -> Self {
        Self {
            bootstrap_address: bootstrap_address.into(),
        }
    }

    pub async fn get_leader_address(&self) -> Result<String> {
        let mut client = self.bootstrap_client().await?;
        let request = tonic::Request::new(GetLeaderRequest {});
        let response = client.get_leader(request).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to get leader info: {}", e),
            )
        })?;
        let leader_info = response.into_inner();
        if leader_info.leader_id == 0
            || leader_info.leader_ip.is_empty()
            || leader_info.leader_ip == "unknown"
        {
            return Err(io::Error::new(io::ErrorKind::Other, "Leader is not available yet").into());
        }
        Ok(leader_info.leader_ip)
    }

    pub async fn send_command(&self, cmd: MetaCmd) -> Result<()> {
        let leader_address = self.get_leader_address().await?;
        tracing::debug!("Leader address: {}", leader_address);
        let dst_url = format!("http://{}", leader_address);
        tracing::debug!("Connecting to leader at: {}", dst_url);
        let mut client = MetaApiClient::connect(dst_url).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to connect to leader: {}", e),
            )
        })?;

        let request = tonic::Request::new(cmd);
        let response = client.apply(request).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to send command to leader: {}", e),
            )
        })?;
        tracing::debug!("Command applied: {:?}", response.into_inner());
        Ok(())
    }

    pub async fn get_file_metadata(&self, path: String) -> Result<Option<FileMetadata>> {
        let leader_address = self.get_leader_address().await?;
        tracing::debug!("Leader address: {}", leader_address);
        let dst_url = format!("http://{}", leader_address);
        tracing::debug!("Connecting to leader at: {}", dst_url);
        let mut client = MetaApiClient::connect(dst_url).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to connect to leader: {}", e),
            )
        })?;

        let request = tonic::Request::new(GetFileMetaRequest { full_path: path });
        let response = client.get_file_meta(request).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to get file metadata from leader: {}", e),
            )
        })?;
        let file_meta_response = response.into_inner();
        Ok(file_meta_response.metadata)
    }

    pub async fn get_block_nodes(&self) -> Result<Vec<BlockNodeInfo>> {
        let leader_address = self.get_leader_address().await?;
        tracing::debug!("Leader address: {}", leader_address);
        let dst_url = format!("http://{}", leader_address);
        tracing::debug!("Connecting to leader at: {}", dst_url);
        let mut client = MetaApiClient::connect(dst_url).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to connect to leader: {}", e),
            )
        })?;
        let request = tonic::Request::new(GetBlockNodesRequest {});
        let response = client.get_block_nodes(request).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to get cache nodes: {}", e),
            )
        })?;
        let nodes_response = response.into_inner();
        let nodes: Vec<BlockNodeInfo> = nodes_response.nodes;
        Ok(nodes)
    }

    async fn bootstrap_client(&self) -> Result<MetaApiClient<tonic::transport::Channel>> {
        let mut attempts = 0;
        loop {
            match MetaApiClient::connect(self.bootstrap_address.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) if attempts < 3 => {
                    attempts += 1;
                    tracing::warn!(
                        "Failed to connect to bootstrap {} (attempt {}): {}",
                        self.bootstrap_address,
                        attempts,
                        e
                    );
                    sleep(Duration::from_millis(200 * attempts)).await;
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Failed to connect to bootstrap {} after {} attempts: {}",
                            self.bootstrap_address, attempts, e
                        ),
                    )
                    .into());
                }
            }
        }
    }
}
