// create client class
use crate::meta::{
    meta_api_client::MetaApiClient,
    MetaCmd,
    meta_cmd, // module generated for the oneof cases
    Mkdir,
    GetLeaderRequest, // Add this import
};

#[derive(Clone)]
pub struct RaftClient {
    bootstrap_address: String,
}

impl RaftClient {
    pub async fn new() -> Self {
        Self {
            bootstrap_address: "http://localhost:50051".to_string(),
        }
    }

    pub async fn get_leader_address(&self) -> Result<String, Box<dyn std::error::Error>> {
        // In a real implementation, you would query the cluster for the current leader.
        // Get the leader node id
        let mut client = MetaApiClient::connect(self.bootstrap_address.clone()).await?;
        let request = tonic::Request::new(GetLeaderRequest {});
        let response = client.get_leader(request).await?;
        let leader_info = response.into_inner();
        Ok(leader_info.leader_ip)
    }

    pub async fn send_command(
        &self,
        cmd: MetaCmd,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let leader_address = self.get_leader_address().await?;
        tracing::info!("Leader address: {}", leader_address);
        let dst_url = format!("http://{}", leader_address);
        tracing::info!("Connecting to leader at: {}", dst_url);
        let mut client = MetaApiClient::connect(dst_url).await?;

        let request = tonic::Request::new(cmd);
        let response = client.apply(request).await?;
        tracing::info!("Command applied: {:?}", response.into_inner());
        Ok(())
    }
}
