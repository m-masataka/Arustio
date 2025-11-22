use anyhow::Result;
use async_trait::async_trait;
use common::raftio;
use protobuf::Message as ProtobufMessage; // Import the trait for write_to_bytes
use raft::prelude::Message;
use raft::prelude::Message as RaftMessage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct PeerStore {
    map: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<u64, String>>>,
}

impl PeerStore {
    pub fn new(initial: Vec<(u64, String)>) -> Self {
        Self {
            map: std::sync::Arc::new(tokio::sync::RwLock::new(initial.into_iter().collect())),
        }
    }
    async fn resolve(&self, id: u64) -> Option<String> {
        self.map.read().await.get(&id).cloned()
    }
    async fn upsert(&self, id: u64, addr: String) {
        self.map.write().await.insert(id, addr);
    }
    async fn remove(&self, id: u64) {
        self.map.write().await.remove(&id);
    }

    pub async fn get_peer_ip(&self, id: u64) -> String {
        match self.resolve(id).await {
            Some(addr) => addr,
            None => "unknown".to_string(),
        }
    }
}

pub struct GrpcTransport {
    self_id: u64,
    peers: PeerStore, // gRPC channels to each peer
    loopback_tx: mpsc::Sender<RaftMessage>,
    clients: Arc<RwLock<HashMap<u64, raftio::raft_client::RaftClient<Channel>>>>,
}

impl GrpcTransport {
    pub fn new(self_id: u64, peers: PeerStore, loopback_tx: mpsc::Sender<RaftMessage>) -> Self {
        Self {
            self_id,
            peers,
            loopback_tx,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_or_connect_client(
        &self,
        to: u64,
    ) -> Result<raftio::raft_client::RaftClient<Channel>> {
        // Cache hit?
        if let Some(c) = self.clients.read().await.get(&to).cloned() {
            return Ok(c);
        }
        // Resolve destination
        let addr = self
            .peers
            .resolve(to)
            .await
            .ok_or_else(|| anyhow::anyhow!("no route to id={}", to))?;
        // Establish tonic Channel (needs http://)
        let endpoint = format!("http://{}", addr);
        let ch = Channel::from_shared(endpoint)?.connect().await?;
        let client = raftio::raft_client::RaftClient::new(ch);
        self.clients.write().await.insert(to, client.clone());
        Ok(client)
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn send(&self, msg: RaftMessage) -> anyhow::Result<()> {
        tracing::debug!("GrpcTransport::send to {}: {:?}", msg.get_to(), msg);
        // Self-targeted messages take the loopback fast path
        if msg.get_to() == self.self_id {
            self.loopback_tx.send(msg).await?;
            return Ok(());
        }

        // Serialize to bytes via rust-protobuf
        let bytes = msg.write_to_bytes()?;

        // Acquire client, then send over gRPC
        let mut client = self.get_or_connect_client(msg.get_to()).await?;
        use raftio::Bytes;
        use tonic::Request;
        client
            .send(Request::new(Bytes { data: bytes }))
            .await
            .map_err(|e| anyhow::anyhow!("gRPC send: {}", e))?;
        Ok(())
    }
}

/// Interface that delivers Raft messages to other nodes
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn send(&self, msg: Message) -> anyhow::Result<()>;
}
