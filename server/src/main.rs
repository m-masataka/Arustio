mod file_server;
mod raft_server;

use std::{net::SocketAddr, sync::Arc};

use clap::Parser;
use common::{NodeConfig, Result, raft_client::RaftClient};
use file_server::{ArustioFileService, ArustioMountService};
use metadata::{
    RocksMetadataStore,
    metadata::MetadataStore,
    raft::{
        linearizable_read::LinearizableReadHandle, raft_store::RaftMetadataStore,
        rocks_store::RocksStorage,
    },
};
use raft_server::start_raft_server;
use tokio::sync::{RwLock, mpsc};
use tonic::transport::Server;
use vfs::{
    VirtualFileSystem,
    block_accessor::BlockAccessor,
    cache::{
        manager::CacheManager,
        node::{CacheNode, CacheNodeStatus},
    },
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Mode to run: "server" to start gRPC server, or example name ("basic", "s3", "mount", "docker")
    #[arg(short, long, default_value = "server")]
    mode: String,

    /// gRPC server address
    #[arg(long, default_value = "0.0.0.0:50051")]
    addr: String,

    /// Use Raft-based metadata store (distributed)
    #[arg(long, default_value = "false")]
    raft: bool,

    /// Raft node ID (required when --raft is enabled)
    #[arg(long)]
    node_id: Option<u64>,

    /// Raft data directory (default: ./data/node-{id})
    #[arg(long)]
    data_dir: Option<String>,

    /// Raft bind address for inter-node communication (default: 0.0.0.0:7001)
    #[arg(long)]
    raft_addr: Option<String>,

    /// Path to configuration file
    #[arg(long, default_value = "config.toml")]
    config_path: String,

    /// Cache node ID for this instance
    #[arg(long)]
    cache_node_id: Option<String>,

    /// Cache node endpoint URL for this instance
    #[arg(long)]
    cache_node_url: Option<String>,

    /// Cache node listen address (default: 0.0.0.0:50052)
    #[arg(long)]
    cache_listen_addr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    let args = Args::parse();
    let node_id = args.node_id.unwrap_or(1);
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| format!("./data/node-{}", node_id));
    let config_path = args.config_path.clone();
    let cache_node_id = match args.cache_node_id {
        Some(id) => id,
        None => format!("cachenode-{}", node_id),
    };
    let cache_node_url = match args.cache_node_url {
        Some(url) => url,
        None => "127.0.0.1:50052".to_string(),
    };
    tracing::info!(
        "Starting Arustio node {} with data dir: {}",
        node_id,
        data_dir
    );
    let node_cfg = NodeConfig::from_file(&config_path)
        .map_err(|e| common::Error::Internal(format!("NodeConfig::from_file: {e}")))?;
    tracing::debug!("Loaded config: {:?}", node_cfg);
    let listen_addr = node_cfg.fs_listen.parse().map_err(|e| {
        common::Error::Internal(format!("Invalid fs_listen address in config: {}", e))
    })?;

    let raft_bootstrap_addr = format!("http://{}", node_cfg.listen);
    let raft_client = RaftClient::new(raft_bootstrap_addr);

    let mut ring_nodes: Vec<String> = Vec::new();
    let cache_node = CacheNode {
        node_id: cache_node_id.clone(),
        url: cache_node_url,
        weight: 1,
        status: CacheNodeStatus::Up,
        last_heartbeat: None,
        zone: "default".to_string(),
        description: "Default cache node".to_string(),
    };
    ring_nodes.push(cache_node_id.clone());
    let cache_manager = Arc::new(CacheManager::new(
        raft_client.clone(),
        cache_node,
        ring_nodes,
        512 * 1024 * 1024,
    ));

    let block_accessor = Arc::new(BlockAccessor::new(raft_client.clone()));

    // Spawn CacheManager maintenance task
    {
        let cache = cache_manager.clone();
        tokio::spawn(async move {
            loop {
                cache.run_maintenance().await;
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });
    }

    if args.raft {
        let storage = RocksStorage::open(&data_dir)
            .map_err(|e| common::Error::Internal(format!("RocksStorage::open: {e}")))?;
        let (read_tx, read_rx) = mpsc::channel(128);
        let linearizer = LinearizableReadHandle::new(read_tx);
        let metadata: Arc<dyn MetadataStore> = Arc::new(RaftMetadataStore::new(
            raft_client,
            storage.clone(),
            linearizer.clone(),
        ));

        let grpc_metadata = metadata.clone();
        tokio::spawn(async move {
            if let Err(e) = run_file_server(
                listen_addr,
                grpc_metadata,
                block_accessor.clone(),
                cache_manager.clone(),
            )
            .await
            {
                tracing::error!("gRPC server exited: {}", e);
            }
        });

        start_raft_server(node_id, node_cfg, storage, read_rx, linearizer).await?;
    } else {
        let metadata: Arc<dyn MetadataStore> = Arc::new(
            RocksMetadataStore::open(&data_dir)
                .map_err(|e| common::Error::Internal(format!("RocksMetadataStore::open: {e}")))?,
        );
        run_file_server(
            listen_addr,
            metadata,
            block_accessor.clone(),
            cache_manager.clone(),
        )
        .await?;
    }

    Ok(())
}

/// Run the file server
async fn run_file_server(
    addr: SocketAddr,
    metadata: Arc<dyn MetadataStore>,
    block: Arc<BlockAccessor>,
    cache: Arc<CacheManager>,
) -> Result<()> {
    // Create Virtual FS with mount support
    let vfs = Arc::new(RwLock::new(VirtualFileSystem::new(metadata, block, cache)));

    // Create gRPC services
    let mount_service = ArustioMountService::new(vfs.clone()).into_server();
    let file_service = ArustioFileService::new(vfs.clone()).into_server();

    tracing::info!("Ready to accept mount and file operations");

    Server::builder()
        .add_service(mount_service)
        .add_service(file_service)
        .serve(addr)
        .await
        .map_err(|e| common::Error::Internal(format!("Server error: {}", e)))?;

    Ok(())
}
