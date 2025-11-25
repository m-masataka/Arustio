mod grpc_server;
mod raft_server;

use clap::Parser;
use common::{NodeConfig, Result};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{RwLock, mpsc};
use tonic::transport::Server;
use vfs::{FileSystem, VirtualFsWithMounts};

use common::raft_client::RaftClient;
use grpc_server::{ArustioFileService, ArustioMountService};
use metadata::RocksMetadataStore;
use metadata::metadata::MetadataStore;
use metadata::raft::linearizable_read::LinearizableReadHandle;
use metadata::raft::raft_store::RaftMetadataStore;
use metadata::raft::rocks_store::RocksStorage;
use raft_server::start_raft_server;

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
    config_path: Option<String>,
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
    let config_path = args
        .config_path
        .clone()
        .unwrap_or_else(|| "config.toml".to_string());
    tracing::info!(
        "Starting Arustio node {} with data dir: {}",
        node_id,
        data_dir
    );
    let node_cfg = NodeConfig::from_file(&config_path)
        .map_err(|e| common::Error::Internal(format!("NodeConfig::from_file: {e}")))?;
    tracing::info!("Loaded config: {:?}", node_cfg);
    let listen_addr = node_cfg.fs_listen.parse().map_err(|e| {
        common::Error::Internal(format!("Invalid fs_listen address in config: {}", e))
    })?;

    if args.raft {
        let storage = RocksStorage::open(&data_dir)
            .map_err(|e| common::Error::Internal(format!("RocksStorage::open: {e}")))?;
        let bootstrap_addr = format!("http://{}", node_cfg.listen);
        let raft_client = RaftClient::new(bootstrap_addr);
        let (read_tx, read_rx) = mpsc::channel(128);
        let linearizer = LinearizableReadHandle::new(read_tx);
        let metadata: Arc<dyn MetadataStore> = Arc::new(RaftMetadataStore::new(
            raft_client,
            storage.clone(),
            linearizer.clone(),
        ));

        let grpc_metadata = metadata.clone();
        tokio::spawn(async move {
            if let Err(e) = run_grpc_server(listen_addr, grpc_metadata).await {
                tracing::error!("gRPC server exited: {}", e);
            }
        });

        start_raft_server(node_id, node_cfg, storage, read_rx, linearizer).await?;
    } else {
        let metadata: Arc<dyn MetadataStore> = Arc::new(
            RocksMetadataStore::open(&data_dir)
                .map_err(|e| common::Error::Internal(format!("RocksMetadataStore::open: {e}")))?,
        );
        run_grpc_server(listen_addr, metadata).await?;
    }

    Ok(())
}

/// Run the gRPC server
async fn run_grpc_server(addr: SocketAddr, metadata: Arc<dyn MetadataStore>) -> Result<()> {
    // Create Virtual FS with mount support
    let vfs = Arc::new(RwLock::new(VirtualFsWithMounts::new(metadata)));

    // Create mount root directory
    {
        let vfs_lock = vfs.read().await;
        if let Err(e) = vfs_lock.mkdir("/mnt").await {
            tracing::warn!("Failed to create /mnt directory: {}", e);
        }

        // Restore mounts from metadata store
        tracing::info!("Restoring mount points from metadata store...");
        if let Err(e) = vfs_lock.restore_mounts().await {
            tracing::error!("Failed to restore mounts: {}", e);
        } else {
            tracing::info!("Mount points restored successfully");
        }
    }

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
