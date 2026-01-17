use std::{net::SocketAddr, sync::Arc};

use clap::{Parser, Subcommand };
use tokio::sync::{RwLock, mpsc};
use tonic::transport::Server;
use arustio::{
    block::{
        manager::BlockManager, node::{BlockNode, BlockNodeStatus}
    }, cache::manager::CacheManager, common::{Error, NodeConfig, Result, raft_client::RaftClient}, metadata::{
        RocksMetadataStore,
        metadata::MetadataStore,
        raft::{
            linearizable_read::LinearizableReadHandle, raft_store::RaftMetadataStore,
            rocks_store::RocksStorage,
        },
    }, server::{
        file_server::{ArustioFileService, ArustioMountService},
        raft_server::start_raft_server,
    },
    vfs::virtual_file_system::VirtualFileSystem,
    cmd::{
        fs_command::FsCommands,
        handler::{
            handle_fs_command,
        }
    },
};

#[derive(Parser)]
#[command(name = "arustio")]
#[command(version, about = "Arustio distributed file system CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// File system operations
    Server {
        /// gRPC server address
        #[arg(long, default_value = "0.0.0.0:50051")]
        addr: String,

        /// Use Raft-based metadata store (distributed)
        #[arg(long, default_value = "false")]
        raft: bool,

        /// Raft node ID (required when --raft is enabled)
        #[arg(long)]
        raft_node_id: Option<u64>,

        /// Raft data directory (default: ./data/node-{id})
        #[arg(long)]
        data_dir: Option<String>,

        /// Raft bind address for inter-node communication (default: 0.0.0.0:7001)
        #[arg(long)]
        raft_addr: Option<String>,

        /// Path to configuration file
        #[arg(long, default_value = "config.toml")]
        config_path: String,

        /// Block node ID for this instance
        #[arg(long)]
        block_node_id: Option<String>,

        /// Block node endpoint URL for this instance
        #[arg(long)]
        block_node_url: Option<String>,
    },
    /// File system operations
    Fs {
        #[command(subcommand)]
        fs_command: FsCommands,

        /// Server address to connect to
        #[arg(short, long, default_value = "http://localhost:50051")]
        server: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Server {
            addr,
            data_dir,
            raft,
            raft_node_id,
            raft_addr: _,
            config_path,
            block_node_id,
            block_node_url,
        } => {
            tracing::info!("Starting Arustio server...");
            let socket_addr: SocketAddr = addr.parse().map_err(|e| {
                Error::Internal(format!("Invalid server address '{}': {}", addr, e))
            })?;
            let block_node_id = match block_node_id {
                Some(id) => id,
                None => format!("cachenode-{}", raft_node_id.unwrap_or(1)),
            };
            let block_node_url = match block_node_url {
                Some(url) => url,
                None => format!("http://localhost:{}", 23451),
            };
            let data_dir = match data_dir {
                Some(dir) => dir,
                None => format!("./data/node-{}", block_node_id),
            };
            run_server(
                data_dir,
                config_path,
                block_node_id,
                block_node_url,
                raft,
                raft_node_id,
            )
            .await
        },
        Commands::Fs { fs_command, server } => {
            let _ = handle_fs_command(fs_command, &server).await;
            Ok(())
        }
    }
}

async fn run_server(
        data_dir: String,
        config_path: String,
        block_node_id: String,
        block_node_url: String,
        raft: bool,
        raft_node_id: Option<u64>,
    ) -> Result<()> {
    tracing::info!(
        "Starting Arustio node {} with data dir: {}",
        block_node_id,
        data_dir
    );
    let node_cfg = NodeConfig::from_file(&config_path)
        .map_err(|e| Error::Internal(format!("NodeConfig::from_file: {e}")))?;
    tracing::debug!("Loaded config: {:?}", node_cfg);
    let listen_addr = node_cfg.fs_listen.parse().map_err(|e| {
        Error::Internal(format!("Invalid fs_listen address in config: {}", e))
    })?;


    let mut ring_nodes: Vec<String> = Vec::new();
    let block_node = BlockNode {
        node_id: block_node_id.clone(),
        url: block_node_url,
        weight: 1,
        status: BlockNodeStatus::Up,
        last_heartbeat: None,
        zone: "default".to_string(),
        description: "Default block node".to_string(),
    };
    ring_nodes.push(block_node_id.clone());

    let capacity_bytes= 0;
    let cache_manager = Arc::new(CacheManager::new(capacity_bytes));

    tracing::info!(
        "Starting file server on {} (Raft enabled: {})",
        listen_addr,
        raft
    );
    tracing::info!("raft_listen = {}", node_cfg.raft_listen);

    if raft {
        tracing::debug!("Initializing Raft-based MetadataStore ...");
        let raft_node_id = match raft_node_id {
            Some(id) => id,
            None => {
                return Err(Error::Internal(
                    "Raft node ID must be specified when --raft is enabled".to_string(),
                ))
            }
        };
        let raft_bootstrap_addr = format!("http://{}", node_cfg.raft_listen);
        let raft_client = RaftClient::new(raft_bootstrap_addr);
        let storage = RocksStorage::open(&data_dir)
            .map_err(|e| Error::Internal(format!("RocksStorage::open: {e}")))?;
        let (read_tx, read_rx) = mpsc::channel(128);
        let linearizer = LinearizableReadHandle::new(read_tx);
        let metadata_store: Arc<dyn MetadataStore> = Arc::new(RaftMetadataStore::new(
            raft_client,
            storage.clone(),
            linearizer.clone(),
        ));
        let block_manager = Arc::new(BlockManager::new(
            metadata_store.clone(),
            block_node,
            ring_nodes,
        ));

        // Spawn BlockManager maintenance task
        {
            let block_manager = block_manager.clone();
            tokio::spawn(async move {
                loop {
                    block_manager.run_maintenance().await;
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                }
            });
        }

        tracing::info!("Starting gRPC file server ...");

        let grpc_metadata_store = metadata_store.clone();
        tokio::spawn(async move {
            tracing::debug!("gRPC file server task started addressing {}", listen_addr);
            if let Err(e) = run_file_server(
                listen_addr,
                grpc_metadata_store.clone(),
                cache_manager.clone(),
            )
            .await
            {
                tracing::error!("gRPC server exited: {}", e);
            }
        });


        tracing::info!("Starting Raft server with node ID {}", raft_node_id);
        
        start_raft_server(raft_node_id, node_cfg, storage, read_rx, linearizer).await?;
    } else {
        let metadata_store: Arc<dyn MetadataStore> = Arc::new(
            RocksMetadataStore::open(&data_dir)
                .map_err(|e| Error::Internal(format!("RocksMetadataStore::open: {e}")))?,
        );
        let block_manager = Arc::new(BlockManager::new(
            metadata_store.clone(),
            block_node,
            ring_nodes,
        ));

        // Spawn CacheManager maintenance task
        {
            let block_manager = block_manager.clone();
            tokio::spawn(async move {
                loop {
                    block_manager.run_maintenance().await;
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                }
            });
        }

        run_file_server(
            listen_addr,
            metadata_store,
            cache_manager.clone(),
        )
        .await?;
    }

    Ok(())
}

/// Run the file server
async fn run_file_server(
    addr: SocketAddr,
    metadata_store: Arc<dyn MetadataStore>,
    cache: Arc<CacheManager>,
) -> Result<()> {
    tracing::info!("Starting gRPC file server on {}", addr);
    // Create Virtual FS with mount support
    let vfs = Arc::new(RwLock::new(VirtualFileSystem::new(metadata_store, cache)));
    tracing::debug!("VirtualFileSystem initialized");

    // Create gRPC services
    let mount_service = ArustioMountService::new(vfs.clone()).into_server();
    tracing::debug!("Mount service initialized");
    let file_service = ArustioFileService::new(vfs.clone()).into_server();

    tracing::info!("Ready to accept mount and file operations");

    Server::builder()
        .add_service(mount_service)
        .add_service(file_service)
        .serve(addr)
        .await
        .map_err(|e| Error::Internal(format!("Server error: {}", e)))?;

    Ok(())
}
