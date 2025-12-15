use std::alloc::System;
use std::io::Read;

use clap::{Parser, Subcommand};
use common::file::{
    CopyFromLocalRequest, ListFilesRequest, MkdirRequest, ReadRequest, StatRequest,
    copy_from_local_request, copy_to_local_response, file_service_client::FileServiceClient,
};
use common::meta::{
    MetaCmd,
    Mkdir,
    meta_api_client::MetaApiClient,
    meta_cmd, // module generated for the oneof cases
};
use common::mount::{
    ListRequest, MountRequest, UnmountRequest, mount_service_client::MountServiceClient,
};
use common::read_response;
use common::utils::timestamp_to_system_time;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt;
use ufs::UfsConfig;

#[derive(Parser)]
#[command(name = "arustio")]
#[command(version, about = "Arustio distributed file system CLI", long_about = None)]
struct Cli {
    /// Server address (default: http://localhost:50051)
    #[arg(short, long, default_value = "http://localhost:50051", global = true)]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// File system operations
    Fs {
        #[command(subcommand)]
        fs_command: FsCommands,
    },
    /// Metadata operations
    Meta {
        #[command(subcommand)]
        meta_command: MetaCommands,
    },
}

#[derive(Subcommand, Debug)]
enum MetaCommands {
    /// Create a directory
    MkDir {
        /// Full path (/foo/bar)
        full_path: String,
        /// Inode number (can be set manually, or 0 to let the server assign one)
        inode: u64,
    },
}

#[derive(Subcommand)]
enum FsCommands {
    /// Mount an under file system to a path
    Mount {
        /// Virtual path to mount to (e.g., /dev/dev1)
        #[arg(value_name = "PATH")]
        path: String,

        /// URI of the under file system (e.g., s3a://bucket1/dir1/dir2)
        #[arg(value_name = "URI")]
        uri: String,

        /// Optional mount options (e.g., readonly, write-policy)
        #[arg(short, long, value_name = "KEY=VALUE")]
        option: Vec<String>,
    },

    /// Unmount a path
    Unmount {
        /// Virtual path to unmount (e.g., /dev/dev1)
        #[arg(value_name = "PATH")]
        path: String,
    },

    /// List all mount points
    List,

    /// List files and directories (like ls)
    Ls {
        /// Path to list
        #[arg(value_name = "PATH", default_value = "/")]
        path: String,

        /// Long format (detailed information)
        #[arg(short, long)]
        long: bool,
    },

    /// Stat a file or directory
    Stat {
        /// Path to stat
        #[arg(value_name = "PATH")]
        path: String,
    },

    /// Create a directory
    Mkdir {
        /// Path to create
        #[arg(value_name = "PATH")]
        path: String,
    },

    /// Copy file from local filesystem to Arustio
    CopyFromLocal {
        /// Local file path
        #[arg(value_name = "LOCAL_PATH")]
        local_path: String,

        /// Arustio destination path
        #[arg(value_name = "REMOTE_PATH")]
        remote_path: String,
    },

    /// Copy file from Arustio to local filesystem
    CopyToLocal {
        /// Arustio source path
        #[arg(value_name = "REMOTE_PATH")]
        remote_path: String,

        /// Local destination path
        #[arg(value_name = "LOCAL_PATH")]
        local_path: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Fs { fs_command } => {
            handle_fs_command(fs_command, &cli.server).await?;
        }
        Commands::Meta { meta_command } => {
            handle_meta_command(meta_command, &cli.server).await?;
        }
    }

    Ok(())
}

async fn handle_meta_command(command: MetaCommands, server: &str) -> anyhow::Result<()> {
    let mut client = MetaApiClient::connect(normalize_server_url(server)).await?;

    match command {
        MetaCommands::MkDir { full_path, inode } => {
            let request = tonic::Request::new(MetaCmd {
                op: Some(meta_cmd::Op::Mkdir(Mkdir { full_path, inode })),
            });

            let response = client.apply(request).await?;
            let response = response.into_inner();
            println!("MkDir response: {:?}", response);
        }
    }

    Ok(())
}

async fn handle_fs_command(command: FsCommands, server: &str) -> anyhow::Result<()> {
    match command {
        FsCommands::Mount { path, uri, option } => {
            handle_mount(path, uri, option, server).await?;
        }
        FsCommands::Unmount { path } => {
            handle_unmount(path, server).await?;
        }
        FsCommands::List => {
            handle_list_mounts(server).await?;
        }
        FsCommands::Ls { path, long } => {
            handle_ls(path, long, server).await?;
        }
        FsCommands::Stat { path } => {
            handle_stat(path, server).await?;
        }
        FsCommands::Mkdir { path } => {
            handle_mkdir(path, server).await?;
        }
        FsCommands::CopyFromLocal {
            local_path,
            remote_path,
        } => {
            handle_copy_from_local(local_path, remote_path, server).await?;
        }
        FsCommands::CopyToLocal {
            remote_path,
            local_path,
        } => {
            handle_copy_to_local(remote_path, local_path, server).await?;
        }
    }
    Ok(())
}

async fn handle_mount(
    path: String,
    uri: String,
    options: Vec<String>,
    server: &str,
) -> anyhow::Result<()> {
    println!("Mounting {} to {}", uri, path);

    // Parse options
    let mut mount_options = std::collections::HashMap::new();
    for opt in options {
        if let Some((key, value)) = opt.split_once('=') {
            mount_options.insert(key.to_string(), value.to_string());
        } else {
            anyhow::bail!("Invalid option format: {}. Expected KEY=VALUE", opt);
        }
    }

    // Connect to server and send mount request
    let mut client = MountServiceClient::connect(normalize_server_url(server)).await?;

    let request = tonic::Request::new(MountRequest {
        path: path.clone(),
        uri: uri.clone(),
        options: mount_options,
    });

    let response = client.mount(request).await?;
    let response = response.into_inner();

    if response.success {
        println!("Successfully mounted {} to {}", uri, path);
    } else {
        anyhow::bail!("Failed to mount: {}", response.message);
    }

    Ok(())
}

async fn handle_unmount(path: String, server: &str) -> anyhow::Result<()> {
    println!("Unmounting {}", path);

    // Connect to server and send unmount request
    let mut client = MountServiceClient::connect(normalize_server_url(server)).await?;

    let request = tonic::Request::new(UnmountRequest { path: path.clone() });

    let response = client.unmount(request).await?;
    let response = response.into_inner();

    if response.success {
        println!("Successfully unmounted {}", path);
    } else {
        anyhow::bail!("Failed to unmount: {}", response.message);
    }

    Ok(())
}

async fn handle_list_mounts(server: &str) -> anyhow::Result<()> {
    // Connect to server and get list of mount points
    let mut client = MountServiceClient::connect(normalize_server_url(server)).await?;

    let request = tonic::Request::new(ListRequest {});

    let response = client.list(request).await?;
    let response = response.into_inner();

    if response.mounts.is_empty() {
        println!("No mount points found.");
        return Ok(());
    }

    // Print header
    println!(
        "{:<30} {:<15} {:<50} {}",
        "Mount Point", "Type", "URI", "Description"
    );
    println!("{}", "-".repeat(110));

    for mount in response.mounts {
        let path = mount.full_path;
        let ufs_config: UfsConfig = UfsConfig::try_from(mount.ufs_config.ok_or_else(|| {
            common::Error::Internal("Missing ufs_config in mount entry".to_string())
        })?)
        .map_err(|e| common::Error::Internal(format!("Failed to convert UfsConfig: {}", e)))?;
        let description = mount.description;
        let mut ufs_type = String::new();
        let mut ufs_uri = String::new();
        match ufs_config {
            UfsConfig::S3 { bucket, .. } => {
                ufs_type = "S3".to_string();
                ufs_uri = format!("s3a://{}/", bucket.clone());
            }
            UfsConfig::Local { .. } => {
                ufs_type = "Local".to_string();
                ufs_uri = "local://".to_string();
            }
            other => {
                ufs_type = "Other".to_string();
                ufs_uri = format!("{:?}", other);
            }
        };
        println!(
            "{:<30} {:<15} {:<50} {}",
            path, ufs_type, ufs_uri, description
        );
    }

    Ok(())
}

async fn handle_ls(path: String, long_format: bool, server: &str) -> anyhow::Result<()> {
    let mut client = FileServiceClient::connect(normalize_server_url(server)).await?;

    let request = tonic::Request::new(ListFilesRequest { path: path.clone() });

    let response = client.list_files(request).await?;
    let entries = response.into_inner().entries;

    if entries.is_empty() {
        println!("No files found in {}", path);
        return Ok(());
    }

    for entry in entries {
        if long_format {
            let file_type = if entry.file_type == common::meta::FileType::Directory as i32 {
                "d"
            } else {
                "-"
            };
            let timestamp = match entry.modified_at {
                None => std::time::SystemTime::UNIX_EPOCH,
                Some(ts) => timestamp_to_system_time(&ts),
            };
            let modified_at = chrono::DateTime::<chrono::Local>::from(timestamp);
            println!(
                "{} {:>10} {} {}",
                file_type, entry.size, modified_at, entry.path
            );
        } else {
            println!("{}", entry.path);
        }
    }

    Ok(())
}

async fn handle_stat(path: String, server: &str) -> anyhow::Result<()> {
    let mut client = FileServiceClient::connect(normalize_server_url(server)).await?;

    let request = tonic::Request::new(StatRequest { path: path.clone() });

    let response = client.stat(request).await?;
    let entry_opt = response.into_inner().entry;

    let entry = match entry_opt {
        None => anyhow::bail!("Path not found: {}", path),
        Some(e) => e,
    };

    // Assuming path is unique, take the first entry
    let file_type = if entry.file_type == common::meta::FileType::Directory as i32 {
        "Directory"
    } else {
        "File"
    };

    let timestamp = match entry.modified_at {
        None => std::time::SystemTime::UNIX_EPOCH,
        Some(ts) => timestamp_to_system_time(&ts),
    };
    let modified_at = chrono::DateTime::<chrono::Local>::from(timestamp);
    let blocks = entry
        .blocks
        .iter()
        .map(|block| format!("[index {}, size {}]", block.index, block.size))
        .collect::<Vec<_>>()
        .join(", ");
    println!("Path: {}", path);
    println!("Type: {}", file_type);
    println!("Size: {} bytes", entry.size);
    println!("Modified At: {}", modified_at);
    println!("Blocks: {}", blocks);
    Ok(())
}

async fn handle_mkdir(path: String, server: &str) -> anyhow::Result<()> {
    println!("Creating directory {}", path);

    let mut client = FileServiceClient::connect(normalize_server_url(server)).await?;

    let request = tonic::Request::new(MkdirRequest { path: path.clone() });

    let response = client.mkdir(request).await?;
    let response = response.into_inner();

    if response.success {
        println!("Successfully created directory {}", path);
    } else {
        anyhow::bail!("Failed to create directory: {}", response.message);
    }

    Ok(())
}

async fn handle_copy_from_local(
    local_path: String,
    remote_path: String,
    server: &str,
) -> anyhow::Result<()> {
    Ok(())
}

async fn handle_copy_to_local(
    remote_path: String,
    local_path: String,
    server: &str,
) -> anyhow::Result<()> {
    println!("Copying {} to {}", remote_path, local_path);

    let mut client = FileServiceClient::connect(normalize_server_url(server)).await?;

    let request = tonic::Request::new(ReadRequest {
        path: remote_path.clone(),
    });

    let mut stream = client.read(request).await?.into_inner();

    // Read metadata first
    let first_msg = stream
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("Empty response from server"))??;

    let file_size = match first_msg.data {
        Some(read_response::Data::Metadata(m)) => m.size,
        _ => anyhow::bail!("Expected metadata as first message"),
    };

    println!("File size: {} bytes", file_size);

    // Create local file
    let mut file = File::create(&local_path).await?;
    let mut total_bytes = 0u64;

    println!("Downloading...");
    // Read chunks
    while let Some(msg) = stream.next().await {
        let msg = msg?;
        match msg.data {
            Some(read_response::Data::Chunk(chunk)) => {
                file.write_all(&chunk).await?;
                total_bytes += chunk.len() as u64;
            }
            _ => {}
        }
    }

    println!(
        "Successfully downloaded {} bytes to {}",
        total_bytes, local_path
    );

    Ok(())
}

/// Add http:// prefix to server address if not present
fn normalize_server_url(server: &str) -> String {
    if server.starts_with("http://") || server.starts_with("https://") {
        server.to_string()
    } else {
        format!("http://{}", server)
    }
}
