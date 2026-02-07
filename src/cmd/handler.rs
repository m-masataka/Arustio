use crate::{
    client::virtual_file_system::VirtualFileSystem, cmd::fs_command::FsCommands, common::{
        error::Error
    },
    ufs::config::{UfsConfig, parse_uri_to_config},
    core::{
        file_system::FileSystem,
        file_metadata::FileType,
    },
};

use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use bytes::Bytes;

pub async fn handle_fs_command(command: FsCommands, vfs: VirtualFileSystem) -> anyhow::Result<()> {
    match command {
        FsCommands::Mount { path, uri, option } => {
            handle_mount(path, uri, option, &vfs).await?;
        }
        FsCommands::Unmount { path } => {
            handle_unmount(path, &vfs).await?;
        }
        FsCommands::List => {
            handle_list_mounts(&vfs).await?;
        }
        FsCommands::Ls { path, long } => {
            handle_ls(path, long, &vfs).await?;
        }
        FsCommands::Stat { path } => {
            handle_stat(path, &vfs).await?;
        }
        FsCommands::Mkdir { path } => {
            handle_mkdir(path, &vfs).await?;
        }
        FsCommands::CopyFromLocal {
            local_path,
            remote_path,
        } => {
            handle_copy_from_local(local_path, remote_path, &vfs).await?;
        }
        FsCommands::CopyToLocal {
            remote_path,
            local_path,
        } => {
            handle_copy_to_local(remote_path, local_path, &vfs).await?;
        }
    }
    Ok(())
}

async fn handle_mount(
    path: String,
    uri: String,
    options: Vec<String>,
    vfs: &VirtualFileSystem,
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
    let ufs_config = parse_uri_to_config(&uri, mount_options)
        .map_err(|e| anyhow::anyhow!("Failed to parse URI to UfsConfig: {}", e))?;
    vfs.mount(&path, ufs_config).await?;
    println!("Successfully mounted {} to {}", uri, path);
    Ok(())
}

async fn handle_unmount(path: String, vfs: &VirtualFileSystem) -> anyhow::Result<()> {
    println!("Unmounting {}", path);
    vfs.unmount(&path).await?;
    println!("Successfully unmounted {}", path);
    Ok(())
}

async fn handle_list_mounts(vfs: &VirtualFileSystem) -> anyhow::Result<()> {
    let mounts = vfs.list_mounts().await;

    if mounts.is_empty() {
        println!("No mount points found.");
        return Ok(());
    }

    // Print header
    println!(
        "{:<30} {:<15} {:<50} {}",
        "Mount Point", "Type", "URI", "Description"
    );
    println!("{}", "-".repeat(110));

    for mount in mounts {
        let path = mount.path;
        let ufs_config: UfsConfig = UfsConfig::try_from(mount.ufs_config.clone())
            .map_err(|e| Error::Internal(format!("Failed to convert UfsConfig: {}", e)))?;
        let description = match mount.description {
            Some(desc) => desc,
            None => "".to_string(),
        };
        let (ufs_type, ufs_uri) = match ufs_config {
            UfsConfig::S3 { bucket, .. } => (
                "S3".to_string(),
                format!("s3a://{}/", bucket.clone()),
            ),
            UfsConfig::Local { .. } => (
                "Local".to_string(),
                "local://".to_string(),
            ),
            other => (
                "Other".to_string(),
                format!("{:?}", other),
            ),
        };
        println!(
            "{:<30} {:<15} {:<50} {}",
            path, ufs_type, ufs_uri, description
        );
    }

    Ok(())
}

async fn handle_ls(path: String, long_format: bool, vfs: &VirtualFileSystem) -> anyhow::Result<()> {
    
    let entries = vfs.list(&path).await?;

    if entries.is_empty() {
        println!("No files found in {}", path);
        return Ok(());
    }

    for entry in entries {
        if long_format {
            let file_type = if entry.file_type == FileType::Directory {
                "d"
            } else {
                "-"
            };
            let timestamp = entry.modified_at;
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

async fn handle_stat(path: String, vfs: &VirtualFileSystem) -> anyhow::Result<()> {
    let entry = vfs.stat(&path).await?;

    // Assuming path is unique, take the first entry
    let file_type = if entry.file_type == FileType::Directory {
        "Directory"
    } else {
        "File"
    };

    let timestamp = entry.modified_at;
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

async fn handle_mkdir(path: String, vfs: &VirtualFileSystem) -> anyhow::Result<()> {
    println!("Creating directory {}", path);
    match vfs.mkdir(&path).await {
        Err(e) => {
            anyhow::bail!("Failed to create directory {}: {}", path, e);
        }
        Ok(_) => {}
    }

    println!("Successfully created directory {}", path);
    Ok(())
}

async fn handle_copy_from_local(
    local_path: String,
    remote_path: String,
    vfs: &VirtualFileSystem,
) -> anyhow::Result<()> {
    println!("Copying {} to {}", local_path, remote_path);
    let file = File::open(&local_path).await?;
    let stream = ReaderStream::new(file)
        .map(|r| r.map_err(|e| crate::common::error::Error::Io(e)));
    vfs.write(&remote_path, Box::pin(stream)).await?;
    println!(
        "Successfully uploaded {} to {}",
        local_path, remote_path
    );
    Ok(())
}

async fn handle_copy_to_local(
    remote_path: String,
    local_path: String,
    vfs: &VirtualFileSystem,
) -> anyhow::Result<()> {
    println!("Copying {} to {}", remote_path, local_path);
    let (m, mut stream) = vfs.read(&remote_path).await?;
    // Read metadata first
    let size = m.size;

    println!("File size: {} bytes", size);

    // Create local file
    let mut file = File::create(&local_path).await?;
    let total_bytes = 0u64;

    println!("Downloading...");
    // Read chunks
    while let Some(chunk_res) = stream.next().await {
        let chunk: Bytes = chunk_res?;     // stream の Err をここで返す
        file.write_all(&chunk).await?;
    }
    file.flush().await?;

    println!(
        "Successfully downloaded {} bytes to {}",
        total_bytes, local_path
    );

    Ok(())
}
