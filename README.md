# Arustio

A high-performance, distributed file system built in Rust, inspired by Alluxio. Arustio provides a unified namespace layer over multiple object storage backends (S3, GCS, Azure Blob Storage) with distributed metadata management.

## Features

- **Mount Support (like Alluxio)**: Mount different storage backends to VFS paths
  - Mount S3 buckets, GCS buckets, Azure containers, etc. to different paths
  - Multiple mounts in a single namespace
  - Dynamic mount/unmount operations
  - Unified namespace across heterogeneous storage systems

- **Unified File System (UFS)**: Unified interface for multiple object storage backends
  - Amazon S3
  - Google Cloud Storage (GCS)
  - Azure Blob Storage
  - Local filesystem (for testing)
  - In-memory storage (for testing)

- **Virtual File System (VFS)**: Filesystem-like operations on top of object storage
  - Create/read/write files
  - Create/list/remove directories
  - File metadata management
  - Path-based operations

- **Distributed Metadata**: Metadata management with high availability
  - OpenRaft-based consensus protocol (planned)
  - RocksDB for persistent storage
  - In-memory storage for development

- **High Performance**: Built with Rust for performance and safety
  - Async/await with Tokio runtime
  - Zero-copy operations where possible
  - Efficient memory management

## Architecture

```
┌─────────────────────────────────────────┐
│         Virtual FS Layer (VFS)          │
│  (File operations: mkdir, read, write)  │
└─────────────────────────────────────────┘
                    │
        ┌───────────┴────────────┐
        │                        │
┌───────▼────────┐    ┌─────────▼─────────┐
│   Metadata     │    │   UFS (Unified    │
│   Management   │    │   File System)    │
│ (Raft+RocksDB) │    │   (object_store)  │
└────────────────┘    └─────────┬─────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
        ┌─────▼─────┐   ┌──────▼──────┐   ┌─────▼─────┐
        │    S3     │   │     GCS     │   │   Azure   │
        └───────────┘   └─────────────┘   └───────────┘
```

### Metadata Layer (Raft + RocksDB)

Arustio persists the namespace tree, file metadata, and mount table through a Raft-backed metadata service. Each node embeds a RocksDB instance that stores:

- File metadata records keyed by normalized path and UUID
- Mount definitions (VFS path → UFS configuration)
- Cache node information and lease metadata

Writes flow through the `RaftClient`, which contacts the current leader and replicates a `MetaCmd` entry. Followers apply entries via `apply_to_kv`, ensuring every node converges to the same metadata view. Reads can be linearized by sending them through the `LinearizableReadHandle`, allowing strict consistency when required.

### Cache Layer

To reduce repeated reads from remote object stores, the Virtual File System talks to a distributed cache tier. Each cache node registers itself via the metadata Raft log (`AddCacheNode` command). The `CacheManager` periodically fetches the latest node list, builds a consistent hash ring, and exposes APIs to store or retrieve byte ranges. Reads follow this flow:

1. `BlockAccessor` plans block boundaries (`CHUNK_SIZE`) for a file and maps each block to a cache node using the hash ring.
2. `CacheManager` attempts to serve requested blocks from the local in-process cache (Moka). Misses fall back to the owning cache node or the backing UFS.
3. Cache writes propagate asynchronously, so UFS remains the source of truth while cached blocks speed up hot paths.

This design lets the cluster scale horizontally: more cache nodes simply join the ring, and Raft metadata ensures every server instance observes the same membership list.

For additional detail on each subsystem, see `docs/architecture.md`.

## Project Structure

- **common**: Common types, error handling, and utilities
- **ufs**: Unified File System layer (object storage abstraction)
- **vfs**: Virtual File System layer (filesystem operations)
- **metadata**: Metadata management (Raft + RocksDB)
- **server**: Server implementation and examples
- **command**: CLI tool for interacting with Arustio server

## Quick Start

### Prerequisites

- Rust 1.70 or later
- For S3/GCS/Azure examples: appropriate cloud credentials

### Build

```bash
cargo build --release
```

### Run Basic Example

```bash
cargo run --bin server -- --example basic
```

This runs an in-memory example demonstrating:
- Creating directories
- Writing and reading files
- Listing directory contents
- File operations (stat, update, delete)

### Run Mount Example

```bash
cargo run --bin server -- --example mount
```

This demonstrates the mount functionality, similar to Alluxio:
- Mounting multiple storage backends to different VFS paths
- Writing and reading files from different mount points
- Unmounting storage backends
- Listing active mount points

### Run S3 Example

```bash
# Set environment variables
export S3_BUCKET=your-bucket-name
export S3_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Run the example
cargo run --bin server -- --example s3
```

## Docker Quick Start

The easiest way to try Arustio with real S3-compatible storage (MinIO):

### Automated Test (Recommended)

```bash
# Run the automated test script
./test-docker.sh
```

This script will:
- Start MinIO and Arustio containers
- Wait for services to be ready
- Run the mount example
- Show results and ask if you want to keep services running

### Manual Start

```bash
# Using Make
make docker-up      # Start services
make docker-test    # Run mount example
make docker-down    # Stop services

# Or using docker-compose
docker-compose up -d
sleep 10
docker-compose exec arustio arustio-server --example docker
```

### What's Included

- **MinIO**: S3-compatible object storage running on `localhost:9000`
- **MinIO Console**: Web UI at `http://localhost:9001` (login: minioadmin/minioadmin)
- **Arustio Server**: Pre-configured to use MinIO

### The Docker Example Demonstrates

1. Mounting three different MinIO buckets to different VFS paths:
   - `test-bucket` → `/mnt/s3-test`
   - `data-bucket` → `/mnt/s3-data`
   - `temp-bucket` → `/mnt/s3-temp`

2. Creating files in different buckets through the unified namespace

3. Reading files across different storage backends

4. Listing directory contents

5. Cross-bucket file operations

### Interactive Testing

```bash
# Enter the container
docker-compose exec arustio /bin/bash

# Run different examples
arustio-server --example basic
arustio-server --example mount
arustio-server --example docker

# View MinIO buckets
# Open http://localhost:9001 in your browser
```

### Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v
```

## CLI Tool (arustio)

Arustio provides a command-line tool for interacting with the Arustio server, similar to Alluxio CLI.

### Installation

```bash
# Build and install the CLI
make install-cli

# Or build it manually
cargo build --release -p command
cp target/release/arustio ~/.cargo/bin/
```

### Usage Examples

#### Starting the Server

```bash
# Start the gRPC server (default port: 50051)
cargo run --bin server

# Or specify a custom address
cargo run --bin server -- --addr 0.0.0.0:50051
```

#### Mount Operations

```bash
# Mount memory storage
arustio fs mount /mnt/memory mem://memory

# Mount S3 bucket with credentials
arustio fs mount \
  -o access_key_id=YOUR_ACCESS_KEY \
  -o secret_access_key=YOUR_SECRET_KEY \
  -o region=us-west-2 \
  /mnt/s3 s3a://my-bucket

# Mount S3 using environment variables (recommended)
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
export AWS_REGION=us-west-2
arustio fs mount /mnt/s3 s3a://my-bucket

# Mount MinIO (S3-compatible storage)
arustio fs mount \
  -o access_key_id=minioadmin \
  -o secret_access_key=minioadmin \
  -o endpoint=http://minio:9000 \
  -o region=us-east-1 \
  /mnt/minio s3://test-bucket

# Mount GCS with service account
arustio fs mount \
  -o service_account_path=/path/to/credentials.json \
  /mnt/gcs gs://my-bucket

# Mount with custom server
arustio -s http://server:50051 fs mount /mnt/s3 s3a://bucket1/dir1/dir2

# List all mount points (shows detailed mount table)
arustio fs list
# Output example:
# Mount Point                    Type            URI                                                Options
# --------------------------------------------------------------------------------------------------------------
# /mnt/local                     Local           local:///tmp/arustio-test                          -
# /mnt/memory                    Memory          mem://memory                                       -
# /mnt/s3                        S3              s3a://my-bucket                                    region=us-west-2

# Unmount a path
arustio fs unmount /mnt/memory
```

#### File Operations

```bash
# Create a directory
arustio fs mkdir /mnt/memory/data

# List files (short format)
arustio fs ls /mnt/memory

# List files (long format with details)
arustio fs ls -l /mnt/memory/data

# Upload a file from local to Arustio
arustio fs copy-from-local ./local-file.txt /mnt/memory/data/remote-file.txt

# Download a file from Arustio to local
arustio fs copy-to-local /mnt/memory/data/remote-file.txt ./downloaded-file.txt
```

### Available Commands

**Mount Management:**
- `arustio fs mount [-o KEY=VALUE...] <PATH> <URI>` - Mount a storage backend to a virtual path
  - Supported URIs:
    - `s3a://bucket` - Amazon S3 or S3-compatible storage
    - `gs://bucket` - Google Cloud Storage
    - `local://path` - Local filesystem
    - `mem://memory` - In-memory storage (for testing)
  - Mount options (`-o`):
    - S3: `access_key_id`, `secret_access_key`, `region`, `endpoint`
    - GCS: `service_account_path`
  - Environment variables (alternative to `-o`):
    - S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_ENDPOINT`
- `arustio fs unmount <PATH>` - Unmount a virtual path
- `arustio fs list` - List all mount points

**File Operations:**
- `arustio fs ls [PATH]` - List files and directories
  - `-l, --long` - Show detailed information (size, modified time, type)
  - Default path: `/`
- `arustio fs mkdir <PATH>` - Create a directory
- `arustio fs copy-from-local <LOCAL_PATH> <REMOTE_PATH>` - Upload a file
- `arustio fs copy-to-local <REMOTE_PATH> <LOCAL_PATH>` - Download a file

### Global Options

- `-s, --server <SERVER>` - Server address (default: http://localhost:50051)
- `-h, --help` - Print help information
- `-V, --version` - Print version information

## Usage

### Creating a Virtual File System with Mount Support

```rust
use ufs::UfsConfig;
use vfs::{VirtualFsWithMounts, FileSystem, MountableFileSystem};
use metadata::InMemoryMetadataStore;
use bytes::Bytes;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create metadata store
    let metadata = Arc::new(InMemoryMetadataStore::new());

    // Create Virtual FS with mount support
    let vfs = VirtualFsWithMounts::new(metadata);

    // Create mount point
    vfs.mkdir("/mnt").await?;

    // Mount S3 bucket to /mnt/s3
    let s3_config = UfsConfig::S3 {
        bucket: "my-bucket".to_string(),
        region: "us-east-1".to_string(),
        access_key_id: Some("...".to_string()),
        secret_access_key: Some("...".to_string()),
        endpoint: None,
    };
    vfs.mount("/mnt/s3", s3_config).await?;

    // Mount GCS bucket to /mnt/gcs
    let gcs_config = UfsConfig::Gcs {
        bucket: "my-gcs-bucket".to_string(),
        service_account_path: None,
    };
    vfs.mount("/mnt/gcs", gcs_config).await?;

    // Use the mounted filesystems
    vfs.mkdir("/mnt/s3/data").await?;
    vfs.create("/mnt/s3/data/file.txt", Bytes::from("Hello S3!")).await?;

    vfs.mkdir("/mnt/gcs/data").await?;
    vfs.create("/mnt/gcs/data/file.txt", Bytes::from("Hello GCS!")).await?;

    // List mount points
    let mounts = vfs.list_mounts().await;
    println!("Active mounts: {:?}", mounts);

    // Unmount when done
    vfs.unmount("/mnt/s3").await?;

    Ok(())
}
```

### Basic Virtual File System (Single Backend)

```rust
use ufs::{Ufs, UfsConfig};
use vfs::{VirtualFs, FileSystem};
use metadata::InMemoryMetadataStore;
use bytes::Bytes;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create UFS with in-memory backend
    let ufs_config = UfsConfig::Memory;
    let ufs = Arc::new(Ufs::new(ufs_config).await?);

    // Create metadata store
    let metadata = Arc::new(InMemoryMetadataStore::new());

    // Create Virtual FS
    let vfs = VirtualFs::new(ufs, metadata);

    // Use the filesystem
    vfs.mkdir("/test").await?;
    vfs.create("/test/file.txt", Bytes::from("Hello!")).await?;
    let data = vfs.read("/test/file.txt").await?;

    Ok(())
}
```

### Configuring Different Storage Backends

```rust
// Amazon S3
let config = UfsConfig::S3 {
    bucket: "my-bucket".to_string(),
    region: "us-east-1".to_string(),
    access_key_id: Some("...".to_string()),
    secret_access_key: Some("...".to_string()),
    endpoint: None,
};

// Google Cloud Storage
let config = UfsConfig::Gcs {
    bucket: "my-bucket".to_string(),
    service_account_path: Some("/path/to/credentials.json".to_string()),
};

// Azure Blob Storage
let config = UfsConfig::Azure {
    container: "my-container".to_string(),
    account: "my-account".to_string(),
    access_key: Some("...".to_string()),
};

// Local filesystem
let config = UfsConfig::Local {
    root_path: "/tmp/arustio".to_string(),
};
```

## Development Status

Arustio is currently in early development. The following features are implemented:

- ✅ UFS layer with object_store integration
- ✅ Virtual FS layer with basic filesystem operations
- ✅ Mount support (mount/unmount storage backends)
- ✅ In-memory metadata store
- ✅ RocksDB-backed metadata store
- ✅ CLI tool (arustio command)
- ⚠️ gRPC API for mount operations (basic structure, needs full implementation)
- ⚠️ Raft-based distributed metadata (basic structure, needs full implementation)
- ❌ Caching layer (planned)
- ❌ REST API (planned)
- ❌ FUSE integration (planned)
- ❌ Performance optimizations (planned)

## Roadmap

1. **Phase 1 (Current)**: Core functionality
   - ✅ Basic UFS/VFS layers
   - ✅ In-memory metadata
   - ⚠️ Distributed metadata (in progress)

2. **Phase 2**: Distributed system
   - Complete Raft integration
   - Multi-node testing
   - Fault tolerance

3. **Phase 3**: Performance & Features
   - Caching layer
   - Performance optimizations
   - API layer (gRPC/REST)

4. **Phase 4**: Advanced features
   - FUSE integration
   - Replication policies
   - Tiered storage

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.

## Acknowledgments

Inspired by [Alluxio](https://www.alluxio.io/), a distributed data orchestration system.
