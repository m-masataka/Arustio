# Arustio

Arustio is a distributed file system in Rust inspired by Alluxio. A client-side FileSystem provides a unified namespace over object storage backends, while metadata and cache are served by dedicated nodes.

## What It Is

- **Client-side FileSystem (VFS)**: The client library owns the FileSystem API and talks to the metadata server and block nodes.
- **Metadata Server (Raft + RocksDB)**: Maintains the namespace, file metadata, and mount table with consistent reads/writes.
- **Block Nodes (Cache)**: Serve cached file blocks. These are cache-only nodes.
- **UFS (Unified Storage)**: Object storage backends (S3/MinIO, local, memory). The client FileSystem reads/writes UFS directly.

## Architecture (Current)

```
┌──────────────────────────────┐
│   Client (FileSystem / VFS)  │
│  - path ops / read / write   │
│  - talks to Metadata Server  │
│  - talks to Block Nodes      │
│  - accesses UFS directly     │
└───────────────┬──────────────┘
                │
        ┌───────┴────────┐
        │                │
┌───────▼────────┐  ┌────▼─────────┐
│ Metadata Server│  │  Block Nodes │
│ (Raft+RocksDB) │  │   (Cache)    │
└───────┬────────┘  └────┬─────────┘
        │                 
        │          (cached blocks)
        │
        ▼
┌──────────────────────────────┐
│     UFS (Object Storage)     │
│  S3 / MinIO / Local / Memory │
└──────────────────────────────┘
```

More detail: `docs/architecture.md`.

## Features

- Mount multiple storage backends into a single namespace
- Client-side FileSystem with async read/write
- Metadata server backed by Raft + RocksDB
- Block cache nodes for hot data
- Object storage backends via UFS

## Project Structure

- `src/client`: client-side FileSystem, cache client, metadata client
- `src/server`: metadata server and block node service
- `src/metadata`: RocksDB + Raft metadata store
- `src/ufs`: object storage backends
- `src/block`: block/cache management
- `proto`: gRPC definitions
- `docs`: architecture and design notes

## Quick Start (Docker)

Uses `docker-compose.yml` (MinIO + Arustio server).

```bash
docker-compose up -d
sleep 10

# Mount MinIO bucket
./local/mount.sh
```

MinIO console: `http://localhost:9001` (default: `minioadmin` / `minioadmin`).

## CLI (arustio)

```bash
# list mounts
arustio fs list

# mount S3-compatible (MinIO)
arustio fs mount \
  -o access_key_id=minioadmin \
  -o secret_access_key=minioadmin \
  -o endpoint=http://minio:9000 \
  /data s3a://test-bucket

# read file to local
arustio fs copy-to-local /data/bigfile.bin /data/bigfile.bin
```

## Development Status

- Client-side FileSystem is the primary entry point
- Metadata server uses Raft + RocksDB
- Block nodes act as cache-only nodes
- UFS access happens from the client FileSystem

## License

Apache License 2.0. See `LICENSE`.
