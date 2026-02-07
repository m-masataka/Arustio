# Architecture Overview

This document describes the current runtime architecture.

## Components

### Client FileSystem (VFS)

The client owns the FileSystem API and is the primary entry point. It performs:

- Path operations (mkdir, ls, stat)
- Reads and writes
- Direct access to UFS for object data
- Cache reads/writes via Block Nodes
- Metadata lookups via Metadata Server

### Metadata Server (Raft + RocksDB)

The metadata server is authoritative for namespace and file metadata. It uses:

- **RocksDB** for persistence
- **Raft** for consistency and replication

Responsibilities:

- Path to metadata mapping
- File ID allocation and tracking
- Mount table storage
- Block node registry

### Block Nodes (Cache-Only)

Block nodes serve cached blocks and store them in a local cache. They are not authoritative for data and can be rebuilt from UFS and metadata as needed.

### UFS (Unified Storage)

UFS abstracts backing stores (S3/MinIO, local, memory). The client FileSystem reads/writes UFS directly.

## Read Path

1. Client FileSystem resolves metadata via Metadata Server.
2. Client attempts to read blocks from Block Nodes (cache).
3. Cache miss falls back to UFS read.
4. Read data is written back to Block Nodes for future hits.

## Write Path

1. Client FileSystem writes to UFS.
2. Metadata is updated via Metadata Server.
3. Written blocks are optionally cached on Block Nodes.

## Notes

- Block Nodes are cache-only and can be scaled independently.
- Metadata server is the source of truth for namespace and file metadata.
- UFS is the source of truth for object data.
