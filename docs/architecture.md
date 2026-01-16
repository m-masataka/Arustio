# Architecture Overview

This document outlines the major runtime subsystems that complement the high-level overview in the README. It focuses on two components that commonly raise questions for operators: metadata management and the distributed cache tier.

## Metadata Management (Raft + RocksDB)

Each Arustio server embeds a metadata store that is backed by RocksDB for persistence and coordinated through Raft for consistency.

### Write Path

1. A gRPC handler or internal task creates a `MetaCmd` (e.g., `PutFileMeta`, `Mount`, `AddCacheNode`).
2. The `RaftClient` resolves the current leader and submits the command via the metadata API.
3. The leader appends the entry to its Raft log and replicates it to followers. Once a majority acknowledges the entry, it is committed.
4. Every node applies the entry in order through `apply_to_kv`, writing the change into RocksDB. Because RocksDB is the authoritative store, nodes can restart and rebuild state from disk, while Raft guarantees the same order of updates everywhere.

### Read Path

- **Linearizable Reads**: Operations that require strong consistency (e.g., CLI commands manipulating the namespace) go through the `LinearizableReadHandle`. The handle coordinates with Raft to ensure the local store has applied up-to-date entries before responding.
- **Local Reads**: Non-critical lookups (listing cached entries, health probes) can read directly from RocksDB. Data is still fresh because Raft continually applies log entries, but full linearizability is optional.

This split delivers strong consistency when needed while keeping read latency low in hot paths.

## Cache Layer

Fetching large objects from remote storage repeatedly is expensive, so Arustio provides an integrated cache tier.

### Components

- **Cache Nodes**: Each node advertises itself (ID, URL, weight, zone) via the metadata Raft log. These nodes can be colocated with Arustio servers or run as dedicated cache daemons.
- **Hash Ring**: A consistent hash ring (`HashRing`) maps `(file_id, block_index)` pairs to cache node IDs, providing even distribution and stable assignments when membership changes.
- **CacheManager**: Maintains the latest node list, constructs the hash ring, and stores hot blocks in an in-process Moka cache for ultra-fast access.
- **BlockAccessor**: Plans block boundaries and asks the cache manager which node owns each block. It is also responsible for writing freshly read UFS blocks back into the cache tier.

### Data Flow

1. A VFS read determines the needed blocks and asks `BlockAccessor` for node placements.
2. For each block, the cache manager first checks the local Moka cache. If it misses, the system issues a network read to the owning cache node. If that miss occurs as well, the request falls back to the corresponding UFS backend.
3. When data has to be fetched from UFS, the VFS streams it to the client while simultaneously chunking and writing blocks back through `CacheManager::store_block` so subsequent reads become cache hits.

By decoupling ownership (hash ring) from local caching (Moka), the system can scale horizontally and tolerate node churn without central coordination. Cache membership is simply another piece of metadata replicated through Raft, so all servers share a consistent view.
