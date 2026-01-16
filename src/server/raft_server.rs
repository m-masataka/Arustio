use crate::meta::MetaCmd;
use crate::meta::meta_api_server;
use crate::raftio;
use crate::common::{Error, NodeConfig, Result};
use crate::metadata::raft::apply::apply_to_kv;
use crate::metadata::raft::cluster::RaftClusterState;
use crate::metadata::raft::linearizable_read::{LinearizableReadHandle, LinearizableReadRequest};
use crate::metadata::raft::raft_transport::{GrpcTransport, PeerStore, Transport};
use crate::metadata::raft::rocks_store::{KEY_CONF_STATE, RocksStorage};
use crate::meta::{
    GetBlockNodesRequest,
    GetBlockNodesResponse,
    GetFileMetaRequest,
    GetFileMetaResponse,
    GetLeaderRequest,
    GetLeaderResponse,
};
use protobuf::CodedInputStream; // If using protobuf 2.x
// If using protobuf 3.x or later, remove this line and use Message::merge_from_bytes directly in your code.
use protobuf::Message;
use raft::default_logger;
use raft::prelude::Message as RaftMessage;
use raft::prelude::Snapshot;
use raft::prelude::{ConfChange, ConfChangeV2, Config, EntryType, ReadState};
use raft::{RawNode, StateRole};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

struct PendingLinearizableRead {
    request: LinearizableReadRequest,
    read_index: Option<u64>,
}

fn try_complete_reads(pending: &mut HashMap<Vec<u8>, PendingLinearizableRead>, applied_index: u64) {
    let ready: Vec<Vec<u8>> = pending
        .iter()
        .filter_map(|(ctx, entry)| {
            entry
                .read_index
                .filter(|idx| applied_index >= *idx)
                .map(|_| ctx.clone())
        })
        .collect();
    for ctx in ready {
        if let Some(entry) = pending.remove(&ctx) {
            let _ = entry.request.completion.send(Ok(()));
        }
    }
}

fn fail_pending_reads(pending: &mut HashMap<Vec<u8>, PendingLinearizableRead>, message: &str) {
    for (_, entry) in pending.drain() {
        let _ = entry
            .request
            .completion
            .send(Err(Error::Internal(message.to_string())));
    }
}

pub async fn start_raft_server(
    node_id: u64,
    node_cfg: NodeConfig,
    storage: RocksStorage,
    read_rx: mpsc::Receiver<LinearizableReadRequest>,
    read_handle: LinearizableReadHandle,
) -> Result<()> {
    tracing::info!("Starting Raft server with node ID {}", node_id);
    // Raft starting
    let cfg = Config {
        id: node_id,
        election_tick: 10, // ≈1s if 1 tick = 100ms
        heartbeat_tick: 1, // 100ms
        max_inflight_msgs: 256,
        check_quorum: true,
        pre_vote: true,
        // Set `applied` if logs are already applied; leave as 0 otherwise
        ..Default::default()
    };
    tracing::info!("Start Raft Server");
    cfg.validate()
        .map_err(|e| Error::Internal(format!("Raft Config validate: {e}")))?;


    let st = storage.clone();
    let grpc_storage = st.clone();
    let read_handle_for_service = read_handle.clone();

    tracing::info!("Starting Raft server with node ID {}", node_id);
    let cluster_state = Arc::new(RaftClusterState::new(node_id));

    // Run the bootstrap check and initialization once at startup
    bootstrap_if_needed(&storage, node_cfg.clone())
        .map_err(|e| Error::Internal(format!("bootstrap_if_needed: {e}")))?;

    let logger = default_logger();
    // Assume the storage is empty (no HardState/ConfState)
    let rn = RawNode::new(&cfg, storage, &logger)
        .map_err(|e| Error::Internal(format!("RawNode::new: {e}")))?;

    let (net_tx, net_rx) = mpsc::channel::<RaftMessage>(2048);
    let (prop_tx, prop_rx) = mpsc::channel::<MetaCmd>(1024);
    let listen_addr = node_cfg.raft_listen.clone();

    // Define PeerStore struct if missing
    let peers: Vec<(u64, String)> = node_cfg
        .peers
        .iter()
        .map(|p| (p.id, p.addr.clone()))
        .collect();
    let peer_store = PeerStore::new(peers);

    tokio::spawn({
        let listen_addr = listen_addr.clone();
        let net_tx = net_tx.clone();
        let prop_tx = prop_tx.clone();
        let cluster_state = cluster_state.clone();
        let peer_store = peer_store.clone();
        let storage = grpc_storage.clone();
        let read_handle = read_handle_for_service.clone();
        async move {
            let _ = start_raft_grpc_server(
                &listen_addr,
                net_tx,
                prop_tx,
                cluster_state,
                peer_store.clone(),
                read_handle,
                storage,
            )
            .await;
        }
    });

    let tx = GrpcTransport::new(node_id, peer_store, net_tx.clone());
    run_raft_node(rn, st, net_rx, prop_rx, read_rx, tx, cluster_state)
        .await
        .map_err(|e| Error::Internal(format!("run_raft_node: {e}")))?;
    Ok(())
}

pub async fn run_raft_node(
    mut rn: RawNode<RocksStorage>,
    st: RocksStorage,
    mut net_rx: mpsc::Receiver<RaftMessage>,
    mut prop_rx: mpsc::Receiver<MetaCmd>,
    mut read_rx: mpsc::Receiver<LinearizableReadRequest>,
    tx: impl Transport,
    cluster_state: Arc<RaftClusterState>,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    let mut last_applied: u64 = 0;
    let mut pending_reads: HashMap<Vec<u8>, PendingLinearizableRead> = HashMap::new();
    let mut next_read_ctx: u64 = 1;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let _ = rn.tick();
            },
            Some(msg) = net_rx.recv() => {
                // Raft message received from another node
                tracing::debug!("Received Raft message: {:?}", msg);
                rn.step(msg)?;
            },
            Some(cmd) = prop_rx.recv()=> {
                let data = prost::Message::encode_to_vec(&cmd);
                rn.propose(vec![], data)?;
            }
            Some(read_req) = read_rx.recv() => {
                let ctx = next_read_ctx.to_be_bytes().to_vec();
                next_read_ctx = next_read_ctx.wrapping_add(1);
                pending_reads.insert(
                    ctx.clone(),
                    PendingLinearizableRead {
                        request: read_req,
                        read_index: None,
                    },
                );
                rn.read_index(ctx);
            }
        }

        if rn.has_ready() {
            tracing::debug!("Raft node has ready");
            let mut rd = rn.ready();
            // Check the read state
            if let Some(ss) = rd.ss() {
                tracing::debug!(
                    "SoftState changed: leader_id={} raft_state={:?}",
                    ss.leader_id,
                    ss.raft_state
                );
                cluster_state.set_leader(ss.leader_id);
                if ss.raft_state != StateRole::Leader {
                    fail_pending_reads(&mut pending_reads, "node stepped down from leader role");
                }
            }
            // 1) Persist: always save HardState / Entries / received Snapshots first
            if let Some(hs) = rd.hs().cloned() {
                tracing::debug!("Persisting HardState: {:?}", hs);
                st.set_hard_state(&hs)?;
                cluster_state.set_term(hs.term);
            }
            if !rd.entries().is_empty() {
                tracing::debug!("Persisting {} log entries", rd.entries().len());
                st.append(rd.entries())?;
            }
            if !rd.snapshot().is_empty() {
                tracing::debug!(
                    "Persisting snapshot at index {}",
                    rd.snapshot().get_metadata().get_index()
                );
                // Apply a received snapshot by replacing the KV DB, then install it on the Raft side
                apply_snapshot_to_kv(&st, rd.snapshot())?;
            }

            // 2) Send: forward to other nodes
            for m in rd.take_messages() {
                tracing::debug!("Sending Raft message: {:?}", m);
                tx.send(m).await?;
            }
            // Send persisted_messages only after persistence (PreVote currently sits here)
            for m in rd.take_persisted_messages() {
                tracing::debug!(
                    "Sending Raft message (persisted): from={} to={} type={:?}",
                    m.get_from(),
                    m.get_to(),
                    m.get_msg_type()
                );
                tx.send(m).await?;
            }

            // Apply: feed committed entries to the KV state machine in order.
            for ent in rd.take_committed_entries() {
                match ent.entry_type {
                    EntryType::EntryConfChange => {
                        tracing::debug!("Applying committed conf change: {:?}", ent);
                        let mut cc = ConfChange::default();
                        let mut cis = CodedInputStream::from_bytes(ent.get_data());
                        cc.merge_from(&mut cis)?;
                        let cs = rn.apply_conf_change(&cc)?;
                        st.set_conf_state(&cs)?;
                    }
                    EntryType::EntryConfChangeV2 => {
                        tracing::debug!("Applying committed conf change v2: {:?}", ent);
                        let mut cc = ConfChangeV2::default();
                        <ConfChangeV2 as protobuf::Message>::merge_from_bytes(
                            &mut cc,
                            ent.get_data(),
                        )?;
                        let cs = rn.apply_conf_change(&cc)?;
                        st.set_conf_state(&cs)?;
                    }
                    EntryType::EntryNormal => {
                        tracing::debug!("Applying committed normal entry: {:?}", ent);
                        if !ent.data.is_empty() {
                            let cmd: MetaCmd = prost::Message::decode(ent.data.as_ref())?;
                            apply_to_kv(&st, cmd)?; // Updates the KV column family
                        }
                    }
                }
                last_applied = ent.index;
                try_complete_reads(&mut pending_reads, last_applied);
            }

            // Handle read states for linearizable reads
            for ReadState { index, request_ctx } in rd.take_read_states() {
                if let Some(pending) = pending_reads.get_mut(&request_ctx) {
                    pending.read_index = Some(index);
                } else {
                    tracing::warn!("Unknown read context received len={}", request_ctx.len());
                }
            }
            try_complete_reads(&mut pending_reads, last_applied);

            // Advance: inform Raft that Ready handling is complete
            let mut light_rd = rn.advance(rd);
            tracing::debug!("Advanced Raft node, LightReady: {:?}", light_rd);
            for m in light_rd.take_messages() {
                tracing::debug!("Sending Raft message (light): {:?}", m);
                tx.send(m).await?;
            }

            for ent in light_rd.take_committed_entries() {
                tracing::debug!("Applying committed entry: {:?}", ent);
                match ent.entry_type {
                    EntryType::EntryConfChange => {
                        tracing::debug!("Applying committed conf change: {:?}", ent);
                        let mut cc = ConfChange::default();
                        let mut cis = CodedInputStream::from_bytes(ent.get_data());
                        cc.merge_from(&mut cis)?;
                        let cs = rn.apply_conf_change(&cc)?;
                        st.set_conf_state(&cs)?;
                    }
                    EntryType::EntryConfChangeV2 => {
                        tracing::debug!("Applying committed conf change v2: {:?}", ent);
                        let mut cc = ConfChangeV2::default();
                        <ConfChangeV2 as protobuf::Message>::merge_from_bytes(
                            &mut cc,
                            ent.get_data(),
                        )?;
                        let cs = rn.apply_conf_change(&cc)?;
                        st.set_conf_state(&cs)?;
                    }
                    EntryType::EntryNormal => {
                        tracing::debug!("Applying committed normal entry: {:?}", ent);
                        if !ent.data.is_empty() {
                            let cmd: MetaCmd = prost::Message::decode(ent.data.as_ref())?;
                            apply_to_kv(&st, cmd)?;
                        }
                    }
                }
                last_applied = ent.index;
                try_complete_reads(&mut pending_reads, last_applied);
            }
        }

        // Optional: create snapshots when the log exceeds a threshold
        maybe_make_snapshot(&st, &mut rn).await?;
    }
}

async fn maybe_make_snapshot(_st: &RocksStorage, _rn: &mut RawNode<RocksStorage>) -> Result<()> {
    // TODO: implement snapshot creation logic
    // let last_index = st.last_index_inner()?;
    // let first_index = st.first_index_inner()?;
    // let log_count = last_index.saturating_sub(first_index);

    // const SNAPSHOT_LOG_THRESHOLD: u64 = 1000;

    // if log_count >= SNAPSHOT_LOG_THRESHOLD {
    //     let snap_index = last_index;
    //     let snap_term = {
    //         let ents = st.entries(first_index, snap_index + 1, NO_LIMIT)?;
    //         ents.last().map(|e| e.term).unwrap_or(0)
    //     };

    //     tracing::info!("Creating snapshot at index {}, term {}", snap_index, snap_term);

    //     // Request snapshot creation
    //     rn.snapshot(snap_index)?;

    //     // Delete older logs after the snapshot is created
    //     let new_first = snap_index + 1;
    //     st.set_first_index(new_first)?;

    //     if last_index >= first_index {
    //         st.db.delete_range_cf(st.cf_log, &u64be(first_index), &u64be(new_first))?;
    //     }

    //     tracing::info!("Snapshot created and logs up to index {} deleted", snap_index);
    // }

    Ok(())
}

fn apply_snapshot_to_kv(_st: &RocksStorage, _snap: &Snapshot) -> Result<()> {
    // TODO: implement snapshot application logic

    // // 0) Validate snapshot metadata
    // let meta = snap.get_metadata();
    // let snap_idx = meta.get_index();
    // let snap_term = meta.get_term();

    // // Skip if an equal-or-newer snapshot was already applied
    // let last = st.last_index_inner()?;
    // if snap_idx <= last {
    //     // Treat as stale/already applied
    //     return Ok(());
    // }

    // // 1) Decode the data (format is implementation-defined)
    // let data = snap.get_data();
    // let sdata: snap::SnapshotData = prost::Message::decode(data.as_ref())?;

    // // 2) Replace the KV column family (clear then bulk write)
    // //    delete_range is faster for clearing; dropping/recreating the CF also works.
    // st.db.delete_range_cf(st.cf_kv, [], [0xFF])?; // full delete (rocksdb 0.22 accepts &[u8])

    // let mut wb = WriteBatch::default();
    // for kv in sdata.kvs {
    //     wb.put_cf(st.cf_kv, kv.key, kv.value);
    // }
    // // Update Raft boundary keys (first/last)
    // wb.put_cf(st.cf_state, KEY_FIRST_INDEX, &u64be_vec(snap_idx + 1));
    // wb.put_cf(st.cf_state, KEY_LAST_INDEX,  &u64be_vec(snap_idx));
    // // Persist the ConfState too (important)
    // let cs = meta.get_conf_state();
    // let mut cs_buf = Vec::new();
    // prost::Message::encode(cs, &mut cs_buf)?;
    // wb.put_cf(st.cf_state, KEY_CONF_STATE, cs_buf);

    // // Delete older RaftLog entries (first..=snap_idx)
    // // Since first/last were rewritten above, clean that region as well
    // // Do it with a range delete
    // if st.last_index_inner()? > 0 {
    //     let first = st.first_index_inner()?;
    //     if snap_idx >= first {
    //         st.db.delete_range_cf(st.cf_log, &u64be(first), &u64be(snap_idx + 1))?;
    //     }
    // }

    // st.db.write(wb)?;

    // // Persist snapshot metadata (file paths, etc.) if needed
    // // st.db.put_cf(st.cf_snap, b"last", ...)

    // // At this point the state machine matches the snapshot contents
    // // and the Raft first/last pointers align with the snapshot.
    Ok(())
}

// Called once right after opening storage during startup
fn bootstrap_if_needed(st: &RocksStorage, node_cfg: NodeConfig) -> anyhow::Result<()> {
    // Return early if initialization already happened
    if st.db.get_cf(&st.cf_state, KEY_CONF_STATE)?.is_some() {
        return Ok(());
    }
    // Build the voters list from configured peers
    let voters: Vec<u64> = node_cfg.peers.iter().map(|p| p.id).collect();
    tracing::debug!("bootstrap: ConfState voters = {:?}", voters);

    let mut cs = raft::prelude::ConfState::default();
    cs.voters = voters;

    // Persist the ConfState
    st.set_conf_state(&cs)?;

    // Default HardState works fine
    st.set_hard_state(&raft::prelude::HardState::default())?;
    Ok(())
}

// gRPC Raft service implementation
pub struct RaftService {
    net_tx: mpsc::Sender<RaftMessage>,
}
impl RaftService {
    pub fn new(net_tx: mpsc::Sender<RaftMessage>) -> Self {
        Self { net_tx }
    }
}

#[tonic::async_trait]
impl raftio::raft_server::Raft for RaftService {
    async fn send(&self, req: Request<raftio::Bytes>) -> std::result::Result<Response<()>, Status> {
        let data = req.into_inner().data;
        let msg = raft::prelude::Message::parse_from_bytes(&data)
            .map_err(|_| Status::invalid_argument("decode raft message"))?;

        self.net_tx
            .send(msg)
            .await
            .map_err(|_| Status::unavailable("node busy/dropped"))?;
        Ok(Response::new(()))
    }
}

pub async fn start_raft_grpc_server(
    listen: &str,
    net_tx: mpsc::Sender<RaftMessage>,
    prop_tx: mpsc::Sender<MetaCmd>,
    cluster_state: Arc<RaftClusterState>,
    peer_store: PeerStore,
    read_handle: LinearizableReadHandle,
    storage: RocksStorage,
) -> anyhow::Result<()> {
    Server::builder()
        .add_service(raftio::raft_server::RaftServer::new(RaftService::new(
            net_tx,
        )))
        .add_service(meta_api_server::MetaApiServer::new(MetaService::new(
            prop_tx,
            cluster_state.clone(),
            peer_store.clone(),
            read_handle,
            storage,
        )))
        .serve(listen.parse()?)
        .await?;
    Ok(())
}

// MetadataService
pub struct MetaService {
    prop_tx: mpsc::Sender<MetaCmd>,
    cluster_state: Arc<RaftClusterState>,
    peer_store: PeerStore,
    read_handle: LinearizableReadHandle,
    storage: RocksStorage,
}

impl MetaService {
    pub fn new(
        prop_tx: mpsc::Sender<MetaCmd>,
        cluster_state: Arc<RaftClusterState>,
        peer_store: PeerStore,
        read_handle: LinearizableReadHandle,
        storage: RocksStorage,
    ) -> Self {
        Self {
            prop_tx,
            cluster_state,
            peer_store,
            read_handle,
            storage,
        }
    }
}

#[tonic::async_trait]
impl meta_api_server::MetaApi for MetaService {
    async fn apply(&self, req: Request<MetaCmd>) -> std::result::Result<Response<()>, Status> {
        let cmd = req.into_inner();

        self.prop_tx
            .send(cmd)
            .await
            .map_err(|_| Status::unavailable("Raft node not ready"))?;

        Ok(Response::new(()))
    }

    async fn get_leader(
        &self,
        _req: Request<GetLeaderRequest>,
    ) -> std::result::Result<Response<GetLeaderResponse>, Status> {
        let (leader_id, term) = self.cluster_state.get_leader();
        let leader_ip = self.peer_store.get_peer_ip(leader_id).await.to_string();

        let response = GetLeaderResponse {
            leader_id,
            term,
            leader_ip,
        };

        Ok(Response::new(response))
    }

    async fn get_file_meta(
        &self,
        req: Request<GetFileMetaRequest>,
    ) -> std::result::Result<Response<GetFileMetaResponse>, Status> {
        let path = req.into_inner().full_path;
        if !self.cluster_state.is_leader() {
            // if not leader, return error
            return Err(Status::failed_precondition("not leader"));
        }
        self.read_handle
            .wait()
            .await
            .map_err(|e| Status::internal(format!("read barrier failed: {}", e)))?;
        let meta_opt = self
            .storage
            .get_file_metadata(&path)
            .await
            .map_err(|e| Status::internal(format!("failed to get file meta: {}", e)))?;

        let response = GetFileMetaResponse { metadata: meta_opt };
        Ok(Response::new(response))
    }

    async fn get_block_nodes(
        &self,
        _req: Request<GetBlockNodesRequest>,
    ) -> std::result::Result<Response<GetBlockNodesResponse>, Status> {
        if !self.cluster_state.is_leader() {
            return Err(Status::failed_precondition("not leader"));
        }
        self.read_handle
            .wait()
            .await
            .map_err(|e| Status::internal(format!("read barrier failed: {}", e)))?;
        let nodes = self
            .storage
            .get_block_nodes()
            .await
            .map_err(|e| Status::internal(format!("failed to get block nodes: {}", e)))?;

        let response = GetBlockNodesResponse { nodes: nodes };
        Ok(Response::new(response))
    }
}
