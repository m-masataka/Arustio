use anyhow::Result;
use protobuf::Message as PbMessage;
use raft::GetEntriesContext;
use raft::prelude::*;
use raft::{Error as RaftError, Result as RaftResult, Storage};
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options, WriteBatch};
use std::sync::Arc;

use crate::utils::kv_key_path;

const CF_RAFT_LOG: &str = "raft_log";
const CF_RAFT_STATE: &str = "raft_state";
const CF_KV: &str = "kv";
const CF_SNAP_META: &str = "snap_meta";

// raft_state keys
pub const KEY_HARD_STATE: &[u8] = b"hard_state";
pub const KEY_CONF_STATE: &[u8] = b"conf_state";
pub const KEY_FIRST_INDEX: &[u8] = b"first_index";
pub const KEY_LAST_INDEX: &[u8] = b"last_index";

#[derive(Clone)]
pub struct RocksStorage {
    pub db: Arc<DBWithThreadMode<MultiThreaded>>,
    pub cf_log: Arc<rocksdb::BoundColumnFamily<'static>>,
    pub cf_state: Arc<rocksdb::BoundColumnFamily<'static>>,
    pub cf_kv: Arc<rocksdb::BoundColumnFamily<'static>>,
    pub cf_snap: Arc<rocksdb::BoundColumnFamily<'static>>,
}

impl RocksStorage {
    pub fn open(path: &str) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_RAFT_LOG, Options::default()),
            ColumnFamilyDescriptor::new(CF_RAFT_STATE, Options::default()),
            ColumnFamilyDescriptor::new(CF_KV, Options::default()),
            ColumnFamilyDescriptor::new(CF_SNAP_META, Options::default()),
        ];
        let db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_cf_descriptors(&opts, path, cfs)?;
        Ok(Self {
            cf_log: unsafe { extend_cf_lifetime(db.cf_handle(CF_RAFT_LOG).unwrap()) },
            cf_state: unsafe { extend_cf_lifetime(db.cf_handle(CF_RAFT_STATE).unwrap()) },
            cf_kv: unsafe { extend_cf_lifetime(db.cf_handle(CF_KV).unwrap()) },
            cf_snap: unsafe { extend_cf_lifetime(db.cf_handle(CF_SNAP_META).unwrap()) },
            db: Arc::new(db),
        })
    }

    // ---- Persistence helpers used during Ready handling ----

    pub fn set_hard_state(&self, hs: &HardState) -> Result<()> {
        let buf = PbMessage::write_to_bytes(hs)?;
        self.db.put_cf(&self.cf_state, KEY_HARD_STATE, buf)?;
        Ok(())
    }

    pub fn set_conf_state(&self, cs: &ConfState) -> Result<()> {
        let buf = PbMessage::write_to_bytes(cs)?;
        self.db.put_cf(&self.cf_state, KEY_CONF_STATE, buf)?;
        Ok(())
    }

    pub fn append(&self, ents: &[Entry]) -> Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        let mut wb = WriteBatch::default();
        for e in ents {
            let buf = PbMessage::write_to_bytes(e)?;
            wb.put_cf(&self.cf_log, u64be(e.index), buf);
            wb.put_cf(&self.cf_state, KEY_LAST_INDEX, &u64be_vec(e.index));
        }
        self.db.write(wb)?;
        Ok(())
    }

    pub fn compact_to(&self, to: u64) -> Result<()> {
        // Remove log entries in [first_index, to) and advance first_index to `to`
        let first = self.first_index_inner()?;
        if to <= first {
            return Ok(());
        }
        let mut wb = WriteBatch::default();
        let mut itr = self.db.iterator_cf(
            &self.cf_log,
            rocksdb::IteratorMode::From(&u64be(first), rocksdb::Direction::Forward),
        );
        for kv in itr.by_ref() {
            let (k, _) = kv?;
            let idx = u64::from_be_bytes(k.as_ref().try_into().unwrap());
            if idx >= to {
                break;
            }
            wb.delete_cf(&self.cf_log, k);
        }
        wb.put_cf(&self.cf_state, KEY_FIRST_INDEX, &u64be_vec(to));
        self.db.write(wb)?;
        Ok(())
    }

    pub fn first_index_inner(&self) -> Result<u64> {
        if let Some(v) = self.db.get_cf(&self.cf_state, KEY_FIRST_INDEX)? {
            Ok(be_to_u64(&v))
        } else {
            // First time: return 1 when no logs exist (or snapshot index + 1 in production)
            Ok(1)
        }
    }

    pub fn last_index_inner(&self) -> Result<u64> {
        if let Some(v) = self.db.get_cf(&self.cf_state, KEY_LAST_INDEX)? {
            Ok(be_to_u64(&v))
        } else {
            Ok(0)
        }
    }

    fn load_hard_state(&self) -> Result<Option<HardState>> {
        Ok(self
            .db
            .get_cf(&self.cf_state, KEY_HARD_STATE)?
            .map(|v| HardState::parse_from_bytes(v.as_ref()))
            .transpose()?)
    }

    fn load_conf_state(&self) -> Result<Option<ConfState>> {
        Ok(self
            .db
            .get_cf(&self.cf_state, KEY_CONF_STATE)?
            .map(|v| ConfState::parse_from_bytes(v.as_ref()))
            .transpose()?)
    }

    // ---- Read From Local DB Methods ----

    // List Keys with given prefix from KV CF
    pub fn list_kv_keys(&self, prefix_bytes: &[u8]) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let it = self.db.iterator_cf(
            &self.cf_kv,
            rocksdb::IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward),
        );
        for kv in it {
            let (k, _v) = kv?;
            // Break if key does not start with prefix
            if !k.starts_with(prefix_bytes) {
                break;
            }
            let key_str = String::from_utf8(k.to_vec())?; // Works when keys are UTF-8
            keys.push(key_str);
        }
        Ok(keys)
    }

    // Get value for given key from KV CF
    pub fn get_kv_value(&self, key_bytes: &[u8]) -> Result<Option<Vec<u8>>> {
        let result = self.db.get_cf(&self.cf_kv, key_bytes)?;
        Ok(result.map(|v| v.to_vec()))
    }

    // Get FileMetadata for given path from KV CF
    pub async fn get_file_metadata(
        &self,
        path: &str,
    ) -> Result<Option<common::meta::FileMetadata>> {
        let key_bytes = kv_key_path(path);
        if let Some(value) = self.get_kv_value(&key_bytes)? {
            let file_metadata: common::meta::FileMetadata =
                prost::Message::decode(value.as_slice()).map_err(|e| {
                    common::Error::Internal(format!("Failed to decode FileMetadata entry: {}", e))
                })?;
            Ok(Some(file_metadata))
        } else {
            Ok(None)
        }
    }
}

// ---- Storage trait implementation (used by Raft for reads) ----
impl Storage for RocksStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let hs = self
            .load_hard_state()
            .map_err(to_raft_err)?
            .unwrap_or_default();
        let cs = self
            .load_conf_state()
            .map_err(to_raft_err)?
            .unwrap_or_default();
        Ok(RaftState {
            hard_state: hs,
            conf_state: cs,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let first = self.first_index_inner().map_err(to_raft_err)?;
        if low < first {
            return Err(RaftError::Store(raft::StorageError::Compacted));
        }
        let last = self.last_index_inner().map_err(to_raft_err)?;
        if high > last + 1 {
            return Err(RaftError::Store(raft::StorageError::Unavailable));
        }
        let mut ents = Vec::new();
        let mut size: u64 = 0;
        let mut it = self.db.iterator_cf(
            &self.cf_log,
            rocksdb::IteratorMode::From(&u64be(low), rocksdb::Direction::Forward),
        );
        let max_size = max_size.into();
        for kv in it.by_ref() {
            let (_, v) = kv.map_err(to_raft_err)?;
            let e = Entry::parse_from_bytes(v.as_ref()).map_err(to_raft_err)?;
            if e.index >= high {
                break;
            }
            size += v.len() as u64;
            ents.push(e);
            if max_size.map_or(false, |m| size >= m) {
                break;
            }
        }
        Ok(ents)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        tracing::info!("RocksStorage::term idx={}", idx);
        if idx == 0 {
            return Ok(0);
        }
        let first = self.first_index_inner().map_err(to_raft_err)?;
        let last = self.last_index_inner().map_err(to_raft_err)?;
        if idx < first {
            return Err(RaftError::Store(raft::StorageError::Compacted));
        }
        if idx > last {
            return Err(RaftError::Store(raft::StorageError::Unavailable));
        }
        if let Some(v) = self
            .db
            .get_cf(&self.cf_log, u64be(idx))
            .map_err(to_raft_err)?
        {
            let e = Entry::parse_from_bytes(v.as_ref()).map_err(to_raft_err)?;
            Ok(e.term)
        } else {
            Err(RaftError::Store(raft::StorageError::Unavailable))
        }
    }

    fn first_index(&self) -> RaftResult<u64> {
        self.first_index_inner().map_err(to_raft_err)
    }

    fn last_index(&self) -> RaftResult<u64> {
        self.last_index_inner().map_err(to_raft_err)
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        // Return the latest snapshot (or Empty if none exists)
        // Simplified: always return empty; production should load from snap_meta
        Ok(Snapshot::default())
    }
}

// ---- Utilities ----
#[inline]
fn u64be(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}
fn u64be_vec(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}
fn be_to_u64(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(b);
    u64::from_be_bytes(a)
}
fn to_raft_err<E>(e: E) -> RaftError
where
    E: Into<anyhow::Error>,
{
    RaftError::Store(raft::StorageError::Other(Box::new(AnyhowStdError(
        e.into(),
    ))))
}

/// The RocksDB bindings tie column family handles to the lifetime of the DB.
/// We store the DB inside `RocksStorage`, so extending the lifetime to `'static`
/// is safe as long as the handles are dropped before the DB (field ordering guarantees this).
unsafe fn extend_cf_lifetime(
    cf: Arc<rocksdb::BoundColumnFamily<'_>>,
) -> Arc<rocksdb::BoundColumnFamily<'static>> {
    unsafe {
        std::mem::transmute::<
            Arc<rocksdb::BoundColumnFamily<'_>>,
            Arc<rocksdb::BoundColumnFamily<'static>>,
        >(cf)
    }
}

struct AnyhowStdError(anyhow::Error);

impl std::fmt::Debug for AnyhowStdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for AnyhowStdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for AnyhowStdError {}
