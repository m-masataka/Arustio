use crate::raft::rocks_store::RocksStorage;
use crate::utils::{kv_key_mount_path, kv_key_path, u64be_bytes};
use common::Result;
use common::meta::MetaCmd;
use common::meta::Mount;
use common::meta::meta_cmd::Op;
use prost::Message;
use rocksdb::WriteBatch;

pub fn apply_to_kv(st: &RocksStorage, cmd: MetaCmd) -> Result<()> {
    tracing::info!("Applying MetaCmd to KV store: {:?}", cmd);

    let mut wb = WriteBatch::default();
    match cmd.op {
        Some(Op::Mkdir(m)) => {
            let key = kv_key_path(m.full_path);
            let val = u64be_bytes(m.inode);
            wb.put_cf(&st.cf_kv, key, val);
        }
        Some(Op::Load(l)) => {
            // TODO
        }
        Some(Op::Mount(m)) => {
            let key = kv_key_mount_path(m.full_path.clone());
            let mount_entry = Mount {
                full_path: m.full_path,
                ufs_config: m.ufs_config,
                description: m.description,
            };
            let mut val = Vec::new();
            mount_entry.encode(&mut val).map_err(|e| {
                common::Error::Internal(format!("Failed to encode Mount entry: {}", e))
            })?;
            wb.put_cf(&st.cf_kv, key, val);
        }
        Some(Op::Unmount(u)) => {
            let key = kv_key_mount_path(u.full_path);
            wb.delete_cf(&st.cf_kv, key);
        }
        Some(Op::PutFileMeta(p)) => {
            let key = kv_key_path(p.full_path.clone());
            let mut val = Vec::new();
            match &p.file_metadata {
                Some(file_metadata) => {
                    file_metadata.encode(&mut val).map_err(|e| {
                        common::Error::Internal(format!(
                            "Failed to encode FileMetadata entry: {}",
                            e
                        ))
                    })?;
                    wb.put_cf(&st.cf_kv, key, val);
                }
                None => {
                    return Err(common::Error::Internal("FileMetadata is None".to_string()));
                }
            }
        }
        None => {
            tracing::warn!("Received MetaCmd with no operation");
        }
    }
    st.db
        .write(wb)
        .map_err(|e| common::Error::Internal(format!("apply_to_kv write: {e}")))?;
    // TODO: flesh out the state-machine (KV CF) apply logic
    Ok(())
}
