use crate::metadata::raft::rocks_store::RocksStorage;
use crate::metadata::utils::{
    kv_block_meta_key, kv_block_node_key, kv_key_mount_path, kv_key_path, u64be_bytes,
};
use crate::common::{
    Result,
    Error
};
use crate::meta::{
    MetaCmd,
    Mount,
    meta_cmd::Op,
};
use prost::Message;
use rocksdb::WriteBatch;

pub fn apply_to_kv(st: &RocksStorage, cmd: MetaCmd) -> Result<()> {
    tracing::debug!("Applying MetaCmd to KV store: {:?}", cmd);

    let mut wb = WriteBatch::default();
    match cmd.op {
        Some(Op::Mkdir(m)) => {
            let key = kv_key_path(m.full_path);
            let val = u64be_bytes(m.inode);
            wb.put_cf(&st.cf_kv, key, val);
        }
        Some(Op::Load(_)) => {
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
                Error::Internal(format!("Failed to encode Mount entry: {}", e))
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
                        Error::Internal(format!(
                            "Failed to encode FileMetadata entry: {}",
                            e
                        ))
                    })?;
                    wb.put_cf(&st.cf_kv, key, val);
                }
                None => {
                    return Err(Error::Internal("FileMetadata is None".to_string()));
                }
            }
        }
        Some(Op::AddBlockNode(a)) => {
            let mut val = Vec::new();
            match &a.node {
                Some(node) => {
                    let key = kv_block_node_key(node.node_id.clone());
                    node.encode(&mut val).map_err(|e| {
                        Error::Internal(format!("Failed to encode BlockNode entry: {}", e))
                    })?;
                    wb.put_cf(&st.cf_kv, key, val);
                }
                None => {
                    return Err(Error::Internal(
                        "CacheNode is None in AddCacheNode".to_string(),
                    ));
                }
            }
        }
        Some(Op::RemoveBlockNode(r)) => {
            let key = kv_block_node_key(r.node_id);
            wb.delete_cf(&st.cf_kv, key);
        }
        Some(Op::UpdateBlockNodeStatus(_u)) => {
            // No-op for now; implement as needed
        }
        Some(Op::PutBlockMeta(p)) => {
            let file_id = p.file_id.clone();
            let key = kv_block_meta_key(file_id, p.index);
            let mut val = Vec::new();
            p.encode(&mut val).map_err(|e| {
                Error::Internal(format!("Failed to encode PutBlockMeta entry: {}", e))
            })?;
            wb.put_cf(&st.cf_kv, key, val);
        }
        None => {
            tracing::warn!("Received MetaCmd with no operation");
        }
    }
    st.db
        .write(wb)
        .map_err(|e| Error::Internal(format!("apply_to_kv write: {e}")))?;
    // TODO: flesh out the state-machine (KV CF) apply logic
    Ok(())
}
